import logging
import boto3
import threading
from datetime import datetime, timezone 
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = 'exchange_broker:9092'
KAFKA_TOPIC = 'crypto_exchange_trades'
DYNAMODB_ENDPOINT = os.getenv('DYNAMODB_ENDPOINT', 'http://dynamodb-local:8000')
AWS_REGION = 'us-east-1'
TRADES_TABLE = 'MarketTrades'
PREDICTIONS_TABLE = 'PricePredictions'
MODEL_PATH = '/tmp/gbt_price_model'
PREDICT_MINUTES_AHEAD = 5
MIN_ROWS_TO_TRAIN = 500
RETRAIN_INTERVAL = 300

# PART 1: DYNAMODB SETUP

def get_dynamo_client():
    return boto3.client(
        'dynamodb',
        region_name=AWS_REGION,
        endpoint_url=DYNAMODB_ENDPOINT,
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'DUMMYIDEXAMPLE'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'DUMMYEXAMPLEKEY')
    )

def get_dynamo_resource():
    return boto3.resource(
        'dynamodb',
        region_name=AWS_REGION,
        endpoint_url=DYNAMODB_ENDPOINT,
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'DUMMYIDEXAMPLE'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'DUMMYEXAMPLEKEY')
    )

def check_predictions_table():
    """Create PricePredictions table if it doesn't exist."""
    client = get_dynamo_client()
    try:
        client.describe_table(TableName=PREDICTIONS_TABLE)
        logging.info(f" Table '{PREDICTIONS_TABLE}' already exists")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logging.info(f"Creating table '{PREDICTIONS_TABLE}' ...")
            try:
                client.create_table(
                    TableName=PREDICTIONS_TABLE,
                    KeySchema=[
                        {'AttributeName': 'Symbol',   'KeyType': 'HASH'},
                        {'AttributeName': 'PredictedAt',   'KeyType': 'RANGE'},
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'Symbol',   'AttributeType': 'S'},
                        {'AttributeName': 'PredictedAt',   'AttributeType': 'S'},
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                waiter = client.get_waiter('table_exists')
                waiter.wait(TableName=PREDICTIONS_TABLE, WaiterConfig={'Delay': 2, 'MaxAttempts': 10})
                logging.info(f" Table '{PREDICTIONS_TABLE}' created and active.")
            except Exception as creation_err:
                    logging.error(f"Failed to create DynamoDB Table: {creation_err}")

# Part 2. Load historical dnata from dynamodb for training

_scan_lock = threading.Lock()

def load_historical_data(spark: SparkSession):
    """
    Pulls all rows from MarketTrades and returns a Spark DataFrame
    ready for feature engineering
    """
    import time
    resource = get_dynamo_resource()
    table = resource.Table(TRADES_TABLE)
    items = []
    with _scan_lock:
        try:

            response =  table.scan(Limit=600)
            items.extend(response.get('Items', []))
            while 'LastEvaluatedKey' in response:
                time.sleep(0.1)
                response = table.scan(Limit=600, ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response.get('Items', []))
        except Exception as e:
            logging.error(f"Scan failed: {e}")
            return None
        
    if len(items) < MIN_ROWS_TO_TRAIN:
        logging.warning(f" Only {len(items)} rows - need {MIN_ROWS_TO_TRAIN} to train")
        return None
    logging.info(f"Loaded {len(items)} historical rows from DynamoDB.")

    cleaned = []
    for row in items:
        try:
            cleaned.append({
                'symbol': str(row['symbol']),
                'exchange': str(row['exchange']),
                'side': str(row['side']),
                'price': float(row['price']),
                'quantity': float(row['quantity']),
                'notional_value': float(row['notional_value']),
                'timestamp': str(row['timestamp'])

            })
        except (KeyError, ValueError):
            continue   # silently skip malformed rows

    schema = StructType([
        StructField('symbol', StringType(), True),
        StructField('exchange', StringType(), True),
        StructField('side', StringType(), True),
        StructField('price', DoubleType(), True),
        StructField('quantity', DoubleType(), True),
        StructField('notional_value', DoubleType(), True),
        StructField('timestamp', StringType(), True),
    ])

    df = spark.createDataFrame(cleaned, schema)

    return df

# Part 3. Feature Engineering

def engineer_features(df, feature_cols_only=False):
    from pyspark.sql.window import Window

    # Handle both unix milliseconds AND ISO string timestamps
    df = df.withColumn(
        'ts',
        F.coalesce(
            # Format 1: Unix milliseconds e.g. 1783080654212
            F.when(
                F.col('timestamp').rlike('^[0-9]{13}$'),
                (F.col('timestamp').cast('long') / 1000).cast('double')
            ),
            # Format 2: Unix seconds e.g. 1783080654
            F.when(
                F.col('timestamp').rlike('^[0-9]{10}$'),
                F.col('timestamp').cast('double')
            ),
            # Format 3: ISO with microseconds and Z e.g. 2026-07-03T10:47:56.627012Z
            F.unix_timestamp(
                F.regexp_replace(F.col('timestamp'), 'Z$', ''),
                "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
            ).cast('double'),
            # Format 4: ISO with milliseconds e.g. 2026-07-03T10:47:56.627Z
            F.unix_timestamp(
                F.regexp_replace(F.col('timestamp'), 'Z$', ''),
                "yyyy-MM-dd'T'HH:mm:ss.SSS"
            ).cast('double'),
            # Format 5: ISO plain e.g. 2026-07-03T10:47:56
            F.unix_timestamp(
                F.regexp_replace(F.col('timestamp'), 'Z$', ''),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).cast('double')
        )
    )

    # Drop any rows where timestamp still couldn't be parsed
    bad = df.filter(F.col('ts').isNull()).count()
    if bad > 0:
        logging.warning(f"Dropping {bad} rows with unparseable timestamps.")
    df = df.filter(F.col('ts').isNotNull())

    symbol_time_window = Window.partitionBy('symbol').orderBy('ts')

    df = (df
        .withColumn('price_lag_1',  F.lag('price', 1).over(symbol_time_window))
        .withColumn('price_lag_3',  F.lag('price', 3).over(symbol_time_window))
        .withColumn('price_lag_5',  F.lag('price', 5).over(symbol_time_window))
        .withColumn('price_change_1',
                    (F.col('price') - F.col('price_lag_1')) / F.col('price_lag_1'))
        .withColumn('price_change_3',
                    (F.col('price') - F.col('price_lag_3')) / F.col('price_lag_3'))
        .withColumn('rolling_avg_5',
                    F.avg('price').over(symbol_time_window.rowsBetween(-5, 0)))
        .withColumn('rolling_avg_10',
                    F.avg('price').over(symbol_time_window.rowsBetween(-10, 0)))
        .withColumn('rolling_vol_5',
                    F.sum('quantity').over(symbol_time_window.rowsBetween(-5, 0)))
        .withColumn('volatility_5',
                    F.stddev('price').over(symbol_time_window.rowsBetween(-5, 0)))
        .withColumn('side_numeric',
                    F.when(F.col('side') == 'buy',   1.0)
                     .when(F.col('side') == 'sell', -1.0)
                     .otherwise(0.0))
        .withColumn('label',
                    F.lead('price', PREDICT_MINUTES_AHEAD).over(symbol_time_window))
    )

    feature_cols = [
        'price', 'quantity', 'notional_value',
        'price_lag_1', 'price_lag_3', 'price_lag_5',
        'price_change_1', 'price_change_3',
        'rolling_avg_5', 'rolling_avg_10',
        'rolling_vol_5', 'volatility_5',
        'side_numeric', 'label'
    ]

    return df.dropna(subset=feature_cols), feature_cols[:-1]

# Part 4. Train the gradient boost pipeline

def train_model(spark: SparkSession) -> PipelineModel | None:
    """
    Loads historical data, engineers features, trains a GBT model
    and saves it to MODEL_PATH
    """

    logging.info("Loading historical data for training...")
    df = load_historical_data(spark)
    if df is None:
        return None

    df, feature_cols = engineer_features(df)

    assembler = VectorAssembler(inputCols=feature_cols, outputCol='raw_features', handleInvalid="skip")
    scaler = StandardScaler(inputCol='raw_features', outputCol='features', withStd=True, withMean=True)

    gbt = GBTRegressor(
        featuresCol = 'features',
        labelCol = 'label',
        maxIter = 100,
        maxDepth = 5,
        stepSize = 0.1,
        subsamplingRate = 0.8,
        featureSubsetStrategy = 'sqrt'
    )

    pipeline = Pipeline(stages=[assembler, scaler, gbt])

    train_df, val_df = df.randomSplit([0.8, 0.2], seed=42)

    logging.info(" Training GBT model...")
    model = pipeline.fit(train_df)

    # Evaluate
    predictions = model.transform(val_df)
    evaluator = RegressionEvaluator(labelCol='label', predictionCol='prediction')
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: 'rmse'})
    mae = evaluator.evaluate(predictions, {evaluator.metricName: 'mae'})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})

    logging.info(f" Model trained | RMSE={rmse:.4f} | MAE={mae:.4f} | R2={r2:.4f} ")

    model.write().overwrite().save(MODEL_PATH)
    logging.info(f"Model saved to {MODEL_PATH}")
    return model

# Part 5. Write predictions to dynamodb

def write_predictions_to_dynamo(partition, endpoint="http://dynamodb-local:8000", region="us-east-1"):
    """Runs on Spark workers - writes preedicted prices to DynamoDB."""
    resource = boto3.resource(
        'dynamodb',
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'DUMMYIDEXAMPLE'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'DUMMYEXAMPLEKEY')
    )
    table = resource.Table(PREDICTIONS_TABLE)
    try:
        with table.batch_writer(overwrite_by_pkeys=['Symbol', 'Timestamp']) as batch:

            for row in partition:
                batch.put_item(Item={
                    'Symbol':            row['symbol'],
                    'PredictedAt':       datetime.now(timezone.utc).isoformat(),
                    'CurrentPrice':      str(row['price']),
                    'PredictedPrice':    str(round(row['prediction'], 6)),
                    'PredictedInMinutes': str(PREDICT_MINUTES_AHEAD),
                    'Exchange':          row['exchange'],
                    'Timestamp':         row['timestamp']
                })
                if len(batch_items) >= 25:
                    _flush_batch(table, batch_items)
                    batch_items = []
            
        if batch_items:
            _flush_batch(table, batch_items)
    except Exception as e:
        logging.error(f"Flushed: {e}")

def _flush_batch(table, items):
    try:
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
    except Exception as e:
        logging.error(f"Batch flush failed: {e}")

def write_predictions_batch(df_batch, batch_id, endpoint_bc="http://dynamodb-local:8000", region_bc="us-east-1"):
    count = df_batch.count()
    if count == 0:
        logging.info("Bleeeeeeeh")
        return
    logging.info(f"💡 Writing {count} predictions (batch {batch_id})...")
    df_batch.coalesce(2).foreachPartition(
        lambda p: write_predictions_to_dynamo(p, endpoint_bc.value, region_bc.value)
    )


# Part 6: Streaming prediction pipeline

def run_ml_pipeline(spark: SparkSession, endpoint_bc="http://dynamodb-local:8000", region_bc="us-east-1"):
    

    check_predictions_table()

    model = train_model(spark)
    # -- initial training --
    if model is None:
        logging.error("Not enough data to train. Collect more trades first.")
        return None

    # -- Periodic retraining in background --
    def retrain_loop():
        import time
        while True:
            time.sleep(RETRAIN_INTERVAL)
            logging.info("Retraining model on latest data...")
            train_model(spark)

    threading.Thread(target=retrain_loop, daemon=True).start()

    # -- Broadcast config --
    endpoint_bc = spark.sparkContext.broadcast(DYNAMODB_ENDPOINT)
    region_bc = spark.sparkContext.broadcast(AWS_REGION)


    # -- Kafka stream --

    trade_schema = StructType([
        StructField('exchange',  StringType(), True),
        StructField('symbol',    StringType(), True),
        StructField('side',      StringType(), True),
        StructField('price',     DoubleType(), True),
        StructField('quantity',  DoubleType(), True),
        StructField('timestamp', StringType(), True)
    ])
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    trade_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(F.from_json("json_payload", trade_schema).alias("data")) \
    .select("data.*")

    # ── Build micro-batch features ────────────────────────────────
    from pyspark.sql.window import Window

    def predict_batch(df_batch, batch_id):
        if df_batch.isEmpty():
            return

        # Reload model each batch to pick up any retrained version
        try:
            current_model = PipelineModel.load(MODEL_PATH)

        except Exception:
            current_model = model     # fall back to initial model

        symbol_time_window = Window.partitionBy('symbol').orderBy('ts')

        featured = (df_batch
            .withColumn('ts', F.coalesce(
                F.when(F.col('timestamp').rlike('^[0-9]{13}$'),
                       (F.col('timestamp').cast('long') / 1000).cast('double')),
                F.when(F.col('timestamp').rlike('^[0-9]{10}$'),
                       F.col('timestamp').cast('double')),
                F.unix_timestamp(
                    F.regexp_replace('timestamp', 'Z$', ''),
                    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").cast('double'),
                F.unix_timestamp(
                    F.regexp_replace('timestamp', 'Z$', ''),
                    "yyyy-MM-dd'T'HH:mm:ss.SSS").cast('double'),
            ))
            .withColumn('notional_value',
                        F.round(F.col('price') * F.col('quantity'), 2))
            .withColumn('price_lag_1',
                        F.lag('price', 1).over(symbol_time_window))
            .withColumn('price_lag_3',
                        F.lag('price', 3).over(symbol_time_window))
            .withColumn('price_lag_5',
                        F.lag('price', 5).over(symbol_time_window))
            .withColumn('price_change_1',
                        (F.col('price') - F.col('price_lag_1')) / F.col('price_lag_1'))
            .withColumn('price_change_3',
                        (F.col('price') - F.col('price_lag_3')) / F.col('price_lag_3'))
            .withColumn('rolling_avg_5',
                        F.avg('price').over(symbol_time_window.rowsBetween(-5, 0)))
            .withColumn('rolling_avg_10',
                        F.avg('price').over(symbol_time_window.rowsBetween(-10, 0)))
            .withColumn('rolling_vol_5',
                        F.sum('quantity').over(symbol_time_window.rowsBetween(-5, 0)))
            .withColumn('volatility_5',
            F.stddev('price').over(symbol_time_window.rowsBetween(-5, 0)))
            .withColumn('side_numeric',
                        F.when(F.col('side') == 'buy',   1.0)
                         .when(F.col('side') == 'sell', -1.0)
                         .otherwise(0.0))
            .filter(F.col('ts').isNotNull()) \
            .dropna(subset=['price', 'quantity', 'notional_value', 'ts', 'side_numeric'])
            
        )
        if featured.count() == 0:
            logging.info("No features recorded")
            return

        predictions = current_model.transform(featured)
        pred_count = predictions.filter(F.col('prediction').isNotNull())
        logging.info(f" Ml batch {batch_id}: {pred_count.count()} predictions written")
        write_predictions_batch(
            pred_count.select(
                'symbol', 'exchange', 'timestamp',
                'price', 'prediction'
                ),
            batch_id,
            endpoint_bc,
            region_bc
        )

    # ── Start streaming ───────────────────────────────────────────
    logging.info(f"ML pipeline running — predicting prices {PREDICT_MINUTES_AHEAD} minutes ahead.")
    return trade_stream.writeStream \
        .outputMode("append") \
        .foreachBatch(predict_batch) \
        .trigger(processingTime="15 seconds") \
        .queryName("ml_predictions") \
        .start()
