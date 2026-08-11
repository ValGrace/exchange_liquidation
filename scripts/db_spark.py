import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import boto3
import time
import logging
from botocore.exceptions import ClientError
import threading

KAFKA_BOOTSTRAP_SERVERS = 'broker:9092'
KAFKA_TOPIC = 'crypto_exchange_trades'
DYNAMODB_TABLE = 'MarketTrades'
AWS_REGION = 'us-east-1'
DYNAMODB_ENDPOINT = 'http://dynamodb-local:8000' # Change to None in production AWS environment


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def verify_and_create_dynamodb_table():
    """Ensures the DynamoDB table exists prior to starting the Spark Stream."""
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION, endpoint_url=DYNAMODB_ENDPOINT)
    try:
        logging.info(f"Checking if DynamoDB table '{DYNAMODB_TABLE}' exists...")
        # table = dynamodb.Table()
        dynamodb.describe_table(TableName=DYNAMODB_TABLE)
        logging.info(f"DynamoDB Table '{DYNAMODB_TABLE}' is active.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logging.info(f"Table '{DYNAMODB_TABLE}' not found. Creating table...")
            try:
                dynamodb.create_table(
                    TableName=DYNAMODB_TABLE,
                    KeySchema=[
                        {'AttributeName': 'TradePartition', 'KeyType': 'HASH'},
                        {'AttributeName': 'TradeID', 'KeyType': 'RANGE'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'TradePartition', 'AttributeType': 'S'},
                        {'AttributeName': 'TradeID', 'AttributeType': 'S'}
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                waiter = dynamodb.get_waiter('table_exists')
                waiter.wait(TableName=DYNAMODB_TABLE, WaiterConfig={'Delay': 2, 'MaxAttempts': 10})
                logging.info(f"Table '{DYNAMODB_TABLE}' successfully created and ACTIVE.")
            except Exception as creation_err:
                logging.error(f" Failed to create DynamoDB Table: {creation_err}")
                
_dynamo_semaphore = threading.Semaphore(2)

def send_partition_to_dynamo(partition):
    rows = list(partition)
    if not rows:
        return

    with _dynamo_semaphore:
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, endpoint_url=DYNAMODB_ENDPOINT)
        table = dynamodb.Table(DYNAMODB_TABLE)
        # Write in chunks
        chunks = [rows[i:i+10] for i in range(0, len(rows), 10)]   
        try:
            with table.batch_writer() as batch:
                for chunk in chunks:

                    for row in chunk:
                        item = {
                            "TradePartition": row["TradePartition"],
                            "TradeID": row["TradeID"],
                            "exchange": row["exchange"],
                            "symbol": row["symbol"],
                            "side": row["side"],
                            "price": str(row["price"]),
                            "quantity": str(row["quantity"]),
                            "notional_value": str(row["notional_value"]),
                            "timestamp": str(row["timestamp"])
                        }
                        batch.put_item(Item=item)
                    time.sleep(0.05)
        except Exception as e:
            logging.error(f" Failed to write partition to DynamoDB: {e}")
            raise  


def write_to_dynamodb(df_batch, batch_id):
    if df_batch.isEmpty():
        logging.info(f"Batch {batch_id} is empty, skipping.")
        return
    
    count = df_batch.count()
    logging.info(f"Processing batch {batch_id} with {count} rows...") 
    
    try:
        df_batch.foreachPartition(send_partition_to_dynamo)
        logging.info(f" Batch {batch_id} written successfully.")
    except Exception as e:
        logging.error(f"Batch {batch_id} failed: {e}")
        raise

def run_spark_consumer(spark: SparkSession):

    spark.sparkContext.setLogLevel("INFO")
    
    verify_and_create_dynamodb_table()

    logging.info("Starting PySpark Structured streaming pipeline...")
    
    logging.info(f"SCALA VERSION: {spark.sparkContext._gateway.jvm.scala.util.Properties.versionString()}")

    # endpoint_bc  = spark.sparkContext.broadcast(DYNAMODB_ENDPOINT)
    # region_bc    = spark.sparkContext.broadcast(AWS_REGION)
    # table_name_bc = spark.sparkContext.broadcast(DYNAMODB_TABLE)

    trade_schema = StructType([
        StructField('exchange', StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("side", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.metadata.max.age.ms", "30000") \
        .option("kafka.reconnect.backoff.ms", "1000") \
        .option("kafka.reconnect.backoff.max.ms", "5000") \
        .load()
    
    json_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(F.from_json("json_payload", trade_schema).alias("data")) \
        .select("data.*")

    transformed_df = json_df \
    .withColumn("notional_value", F.round(F.col("price") * F.col("quantity"), 2)) \
    .withColumn("timestamp",
        F.when(
            F.col("timestamp").rlike('^[0-9]{13}$'),
            F.from_unixtime(F.col("timestamp").cast('long') / 1000,
                           "yyyy-MM-dd'T'HH:mm:ss.SSS")
        ).otherwise(F.col("timestamp"))
    ) \
    .withColumn("date_str", F.substring("timestamp", 1, 10)) \
    .withColumn("TradePartition", F.concat_ws("#", F.col("symbol"), F.col("date_str"))) \
    .withColumn("uuid", F.expr("uuid()")) \
    .withColumn("TradeID", F.concat_ws("#", F.col("timestamp"), F.col("exchange"), F.col("uuid")))
                                             
    final_df = transformed_df.drop("date_str", "uuid")

    return final_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_dynamodb) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    

