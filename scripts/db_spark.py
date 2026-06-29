import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from decimal import Decimal
import boto3
import logging
from botocore.exceptions import ClientError
import uuid

KAFKA_BOOTSTRAP_SERVERS = 'broker:9092'
KAFKA_TOPIC = 'crypto_exchange_trades'
DYNAMODB_TABLE = 'MarketTrades'
AWS_REGION = 'us-east-1'
DYNAMODB_ENDPOINT = 'http://dynamodb-local:8000' # Change to None in production AWS environment

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =====================================================================
# PART 1: DYNAMODB INITIALIZATION (Runs ONCE on Driver)
# =====================================================================
def verify_and_create_dynamodb_table():
    """Ensures the DynamoDB table exists prior to starting the Spark Stream."""
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION, endpoint_url=DYNAMODB_ENDPOINT)
    try:
        logging.info(f"Checking if DynamoDB table '{DYNAMODB_TABLE}' exists...")
        dynamodb.describe_table(TableName=DYNAMODB_TABLE)
        logging.info(f"✅ DynamoDB Table '{DYNAMODB_TABLE}' is active.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logging.info(f"⚠️ Table '{DYNAMODB_TABLE}' not found. Creating table...")
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
                logging.info(f"✅ Table '{DYNAMODB_TABLE}' successfully created and ACTIVE.")
            except Exception as creation_err:
                logging.error(f"❌ Failed to create DynamoDB Table: {creation_err}")
                sys.exit(1)

# =====================================================================
# PART 2: DISTRIBUTED PYSPARK SPARK CONSUMER
# =====================================================================
def send_partition_to_dynamo(partition):
    """
    Executes distributedly on Spark WORKER nodes.
    Each worker processes its own slice of data asynchronously.
    """
    # Boto3 client/resource must be instantiated INSIDE the worker execution block
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION, endpoint_url=DYNAMODB_ENDPOINT)
    table = dynamodb.Table(DYNAMODB_TABLE)
    
    with table.batch_writer() as batch:
        for row in partition:
            item = {
                "TradePartition": row["TradePartition"],
                "TradeID": row["TradeID"],
                "exchange": row["exchange"],
                "symbol": row["symbol"],
                "side": row["side"],
                "price": Decimal(str(row["price"])),
                "quantity": Decimal(str(row["quantity"])),
                "notional_value": Decimal(str(row["notional_value"])),
                "timestamp": row["timestamp"]
            }
            batch.put_item(Item=item)

def write_to_dynamodb(df_batch, batch_id):
    """Handles micro-batches by initiating distributed partition processing."""
    if df_batch.isEmpty():
        return
    logging.info(f"Processing micro-batch {batch_id} distributedly...")
    # Using foreachPartition avoids pulling data back to the driver
    df_batch.foreachPartition(send_partition_to_dynamo)

def run_spark_consumer():
    """Main execution block for Spark Structured Streaming."""
    # 1. Initialize DB Table before running the stream
    verify_and_create_dynamodb_table()

    logging.info("Starting PySpark Structured streaming pipeline...")
    spark = SparkSession.builder \
        .appName("CryptoMarketTradesConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

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
        .option("startingOffsets", "latest") \
        .load()
    
    json_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(F.from_json("json_payload", trade_schema).alias("data")) \
        .select("data.*")

    transformed_df = json_df \
        .withColumn("notional_value", F.round(F.col("price") * F.col("quantity"), 2)) \
        .withColumn("date_str", F.substring("timestamp", 1, 10)) \
        .withColumn("TradePartition", F.concat_ws("#", F.col("symbol"), F.col("date_str"))) \
        .withColumn("uuid", F.expr("uuid()")) \
        .withColumn("TradeID", F.concat_ws("#", F.col("timestamp"), F.col("exchange"), F.col("uuid")))
                                             
    final_df = transformed_df.drop("date_str", "uuid")

    query = final_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_dynamodb) \
        .trigger(processingTime="2 seconds") \
        .start()
    
    query.awaitTermination()