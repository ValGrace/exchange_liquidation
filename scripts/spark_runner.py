from scripts.db_spark import run_spark_consumer, verify_and_create_dynamodb_table
from scripts.ml_consumer import run_ml_pipeline
from pyspark.sql import SparkSession
import os
import logging

def get_or_create_spark():
    return SparkSession.builder \
        .appName("liquidation_platform") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.memoryOverhead", "512m") \
        .config("spark.python.worker.memory", "512m") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.jars.packages",
                # ✅ All 2.13, all 4.1.2 — consistent versions
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,"
                "org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.1.2,"
                "org.apache.kafka:kafka-clients:3.9.1") \
        .getOrCreate()

def run_all():
    verify_and_create_dynamodb_table()

    spark = get_or_create_spark()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.setCheckpointDir('/tmp/spark_checkpoints')

    endpoint_bc = spark.sparkContext.broadcast(
        os.getenv('DYNAMODB_ENDPOINT', 'http://dynamodb-local:8000')
    )
    region_bc = spark.sparkContext.broadcast('us-east-1')

    try:
        #  Build ETL query on main thread — returns StreamingQuery, doesn't block
        etl_query = run_spark_consumer(spark)
        logging.info(" ETL streaming query started.")

        #  Build ML query on main thread — returns StreamingQuery, doesn't block
        ml_query = run_ml_pipeline(spark, endpoint_bc, region_bc)
        if ml_query:
            logging.info("ML streaming query started.")
        else:
            logging.warning(" ML query not started — not enough data to train yet.")

    except Exception as e:
        logging.error(f" Failed to start queries: {e}")
        raise

    # Log all active queries
    for q in spark.streams.active:
        logging.info(f" Active stream: '{q.name}' | {q.status['message']}")

    # Single blocking call — manages both queries
    spark.streams.awaitTermination()

    etl_query.awaitTermination()