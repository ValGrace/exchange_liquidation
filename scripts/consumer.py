import json
import sys
import boto3
from kafka import KafkaConsumer
from botocore.exceptions import BotoCoreError, ClientError

# ---------- CONFIGURATION ----------
KAFKA_BROKER = "exchange_broker:9092"       # Change to your Kafka broker
KAFKA_TOPIC = "crypto_exchange_trades"              # Change to your Kafka topic
DYNAMODB_TABLE = "MarketTrades"    # Change to your DynamoDB table name
AWS_REGION = "us-east-1"              # Change to your AWS region

# ---------- SETUP ----------
# Initialize DynamoDB resource
try:
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = dynamodb.Table(DYNAMODB_TABLE)
except (BotoCoreError, ClientError) as e:
    print(f"❌ Failed to connect to DynamoDB: {e}")
    sys.exit(1)

# Initialize Kafka consumer
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="earliest",  # Start from earliest if no offset is committed
        enable_auto_commit=True,
        group_id="dynamodb-writer-group",
        value_deserializer=lambda m: m.decode("utf-8")
    )
except Exception as e:
    print(f"❌ Failed to connect to Kafka: {e}")
    sys.exit(1)

print(f"✅ Listening to Kafka topic '{KAFKA_TOPIC}' and writing to DynamoDB table '{DYNAMODB_TABLE}'...")

# ---------- CONSUME & WRITE ----------
for message in consumer:
    try:
        # Parse message value (assumes JSON)
        data = json.loads(message.value)

        # Ensure 'id' exists for DynamoDB primary key
        if "id" not in data:
            print(f"⚠️ Skipping message without 'id': {data}")
            continue

        # Write to DynamoDB
        table.put_item(Item=data)
        print(f"✅ Inserted into DynamoDB: {data}")

    except json.JSONDecodeError:
        print(f"⚠️ Invalid JSON message: {message.value}")
    except (BotoCoreError, ClientError) as e:
        print(f"❌ DynamoDB write error: {e}")
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")