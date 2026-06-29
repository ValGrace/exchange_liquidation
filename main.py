import asyncio
from socket_handler import websocket_handler
from exchanges.binance import parse_binance_liquidations, BINANCE_WS_URL
from exchanges.bybit import BYBIT_WS_URL, parse_bybit_message, bybit_subscribe_payload
from exchanges.okx import parse_okx_message, OKX_WS_URL, okx_subscribe_payload
from exchanges.kraken import parse_kraken_message, KRAKEN_WS_URL, kraken_subscribe_payload
from exchanges.coin_base import parse_coinbase_message, COINBASE_WS_URL, coinbase_subscribe_payload
from kafka import KafkaProducer
import logging

KAFKA_TOPIC = 'crypto_exchange_topic'

async def main():
    logging.info("Initializing high-throughput Kafka producer...")

    k_producer = KafkaProducer(
        bootstrap_servers = 'broker:9092',
        api_version=(2, 3, 1),
        enable_idempotence=True
    )

    await k_producer.start()
    logging.info("✅ Kafka producer started successfully.")

    binance_task = websocket_handler("Binance exchange", BINANCE_WS_URL, None, parse_binance_liquidations, k_producer, KAFKA_TOPIC)
    bybit_task = websocket_handler("ByBit exchange", BYBIT_WS_URL, bybit_subscribe_payload, parse_bybit_message, k_producer, KAFKA_TOPIC)
    okx_task = websocket_handler("OKX exchange", OKX_WS_URL, okx_subscribe_payload, parse_okx_message, k_producer, KAFKA_TOPIC)
    kraken_task = websocket_handler("KRAKEN exchange", KRAKEN_WS_URL, kraken_subscribe_payload, parse_kraken_message, k_producer, KAFKA_TOPIC)
    coinbase_task = websocket_handler("Coinbase exchange", COINBASE_WS_URL, coinbase_subscribe_payload, parse_coinbase_message, k_producer, KAFKA_TOPIC)

    try:
        results = await asyncio.gather(
            binance_task,
            bybit_task,
            okx_task,
            kraken_task,
            coinbase_task,
            return_exceptions=True
        )
        print(results)
    finally:
        # Guarantee cleanup on shut down
        logging.info("Flushing and shutting down Kafka producer...")
        await k_producer.stop()
        logging.info("✅ Pipelines cleanly offline.")

if __name__ == "__main__":
    asyncio.run(main())