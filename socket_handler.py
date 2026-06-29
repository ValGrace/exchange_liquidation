import asyncio
import websockets
import json
import logging


# Configure basic logging
logging.basicConfig(level=logging.INFO)


async def websocket_handler(exchange_name, url, subscribe_payload, message_parser, producer, topic):
    """Base handler for connecting, subscribing and parsing streams."""
    retry_delay = 5
    while True:
        try:
            async with websockets.connect(url) as ws:
                logging.info(f"Connected to {exchange_name}")

                if subscribe_payload:
                    await ws.send(json.dumps(subscribe_payload))

                async for message in ws:
                   data = json.loads(message)
                   parsed_event = message_parser(data)

                   if parsed_event:
                       for event in parsed_event:
                            # logging.info(
                            #     f" [{exchange_name.upper()} TRADE]"
                            #     f"{event['symbol']} | {event['side']} | "
                            #     f"${event['price']:.2f} | QTY: {event['quantity']}"
                            # )
                            payload_bytes = json.dumps(event).encode("utf-8")

                            key_bytes = event["symbol"].encode("utf-8")

                            producer.send(topic=topic, value=payload_bytes, key=key_bytes)

                    
        
        except websockets.ConnectionClosed:
            logging.warning(f"{exchange_name} disconnected. Reconnecting in 5s...")
            await asyncio.sleep(5)
        
        except Exception as e:
            logging.error(f"Error in {exchange_name}: {e}")
            await asyncio.sleep(retry_delay)
