import asyncio
import websockets
import json
import logging


# Configure basic logging
logging.basicConfig(level=logging.INFO)

async def websocket_handler(exchange_name, url, subscribe_payload, message_parser):
    """Base handler for connecting, subscribing and parsing streams."""
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
                       print(
                           f"💥 [{exchange_name} LIQUIDATION] {parsed_event['symbol']} | {parsed_event['side']} | ${parsed_event['price']} | QTY: {parsed_event['quantity']}"
                       )
        
        except websockets.ConnectionClosed:
            logging.warning(f"{exchange_name} disconnected. Reconnecting in 5s...")
            await asyncio.sleep(5)
        
        except Exception as e:
            logging.error(f"Error in {exchange_name}: {e}")
            await asyncio.sleep(5)
