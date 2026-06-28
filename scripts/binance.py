import asyncio
import json
import os
from websockets import connect

websocket_uri = "wss://fstream.binance.com/ws/!forceOrder@arr"
filename = "binance.csv"

if not os.path.isfile(filename):
    with open(filename, "w") as f:
        f.write(",".join([
            "symbol", "side", "order_type", "time_in_force",
            "original_quantity", "price", "average_price",
            "order_status", "order_last_filled_quantity", "order_filled_accumulated_quantity", "order_trade_time"
        ]) + "\n")

async def binance_liquidations(uri, filename):
    async for websocket in connect(uri):
        try:
            while True:
                msg = await websocket.recv()
                print(msg)
        except Exception as e:
            print(e)
            continue

asyncio.run(binance_liquidations(websocket_uri, filename))