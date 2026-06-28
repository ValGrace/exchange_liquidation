import asyncio
from socket_handler import websocket_handler
from binance import parse_binance_liquidations, binance_liq_url

async def main():
    binance_task = websocket_handler("Binance", binance_liq_url, None, parse_binance_liquidations)

    try:
        results = await asyncio.gather(
            binance_task,
            return_exceptions=True
        )
        print(results)
    except KeyboardInterrupt:
        print("\nEngine shutting down manually")

asyncio.run(main())