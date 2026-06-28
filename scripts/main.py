import asyncio
from socket_handler import websocket_handler
from binance import parse_binance_liquidations, binance_liq_url
from bybit import bybit_url, parse_bybit_message
from okx import parse_okx_message, okx_url

async def main():
    binance_task = websocket_handler("Binance exchange", binance_liq_url, None, parse_binance_liquidations)
    bybit_task = websocket_handler("ByBit exchange", bybit_url, None, parse_bybit_message)
    okx_task = websocket_handler("OKX exchange", okx_url, None, parse_okx_message)

    try:
        results = await asyncio.gather(
            binance_task,
            bybit_task,
            okx_task,
            return_exceptions=True
        )
        print(results)
    except KeyboardInterrupt:
        print("\nEngine shutting down manually")

asyncio.run(main())