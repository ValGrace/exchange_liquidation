import asyncio
from socket_handler import websocket_handler
from binance import parse_binance_liquidations, BINANCE_WS_URL
from bybit import BYBIT_WS_URL, parse_bybit_message, bybit_subscribe_payload
from okx import parse_okx_message, OKX_WS_URL, okx_subscribe_payload
from kraken import parse_kraken_message, KRAKEN_WS_URL, kraken_subscribe_payload
from coin_base import parse_coinbase_message, COINBASE_WS_URL, coinbase_subscribe_payload


async def main():
    binance_task = websocket_handler("Binance exchange", BINANCE_WS_URL, None, parse_binance_liquidations)
    bybit_task = websocket_handler("ByBit exchange", BYBIT_WS_URL, bybit_subscribe_payload, parse_bybit_message)
    okx_task = websocket_handler("OKX exchange", OKX_WS_URL, okx_subscribe_payload, parse_okx_message)
    kraken_task = websocket_handler("KRAKEN exchange", KRAKEN_WS_URL, kraken_subscribe_payload, parse_kraken_message)
    coinbase_task = websocket_handler("Coinbase exchange", COINBASE_WS_URL, coinbase_subscribe_payload, parse_coinbase_message)

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
    except KeyboardInterrupt:
        print("\nEngine shutting down manually")

asyncio.run(main())