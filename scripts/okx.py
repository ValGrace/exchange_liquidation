okx_url = "wss://ws.okx.com:8443/ws/v5/public" # Verify this URL with OKX API documentation if connection issues (e.g., 404 errors) occur.

okx_subscribe_payload = {
    "op": "subscribe",
    "args": [
        {"channel": "liquidation-orders", "instType": "SWAP"},
        {"channel": "books", "instId": "BTC-USDT-SWAP"}
    ]
}

def parse_okx_message(data):
    if 'event' in data: return None # Ignore subscription confirmation events
    if 'arg' not in data or 'data' not in data: return None

    channel = data['arg']['channel']

    if channel == "liquidation-orders":
        for item in data['data']:
            # OKX returns nested arrays containing multiple liquidations
            for detail in item['details']:
                return {
                    "exchange": "okx",
                    "symbol": item['instId'],
                    "side": detail['side'],
                    "price": float(detail['bkPx']), # Bankruptcy price
                    "quantity": float(detail['sz']),
                    "timestamp": detail['ts']
                }