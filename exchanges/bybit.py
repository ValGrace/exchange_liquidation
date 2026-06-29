BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

bybit_subscribe_payload = {
    "op": "subscribe",
    "args": [
        "liquidation.BTCUSDT",
        "orderbook.1.BTCUSDT"
    ]
}

def parse_bybit_message(data):
    if 'topic' not in data or 'data' not in data: return None

    topic = data['topic']
    payload = data['data']

    parsed_events = []

    if topic.startswith("liquidation"):
        parsed_events.append({
            "exchange": "bybit",
            "symbol": payload['symbol'],
            "side": payload['side'],
            "price": float(payload['price']),
            "quantity": float(payload['size']),
            "timestamp": payload['updatedTime']
        })
        return parsed_events
    elif topic.startswith("orderbook"):
        # Handle orderbook delta logic here
        pass