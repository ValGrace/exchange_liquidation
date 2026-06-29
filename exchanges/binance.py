
BINANCE_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"


def parse_binance_liquidations(message_data):
    if 'data' not in message_data or 'o' not in message_data['data']:
        return "no data"
    parsed_events = []
    order_data = message_data['data']['o']
    parsed_events.append({
        "exchange": "Binance",
        "symbol": order_data.get('s'),
        "side": order_data.get('S'),
        "price": float(order_data.get('p', 0)),
        "quantity": float(order_data.get('q', 0)),
        "timestamp": order_data.get('T')
    })
    return parsed_events
