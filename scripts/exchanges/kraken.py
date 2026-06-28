KRAKEN_WS_URL = "wss://ws.kraken.com/v2"

kraken_subscribe_payload = {
    "method": "subscribe",
    "params": {
        "channel": "trade",
        "symbol": ["BTC/USD", "ETH/USD"],
        "snapshot": False
    },
    "req_id": 1001
}

def parse_kraken_message(message_data):
    """
    Parses Kraken V2 Websocket messages.
    Kraken trade events have a distinct structure under the trade channel
    """

    if not isinstance(message_data, dict):
        return None
    
    if "method" in message_data or message_data.get("channel") == "status":
        return None
    
    channel = message_data.get("channel")
    data_list = message_data.get("data")

    if channel == "trade" and isinstance(data_list, list):
        parsed_events = []
        for trade in data_list:
            try:
                parsed_events.append({
                    "exchange": "kraken",
                    "symbol": trade.get("symbol").replace("/", "-"),
                    "side": trade.get("side").upper(),
                    "price": float(trade.get("price", 0)),
                    "quantity": float(trade.get("qty", 0)),
                    "timestamp": trade.get("timestamp")
                })
            except (ValueError, TypeError, AttributeError):
                continue
        return parsed_events
    return None