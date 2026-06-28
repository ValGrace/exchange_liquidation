COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"

coinbase_subscribe_payload = {
    "type": "subscribe",
    "product_ids": ["BTC-USD", "ETH-USD"],
    "channel": "market_trades"
}

def parse_coinbase_message(message_data):
    """
        Parses Coinbase Advanced Trade market_trades messages
    """
    if not isinstance(message_data, dict):
        return None

    if message_data.get("channel") != "market_trades":
        return None
    
    events_list = message_data.get("events")
    if not isinstance(events_list, list):
        return None
    
    parsed_events = []
    for event in events_list:
        trades = event.get("trades")
        if not isinstance(trades, list):
            continue
        for trade in trades:
            try:
                parsed_events.append({
                    "exchange": "coinbase",
                    "symbol": trade.get("product_id"),
                    "side": trade.get("side").upper(),
                    "price": float(trade.get("price", 0)),
                    "quantity": float(trade.get("size", 0)),
                    "timestamp": trade.get("time")
                })
            except (ValueError, TypeError, AttributeError):
                continue
    return parsed_events

