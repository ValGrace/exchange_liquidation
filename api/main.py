from fastapi import FastAPI, HTTPException, Query
from scripts.db_query import get_all_market_trades
from typing import Optional
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os

app = FastAPI()


dynamodb = boto3.resource(
    'dynamodb',
    region_name='us-east-1',
    endpoint_url=os.getenv("DYNAMODB_ENDPOINT", "http://localhost:8000"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "DUMMYIDEXAMPLE"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "DUMMYEXAMPLEKEY")
)

table = dynamodb.Table("MarketTrades")

# handle pagination
def paginate_query(operation, **kwargs):
    results = []
    response = operation(**kwargs)
    results.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = operation(**kwargs, ExclusiveStartKey=response["LastEvaluatedKey"])
        results.extend(response.get("Items", []))
    return results

@app.get("/")
def read_root():
    return {"Bienvenue": "à le liquidation d'echange"}

@app.get("/trades")
def get_data(limit: Optional[int] = Query(default=1000, le=1000)):
    try:
        response = table.scan(Limit=limit)
        return {
            "count": response["Count"],
            "items": response.get("Items", [])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/trades/{symbol}")
def get_trade_by_symbol(
    symbol: str, 
    date: Optional[str] = Query(default=None, description="Format: YYYY-MM-DD"),
):
    try:
        if date:
            partition_key = f"{symbol}#{date}"
            items = paginate_query(
                table.query,
                KeyConditionExpression=Key("TradePartition").eq(partition_key)
            )
        else:
            items = paginate_query(
                table.scan,
                FilterExpression=Attr("symbol").eq(symbol)
            )
        
        if not items:
            raise HTTPException(status_code=404, detail=f"No trades found for symbol: {symbol}")
        return {"symbol": symbol, "count": len(items), "items": items}
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/trades/{symbol}/summary")
def get_symbol_summary(symbol: str, date: str = Query(description="Format: YYYY-MM-DD")):
    """Aggregated stats for a symbol on a given date."""
    try:
        partition_key = f"{symbol}#{date}"
        items = paginate_query(
            table.query,
            KeyConditionExpression=Key("TradePartition").eq(partition_key)
        )

        if not items:
            raise HTTPException(status_code=404, detail=f"No trades found for {symbol} on {date}")

        prices = [float(i["price"]) for i in items]
        quantities = [float(i["quantity"]) for i in items]
        notional_values = [float(i["notional_value"]) for i in items]

        return {
            "symbol": symbol,
            "date": date,
            "trade_count": len(items),
            "avg_price": round(sum(prices) / len(prices), 4),
            "high_price": max(prices),
            "low_price": min(prices),
            "total_volume": round(sum(quantities), 4),
            "total_notional": round(sum(notional_values), 2),
            "buy_count": sum(1 for i in items if i["side"] == "buy"),
            "sell_count": sum(1 for i in items if i["side"] == "sell"),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health():
    try:
        table.load()
        return {"status": "healthy", "table": "MarketTrades"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

