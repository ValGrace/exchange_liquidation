from fastapi import FastAPI, HTTPException, Query
from scripts.db_query import get_all_market_trades
from typing import Optional
from scripts.ml_consumer import get_dynamo_resource
from contextlib import asynccontextmanager
from botocore.exceptions import ClientError
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Runs on startup — ensures both tables exist before any request hits
    from scripts.ml_consumer import check_predictions_table
    from scripts.db_spark import verify_and_create_dynamodb_table
    verify_and_create_dynamodb_table()    # MarketTrades
    check_predictions_table()            # PricePredictions
    yield
app = FastAPI()


dynamodb = boto3.resource(
    'dynamodb',
    region_name='us-east-1',
    endpoint_url=os.getenv("DYNAMODB_ENDPOINT", "http://dynamodb-local:8000"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "DUMMYIDEXAMPLE"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "DUMMYEXAMPLEKEY")
)

table = dynamodb.Table("MarketTrades")
pred_table = dynamodb.Table("PricePredictions")

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

@app.get(
    "/predictions",
    tags=["ML Predictions"],
    summary="List all symbols with active price predictions"
)
def list_prediction_symbols():
    """Lists all symbols that have predictions."""
    try:
        dynamodb = get_dynamo_resource()
        pred_table = dynamodb.Table("PricePredictions")
        # table.load()
        results = []
        response = pred_table.scan()
        results.extend(response.get("Items", []))
        # items = response.get("items", [])

        while "LastEvaluatedKey" in response:
                response = pred_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                results.extend(response.get("Items", []))
        
        return results
    except ClientError as e:
        raise HTTPException(
            status_code=500,
            detail=f"DynamoDB scan failed: {e.response['Error']['Message']}"
        )


@app.get("/predictions/{symbol}", tags=["ML Predictions"],
    summary="Find a prediction by symbol")
def get_predictions(symbol: str):
    try:
        dynamodb = get_dynamo_resource()
        table = dynamodb.Table("PricePredictions")

        # Verify table exists before querying
        table.load()

        response = table.query(
            KeyConditionExpression=Key("Symbol").eq(symbol),
            ScanIndexForward=False,  # latest first
            Limit=50
        )
        items = response.get("Items", [])

        if not items:
            raise HTTPException(
                status_code=404,
                detail=f"No predictions found for '{symbol}'. "
                       f"Ensure the ML pipeline is running and has enough data to train."
            )

        return {"symbol": symbol, "count": len(items), "predictions": items}

    except HTTPException:
        raise
    except Exception as e:
        
        print(f"Predictions table not ready yet — ML pipeline may still be initializing.{e}")
        # raise HTTPException(status_code=500, detail=str(e))

