import boto3
from boto3.dynamodb.conditions import Attr

def get_all_market_trades(endpoint_url: str = None) -> list[dict]:
    dynamodb = boto3.resource(
        "dynamodb",
        region_name="us-east-1",
        endpoint_url=endpoint_url
    )

    table = dynamodb.Table("MarketTrades")
    results = []

    response = table.scan()
    results.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        results.extend(response.get("Items", []))

    return results

