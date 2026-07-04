from fastapi import FastAPI
from scripts.db_query import get_all_market_trades

app = FastAPI()

@app.get("/")
def read_root():
    return {"Bienvenue": "à le liquidation d'echange"}

@app.get("/data")
def get_data():
    return get_all_market_trades("http://dynamodb-local:8080")