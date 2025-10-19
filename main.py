# main.py
import os
import psycopg2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="YWR Factor Scores API",
              description="Retrieve the latest YWR factor scores for a stock ticker",
              version="1.0.0")

DATABASE_URL = os.getenv("DATABASE_URL")

class FactorScoreResponse(BaseModel):
    ticker: str
    name: str
    country: str
    industry: str
    mkt_val: float
    estimate_score: float
    value_score: float
    price_score: float
    total_score: float
    date: str

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

@app.get("/factor_scores/{ticker}", response_model=FactorScoreResponse)
def read_factor_scores(ticker: str):
    """Return the latest YWR factor scores for a given stock ticker."""
    query = """
        SELECT ticker, name, country, industry, mkt_val,
               estimate_score, value_score, price_score, total_score, date
        FROM ywr_factor_scores
        WHERE ticker = %s
        ORDER BY date DESC
        LIMIT 1;
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (ticker.upper(),))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Ticker not found")
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))