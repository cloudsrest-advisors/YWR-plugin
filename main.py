# main.py
import os
import psycopg2
from psycopg2 import pool
from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
from datetime import date
from typing import Optional
from fastapi.staticfiles import StaticFiles

# Create the FastAPI app first
app = FastAPI(
    title="YWR Factor Scores API",
    description="Retrieve the latest YWR factor model scores, QARV scores, and generated reports by ticker",
    version="1.0.0"
)

# Mount static files
app.mount("/.well-known", StaticFiles(directory=".well-known"), name="static")
app.mount("/", StaticFiles(directory="."), name="root")

DATABASE_URL = os.getenv("DATABASE_URL")
MIN_CONN = 1
MAX_CONN = 10
_db_pool: Optional[pool.SimpleConnectionPool] = None


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
    date: date


@app.on_event("startup")
def startup():
    global _db_pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    _db_pool = psycopg2.pool.SimpleConnectionPool(
        MIN_CONN, MAX_CONN, dsn=DATABASE_URL
    )


@app.on_event("shutdown")
def shutdown():
    global _db_pool
    if _db_pool:
        _db_pool.closeall()
        _db_pool = None


def get_db_connection():
    if not _db_pool:
        raise RuntimeError("Database pool not initialized")
    return _db_pool.getconn()


def put_db_connection(conn):
    if _db_pool and conn:
        _db_pool.putconn(conn)


# Routers
health_router = APIRouter(prefix="/v1", tags=["health"])


@health_router.get("/health")
def health():
    return {"status": "ok"}


factor_router = APIRouter(prefix="/v1/factor_scores", tags=["factor_scores"])


@factor_router.get("/{ticker}", response_model=FactorScoreResponse)
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
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, (ticker.upper(),))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Ticker not found")
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))
    finally:
        put_db_connection(conn)


# Placeholder routers for future services
qarv_router = APIRouter(prefix="/v1/qarv_scores", tags=["qarv_scores"])


@qarv_router.get("/{ticker}")
def get_qarv_scores(ticker: str):
    raise HTTPException(status_code=501, detail="QARV scores not implemented")


reports_router = APIRouter(prefix="/v1/reports", tags=["reports"])


@reports_router.get("/{ticker}")
def get_report(ticker: str):
    raise HTTPException(status_code=501, detail="Reports not implemented")


# Register routers
app.include_router(health_router)
app.include_router(factor_router)
app.include_router(qarv_router)
app.include_router(reports_router)