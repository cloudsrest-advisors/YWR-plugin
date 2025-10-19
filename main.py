# main.py
import os
import logging
import psycopg2
from psycopg2 import pool
from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
from datetime import date
from typing import Optional
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create the FastAPI app first
app = FastAPI(
    title="YWR Factor Scores API",
    description="Retrieve the latest YWR factor model scores, QARV scores, and generated reports by ticker",
    version="1.0.0"
)

# Root route for Railway health check
@app.get("/", include_in_schema=False)
def root():
    return {"status": "ok"}

# Mount .well-known for ai-plugin.json and logo.png only if directory exists
WELL_KNOWN_DIR = os.path.join(BASE_DIR, ".well-known")
if os.path.isdir(WELL_KNOWN_DIR):
    app.mount("/.well-known", StaticFiles(directory=WELL_KNOWN_DIR), name="static")
else:
    logger.warning(".well-known directory not found at %s; /.well-known not mounted", WELL_KNOWN_DIR)

# Explicit route for openapi.yaml
@app.get("/openapi.yaml", include_in_schema=False)
def get_openapi_yaml():
    file_path = os.path.join(BASE_DIR, "openapi.yaml")
    if not os.path.isfile(file_path):
        logger.warning("openapi.yaml not found at %s", file_path)
        raise HTTPException(status_code=404, detail="openapi.yaml not found")
    return FileResponse(file_path, media_type="application/yaml")

# Database settings
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


# Do not register custom signal handlers — let uvicorn manage signals
# This avoids unexpected early exits under platform SIGTERM

@app.on_event("startup")
def startup():
    global _db_pool
    if not DATABASE_URL:
        logger.warning("DATABASE_URL environment variable is not set; skipping DB pool initialization")
        return
    try:
        _db_pool = psycopg2.pool.SimpleConnectionPool(
            MIN_CONN, MAX_CONN, dsn=DATABASE_URL
        )
        logger.info("Database pool initialized")
    except Exception:
        logger.exception("Failed to initialize database pool; continuing without DB (endpoints will return 503)")
        _db_pool = None
        # do NOT re-raise here so the process stays up for health checks

@app.on_event("shutdown")
def shutdown():
    global _db_pool
    logger.info("Application shutdown starting")
    if _db_pool:
        _db_pool.closeall()
        _db_pool = None
    logger.info("Application shutdown complete")


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
    if not _db_pool:
        raise HTTPException(status_code=503, detail="Database not configured")
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