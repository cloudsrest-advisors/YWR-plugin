# main.py
import os
import logging
import psycopg2
from psycopg2 import pool, sql
from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel
from datetime import date
from typing import Optional, List
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import traceback
from contextlib import asynccontextmanager

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database settings (move before lifespan/app)
DATABASE_URL = os.getenv("DATABASE_URL")
MIN_CONN = 1
MAX_CONN = 10
_db_pool: Optional[pool.SimpleConnectionPool] = None
TICKER_MAP_TABLE = os.getenv("TICKER_MAP_TABLE", "ticker_map")
TICKER_MAP_FACTSET_COLUMN = os.getenv("TICKER_MAP_FACTSET_COLUMN", "factset_ticker")
TICKER_MAP_FMP_COLUMN = os.getenv("TICKER_MAP_FMP_COLUMN", "fmp_ticker")
_ticker_map_lookup_failed = False

# Lifespan handler to initialize/cleanup DB pool
@asynccontextmanager
async def lifespan(app):
    global _db_pool
    logger.info("Starting app PID=%s PORT=%s DATABASE_URL_set=%s", os.getpid(), os.getenv("PORT"), bool(os.getenv("DATABASE_URL")))
    if not DATABASE_URL:
        logger.warning("DATABASE_URL environment variable is not set; skipping DB pool initialization")
    else:
        try:
            _db_pool = psycopg2.pool.SimpleConnectionPool(MIN_CONN, MAX_CONN, dsn=DATABASE_URL)
            logger.info("Database pool initialized")
        except Exception:
            logger.exception("Failed to initialize database pool; continuing without DB (endpoints will return 503)")
            _db_pool = None

    yield

    logger.info("Application shutdown starting (PID=%s)", os.getpid())
    if _db_pool:
        _db_pool.closeall()
        _db_pool = None
    logger.info("Application shutdown complete")

# Create the FastAPI app using the lifespan handler BEFORE defining routes
app = FastAPI(
    title="YWR Factor Scores API",
    description="Retrieve the latest YWR factor model scores, QARV scores, and generated reports by ticker",
    version="1.0.0",
    lifespan=lifespan
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

class TickerMatch(BaseModel):
    factset_ticker: Optional[str]
    fmp_ticker: Optional[str]
    name: Optional[str]
    source: str

class TickerResolutionResponse(BaseModel):
    query: str
    matches: List[TickerMatch]

def get_db_connection():
    if not _db_pool:
        raise RuntimeError("Database pool not initialized")
    return _db_pool.getconn()

def put_db_connection(conn):
    if _db_pool and conn:
        _db_pool.putconn(conn)

def _normalize_ticker(raw_ticker: str) -> str:
    return raw_ticker.strip().upper()

def _ticker_exists(cur, factset_ticker: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM ywr_factor_scores
        WHERE ticker = %s
        LIMIT 1;
        """,
        (factset_ticker,),
    )
    return cur.fetchone() is not None

def _lookup_factset_ticker(cur, fmp_ticker: str) -> Optional[str]:
    global _ticker_map_lookup_failed
    if _ticker_map_lookup_failed or not TICKER_MAP_TABLE:
        return None
    try:
        query = sql.SQL(
            "SELECT {factset_col} FROM {table} WHERE {fmp_col} = %s LIMIT 1;"
        ).format(
            factset_col=sql.Identifier(TICKER_MAP_FACTSET_COLUMN),
            table=sql.Identifier(TICKER_MAP_TABLE),
            fmp_col=sql.Identifier(TICKER_MAP_FMP_COLUMN),
        )
        cur.execute(query, (fmp_ticker,))
        row = cur.fetchone()
        return row[0].upper() if row and row[0] else None
    except Exception:
        if not _ticker_map_lookup_failed:
            logger.exception(
                "Failed to query ticker map table %s; falling back to provided tickers",
                TICKER_MAP_TABLE,
            )
        _ticker_map_lookup_failed = True
        return None

def resolve_ticker(conn, raw_ticker: str) -> str:
    normalized = _normalize_ticker(raw_ticker)
    if not normalized:
        raise HTTPException(status_code=400, detail="Ticker parameter is required")
    with conn.cursor() as cur:
        if _ticker_exists(cur, normalized):
            return normalized
        mapped = _lookup_factset_ticker(cur, normalized)
        if mapped and mapped != normalized and _ticker_exists(cur, mapped):
            logger.info("Mapped ticker %s -> %s using %s", normalized, mapped, TICKER_MAP_TABLE)
            return mapped
    return normalized

def _attach_fmp_symbols(conn, matches: List[dict]):
    global _ticker_map_lookup_failed
    if _ticker_map_lookup_failed or not matches:
        return
    factset_values = [m.get("factset_ticker") for m in matches if m.get("factset_ticker")]
    if not factset_values:
        return
    try:
        query = sql.SQL(
            "SELECT {factset_col}, {fmp_col} FROM {table} WHERE {factset_col} = ANY(%s);"
        ).format(
            factset_col=sql.Identifier(TICKER_MAP_FACTSET_COLUMN),
            table=sql.Identifier(TICKER_MAP_TABLE),
            fmp_col=sql.Identifier(TICKER_MAP_FMP_COLUMN),
        )
        with conn.cursor() as cur:
            cur.execute(query, (factset_values,))
            mapping = {row[0].upper(): row[1] for row in cur.fetchall() if row and row[1]}
        for match in matches:
            factset = match.get("factset_ticker")
            if factset:
                match["fmp_ticker"] = mapping.get(factset.upper())
    except Exception:
        if not _ticker_map_lookup_failed:
            logger.exception("Failed to attach FMP symbols from %s", TICKER_MAP_TABLE)
        _ticker_map_lookup_failed = True

def _fetch_factor_score_matches(conn, normalized_ticker: str, like_pattern: str, limit: int) -> List[dict]:
    query = """
        WITH latest AS (
            SELECT DISTINCT ON (ticker) ticker, name
            FROM ywr_factor_scores
            ORDER BY ticker, date DESC
        )
        SELECT ticker, name,
               CASE
                   WHEN ticker = %s THEN 0
                   WHEN ticker ILIKE %s THEN 1
                   WHEN name ILIKE %s THEN 2
                   ELSE 3
               END AS rank
        FROM latest
        WHERE ticker ILIKE %s OR name ILIKE %s
        ORDER BY rank, ticker
        LIMIT %s;
    """
    params = (
        normalized_ticker,
        like_pattern,
        like_pattern,
        like_pattern,
        like_pattern,
        max(limit * 3, limit),
    )
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    matches = [
        {
            "factset_ticker": row[0],
            "name": row[1],
            "source": "factor_scores",
        }
        for row in rows
    ]
    _attach_fmp_symbols(conn, matches)
    return matches

def _search_ticker_map_candidates(conn, normalized_ticker: str, like_pattern: str, limit: int) -> List[dict]:
    global _ticker_map_lookup_failed
    if _ticker_map_lookup_failed:
        return []
    query = sql.SQL(
        """
        WITH latest AS (
            SELECT DISTINCT ON (ticker) ticker, name
            FROM ywr_factor_scores
            ORDER BY ticker, date DESC
        )
        SELECT tm.{factset_col} AS factset_ticker,
               tm.{fmp_col} AS fmp_ticker,
               latest.name,
               CASE
                   WHEN tm.{fmp_col} = %s THEN 0
                   WHEN tm.{factset_col} = %s THEN 0
                   WHEN tm.{fmp_col} ILIKE %s THEN 1
                   WHEN tm.{factset_col} ILIKE %s THEN 2
                   ELSE 3
               END AS rank
        FROM {table} tm
        LEFT JOIN latest ON latest.ticker = tm.{factset_col}
        WHERE tm.{fmp_col} ILIKE %s OR tm.{factset_col} ILIKE %s
        ORDER BY rank, tm.{factset_col}
        LIMIT %s;
        """
    ).format(
        factset_col=sql.Identifier(TICKER_MAP_FACTSET_COLUMN),
        fmp_col=sql.Identifier(TICKER_MAP_FMP_COLUMN),
        table=sql.Identifier(TICKER_MAP_TABLE),
    )
    params = (
        normalized_ticker,
        normalized_ticker,
        like_pattern,
        like_pattern,
        like_pattern,
        like_pattern,
        max(limit * 3, limit),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    except Exception:
        if not _ticker_map_lookup_failed:
            logger.exception("Failed to search ticker map table %s", TICKER_MAP_TABLE)
        _ticker_map_lookup_failed = True
        return []
    return [
        {
            "factset_ticker": row[0],
            "fmp_ticker": row[1],
            "name": row[2],
            "source": "ticker_map",
        }
        for row in rows
    ]

def find_ticker_matches(conn, raw_query: str, limit: int) -> List[dict]:
    cleaned_query = raw_query.strip()
    if not cleaned_query:
        raise HTTPException(status_code=400, detail="Query parameter is required")
    normalized_ticker = cleaned_query.upper()
    like_pattern = f"%{cleaned_query}%"
    matches: List[dict] = []
    seen_factset = set()

    for match in _fetch_factor_score_matches(conn, normalized_ticker, like_pattern, limit):
        factset = match.get("factset_ticker")
        if factset and factset in seen_factset:
            continue
        seen_factset.add(factset)
        matches.append(match)
        if len(matches) >= limit:
            return matches

    if len(matches) < limit:
        for match in _search_ticker_map_candidates(conn, normalized_ticker, like_pattern, limit):
            factset = match.get("factset_ticker")
            if factset and factset in seen_factset:
                continue
            if factset:
                seen_factset.add(factset)
            matches.append(match)
            if len(matches) >= limit:
                break

    return matches

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
        resolved_ticker = resolve_ticker(conn, ticker)
        with conn.cursor() as cur:
            cur.execute(query, (resolved_ticker,))
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

ticker_router = APIRouter(prefix="/v1/tickers", tags=["tickers"])

@ticker_router.get("/resolve", response_model=TickerResolutionResponse)
def resolve_ticker_endpoint(query: str, limit: int = 5):
    if not _db_pool:
        raise HTTPException(status_code=503, detail="Database not configured")
    limit = max(1, min(limit, 20))
    conn = get_db_connection()
    try:
        matches = find_ticker_matches(conn, query, limit)
        if not matches:
            raise HTTPException(status_code=404, detail="No matching tickers found")
        return {"query": query.strip(), "matches": matches}
    finally:
        put_db_connection(conn)

# Register routers
app.include_router(health_router)
app.include_router(factor_router)
app.include_router(qarv_router)
app.include_router(reports_router)
app.include_router(ticker_router)


# ---- Add privacy route ----

from fastapi.responses import HTMLResponse

@app.get("/privacy", include_in_schema=False)
def privacy():
    html = """
    <html>
      <head><title>YWR Intelligence – Privacy Policy</title></head>
      <body style="font-family: sans-serif; max-width: 800px; margin: 40px auto;">
        <h1>Privacy Policy</h1>
        <p>Last updated: 2025-01-01</p>

        <p>
        This plugin does not store or collect personal data from users.  
        Any data provided to the API (such as ticker queries) is used only 
        to generate responses and is not retained beyond what is required 
        for technical operation.
        </p>

        <p>
        For questions, contact us at: support@ywr-intelligence.world
        </p>
      </body>
    </html>
    """
    return HTMLResponse(content=html, status_code=200)