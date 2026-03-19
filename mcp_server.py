"""
YWR Intelligence MCP Server
============================
Exposes YWR proprietary factor scores and QARV scores as MCP tools
for use in Claude Desktop or any MCP-compatible client.

Setup:
  pip install mcp psycopg2-binary python-dotenv

Run locally:
  python mcp_server.py

Claude Desktop config (~/.claude/claude_desktop_config.json):
  {
    "mcpServers": {
      "ywr": {
        "command": "python",
        "args": ["/path/to/mcp_server.py"],
        "env": {
          "DATABASE_URL": "postgresql://..."
        }
      }
    }
  }
"""

import os
import json
import logging
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
server = Server("ywr-intelligence")


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable not set")
    return psycopg2.connect(DATABASE_URL)


def query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT and return list of dicts, sanitising NaN/None."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [_sanitize(dict(r)) for r in rows]


def _sanitize(row: dict) -> dict:
    """Replace NaN floats and Decimal types with JSON-safe values."""
    import math
    from decimal import Decimal
    import datetime
    out = {}
    for k, v in row.items():
        if isinstance(v, float) and math.isnan(v):
            out[k] = None
        elif isinstance(v, Decimal):
            f = float(v)
            out[k] = None if math.isnan(f) else f
        elif isinstance(v, datetime.date):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


NAN_FILTER = "AND {col}::text != 'NaN'"


# ── Tool definitions ──────────────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="get_factor_scores",
            description=(
                "Get YWR factor model scores for a specific stock ticker. "
                "Returns estimate_score (earnings revision momentum), value_score "
                "(valuation cheapness), price_score (6-month price momentum), and "
                "total_score (60% estimate + 30% value + 10% price). "
                "All scores are percentile ranks 1–100 vs 10,000+ global stocks."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "FactSet ticker (e.g. AAPL-US, 7203-TYO). Use resolve_ticker first if unsure."
                    }
                },
                "required": ["ticker"]
            }
        ),
        types.Tool(
            name="get_qarv_scores",
            description=(
                "Get YWR QARV scores for a specific stock ticker. "
                "QARV = 70% quality + 30% value composite. "
                "Returns quality_subscore, value_subscore, overall_rank_quality_70_value_30, "
                "and their ranks vs the full universe. All scores are percentile ranks 1–100."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "FactSet ticker (e.g. AAPL-US, 7203-TYO). Use resolve_ticker first if unsure."
                    }
                },
                "required": ["ticker"]
            }
        ),
        types.Tool(
            name="get_top_ranked",
            description=(
                "Get the top-ranked stocks from the YWR universe, sorted by a chosen score. "
                "Use sort_by='estimate_score' for earnings revision leaders, "
                "'value_score' for cheapest stocks, 'price_score' for momentum leaders, "
                "'total_score' for the composite factor rank, "
                "or 'qarv' for quality+value composite. "
                "Optionally filter by country or industry."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sort_by": {
                        "type": "string",
                        "enum": ["total_score", "estimate_score", "value_score", "price_score", "qarv"],
                        "description": "Which score to rank by. Default: total_score",
                        "default": "total_score"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of results (default 20, max 100)",
                        "default": 20
                    },
                    "country": {
                        "type": "string",
                        "description": "Filter by country name (e.g. United States, Japan, Germany)"
                    },
                    "industry": {
                        "type": "string",
                        "description": "Filter by industry (e.g. Semiconductors, Banks, Oil)"
                    }
                }
            }
        ),
        types.Tool(
            name="resolve_ticker",
            description=(
                "Search for a stock by company name, brand, or approximate ticker and return "
                "the matching FactSet ticker(s). Always use this first when the user provides "
                "a company name or an uncertain ticker before calling get_factor_scores or get_qarv_scores."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Company name, brand, or ticker (e.g. Apple, Nvidia, AAPL, 7203)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (default 5)",
                        "default": 5
                    }
                },
                "required": ["query"]
            }
        ),
    ]


# ── Tool handlers ─────────────────────────────────────────────────────────────

@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    try:
        if name == "get_factor_scores":
            result = _get_factor_scores(arguments["ticker"])
        elif name == "get_qarv_scores":
            result = _get_qarv_scores(arguments["ticker"])
        elif name == "get_top_ranked":
            result = _get_top_ranked(arguments)
        elif name == "resolve_ticker":
            result = _resolve_ticker(arguments["query"], arguments.get("limit", 5))
        else:
            result = {"error": f"Unknown tool: {name}"}
    except Exception as e:
        logger.error(f"Tool {name} error: {e}", exc_info=True)
        result = {"error": str(e)}

    return [types.TextContent(type="text", text=json.dumps(result, default=str))]


# ── Implementations ───────────────────────────────────────────────────────────

def _get_factor_scores(ticker: str) -> dict:
    ticker = ticker.strip().upper()
    rows = query(
        """
        SELECT ticker, name, country, industry, mkt_val,
               estimate_score, value_score, price_score, total_score, date
        FROM ywr_factor_scores
        WHERE ticker = %s
          AND date = (SELECT MAX(date) FROM ywr_factor_scores)
        LIMIT 1
        """,
        (ticker,)
    )
    if not rows:
        # Try partial match
        rows = query(
            """
            SELECT ticker, name, country, industry, mkt_val,
                   estimate_score, value_score, price_score, total_score, date
            FROM ywr_factor_scores
            WHERE ticker ILIKE %s
              AND date = (SELECT MAX(date) FROM ywr_factor_scores)
            ORDER BY ticker
            LIMIT 1
            """,
            (f"%{ticker}%",)
        )
    if not rows:
        return {"error": f"No factor scores found for ticker: {ticker}. Try resolve_ticker first."}
    return rows[0]


def _get_qarv_scores(ticker: str) -> dict:
    ticker = ticker.strip().upper()
    rows = query(
        """
        SELECT ticker, name, country, industry, mcap_mn,
               quality_subscore, value_subscore, total_score,
               quality_subscore_rank, value_subscore_rank,
               overall_rank_quality_70_value_30, date
        FROM qarv_scores
        WHERE ticker = %s
          AND date = (SELECT MAX(date) FROM qarv_scores)
        LIMIT 1
        """,
        (ticker,)
    )
    if not rows:
        rows = query(
            """
            SELECT ticker, name, country, industry, mcap_mn,
                   quality_subscore, value_subscore, total_score,
                   quality_subscore_rank, value_subscore_rank,
                   overall_rank_quality_70_value_30, date
            FROM qarv_scores
            WHERE ticker ILIKE %s
              AND date = (SELECT MAX(date) FROM qarv_scores)
            ORDER BY ticker
            LIMIT 1
            """,
            (f"%{ticker}%",)
        )
    if not rows:
        return {"error": f"No QARV scores found for ticker: {ticker}. Try resolve_ticker first."}
    return rows[0]


def _get_top_ranked(args: dict) -> dict:
    sort_by  = args.get("sort_by", "total_score")
    limit    = min(int(args.get("limit", 20)), 100)
    country  = args.get("country")
    industry = args.get("industry")

    country_filter  = "AND f.country ILIKE %s"  if country  else ""
    industry_filter = "AND f.industry ILIKE %s" if industry else ""

    if sort_by == "qarv":
        params = []
        if country:  params.append(f"%{country}%")
        if industry: params.append(f"%{industry}%")
        params.append(limit)
        rows = query(
            f"""
            WITH latest AS (SELECT MAX(date) AS d FROM qarv_scores)
            SELECT q.ticker, q.name, q.country, q.industry, q.mcap_mn,
                   q.quality_subscore, q.value_subscore,
                   q.overall_rank_quality_70_value_30 AS qarv_score,
                   q.date
            FROM qarv_scores q, latest
            WHERE q.date = latest.d
              AND q.overall_rank_quality_70_value_30 IS NOT NULL
              AND q.overall_rank_quality_70_value_30::text != 'NaN'
              {country_filter} {industry_filter}
            ORDER BY q.overall_rank_quality_70_value_30 DESC NULLS LAST
            LIMIT %s
            """,
            tuple(params)
        )
        return {"sort_by": "qarv", "count": len(rows), "results": rows}

    # Factor model sorts
    valid_cols = {"total_score", "estimate_score", "value_score", "price_score"}
    if sort_by not in valid_cols:
        sort_by = "total_score"

    params = []
    if country:  params.append(f"%{country}%")
    if industry: params.append(f"%{industry}%")
    params.append(limit)

    rows = query(
        f"""
        WITH latest AS (SELECT MAX(date) AS d FROM ywr_factor_scores)
        SELECT f.ticker, f.name, f.country, f.industry, f.mkt_val,
               f.estimate_score, f.value_score, f.price_score, f.total_score, f.date
        FROM ywr_factor_scores f, latest
        WHERE f.date = latest.d
          AND f.{sort_by} IS NOT NULL
          AND f.{sort_by}::text != 'NaN'
          {country_filter} {industry_filter}
        ORDER BY f.{sort_by} DESC NULLS LAST
        LIMIT %s
        """,
        tuple(params)
    )
    return {"sort_by": sort_by, "count": len(rows), "results": rows}


def _resolve_ticker(query_str: str, limit: int = 5) -> dict:
    limit = min(max(1, limit), 20)
    like  = f"%{query_str.strip()}%"
    upper = query_str.strip().upper()

    rows = query(
        """
        WITH latest AS (SELECT MAX(date) AS d FROM ywr_factor_scores),
        candidates AS (
            SELECT ticker AS factset_ticker, name,
                   CASE
                       WHEN ticker = %s THEN 0
                       WHEN ticker ILIKE %s THEN 1
                       ELSE 2
                   END AS rank
            FROM ywr_factor_scores, latest
            WHERE date = latest.d
              AND (ticker ILIKE %s OR name ILIKE %s)
        )
        SELECT DISTINCT ON (factset_ticker) factset_ticker, name
        FROM candidates
        ORDER BY factset_ticker, rank
        LIMIT %s
        """,
        (upper, like, like, like, limit)
    )
    if not rows:
        return {"query": query_str, "matches": [], "message": "No matching tickers found"}
    return {"query": query_str, "matches": rows}


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
