"""
YWR Intelligence MCP Server
============================
Connects to the YWR Data API using a subscriber API key.
No direct database access — auth and rate limiting handled by the API.

Setup for subscribers:
  pip install mcp httpx python-dotenv

Claude Desktop config (~/.claude/claude_desktop_config.json or
~/Library/Application Support/Claude/claude_desktop_config.json on Mac):

  {
    "mcpServers": {
      "ywr": {
        "command": "/path/to/python",
        "args": ["/path/to/mcp_server.py"],
        "env": {
          "YWR_API_KEY": "your-api-key-here",
          "YWR_API_URL": "https://ywr-data-api.up.railway.app"
        }
      }
    }
  }
"""

import os
import re
import json
import logging
from urllib.parse import quote
from typing import Any

import httpx
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

YWR_API_KEY = os.getenv("YWR_API_KEY")
YWR_API_URL = os.getenv("YWR_API_URL", "https://ywr-data-api.up.railway.app").rstrip("/")

server = Server("ywr-intelligence")


# ── API client ────────────────────────────────────────────────────────────────

def api_get(path: str, params: dict = None) -> dict:
    if not YWR_API_KEY:
        return {"error": "YWR_API_KEY not set. Add it to your Claude Desktop MCP config."}
    try:
        r = httpx.get(
            f"{YWR_API_URL}{path}",
            headers={"X-YWR-Api-Key": YWR_API_KEY},
            params=params or {},
            timeout=30,
        )
        if r.status_code == 401:
            return {"error": "Invalid API key. Contact YWR Intelligence to verify your subscription."}
        if r.status_code == 404:
            return {"error": "Not found", "detail": r.json().get("detail", "")}
        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"API error {e.response.status_code} for {path}")
        return {"error": f"API error: {e.response.status_code}", "detail": e.response.text}
    except Exception as e:
        logger.error(f"API request failed: {e}")
        return {"error": str(e)}


def api_post(path: str, body: dict) -> dict:
    if not YWR_API_KEY:
        return {"error": "YWR_API_KEY not set. Add it to your Claude Desktop MCP config."}
    try:
        r = httpx.post(
            f"{YWR_API_URL}{path}",
            headers={"X-YWR-Api-Key": YWR_API_KEY},
            json=body,
            timeout=30,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error(f"API POST {path} failed: {e}")
        return {"error": str(e)}


# Queries the Registry accepts (mirrors the data API's check)
_REGISTRY_QUERY_RE = re.compile(r"^[A-Z0-9][A-Z0-9.:-]{0,63}$")


def resolve_via_registry(query: str) -> dict | None:
    """Resolve a ticker or name through YWR Registry. None if no match.
    Registry aliases are matched with spaces and punctuation stripped, so
    "Bank of America" is sent as "BANKOFAMERICA" and still matches."""
    q = re.sub(r"[^A-Z0-9.:-]", "", query.upper())
    if not _REGISTRY_QUERY_RE.fullmatch(q):
        return None
    result = api_post("/registry/resolve", {"tickers": [q]})
    for row in result.get("resolved", []):
        if row.get("factset_ticker"):
            return {
                "query": query,
                "source": "YWR Registry",
                "factset_ticker": row["factset_ticker"],
                "name": row.get("target_name"),
                "registry_uid": row.get("target_uid"),
            }
    return None


# ── Tool definitions ──────────────────────────────────────────────────────────

@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="get_factor_scores",
            description=(
                "Get YWR factor model scores for a specific stock ticker. "
                "Returns: estimate_score (earnings revision momentum), "
                "factor_value_score (valuation cheapness — this is the FACTOR MODEL value score, "
                "different from the QARV value score), "
                "price_score (6-month price momentum), and "
                "total_score (composite: 60% estimate + 30% factor_value + 10% price). "
                "All scores are percentile ranks 1–100 vs 10,000+ global stocks. "
                "Requires the exact FactSet ticker (never substitutes another company). "
                "Use this tool when the user asks for 'factor scores', 'estimate score', "
                "'momentum score', or 'factor value score'. "
                "Use resolve_ticker first if you only have a company name."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "FactSet ticker (e.g. AAPL-USA, 7203-TKS, 000660-KRX)"
                    }
                },
                "required": ["ticker"]
            }
        ),
        types.Tool(
            name="get_qarv_scores",
            description=(
                "Get YWR QARV scores for a specific stock ticker. "
                "QARV = 70% quality + 30% value composite — focuses on high-quality businesses "
                "at reasonable prices, with NO momentum signal. "
                "Coverage is narrower than the factor model (~4,700 vs ~11,000 stocks): most developed "
                "markets, partial China/Japan/Taiwan, and none for South Korea, Saudi Arabia, India or "
                "Thailand — a stock outside it returns an explicit 'no QARV score' error, so use "
                "get_factor_scores there. Requires the exact FactSet ticker. "
                "Returns: quality_subscore (business quality), "
                "qarv_value_score (valuation — this is the QARV value score, "
                "different from the factor model value score), "
                "and overall_rank_quality_70_value_30 (composite QARV rank). "
                "All scores are percentile ranks 1–100. "
                "Use this tool when the user asks for 'QARV scores', 'quality score', "
                "'quality value score', or 'QARV value score'. "
                "Use resolve_ticker first if you only have a company name."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "FactSet ticker (e.g. AAPL-USA, 7203-TKS)"
                    }
                },
                "required": ["ticker"]
            }
        ),
        types.Tool(
            name="get_score_history",
            description=(
                "Get historical YWR scores for a specific stock over a date range: both "
                "factor model scores (factor_score, estimate_score, value_score, price_score) "
                "and QARV scores (qarv_score, quality_subscore, value_subscore) over time. "
                "All scores are percentile ranks 1–100. With weekly/monthly/quarterly frequency, "
                "each row is the last available score in that period. "
                "Use this tool when the user asks how a stock's scores have changed, "
                "for score trends, or for scores as of a past date. "
                "Use resolve_ticker first if you only have a company name."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "FactSet ticker (e.g. AAPL-USA, 7203-TKS)"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date YYYY-MM-DD (default: 1 year before end_date)"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date YYYY-MM-DD (default: today)"
                    },
                    "frequency": {
                        "type": "string",
                        "enum": ["daily", "weekly", "monthly", "quarterly"],
                        "description": "Sampling frequency. Default: monthly. Use daily only for short ranges.",
                        "default": "monthly"
                    }
                },
                "required": ["ticker"]
            }
        ),
        types.Tool(
            name="get_portfolio",
            description=(
                "Get a YWR model portfolio — e.g. 'Dirty Dividends' or 'New Era Finance' — with "
                "each holding's weight (percent) and its current YWR factor and QARV scores, plus "
                "portfolio-weighted average scores. 'input_ticker' is the ticker the portfolio "
                "actually holds (may be an ADR such as BCS); scores come from the company's home "
                "listing (factset_ticker). ETFs and cash have no scores. "
                "Call with no portfolio to list the available portfolios and their dates. "
                "Pass date to see the portfolio as it was on a past date (latest rebalance on or "
                "before that date, with scores from that time)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "portfolio": {
                        "type": "string",
                        "description": "Portfolio name, e.g. 'Dirty Dividends'. Omit to list all portfolios."
                    },
                    "date": {
                        "type": "string",
                        "description": "As-of date YYYY-MM-DD (default: latest)"
                    }
                }
            }
        ),
        types.Tool(
            name="get_top_ranked",
            description=(
                "Get the top-ranked stocks from the YWR universe. "
                "sort_by options: 'total_score' (composite), 'estimate_score' (earnings revision momentum), "
                "'value_score' (cheapest stocks), 'price_score' (momentum leaders), 'qarv' (quality+value). "
                "Optionally filter by country or industry."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sort_by": {
                        "type": "string",
                        "enum": ["total_score", "estimate_score", "value_score", "price_score", "qarv"],
                        "description": "Score to rank by. Default: total_score",
                        "default": "total_score"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of results (default 20, max 100)",
                        "default": 20
                    },
                    "country": {
                        "type": "string",
                        "description": "Filter by country (e.g. United States, Japan, Germany)"
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
                "Search for a stock by ticker (any common format, including old tickers) or "
                "company name and return the matching FactSet ticker and name, using the "
                "YWR Registry with a fuzzy-search fallback. Always use this first when the "
                "user provides a company name before calling get_factor_scores, "
                "get_qarv_scores or get_score_history."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Company name or ticker (e.g. Apple, Nvidia, AAPL, Samsung)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 5)",
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
            # exact ticker only: never answer with a different company's scores
            result = api_get(f"/rankings/company/{quote(arguments['ticker'], safe='')}", {"exact": "true"})
            if "factor_scores" in result:
                result = result["factor_scores"] or {
                    "error": f"No factor scores for {result['resolved_ticker']} ({result['name']})."}
            elif "error" in result:
                result = {"error": f"No scored stock with ticker {arguments['ticker']}. "
                                   "Use resolve_ticker to find the FactSet ticker."}

        elif name == "get_qarv_scores":
            result = api_get(f"/rankings/company/{quote(arguments['ticker'], safe='')}", {"exact": "true"})
            if "qarv_scores" in result:
                result = result["qarv_scores"] or {
                    "error": f"No QARV score for {result['resolved_ticker']} ({result['name']}): it is not in "
                             "the QARV universe, which is narrower than the factor model's (mostly developed "
                             "markets). Use get_factor_scores for this stock."}
            elif "error" in result:
                result = {"error": f"No scored stock with ticker {arguments['ticker']}. "
                                   "Use resolve_ticker to find the FactSet ticker."}

        elif name == "get_score_history":
            params = {"frequency": arguments.get("frequency", "monthly"), "exact": "true"}
            if arguments.get("start_date"):
                params["start"] = arguments["start_date"]
            if arguments.get("end_date"):
                params["end"] = arguments["end_date"]
            result = api_get(f"/rankings/company/{quote(arguments['ticker'], safe='')}/history", params)

        elif name == "get_portfolio":
            if arguments.get("portfolio"):
                params = {"date": arguments["date"]} if arguments.get("date") else {}
                result = api_get(f"/portfolios/{quote(arguments['portfolio'], safe='')}", params)
            else:
                result = api_get("/portfolios")

        elif name == "get_top_ranked":
            sort_by = arguments.get("sort_by", "total_score")
            params = {"limit": arguments.get("limit", 20)}
            if arguments.get("country"):
                params["country"] = arguments["country"]
            if arguments.get("industry"):
                params["industry"] = arguments["industry"]

            if sort_by == "qarv":
                params["score_type"] = "qarv"
            else:
                params["score_type"] = "factor"
                params["sort_by"] = sort_by

            result = api_get("/rankings/top", params)

        elif name == "resolve_ticker":
            # YWR Registry first; fall back to fuzzy search over scores and ticker_map
            result = resolve_via_registry(arguments["query"])
            if result is None:
                fallback = api_get(f"/rankings/company/{quote(arguments['query'], safe='')}")
                if "error" not in fallback:
                    ticker_map = fallback.get("ticker_map") or {}
                    scores = fallback.get("factor_scores") or fallback.get("qarv_scores") or {}
                    result = {
                        "query": arguments["query"],
                        "source": "search",
                        "match": fallback.get("match"),
                        "factset_ticker": fallback.get("resolved_ticker") or ticker_map.get("factset_ticker") or scores.get("ticker"),
                        "name": fallback.get("name") or ticker_map.get("name") or scores.get("name"),
                    }
                    if fallback.get("match") == "fuzzy":
                        result["note"] = "Best partial match — confirm this is the company the user meant."
                else:
                    result = {"query": arguments["query"], "error": "No matching ticker found. Try a different name or ticker format."}

        else:
            result = {"error": f"Unknown tool: {name}"}

    except Exception as e:
        logger.error(f"Tool {name} error: {e}", exc_info=True)
        result = {"error": str(e)}

    return [types.TextContent(type="text", text=json.dumps(result, default=str))]


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
