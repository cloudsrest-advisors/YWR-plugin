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
import json
import logging
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
                "Use this tool when the user asks for 'factor scores', 'estimate score', "
                "'momentum score', or 'factor value score'. "
                "Use resolve_ticker first if you only have a company name."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "FactSet ticker (e.g. AAPL-US, 7203-TYO, 000660-KRX)"
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
                        "description": "FactSet ticker (e.g. AAPL-US, 7203-TYO)"
                    }
                },
                "required": ["ticker"]
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
                "Search for a stock by company name or approximate ticker and return "
                "the matching FactSet ticker. Always use this first when the user provides "
                "a company name before calling get_factor_scores or get_qarv_scores."
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
            result = api_get(f"/rankings/company/{arguments['ticker']}")
            # Return just the factor scores portion
            if "factor_scores" in result:
                result = result["factor_scores"] or {"error": f"No factor scores found for {arguments['ticker']}"}

        elif name == "get_qarv_scores":
            result = api_get(f"/rankings/company/{arguments['ticker']}")
            # Return just the QARV scores portion
            if "qarv_scores" in result:
                result = result["qarv_scores"] or {"error": f"No QARV scores found for {arguments['ticker']}"}

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
            # Use the top endpoint with a search — fall back to company lookup
            result = api_get(
                f"/rankings/company/{arguments['query']}",
            )
            if "error" not in result:
                # Return ticker map info
                result = {
                    "query": arguments["query"],
                    "ticker_map": result.get("ticker_map"),
                    "factor_scores": result.get("factor_scores"),
                }
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
