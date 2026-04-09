"""
YWR Intelligence MCP Server — HTTP/SSE transport for Railway deployment.

Users connect via Claude Desktop or Claude.ai by adding to their MCP config:

  {
    "mcpServers": {
      "ywr": {
        "type": "sse",
        "url": "https://ywr-plugin.up.railway.app/sse?token=YOUR_TOKEN"
      }
    }
  }

Set MCP_ACCESS_TOKENS in Railway env vars as a comma-separated list of valid tokens.
If not set, the server is open (useful for testing).
"""

import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from mcp.server.sse import SseServerTransport
from starlette.routing import Mount

from mcp_server import server as mcp_server

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Optional access control — comma-separated list of valid tokens
_raw_tokens = os.getenv("MCP_ACCESS_TOKENS", "")
VALID_TOKENS: set[str] = {t.strip() for t in _raw_tokens.split(",") if t.strip()}


def _check_token(request: Request):
    """Validate token from query param or Authorization header. No-op if no tokens configured."""
    if not VALID_TOKENS:
        return  # open access
    token = (
        request.query_params.get("token")
        or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    )
    if token not in VALID_TOKENS:
        raise HTTPException(status_code=401, detail="Invalid or missing token")


sse_transport = SseServerTransport("/messages/")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("YWR MCP server starting")
    yield
    logger.info("YWR MCP server stopping")


app = FastAPI(title="YWR Intelligence MCP", lifespan=lifespan)

# Mount the SSE message handler
app.mount("/messages/", app=sse_transport.handle_post_message)


@app.get("/sse")
async def handle_sse(request: Request):
    """SSE endpoint — clients connect here to start an MCP session."""
    _check_token(request)
    async with sse_transport.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await mcp_server.run(
            streams[0], streams[1], mcp_server.create_initialization_options()
        )


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok", "server": "ywr-intelligence-mcp"})


@app.get("/")
async def root():
    return JSONResponse({
        "name": "YWR Intelligence MCP Server",
        "transport": "SSE",
        "endpoint": "/sse",
        "docs": "Add ?token=YOUR_TOKEN to the SSE URL in your Claude MCP config",
    })
