"""
YWR Intelligence MCP Server — HTTP/SSE transport for Railway deployment.

Subscribers add a custom connector in Claude with just the URL:

  https://api.ywr-intelligence.world/mcp

Claude discovers the OAuth sign-in (oauth.py), registers itself, and opens the
YWR login page: email -> 6-digit code -> connected. Access follows Stevie's
authorized_users table. OAuth is enabled when NEON_STEVIE_DB_URL is set.

Static tokens still work alongside OAuth (header "Authorization: Bearer <token>"
or ?token=<token>): set MCP_ACCESS_TOKENS as a comma-separated list. With neither
OAuth nor static tokens configured the server is open (useful for testing).

  https://api.ywr-intelligence.world/sse   (legacy SSE transport; static tokens only)
"""

import os
import logging
from contextlib import asynccontextmanager

import anyio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mcp.server.auth.routes import (
    build_resource_metadata_url, create_auth_routes, create_protected_resource_routes,
)
from mcp.server.auth.settings import ClientRegistrationOptions, RevocationOptions
from mcp.server.sse import SseServerTransport
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.routing import Route

import oauth
from mcp_server import server as mcp_server

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Static access tokens — comma-separated list (kept alongside OAuth)
_raw_tokens = os.getenv("MCP_ACCESS_TOKENS", "")
VALID_TOKENS: set[str] = {t.strip() for t in _raw_tokens.split(",") if t.strip()}

OAUTH_ENABLED = bool(oauth.STEVIE_DB_URL)
oauth_provider = oauth.YWROAuthProvider()
token_verifier = oauth.YWRTokenVerifier(oauth_provider, VALID_TOKENS)
RESOURCE_METADATA_URL = str(build_resource_metadata_url(oauth.RESOURCE_URL))


async def _authorized(request: Request) -> bool:
    """Bearer header (OAuth access token or static token) or ?token= (static only)."""
    if not VALID_TOKENS and not OAUTH_ENABLED:
        return True  # open access
    header = request.headers.get("Authorization", "")
    if header.lower().startswith("bearer "):
        if await token_verifier.verify_token(header[7:].strip()):
            return True
    return request.query_params.get("token", "") in VALID_TOKENS


def _unauthorized() -> JSONResponse:
    """401 that points Claude at the OAuth discovery document."""
    headers = {}
    if OAUTH_ENABLED:
        headers["WWW-Authenticate"] = (f'Bearer error="invalid_token", error_description="Authentication required", '
                                       f'resource_metadata="{RESOURCE_METADATA_URL}"')
    return JSONResponse({"detail": "Invalid or missing token"}, status_code=401, headers=headers)


sse_transport = SseServerTransport("/messages/")

# Stateless so any replica can serve any request — no session to lose across restarts
http_session_manager = StreamableHTTPSessionManager(
    app=mcp_server, stateless=True, json_response=True
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"YWR MCP server starting (OAuth {'on' if OAUTH_ENABLED else 'off'}, "
                f"{len(VALID_TOKENS)} static tokens)")
    if OAUTH_ENABLED:
        await anyio.to_thread.run_sync(oauth.init_schema)
    async with http_session_manager.run():
        yield
    logger.info("YWR MCP server stopping")


app = FastAPI(title="YWR Intelligence MCP", lifespan=lifespan)

if OAUTH_ENABLED:
    # /.well-known/oauth-authorization-server, /authorize, /token, /register, /revoke
    app.router.routes.extend(create_auth_routes(
        oauth_provider,
        issuer_url=oauth.ISSUER_URL,
        client_registration_options=ClientRegistrationOptions(
            enabled=True, valid_scopes=[oauth.SCOPE], default_scopes=[oauth.SCOPE]),
        revocation_options=RevocationOptions(enabled=True),
    ))
    # /.well-known/oauth-protected-resource/mcp (RFC 9728), plus the bare path some clients try first
    resource_routes = create_protected_resource_routes(
        oauth.RESOURCE_URL, [oauth.ISSUER_URL], scopes_supported=[oauth.SCOPE],
        resource_name="YWR Intelligence")
    app.router.routes.extend(resource_routes)
    app.router.routes.append(Route("/.well-known/oauth-protected-resource",
                                   endpoint=resource_routes[0].endpoint, methods=["GET", "OPTIONS"]))
    app.router.routes.extend(oauth.login_routes)

# Mount the SSE message handler
app.mount("/messages/", app=sse_transport.handle_post_message)


@app.get("/sse")
async def handle_sse(request: Request):
    """SSE endpoint — clients connect here to start an MCP session."""
    if not await _authorized(request):
        return _unauthorized()
    async with sse_transport.connect_sse(
        request.scope, request.receive, request._send
    ) as streams:
        await mcp_server.run(
            streams[0], streams[1], mcp_server.create_initialization_options()
        )


class StreamableHTTPApp:
    """ASGI endpoint for Streamable HTTP transport, with token check."""

    async def __call__(self, scope, receive, send):
        if not await _authorized(Request(scope)):
            await _unauthorized()(scope, receive, send)
            return
        await http_session_manager.handle_request(scope, receive, send)


app.router.routes.append(Route("/mcp", endpoint=StreamableHTTPApp(), methods=["GET", "POST", "DELETE"]))


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok", "server": "ywr-intelligence-mcp"})


@app.get("/")
async def root():
    return JSONResponse({
        "name": "YWR Intelligence MCP Server",
        "endpoints": {"streamable_http": "/mcp", "sse": "/sse"},
        "docs": "Add https://api.ywr-intelligence.world/mcp as a custom connector in Claude and sign in "
                "with your YWR subscription email.",
    })
