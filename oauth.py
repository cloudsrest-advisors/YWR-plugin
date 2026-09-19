"""
OAuth sign-in for the YWR MCP connector.

Claude discovers this automatically: a subscriber adds the connector with just
the URL, Claude registers itself (dynamic client registration), and a YWR login
page opens. The subscriber enters their email, gets a 6-digit code by email,
types it in, and is connected. No headers or tokens to copy.

Who may sign in: the same rule as Stevie — an active, unexpired row in Stevie's
authorized_users table (kept in sync with Stripe/Substack by Stevie's webhooks).
Access tokens last an hour; every refresh re-checks the subscription, so a
cancelled subscriber loses access within the hour.

Storage: mcp_oauth_* tables in the Stevie database. Only SHA-256 hashes of
tokens and codes are stored.

Env: NEON_STEVIE_DB_URL, SENDGRID_API_KEY, SENDGRID_FROM_EMAIL (as Stevie),
     PUBLIC_URL (default https://api.ywr-intelligence.world).
"""

import hashlib
import html
import json
import logging
import os
import secrets
import time

import anyio
import httpx
import psycopg
from psycopg.rows import dict_row
from pydantic import AnyHttpUrl
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse
from starlette.routing import Route

from mcp.server.auth.provider import (
    AccessToken, AuthorizationCode, AuthorizationParams, RefreshToken,
    TokenError, TokenVerifier, construct_redirect_uri,
)
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken

logger = logging.getLogger(__name__)

PUBLIC_URL = os.getenv("PUBLIC_URL", "https://api.ywr-intelligence.world").rstrip("/")
STEVIE_DB_URL = os.getenv("NEON_STEVIE_DB_URL", "")
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
SENDGRID_FROM_EMAIL = os.getenv("SENDGRID_FROM_EMAIL", "noreply@ywr-intelligence.world")

SCOPE = "ywr"
ACCESS_TTL = 3600               # 1 hour
REFRESH_TTL = 30 * 24 * 3600    # 30 days
AUTH_CODE_TTL = 300             # OAuth authorization code: 5 minutes
LOGIN_TTL = 1800                # a pending sign-in: 30 minutes
EMAIL_CODE_TTL = 600            # emailed 6-digit code: 10 minutes
MAX_CODE_ATTEMPTS = 5            # wrong guesses per emailed code
MAX_CODES_PER_HOUR = 5           # code emails per address per hour

SCHEMA = """
CREATE TABLE IF NOT EXISTS mcp_oauth_clients (
    client_id   text PRIMARY KEY,
    client_info jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
-- a sign-in in progress: the OAuth request from Claude, then the email + emailed code
CREATE TABLE IF NOT EXISTS mcp_oauth_logins (
    login_id        text PRIMARY KEY,
    client_id       text NOT NULL,
    params          jsonb NOT NULL,
    email           text,
    code_hash       text,
    code_expires_at timestamptz,
    attempts        int NOT NULL DEFAULT 0,
    expires_at      timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS mcp_oauth_codes (
    code_hash  text PRIMARY KEY,
    client_id  text NOT NULL,
    email      text NOT NULL,
    data       jsonb NOT NULL,
    expires_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS mcp_oauth_tokens (
    token_hash text PRIMARY KEY,
    kind       text NOT NULL CHECK (kind IN ('access', 'refresh')),
    family     text NOT NULL,           -- one sign-in; revoking one token revokes the family
    client_id  text NOT NULL,
    email      text NOT NULL,
    scopes     text[] NOT NULL,
    resource   text,
    expires_at timestamptz NOT NULL,
    revoked    boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS mcp_oauth_tokens_family_idx ON mcp_oauth_tokens (family);
"""


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _db(sql: str, params: tuple = (), fetch: str | None = None):
    """Run one statement against the Stevie database (sync; call via _run)."""
    with psycopg.connect(STEVIE_DB_URL, row_factory=dict_row, connect_timeout=10, autocommit=True) as conn:
        cur = conn.execute(sql, params)
        if fetch == "one":
            return cur.fetchone()
        if fetch == "all":
            return cur.fetchall()


async def _run(sql: str, params: tuple = (), fetch: str | None = None):
    return await anyio.to_thread.run_sync(lambda: _db(sql, params, fetch))


def init_schema() -> None:
    if STEVIE_DB_URL:
        _db(SCHEMA)
        _db("DELETE FROM mcp_oauth_logins WHERE expires_at < now()")
        _db("DELETE FROM mcp_oauth_codes WHERE expires_at < now()")
        _db("DELETE FROM mcp_oauth_tokens WHERE expires_at < now() - interval '1 day'")


async def is_subscriber(email: str) -> bool:
    """Stevie's rule (database.is_user_authorized): active, unexpired, case-insensitive."""
    row = await _run(
        """SELECT 1 FROM authorized_users
           WHERE lower(email) = lower(%s) AND active = true
             AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)""",
        (email,), fetch="one")
    return row is not None


class YWROAuthProvider:
    """OAuthAuthorizationServerProvider backed by the Stevie database."""

    # ---- clients (dynamic registration) ----
    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        row = await _run("SELECT client_info FROM mcp_oauth_clients WHERE client_id = %s", (client_id,), fetch="one")
        return OAuthClientInformationFull.model_validate(row["client_info"]) if row else None

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        await _run("INSERT INTO mcp_oauth_clients (client_id, client_info) VALUES (%s, %s) "
                   "ON CONFLICT (client_id) DO UPDATE SET client_info = EXCLUDED.client_info",
                   (client_info.client_id, client_info.model_dump_json()))

    # ---- authorize: hand off to the YWR login page ----
    async def authorize(self, client: OAuthClientInformationFull, params: AuthorizationParams) -> str:
        login_id = secrets.token_urlsafe(24)
        await _run("INSERT INTO mcp_oauth_logins (login_id, client_id, params, expires_at) "
                   "VALUES (%s, %s, %s, now() + make_interval(secs => %s))",
                   (login_id, client.client_id, params.model_dump_json(), LOGIN_TTL))
        return f"{PUBLIC_URL}/login?lid={login_id}"

    # ---- authorization code ----
    async def load_authorization_code(self, client: OAuthClientInformationFull, authorization_code: str):
        row = await _run("SELECT data FROM mcp_oauth_codes WHERE code_hash = %s AND client_id = %s AND expires_at > now()",
                         (_hash(authorization_code), client.client_id), fetch="one")
        return AuthorizationCode.model_validate(row["data"]) if row else None

    async def exchange_authorization_code(self, client: OAuthClientInformationFull,
                                          authorization_code: AuthorizationCode) -> OAuthToken:
        await _run("DELETE FROM mcp_oauth_codes WHERE code_hash = %s", (_hash(authorization_code.code),))
        email = authorization_code.subject
        if not email or not await is_subscriber(email):
            raise TokenError("invalid_grant", "This email no longer has an active YWR subscription.")
        return await self._issue(client.client_id, email, authorization_code.scopes,
                                 authorization_code.resource, family=secrets.token_hex(16))

    # ---- refresh: rotate, and re-check the subscription every time ----
    async def load_refresh_token(self, client: OAuthClientInformationFull, refresh_token: str):
        row = await _run("""SELECT * FROM mcp_oauth_tokens WHERE token_hash = %s AND kind = 'refresh'
                            AND client_id = %s AND NOT revoked AND expires_at > now()""",
                         (_hash(refresh_token), client.client_id), fetch="one")
        if not row:
            return None
        return RefreshToken(token=refresh_token, client_id=row["client_id"], scopes=row["scopes"],
                            expires_at=int(row["expires_at"].timestamp()), resource=row["resource"],
                            subject=row["email"])

    async def exchange_refresh_token(self, client: OAuthClientInformationFull, refresh_token: RefreshToken,
                                     scopes: list[str]) -> OAuthToken:
        row = await _run("UPDATE mcp_oauth_tokens SET revoked = true WHERE token_hash = %s RETURNING family",
                         (_hash(refresh_token.token),), fetch="one")
        family = row["family"] if row else secrets.token_hex(16)
        if not await is_subscriber(refresh_token.subject or ""):
            await _run("UPDATE mcp_oauth_tokens SET revoked = true WHERE family = %s", (family,))
            raise TokenError("invalid_grant", "This email no longer has an active YWR subscription.")
        return await self._issue(client.client_id, refresh_token.subject, scopes or refresh_token.scopes,
                                 refresh_token.resource, family=family)

    # ---- access tokens ----
    async def load_access_token(self, token: str) -> AccessToken | None:
        row = await _run("""SELECT * FROM mcp_oauth_tokens WHERE token_hash = %s AND kind = 'access'
                            AND NOT revoked AND expires_at > now()""", (_hash(token),), fetch="one")
        if not row:
            return None
        return AccessToken(token=token, client_id=row["client_id"], scopes=row["scopes"],
                           expires_at=int(row["expires_at"].timestamp()), resource=row["resource"],
                           subject=row["email"])

    async def revoke_token(self, token) -> None:
        await _run("""UPDATE mcp_oauth_tokens SET revoked = true WHERE family =
                      (SELECT family FROM mcp_oauth_tokens WHERE token_hash = %s)""", (_hash(token.token),))

    async def _issue(self, client_id: str, email: str, scopes: list[str], resource: str | None,
                     family: str) -> OAuthToken:
        access, refresh = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        for tok, kind, ttl in ((access, "access", ACCESS_TTL), (refresh, "refresh", REFRESH_TTL)):
            await _run("""INSERT INTO mcp_oauth_tokens (token_hash, kind, family, client_id, email, scopes, resource, expires_at)
                          VALUES (%s, %s, %s, %s, %s, %s, %s, now() + make_interval(secs => %s))""",
                       (_hash(tok), kind, family, client_id, email, scopes, resource, ttl))
        logger.info(f"OAuth tokens issued for {email} (client {client_id})")
        return OAuthToken(access_token=access, token_type="Bearer", expires_in=ACCESS_TTL,
                          refresh_token=refresh, scope=" ".join(scopes))


class YWRTokenVerifier(TokenVerifier):
    """OAuth access tokens, plus the legacy static tokens (MCP_ACCESS_TOKENS) during the switch."""

    def __init__(self, provider: YWROAuthProvider, static_tokens: set[str]):
        self.provider, self.static_tokens = provider, static_tokens

    async def verify_token(self, token: str) -> AccessToken | None:
        if token in self.static_tokens:
            return AccessToken(token=token, client_id="static-token", scopes=[SCOPE], subject="static-token")
        if not STEVIE_DB_URL:
            return None
        return await self.provider.load_access_token(token)


# ---------------------------------------------------------------- login page

def _page(body: str, status: int = 200) -> HTMLResponse:
    return HTMLResponse(f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>YWR Intelligence — Sign in</title>
<style>
 :root {{ --bg:#f6f5f2; --card:#fff; --ink:#1c1c1c; --muted:#666; --line:#dcd9d2; --accent:#1f4e79; }}
 @media (prefers-color-scheme: dark) {{ :root {{ --bg:#141414; --card:#1e1e1e; --ink:#eee; --muted:#aaa; --line:#333; --accent:#7fb0e0; }} }}
 body {{ margin:0; background:var(--bg); color:var(--ink); font:16px/1.5 -apple-system,system-ui,Segoe UI,sans-serif;
        display:flex; min-height:100vh; align-items:center; justify-content:center; padding:16px; box-sizing:border-box; }}
 .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:32px; width:100%; max-width:380px; }}
 h1 {{ font-size:20px; margin:0 0 4px; }} p {{ color:var(--muted); margin:0 0 20px; }}
 input {{ width:100%; box-sizing:border-box; padding:12px; font-size:16px; border:1px solid var(--line);
         border-radius:8px; background:var(--bg); color:var(--ink); margin-bottom:12px; }}
 button {{ width:100%; padding:12px; font-size:16px; border:0; border-radius:8px; background:var(--accent); color:#fff; cursor:pointer; }}
 .err {{ color:#b3261e; margin:0 0 12px; }}
</style></head><body><div class="card"><h1>YWR Intelligence</h1>{body}</div></body></html>""", status_code=status)


async def _load_login(login_id: str):
    return await _run("SELECT * FROM mcp_oauth_logins WHERE login_id = %s AND expires_at > now()",
                      (login_id,), fetch="one")


def _email_form(lid: str, error: str = "") -> HTMLResponse:
    err = f'<p class="err">{html.escape(error)}</p>' if error else ""
    return _page(f"""<p>Sign in to connect YWR to Claude. Use the email address of your YWR subscription.</p>{err}
<form method="post" action="/login"><input type="hidden" name="lid" value="{html.escape(lid)}">
<input type="email" name="email" placeholder="you@example.com" autocomplete="email" required autofocus>
<button type="submit">Email me a sign-in code</button></form>""")


def _code_form(lid: str, email: str, error: str = "") -> HTMLResponse:
    err = f'<p class="err">{html.escape(error)}</p>' if error else ""
    return _page(f"""<p>We sent a 6-digit code to <b>{html.escape(email)}</b>. It expires in 10 minutes.</p>{err}
<form method="post" action="/login/code"><input type="hidden" name="lid" value="{html.escape(lid)}">
<input name="code" inputmode="numeric" pattern="[0-9]{{6}}" maxlength="6" placeholder="123456" autocomplete="one-time-code" required autofocus>
<button type="submit">Sign in</button></form>""")


async def _send_code(email: str, code: str) -> None:
    if not SENDGRID_API_KEY:
        raise RuntimeError("SENDGRID_API_KEY not set")
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {SENDGRID_API_KEY}"},
            json={"personalizations": [{"to": [{"email": email}]}],
                  "from": {"email": SENDGRID_FROM_EMAIL, "name": "YWR Intelligence"},
                  "subject": f"Your YWR sign-in code: {code}",
                  "content": [{"type": "text/plain", "value":
                      f"Your code to connect YWR Intelligence to Claude is {code}\n\n"
                      "It expires in 10 minutes. If you didn't request this, you can ignore this email."}]})
        r.raise_for_status()


async def login_get(request: Request):
    lid = request.query_params.get("lid", "")
    if not await _load_login(lid):
        return _page("<p>This sign-in link has expired. Please start again from Claude.</p>", 400)
    return _email_form(lid)


async def login_post(request: Request):
    form = await request.form()
    lid, email = str(form.get("lid", "")), str(form.get("email", "")).strip()
    if not await _load_login(lid):
        return _page("<p>This sign-in has expired. Please start again from Claude.</p>", 400)
    if not await is_subscriber(email):
        return _email_form(lid, "We couldn't find an active YWR subscription for that email. "
                                "Please use the address you subscribe with.")
    sent = await _run("""SELECT count(*) AS n FROM mcp_oauth_logins WHERE lower(email) = lower(%s)
                         AND code_expires_at > now() + make_interval(secs => %s) - interval '1 hour'""",
                      (email, EMAIL_CODE_TTL), fetch="one")
    if sent["n"] >= MAX_CODES_PER_HOUR:
        return _email_form(lid, "Too many sign-in codes requested for this email. Please try again in an hour.")
    code = f"{secrets.randbelow(10**6):06d}"
    await _run("""UPDATE mcp_oauth_logins SET email = %s, code_hash = %s, attempts = 0,
                  code_expires_at = now() + make_interval(secs => %s) WHERE login_id = %s""",
               (email, _hash(lid + code), EMAIL_CODE_TTL, lid))
    try:
        await _send_code(email, code)
    except Exception as e:
        logger.error(f"Sign-in code email failed for {email}: {e}")
        return _email_form(lid, "We couldn't send the email just now. Please try again in a minute.")
    return _code_form(lid, email)


async def login_code(request: Request):
    form = await request.form()
    lid, code = str(form.get("lid", "")), str(form.get("code", "")).strip()
    login = await _load_login(lid)
    if not login or not login["email"]:
        return _page("<p>This sign-in has expired. Please start again from Claude.</p>", 400)
    if login["attempts"] >= MAX_CODE_ATTEMPTS or login["code_expires_at"].timestamp() < time.time():
        return _email_form(lid, "That code has expired. Enter your email to get a new one.")
    if not secrets.compare_digest(_hash(lid + code), login["code_hash"] or ""):
        await _run("UPDATE mcp_oauth_logins SET attempts = attempts + 1 WHERE login_id = %s", (lid,))
        return _code_form(lid, login["email"], "That code isn't right. Please check the email and try again.")

    # signed in: issue the OAuth authorization code and send the browser back to Claude
    params = AuthorizationParams.model_validate(login["params"])
    auth_code = secrets.token_urlsafe(32)
    data = AuthorizationCode(code=auth_code, scopes=params.scopes or [SCOPE],
                             expires_at=time.time() + AUTH_CODE_TTL, client_id=login["client_id"],
                             code_challenge=params.code_challenge, redirect_uri=params.redirect_uri,
                             redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
                             resource=params.resource, subject=login["email"])
    await _run("INSERT INTO mcp_oauth_codes (code_hash, client_id, email, data, expires_at) "
               "VALUES (%s, %s, %s, %s, now() + make_interval(secs => %s))",
               (_hash(auth_code), login["client_id"], login["email"], data.model_dump_json(), AUTH_CODE_TTL))
    await _run("DELETE FROM mcp_oauth_logins WHERE login_id = %s", (lid,))
    logger.info(f"Sign-in completed for {login['email']}")
    return RedirectResponse(construct_redirect_uri(str(params.redirect_uri), code=auth_code, state=params.state),
                            status_code=302)


login_routes = [
    Route("/login", endpoint=login_get, methods=["GET"]),
    Route("/login", endpoint=login_post, methods=["POST"]),
    Route("/login/code", endpoint=login_code, methods=["POST"]),
]

ISSUER_URL = AnyHttpUrl(PUBLIC_URL)
RESOURCE_URL = AnyHttpUrl(f"{PUBLIC_URL}/mcp")
