"""
get_token.py

Centralized Microsoft Graph token helper for this project.
Supports two independent tokens:
  - SOURCE: used for pulling SharePoint pages/content
  - UPLOAD: used for uploading backups to SharePoint (can be a different tenant/app)

Usage:
    from get_token import get_graph_token
    src_token = get_graph_token("source")
    up_token  = get_graph_token("upload")

Env variables (client credentials flow):
  # Defaults for both when specific ones are not provided
  TENANT_ID=...
  CLIENT_ID=...
  CLIENT_SECRET=...
  SCOPE=https://graph.microsoft.com/.default

  # SOURCE overrides (optional)
  SOURCE_TENANT_ID=...
  SOURCE_CLIENT_ID=...
  SOURCE_CLIENT_SECRET=...
  SOURCE_SCOPE=https://graph.microsoft.com/.default

  # UPLOAD overrides (optional)
  UPLOAD_TENANT_ID=...
  UPLOAD_CLIENT_ID=...
  UPLOAD_CLIENT_SECRET=...
  UPLOAD_SCOPE=https://graph.microsoft.com/.default

Notes:
- Tokens are cached in-memory until ~120s before expiration.
- This module uses the OAuth2 client credentials grant (application permissions).
- Ensure the respective Entra apps have appropriate Graph permissions and admin consent.
"""
from __future__ import annotations

import os
import time
from typing import Dict, Tuple

import requests

_TOKEN_CACHE: Dict[str, Dict[str, float | str]] = {}


def _client_credentials_token(tenant_id: str, client_id: str, client_secret: str, scope: str) -> Tuple[str, float]:
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope or "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }
    resp = requests.post(url, data=data, timeout=30)
    try:
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Failed to get token for tenant {tenant_id}: {resp.status_code} {resp.text}") from e
    payload = resp.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(f"Token endpoint did not return access_token: {payload}")
    expires_in = int(payload.get("expires_in", 3600))
    # Refresh 120s before expiry
    expiry_ts = time.time() + max(60, expires_in - 120)
    return token, expiry_ts


def _resolve_creds(kind: str) -> Tuple[str, str, str, str]:
    kind = kind.lower()
    if kind == "source":
        tenant = os.getenv("SOURCE_TENANT_ID") or os.getenv("TENANT_ID")
        client = os.getenv("SOURCE_CLIENT_ID") or os.getenv("CLIENT_ID")
        secret = os.getenv("SOURCE_CLIENT_SECRET") or os.getenv("CLIENT_SECRET")
        scope = os.getenv("SOURCE_SCOPE") or os.getenv("SCOPE", "https://graph.microsoft.com/.default")
    elif kind == "upload":
        tenant = os.getenv("UPLOAD_TENANT_ID") or os.getenv("TENANT_ID")
        client = os.getenv("UPLOAD_CLIENT_ID") or os.getenv("CLIENT_ID")
        secret = os.getenv("UPLOAD_CLIENT_SECRET") or os.getenv("CLIENT_SECRET")
        scope = os.getenv("UPLOAD_SCOPE") or os.getenv("SCOPE", "https://graph.microsoft.com/.default")
    else:
        raise ValueError("kind must be 'source' or 'upload'")

    if not (tenant and client and secret):
        raise RuntimeError(f"Missing credentials for {kind.upper()} token (tenant/client/secret)")
    return tenant, client, secret, scope


def get_graph_token(kind: str) -> str:
    """Return a Graph access token for the requested purpose ('source' or 'upload')."""
    kind = kind.lower().strip()
    if kind not in ("source", "upload"):
        raise ValueError("kind must be 'source' or 'upload'")

    cached = _TOKEN_CACHE.get(kind)
    now = time.time()
    if cached and now < float(cached["expiry"]):
        return str(cached["token"])  # type: ignore

    tenant, client, secret, scope = _resolve_creds(kind)
    token, expiry = _client_credentials_token(tenant, client, secret, scope)
    _TOKEN_CACHE[kind] = {"token": token, "expiry": expiry}
    return token


if __name__ == "__main__":
    # Simple smoke test; prints the first few chars of the tokens if available
    try:
        t1 = get_graph_token("source")
        print("SOURCE token acquired (len):", len(t1))
    except Exception as e:
        print("SOURCE token error:", e)
    try:
        t2 = get_graph_token("upload")
        print("UPLOAD token acquired (len):", len(t2))
    except Exception as e:
        print("UPLOAD token error:", e)
