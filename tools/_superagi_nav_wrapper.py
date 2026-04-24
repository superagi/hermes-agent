"""SuperAGI-specific browser_navigate URL rewriter.

Activated when the SuperAGI platform adapter is in use (detected via env).
Moves auth assembly (agent_token minting/refresh, param-name normalization,
staging host rewrite) out of LLM-controlled tool args and into deterministic
Python: the agent can pass any *.superagi.com URL and the call will arrive
authenticated.

No-op for any host outside *.superagi.com or when SuperAGI env vars aren't set.
"""
from __future__ import annotations

import base64
import json
import os
import re
import time
import urllib.request


def _exp_ts(tok: str) -> int:
    """Decode a JWT's exp claim without verifying the signature."""
    try:
        parts = tok.split(".")
        if len(parts) != 3:
            return 0
        pad = "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad).decode())
        return int(payload.get("exp", 0))
    except Exception:
        return 0


def _mint() -> str | None:
    base = (os.environ.get("AGENT_AUTH_BASE_URL") or "https://api-staging.superagi.com").rstrip("/")
    key = os.environ.get("API_KEY") or os.environ.get("SUPERAGI_API_KEY") or ""
    if not key:
        return None
    try:
        req = urllib.request.Request(
            base + "/agent-auth/web-token",
            method="POST",
            headers={"X-Api-Key": key},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            tok = json.loads(resp.read().decode()).get("token")
            if tok:
                os.environ["SUPERAGI_AGENT_TOKEN"] = tok
                try:
                    env_path = "/root/.hermes/.env"
                    if os.path.exists(env_path):
                        lines = open(env_path).readlines()
                        replaced = False
                        for i, ln in enumerate(lines):
                            if ln.startswith("SUPERAGI_AGENT_TOKEN="):
                                lines[i] = "SUPERAGI_AGENT_TOKEN=" + tok + "\n"
                                replaced = True
                                break
                        if not replaced:
                            lines.append("SUPERAGI_AGENT_TOKEN=" + tok + "\n")
                        open(env_path, "w").writelines(lines)
                except Exception:
                    pass
            return tok
    except Exception as e:
        print("[superagi-nav] mint error: " + str(e))
        return None


def _fresh_token() -> str:
    tok = os.environ.get("SUPERAGI_AGENT_TOKEN", "")
    if tok and _exp_ts(tok) > time.time() + 60:
        return tok
    return _mint() or tok


def rewrite_url(url: str) -> str:
    """Return a possibly-rewritten URL for super_sales navigations.

    - sales.superagi.com → sales-staging.superagi.com when ENV=staging
    - agent_auth_token / auth_token param → agent_token
    - agent_token appended if missing
    - No-op for non-SuperAGI hosts
    """
    if not url:
        return url
    low = url.lower()
    if ".superagi.com" not in low:
        return url
    env = os.environ.get("ENV") or os.environ.get("SUPERAGI_ENV")
    if env == "staging" and "://sales.superagi.com" in url:
        url = url.replace("://sales.superagi.com", "://sales-staging.superagi.com")
    url = re.sub(r"([?&])agent_auth_token=", r"\1agent_token=", url)
    url = re.sub(r"([?&])auth_token=", r"\1agent_token=", url)
    if "agent_token=" not in url:
        tok = _fresh_token()
        if tok:
            sep = "&" if "?" in url else "?"
            url = url + sep + "agent_token=" + tok
            print("[superagi-nav] auto-appended agent_token to " + url[:90] + "...")
    return url
