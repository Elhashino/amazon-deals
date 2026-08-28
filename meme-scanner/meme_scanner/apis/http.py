"""Shared HTTP helper: one session, timeouts, retries, polite backoff.

Every external call in the scanner goes through get_json/post_json so a
single flaky response never kills the loop, and 429s slow us down
instead of getting us banned.
"""

from __future__ import annotations

import time

import requests

_session = requests.Session()
_session.headers.update({"User-Agent": "meme-scanner/2.0 (+personal research tool)"})

DEFAULT_TIMEOUT = 15
RETRIES = 3


class ApiError(Exception):
    """A call failed after retries; callers decide whether it's fatal."""


def _request(method: str, url: str, **kwargs) -> requests.Response | None:
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    last_error: Exception | None = None
    for attempt in range(RETRIES):
        try:
            resp = _session.request(method, url, **kwargs)
            if resp.status_code == 429:
                # Respect Retry-After when present, otherwise back off hard.
                wait = float(resp.headers.get("Retry-After", 2 ** (attempt + 2)))
                time.sleep(min(wait, 60))
                continue
            if resp.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            return resp
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(2 ** attempt)
    if last_error is not None:
        raise ApiError(f"{method} {url} failed after {RETRIES} tries: {last_error}")
    raise ApiError(f"{method} {url} kept returning 429/5xx after {RETRIES} tries")


def get_json(url: str, **kwargs):
    """GET a JSON endpoint. Returns parsed JSON, or None for a 404."""
    resp = _request("GET", url, **kwargs)
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        raise ApiError(f"GET {url} -> HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def post_json(url: str, payload: dict, **kwargs):
    """POST JSON, return parsed JSON response body."""
    resp = _request("POST", url, json=payload, **kwargs)
    if resp.status_code >= 400:
        raise ApiError(f"POST {url} -> HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.json()
