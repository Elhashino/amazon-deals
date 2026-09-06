"""Shared HTTP helper: one session, timeouts, retries, polite backoff.

Every external call in the scanner goes through get_json/post_json so a
single flaky response never kills the loop, and 429s slow us down
instead of getting us banned.

Secrets never reach the console: the Telegram bot token lives in the URL
path and Helius keys in the query string, so every error message is
redacted before it is raised.
"""

from __future__ import annotations

import re
import time
from email.utils import parsedate_to_datetime

import requests

_session = requests.Session()
_session.headers.update({"User-Agent": "meme-scanner/2.0 (+personal research tool)"})

DEFAULT_TIMEOUT = 15
RETRIES = 3

_TELEGRAM_TOKEN_RE = re.compile(r"(/bot)\d{5,}:[A-Za-z0-9_-]+", re.IGNORECASE)
_API_KEY_RE = re.compile(r"([?&](?:api[-_]?key)=)[^&\s]+", re.IGNORECASE)


def redact(text: str) -> str:
    """Mask credentials that live inside URLs before anything is printed."""
    text = _TELEGRAM_TOKEN_RE.sub(r"\1<TOKEN>", text)
    return _API_KEY_RE.sub(r"\1<KEY>", text)


class ApiError(Exception):
    """A call failed after retries; callers decide whether it's fatal."""


def _retry_after_seconds(raw: str | None, attempt: int) -> float:
    """Retry-After is delta-seconds OR an HTTP-date (RFC 9110). Neither
    form may ever raise — an unparseable header falls back to backoff."""
    fallback = float(2 ** (attempt + 2))
    if not raw:
        return fallback
    try:
        return float(raw)
    except (TypeError, ValueError):
        pass
    try:
        when = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        return fallback
    if when is None:
        return fallback
    try:
        import datetime as _dt

        now = _dt.datetime.now(when.tzinfo) if when.tzinfo else _dt.datetime.now()
        return max(0.0, (when - now).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return fallback


def _request(method: str, url: str, **kwargs) -> requests.Response | None:
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    last_error: Exception | None = None
    for attempt in range(RETRIES):
        try:
            resp = _session.request(method, url, **kwargs)
            if resp.status_code == 429:
                time.sleep(min(_retry_after_seconds(resp.headers.get("Retry-After"), attempt), 60.0))
                continue
            if resp.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            return resp
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(2 ** attempt)
    if last_error is not None:
        raise ApiError(redact(f"{method} {url} failed after {RETRIES} tries: {last_error}"))
    raise ApiError(redact(f"{method} {url} kept returning 429/5xx after {RETRIES} tries"))


def get_json(url: str, **kwargs):
    """GET a JSON endpoint. Returns parsed JSON, or None for a 404."""
    resp = _request("GET", url, **kwargs)
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        raise ApiError(redact(f"GET {url} -> HTTP {resp.status_code}: {resp.text[:200]}"))
    try:
        return resp.json()
    except ValueError as exc:
        raise ApiError(redact(f"GET {url} -> non-JSON response: {exc}"))


def post_json(url: str, payload: dict, **kwargs):
    """POST JSON, return parsed JSON response body."""
    resp = _request("POST", url, json=payload, **kwargs)
    if resp.status_code >= 400:
        raise ApiError(redact(f"POST {url} -> HTTP {resp.status_code}: {resp.text[:200]}"))
    try:
        return resp.json()
    except ValueError as exc:
        raise ApiError(redact(f"POST {url} -> non-JSON response: {exc}"))
