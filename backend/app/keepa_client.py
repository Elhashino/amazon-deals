from __future__ import annotations

from typing import Any, Iterable
import time

import keepa
import requests

from .config import settings


class KeepaClient:
    # Keepa expects "GB" for Amazon UK
    UK_DOMAIN = "GB"
    UK_DOMAIN_ID = 2  # Amazon UK in Keepa API

    def __init__(self) -> None:
        # Default in keepa lib is 10s; bump it to avoid ReadTimeouts.
        # You can increase to 90/120 if your connection is slow.
        self.api = keepa.Keepa(settings.KEEPA_API_KEY, timeout=60, logging_level="WARNING")

    def _call(self, fn_name: str, *args, **kwargs):
        fn = getattr(self.api, fn_name)
        return fn(*args, domain=self.UK_DOMAIN, **kwargs)

    def _call_with_retries(self, fn_name: str, *args, **kwargs):
        # Basic exponential backoff on transient ReadTimeouts.
        delays = [2, 5, 12]
        last_err: Exception | None = None
        for i, d in enumerate(delays, start=1):
            try:
                return self._call(fn_name, *args, **kwargs)
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
                last_err = e
                time.sleep(d)
        # final attempt (lets you see the real error if it persists)
        if last_err:
            raise last_err
        return self._call(fn_name, *args, **kwargs)

    def uk_root_categories(self) -> dict[str, Any]:
        return self._call_with_retries("category_lookup", 0)

    def deals(self, include_categories: Iterable[int] | int, page: int = 0) -> dict[str, Any]:
        if isinstance(include_categories, int):
            include_categories = [include_categories]
        else:
            include_categories = list(include_categories)

        deal_parms = {
            "page": int(page),
            "domainId": int(self.UK_DOMAIN_ID),
            "includeCategories": include_categories,
            "isFilterEnabled": True,
            "filterErotic": True,
        }
        return self._call_with_retries("deals", deal_parms)

    def products(self, asins: list[str]) -> list[dict[str, Any]]:
        # Chunking reduces payload size and lowers the chance of timeouts.
        chunk_size = 25
        out: list[dict[str, Any]] = []
        for i in range(0, len(asins), chunk_size):
            chunk = asins[i : i + chunk_size]
            resp = self._call_with_retries(
                "query",
                chunk,
                stats=90,
                history=True,
                rating=True,
                buybox=False,
                offers=None,
                progress_bar=False,
                # keep data lighter than 180d while still enough for 90d stats
                days=120,
                # wait for tokens if needed instead of failing
                wait=True,
            )
            # keepa returns a list of product dicts
            out.extend(resp or [])
        return out
