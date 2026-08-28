"""Telegram alerts. HTML parse mode — only three characters need escaping,
unlike MarkdownV2 where a stray '.' or '-' in a token name breaks the send.

With no bot token configured the alerter degrades to console printing, so
the scanner is fully usable before Telegram is set up.
"""

from __future__ import annotations

from .apis.http import ApiError, post_json

MAX_LEN = 4096  # Telegram message hard limit


def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class Alerter:
    def __init__(self, bot_token: str, chat_id: str) -> None:
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    def send(self, html_text: str) -> bool:
        """Send one alert. Returns True on success; never raises."""
        if len(html_text) > MAX_LEN:
            html_text = html_text[: MAX_LEN - 20] + "\n[truncated]"
        if not self.enabled:
            print("\n=== ALERT (Telegram not configured — console only) ===")
            print(html_text)
            print("======================================================\n")
            return True
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": html_text,
            "parse_mode": "HTML",
            # Bot API 7.0+: disable_web_page_preview is deprecated
            "link_preview_options": {"is_disabled": True},
        }
        try:
            resp = post_json(url, payload)
        except ApiError as exc:
            print(f"  [WARN] Telegram send failed: {exc}")
            return False
        if not resp.get("ok", False):
            print(f"  [WARN] Telegram rejected message: {resp.get('description', resp)}")
            return False
        return True
