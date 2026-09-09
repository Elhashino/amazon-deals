"""Telegram alerts. HTML parse mode — only three characters need escaping,
unlike MarkdownV2 where a stray '.' or '-' in a token name breaks the send.

With no bot token configured the alerter degrades to console printing, so
the scanner is fully usable before Telegram is set up.
"""

from __future__ import annotations

from .apis.http import ApiError, post_json

MAX_LEN = 4096  # Telegram message hard limit


def escape_html(text: str) -> str:
    """Escape quotes as well as tags: interpolated values also land inside
    href="..." attributes, where a bare quote would break out of them."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


class Alerter:
    def __init__(self, bot_token: str, chat_id: str, extra_sinks=()) -> None:
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()
        # Other places the same alert goes. Each needs .enabled and .send.
        self.extra_sinks = [s for s in extra_sinks if getattr(s, "enabled", False)]

    @property
    def telegram_enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    @property
    def enabled(self) -> bool:
        return self.telegram_enabled or bool(self.extra_sinks)

    def send(self, html_text: str) -> bool:
        """Send one alert to every configured channel.

        True means it reached at least one of them. A coin is marked seen only
        once an alert is delivered, so demanding that every channel succeed
        would replay the alert to the working ones on each cycle whenever one
        channel happened to be down.
        """
        delivered = False
        for sink in self.extra_sinks:
            try:
                delivered = sink.send(html_text) or delivered
            except Exception as exc:  # one broken sink must not lose the alert
                print(f"  [WARN] {type(sink).__name__} raised: {exc}")
        if len(html_text) > MAX_LEN:
            html_text = html_text[: MAX_LEN - 20] + "\n[truncated]"
        if not self.telegram_enabled:
            if not self.extra_sinks:
                print("\n=== ALERT (no channels configured — console only) ===")
                print(html_text)
                print("======================================================\n")
                return True
            return delivered
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": html_text,
            "parse_mode": "HTML",
            # Bot API 7.0+: disable_web_page_preview is deprecated
            "link_preview_options": {"is_disabled": True},
        }
        # Telegram failing does not undo a sibling channel that already took
        # the alert, so these fall back to what the other sinks managed.
        try:
            resp = post_json(url, payload)
        except ApiError as exc:
            print(f"  [WARN] Telegram send failed: {exc}")
            return delivered
        if not resp.get("ok", False):
            print(f"  [WARN] Telegram rejected message: {resp.get('description', resp)}")
            return delivered
        return True
