"""Discord alerts via an incoming webhook.

Alerts are composed once, as Telegram-flavoured HTML. Discord speaks
Markdown and renders nothing of that, so the same message is translated
here rather than being written twice — one alert body means the phone and
the channel can never drift apart as the format changes.

A webhook URL is a credential: anyone holding it can post to the channel as
this bot. It lives in .env with the Telegram token, and is redacted from
error output the same way, so a failing send cannot paste it into a log.
"""

from __future__ import annotations

import re

from .apis.http import ApiError, post_json

MAX_LEN = 2000  # Discord message hard limit — a quarter of Telegram's


def html_to_markdown(html_text: str) -> str:
    """Translate the alert's HTML into what Discord actually renders.

    Anchors become Markdown links, <b> becomes bold, <code> becomes an
    inline code span, and the entities escaped on the way into HTML are
    unescaped so a token called "Bob's" does not reach the channel as
    "Bob&#39;s".
    """
    text = re.sub(r'<a href="([^"]*)">(.*?)</a>', r"[\2](\1)", html_text, flags=re.S)
    text = re.sub(r"</?b>", "**", text)
    text = re.sub(r"</?code>", "`", text)
    text = re.sub(r"<[^>]+>", "", text)  # anything else: drop the tag, keep the text
    return (
        text.replace("&quot;", '"')
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")  # last: an escaped &amp;lt; must not become <
    )


def _redact(text: str) -> str:
    """Keep the webhook's own URL out of anything printed about a failure."""
    return re.sub(r"(https://discord(?:app)?\.com/api/webhooks/)\S+", r"\1<REDACTED>", text)


class DiscordAlerter:
    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = (webhook_url or "").strip()

    @property
    def enabled(self) -> bool:
        return self.webhook_url.startswith("https://")

    def send(self, html_text: str) -> bool:
        """Post one alert. Returns True on success; never raises."""
        if not self.enabled:
            return False
        body = html_to_markdown(html_text)
        if len(body) > MAX_LEN:
            body = body[: MAX_LEN - 14] + "\n[truncated]"
        # A bare webhook answers 204 with an empty body, which the shared JSON
        # poster treats as a failed call. wait=true makes Discord return the
        # created message instead, so a success parses as one — and the reply
        # is real confirmation the message exists rather than an assumption.
        url = self.webhook_url + ("&" if "?" in self.webhook_url else "?") + "wait=true"
        try:
            post_json(url, {"content": body, "flags": 4})  # 4 = suppress embeds
        except ApiError as exc:
            print(f"  [WARN] Discord send failed: {_redact(str(exc))}")
            return False
        return True
