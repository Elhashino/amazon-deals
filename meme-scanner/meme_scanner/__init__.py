"""Solana meme-coin launch scanner with a kill-filter.

Watches new Solana pairs, auto-rejects the launches that are structured to
take your money (unlocked LP, live mint/freeze authority, concentrated or
bundled holders), scores the survivors on momentum, and pushes Telegram
alerts. It filters — it does not predict winners.
"""

__version__ = "2.0.0"


def _force_utf8_console() -> None:
    """Make the alert/console output survive a legacy Windows code page.

    The Windows console defaults to cp1252, which cannot encode the emoji and
    box-drawing characters in the alert text — printing one raises
    UnicodeEncodeError mid-pipeline and takes the alert down with it. Reconfigure
    the streams to UTF-8, and fall back to replacing unencodable characters if
    even that is refused, so output is never fatal.
    """
    import sys

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue  # redirected to something that isn't a TextIOWrapper
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (OSError, ValueError):
            pass


_force_utf8_console()
