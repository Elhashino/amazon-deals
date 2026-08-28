"""Solana meme-coin launch scanner with a kill-filter.

Watches new Solana pairs, auto-rejects the launches that are structured to
take your money (unlocked LP, live mint/freeze authority, concentrated or
bundled holders), scores the survivors on momentum, and pushes Telegram
alerts. It filters — it does not predict winners.
"""

__version__ = "2.0.0"
