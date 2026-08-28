# Solana Meme-Coin Launch Scanner

Watches new Solana token launches 24/7, auto-rejects the ones structured to
take your money, scores the survivors on momentum, and pushes Telegram
alerts to your phone.

**What this is:** a kill-filter. Roughly 98% of new launches are designed
as exit liquidity — unlocked LP, live mint authority, one dev bag split
across dozens of "different" wallets. This bins them automatically and
logs *why*, so you see the handful worth even looking at, minutes after
launch instead of hours.

**What this is not:** a crystal ball. A coin passing every filter can
still go to zero — most do. Never put in money you aren't fully prepared
to lose.

## The pipeline

Each new token goes through four stages, cheapest first. Failing any hard
check bins it permanently (logged in `data/rejections.csv`). Missing data
defers it to the next poll — a 30-second-old coin isn't binned just
because RugCheck hasn't indexed it yet.

1. **Market** (DexScreener): liquidity ≥ $20k, pair age 20 min–24 h.
2. **Safety** (RugCheck): mint & freeze authority renounced, LP ≥ 80%
   locked/burned, top-10 holders ≤ 30% (AMM vaults excluded), no single
   wallet > 15%, insiders ≤ 25%, ≥ 100 holders, risk score sane, not
   already flagged rugged.
3. **Bundles** (Solana RPC): top holders funded from a common parent
   wallet or buying in the same block = one entity pretending to be many.
   More than 20% of supply in bundled wallets → binned.
4. **Demand**: ≥ 50 unique buying wallets in the last hour — bots fake
   volume cheaply, hundreds of distinct funded wallets cost real money.

Survivors get a 0–100 momentum score (buy pressure, volume acceleration,
unique demand, holder growth, liquidity health, identity). Score ≥ 60 →
Telegram alert. Alerted coins are re-checked for 12 h; if liquidity drops
60%+ you get one **RUG WARNING** message.

## Setup (Windows, ~10 minutes)

1. Install [Python 3.11+](https://www.python.org/downloads/) — tick
   **"Add python.exe to PATH"** in the installer.
2. Put this folder somewhere permanent, e.g. `C:\jarvis\meme-scanner`.
3. In that folder, open a terminal and run:
   ```
   pip install -r requirements.txt
   python -m pytest tests -q        # optional: all tests should pass
   python scanner.py --once         # one shakedown cycle, console output
   ```
   With no Telegram configured, alerts print to the console — the scanner
   is fully usable before step 4.

### 4. Telegram alerts (free, ~2 minutes)

1. In Telegram, message **@BotFather** → `/newbot` → pick a name → copy
   the **bot token** (looks like `123456789:AAF...`).
2. Send your new bot any message (it can't message you first).
3. Open `https://api.telegram.org/bot<TOKEN>/getUpdates` in a browser and
   copy `"chat":{"id":...}` — that number is your **chat id**.
4. Copy `.env.example` to `.env` and fill in `TELEGRAM_BOT_TOKEN` and
   `TELEGRAM_CHAT_ID`.

### 5. Run it for real

```
python scanner.py
```
Or double-click `run_scanner.bat`. To make it a permanent Jarvis resident,
add that .bat to Task Scheduler (Run at startup, restart on failure).

### Optional: free Helius key

Bundle detection uses the free public Solana RPC, which is heavily
rate-limited — when it's saturated the scanner marks bundle data
"unknown" rather than blocking. A free key from
[dev.helius.xyz](https://dev.helius.xyz) in `.env` (`HELIUS_API_KEY=`)
makes bundle checks fast and reliable.

## Tuning

Every threshold is a `.env` variable — see `.env.example`. Strictness is
free; loosening filters is how you become exit liquidity. If alerts feel
too rare, lower `MIN_ALERT_SCORE` before touching the safety filters.

## Watch the rejection log

`data/rejections.csv` records every binned coin, the stage, and the exact
reason. Ten minutes of reading it teaches you more about how launches are
engineered than any caller channel — and every "caller" pick you see on
TikTok, you can look up here and check which filter it failed.

## Honest limitations

- **Discovery**: DexScreener has no public "newest pairs" feed, so
  discovery polls its paid-profile/boost feeds plus RugCheck's
  new-token stats. Teams that never buy any profile/boost and never get
  indexed appear late or not at all — those are also disproportionately
  the scams.
- **Speed**: insiders and sniper bots are in at block one. This tool gets
  you in the first minutes with evidence, not the first seconds blind.
- **False negatives survive**: passing every check means "not obviously
  rigged", not "safe". Position sizing is your only real protection.
