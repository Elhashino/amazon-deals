"""GOLD 15:05 ALERT — BACKUP sender (rides the amazon-deals repo, whose
GitHub Actions schedules have fired reliably for months).

Runs ~35 min after the primary (Elhashino/gold-alert). Before doing anything
it polls the ntfy channel: if ANY gold message was already posted in the last
6 hours, the primary delivered and this exits silently. Otherwise it computes
the same frozen verdict and sends it, titled "(backup)". A duplicate ping is
acceptable; silence never is.
"""
from __future__ import annotations

import os
import sys
import time as _time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import dukascopy_python as dk
import dukascopy_python.instruments as ins

TOPIC = "sunblessed-gold-mm-7x3q"
NY = ZoneInfo("America/New_York")
RETRIES, WAIT_S = 5, 240


def already_delivered() -> bool:
    try:
        r = requests.get(f"https://ntfy.sh/{TOPIC}/json?poll=1&since=6h", timeout=20)
        return any(line.strip() and '"message"' in line for line in r.text.splitlines())
    except Exception as e:
        print("dedupe poll failed, will send anyway:", e)
        return False                      # duplicate beats silence


def push(title: str, msg: str):
    try:
        requests.post(f"https://ntfy.sh/{TOPIC}", data=msg.encode(),
                      headers={"Title": title}, timeout=15)
        print(f"pushed | {title} | {msg}")
    except Exception as e:
        print("push failed:", e)


def verdict():
    end = datetime.now(timezone.utc).replace(tzinfo=None)
    df = dk.fetch(ins.INSTRUMENT_FX_METALS_XAU_USD, dk.INTERVAL_MIN_5,
                  dk.OFFER_SIDE_BID, start=end - timedelta(days=45), end=end)
    df.index = (df.index.tz_localize("UTC") if df.index.tz is None else df.index
                ).tz_convert("America/New_York")
    df = df.rename(columns=str.lower).dropna(subset=["open", "high", "low", "close"])
    today = df.index[-1].date()

    mins = df.index.hour * 60 + df.index.minute
    rth = df[(mins >= 570) & (mins <= 955)]
    rngs = []
    for date, day in rth.groupby(rth.index.date):
        if date < today and len(day) >= 60:
            rngs.append(float(day["high"].max() - day["low"].min()))
    if len(rngs) < 12:
        return "ERROR", "not enough history in fetch"
    rngs = rngs[-20:]
    prior_range, med20 = rngs[-1], float(np.median(rngs))

    daily = df["close"].resample("1D").last().dropna()
    up = daily.shift(1) > daily.shift(6)
    tmap = {d.date(): bool(v) for d, v in up.items() if not pd.isna(v)}
    trend = tmap.get(today)

    morn = df[(df.index.date == today) & (mins >= 570) & (mins <= 600)]
    if len(morn) < 7:
        return "WAIT", ""

    if prior_range <= med20:
        return "NO TRADE", f"vol gate shut ({prior_range:.1f} <= {med20:.1f})"
    if trend is None:
        return "NO TRADE", "trend unknown"
    o930 = float(morn["open"].iloc[0]); c1000 = float(morn["close"].iloc[-1])
    if c1000 == o930 or (c1000 > o930) != trend:
        return "NO TRADE", "morning disagreed with the trend"
    side = "LONG" if trend else "SHORT"
    stop = float(morn["low"].min()) if trend else float(morn["high"].max())
    return "SIGNAL", (f"{side} @ {c1000:.2f}, stop {stop:.2f}, "
                      f"flat at 15:55 NY - paper it now")


def main():
    forced = os.environ.get("FORCE") == "1"
    now_ny = datetime.now(NY)
    if not forced and now_ny.hour not in (10, 11):
        print(f"time gate: {now_ny:%H:%M} NY outside backup window")
        sys.exit(0)
    if not forced and now_ny.weekday() >= 5:
        print("weekend - nothing to do")
        sys.exit(0)
    if not forced and already_delivered():
        print("primary already delivered today - backup stands down")
        sys.exit(0)

    for attempt in range(RETRIES):
        try:
            status, msg = verdict()
        except Exception as e:
            push("Gold alert PROBLEM (backup)", f"script error: {str(e)[:180]}")
            raise
        if status == "WAIT":
            print(f"feed lag, retry {attempt + 1}/{RETRIES} in {WAIT_S}s")
            _time.sleep(WAIT_S)
            continue
        if status == "SIGNAL":
            push("GOLD SIGNAL 15:05 (backup)", msg)
        elif status == "NO TRADE":
            push("Gold: no trade today (backup)", msg + " - correct silence")
        else:
            push("Gold alert PROBLEM (backup)", msg)
        return
    push("Gold alert PROBLEM (backup)",
         "feed never delivered the 10:00 candle - check chart")


if __name__ == "__main__":
    main()
