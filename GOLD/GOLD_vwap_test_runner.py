"""
GOLD VWAP 6:00–12:00 IST Strategy — Dry-Run Simulator
======================================================
Replays past trading days through the exact same logic as gold_vwap_runner.py.
No real orders placed. Uses real historical 5-min candle data from Capital.com.

Logic replayed:
  1. Compute rolling VWAP from 06:00 IST on 5-min candles
  2. Detect VWAP crossover → Mother candle
  3. Next candle that does NOT touch VWAP → Entry candle
  4. If entry candle touches VWAP → setup invalidated, scan for new mother candle
  5. Enter on break of entry candle H (BUY) or L (SELL)
  6. SL  = mother candle opposite extreme | TP = entry ± 3× sl_dist (1:3 R:R)
  7. ONE trade per day — done after TP or SL hit (no T2 reversal)
  8. Entry must occur before 12:00 IST
  9. Any open trade at 12:00 IST → closed at market

Usage:
    python3 gold_vwap_test_runner.py --date 2026-04-10
    python3 gold_vwap_test_runner.py --date 2026-04-09 --date 2026-04-08
    python3 gold_vwap_test_runner.py --month 2026-03
    python3 gold_vwap_test_runner.py --month 2026-03 --month 2026-04
    python3 gold_vwap_test_runner.py --year 2026
"""

import argparse
import time
import calendar
from datetime import datetime, timezone, timedelta, date
from typing import Optional
from dotenv import load_dotenv
import os
from capitalcom_api import CapitalComAPI

load_dotenv()

# ── Config (must match gold_vwap_runner.py) ───────────────────────────────────
EPIC          = "GOLD"
RISK_PCT      = 0.01
RR_RATIO      = 3.0
STOP_BUFFER   = 0.10        # $ buffer above entry candle H/L for trigger
PIP_VALUE_USD = 1.0
USD_TO_AED    = 3.67
IST           = timezone(timedelta(hours=5, minutes=30))

# 06:00 IST = 00:30 UTC | 12:00 IST = 06:30 UTC
WINDOW_START_UTC = (0, 30)
WINDOW_END_UTC   = (6, 30)

CAPITAL_API_KEY    = os.getenv("CAPITAL_API_KEY",    "")
CAPITAL_IDENTIFIER = os.getenv("CAPITAL_IDENTIFIER", "")
CAPITAL_PASSWORD   = os.getenv("CAPITAL_PASSWORD",   "")
DEMO_MODE          = os.getenv("CAPITAL_DEMO", "true").lower() == "true"


# ── Helpers ───────────────────────────────────────────────────────────────────
def compute_size(balance: float, sl_distance: float) -> float:
    risk_aed = balance * RISK_PCT
    size     = risk_aed / (sl_distance * PIP_VALUE_USD * USD_TO_AED)
    return round(max(0.01, round(size, 2)), 2)

def compute_vwap(candles: list) -> Optional[float]:
    """VWAP = sum(typical_price × volume) / sum(volume)"""
    if not candles:
        return None
    total_tp_vol = 0.0
    total_vol    = 0.0
    for c in candles:
        tp  = (c["h"] + c["l"] + c["c"]) / 3.0
        vol = c.get("v", 1.0) or 1.0
        total_tp_vol += tp * vol
        total_vol    += vol
    return round(total_tp_vol / total_vol, 3) if total_vol else None

def candle_side(close: float, vwap: float) -> str:
    return "above" if close > vwap else "below"

def candle_touches_vwap(h: float, l: float, vwap: float) -> bool:
    return l <= vwap <= h

def bar_utc_hm(bar: dict) -> tuple:
    """Return (hour, minute) UTC from a bar's snapshotTimeUTC string."""
    ts = bar.get("snapshotTimeUTC", bar.get("snapshotTime", ""))
    try:
        dt = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
        return (dt.hour, dt.minute)
    except Exception:
        return (0, 0)

def bar_in_window(bar: dict) -> bool:
    """True if this bar opens within the 06:00–12:00 IST (00:30–06:30 UTC) window."""
    h, m = bar_utc_hm(bar)
    hm = (h, m)
    return WINDOW_START_UTC <= hm < WINDOW_END_UTC

def is_weekend(sim_date: date) -> bool:
    return sim_date.weekday() >= 5


# ── Fetch all 5-min bars for the day ─────────────────────────────────────────
def fetch_day_bars(api: CapitalComAPI, sim_date: date) -> list:
    """
    Fetch 5-min bars from 00:30 UTC (06:00 IST) to 06:30 UTC (12:00 IST).
    Returns list of raw bar dicts with highPrice, lowPrice, closePrice, volume.
    """
    from_ts  = datetime(sim_date.year, sim_date.month, sim_date.day,
                        0, 30, 0, tzinfo=timezone.utc)
    to_ts    = datetime(sim_date.year, sim_date.month, sim_date.day,
                        23, 55, 0, tzinfo=timezone.utc)
    from_str = from_ts.strftime("%Y-%m-%dT%H:%M:%S")
    to_str   = to_ts.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        api._ensure_session()
        resp = api._session.get(
            f"{api.base_url}/api/v1/prices/{EPIC}",
            params={
                "resolution": "MINUTE_5",
                "from":       from_str,
                "to":         to_str,
                "max":        500,
            },
            timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("prices", [])
    except Exception as e:
        print(f"  [ERROR] Candle fetch failed: {e}")
        return []


# ── Single-day simulation ─────────────────────────────────────────────────────
def simulate_day(api: CapitalComAPI, sim_date: date, balance: float) -> float:
    date_str = sim_date.strftime("%Y-%m-%d")
    weekday  = sim_date.strftime("%A")
    print(f"\n{'='*62}")
    print(f"  SIMULATION — {date_str} ({weekday})  |  Balance: AED {balance:,.2f}")
    print(f"{'='*62}")

    if is_weekend(sim_date):
        print("  [SKIP] Weekend — market closed")
        return balance

    bars = fetch_day_bars(api, sim_date)
    if not bars:
        print("  [SKIP] No candle data — market holiday or data unavailable")
        return balance

    print(f"  [BARS] {len(bars)} x 5-min bars loaded (06:00 IST onward)")

    # ── Replay state ──────────────────────────────────────────────────────────
    accumulated_candles: list  = []   # candles since 06:00 IST for VWAP
    mother_candle:       Optional[dict] = None
    entry_candle:        Optional[dict] = None
    setup_direction:     Optional[str]  = None
    prev_side:           Optional[str]  = None
    prev_bar:            Optional[dict] = None   # for rare-case SL

    traded_today  = False
    done_for_day  = False

    active_entry  = None
    active_sl     = None
    active_tp     = None
    active_dir    = None
    active_size   = None

    trade_log: list = []
    setup_log: list = []

    for bar in bars:
        if done_for_day:
            break

        ts       = bar.get("snapshotTimeUTC", bar.get("snapshotTime", "?"))
        bar_h    = bar["highPrice"]["bid"]
        bar_l    = bar["lowPrice"]["bid"]
        bar_c    = bar["closePrice"]["bid"]
        bar_o    = bar["openPrice"]["bid"]
        bar_v    = bar.get("lastTradedVolume", 1.0) or 1.0

        # Accumulate candle into VWAP pool
        accumulated_candles.append({"h": bar_h, "l": bar_l, "c": bar_c, "v": bar_v})
        vwap = compute_vwap(accumulated_candles)
        if vwap is None:
            continue

        curr_side = candle_side(bar_c, vwap)

        # ── Monitor active trade ───────────────────────────────────────────
        if active_entry is not None:
            sl_hit = (active_dir == "BUY"  and bar_l <= active_sl) or \
                     (active_dir == "SELL" and bar_h >= active_sl)
            tp_hit = (active_dir == "BUY"  and bar_h >= active_tp) or \
                     (active_dir == "SELL" and bar_l <= active_tp)

            # Both hit same bar — use open to determine order
            if sl_hit and tp_hit:
                if active_dir == "BUY":
                    sl_hit = bar_o <= active_entry
                    tp_hit = not sl_hit
                else:
                    sl_hit = bar_o >= active_entry
                    tp_hit = not sl_hit

            if tp_hit:
                pnl     = abs(active_tp - active_entry) * active_size * PIP_VALUE_USD * USD_TO_AED
                pnl_pct = (pnl / balance) * 100
                balance += pnl
                trade_log.append(
                    f"    ✅ TP HIT  @ {active_tp:.2f}  |  +AED {pnl:,.2f} (+{pnl_pct:.2f}%)  "
                    f"|  {ts}")
                active_entry = None
                done_for_day = True
                continue

            if sl_hit:
                pnl     = -abs(active_sl - active_entry) * active_size * PIP_VALUE_USD * USD_TO_AED
                pnl_pct = (pnl / balance) * 100
                balance += pnl
                trade_log.append(
                    f"    ❌ SL HIT  @ {active_sl:.2f}  |  -AED {abs(pnl):,.2f} ({pnl_pct:.2f}%)  "
                    f"|  {ts}")
                active_entry = None
                done_for_day = True
                continue

        if done_for_day or traded_today or not bar_in_window(bar):
            prev_side = curr_side
            prev_bar  = {"h": bar_h, "l": bar_l, "c": bar_c, "ts": ts}
            continue

        # ── VWAP crossover detection → Mother candle ───────────────────────
        if mother_candle is None:
            if prev_side is not None and curr_side != prev_side:
                # Rare-case SL (PDF p.9): SL = more extreme of prev_bar & curr_bar.
                # For BUY: SL = lower low; For SELL: SL = higher high.
                if curr_side == "above":   # BUY crossover
                    sl_ref = min(prev_bar["l"] if prev_bar else bar_l, bar_l)
                else:                       # SELL crossover
                    sl_ref = max(prev_bar["h"] if prev_bar else bar_h, bar_h)
                mother_candle    = {"h": bar_h, "l": bar_l, "c": bar_c,
                                    "side": curr_side, "ts": ts, "sl_ref": sl_ref}
                setup_direction  = "BUY" if curr_side == "above" else "SELL"
                entry_candle     = None
                setup_log.append(
                    f"  [MOTHER] VWAP cross {prev_side}→{curr_side} | "
                    f"H={bar_h:.2f} L={bar_l:.2f} VWAP={vwap:.2f} SL_ref={sl_ref:.2f} | {ts}")

        # ── Trigger candle detection ───────────────────────────────────────
        elif entry_candle is None:
            # Trigger candle must be ENTIRELY on the correct side — no VWAP touch
            trigger_valid = (
                bar_l > vwap if setup_direction == "BUY" else bar_h < vwap
            )
            if not trigger_valid:
                # Trigger candle touched or crossed VWAP — invalidate setup
                setup_log.append(
                    f"  [RESET]  Trigger candle touched VWAP={vwap:.2f} — setup INVALIDATED | {ts}")
                mother_candle   = None
                entry_candle    = None
                setup_direction = None
            else:
                entry_candle = {"h": bar_h, "l": bar_l, "c": bar_c, "ts": ts}
                setup_log.append(
                    f"  [TRIGGER] Confirmed — H={bar_h:.2f} L={bar_l:.2f} | "
                    f"Dir={setup_direction} | {ts}")

        # ── Post-trigger VWAP touch → invalidate (PDF p.10 Wrong Entry Rule) ──
        elif not traded_today:
            if candle_touches_vwap(bar_h, bar_l, vwap):
                setup_log.append(
                    f"  [RESET]  Post-trigger VWAP touch — setup INVALIDATED | {ts}")
                mother_candle   = None
                entry_candle    = None
                setup_direction = None

        # ── Entry trigger — one trade per day ─────────────────────────────
        if (mother_candle and entry_candle and
                not traded_today and active_entry is None):
            mc  = mother_candle
            ec  = entry_candle

            buy_triggered  = bar_h > ec["h"] + STOP_BUFFER and setup_direction == "BUY"
            sell_triggered = bar_l < ec["l"] - STOP_BUFFER and setup_direction == "SELL"

            if buy_triggered:
                entry_px = round(ec["h"] + STOP_BUFFER, 2)
                sl       = round(mc["sl_ref"] - STOP_BUFFER, 2)
                sl_dist  = round(entry_px - sl, 2)
                size     = compute_size(balance, sl_dist)
                tp       = round(entry_px + RR_RATIO * sl_dist, 2)
                active_entry  = entry_px
                active_sl     = sl
                active_tp     = tp
                active_dir    = "BUY"
                active_size   = size
                traded_today  = True
                trade_log.append(
                    f"  [BUY]  Entry={entry_px:.2f}  SL={sl:.2f}  TP={tp:.2f}  "
                    f"Size={size}  SL-dist={sl_dist:.2f}pts | {ts}")

            elif sell_triggered:
                entry_px = round(ec["l"] - STOP_BUFFER, 2)
                sl       = round(mc["sl_ref"] + STOP_BUFFER, 2)
                sl_dist  = round(sl - entry_px, 2)
                size     = compute_size(balance, sl_dist)
                tp       = round(entry_px - RR_RATIO * sl_dist, 2)
                active_entry  = entry_px
                active_sl     = sl
                active_tp     = tp
                active_dir    = "SELL"
                active_size   = size
                traded_today  = True
                trade_log.append(
                    f"  [SELL] Entry={entry_px:.2f}  SL={sl:.2f}  TP={tp:.2f}  "
                    f"Size={size}  SL-dist={sl_dist:.2f}pts | {ts}")

        prev_side = curr_side
        prev_bar  = {"h": bar_h, "l": bar_l, "c": bar_c, "ts": ts}

    # ── EOD close — close any open position at last fetched bar price ──────
    if active_entry is not None:
        last_bar    = bars[-1]
        last_price  = last_bar["closePrice"]["bid"]
        last_ts     = last_bar.get("snapshotTimeUTC", "EOD")
        if active_dir == "BUY":
            pnl = (last_price - active_entry) * active_size * PIP_VALUE_USD * USD_TO_AED
        else:
            pnl = (active_entry - last_price) * active_size * PIP_VALUE_USD * USD_TO_AED
        pnl = round(pnl, 2)
        balance += pnl
        trade_log.append(
            f"    ⏹ EOD CLOSE @ {last_price:.2f}  |  AED {pnl:+,.2f}  |  {last_ts}")

    # ── Print setup log ────────────────────────────────────────────────────
    for line in setup_log:
        print(line)

    # ── Print trade log ────────────────────────────────────────────────────
    if trade_log:
        for line in trade_log:
            print(line)
    else:
        print("  [NO TRADE] No valid VWAP setup formed within 06:00–12:00 IST")

    print(f"\n  Balance after: AED {balance:,.2f}")
    return balance


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="GOLD VWAP 6:00–12:00 IST Strategy Dry-Run Simulator"
    )
    parser.add_argument("--date",  action="append", default=[],
                        help="Specific date (YYYY-MM-DD). Repeatable.")
    parser.add_argument("--month", action="append", default=[],
                        help="Full month (YYYY-MM). Runs all weekdays. Repeatable.")
    parser.add_argument("--year",  action="append", default=[],
                        help="Full year (YYYY). Runs all weekdays. Repeatable.")
    args = parser.parse_args()

    if not args.date and not args.month and not args.year:
        parser.error("Provide at least one --date, --month, or --year")

    # ── Build sorted date list ─────────────────────────────────────────────
    today = date.today()
    dates = []

    for d in args.date:
        dates.append(datetime.strptime(d, "%Y-%m-%d").date())

    for m in args.month:
        year, month = int(m.split("-")[0]), int(m.split("-")[1])
        _, last_day = calendar.monthrange(year, month)
        for day in range(1, last_day + 1):
            d = date(year, month, day)
            if d.weekday() < 5 and d <= today:
                dates.append(d)

    for y in args.year:
        for month in range(1, 13):
            _, last_day = calendar.monthrange(int(y), month)
            for day in range(1, last_day + 1):
                d = date(int(y), month, day)
                if d.weekday() < 5 and d <= today:
                    dates.append(d)

    dates = sorted(set(dates))

    # ── Connect ────────────────────────────────────────────────────────────
    api = CapitalComAPI(
        api_key=CAPITAL_API_KEY,
        identifier=CAPITAL_IDENTIFIER,
        password=CAPITAL_PASSWORD,
        demo=DEMO_MODE
    )
    api.create_session()
    acc           = api.get_account_info()
    start_balance = acc.get("balance", {}).get("balance", 0)
    balance       = start_balance

    print(f"\nGOLD VWAP 6:00–12:00 IST — Dry-Run Simulator")
    print(f"Starting balance : AED {start_balance:,.2f}")
    print(f"Simulating       : {len(dates)} trading day(s)")
    print(f"Risk per trade   : {RISK_PCT*100:.0f}%  |  R:R = 1:{RR_RATIO:.0f}")

    # ── Per-month tracking ────────────────────────────────────────────────
    monthly_start: dict = {}
    monthly_end:   dict = {}
    daily_results: list = []

    for sim_date in dates:
        month_key = sim_date.strftime("%Y-%m")
        if month_key not in monthly_start:
            monthly_start[month_key] = balance
        bal_before = balance
        balance    = simulate_day(api, sim_date, balance)
        monthly_end[month_key] = balance
        daily_results.append((sim_date, bal_before, balance))
        time.sleep(1)   # avoid hammering API

    # ── Monthly breakdown ─────────────────────────────────────────────────
    if len(monthly_start) > 1:
        print(f"\n{'='*62}")
        print(f"  MONTHLY BREAKDOWN")
        print(f"{'='*62}")
        for month_key in sorted(monthly_start):
            m_start = monthly_start[month_key]
            m_end   = monthly_end[month_key]
            m_pnl   = m_end - m_start
            m_pct   = (m_pnl / m_start) * 100
            marker  = "✅" if m_pnl >= 0 else "❌"
            print(f"  {marker}  {month_key}  |  AED {m_pnl:+10,.2f}  ({m_pct:+6.2f}%)")

    # ── Final summary ─────────────────────────────────────────────────────
    pnl     = balance - start_balance
    pnl_pct = (pnl / start_balance) * 100

    wins  = sum(1 for _, b, a in daily_results if a > b)
    loss  = sum(1 for _, b, a in daily_results if a < b)
    flat  = sum(1 for _, b, a in daily_results if a == b)
    total = len(daily_results)

    print(f"\n{'='*62}")
    print(f"  STARTING BALANCE  : AED {start_balance:>12,.2f}")
    print(f"  FINAL BALANCE     : AED {balance:>12,.2f}")
    print(f"  NET P&L           : AED {pnl:>+12,.2f}  ({pnl_pct:+.2f}%)")
    print(f"  DAYS SIMULATED    : {total}  "
          f"(W={wins} / L={loss} / Flat={flat})")
    print(f"{'='*62}\n")


if __name__ == "__main__":
    main()
