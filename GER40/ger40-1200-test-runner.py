"""
GER40 12:00 PM IST Strategy — Dry-Run Simulator
================================================
Replays a past trading day through the exact same logic as ger40-1200-runner.py.
No real orders are placed. Uses real historical candle data from Capital.com API.

Usage:
    python3 ger40-1200-test-runner.py --date 2026-04-10
    python3 ger40-1200-test-runner.py --date 2026-04-09 --date 2026-04-08
    python3 ger40-1200-test-runner.py --month 2026-03
    python3 ger40-1200-test-runner.py --month 2026-03 --month 2026-04
    python3 ger40-1200-test-runner.py --year 2026
"""

import argparse
import time
import sys
import os
import calendar
from datetime import datetime, timezone, timedelta, date
from typing import Optional
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'US500'))
from capitalcom_api import CapitalComAPI

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'), override=True)

# ── Config (must match runner) ────────────────────────────────────────────────
EPIC           = "DE40"
FIXED_SL_AED   = 40.0      # simulation risk per trade in AED (adjust for your backtesting capital)
RR_RATIO       = 3.0
STOP_BUFFER    = 1.0          # points beyond candle H/L for entry trigger
SL_BUFFER      = 0.0
PIP_VALUE_EUR  = 1.0
EUR_TO_AED     = 3.97
START_CAPITAL  = 4000.0    # starting capital in AED for simulation (set None to use live balance)
IST    = timezone(timedelta(hours=5, minutes=30))
BERLIN = ZoneInfo("Europe/Berlin")


def candle_utc_hours(d: date) -> tuple:
    """
    Returns (open_h, open_m, close_h, close_m) in UTC for the strategy candle.

    DAX opens at 09:00 CET/CEST. We use the 30-min candle that closes at that open.

    CEST (summer, UTC+2): DAX open = 07:00 UTC → candle = 06:30–07:00 UTC  (12:00–12:30 PM IST)
    CET  (winter, UTC+1): DAX open = 08:00 UTC → candle = 07:30–08:00 UTC  ( 1:00–1:30  PM IST)
    """
    is_cest = bool(datetime(d.year, d.month, d.day, 12, 0, tzinfo=BERLIN).dst())
    if is_cest:
        return 6, 30, 7, 0
    else:
        return 7, 30, 8, 0


def candle_ist_label(open_h: int, open_m: int) -> str:
    """Convert candle open UTC time to a human-readable IST label."""
    total_m = open_h * 60 + open_m + 5 * 60 + 30   # IST = UTC + 5:30
    h, m    = (total_m // 60) % 24, total_m % 60
    period  = "AM" if h < 12 else "PM"
    return f"{h % 12 or 12}:{m:02d} {period} IST"

CAPITAL_API_KEY    = os.getenv('CAPITAL_API_KEY',    '')
CAPITAL_IDENTIFIER = os.getenv('CAPITAL_IDENTIFIER', '')
CAPITAL_PASSWORD   = os.getenv('CAPITAL_PASSWORD',   '')
DEMO_MODE          = os.getenv('CAPITAL_DEMO', 'true').lower() == 'true'


# ── Helpers ────────────────────────────────────────────────────────────────────
def compute_size(sl_distance: float) -> float:
    size = FIXED_SL_AED / (sl_distance * PIP_VALUE_EUR * EUR_TO_AED)
    return round(max(0.1, round(size, 1)), 1)


def fetch_candle(api, sim_date: date) -> Optional[dict]:
    """Fetch the strategy candle for sim_date, using DST-aware UTC hours."""
    open_h, open_m, close_h, close_m = candle_utc_hours(sim_date)
    from_ts  = datetime(sim_date.year, sim_date.month, sim_date.day,
                        open_h, open_m, 0, tzinfo=timezone.utc)
    to_ts    = datetime(sim_date.year, sim_date.month, sim_date.day,
                        close_h, close_m, 0, tzinfo=timezone.utc) - timedelta(seconds=1)
    from_str = from_ts.strftime("%Y-%m-%dT%H:%M:%S")
    to_str   = to_ts.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        api._ensure_session()
        resp = api._session.get(
            f"{api.base_url}/api/v1/prices/{EPIC}",
            params={"resolution": "MINUTE_30", "from": from_str, "to": to_str},
            timeout=15
        )
        resp.raise_for_status()
        bars = resp.json().get("prices", [])
        if not bars:
            return None
        bar   = bars[0]
        h_bid = bar["highPrice"]["bid"]
        h_ask = bar["highPrice"].get("ask", h_bid)
        l_bid = bar["lowPrice"]["bid"]
        c     = bar["closePrice"]["bid"]
        return {"high": h_bid, "high_ask": h_ask, "low": l_bid,
                "close": c, "range": round(h_bid - l_bid, 2)}
    except Exception:
        return None


def fetch_replay_bars(api, sim_date: date) -> list:
    """
    Fetch 30-min bars from candle close through to next day's candle open - 1s.
    Both boundaries are DST-aware: the candle close for sim_date and the candle
    open for the next calendar day may differ (handles overnight DST transitions).
    """
    _, _, close_h, close_m = candle_utc_hours(sim_date)
    from_ts  = datetime(sim_date.year, sim_date.month, sim_date.day,
                        close_h, close_m, 0, tzinfo=timezone.utc)

    next_d   = sim_date + timedelta(days=1)
    next_open_h, next_open_m, _, _ = candle_utc_hours(next_d)
    to_ts    = datetime(next_d.year, next_d.month, next_d.day,
                        next_open_h, next_open_m, 0, tzinfo=timezone.utc) - timedelta(seconds=1)

    from_str = from_ts.strftime("%Y-%m-%dT%H:%M:%S")
    to_str   = to_ts.strftime("%Y-%m-%dT%H:%M:%S")

    api._ensure_session()
    resp = api._session.get(
        f"{api.base_url}/api/v1/prices/{EPIC}",
        params={"resolution": "MINUTE_30", "from": from_str, "to": to_str, "max": 1000},
        timeout=15
    )
    resp.raise_for_status()
    return resp.json().get("prices", [])


# ── Simulator ──────────────────────────────────────────────────────────────────
def simulate_day(api, sim_date: date, balance: float) -> float:
    date_str                         = sim_date.strftime("%Y-%m-%d")
    open_h, open_m, close_h, close_m = candle_utc_hours(sim_date)
    ist_label                        = candle_ist_label(open_h, open_m)
    is_cest                          = (open_h == 6)

    print(f"\n{'='*60}")
    print(f"  SIMULATION — {date_str}  |  Balance: AED {balance:,.2f}  "
          f"|  {'CEST (summer)' if is_cest else 'CET (winter)'}")
    print(f"{'='*60}")

    candle = fetch_candle(api, sim_date)
    if candle is None:
        print("  [SKIP] No candle data — market holiday or data unavailable")
        return balance

    H     = candle["high"]
    H_ask = candle["high_ask"]
    L     = candle["low"]
    R     = candle["range"]
    print(f"  [CANDLE] {ist_label} | H={H} (ask={H_ask})  L={L}  Range={R} pts")
    print(f"  [LEVELS] BUY trigger > {H_ask + STOP_BUFFER:.2f}  |  SELL trigger < {L - STOP_BUFFER:.2f}")

    try:
        bars = fetch_replay_bars(api, sim_date)
    except Exception as e:
        print(f"  [ERROR] Could not fetch replay bars: {e}")
        return balance

    if not bars:
        print("  [SKIP] No replay bars available")
        return balance

    print(f"  [REPLAY] {len(bars)} 30-min bars loaded from {close_h:02d}:{close_m:02d} UTC")

    # ── Replay state ───────────────────────────────────────────────────────────
    trades_today = 0
    t1_direction = None
    t1_sl_hit    = False
    done_for_day = False
    active_entry = None
    active_sl    = None
    active_tp    = None
    active_size  = None
    active_dir   = None
    trade_log    = []

    for bar in bars:
        if done_for_day:
            break

        ts           = bar.get("snapshotTimeUTC", bar.get("snapshotTime", "?"))
        bar_high_bid = bar["highPrice"]["bid"]
        bar_high_ask = bar["highPrice"].get("ask", bar["highPrice"]["bid"])
        bar_low_bid  = bar["lowPrice"]["bid"]
        bar_open     = bar["openPrice"]["bid"]

        # ── Monitor active trade ───────────────────────────────────────────────
        if active_entry is not None:
            sl_hit = (active_dir == "BUY"  and bar_low_bid  <= active_sl) or \
                     (active_dir == "SELL" and bar_high_ask >= active_sl)
            tp_hit = (active_dir == "BUY"  and bar_high_bid >= active_tp) or \
                     (active_dir == "SELL" and bar_low_bid  <= active_tp)

            # Both hit in same bar — determine order by bar open direction
            if sl_hit and tp_hit:
                if active_dir == "BUY":
                    tp_hit = bar_open >= active_entry   # opened in profit zone → TP first
                    sl_hit = not tp_hit
                else:
                    tp_hit = bar_open <= active_entry   # opened below entry (profit) → TP first
                    sl_hit = not tp_hit

            if tp_hit:
                pnl     = abs(active_tp - active_entry) * active_size * PIP_VALUE_EUR * EUR_TO_AED
                pnl_pct = (pnl / balance) * 100
                balance += pnl
                trade_log.append(
                    f"    ✅ TP HIT @ {active_tp:.2f}  |  +AED {pnl:.2f} (+{pnl_pct:.1f}%)  |  ts={ts}")
                active_entry = None
                done_for_day = True
                continue

            if sl_hit:
                pnl     = -abs(active_sl - active_entry) * active_size * PIP_VALUE_EUR * EUR_TO_AED
                pnl_pct = (pnl / balance) * 100
                balance += pnl
                trade_log.append(
                    f"    ❌ SL HIT @ {active_sl:.2f}  |  -AED {abs(pnl):.2f} ({pnl_pct:.1f}%)  |  ts={ts}")
                active_entry = None

                if trades_today == 1:
                    t1_sl_hit = True
                else:
                    done_for_day = True
                continue

        # ── Entry logic ────────────────────────────────────────────────────────
        if done_for_day or trades_today >= 2:
            continue

        if trades_today == 0:
            buy_triggered  = bar_high_ask > H_ask + STOP_BUFFER
            sell_triggered = bar_low_bid  < L     - STOP_BUFFER

            if buy_triggered and sell_triggered:
                mid            = (H + L) / 2
                sell_triggered = bar_open <= mid
                buy_triggered  = not sell_triggered

            if buy_triggered:
                entry   = round(H_ask + STOP_BUFFER, 2)   # simulate market fill at breakout level
                sl      = round(L   - SL_BUFFER, 2)
                sl_dist = round(entry - sl, 2)
                size    = compute_size(sl_dist)
                tp      = round(entry + RR_RATIO * sl_dist, 2)
                label   = "T1 BUY"
                t1_direction = "BUY"
                active_entry = entry
                active_sl    = sl
                active_tp    = tp
                active_size  = size
                active_dir   = "BUY"
                trades_today = 1
                trade_log.append(
                    f"  [{label}] Entry={entry:.2f}  SL={sl:.2f}  TP={tp:.2f}  "
                    f"Size={size}  SL-dist={sl_dist:.2f}pts  |  ts={ts}")

            elif sell_triggered:
                entry   = round(L - STOP_BUFFER, 2)       # simulate market fill at breakout level
                sl      = round(H_ask + SL_BUFFER, 2)
                sl_dist = round(sl - entry, 2)
                size    = compute_size(sl_dist)
                tp      = round(entry - RR_RATIO * sl_dist, 2)
                label   = "T1 SELL"
                t1_direction = "SELL"
                active_entry = entry
                active_sl    = sl
                active_tp    = tp
                active_size  = size
                active_dir   = "SELL"
                trades_today = 1
                trade_log.append(
                    f"  [{label}] Entry={entry:.2f}  SL={sl:.2f}  TP={tp:.2f}  "
                    f"Size={size}  SL-dist={sl_dist:.2f}pts  |  ts={ts}")

        elif trades_today == 1 and t1_sl_hit:
            if t1_direction == "BUY":
                # T1 BUY SL hit near L → T2 SELL at candle L boundary
                t2_entry  = round(L   - STOP_BUFFER, 2)
                t2_sl     = round(H_ask + SL_BUFFER, 2)
                t2_sldist = round(t2_sl - t2_entry, 2)
                t2_tp     = round(t2_entry - RR_RATIO * t2_sldist, 2)
                active_dir   = "SELL"
                label        = "T2 SELL"
            else:
                # T1 SELL SL hit near H → T2 BUY at candle H boundary
                t2_entry  = round(H_ask + STOP_BUFFER, 2)
                t2_sl     = round(L   - SL_BUFFER, 2)
                t2_sldist = round(t2_entry - t2_sl, 2)
                t2_tp     = round(t2_entry + RR_RATIO * t2_sldist, 2)
                active_dir   = "BUY"
                label        = "T2 BUY"

            active_entry = t2_entry
            active_sl    = t2_sl
            active_tp    = t2_tp
            active_size  = compute_size(t2_sldist)
            trades_today = 2
            t1_sl_hit    = False
            trade_log.append(
                f"  [{label}] Entry={t2_entry:.2f}  SL={t2_sl:.2f}  "
                f"TP={t2_tp:.2f}  Size={active_size}  SL-dist={t2_sldist:.2f}pts  |  ts={ts}")

    # ── End of day — close any open position at last bar close ─────────────────
    if active_entry is not None:
        last_price = bars[-1]["closePrice"]["bid"]
        last_ts    = bars[-1].get("snapshotTimeUTC", "EOD")
        if active_dir == "BUY":
            pnl = (last_price - active_entry) * active_size * PIP_VALUE_EUR * EUR_TO_AED
        else:
            pnl = (active_entry - last_price) * active_size * PIP_VALUE_EUR * EUR_TO_AED
        pnl_aed  = round(pnl, 2)
        balance += pnl_aed
        trade_log.append(
            f"    ⏹ EOD CLOSE @ {last_price:.2f}  |  AED {pnl_aed:+.2f}  |  ts={last_ts}")

    # ── Print summary ──────────────────────────────────────────────────────────
    for line in trade_log:
        print(line)

    if not trade_log:
        print("  [NO TRADE] Price never broke out of candle range")

    print(f"\n  Balance after: AED {balance:,.2f}")
    return balance


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="GER40 12:00 PM IST Strategy Simulator")
    parser.add_argument("--date",  action="append", default=[],
                        help="Specific date (YYYY-MM-DD). Repeatable.")
    parser.add_argument("--month", action="append", default=[],
                        help="Full month (YYYY-MM). Runs all weekdays. Repeatable.")
    parser.add_argument("--year",  action="append", default=[],
                        help="Full year (YYYY). Runs all weekdays. Repeatable.")
    args = parser.parse_args()

    if not args.date and not args.month and not args.year:
        parser.error("Provide at least one --date, --month, or --year")

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

    api = CapitalComAPI(
        api_key=CAPITAL_API_KEY,
        identifier=CAPITAL_IDENTIFIER,
        password=CAPITAL_PASSWORD,
        demo=DEMO_MODE
    )
    api.create_session()

    if START_CAPITAL is not None:
        start_balance = START_CAPITAL
    else:
        acc           = api.get_account_info()
        start_balance = acc.get("balance", {}).get("balance", 0)
    balance = start_balance

    print(f"\nGER40 12:00 PM IST — Dry-Run Simulator")
    print(f"Starting balance: AED {balance:,.2f}")
    print(f"Simulating {len(dates)} trading day(s)\n")

    monthly_start: dict = {}
    monthly_end:   dict = {}

    for sim_date in dates:
        month_key = sim_date.strftime("%Y-%m")
        if month_key not in monthly_start:
            monthly_start[month_key] = balance
        balance = simulate_day(api, sim_date, balance)
        monthly_end[month_key] = balance
        time.sleep(1)  # avoid hammering API

    pnl     = balance - start_balance
    pnl_pct = (pnl / start_balance) * 100

    if len(monthly_start) > 1:
        print(f"\n{'='*60}")
        print(f"  MONTHLY BREAKDOWN")
        print(f"{'='*60}")
        for month_key in sorted(monthly_start):
            m_start = monthly_start[month_key]
            m_end   = monthly_end[month_key]
            m_pnl   = m_end - m_start
            m_pct   = (m_pnl / start_balance) * 100
            marker  = "✅" if m_pnl >= 0 else "❌"
            print(f"  {marker}  {month_key}  |  AED {m_pnl:+8,.2f}  ({m_pct:+6.2f}%)")

    print(f"\n{'='*60}")
    print(f"  STARTING BALANCE : AED {start_balance:,.2f}")
    print(f"  FINAL BALANCE    : AED {balance:,.2f}")
    print(f"  NET P&L          : AED {pnl:+,.2f} ({pnl_pct:+.2f}%)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
