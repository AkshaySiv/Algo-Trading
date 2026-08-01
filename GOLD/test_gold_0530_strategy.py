"""
GOLD 5:30 AM IST Strategy — Historical Dry-Run Simulator
==========================================================
Replays a past GOLD trading day through the same two-stage breakout logic as
GOLD/gold-0530-runner.py. No real orders are placed; the runner requests
historical candles from the Capital.com API only.

Reference candle:
    05:30–06:00 AM IST (00:00–00:30 UTC), the Tokyo-session reference window.

Usage:
    python3 test_gold_0530_strategy.py --date 2026-04-10
    python3 test_gold_0530_strategy.py --date 2026-04-09 --date 2026-04-08
    python3 test_gold_0530_strategy.py --month 2026-03
    python3 test_gold_0530_strategy.py --month 2026-03 --month 2026-04
    python3 test_gold_0530_strategy.py --year 2026
    python3 test_gold_0530_strategy.py --year 2026 --min-range 2.0 --no-t2
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

from capitalcom_api import CapitalComAPI

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'), override=True)

# ── Config (must match gold-0530-runner.py) ─────────────────────────────
EPIC = "GOLD"
FIXED_SL_AED = 40.0          # Simulation risk per trade in AED.
RR_RATIO = 3.0
STOP_BUFFER = 1.0            # Points beyond candle H/L for the breakout entry.
SL_BUFFER = 0.0
PIP_VALUE_USD = 1.0          # Verify against the Capital.com GOLD contract.
USD_TO_AED = 3.67
START_CAPITAL = 4000.0       # Set to None to initialize from account balance.
IST = ZoneInfo("Asia/Kolkata")


def strategy_candle_window(sim_date: date) -> tuple[datetime, datetime]:
    """Return the 05:30–06:00 IST reference candle as UTC boundaries."""
    candle_open_ist = datetime(
        sim_date.year, sim_date.month, sim_date.day, 5, 30, tzinfo=IST
    )
    candle_close_ist = candle_open_ist + timedelta(minutes=30)
    return (
        candle_open_ist.astimezone(timezone.utc),
        candle_close_ist.astimezone(timezone.utc),
    )


def candle_ist_label(sim_date: date) -> str:
    """Return a human-readable India-time label for the reference candle."""
    candle_open_utc, candle_close_utc = strategy_candle_window(sim_date)
    candle_open_ist = candle_open_utc.astimezone(IST)
    candle_close_ist = candle_close_utc.astimezone(IST)
    return (
        f"{candle_open_ist.strftime('%I:%M %p')}–"
        f"{candle_close_ist.strftime('%I:%M %p')} IST"
    )

CAPITAL_API_KEY    = os.getenv('CAPITAL_API_KEY',    '')
CAPITAL_IDENTIFIER = os.getenv('CAPITAL_IDENTIFIER', '')
CAPITAL_PASSWORD   = os.getenv('CAPITAL_PASSWORD',   '')
DEMO_MODE          = os.getenv('CAPITAL_DEMO', 'true').lower() == 'true'


# ── Helpers ────────────────────────────────────────────────────────────────────
def compute_size(sl_distance: float) -> float:
    size = FIXED_SL_AED / (sl_distance * PIP_VALUE_USD * USD_TO_AED)
    return round(max(0.1, round(size, 1)), 1)


def fetch_candle(api, sim_date: date) -> Optional[dict]:
    """Fetch the 05:30–06:00 IST GOLD reference candle for ``sim_date``."""
    from_ts, to_ts_exclusive = strategy_candle_window(sim_date)
    to_ts = to_ts_exclusive - timedelta(seconds=1)
    from_str = from_ts.strftime("%Y-%m-%dT%H:%M:%S")
    to_str = to_ts.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        api._ensure_session()
        resp = api._session.get(
            f"{api.base_url}/api/v1/prices/{EPIC}",
            params={"resolution": "MINUTE_30", "from": from_str, "to": to_str},
            timeout=15,
        )
        resp.raise_for_status()
        bars = resp.json().get("prices", [])
        if not bars:
            return None
        bar = bars[0]
        high_bid = bar["highPrice"]["bid"]
        high_ask = bar["highPrice"].get("ask", high_bid)
        low_bid = bar["lowPrice"]["bid"]
        close_bid = bar["closePrice"]["bid"]
        return {
            "high": high_bid,
            "high_ask": high_ask,
            "low": low_bid,
            "close": close_bid,
            "range": round(high_bid - low_bid, 2),
        }
    except Exception:
        return None


def fetch_replay_bars(api, sim_date: date) -> list:
    """
    Fetch 30-minute bars from the reference-candle close through immediately
    before the next calendar day's 05:30 AM IST reference candle begins.
    """
    _, from_ts = strategy_candle_window(sim_date)
    next_open_ts, _ = strategy_candle_window(sim_date + timedelta(days=1))
    to_ts = next_open_ts - timedelta(seconds=1)

    api._ensure_session()
    resp = api._session.get(
        f"{api.base_url}/api/v1/prices/{EPIC}",
        params={
            "resolution": "MINUTE_30",
            "from": from_ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "to": to_ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "max": 1000,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("prices", [])


# ── Simulator ──────────────────────────────────────────────────────────────────
def simulate_day(
    api,
    sim_date: date,
    balance: float,
    *,
    min_range: float = 0.0,
    enable_t2: bool = True,
) -> float:
    date_str = sim_date.strftime("%Y-%m-%d")
    _, candle_close_utc = strategy_candle_window(sim_date)
    ist_label = candle_ist_label(sim_date)

    print(f"\n{'='*60}")
    print(f"  SIMULATION — {date_str}  |  Balance: AED {balance:,.2f}")
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

    if R < min_range:
        print(
            f"  [SKIP] Opening range {R:.2f} pts is below the "
            f"minimum {min_range:.2f} pts"
        )
        return balance

    try:
        bars = fetch_replay_bars(api, sim_date)
    except Exception as e:
        print(f"  [ERROR] Could not fetch replay bars: {e}")
        return balance

    if not bars:
        print("  [SKIP] No replay bars available")
        return balance

    print(f"  [REPLAY] {len(bars)} 30-min bars loaded from "
          f"{candle_close_utc.strftime('%H:%M')} UTC")

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
                pnl     = abs(active_tp - active_entry) * active_size * PIP_VALUE_USD * USD_TO_AED
                pnl_pct = (pnl / balance) * 100
                balance += pnl
                trade_log.append(
                    f"    ✅ TP HIT @ {active_tp:.2f}  |  +AED {pnl:.2f} (+{pnl_pct:.1f}%)  |  ts={ts}")
                active_entry = None
                done_for_day = True
                continue

            if sl_hit:
                pnl     = -abs(active_sl - active_entry) * active_size * PIP_VALUE_USD * USD_TO_AED
                pnl_pct = (pnl / balance) * 100
                balance += pnl
                trade_log.append(
                    f"    ❌ SL HIT @ {active_sl:.2f}  |  -AED {abs(pnl):.2f} ({pnl_pct:.1f}%)  |  ts={ts}")
                active_entry = None

                if trades_today == 1 and enable_t2:
                    t1_sl_hit = True
                else:
                    done_for_day = True
                continue

        # ── Entry logic ────────────────────────────────────────────────────────
        max_trades_today = 2 if enable_t2 else 1
        if done_for_day or trades_today >= max_trades_today:
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

        elif trades_today == 1 and t1_sl_hit and enable_t2:
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
            pnl = (last_price - active_entry) * active_size * PIP_VALUE_USD * USD_TO_AED
        else:
            pnl = (active_entry - last_price) * active_size * PIP_VALUE_USD * USD_TO_AED
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
    parser = argparse.ArgumentParser(description="GOLD 5:30 AM IST Strategy Simulator")
    parser.add_argument("--date",  action="append", default=[],
                        help="Specific date (YYYY-MM-DD). Repeatable.")
    parser.add_argument("--month", action="append", default=[],
                        help="Full month (YYYY-MM). Runs all weekdays. Repeatable.")
    parser.add_argument("--year",  action="append", default=[],
                        help="Full year (YYYY). Runs all weekdays. Repeatable.")
    parser.add_argument("--min-range", type=float, default=0.0,
                        help="Skip a day when the 05:30 IST candle range is below this many points. Default: 0.0.")
    parser.add_argument("--no-t2", action="store_true",
                        help="Use T1 only: do not open the opposite-side T2 reversal after a T1 stop-out.")
    args = parser.parse_args()

    if args.min_range < 0:
        parser.error("--min-range must be zero or greater")

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

    print(f"\nGOLD 5:30 AM IST — Historical Dry-Run Simulator")
    print(f"Starting balance: AED {balance:,.2f}")
    print(f"Minimum opening range: {args.min_range:.2f} points")
    print(f"T2 reversal: {'disabled (T1 only)' if args.no_t2 else 'enabled'}")
    print(f"Simulating {len(dates)} trading day(s)\n")

    monthly_start: dict = {}
    monthly_end:   dict = {}

    for sim_date in dates:
        month_key = sim_date.strftime("%Y-%m")
        if month_key not in monthly_start:
            monthly_start[month_key] = balance
        balance = simulate_day(
            api,
            sim_date,
            balance,
            min_range=args.min_range,
            enable_t2=not args.no_t2,
        )
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
