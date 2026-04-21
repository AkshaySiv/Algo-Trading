"""
US500 19:05 IST Strategy — Dry-Run Simulator
=============================================
Replays a past trading day through the exact same logic as us500-1905-runner.py.
No real orders are placed. Uses real historical candle data from Capital.com API.

Usage:
    python3 test_runner.py --date 2026-04-10
    python3 test_runner.py --date 2026-04-09 --date 2026-04-08
    python3 test_runner.py --month 2026-03
    python3 test_runner.py --month 2026-03 --month 2026-04
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

# ── Config (must match runner) ─────────────────────────────────────────────────
EPIC           = "US500"
FIXED_SL_AED   = 50.0        # fixed risk per trade in AED — must match runner
RR_RATIO       = 3.0
STOP_BUFFER    = 0.1         # points beyond candle H/L for entry trigger
SL_BUFFER      = 0.0         # extra points beyond candle H/L for SL (0 = SL at exact candle H/L)
PIP_VALUE_USD  = 1.0
USD_TO_AED     = 3.67
START_CAPITAL  = 10_000.0    # starting capital in AED for simulation (set None to use live balance)
IST           = timezone(timedelta(hours=5, minutes=30))

CAPITAL_API_KEY    = os.getenv('CAPITAL_API_KEY',    '')
CAPITAL_IDENTIFIER = os.getenv('CAPITAL_IDENTIFIER', '')
CAPITAL_PASSWORD   = os.getenv('CAPITAL_PASSWORD',   '')
DEMO_MODE          = os.getenv('CAPITAL_DEMO', 'true').lower() == 'true'


# ── DST-aware NY market open candle time ──────────────────────────────────────
def candle_utc_hour(d: date) -> int:
    """
    Returns the UTC hour of the first 5-min candle after NY market open.
    EDT (mid-Mar to early Nov): NY opens 13:30 UTC → candle at 13:35 UTC
    EST (early Nov to mid-Mar): NY opens 14:30 UTC → candle at 14:35 UTC
    """
    year = d.year
    # DST start: 2nd Sunday in March
    mar1 = date(year, 3, 1)
    dst_start = mar1 + timedelta(days=(6 - mar1.weekday()) % 7 + 7)
    # DST end: 1st Sunday in November
    nov1 = date(year, 11, 1)
    dst_end = nov1 + timedelta(days=(6 - nov1.weekday()) % 7)
    return 13 if dst_start <= d < dst_end else 14


# ── Helpers ────────────────────────────────────────────────────────────────────
def compute_size(sl_distance: float) -> float:
    size = FIXED_SL_AED / (sl_distance * PIP_VALUE_USD * USD_TO_AED)
    return round(max(0.1, round(size, 1)), 1)


def fetch_candle(api, sim_date: date) -> Optional[dict]:
    """Fetch the opening 5-min candle for the given date. Adjusts for US DST."""
    ch       = candle_utc_hour(sim_date)
    from_ts  = datetime(sim_date.year, sim_date.month, sim_date.day,
                        ch, 35, 0, tzinfo=timezone.utc)
    to_ts    = datetime(sim_date.year, sim_date.month, sim_date.day,
                        ch, 39, 59, tzinfo=timezone.utc)
    from_str = from_ts.strftime("%Y-%m-%dT%H:%M:%S")
    to_str   = to_ts.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        api._ensure_session()
        resp = api._session.get(
            f"{api.base_url}/api/v1/prices/{EPIC}",
            params={"resolution": "MINUTE_5", "from": from_str, "to": to_str},
            timeout=15
        )
        resp.raise_for_status()
        bars = resp.json().get("prices", [])
        if not bars:
            return None
        bar   = bars[0]
        h_bid = bar["highPrice"]["bid"]
        h_ask = bar["highPrice"].get("ask", h_bid)  # ask high for SELL SL
        l_bid = bar["lowPrice"]["bid"]
        c     = bar["closePrice"]["bid"]
        return {"high": h_bid, "high_ask": h_ask, "low": l_bid,
                "close": c, "range": round(h_bid - l_bid, 2)}
    except Exception:
        return None


def fetch_minute_bars(api, sim_date: date) -> list:
    """
    Fetch 5-min bars from candle close UTC through to next day's 13:40 UTC close.
    Matches live runner: trade runs overnight, closed before next day's fresh candle.
    """
    ch       = candle_utc_hour(sim_date)
    from_ts  = datetime(sim_date.year, sim_date.month, sim_date.day,
                        ch, 40, 0, tzinfo=timezone.utc)
    # Extend to next day 13:40 UTC — mirrors live runner's overnight close logic
    next_day  = datetime(sim_date.year, sim_date.month, sim_date.day,
                         tzinfo=timezone.utc) + timedelta(days=1)
    ch_next   = candle_utc_hour(next_day.date())
    to_ts     = datetime(next_day.year, next_day.month, next_day.day,
                         ch_next, 39, 59, tzinfo=timezone.utc)
    from_str = from_ts.strftime("%Y-%m-%dT%H:%M:%S")
    to_str   = to_ts.strftime("%Y-%m-%dT%H:%M:%S")

    api._ensure_session()
    resp = api._session.get(
        f"{api.base_url}/api/v1/prices/{EPIC}",
        params={"resolution": "MINUTE_5", "from": from_str, "to": to_str, "max": 1000},
        timeout=15
    )
    resp.raise_for_status()
    return resp.json().get("prices", [])


# ── Simulator ──────────────────────────────────────────────────────────────────
def simulate_day(api, sim_date: date, balance: float):
    date_str = sim_date.strftime("%Y-%m-%d")
    print(f"\n{'='*60}")
    print(f"  SIMULATION — {date_str}  |  Balance: AED {balance:,.2f}")
    print(f"{'='*60}")

    # Fetch 19:05 IST candle
    candle = fetch_candle(api, sim_date)
    if candle is None:
        print("  [SKIP] No candle data — market holiday or data unavailable")
        return balance

    H     = candle["high"]       # bid high — used for BUY entry trigger & BUY TP monitor
    H_ask = candle["high_ask"]   # ask high — used for SELL SL (BUY stop triggers at ask)
    L     = candle["low"]
    R     = candle["range"]
    ch         = candle_utc_hour(sim_date)
    ist_min    = 35 + 30  # UTC+5:30 → +5h30m → minute stays 35+30=65 → 05 next hour
    ist_hour   = ch + 5 + (1 if ist_min >= 60 else 0)
    ist_min   %= 60
    candle_ist = f"{ist_hour:02d}:{ist_min:02d} IST"
    print(f"  [CANDLE] {candle_ist} | H={H} (ask={H_ask})  L={L}  Range={R} pts")
    print(f"  [LEVELS] BUY trigger > {H + STOP_BUFFER}  |  SELL trigger < {L - STOP_BUFFER}")

    # Fetch 1-min bars for replay
    try:
        bars = fetch_minute_bars(api, sim_date)
    except Exception as e:
        print(f"  [ERROR] Could not fetch replay bars: {e}")
        return balance

    if not bars:
        print("  [SKIP] No replay bars available")
        return balance

    print(f"  [REPLAY] {len(bars)} 5-min bars loaded from 13:40 UTC")

    # ── Replay state ───────────────────────────────────────────────────────────
    trades_today   = 0
    t1_direction   = None
    t1_sl_hit      = False
    done_for_day   = False
    active_entry   = None
    active_sl      = None
    active_tp      = None
    active_dir     = None
    trade_log      = []

    for bar in bars:
        if done_for_day:
            break

        ts           = bar.get("snapshotTimeUTC", bar.get("snapshotTime", "?"))
        bar_high_bid = bar["highPrice"]["bid"]
        bar_high_ask = bar["highPrice"].get("ask", bar["highPrice"]["bid"])
        bar_low_bid  = bar["lowPrice"]["bid"]
        bar_open     = bar["openPrice"]["bid"]

        # ── Monitor active trade ───────────────────────────────────────────────
        # SELL SL is a BUY stop → triggers at ask; BUY SL is a SELL stop → triggers at bid
        # TP for both sides monitored at bid (Capital.com TP closes at bid for SELL, bid for BUY)
        if active_entry is not None:
            sl_hit = (active_dir == "BUY"  and bar_low_bid  <= active_sl) or \
                     (active_dir == "SELL" and bar_high_bid >= active_sl)
            tp_hit = (active_dir == "BUY"  and bar_high_bid >= active_tp) or \
                     (active_dir == "SELL" and bar_low_bid  <= active_tp)

            # If both hit in same bar, determine order by open price direction
            if sl_hit and tp_hit:
                if active_dir == "BUY":
                    sl_hit = bar_open >= active_entry  # opened flat/up → SL hit first if down move
                    tp_hit = not sl_hit
                else:
                    sl_hit = bar_open <= active_entry
                    tp_hit = not sl_hit

            if tp_hit:
                pnl = abs(active_tp - active_entry) * active_size * PIP_VALUE_USD * USD_TO_AED
                pnl_pct = (pnl / balance) * 100
                balance += pnl
                trade_log.append(
                    f"    ✅ TP HIT @ {active_tp:.2f}  |  +AED {pnl:.2f} (+{pnl_pct:.1f}%)  |  ts={ts}")
                active_entry = None
                done_for_day = True
                continue

            if sl_hit:
                pnl = -abs(active_sl - active_entry) * active_size * PIP_VALUE_USD * USD_TO_AED
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
            continue  # no new entries, but monitoring above still runs each bar

        if trades_today == 0:
            sell_triggered = bar_low_bid  < L - STOP_BUFFER
            buy_triggered  = bar_high_bid > H + STOP_BUFFER

            if sell_triggered and buy_triggered:
                # Both breached in same bar — use open price to decide direction
                sell_triggered = bar_open <= (H + L) / 2
                buy_triggered  = not sell_triggered

            if sell_triggered:
                entry   = round(L - STOP_BUFFER, 2)
                sl      = round(H + SL_BUFFER, 2)         # SELL SL = candle bid high
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

            elif buy_triggered:
                entry   = round(H + STOP_BUFFER, 2)
                sl      = round(L - SL_BUFFER, 2)         # BUY SL = candle bid low
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

        elif trades_today == 1 and t1_sl_hit:
            if t1_direction == "BUY":
                # T1 BUY stopped at L-buf → T2 SELL, entry at reversal price, SL above ask high
                t2_entry  = round(L - STOP_BUFFER, 2)     # price where T1 BUY SL was hit
                t2_sl     = round(H + SL_BUFFER, 2)       # SELL SL = candle bid high
                t2_sldist = round(t2_sl - t2_entry, 2)
                t2_tp     = round(t2_entry - RR_RATIO * t2_sldist, 2)
                active_entry = t2_entry
                active_sl    = t2_sl
                active_tp    = t2_tp
                active_dir   = "SELL"
                label        = "T2 SELL"
                active_size  = compute_size(t2_sldist)
                trade_log.append(
                    f"  [{label}] Entry={t2_entry:.2f}  SL={t2_sl:.2f}  "
                    f"TP={t2_tp:.2f}  Size={active_size}  SL-dist={t2_sldist:.2f}pts  |  ts={ts}")
            else:
                # T1 SELL stopped at H_ask+buf → T2 BUY, entry at reversal price, SL below bid low
                t2_entry  = round(H + STOP_BUFFER, 2)     # price where T1 SELL SL was hit
                t2_sl     = round(L - SL_BUFFER, 2)       # BUY SL = candle bid low
                t2_sldist = round(t2_entry - t2_sl, 2)
                t2_tp     = round(t2_entry + RR_RATIO * t2_sldist, 2)
                active_entry = t2_entry
                active_sl    = t2_sl
                active_tp    = t2_tp
                active_dir   = "BUY"
                label        = "T2 BUY"
                active_size  = compute_size(t2_sldist)
                trade_log.append(
                    f"  [{label}] Entry={t2_entry:.2f}  SL={t2_sl:.2f}  "
                    f"TP={t2_tp:.2f}  Size={active_size}  SL-dist={t2_sldist:.2f}pts  |  ts={ts}")

            trades_today = 2
            t1_sl_hit    = False

    # ── End of day — close any open position at last bar close ─────────────────
    if active_entry is not None:
        last_price = bars[-1]["closePrice"]["bid"]
        last_ts    = bars[-1].get("snapshotTimeUTC", "EOD")
        if active_dir == "BUY":
            pnl = (last_price - active_entry) * active_size * PIP_VALUE_USD * USD_TO_AED
        else:
            pnl = (active_entry - last_price) * active_size * PIP_VALUE_USD * USD_TO_AED
        pnl_aed = round(pnl, 2)
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
    parser = argparse.ArgumentParser(description="US500 19:05 IST Strategy Simulator")
    parser.add_argument("--date",  action="append", default=[],
                        help="Specific date (YYYY-MM-DD). Repeatable.")
    parser.add_argument("--month", action="append", default=[],
                        help="Full month (YYYY-MM). Runs all weekdays. Repeatable.")
    parser.add_argument("--year",  action="append", default=[],
                        help="Full year (YYYY). Runs all weekdays. Repeatable.")
    args = parser.parse_args()

    if not args.date and not args.month and not args.year:
        parser.error("Provide at least one --date, --month, or --year")

    # Build sorted list of dates
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

    print(f"\nUS500 19:05 IST — Dry-Run Simulator")
    print(f"Starting balance: AED {balance:,.2f}")
    print(f"Simulating {len(dates)} trading day(s)\n")

    monthly_start: dict[str, float] = {}
    monthly_end:   dict[str, float] = {}

    for sim_date in dates:
        month_key = sim_date.strftime("%Y-%m")
        if month_key not in monthly_start:
            monthly_start[month_key] = balance
        balance = simulate_day(api, sim_date, balance)
        monthly_end[month_key] = balance
        time.sleep(1)  # avoid hammering API

    pnl     = balance - start_balance
    pnl_pct = (pnl / start_balance) * 100

    # Monthly breakdown (only shown when more than one month was simulated)
    if len(monthly_start) > 1:
        print(f"\n{'='*60}")
        print(f"  MONTHLY BREAKDOWN")
        print(f"{'='*60}")
        for month_key in sorted(monthly_start):
            m_start = monthly_start[month_key]
            m_end   = monthly_end[month_key]
            m_pnl   = m_end - m_start
            m_pct   = (m_pnl / m_start) * 100
            marker  = "✅" if m_pnl >= 0 else "❌"
            print(f"  {marker}  {month_key}  |  AED {m_pnl:+8,.2f}  ({m_pct:+6.2f}%)")

    print(f"\n{'='*60}")
    print(f"  STARTING BALANCE : AED {start_balance:,.2f}")
    print(f"  FINAL BALANCE    : AED {balance:,.2f}")
    print(f"  NET P&L          : AED {pnl:+,.2f} ({pnl_pct:+.2f}%)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
