"""
GOLD 5:30 AM IST Strategy — Dry-Run Simulator
================================================
Replays historical GOLD trading days through the same two-stage breakout logic as
GER40/ger40-1200-test-runner.py. No real orders are placed; the script requests
historical candles from the Capital.com API only.

Strategy timing:
    The reference candle opens at 5:30 AM India time (Tokyo equity-session open)
    and lasts 30 minutes: 05:30–06:00 IST (00:00–00:30 UTC).

Usage:
    python3 gold-0530-test-runner.py --date 2026-04-10
    python3 gold-0530-test-runner.py --date 2026-04-09 --date 2026-04-08
    python3 gold-0530-test-runner.py --month 2026-03
    python3 gold-0530-test-runner.py --month 2026-03 --month 2026-04
    python3 gold-0530-test-runner.py --year 2026

Before relying on results, confirm the GOLD contract's point value, minimum deal
size, and the API epic with the broker for the relevant account.
"""

import argparse
import calendar
import os
import time
from datetime import date, datetime, time as clock_time, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from capitalcom_api import CapitalComAPI


# ── Config ────────────────────────────────────────────────────────────────────
# These values are deliberately grouped here so the GOLD contract assumptions can
# be checked and adjusted without changing the entry/exit logic.
EPIC = "GOLD"
FIXED_SL_AED = 40.0          # Simulation risk per trade in AED.
RR_RATIO = 3.0               # Target = 3 × initial stop distance.
STOP_BUFFER = 1.0            # Points beyond the reference candle H/L for entry.
SL_BUFFER = 0.0              # Extra points beyond the opposite candle extreme.
PIP_VALUE_USD = 1.0          # Verify against the Capital.com GOLD contract.
USD_TO_AED = 3.67            # Conversion assumption used for simulation P&L.
START_CAPITAL = 4000.0       # Set to None to initialize from the account balance.

ENTRY_TIME_IST = clock_time(5, 30)
CANDLE_MINUTES = 30
IST = ZoneInfo("Asia/Kolkata")
UTC = timezone.utc


load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)

CAPITAL_API_KEY = os.getenv("CAPITAL_API_KEY", "")
CAPITAL_IDENTIFIER = os.getenv("CAPITAL_IDENTIFIER", "")
CAPITAL_PASSWORD = os.getenv("CAPITAL_PASSWORD", "")
DEMO_MODE = os.getenv("CAPITAL_DEMO", "true").lower() == "true"


# ── Time helpers ──────────────────────────────────────────────────────────────
def strategy_candle_window(sim_date: date) -> tuple[datetime, datetime]:
    """Return the reference candle's inclusive UTC start and exclusive UTC end."""
    candle_open_ist = datetime.combine(sim_date, ENTRY_TIME_IST, tzinfo=IST)
    candle_open_utc = candle_open_ist.astimezone(UTC)
    candle_close_utc = candle_open_utc + timedelta(minutes=CANDLE_MINUTES)
    return candle_open_utc, candle_close_utc


def candle_ist_label(sim_date: date) -> str:
    """Return the configured reference-candle window as an India-time label."""
    candle_open_utc, candle_close_utc = strategy_candle_window(sim_date)
    candle_open_ist = candle_open_utc.astimezone(IST)
    candle_close_ist = candle_close_utc.astimezone(IST)
    return (
        f"{candle_open_ist.strftime('%I:%M %p')}–"
        f"{candle_close_ist.strftime('%I:%M %p')} IST"
    )


# ── Helpers ───────────────────────────────────────────────────────────────────
def compute_size(sl_distance: float) -> float:
    """Calculate a one-decimal deal size from fixed AED risk."""
    if sl_distance <= 0:
        raise ValueError("Stop-loss distance must be positive")

    size = FIXED_SL_AED / (sl_distance * PIP_VALUE_USD * USD_TO_AED)
    return round(max(0.1, round(size, 1)), 1)


def fetch_candle(api: CapitalComAPI, sim_date: date) -> Optional[dict]:
    """Fetch the 5:30–6:00 AM IST reference candle for one simulation date."""
    from_ts, to_ts_exclusive = strategy_candle_window(sim_date)
    to_ts = to_ts_exclusive - timedelta(seconds=1)

    try:
        api._ensure_session()
        response = api._session.get(
            f"{api.base_url}/api/v1/prices/{EPIC}",
            params={
                "resolution": "MINUTE_30",
                "from": from_ts.strftime("%Y-%m-%dT%H:%M:%S"),
                "to": to_ts.strftime("%Y-%m-%dT%H:%M:%S"),
            },
            timeout=15,
        )
        response.raise_for_status()
        bars = response.json().get("prices", [])
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


def fetch_replay_bars(api: CapitalComAPI, sim_date: date) -> list:
    """
    Fetch 30-minute replay bars from reference-candle close until immediately
    before the next calendar day's 5:30 AM IST reference candle begins.
    """
    _, from_ts = strategy_candle_window(sim_date)
    next_open_ts, _ = strategy_candle_window(sim_date + timedelta(days=1))
    to_ts = next_open_ts - timedelta(seconds=1)

    api._ensure_session()
    response = api._session.get(
        f"{api.base_url}/api/v1/prices/{EPIC}",
        params={
            "resolution": "MINUTE_30",
            "from": from_ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "to": to_ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "max": 1000,
        },
        timeout=15,
    )
    response.raise_for_status()
    return response.json().get("prices", [])


def determine_exit_outcome(
    direction: str,
    entry: float,
    stop: float,
    target: float,
    bar_high_bid: float,
    bar_high_ask: float,
    bar_low_bid: float,
    bar_open: float,
) -> Optional[str]:
    """
    Return ``TP`` or ``SL`` when an active position's target or stop was crossed.

    A 30-minute OHLC bar cannot reveal the intrabar sequence when both levels
    are crossed. In that case, preserve the GER40 convention: resolve a BUY as
    TP-first only when the bar opened at/above entry, and a SELL as TP-first
    only when it opened at/below entry. Otherwise resolve the stop first.
    """
    sl_hit = (
        (direction == "BUY" and bar_low_bid <= stop)
        or (direction == "SELL" and bar_high_ask >= stop)
    )
    tp_hit = (
        (direction == "BUY" and bar_high_bid >= target)
        or (direction == "SELL" and bar_low_bid <= target)
    )

    if sl_hit and tp_hit:
        if direction == "BUY":
            return "TP" if bar_open >= entry else "SL"
        return "TP" if bar_open <= entry else "SL"
    if tp_hit:
        return "TP"
    if sl_hit:
        return "SL"
    return None


# ── Simulator ─────────────────────────────────────────────────────────────────
def simulate_day(api: CapitalComAPI, sim_date: date, balance: float) -> float:
    """Run the GER40-style two-trade breakout replay for one GOLD trading day."""
    date_str = sim_date.strftime("%Y-%m-%d")
    _, candle_close_utc = strategy_candle_window(sim_date)
    ist_label = candle_ist_label(sim_date)

    print(f"\n{'=' * 60}")
    print(f"  SIMULATION — {date_str}  |  Balance: AED {balance:,.2f}")
    print(f"{'=' * 60}")

    candle = fetch_candle(api, sim_date)
    if candle is None:
        print("  [SKIP] No candle data — market holiday or data unavailable")
        return balance

    high_bid = candle["high"]
    high_ask = candle["high_ask"]
    low_bid = candle["low"]
    candle_range = candle["range"]
    print(
        f"  [CANDLE] {ist_label} | H={high_bid} (ask={high_ask})  "
        f"L={low_bid}  Range={candle_range} pts"
    )
    print(
        f"  [LEVELS] BUY trigger > {high_ask + STOP_BUFFER:.2f}  |  "
        f"SELL trigger < {low_bid - STOP_BUFFER:.2f}"
    )

    try:
        bars = fetch_replay_bars(api, sim_date)
    except Exception as error:
        print(f"  [ERROR] Could not fetch replay bars: {error}")
        return balance

    if not bars:
        print("  [SKIP] No replay bars available")
        return balance

    print(
        f"  [REPLAY] {len(bars)} 30-min bars loaded from "
        f"{candle_close_utc.strftime('%H:%M')} UTC"
    )

    # ── Replay state ───────────────────────────────────────────────────────────
    trades_today = 0
    t1_direction: Optional[str] = None
    t1_sl_hit = False
    done_for_day = False
    active_entry: Optional[float] = None
    active_sl: Optional[float] = None
    active_tp: Optional[float] = None
    active_size: Optional[float] = None
    active_dir: Optional[str] = None
    trade_log: list[str] = []

    def settle_active_trade(outcome: str, timestamp: str, *, entry_bar: bool) -> None:
        """Record a TP/SL outcome and update the two-trade replay state."""
        nonlocal active_dir, active_entry, active_size, active_sl, active_tp
        nonlocal balance, done_for_day, t1_sl_hit

        assert active_entry is not None
        assert active_sl is not None
        assert active_tp is not None
        assert active_size is not None

        if outcome == "TP":
            close_level = active_tp
            pnl = (
                abs(close_level - active_entry)
                * active_size
                * PIP_VALUE_USD
                * USD_TO_AED
            )
            pnl_pct = (pnl / balance) * 100 if balance else 0.0
            balance += pnl
            trade_log.append(
                f"    ✅ TP HIT @ {close_level:.2f}  |  +AED {pnl:.2f} "
                f"(+{pnl_pct:.1f}%)  |  ts={timestamp}"
            )
            done_for_day = True
        elif outcome == "SL":
            close_level = active_sl
            pnl = -(
                abs(close_level - active_entry)
                * active_size
                * PIP_VALUE_USD
                * USD_TO_AED
            )
            pnl_pct = (pnl / balance) * 100 if balance else 0.0
            balance += pnl
            hit_note = " (entry-bar reversal)" if entry_bar else ""
            trade_log.append(
                f"    ❌ SL HIT @ {close_level:.2f}{hit_note}  |  -AED {abs(pnl):.2f} "
                f"({pnl_pct:.1f}%)  |  ts={timestamp}"
            )
            if trades_today == 1:
                t1_sl_hit = True
            else:
                done_for_day = True
        else:
            raise ValueError(f"Unsupported trade outcome: {outcome}")

        active_entry = None
        active_sl = None
        active_tp = None
        active_size = None
        active_dir = None

    for bar in bars:
        if done_for_day:
            break

        timestamp = bar.get("snapshotTimeUTC", bar.get("snapshotTime", "?"))
        bar_high_bid = bar["highPrice"]["bid"]
        bar_high_ask = bar["highPrice"].get("ask", bar_high_bid)
        bar_low_bid = bar["lowPrice"]["bid"]
        bar_open = bar["openPrice"]["bid"]

        # ── Monitor an active trade from an earlier replay bar ────────────────
        if active_entry is not None:
            assert active_sl is not None
            assert active_tp is not None
            assert active_dir is not None

            outcome = determine_exit_outcome(
                active_dir,
                active_entry,
                active_sl,
                active_tp,
                bar_high_bid,
                bar_high_ask,
                bar_low_bid,
                bar_open,
            )
            if outcome is not None:
                settle_active_trade(outcome, timestamp, entry_bar=False)
                # A T2 reversal is deliberately armed on the next replay bar.
                continue

        # ── Entry logic ─────────────────────────────────────────────────────────
        opened_this_bar = False
        if done_for_day or trades_today >= 2:
            continue

        if trades_today == 0:
            buy_triggered = bar_high_ask > high_ask + STOP_BUFFER
            sell_triggered = bar_low_bid < low_bid - STOP_BUFFER

            if buy_triggered and sell_triggered:
                midpoint = (high_bid + low_bid) / 2
                sell_triggered = bar_open <= midpoint
                buy_triggered = not sell_triggered

            if buy_triggered:
                entry = round(high_ask + STOP_BUFFER, 2)
                sl = round(low_bid - SL_BUFFER, 2)
                sl_distance = round(entry - sl, 2)
                size = compute_size(sl_distance)
                tp = round(entry + RR_RATIO * sl_distance, 2)
                label = "T1 BUY"
                t1_direction = "BUY"
                active_entry = entry
                active_sl = sl
                active_tp = tp
                active_size = size
                active_dir = "BUY"
                trades_today = 1
                opened_this_bar = True
                trade_log.append(
                    f"  [{label}] Entry={entry:.2f}  SL={sl:.2f}  TP={tp:.2f}  "
                    f"Size={size}  SL-dist={sl_distance:.2f}pts  |  ts={timestamp}"
                )

            elif sell_triggered:
                entry = round(low_bid - STOP_BUFFER, 2)
                sl = round(high_ask + SL_BUFFER, 2)
                sl_distance = round(sl - entry, 2)
                size = compute_size(sl_distance)
                tp = round(entry - RR_RATIO * sl_distance, 2)
                label = "T1 SELL"
                t1_direction = "SELL"
                active_entry = entry
                active_sl = sl
                active_tp = tp
                active_size = size
                active_dir = "SELL"
                trades_today = 1
                opened_this_bar = True
                trade_log.append(
                    f"  [{label}] Entry={entry:.2f}  SL={sl:.2f}  TP={tp:.2f}  "
                    f"Size={size}  SL-dist={sl_distance:.2f}pts  |  ts={timestamp}"
                )

        elif trades_today == 1 and t1_sl_hit:
            if t1_direction == "BUY":
                # T1 BUY hit its stop near L: T2 reverses short at the L boundary.
                t2_entry = round(low_bid - STOP_BUFFER, 2)
                t2_sl = round(high_ask + SL_BUFFER, 2)
                t2_sl_distance = round(t2_sl - t2_entry, 2)
                t2_tp = round(t2_entry - RR_RATIO * t2_sl_distance, 2)
                active_dir = "SELL"
                label = "T2 SELL"
            else:
                # T1 SELL hit its stop near H: T2 reverses long at the H boundary.
                t2_entry = round(high_ask + STOP_BUFFER, 2)
                t2_sl = round(low_bid - SL_BUFFER, 2)
                t2_sl_distance = round(t2_entry - t2_sl, 2)
                t2_tp = round(t2_entry + RR_RATIO * t2_sl_distance, 2)
                active_dir = "BUY"
                label = "T2 BUY"

            active_entry = t2_entry
            active_sl = t2_sl
            active_tp = t2_tp
            active_size = compute_size(t2_sl_distance)
            trades_today = 2
            t1_sl_hit = False
            opened_this_bar = True
            trade_log.append(
                f"  [{label}] Entry={t2_entry:.2f}  SL={t2_sl:.2f}  "
                f"TP={t2_tp:.2f}  Size={active_size}  "
                f"SL-dist={t2_sl_distance:.2f}pts  |  ts={timestamp}"
            )

        # A position opened by a breakout can hit its TP or SL inside the very
        # same 30-minute candle. Evaluate it before moving to the next bar.
        if opened_this_bar and active_entry is not None:
            assert active_sl is not None
            assert active_tp is not None
            assert active_dir is not None
            outcome = determine_exit_outcome(
                active_dir,
                active_entry,
                active_sl,
                active_tp,
                bar_high_bid,
                bar_high_ask,
                bar_low_bid,
                bar_open,
            )
            if outcome is not None:
                settle_active_trade(outcome, timestamp, entry_bar=True)

    # ── End of day: close any simulated position at the final replay-bar close ─
    if active_entry is not None:
        assert active_size is not None
        assert active_dir is not None
        last_price = bars[-1]["closePrice"]["bid"]
        last_timestamp = bars[-1].get("snapshotTimeUTC", "EOD")
        if active_dir == "BUY":
            pnl = (last_price - active_entry) * active_size * PIP_VALUE_USD * USD_TO_AED
        else:
            pnl = (active_entry - last_price) * active_size * PIP_VALUE_USD * USD_TO_AED
        pnl_aed = round(pnl, 2)
        balance += pnl_aed
        trade_log.append(
            f"    EOD CLOSE @ {last_price:.2f}  |  AED {pnl_aed:+.2f}  "
            f"|  ts={last_timestamp}"
        )

    for line in trade_log:
        print(line)

    if not trade_log:
        print("  [NO TRADE] Price never broke out of candle range")

    print(f"\n  Balance after: AED {balance:,.2f}")
    return balance


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="GOLD 5:30 AM IST GER40-style breakout simulator"
    )
    parser.add_argument(
        "--date", action="append", default=[], help="Specific date (YYYY-MM-DD). Repeatable."
    )
    parser.add_argument(
        "--month", action="append", default=[], help="Full month (YYYY-MM). Runs weekdays."
    )
    parser.add_argument(
        "--year", action="append", default=[], help="Full year (YYYY). Runs weekdays."
    )
    args = parser.parse_args()

    if not args.date and not args.month and not args.year:
        parser.error("Provide at least one --date, --month, or --year")

    today = date.today()
    dates: list[date] = []

    for date_argument in args.date:
        dates.append(datetime.strptime(date_argument, "%Y-%m-%d").date())

    for month_argument in args.month:
        year, month = map(int, month_argument.split("-"))
        _, last_day = calendar.monthrange(year, month)
        for day in range(1, last_day + 1):
            candidate = date(year, month, day)
            if candidate.weekday() < 5 and candidate <= today:
                dates.append(candidate)

    for year_argument in args.year:
        for month in range(1, 13):
            _, last_day = calendar.monthrange(int(year_argument), month)
            for day in range(1, last_day + 1):
                candidate = date(int(year_argument), month, day)
                if candidate.weekday() < 5 and candidate <= today:
                    dates.append(candidate)

    dates = sorted(set(dates))

    api = CapitalComAPI(
        api_key=CAPITAL_API_KEY,
        identifier=CAPITAL_IDENTIFIER,
        password=CAPITAL_PASSWORD,
        demo=DEMO_MODE,
    )
    api.create_session()

    if START_CAPITAL is not None:
        start_balance = START_CAPITAL
    else:
        account = api.get_account_info()
        start_balance = account.get("balance", {}).get("balance", 0)
    balance = start_balance

    print("\nGOLD 5:30 AM IST — GER40-Style Dry-Run Simulator")
    print(f"Reference candle: {CANDLE_MINUTES} min from 05:30 IST")
    print(f"Starting balance: AED {balance:,.2f}")
    print(f"Simulating {len(dates)} trading day(s)\n")

    monthly_start: dict[str, float] = {}
    monthly_end: dict[str, float] = {}

    for sim_date in dates:
        month_key = sim_date.strftime("%Y-%m")
        if month_key not in monthly_start:
            monthly_start[month_key] = balance
        balance = simulate_day(api, sim_date, balance)
        monthly_end[month_key] = balance
        time.sleep(1)  # Avoid hammering the API during multi-day simulation.

    pnl = balance - start_balance
    pnl_pct = (pnl / start_balance) * 100 if start_balance else 0.0

    if len(monthly_start) > 1:
        print(f"\n{'=' * 60}")
        print("  MONTHLY BREAKDOWN")
        print(f"{'=' * 60}")
        for month_key in sorted(monthly_start):
            month_start = monthly_start[month_key]
            month_end = monthly_end[month_key]
            month_pnl = month_end - month_start
            month_pct = (month_pnl / month_start) * 100 if month_start else 0.0
            marker = "WIN" if month_pnl >= 0 else "LOSS"
            print(
                f"  {marker:4} {month_key}  |  AED {month_pnl:+8,.2f}  "
                f"({month_pct:+6.2f}%)"
            )

    print(f"\n{'=' * 60}")
    print(f"  STARTING BALANCE : AED {start_balance:,.2f}")
    print(f"  FINAL BALANCE    : AED {balance:,.2f}")
    print(f"  NET P&L          : AED {pnl:+,.2f} ({pnl_pct:+.2f}%)")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
