#!/usr/bin/env python3
"""
US500 New York Open First-Swing BOS — Capital.com Dry-Run Backtester
===================================================================

Faithful Python replay of the TradingView strategy discussed in this project.
It reads Capital.com historical 1-minute US500 bars and NEVER opens, closes,
or modifies a Capital.com position.

Strategy rules
--------------
1. Session begins at the 09:30 New York cash-equity open. This is 19:00 IST
   during U.S. daylight time and 20:00 IST during U.S. standard time. Entries
   are permitted only during the following three hours.
2. Capture the first confirmed pivot high and pivot low after the 09:30 New
   York opening.
   Those two levels define the BOS range.
3. The first close (or wick, if selected) through the range high is a BUY BOS;
   the first close/wick through the range low is a SELL BOS.
4. Take only one trade per IST session.
5. Stop-loss hierarchy:
      BUY  -> latest confirmed swing low below entry, otherwise first swing low,
              otherwise the breakout candle low.
      SELL -> latest confirmed swing high above entry, otherwise first swing high,
              otherwise the breakout candle high.
6. Take-profit is a fixed 1:2 reward:risk target.

The runner uses bid OHLC for signal structure, mirroring a chart series. Its
execution model uses Capital.com's bid/ask data: long entries use the offer;
short entries use the bid; long exits test bid prices; short exits test offer
prices. If one 1-minute bar reaches both SL and TP, the runner records SL first
as the conservative, path-unknown result.

Examples
--------
    python3 us500-1900-first-swing-bos-test-runner.py --date 2026-09-04
    python3 us500-1900-first-swing-bos-test-runner.py --month 2026-08
    python3 us500-1900-first-swing-bos-test-runner.py --year 2026 --risk-aed 40
    python3 us500-1900-first-swing-bos-test-runner.py --self-test

Credentials are loaded from US500/.env (or inherited environment variables):
    CAPITAL_API_KEY
    CAPITAL_IDENTIFIER
    CAPITAL_PASSWORD
    CAPITAL_DEMO=true
"""

from __future__ import annotations

import argparse
import calendar
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from capitalcom_api import CapitalComAPI


# ── Strategy and Capital.com configuration ───────────────────────────────────
EPIC = "US500"
IST = ZoneInfo("Asia/Kolkata")
NEW_YORK = ZoneInfo("America/New_York")
UTC = timezone.utc
NEW_YORK_OPEN_HOUR = 9
NEW_YORK_OPEN_MINUTE = 30
ENTRY_WINDOW_MINUTES = 180  # Three hours from the 09:30 New York cash open.
REWARD_MULTIPLE = 2.0      # 1:2 reward:risk
DEFAULT_RISK_AED = 40.0
DEFAULT_STARTING_CAPITAL_AED = 4_000.0
DEFAULT_VALUE_PER_POINT_USD = 1.0
USD_TO_AED = 3.6725
DEFAULT_MIN_DEAL_SIZE = 0.1
DEFAULT_SIZE_INCREMENT = 0.1
REQUEST_CHUNK_MINUTES = 900  # Capital.com price endpoint returns at most 1,000 bars
API_PAUSE_SECONDS = 0.25

PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env", override=False)


@dataclass(frozen=True)
class Candle:
    """A single Capital.com one-minute bar with bid and ask OHLC values."""

    timestamp: datetime
    open_bid: float
    high_bid: float
    low_bid: float
    close_bid: float
    open_ask: float
    high_ask: float
    low_ask: float
    close_ask: float


@dataclass
class Trade:
    """One simulated Capital.com transaction."""

    direction: str
    entry_time: datetime
    entry: float
    stop: float
    target: float
    size: float
    planned_risk_aed: float
    stop_source: str
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    pnl_aed: Optional[float] = None


@dataclass
class SimulationResult:
    """Replay result for one New York trading-session date."""

    session_date: date
    first_swing_high: Optional[float]
    first_swing_low: Optional[float]
    trade: Optional[Trade]
    note: str


# ── Date and timestamp helpers ───────────────────────────────────────────────
def new_york_open(session_date: date) -> datetime:
    """Return the 09:30 New York cash-equity opening timestamp for a session."""
    return datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        NEW_YORK_OPEN_HOUR,
        NEW_YORK_OPEN_MINUTE,
        tzinfo=NEW_YORK,
    )


def next_weekday(session_date: date) -> date:
    """Return the next Monday-Friday calendar date without assuming it is a trading day."""
    candidate = session_date + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def session_bounds_utc(session_date: date, pivot_left: int) -> tuple[datetime, datetime, datetime, datetime]:
    """Return data start, 09:30 NY start, entry cutoff, and expected reset in UTC.

    The strategy is tied to the U.S. cash open rather than a fixed IST hour.
    This avoids taking the setup one hour before the U.S. cash session during
    the U.S. standard-time period. The expected reset is the next weekday's
    09:30 New York opening; market-holiday handling finds the first actual
    available opening bar if that expected day is closed.
    """
    session_start_ny = new_york_open(session_date)
    expected_reset_ny = new_york_open(next_weekday(session_date))
    return (
        (session_start_ny - timedelta(minutes=pivot_left + 1)).astimezone(UTC),
        session_start_ny.astimezone(UTC),
        (session_start_ny + timedelta(minutes=ENTRY_WINDOW_MINUTES)).astimezone(UTC),
        expected_reset_ny.astimezone(UTC),
    )


def is_next_session_reset_bar(timestamp: datetime, session_start: datetime) -> bool:
    """Return whether a bar starts the next actual 09:30 New York session.

    On a U.S. holiday the expected weekday has no 09:30 bar. The next available
    weekday 09:30 bar becomes the reset, matching a real trading-session reset
    rather than flattening at an unrelated subsequent price bar.
    """
    local = timestamp.astimezone(NEW_YORK)
    return (
        timestamp > session_start
        and local.weekday() < 5
        and local.hour == NEW_YORK_OPEN_HOUR
        and local.minute == NEW_YORK_OPEN_MINUTE
    )


def parse_capital_timestamp(value: str) -> datetime:
    """Parse documented and observed Capital.com timestamp formats as UTC."""
    cleaned = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        for pattern in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(cleaned, pattern)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Unsupported Capital.com timestamp: {value!r}")
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def price_value(container: dict, side: str, fallback: float = 0.0) -> float:
    """Read one bid/ask value, falling back safely when ask is omitted."""
    raw = container.get(side)
    if raw is None:
        raw = container.get("bid", fallback)
    return float(raw)


def candle_from_api(bar: dict) -> Candle:
    """Convert a Capital.com /prices response row into a normalized candle."""
    timestamp_value = bar.get("snapshotTimeUTC") or bar.get("snapshotTime")
    if not timestamp_value:
        raise ValueError("Capital.com bar does not contain snapshotTimeUTC")

    open_price = bar.get("openPrice", {})
    high_price = bar.get("highPrice", {})
    low_price = bar.get("lowPrice", {})
    close_price = bar.get("closePrice", {})

    open_bid = price_value(open_price, "bid")
    high_bid = price_value(high_price, "bid")
    low_bid = price_value(low_price, "bid")
    close_bid = price_value(close_price, "bid")

    return Candle(
        timestamp=parse_capital_timestamp(timestamp_value),
        open_bid=open_bid,
        high_bid=high_bid,
        low_bid=low_bid,
        close_bid=close_bid,
        open_ask=price_value(open_price, "ask", open_bid),
        high_ask=price_value(high_price, "ask", high_bid),
        low_ask=price_value(low_price, "ask", low_bid),
        close_ask=price_value(close_price, "ask", close_bid),
    )


def minutes_to_iso(value: datetime) -> str:
    """Format an API timestamp without a timezone suffix, per existing runners."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")


# ── Capital.com data access ──────────────────────────────────────────────────
def fetch_minute_bars(
    api: CapitalComAPI,
    start_utc: datetime,
    end_utc: datetime,
) -> list[Candle]:
    """Fetch a one-minute US500 range without failing on closed-weekend periods.

    Capital.com's /prices endpoint returns HTTP 404, rather than an empty
    ``prices`` array, when a request lies wholly within a market-closed period.
    Fetching in UTC-day chunks lets a Friday replay skip Saturday safely while
    preserving Friday, Sunday reopen, Monday data, and U.S. market holidays.
    A missing chunk is recorded as unavailable data; it must not invalidate
    usable earlier bars from the session being replayed.
    """
    if start_utc > end_utc:
        raise ValueError("Price range start must not be after end")

    bars_by_time: dict[datetime, Candle] = {}
    cursor = start_utc

    while cursor <= end_utc:
        # Capital.com returns 404 for any Saturday-only historical US500 query.
        # Skip to Sunday 00:00 UTC; Sunday is retained because index futures may
        # reopen late on Sunday and can hit an existing Friday stop or target.
        if cursor.weekday() == 5:
            cursor = datetime(
                cursor.year,
                cursor.month,
                cursor.day,
                tzinfo=UTC,
            ) + timedelta(days=1)
            continue

        next_utc_midnight = datetime(
            cursor.year,
            cursor.month,
            cursor.day,
            tzinfo=UTC,
        ) + timedelta(days=1)
        chunk_end = min(
            cursor + timedelta(minutes=REQUEST_CHUNK_MINUTES) - timedelta(seconds=1),
            next_utc_midnight - timedelta(seconds=1),
            end_utc,
        )
        api._ensure_session()
        response = api._session.get(
            f"{api.base_url}/api/v1/prices/{EPIC}",
            params={
                "resolution": "MINUTE",
                "from": minutes_to_iso(cursor),
                "to": minutes_to_iso(chunk_end),
                "max": 1000,
            },
            timeout=20,
        )

        if response.status_code == 404:
            # Closed Saturday and Sunday periods, and U.S. market holidays,
            # legitimately return 404. Advance by a whole queried chunk so a
            # valid session before the closure remains available for replay.
            cursor = chunk_end + timedelta(seconds=1)
            continue
        response.raise_for_status()

        for raw_bar in response.json().get("prices", []):
            candle = candle_from_api(raw_bar)
            bars_by_time[candle.timestamp] = candle

        cursor = chunk_end + timedelta(seconds=1)
        if cursor <= end_utc:
            time.sleep(API_PAUSE_SECONDS)

    return [bars_by_time[key] for key in sorted(bars_by_time)]


def get_deal_size_constraints(api: CapitalComAPI) -> tuple[float, float]:
    """Read the broker's US500 minimum size and size increment with safe fallbacks."""
    details = api.get_market_details(EPIC)
    rules = details.get("dealingRules", {})
    min_size = float(rules.get("minDealSize", {}).get("value") or DEFAULT_MIN_DEAL_SIZE)
    size_increment = float(rules.get("minSizeIncrement", {}).get("value") or DEFAULT_SIZE_INCREMENT)

    if min_size <= 0 or size_increment <= 0:
        raise RuntimeError("Capital.com returned invalid US500 deal-size constraints")
    return min_size, size_increment


# ── Strategy mechanics ───────────────────────────────────────────────────────
def is_pivot_high(bars: list[Candle], pivot_index: int, left: int, right: int) -> bool:
    """Return a conservative confirmed pivot-high test.

    Equal highs are intentionally rejected. This avoids ambiguous, noisy double
    tops and guarantees the detected swing is a unique local high.
    """
    if pivot_index - left < 0 or pivot_index + right >= len(bars):
        return False
    level = bars[pivot_index].high_bid
    return all(
        level > bars[index].high_bid
        for index in range(pivot_index - left, pivot_index + right + 1)
        if index != pivot_index
    )


def is_pivot_low(bars: list[Candle], pivot_index: int, left: int, right: int) -> bool:
    """Return a conservative confirmed pivot-low test with no equal lows."""
    if pivot_index - left < 0 or pivot_index + right >= len(bars):
        return False
    level = bars[pivot_index].low_bid
    return all(
        level < bars[index].low_bid
        for index in range(pivot_index - left, pivot_index + right + 1)
        if index != pivot_index
    )


def select_long_stop(
    entry: float,
    nearest_swing_low: Optional[float],
    first_swing_low: Optional[float],
    breakout_low: float,
) -> tuple[float, str]:
    """Choose BUY stop: latest support, then initial range low, then candle low."""
    if nearest_swing_low is not None and nearest_swing_low < entry:
        return nearest_swing_low, "nearest confirmed swing low"
    if first_swing_low is not None and first_swing_low < entry:
        return first_swing_low, "first identified swing low"
    return breakout_low, "breakout candle low fallback"


def select_short_stop(
    entry: float,
    nearest_swing_high: Optional[float],
    first_swing_high: Optional[float],
    breakout_high: float,
) -> tuple[float, str]:
    """Choose SELL stop: latest resistance, then initial range high, then candle high."""
    if nearest_swing_high is not None and nearest_swing_high > entry:
        return nearest_swing_high, "nearest confirmed swing high"
    if first_swing_high is not None and first_swing_high > entry:
        return first_swing_high, "first identified swing high"
    return breakout_high, "breakout candle high fallback"


def compute_size(
    stop_distance: float,
    risk_aed: float,
    value_per_point_usd: float,
    min_deal_size: float,
    size_increment: float,
) -> tuple[Optional[float], float, Optional[str]]:
    """Size a trade so planned stop loss does not exceed the AED risk budget."""
    if stop_distance <= 0:
        return None, 0.0, "non-positive stop distance"
    if risk_aed <= 0 or value_per_point_usd <= 0:
        return None, 0.0, "risk budget and value per point must be positive"
    if min_deal_size <= 0 or size_increment <= 0:
        return None, 0.0, "invalid Capital.com size constraints"

    risk_budget = Decimal(str(risk_aed))
    cost_per_unit = Decimal(str(stop_distance)) * Decimal(str(value_per_point_usd)) * Decimal(str(USD_TO_AED))
    raw_size = risk_budget / cost_per_unit
    step = Decimal(str(size_increment))
    rounded_size = (raw_size / step).to_integral_value(rounding=ROUND_DOWN) * step
    minimum = Decimal(str(min_deal_size))

    if rounded_size < minimum:
        return None, 0.0, (
            f"minimum size {min_deal_size:g} would exceed planned AED {risk_aed:.2f} risk"
        )

    planned_risk = rounded_size * cost_per_unit
    if planned_risk > risk_budget:
        return None, 0.0, "rounded position size exceeds risk budget"

    return float(rounded_size), float(planned_risk), None


def pnl_aed(direction: str, entry: float, exit_price: float, size: float, value_per_point_usd: float) -> float:
    """Convert point P&L into AED using the explicit, configurable USD/AED assumption."""
    points = exit_price - entry if direction == "BUY" else entry - exit_price
    return points * size * value_per_point_usd * USD_TO_AED


def resolve_trade_exit(
    trade: Trade,
    bar: Candle,
    value_per_point_usd: float,
) -> Optional[tuple[float, str, float]]:
    """Return (exit price, reason, P&L AED) if current minute reaches SL or TP.

    With a minute bar, intrabar order is unknowable when both levels are crossed.
    The simulator resolves that exact case conservatively as SL first.
    """
    if trade.direction == "BUY":
        stop_hit = bar.low_bid <= trade.stop
        target_hit = bar.high_bid >= trade.target
    else:
        stop_hit = bar.high_ask >= trade.stop
        target_hit = bar.low_ask <= trade.target

    if stop_hit:
        return trade.stop, "SL", pnl_aed(
            trade.direction, trade.entry, trade.stop, trade.size, value_per_point_usd
        )
    if target_hit:
        return trade.target, "TP", pnl_aed(
            trade.direction, trade.entry, trade.target, trade.size, value_per_point_usd
        )
    return None


def simulate_session(
    bars: list[Candle],
    session_date: date,
    pivot_left: int,
    pivot_right: int,
    bos_mode: str,
    risk_aed: float,
    value_per_point_usd: float,
    min_deal_size: float,
    size_increment: float,
) -> SimulationResult:
    """Replay one New York-opening session using only completed one-minute candles."""
    if not bars:
        return SimulationResult(session_date, None, None, None, "No US500 1-minute bars returned")

    _, session_start, entry_cutoff, _ = session_bounds_utc(session_date, pivot_left)

    first_swing_high: Optional[float] = None
    first_swing_low: Optional[float] = None
    nearest_swing_high: Optional[float] = None
    nearest_swing_low: Optional[float] = None
    trade: Optional[Trade] = None
    trade_taken = False

    for index, bar in enumerate(bars):
        # Stops and targets apply from the candle after entry. Entry is modelled
        # at the signal candle close, so that candle cannot exit the new trade.
        if trade is not None and trade.exit_time is None and bar.timestamp > trade.entry_time:
            resolved = resolve_trade_exit(trade, bar, value_per_point_usd)
            if resolved is not None:
                exit_price, reason, trade_pnl = resolved
                trade.exit_time = bar.timestamp
                trade.exit_price = exit_price
                trade.exit_reason = reason
                trade.pnl_aed = trade_pnl

        # Flatten at the next actual 09:30 New York session reset if position is
        # still alive. ``next_reset`` is only the expected weekday boundary;
        # a U.S. market holiday may mean the first eligible bar is later.
        if trade is not None and trade.exit_time is None and is_next_session_reset_bar(
            bar.timestamp, session_start
        ):
            exit_price = bar.close_bid if trade.direction == "BUY" else bar.close_ask
            trade.exit_time = bar.timestamp
            trade.exit_price = exit_price
            trade.exit_reason = "NEXT_SESSION_FLATTEN"
            trade.pnl_aed = pnl_aed(
                trade.direction, trade.entry, exit_price, trade.size, value_per_point_usd
            )
            break

        # Pine's pivots become available only after pivot_right following bars.
        pivot_index = index - pivot_right
        if pivot_index >= 0:
            pivot_bar = bars[pivot_index]
            confirmation_is_in_window = session_start <= bar.timestamp < entry_cutoff
            pivot_is_after_open = pivot_bar.timestamp >= session_start

            if confirmation_is_in_window and pivot_is_after_open:
                if is_pivot_high(bars, pivot_index, pivot_left, pivot_right):
                    nearest_swing_high = pivot_bar.high_bid
                    if first_swing_high is None:
                        first_swing_high = pivot_bar.high_bid

                if is_pivot_low(bars, pivot_index, pivot_left, pivot_right):
                    nearest_swing_low = pivot_bar.low_bid
                    if first_swing_low is None:
                        first_swing_low = pivot_bar.low_bid

        # No new signal after the three-hour entry window or after one daily trade.
        if not (session_start <= bar.timestamp < entry_cutoff):
            continue
        if trade_taken or first_swing_high is None or first_swing_low is None or index == 0:
            continue

        previous = bars[index - 1]
        if bos_mode == "Close":
            breaks_high = bar.close_bid > first_swing_high and previous.close_bid <= first_swing_high
            breaks_low = bar.close_bid < first_swing_low and previous.close_bid >= first_swing_low
        else:
            breaks_high = bar.high_bid > first_swing_high and previous.high_bid <= first_swing_high
            breaks_low = bar.low_bid < first_swing_low and previous.low_bid >= first_swing_low

        # A bar breaking both range boundaries is intentionally ignored.
        if breaks_high == breaks_low:
            continue

        if breaks_high:
            direction = "BUY"
            entry = bar.close_ask
            stop, stop_source = select_long_stop(
                entry, nearest_swing_low, first_swing_low, bar.low_bid
            )
            if stop >= entry:
                continue
            target = entry + REWARD_MULTIPLE * (entry - stop)
        else:
            direction = "SELL"
            entry = bar.close_bid
            stop, stop_source = select_short_stop(
                entry, nearest_swing_high, first_swing_high, bar.high_ask
            )
            if stop <= entry:
                continue
            target = entry - REWARD_MULTIPLE * (stop - entry)

        size, planned_risk, sizing_error = compute_size(
            abs(entry - stop), risk_aed, value_per_point_usd, min_deal_size, size_increment
        )
        if size is None:
            return SimulationResult(
                session_date,
                first_swing_high,
                first_swing_low,
                None,
                f"Valid {direction} BOS found, but not traded: {sizing_error}",
            )

        trade = Trade(
            direction=direction,
            entry_time=bar.timestamp,
            entry=entry,
            stop=stop,
            target=target,
            size=size,
            planned_risk_aed=planned_risk,
            stop_source=stop_source,
        )
        trade_taken = True

    if trade is None:
        if first_swing_high is None or first_swing_low is None:
            note = "No complete confirmed first-swing range before the three-hour entry cutoff"
        else:
            note = "No valid BOS before the three-hour entry cutoff"
    elif trade.exit_time is None:
        note = "Trade remains open after replay data ended"
    else:
        note = f"{trade.exit_reason} exit"

    return SimulationResult(session_date, first_swing_high, first_swing_low, trade, note)


# ── Reporting ────────────────────────────────────────────────────────────────
def format_ist(timestamp: Optional[datetime]) -> str:
    """Human-readable IST timestamp for output."""
    if timestamp is None:
        return "—"
    return timestamp.astimezone(IST).strftime("%Y-%m-%d %H:%M IST")


def print_result(result: SimulationResult, balance: float) -> float:
    """Print one session's replay result and return revised notional balance."""
    print("\n" + "=" * 78)
    print(f"SESSION {result.session_date.isoformat()} | Balance before: AED {balance:,.2f}")
    print("=" * 78)
    high_text = "—" if result.first_swing_high is None else f"{result.first_swing_high:.2f}"
    low_text = "—" if result.first_swing_low is None else f"{result.first_swing_low:.2f}"
    print(f"First swing range: High={high_text} | Low={low_text}")

    if result.trade is None:
        print(f"[NO TRADE] {result.note}")
        return balance

    trade = result.trade
    print(
        f"[{trade.direction}] Entry={trade.entry:.2f} @ {format_ist(trade.entry_time)} | "
        f"SL={trade.stop:.2f} ({trade.stop_source}) | TP={trade.target:.2f} | "
        f"Size={trade.size:g} | Planned risk=AED {trade.planned_risk_aed:.2f}"
    )

    if trade.exit_time is None or trade.pnl_aed is None:
        print(f"[OPEN] {result.note}")
        return balance

    updated_balance = balance + trade.pnl_aed
    print(
        f"[{trade.exit_reason}] Exit={trade.exit_price:.2f} @ {format_ist(trade.exit_time)} | "
        f"P&L=AED {trade.pnl_aed:+.2f} | Balance after: AED {updated_balance:,.2f}"
    )
    return updated_balance


# ── Argument parsing ─────────────────────────────────────────────────────────
def collect_dates(args: argparse.Namespace) -> list[date]:
    """Build a unique, sorted list of requested weekday IST session dates."""
    selected: set[date] = set()
    today_ist = datetime.now(IST).date()

    for value in args.date:
        selected.add(datetime.strptime(value, "%Y-%m-%d").date())

    for value in args.month:
        try:
            year, month = (int(part) for part in value.split("-", maxsplit=1))
            _, last_day = calendar.monthrange(year, month)
        except ValueError as error:
            raise argparse.ArgumentTypeError(f"Invalid month {value!r}; expected YYYY-MM") from error
        for day in range(1, last_day + 1):
            candidate = date(year, month, day)
            if candidate.weekday() < 5 and candidate <= today_ist:
                selected.add(candidate)

    for value in args.year:
        try:
            year = int(value)
        except ValueError as error:
            raise argparse.ArgumentTypeError(f"Invalid year {value!r}; expected YYYY") from error
        for month in range(1, 13):
            _, last_day = calendar.monthrange(year, month)
            for day in range(1, last_day + 1):
                candidate = date(year, month, day)
                if candidate.weekday() < 5 and candidate <= today_ist:
                    selected.add(candidate)

    return sorted(selected)


def build_parser() -> argparse.ArgumentParser:
    """Build command-line interface."""
    parser = argparse.ArgumentParser(
        description="Replay the US500 09:30 New York first-swing BOS strategy on Capital.com 1-minute data."
    )
    parser.add_argument("--date", action="append", default=[], help="New York session date YYYY-MM-DD. Repeatable.")
    parser.add_argument("--month", action="append", default=[], help="Whole New York session month YYYY-MM. Repeatable.")
    parser.add_argument("--year", action="append", default=[], help="Whole New York session year YYYY. Repeatable.")
    parser.add_argument("--pivot-left", type=int, default=5, choices=range(1, 21), metavar="1-20")
    parser.add_argument("--pivot-right", type=int, default=5, choices=range(1, 21), metavar="1-20")
    parser.add_argument("--bos-mode", choices=("Close", "Wick"), default="Close")
    parser.add_argument("--risk-aed", type=float, default=DEFAULT_RISK_AED, help="Maximum planned loss per trade in AED.")
    parser.add_argument(
        "--value-per-point-usd",
        type=float,
        default=DEFAULT_VALUE_PER_POINT_USD,
        help="USD P&L per US500 point per 1.0 deal size; default 1.0.",
    )
    parser.add_argument(
        "--starting-capital-aed",
        type=float,
        default=DEFAULT_STARTING_CAPITAL_AED,
        help="Notional starting balance used in printed results.",
    )
    parser.add_argument("--self-test", action="store_true", help="Run built-in offline tests; no API calls or credentials.")
    return parser


# ── Offline unit tests ───────────────────────────────────────────────────────
def synthetic_candle(timestamp: datetime, high: float, low: float, close: float, spread: float = 0.2) -> Candle:
    """Build deterministic test data with valid bid/ask fields."""
    open_bid = close
    return Candle(
        timestamp=timestamp,
        open_bid=open_bid,
        high_bid=high,
        low_bid=low,
        close_bid=close,
        open_ask=open_bid + spread,
        high_ask=high + spread,
        low_ask=low + spread,
        close_ask=close + spread,
    )


def run_self_test() -> None:
    """Test sizing, SL priority, pivot detection, one-trade, and Friday handling."""
    size, planned_risk, error = compute_size(10.0, 40.0, 1.0, 0.1, 0.1)
    assert error is None and size is not None and planned_risk <= 40.0
    assert select_long_stop(110.0, 104.0, 98.0, 105.0) == (104.0, "nearest confirmed swing low")
    assert select_short_stop(90.0, 96.0, 102.0, 94.0) == (96.0, "nearest confirmed swing high")

    # Friday positions must reset at Monday 09:30 New York, never Saturday.
    _, _, _, summer_friday_reset = session_bounds_utc(date(2026, 8, 14), pivot_left=5)
    assert summer_friday_reset.astimezone(IST) == datetime(2026, 8, 17, 19, 0, tzinfo=IST)

    # The same cash open shifts by one IST hour when New York leaves daylight time.
    _, winter_start, winter_cutoff, winter_friday_reset = session_bounds_utc(date(2026, 1, 9), pivot_left=5)
    assert winter_start.astimezone(IST) == datetime(2026, 1, 9, 20, 0, tzinfo=IST)
    assert winter_cutoff.astimezone(IST) == datetime(2026, 1, 9, 23, 0, tzinfo=IST)
    assert winter_friday_reset.astimezone(IST) == datetime(2026, 1, 12, 20, 0, tzinfo=IST)

    test_date = date(2026, 4, 10)
    start_ist = datetime(2026, 4, 10, 18, 58, tzinfo=IST)
    values = [
        (101, 99, 100), (102, 100, 101), (103, 101, 102), (104, 102, 103),
        (105, 103, 104), (104, 102, 102), (103, 101, 101), (102, 100, 100),
        (101, 99, 99), (100, 98, 98), (99, 97, 98), (100, 98, 99),
        (101, 99, 100), (107, 100, 106), (125, 104, 124), (90, 80, 85),
    ]
    bars = [
        synthetic_candle((start_ist + timedelta(minutes=index)).astimezone(UTC), high, low, close)
        for index, (high, low, close) in enumerate(values)
    ]

    result = simulate_session(
        bars=bars,
        session_date=test_date,
        pivot_left=2,
        pivot_right=2,
        bos_mode="Close",
        risk_aed=40.0,
        value_per_point_usd=1.0,
        min_deal_size=0.1,
        size_increment=0.1,
    )
    assert result.first_swing_high == 105
    assert result.first_swing_low == 97
    assert result.trade is not None
    assert result.trade.direction == "BUY"
    assert result.trade.stop == 97
    assert result.trade.stop_source == "nearest confirmed swing low"
    assert result.trade.exit_reason == "TP"
    print(
        "Self-test passed: pivot range, nearest-structure SL, 1:2 TP, "
        "one-trade guard, Friday reset, and New York DST timing verified."
    )


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.self_test:
        run_self_test()
        return

    if not args.date and not args.month and not args.year:
        parser.error("Provide at least one --date, --month, or --year (or use --self-test).")
    if args.risk_aed <= 0 or args.value_per_point_usd <= 0 or args.starting_capital_aed <= 0:
        parser.error("Risk, value per point, and starting capital must all be positive.")

    try:
        replay_dates = collect_dates(args)
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))
    if not replay_dates:
        parser.error("No eligible dates were selected.")

    api_key = os.getenv("CAPITAL_API_KEY", "")
    identifier = os.getenv("CAPITAL_IDENTIFIER", "")
    password = os.getenv("CAPITAL_PASSWORD", "")
    demo_mode = os.getenv("CAPITAL_DEMO", "true").lower() == "true"
    if not all((api_key, identifier, password)):
        raise SystemExit(
            "Missing Capital.com credentials. Set CAPITAL_API_KEY, CAPITAL_IDENTIFIER, and "
            "CAPITAL_PASSWORD in US500/.env or the environment. This runner only reads data."
        )

    api = CapitalComAPI(
        api_key=api_key,
        identifier=identifier,
        password=password,
        demo=demo_mode,
    )
    if not api.create_session():
        raise SystemExit("Could not create Capital.com session for historical read-only replay.")

    min_deal_size, size_increment = get_deal_size_constraints(api)
    balance = args.starting_capital_aed

    print("\nUS500 09:30 New York First-Swing BOS — Capital.com Dry-Run Backtester")
    print("Orders: DISABLED (historical data only)")
    print(f"Mode: {'DEMO' if demo_mode else 'LIVE DATA / NO ORDERS'}")
    print("Entry window: 09:30–12:30 New York | 19:00–22:00 IST in EDT; 20:00–23:00 IST in EST")
    print(f"One trade per session | BOS: {args.bos_mode}")
    print(f"Pivot strength: {args.pivot_left}/{args.pivot_right} | TP: 1:2 | Risk cap: AED {args.risk_aed:.2f}")
    print(f"US500 size rules: min={min_deal_size:g}, increment={size_increment:g}")
    print(f"USD/AED conversion assumption: {USD_TO_AED:.4f}\n")

    executed_trades: list[Trade] = []
    for replay_date in replay_dates:
        start_utc, _, _, end_utc = session_bounds_utc(replay_date, args.pivot_left)
        try:
            bars = fetch_minute_bars(api, start_utc, end_utc)
        except Exception as error:
            print(f"\n[ERROR] {replay_date}: unable to load Capital.com minute bars: {error}")
            continue

        result = simulate_session(
            bars=bars,
            session_date=replay_date,
            pivot_left=args.pivot_left,
            pivot_right=args.pivot_right,
            bos_mode=args.bos_mode,
            risk_aed=args.risk_aed,
            value_per_point_usd=args.value_per_point_usd,
            min_deal_size=min_deal_size,
            size_increment=size_increment,
        )
        balance = print_result(result, balance)
        if result.trade is not None:
            executed_trades.append(result.trade)
        time.sleep(API_PAUSE_SECONDS)

    closed_trades = [trade for trade in executed_trades if trade.pnl_aed is not None]
    total_pnl = sum(trade.pnl_aed or 0.0 for trade in closed_trades)
    wins = sum(1 for trade in closed_trades if (trade.pnl_aed or 0.0) > 0)
    losses = sum(1 for trade in closed_trades if (trade.pnl_aed or 0.0) < 0)

    print("\n" + "=" * 78)
    print("BACKTEST SUMMARY")
    print("=" * 78)
    print(f"Sessions requested: {len(replay_dates)}")
    print(f"Trades entered:     {len(executed_trades)}")
    print(f"Trades closed:      {len(closed_trades)} | Wins: {wins} | Losses: {losses}")
    if closed_trades:
        print(f"Win rate:           {wins / len(closed_trades) * 100:.1f}%")
    print(f"Starting capital:   AED {args.starting_capital_aed:,.2f}")
    print(f"Final capital:      AED {balance:,.2f}")
    print(f"Net P&L:            AED {total_pnl:+,.2f}")
    print("=" * 78)


if __name__ == "__main__":
    main()
