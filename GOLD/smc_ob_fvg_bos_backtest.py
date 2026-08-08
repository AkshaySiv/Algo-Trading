#!/usr/bin/env python3
"""Historical-only XAUUSD SMC backtester using Capital.com OHLC data.

This program does not create, modify, or close positions. It only calls:
  GET /api/v1/markets       (when --verify-epic is used)
  GET /api/v1/prices/{epic} (when --download is used)

Strategy (all rules are deterministic):
  1. Find a fresh 1H or 4H order block created by a candle-close break of
     confirmed higher-timeframe structure.
  2. During 05:30–11:30 IST, wait for price to tap that zone. By default,
     the tap must also sweep the prior 12 five-minute lows/highs.
  3. After the tap, require a five-minute BOS candle close and a three-candle
     FVG in the same direction. The last opposing candle before that BOS is
     the local order block.
  4. Place a simulated limit order at that local block's midpoint. The stop is
     beyond the Asian liquidity-sweep extreme; the target is a configurable R
     multiple. One trade per session by default.

It is deliberately conservative:
  * Entries cannot fill on the BOS candle itself (no intrabar look-ahead).
  * When both stop and target trade inside the same OHLC bar, STOP is assumed
    first by default.
  * A fixed configurable spread is deducted from each trade's R result.

The output reports historical performance only. It does not demonstrate a
future edge and should not be connected to a live trading workflow.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python < 3.9 is not supported by this project.
    ZoneInfo = None  # type: ignore

IST = timezone(timedelta(hours=5, minutes=30), "Asia/Kolkata")
UTC = timezone.utc
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = SCRIPT_DIR / "smc_backtest_config.example.json"


@dataclass(frozen=True)
class Bar:
    timestamp: datetime  # UTC, start time of the bar
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class Pivot:
    index: int
    timestamp: datetime
    price: float
    kind: str  # "high" or "low"


@dataclass(frozen=True)
class OrderBlock:
    direction: str  # "long" / "short"
    low: float
    high: float
    created_at: datetime
    source_timeframe: str
    source_index: int


@dataclass
class PendingOrder:
    direction: str
    created_at: datetime
    entry: float
    stop: float
    target: float
    source_ob_low: float
    source_ob_high: float
    htf: str
    fvg_size: float
    expiry: datetime


@dataclass
class OpenTrade:
    direction: str
    entry_time: datetime
    entry: float
    stop: float
    target: float
    htf: str
    fvg_size: float
    source_ob_low: float
    source_ob_high: float


@dataclass
class Trade:
    session_date_ist: str
    direction: str
    htf: str
    entry_time_utc: str
    exit_time_utc: str
    entry: float
    stop: float
    target: float
    exit: float
    result: str
    risk_points: float
    gross_r: float
    net_r: float
    fvg_size: float
    source_ob_low: float
    source_ob_high: float


class BacktestError(RuntimeError):
    """Raised for unusable data or invalid strategy parameters."""


def parse_iso_datetime(value: str) -> datetime:
    """Parse common Capital.com and normalized ISO timestamps as UTC."""
    value = value.strip().replace("Z", "+00:00")
    for parser in (
        lambda: datetime.fromisoformat(value),
        lambda: datetime.strptime(value, "%Y-%m-%dT%H:%M:%S"),
        lambda: datetime.strptime(value, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            parsed = parser()
            return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)
        except ValueError:
            pass
    raise BacktestError(f"Unrecognised timestamp: {value!r}")


def load_config(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        config = json.load(handle)
    required = {
        "epic", "data_resolution", "context_timeframes", "asia_session_start_ist",
        "asia_session_end_ist", "pivot_left_bars", "pivot_right_bars",
        "order_block_lookback_bars", "order_block_max_age_hours",
        "require_fresh_order_block", "require_asian_liquidity_sweep",
        "sweep_lookback_bars", "require_fvg", "fvg_min_size_points",
        "fvg_max_age_bars", "entry_mode", "stop_buffer_points",
        "risk_reward_ratio", "spread_points", "max_trades_per_session",
        "force_exit_at_session_end", "same_bar_exit_policy", "warmup_days",
        "out_of_sample_fraction",
    }
    missing = sorted(required.difference(config))
    if missing:
        raise BacktestError(f"Config is missing required keys: {', '.join(missing)}")
    if config["data_resolution"] != "MINUTE_5":
        raise BacktestError("This implementation requires MINUTE_5 execution data.")
    if config["entry_mode"] not in {"midpoint", "proximal"}:
        raise BacktestError("entry_mode must be 'midpoint' or 'proximal'.")
    if config["same_bar_exit_policy"] not in {"stop_first", "target_first"}:
        raise BacktestError("same_bar_exit_policy must be 'stop_first' or 'target_first'.")
    if not 0 <= float(config["out_of_sample_fraction"]) < 1:
        raise BacktestError("out_of_sample_fraction must be >= 0 and < 1.")
    return config


def capital_bar_to_mid(raw: dict) -> Bar:
    """Turn a Capital.com price payload into a mid-price OHLC bar.

    Capital.com returns bid/ask OHLC. The signal is based on the midpoint when
    both prices are available. Transaction cost is separately deducted through
    config['spread_points'], keeping fills transparent rather than pretending a
    midpoint fill is executable.
    """
    def midpoint(price: dict) -> float:
        bid = price.get("bid")
        ask = price.get("ask")
        if bid is None and ask is None:
            raise BacktestError(f"Capital.com price field has no bid or ask: {price}")
        if bid is None:
            return float(ask)
        if ask is None:
            return float(bid)
        return (float(bid) + float(ask)) / 2.0

    timestamp = raw.get("snapshotTimeUTC") or raw.get("snapshotTime")
    if not timestamp:
        raise BacktestError(f"Capital.com bar has no timestamp: {raw}")
    return Bar(
        timestamp=parse_iso_datetime(timestamp),
        open=midpoint(raw["openPrice"]),
        high=midpoint(raw["highPrice"]),
        low=midpoint(raw["lowPrice"]),
        close=midpoint(raw["closePrice"]),
    )


def read_normalized_csv(path: Path) -> List[Bar]:
    """Read the normalized CSV exported by this program.

    Required fields: timestamp_utc, open, high, low, close.
    """
    bars: List[Bar] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fields = set(reader.fieldnames or [])
        required = {"timestamp_utc", "open", "high", "low", "close"}
        if not required.issubset(fields):
            missing = ", ".join(sorted(required.difference(fields)))
            raise BacktestError(f"CSV is not normalized; missing fields: {missing}")
        for row in reader:
            bars.append(Bar(
                timestamp=parse_iso_datetime(row["timestamp_utc"]),
                open=float(row["open"]), high=float(row["high"]),
                low=float(row["low"]), close=float(row["close"]),
            ))
    return normalise_bars(bars)


def write_normalized_csv(path: Path, bars: Sequence[Bar]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp_utc", "open", "high", "low", "close"])
        writer.writeheader()
        for bar in bars:
            writer.writerow({
                "timestamp_utc": bar.timestamp.isoformat(),
                "open": f"{bar.open:.6f}",
                "high": f"{bar.high:.6f}",
                "low": f"{bar.low:.6f}",
                "close": f"{bar.close:.6f}",
            })


def normalise_bars(bars: Iterable[Bar]) -> List[Bar]:
    """Sort, deduplicate, and reject logically impossible OHLC bars."""
    by_timestamp: Dict[datetime, Bar] = {}
    for bar in bars:
        if bar.high < max(bar.open, bar.close) or bar.low > min(bar.open, bar.close) or bar.high < bar.low:
            raise BacktestError(f"Invalid OHLC bar at {bar.timestamp.isoformat()}")
        by_timestamp[bar.timestamp.astimezone(UTC)] = bar
    ordered = [by_timestamp[key] for key in sorted(by_timestamp)]
    if len(ordered) < 100:
        raise BacktestError("At least 100 five-minute bars are required to run this backtest.")
    return ordered


def day_chunks(start: datetime, end: datetime) -> Iterable[Tuple[datetime, datetime]]:
    """Yield one-day UTC chunks; 288 five-minute bars fit below API max=1000."""
    cursor = start
    while cursor < end:
        next_cursor = min(cursor + timedelta(days=1), end)
        yield cursor, next_cursor
        cursor = next_cursor


def download_capital_bars(config: dict, start: datetime, end: datetime, *, demo: bool, verify_epic: bool) -> List[Bar]:
    """Download 5-minute Capital.com history using local .env credentials.

    The function calls only market-data endpoints. It intentionally contains no
    order or position functionality.
    """
    try:
        from dotenv import load_dotenv
        from capitalcom_api import CapitalComAPI
    except ImportError as error:
        raise BacktestError(f"Missing required local package/module: {error}") from error

    load_dotenv(SCRIPT_DIR / ".env", override=False)
    api_key = os.getenv("CAPITAL_API_KEY", "")
    identifier = os.getenv("CAPITAL_IDENTIFIER", "")
    password = os.getenv("CAPITAL_PASSWORD", "")
    if not all((api_key, identifier, password)):
        raise BacktestError(
            "Missing Capital.com credentials in GOLD/.env. Set CAPITAL_API_KEY, "
            "CAPITAL_IDENTIFIER, and CAPITAL_PASSWORD, or use --csv."
        )

    api = CapitalComAPI(api_key=api_key, identifier=identifier, password=password, demo=demo)
    if not api.create_session():
        raise BacktestError("Capital.com session creation failed. Check demo/live setting and credentials.")

    try:
        if verify_epic:
            markets = api.search_markets("gold")
            known_epics = {str(item.get("epic", "")).upper() for item in markets}
            if str(config["epic"]).upper() not in known_epics:
                print(
                    "WARNING: configured epic {!r} was not returned by a gold market search. "
                    "Continuing because account instrument names can differ. Available: {}".format(
                        config["epic"], ", ".join(sorted(known_epics)) or "none"
                    ),
                    file=sys.stderr,
                )

        all_bars: List[Bar] = []
        for chunk_start, chunk_end in day_chunks(start, end):
            params = {
                "resolution": config["data_resolution"],
                "from": chunk_start.strftime("%Y-%m-%dT%H:%M:%S"),
                "to": (chunk_end - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S"),
                "max": 1000,
            }
            api._ensure_session()
            response = api._session.get(
                f"{api.base_url}/api/v1/prices/{config['epic']}", params=params, timeout=20
            )
            response.raise_for_status()
            payload = response.json().get("prices", [])
            all_bars.extend(capital_bar_to_mid(raw) for raw in payload)
            time.sleep(0.12)  # stay well inside the documented rate limit
        return normalise_bars(all_bars)
    finally:
        try:
            api.delete_session()
        except Exception:
            pass


def resample_bars(bars: Sequence[Bar], minutes: int) -> List[Bar]:
    """Resample UTC 5m bars and timestamp each aggregate at its close time.

    Marking bars by close time prevents using an unfinished 1H/4H candle when
    detecting higher-timeframe structure.
    """
    if minutes % 5:
        raise ValueError("Resampling interval must be divisible by five minutes.")
    buckets: Dict[datetime, List[Bar]] = defaultdict(list)
    for bar in bars:
        minute = (bar.timestamp.minute // minutes) * minutes
        bucket_start = bar.timestamp.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=minute)
        buckets[bucket_start].append(bar)

    result: List[Bar] = []
    expected = minutes // 5
    for bucket_start in sorted(buckets):
        values = sorted(buckets[bucket_start], key=lambda item: item.timestamp)
        # Skipping incomplete buckets avoids manufacturing an HTF close.
        if len(values) != expected:
            continue
        result.append(Bar(
            timestamp=bucket_start + timedelta(minutes=minutes),
            open=values[0].open,
            high=max(item.high for item in values),
            low=min(item.low for item in values),
            close=values[-1].close,
        ))
    return result


def confirmed_pivots(bars: Sequence[Bar], left: int, right: int) -> List[Pivot]:
    """Return pivots at the point they become confirmed.

    The pivot's index is the original swing index. It is only eligible for a
    signal after the right-hand confirmation bars have closed.
    """
    pivots: List[Pivot] = []
    for index in range(left, len(bars) - right):
        centre = bars[index]
        before = bars[index - left:index]
        after = bars[index + 1:index + 1 + right]
        if centre.high > max(item.high for item in before) and centre.high >= max(item.high for item in after):
            pivots.append(Pivot(index=index, timestamp=centre.timestamp, price=centre.high, kind="high"))
        if centre.low < min(item.low for item in before) and centre.low <= min(item.low for item in after):
            pivots.append(Pivot(index=index, timestamp=centre.timestamp, price=centre.low, kind="low"))
    return pivots


def last_opposing_candle(bars: Sequence[Bar], end_index: int, direction: str, lookback: int) -> Optional[Bar]:
    """Find the last opposing candle before a confirmed displacement candle."""
    start_index = max(0, end_index - lookback)
    for index in range(end_index - 1, start_index - 1, -1):
        bar = bars[index]
        if direction == "long" and bar.close < bar.open:
            return bar
        if direction == "short" and bar.close > bar.open:
            return bar
    return None


def build_htf_order_blocks(bars: Sequence[Bar], timeframe: str, config: dict) -> List[OrderBlock]:
    """Build HTF OBs from a confirmed pivot and later candle-close BOS.

    Bullish: close > latest confirmed pivot high, then zone = last bearish candle.
    Bearish: close < latest confirmed pivot low, then zone = last bullish candle.
    """
    minutes = {"HOUR": 60, "HOUR_4": 240}.get(timeframe)
    if minutes is None:
        raise BacktestError(f"Unsupported context timeframe: {timeframe}")
    htf = resample_bars(bars, minutes)
    left, right = int(config["pivot_left_bars"]), int(config["pivot_right_bars"])
    pivots = confirmed_pivots(htf, left, right)
    pivots_by_confirmation: Dict[int, List[Pivot]] = defaultdict(list)
    for pivot in pivots:
        pivots_by_confirmation[pivot.index + right].append(pivot)

    latest_high: Optional[Pivot] = None
    latest_low: Optional[Pivot] = None
    blocks: List[OrderBlock] = []
    last_bull_break_pivot: Optional[int] = None
    last_bear_break_pivot: Optional[int] = None

    for index, bar in enumerate(htf):
        for pivot in pivots_by_confirmation.get(index, []):
            if pivot.kind == "high":
                latest_high = pivot
            else:
                latest_low = pivot

        if latest_high and bar.close > latest_high.price and last_bull_break_pivot != latest_high.index:
            ob_candle = last_opposing_candle(htf, index, "long", int(config["order_block_lookback_bars"]))
            if ob_candle:
                blocks.append(OrderBlock(
                    direction="long", low=ob_candle.low, high=ob_candle.high,
                    created_at=bar.timestamp, source_timeframe=timeframe, source_index=index,
                ))
                last_bull_break_pivot = latest_high.index

        if latest_low and bar.close < latest_low.price and last_bear_break_pivot != latest_low.index:
            ob_candle = last_opposing_candle(htf, index, "short", int(config["order_block_lookback_bars"]))
            if ob_candle:
                blocks.append(OrderBlock(
                    direction="short", low=ob_candle.low, high=ob_candle.high,
                    created_at=bar.timestamp, source_timeframe=timeframe, source_index=index,
                ))
                last_bear_break_pivot = latest_low.index

    return sorted(blocks, key=lambda block: (block.created_at, block.source_timeframe))


def bar_taps_zone(bar: Bar, block: OrderBlock) -> bool:
    return bar.low <= block.high and bar.high >= block.low


def is_block_fresh(block: OrderBlock, bars: Sequence[Bar], candidate_index: int) -> bool:
    """A block is fresh only when no completed five-minute bar touched it earlier."""
    for earlier in bars[:candidate_index]:
        if earlier.timestamp <= block.created_at:
            continue
        if bar_taps_zone(earlier, block):
            return False
    return True


def eligible_blocks(now: datetime, bars: Sequence[Bar], index: int, blocks: Sequence[OrderBlock], config: dict) -> List[OrderBlock]:
    max_age = timedelta(hours=float(config["order_block_max_age_hours"]))
    candidates = [
        block for block in blocks
        if block.created_at <= now and now - block.created_at <= max_age
        and (not config["require_fresh_order_block"] or is_block_fresh(block, bars, index))
    ]
    # H4 precedes H1 only when otherwise equal; smaller zones are more precise.
    return sorted(candidates, key=lambda block: (0 if block.source_timeframe == "HOUR_4" else 1, block.high - block.low))


def liquidity_sweep(bars: Sequence[Bar], index: int, direction: str, lookback: int) -> bool:
    """Detect a current-bar sweep of the preceding local five-minute extremes."""
    if index < lookback:
        return False
    history = bars[index - lookback:index]
    current = bars[index]
    if direction == "long":
        return current.low < min(item.low for item in history)
    return current.high > max(item.high for item in history)


def pivots_available_at(bars: Sequence[Bar], index: int, left: int, right: int) -> Tuple[Optional[Pivot], Optional[Pivot]]:
    """Return the latest high/low pivot known at the current bar close."""
    pivots = confirmed_pivots(bars[:index + 1], left, right)
    highs = [pivot for pivot in pivots if pivot.kind == "high" and pivot.index + right <= index]
    lows = [pivot for pivot in pivots if pivot.kind == "low" and pivot.index + right <= index]
    return (highs[-1] if highs else None, lows[-1] if lows else None)


def fair_value_gap(bars: Sequence[Bar], index: int, direction: str, min_size: float) -> Optional[float]:
    """Return a three-candle FVG size on the current displacement candle.

    Bullish FVG: current low > high two bars ago.
    Bearish FVG: current high < low two bars ago.
    """
    if index < 2:
        return None
    if direction == "long":
        gap = bars[index].low - bars[index - 2].high
    else:
        gap = bars[index - 2].low - bars[index].high
    return gap if gap >= min_size else None


def build_pending_order(
    bars: Sequence[Bar], index: int, direction: str, htf_block: OrderBlock,
    sweep_extreme: float, session_end: datetime, config: dict,
) -> Optional[PendingOrder]:
    local_ob = last_opposing_candle(
        bars, index, direction, int(config["order_block_lookback_bars"])
    )
    if local_ob is None:
        return None
    fvg_size = fair_value_gap(bars, index, direction, float(config["fvg_min_size_points"]))
    if config["require_fvg"] and fvg_size is None:
        return None
    if fvg_size is None:
        fvg_size = 0.0

    if direction == "long":
        entry = (local_ob.low + local_ob.high) / 2.0 if config["entry_mode"] == "midpoint" else local_ob.high
        stop = sweep_extreme - float(config["stop_buffer_points"])
        risk = entry - stop
        target = entry + float(config["risk_reward_ratio"]) * risk
    else:
        entry = (local_ob.low + local_ob.high) / 2.0 if config["entry_mode"] == "midpoint" else local_ob.low
        stop = sweep_extreme + float(config["stop_buffer_points"])
        risk = stop - entry
        target = entry - float(config["risk_reward_ratio"]) * risk
    if risk <= 0:
        return None

    return PendingOrder(
        direction=direction, created_at=bars[index].timestamp, entry=entry, stop=stop,
        target=target, source_ob_low=local_ob.low, source_ob_high=local_ob.high,
        htf=htf_block.source_timeframe, fvg_size=fvg_size, expiry=session_end,
    )


def order_fills(order: PendingOrder, bar: Bar) -> bool:
    return bar.low <= order.entry <= bar.high


def resolve_exit(trade: OpenTrade, bar: Bar, config: dict) -> Optional[Tuple[float, str]]:
    """Return the exit price/reason if stop or target is reached in this bar."""
    if trade.direction == "long":
        stop_hit = bar.low <= trade.stop
        target_hit = bar.high >= trade.target
    else:
        stop_hit = bar.high >= trade.stop
        target_hit = bar.low <= trade.target
    if not stop_hit and not target_hit:
        return None
    if stop_hit and target_hit:
        if config["same_bar_exit_policy"] == "stop_first":
            return trade.stop, "stop_same_bar"
        return trade.target, "target_same_bar"
    if stop_hit:
        return trade.stop, "stop"
    return trade.target, "target"


def session_bounds(day_ist: date, config: dict) -> Tuple[datetime, datetime]:
    start_hour, start_minute = (int(part) for part in config["asia_session_start_ist"].split(":"))
    end_hour, end_minute = (int(part) for part in config["asia_session_end_ist"].split(":"))
    start = datetime.combine(day_ist, dt_time(start_hour, start_minute), tzinfo=IST).astimezone(UTC)
    end = datetime.combine(day_ist, dt_time(end_hour, end_minute), tzinfo=IST).astimezone(UTC)
    if end <= start:
        end += timedelta(days=1)
    return start, end


def unique_ist_dates(bars: Sequence[Bar], config: dict) -> List[date]:
    dates = {bar.timestamp.astimezone(IST).date() for bar in bars}
    return sorted(day for day in dates if day.weekday() < 5)


def simulate_session(day_ist: date, bars: Sequence[Bar], htf_blocks: Sequence[OrderBlock], config: dict) -> List[Trade]:
    """Replay one IST Asian session without using future price information."""
    session_start, session_end = session_bounds(day_ist, config)
    indices = [index for index, bar in enumerate(bars) if session_start <= bar.timestamp < session_end]
    if not indices:
        return []

    left, right = int(config["pivot_left_bars"]), int(config["pivot_right_bars"])
    trades: List[Trade] = []
    active_tap: Optional[Tuple[str, OrderBlock, float, int]] = None
    pending: Optional[PendingOrder] = None
    position: Optional[OpenTrade] = None
    last_index = indices[-1]

    for index in indices:
        bar = bars[index]

        if position is not None:
            outcome = resolve_exit(position, bar, config)
            if outcome:
                exit_price, result = outcome
                risk = abs(position.entry - position.stop)
                direction_multiplier = 1.0 if position.direction == "long" else -1.0
                gross_r = direction_multiplier * (exit_price - position.entry) / risk
                net_r = gross_r - float(config["spread_points"]) / risk
                trades.append(Trade(
                    session_date_ist=day_ist.isoformat(), direction=position.direction, htf=position.htf,
                    entry_time_utc=position.entry_time.isoformat(), exit_time_utc=bar.timestamp.isoformat(),
                    entry=position.entry, stop=position.stop, target=position.target, exit=exit_price,
                    result=result, risk_points=risk, gross_r=gross_r, net_r=net_r,
                    fvg_size=position.fvg_size, source_ob_low=position.source_ob_low,
                    source_ob_high=position.source_ob_high,
                ))
                position = None
                active_tap = None
                pending = None
                if len(trades) >= int(config["max_trades_per_session"]):
                    break
            continue

        if pending is not None:
            if bar.timestamp > pending.expiry:
                pending = None
            elif bar.timestamp > pending.created_at and order_fills(pending, bar):
                position = OpenTrade(
                    direction=pending.direction, entry_time=bar.timestamp, entry=pending.entry,
                    stop=pending.stop, target=pending.target, htf=pending.htf,
                    fvg_size=pending.fvg_size, source_ob_low=pending.source_ob_low,
                    source_ob_high=pending.source_ob_high,
                )
                pending = None
            continue

        if active_tap is None:
            for block in eligible_blocks(bar.timestamp, bars, index, htf_blocks, config):
                if not bar_taps_zone(bar, block):
                    continue
                if config["require_asian_liquidity_sweep"] and not liquidity_sweep(
                    bars, index, block.direction, int(config["sweep_lookback_bars"])
                ):
                    continue
                extreme = bar.low if block.direction == "long" else bar.high
                active_tap = (block.direction, block, extreme, index)
                break
            continue

        direction, htf_block, sweep_extreme, tap_index = active_tap
        if direction == "long":
            sweep_extreme = min(sweep_extreme, bar.low)
        else:
            sweep_extreme = max(sweep_extreme, bar.high)
        active_tap = (direction, htf_block, sweep_extreme, tap_index)

        latest_high, latest_low = pivots_available_at(bars, index, left, right)
        if direction == "long":
            bos = latest_high is not None and bar.close > latest_high.price
        else:
            bos = latest_low is not None and bar.close < latest_low.price
        if not bos:
            continue

        pending = build_pending_order(
            bars, index, direction, htf_block, sweep_extreme, session_end, config
        )
        active_tap = None

    if position is not None and config["force_exit_at_session_end"]:
        last_bar = bars[last_index]
        risk = abs(position.entry - position.stop)
        direction_multiplier = 1.0 if position.direction == "long" else -1.0
        gross_r = direction_multiplier * (last_bar.close - position.entry) / risk
        net_r = gross_r - float(config["spread_points"]) / risk
        trades.append(Trade(
            session_date_ist=day_ist.isoformat(), direction=position.direction, htf=position.htf,
            entry_time_utc=position.entry_time.isoformat(), exit_time_utc=last_bar.timestamp.isoformat(),
            entry=position.entry, stop=position.stop, target=position.target, exit=last_bar.close,
            result="session_close", risk_points=risk, gross_r=gross_r, net_r=net_r,
            fvg_size=position.fvg_size, source_ob_low=position.source_ob_low,
            source_ob_high=position.source_ob_high,
        ))
    return trades


def run_backtest(bars: Sequence[Bar], config: dict, start_day: date, end_day: date) -> List[Trade]:
    htf_blocks: List[OrderBlock] = []
    for timeframe in config["context_timeframes"]:
        htf_blocks.extend(build_htf_order_blocks(bars, timeframe, config))
    all_trades: List[Trade] = []
    for day_ist in unique_ist_dates(bars, config):
        if not start_day <= day_ist <= end_day:
            continue
        all_trades.extend(simulate_session(day_ist, bars, htf_blocks, config))
    return sorted(all_trades, key=lambda trade: trade.entry_time_utc)


def max_drawdown_r(trades: Sequence[Trade]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for trade in trades:
        equity += trade.net_r
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
    return max_drawdown


def period_summary(trades: Sequence[Trade]) -> dict:
    net_results = [trade.net_r for trade in trades]
    wins = [value for value in net_results if value > 0]
    losses = [value for value in net_results if value < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor: Optional[float]
    if gross_loss == 0:
        profit_factor = None if gross_profit == 0 else math.inf
    else:
        profit_factor = gross_profit / gross_loss
    return {
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (len(wins) / len(trades) * 100.0) if trades else None,
        "net_r": sum(net_results),
        "average_r": statistics.mean(net_results) if net_results else None,
        "median_r": statistics.median(net_results) if net_results else None,
        "profit_factor": profit_factor,
        "max_drawdown_r": max_drawdown_r(trades),
    }


def monthly_summary(trades: Sequence[Trade]) -> List[dict]:
    groups: Dict[str, List[Trade]] = defaultdict(list)
    for trade in trades:
        groups[trade.session_date_ist[:7]].append(trade)
    rows: List[dict] = []
    for month, rows_trades in sorted(groups.items()):
        summary = period_summary(rows_trades)
        summary["month"] = month
        rows.append(summary)
    return rows


def out_of_sample_split(trades: Sequence[Trade], fraction: float) -> Tuple[List[Trade], List[Trade]]:
    if not trades or fraction <= 0:
        return list(trades), []
    split_at = max(1, math.floor(len(trades) * (1 - fraction)))
    return list(trades[:split_at]), list(trades[split_at:])


def serialise_value(value: object) -> object:
    if isinstance(value, float) and math.isinf(value):
        return "infinite"
    return value


def write_report(output_dir: Path, config: dict, trades: Sequence[Trade], start_day: date, end_day: date) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    trades_path = output_dir / "trades.csv"
    with trades_path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = list(asdict(trades[0]).keys()) if trades else list(Trade.__dataclass_fields__.keys())
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for trade in trades:
            writer.writerow(asdict(trade))

    monthly = monthly_summary(trades)
    monthly_path = output_dir / "monthly_summary.csv"
    with monthly_path.open("w", newline="", encoding="utf-8") as handle:
        fields = ["month", "trades", "wins", "losses", "win_rate_pct", "net_r", "average_r", "median_r", "profit_factor", "max_drawdown_r"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in monthly:
            writer.writerow({key: serialise_value(row.get(key)) for key in fields})

    in_sample, out_of_sample = out_of_sample_split(trades, float(config["out_of_sample_fraction"]))
    profitable_months = sum(1 for row in monthly if float(row["net_r"]) > 0)
    report = {
        "period_ist": {"from": start_day.isoformat(), "to": end_day.isoformat()},
        "strategy_definition": {
            "description": "HTF order-block tap + Asian liquidity sweep + 5m BOS/FVG + OB retest",
            "signal_data": "Capital.com 5-minute midpoint OHLC; fixed spread deducted in R",
            "lookahead_protection": "HTF candles are used only after close; 5m pivots wait for right-side confirmation; entry cannot fill on BOS candle",
            "same_bar_exit_policy": config["same_bar_exit_policy"],
        },
        "parameters": config,
        "overall": {key: serialise_value(value) for key, value in period_summary(trades).items()},
        "in_sample": {key: serialise_value(value) for key, value in period_summary(in_sample).items()},
        "out_of_sample": {key: serialise_value(value) for key, value in period_summary(out_of_sample).items()},
        "monthly": [{key: serialise_value(value) for key, value in row.items()} for row in monthly],
        "monthly_consistency": {
            "months_with_trades": len(monthly),
            "profitable_months": profitable_months,
            "profitable_month_rate_pct": (profitable_months / len(monthly) * 100.0) if monthly else None,
        },
        "limitations": [
            "Historical result only; it does not prove future profitability.",
            "Order-block and BOS definitions are the explicit code definitions in the config/script, not a universal institutional standard.",
            "OHLC bars cannot establish the order of all intrabar events; same-bar stop/target outcomes follow the configured conservative rule.",
            "Fixed spread is an approximation. Financing, slippage, gaps, commissions, and account-specific contract values are not modelled.",
        ],
    }
    report_path = output_dir / "summary.json"
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, allow_nan=False)
    return report_path


def print_summary(report_path: Path) -> None:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    overall = report["overall"]
    print("\nHistorical Backtest Summary (results are not a future-performance claim)")
    print(f"Period (IST): {report['period_ist']['from']} to {report['period_ist']['to']}")
    print(f"Trades: {overall['trades']} | Win rate: {format_metric(overall['win_rate_pct'], '%')}")
    print(f"Net R: {format_metric(overall['net_r'])} | Avg R: {format_metric(overall['average_r'])}")
    print(f"Profit factor: {format_metric(overall['profit_factor'])} | Max drawdown: {format_metric(overall['max_drawdown_r'])} R")
    monthly = report["monthly_consistency"]
    print(
        "Profitable months: {}/{} ({})".format(
            monthly["profitable_months"], monthly["months_with_trades"],
            format_metric(monthly["profitable_month_rate_pct"], "%"),
        )
    )
    out_sample = report["out_of_sample"]
    if out_sample["trades"]:
        print(
            "Out-of-sample: {} trades | Net R: {} | PF: {}".format(
                out_sample["trades"], format_metric(out_sample["net_r"]),
                format_metric(out_sample["profit_factor"]),
            )
        )
    print(f"\nSaved: {report_path}")


def format_metric(value: object, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    if value == "infinite" or (isinstance(value, float) and math.isinf(value)):
        return "infinite"
    return f"{float(value):.2f}{suffix}"


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as error:
        raise argparse.ArgumentTypeError("Use YYYY-MM-DD.") from error


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Historical-only Capital.com XAUUSD SMC FVG/OB/BOS backtester. No order endpoints are used."
    )
    parser.add_argument("--from", dest="start", required=True, type=parse_date, help="Start date in IST, YYYY-MM-DD.")
    parser.add_argument("--to", dest="end", required=True, type=parse_date, help="End date in IST, YYYY-MM-DD.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="JSON strategy parameters.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--csv", type=Path, help="Normalized OHLC CSV previously exported by this tool.")
    source.add_argument("--download", action="store_true", help="Download historical OHLC from Capital.com using GOLD/.env.")
    parser.add_argument("--demo", action="store_true", help="Use Capital.com demo endpoint when downloading.")
    parser.add_argument("--verify-epic", action="store_true", help="Search Capital.com markets for gold before downloading.")
    parser.add_argument("--data-out", type=Path, help="Where to save normalized downloaded OHLC CSV.")
    parser.add_argument("--output-dir", type=Path, default=SCRIPT_DIR / "backtest_output" / "smc_ob_fvg_bos")
    args = parser.parse_args()

    if args.end < args.start:
        parser.error("--to must be on or after --from.")
    config = load_config(args.config)
    warmup_days = int(config["warmup_days"])
    download_start = datetime.combine(args.start - timedelta(days=warmup_days), dt_time.min, tzinfo=IST).astimezone(UTC)
    download_end = datetime.combine(args.end + timedelta(days=1), dt_time.min, tzinfo=IST).astimezone(UTC)

    try:
        if args.download:
            bars = download_capital_bars(
                config, download_start, download_end, demo=args.demo, verify_epic=args.verify_epic
            )
            data_out = args.data_out or (SCRIPT_DIR / "data" / f"{config['epic']}_MINUTE_5_{args.start}_{args.end}.csv")
            write_normalized_csv(data_out, bars)
            print(f"Downloaded and saved {len(bars)} normalized 5-minute bars to {data_out}")
        else:
            bars = read_normalized_csv(args.csv)
        trades = run_backtest(bars, config, args.start, args.end)
        report_path = write_report(args.output_dir, config, trades, args.start, args.end)
        print_summary(report_path)
        return 0
    except (BacktestError, OSError, ValueError) as error:
        print(f"BACKTEST FAILED: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
