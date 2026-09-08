#!/usr/bin/env python3
"""US500 09:30 New York First-Swing BOS — guarded Capital.com live runner.

This runner starts directly with ``python3 us500_ny_open_bos_live.py``. It uses
the same 1-minute, 09:30 America/New_York first-swing BOS mechanics as
``us500_ny_open_bos_backtest.py`` with the validated entry filter:

    abs(close_bid - open_bid) / (high_bid - low_bid) >= 0.75

The fixed planned risk is ``RISK_PER_TRADE_AED`` (AED 40.00). Account mode is
selected only through ``CAPITAL_DEMO`` in ``.env``:

    CAPITAL_DEMO=true   # Capital.com demo account
    CAPITAL_DEMO=false  # Capital.com live account

The close-confirmed strategy intentionally uses a protected MARKET entry, not a
pre-armed STOP entry: the body-quality condition is knowable only after the
breakout minute closes, when price has normally already crossed the level.
Submitting a STOP working order before that close would bypass the 75% candle
filter and implement a different strategy. Every market entry is submitted with
broker-hosted absolute SL and TP in the same request; a naked-order fallback is
never permitted. If the broker accepts a position but protection cannot be
verified and repaired, the runner attempts an immediate protective close and
locks itself.

This program is designed for a durable host. The current sandbox must not be
used as an always-on execution environment.
"""

from __future__ import annotations

import fcntl
import importlib.util
import json
import logging
import math
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Iterator, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from capitalcom_api import CapitalComAPI

PROJECT_DIR = Path(__file__).resolve().parent
SOURCE_RUNNER = PROJECT_DIR / "us500_ny_open_bos_backtest.py"
STATE_DIR = PROJECT_DIR / "state"
LOG_DIR = PROJECT_DIR / "logs"
DEFAULT_STATE_PATH = STATE_DIR / "us500_ny_open_bos_body75_live_state.json"
DEFAULT_LOCK_PATH = STATE_DIR / "us500_ny_open_bos_body75_live.lock"
EPIC = "US500"
IST = ZoneInfo("Asia/Kolkata")
NEW_YORK = ZoneInfo("America/New_York")
UTC = timezone.utc
PIVOT_LEFT = 5
PIVOT_RIGHT = 5
BODY_THRESHOLD = 0.75
REWARD_MULTIPLE = 2.0
VALUE_PER_POINT_USD = 1.0
USD_TO_AED = 3.6725
DEFAULT_POLL_SECONDS = 5.0
DEFAULT_MAX_ENTRY_SLIPPAGE_POINTS = 2.0
RISK_PER_TRADE_AED = 40.0
ORDER_EXECUTION_ENABLED = True
POSITION_RECONCILIATION_RETRIES = 3
RECONCILIATION_SLEEP_SECONDS = 0.75

load_dotenv(PROJECT_DIR / ".env", override=False)

source_spec = importlib.util.spec_from_file_location("us500_live_strategy_source", SOURCE_RUNNER)
if source_spec is None or source_spec.loader is None:
    raise RuntimeError(f"Unable to import strategy source: {SOURCE_RUNNER}")
source = importlib.util.module_from_spec(source_spec)
sys.modules[source_spec.name] = source
source_spec.loader.exec_module(source)


@dataclass(frozen=True)
class Signal:
    """One completed-bar BOS signal that passed the body-quality filter."""

    session_date: str
    signal_time_utc: str
    direction: str
    first_swing_high: float
    first_swing_low: float
    nearest_swing_high: Optional[float]
    nearest_swing_low: Optional[float]
    reference_entry: float
    reference_stop: float
    stop_source: str
    body_fraction: float


@dataclass(frozen=True)
class BrokerConstraints:
    """Current broker deal and price constraints read before any execution."""

    min_deal_size: float
    size_increment: float
    price_increment: float
    min_stop_distance: float
    min_limit_distance: float


@dataclass(frozen=True)
class TradePlan:
    """A fresh-quote protected market entry ready for broker submission."""

    direction: str
    entry: float
    stop: float
    target: float
    size: float
    planned_risk_aed: float
    stop_distance: float
    target_distance: float
    adverse_slippage_points: float
    stop_source: str


def configure_logging(verbose: bool) -> logging.Logger:
    """Configure terminal and durable runner logs without exposing credentials."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("us500_ny_open_live")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    file_handler = logging.FileHandler(LOG_DIR / "us500_ny_open_bos_body75_live.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(stream)
    logger.addHandler(file_handler)
    return logger


@contextmanager
def exclusive_lock(lock_path: Path) -> Iterator[None]:
    """Prevent simultaneous runner processes from controlling the same state."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SystemExit(f"Another US500 live runner owns {lock_path}; refusing duplicate execution.") from error
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def iso_utc(value: datetime) -> str:
    """Format a UTC timestamp for state and Capital.com good-till dates."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")


def parse_iso_utc(value: str) -> datetime:
    """Parse the runner's persisted UTC timestamp format."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def new_state(session_date: date, config: dict[str, Any]) -> dict[str, Any]:
    """Build a minimal, auditable state for one New York session."""
    start = source.new_york_open(session_date).astimezone(UTC)
    cutoff = start + timedelta(minutes=source.ENTRY_WINDOW_MINUTES)
    return {
        "version": 1,
        "session_date": session_date.isoformat(),
        "session_start_utc": iso_utc(start),
        "entry_cutoff_utc": iso_utc(cutoff),
        "status": "WAITING",  # WAITING | IN_POSITION | DONE | LOCKED
        "trade_taken": False,
        "first_swing_high": None,
        "first_swing_low": None,
        "nearest_swing_high": None,
        "nearest_swing_low": None,
        "signal": None,
        "trade": None,
        "lock_reason": None,
        "config": config,
        "updated_at_utc": iso_utc(datetime.now(UTC)),
    }


def load_state(path: Path) -> Optional[dict[str, Any]]:
    """Load state only when it is a valid JSON object."""
    if not path.exists():
        return None
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Cannot read live-runner state {path}: {error}") from error
    if not isinstance(loaded, dict):
        raise RuntimeError(f"Live-runner state {path} is not an object")
    return loaded


def save_state(path: Path, state: dict[str, Any]) -> None:
    """Persist state atomically so process restarts cannot double-submit orders."""
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at_utc"] = iso_utc(datetime.now(UTC))
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def current_session_date(now_utc: datetime) -> date:
    """Return the New York calendar date relevant to the strategy session."""
    return now_utc.astimezone(NEW_YORK).date()


def session_window(session_date: date) -> tuple[datetime, datetime]:
    """Return 09:30 and 12:30 New York timestamps in UTC."""
    start = source.new_york_open(session_date).astimezone(UTC)
    return start, start + timedelta(minutes=source.ENTRY_WINDOW_MINUTES)


def completed_bar_end(now_utc: datetime) -> datetime:
    """Return the last fully completed one-minute candle boundary in UTC."""
    floored = now_utc.astimezone(UTC).replace(second=0, microsecond=0)
    return floored - timedelta(minutes=1)


def price_increment_from_market(market: dict[str, Any]) -> float:
    """Infer the instrument's price precision from Capital.com market metadata."""
    snapshot = market.get("snapshot", {})
    scaling_factor = snapshot.get("scalingFactor")
    try:
        scaling = int(scaling_factor)
    except (TypeError, ValueError):
        scaling = 1
    if scaling < 0 or scaling > 8:
        raise RuntimeError(f"Invalid Capital.com US500 scaling factor: {scaling_factor!r}")
    return 10.0 ** (-scaling)


def deal_rule_value(rules: dict[str, Any], name: str) -> float:
    """Read an optional non-negative distance rule, returning zero if absent."""
    raw = rules.get(name, {})
    if isinstance(raw, dict):
        raw = raw.get("value", 0)
    try:
        value = float(raw or 0.0)
    except (TypeError, ValueError) as error:
        raise RuntimeError(f"Invalid Capital.com rule {name}: {raw!r}") from error
    if value < 0:
        raise RuntimeError(f"Negative Capital.com rule {name}: {value}")
    return value


def read_broker_constraints(api: CapitalComAPI) -> BrokerConstraints:
    """Load current US500 sizing, precision, and protection-distance rules."""
    market = api.get_market_details(EPIC)
    rules = market.get("dealingRules", {})
    min_size = float(rules.get("minDealSize", {}).get("value") or 0.0)
    increment = float(rules.get("minSizeIncrement", {}).get("value") or 0.0)
    if min_size <= 0 or increment <= 0:
        raise RuntimeError("Capital.com returned invalid US500 deal-size constraints")
    return BrokerConstraints(
        min_deal_size=min_size,
        size_increment=increment,
        price_increment=price_increment_from_market(market),
        min_stop_distance=deal_rule_value(rules, "minNormalStopOrLimitDistance"),
        min_limit_distance=deal_rule_value(rules, "minNormalStopOrLimitDistance"),
    )


def decimal_round(value: float, increment: float, mode: str) -> float:
    """Round a price to the broker increment in a direction that preserves risk."""
    if increment <= 0:
        raise ValueError("Price increment must be positive")
    rounding = ROUND_FLOOR if mode == "floor" else ROUND_CEILING
    step = Decimal(str(increment))
    rounded = (Decimal(str(value)) / step).to_integral_value(rounding=rounding) * step
    return float(rounded)


def floor_size(size: float, increment: float) -> float:
    """Round a deal size down to a permitted Capital.com increment."""
    if increment <= 0:
        raise ValueError("Size increment must be positive")
    step = Decimal(str(increment))
    rounded = (Decimal(str(size)) / step).to_integral_value(rounding=ROUND_FLOOR) * step
    return float(rounded)


def compute_size(stop_distance: float, risk_aed: float, constraints: BrokerConstraints) -> tuple[Optional[float], float, Optional[str]]:
    """Size a position so planned price-stop risk never exceeds the AED cap."""
    if stop_distance <= 0:
        return None, 0.0, "non-positive stop distance"
    if risk_aed <= 0:
        return None, 0.0, "non-positive risk budget"
    denominator = stop_distance * VALUE_PER_POINT_USD * USD_TO_AED
    raw_size = risk_aed / denominator
    size = floor_size(raw_size, constraints.size_increment)
    if size < constraints.min_deal_size:
        return None, 0.0, "broker minimum size exceeds risk budget"
    planned_risk = size * denominator
    if planned_risk > risk_aed + 1e-8:
        return None, 0.0, "rounded size breaches risk budget"
    return size, planned_risk, None


def body_fraction(bar: source.Candle) -> float:
    """Return the completed candle body as a proportion of full bid range."""
    total_range = bar.high_bid - bar.low_bid
    if total_range <= 0:
        return 0.0
    return abs(bar.close_bid - bar.open_bid) / total_range


def latest_close_signal(bars: list[source.Candle], session_date: date, now_utc: datetime) -> tuple[Optional[Signal], dict[str, Optional[float]]]:
    """Evaluate only the newest completed bar; missed bars never trigger late trades.

    Pivot levels are rebuilt causally from the session's completed history. The
    returned signal can only refer to ``bars[-1]``. This strict recency guard
    prevents a restart or data outage from sending a market order for an old
    breakout that may be many points away from its original close.
    """
    if not bars:
        return None, {"first_swing_high": None, "first_swing_low": None, "nearest_swing_high": None, "nearest_swing_low": None}
    session_start, cutoff = session_window(session_date)
    latest = bars[-1]
    if latest.timestamp >= now_utc or latest.timestamp < session_start or latest.timestamp >= cutoff:
        return None, {"first_swing_high": None, "first_swing_low": None, "nearest_swing_high": None, "nearest_swing_low": None}

    first_high: Optional[float] = None
    first_low: Optional[float] = None
    nearest_high: Optional[float] = None
    nearest_low: Optional[float] = None
    candidate: Optional[Signal] = None

    for index, bar in enumerate(bars):
        pivot_index = index - PIVOT_RIGHT
        if pivot_index >= 0:
            pivot = bars[pivot_index]
            confirmation_in_window = session_start <= bar.timestamp < cutoff
            pivot_after_open = pivot.timestamp >= session_start
            if confirmation_in_window and pivot_after_open:
                if source.is_pivot_high(bars, pivot_index, PIVOT_LEFT, PIVOT_RIGHT):
                    nearest_high = pivot.high_bid
                    if first_high is None:
                        first_high = pivot.high_bid
                if source.is_pivot_low(bars, pivot_index, PIVOT_LEFT, PIVOT_RIGHT):
                    nearest_low = pivot.low_bid
                    if first_low is None:
                        first_low = pivot.low_bid

        # A signal must occur on the latest fully completed bar only.
        if index != len(bars) - 1 or index == 0:
            continue
        if first_high is None or first_low is None:
            continue
        previous = bars[index - 1]
        breaks_high = bar.close_bid > first_high and previous.close_bid <= first_high
        breaks_low = bar.close_bid < first_low and previous.close_bid >= first_low
        if breaks_high == breaks_low:
            continue
        fraction = body_fraction(bar)
        if fraction < BODY_THRESHOLD:
            continue

        if breaks_high:
            direction = "BUY"
            reference_entry = bar.close_ask
            reference_stop, stop_source = source.select_long_stop(
                reference_entry, nearest_low, first_low, bar.low_bid
            )
            if reference_stop >= reference_entry:
                continue
        else:
            direction = "SELL"
            reference_entry = bar.close_bid
            reference_stop, stop_source = source.select_short_stop(
                reference_entry, nearest_high, first_high, bar.high_ask
            )
            if reference_stop <= reference_entry:
                continue

        candidate = Signal(
            session_date=session_date.isoformat(),
            signal_time_utc=iso_utc(bar.timestamp),
            direction=direction,
            first_swing_high=first_high,
            first_swing_low=first_low,
            nearest_swing_high=nearest_high,
            nearest_swing_low=nearest_low,
            reference_entry=reference_entry,
            reference_stop=reference_stop,
            stop_source=stop_source,
            body_fraction=fraction,
        )

    levels = {
        "first_swing_high": first_high,
        "first_swing_low": first_low,
        "nearest_swing_high": nearest_high,
        "nearest_swing_low": nearest_low,
    }
    return candidate, levels


def live_price(api: CapitalComAPI) -> tuple[float, float, str]:
    """Return fresh bid/offer quotes and a tradeability status, never guessing values."""
    quote = api.get_current_price(EPIC)
    bid = float(quote.get("bid") or 0.0)
    offer = float(quote.get("offer") or 0.0)
    status = str(quote.get("status") or "")
    if bid <= 0 or offer <= 0 or offer < bid:
        raise RuntimeError(f"Invalid US500 live quote: bid={bid}, offer={offer}, status={status}")
    return bid, offer, status


def build_trade_plan(signal: Signal, bid: float, offer: float, risk_aed: float,
                     max_entry_slippage: float, constraints: BrokerConstraints) -> tuple[Optional[TradePlan], str]:
    """Recalculate a protected market plan from a fresh quote inside a strict slip cap."""
    if signal.direction == "BUY":
        adverse_slip = offer - signal.reference_entry
        if adverse_slip > max_entry_slippage:
            return None, f"BUY adverse slippage {adverse_slip:.2f} exceeds cap {max_entry_slippage:.2f}"
        entry = decimal_round(offer, constraints.price_increment, "ceiling")
        raw_stop = signal.nearest_swing_low if signal.nearest_swing_low is not None else signal.reference_stop
        stop = decimal_round(raw_stop, constraints.price_increment, "floor")
        distance = entry - stop
        if distance <= 0:
            return None, "BUY stop is not below fresh entry"
        target = decimal_round(entry + REWARD_MULTIPLE * distance, constraints.price_increment, "ceiling")
    else:
        adverse_slip = signal.reference_entry - bid
        if adverse_slip > max_entry_slippage:
            return None, f"SELL adverse slippage {adverse_slip:.2f} exceeds cap {max_entry_slippage:.2f}"
        entry = decimal_round(bid, constraints.price_increment, "floor")
        raw_stop = signal.nearest_swing_high if signal.nearest_swing_high is not None else signal.reference_stop
        stop = decimal_round(raw_stop, constraints.price_increment, "ceiling")
        distance = stop - entry
        if distance <= 0:
            return None, "SELL stop is not above fresh entry"
        target = decimal_round(entry - REWARD_MULTIPLE * distance, constraints.price_increment, "floor")

    target_distance = abs(target - entry)
    required_distance = max(constraints.min_stop_distance, constraints.min_limit_distance)
    if distance + 1e-9 < constraints.min_stop_distance:
        return None, f"stop distance {distance:.4f} is below broker minimum {constraints.min_stop_distance:.4f}"
    if target_distance + 1e-9 < constraints.min_limit_distance:
        return None, f"target distance {target_distance:.4f} is below broker minimum {constraints.min_limit_distance:.4f}"
    if distance + 1e-9 < required_distance:
        return None, f"protection distance {distance:.4f} is below broker rule {required_distance:.4f}"

    size, planned_risk, size_error = compute_size(distance, risk_aed, constraints)
    if size is None:
        return None, size_error or "sizing failed"
    return TradePlan(
        direction=signal.direction,
        entry=entry,
        stop=stop,
        target=target,
        size=size,
        planned_risk_aed=planned_risk,
        stop_distance=distance,
        target_distance=target_distance,
        adverse_slippage_points=max(0.0, adverse_slip),
        stop_source=signal.stop_source,
    ), ""


def us500_positions(api: CapitalComAPI) -> list[dict[str, Any]]:
    """Return all open US500 positions to detect ambiguity and duplicate exposure."""
    return [
        position for position in api.get_all_positions()
        if position.get("market", {}).get("epic") == EPIC
    ]


def us500_working_orders(api: CapitalComAPI) -> list[dict[str, Any]]:
    """Return all broker-hosted US500 orders so stale orders cannot be ignored."""
    return [
        order for order in api.get_working_orders()
        if order.get("workingOrderData", {}).get("epic") == EPIC
    ]


def position_protection(position: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    """Read stop and limit levels from observed Capital.com position variants."""
    raw = position.get("position", position)
    stop_raw = raw.get("stopLevel", raw.get("stop_level"))
    target_raw = raw.get("limitLevel", raw.get("profitLevel", raw.get("profit_level")))
    try:
        stop = float(stop_raw) if stop_raw is not None else None
    except (TypeError, ValueError):
        stop = None
    try:
        target = float(target_raw) if target_raw is not None else None
    except (TypeError, ValueError):
        target = None
    return stop, target


def expected_protection_matches(plan: TradePlan, position: dict[str, Any], tolerance: float) -> bool:
    """Check the broker reports both expected protective levels within one tick."""
    actual_stop, actual_target = position_protection(position)
    return (
        actual_stop is not None
        and actual_target is not None
        and abs(actual_stop - plan.stop) <= tolerance
        and abs(actual_target - plan.target) <= tolerance
    )


def confirm_reference(api: CapitalComAPI, deal_reference: str, log: logging.Logger) -> dict[str, Any]:
    """Confirm Capital.com acceptance, retaining the broker response for audit logs."""
    if not deal_reference:
        raise RuntimeError("Capital.com response omitted dealReference")
    time.sleep(RECONCILIATION_SLEEP_SECONDS)
    confirmation = api.confirm_deal(deal_reference)
    status = str(confirmation.get("dealStatus") or "")
    if status != "ACCEPTED":
        raise RuntimeError(f"Capital.com deal not accepted: {confirmation}")
    log.info("[CONFIRM] Accepted reference=%s dealId=%s", deal_reference, confirmation.get("dealId"))
    return confirmation


def find_position_by_id(positions: list[dict[str, Any]], expected_deal_id: Optional[str]) -> Optional[dict[str, Any]]:
    """Find a confirmed deal ID, allowing a single US500 position as a broker-ID fallback."""
    if expected_deal_id:
        for position in positions:
            if position.get("position", {}).get("dealId") == expected_deal_id:
                return position
    return positions[0] if len(positions) == 1 else None


def close_unprotected_position(api: CapitalComAPI, position: dict[str, Any], log: logging.Logger) -> None:
    """Attempt an immediate close when attached SL/TP cannot be verified or repaired."""
    deal_id = position.get("position", {}).get("dealId")
    if not deal_id:
        raise RuntimeError("Cannot close unprotected position because Capital.com omitted dealId")
    log.critical("[FAIL-CLOSED] Closing unprotected US500 position %s", deal_id)
    response = api.close_position(deal_id)
    reference = response.get("dealReference", "")
    if reference:
        confirm_reference(api, reference, log)


def reconcile_protection(api: CapitalComAPI, confirmed_deal_id: Optional[str], plan: TradePlan,
                         constraints: BrokerConstraints, log: logging.Logger) -> dict[str, Any]:
    """Verify broker SL/TP; repair once through PUT, then close if still unsafe."""
    position: Optional[dict[str, Any]] = None
    for attempt in range(1, POSITION_RECONCILIATION_RETRIES + 1):
        positions = us500_positions(api)
        position = find_position_by_id(positions, confirmed_deal_id)
        if position is not None and expected_protection_matches(plan, position, constraints.price_increment + 1e-8):
            log.info("[PROTECTION] Broker SL=%s and TP=%s verified", plan.stop, plan.target)
            return position
        time.sleep(RECONCILIATION_SLEEP_SECONDS * attempt)

    if position is None:
        raise RuntimeError("Accepted order could not be reconciled to exactly one US500 position")
    deal_id = position.get("position", {}).get("dealId")
    if not deal_id:
        raise RuntimeError("Accepted US500 position omitted dealId")
    log.warning("[PROTECTION] Attached levels absent or mismatched; attempting guarded repair")
    try:
        api.update_position(deal_id, stop_level=plan.stop, profit_level=plan.target)
        time.sleep(RECONCILIATION_SLEEP_SECONDS)
        repaired = find_position_by_id(us500_positions(api), deal_id)
        if repaired is not None and expected_protection_matches(plan, repaired, constraints.price_increment + 1e-8):
            log.warning("[PROTECTION] Guarded repair verified")
            return repaired
    except Exception as error:
        log.critical("[PROTECTION] Guarded repair failed: %s", error)

    close_unprotected_position(api, position, log)
    raise RuntimeError("Position protection could not be verified; position was closed and runner locked")


def submit_protected_market_order(api: CapitalComAPI, plan: TradePlan, constraints: BrokerConstraints,
                                  execute: bool, log: logging.Logger) -> dict[str, Any]:
    """Submit only a protected market order; dry-run returns a simulated receipt.

    The function intentionally has no naked-market fallback. It either creates
    a market position carrying SL and TP in the same API request or places no
    trade. An accepted but unprotected position is repaired and then closed if
    repair cannot be verified.
    """
    if not execute:
        log.warning(
            "[DRY-RUN] Would submit %s US500 size=%s entry≈%.4f SL=%.4f TP=%.4f risk=AED %.2f",
            plan.direction, plan.size, plan.entry, plan.stop, plan.target, plan.planned_risk_aed,
        )
        return {"mode": "DRY_RUN", "deal_id": None, "deal_reference": None}

    response = api.open_position(
        epic=EPIC,
        direction=plan.direction,
        size=plan.size,
        stop_level=plan.stop,
        profit_level=plan.target,
        guaranteed_stop=False,
    )
    confirmation = confirm_reference(api, str(response.get("dealReference") or ""), log)
    position = reconcile_protection(api, confirmation.get("dealId"), plan, constraints, log)
    deal_id = position.get("position", {}).get("dealId")
    if not deal_id:
        raise RuntimeError("Protection verified but Capital.com position has no dealId")
    return {
        "mode": "EXECUTED",
        "deal_id": deal_id,
        "deal_reference": response.get("dealReference"),
        "confirmed_deal_id": confirmation.get("dealId"),
    }


def close_tracked_position(api: CapitalComAPI, trade_state: dict[str, Any], execute: bool, log: logging.Logger) -> None:
    """Close only the stored US500 position at the next New York session reset."""
    positions = us500_positions(api)
    if not positions:
        log.info("[RESET] No tracked US500 position remains")
        return
    stored_id = trade_state.get("deal_id")
    position = find_position_by_id(positions, stored_id)
    if position is None:
        raise RuntimeError("US500 position ambiguity at session reset; refusing to close any position")
    deal_id = position.get("position", {}).get("dealId")
    if not deal_id:
        raise RuntimeError("Tracked US500 position has no dealId")
    if not execute:
        log.warning("[DRY-RUN] Would close tracked US500 position %s at session reset", deal_id)
        return
    response = api.close_position(deal_id)
    reference = str(response.get("dealReference") or "")
    if reference:
        confirm_reference(api, reference, log)
    log.info("[RESET] Closed prior-session US500 position %s", deal_id)


def cancel_tracked_working_order(api: CapitalComAPI, order_id: Optional[str], execute: bool, log: logging.Logger) -> None:
    """Cancel only an explicitly stored runner order, never unrelated user orders."""
    if not order_id:
        return
    active_ids = {
        order.get("workingOrderData", {}).get("dealId")
        for order in us500_working_orders(api)
    }
    if order_id not in active_ids:
        return
    if not execute:
        log.warning("[DRY-RUN] Would cancel tracked US500 working order %s", order_id)
        return
    api.cancel_working_order(order_id)
    log.info("[RESET] Cancelled tracked US500 working order %s", order_id)


def assert_no_unmanaged_exposure(api: CapitalComAPI, state: dict[str, Any]) -> None:
    """Fail closed if the account contains US500 exposure not owned by this runner."""
    tracked_trade_id = (state.get("trade") or {}).get("deal_id")
    tracked_order_id = (state.get("trade") or {}).get("working_order_id")
    positions = us500_positions(api)
    orders = us500_working_orders(api)

    if len(positions) > 1:
        raise RuntimeError("Multiple US500 positions found; refusing ambiguous live execution")
    if positions:
        actual_id = positions[0].get("position", {}).get("dealId")
        if not tracked_trade_id or actual_id != tracked_trade_id:
            raise RuntimeError("Unmanaged US500 position found; refusing live execution")
    for order in orders:
        actual_id = order.get("workingOrderData", {}).get("dealId")
        if not tracked_order_id or actual_id != tracked_order_id:
            raise RuntimeError("Unmanaged US500 working order found; refusing live execution")


def should_reset(state: dict[str, Any], now_utc: datetime) -> bool:
    """Return whether an older state has reached the next 09:30 New York session."""
    session_date = date.fromisoformat(state["session_date"])
    next_date = source.next_weekday(session_date)
    next_open = source.new_york_open(next_date).astimezone(UTC)
    return now_utc >= next_open


def reset_if_due(api: CapitalComAPI, state: Optional[dict[str, Any]], now_utc: datetime,
                 execute: bool, config: dict[str, Any], log: logging.Logger) -> dict[str, Any]:
    """Flatten tracked prior-session exposure at the next cash-open reset.

    The simulator uses this same next-session reset concept. The runner refuses
    to reset around unowned US500 exposure and never cancels orders it did not
    create and persist.
    """
    today = current_session_date(now_utc)
    if state is None:
        return new_state(today, config)
    if state.get("session_date") == today.isoformat():
        return state
    if not should_reset(state, now_utc):
        return state

    trade = state.get("trade") or {}
    if state.get("status") == "IN_POSITION":
        close_tracked_position(api, trade, execute, log)
    cancel_tracked_working_order(api, trade.get("working_order_id"), execute, log)
    log.info("[SESSION] Resetting from %s to %s", state.get("session_date"), today.isoformat())
    return new_state(today, config)


def runner_config() -> dict[str, Any]:
    """Return the fixed configuration used by the parameter-free runner."""
    return {
        "risk_aed": RISK_PER_TRADE_AED,
        "body_threshold": BODY_THRESHOLD,
        "max_entry_slippage_points": DEFAULT_MAX_ENTRY_SLIPPAGE_POINTS,
        "pivot_left": PIVOT_LEFT,
        "pivot_right": PIVOT_RIGHT,
        "reward_multiple": REWARD_MULTIPLE,
        "mode": "EXECUTE" if ORDER_EXECUTION_ENABLED else "DRY_RUN",
    }


def refresh_waiting_state_config(state: dict[str, Any], config: dict[str, Any], log: logging.Logger) -> None:
    """Accept revised settings only before a session has recorded any trade decision.

    A persisted ``WAITING`` state may be created before the operator settles on
    the risk amount. Refreshing it is safe because no entry signal, order, or
    position exists. Once the runner has recorded a trade, a skipped signal, a
    completed session, or a lock, configuration drift remains a hard error so a
    restart cannot alter the audit trail or revive a completed decision.
    """
    stored_config = state.get("config")
    if stored_config == config:
        return
    safe_to_refresh = (
        state.get("status") == "WAITING"
        and not state.get("trade_taken", False)
        and state.get("trade") is None
        and state.get("signal") is None
        and state.get("lock_reason") is None
    )
    if not safe_to_refresh:
        raise RuntimeError(
            "Runner configuration differs from persisted state after a session decision; "
            "review the state file instead of changing risk or execution mode."
        )
    log.warning(
        "[CONFIG] Refreshing unused waiting-state configuration: risk AED %s -> %s",
        (stored_config or {}).get("risk_aed"), config.get("risk_aed"),
    )
    state["config"] = config


def run_cycle(api: CapitalComAPI, state: dict[str, Any], constraints: BrokerConstraints,
              log: logging.Logger) -> tuple[dict[str, Any], bool]:
    """Execute one deterministic lifecycle cycle; return state and stop flag."""
    now_utc = datetime.now(UTC)
    session_date = date.fromisoformat(state["session_date"])
    start, cutoff = session_window(session_date)

    if state.get("status") == "LOCKED":
        log.critical("[LOCKED] %s", state.get("lock_reason") or "Manual review required")
        return state, True
    if state.get("status") == "DONE":
        log.info("[DONE] Session already completed: %s", state.get("lock_reason") or "trade or skip recorded")
        return state, False

    assert_no_unmanaged_exposure(api, state)

    if now_utc.weekday() >= 5:
        log.info("[WAIT] Weekend in UTC; no action")
        return state, False
    if now_utc < start:
        log.info("[WAIT] Next session begins %s (now %s)", iso_utc(start), iso_utc(now_utc))
        return state, False
    if now_utc >= cutoff:
        if state.get("status") == "WAITING":
            state["status"] = "DONE"
            state["lock_reason"] = "No valid fresh BOS signal before 12:30 New York cutoff"
            log.info("[DONE] Entry window ended without a trade")
        return state, False
    if state.get("trade_taken") or state.get("status") == "IN_POSITION":
        log.info("[MONITOR] One-trade guard active; broker-managed SL/TP remains attached")
        return state, False

    end = completed_bar_end(now_utc)
    if end < start:
        log.info("[WAIT] No completed post-open minute bar yet")
        return state, False
    data_start = start - timedelta(minutes=PIVOT_LEFT + PIVOT_RIGHT + 2)
    bars = source.fetch_minute_bars(api, data_start, end)
    signal, levels = latest_close_signal(bars, session_date, now_utc)
    state.update(levels)
    if signal is None:
        log.info(
            "[SCAN] No fresh qualified BOS | first H=%s L=%s",
            levels["first_swing_high"], levels["first_swing_low"],
        )
        return state, False

    # The signal minute must still be fresh. A delayed process may scan history,
    # but must never enter a stale market order.
    signal_time = parse_iso_utc(signal.signal_time_utc)
    if now_utc - signal_time > timedelta(minutes=1, seconds=10):
        state["status"] = "DONE"
        state["lock_reason"] = f"Qualified BOS at {signal.signal_time_utc} is stale; no late market entry"
        log.warning("[STALE] %s", state["lock_reason"])
        return state, False

    bid, offer, market_status = live_price(api)
    if market_status != "TRADEABLE":
        state["status"] = "DONE"
        state["lock_reason"] = f"US500 is not TRADEABLE ({market_status!r}) at valid signal"
        log.warning("[MARKET] %s", state["lock_reason"])
        return state, False

    plan, reason = build_trade_plan(
        signal, bid, offer, RISK_PER_TRADE_AED, DEFAULT_MAX_ENTRY_SLIPPAGE_POINTS, constraints
    )
    state["signal"] = asdict(signal)
    if plan is None:
        state["status"] = "DONE"
        state["lock_reason"] = f"Qualified signal skipped: {reason}"
        log.warning("[SKIP] %s", state["lock_reason"])
        return state, False

    log.warning(
        "[SIGNAL] %s body=%.3f | ref=%.4f | fresh entry=%.4f SL=%.4f TP=%.4f size=%s risk=AED %.2f slip=%.2f",
        plan.direction, signal.body_fraction, signal.reference_entry, plan.entry, plan.stop,
        plan.target, plan.size, plan.planned_risk_aed, plan.adverse_slippage_points,
    )
    try:
        receipt = submit_protected_market_order(api, plan, constraints, ORDER_EXECUTION_ENABLED, log)
    except Exception as error:
        state["status"] = "LOCKED"
        state["lock_reason"] = f"Order lifecycle failed: {type(error).__name__}: {error}"
        log.critical("[LOCKED] %s", state["lock_reason"])
        return state, True

    state["trade_taken"] = True
    state["trade"] = {**asdict(plan), **receipt, "submitted_at_utc": iso_utc(datetime.now(UTC))}
    if ORDER_EXECUTION_ENABLED:
        state["status"] = "IN_POSITION"
        log.warning("[LIVE] Protected Capital.com position active; no second trade will be submitted today")
    else:
        state["status"] = "DONE"
        log.warning("[DRY-RUN] Signal recorded; no order was sent")
    return state, False


class RejectingAPI:
    """Offline test double that proves dry-run submission never calls an order endpoint."""

    def open_position(self, **_: Any) -> dict[str, Any]:
        raise AssertionError("Dry-run attempted to call open_position")


def run_self_test() -> None:
    """Validate deterministic sizing, signal filtering, rounding, and dry-run execution gates."""
    constraints = BrokerConstraints(
        min_deal_size=0.01,
        size_increment=0.01,
        price_increment=0.1,
        min_stop_distance=0.0,
        min_limit_distance=0.0,
    )
    assert decimal_round(100.04, 0.1, "floor") == 100.0
    assert decimal_round(100.04, 0.1, "ceiling") == 100.1
    assert floor_size(1.239, 0.01) == 1.23
    size, risk, error = compute_size(10.0, 40.0, constraints)
    assert error is None and size is not None and risk <= 40.0

    logger = logging.getLogger("us500_self_test")
    old_config = {"risk_aed": 1.0, "mode": "DRY_RUN"}
    fresh_config = {"risk_aed": 40.0, "mode": "DRY_RUN"}
    waiting_state = new_state(date(2026, 4, 10), old_config)
    refresh_waiting_state_config(waiting_state, fresh_config, logger)
    assert waiting_state["config"] == fresh_config
    waiting_state["status"] = "DONE"
    try:
        refresh_waiting_state_config(waiting_state, {"risk_aed": 50.0, "mode": "DRY_RUN"}, logger)
    except RuntimeError:
        pass
    else:
        raise AssertionError("Completed session accepted a configuration change")

    summer_start, summer_cutoff = session_window(date(2026, 8, 14))
    winter_start, winter_cutoff = session_window(date(2026, 1, 9))
    assert summer_start.astimezone(IST).hour == 19 and summer_cutoff.astimezone(IST).hour == 22
    assert winter_start.astimezone(IST).hour == 20 and winter_cutoff.astimezone(IST).hour == 23

    start = datetime(2026, 4, 10, 13, 24, tzinfo=UTC)
    bars = [
        source.synthetic_candle(start + timedelta(minutes=index), high, low, close)
        for index, (high, low, close) in enumerate([
            (100, 99, 99), (101, 100, 100), (102, 101, 101), (103, 102, 102),
            (104, 103, 103), (105, 104, 104), (106, 105, 105), (107, 106, 106),
            (106, 105, 105), (105, 104, 104), (104, 103, 103), (103, 102, 102),
            (102, 101, 101), (101, 100, 100), (100, 99, 99), (99, 97, 98),
            (100, 98, 99), (101, 99, 100), (102, 100, 101), (103, 101, 102),
            (104, 102, 103), (108, 100, 108),
        ])
    ]
    qualified_bar = source.Candle(
        timestamp=bars[-1].timestamp,
        open_bid=100.0, high_bid=108.0, low_bid=100.0, close_bid=108.0,
        open_ask=100.2, high_ask=108.2, low_ask=100.2, close_ask=108.2,
    )
    bars = [*bars[:-1], qualified_bar]
    now = bars[-1].timestamp + timedelta(minutes=1, seconds=5)
    signal, _ = latest_close_signal(bars, date(2026, 4, 10), now)
    assert signal is not None and signal.direction == "BUY" and signal.body_fraction >= BODY_THRESHOLD
    plan, reason = build_trade_plan(signal, 108.1, 108.4, 40.0, 2.0, constraints)
    assert reason == "" and plan is not None and plan.planned_risk_aed <= 40.0 and plan.stop < plan.entry < plan.target
    receipt = submit_protected_market_order(RejectingAPI(), plan, constraints, False, logging.getLogger("us500_self_test"))
    assert receipt["mode"] == "DRY_RUN"

    weak_bar = source.Candle(
        timestamp=bars[-1].timestamp,
        open_bid=105.0, high_bid=125.0, low_bid=104.0, close_bid=106.0,
        open_ask=105.2, high_ask=125.2, low_ask=104.2, close_ask=106.2,
    )
    weak_bars = [*bars[:-1], weak_bar]
    weak_signal, _ = latest_close_signal(weak_bars, date(2026, 4, 10), now)
    assert weak_signal is None
    print("Self-test passed: DST windows, causal body filter, protected sizing, price rounding, and dry-run order gate.")


def account_balance_summary(account: dict[str, Any]) -> tuple[str, float, Optional[float]]:
    """Extract active-account currency, balance, and optional available funds safely."""
    balance_data = account.get("balance", {})
    if not isinstance(balance_data, dict):
        balance_data = {}
    currency = str(account.get("currency") or account.get("currencyIsoCode") or "UNKNOWN")
    balance = float(balance_data.get("balance") or 0.0)
    available_raw = balance_data.get("available")
    available = float(available_raw) if available_raw is not None else None
    return currency, balance, available


def main() -> None:
    """Run the live runner using settings defined directly in this file and .env."""
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        run_self_test()
        return
    if len(sys.argv) > 1:
        raise SystemExit("No command-line parameters are used. Run: python3 us500_ny_open_bos_live.py")

    log = configure_logging(False)
    config = runner_config()
    demo_mode = os.getenv("CAPITAL_DEMO", "true").lower() == "true"
    credentials = {
        "api_key": os.getenv("CAPITAL_API_KEY", ""),
        "identifier": os.getenv("CAPITAL_IDENTIFIER", ""),
        "password": os.getenv("CAPITAL_PASSWORD", ""),
    }
    if not all(credentials.values()):
        raise SystemExit("Missing CAPITAL_API_KEY, CAPITAL_IDENTIFIER, or CAPITAL_PASSWORD in US500/.env or environment.")

    lock_path = DEFAULT_LOCK_PATH
    with exclusive_lock(lock_path):
        api = CapitalComAPI(demo=demo_mode, **credentials)
        if not api.create_session():
            raise SystemExit("Capital.com session creation failed; runner stopped before any order path.")
        try:
            constraints = read_broker_constraints(api)
            account = api.get_account_info()
            currency, balance, available = account_balance_summary(account)
            log.warning("=" * 76)
            log.warning("US500 NY-open BOS body>=75%% runner | %s account | execution=%s", "DEMO" if demo_mode else "LIVE", ORDER_EXECUTION_ENABLED)
            log.warning("Session: 09:30-12:30 America/New_York | one trade | 1:2 target | risk cap=AED %.2f", RISK_PER_TRADE_AED)
            if available is None:
                log.warning("Account balance: %s %.2f", currency, balance)
            else:
                log.warning("Account balance: %s %.2f | available: %.2f", currency, balance, available)
            log.warning("Broker: min size=%s step=%s price tick=%s min stop=%s", constraints.min_deal_size, constraints.size_increment, constraints.price_increment, constraints.min_stop_distance)
            log.warning("=" * 76)

            while True:
                now_utc = datetime.now(UTC)
                state = reset_if_due(api, load_state(DEFAULT_STATE_PATH), now_utc, ORDER_EXECUTION_ENABLED, config, log)
                refresh_waiting_state_config(state, config, log)
                state, stop = run_cycle(api, state, constraints, log)
                save_state(DEFAULT_STATE_PATH, state)
                if stop:
                    break
                time.sleep(DEFAULT_POLL_SECONDS)
        finally:
            try:
                api.delete_session()
            except Exception as error:
                log.debug("Session close skipped: %s", error)


if __name__ == "__main__":
    main()
