"""
GER40 12:00 PM IST Breakout — Daily Runner
==========================================
Simple, focused script for one strategy only.

Logic:
  1. At 12:30 PM IST daily — fetch the 12:00 PM 30-min candle (06:30–07:00 UTC)
  2. Place BUY stop at H+buf and SELL stop at L-buf immediately after candle close
  3. SL = opposite side of candle | TP = entry ± 3× range (1:3 R:R)
  4. If T1 SL hit → reverse trade (T2) via pre-armed stop order
  5. If T1 TP hit → done for day
  6. Trade runs overnight — TP/SL managed by broker
  7. Close any open trade at 12:30 PM IST next day before fresh candle
  8. Max 2 trades per day

Run this script continuously — it sleeps between checks.
"""

import time
import logging
import json
import os
import sys
from typing import Optional
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo

from capitalcom_api import CapitalComAPI

# ── Config ────────────────────────────────────────────────────────────────────
EPIC          = "DE40"       # GER40 / DAX on Capital.com
FIXED_SL_AED  = 40.0         # fixed risk per trade in AED
RR_RATIO      = 3.0          # 1:3 reward-to-risk
STOP_BUFFER   = 1.0          # points beyond candle H/L for entry trigger
SL_BUFFER     = 0.0          # extra points beyond candle H/L for SL (0 = exact candle boundary)
MAX_ENTRY_SLIP = 5.0         # pts: if price is more than this past entry, skip market-order fallback
CHECK_EVERY   = 30           # seconds between scans (pre-candle)
CHECK_FAST    = 0.5          # seconds between scans (post-candle, waiting for breakout)
PIP_VALUE_EUR = 1.0          # DE40: 1 pt = €1 per unit
EUR_TO_AED    = 3.97         # EUR → AED conversion (update if rate moves significantly)
STATE_FILE    = "state/ger40_runner_state.json"

# ── Timezones ─────────────────────────────────────────────────────────────────
IST    = timezone(timedelta(hours=5, minutes=30))
BERLIN = ZoneInfo("Europe/Berlin")


def candle_utc_hours(d: date) -> tuple:
    """
    Returns (open_h, open_m, close_h, close_m) in UTC for the strategy candle.
    DAX opens at 09:00 CET/CEST. We use the 30-min candle that closes at that open.

    CEST (summer, UTC+2): DAX open = 07:00 UTC → candle = 06:30–07:00 UTC  (12:00–12:30 PM IST)
    CET  (winter, UTC+1): DAX open = 08:00 UTC → candle = 07:30–08:00 UTC  ( 1:00– 1:30  PM IST)
    """
    is_cest = bool(datetime(d.year, d.month, d.day, 12, 0, tzinfo=BERLIN).dst())
    if is_cest:
        return 6, 30, 7, 0
    else:
        return 7, 30, 8, 0

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/ger40_runner.log")
    ]
)
log = logging.getLogger("ger40")

# ── Credentials ───────────────────────────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'), override=True)

CAPITAL_API_KEY    = os.getenv('CAPITAL_API_KEY',    '')
CAPITAL_IDENTIFIER = os.getenv('CAPITAL_IDENTIFIER', '')
CAPITAL_PASSWORD   = os.getenv('CAPITAL_PASSWORD',   '')
DEMO_MODE          = os.getenv('CAPITAL_DEMO', 'true').lower() == 'true'


# ── State helpers ─────────────────────────────────────────────────────────────
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}

def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def today_ist():
    return datetime.now(IST).strftime("%Y-%m-%d")

def fresh_state():
    return {
        "date":            today_ist(),
        "candle_high":     None,
        "candle_high_ask": None,
        "candle_low":      None,
        "candle_range":    None,
        "trades_today":    0,
        "t1_direction":    None,
        "t2_direction":    None,
        "t1_sl_hit":       False,
        "t1_tp_hit":       False,
        "t2_tp_hit":       False,
        "t2_sl_hit":       False,
        "active_deal_id":  None,
        "active_tp":       None,
        "active_sl":       None,
        "active_dir":      None,
        "done_for_day":    False,
        # Stop-order breakout entry
        "orders_placed":   False,  # True once BUY/SELL stop orders are live
        "buy_order_id":    None,   # working-order dealId for BUY stop
        "sell_order_id":   None,   # working-order dealId for SELL stop
        "buy_entry":       None,   # planned BUY entry level (H_ask + STOP_BUFFER)
        "sell_entry":      None,   # planned SELL entry level (L - STOP_BUFFER)
        "buy_sl":          None,
        "sell_sl":         None,
        "buy_tp":          None,
        "sell_tp":         None,
        "buy_size":        None,
        "sell_size":       None,
    }


# ── Candle fetch ──────────────────────────────────────────────────────────────
def fetch_1200_candle(api: CapitalComAPI, candle_date_str: str) -> Optional[dict]:
    """
    Fetch the DAX open 30-min candle (DST-aware: 06:30–07:00 UTC in CEST, 07:30–08:00 UTC in CET).
    candle_date_str: IST date string "YYYY-MM-DD"
    Returns {high, high_ask, low, close, range, ts} or None if not available.
    """
    today                            = datetime.strptime(candle_date_str, "%Y-%m-%d").date()
    open_h, open_m, close_h, close_m = candle_utc_hours(today)
    from_ts  = datetime(today.year, today.month, today.day,
                        open_h, open_m, 0, tzinfo=timezone.utc)
    to_ts    = datetime(today.year, today.month, today.day,
                        close_h, close_m, 0, tzinfo=timezone.utc) - timedelta(seconds=1)
    from_str = from_ts.strftime("%Y-%m-%dT%H:%M:%S")
    to_str   = to_ts.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        api._ensure_session()
        resp = api._session.get(
            f"{api.base_url}/api/v1/prices/{EPIC}",
            params={
                "resolution": "MINUTE_30",
                "from": from_str,
                "to":   to_str,
            },
            timeout=15
        )
        resp.raise_for_status()
        bars = resp.json().get("prices", [])

        if not bars:
            log.debug("No bar returned for 06:30 UTC window yet")
            return None

        bar   = bars[0]
        ts    = bar.get("snapshotTimeUTC", bar.get("snapshotTime", "?"))
        h_bid = bar.get("highPrice",  {}).get("bid", 0)
        h_ask = bar.get("highPrice",  {}).get("ask", h_bid)
        l_bid = bar.get("lowPrice",   {}).get("bid", 0)
        c     = bar.get("closePrice", {}).get("bid", 0)
        r     = round(h_bid - l_bid, 2)

        log.info(f"  [CANDLE] 12:00 PM IST bar: ts={ts} | H={h_bid} (ask={h_ask}) "
                 f"L={l_bid} Range={r}pts")
        return {"high": h_bid, "high_ask": h_ask, "low": l_bid, "close": c, "range": r, "ts": ts}

    except Exception as e:
        log.warning(f"  [CANDLE] Failed to fetch: {e}")
        return None


# ── Position sizing ───────────────────────────────────────────────────────────
def compute_size(sl_distance: float) -> float:
    size = FIXED_SL_AED / (sl_distance * PIP_VALUE_EUR * EUR_TO_AED)
    return round(max(0.1, round(size, 1)), 1)


# ── Active position check ─────────────────────────────────────────────────────
def get_ger40_position(api: CapitalComAPI) -> Optional[dict]:
    positions = api.get_all_positions()
    for p in positions:
        if p.get("market", {}).get("epic") == EPIC:
            return p
    return None


# ── Close any open GER40 position and cancel working orders ───────────────────
def close_ger40_position(api: CapitalComAPI, reason: str = ""):
    pos = get_ger40_position(api)
    if pos:
        deal_id = pos["position"]["dealId"]
        log.info(f"  [CLOSE] Closing GER40 position {deal_id} | {reason}")
        api.close_position(deal_id)
        time.sleep(1)
    # Also cancel any working stop orders for this epic
    try:
        orders = api.get_working_orders()
        for o in orders:
            if o.get("workingOrderData", {}).get("epic") == EPIC:
                api.cancel_working_order(o["workingOrderData"]["dealId"])
                log.info("  [CLOSE] Cancelled working order")
    except Exception as e:
        log.warning(f"  [CLOSE] Cancel orders failed: {e}")


# ── Stop-order helpers ────────────────────────────────────────────────────────
def get_ger40_working_order_ids(api: CapitalComAPI) -> Optional[set]:
    """Returns set of active working-order dealIds for DE40, or None on API failure."""
    try:
        orders = api.get_working_orders()
        return {
            o["workingOrderData"]["dealId"]
            for o in orders
            if o.get("workingOrderData", {}).get("epic") == EPIC
        }
    except Exception as e:
        log.warning(f"  [ORDERS] Failed to fetch working orders: {e}")
        return None


def place_breakout_stop(api: CapitalComAPI, direction: str, entry_level: float,
                        sl: float, tp: float, size: float, label: str) -> Optional[str]:
    """
    Place a stop working order for breakout entry.
    Returns the working-order dealId on acceptance, None on failure.
    """
    log.info(f"  [STOP ORDER] {direction} @ {entry_level} | SL={sl} TP={tp} size={size} | {label}")
    try:
        r = api.place_stop_order(
            epic=EPIC, direction=direction, size=size,
            level=entry_level, stop_level=sl, profit_level=tp
        )
        time.sleep(0.5)
        confirm = api.confirm_deal(r.get("dealReference", ""))
        status  = confirm.get("dealStatus", "?")
        deal_id = confirm.get("dealId", "")
        log.info(f"  [STOP ORDER] {status} | dealId={deal_id}")
        if status == "ACCEPTED":
            return deal_id
        log.warning(f"  [STOP ORDER] Not accepted: {confirm}")
    except Exception as e:
        log.error(f"  [STOP ORDER] Failed: {e}")
    return None


def place_market_order(api: CapitalComAPI, direction: str, sl: float,
                       tp: float, size: float, label: str) -> Optional[str]:
    """
    Place a market order (immediate fill) as fallback when stop order is rejected.
    Returns the position dealId on success, None on failure.
    """
    log.warning(f"  [MARKET FALLBACK] Placing MARKET {direction} | SL={sl} TP={tp} size={size} | {label}")
    try:
        r = api.open_position(
            epic=EPIC, direction=direction, size=size,
            stop_level=sl, profit_level=tp
        )
        time.sleep(0.5)
        confirm = api.confirm_deal(r.get("dealReference", ""))
        status  = confirm.get("dealStatus", "?")
        deal_id = confirm.get("dealId", "")
        log.info(f"  [MARKET FALLBACK] {status} | dealId={deal_id}")
        if status == "ACCEPTED":
            return deal_id
        log.warning(f"  [MARKET FALLBACK] Not accepted: {confirm}")
    except Exception as e:
        log.error(f"  [MARKET FALLBACK] Failed: {e}")
    return None


# ── Main loop ─────────────────────────────────────────────────────────────────
def run():
    api = CapitalComAPI(
        api_key=CAPITAL_API_KEY,
        identifier=CAPITAL_IDENTIFIER,
        password=CAPITAL_PASSWORD,
        demo=DEMO_MODE
    )
    api.create_session()
    acc     = api.get_account_info()
    balance = acc.get('balance', {}).get('balance', 0)
    mode    = "DEMO" if DEMO_MODE else "LIVE"
    log.info("=" * 60)
    log.info("  GER40 12:00 PM IST Runner — STARTED")
    log.info(f"  Account: CFD | Currency: AED | Mode: {mode}")
    log.info(f"  Balance: AED {balance:,.2f}")
    log.info("=" * 60)

    state = load_state()

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc.astimezone(IST)
            today   = today_ist()

            # ── New day — reset state ─────────────────────────────────────
            if state.get("date") != today:
                log.info(f"  [DAY] New IST day {today} — resetting state")
                state = fresh_state()
                save_state(state)

            # ── Weekend guard (market closed Sat & Sun UTC) ───────────────
            if now_utc.weekday() >= 5:
                log.info(f"  [WEEKEND] Market closed ({now_utc.strftime('%A')}) — sleeping 1h")
                time.sleep(3600)
                continue

            # ── Done for day — sleep until next day ───────────────────────
            if state["done_for_day"]:
                log.debug("  [DAY] Done for today — waiting for tomorrow")
                time.sleep(CHECK_EVERY)
                continue

            # ── Step 1: Capture 12:00 PM IST candle (after 07:00 UTC) ─────
            if state["candle_high"] is None:
                state_date              = datetime.strptime(state["date"], "%Y-%m-%d").date()
                _, _, close_h, close_m  = candle_utc_hours(state_date)
                candle_close_utc        = datetime(
                    state_date.year, state_date.month, state_date.day,
                    close_h, close_m, 0, tzinfo=timezone.utc)

                if now_utc < candle_close_utc:
                    log.info(f"  [WAIT] Waiting for 12:00 PM IST candle to close | "
                             f"Now={now_ist.strftime('%H:%M')} IST | "
                             f"Candle closes {close_h:02d}:{close_m:02d} UTC")
                    time.sleep(CHECK_EVERY)
                    continue

                # Close anything running from previous day before fresh trade
                close_ger40_position(api, "Pre-candle cleanup — closing previous day trade")
                state["active_deal_id"] = None
                state["t1_sl_hit"]      = False
                state["trades_today"]   = 0
                save_state(state)

                candle = fetch_1200_candle(api, state["date"])
                if candle is None:
                    log.info("  [WAIT] Candle not available yet — retrying")
                    time.sleep(30)
                    continue

                state["candle_high"]     = candle["high"]
                state["candle_high_ask"] = candle["high_ask"]
                state["candle_low"]      = candle["low"]
                state["candle_range"]    = candle["range"]
                save_state(state)
                log.info(f"  [CANDLE] Captured ✅ H={candle['high']} (ask={candle['high_ask']}) "
                         f"L={candle['low']} Range={candle['range']}pts")

            H     = state["candle_high"]
            H_ask = state.get("candle_high_ask", H)  # ask high — used for BUY entry & SELL SL
            L     = state["candle_low"]
            R     = state["candle_range"]

            # ── Step 2: Monitor active trade ──────────────────────────────
            if state["active_deal_id"]:
                pos = get_ger40_position(api)

                if pos is None:
                    # Position closed by Capital.com (TP or SL hit)
                    log.info("  [MONITOR] Position closed by Capital.com")

                    try:
                        cp      = api.get_current_price(EPIC)
                        current = cp.get("bid", 0)
                    except Exception:
                        current = (H + L) / 2

                    active_tp  = state.get("active_tp")
                    active_sl  = state.get("active_sl")
                    active_dir = state.get("active_dir")
                    if active_tp and active_sl and active_dir == "BUY":
                        tp_hit = (active_tp - current) <= (current - active_sl)
                    elif active_tp and active_sl and active_dir == "SELL":
                        tp_hit = (current - active_tp) <= (active_sl - current)
                    else:
                        tp_hit = False

                    state["active_deal_id"] = None
                    state["active_tp"]      = None
                    state["active_sl"]      = None
                    state["active_dir"]     = None

                    if state["trades_today"] >= 2:
                        # T2 just closed (TP or SL) — done for day either way
                        state["t2_tp_hit"]    = tp_hit
                        state["t2_sl_hit"]    = not tp_hit
                        state["done_for_day"] = True
                        save_state(state)
                        result = "TP ✅" if tp_hit else "SL ❌"
                        log.info(f"  [RESULT] T2 closed — {result} — done for day")
                        time.sleep(CHECK_EVERY)
                        continue
                    elif tp_hit:
                        # Cancel any working orders for this epic — covers the case where
                        # the T2 pre-arm was placed successfully but confirm_deal returned
                        # 404, so sell_order_id/buy_order_id may be None even though an
                        # order is live in the broker.
                        try:
                            working = api.get_working_orders()
                            for o in working:
                                if o.get("workingOrderData", {}).get("epic") == EPIC:
                                    oid = o["workingOrderData"]["dealId"]
                                    api.cancel_working_order(oid)
                                    log.info(f"  [ORDER] Cancelled working order {oid}")
                        except Exception as e:
                            log.warning(f"  [ORDER] Cancel working orders failed: {e}")
                        state["t1_tp_hit"]     = True
                        state["done_for_day"]  = True
                        state["orders_placed"] = False
                        state["buy_order_id"]  = None
                        state["sell_order_id"] = None
                        save_state(state)
                        log.info("  [RESULT] T1 TP hit — done for day ✅")
                        time.sleep(CHECK_EVERY)
                        continue
                    else:
                        # T1 SL hit
                        state["t1_sl_hit"] = True
                        save_state(state)
                        if state.get("orders_placed"):
                            log.info("  [RESULT] T1 SL hit — T2 stop already pre-armed ✅")
                        else:
                            log.info("  [RESULT] T1 SL hit — arming T2 reversal")

                else:
                    # Position still open — just monitor
                    p_data   = pos.get("position", {})
                    profit   = p_data.get("profit", 0)
                    deal_dir = p_data.get("direction", "?")
                    log.info(f"  [MONITOR] DE40 {deal_dir} open | P&L={profit:+.2f}")
                    time.sleep(CHECK_EVERY)
                    continue

            # ── Step 3: Max trades guard ───────────────────────────────────
            if state["trades_today"] >= 2 or state["done_for_day"]:
                log.info("  [GUARD] Max 2 trades reached — done for day")
                state["done_for_day"] = True
                save_state(state)
                time.sleep(CHECK_EVERY)
                continue

            # ── Step 4: Place breakout stop orders once after candle capture ─
            # Instead of polling price and firing market orders (which fill
            # multiple pts past H/L due to latency), place stop working orders
            # at the exact breakout levels immediately. The broker fires them
            # at H+STOP_BUFFER / L-STOP_BUFFER with pre-computed SL/TP/size.
            if state["trades_today"] == 0 and not state.get("orders_placed"):
                buy_entry  = round(H_ask + STOP_BUFFER, 2)   # BUY triggers on ask
                sell_entry = round(L   - STOP_BUFFER, 2)     # SELL triggers on bid

                buy_sl    = round(L - SL_BUFFER, 2)
                buy_dist  = round(buy_entry - buy_sl, 2)
                buy_tp    = round(buy_entry + RR_RATIO * buy_dist, 2)
                buy_size  = compute_size(buy_dist)

                sell_sl   = round(H_ask + SL_BUFFER, 2)      # SELL SL on ask (BUY would trigger here)
                sell_dist = round(sell_sl - sell_entry, 2)
                sell_tp   = round(sell_entry - RR_RATIO * sell_dist, 2)
                sell_size = compute_size(sell_dist)

                log.info(f"  [ORDERS] Placing breakout stop orders | "
                         f"BUY@{buy_entry} SL={buy_sl} TP={buy_tp} sz={buy_size} | "
                         f"SELL@{sell_entry} SL={sell_sl} TP={sell_tp} sz={sell_size}")

                # Check current price — fallback to market if stop rejected near entry
                try:
                    cp      = api.get_current_price(EPIC)
                    cur_bid = cp.get("bid", 0)
                    cur_ask = cp.get("offer", cur_bid + 1.0)
                except Exception:
                    cur_bid = (H + L) / 2
                    cur_ask = cur_bid + 1.0

                # ── BUY side ──────────────────────────────────────────────
                buy_oid         = None
                buy_market_deal = None
                if cur_ask >= buy_entry:
                    buy_slip = round(cur_ask - buy_entry, 2)
                    if buy_slip > MAX_ENTRY_SLIP:
                        log.warning(f"  [ORDERS] Ask {cur_ask} is {buy_slip}pts past BUY entry "
                                    f"{buy_entry} — exceeds MAX_ENTRY_SLIP ({MAX_ENTRY_SLIP}pts) "
                                    f"— skipping market BUY")
                    else:
                        mkt_sl_dist = round(cur_ask - buy_sl, 2)
                        buy_tp      = round(cur_ask + RR_RATIO * mkt_sl_dist, 2)
                        buy_size    = compute_size(mkt_sl_dist)
                        log.warning(f"  [ORDERS] Ask {cur_ask} at/above BUY entry {buy_entry} "
                                    f"(slip={buy_slip}pts) — MARKET BUY recalculated: "
                                    f"SL={buy_sl} TP={buy_tp} sz={buy_size}")
                        buy_market_deal = place_market_order(api, "BUY", buy_sl, buy_tp, buy_size, "T1 BUY market")
                else:
                    buy_oid = place_breakout_stop(api, "BUY", buy_entry, buy_sl, buy_tp, buy_size, "T1 BUY stop")
                    if buy_oid is None:
                        # Stop rejected — possibly minimum-distance violation; re-check price
                        try:
                            cp2     = api.get_current_price(EPIC)
                            cur_ask = cp2.get("offer", cur_ask)
                        except Exception:
                            pass
                        if cur_ask >= buy_entry - 2.0:
                            buy_slip = max(0.0, round(cur_ask - buy_entry, 2))
                            if buy_slip > MAX_ENTRY_SLIP:
                                log.warning(f"  [ORDERS] BUY stop rejected, ask {cur_ask} is "
                                            f"{buy_slip}pts past entry {buy_entry} — exceeds "
                                            f"MAX_ENTRY_SLIP ({MAX_ENTRY_SLIP}pts) — skipping market BUY")
                            else:
                                mkt_sl_dist = round(cur_ask - buy_sl, 2)
                                buy_tp      = round(cur_ask + RR_RATIO * mkt_sl_dist, 2)
                                buy_size    = compute_size(mkt_sl_dist)
                                log.warning(f"  [ORDERS] BUY stop rejected, ask {cur_ask} near entry "
                                            f"{buy_entry} (slip={buy_slip}pts) — MARKET BUY recalculated: "
                                            f"SL={buy_sl} TP={buy_tp} sz={buy_size}")
                                buy_market_deal = place_market_order(api, "BUY", buy_sl, buy_tp, buy_size, "T1 BUY market fallback")

                # ── SELL side (skip entirely if BUY market already fired) ──
                sell_oid         = None
                sell_market_deal = None
                if buy_market_deal is not None:
                    pass  # BUY market live — avoid placing a competing SELL stop
                elif cur_bid <= sell_entry:
                    sell_slip = round(sell_entry - cur_bid, 2)
                    if sell_slip > MAX_ENTRY_SLIP:
                        log.warning(f"  [ORDERS] Bid {cur_bid} is {sell_slip}pts past SELL entry "
                                    f"{sell_entry} — exceeds MAX_ENTRY_SLIP ({MAX_ENTRY_SLIP}pts) "
                                    f"— skipping market SELL")
                    else:
                        mkt_sl_dist = round(sell_sl - cur_bid, 2)
                        sell_tp     = round(cur_bid - RR_RATIO * mkt_sl_dist, 2)
                        sell_size   = compute_size(mkt_sl_dist)
                        log.warning(f"  [ORDERS] Bid {cur_bid} at/below SELL entry {sell_entry} "
                                    f"(slip={sell_slip}pts) — MARKET SELL recalculated: "
                                    f"SL={sell_sl} TP={sell_tp} sz={sell_size}")
                        sell_market_deal = place_market_order(api, "SELL", sell_sl, sell_tp, sell_size, "T1 SELL market")
                else:
                    sell_oid = place_breakout_stop(api, "SELL", sell_entry, sell_sl, sell_tp, sell_size, "T1 SELL stop")
                    if sell_oid is None:
                        try:
                            cp2     = api.get_current_price(EPIC)
                            cur_bid = cp2.get("bid", cur_bid)
                        except Exception:
                            pass
                        if cur_bid <= sell_entry + 2.0:
                            sell_slip = max(0.0, round(sell_entry - cur_bid, 2))
                            if sell_slip > MAX_ENTRY_SLIP:
                                log.warning(f"  [ORDERS] SELL stop rejected, bid {cur_bid} is "
                                            f"{sell_slip}pts past entry {sell_entry} — exceeds "
                                            f"MAX_ENTRY_SLIP ({MAX_ENTRY_SLIP}pts) — skipping market SELL")
                            else:
                                mkt_sl_dist = round(sell_sl - cur_bid, 2)
                                sell_tp     = round(cur_bid - RR_RATIO * mkt_sl_dist, 2)
                                sell_size   = compute_size(mkt_sl_dist)
                                log.warning(f"  [ORDERS] SELL stop rejected, bid {cur_bid} near entry "
                                            f"{sell_entry} (slip={sell_slip}pts) — MARKET SELL recalculated: "
                                            f"SL={sell_sl} TP={sell_tp} sz={sell_size}")
                                sell_market_deal = place_market_order(api, "SELL", sell_sl, sell_tp, sell_size, "T1 SELL market fallback")

                # ── T1 BUY filled via market — pre-arm T2 SELL ───────────
                if buy_market_deal:
                    if sell_oid:
                        try:
                            api.cancel_working_order(sell_oid)
                            log.info(f"  [ORDERS] Cancelled competing T1 SELL stop {sell_oid}")
                        except Exception as e:
                            log.warning(f"  [ORDERS] Cancel SELL stop failed: {e}")
                    t2_oid = place_breakout_stop(api, "SELL", sell_entry, sell_sl,
                                                 sell_tp, sell_size, "T2 SELL pre-arm")
                    if t2_oid:
                        log.info("  [T2 PRE-ARM] ✅ SELL stop pre-armed (market fallback path)")
                    else:
                        log.warning("  [T2 PRE-ARM] Failed — Step 6 will retry after T1 SL hits")
                    state["trades_today"]   = 1
                    state["t1_direction"]   = "BUY"
                    state["active_deal_id"] = buy_market_deal
                    state["active_tp"]      = buy_tp
                    state["active_sl"]      = buy_sl
                    state["active_dir"]     = "BUY"
                    state["sell_order_id"]  = t2_oid
                    state["buy_order_id"]   = None
                    state["orders_placed"]  = t2_oid is not None
                    state["buy_entry"]      = buy_entry
                    state["sell_entry"]     = sell_entry
                    state["buy_sl"]         = buy_sl
                    state["sell_sl"]        = sell_sl
                    state["buy_tp"]         = buy_tp
                    state["sell_tp"]        = sell_tp
                    state["buy_size"]       = buy_size
                    state["sell_size"]      = sell_size
                    save_state(state)
                    log.info(f"  [FILL] T1 BUY market | dealId={buy_market_deal} | SL={buy_sl} TP={buy_tp}")
                    time.sleep(CHECK_FAST)
                    continue

                # ── T1 SELL filled via market — pre-arm T2 BUY ───────────
                if sell_market_deal:
                    if buy_oid:
                        try:
                            api.cancel_working_order(buy_oid)
                            log.info(f"  [ORDERS] Cancelled competing T1 BUY stop {buy_oid}")
                        except Exception as e:
                            log.warning(f"  [ORDERS] Cancel BUY stop failed: {e}")
                    t2_oid = place_breakout_stop(api, "BUY", buy_entry, buy_sl,
                                                 buy_tp, buy_size, "T2 BUY pre-arm")
                    if t2_oid:
                        log.info("  [T2 PRE-ARM] ✅ BUY stop pre-armed (market fallback path)")
                    else:
                        log.warning("  [T2 PRE-ARM] Failed — Step 6 will retry after T1 SL hits")
                    state["trades_today"]   = 1
                    state["t1_direction"]   = "SELL"
                    state["active_deal_id"] = sell_market_deal
                    state["active_tp"]      = sell_tp
                    state["active_sl"]      = sell_sl
                    state["active_dir"]     = "SELL"
                    state["buy_order_id"]   = t2_oid
                    state["sell_order_id"]  = None
                    state["orders_placed"]  = t2_oid is not None
                    state["buy_entry"]      = buy_entry
                    state["sell_entry"]     = sell_entry
                    state["buy_sl"]         = buy_sl
                    state["sell_sl"]        = sell_sl
                    state["buy_tp"]         = buy_tp
                    state["sell_tp"]        = sell_tp
                    state["buy_size"]       = buy_size
                    state["sell_size"]      = sell_size
                    save_state(state)
                    log.info(f"  [FILL] T1 SELL market | dealId={sell_market_deal} | SL={sell_sl} TP={sell_tp}")
                    time.sleep(CHECK_FAST)
                    continue

                # ── Both sides failed entirely ─────────────────────────────
                if buy_oid is None and sell_oid is None:
                    log.error("  [ORDERS] Both stop orders failed — retrying next cycle")
                    time.sleep(CHECK_FAST)
                    continue

                state["orders_placed"]  = True
                state["buy_order_id"]   = buy_oid
                state["sell_order_id"]  = sell_oid
                state["buy_entry"]      = buy_entry
                state["sell_entry"]     = sell_entry
                state["buy_sl"]         = buy_sl
                state["sell_sl"]        = sell_sl
                state["buy_tp"]         = buy_tp
                state["sell_tp"]        = sell_tp
                state["buy_size"]       = buy_size
                state["sell_size"]      = sell_size
                save_state(state)
                time.sleep(CHECK_FAST)
                continue

            # ── Step 5: Monitor stop orders — detect T1 fill ──────────────
            if state["trades_today"] == 0 and state.get("orders_placed"):
                active_ids = get_ger40_working_order_ids(api)
                if active_ids is None:   # API error — skip cycle
                    time.sleep(CHECK_FAST)
                    continue

                buy_alive  = state.get("buy_order_id")  in active_ids
                sell_alive = state.get("sell_order_id") in active_ids
                pos        = get_ger40_position(api)

                filled_dir = None
                cancel_oid = None
                active_sl  = None
                active_tp  = None

                if state.get("buy_order_id") and not buy_alive and pos is not None:
                    filled_dir = "BUY"
                    cancel_oid = state.get("sell_order_id")
                    active_sl  = state["buy_sl"]
                    active_tp  = state["buy_tp"]
                elif state.get("sell_order_id") and not sell_alive and pos is not None:
                    filled_dir = "SELL"
                    cancel_oid = state.get("buy_order_id")
                    active_sl  = state["sell_sl"]
                    active_tp  = state["sell_tp"]
                else:
                    log.info(f"  [WAIT] Stop orders live | "
                             f"BUY@{state.get('buy_entry')} SELL@{state.get('sell_entry')}")
                    time.sleep(CHECK_FAST)
                    continue

                # Cancel the unfilled opposite order
                if cancel_oid:
                    try:
                        api.cancel_working_order(cancel_oid)
                        log.info(f"  [ORDER] Cancelled opposite stop order {cancel_oid}")
                    except Exception as e:
                        log.warning(f"  [ORDER] Cancel failed (may already be gone): {e}")

                deal_id    = pos["position"]["dealId"]
                fill_price = pos["position"].get("openLevel", "?")
                log.info(f"  [FILL] T1 {filled_dir} stop filled @ {fill_price} | "
                         f"dealId={deal_id} | SL={active_sl} TP={active_tp}")

                # Pre-arm T2 immediately while T1 is running.
                # The opposite candle boundary is far enough away that the stop level
                # is valid. If T1 hits TP we cancel it; if T1 hits SL the stop is
                # already live and triggers without any race condition.
                t2_oid = None
                if filled_dir == "BUY":
                    t2_entry = state["sell_entry"]
                    t2_sl    = state["sell_sl"]
                    t2_tp    = state["sell_tp"]
                    t2_size  = state["sell_size"]
                    log.info(f"  [T2 PRE-ARM] SELL stop @ {t2_entry} | "
                             f"SL={t2_sl} TP={t2_tp} sz={t2_size}")
                    t2_oid = place_breakout_stop(api, "SELL", t2_entry, t2_sl,
                                                 t2_tp, t2_size,
                                                 "T2 SELL stop pre-armed")
                    if t2_oid:
                        log.info("  [T2 PRE-ARM] ✅ Live — triggers only if price falls to T1 SL")
                    else:
                        log.warning("  [T2 PRE-ARM] Failed — will retry in Step 6 after T1 SL hits")
                    state["sell_order_id"] = t2_oid
                    state["buy_order_id"]  = None

                elif filled_dir == "SELL":
                    t2_entry = state["buy_entry"]
                    t2_sl    = state["buy_sl"]
                    t2_tp    = state["buy_tp"]
                    t2_size  = state["buy_size"]
                    log.info(f"  [T2 PRE-ARM] BUY stop @ {t2_entry} | "
                             f"SL={t2_sl} TP={t2_tp} sz={t2_size}")
                    t2_oid = place_breakout_stop(api, "BUY", t2_entry, t2_sl,
                                                 t2_tp, t2_size,
                                                 "T2 BUY stop pre-armed")
                    if t2_oid:
                        log.info("  [T2 PRE-ARM] ✅ Live — triggers only if price rises to T1 SL")
                    else:
                        log.warning("  [T2 PRE-ARM] Failed — will retry in Step 6 after T1 SL hits")
                    state["buy_order_id"]  = t2_oid
                    state["sell_order_id"] = None

                state["trades_today"]   = 1
                state["t1_direction"]   = filled_dir
                state["active_deal_id"] = deal_id
                state["active_tp"]      = active_tp
                state["active_sl"]      = active_sl
                state["active_dir"]     = filled_dir
                state["orders_placed"]  = t2_oid is not None
                save_state(state)
                time.sleep(CHECK_FAST)
                continue

            # ── Step 6: T2 reversal — place stop order if not pre-armed ──────
            elif state["trades_today"] == 1 and state["t1_sl_hit"] \
                    and not state.get("orders_placed"):
                t1 = state["t1_direction"]

                if t1 == "BUY":
                    # T1 BUY SL hit near L → T2 SELL stop just below L
                    sell_entry = round(L   - STOP_BUFFER, 2)
                    sell_sl    = round(H_ask + SL_BUFFER, 2)
                    sell_dist  = round(sell_sl - sell_entry, 2)
                    sell_tp    = round(sell_entry - RR_RATIO * sell_dist, 2)
                    sell_size  = compute_size(sell_dist)
                    log.info(f"  [T2 ORDER] SELL stop @ {sell_entry} | "
                             f"SL={sell_sl} TP={sell_tp} sz={sell_size}")
                    oid = place_breakout_stop(api, "SELL", sell_entry, sell_sl,
                                             sell_tp, sell_size,
                                             "T2 REVERSAL SELL stop")
                    if oid:
                        state["orders_placed"]  = True
                        state["sell_order_id"]  = oid
                        state["sell_entry"]     = sell_entry
                        state["sell_sl"]        = sell_sl
                        state["sell_tp"]        = sell_tp
                        state["sell_size"]      = sell_size
                        state["buy_order_id"]   = None
                        save_state(state)
                    else:
                        # Stop rejected — price likely at sell_entry since T1 BUY SL just hit
                        deal_id = place_market_order(api, "SELL", sell_sl, sell_tp, sell_size,
                                                     "T2 REVERSAL SELL market fallback")
                        if deal_id:
                            state["orders_placed"]  = False
                            state["sell_order_id"]  = None
                            state["buy_order_id"]   = None
                            state["trades_today"]   = 2
                            state["t2_direction"]   = "SELL"
                            state["active_deal_id"] = deal_id
                            state["active_tp"]      = sell_tp
                            state["active_sl"]      = sell_sl
                            state["active_dir"]     = "SELL"
                            save_state(state)
                            log.info(f"  [FILL] T2 SELL market | dealId={deal_id} | SL={sell_sl} TP={sell_tp}")
                        else:
                            log.error("  [T2 ORDER] SELL stop + market fallback both failed — retrying next cycle")

                elif t1 == "SELL":
                    # T1 SELL SL hit near H → T2 BUY stop just above H
                    buy_entry = round(H_ask + STOP_BUFFER, 2)
                    buy_sl    = round(L - SL_BUFFER, 2)
                    buy_dist  = round(buy_entry - buy_sl, 2)
                    buy_tp    = round(buy_entry + RR_RATIO * buy_dist, 2)
                    buy_size  = compute_size(buy_dist)
                    log.info(f"  [T2 ORDER] BUY stop @ {buy_entry} | "
                             f"SL={buy_sl} TP={buy_tp} sz={buy_size}")
                    oid = place_breakout_stop(api, "BUY", buy_entry, buy_sl,
                                             buy_tp, buy_size,
                                             "T2 REVERSAL BUY stop")
                    if oid:
                        state["orders_placed"]  = True
                        state["buy_order_id"]   = oid
                        state["buy_entry"]      = buy_entry
                        state["buy_sl"]         = buy_sl
                        state["buy_tp"]         = buy_tp
                        state["buy_size"]       = buy_size
                        state["sell_order_id"]  = None
                        save_state(state)
                    else:
                        # Stop rejected — price likely at buy_entry since T1 SELL SL just hit
                        deal_id = place_market_order(api, "BUY", buy_sl, buy_tp, buy_size,
                                                     "T2 REVERSAL BUY market fallback")
                        if deal_id:
                            state["orders_placed"]  = False
                            state["buy_order_id"]   = None
                            state["sell_order_id"]  = None
                            state["trades_today"]   = 2
                            state["t2_direction"]   = "BUY"
                            state["active_deal_id"] = deal_id
                            state["active_tp"]      = buy_tp
                            state["active_sl"]      = buy_sl
                            state["active_dir"]     = "BUY"
                            save_state(state)
                            log.info(f"  [FILL] T2 BUY market | dealId={deal_id} | SL={buy_sl} TP={buy_tp}")
                        else:
                            log.error("  [T2 ORDER] BUY stop + market fallback both failed — retrying next cycle")

            # ── Step 7: Monitor T2 stop order for fill ────────────────────
            elif state["trades_today"] == 1 and state["t1_sl_hit"] \
                    and state.get("orders_placed"):
                active_ids = get_ger40_working_order_ids(api)
                if active_ids is None:
                    time.sleep(CHECK_FAST)
                    continue

                t1         = state["t1_direction"]
                pos        = get_ger40_position(api)
                filled_dir = None
                active_sl  = None
                active_tp  = None
                oid        = state.get("sell_order_id") if t1 == "BUY" else state.get("buy_order_id")

                if oid and oid not in active_ids and pos is not None:
                    filled_dir = "SELL" if t1 == "BUY" else "BUY"
                    active_sl  = state["sell_sl"] if t1 == "BUY" else state["buy_sl"]
                    active_tp  = state["sell_tp"] if t1 == "BUY" else state["buy_tp"]
                elif oid and oid not in active_ids and pos is None:
                    # T2 stop filled AND position already closed before this cycle
                    t2_dir = "SELL" if t1 == "BUY" else "BUY"
                    log.info(f"  [RESULT] T2 {t2_dir} filled & closed before detection — SL ❌ — done for day")
                    state["trades_today"]   = 2
                    state["t2_direction"]   = t2_dir
                    state["t2_sl_hit"]      = True
                    state["done_for_day"]   = True
                    state["orders_placed"]  = False
                    state["buy_order_id"]   = None
                    state["sell_order_id"]  = None
                    save_state(state)
                    time.sleep(CHECK_FAST)
                    continue
                else:
                    entry_lvl = state.get("sell_entry") if t1 == "BUY" else state.get("buy_entry")
                    log.info(f"  [WAIT] T2 stop order live | "
                             f"{'SELL' if t1 == 'BUY' else 'BUY'}@{entry_lvl}")
                    time.sleep(CHECK_FAST)
                    continue

                deal_id    = pos["position"]["dealId"]
                fill_price = pos["position"].get("openLevel", "?")
                log.info(f"  [FILL] T2 {filled_dir} stop filled @ {fill_price} | "
                         f"dealId={deal_id} | SL={active_sl} TP={active_tp}")

                state["trades_today"]   = 2
                state["t2_direction"]   = filled_dir
                state["active_deal_id"] = deal_id
                state["active_tp"]      = active_tp
                state["active_sl"]      = active_sl
                state["active_dir"]     = filled_dir
                state["t1_sl_hit"]      = False
                state["orders_placed"]  = False
                state["buy_order_id"]   = None
                state["sell_order_id"]  = None
                save_state(state)

        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)

        # Fast polling once candle is captured, slow before
        time.sleep(CHECK_FAST if state.get("candle_high") else CHECK_EVERY)


if __name__ == "__main__":
    run()
