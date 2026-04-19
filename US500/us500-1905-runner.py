"""
US500 19:05 IST Breakout — Daily Runner
========================================
Simple, focused script for one strategy only.

Logic:
  1. At 13:40 UTC daily — fetch the 13:35 candle directly by date range
  2. Price above candle H → BUY  | Price below candle L → SELL
  3. SL = opposite side of candle | TP = entry ± 3× range (1:3 R:R)
  4. If T1 SL hit → reverse trade (T2)
  5. If T1 TP hit → done for day
  6. Trade runs overnight — TP/SL managed by broker
  7. Close any open trade at 13:40 UTC next day before fresh candle
  8. Max 2 trades per day

Run this script continuously — it sleeps between checks.
"""

import time
import logging
import json
import os
from typing import Optional
from datetime import datetime, timezone, timedelta
from capitalcom_api import CapitalComAPI

# ── Config ────────────────────────────────────────────────────────────────────
EPIC          = "US500"
FIXED_SL_AED  = 50.0        # fixed risk per trade in AED
FIXED_TP_AED  = 150.0       # fixed profit target in AED (1:3)
RR_RATIO      = 3.0         # 1:3
STOP_BUFFER   = 0.1         # points above/below candle H/L for entry
CHECK_EVERY   = 30          # seconds between scans (pre-candle)
CHECK_FAST    = 0.5         # seconds between scans (post-candle, waiting for breakout)
PIP_VALUE_USD = 1.0         # US500: 1pt = $1/unit
USD_TO_AED    = 3.67
STATE_FILE    = "state/us500_runner_state.json"

# ── IST timezone ──────────────────────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/us500_runner.log")
    ]
)
log = logging.getLogger("us500")

# ── Credentials (same as bot.py — env vars or hardcoded fallback) ─────────────
import os
from dotenv import load_dotenv
load_dotenv()

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

def candle_utc_hour() -> int:
    """
    Returns UTC hour of the opening candle, adjusted for US DST.
    EDT (mid-Mar to early Nov): NY opens 13:30 UTC → candle at 13:35 UTC
    EST (early Nov to mid-Mar): NY opens 14:30 UTC → candle at 14:35 UTC
    """
    today = datetime.now(timezone.utc).date()
    year  = today.year
    mar1  = datetime(year, 3, 1, tzinfo=timezone.utc).date()
    dst_start = mar1 + timedelta(days=(6 - mar1.weekday()) % 7 + 7)  # 2nd Sunday March
    nov1  = datetime(year, 11, 1, tzinfo=timezone.utc).date()
    dst_end   = nov1 + timedelta(days=(6 - nov1.weekday()) % 7)       # 1st Sunday November
    return 13 if dst_start <= today < dst_end else 14

def fresh_state():
    return {
        "date":           today_ist(),
        "candle_high":    None,
        "candle_low":     None,
        "candle_range":   None,
        "trades_today":   0,
        "t1_direction":   None,
        "t2_direction":   None,
        "t1_sl_hit":      False,
        "t1_tp_hit":      False,
        "t2_tp_hit":      False,
        "t2_sl_hit":      False,
        "active_deal_id": None,
        "active_tp":      None,   # exact TP level for current trade
        "active_sl":      None,   # exact SL level for current trade
        "active_dir":     None,   # direction of current trade
        "done_for_day":   False,
    }


# ── Candle fetch ──────────────────────────────────────────────────────────────
def fetch_1905_candle(api: CapitalComAPI, candle_date_str: str) -> Optional[dict]:
    """
    Fetch the 19:05 IST (13:35 UTC) 5-min candle directly.
    Uses date-range API call: from=13:33 to=13:38 → only one bar returned.
    candle_date_str: IST date string "YYYY-MM-DD" — the candle UTC date matches this.
    Returns {high, low, close} or None if not available yet.
    """
    today   = datetime.strptime(candle_date_str, "%Y-%m-%d").date()
    ch      = candle_utc_hour()

    # Build exact window: XX:35:00 to XX:39:59 UTC today (DST-adjusted)
    from_ts = datetime(today.year, today.month, today.day,
                       ch, 35, 0, tzinfo=timezone.utc)
    to_ts   = datetime(today.year, today.month, today.day,
                       ch, 39, 59, tzinfo=timezone.utc)

    # Format for Capital.com API: YYYY-MM-DDTHH:MM:SS
    from_str = from_ts.strftime("%Y-%m-%dT%H:%M:%S")
    to_str   = to_ts.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        api._ensure_session()
        resp = api._session.get(
            f"{api.base_url}/api/v1/prices/{EPIC}",
            params={
                "resolution": "MINUTE_5",
                "from": from_str,
                "to":   to_str,
            },
            timeout=15
        )
        resp.raise_for_status()
        bars = resp.json().get("prices", [])

        if not bars:
            log.debug("No bar returned for 13:35 UTC window yet")
            return None

        # Should be exactly 1 bar
        bar = bars[0]
        ts  = bar.get("snapshotTimeUTC", bar.get("snapshotTime", "?"))
        h   = bar.get("highPrice",  {}).get("bid", 0)
        l   = bar.get("lowPrice",   {}).get("bid", 0)
        c   = bar.get("closePrice", {}).get("bid", 0)
        r   = round(h - l, 2)

        log.info(f"  [CANDLE] 19:05 IST bar: ts={ts} | H={h} L={l} Range={r}pts")
        return {"high": h, "low": l, "close": c, "range": r, "ts": ts}

    except Exception as e:
        log.warning(f"  [CANDLE] Failed to fetch: {e}")
        return None


# ── Position sizing ───────────────────────────────────────────────────────────
def compute_size(sl_distance: float) -> float:
    size = FIXED_SL_AED / (sl_distance * PIP_VALUE_USD * USD_TO_AED)
    return round(max(0.1, round(size, 1)), 1)


# ── Active position check ─────────────────────────────────────────────────────
def get_us500_position(api: CapitalComAPI) -> Optional[dict]:
    positions = api.get_all_positions()
    for p in positions:
        if p.get("market", {}).get("epic") == EPIC:
            return p
    return None


# ── Close any open US500 position ─────────────────────────────────────────────
def close_us500_position(api: CapitalComAPI, reason: str = ""):
    pos = get_us500_position(api)
    if pos:
        deal_id = pos["position"]["dealId"]
        log.info(f"  [CLOSE] Closing US500 position {deal_id} | {reason}")
        api.close_position(deal_id)
        time.sleep(1)
    # Also cancel any working stop orders
    try:
        orders = api.get_working_orders()
        for o in orders:
            if o.get("workingOrderData", {}).get("epic") == EPIC:
                api.cancel_working_order(o["workingOrderData"]["dealId"])
                log.info("  [CLOSE] Cancelled working order")
    except Exception as e:
        log.warning(f"  [CLOSE] Cancel orders failed: {e}")


# ── Open a trade ──────────────────────────────────────────────────────────────
def open_trade(api: CapitalComAPI, direction: str,
               sl: float, tp: float, size: float, label: str) -> Optional[str]:
    log.info(f"  [TRADE] {direction} {size} US500 | SL={sl} TP={tp} | {label}")
    try:
        r = api.open_position(
            epic=EPIC, direction=direction, size=size,
            stop_level=sl, profit_level=tp
        )
        time.sleep(1)
        confirm = api.confirm_deal(r.get("dealReference", ""))
        status  = confirm.get("dealStatus", "?")
        deal_id = confirm.get("dealId", "")
        log.info(f"  [TRADE] {status} | dealId={deal_id}")
        if status == "ACCEPTED":
            return deal_id
    except Exception as e:
        log.error(f"  [TRADE] Failed: {e}")
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
    log.info("  US500 19:05 IST Runner — STARTED")
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

            # ── Weekend guard (market closed Sat & Sun UTC) ───────────
            if now_utc.weekday() >= 5:  # 5=Saturday, 6=Sunday
                log.info(f"  [WEEKEND] Market closed ({now_utc.strftime('%A')}) — sleeping 1h")
                time.sleep(3600)
                continue

            # ── Done for day — sleep until next day ───────────────────────
            if state["done_for_day"]:
                log.debug("  [DAY] Done for today — waiting for tomorrow")
                time.sleep(CHECK_EVERY)
                continue

            # ── Step 1: Capture 19:05 IST candle (after 13:40 UTC) ───────
            if state["candle_high"] is None:
                # Only attempt after candle has closed (DST-adjusted)
                # IMPORTANT: use the IST state date (not now_utc.date()) so that
                # after midnight-IST rollover (e.g. 22:30 UTC Apr 14 → IST Apr 15)
                # we wait for Apr 15 13:40 UTC, not the already-passed Apr 14 13:40 UTC.
                ch = candle_utc_hour()
                state_date = datetime.strptime(state["date"], "%Y-%m-%d")
                candle_close_utc = datetime(
                    state_date.year, state_date.month, state_date.day,
                    ch, 40, 0, tzinfo=timezone.utc)
                if now_utc < candle_close_utc:
                    log.info(f"  [WAIT] Waiting for 19:05 IST candle to close | "
                             f"Now={now_ist.strftime('%H:%M')} IST")
                    time.sleep(CHECK_EVERY)
                    continue

                # Close anything running from previous day before fresh trade
                close_us500_position(api, "Pre-candle cleanup — closing previous day trade")
                state["active_deal_id"] = None
                state["t1_sl_hit"]      = False
                state["trades_today"]   = 0
                save_state(state)

                candle = fetch_1905_candle(api, state["date"])
                if candle is None:
                    log.info("  [WAIT] Candle not available yet — retrying")
                    time.sleep(30)
                    continue

                state["candle_high"]  = candle["high"]
                state["candle_low"]   = candle["low"]
                state["candle_range"] = candle["range"]
                save_state(state)
                log.info(f"  [CANDLE] Captured ✅ H={candle['high']} "
                         f"L={candle['low']} Range={candle['range']}pts")

            H = state["candle_high"]
            L = state["candle_low"]
            R = state["candle_range"]

            # ── Step 2: Monitor active trade ──────────────────────────────
            if state["active_deal_id"]:
                pos = get_us500_position(api)

                if pos is None:
                    # Position closed by Capital.com (TP or SL hit)
                    profit = 0  # can't get profit after close easily
                    log.info("  [MONITOR] Position closed by Capital.com")

                    # Determine if TP or SL hit using exact stored TP level
                    try:
                        cp = api.get_current_price(EPIC)
                        current = cp.get("bid", 0)
                    except:
                        current = (H + L) / 2  # midpoint fallback

                    active_tp  = state.get("active_tp")
                    active_sl  = state.get("active_sl")
                    active_dir = state.get("active_dir")
                    if active_tp and active_sl and active_dir == "BUY":
                        # price closer to TP (above) than SL (below) → TP hit
                        tp_hit = (active_tp - current) <= (current - active_sl)
                    elif active_tp and active_sl and active_dir == "SELL":
                        # price closer to TP (below) than SL (above) → TP hit
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
                        state["t1_tp_hit"]    = True
                        state["done_for_day"] = True
                        save_state(state)
                        log.info("  [RESULT] T1 TP hit — done for day ✅")
                        time.sleep(CHECK_EVERY)
                        continue
                    else:
                        # T1 SL hit — arm T2
                        state["t1_sl_hit"] = True
                        save_state(state)
                        log.info("  [RESULT] T1 SL hit — arming T2 reversal")

                else:
                    # Position still open — just monitor
                    p_data   = pos.get("position", {})
                    profit   = p_data.get("profit", 0)
                    deal_dir = p_data.get("direction", "?")
                    log.info(f"  [MONITOR] US500 {deal_dir} open | P&L={profit:+.2f}")
                    time.sleep(CHECK_EVERY)
                    continue

            # ── Step 4: Max trades guard ───────────────────────────────────
            if state["trades_today"] >= 2 or state["done_for_day"]:
                log.info("  [GUARD] Max 2 trades reached — done for day")
                state["done_for_day"] = True
                save_state(state)
                time.sleep(CHECK_EVERY)
                continue

            # ── Step 5: Get current price and decide entry ─────────────────
            try:
                cp      = api.get_current_price(EPIC)
                bid     = cp.get("bid", 0)
                offer   = cp.get("offer", bid)   # ASK — BUY fills at offer
            except Exception as e:
                log.warning(f"  [PRICE] Failed to get price: {e}")
                time.sleep(CHECK_FAST)
                continue

            # ── Step 6: T1 entry (first trade) ────────────────────────────
            if state["trades_today"] == 0:
                if bid > H + STOP_BUFFER:
                    # BUY breakout — use offer (ASK) for sl_dist/TP → true 1:3 from fill
                    sl      = round(L - STOP_BUFFER, 2)
                    sl_dist = round(offer - sl, 2)
                    size    = compute_size(sl_dist)
                    tp      = round(offer + RR_RATIO * sl_dist, 2)
                    deal_id = open_trade(api, "BUY", sl, tp, size, "T1 BUY breakout")
                    if deal_id:
                        state["trades_today"]   = 1
                        state["t1_direction"]   = "BUY"
                        state["active_deal_id"] = deal_id
                        state["active_tp"]      = tp
                        state["active_sl"]      = sl
                        state["active_dir"]     = "BUY"
                        save_state(state)

                elif bid < L - STOP_BUFFER:
                    # SELL breakout — use bid (fills at bid) → true 1:3 from fill
                    sl      = round(H + STOP_BUFFER, 2)
                    sl_dist = round(sl - bid, 2)
                    size    = compute_size(sl_dist)
                    tp      = round(bid - RR_RATIO * sl_dist, 2)
                    deal_id = open_trade(api, "SELL", sl, tp, size, "T1 SELL breakout")
                    if deal_id:
                        state["trades_today"]   = 1
                        state["t1_direction"]   = "SELL"
                        state["active_deal_id"] = deal_id
                        state["active_tp"]      = tp
                        state["active_sl"]      = sl
                        state["active_dir"]     = "SELL"
                        save_state(state)

                else:
                    log.info(f"  [WAIT] Price {bid} inside range "
                             f"[{L-STOP_BUFFER:.1f} – {H+STOP_BUFFER:.1f}] — waiting")
                    time.sleep(CHECK_FAST)
                    continue

            # ── Step 7: T2 reversal (after T1 SL hit) ─────────────────────
            elif state["trades_today"] == 1 and state["t1_sl_hit"]:
                t1 = state["t1_direction"]

                if t1 == "BUY":
                    # T1 BUY stopped at L → T2 SELL, use bid (sells fill at bid)
                    sl      = round(H + STOP_BUFFER, 2)
                    sl_dist = round(sl - bid, 2)
                    tp      = round(bid - RR_RATIO * sl_dist, 2)
                    size    = compute_size(sl_dist)
                    deal_id = open_trade(api, "SELL", sl, tp, size,
                                        "T2 REVERSAL SELL (T1 BUY SL hit)")
                    if deal_id:
                        state["trades_today"]   = 2
                        state["t2_direction"]   = "SELL"
                        state["active_deal_id"] = deal_id
                        state["active_tp"]      = tp
                        state["active_sl"]      = sl
                        state["active_dir"]     = "SELL"
                        state["t1_sl_hit"]      = False
                        save_state(state)

                elif t1 == "SELL":
                    # T1 SELL stopped at H → T2 BUY, use offer (buys fill at offer)
                    sl      = round(L - STOP_BUFFER, 2)
                    sl_dist = round(offer - sl, 2)
                    tp      = round(offer + RR_RATIO * sl_dist, 2)
                    size    = compute_size(sl_dist)
                    deal_id = open_trade(api, "BUY", sl, tp, size,
                                        "T2 REVERSAL BUY (T1 SELL SL hit)")
                    if deal_id:
                        state["trades_today"]   = 2
                        state["t2_direction"]   = "BUY"
                        state["active_deal_id"] = deal_id
                        state["active_tp"]      = tp
                        state["active_sl"]      = sl
                        state["active_dir"]     = "BUY"
                        state["t1_sl_hit"]      = False
                        save_state(state)

        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)

        # Fast polling once candle is captured, slow before
        time.sleep(CHECK_FAST if state.get("candle_high") else CHECK_EVERY)


if __name__ == "__main__":
    run()

