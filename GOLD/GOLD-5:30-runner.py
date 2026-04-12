"""
GOLD 5:30 IST Breakout — Daily Runner
======================================
Simple, focused script for one strategy only.

Logic:
  1. At 00:15 UTC daily — fetch the 5:30 AM IST (00:00 UTC) 15-min candle
  2. Price above candle H → BUY  | Price below candle L → SELL
  3. SL = opposite side of candle | TP = entry ± 3× range (1:3 R:R)
  4. If T1 SL hit → reverse trade (T2)
  5. If T1 TP hit → done for day
  6. Trade runs overnight — TP/SL managed by broker
  7. Close any open trade at 00:15 UTC next day before fresh candle
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
EPIC          = "GOLD"
RISK_PCT      = 0.01        # 1% per trade
RR_RATIO      = 3.0         # 1:3
STOP_BUFFER   = 0.5         # points above/below candle H/L for entry (larger for GOLD)
CHECK_EVERY   = 30          # seconds between scans
PIP_VALUE_USD = 1.0         # GOLD: 1pt = $1/unit (1 unit = 1 troy oz)
USD_TO_AED    = 3.67
STATE_FILE    = "state/gold_runner_state.json"

# ── IST timezone ──────────────────────────────────────────────────────────────
IST = timezone(timedelta(hours=5, minutes=30))

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/gold_runner.log")
    ]
)
log = logging.getLogger("gold")

# ── Credentials ───────────────────────────────────────────────────────────────
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

def fresh_state():
    return {
        "date":           today_ist(),
        "candle_high":    None,
        "candle_low":     None,
        "candle_range":   None,
        "trades_today":   0,
        "t1_direction":   None,
        "t1_sl_hit":      False,
        "t1_tp_hit":      False,
        "active_deal_id": None,
        "active_tp":      None,   # exact TP level for current trade
        "active_dir":     None,   # direction of current trade
        "done_for_day":   False,
    }


# ── Candle fetch ──────────────────────────────────────────────────────────────
def fetch_530_candle(api: CapitalComAPI) -> Optional[dict]:
    """
    Fetch the 5:30 AM IST (00:00 UTC) 15-min candle directly.
    IST has no DST so the UTC offset is always +05:30 — candle always at 00:00 UTC.
    Uses date-range API call: from=00:00 to=00:14:59 → only one bar returned.
    Returns {high, low, close} or None if not available yet.
    """
    now_utc = datetime.now(timezone.utc)
    today   = now_utc.date()

    # Build exact window: 00:00:00 to 00:14:59 UTC today
    from_ts = datetime(today.year, today.month, today.day,
                       0, 0, 0, tzinfo=timezone.utc)
    to_ts   = datetime(today.year, today.month, today.day,
                       0, 14, 59, tzinfo=timezone.utc)

    # Format for Capital.com API: YYYY-MM-DDTHH:MM:SS
    from_str = from_ts.strftime("%Y-%m-%dT%H:%M:%S")
    to_str   = to_ts.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        api._ensure_session()
        resp = api._session.get(
            f"{api.base_url}/api/v1/prices/{EPIC}",
            params={
                "resolution": "MINUTE_15",
                "from": from_str,
                "to":   to_str,
            },
            timeout=15
        )
        resp.raise_for_status()
        bars = resp.json().get("prices", [])

        if not bars:
            log.debug("No bar returned for 00:00 UTC window yet")
            return None

        # Should be exactly 1 bar
        bar = bars[0]
        ts  = bar.get("snapshotTimeUTC", bar.get("snapshotTime", "?"))
        h   = bar.get("highPrice",  {}).get("bid", 0)
        l   = bar.get("lowPrice",   {}).get("bid", 0)
        c   = bar.get("closePrice", {}).get("bid", 0)
        r   = round(h - l, 2)

        log.info(f"  [CANDLE] 5:30 IST bar: ts={ts} | H={h} L={l} Range={r}pts")
        return {"high": h, "low": l, "close": c, "range": r, "ts": ts}

    except Exception as e:
        log.warning(f"  [CANDLE] Failed to fetch: {e}")
        return None


# ── Position sizing ───────────────────────────────────────────────────────────
def compute_size(balance: float, sl_distance: float) -> float:
    risk_aed   = balance * RISK_PCT
    size       = risk_aed / (sl_distance * PIP_VALUE_USD * USD_TO_AED)
    return round(max(0.1, round(size, 1)), 1)


# ── Active position check ─────────────────────────────────────────────────────
def get_gold_position(api: CapitalComAPI) -> Optional[dict]:
    positions = api.get_all_positions()
    for p in positions:
        if p.get("market", {}).get("epic") == EPIC:
            return p
    return None


# ── Close any open GOLD position ──────────────────────────────────────────────
def close_gold_position(api: CapitalComAPI, reason: str = ""):
    pos = get_gold_position(api)
    if pos:
        deal_id = pos["position"]["dealId"]
        log.info(f"  [CLOSE] Closing GOLD position {deal_id} | {reason}")
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
    log.info(f"  [TRADE] {direction} {size} GOLD | SL={sl} TP={tp} | {label}")
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
    log.info("  GOLD 5:30 IST Runner — STARTED")
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

            # ── Done for day — sleep until next day ───────────────────────
            if state["done_for_day"]:
                log.debug("  [DAY] Done for today — waiting for tomorrow")
                time.sleep(CHECK_EVERY)
                continue

            # ── Weekend guard (GOLD closed Saturday UTC; opens Sunday ~23:00 UTC)
            # Skip Saturday entirely; also skip Sunday before 23:00 UTC
            wd = now_utc.weekday()  # 5=Saturday, 6=Sunday
            if wd == 5:
                log.info(f"  [WEEKEND] Market closed (Saturday) — sleeping 1h")
                time.sleep(3600)
                continue
            if wd == 6 and now_utc.hour < 23:
                log.info(f"  [WEEKEND] Market closed (Sunday before 23:00 UTC) — sleeping 1h")
                time.sleep(3600)
                continue

            # ── Step 1: Capture 5:30 IST candle (after 00:15 UTC) ────────
            if state["candle_high"] is None:
                # Only attempt after the 15-min candle has closed (00:15 UTC)
                candle_close_utc = now_utc.replace(
                    hour=0, minute=15, second=0, microsecond=0)
                if now_utc < candle_close_utc:
                    log.info(f"  [WAIT] Waiting for 5:30 IST candle to close | "
                             f"Now={now_ist.strftime('%H:%M')} IST")
                    time.sleep(CHECK_EVERY)
                    continue

                # Close anything running from previous day before fresh trade
                close_gold_position(api, "Pre-candle cleanup — closing previous day trade")
                state["active_deal_id"] = None
                state["t1_sl_hit"]      = False
                state["trades_today"]   = 0
                save_state(state)

                candle = fetch_530_candle(api)
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
                pos = get_gold_position(api)

                if pos is None:
                    # Position closed by Capital.com (TP or SL hit)
                    log.info("  [MONITOR] Position closed by Capital.com")

                    # Determine if TP or SL hit using exact stored TP level
                    try:
                        cp = api.get_current_price(EPIC)
                        current = cp.get("bid", 0)
                    except:
                        current = (H + L) / 2  # midpoint fallback

                    active_tp  = state.get("active_tp")
                    active_dir = state.get("active_dir")
                    if active_tp and active_dir == "BUY":
                        tp_hit = current >= active_tp * 0.999  # within 0.1% of TP
                    elif active_tp and active_dir == "SELL":
                        tp_hit = current <= active_tp * 1.001
                    else:
                        tp_hit = False

                    state["active_deal_id"] = None
                    state["active_tp"]      = None
                    state["active_dir"]     = None

                    if state["trades_today"] >= 2:
                        # T2 just closed (TP or SL) — done for day either way
                        state["t1_tp_hit"]    = tp_hit
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
                    log.info(f"  [MONITOR] GOLD {deal_dir} open | P&L={profit:+.2f}")
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
                current = cp.get("bid", 0)
            except Exception as e:
                log.warning(f"  [PRICE] Failed to get price: {e}")
                time.sleep(CHECK_EVERY)
                continue

            acc     = api.get_account_info()
            balance = acc.get("balance", {}).get("balance", 0)

            # ── Step 6: T1 entry (first trade) ────────────────────────────
            if state["trades_today"] == 0:
                if current > H + STOP_BUFFER:
                    # BUY breakout — risk 1%, TP = entry + 3×sl_dist
                    sl      = round(L, 2)
                    sl_dist = round(current - sl, 2)
                    size    = compute_size(balance, sl_dist)
                    tp      = round(current + RR_RATIO * sl_dist, 2)
                    deal_id = open_trade(api, "BUY", sl, tp, size, "T1 BUY breakout")
                    if deal_id:
                        state["trades_today"]   = 1
                        state["t1_direction"]   = "BUY"
                        state["active_deal_id"] = deal_id
                        state["active_tp"]      = tp
                        state["active_dir"]     = "BUY"
                        save_state(state)

                elif current < L - STOP_BUFFER:
                    # SELL breakout — risk 1%, TP = entry - 3×sl_dist
                    sl      = round(H, 2)
                    sl_dist = round(sl - current, 2)
                    size    = compute_size(balance, sl_dist)
                    tp      = round(current - RR_RATIO * sl_dist, 2)
                    deal_id = open_trade(api, "SELL", sl, tp, size, "T1 SELL breakout")
                    if deal_id:
                        state["trades_today"]   = 1
                        state["t1_direction"]   = "SELL"
                        state["active_deal_id"] = deal_id
                        state["active_tp"]      = tp
                        state["active_dir"]     = "SELL"
                        save_state(state)

                else:
                    log.info(f"  [WAIT] Price {current} inside range "
                             f"[{L-STOP_BUFFER:.1f} – {H+STOP_BUFFER:.1f}] — waiting")

            # ── Step 7: T2 reversal (after T1 SL hit) ─────────────────────
            elif state["trades_today"] == 1 and state["t1_sl_hit"]:
                t1 = state["t1_direction"]

                if t1 == "BUY":
                    # T1 BUY stopped at L → T2 SELL anchored to L, SL=H, TP=L-3R
                    sl      = round(H, 2)
                    tp      = round(L - RR_RATIO * R, 2)
                    size    = compute_size(balance, R)
                    deal_id = open_trade(api, "SELL", sl, tp, size,
                                        "T2 REVERSAL SELL (T1 BUY SL hit)")
                    if deal_id:
                        state["trades_today"]   = 2
                        state["t1_direction"]   = "SELL"
                        state["active_deal_id"] = deal_id
                        state["active_tp"]      = tp
                        state["active_dir"]     = "SELL"
                        state["t1_sl_hit"]      = False
                        save_state(state)

                elif t1 == "SELL":
                    # T1 SELL stopped at H → T2 BUY anchored to H, SL=L, TP=H+3R
                    sl      = round(L, 2)
                    tp      = round(H + RR_RATIO * R, 2)
                    size    = compute_size(balance, R)
                    deal_id = open_trade(api, "BUY", sl, tp, size,
                                        "T2 REVERSAL BUY (T1 SELL SL hit)")
                    if deal_id:
                        state["trades_today"]   = 2
                        state["t1_direction"]   = "BUY"
                        state["active_deal_id"] = deal_id
                        state["active_tp"]      = tp
                        state["active_dir"]     = "BUY"
                        state["t1_sl_hit"]      = False
                        save_state(state)

        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)

        time.sleep(CHECK_EVERY)


if __name__ == "__main__":
    run()
