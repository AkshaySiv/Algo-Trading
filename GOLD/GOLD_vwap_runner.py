"""
GOLD VWAP 5-Min Strategy — Live Runner
========================================
Implements the MarketWealth XAUUSD VWAP 5-Min Strategy on Capital.com.

Strategy rules (from PDF):
  VWAP  : Session-anchored, source = (H+L+C)/3, resets at 06:00 IST each day
  LONG  : 1. Mother candle crosses & closes ABOVE VWAP
           2. Trigger candle: next bar closes above VWAP, low does NOT touch VWAP
           3. Enter when current price breaks above trigger candle HIGH + buffer
           4. SL  = low of mother candle (rare case: min of prev_bar low & mother low)
           5. TP  = entry + 3 × SL-distance (1:3 R:R)
  SHORT : Mirror of LONG rules (close below, high doesn't touch, break below trigger LOW)
           SL = high of mother candle (rare case: max of prev_bar high & mother high)
  INVALID: If VWAP is touched after trigger candle forms but before entry triggers → skip
  WINDOW : Entries ONLY 06:00–12:00 IST; existing trades can be held past 12:00
  ONE TRADE PER DAY

Run this script continuously — it sleeps between checks.
"""

import time
import logging
import json
import os
from typing import Optional
from datetime import datetime, timezone, timedelta
from capitalcom_api import CapitalComAPI
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
EPIC           = "GOLD"
RISK_PCT       = 0.01        # 1% account risk per trade
RR_RATIO       = 3.0         # 1:3 minimum
ENTRY_BUFFER   = 0.10        # $ above/below trigger candle H/L to confirm breakout
PIP_VALUE_USD  = 1.0         # GOLD: 1 pt = $1 per unit
USD_TO_AED     = 3.67
CHECK_EVERY    = 30          # seconds between normal scans
FAST_SCAN      = 1           # seconds when trigger is armed (match breakout fast)
STATE_FILE     = "state/gold_vwap_state.json"
IST            = timezone(timedelta(hours=5, minutes=30))

# Trading window: 06:00–12:00 IST (new entries only)
WINDOW_OPEN  = (6,  0)
WINDOW_CLOSE = (12, 0)

# ── Credentials ────────────────────────────────────────────────────────────────
CAPITAL_API_KEY    = os.getenv("CAPITAL_API_KEY",    "")
CAPITAL_IDENTIFIER = os.getenv("CAPITAL_IDENTIFIER", "")
CAPITAL_PASSWORD   = os.getenv("CAPITAL_PASSWORD",   "")
DEMO_MODE          = os.getenv("CAPITAL_DEMO", "true").lower() == "true"

# ── Logging ────────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/gold_vwap_runner.log"),
    ],
)
log = logging.getLogger("gold_vwap")


# ── State helpers ──────────────────────────────────────────────────────────────
def fresh_state() -> dict:
    return {
        "date":           datetime.now(IST).strftime("%Y-%m-%d"),
        # Phase drives the main loop:
        #   SCANNING      → looking for mother candle
        #   TRIGGER_ARMED → trigger candle confirmed, monitoring live price
        #   IN_TRADE      → position open, monitoring broker
        #   DONE          → no more entries today
        "phase":          "SCANNING",
        # Setup metadata (for logging / state recovery)
        "mother_ts":      None,
        "mother_h":       None,
        "mother_l":       None,
        "trigger_ts":     None,
        "trigger_h":      None,
        "trigger_l":      None,
        "setup_dir":      None,   # "BUY" | "SELL"
        "sl_level":       None,   # stop-loss price
        # Active trade
        "active_deal_id": None,
        "active_entry":   None,
        "active_sl":      None,
        "active_tp":      None,
        "active_dir":     None,
    }


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return fresh_state()


def save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def in_trading_window() -> bool:
    t = datetime.now(IST)
    return WINDOW_OPEN <= (t.hour, t.minute) < WINDOW_CLOSE


# ── VWAP helpers ───────────────────────────────────────────────────────────────
def compute_session_vwap(candles: list) -> Optional[float]:
    """Session VWAP = Σ(typical_price × volume) / Σ(volume)."""
    cum_tp_vol = 0.0
    cum_vol    = 0.0
    for c in candles:
        tp = (c["h"] + c["l"] + c["c"]) / 3.0
        v  = c.get("v", 1.0) or 1.0
        cum_tp_vol += tp * v
        cum_vol    += v
    return round(cum_tp_vol / cum_vol, 3) if cum_vol else None


def valid_trigger_candle(bar_h: float, bar_l: float, vwap: float,
                         setup_dir: str) -> bool:
    """
    Trigger candle is valid only if the ENTIRE bar is on the correct side of VWAP
    (no wick touching VWAP). For BUY: bar_l > vwap. For SELL: bar_h < vwap.
    """
    if setup_dir == "BUY":
        return bar_l > vwap   # whole candle above VWAP, confirms bullish
    return bar_h < vwap       # whole candle below VWAP, confirms bearish


def touches_vwap(bar_h: float, bar_l: float, vwap: float) -> bool:
    """True if any part of the bar (including wicks) touches VWAP."""
    return bar_l <= vwap <= bar_h


# ── Bar fetching ───────────────────────────────────────────────────────────────
def fetch_today_bars(api: CapitalComAPI) -> list:
    """
    Fetch all COMPLETED 5-min bars since 06:00 IST (00:30 UTC) today.
    Bars still forming are excluded (bar_open_time + 5 min > now).
    """
    today    = datetime.now(IST).date()
    from_ts  = datetime(today.year, today.month, today.day, 0, 30, 0,
                        tzinfo=timezone.utc)           # 06:00 IST
    to_ts    = datetime.now(timezone.utc)
    from_str = from_ts.strftime("%Y-%m-%dT%H:%M:%S")
    to_str   = to_ts.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        api._ensure_session()
        resp = api._session.get(
            f"{api.base_url}/api/v1/prices/{EPIC}",
            params={"resolution": "MINUTE_5", "from": from_str,
                    "to": to_str, "max": 200},
            timeout=15,
        )
        resp.raise_for_status()
        raw_bars = resp.json().get("prices", [])
    except Exception as e:
        log.warning(f"[BARS] Fetch failed: {e}")
        return []

    now_utc = datetime.now(timezone.utc)
    parsed  = []
    for bar in raw_bars:
        ts_str = bar.get("snapshotTimeUTC", bar.get("snapshotTime", ""))
        try:
            bar_dt = datetime.strptime(ts_str[:16], "%Y-%m-%dT%H:%M").replace(
                tzinfo=timezone.utc)
        except Exception:
            continue
        if bar_dt + timedelta(minutes=5) > now_utc:
            continue   # still forming — skip
        parsed.append({
            "h":  bar["highPrice"]["bid"],
            "l":  bar["lowPrice"]["bid"],
            "c":  bar["closePrice"]["bid"],
            "o":  bar["openPrice"]["bid"],
            "v":  bar.get("lastTradedVolume", 1.0) or 1.0,
            "ts": ts_str,
        })
    return parsed


# ── Setup detection (full replay each cycle) ───────────────────────────────────
def detect_setup(bars: list) -> dict:
    """
    Replay all completed bars since 06:00 IST to determine the current setup.

    Returns a dict:
        phase        : "SCANNING" | "TRIGGER_ARMED"
        vwap         : latest VWAP float or None
        mother_bar   : {h, l, c, ts} or None
        trigger_bar  : {h, l, c, ts} or None
        setup_dir    : "BUY" | "SELL" or None
        sl_level     : stop-loss price or None
    """
    accumulated = []
    prev_side   = None
    prev_bar    = None   # raw bar dict — needed for rare-case SL
    mother_bar  = None
    trigger_bar = None
    setup_dir   = None
    sl_level    = None
    vwap        = None

    for bar in bars:
        h, l, c, v = bar["h"], bar["l"], bar["c"], bar["v"]
        accumulated.append({"h": h, "l": l, "c": c, "v": v})
        vwap     = compute_session_vwap(accumulated)
        if vwap is None:
            prev_bar = bar
            continue

        curr_side = "above" if c > vwap else "below"

        # ── 1. Detect VWAP crossover → Mother candle ─────────────────────
        if mother_bar is None:
            if prev_side is not None and curr_side != prev_side:
                # Rare-case SL rule (PDF p.9):
                #   SELL: SL = HIGH of the candle that was above VWAP (prev or curr)
                #   BUY : SL = LOW  of the candle that was below VWAP (prev or curr)
                if curr_side == "above":   # BUY crossover
                    sl_level = round(
                        min(prev_bar["l"] if prev_bar else l, l), 2)
                else:                       # SELL crossover
                    sl_level = round(
                        max(prev_bar["h"] if prev_bar else h, h), 2)

                mother_bar = {"h": h, "l": l, "c": c, "ts": bar["ts"]}
                setup_dir  = "BUY" if curr_side == "above" else "SELL"
                log.debug(
                    f"  [SETUP] Mother candle {bar['ts']} | dir={setup_dir} | "
                    f"VWAP={vwap:.2f} | SL={sl_level:.2f}"
                )

        # ── 2. Trigger candle (next bar after mother) ─────────────────────
        elif trigger_bar is None:
            if not valid_trigger_candle(h, l, vwap, setup_dir):
                # Trigger candle touched VWAP — invalidate
                log.info(
                    f"  [RESET] Trigger candle touched VWAP={vwap:.2f} → reset | "
                    f"{bar['ts']}"
                )
                mother_bar  = None
                trigger_bar = None
                setup_dir   = None
                sl_level    = None
            else:
                trigger_bar = {"h": h, "l": l, "c": c, "ts": bar["ts"]}
                log.info(
                    f"  [SETUP] Trigger candle confirmed {bar['ts']} | "
                    f"dir={setup_dir} | H={h:.2f} L={l:.2f} VWAP={vwap:.2f}"
                )

        # ── 3. Post-trigger VWAP invalidation (PDF p.10 — Wrong Entry Rule) ──
        else:
            # Any subsequent bar (after trigger, before entry triggers) that
            # touches VWAP signals weakness — do NOT take the trade.
            if touches_vwap(h, l, vwap):
                log.info(
                    f"  [RESET] Post-trigger VWAP touch → setup invalidated | "
                    f"{bar['ts']}"
                )
                mother_bar  = None
                trigger_bar = None
                setup_dir   = None
                sl_level    = None

        prev_side = curr_side
        prev_bar  = bar

    phase = "TRIGGER_ARMED" if (mother_bar and trigger_bar) else "SCANNING"
    return {
        "phase":       phase,
        "vwap":        vwap,
        "mother_bar":  mother_bar,
        "trigger_bar": trigger_bar,
        "setup_dir":   setup_dir,
        "sl_level":    sl_level,
    }


# ── Position sizing ────────────────────────────────────────────────────────────
def compute_size(balance: float, sl_distance: float) -> float:
    risk_aed = balance * RISK_PCT
    size     = risk_aed / (sl_distance * PIP_VALUE_USD * USD_TO_AED)
    return round(max(0.01, round(size, 2)), 2)


# ── Trade helpers ──────────────────────────────────────────────────────────────
def get_gold_position(api: CapitalComAPI) -> Optional[dict]:
    for p in api.get_all_positions():
        if p.get("market", {}).get("epic") == EPIC:
            return p
    return None


def close_gold_position(api: CapitalComAPI, reason: str = "") -> None:
    pos = get_gold_position(api)
    if pos:
        deal_id = pos["position"]["dealId"]
        log.info(f"  [CLOSE] Closing GOLD {deal_id} | {reason}")
        api.close_position(deal_id)
        time.sleep(1)


def open_trade(api: CapitalComAPI, direction: str,
               entry: float, sl: float, tp: float,
               size: float, label: str) -> Optional[str]:
    log.info(
        f"  [TRADE] {direction} {size:.2f} GOLD | "
        f"Entry≈{entry:.2f} | SL={sl:.2f} | TP={tp:.2f} | {label}"
    )
    try:
        r       = api.open_position(epic=EPIC, direction=direction, size=size,
                                    stop_level=sl, profit_level=tp)
        time.sleep(1)
        confirm = api.confirm_deal(r.get("dealReference", ""))
        status  = confirm.get("dealStatus", "?")
        deal_id = confirm.get("dealId", "")
        log.info(f"  [TRADE] {status} | dealId={deal_id}")
        return deal_id if status == "ACCEPTED" else None
    except Exception as e:
        log.error(f"  [TRADE] Failed: {e}")
        return None


# ── Main loop ──────────────────────────────────────────────────────────────────
def run():
    api = CapitalComAPI(
        api_key=CAPITAL_API_KEY,
        identifier=CAPITAL_IDENTIFIER,
        password=CAPITAL_PASSWORD,
        demo=DEMO_MODE,
    )
    api.create_session()
    acc     = api.get_account_info()
    balance = acc.get("balance", {}).get("balance", 0)
    mode    = "DEMO" if DEMO_MODE else "LIVE"
    log.info("=" * 62)
    log.info("  GOLD VWAP 5-Min Runner — STARTED")
    log.info(f"  Account: CFD | Mode: {mode} | Balance: AED {balance:,.2f}")
    log.info(f"  Risk: {RISK_PCT*100:.0f}% | R:R = 1:{RR_RATIO:.0f} | "
             f"Window: 06:00–12:00 IST")
    log.info("=" * 62)

    state = load_state()

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc.astimezone(IST)

            # ── New IST day — reset ────────────────────────────────────────
            if state.get("date") != today_ist():
                log.info(f"  [DAY] New day {today_ist()} — resetting state")
                close_gold_position(api, "New-day cleanup")
                state = fresh_state()
                save_state(state)

            # ── Weekend guard ──────────────────────────────────────────────
            wd = now_utc.weekday()   # 5=Sat, 6=Sun
            if wd == 5:
                log.info("  [WEEKEND] Saturday — sleeping 1h")
                time.sleep(3600)
                continue
            if wd == 6 and now_utc.hour < 23:
                log.info("  [WEEKEND] Sunday pre-open — sleeping 1h")
                time.sleep(3600)
                continue

            phase = state.get("phase", "SCANNING")

            # ── IN_TRADE: Monitor broker ───────────────────────────────────
            if phase == "IN_TRADE":
                pos = get_gold_position(api)
                if pos is None:
                    # Position closed by broker — determine TP or SL via distance
                    try:
                        cp      = api.get_current_price(EPIC)
                        current = cp.get("bid", 0)
                    except Exception:
                        current = 0

                    active_tp  = state.get("active_tp")
                    active_sl  = state.get("active_sl")
                    active_dir = state.get("active_dir")

                    if active_tp and active_sl and current and active_dir == "BUY":
                        tp_hit = (active_tp - current) <= (current - active_sl)
                    elif active_tp and active_sl and current and active_dir == "SELL":
                        tp_hit = (current - active_tp) <= (active_sl - current)
                    else:
                        tp_hit = False

                    result = "TP ✅" if tp_hit else "SL ❌"
                    log.info(f"  [RESULT] Position closed by broker — {result} — done for day")
                    state["phase"]          = "DONE"
                    state["active_deal_id"] = None
                    state["active_tp"]      = None
                    state["active_sl"]      = None
                    state["active_dir"]     = None
                    save_state(state)
                else:
                    p      = pos.get("position", {})
                    profit = p.get("profit", 0)
                    direc  = p.get("direction", "?")
                    log.info(
                        f"  [MONITOR] GOLD {direc} open | "
                        f"P&L={profit:+.2f} | "
                        f"SL={state['active_sl']} TP={state['active_tp']}"
                    )
                time.sleep(CHECK_EVERY)
                continue

            # ── DONE: No more entries today ────────────────────────────────
            if phase == "DONE":
                log.debug("  [DONE] No more trades today — waiting for next day")
                time.sleep(CHECK_EVERY)
                continue

            # ── Outside entry window (not in an active trade) ──────────────
            if not in_trading_window():
                log.debug(
                    f"  [WINDOW] Outside 06:00–12:00 IST | "
                    f"Now={now_ist.strftime('%H:%M')} IST"
                )
                time.sleep(CHECK_EVERY)
                continue

            # ── Fetch completed bars and replay to find setup ──────────────
            bars  = fetch_today_bars(api)
            setup = detect_setup(bars)
            vwap_str = f"{setup['vwap']:.2f}" if setup["vwap"] else "N/A"

            # ── TRIGGER_ARMED: Check live price for breakout ───────────────
            if setup["phase"] == "TRIGGER_ARMED":
                tc  = setup["trigger_bar"]
                sl  = setup["sl_level"]
                dir = setup["setup_dir"]

                # Cache trigger info to state (for logging / crash recovery)
                state["phase"]      = "TRIGGER_ARMED"
                state["mother_ts"]  = setup["mother_bar"]["ts"]
                state["trigger_ts"] = tc["ts"]
                state["trigger_h"]  = tc["h"]
                state["trigger_l"]  = tc["l"]
                state["setup_dir"]  = dir
                state["sl_level"]   = sl
                save_state(state)

                try:
                    cp    = api.get_current_price(EPIC)
                    bid   = cp.get("bid",   0)
                    offer = cp.get("offer", bid)   # ASK — BUY fills at offer
                except Exception as e:
                    log.warning(f"  [PRICE] Failed: {e}")
                    time.sleep(FAST_SCAN)
                    continue

                buy_trigger  = (dir == "BUY"  and bid > tc["h"] + ENTRY_BUFFER)
                sell_trigger = (dir == "SELL" and bid < tc["l"] - ENTRY_BUFFER)

                if buy_trigger or sell_trigger:
                    acc     = api.get_account_info()
                    balance = acc.get("balance", {}).get("balance", 0)

                    if buy_trigger:
                        # BUY fills at offer — compute sl_dist/TP from actual fill price
                        entry_px = round(offer, 2)
                        sl_dist  = round(entry_px - sl, 2)
                        if sl_dist <= 0:
                            log.warning(f"  [GUARD] BUY SL dist={sl_dist:.2f} ≤ 0 — skip")
                            time.sleep(FAST_SCAN)
                            continue
                        size    = compute_size(balance, sl_dist)
                        tp      = round(entry_px + RR_RATIO * sl_dist, 2)
                        deal_id = open_trade(api, "BUY", entry_px, sl, tp, size,
                                             f"VWAP BUY | trigger>{tc['h']:.2f}")
                    else:
                        # SELL fills at bid — compute sl_dist/TP from actual fill price
                        entry_px = round(bid, 2)
                        sl_dist  = round(sl - entry_px, 2)
                        if sl_dist <= 0:
                            log.warning(f"  [GUARD] SELL SL dist={sl_dist:.2f} ≤ 0 — skip")
                            time.sleep(FAST_SCAN)
                            continue
                        size    = compute_size(balance, sl_dist)
                        tp      = round(entry_px - RR_RATIO * sl_dist, 2)
                        deal_id = open_trade(api, "SELL", entry_px, sl, tp, size,
                                             f"VWAP SELL | trigger<{tc['l']:.2f}")

                    if deal_id:
                        state["phase"]          = "IN_TRADE"
                        state["active_deal_id"] = deal_id
                        state["active_entry"]   = entry_px
                        state["active_sl"]      = sl
                        state["active_tp"]      = tp
                        state["active_dir"]     = "BUY" if buy_trigger else "SELL"
                        save_state(state)
                        time.sleep(CHECK_EVERY)
                    else:
                        time.sleep(FAST_SCAN)
                    continue

                # Not triggered yet — log and scan fast
                entry_level = (tc["h"] + ENTRY_BUFFER if dir == "BUY"
                               else tc["l"] - ENTRY_BUFFER)
                log.info(
                    f"  [ARMED] {dir} | Entry @ {'>' if dir == 'BUY' else '<'}"
                    f"{entry_level:.2f} | Bid={bid:.2f} | "
                    f"VWAP={vwap_str} | SL={sl:.2f}"
                )
                time.sleep(FAST_SCAN)

            else:
                # SCANNING — waiting for a VWAP crossover to form
                log.info(
                    f"  [SCAN] {now_ist.strftime('%H:%M')} IST | "
                    f"VWAP={vwap_str} | {len(bars)} bars | "
                    f"mother={'yes' if setup['mother_bar'] else 'no'}"
                )
                if phase != "SCANNING":
                    state["phase"] = "SCANNING"
                    save_state(state)
                time.sleep(CHECK_EVERY)

        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)
            time.sleep(CHECK_EVERY)


if __name__ == "__main__":
    run()
