"""Offline checks for gold-0530-test-runner.py; no broker requests are made."""

import contextlib
import importlib.util
import io
import sys
import types
from datetime import date
from pathlib import Path

# The simulator imports python-dotenv to load credentials. The test does not use
# credentials, so provide a lightweight stand-in if that optional package is not
# installed in the local validation environment.
if "dotenv" not in sys.modules:
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda *args, **kwargs: False
    sys.modules["dotenv"] = dotenv_stub

MODULE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(MODULE_DIR))

spec = importlib.util.spec_from_file_location(
    "gold_0530_test_runner", MODULE_DIR / "gold-0530-test-runner.py"
)
assert spec is not None and spec.loader is not None
strategy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(strategy)


SIM_DATE = date(2026, 4, 10)
START_BALANCE = 4000.0


def bar(timestamp, high_bid, high_ask, low_bid, open_bid, close_bid):
    """Build one Capital.com-style replay candle."""
    return {
        "snapshotTimeUTC": timestamp,
        "highPrice": {"bid": high_bid, "ask": high_ask},
        "lowPrice": {"bid": low_bid, "ask": low_bid + 0.2},
        "openPrice": {"bid": open_bid, "ask": open_bid + 0.2},
        "closePrice": {"bid": close_bid, "ask": close_bid + 0.2},
    }


REFERENCE_CANDLE = {
    "highPrice": {"bid": 100.0, "ask": 100.2},
    "lowPrice": {"bid": 90.0, "ask": 90.2},
    "closePrice": {"bid": 95.0, "ask": 95.2},
}


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, replay_bars):
        self.replay_bars = replay_bars

    def get(self, url, params, timeout):
        del url, timeout
        if params["from"].endswith("T00:00:00"):
            return FakeResponse({"prices": [REFERENCE_CANDLE]})
        return FakeResponse({"prices": self.replay_bars})


class FakeAPI:
    base_url = "https://example.invalid"

    def __init__(self, replay_bars):
        self._session = FakeSession(replay_bars)

    def _ensure_session(self):
        return None


def replay(replay_bars):
    """Run an isolated simulator replay and return its balance plus console text."""
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        final_balance = strategy.simulate_day(
            FakeAPI(replay_bars), SIM_DATE, START_BALANCE
        )
    return final_balance, output.getvalue()


# ── Time and hit-order rules ──────────────────────────────────────────────────
window_open, window_close = strategy.strategy_candle_window(SIM_DATE)
assert window_open.isoformat() == "2026-04-10T00:00:00+00:00"
assert window_close.isoformat() == "2026-04-10T00:30:00+00:00"
assert strategy.candle_ist_label(SIM_DATE) == "05:30 AM–06:00 AM IST"

# If a single candle crosses both levels, the deterministic GER40 convention
# chooses TP first only when the bar opens in the position's profit zone.
assert strategy.determine_exit_outcome(
    "BUY", 100.0, 90.0, 110.0, 111.0, 111.2, 89.0, 100.0
) == "TP"
assert strategy.determine_exit_outcome(
    "BUY", 100.0, 90.0, 110.0, 111.0, 111.2, 89.0, 99.0
) == "SL"
assert strategy.determine_exit_outcome(
    "SELL", 100.0, 110.0, 90.0, 111.0, 111.2, 89.0, 100.0
) == "TP"
assert strategy.determine_exit_outcome(
    "SELL", 100.0, 110.0, 90.0, 111.0, 111.2, 89.0, 101.0
) == "SL"


# ── Later-bar TP/SL handling ──────────────────────────────────────────────────
# T1 BUY opens on bar one, stops on the next bar, then T2 SELL takes profit on a
# later bar. Neither hit should be labeled as an entry-bar event.
later_bar_replay = [
    bar("2026-04-10T00:30:00", 101.3, 101.5, 95.0, 95.0, 101.0),
    bar("2026-04-10T01:00:00", 99.0, 99.2, 89.0, 98.0, 90.0),
    bar("2026-04-10T01:30:00", 95.0, 95.2, 88.0, 89.0, 88.5),
    bar("2026-04-10T02:00:00", 80.0, 80.2, 55.0, 80.0, 56.0),
]
final_balance, output = replay(later_bar_replay)
assert abs(final_balance - 4082.208) < 1e-9, final_balance
assert output.count("✅ TP HIT") == 1
assert output.count("❌ SL HIT") == 1
assert "entry-bar" not in output


# ── Entry-bar TP and SL handling ──────────────────────────────────────────────
# A T1 BUY enters and reaches its target inside its breakout candle.
entry_bar_tp_replay = [
    bar("2026-04-10T00:30:00", 135.0, 135.2, 95.0, 101.2, 134.8),
]
final_balance, output = replay(entry_bar_tp_replay)
assert abs(final_balance - 4123.312) < 1e-9, final_balance
assert "✅ TP HIT @ 134.80  |" in output
assert "❌ SL HIT" not in output

# A T1 BUY enters and reverses to its stop in the same candle. T2 is armed on the
# following candle and then reaches target in its own entry candle.
entry_bar_sl_replay = [
    bar("2026-04-10T00:30:00", 101.3, 101.5, 89.0, 95.0, 90.0),
    bar("2026-04-10T01:00:00", 95.0, 95.2, 55.0, 89.0, 56.0),
]
final_balance, output = replay(entry_bar_sl_replay)
assert abs(final_balance - 4082.208) < 1e-9, final_balance
assert "❌ SL HIT @ 90.00 (entry-bar reversal)" in output
assert "✅ TP HIT @ 55.40  |" in output

print("PASS: TP/SL detection and reporting verified for entry and later replay bars")
