"""Offline checks for gold-0530-test-runner.py; no broker requests are made."""

import importlib.util
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


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def get(self, url, params, timeout):
        del url, timeout
        if params["from"].endswith("T00:00:00"):
            return FakeResponse(
                {
                    "prices": [
                        {
                            "highPrice": {"bid": 100.0, "ask": 100.2},
                            "lowPrice": {"bid": 90.0, "ask": 90.2},
                            "closePrice": {"bid": 95.0, "ask": 95.2},
                        }
                    ]
                }
            )

        def bar(timestamp, high_bid, high_ask, low_bid, open_bid, close_bid):
            return {
                "snapshotTimeUTC": timestamp,
                "highPrice": {"bid": high_bid, "ask": high_ask},
                "lowPrice": {"bid": low_bid, "ask": low_bid + 0.2},
                "openPrice": {"bid": open_bid, "ask": open_bid + 0.2},
                "closePrice": {"bid": close_bid, "ask": close_bid + 0.2},
            }

        # T1 BUY breaks out, then stops; T2 SELL reverses and reaches target.
        return FakeResponse(
            {
                "prices": [
                    bar("2026-04-10T00:30:00", 101.3, 101.5, 95.0, 95.0, 101.0),
                    bar("2026-04-10T01:00:00", 99.0, 99.2, 89.0, 98.0, 90.0),
                    bar("2026-04-10T01:30:00", 95.0, 95.2, 88.0, 89.0, 88.5),
                    bar("2026-04-10T02:00:00", 80.0, 80.2, 55.0, 80.0, 56.0),
                ]
            }
        )


class FakeAPI:
    base_url = "https://example.invalid"
    _session = FakeSession()

    def _ensure_session(self):
        return None


sim_date = date(2026, 4, 10)
window_open, window_close = strategy.strategy_candle_window(sim_date)
assert window_open.isoformat() == "2026-04-10T00:00:00+00:00"
assert window_close.isoformat() == "2026-04-10T00:30:00+00:00"
assert strategy.candle_ist_label(sim_date) == "05:30 AM–06:00 AM IST"

final_balance = strategy.simulate_day(FakeAPI(), sim_date, 4000.0)
expected_balance = 4082.208
assert abs(final_balance - expected_balance) < 1e-9, final_balance

print("PASS: India-time window and T1/T2 breakout-reversal replay verified")
