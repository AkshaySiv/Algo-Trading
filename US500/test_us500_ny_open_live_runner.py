#!/usr/bin/env python3
"""Offline regression tests for US500 NY-open live runner and Capital.com STOP payload."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def load_module(name: str, filename: str):
    """Load a hyphenated runner filename as an importable test module."""
    spec = importlib.util.spec_from_file_location(name, ROOT / filename)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    """Minimal requests-like response for adapter payload inspection."""

    status_code = 200
    text = ""

    def json(self):
        return {"dealReference": "o_test_reference"}

    def raise_for_status(self):
        raise AssertionError("Unexpected HTTP error path")


class FakeSession:
    """Capture outgoing REST calls without contacting Capital.com."""

    def __init__(self):
        self.calls = []

    def post(self, url, json, timeout):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        return FakeResponse()


class ProtectedExecutionAPI:
    """Offline broker double that exposes an accepted protected position."""

    def __init__(self, plan):
        self.plan = plan
        self.open_payload = None
        self.updated = False
        self.closed = False

    def open_position(self, **payload):
        self.open_payload = payload
        return {"dealReference": "o_position_reference"}

    def confirm_deal(self, reference):
        assert reference == "o_position_reference"
        return {"dealStatus": "ACCEPTED", "dealId": "D-TEST"}

    def get_all_positions(self):
        return [{
            "market": {"epic": "US500"},
            "position": {
                "dealId": "D-TEST",
                "stopLevel": self.plan.stop,
                "limitLevel": self.plan.target,
            },
        }]

    def get_working_orders(self):
        return []

    def update_position(self, **_):
        self.updated = True
        raise AssertionError("Protection repair should not be needed")

    def close_position(self, *_):
        self.closed = True
        raise AssertionError("Protection close should not be needed")


def test_stop_order_payload() -> None:
    """Verify the adapter sends the documented STOP working-order contract."""
    adapter = load_module("us500_adapter_test", "capitalcom_api.py")
    api = adapter.CapitalComAPI("key", "identifier", "password", demo=True)
    session = FakeSession()
    api._session = session
    api._ensure_session = lambda: None
    response = api.place_stop_order(
        epic="US500",
        direction="BUY",
        size=1.23,
        level=6001.1,
        stop_level=5990.0,
        profit_level=6023.3,
        good_till_date="2026-09-08T16:30:00",
    )
    assert response["dealReference"] == "o_test_reference"
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"].endswith("/api/v1/workingorders")
    assert call["timeout"] == 15
    assert call["json"] == {
        "epic": "US500",
        "direction": "BUY",
        "size": 1.23,
        "level": 6001.1,
        "type": "STOP",
        "guaranteedStop": False,
        "stopLevel": 5990.0,
        "profitLevel": 6023.3,
        "goodTillDate": "2026-09-08T16:30:00",
    }


def test_live_runner_offline_safety() -> None:
    """Run the runner's deterministic filter, DST, sizing, and dry-run tests."""
    runner = load_module("us500_live_runner_test", "us500-ny-open-bos-body75-live-runner.py")
    runner.run_self_test()


def test_protected_execution_contract() -> None:
    """Verify live execution passes SL and TP in the single entry request."""
    runner = load_module("us500_live_runner_execution_test", "us500-ny-open-bos-body75-live-runner.py")
    constraints = runner.BrokerConstraints(0.01, 0.01, 0.1, 0.0, 0.0)
    plan = runner.TradePlan(
        direction="BUY", entry=6000.0, stop=5990.0, target=6020.0,
        size=1.0, planned_risk_aed=36.725, stop_distance=10.0,
        target_distance=20.0, adverse_slippage_points=0.0,
        stop_source="nearest confirmed swing low",
    )
    api = ProtectedExecutionAPI(plan)
    receipt = runner.submit_protected_market_order(
        api, plan, constraints, True, runner.logging.getLogger("us500_execution_test")
    )
    assert receipt["mode"] == "EXECUTED"
    assert receipt["deal_id"] == "D-TEST"
    assert api.open_payload == {
        "epic": "US500",
        "direction": "BUY",
        "size": 1.0,
        "stop_level": 5990.0,
        "profit_level": 6020.0,
        "guaranteed_stop": False,
    }
    assert not api.updated and not api.closed


if __name__ == "__main__":
    test_stop_order_payload()
    test_live_runner_offline_safety()
    test_protected_execution_contract()
    print("Offline live-runner tests passed: STOP payload and runner safeguards.")
