#!/usr/bin/env python3
"""Focused unit tests for smc_ob_fvg_bos_backtest.py."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from smc_ob_fvg_bos_backtest import (
    Bar,
    OpenTrade,
    fair_value_gap,
    last_opposing_candle,
    resolve_exit,
)

UTC = timezone.utc


def bar(offset: int, open_: float, high: float, low: float, close: float) -> Bar:
    return Bar(
        timestamp=datetime(2025, 1, 2, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * offset),
        open=open_, high=high, low=low, close=close,
    )


class TestSmcBacktestRules(unittest.TestCase):
    def setUp(self) -> None:
        self.config = {"same_bar_exit_policy": "stop_first"}

    def test_bullish_fvg_requires_three_candle_gap(self) -> None:
        bars = [
            bar(0, 100.0, 101.0, 99.0, 100.5),
            bar(1, 100.5, 104.0, 100.2, 103.8),
            bar(2, 103.5, 106.0, 102.0, 105.5),
        ]
        self.assertAlmostEqual(fair_value_gap(bars, 2, "long", 0.5), 1.0)
        self.assertIsNone(fair_value_gap(bars, 2, "long", 1.1))

    def test_bearish_fvg_requires_three_candle_gap(self) -> None:
        bars = [
            bar(0, 105.0, 106.0, 103.0, 104.0),
            bar(1, 104.0, 104.2, 100.0, 100.5),
            bar(2, 100.5, 102.0, 98.0, 99.0),
        ]
        self.assertAlmostEqual(fair_value_gap(bars, 2, "short", 0.5), 1.0)
        self.assertIsNone(fair_value_gap(bars, 2, "short", 1.1))

    def test_last_opposing_candle_for_long_is_bearish(self) -> None:
        bars = [
            bar(0, 100.0, 101.0, 99.5, 100.8),
            bar(1, 100.8, 101.2, 99.8, 100.0),  # bearish, should be selected
            bar(2, 100.0, 104.0, 99.9, 103.8),
        ]
        chosen = last_opposing_candle(bars, 2, "long", 2)
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.timestamp, bars[1].timestamp)

    def test_same_bar_stop_first_is_conservative(self) -> None:
        trade = OpenTrade(
            direction="long", entry_time=bar(0, 100, 101, 99, 100).timestamp,
            entry=100.0, stop=98.0, target=104.0, htf="HOUR",
            fvg_size=1.0, source_ob_low=99.0, source_ob_high=100.0,
        )
        simultaneous = bar(1, 100.0, 104.5, 97.5, 101.0)
        exit_price, reason = resolve_exit(trade, simultaneous, self.config)
        self.assertEqual((exit_price, reason), (98.0, "stop_same_bar"))


if __name__ == "__main__":
    unittest.main()
