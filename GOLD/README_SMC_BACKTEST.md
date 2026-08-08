# XAUUSD Asian SMC Backtest

## What this tool does

`smc_ob_fvg_bos_backtest.py` is a **historical-only** tester for the XAUUSD Asian-session setup discussed in this project. It uses 5-minute Capital.com OHLC data and produces `trades.csv`, `monthly_summary.csv`, and `summary.json`. It contains no order-placement, position-modification, or position-closing calls.

The tool measures **historical results in R**, where one R equals the initial stop-loss distance. It reports the trade count, win rate, net R, average R, profit factor, maximum drawdown in R, month-by-month results, and a chronological out-of-sample segment. These are measurements of the selected data and rules, not evidence of future profitability.

## Exact code rules

The terms **order block**, **BOS**, and **FVG** are subjective when drawn manually. This tester uses the following fixed definitions so the test can be reproduced.

| Step | Long rule | Short rule |
|---|---|---|
| Higher-timeframe order block | On 1H or 4H, a candle closes above the latest confirmed swing high. The zone is the last bearish candle in the configured lookback before that break. | On 1H or 4H, a candle closes below the latest confirmed swing low. The zone is the last bullish candle in the configured lookback before that break. |
| Freshness | The zone is ignored if it was tapped after its creation and before the current session setup. | The same rule applies. |
| Asian-session tap | From 05:30 to 11:30 IST, 5-minute price overlaps the HTF bullish zone. By default, it must also make a lower low than the previous 12 five-minute bars. | From 05:30 to 11:30 IST, price overlaps HTF bearish zone and, by default, makes a higher high than the previous 12 five-minute bars. |
| Lower-timeframe BOS | After the tap, a 5-minute candle **closes above** the latest confirmed five-minute swing high. | After the tap, a 5-minute candle **closes below** the latest confirmed five-minute swing low. |
| FVG | The current bar low is above the high from two bars earlier by at least the configured size. | The current bar high is below the low from two bars earlier by at least the configured size. |
| Local order block / entry | The final bearish candle before the BOS is the local block. A simulated limit entry waits at its midpoint. | The final bullish candle before the BOS is the local block. A simulated limit entry waits at its midpoint. |
| Stop and target | Stop is below the Asian sweep extreme plus a buffer. Target is entry plus the configured R multiple. | Stop is above the Asian sweep extreme plus a buffer. Target is entry minus the configured R multiple. |

The default parameters are in `smc_backtest_config.example.json`. Copy that file before changing inputs, so every test run keeps an auditable parameter set.

## Capital.com historical data setup

Capital.com documents its market search endpoint as `GET /markets?searchTerm=...` and historical-prices endpoint as `GET /prices/{epic}`. Its documentation also states that API access requires an API key and a `POST /session` authentication flow. [1]

Capital.com states that its API keys do not have a read-only privilege; the key type allows trading. [1] Therefore, use a **dedicated Capital.com demo account**, create a dedicated API key with a custom API password, and never share the values in chat or commit them to Git.

Create `GOLD/.env` locally; this file is ignored by Git:

```dotenv
CAPITAL_API_KEY=replace_with_your_dedicated_demo_key
CAPITAL_IDENTIFIER=your_capital_login_email
CAPITAL_PASSWORD=the_custom_password_created_for_this_api_key
```

The downloader will use the demo API only when `--demo` is included. It only requests historical market data, but the account/key selection remains your responsibility.

## Run a Capital.com backtest

From the repository root, request a period with an eight-day warm-up for the higher-timeframe calculations:

```bash
cd GOLD
python3 smc_ob_fvg_bos_backtest.py \
  --from 2025-01-01 \
  --to 2025-12-31 \
  --download --demo --verify-epic
```

The script saves normalized data under `GOLD/data/` and results under `GOLD/backtest_output/smc_ob_fvg_bos/`. Both paths are ignored by Git. The summary reports three sections: **overall**, **in-sample**, and the final chronological **out-of-sample** portion. The out-of-sample result is useful because the parameters are kept fixed rather than selected after viewing that segment.

## Run from a previously downloaded CSV

The downloader writes a normalized CSV containing `timestamp_utc`, `open`, `high`, `low`, and `close`. You can rerun the exact test without using Capital.com again:

```bash
cd GOLD
python3 smc_ob_fvg_bos_backtest.py \
  --from 2025-01-01 \
  --to 2025-12-31 \
  --csv data/GOLD_MINUTE_5_2025-01-01_2025-12-31.csv
```

## How to judge “consistent” historical results

There is no single universal threshold. A cautious review should require a non-trivial number of trades, positive out-of-sample net R after the configured spread deduction, a profit factor above 1, tolerable maximum drawdown, and positive outcomes in more than a few isolated months. Do not change parameters repeatedly until the historical result looks good; that creates an overfitting risk. Keep the final configuration fixed and test it on a later untouched period.

## Known limitations

The signal uses midpoint OHLC for chart logic and deducts a fixed configurable spread in R. It does not model variable spread, gaps, financing, commissions, slippage, partial fills, latency, or account-specific contract sizing. An OHLC bar does not disclose the exact order of price movements within a five-minute bar; where both stop and target are reachable in the same bar, the default is the conservative `stop_first` assumption.

> This code is research tooling, not execution software and not personalized financial advice. CFDs are leveraged instruments and can incur material losses.

## References

[1]: https://open-api.capital.com/ "Capital.com Public API documentation"
