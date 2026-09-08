# US500 09:30 New York First-Swing BOS Live Runner

## Purpose

`us500-ny-open-bos-body75-live-runner.py` implements the tested US500 1-minute first-swing BOS rules as a guarded Capital.com runner. It uses the **09:30 New York cash open**, so the observed IST session changes automatically with U.S. daylight saving time:

| U.S. time regime | Entry window in New York | Entry window in IST |
|---|---|---|
| U.S. daylight time | 09:30–12:30 | 19:00–22:00 |
| U.S. standard time | 09:30–12:30 | 20:00–23:00 |

The runner supports both **BUY** and **SELL** BOS signals. A first confirmed pivot high and pivot low are established after the opening. The first qualifying close through the high creates a BUY candidate; the first qualifying close through the low creates a SELL candidate. It permits no more than one trade per New York session.

## Exact strategy implementation

The live runner preserves the previously tested rules:

1. It uses 1-minute Capital.com bid/ask OHLC data.
2. It confirms pivots using 5 candles on each side.
3. It requires both initial swing boundaries before any signal.
4. It uses close-based BOS, not wick-based BOS.
5. It accepts a BOS only when the completed breakout candle meets the quality condition:

   ```text
   abs(close_bid - open_bid) / (high_bid - low_bid) >= 0.75
   ```

6. The stop source is the latest confirmed opposing swing, then the first swing, then the breakout candle extreme.
7. The target is recalculated from the fresh broker quote at exactly **2×** the price-stop distance.
8. The size is rounded **down** to the current Capital.com size increment so planned price-stop risk cannot exceed `--risk-aed`.

## Protected order behaviour

> The runner never submits a naked market order. Every live entry is sent with absolute `stopLevel` and `profitLevel` in the same Capital.com position request.

The runner also verifies the broker-reported protection levels after acceptance. If the broker reports missing or mismatched protection, it attempts one protected update. If that update cannot be verified, the runner attempts an immediate close of the unprotected position and writes a locked state that requires manual review.

### Why this strategy uses a protected market entry

Capital.com supports `STOP` working orders, and `capitalcom_api.py` now contains `place_stop_order()` using the documented `POST /api/v1/workingorders` contract with `type: "STOP"`, `level`, `stopLevel`, `profitLevel`, and optional `goodTillDate`.[1]

A pre-armed STOP entry is **not used by this strategy**, because it would enter before the complete 1-minute BOS candle exists. The 75% body condition is only known after that candle closes. Pre-arming a breakout stop would therefore implement a different, unfiltered strategy.

The correct execution path for this exact close-confirmed strategy is:

1. Wait for the one-minute candle to close.
2. Verify BOS and body fraction ≥75% using only completed bars.
3. Require the signal to be less than 70 seconds old; stale signals are rejected.
4. Fetch a fresh bid/offer quote.
5. Reject the order if adverse entry slippage exceeds the configured cap.
6. Recalculate size, stop distance, and 1:2 target from the fresh executable side of the quote.
7. Submit one broker-protected market position and confirm it.
8. Verify the accepted position has the expected SL and TP.

This is a **fail-closed fallback policy**: if price is stale, market status is not tradeable, slippage is excessive, sizing is invalid, broker protection cannot be verified, or account exposure is ambiguous, the runner submits **no replacement naked order**.

## Safety controls

| Control | Behaviour |
|---|---|
| Dry-run default | No position, working order, update, close, or cancellation request is sent unless `--execute` is supplied. |
| Explicit arming | Execution also requires `--yes-i-accept-live-orders`. |
| Live-account gate | When `CAPITAL_DEMO=false`, execution additionally requires `--allow-live-account`. |
| Explicit risk | There is no default live-risk amount. Every invocation must specify a positive `--risk-aed`. |
| One-trade guard | A persisted state records the accepted trade before the next scan; no second daily entry is permitted. |
| Single-process lock | An OS lock prevents two runner processes from controlling the same state. |
| Unmanaged-exposure guard | The runner stops if it sees an untracked US500 position or working order, avoiding interference with manual trades. |
| Freshness guard | A signal more than 70 seconds old is rejected; a restart cannot turn an old historical candle into a late market entry. |
| Slippage guard | The runner rejects a market entry when adverse slippage exceeds `--max-entry-slippage-points` (default 2.0 points). |
| Broker constraints | Current minimum size, deal increment, price increment, and minimum stop/limit distances are read from Capital.com before execution. |
| Broker protection verification | The runner checks post-confirmation SL and TP, attempts one repair, then attempts a protective close and locks if verification fails. |
| Session reset | A tracked trade is closed at the next 09:30 New York session reset. The runner only closes its own persisted deal ID. |

## Setup

Create `US500/.env` locally. This file is ignored by Git and must never be committed.

```dotenv
CAPITAL_DEMO=true
CAPITAL_API_KEY=...
CAPITAL_IDENTIFIER=...
CAPITAL_PASSWORD=...
```

Install the Python dependencies once in the runtime environment:

```bash
python3 -m pip install --user python-dotenv requests
```

Run the offline tests first. These make no network or Capital.com calls:

```bash
cd US500
python3 test_us500_ny_open_live_runner.py
```

## First use: demo dry-run

Run a single scan cycle against the **demo** account. It can read current market data and account constraints but cannot submit, update, close, or cancel an order:

```bash
cd US500
python3 us500-ny-open-bos-body75-live-runner.py \
  --risk-aed 40 \
  --once
```

For continuous **demo dry-run** observation, omit `--once`:

```bash
python3 us500-ny-open-bos-body75-live-runner.py \
  --risk-aed 40 \
  --poll-seconds 5
```

## Explicit demo order mode

Only after reviewing several dry-run sessions, an operator may deliberately arm **demo** order submission:

```bash
python3 us500-ny-open-bos-body75-live-runner.py \
  --risk-aed 40 \
  --poll-seconds 5 \
  --execute \
  --yes-i-accept-live-orders
```

`--risk-aed 1000` is permitted by code but represents **25% risk on an AED 4,000 starting balance**. The prior historical risk-scale study showed a maximum realised drawdown that exceeded the starting balance under this risk amount. It should not be used as an initial demo or live setting.

## Continuous operation options

The runner scans 1-minute data and requires persistence. Choose one of these two options before continuous operation.

| Approach | Trade-offs | Cost | Setup complexity |
|---|---|---:|---|
| Keep the runner on a machine you control | No third-party hosting cost; the machine must remain powered, connected, and supervised during the New York entry window. | No added hosting cost | Low |
| Deploy as a persistent service with logs, restart policy, and protected environment variables | Runs independently of a personal machine and is better suited to continuous polling. It requires server administration and secure credential management. | Depends on the chosen host | Medium |

Do **not** use a task scheduler that starts a full agent session every minute. The strategy is deterministic and needs a persistent low-latency process instead.

## Manual recovery

When state is `LOCKED`, stop the process and review the `state/us500_ny_open_bos_body75_live_state.json` record, the Capital.com account, and `logs/us500_ny_open_bos_body75_live.log` before restarting.

Do not delete state files while a position may be open. The state is how the runner distinguishes its own position from a manual position. A fresh state file is only appropriate after confirming there is no open US500 position or working order belonging to the runner.

## Verification status

The included offline suite validates:

- U.S. daylight and standard-time session conversion;
- causal first-swing detection and 75% body filter;
- size rounding and risk-cap compliance;
- stop and target price rounding;
- dry-run rejection of order API calls;
- the exact Capital.com `STOP` working-order request payload used by the adapter.

The test suite does not execute live or demo orders. Before any live account use, the complete order lifecycle must be observed in the Capital.com demo account, including acceptance, broker-side SL/TP attachment, position reconciliation, target exit, stop exit, restart recovery, and session-reset close.

## References

[1]: https://open-api.capital.com "Capital.com REST API documentation"

**Risk disclosure.** CFDs are leveraged products and can lose money rapidly. Historical backtests, demo fills, and broker-side stop orders do not eliminate execution, gap, liquidity, or platform risks. This is research and software engineering work only, not personalized financial advice.
