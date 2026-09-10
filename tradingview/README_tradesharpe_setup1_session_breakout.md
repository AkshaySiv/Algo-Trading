# Tradesharpe Setup 1 — 15-Minute Session Breakout

## Purpose

This Pine Script is a **visual planning indicator** for a mechanical approximation of the Setup 1 concept: a confirmed 15-minute breakout from a session-created range or zone. It paints the range, highlights only qualified potential BUY or SELL areas, calculates a structure-based stop and fixed-R planning target, and prevents a second signal in the same selected session.

The original method is partly discretionary. Its high-level description is to trade a breakout from a range/zone in the session direction, without trading directly into a major higher-timeframe level. It also says to wait for fresh structure after an extended move, high momentum without a pullback, a key-level break, news, or a period when breakouts are less reliable.[1] The indicator converts those judgment calls into inputs and objective filters. It cannot reproduce discretionary chart reading or classify news events from OHLC data.

## Required chart configuration

Apply `tradesharpe_setup1_session_breakout.pine` to a **15-minute standard-candle chart**. The script displays a yellow `!` marker on another timeframe and blocks trade signals there. It is intended for spot-Forex pairs, gold or index CFDs, and similar liquid intraday instruments. Signal levels will reflect the chart provider's price series, so CFD levels can differ modestly from the related futures market.

The default time zone is `Europe/London`. This makes the London session stable across UK daylight-saving changes. London and New York session windows remain user-adjustable because the United States and the United Kingdom change clocks on different dates in March and October. Forex sessions are not exchange sessions; brokers can also use different daily-rollover timestamps.

| Default enabled session | Default window in `Europe/London` | Purpose |
| --- | --- | --- |
| London | 08:00–11:00 | Primary European high-liquidity window |
| New York | 13:30–16:30 | U.S. high-liquidity window |
| Frankfurt | Disabled; 06:00–08:00 if enabled | Optional pre-London window |

Each session maintains its own state. A session can produce **one total signal only**: either a BUY or a SELL. A second setup is never generated after the first qualifying setup, even after its stop or target is reached.

## Mechanical rules implemented

### 1. Session structure

At the beginning of each enabled session, the indicator collects the high and low of the first `rangeBarsRequired` 15-minute candles. The default is four bars, which creates one hour of opening structure. Once that range is locked, it does not expand. This is intentional: a later candle must prove a close out of pre-existing structure rather than redefine the range while breaking it.

The range must be neither too small nor too large relative to ATR. Default bounds are `0.25 × ATR` to `1.50 × ATR`. This rejects very narrow noise ranges and opening periods already expanded by abnormal volatility.

### 2. Directional bias

The default bias is `Daily + 4H alignment`:

- A potential BUY requires the prior completed daily candle and prior completed four-hour candle to be bullish.
- A potential SELL requires both to be bearish.

This is a deliberately simple and inspectable proxy for the video's discretionary session-bias concept. It uses completed higher-timeframe candles only. Use `Previous Daily` for a less restrictive filter, or `Off` to study unfiltered breakouts. None of these options should be interpreted as a complete definition of market structure.

### 3. Entry qualification

A BUY label appears only when a completed 15-minute candle closes above the locked range high by the configured ATR buffer. A SELL label appears only when a completed 15-minute candle closes below the locked range low by the same buffer.

The breakout candle must have a substantial body relative to its own high-to-low range. The default body requirement is 50%. A wick through a boundary with a weak close is therefore rejected. The script also rejects an unusually large breakout bar and a signal after price has already travelled too far from the session opening price. These two conditions are transparent OHLC-based proxies for the source material's caution against news-driven, extended, or already-completed moves.

### 4. Major-level room filter

The source method says not to buy into a major resistance or sell into a major support.[1] The indicator estimates those barriers using confirmed 4-hour and daily pivot highs/lows. For a BUY, the fixed-R target must remain below the nearest known 4-hour or daily resistance by the configured clearance. For a SELL, the target must remain above the nearest known 4-hour or daily support.

A pivot requires completed right-side bars before it is known. This creates a deliberate confirmation delay and avoids future-data repainting. Pivot levels are only an objective proxy for a manually drawn major level. Keep the 4-hour and daily levels visible, and reject a signal manually if price is running into a clear unmarked level.

### 5. Initial stop and target

| Direction | Entry | Initial stop | Planning target |
| --- | --- | --- | --- |
| BUY | Close of the qualifying breakout bar | Locked range low minus `stopBufferATR × ATR` | Entry + (`riskReward × initial risk`) |
| SELL | Close of the qualifying breakout bar | Locked range high plus `stopBufferATR × ATR` | Entry − (`riskReward × initial risk`) |

The source material describes trading from one level/range to the next, rather than prescribing one universal risk/reward ratio.[1] The target is therefore a planning value, not a claim that the video mandates `1R`. Change `Fixed target in R` only after testing it on a stable sample. The script marks a target/stop collision in one 15-minute bar as **stop first** because the intrabar path cannot be recovered from that bar's OHLC values.

By default, an active plan is marked closed at the end of its session. This avoids carrying a session-specific setup into the next session. It is not a broker order and does not represent an actual execution fill.

## What the shared workbook adds

The shared journal includes 219 rows labelled Set Up 1 across GBPJPY, XAUUSD, DAX, NAS and other instruments. Of these, 140 use a 15-minute entry timeframe. The rows include winners, losses and break-even outcomes, confirming that a range break alone is not sufficient. However, the worksheet does not contain the full universe of rejected setups, initial entry/stop/target prices, a common risk unit, session boundaries, or complete trade-management records. It should not be treated as an auditable historical backtest.

The [Setup 1 journal evidence summary](setup1_journal_evidence_summary.md) reports the raw counts and data gaps. In particular, the workbook notes cases involving news, bad markup, higher-timeframe context, low volatility, drawdown, early break-even movement and manual management. These notes support retaining the script's filters and treating every label as a **potential area**, not an automatic trade.

## Recommended use

Open the chart during a selected session. First check whether the daily/4-hour context and visible major levels support the direction. Then wait for the cyan, orange, or magenta session range to lock. A BUY/SELL triangle plus label means all objective filters passed. Review the displayed entry, stop and target before acting. If the chart has an obvious key level, a scheduled high-impact release, abnormal broker spread, or a move already beyond normal session behaviour, skip the setup even if the label appears.

Create TradingView alerts only with **Once Per Bar Close**. The script requires `barstate.isconfirmed`, but configuring an alert at bar close prevents a premature alert during a still-forming candle.

## Important limits

This indicator has no news calendar, true order-flow data, spread-aware CFD execution logic, position sizing, slippage model, partial profit-taking logic, or automatic live execution. Its labels are not trade recommendations and do not validate profitability. Test each instrument, broker feed, session window, and parameter set out of sample with spreads and realistic transaction costs before risking capital.

## Sources

[1]: https://www.scribd.com/document/1022024196/PA-Trading-Complete-Handbook "PA Trading - Complete Handbook"
[2]: https://www.youtube.com/watch?v=60d85BdZ6_E "Full Trading & Forex Course 2025 | A - Z Price Action By Tradesharpe"
