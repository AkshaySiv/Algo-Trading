# Research Evidence — Tradesharpe Setup 1 Session Breakout

## Video source

The supplied video is **“Full Trading & Forex Course 2025 | A - Z Price Action By Tradesharpe”**, published December 22, 2024, with a running time of 1:48:32. Its public video page identifies chapters for **Session Bias** at 44:00 and **Setups 1, 2, 3** at 53:03. The video page exposed chapter metadata but did not return an accessible transcript in this environment. The mechanism in the final Pine script therefore implements only rules that can be supported by the other supplied material and plainly labels all proxies.

## Setup 1 rules recovered from the supplied journal and public handbook

A publicly accessible handbook describes the technical approach as trading pullbacks and breakouts from one level or range to the next, respecting session bias, and generally taking 15-minute setups during the London, New York, and Frankfurt high-volume sessions. Its Setup 1 description calls for a breakout of a range or zone and says not to buy into a major four-hour resistance or sell into a daily support. It warns that, after a large move, high momentum with no pullback, an important-level break, or a poor breakout period/area, the trader should wait for new structure. It rejects news-driven breakouts except where post-news structure subsequently forms. It also advises checking whether the expected session continuation has already happened.

## Shared spreadsheet evidence

The supplied spreadsheet’s *The Journal* tab contains 219 rows labelled `SET UP 1`. They cover GBPJPY, XAUUSD, DAX, NAS, and smaller samples of other instruments. There are 140 recorded 15-minute entries, 74 five-minute entries, three 30-minute entries, and two one-hour entries. The workbook includes positive, negative, and break-even recorded outcomes. It does not contain an auditable full signal population, session time-zone definition, entry price, initial stop, target, transaction costs, or an unambiguous record of trade management. It is evidence that the idea was applied across several markets but is not a complete backtest.

## Pine Script compatibility verification

The official Pine Script v6 function list contains `input.string()`, `input.session()`, `input.time()`, and `input.timeframe()`, but no `input.timezone()` function. The final implementation uses a restricted `input.string()` set of IANA time zones and passes that string as the `timezone` parameter of `time()`.

## Sources

[1]: https://www.youtube.com/watch?v=60d85BdZ6_E "Full Trading & Forex Course 2025 | A - Z Price Action By Tradesharpe"
[2]: https://www.scribd.com/document/1022024196/PA-Trading-Complete-Handbook "PA Trading - Complete Handbook"
[3]: https://docs.google.com/spreadsheets/d/1ybiIYEUzVnxUUhN1z4QyE6lU5XGggE3u/edit?gid=1311789311#gid=1311789311 "Tradesharpe Setup 1 journal"
[4]: https://www.tradingview.com/pine-script-reference/v6/ "Pine Script Language Reference Manual — Version 6"
