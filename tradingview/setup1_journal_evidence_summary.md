# Setup 1 Workbook Evidence Summary

This is a descriptive audit of the user-provided journal. It does not constitute a leakage-controlled strategy backtest because the workbook does not record the entry price, initial stop, target, session boundary, risk per trade, or the complete population of valid and rejected signals.

## Recorded sample

| Metric | Value |
| --- | ---: |
| Setup 1 rows | 219 |
| Positive outcomes | 109 |
| Negative outcomes | 47 |
| Break-even outcomes | 63 |
| Win rate excluding break-even | 69.9% |
| Mean recorded result | 7.22 pips/points |

## Results by entry timeframe

| Timeframe | Trades | Wins | Losses | Break-even | Mean result |
| --- | ---: | ---: | ---: | ---: | ---: |
| 15Min | 140 | 67 | 32 | 41 | 5.86 |
| 1HR | 2 | 0 | 1 | 1 | -12.50 |
| 30Min | 3 | 1 | 0 | 2 | 5.67 |
| 5Min | 74 | 41 | 14 | 19 | 10.39 |

## Results by instrument

| Instrument | Trades | Wins | Losses | Break-even | Mean result |
| --- | ---: | ---: | ---: | ---: | ---: |
| GBPJPY | 76 | 40 | 15 | 21 | 4.48 |
| XAUUSD | 62 | 30 | 11 | 21 | 11.37 |
| DAX | 56 | 30 | 13 | 13 | 10.55 |
| NAS | 20 | 7 | 5 | 8 | -1.30 |
| GBPUSD | 3 | 1 | 2 | 0 | -5.00 |
| XAGUSD | 2 | 1 | 1 | 0 | -7.50 |

## What the journal clarifies

The recorded Set Up 1 examples span foreign-exchange pairs, XAUUSD, DAX, NAS/US indices, and several entry timeframes. This supports treating Set Up 1 as a general breakout-from-range/zone concept rather than a single-symbol rule. The sample also contains winning, losing, and break-even trades. It therefore does not support a claim that every range break is tradable.

## What remains non-mechanical

The journal does not capture the definition of a valid range/zone, how the session bias was decided, what qualifies as a major support or resistance level, whether a liquidity sweep/structure reset was required, precise session hours and time zone, the entry buffer, initial stop placement, target selection, break-even management, news avoidance, or invalidation after an extended move. Consequently, those concepts must be represented in Pine with transparent configurable proxies rather than asserted as exact rules from the journal.

## Selected Set Up 1 records

| Date | Instrument | TF | Direction | Result | Notes | Chart |
| --- | --- | --- | --- | ---: | --- | --- |
| 2023-06-07 | GBPJPY | 15Min | Sell | 16.00 | — | [chart](https://www.tradingview.com/x/ktP4fVUu/) |
| 2023-06-12 | GBPJPY | 15Min | Buy | 19.00 | — | [chart](https://www.tradingview.com/x/Dc3wTE1l/) |
| 2023-06-14 | GBPJPY | 15Min | Buy | 15.00 | — | [chart](https://www.tradingview.com/x/R0Lc3Swu/) |
| 2023-06-16 | GBPJPY | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/3XQdvUif/) |
| 2023-06-23 | GBPJPY | 15Min | Buy | 17.00 | — | [chart](https://www.tradingview.com/x/rlGkh5XY/) |
| 2023-07-04 | GBPJPY | 15Min | Buy | 12.00 | — | [chart](https://www.tradingview.com/x/WLwOLWRE/) |
| 2023-07-07 | GBPJPY | 15Min | Sell | 10.00 | — | [chart](https://www.tradingview.com/x/telVnPTW/) |
| 2023-07-25 | GBPJPY | 15Min | Buy | 13.00 | — | [chart](https://www.tradingview.com/x/Sc39Tt97/) |
| 2023-08-07 | GBPJPY | 15Min | Buy | 17.00 | 1hr Setup 2 | [chart](https://www.tradingview.com/x/R6woGdfm/) |
| 2023-08-10 | GBPJPY | 15Min | Buy | 0.00 | Played out fully some held was at WR so aggresive managment held first pos | [chart](https://www.tradingview.com/x/xVRc9g7F/) |
| 2023-08-17 | GBPJPY | 15Min | Buy | -16.00 | push 7 pip profit | [chart](https://www.tradingview.com/x/RWYrCFFh/) |
| 2023-08-23 | GBPJPY | 15Min | Sell | 16.00 | heavy drawdown 10 am cleaner move | [chart](https://www.tradingview.com/x/LY8Hwh91/) |
| 2023-08-30 | GBPJPY | 15Min | Buy | 16.00 | — | [chart](https://www.tradingview.com/x/7tXQvKL0/) |
| 2023-08-31 | GBPJPY | 5Min | Sell | 12.00 | — | [chart](https://www.tradingview.com/x/JU1cTStZ/) |
| 2023-09-06 | GBPJPY | 15Min | Buy | 0.00 | News move | [chart](https://www.tradingview.com/x/WaAMPNpP/) |
| 2023-09-13 | GBPJPY | 15Min | Sell | 0.00 | correct decison would of hit SL | [chart](https://www.tradingview.com/x/7a0FsHY7/) |
| 2023-09-15 | GBPJPY | 15Min | Buy | 5.00 | closed 5 pips but played out | [chart](https://www.tradingview.com/x/sLpIVRJf/) |
| 2023-09-18 | GBPJPY | 5Min | Sell | 12.00 | — | [chart](https://www.tradingview.com/x/6JnLV3Kp/) |
| 2023-09-28 | GBPJPY | 15Min | Sell | 10.00 | — | [chart](https://www.tradingview.com/x/4VoT5Tqo/) |
| 2023-10-03 | GBPJPY | 15Min | Buy | -16.00 | — | [chart](https://www.tradingview.com/x/NKi0I0pV/) |
| 2023-10-06 | GBPJPY | 5Min | Buy | 11.00 | — | [chart](https://www.tradingview.com/x/24ZtiSko/) |
| 2023-10-10 | GBPJPY | 15Min | Buy | 0.00 | TP was hit but alot of rejection wicks | [chart](https://www.tradingview.com/x/YsrrguO9/) |
| 2023-10-11 | GBPJPY | 15Min | Buy | -12.00 | — | [chart](https://www.tradingview.com/x/geC2eZer/) |
| 2023-10-26 | GBPJPY | 15Min | Buy | 0.00 | DST | [chart](https://www.tradingview.com/x/sw095Ufc/) |
| 2023-11-01 | GBPJPY | 15Min | Buy | -15.00 | — | [chart](https://www.tradingview.com/x/m3LLVwNa/) |
| 2023-11-06 | XAUUSD | 15Min | Buy | 12.00 | Managed at entry left rest | [chart](https://www.tradingview.com/x/JtBZ4rPK/) |
| 2023-11-13 | XAUUSD | 15Min | Sell | 0.00 | ran 10 pips then BE | [chart](https://www.tradingview.com/x/1hysgcqQ/) |
| 2023-11-15 | XAUUSD | 15Min | Buy | -17.00 | played out stop to tight | [chart](https://www.tradingview.com/x/ilTmbfri/) |
| 2023-11-17 | GBPJPY | 5Min | Sell | 0.00 | agressive 1 min entry | [chart](https://www.tradingview.com/x/TkR3p7XP/) |
| 2023-11-22 | XAUUSD | 15Min | Buy | 11.00 | — | [chart](https://www.tradingview.com/x/MX9tN6Jr/) |
| 2023-12-01 | GBPJPY | 15Min | Buy | 13.00 | — | [chart](https://www.tradingview.com/x/wkc3xwlX/) |
| 2023-12-04 | GBPJPY | 15Min | Buy | 15.00 | — | [chart](https://www.tradingview.com/x/tlP3cFih/) |
| 2023-12-05 | GBPJPY | 15Min | Sell | 0.00 | — | [chart](https://www.tradingview.com/x/9s4YpYN5/) |
| 2024-01-09 | XAUUSD | 1HR | Buy | -25.00 | — | [chart](https://www.tradingview.com/x/faRQNzgF/) |
| 2024-01-25 | GBPJPY | 15Min | Buy | 11.00 | — | [chart](https://www.tradingview.com/x/8DT74lMV/) |
| 2024-02-05 | GBPJPY | 15Min | Buy | 16.00 | — | [chart](https://www.tradingview.com/x/YCgcJM81/) |
| 2024-02-07 | GBPJPY | 15Min | Buy | 0.00 | would of hit SL | [chart](https://www.tradingview.com/x/0TgkS7xI/) |
| 2024-02-13 | GBPJPY | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/dJfK2eaU/) |
| 2024-02-15 | GBPJPY | 15Min | Sell | 0.00 | 1hr opend no top wick at entry but would of hit TP | [chart](https://www.tradingview.com/x/RdyMacjD/) |
| 2024-02-22 | GBPJPY | 15Min | Buy | 16.00 | 25 pips but closed some so net 16pips | [chart](https://www.tradingview.com/x/gAhUp1UH/) |
| 2024-02-23 | GBPJPY | 15Min | Buy | 11.00 | — | [chart](https://www.tradingview.com/x/D8ms64Ic/) |
| 2024-02-27 | GBPJPY | 15Min | Buy | -11.00 | — | [chart](https://www.tradingview.com/x/AAVMiDVQ/) |
| 2024-03-01 | GBPJPY | 15Min | Buy | 7.00 | Over managed closed some at entry was a 21 pip move! | [chart](https://www.tradingview.com/x/X2qVnzRX/) |
| 2024-03-04 | GBPJPY | 5Min | Buy | 9.50 | — | [chart](https://www.tradingview.com/x/noRGmBxC/) |
| 2024-03-07 | GBPJPY | 5Min | Sell | 0.00 | would of hit TP | [chart](https://www.tradingview.com/x/2nkpVnqe/) |
| 2024-03-12 | GBPJPY | 15Min | Sell | 16.00 | — | [chart](https://www.tradingview.com/x/JVpUXyio/) |
| 2024-03-14 | GBPJPY | 15Min | Buy | 8.00 | CLOSED 80% attemted runner stopped BE net 8 pips | [chart](https://www.tradingview.com/x/KqMGmeOL/) |
| 2024-03-21 | GBPJPY | 5Min | Buy | 15.00 | — | [chart](https://www.tradingview.com/x/aRCFtLo5/) |
| 2024-03-27 | GBPJPY | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/gIYGvWOF/) |
| 2024-04-03 | GBPJPY | 5Min | Sell | -5.00 | went for 4th deep liquidity grab  then played out | [chart](https://www.tradingview.com/x/j3hrCqvn/) |
| 2024-04-10 | GBPJPY | 15Min | Buy | 11.00 | — | [chart](https://www.tradingview.com/x/8ncOfu4t/) |
| 2024-04-12 | XAUUSD | 15Min | Buy | 23.00 | — | [chart](https://www.tradingview.com/x/ZcgPsbD2/) |
| 2024-04-16 | GBPJPY | 15Min | Buy | 0.00 | would have hit TP | [chart](https://www.tradingview.com/x/mPDUkE5N/) |
| 2024-04-18 | GBPJPY | 15Min | Buy | -18.00 | 2nd liq grab then to TP need to adapt to intra day | [chart](https://www.tradingview.com/x/yHV2K1LE/) |
| 2024-04-25 | GBPJPY | 15Min | Buy | 1.00 | adjusting lot missed my entry +10 pips | [chart](https://www.tradingview.com/x/tVgtthum/) |
| 2024-05-02 | XAUUSD | 15Min | Sell | 30.00 | — | [chart](https://www.tradingview.com/x/NZbkhGNh/) |
| 2024-05-08 | XAUUSD | 15Min | Sell | 0.00 | would of hit TP | [chart](https://www.tradingview.com/x/YrRwuZ9x/) |
| 2024-05-08 | XAUUSD | 5Min | Sell | 12.00 | — | [chart](https://www.tradingview.com/x/DjHudFPz/) |
| 2024-05-16 | GBPJPY | 15Min | Buy | 14.80 | RAN To full TP worth to hold Psy | [chart](https://www.tradingview.com/x/qUOecc5q/) |
| 2024-05-23 | XAUUSD | 15Min | Sell | 38.00 | — | [chart](https://www.tradingview.com/x/htSTp1Fr/) |
| 2024-05-24 | XAUUSD | 15Min | Buy | 0.00 | -2.5 pips | [chart](https://www.tradingview.com/x/fBG39m6f/) |
| 2024-06-03 | GBPJPY | 15Min | Sell | 0.00 | — | [chart](https://www.tradingview.com/x/dVqN7z4x/) |
| 2024-06-06 | GBPJPY | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/aSnTeygD/) |
| 2024-06-11 | GBPJPY | 15Min | Buy | 14.00 | runner hit BE was trying to hold | [chart](https://www.tradingview.com/x/d0y4M4RD/) |
| 2024-06-11 | XAUUSD | 1HR | Buy | 0.00 | HIT tp WAS IN 2 POSITIONS WAS A W | [chart](https://www.tradingview.com/x/OxrFIxcQ/) |
| 2024-06-12 | GBPJPY | 15Min | Buy | 12.00 | — | [chart](https://www.tradingview.com/x/7XpDjBQJ/) |
| 2024-06-13 | GBPJPY | 30Min | Buy | 17.00 | — | [chart](https://www.tradingview.com/x/yFXVcuGR/) |
| 2024-06-21 | XAUUSD | 5Min | Buy | 17.00 | — | [chart](https://www.tradingview.com/x/YAkjptqP/) |
| 2024-06-27 | XAUUSD | 15Min | Buy | 14.00 | — | [chart](https://www.tradingview.com/x/tVSrNO2s/) |
| 2024-07-01 | GBPJPY | 5Min | Buy | 0.00 | MISSED TP BY 1 PIP | [chart](https://www.tradingview.com/x/Hfyg7JVS/) |
| 2024-07-03 | GBPJPY | 15Min | Buy | 13.00 | — | [chart](https://www.tradingview.com/x/0YM2d7Oy/) |
| 2024-07-05 | GBPJPY | 15Min | Buy | -12.00 | BAD MANAGEMENT NOT GOOD SL HIT TP | [chart](https://www.tradingview.com/x/lz7RkCxF/) |
| 2024-07-08 | XAUUSD | 15Min | Sell | 20.00 | — | [chart](https://www.tradingview.com/x/ZLdol48L/) |
| 2024-07-18 | XAUUSD | 15Min | Buy | 27.00 | TAPPED 4HR ON BROKER NOT ON TRADING VIEW | [chart](https://www.tradingview.com/x/v4HrYJ1s/) |
| 2024-07-29 | XAUUSD | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/QPpT4GBO/) |
| 2024-08-09 | GBPUSD | 15Min | Buy | 7.00 | — | [chart](https://www.tradingview.com/x/gzlqtmww/) |
| 2024-08-20 | GBPJPY | 15Min | Buy | -27.00 | — | [chart](https://www.tradingview.com/x/3IXf5xiW/) |
| 2024-08-29 | XAUUSD | 15Min | Buy | 0.00 | TP HIT BUT MARKET WAS RANAGING FOR ALOT OF DAYS | [chart](https://www.tradingview.com/x/EPhhUIL6/) |
| 2024-09-02 | GBPJPY | 15Min | Buy | 0.00 | CORRECT TO BE | [chart](https://www.tradingview.com/x/xNrMQqbH/) |
| 2024-09-04 | XAUUSD | 15Min | Sell | 21.00 | — | [chart](https://www.tradingview.com/x/H0vTmhH0/) |
| 2024-09-09 | GBPJPY | 15Min | Sell | -21.00 | — | [chart](https://www.tradingview.com/x/WvKKJRvZ/) |
| 2024-09-20 | GBPUSD | 5Min | Buy | -12.00 | — | [chart](https://www.tradingview.com/x/5MqtJgiN/) |
| 2024-09-25 | XAUUSD | 15Min | Sell | 0.00 | corecct be not 1-1 | [chart](https://www.tradingview.com/x/2HIlh5vP/) |
| 2024-10-01 | GBPJPY | 15Min | Buy | 0.00 | CORRECT BREAK EVEN | [chart](https://www.tradingview.com/x/FHEAxa3t/) |
| 2024-10-02 | GBPUSD | 15Min | Sell | -10.00 | BROKE PLAN | [chart](https://www.tradingview.com/x/ZoqeWywF/) |
| 2024-10-03 | GBPJPY | 15Min | Sell | 13.00 | 1.2%  WITH NEW RISK | [chart](https://www.tradingview.com/x/vf4GR2QI/) |
| 2024-10-09 | GBPJPY | 15Min | Buy | 10.00 | — | [chart](https://www.tradingview.com/x/4M0p3UP4/) |
| 2024-10-10 | XAUUSD | 15Min | Sell | -7.00 | — | [chart](https://www.tradingview.com/x/Mdz6QOj6/) |
| 2024-10-15 | DAX | 5Min | Buy | 12.00 | — | [chart](https://www.tradingview.com/x/HEWs8c7P/) |
| 2024-10-16 | DAX | 15Min | Sell | 15.00 | — | [chart](https://www.tradingview.com/x/NywZvq2v/) |
| 2024-10-22 | GBPJPY | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/8makTgnA/) |
| 2024-11-01 | GBPJPY | 15Min | Buy | 26.00 | — | [chart](https://www.tradingview.com/x/qmQWWLIx/) |
| 2024-11-08 | DAX | 5Min | Sell | 0.00 | correct BE | [chart](https://www.tradingview.com/x/XEB2eLq8/) |
| 2024-11-15 | DAX | 5Min | Buy | 16.00 | — | [chart](https://www.tradingview.com/x/rkNXAEsL/) |
| 2024-11-18 | XAUUSD | 15Min | Buy | 0.00 | correct decsion a+ trade | [chart](https://www.tradingview.com/x/bJ8vnFTq/) |
| 2024-11-22 | DAX | 5Min | Buy | -11.00 | — | [chart](https://www.tradingview.com/x/Iy2ORrVI/) |
| 2024-12-02 | XAUUSD | 15Min | Buy | 0.00 | COULD CL0SE 1:0.4 BUT CHOSE TO HOLD CORRECT BE | [chart](https://www.tradingview.com/x/3JVGTX11/) |
| 2024-12-03 | XAUUSD | 15Min | Sell | 0.00 | — | [chart](https://www.tradingview.com/x/XyHdNUe1/) |
| 2024-12-03 | DAX | 5Min | Buy | 22.00 | — | [chart](https://www.tradingview.com/x/9V1j9OY5/) |
| 2024-12-05 | DAX | 5Min | Buy | 26.00 | — | [chart](https://www.tradingview.com/x/KHj0HdGl/) |
| 2024-12-06 | DAX | 5Min | Buy | 20.00 | — | [chart](https://www.tradingview.com/x/i93lpgiY/) |
| 2024-12-12 | GBPJPY | 15Min | Buy | -14.00 | — | [chart](https://www.tradingview.com/x/ZTO7HWg4/) |
| 2024-12-13 | DAX | 5Min | Buy | 0.00 | re entered https://www.tradingview.com/x/dtRWBldV/ 0.76% | [chart](https://www.tradingview.com/x/fB5IDjwB/) |
| 2024-12-19 | XAUUSD | 15Min | Buy | 15.00 | MAX TP HIT WANTED A WIN END OF YEAR | [chart](https://www.tradingview.com/x/zKPxvYhW/) |
| 2025-01-07 | GBPJPY | 15Min | Sell | 0.00 | WOULD HAVE HIT TP | [chart](https://www.tradingview.com/x/3XPWdOfM/) |
| 2025-01-13 | DAX | 5Min | Sell | 47.00 | — | [chart](https://www.tradingview.com/x/7tNl2Ytl/) |
| 2025-01-15 | DAX | 15Min | Buy | 0.00 | HIT TP SLIPPED ENTRY | [chart](https://www.tradingview.com/x/aM9Ex5LT/) |
| 2025-01-16 | DAX | 5Min | Buy | 64.00 | — | [chart](https://www.tradingview.com/x/6p3QbiLB/) |
| 2025-01-17 | DAX | 15Min | Buy | 42.00 | — | [chart](https://www.tradingview.com/x/ftEhW1FG/) |
| 2025-01-20 | DAX | 15Min | Buy | -19.00 | USD BANK HOLIDAY | [chart](https://www.tradingview.com/x/BdTkPhmA/) |
| 2025-01-23 | DAX | 5Min | Buy | -18.00 | — | [chart](https://www.tradingview.com/x/9xByEWt8/) |
| 2025-01-28 | XAUUSD | 15Min | Sell | 19.00 | — | [chart](https://www.tradingview.com/x/yM4HSABr/) |
| 2025-01-31 | DAX | 15Min | Buy | 20.00 | WAS FLOATING +1% LEFT RUNNER REST BE | [chart](https://www.tradingview.com/x/hifiicMv/) |
| 2025-02-03 | GBPJPY | 15Min | Buy | -19.00 | plan 100 % unlucky wick out was till correct SL | [chart](https://www.tradingview.com/x/LUqiEIVR/) |
| 2025-02-06 | DAX | 5Min | Buy | 30.00 | — | [chart](https://www.tradingview.com/x/UTzTpyu9/) |
| 2025-02-07 | DAX | 30Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/eOPQUste/) |
| 2025-02-10 | DAX | 5Min | Buy | 50.00 | — | [chart](https://www.tradingview.com/x/tT0xvaag/) |
| 2025-02-11 | DAX | 15Min | Buy | 21.00 | — | [chart](https://www.tradingview.com/x/RuT2uUoG/) |
| 2025-02-20 | XAUUSD | 15Min | Buy | 54.00 | — | [chart](https://www.tradingview.com/x/tlLYOsyA/) |
| 2025-02-24 | DAX | 5Min | Sell | 0.00 | Correct BE | [chart](https://www.tradingview.com/x/jp2MN7Th/) |
| 2025-02-25 | GBPJPY | 15Min | Sell | 32.00 | — | [chart](https://www.tradingview.com/x/PbQM40Aw/) |
| 2025-03-05 | DAX | 15Min | Buy | 0.00 | CORRECT BE LDN OPEN | [chart](https://www.tradingview.com/x/aGbKNo07/) |
| 2025-03-06 | DAX | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/WXVGNiVl/) |
| 2025-03-12 | GBPJPY | 15Min | Buy | 19.00 | — | [chart](https://www.tradingview.com/x/Sm2WthOe/) |
| 2025-03-14 | XAUUSD | 15Min | Buy | 32.00 | — | [chart](https://www.tradingview.com/x/6mixHz0q/) |
| 2025-03-18 | XAUUSD | 15Min | Buy | 43.00 | CLOSED 50% AT ENTRY | [chart](https://www.tradingview.com/x/xK92GxIq/) |
| 2025-03-19 | DAX | 5Min | Sell | 32.00 | 1:1 BUT SLIPPAGE | [chart](https://www.tradingview.com/x/fdGcVTug/) |
| 2025-03-21 | DAX | 5Min | Sell | 33.00 | — | [chart](https://www.tradingview.com/x/Jw5nh0Ul/) |
| 2025-03-25 | XAUUSD | 15Min | Sell | 0.00 | CORRECT BE | [chart](https://www.tradingview.com/x/wkm6I9gp/) |
| 2025-03-27 | DAX | 5Min | Sell | 20.00 | — | [chart](https://www.tradingview.com/x/EJAVCzkm/) |
| 2025-03-31 | DAX | 5Min | Sell | -9.00 | pushed 20 points ASP had BE ez option | [chart](https://www.tradingview.com/x/quPFrayQ/) |
| 2025-04-03 | DAX | 5Min | Buy | -35.00 | — | [chart](https://www.tradingview.com/x/mrKpVWjZ/) |
| 2025-04-08 | XAUUSD | 15Min | Buy | 63.00 | — | [chart](https://www.tradingview.com/x/8mcGTjjZ/) |
| 2025-04-15 | DAX | 5Min | Buy | 80.00 | — | [chart](https://www.tradingview.com/x/2C1ym0Jk/) |
| 2025-04-24 | GBPJPY | 15Min | Sell | -18.00 | had BE decided not to move | [chart](https://www.tradingview.com/x/k9fmNIqt/) |
| 2025-04-28 | DAX | 5Min | Buy | 23.00 | — | [chart](https://www.tradingview.com/x/DUB8cWur/) |
| 2025-05-02 | DAX | 5Min | Sell | -27.00 | COUNTER BIAS! | [chart](https://www.tradingview.com/x/0NsK467h/) |
| 2025-05-08 | GBPJPY | 15Min | Buy | -16.00 | BAD MARK UP COMPLEATLY MISSED A LEVEL | [chart](https://www.tradingview.com/x/g920rOmS/) |
| 2025-05-09 | XAUUSD | 30Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/MDrIp5QN/) |
| 2025-05-09 | DAX | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/mLT8dFV2/) |
| 2025-05-16 | DAX | 5Min | Buy | 39.00 | — | [chart](https://www.tradingview.com/x/ll4D2pSx/) |
| 2025-05-23 | DAX | 5Min | Buy | 21.00 | — | [chart](https://www.tradingview.com/x/T0q00oNc/) |
| 2025-05-27 | GBPJPY | 15Min | Buy | 14.00 | — | [chart](https://www.tradingview.com/x/8NEkdqv6/) |
| 2025-05-29 | DAX | 5Min | Sell | 41.00 | — | [chart](https://www.tradingview.com/x/Xk7KwDdE/) |
| 2025-06-06 | DAX | 5Min | Sell | -10.00 | — | [chart](https://www.tradingview.com/x/u1cPEeGS/) |
| 2025-06-09 | XAUUSD | 15Min | Buy | 27.00 | — | [chart](https://www.tradingview.com/x/L3K9Yu2B/) |
| 2025-06-13 | DAX | 5Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/tmelv8iu/) |
| 2025-06-18 | GBPJPY | 5Min | Sell | -10.00 | PROOF RISKY POSITONS ALL PLAYED OUT BUT THE ONE U TAKE FAILS | [chart](https://www.tradingview.com/x/evSNQTKH/) |
| 2025-06-19 | DAX | 5Min | Sell | 37.00 | — | [chart](https://www.tradingview.com/x/jbDxH0Em/) |
| 2025-06-24 | XAUUSD | 5Min | Sell | 0.00 | — | [chart](https://www.tradingview.com/x/HTrGZkXr/) |
| 2025-06-25 | DAX | 5Min | Sell | -19.00 | — | [chart](https://www.tradingview.com/x/L4LBTvbR/) |
| 2025-06-27 | DAX | 5Min | Buy | 18.00 | — | [chart](https://www.tradingview.com/x/7qkivqfM/) |
| 2025-06-30 | DAX | 5Min | Sell | 0.00 | — | [chart](https://www.tradingview.com/x/deBKmiHu/) |
| 2025-07-07 | DAX | 5Min | Sell | 14.00 | — | [chart](https://www.tradingview.com/x/t7yO1JS9/) |
| 2025-07-09 | XAUUSD | 5Min | Sell | 0.00 | CORRECT BE | [chart](https://www.tradingview.com/x/qojsl19f/) |
| 2025-07-22 | XAUUSD | 15Min | Sell | 0.00 | Holiday break | [chart](https://www.tradingview.com/x/oP2gmsn4/) |
| 2025-07-25 | DAX | 15Min | Sell | 17.00 | — | [chart](https://www.tradingview.com/x/Fc9L6pG5/) |
| 2025-07-29 | XAUUSD | 15Min | Buy | -35.00 | — | [chart](https://www.tradingview.com/x/rAwg6rlz/) |
| 2025-08-01 | DAX | 15Min | Buy | -26.00 | — | [chart](https://www.tradingview.com/x/oEAUTuYE/) |
| 2025-08-13 | DAX | 5Min | Buy | 5.00 | 0.75% to TP | [chart](https://www.tradingview.com/x/kK3Y4JoG/) |
| 2025-09-08 | XAUUSD | 5Min | Buy | 27.00 | — | [chart](https://www.tradingview.com/x/suIaSkRZ/) |
| 2025-09-09 | XAUUSD | 15Min | Buy | -25.00 | — | [chart](https://www.tradingview.com/x/GNOaFT6j/) |
| 2025-09-15 | XAUUSD | 15Min | Buy | 0.00 | WOULD OF HIT SL | [chart](https://www.tradingview.com/x/fwSfexmy/) |
| 2025-09-16 | DAX | 5Min | Buy | 21.00 | — | [chart](https://www.tradingview.com/x/pe4Hu5IH/) |
| 2025-09-22 | XAUUSD | 15Min | Buy | 38.00 | — | [chart](https://www.tradingview.com/x/psgb8Akd/) |
| 2025-09-26 | DAX | 5Min | Buy | -20.00 | — | [chart](https://www.tradingview.com/x/kYhYffjG/) |
| 2025-10-01 | XAUUSD | 15Min | Buy | 0.00 | hit TP | [chart](https://www.tradingview.com/x/2QdrgGRU/) |
| 2025-10-03 | XAUUSD | 15Min | Buy | 15.00 | — | [chart](https://www.tradingview.com/x/m5XAdEtq/) |
| 2025-10-08 | XAUUSD | 15Min | Buy | 0.00 | never broke high got slipped | [chart](https://www.tradingview.com/x/oKwGOsvx/) |
| 2025-10-09 | DAX | 5Min | Buy | -50.00 | — | [chart](https://www.tradingview.com/x/f4NbUaeD/) |
| 2025-10-13 | XAUUSD | 15Min | Buy | 43.00 | — | [chart](https://www.tradingview.com/x/jcLAkDnk/) |
| 2025-10-21 | DAX | 5Min | Buy | -20.00 | — | [chart](https://www.tradingview.com/x/WvVOpL9P/) |
| 2025-10-27 | XAUUSD | 15Min | Buy | 60.00 | HIT TP | [chart](https://www.tradingview.com/x/zIl29aGf/) |
| 2025-10-28 | XAUUSD | 15Min | Buy | -50.00 | — | [chart](https://www.tradingview.com/x/Gn7vL2p6/) |
| 2025-10-31 | XAUUSD | 15Min | Buy | 7.00 | — | [chart](https://www.tradingview.com/x/7ujDMiwH/) |
| 2025-11-11 | XAUUSD | 15Min | Buy | -50.00 | USD BANK HOLIDAY | [chart](https://www.tradingview.com/x/pus1i2qN/) |
| 2025-11-13 | XAUUSD | 15Min | Buy | 44.00 | — | [chart](https://www.tradingview.com/x/vsAaS0g6/) |
| 2025-12-02 | XAUUSD | 15Min | Buy | -29.00 | — | [chart](https://www.tradingview.com/x/xcSZOIMy/) |
| 2025-12-03 | XAUUSD | 15Min | Sell | 0.00 | — | [chart](https://www.tradingview.com/x/MMl5ADJA/) |
| 2025-12-04 | GBPJPY | 15Min | Buy | 35.00 | — | [chart](https://www.tradingview.com/x/hlnGsjdv/) |
| 2026-01-07 | DAX | 5Min | Buy | -13.00 | — | [chart](https://www.tradingview.com/x/8qrLtQOV/) |
| 2026-01-07 | DAX | 5Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/S0AY1O5P/) |
| 2026-01-20 | XAUUSD | 15Min | Buy | 40.00 | — | [chart](https://www.tradingview.com/x/CxHAShBY/) |
| 2026-01-21 | XAUUSD | 5Min | Buy | 56.00 | — | [chart](https://www.tradingview.com/x/pAEHZGWX/) |
| 2026-01-22 | DAX | 5Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/QCWb2eTm/) |
| 2026-01-23 | DAX | 5Min | Sell | 0.00 | — | [chart](https://www.tradingview.com/x/ZeN8comD/) |
| 2026-02-03 | XAUUSD | 15Min | Buy | 179.00 | — | [chart](https://www.tradingview.com/x/vLd909qG/) |
| 2026-02-04 | NAS | 15Min | Buy | -10.00 | — | [chart](https://www.tradingview.com/x/SzMAaDPf/) |
| 2026-02-05 | XAGUSD | 5Min | Buy | 5.00 | — | [chart](https://www.tradingview.com/x/FPjRd6XB/) |
| 2026-02-10 | NAS | 5Min | Buy | 13.00 | — | [chart](https://www.tradingview.com/x/SivBTDIK/) |
| 2026-02-11 | XAUUSD | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/j4q42huI/) |
| 2026-02-19 | NAS | 5Min | Buy | 18.00 | — | [chart](https://www.tradingview.com/x/TN5byymh/) |
| 2026-02-24 | XAUUSD | 15Min | Buy | 75.00 | — | [chart](https://www.tradingview.com/x/kTMBDg0f/) |
| 2026-02-26 | NAS | 5Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/VEILxPVP/) |
| 2026-03-09 | XAUUSD | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/yVwO1YDe/) |
| 2026-03-10 | XAGUSD | 15Min | Buy | -20.00 | — | [chart](https://prnt.sc/uvtzxGaBev88) |
| 2026-03-11 | NAS | 15Min | Buy | 1.00 | — | [chart](https://www.tradingview.com/x/fFrUkI6a/) |
| 2026-03-16 | NAS | 15Min | Buy | -8.00 | — | [chart](https://www.tradingview.com/x/3aUms738/) |
| 2026-03-26 | NAS | 15Min | Buy | -30.00 | — | [chart](https://www.tradingview.com/x/nLugMc88/) |
| 2026-04-09 | NAS | 5Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/1IITL9Y2/) |
| 2026-04-13 | NAS | 15Min | Buy | 16.00 | — | [chart](https://www.tradingview.com/x/i0PIBVFC/) |
| 2026-04-13 | XAUUSD | 15Min | Buy | -50.00 | — | [chart](https://www.tradingview.com/x/WFu9LSNG/) |
| 2026-04-15 | XAUUSD | 15Min | Sell | -20.00 | — | [chart](https://www.tradingview.com/x/UchlIRrQ/) |
| 2026-04-29 | XAUUSD | 5Min | Sell | 1.00 | hit tP | [chart](https://www.tradingview.com/x/PM6bSKVP/) |
| 2026-05-06 | DAX | 5Min | Buy | 30.00 | https://prnt.sc/pd5PtYNpGHNt TS exclusive group | [chart](https://www.tradingview.com/x/eR8MSGH5/) |
| 2026-05-07 | NAS | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/XlIFRF1J/) |
| 2026-05-07 | NAS | 5Min | Buy | 18.00 | — | [chart](https://www.tradingview.com/x/AwxhpNrM/) |
| 2026-05-14 | NAS | 5Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/KpIuNRbS/) |
| 2026-05-18 | NAS | 15Min | Sell | -42.00 | — | [chart](https://www.tradingview.com/x/kUjpXzog/) |
| 2026-05-20 | NAS | 5Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/6NjzfTxe/) |
| 2026-05-27 | DAX | 5Min | Buy | 28.00 | — | [chart](https://www.tradingview.com/x/LQy44b5j/) |
| 2026-06-02 | NAS | 5Min | Buy | 16.00 | supposed to be 1R | [chart](https://www.tradingview.com/x/iOp3VzQx/) |
| 2026-06-04 | NAS | 15Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/nqzymbNL/) |
| 2026-06-30 | DAX | 5Min | Buy | 24.00 | — | [chart](https://www.tradingview.com/x/UVjBH7N9/) |
| 2026-07-02 | NAS | 5Min | Buy | 32.00 | — | [chart](https://www.tradingview.com/x/pDw9GsP8/) |
| 2026-07-07 | NAS | 5Min | Buy | 0.00 | — | [chart](https://www.tradingview.com/x/NKhdeJ7F/) |
| 2026-07-23 | NAS | 5Min | Sell | 0.00 | — | [chart](https://www.tradingview.com/x/zucpjCTM/) |
| 2026-07-30 | NAS | 15Min | Sell | -50.00 | — | [chart](https://www.tradingview.com/x/tReD2Bjq/) |
| 2026-08-21 | XAUUSD | 15Min | Sell | -50.00 | — | [chart](https://www.tradingview.com/x/CnQzaKQi/) |
