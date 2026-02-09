# Alpaca Requirements Recap

Ticker: TQQQ

Timeframe: 15-min candles

Position limit: 1 open position at a time

Position size: 7.5% of account equity

Entry(meet all reqs):

1. Price 4% below 24-hour high

2. close < SMA(20) - 1.5 * rolling_std(20)

3. RSI(14) ≤ 40 AND RSI_now ≥ RSI_prev

Exit:

Take profit: +3% OR close ≥ SMA(20) + 0.5*rolling_std(20)

Stop loss: -1.25%

Time stop: 20 candles (~2 hours)

Paper trading: Alpaca free tier REST API

Trade journal: CSV logging
