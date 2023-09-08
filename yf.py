import yfinance as yf
import json

data = yf.Ticker("MSFT")

prices = data.history(start='2021-01-01', end='2021-04-01').Close
returns = prices.pct_change().dropna()

print(returns)