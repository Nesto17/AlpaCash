import pandas as pd
import boto3
import pyarrow

from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import datetime
import time

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from alpaca.data.requests import StockBarsRequest, StockTradesRequest
from alpaca.data.timeframe import TimeFrame

load_dotenv()
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")

BAR_SCHEMA = {
    "symbol": str,
    "high": "float64",
    "low": "float64",
    "open": "float64",
    "timestamp": "datetime64[ns]",
    "trade_count": "float64",
    "volume": "float64",
    "vwap": "float64"
}

def yearly_ingestion(years):
    hist_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    nasdaq = pd.read_csv("https://s3-alpaca-stock-data.s3.us-west-1.amazonaws.com/tickers/nasdaq100.csv")
    nyse = pd.read_csv("https://s3-alpaca-stock-data.s3.us-west-1.amazonaws.com/tickers/nyse100.csv")

    nasdaq_tickers = list(nasdaq.iloc[:, 0])
    nyse_tickers = list(nyse.iloc[:, 0])
    tickers = set(nasdaq_tickers + nyse_tickers)

    def getStockHistoricalData(client: StockHistoricalDataClient, start_date, end_date):
        bar_request = StockBarsRequest(
            symbol_or_symbols=tickers,
            timeframe=TimeFrame.Day,
            start=start_date,
            end=end_date
        )

        res = client.get_stock_bars(bar_request)
        return(res)
    
    s3 = boto3.client("s3")
    bucket_name = "s3-alpaca-stock-data"

    for year in years:
        start_time = time.time()

        bars = getStockHistoricalData(hist_client, datetime(year, 1, 1), datetime(year + 1, 1, 1)).data
        df = pd.DataFrame(columns=BAR_SCHEMA.keys()).astype(BAR_SCHEMA)

        for i, ticker in enumerate(bars):
            for bar in bars[ticker]:
                entry = {
                    "symbol": bar.symbol,
                    "high": bar.high,
                    "low": bar.low,
                    "open": bar.open,
                    "timestamp": bar.timestamp,
                    "trade_count": bar.trade_count,
                    "volume": bar.volume,
                    "vwap": bar.vwap
                }
                df.loc[len(df)] = entry
        
        path = Path(f"data/{year}-stocks.parquet")
        df.to_parquet(path, engine = "pyarrow", compression = "gzip")
        s3.upload_file(path, bucket_name, f"historical/{year}-stocks.parquet.gzip")

        end_time = time.time()
        print(f"Runtime for year {year}: {end_time - start_time: .2f}")

if __name__ == "__main__":
    years = [2022, 2021, 2020]
    yearly_ingestion(years)