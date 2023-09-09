import pandas as pd
import boto3
import pyarrow

from prefect import flow, task

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from alpaca.data.requests import StockBarsRequest, StockTradesRequest
from alpaca.data.timeframe import TimeFrame

from pathlib import Path
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

load_dotenv()
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY")

@task(log_prints=True)
def fetch_tickers() -> set():
    nasdaq = pd.read_csv("https://s3-alpaca-stock-data.s3.us-west-1.amazonaws.com/tickers/nasdaq100.csv")
    nyse = pd.read_csv("https://s3-alpaca-stock-data.s3.us-west-1.amazonaws.com/tickers/nyse100.csv")

    nasdaq_tickers = list(nasdaq.iloc[:, 0])
    nyse_tickers = list(nyse.iloc[:, 0])
    return(set(nasdaq_tickers + nyse_tickers))

@task(log_prints=True)
def fetch_bars(tickers) -> pd.DataFrame:
    hist_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    def getStockHistoricalData(client: StockHistoricalDataClient, start_date, end_date = datetime.today()):
        bar_request = StockBarsRequest(
            symbol_or_symbols=tickers,
            timeframe=TimeFrame.Day,
            start=start_date,
            end=end_date
        )

        res = client.get_stock_bars(bar_request)
        return(res)

    bars = getStockHistoricalData(hist_client, datetime.today() - timedelta(days = 1)).data
    
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
    
    return df


@task(log_prints=True)
def parquetize_and_write(df: pd.DataFrame) -> Path:
    path = Path(f"data/stocks.parquet")
    df.to_parquet(path, engine = "pyarrow", compression = "gzip")
    return path


@task(log_prints=True)
def export_to_s3(path: Path) -> None:
    s3 = boto3.client("s3")
    bucket_name = "s3-alpaca-stock-data"
    s3.upload_file(path, bucket_name, "historical/test.parquet.gzip")
    return


@flow(name="Daily Data Ingestion", log_prints=True)
def daily_data_ingestion_flow():
    tickers = fetch_tickers()
    df = fetch_bars(tickers)
    export_to_s3(parquetize_and_write(df))


if __name__ == "__main__":
    daily_data_ingestion_flow.serve(name = "Daily Deployment", cron = "5 4 * * 1-5")