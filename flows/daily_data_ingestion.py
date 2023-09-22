import pandas as pd
import awswrangler as wr
import boto3
import pyarrow

from prefect import flow, task
from prefect.blocks.system import Secret

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from alpaca.data.requests import StockBarsRequest, StockTradesRequest
from alpaca.data.timeframe import TimeFrame

from pathlib import Path
from datetime import datetime, timedelta

ALPACA_API_KEY = Secret.load("alpaca-api-key")
ALPACA_SECRET_KEY = Secret.load("alpaca-secret-key")
AWS_ACCESS_KEY = Secret.load("aws-access-key")
AWS_SECRET_KEY = Secret.load("aws-secret-key")


@task(log_prints=True)
def fetch_tickers() -> set():
    nasdaq = pd.read_csv("https://s3-alpaca-stock-data.s3.us-west-1.amazonaws.com/tickers/nasdaq100.csv")
    nyse = pd.read_csv("https://s3-alpaca-stock-data.s3.us-west-1.amazonaws.com/tickers/nyse100.csv")

    nasdaq_tickers = list(nasdaq.iloc[:, 0])
    nyse_tickers = list(nyse.iloc[:, 0])
    return(set(nasdaq_tickers + nyse_tickers))


@task(log_prints=True)
def extract(tickers) -> pd.DataFrame:
    hist_client = StockHistoricalDataClient(ALPACA_API_KEY.get(), ALPACA_SECRET_KEY.get())

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
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day

    return df


@task(log_prints=True)
def parquetize_and_write(df: pd.DataFrame) -> Path:
    path = Path(f"../data/stocks.parquet")
    df.to_parquet(path, engine = "pyarrow", compression = "gzip")
    return path


@task(log_prints=True)
def load(df: pd.DataFrame) -> None:
    glue_db_name = "alpaca_stocks_database"
    glue_table_name = f"stocks_table_{datetime.now().year}_{datetime.now().month}"
    
    aws_session = boto3.Session(
        aws_access_key_id = AWS_ACCESS_KEY.get(),
        aws_secret_access_key = AWS_SECRET_KEY.get(),
        region_name = "us-west-1"
    )

    wr.s3.to_parquet(
        df = df,
        path = "s3://s3-alpaca-stock-data/daily/",
        dataset = True,
        partition_cols = ["year", "month", "day"],
        database = glue_db_name,
        table = glue_table_name,
        boto3_session = aws_session,
        mode = "overwrite_partitions"  
    )
    return


@flow(name="Daily Data Ingestion", log_prints=True)
def daily_data_ingestion_flow():
    tickers = fetch_tickers()
    df = extract(tickers)
    load(transform(df))


if __name__ == "__main__":
    daily_data_ingestion_flow.serve(name = "Daily Ingestion Deployment", cron = "5 4 * * 1-5")