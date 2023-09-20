import pandas as pd
import awswrangler as wr
import boto3
import pyarrow

from prefect import flow, task
from prefect.blocks.system import Secret

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
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

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

@task(log_prints=True)
def fetch_tickers() -> set():
    nasdaq = pd.read_csv("https://s3-alpaca-stock-data.s3.us-west-1.amazonaws.com/tickers/nasdaq100.csv")
    nyse = pd.read_csv("https://s3-alpaca-stock-data.s3.us-west-1.amazonaws.com/tickers/nyse100.csv")

    nasdaq_tickers = list(nasdaq.iloc[:, 0])
    nyse_tickers = list(nyse.iloc[:, 0])
    return(set(nasdaq_tickers + nyse_tickers))

@task(log_prints=True)
def etl(tickers, years) -> None:
    hist_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    def getStockHistoricalData(client: StockHistoricalDataClient, start_date, end_date):
        bar_request = StockBarsRequest(
            symbol_or_symbols=tickers,
            timeframe=TimeFrame.Day,
            start=start_date,
            end=end_date
        )

        res = client.get_stock_bars(bar_request)
        return(res)

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

        df['year'] = df['timestamp'].dt.year
        df['month'] = df['timestamp'].dt.month
        df['day'] = df['timestamp'].dt.day
        
        glue_db_name = "alpaca_stocks_database"
        glue_table_name = "stocks_table_historical"

        aws_session = boto3.Session(
            aws_access_key_id = AWS_ACCESS_KEY,
            aws_secret_access_key = AWS_SECRET_KEY,
            region_name = "us-west-1"
        )

        wr.s3.to_parquet(
            df = df,
            path = "s3://s3-alpaca-stock-data/historical/",
            dataset = True,
            partition_cols = ["year", "month", "day"],
            database = glue_db_name,
            table = glue_table_name,
            boto3_session = aws_session,
            mode = "overwrite_partitions"  
        )

        end_time = time.time()
        print(f"Runtime for year {year}: {end_time - start_time: .2f}")

    return

@flow(name="Yearly Data Ingestion Job", log_prints=True)
def yearly_data_ingestion_flow():
    years = [2022, 2021, 2020, 2019, 2018, 2017, 2016]
    tickers = fetch_tickers()
    etl(tickers, years)

if __name__ == "__main__":
    yearly_data_ingestion_flow.serve(name = "Yearly Ingestion Deployment")