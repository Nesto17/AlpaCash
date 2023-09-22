import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from prophet import Prophet
from prophet.plot import plot_plotly, plot_components_plotly
from prophet.serialize import model_to_json, model_from_json
import plotly.graph_objs as go

import boto3
import awswrangler as wr

import json
import os
from dotenv import load_dotenv
from datetime import datetime
from time import time

ALPACA_API_KEY = Secret.load("alpaca-api-key")
ALPACA_SECRET_KEY = Secret.load("alpaca-secret-key")
AWS_ACCESS_KEY = Secret.load("aws-access-key")
AWS_SECRET_KEY = Secret.load("aws-secret-key")

s3_bucket = 's3-alpaca-stock-data'
s3_historical_prefix = 'historical'
s3_model_prefix = 'prophet-models'

glue_db = 'alpaca_stocks_database'
glue_monthly_table = f'stocks_table_{datetime.now().year}_{datetime.now().month}'
glue_historical_table = 'stocks_table_historical'

@task(log_prints=True)
def fetch_tickers() -> set():
    nasdaq = pd.read_csv('https://s3-alpaca-stock-data.s3.us-west-1.amazonaws.com/tickers/nasdaq_tickers.csv')
    nyse = pd.read_csv('https://s3-alpaca-stock-data.s3.us-west-1.amazonaws.com/tickers/nyse_tickers.csv')

    nasdaq_tickers = list(nasdaq.iloc[:, 0])
    nyse_tickers = list(nyse.iloc[:, 0])
    return(set(nasdaq_tickers + nyse_tickers))


@task(log_prints=True)
def get_dataset(session) -> pd.DataFrame:
    athena_query = f'''
        SELECT *
        FROM {glue_historical_table}
        ORDER BY timestamp ASC
    '''

    return wr.athena.read_sql_query(athena_query, database=glue_db, ctas_approach=True, boto3_session=session)

@task(log_prints=True)
def model_training(df, session) -> None:
    s3 = session.client('s3')
    s3_bucket = 's3-alpaca-stock-data'
    s3_model_prefix = 'prophet-models'

    def warm_start_params(m):
        res = {}
        for pname in ['k', 'm', 'sigma_obs']:
            if m.mcmc_samples == 0:
                res[pname] = m.params[pname][0][0]
            else:
                res[pname] = np.mean(m.params[pname])
        for pname in ['delta', 'beta']:
            if m.mcmc_samples == 0:
                res[pname] = m.params[pname][0]
            else:
                res[pname] = np.mean(m.params[pname], axis=0)
        return res

    s3_model = s3.list_objects(Bucket=s3_bucket, Prefix=f'{s3_model_prefix}/{ticker}_model.json')

    if 'Contents' in s3_model:
        res = s3.get_object(Bucket=s3_bucket, Key=f'{s3_model_prefix}/{ticker}_model.json')
        m1 = model_from_json(res['Body'].read().decode('utf-8'))
        m2 = Prophet().fit(df, init=warm_start_params(m1))
        s3.put_object(Body=model_to_json(m2), Bucket=s3_bucket, Key=f'{s3_model_prefix}/{ticker}_model.json')
    else:
        m = Prophet().fit(df)
        s3.put_object(Body=model_to_json(m), Bucket=s3_bucket, Key=f'{s3_model_prefix}/{ticker}_model.json')


@task(log_prints=True)
def ml_operations(dataset, tickers, session) -> None:
    for ticker in tickers:
        df = dataset[dataset['symbol'] == ticker]
        df = df[['timestamp', 'open']].rename(columns={'timestamp': 'ds', 'open': 'y'})
        df['ds'] = df['ds'].dt.strftime('%Y-%m-%d')

        model_training(df, session)
        


@flow(name="Machine Learning Ops", log_prints=True)
def ml_ops_flow():
    session = boto3.Session(
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY,
        region_name = 'us-west-1'
    )

    tickers = fetch_tickers()
    dataset = get_dataset(session)
    ml_operations(dataset, tickers, session)


if __name__ == "__main__":
    ml_ops_flow.serve(name = "Machine Learning Operations Deployment", cron = "5 4 * * 1-5")