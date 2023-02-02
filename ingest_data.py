"""Pipeline"""

import os
from time import time

import pandas as pd
import pyarrow.parquet as pq
from decouple import config
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3)
def extract_data(parquet_url):
    parquet_path = 'output.parquet'
    parquet_path = 'data_files/yellow_tripdata_2021-01.parquet'

    # os.system(f'wget {parquet_url} -O {parquet_path}')

    parquet_file = pq.ParquetFile(parquet_path)

    return parquet_file


# @task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, parquet_file):
    """Loads data from a parquet file to a table in postgres."""


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    for batch in parquet_file.iter_batches():
        # batch size - 65536 rows
        t_start = time()

        df = batch.to_pandas()
        df = transform_data(df)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print(f'chunk inserted in {(t_end - t_start)} seconds')


@flow(name='Ingest Flow')
def main_flow():
    raw_data = extract_data(config('PARQUET_URL'))
    ingest_data(
        config('USER'),
        config('PASSWORD'),
        config('HOST'),
        config('PORT'),
        config('DB'),
        config('TABLE_NAME'),
        raw_data
    )

if __name__ == '__main__':
    main_flow()
