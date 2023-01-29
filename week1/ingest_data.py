"""Pipeline"""

import os
from time import time

import pandas as pd
import pyarrow.parquet as pq
from decouple import config
from sqlalchemy import create_engine


def main(user, password, host, port, db, table_name, parquet_url):
    """Loads data from a parquet file to a table in postgres."""

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    parquet_path = 'output.parquet'
    # parquet_path = 'data_files/green_tripdata_2019-01.parquet'

    os.system(f'wget {parquet_url} -O {parquet_path}')

    parquet_file = pq.ParquetFile(parquet_path)

    for batch in parquet_file.iter_batches():
        # batch size - 65536 rows
        t_start = time()

        df = batch.to_pandas()

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print(f'chunk inserted in {(t_end - t_start)} seconds')


if __name__ == '__main__':
    main(
        config('USER'),
        config('PASSWORD'),
        config('HOST'),
        config('PORT'),
        config('DB'),
        config('TABLE_NAME'),
        config('PARQUET_URL')
    )
