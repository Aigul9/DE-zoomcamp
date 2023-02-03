import boto3
import io
import pandas as pd

from decouple import config
from prefect import flow, task
from google.oauth2 import service_account


@task(log_prints=True, retries=3)
def extract(bucket_name: str, path: str) -> pd.DataFrame:
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=config('AWS_ACCESS_KEY'),
        aws_secret_access_key=config('AWS_SECRET_KEY')
    )

    get_object_response = s3.get_object(Bucket=bucket_name, Key=path)
    buf = io.BytesIO(get_object_response['Body'].read())
    df = pd.read_parquet(buf, engine='fastparquet')
    return df


@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task
def ingest_data(df: pd.DataFrame, table_name) -> None:
    credentials = service_account.Credentials.from_service_account_file(config('CREDENTIALS'))
    df.to_gbq(
        destination_table=table_name,
        project_id=config('PROJECT_ID'),
        credentials=credentials,
        chunksize=100000,
        if_exists='append'
    )


@flow
def main_flow():
    """The main ETL function"""
    color = 'green'
    year = 2020
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    path = f'data/{dataset_file}.parquet'
    bucket = config('BUCKET')

    df = extract(bucket, path)
    df = transform(df)
    table_name = f'trips_data_all.{dataset_file}'
    ingest_data(df, table_name)


if __name__ == '__main__':
    main_flow()
