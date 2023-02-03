import boto3
import wget

from decouple import config
from prefect import flow, task


@task(log_prints=True, retries=3)
def fetch(dataset_url: str, path: str) -> None:
    """Save parquet file from web to a local folder"""
    wget.download(dataset_url, out=path)


@task(log_prints=True)
def write_to_yandex_cloud(path: str, bucket_name: str) -> None:
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=config('AWS_ACCESS_KEY'),
        aws_secret_access_key=config('AWS_SECRET_KEY')
    )

    try:
        s3.create_bucket(Bucket=bucket_name)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass
    s3.upload_file(path, bucket_name, path)


@flow
def main_flow():
    """The main ETL function"""
    color = 'green'
    year = 2020
    month = 1
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet'
    path = f'data/{dataset_file}.parquet'

    fetch(dataset_url, path)

    bucket = config('BUCKET')
    write_to_yandex_cloud(path, bucket)


if __name__ == '__main__':
    main_flow()