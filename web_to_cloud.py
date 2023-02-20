import logging
import os

import boto3
import wget

from decouple import config
from prefect import flow, task

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s - %(filename)s:%(lineno)s:%(funcName)s()',
)
logger = logging.getLogger(__name__)
consoleHandler = logging.StreamHandler()
logger.addHandler(consoleHandler)


@task(log_prints=True, retries=3)
def fetch(dataset_url: str, path: str) -> None:
    if not os.path.isfile(path):
        wget.download(dataset_url, out=path)
    else:
        logger.debug(f'Skipped: {dataset_url}')


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
def main_flow(
        months: list, year: int = 2021, color: str = 'yellow'
):
    bucket = config('BUCKET')

    for month in months:
        dataset_file = f'{color}_tripdata_{year}-{month:02}'
        dataset_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet'
        path = f'data/{dataset_file}.parquet'

        fetch(dataset_url, path)
        write_to_yandex_cloud(path, bucket)


if __name__ == '__main__':
    # main_flow([2, 3], 2019)
    main_flow([4], 2019, 'green')
