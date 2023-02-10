import logging
import os
import requests

from decouple import config
from prefect import flow, task

from web_to_cloud import fetch, write_to_yandex_cloud

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s - %(filename)s:%(lineno)s:%(funcName)s()',
)
logger = logging.getLogger(__name__)
consoleHandler = logging.StreamHandler()
logger.addHandler(consoleHandler)


@task(log_prints=True)
def get_filenames(url, name):
    req = requests.get(url)
    if req.status_code == requests.codes.ok:
        req = req.json()
        assets = [doc for doc in req if doc['name'] == name][0]['assets']
        url_list = [asset['browser_download_url'] for asset in assets]
        return url_list
    else:
        logger.error('Content was not found.')
        exit()


@flow(name='load_gz')
def main_flow(url, name):
    bucket = config('BUCKET')
    urls = get_filenames(url, name)
    for url in urls:
        logger.debug(url)
        path = f'data/week3/{os.path.basename(url)}'
        fetch(url, path)
        write_to_yandex_cloud(path, bucket)


if __name__ == '__main__':
    link = 'https://api.github.com/repos/DataTalksClub/nyc-tlc-data/releases'
    dataset = 'FHV NY Taxi data 2019-2021'
    main_flow(link, dataset)
