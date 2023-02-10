import errno
import logging
import os

from decouple import config
from prefect import flow, task
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from web_to_cloud import fetch
from web_to_cloud_gz import get_filenames

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s - %(filename)s:%(lineno)s:%(funcName)s()',
)
logger = logging.getLogger(__name__)
consoleHandler = logging.StreamHandler()
logger.addHandler(consoleHandler)

gauth = GoogleAuth()
gauth.LoadCredentialsFile(config('CREDENTIALS_GDRIVE'))
drive = GoogleDrive(gauth)


@task(log_prints=True)
def mkdir(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


@flow(name='load_to_gdrive')
def main_flow(url, name, folder):
    urls = get_filenames(url, name)
    base_path = 'data/week3/'
    mkdir(base_path)
    for url in urls:
        logger.debug(url)
        path = f'{base_path}{os.path.basename(url)}'
        fetch(url, path)
        gfile = drive.CreateFile({'parents': [{'id': folder}]})
        gfile.SetContentFile(path)
        gfile.Upload()


if __name__ == '__main__':
    link = 'https://api.github.com/repos/DataTalksClub/nyc-tlc-data/releases'
    dataset_name = 'FHV NY Taxi data 2019-2021'
    folder_id = '1KaI7zMmQoWHuRE5UcQAXj0Sn4EW4_gS-'
    main_flow(link, dataset_name, folder_id)
