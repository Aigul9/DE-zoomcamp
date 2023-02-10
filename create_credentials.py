import httplib2

from decouple import config
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow

CLIENT_SECRET = config('SECRET_GDRIVE')
SCOPE = 'https://www.googleapis.com/auth/spreadsheets.readonly'
STORAGE = Storage('credentials.storage')


def authorize_credentials():
    creds = STORAGE.get()
    if creds is None or credentials.invalid:
        flow = flow_from_clientsecrets(CLIENT_SECRET, scope=SCOPE)
        http = httplib2.Http()
        creds = run_flow(flow, STORAGE, http=http)
    return creds


if __name__ == '__main__':
    credentials = authorize_credentials()
