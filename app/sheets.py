import os
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = os.getenv("SHEETS_SERVICE_ACCOUNT_FILE")
SPREADSHEET_ID = '1F6uiXWcidNNUnzkvw6-n7ASR8jtxMs2VxfhYx_OX0PA' 

def get_service():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build('sheets', 'v4', credentials=creds).spreadsheets()

def read_sheet(tab):
    service = get_service()
    resp = service.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{tab}!A1:Z'
    ).execute()

    values = resp.get('values', [])
    if not values:
        return pd.DataFrame()

    headers = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=headers)

def write_sheet(tab, df: pd.DataFrame):
    service = get_service()
    values = [df.columns.tolist()] + df.astype(str).values.tolist()
    service.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{tab}!A1',
        valueInputOption='RAW',
        body={'values': values}
    ).execute()

def read_all():
    return read_sheet('categories'), read_sheet('words')

def write_all(cats_df, words_df):
    write_sheet('categories', cats_df)
    write_sheet('words', words_df)
