from googleapiclient.discovery import build
from google.oauth2 import service_account

# Replace with the path to your service account key file
SERVICE_ACCOUNT_FILE = './google-service-account-key.json'

# Replace with the ID of your Google Sheets document
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Set up authentication using a service account
CREDENTIALS = service_account.Credentials.from_service_account_file(
    filename=SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)

# Build the Sheets API client
SHEETS_API = build('sheets', 'v4', credentials=CREDENTIALS)

def pull_sheet_data(document_id, data_to_pull):
    sheet_data = SHEETS_API.spreadsheets().values().get(
        spreadsheetId=document_id,
        range=data_to_pull
    ).execute().get('values', [])

    if len(sheet_data) == 0:
        raise Exception("No data found")

    return sheet_data
