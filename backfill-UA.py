from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
import os

# Configuration variables for Google Analytics and BigQuery
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = ''  # {
  "type": "service_account",
  "project_id": "ontario-mould-424709",
  "private_key_id": "8d2803dbc4e6f80ce9d77ec24c042db5244cbcc9",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDW4TiRmKkmJEi+\nobYNxmgMkxlzf7+hUpEricd6fflU3ZatNIBS1cN9f97xMUZazGtprMpVHdiiLOVL\nJtTxLem38oIjQneZoS91NmewCaFoWyqRUFgrRnwpoy7nowrsnfJ88dmws42L7sR8\nXIyaIRgk1orYewklYj9Rr0BVWkw4hXXPVbCntYeC7dFwn6KlEdOSrNUhlhFPpFLC\nVtpONA3Ho8VfbvjO57qq9PUU2NxxiPYQkQY5KmVfQr5uTjz37n71ny7JG9TwzSBE\nh5dlaS/pjgttQjTgakBeyv+WZMjfD4cT8ZIPN9CVozgV10/X5ES1d7UCfpvcUCWD\nXsplA3JVAgMBAAECggEAGdC2DJvkGm56hNiFuDLxdi1H/f3HhniWcz4axocS6NS0\nUPvBPPJsiYvOu3hZhPgRIYF+T2qGB7nQFE7Gf3EuJojOhLDsEHtyaakPHPGTQCfP\nveSCqzRNH2146aBHZDT3mHYv2pA/vaOCXJR72dqG/Yn+5VImv6SFiek9l+OAy7tY\nhUXRWwV9SUq9gXw5VMMD/BsTqsik2G6SCKmPDc/LFNUCn0YJtgSrxBpGW6cCWArf\nYDajwMF3/4BrnBqT9nK2yweBRN5vn9cAaWfeK0A7VH3/IoOg9ahon/3gOgj+NIWJ\nP5YLxAE0fDRYIWgQigW2U5RxckcwyXc9sF2lEOowUwKBgQD2+GHcyCuWs4JxL5OV\nr3KvTmS+T1mLV85j3jgO/sxBntezTk1ooh0ODhcPI2zRUf82hNrp6W6MKyzDJiaX\nNQyR9wvRwU0LCvr67AmhnxIDMTUaTFjbya4C6Ijpa1OOchzWukGl+RQL2AMJ1WHh\n6ZV5hinx18LEmW+V5NL4C95ugwKBgQDevHmWKqbJdYAxDXCOyfGjZJ1p4hOyl152\nC3WZkVldXQcy8ht/8tbPHhvvu2/IXrjleH65TzCp2PviptBLz2Ditkl7SfvOAHZN\nMdss9rE4TdpgZD1diK9z7SViwhaZ++/y60ahSTyTvrAeMqqXf1QyKe4fB0XL09MD\ni4O6VahERwKBgQDAWN7PN09Lhe+X+7f0Irstcd7goahZ8D8cZNxAQY4PpYjVa6y6\nS1hZs/udnLeJp1UfvwVInLeuj4nDS5lOttBddo1MBkLc0OZ8Ow2dROigd9il7MRg\nGYlfVoAbW62uCY4QZuvuOjm0p75mEDy12FjEVwugAaz9tYrEsXzmF0hbxQKBgQCr\nymxwwNbBHSpRKw98Yg9IOYsbpm4Q2aTWoQID/tIRK3Yo+gjxx2eceZmMbmHKBhzP\nh/8diBF9fsjPyF9xiTItyfCk8awP99VGtsRYSrDnP0zF+apG4OyKGgcyw4XRIDqy\nfnqMXUMmpPWLZQKkNGXJBwQM1HwluGvRGSLxba2JcwKBgQC56RgdNabWAcvuITVH\nRAbX+z9eMTuEBIlm2b9AHdDlR0Hwsu/eI1u5rKBbOC5m6Ahxi459l105sLNmmgxo\nBN17imCsiG4ijeBU3mXv5VHv2Rawhpzw/QwwW7HohQbqtSs6bisTI+ngnyrCIZFQ\nE+RuJq6nQE3S3Tl1hnK31/dHIg==\n-----END PRIVATE KEY-----\n",
  "client_email": "ontario-mould@ontario-mould-424709.iam.gserviceaccount.com",
  "client_id": "106274552641727866882",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/ontario-mould%40ontario-mould-424709.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

VIEW_ID = ''  # 94429575
BIGQUERY_PROJECT = ''  # ontario-mould-424709
BIGQUERY_DATASET = ''  # ontario-mould-424709.ontario
BIGQUERY_TABLE = ''  # on_123

# Setting up the environment variable for Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE_LOCATION

def initialize_analyticsreporting():
    """Initializes the Google Analytics Reporting API client."""
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics

def get_report(analytics, page_token=None):
    """Fetches the report data from Google Analytics."""
    # Here, specify the analytics report request details
    body = {
        'reportRequests': [
            {
                'viewId': VIEW_ID,
                'dateRanges': [{'startDate': '2006-01-01', 'endDate': 'today'}],
                # Metrics and dimensions are specified here
                'metrics': [
                    {'expression': 'ga:sessions'},
                    {'expression': 'ga:pageviews'},
                    {'expression': 'ga:users'},
                    {'expression': 'ga:newUsers'},
                    {'expression': 'ga:bounceRate'},
                    {'expression': 'ga:sessionDuration'},
                    {'expression': 'ga:avgSessionDuration'},
                    {'expression': 'ga:pageviewsPerSession'},
                    # Add or remove metrics as per your requirements
                ],
                'dimensions': [
                    {'name': 'ga:country'},
                    {'name': 'ga:pageTitle'},
                    {'name': 'ga:browser'},
                    {'name': 'ga:channelGrouping'},
                    {'name': 'ga:source'},
                    {'name': 'ga:pagePath'},
                    {'name': 'ga:deviceCategory'},
                    {'name': 'ga:date'}, # get the details by year-month-day
                    # Add or remove dimensions as per your requirements
                ],
                'pageSize': 20000  # Adjust the pageSize as needed
            }
        ]
    }
    if page_token:
        body['reportRequests'][0]['pageToken'] = page_token

    return analytics.reports().batchGet(body=body).execute()

def response_to_dataframe(response):
    """Converts the API response into a pandas DataFrame."""
    list_rows = []
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            row_data = {}
            for header, dimension in zip(dimensionHeaders, dimensions):
                row_data[header] = dimension

            for values in dateRangeValues:
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    row_data[metricHeader.get('name')] = value

            list_rows.append(row_data)

    return pd.DataFrame(list_rows)

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """Uploads the DataFrame to Google BigQuery."""
    # Rename columns from 'ga:' to 'gs_'
    df.columns = [col.replace('ga:', 'gs_') for col in df.columns]

    bigquery_client = bigquery.Client(project=project_id)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    schema = []

    # Define the schema of the table based on DataFrame columns
    for col in df.columns:
        # Choose BigQuery data type based on DataFrame column data type
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            bq_type = 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            bq_type = 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            bq_type = 'BOOLEAN'
        else:
            bq_type = 'STRING'  # Default type

        schema.append(bigquery.SchemaField(col, bq_type))

    # Create a new table if it doesn't exist
    try:
        bigquery_client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        bigquery_client.create_table(table)
        print(f"Created table {table_id}")

    # Upload data to BigQuery
    load_job = bigquery_client.load_table_from_dataframe(df, table_ref)
    load_job.result()
    print(f"Data uploaded")

def main():
    """Main function to execute the script."""
    try:
        page_token = None
        while True:
            # Fetching the report data from Google Analytics
            analytics = initialize_analyticsreporting()
            response = get_report(analytics, page_token)
            df = response_to_dataframe(response)
            upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)
            page_token = response.get('reports', [])[0].get('nextPageToken')
            if not page_token:
                break
            print(f"Fetching next page of results...{page_token}")
        
    except Exception as e:
        # Handling exceptions and printing error messages
        print(f"Error occurred: {e}")

if __name__ == '__main__':
    main()  # Entry point of the script
