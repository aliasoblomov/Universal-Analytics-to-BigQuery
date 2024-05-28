from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
import os

# Configuration variables for Google Analytics and BigQuery
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = ''  # Path to your Google Cloud service account key file
VIEW_ID = ''  # Your Google Analytics View ID
BIGQUERY_PROJECT = ''  # Your Google Cloud Project ID
BIGQUERY_DATASET = ''  # BigQuery Dataset name where the data will be stored
BIGQUERY_TABLE = ''  # BigQuery Table name where the data will be stored, if it does not exist, it will be created

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
