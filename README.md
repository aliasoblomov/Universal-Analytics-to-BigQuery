# Universal-Analytics-to-BigQuery
This repository features a Python script designed extracting data from Universal Analytics, preparing it for compatibility, and subsequently loading it into Google BigQuery. This is particularly beneficial for businesses aiming to transfer their historical UA (GA3) data to BigQuery, especially those without access to Google Analytics 360.For a detailed walkthrough on how to use this script, including setup instructions, customization tips, and best practices, check out my Medium article:[Backfill Universal Analytics to BigQuery: Zero Cost, Full Control](https://medium.com/@aliiz/export-from-universal-analytics-to-bigquery-zero-cost-full-control-6470092713b1) . This article provides in-depth guidance and practical steps to make your data migration journey as smooth as possible. 


## Features
- **Initial Setup**: Configures API scopes, authentication key file location, UA view ID, and BigQuery project details.
- **Google Analytics Reporting API Initialization**: Sets up the connection using service account credentials.
- **Data Retrieval from UA**: Fetches data based on specified metrics, dimensions, and date ranges.
- **Data Conversion to DataFrame**: Converts API response into a Pandas DataFrame.
- **Uploading to BigQuery**: Handles DataFrame column renaming, BigQuery client initialization, table creation, and data uploading.
- **Availability for Both Billing and Sandbox Accounts**: The script supports both billed and sandbox accounts in BigQuery, allowing for versatile testing and deployment environments.


## Prerequisites
1. **Enable Google Analytics Reporting API**: Visit https://console.cloud.google.com/apis/api/analyticsreporting.googleapis.com/ and enable the API.
2. **Service Account and JSON Key File**:
   - Create or use an existing service account in BigQuery with owner access.
   - In the Google Cloud Console, navigate to “IAM & Admin” > “Service Accounts.”
   - Create a new service account or select an existing one with ‘Owner’ level access.
   - Generate and download a JSON key file for the service account.
   - Securely store the JSON key file and note its path for the `KEY_FILE_LOCATION` in the script.
3. **Add Service Account to UA Property Access Management**: Include the service account email in the UA property access management.
4. **Grant the Service Account Access to BigQuery**: Add the service account email to the BigQuery project with the appropriate permissions. AND, grant the service account Bigquery.jobs.create permission.

## Setup and Configuration
Fill in the following data in the script:

1. **KEY_FILE_LOCATION**: Path to your downloaded JSON key file for the service account.
2. **VIEW_ID**: Your Google Analytics View ID, found under Admin → View Settings in Google Analytics.
3. **BIGQUERY_PROJECT**: Your Google Cloud project ID.
4. **BIGQUERY_DATASET**: Dataset in BigQuery to store your data.
5. **BIGQUERY_TABLE**: Table in BigQuery where you want to store your data.

## Customizing Metrics and Dimensions
To tailor the script to your specific analytical needs, you can customize the metrics and dimensions in the `get_report` function. 

- Use the [Universal Analytics to GA4 Reporting API - Dimensions and Metrics](https://developers.google.com/analytics/devguides/migration/api/reporting-ua-to-ga4-dims-mets) documentation to find available dimensions and metrics.
- More dimensions can provide detailed reports but may reduce the granularity in terms of the number of rows. Fewer dimensions can lead to more granular reports with more rows.
- Based on your requirements, modify the `metrics` and `dimensions` lists in the `get_report` function.

## Running the Script
Execute the script directly as a standalone program. The main function orchestrates the data transfer process, with exception handling for potential errors.

## Why Choose This Approach
- **Limited Historical Data in UA**: UA's data retention limits are overcome by BigQuery's indefinite storage.
- **Custom Data Analysis Needs**: BigQuery allows for more sophisticated, customized data analysis.
- **Data Ownership and Portability**: Offers better control over data governance and portability.

## Contributing
Contributions to this project are welcome! The goal of this project is to create an efficient, user-friendly tool for migrating data from Universal Analytics to BigQuery. Whether it's feature enhancements, bug fixes, or documentation improvements, your input is highly valued.

## Contact Information
For help, feedback, or discussions about potential features, please feel free to connect with me on [Linkedin](https://www.linkedin.com/in/ali-iz/).


