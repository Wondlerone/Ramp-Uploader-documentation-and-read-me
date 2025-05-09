import os
import json
import tempfile
import functions_framework
import requests
from google.oauth2 import service_account
from google.cloud import bigquery
import base64
import csv

# Configuration
SPREADSHEET_ID = "1h9QM4_hxLYfpGIWMXXmMg0SjpdNfS_u5jIwGISzxLQs"
CLOUD_RUN_URL = "https://ramp-sftp-uploader-546663743745.europe-west2.run.app/upload"
DELEGATED_USER = "serviceaccount@wondle.io"  # Your dedicated service user

@functions_framework.http
def export_and_upload(request):
    """Cloud Function that connects to BigQuery using delegated credentials and uploads to Cloud Run."""
    try:
        # Get base64-encoded service account key from environment variable
        encoded_key = os.environ.get('SERVICE_ACCOUNT_KEY')
        if not encoded_key:
            raise ValueError("SERVICE_ACCOUNT_KEY environment variable is not set")
        
        # Decode the base64 string back to JSON
        key_json = base64.b64decode(encoded_key).decode('utf-8')
        
        # Create service account credentials with necessary scopes
        credentials = service_account.Credentials.from_service_account_info(
            info=json.loads(key_json),
            scopes=[
                'https://www.googleapis.com/auth/bigquery',
                'https://www.googleapis.com/auth/drive.readonly'
            ]
        )
        
        # Create delegated credentials - this is the key part that enables impersonation
        delegated_credentials = credentials.with_subject(DELEGATED_USER)
        
        # Create BigQuery client with delegated credentials
        client = bigquery.Client(
            project="wondle-reports-452716",
            credentials=delegated_credentials
        )
        
        # Run a query that accesses the external table connected to Google Drive
        query = """
        SELECT * 
        FROM `wondle-reports-452716.wondle_scans.ramp_report_draft` 
        """
        
        # Execute the query
        query_job = client.query(query)
        results = query_job.result()
        
        # Process the results into a CSV file
        column_names = [field.name for field in results.schema]
        
        # Create a CSV file using a temporary file
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp:
            # Create a CSV writer
            csv_writer = csv.writer(temp)
            
            # Write the header row
            csv_writer.writerow(column_names)
            
            # Write the data rows
            row_count = 0
            for row in results:
                # Convert row values to a list
                row_values = [row[column] for column in column_names]
                csv_writer.writerow(row_values)
                row_count += 1
                
            # Get the filename to use in the request
            temp_path = temp.name
            
        # Now send the CSV file to the Cloud Run service
        with open(temp_path, 'rb') as csv_file:
            files = {'file': (os.path.basename(temp_path), csv_file, 'text/csv')}
            # Since we made the function public, we don't need authentication for testing
            response = requests.post(CLOUD_RUN_URL, files=files)
            
        # Clean up the temporary file
        os.unlink(temp_path)
        
        # Check if the upload was successful
        if response.status_code == 200:
            return f"Successfully exported {row_count} rows and uploaded to RAMP Data Upload", 200
        else:
            return f"Error uploading to Cloud Run: {response.text}", 500
            
    except Exception as e:
        import traceback
        return f"Error: {str(e)}\n\nDetails: {traceback.format_exc()}", 500
