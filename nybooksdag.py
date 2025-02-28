from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
import requests
import csv
import os
from datetime import datetime

# Constants
API_KEY = "ln0vq8liSoorAF13jBUdyyiIFelqXbm8"  
BASE_URL = "https://api.nytimes.com/svc/books/v3/lists/current"
CATEGORY = "hardcover-fiction"  
PROJECT_ID = "dataengineeringproject-442104"  
DATASET_NAME = "nybooks"  
TABLE_NAME = "bestsellers"  
GCS_BUCKET = "your-gcs-bucket"  # Optional: To store CSV in Google Cloud Storage

# Credentials and BigQuery client
def get_bigquery_client():
    credentials = service_account.Credentials.from_service_account_file(
        "/path/to/your/service-account-file.json"
    )
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    return client

# Fetch data from the API
def fetch_best_sellers(api_key, category):
    url = f"{BASE_URL}/{category}.json"
    params = {"api-key": api_key}
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()['results']['books']
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return []

# Export data to CSV
def export_to_csv(rows, csv_filename):
    with open(csv_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(rows[0].keys())  # Write headers
        for row in rows:
            writer.writerow(row.values())  # Write row values
    print(f"Data exported to {csv_filename}")

# Load CSV into BigQuery
def load_csv_to_bigquery(csv_filename):
    client = get_bigquery_client()
    table_ref = client.dataset(DATASET_NAME).table(TABLE_NAME)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    with open(csv_filename, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to finish
    print(f"Data loaded into {TABLE_NAME}")

# Define the function to automate the process
def insert_into_bigquery():
    books = fetch_best_sellers(API_KEY, CATEGORY)
    if books:
        csv_filename = "/tmp/books.csv"
        export_to_csv(books, csv_filename)  # Export data to CSV
        load_csv_to_bigquery(csv_filename)  # Load CSV to BigQuery
        os.remove(csv_filename)  # Clean up the CSV file after loading
    else:
        print("No data available.")

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 14),  # Adjust the start date as needed
    'retries': 1,
}

dag = DAG(
    'nytimes_books_data_ingestion',
    default_args=default_args,
    description='Automated pipeline to fetch, export, and load NY Times books data into BigQuery',
    schedule_interval='@daily',  # This can be changed to your preferred schedule
)

# Define the task in the Airflow DAG
ingestion_task = PythonOperator(
    task_id='fetch_and_load_books_data',
    python_callable=insert_into_bigquery,
    dag=dag,
)

# Set task dependencies (if any)
ingestion_task
