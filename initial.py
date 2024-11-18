import requests
from google.cloud import bigquery
import json

from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    "/Users/ayush/Downloads/dataengineeringproject-442104-ef546da50794.json"
)

client = bigquery.Client(credentials=credentials, project="dataengineeringproject-442104")

# Constants
API_KEY = "ln0vq8liSoorAF13jBUdyyiIFelqXbm8"  
BASE_URL = "https://api.nytimes.com/svc/books/v3/lists/current"
CATEGORY = "hardcover-fiction"  
PROJECT_ID = "dataengineeringproject-442104"  
DATASET_NAME = "nybooks"  # Replace with your BigQuery dataset name
TABLE_NAME = "bestsellers"  # Replace with your BigQuery table name


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
    
import csv
from google.cloud import bigquery

def export_to_csv(rows, csv_filename):
    # Write rows to a CSV file
    with open(csv_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(rows[0].keys())
        # Write rows
        for row in rows:
            writer.writerow(row.values())
    print(f"Data exported to {csv_filename}")

def load_csv_to_bigquery(project_id, dataset_name, table_name, csv_filename):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_name).table(table_name)

    # Load the CSV file into BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the header row
        autodetect=True  # Automatically detect schema
    )
    with open(csv_filename, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    print(f"Data loaded into {table_name}")

# Update the `insert_into_bigquery` function
def insert_into_bigquery(rows, project_id, dataset_name, table_name):
    csv_filename = "books.csv"
    export_to_csv(rows, csv_filename)  # Export rows to CSV
    load_csv_to_bigquery(project_id, dataset_name, table_name, csv_filename)  # Load to BigQuery

# Main script
def main():
    print("Fetching data from NYTimes Books API...")
    books = fetch_best_sellers(API_KEY, CATEGORY)
    if books:
        print(f"Fetched {len(books)} books. Inserting data into BigQuery...")
        insert_into_bigquery(books, PROJECT_ID, DATASET_NAME, TABLE_NAME)
    else:
        print("No data available.")

if __name__ == "__main__":
    main()