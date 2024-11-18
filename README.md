NYTimes Books API Data Pipeline

This project implements a data pipeline that fetches data from the New York Times Books API, processes it, and loads it into Google BigQuery for further analysis.

Features

Fetch Data from NYTimes Books API:
Retrieves data on the latest NYT Best Sellers (e.g., Hardcover Fiction).
Data includes book title, author, rank, description, and more.
Export Data to CSV:
Fetched data is saved locally in CSV format for easy inspection and use.
Load Data to Google BigQuery:
Processes the CSV file and loads it into a BigQuery table.
Uses batch loading to comply with the free-tier restrictions.
Error Handling:
Ensures authentication with Google Cloud using a service account.
Handles API response and data validation errors.
