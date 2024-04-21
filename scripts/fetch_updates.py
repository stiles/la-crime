#!/usr/bin/env python
# coding: utf-8

"""
Fetch and save the last LA Crime 10,000 incidents and store
"""

import os
import requests
import pandas as pd

app_token = os.environ.get("SOCRATA_APP_TOKEN")

# Function to request 10,000 records in descending order

def fetch_data_with_pagination_with_progress(base_url, limit=1000, max_iterations=10):
    offset = 0
    all_records = []
    current_iteration = 0

    print("Fetching data...")
    while current_iteration < max_iterations:
        query_url = f"{base_url}&$limit={limit}&$offset={offset}"
        response = requests.get(query_url)
        data = response.json()

        if not data:
            print("No more data to fetch.")
            break  # Stop if there are no more records

        all_records.extend(data)
        offset += limit
        current_iteration += 1

        print(f"Progress: {current_iteration}/{max_iterations} iterations completed.")

    return pd.DataFrame(all_records)



# Call the function to fetch the latest
base_url = "https://data.lacity.org/resource/2nrs-mtv8.json?$order=date_rptd DESC"
df = fetch_data_with_pagination_with_progress(base_url)

# Output the data for cleaning and storage in database
df.to_parquet('../data/raw/updated_incidents_table.parquet', index=False)