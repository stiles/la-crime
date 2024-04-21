#!/usr/bin/env python
# coding: utf-8

"""
Fetch and save LA Crime data from specified URLs.
"""

import requests
from datetime import datetime

def fetch_and_save_data(url, file_path):
    """
    Fetch data from a URL and save it to a local file path.
    
    Args:
        url (str): The URL where we can get the data
        file_path (str): The local path for saving the file
    """
    try:
        print(f"Starting download from {url}")
        response = requests.get(url)
        response.raise_for_status()  # Raises stored HTTPError, if one occurred.

        with open(file_path, "wb") as file:
            file.write(response.content)
        
        print(f"Data successfully saved to {file_path}")
    except requests.exceptions.HTTPError as errh:
        print(f"Http error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout error: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Oops: Something else: {err}")

if __name__ == "__main__":
    today = datetime.now().strftime("%Y%m%d")
    today_epoch = int(datetime.now().timestamp())

    data_sources = [
        {
            "year": "2010-2019",
            "download_url": f"https://data.lacity.org/api/views/63jg-8b9z/rows.csv?fourfour=63jg-8b9z&cacheBust={today_epoch}&date={today}&accessType=DOWNLOAD",
            "file_path": "../data/raw/crime_data_2010_2019.csv",
        },
        {
            "year": "2020-present",
            "download_url": f"https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?fourfour=2nrs-mtv8&cacheBust={today_epoch}&date={today}&accessType=DOWNLOAD",
            "file_path": "../data/raw/crime_data_2020_present.csv",
        },
    ]

    for source in data_sources:
        fetch_and_save_data(source["download_url"], source["file_path"])
