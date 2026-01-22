#!/bin/bash

# Define an array of file URLs to download
file_urls=(
    "https://example.com/file1.txt"
    "https://example.com/file2.txt"
    "https://example.com/file3.txt"
)   

# Create a directory to store downloaded files
mkdir -p /opt/airflow/dags/downloaded_files
cd /opt/airflow/dags/downloaded_files

# Loop through the array and download each file
for url in "${file_urls[@]}"; do
    wget "$url"
done    


echo "All files have been downloaded."


