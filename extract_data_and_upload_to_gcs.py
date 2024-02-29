import requests
import csv
from google.cloud import storage
import os

url = 'https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen'
headers = {
	"X-RapidAPI-Key": "e02c0b6bbbmshdec9332aba61d7cp1070d6jsn994372861eab",
	"X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}
params = {
    'formatType': 'odi'
}

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"gs://us-central1-api-etl-3b6b964d-bucket/dags/scripts/storage_access.json"


response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json().get('rank', [])  # Extracting the 'rank' data
    csv_filename = 'batsmen_rankings.csv'

    if data:
        field_names = ['rank', 'name', 'country']  # Specify required field names

        # Write data to CSV file with only specified field names
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            #writer.writeheader()
            for entry in data:
                writer.writerow({field: entry.get(field) for field in field_names})

        print(f"Data fetched successfully and written to '{csv_filename}'")
        
        # Upload csv file to GCS

        bucket_name='data_eng_staging'
        storage_client=storage.Client()
        bucket=storage_client.bucket(bucket_name)
        destination_blob_name=f'{csv_filename}'

        blob=bucket.blob(destination_blob_name)
        blob.upload_from_filename(csv_filename)

        print(f"File {csv_filename} uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
    else:
        print("No data available from the API.")

else:
    print("Failed to fetch data:", response.status_code)
    
    
