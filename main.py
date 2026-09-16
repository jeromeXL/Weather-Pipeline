import datetime
import os

import functions_framework
import pandas as pd
import requests
from google.cloud import bigquery, storage

BUCKET_NAME = "raw-api-ingest-weather-941b88b1"
DATASET_ID = "weather_analytics"
TABLE_ID = "staging_weather"

# Sydney, matching the australia-southeast1 region the pipeline runs in.
API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=-33.8688&longitude=151.2093"
    "&hourly=temperature_2m,relative_humidity_2m"
)
REQUEST_TIMEOUT_SECONDS = 30


@functions_framework.http
def run_pipeline(request):
    response = requests.get(API_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    data = response.json()

    extracted_at = datetime.datetime.now(datetime.timezone.utc)

    hourly_data = data["hourly"]
    df = pd.DataFrame(
        {
            # Open-Meteo returns naive ISO-8601 strings in the requested
            # timezone, which defaults to GMT. Localise to UTC so the column
            # lands in BigQuery as TIMESTAMP rather than STRING.
            "timestamp": pd.to_datetime(hourly_data["time"], utc=True),
            "temperature_2m": hourly_data["temperature_2m"],
            "humidity": hourly_data["relative_humidity_2m"],
            "extracted_at": extracted_at,
        }
    )

    file_name = (
        f"weather_data_{extracted_at.strftime('%Y%m%d_%H%M%S')}.parquet"
    )
    local_path = f"/tmp/{file_name}"

    try:
        df.to_parquet(local_path, index=False)

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"raw/{file_name}")
        blob.upload_from_filename(local_path)
    finally:
        # /tmp on Cloud Functions is an in-memory tmpfs: files left behind
        # count against the instance memory limit for every warm invocation.
        if os.path.exists(local_path):
            os.remove(local_path)

    bq_client = bigquery.Client()
    table_ref = f"{bq_client.project}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    gcs_uri = f"gs://{BUCKET_NAME}/raw/{file_name}"
    load_job = bq_client.load_table_from_uri(
        gcs_uri, table_ref, job_config=job_config
    )
    load_job.result()

    return f"Successfully loaded {len(df)} rows into {table_ref}", 200
