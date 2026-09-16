#!/usr/bin/env bash
#
# Deploys the weather ELT pipeline to GCP.
#
# Prerequisite: the Google Cloud SDK. It is NOT installed on this machine.
# Easiest path is Google Cloud Shell (https://shell.cloud.google.com), which
# has gcloud + bq preinstalled and already authenticated:
#   upload main.py, requirements.txt, transform.sql, deploy.sh -> bash deploy.sh
#
# To run locally instead, install the SDK first:
#   https://cloud.google.com/sdk/docs/install  then: gcloud auth login
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# "Weather-pipeline" is the project NAME (display only); the ID is below.
# ---------------------------------------------------------------------------
PROJECT_ID="project-941b88b1-704b-4bc4-bd0"

REGION="australia-southeast1"   # Sydney
BUCKET_NAME="raw-api-ingest-weather-941b88b1"
DATASET_ID="weather_analytics"
FUNCTION_NAME="ingest-weather-data"   # hyphens: gen2 names disallow underscores
SCHEDULER_JOB="hourly-weather-ingest"

if [[ -z "$PROJECT_ID" ]]; then
  echo "ERROR: set PROJECT_ID at the top of this script first." >&2
  echo "       Run 'gcloud projects list' and copy the PROJECT_ID column." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 1: environment setup and API activation
# ---------------------------------------------------------------------------
echo "==> Setting active project to ${PROJECT_ID}"
gcloud config set project "$PROJECT_ID"

echo "==> Enabling required services (this can take 1-2 minutes)"
gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  cloudscheduler.googleapis.com \
  eventarc.googleapis.com \
  run.googleapis.com \
  logging.googleapis.com

# Service enablement is eventually consistent; a deploy fired immediately after
# can fail with "API not enabled". Give propagation a moment.
sleep 20

echo "==> Creating GCS bucket gs://${BUCKET_NAME}"
if gcloud storage buckets describe "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
  echo "    bucket already exists, skipping"
else
  gcloud storage buckets create "gs://${BUCKET_NAME}" --location="$REGION"
fi

echo "==> Creating BigQuery dataset ${DATASET_ID}"
if bq --project_id="$PROJECT_ID" show --dataset "${DATASET_ID}" >/dev/null 2>&1; then
  echo "    dataset already exists, skipping"
else
  bq --project_id="$PROJECT_ID" mk --dataset --location="$REGION" "${DATASET_ID}"
fi

# ---------------------------------------------------------------------------
# Step 3: deployment and orchestration
# ---------------------------------------------------------------------------
echo "==> Deploying Cloud Function ${FUNCTION_NAME} (first build takes ~3 min)"
gcloud functions deploy "$FUNCTION_NAME" \
  --gen2 \
  --runtime=python311 \
  --region="$REGION" \
  --source=. \
  --entry-point=run_pipeline \
  --trigger-http \
  --allow-unauthenticated \
  --timeout=120s \
  --memory=512Mi

echo "==> Retrieving HTTP trigger URL"
FUNCTION_URL="$(gcloud functions describe "$FUNCTION_NAME" \
  --region="$REGION" --gen2 --format='value(serviceConfig.uri)')"

if [[ -z "$FUNCTION_URL" ]]; then
  echo "ERROR: could not resolve the function URL." >&2
  exit 1
fi
echo "    ${FUNCTION_URL}"

echo "==> Creating hourly Cloud Scheduler job ${SCHEDULER_JOB}"
if gcloud scheduler jobs describe "$SCHEDULER_JOB" --location="$REGION" >/dev/null 2>&1; then
  echo "    job exists, updating"
  gcloud scheduler jobs update http "$SCHEDULER_JOB" \
    --location="$REGION" \
    --schedule="0 * * * *" \
    --uri="$FUNCTION_URL" \
    --http-method=GET
else
  gcloud scheduler jobs create http "$SCHEDULER_JOB" \
    --location="$REGION" \
    --schedule="0 * * * *" \
    --uri="$FUNCTION_URL" \
    --http-method=GET
fi

echo "==> Triggering the job once to populate initial data"
gcloud scheduler jobs run "$SCHEDULER_JOB" --location="$REGION"

# The scheduler dispatch is asynchronous. transform.sql creates a view over
# staging_weather, which does not exist until the first load job finishes, so
# wait for the table to appear before running the transformation.
echo "==> Waiting for staging_weather to be populated"
for attempt in $(seq 1 30); do
  if bq --project_id="$PROJECT_ID" show \
       "${DATASET_ID}.staging_weather" >/dev/null 2>&1; then
    echo "    table exists"
    break
  fi
  if [[ "$attempt" -eq 30 ]]; then
    echo "ERROR: staging_weather never appeared. Check function logs:" >&2
    echo "  gcloud functions logs read ${FUNCTION_NAME} --region=${REGION} --gen2" >&2
    exit 1
  fi
  sleep 10
done

# ---------------------------------------------------------------------------
# Step 5: transformation
# ---------------------------------------------------------------------------
echo "==> Creating view ${DATASET_ID}.fact_hourly_metrics"
bq --project_id="$PROJECT_ID" query --use_legacy_sql=false < transform.sql

echo "==> Verifying"
bq --project_id="$PROJECT_ID" query --use_legacy_sql=false \
  "SELECT COUNT(*) AS rows_in_view,
          MIN(metric_time) AS earliest,
          MAX(metric_time) AS latest
   FROM \`${DATASET_ID}.fact_hourly_metrics\`"

echo
echo "Done. Pipeline live, running at minute 0 of every hour."
echo "  Function URL : ${FUNCTION_URL}"
echo "  Raw Parquet  : gs://${BUCKET_NAME}/raw/"
echo "  Staging      : ${PROJECT_ID}:${DATASET_ID}.staging_weather"
echo "  View         : ${PROJECT_ID}:${DATASET_ID}.fact_hourly_metrics"
