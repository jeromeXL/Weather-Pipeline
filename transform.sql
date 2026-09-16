CREATE OR REPLACE VIEW `weather_analytics.fact_hourly_metrics` AS
WITH ranked_records AS (
  SELECT
    CAST(`timestamp` AS TIMESTAMP) AS metric_time,
    temperature_2m AS temperature_celsius,
    ROUND((temperature_2m * 9/5) + 32, 2) AS temperature_fahrenheit,
    humidity,
    CAST(extracted_at AS TIMESTAMP) AS extracted_at,
    ROW_NUMBER() OVER (
      PARTITION BY `timestamp`
      ORDER BY extracted_at DESC
    ) AS rn
  FROM
    `weather_analytics.staging_weather`
)
SELECT
  metric_time,
  temperature_celsius,
  temperature_fahrenheit,
  humidity,
  extracted_at
FROM
  ranked_records
WHERE
  rn = 1;
