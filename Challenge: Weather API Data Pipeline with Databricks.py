# Databricks notebook source
# MAGIC %md
# MAGIC This notebook represents a PoC for a Data Pipeline with Ingestion, Storage, Processing, Analysis, Exploration and Visualization.
# MAGIC
# MAGIC We use Databricks (with PySpark), Azure Storage Account and Unity Catalog with External locations for this purpose.
# MAGIC
# MAGIC Practices: DRY, KISS, Clean Code Principles and Error Handling.
# MAGIC Paradigm: Basic functional programming
# MAGIC
# MAGIC Pipeline specifications
# MAGIC 1. Process last 7-days data into the database table(s) when running the pipeline for the first time. Consequent runs must insert only the new records. **This is not possible due to outdated data - In a first exploration I realized that the data is too old; thus, it is not possible to process only last 7 days. Also considering the low volumne of the data, full ingest will be made.**
# MAGIC 2. Re-running the pipeline multiple times, must not result in duplicate records in the table. **Ok**
# MAGIC 3. Feel free to assume other details as required. Document your assumptions. **All assumptions will be commented on the code and if necessary, in a text block of the notebook**
# MAGIC
# MAGIC In terms of doing incremental data, minor changes would be needed to use a temporary table that can be compared with the final one.

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion: Fetch Weather Data for a Single Station

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definitions

# COMMAND ----------

import requests
import json
from pyspark.sql import SparkSession

def fetch_raw_weather_data(station_id: str, observations_url_template: str) -> dict:
    """Fetch raw weather data for a specific station and return it as raw JSON."""
    observations_url = observations_url_template.format(station_id=station_id)
    response = requests.get(observations_url)
    response.raise_for_status()
    return response.json()

def save_raw_data_to_storage(spark: SparkSession, raw_data: dict, raw_storage_path: str) -> None:
    """Ingest raw JSON data into the storage system in Delta format without any transformation."""
    # Convert the raw JSON data into a JSON string format suitable for Delta Lake
    raw_json_str = json.dumps(raw_data)
    
    # Create a DataFrame from the JSON string
    raw_df = spark.createDataFrame([(raw_json_str,)], ["value"])
    
    # Save the raw data to the specified storage path (Delta or raw container)
    raw_df.write.format("delta").mode("overwrite").save(raw_storage_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage

# COMMAND ----------

# Set station ID and API URL template
station_id = "0112W"
observations_url_template = "https://api.weather.gov/stations/{station_id}/observations"

# Fetch the raw weather data (without any extraction or transformation)
raw_weather_data = fetch_raw_weather_data(station_id, observations_url_template)

# Save the raw JSON data to storage in Delta format
raw_storage_path = "abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/weather_observations_raw"
save_raw_data_to_storage(spark, raw_weather_data, raw_storage_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Storage: Load Data into Delta Table with External Location

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definitions

# COMMAND ----------

from pyspark.sql import SparkSession

def create_external_table_if_not_exists(spark: SparkSession, table_name: str, delta_storage_path: str) -> None:
    """Create an external Delta table in Unity Catalog if it does not already exist."""
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{delta_storage_path}'
    """)

def save_raw_data_to_unity_table(spark: SparkSession, raw_data: dict, table_name: str, delta_storage_path: str) -> None:
    """Save the ingested raw JSON data to Unity Catalog in the specified Delta table."""

    create_external_table_if_not_exists(spark, table_name, delta_storage_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage

# COMMAND ----------

# Define the table name and Delta storage path
table_name = "unity_catalog.lab.weather_observations_raw"
delta_storage_path = "abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/weather_observations_raw"

# Fetch the raw weather data (this can be from the previous ingestion stage)
raw_weather_data = fetch_raw_weather_data(station_id, observations_url_template)

# Save the raw data to Unity Catalog as a Delta table
save_raw_data_to_unity_table(spark, raw_weather_data, table_name, delta_storage_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Processing: Preparing, cleaning and structuring the data

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS unity_catalog.lab.weather_observations_cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE unity_catalog.lab.weather_observations_cleaned
# MAGIC LOCATION 'abfss://staging@cs2100320032141b0ad.dfs.core.windows.net/weather_observations_cleaned'
# MAGIC AS
# MAGIC WITH 
# MAGIC parsed AS (
# MAGIC   SELECT
# MAGIC     feature.properties.station AS station_id,
# MAGIC     feature.id AS station_name,
# MAGIC     feature.geometry.coordinates[0] AS longitude,
# MAGIC     feature.geometry.coordinates[1] AS latitude,
# MAGIC     feature.properties.timestamp AS observation_timestamp,
# MAGIC     'UTC' AS station_timezone,  -- Assuming UTC
# MAGIC     round(cast(feature.properties.temperature.value AS double), 2) AS temperature,
# MAGIC     round(cast(feature.properties.windSpeed.value AS double), 2) AS wind_speed,
# MAGIC     round(cast(feature.properties.relativeHumidity.value AS double), 2) AS humidity
# MAGIC   FROM unity_catalog.lab.weather_observations_raw
# MAGIC   LATERAL VIEW EXPLODE(from_json(get_json_object(value, '$.features'), 
# MAGIC     'ARRAY<STRUCT<
# MAGIC       id: STRING, 
# MAGIC       geometry: STRUCT<coordinates: ARRAY<DOUBLE>>, 
# MAGIC       properties: STRUCT<
# MAGIC         station: STRING, 
# MAGIC         timestamp: STRING, 
# MAGIC         temperature: STRUCT<value: DOUBLE>, 
# MAGIC         windSpeed: STRUCT<value: DOUBLE>, 
# MAGIC         relativeHumidity: STRUCT<value: DOUBLE>
# MAGIC       >
# MAGIC     >>')) AS feature
# MAGIC )
# MAGIC SELECT * FROM parsed
# MAGIC WHERE station_id IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploration: Know the data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM unity_catalog.lab.weather_observations_cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check duplications
# MAGIC SELECT DISTINCT * FROM unity_catalog.lab.weather_observations_cleaned;

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis: Required metrics in SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Average observed temperature for the last week (Monday-Sunday)
# MAGIC Note: as the data is outdated, the date filter was commented to explore the analyzed data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE unity_catalog.lab.weather_observations_avg
# MAGIC LOCATION 'abfss://curated@cs2100320032141b0ad.dfs.core.windows.net/weather_observations_avg'
# MAGIC AS
# MAGIC SELECT AVG(temperature) AS avg_temperature_last_week
# MAGIC FROM unity_catalog.lab.weather_observations_cleaned
# MAGIC /*WHERE observation_timestamp >= DATEADD(day, -7, CURRENT_DATE) -- Last 7 days from today
# MAGIC   AND observation_timestamp <= CURRENT_DATE;*/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE unity_catalog.lab.weather_observations_max_speed
# MAGIC LOCATION 'abfss://curated@cs2100320032141b0ad.dfs.core.windows.net/weather_observations_max_speed'
# MAGIC AS
# MAGIC WITH 
# MAGIC ranked_observations AS (
# MAGIC   SELECT 
# MAGIC     station_id, 
# MAGIC     observation_timestamp, 
# MAGIC     wind_speed, 
# MAGIC     ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY observation_timestamp) AS row_num
# MAGIC   FROM unity_catalog.lab.weather_observations_cleaned
# MAGIC   /*WHERE observation_timestamp >= DATEADD(day, -7, CURRENT_DATE)
# MAGIC     AND observation_timestamp <= CURRENT_DATE*/
# MAGIC ),
# MAGIC wind_speed_changes AS (
# MAGIC   SELECT 
# MAGIC     a.station_id,
# MAGIC     a.observation_timestamp,
# MAGIC     ABS(a.wind_speed - b.wind_speed) AS wind_speed_change
# MAGIC   FROM ranked_observations a
# MAGIC   INNER JOIN ranked_observations b
# MAGIC     ON a.station_id = b.station_id
# MAGIC       AND a.row_num = b.row_num + 1 -- Consecutive observations
# MAGIC )
# MAGIC
# MAGIC SELECT MAX(wind_speed_change) AS max_wind_speed_change
# MAGIC FROM wind_speed_changes;

# COMMAND ----------

# MAGIC %md
# MAGIC # Visualization: Databricks visualizations to show Curated Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Note: Select visualization tab for each code block in this section.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from unity_catalog.lab.weather_observations_avg;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from unity_catalog.lab.weather_observations_max_speed
