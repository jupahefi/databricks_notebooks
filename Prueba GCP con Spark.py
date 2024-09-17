# Databricks notebook source
def format_spark_jars(spark_jars: list[str], current_path: str) -> str:
    """
    Converts a list of strings into a single string with elements separated by commas,
    and adds "file://" to the beginning of each element.

    Args:
        spark_jars (list[str]): The list of strings to be formatted.
        ls (str): The path to be prepended to each element.

    Returns:
        str: The formatted string.
    """
    # Validate input
    if not isinstance(spark_jars, list):
        logging.error("spark_jars should be a list of strings.")
        raise TypeError("spark_jars should be a list of strings.")
    if not all(isinstance(jar, str) for jar in spark_jars):
        logging.error("All elements in spark_jars should be strings.")
        raise TypeError("All elements in spark_jars should be strings.")
    if not isinstance(current_path, str):
        logging.error("current_path should be a string.")
        raise TypeError("current_path should be a string.")

    formatted_jars = ""
    # Add "file://" and path to each element, then join them with commas
    for jar in spark_jars:
        formatted_jars += f"file://{current_path}/{jar},"

    # Remove the trailing comma
    formatted_jars = formatted_jars.rstrip(',')

    # Remove the trailing comma
    #formatted_jars = formatted_jars.rstrip(',')
    print(f"spark.jars: {formatted_jars}")
    return formatted_jars

format_spark_jars


# COMMAND ----------

# Logging configurations
import logging
import os
logging.basicConfig(level=logging.DEBUG)

# Spark Session
APP_NAME = "Colab"
SPARK_JARS = [
    "gcs-connector-hadoop3-2.2.0-shaded.jar",
    "hadoop-common-3.3.1.jar",
    "postgresql-42.2.20.jar"
]

# Google Cloud General
PROJECT_ID = "project-id"
GOOGLE_KEY_PATH = "/content/key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_KEY_PATH

# Google Cloud Storage
BUCKET_NAME = "bucket-name"
FOLDER_NAME = "colab_lab"

# Google Cloud BigQuery
DATASET_NAME = "sample_data"
TABLE_NAME = "anscombe"

# Google Colab
SOURCE_PATH = "/content/sample_data/"

# Files
JSON_NAME = "anscombe.json"


# COMMAND ----------

# Spark for Python
%pip install pyspark==3.3.1

# Google Cloud Storage client library
%pip install google-cloud-storage==2.8.0

# Cloud BigQuery Storage client library
%pip install google-cloud-bigquery-storage==2.24.0

# Google Cloud BigQuery client library
%pip install google-cloud-bigquery==3.20.1

# JayDeBeApi jdbc for native Python library
%pip install JayDeBeApi

# Library to wotk with Spark and Postgre jdbc driver
!wget /content/postgresql-42.2.20.jar https://jdbc.postgresql.org/download/postgresql-42.2.20.jar

# Library to work with Spark and Google Storage File System
!wget -nc -O /content/gcs-connector-hadoop3-2.2.0-shaded.jar https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.0/gcs-connector-hadoop3-2.2.0-shaded.jar

# Library to work with Spark and Google Hadoop File System
!wget -nc -O /content/hadoop-common-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.1/hadoop-common-3.3.1.jar
