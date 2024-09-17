# Databricks notebook source
import os

# Path to CSV files in Azure Blob Storage
csv_files_path = "abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/"

# List files
files = dbutils.fs.ls(csv_files_path)

# Filter to include only CSV files
csv_files = [file.path for file in files if file.path.endswith(".csv")]
print(csv_files)

# Iterate over the listed CSV files
for file in csv_files:
    file_name = os.path.basename(file)
    table_name = os.path.splitext(file_name)[0]  # Get the file name without extension
    df = spark.read.option("header", "true").csv(file)  # Read CSV file into DataFrame
    df.createOrReplaceTempView(table_name)  # Create temporary view with file name as table name


# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   round(avg(Female),2) AS avg_female, 
# MAGIC   round(avg(Male),2) AS avg_male
# MAGIC from EntriesGender

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *,
# MAGIC   round(Male / Total,2) AS percentage_male,
# MAGIC   round(Female / Total,2) AS percentage_female
# MAGIC from EntriesGender

# COMMAND ----------

# MAGIC %md
# MAGIC ![1*CT8ZR6aUDFUY2d5PKPfL-A.jpg](./1*CT8ZR6aUDFUY2d5PKPfL-A.jpg "1*CT8ZR6aUDFUY2d5PKPfL-A.jpg")

# COMMAND ----------


