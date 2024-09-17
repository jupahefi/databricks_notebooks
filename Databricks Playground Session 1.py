# Databricks notebook source
path: str = "abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/"

files = dbutils.fs.ls(path)
display(files)

# COMMAND ----------

df_athletes = spark.read.csv("abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/Athletes.csv", header="true")

display(df_athletes)

# COMMAND ----------

df_athletes.createOrReplaceTempView("athletes")

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table if not exists unity_catalog.lab.athlete_spain
# MAGIC using DELTA
# MAGIC location 'abfss://staging@cs2100320032141b0ad.dfs.core.windows.net/lab/athletes/spain' as
# MAGIC select * from athletes 
# MAGIC --where Country = 'Spain'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from unity_catalog.lab.athletes where Country = 'Spain'
# MAGIC

# COMMAND ----------

# MAGIC %ls
