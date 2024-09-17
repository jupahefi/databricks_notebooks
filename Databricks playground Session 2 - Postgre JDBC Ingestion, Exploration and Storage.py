# Databricks notebook source
# MAGIC %md
# MAGIC # Constants definitions

# COMMAND ----------

# Conection parameters - constants must be with UPPER
JDBC_URL = "jdbc:postgresql://64.176.12.179:5432/maestra_factura" # Bill master
PROPERTIES = {
    "user": "testuser",
    "password": "testpassword",
    "driver": "org.postgresql.Driver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Database JDBC connection

# COMMAND ----------

# "20" > "1000000" - String works with ASCII
#  20  <  1000000  - This is true, numeric type
# Reminder: always look at the schema (spark schema is different from database schema)
df_jdbc = spark.read.jdbc(url=JDBC_URL, table="facturas", properties=PROPERTIES)

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploring the Data

# COMMAND ----------

# df_jdbc.show() was replaced with display in databricks
display(df_jdbc)

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing the Data

# COMMAND ----------

# format: delta, parquet, avro, orc
#df_jdbc.write.format("parquet").mode("overwrite").saveAsTable("table_name")

df_jdbc.createOrReplaceTempView("jdbc_data_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- external table is mandatory to have a storage location
# MAGIC create external table if not exists unity_catalog.lab.facturas
# MAGIC using DELTA
# MAGIC location 'abfss://staging@cs2100320032141b0ad.dfs.core.windows.net/lab/facturas' as
# MAGIC select * from jdbc_data_view 

# COMMAND ----------

# MAGIC %md
# MAGIC # Additional excercise with Native Spark

# COMMAND ----------

from pyspark.sql.functions import lit, expr, col
# Recommendation: study spark functions in documentation

df_jdbc_with_flag = df_jdbc.withColumn("flag", lit(1))
display(df_jdbc_with_flag)
display(df_jdbc)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into unity_catalog.lab.facturas
# MAGIC select * from unity_catalog.lab.facturas limit 1
