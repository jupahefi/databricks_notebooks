# Databricks notebook source
#weather_spark_df = spark.read.option("header", "true").csv("abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/weatherAUS.csv")
#df_spark_df = spark.read.option("header", "true").csv("abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/df.csv")
#df_test_spark_df = spark.read.option("header", "true").csv("abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/df_test.csv")
#df_resultados = spark.read.option("header", "true").option("sep", ";").csv("abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/anime/salida-anime-2017-2019.csv")
df_resultados = spark.read.option("header", "true") \
                           .option("sep", ";") \
                           .option("quote", "\"") \
                           .option("escape", "\"") \
                           .option("multiLine", "true") \
                           .csv("abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/anime/*.csv")

df_resultados.createOrReplaceTempView("df_resultados")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from df_resultados 
