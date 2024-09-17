# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Configuración del entorno
# MAGIC
# MAGIC Asegúrate de tener Databricks configurado y las librerías necesarias instaladas.
# MAGIC

# COMMAND ----------

# Instalar el paquete delta-rs y polars
%pip install deltalake polars pyarrow

# Instalar el paquete memory_profiler si no está instalado
%pip install memory_profiler

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

# Define tu SAS token, nombre de cuenta de almacenamiento, nombre del contenedor y la ruta del archivo
sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-07-31T09:43:56Z&st=2024-07-04T01:43:56Z&spr=https&sig=pWMy7us1uqd8dXOJAQnhoeV49zhDVtcMbqcrVvLh%2Bec%3D"
storage_account_name = "cs2100320032141b0ad"
container_name = "raw"
delta_table_path = "benchmark_delta_table"

# Configuración del protocolo (abfs o abfss)
protocol = "abfss"  # Use "abfs" para conexiones no seguras

# Construir la URL para la carpeta especificada
delta_url = f"{protocol}://{container_name}@{storage_account_name}.dfs.core.windows.net/{delta_table_path}"

# Dar el SAS_TOKEN como opción de almacenamiento
storage_options = {"SAS_TOKEN": sas_token}

print(delta_url.replace(sas_token, "<SECRET>"))
print(str(storage_options).replace(sas_token, "<SECRET>"))

# COMMAND ----------

from pyspark.sql.functions import col
import time
import deltalake as dl
import polars as pl
from memory_profiler import memory_usage
from deltalake.writer import write_deltalake

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Crear una Delta Table para la prueba
# MAGIC
# MAGIC Primero, vamos a crear una Delta Table en Databricks que usaremos para las pruebas.

# COMMAND ----------

# Crear datos de prueba y escribir a una Delta Table
qty = 10000000
main_data = [(i, f"Name_{i}") for i in range(qty)]  # 1 millón de filas
df = spark.createDataFrame(main_data, ["id", "name"])

# Escribir a una Delta Table en ADLS sin características avanzadas .option("delta.enableDeletionVectors", "false")
df.write.format("delta").mode("overwrite").option("delta.enableDeletionVectors", "false").option("delta.enableChangeDataFeed", "false").save(delta_url)

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, delta_url)
properties = delta_table.detail().select("properties").collect()
print(properties)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Benchmark con Spark
# MAGIC
# MAGIC Realizaremos la lectura y escritura utilizando Spark y mediremos el tiempo y el uso de memoria.

# COMMAND ----------

# Medir el tiempo de lectura
start_time = time.time()
df_read = spark.read.format("delta").load(delta_url)
read_time_spark = time.time() - start_time

# Mostrar el tiempo de lectura
print(f"Tiempo de lectura con Spark: {read_time_spark} segundos")

# Medir el tiempo de escritura
start_time = time.time()
df_read.write.format("delta").mode("overwrite").save(delta_url + "_spark_write")
write_time_spark = time.time() - start_time

# Mostrar el tiempo de escritura
print(f"Tiempo de escritura con Spark: {write_time_spark} segundos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Benchmark con delta-rs y Polars
# MAGIC
# MAGIC Para esto, necesitas instalar el paquete `deltalake` que proporciona bindings de delta-rs para Python.
# MAGIC

# COMMAND ----------

# Leer la Delta Table usando delta-rs y Polars
start_time = time.time()
dt = dl.DeltaTable(delta_url, storage_options=storage_options)
read_time_delta_rs = time.time() - start_time

# Mostrar el tiempo de lectura
print(f"Tiempo de lectura con delta-rs y Polars: {read_time_delta_rs} segundos")

# Crear datos de prueba para escritura
data = pl.DataFrame(main_data, schema=["id", "name"], orient="row").lazy()

# Escribir usando delta-rs y Polars
start_time = time.time()
write_deltalake(table_or_uri=dt, data=data.collect().to_arrow(), engine="rust", mode="append")
write_time_delta_rs = time.time() - start_time

# Mostrar el tiempo de escritura
print(f"Tiempo de escritura con delta-rs y Polars: {write_time_delta_rs} segundos")

# COMMAND ----------

print(f"""
Tiempo de Ejecución del Benchmark:
🕒 Tiempo de lectura:
  - Spark: {read_time_spark} segundos
  - delta-rs y Polars: {read_time_delta_rs} segundos

🕒 Tiempo de escritura:
  - Spark: {write_time_spark} segundos
  - delta-rs y Polars: {write_time_delta_rs} segundos
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Medición de Uso de Memoria
# MAGIC
# MAGIC Para medir el uso de memoria, puedes utilizar herramientas como `memory_profiler`. Aquí hay un ejemplo de cómo integrarlo:
# MAGIC

# COMMAND ----------

from memory_profiler import memory_usage

# Medir el uso de memoria para lectura con Spark
mem_usage_spark_read = memory_usage((spark.read.format("delta").load, (delta_url,)))

# Medir el uso de memoria para escritura con Spark
mem_usage_spark_write = memory_usage((df_read.write.format("delta").mode("overwrite").save, (delta_url + "_spark_write",)))

# Medir el uso de memoria para lectura con `deltalake` y Polars
def read_delta_rs_polars():
    dt = dl.DeltaTable(delta_url, storage_options=storage_options)
    arrow_table = dt.to_pyarrow_table()
    df_polars = pl.from_arrow(arrow_table).lazy()

mem_usage_delta_rs_read = memory_usage(read_delta_rs_polars)

# Medir el uso de memoria para escritura con `deltalake` y Polars
def write_delta_rs_polars():
    write_deltalake(table_or_uri=delta_url, data=data.collect().to_arrow(), mode="overwrite", engine="rust", storage_options=storage_options)

mem_usage_delta_rs_write = memory_usage(write_delta_rs_polars)

print(f"""
Uso de Memoria del Benchmark:
📈 Uso de memoria para lectura (promedio en MiB):
  - Spark: {sum(mem_usage_spark_read) / len(mem_usage_spark_read)} MiB
  - `deltalake` y Polars: {sum(mem_usage_delta_rs_read) / len(mem_usage_delta_rs_read)} MiB

📈 Uso de memoria para escritura (promedio en MiB):
  - Spark: {sum(mem_usage_spark_write) / len(mem_usage_spark_write)} MiB
  - `deltalake` y Polars: {sum(mem_usage_delta_rs_write) / len(mem_usage_delta_rs_write)} MiB
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusión
# MAGIC
# MAGIC Con estos pasos y el código proporcionado, podrás realizar un benchmark comparativo entre Spark y delta-rs en Databricks, midiendo tanto el tiempo como el uso de memoria para las operaciones de lectura y escritura. ¡Espero que esto te sea útil para tus análisis!

# COMMAND ----------

print(f"""
Cantidad de registros del benchmark: {qty}

Tiempo de Ejecución del Benchmark:
🕒 Tiempo de lectura:
  - Spark: {read_time_spark} segundos
  - delta-rs y Polars: {read_time_delta_rs} segundos

🕒 Tiempo de escritura:
  - Spark: {write_time_spark} segundos
  - delta-rs y Polars: {write_time_delta_rs} segundos

Uso de Memoria del Benchmark:
📈 Uso de memoria para lectura (promedio en MiB):
  - Spark: {sum(mem_usage_spark_read) / len(mem_usage_spark_read)} MiB
  - `deltalake` y Polars: {sum(mem_usage_delta_rs_read) / len(mem_usage_delta_rs_read)} MiB

📈 Uso de memoria para escritura (promedio en MiB):
  - Spark: {sum(mem_usage_spark_write) / len(mem_usage_spark_write)} MiB
  - `deltalake` y Polars: {sum(mem_usage_delta_rs_write) / len(mem_usage_delta_rs_write)} MiB
""")
