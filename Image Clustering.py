# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, IntegerType
from PIL import Image
import io
import numpy as np

# Ruta de la imagen en ADLS Gen2
image_path = "abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/images/z.png"

# Leer la imagen desde ADLS Gen2 como un archivo binario
image_df = spark.read.format("binaryFile").load(image_path)

# Definir una función UDF para convertir el contenido binario en un array multidimensional
def image_to_array(image_binary):
    image = Image.open(io.BytesIO(image_binary))
    return np.array(image).tolist()

# Registrar la UDF
image_to_array_udf = udf(image_to_array, ArrayType(ArrayType(ArrayType(IntegerType()))))

# Aplicar la UDF para convertir la imagen a un array
image_array_df = image_df.withColumn("image_array", image_to_array_udf(col("content")))

# Seleccionar solo la columna del array
image_array_df = image_array_df.select("image_array")

# Mostrar el array (truncado para que no sea muy largo)
image_array_df.show(20)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, array
from pyspark.sql.types import DoubleType, IntegerType, ArrayType
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf
import numpy as np
from PIL import Image

# Explosión de la imagen en píxeles individuales (aplanar la estructura)
pixels_df: DataFrame = image_array_df.select(explode("image_array").alias("row")) \
                          .select(explode("row").alias("pixel"))

# Descomponer cada píxel en sus componentes RGB y convertir a double
pixels_df = pixels_df.select(
    col("pixel")[0].cast(DoubleType()).alias("R"),
    col("pixel")[1].cast(DoubleType()).alias("G"),
    col("pixel")[2].cast(DoubleType()).alias("B")
)

# Usar VectorAssembler para crear la columna 'features' que Spark KMeans espera
assembler = VectorAssembler(inputCols=["R", "G", "B"], outputCol="features")
pixels_df = assembler.transform(pixels_df).select("features")

# Aplicar KMeans para agrupar los píxeles
kmeans: KMeans = KMeans(k=5, seed=1, featuresCol="features", predictionCol="cluster")
model: KMeansModel = kmeans.fit(pixels_df)



# COMMAND ----------

# Obtener los clusters (predicciones)
clustered_pixels_df: DataFrame = model.transform(pixels_df)

# Obtener las dimensiones originales de la imagen
image_dimensions = image_array_df.select("image_array").first()[0]
image_height = len(image_dimensions)
image_width = len(image_dimensions[0])

# Reasignar los colores basados en los centroides
centroids = model.clusterCenters()

# UDF para asignar el color basado en el centroide del cluster
def assign_color(cluster: int) -> list:
    return [int(c) for c in centroids[cluster]]

assign_color_udf = udf(assign_color, ArrayType(IntegerType()))

# Aplicar la UDF para obtener los nuevos colores
colored_pixels_df: DataFrame = clustered_pixels_df.withColumn("new_pixel", assign_color_udf(col("cluster")))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window  # <--- Esto es crucial
from pyspark.sql.types import BinaryType, StructType, StructField, IntegerType, ArrayType

# Agregar un índice para rastrear la posición de los píxeles
indexed_pixels_df = colored_pixels_df.withColumn("index", F.monotonically_increasing_id())

# Calcular las dimensiones de la imagen
image_dimensions = image_array_df.select("image_array").first()[0]
image_height = len(image_dimensions)
image_width = len(image_dimensions[0])

# Reconstruir la imagen usando la indexación
window_spec = Window.orderBy("index")
reconstructed_pixels_df = indexed_pixels_df.withColumn("row_num", F.row_number().over(window_spec)) \
    .withColumn("row_group", F.floor((F.col("row_num") - 1) / image_width)) \
    .groupBy("row_group") \
    .agg(F.collect_list("new_pixel").alias("row_pixels"))

# Crear una estructura binaria de la imagen usando solo Spark
def encode_row_as_bytes(row_pixels):
    # Convertir cada fila de píxeles en bytes y luego combinar
    return bytes([component for pixel in row_pixels for component in pixel])

encode_row_as_bytes_udf = F.udf(encode_row_as_bytes, BinaryType())

binary_image_df = reconstructed_pixels_df.withColumn("binary_row", encode_row_as_bytes_udf(F.col("row_pixels")))

# Combinar las filas binarias para formar la imagen completa
combined_binary_image = binary_image_df.agg(F.collect_list("binary_row").alias("binary_image")).collect()[0][0]



# COMMAND ----------

# Imprimir las dimensiones calculadas
print(f"Dimensiones de la imagen calculadas: Ancho = {image_width}, Altura = {image_height}")

# COMMAND ----------

# Crear un DataFrame con el contenido binario
binary_image_spark_df = spark.createDataFrame([(combined_binary_image,)], ["image_array"])


# COMMAND ----------

from PIL import Image
import io

# Obtener la primera fila del DataFrame
first_row = binary_image_spark_df.first()

# Obtener el campo 'image_array' desde la primera fila del DataFrame
image_array = first_row['image_array']

# Convertir todos los elementos de image_array a una lista de bytes
list_of_bytes = [bytes(ba) for ba in image_array]

# Combinar todos los bytes en un único bytearray
combined_bytes = b''.join(list_of_bytes)

# Asegurarte de que el tamaño de combined_bytes coincida con el esperado
expected_size = image_width * image_height * 3  # Ancho * Alto * 3 (para RGB)
if len(combined_bytes) != expected_size:
    print(f"Advertencia: el tamaño de los datos no coincide. Esperado: {expected_size}, Real: {len(combined_bytes)}")

# Ahora intenta convertirlo en una imagen
try:
    image = Image.frombytes('RGB', (1920, 1080), combined_bytes)
    image.show()  # Mostrar la imagen para ver si se carga correctamente
    image.save('imagen_resultante.png')  # Guardar imagen en el almacenamiento
    print("Imagen guardada exitosamente.")
except Exception as e:
    print(f"Error al intentar crear la imagen: {e}")

# COMMAND ----------

from IPython.display import Image, display

# Show saved image
image_path = 'imagen_resultante.png'
display(Image(filename=image_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imagen Original
# MAGIC ![z.png](./z.png "z.png")
# MAGIC
# MAGIC ### Imagen Con Clustering (k=5)
# MAGIC ![download.png](./download.png "download.png")
