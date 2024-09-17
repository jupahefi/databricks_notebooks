# Databricks notebook source
# MAGIC %md
# MAGIC # Procesamiento de archivos PDF con PyPDF2, Pandas y Spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instalación de librerías
# MAGIC ### Librerías que se probaron
# MAGIC `pip install tabula-py`
# MAGIC `pip install pdf2image pdfplumber`
# MAGIC `pip install fitz frontend tools`
# MAGIC `pip install pdf2html`
# MAGIC
# MAGIC Finalmente se eligió la librería **PyPDF2** ya que extrae de forma exitoso todo el PDF como String

# COMMAND ----------

# MAGIC %pip install PyPDF2 lxml
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solución Funcionando

# COMMAND ----------

import re
import PyPDF2
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import traceback

"""
local_pdf_file_path = "/tmp/" + "Boleta gastos comunes mayo - 2024.pdf"
pdf_document = PyPDF2.PdfReader(open(local_pdf_file_path, "rb"))
#for pdf_page_num in range(len(pdf_document.pages)):
pdf_page = pdf_document.pages[1]
pdf_page_text = pdf_page.extract_text()
pdf_page_text = pdf_page_text.replace("$", " $")
print(pdf_page_text)
"""
print(1)

# COMMAND ----------

def esta_en_mayusculas(texto: str) -> bool:
    return texto.isupper()

def is_first_character_digit(s: str) -> bool:
    """
    Verifica si el primer carácter de una cadena es un dígito.

    Args:
        s (str): La cadena a verificar.

    Returns:
        bool: True si el primer carácter es un dígito, False en caso contrario.
    """
    return s[0].isdigit()

def remove_starting_number(s: str) -> str:
    """
    Elimina el número al comienzo de una cadena.

    Args:
        s (str): La cadena de entrada.

    Returns:
        str: La cadena sin el número al comienzo.
    """
    return s.lstrip('0123456789')

def is_first_char_dollar(s):
    """Verifica si el primer carácter del string es el signo $."""
    return s.strip()[0] == '$' if s.strip() else False

# COMMAND ----------

# Ruta de los archivos PDF en Azure Blob Storage
pdf_files_path = "abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/gc/"

# Listar archivos
archivos = dbutils.fs.ls(pdf_files_path)

# Filtrar para incluir solo archivos PDF
archivos_pdf = [archivo.path for archivo in archivos if archivo.path.endswith(".pdf")]

# Crear la tabla HTML
html_content = "<table border='1'>"
html_content += "<tr><th>periodo</th><th>concepto_principal</th><th>concepto_secundario</th><th>item</th><th>valor_101</th><th>valor_comunidad</th></tr>"

# Diccionario de mapeo de nombres de meses a números de mes
meses = {
    "enero": "01",
    "febrero": "02",
    "marzo": "03",
    "abril": "04",
    "mayo": "05",
    "junio": "06",
    "julio": "07",
    "agosto": "08",
    "septiembre": "09",
    "octubre": "10",
    "noviembre": "11",
    "diciembre": "12"
}

# Expresión regular para extraer el periodo (mes - año) del nombre del archivo
regex = r"(\w+ - \d{4})\.pdf$"

# Recorrer los archivos PDF listados
for archivo in archivos_pdf:
    nombre_archivo = os.path.basename(archivo)
    pdf_file_path = archivo
    print(archivo)
    print(nombre_archivo)
    # Descarga el archivo PDF a un directorio temporal en Databricks
    local_pdf_file_path = "/tmp/" + nombre_archivo
    dbutils.fs.cp(archivo, "file:" + local_pdf_file_path)

# COMMAND ----------

# Recorrer los archivos PDF listados
for archivo in archivos_pdf:
    nombre_archivo = os.path.basename(archivo)
    pdf_file_path = archivo

    # Extraer periodo y convertirlo formato de fecha
    periodo = re.search(regex, archivo)
    if periodo:
        periodo = periodo.group(1)
        mes, anio = periodo.split(" - ")
        fecha = f"01-{meses[mes.lower()]}-{anio}"
        fecha_formateada = datetime.strptime(fecha, "%d-%m-%Y").strftime("%Y-%m-%d")
    else:
        fecha_formateada = "No se encontró el periodo"

    # Directorio temporal en Databricks
    local_pdf_file_path = "/tmp/" + nombre_archivo

    # Abrir el archivo PDF con PyPDF2
    pdf_document = PyPDF2.PdfReader(open(local_pdf_file_path, "rb"))

    # Expresión regular para filtrar las líneas deseadas
    pattern = re.compile(r'^(.*?)\s+\$\s+([\d,.]+)\s+\$\s+([\d,.]+)$')

    if fecha_formateada >= '2024-05-01':
        #Procesar Egresos con formato nuevo
        for index, pdf_page in enumerate(pdf_document.pages):
            pdf_page_text = pdf_page.extract_text()
            pdf_page_text = pdf_page_text.replace("$", " $")
            text_rows = pdf_page_text.split('\n')
            category = ""
            #"Sin categoría"
            sub_category = ""
            #"Sin sub-categoría"
            for row in text_rows:
                match = pattern.match(row.strip())
                if match:
                    if(esta_en_mayusculas(match.group(1))):
                        category = match.group(1)
                    else:
                        item = match.group(1)
                        value_101 = float(match.group(2).replace('$', '').replace('.', '').replace(',', '.'))
                        value_community = float(match.group(3).replace('$', '').replace('.', '').replace(',', '.'))
                        if(is_first_character_digit(item)):
                            sub_category = remove_starting_number(item)
                            item = remove_starting_number(item)
                        html_content += f"<tr><td>{fecha_formateada}</td><td>{category}</td><td>{sub_category}</td><td>{item}</td><td>{value_101}</td><td>{value_community}</td></tr>"
        #print(f"Periodo cargado: {periodo}")

#Cerrar la tabla HTML
html_content += "</table>"
print(html_content)

#TODO: Procesar Medidores
#TODO: Procesar Fondos

# Imprimir el contenido HTML
#print(html_content)


# COMMAND ----------

import pandas as pd
import lxml

# Leer la tabla HTML en un DataFrame de Pandas
df_pandas = pd.read_html(html_content)[0]

# Convertir el DataFrame de Pandas a un DataFrame de Spark
df_spark = spark.createDataFrame(df_pandas).drop_duplicates()
df_spark.createOrReplaceTempView("egresos")
df_spark2 = spark.sql("""
    with
    reparaciones as (
        select '2024-05-01', 'CONSUMOS BÁSICOS', 'Enel Distribución Chile S.A.', 'Consumo Eléctrico Enel Mayo Provisión', 3386, 460000 
    )
    
    select 
        periodo,
        concepto_principal,
        case
            when concepto_secundario = 'Energía Eléctrica'
                then 'Enel Distribución Chile S.A.'
            when concepto_secundario = 'Suministro de Agua'
                then 'Aguas Andinas'
            else concepto_secundario
        end as concepto_secundario,
        item,
        valor_101,
        valor_comunidad
    from egresos
    where concepto_secundario != item
        and concepto_principal = 'CONSUMOS BÁSICOS'
    --union all
    --select * from reparaciones
    --order by concepto_principal asc
""")

df_spark2.write.mode("overwrite").format("delta").saveAsTable("unity_catalog.lab.egresos_3")

# Mostrar el DataFrame de Spark
display(df_spark2)

# COMMAND ----------

# Definir el esquema
schema = StructType([
    StructField("periodo", StringType(), True),
    StructField("concepto_principal", StringType(), True),
    StructField("concepto_secundario", StringType(), True),
    StructField("item", StringType(), True),
    #StructField("n_doc", StringType(), True),
    StructField("valor_101", FloatType(), True),
    StructField("valor_comunidad", FloatType(), True)
])

spark.sql("DROP TABLE IF EXISTS unity_catalog.lab.egresos_1")

# Recorrer los archivos PDF listados
for archivo in archivos_pdf:
    nombre_archivo = os.path.basename(archivo)
    pdf_file_path = archivo

    # Extraer periodo y convertirlo formato de fecha
    periodo = re.search(regex, archivo)
    if periodo:
        periodo = periodo.group(1)
        mes, anio = periodo.split(" - ")
        fecha = f"01-{meses[mes.lower()]}-{anio}"
        fecha_formateada = datetime.strptime(fecha, "%d-%m-%Y").strftime("%Y-%m-%d")
    else:
        fecha_formateada = "No se encontró el periodo"

    # Descarga el archivo PDF a un directorio temporal en Databricks
    local_pdf_file_path = "/tmp/" + nombre_archivo

    # Abrir el archivo PDF con PyPDF2
    pdf_document = PyPDF2.PdfReader(open(local_pdf_file_path, "rb"))

    #Procesar Egresos

    # Extraer consumos básicos
    i = 0
    start = 0
    end = 0
    pdf_text = ""
    records = []

    # Consolida páginas de cada PDF en texto
    for index, pdf_page in enumerate(pdf_document.pages):
        pdf_page_text = pdf_page.extract_text()
        pdf_text += pdf_page_text

    text_rows = pdf_text.split('\n')

    while i < len(text_rows):
        if text_rows[i] == "CONSUMOS BÁSICOS":
            start = i + 3
        if start > 0 and text_rows[i] == " ":
            end = i
            break
        i += 1
    
    if start != 0 and end != 0:
        print(f"{start} and {end}")

        i = start
        while i < end:
            try:
                index = int(text_rows[i])
                company = text_rows[i + 1]
                description = text_rows[i + 2]
                start_index = 3
                account = None
                if not is_first_char_dollar(text_rows[i + 3]):
                    if text_rows[i + start_index] in ['2908569-2', 'Provisionado', '2023','3566908-6 Prov', 'Piscina n°2908569-2 Septiembre', 'n°2908568-4 Septiembre 2023', 'n°2908568-4 Octubre 2023', 'Piscina n°2908569-2 Octubre 2023', '3566908-6','Piscina n°2908569-2 Agosto 2023','n°2908568-4 Agosto 2023','Prov']:
                        if text_rows[i + 5] in ['26-10-2023', '25-08-2023', '27-08-2023', '20-09-2023', '30-09-2023']:
                            description += " " + text_rows[i + 3] + " " + text_rows[i + 5]
                            account = text_rows[i + 4]
                            start_index = 6
                        else:
                            if text_rows[i + 6] in ['20-09-2023']:
                                description += " " + text_rows[i + 3] + " " + text_rows[i + 4] + " " + text_rows[i + 6]
                                account = text_rows[i + 5]
                                start_index = 7
                            else:
                                description += " " + text_rows[i + 3]
                                account = text_rows[i + 4] 
                                start_index = 5
                    else:
                        account = text_rows[i + 3]
                        start_index = 4
                amount1 = float(text_rows[i + start_index].replace('$', '').replace('.', '').replace(',', '.'))
                amount2 = float(text_rows[i + start_index + 1].replace('$', '').replace('.', '').replace(',', '.'))
                records.append((fecha_formateada, "CONSUMOS BÁSICOS", company, description, amount1, amount2))
                #records.append((fecha_formateada, "CONSUMOS BÁSICOS", company, description, account, amount1, amount2))
                i += start_index + 2
            except Exception as e:
                print(f"Error processing line {traceback.extract_tb(e.__traceback__)[0].lineno}: {i} - {text_rows[i]} - {e} - start_index: {start_index}")
                break

        # Crear el DataFrame
        df = spark.createDataFrame(records, schema).drop_duplicates()
        df.write.mode("append").format("delta").saveAsTable("unity_catalog.lab.egresos_1")

        # Mostrar el DataFrame
        print(f"Periodo cargado: {periodo} - start_index: {start_index}")
        display(df)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from unity_catalog.lab.egresos_1

# COMMAND ----------

# Definir el esquema
schema = StructType([
    StructField("periodo", StringType(), True),
    StructField("concepto_principal", StringType(), True),
    StructField("concepto_secundario", StringType(), True),
    StructField("item", StringType(), True),
    #StructField("n_doc", StringType(), True),
    StructField("valor_101", FloatType(), True),
    StructField("valor_comunidad", FloatType(), True)
])

#spark.sql("DROP TABLE IF EXISTS unity_catalog.lab.egresos_2")

# Recorrer los archivos PDF listados
for archivo in archivos_pdf:
    nombre_archivo = os.path.basename(archivo)
    pdf_file_path = archivo

    # Extraer periodo y convertirlo formato de fecha
    periodo = re.search(regex, archivo)
    if periodo:
        periodo = periodo.group(1)
        mes, anio = periodo.split(" - ")
        fecha = f"01-{meses[mes.lower()]}-{anio}"
        fecha_formateada = datetime.strptime(fecha, "%d-%m-%Y").strftime("%Y-%m-%d")
    else:
        fecha_formateada = "No se encontró el periodo"

    # Directorio temporal en Databricks
    local_pdf_file_path = "/tmp/" + nombre_archivo

    # Abrir el archivo PDF con PyPDF2
    pdf_document = PyPDF2.PdfReader(open(local_pdf_file_path, "rb"))

    #Procesar Egresos

    # Extraer consumos básicos
    i = 0
    start = 0
    end = 0
    pdf_text = ""
    records = []

    # Consolida páginas de cada PDF en texto
    for index, pdf_page in enumerate(pdf_document.pages):
        pdf_page_text = pdf_page.extract_text()
        pdf_text += pdf_page_text

    text_rows = pdf_text.split('\n')

    while i < len(text_rows):
        if text_rows[i] == "CONSUMOS BÁSICOS":
            start = i + 3
        if start > 0 and text_rows[i] == " ":
            end = i
            break
        i += 1
    
    if start != 0 and end != 0 and fecha_formateada == "2024-04-01":
        print(f"{start} and {end}")

        i = start
        while i < end:
            try:
                if(is_first_character_digit(text_rows[i])):
                    index = int(text_rows[i])
                    suma = 3
                else:
                    index = 1
                    suma = 0
                company = text_rows[i + 1 + suma]
                description = text_rows[i + 2 + suma]
                start_index = 3 + suma
                account = None
                if not is_first_char_dollar(text_rows[i + 3 + suma]):
                    account = text_rows[i + start_index]
                    start_index = 4 + suma
                amount1 = float(text_rows[i + start_index].replace('$', '').replace('.', '').replace(',', '.'))
                amount2 = float(text_rows[i + start_index + 1].replace('$', '').replace('.', '').replace(',', '.'))
                records.append((fecha_formateada, "CONSUMOS BÁSICOS", company, description, amount1, amount2))
                #records.append((fecha_formateada, "CONSUMOS BÁSICOS", company, description, account, amount1, amount2))
                i += start_index + 2
            except Exception as e:
                print(f"Error processing line {traceback.extract_tb(e.__traceback__)[0].lineno}: {i} - {text_rows[i]} - {e}")
                break

        # Crear el DataFrame
        df = spark.createDataFrame(records, schema).drop_duplicates()
        df.createOrReplaceTempView("egresos")
        df_spark2 = spark.sql("""
            select 
                periodo,
                concepto_principal,
                case
                    when concepto_secundario = 'Movistar'
                        then 'Telefonía e Internet'
                    when concepto_secundario = 'Consumo Agua Jardinería Abril'
                        then 'Aguas Andinas'
                    else concepto_secundario
                end as concepto_secundario,
                case
                    when concepto_secundario = 'Consumo Agua Jardinería Abril'
                        then concepto_secundario
                    else item
                end as item,
                /*case
                    when concepto_secundario = 'Consumo Agua Jardinería Abril'
                        then item
                    else n_doc
                end as n_doc,*/
                valor_101,
                valor_comunidad
            from egresos
            where concepto_secundario != item
        """)
        df_spark2.write.mode("overwrite").format("delta").saveAsTable("unity_catalog.lab.egresos_2")

        # Mostrar el DataFrame
        print(f"Periodo cargado: {periodo}")
        display(df_spark2)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE unity_catalog.lab.egresos_ajustados AS
# MAGIC with
# MAGIC egresos_3 as (
# MAGIC   select * 
# MAGIC   from unity_catalog.lab.egresos_3
# MAGIC ),
# MAGIC egresos_2 as (
# MAGIC   select * 
# MAGIC   from unity_catalog.lab.egresos_2
# MAGIC ),
# MAGIC egresos_1 as (
# MAGIC   select * 
# MAGIC   from unity_catalog.lab.egresos_1
# MAGIC ),
# MAGIC egresos_concatenados as (
# MAGIC   select * from egresos_3
# MAGIC   union all
# MAGIC   select * from egresos_2
# MAGIC   union all
# MAGIC   select * from egresos_1
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC   periodo, 
# MAGIC   concepto_principal,
# MAGIC   case
# MAGIC     when concepto_secundario = 'Enel Distribución Chile S.A.'
# MAGIC       then 'Luz'
# MAGIC     when concepto_secundario = 'Aguas Andinas'
# MAGIC       then 'Agua'
# MAGIC     else concepto_secundario
# MAGIC   end as concepto_secundario,
# MAGIC   item,
# MAGIC   valor_101,
# MAGIC   valor_comunidad
# MAGIC from egresos_concatenados
# MAGIC where concepto_secundario not in ("Telefonía e Internet")
# MAGIC order by periodo desc, concepto_secundario asc, valor_comunidad desc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM unity_catalog.lab.egresos_ajustados
# MAGIC WHERE concepto_secundario NOT IN ("Telefonía e Internet")
# MAGIC ORDER BY 
# MAGIC   periodo DESC, 
# MAGIC   concepto_secundario ASC, 
# MAGIC   valor_comunidad DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from unity_catalog.lab.egresos_ajustados
# MAGIC where concepto_secundario in ("Agua")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from unity_catalog.lab.egresos_ajustados
# MAGIC where concepto_secundario in ("Luz")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table unity_catalog.lab.egresos_2
# MAGIC select * from unity_catalog.lab.egresos_2
# MAGIC where concepto_secundario not in ("Telefonía e Internet")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- probar nvl()
# MAGIC select sum(valor_comunidad) 
# MAGIC from unity_catalog.lab.egresos_ajustados
# MAGIC where concepto_secundario not in ("Telefonía e Internet")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from unity_catalog.lab.egresos_ajustados
# MAGIC where lower(item) like '%provi%'

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_format
from pyspark.sql.types import DateType
import pandas as pd

# Crear un DataFrame con las fechas de septiembre 2023 hasta el mes actual
start_date = '2023-09-01'
end_date = pd.to_datetime('today').strftime('%Y-%m-%d')

# Generar las fechas
date_range = pd.date_range(start=start_date, end=end_date, freq='MS')

# Convertir el rango de fechas a un DataFrame de Spark
dates_df = spark.createDataFrame(pd.DataFrame(date_range, columns=['periodo']))

# Darle formato a las fechas
dates_df = dates_df.withColumn("periodo", date_format(col("periodo"), "yyyy-MM-01"))

# Aquí está el DataFrame que tienes actualmente en `sqldf`
# Por ejemplo: el DataFrame que estás usando podría ser algo así:
current_df = _sqldf  # El DataFrame actual con datos

# Hacer un 'join' entre el calendario generado y el DataFrame actual
final_df = dates_df.join(current_df, on="periodo", how="left")

# Reemplazar los valores nulos con 0 para las columnas que quieres llenar con 0
final_df = final_df.fillna(0, subset=["valor_101", "valor_comunidad"])

# Mostrar el resultado final
display(final_df)
