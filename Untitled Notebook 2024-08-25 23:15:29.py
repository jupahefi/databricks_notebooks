# Databricks notebook source
# MAGIC %sql
# MAGIC WITH personas AS (
# MAGIC   SELECT
# MAGIC     'Juan' AS nombre,
# MAGIC     'Carlos' AS segundo_nombre,
# MAGIC     'Pérez' AS apellido
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     'Ana',
# MAGIC     'María',
# MAGIC     'Gómez'
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     'Luis',
# MAGIC     NULL,
# MAGIC     'Fernández'
# MAGIC )
# MAGIC SELECT
# MAGIC   DISTINCT COALESCE(nombre, '') || ' ' || COALESCE(segundo_nombre, '') || ' ' || COALESCE(apellido, '') AS combinacion
# MAGIC FROM
# MAGIC   personas;
