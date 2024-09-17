# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE personas (
# MAGIC       nombre VARCHAR2(50),
# MAGIC           segundo_nombre VARCHAR2(50),
# MAGIC               apellido VARCHAR2(50)
# MAGIC               );
# MAGIC
# MAGIC               -- Insertar datos ficticios
# MAGIC               INSERT INTO personas (nombre, segundo_nombre, apellido) VALUES ('Juan', 'Carlos', 'Pérez');
# MAGIC               INSERT INTO personas (nombre, segundo_nombre, apellido) VALUES ('Ana', 'María', 'Gómez');
# MAGIC               INSERT INTO personas (nombre, segundo_nombre, apellido) VALUES ('Luis', NULL, 'Fernández');
# MAGIC               COMMIT;
# MAGIC )
