# Databricks notebook source
bucket_name = "pruebas-loliveros"
mount_name = "pruebas-loliveros"
dbutils.fs.mount(f"gs://{bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------


