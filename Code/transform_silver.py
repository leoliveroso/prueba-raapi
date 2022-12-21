# Databricks notebook source
# DBTITLE 1,Import Lib
import pyspark.sql.functions as sql_func
from datetime import date, datetime, timedelta
import pyspark.sql.types as sql_types
from pyspark.sql import DataFrame
from dateutil.parser import parse
import dateutil.relativedelta
from functools import reduce
import pandas as pd
import numpy as np
import math
import json
import os

spark.conf.set("spark.sql.shuffle.partitions",4)

# COMMAND ----------

# DBTITLE 1,Define Params 
route_bronze = '/mnt/pruebas-loliveros/bronze/megelon_meetup'

# Use dbutils secrets to get Snowflake credentials.
user = dbutils.secrets.get("prueba-rappi", "snowflake-user")
password = dbutils.secrets.get("prueba-rappi", "snowflake-password")
schema = route_bronze.split("/")[-1]


options = {
  "sfUrl": "mu10071.us-central1.gcp.snowflakecomputing.com",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "BRONZE",
  "sfSchema": schema,
  "sfWarehouse": "COMPUTE_WH"
}

# COMMAND ----------

# DBTITLE 1,Create members_usuarios_ciudad_aux1
tablename = "members_usuarios_ciudad_aux1"

# read table members
route_tablenme = os.path.join(route_bronze, 'members')
df_members = spark.read.format('delta').load(route_tablenme)
df_members_usuarios_ciudad_aux1 = df_members.groupBy("city", 
                                                     "member_id").agg(sql_func.min(sql_func.col("joined")).alias("joined"))

display(df_members_usuarios_ciudad_aux1.limit(10))

# COMMAND ----------

# DBTITLE 1, Create members_usuarios_ciudad_aux2
#get year from date joined
df_members_usuarios_ciudad_aux2 = df_members_usuarios_ciudad_aux1.withColumn("anio", sql_func.year(sql_func.col("joined")))

#get month from date joined
df_members_usuarios_ciudad_aux2 = df_members_usuarios_ciudad_aux2.withColumn("mes", sql_func.month(sql_func.col("joined")))
display(df_members_usuarios_ciudad_aux2.limit(10))

# COMMAND ----------

# DBTITLE 1,Create members_usuarios_ciudad_aux3 
df_members_usuarios_ciudad_aux3 = df_members_usuarios_ciudad_aux2.groupBy(
    "city", "anio").agg(
        sql_func.count('member_id').alias("count_member")
    ).orderBy("city", "anio")
display(df_members_usuarios_ciudad_aux3.limit(10))

# COMMAND ----------


