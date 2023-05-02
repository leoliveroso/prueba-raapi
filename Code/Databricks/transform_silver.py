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
dbutils.widgets.text("route_bronze", '/mnt/pruebas-loliveros/bronze/megelon_meetup')
dbutils.widgets.text("route_silver", '/mnt/pruebas-loliveros/silver/megelon_meetup')
#dbutils.widgets.text("url_origen", "megelon/meetup")


route_bronze = dbutils.widgets.get("route_bronze")
route_silver = dbutils.widgets.get("route_silver")
path_silver = '/dbfs' + route_silver

#Create folder source if not exist
if not os.path.exists(path_silver):
    print(f"no existe la ruta, por ende se crea {path_silver}")
    os.mkdir(path_silver)

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

#Write
path_dest = os.path.join(route_silver, 'members_usuarios_ciudad_aux3')
df_members_usuarios_ciudad_aux3.write.format('delta').mode('overwrite').save(path_dest)
display(df_members_usuarios_ciudad_aux3.limit(10))



# COMMAND ----------

# DBTITLE 1,Transforms with temp views
# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vm_members_aux1_filtros as 
# MAGIC SELECT 
# MAGIC 	city ,
# MAGIC 	member_id,
# MAGIC 	min(joined) as joined
# MAGIC FROM delta.`/mnt/pruebas-loliveros/bronze/megelon_meetup/members`
# MAGIC group by 1, 2;
# MAGIC 
# MAGIC select * from vm_members_aux1_filtros limit 10;

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vm_members_aux2_filtros as
# MAGIC select
# MAGIC 	city,
# MAGIC 	member_id,
# MAGIC 	joined,
# MAGIC 	year(joined) as anio,
# MAGIC 	month(joined) as mes
# MAGIC from
# MAGIC 	vm_members_aux1_filtros;
# MAGIC     
# MAGIC select * from vm_members_aux2_filtros limit 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vm_members_aux3_filtros as 
# MAGIC select
# MAGIC 	city,
# MAGIC 	anio,
# MAGIC 	count(member_id) as cont_members
# MAGIC from 
# MAGIC 	vm_members_aux2_filtros
# MAGIC group by 1, 2;
# MAGIC 
# MAGIC select * from vm_members_aux3_filtros limit 15;

# COMMAND ----------

df =  spark.sql('select * from vm_members_aux3_filtros')
path_dest = os.path.join(route_silver, 'members_aux3_filtros')
df.write.format('delta').mode('overwrite').save(path_dest)

# COMMAND ----------

# DBTITLE 1,With Subquery
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_usuarios_anuales_por_ciudad as 
# MAGIC select
# MAGIC 	country,
# MAGIC 	state,
# MAGIC 	city,
# MAGIC 	anio,
# MAGIC 	count(member_id) as cont_members
# MAGIC from
# MAGIC 	(
# MAGIC 	select
# MAGIC 		country,
# MAGIC 		state,
# MAGIC 		city,
# MAGIC 		member_id,
# MAGIC 		joined,
# MAGIC 		year(joined) as anio,
# MAGIC 		month(joined) as mes
# MAGIC 	from
# MAGIC 		(
# MAGIC 		select
# MAGIC 			country,
# MAGIC 			state,
# MAGIC 			city,
# MAGIC 			member_id,
# MAGIC 			min(joined) as joined
# MAGIC 		from
# MAGIC 			delta.`/mnt/pruebas-loliveros/bronze/megelon_meetup/members` m
# MAGIC 		group by
# MAGIC 			1,2,3,4
# MAGIC 			) x) y
# MAGIC where state not in ('ca') and city not in ('Chicago Park')
# MAGIC group by 1, 2, 3, 4
# MAGIC order by 1, 2, 3, 4;
# MAGIC 
# MAGIC select * from vw_usuarios_anuales_por_ciudad limit 15;

# COMMAND ----------

df =  spark.sql('select * from vw_usuarios_anuales_por_ciudad')
path_dest = os.path.join(route_silver, 'vw_usuarios_anuales_por_ciudad')
df.write.format('delta').mode('overwrite').save(path_dest)

