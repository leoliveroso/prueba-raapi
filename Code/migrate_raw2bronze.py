# Databricks notebook source
# DBTITLE 1,Import libs
import json
import os

spark.conf.set("spark.sql.shuffle.partitions",100)

# COMMAND ----------

# DBTITLE 1,Parameters
route_raw = '/mnt/pruebas-loliveros/raw/kaggle/megelon_meetup'
route_bronze = '/mnt/pruebas-loliveros/bronze/megelon_meetup'
format = 'csv'


# COMMAND ----------

# DBTITLE 1,Define Functions
def migrate_files_raw2bronze(path_raw = None, path_bronze = None, format = 'csv'):
    response = {
        "status": False
    }

    #Get file from path_raw
    list_files = []
    try:
        for filename in os.listdir('/dbfs' + path_raw):
            path_file = os.path.join(path_raw, filename)
            #print(path_file)
            df = None
            
            #Select method
            if format == 'csv':
                df = spark.read.format('csv').options(header='True', inferSchema='True', delimiter=',').load(path_file)
            elif format == 'json':
                df = spark.read.format("json").load(path_file)
            filename_dest = filename.split(".")
            filename_dest = filename_dest[0]
            path_dest = os.path.join(path_bronze, filename_dest)
            list_files.append(path_dest)
            df.write.format('delta').mode('overwrite').save(path_dest)

        #Create message successed 
        str_list_files_create = (', \n ').join(list_files)
        msg = f"Se han cargado Sactisfactoria mente los archivos: {str_list_files_create}"
        #Create response 
        response['status'] = True 
        response['message'] = msg

    except Exception as e:
        msg = F"Se ha presetando el siguiente error: \n {str(e)}"
        #print(msg)
        response['message'] = msg

    return response 

# COMMAND ----------

response = migrate_files_raw2bronze(path_raw = route_raw, path_bronze = route_bronze)
print(response)

# COMMAND ----------


