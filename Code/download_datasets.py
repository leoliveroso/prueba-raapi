# Databricks notebook source
import os

# Define env variable 
os.environ['KAGGLE_USERNAME'] = "leoliveroso"
os.environ['KAGGLE_KEY'] = dbutils.secrets.get(scope = "prueba-rappi", key = "kaggle-token")

from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd 
import json


dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

# DBTITLE 1,Constants define
dbutils.widgets.text("moun_name", "pruebas-loliveros")
dbutils.widgets.text("path_dest", "raw/kaggle")
dbutils.widgets.text("url_origen", "megelon/meetup")

moun_name = dbutils.widgets.get("moun_name")
path_dest = dbutils.widgets.get("path_dest")
url_origen = dbutils.widgets.get("url_origen")

path_mount = f"/dbfs/mnt/{moun_name}"
route = os.path.join(path_mount, path_dest, url_origen.replace("/", "_"))
print(route)

#Create folder source if not exist
if not os.path.exists(route):
    print(f"no existe la ruta, por ende se crea {route}")
    os.mkdir(route)

NOTEBOOKNAME = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
JOB_ID = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())['tags']['opId']


# COMMAND ----------

def download_data_from_kaggle(dataset_name, path_destino):
    response = {
        "status": False
    }
    try:
        #define client
        ClientKaggle = KaggleApi()
        #Auth client with env variables 
        ClientKaggle.authenticate()
        #get files from kaggle to bucket gcp
        ClientKaggle.dataset_download_files(dataset_name, path_destino, unzip= True)
        
        list_files_create = []
        for filename in os.listdir(path_destino):
            path_file = os.path.join(path_destino, filename)
            #print(path_file)
            list_files_create.append(path_file)
        #Create message successed 
        str_list_files_create = (', \n ').join(list_files_create)
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

# DBTITLE 1,Results process
response = download_data_from_kaggle(url_origen, route) 
response["notebook"] = NOTEBOOKNAME
response["job_id"] = JOB_ID
#print(response)
dbutils.notebook.exit(json.dumps(response))

# COMMAND ----------


