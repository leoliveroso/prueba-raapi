# Databricks notebook source
# DBTITLE 1,Import libs
import json
import os

spark.conf.set("spark.sql.shuffle.partitions",4)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import net.snowflake.spark.snowflake.Utils
# MAGIC 
# MAGIC 
# MAGIC def libs_send_querys(port : Int = 443, verb : Boolean = false) : String = {
# MAGIC   /*Function that initializes the connection with a sql server in DW
# MAGIC 
# MAGIC   Parameters
# MAGIC   ----------
# MAGIC   json_connect : dict 
# MAGIC     Dictionary result of loading a json with the connection information. 
# MAGIC     The content must have  all the necessary information
# MAGIC   port: int
# MAGIC     Connection port
# MAGIC   Returns
# MAGIC   -------
# MAGIC   In tuple:
# MAGIC   
# MAGIC   cnxn.cursor() : pyodbc.connect.cursor
# MAGIC     Returns a new Cursor object using the connectiona database.
# MAGIC   url : string
# MAGIC     url for jdbc connection   
# MAGIC 
# MAGIC   */
# MAGIC   try {
# MAGIC     
# MAGIC     val json_connect = table("json_connect_table")
# MAGIC     val sfUrl       = json_connect.take(1)(0).getAs[String]("sfUrl")
# MAGIC     val sfUser = json_connect.take(1)(0).getAs[String]("sfUser")
# MAGIC     val sfPassword = json_connect.take(1)(0).getAs[String]("sfPassword")
# MAGIC     val sfDatabase     = json_connect.take(1)(0).getAs[String]("sfDatabase")
# MAGIC     val sfSchema         = json_connect.take(1)(0).getAs[String]("sfSchema")
# MAGIC     val sfWarehouse     = json_connect.take(1)(0).getAs[String]("sfWarehouse")
# MAGIC     
# MAGIC     // Create the JDBC URL without passing in the user and password parameters.
# MAGIC     val options = Map(
# MAGIC       "sfUrl" -> sfUrl,
# MAGIC       "sfUser" -> sfUser,
# MAGIC       "sfPassword" -> sfPassword,
# MAGIC       "sfDatabase" -> sfDatabase,
# MAGIC       "sfSchema" -> sfSchema,
# MAGIC       "sfWarehouse" -> sfWarehouse
# MAGIC       )
# MAGIC     
# MAGIC     // Create table for various querys
# MAGIC     val querys = table("querys_table").select("value").collect.map(row=>row.getString(0))
# MAGIC     
# MAGIC     for( (query,i)  <- querys.zipWithIndex) {
# MAGIC       //print(query)
# MAGIC       //stmt.execute(query)   
# MAGIC       Utils.runQuery(options, query)
# MAGIC       if (verb) printf("Query #%d complete!\n",i+1);
# MAGIC     }
# MAGIC     "OK"
# MAGIC   } catch{
# MAGIC     case e: Exception => println("exception caught: " + e);
# MAGIC     "Problem conecction" + e
# MAGIC   }  
# MAGIC }

# COMMAND ----------

def libs_query_for_scala(list_querys,json_connect):
    rdd = sc.parallelize([json_connect])
    spark.read.json(rdd).createOrReplaceTempView("json_connect_table")
    spark.createDataFrame(list_querys, sql_types.StringType()).createOrReplaceTempView("querys_table")

# COMMAND ----------

# DBTITLE 1,Parameters
route_raw = '/mnt/pruebas-loliveros/raw/kaggle/megelon_meetup'
route_bronze = '/mnt/pruebas-loliveros/bronze/megelon_meetup'
format = 'csv'

# Use dbutils secrets to get Snowflake credentials.
user = dbutils.secrets.get("prueba-rappi", "snowflake-user")
password = dbutils.secrets.get("prueba-rappi", "snowflake-password")
 
options = {
  "sfUrl": "mu10071.us-central1.gcp.snowflakecomputing.com",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "pruebas_loliveros",
  "sfSchema": "bronze",
  "sfWarehouse": "COMPUTE_WH"
}

# COMMAND ----------

# DBTITLE 1,Define Functions
def migrate_files_raw2bronze(path_raw = None, path_bronze = None, format = 'csv', config_db = None):
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
            if config_db != None and type(config_db) is dict: 
                df.write.format("snowflake").options(**config_db).mode('overwrite').option("dbtable", filename_dest).save()
            

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

response = migrate_files_raw2bronze(path_raw = route_raw, path_bronze = route_bronze, config_db = options)
print(response)

# COMMAND ----------


