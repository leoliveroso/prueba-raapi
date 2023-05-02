from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator, DatabricksSubmitRunOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow import DAG

from datetime import datetime, timedelta
import csv
import requests
import json
import pytz

def convert_datetime(datetime_string):

    return datetime_string.astimezone(pytz.timezone('America/Bogota')).strftime('%b-%d %H:%M:%S')


def _get_message() -> str:
    return "Se ha actualizado la db"

##### Slack Alerts #####
SLACK_CONN_ID = 'slack_conn_id'
def slack_fail_alert(context):
    '''Adapted from https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105
         Sends message to a slack channel.
            If you want to send it to a "user" -> use "@user",
                if "public channel" -> use "#channel",
                if "private channel" -> use "channel"
    '''
    
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    channel = BaseHook.get_connection(SLACK_CONN_ID).login
    slack_msg = f"""
        :x: Task Failed.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {convert_datetime(context.get('execution_date'))}
        <{context.get('task_instance').log_url}|*Logs*>
    """

    slack_alert = SlackWebhookOperator(
        task_id='slack_fail',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username='airflow',
        http_conn_id=SLACK_CONN_ID
    )

    return slack_alert.execute(context=context)

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "leoliverosoy@gmail.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=1), 
    'on_failure_callback': slack_fail_alert
}

json_conf_databricks = {
    "existing_cluster_id" : "1220-162320-5cq3djy9"
}



querys = {
    "create_db_silver": "CREATE DATABASE IF NOT EXISTS SILVER",
    "create_schema_meetup": "; ".join([
        'USE DATABASE SILVER', 
        'CREATE SCHEMA IF NOT EXISTS megelon_meetup']),
    "create_storage_integration": "; ".join([
        'USE DATABASE SILVER',
        """create storage integration if not exists gcs_silver 
        type = external_stage storage_provider = 'GCS'
        enabled = true storage_allowed_locations = ('gcs://pruebas-loliveros/silver')"""]),
    "create_stage_silver": "; ".join([
        'USE DATABASE SILVER',
        "use schema megelon_meetup",
        """create stage if not exists gcs_stage_silver
        url = 'gcs://pruebas-loliveros/silver'
        storage_integration = gcs_silver
        file_format = (TYPE = PARQUET)"""]),
    "create_externals_tables": "; ".join([
        "USE DATABASE SILVER",
        "use schema megelon_meetup",
        """
        CREATE OR REPLACE EXTERNAL TABLE members_usuarios_ciudad_aux3(
            city VARCHAR as (value:city::varchar),
            anio INT as (value:anio::int),
            count_member INT as (value:count_member::int)
        )
        WITH LOCATION = @gcs_stage_silver/megelon_meetup/members_usuarios_ciudad_aux3
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*[/]part-[^/]*[.]parquet'
        AUTO_REFRESH = false
        """,
        """
        CREATE OR REPLACE EXTERNAL TABLE members_aux3_filtros(
            city VARCHAR as (value:city::varchar),
            anio INT as (value:anio::int),
            count_member INT as (value:count_member::int)
        )
        WITH LOCATION = @gcs_stage_silver/megelon_meetup/members_aux3_filtros
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*[/]part-[^/]*[.]parquet'
        AUTO_REFRESH = false
        """,
        """
        CREATE OR REPLACE EXTERNAL TABLE usuarios_anuales_por_ciudad(
            country VARCHAR as (value:country::varchar),
            state VARCHAR as (value:state::varchar),
            city VARCHAR as (value:city::varchar),
            anio INT as (value:anio::int),
            cont_members INT as (value:count_member::int)
        )
        WITH LOCATION = @gcs_stage_silver/megelon_meetup/usuarios_anuales_por_ciudad
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*[/]part-[^/]*[.]parquet'
        AUTO_REFRESH = false
        """
    ])
}


with DAG("meetup_pipeline", start_date=datetime(2022, 12 ,21), 
    schedule_interval="*/15 * * * *", default_args=default_args, catchup=False) as dag:

    create_db_silver = SnowflakeOperator(
        task_id = "create_db_silver",
        sql = querys["create_db_silver"],
        snowflake_conn_id="snowflake_conn"
    )

    create_schema_meetup = SnowflakeOperator(
        task_id = "create_schema_meetup",
        sql = querys["create_schema_meetup"],
        split_statements=True,
        snowflake_conn_id="snowflake_conn"
    )

    create_storage_integration_silver = SnowflakeOperator(
        task_id = "create_storage_integration_silver",
        sql = querys["create_storage_integration"],
        split_statements=True,
        snowflake_conn_id="snowflake_conn"
    )

    download_datasets_kaggle = DatabricksSubmitRunOperator(
        task_id = 'download_datasets_kaggle',
        databricks_conn_id = 'databricks_default',
        notebook_task= {
            "notebook_path": "/Repos/leoliverosoy@gmail.com/prueba-raapi/Code/download_datasets",
            'base_parameters': {
                'moun_name': 'pruebas-loliveros',
                'path_dest': 'raw/kaggle',
                'url_origen': 'megelon/meetup'
            }
        },
        json = json_conf_databricks
    )
  
    migrate_raw2bronze = DatabricksSubmitRunOperator(
        task_id = 'migrate_raw2bronze',
        databricks_conn_id = 'databricks_default',
        notebook_task= {
            "notebook_path": "/Repos/leoliverosoy@gmail.com/prueba-raapi/Code/migrate_raw2bronze",
            'base_parameters': {
                'route_raw': '/mnt/pruebas-loliveros/raw/kaggle/megelon_meetup',
                'route_bronze': '/mnt/pruebas-loliveros/bronze/megelon_meetup',
                'format': 'csv'
            }
        },
        json = json_conf_databricks
    )

    transform_silver = DatabricksSubmitRunOperator(
        task_id = 'transform_silver',
        databricks_conn_id = 'databricks_default',
        notebook_task= {
            "notebook_path": "/Repos/leoliverosoy@gmail.com/prueba-raapi/Code/transform_silver",
            'base_parameters': {
                'route_bronze': '/mnt/pruebas-loliveros/bronze/megelon_meetup',
                'route_silver': '/mnt/pruebas-loliveros/silver/megelon_meetup'
            }
        },
        json = json_conf_databricks
    )

    create_stage_silver = SnowflakeOperator(
        task_id = "create_stage_silver",
        sql = querys["create_stage_silver"],
        split_statements=True,
        snowflake_conn_id="snowflake_conn"
    )

    create_externals_tables = SnowflakeOperator(
        task_id = "create_externals_tables",
        sql = querys["create_externals_tables"],
        split_statements=True,
        snowflake_conn_id="snowflake_conn"
    )

    # send_email_notification = EmailOperator(
    #     task_id="send_email_notification",
    #     to="leoliverosoy@gmail.com",
    #     subject="Prueba_Rappi_data_pipeline",
    #     html_content="<h3>Se ha actualizado la base de datos correctamente</h3>"
    # )

    send_slack_notification = SlackWebhookOperator(
        task_id="send_slack_notification",
        http_conn_id=SLACK_CONN_ID,
        message=_get_message(),
        channel="#monitoring"
    )

    create_db_silver >> create_schema_meetup >> create_storage_integration_silver >> download_datasets_kaggle
    download_datasets_kaggle >> migrate_raw2bronze >> transform_silver >> create_stage_silver >> create_externals_tables
    create_externals_tables >> send_slack_notification
