USE DATABASE SILVER;
use schema megelon_meetup;

create stage if not exists gcs_stage_silver
    url = 'gcs://pruebas-loliveros/silver'
    storage_integration = gcs_silver
    file_format = (TYPE = PARQUET)