USE DATABASE SILVER;use schema megelon_meetup;

CREATE OR REPLACE EXTERNAL TABLE members_usuarios_ciudad_aux3(
    city VARCHAR as (value:city::varchar),
    anio INT as (value:anio::int),
    count_member INT as (value:count_member::int)
)
WITH LOCATION = @gcs_stage_silver/megelon_meetup/members_usuarios_ciudad_aux3
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*[/]part-[^/]*[.]parquet'
AUTO_REFRESH = false;


CREATE OR REPLACE EXTERNAL TABLE members_aux3_filtros(
    city VARCHAR as (value:city::varchar),
    anio INT as (value:anio::int),
    count_member INT as (value:count_member::int)
)
WITH LOCATION = @gcs_stage_silver/megelon_meetup/members_aux3_filtros
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*[/]part-[^/]*[.]parquet'
AUTO_REFRESH = false;


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
AUTO_REFRESH = false;