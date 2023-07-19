
{{ config(materialized='incremental'
    ,engine='OLAP'
    ,keys=["id"]
    ,table_type='PRIMARY'
    ,distributed_by=['id']
)
    }}


SELECT 1 as id, cast('2023-07-06' as datetime) as dt    ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')   as etl_datetime  -- ETL时间
union all 
SELECT 2 as id, cast('2023-08-06' as datetime) as dt    ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')   as etl_datetime  -- ETL时间
union all 
SELECT 2 as id, cast('2023-08-06' as datetime) as dt    ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')   as etl_datetime  -- ETL时间



