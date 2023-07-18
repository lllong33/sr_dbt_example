
{{ config(materialized='incremental'
    ,engine='OLAP'
    ,distributed_by=['id']
    ,partition_by= ['dt']
    ,partition_by_init= ["PARTITION p1 VALUES [('1971-01-01 00:00:00'), ('1991-01-01 00:00:00')),PARTITION p1972 VALUES [('1991-01-01 00:00:00'), ('1999-01-01 00:00:00')), PARTITION p2099 VALUES [('1999-01-01 00:00:00'), ('2099-01-01 00:00:00'))"]
    ) 
    }}

SELECT 1 as id, cast('2023-07-06' as datetime) as dt    ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')   as etl_datetime  -- ETL时间
union all 
SELECT 2 as id, cast('2023-08-06' as datetime) as dt    ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')   as etl_datetime  -- ETL时间
union all 
SELECT 2 as id, cast('2023-08-06' as datetime) as dt    ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')   as etl_datetime  -- ETL时间



