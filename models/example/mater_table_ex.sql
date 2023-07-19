
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table', distributed_by=['id']) }}

with source_data as (

    select 1 as id, '20230706' as dt
    union all
    select 2 as id, '20230707' as dt
    union all
    select 2 as id, '20230708' as dt
    union all
    select 2 as id, '20230706' as dt

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
