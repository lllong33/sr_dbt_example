
-- Use the `ref` function to select from other models

select *
from {{ ref('mater_table_ex') }}
where id = 1
