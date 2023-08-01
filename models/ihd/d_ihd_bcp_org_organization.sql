

with inc as  (
select 
    distinct t.this_day as stat_dt
from dim_day  t
where t.this_day >= '20230506'
    and t.this_day <= DATE_FORMAT(date_trunc('day', days_sub(NOW(), 45)), '%Y%m%d')
    -- and t.this_day <= DATE_FORMAT(date_trunc('day', days_sub(NOW(), -45)), '%Y%m%d')
)

select 
    inc.stat_dt
    ,a.*    
    ,a.org_code as org_cd
from o__hr_mhc__bcp_org_organization a 
join inc 
    on inc.stat_dt between replace(substr(a.start_date, 1, 10), '-', '') and replace(substr(a.end_date, 1, 10), '-', '')
where a.valid_flag = '0'    



