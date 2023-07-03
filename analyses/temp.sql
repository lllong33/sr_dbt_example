select 
     replace('{{ var("start_dt") }}','-','') as start_dt 
    ,replace('{{ var("end_dt") }}','-','') as end_dt
    ,shop_cd
    ,t3.thrd_manage_typ 
    ,sum(prd_cnt) as prd_cnt
    ,sum(prd_amt) as prd_amt
from  {{ ref('t3_pos_fix_data') }} as temp3 
left join dim_ka_pub_product_master_data t3 on t3.product_cd=temp3.product_cd
where audit_sts='通过' 
    and data_dt between replace('{{ var("start_dt") }}','-','') and replace('{{ var("end_dt") }}','-','')
group by 
     replace('{{ var("start_dt") }}','-','') 
    ,replace('{{ var("end_dt") }}','-','') 
    ,shop_cd
    ,t3.thrd_manage_typ



