
-------------------------------
-- temp7	SFA无效-核算
-------------------------------
-- /*1、SFA无效-核算：SFA数据筛【审核结果】为“无效”部分，核算数量、核算金额均为0*/
with  temp7 as (
select  
    data_dt
    ,shop_cd
    ,emp_cd
    ,product_cd
    ,prd_cnt
    ,prd_amt
    ,barcode
    ,audit_sts
    ,0 as prd_cnt1
    ,0 as prd_amt1
    ,submit_typ
    ,kpi_typ
    ,thrd_manage_typ
from {{ ref('t3_pos_fix_data') }} as temp3 
where audit_sts='无效'
    and temp3.data_dt between replace('{{ var("start_dt") }}','-','') and replace('{{ var("end_dt") }}','-','')
)
-- select * from temp7 limit 100; 


-------------------------------
-- temp9	POS剩余-调整率计算
-------------------------------
/*1、SFA总核算：合并【SFA已通过-核算】、【SFA无效-核算】、【SFA其余-核算】*/
, a01 AS (
SELECT 
    start_dt
    ,end_dt
    ,shop_cd
    ,thrd_manage_typ
    ,sum(tt.prd_cnt1) AS prd_cnt 
    ,sum(tt.prd_amt1) AS prd_amt 
FROM (
    SELECT 
        replace('{{ var("start_dt") }}','-','') as start_dt 
        ,replace('{{ var("end_dt") }}','-','') as end_dt
        ,shop_cd
        ,thrd_manage_typ
        ,prd_cnt1
        ,prd_amt1
    from {{ ref('t6_sfa_adjust_ratio') }} as temp6
    where data_dt between replace('{{ var("start_dt") }}','-','') and replace('{{ var("end_dt") }}','-','')
    
    UNION ALL 
    
    SELECT 
        replace('{{ var("start_dt") }}','-','') as start_dt 
        ,replace('{{ var("end_dt") }}','-','') as end_dt
        ,shop_cd
        ,thrd_manage_typ
        ,prd_cnt1
        ,prd_amt1
    FROM  temp7 
    where data_dt between replace('{{ var("start_dt") }}','-','') and replace('{{ var("end_dt") }}','-','')

    union all 

    SELECT 
        replace('{{ var("start_dt") }}','-','') as start_dt 
        ,replace('{{ var("end_dt") }}','-','') as end_dt
        ,shop_cd
        ,thrd_manage_typ
        ,prd_cnt1
        ,prd_amt1
    FROM {{ ref('t8_sfa_residue_audit') }} temp8
    where data_dt between replace('{{ var("start_dt") }}','-','') and replace('{{ var("end_dt") }}','-','')
)tt 
GROUP BY 1,2,3,4
)
/*2:POS剩余-调整率计算*/
SELECT  
    a01.start_dt 
    ,a01.end_dt
    ,a01.shop_cd
    ,a01.thrd_manage_typ
    ,CASE WHEN COALESCE(a01.prd_amt,0)=0 THEN 1 ELSE 1-a01.prd_amt/temp5.prd_amt END AS tz_rate -- POS剩余-调整率
FROM a01 
LEFT JOIN {{ ref('t5_pos_summary') }}  as temp5 
    ON a01.start_dt =temp5.start_dt 
    and a01.end_dt=temp5.end_dt 
    AND a01.shop_cd=temp5.shop_cd 
    AND a01.thrd_manage_typ=temp5.thrd_manage_typ
