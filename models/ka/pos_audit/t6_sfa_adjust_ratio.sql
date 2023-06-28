
-- temp4	SFA已通过-汇总	取temp3审核结果为通过，并按筛选区间(开始结束日期)+门店+调整分类汇总  -- 入参（开始日期+结束日期）
with temp4 as (
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
group by 1,2,3,4
) 

-- select * from temp4 limit 100;

-------------------------------
-- temp6	SFA已通过-调整率计算 & SFA已通过-核算
-------------------------------
-- 计算 SFA已通过-调整率:【SFA已通过-汇总】按门店编码-调整分类维度，匹配【POS-汇总】中POS金额，计算调整率IIf([零售金额之合计] Is Null,0,IIf([零售金额之合计]<0,0,IIf([确认后金额之合计]=0,0,IIf([确认后金额之合计]<[零售金额之合计],1,[零售金额之合计]/[确认后金额之合计]))))
, t001 as (
select 
     temp4.start_dt 
    ,temp4.end_dt
    ,temp4.shop_cd
    ,temp4.thrd_manage_typ
    ,case when coalesce(temp5.prd_amt,0)<=0 then 0 
        when coalesce(temp4.prd_amt,0)=0 then 0 
        when coalesce(temp4.prd_amt,0)<coalesce(temp5.prd_amt) then 1 
        else coalesce(temp5.prd_amt,0)/coalesce(temp4.prd_amt,0) 
        end as tz_rate -- SFA已通过-调整率计算  调整率
from  temp4 
left join {{ ref('t5_pos_summary') }} as temp5 
    on temp4.start_dt = temp5.start_dt 
    and temp4.end_dt = temp5.end_dt 
    and temp4.shop_cd=temp5.shop_cd 
    and temp4.thrd_manage_typ=temp5.thrd_manage_typ
)
-- select * from t001 limit 100;
-- 计算：SFA已通过-核算 ：SFA数据筛【审核结果】为“通过”部分，按门店编码-调整分类匹配【SFA已通过-调整率计算】中调整率，计算核算数量IIf([调整分类] Is Null,Null,[确认后数量]*[调整率])，与核算金额IIf([调整分类] Is Null,Null,[确认后金额]*[调整率])

,temp6 as (
select
     temp3.data_dt
    ,temp3.shop_cd
    ,temp3.emp_cd
    ,temp3.product_cd
    ,temp3.barcode
    ,temp3.thrd_manage_typ
    ,temp3.audit_sts
    ,temp3.prd_cnt
    ,temp3.prd_amt
    ,temp3.submit_typ
    ,temp3.kpi_typ
    ,t001.tz_rate
    ,case when coalesce(temp3.thrd_manage_typ ,'')='' then null else tz_rate*temp3.prd_cnt end  as prd_cnt1  -- SFA已通过-核算 数量
    ,case when coalesce(temp3.thrd_manage_typ ,'')='' then null else tz_rate*temp3.prd_amt end  as prd_amt1  -- SFA已通过-核算 金额
from {{ ref('t3_pos_fix_data') }} as temp3
left join t001 
	on temp3.shop_cd=t001.shop_cd 
	and temp3.thrd_manage_typ=t001.thrd_manage_typ 
    and replace('{{ var("start_dt") }}','-','') = t001.start_dt
    and replace('{{ var("end_dt") }}','-','') = t001.end_dt
where temp3.audit_sts='通过'
    and temp3.data_dt between replace('{{ var("start_dt") }}','-','') and replace('{{ var("end_dt") }}','-','')
)

select * from temp6 

