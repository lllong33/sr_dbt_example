-------------------------------
-- temp8	计算  SFA其余-核算
-------------------------------
/*1、POS-已通过剩余：按门店编码-调整分类-核算金额汇总【SFA已通过-核算】，处理为负数，同【POS-汇总】合并，POS再做门店编码-调整分类-核算金额汇总*/
with t01 as (
-- ①temp6表按照门店、月份、调整分类汇总数据，并将数量金额转为负数
-- ② 合并temp5表并按照门店、月、调整分类汇总数据
select 
    tt.start_dt,tt.end_dt,tt.shop_cd,tt.thrd_manage_typ
    ,sum(prd_cnt1) as prd_cnt 
    ,sum(tt.prd_amt1) as prd_amt
from (
    select  
        replace('{{ var("start_dt") }}','-','')  as start_dt
        ,replace('{{ var("end_dt") }}','-','')  as end_dt
        ,shop_cd
        ,thrd_manage_typ
        ,-abs(sum(prd_cnt1)) as prd_cnt1
        ,-abs(sum(prd_amt1)) as prd_amt1
    from {{ ref('t6_sfa_adjust_ratio') }} as temp6 
    where  temp6.data_dt between replace('{{ var("start_dt") }}','-','') and replace('{{ var("end_dt") }}','-','') 
    group by 
        replace('{{ var("start_dt") }}','-','') 
        ,replace('{{ var("end_dt") }}','-','') 
        ,shop_cd
        ,thrd_manage_typ

    union all 

    select  
        start_dt
        ,end_dt
        ,shop_cd
        ,thrd_manage_typ
        ,prd_cnt as prd_cnt1
        ,prd_amt as prd_amt1
    from {{ ref('t5_pos_summary') }} as temp5
)tt 
group by 1,2,3,4
)

--/*3、SFA其余-汇总：SFA数据筛【审核结果】为“不通过”或为空部分，按门店编码-调整分类-确认后金额进行汇总*/
,t03 as (
select  
    replace('{{ var("start_dt") }}','-','') as start_dt 
    ,replace('{{ var("end_dt") }}','-','') as end_dt 
    ,shop_cd
    ,thrd_manage_typ
    ,sum(prd_cnt) as prd_cnt
    ,sum(prd_amt) as prd_amt
from {{ ref('t3_pos_fix_data') }} as temp3 
where audit_sts='不通过' or coalesce(audit_sts,'')=''
and temp3.data_dt between replace('{{ var("start_dt") }}','-','') and replace('{{ var("end_dt") }}','-','')
group by 
    replace('{{ var("start_dt") }}','-','') 
    ,replace('{{ var("end_dt") }}','-','') 
    ,shop_cd
    ,thrd_manage_typ
)

-- select * from t03 limit 100;

--/*4、SFA其余-调整率计算*/
-- 指标逻辑：【SFA其余-汇总】按门店编码-调整分类维度，匹配【POS-已通过剩余】中POS金额，计算调整率IIf([零售金额之合计] Is Null,0,IIf([零售金额之合计]<0,0,IIf([确认后金额之合计]=0,0,IIf([确认后金额之合计]<[零售金额之合计],1,[零售金额之合计]/[确认后金额之合计]))))
,t04 as (
select  
    t03.start_dt
    ,t03.end_dt
    ,t03.shop_cd
    ,t03.thrd_manage_typ
    ,case when coalesce(t01.prd_amt,0)<=0 then 0 
        when coalesce(t03.prd_amt,0)=0 then 0 
        when coalesce(t03.prd_amt,0)<coalesce(t01.prd_amt,0) then 1 
        else coalesce(t01.prd_amt,0)/coalesce(t03.prd_amt,0) 
        end as tz_rate_qy
from t03 
left join t01 
    on t01.start_dt =t03.start_dt 
    and t01.end_dt=t03.end_dt 
    and t03.thrd_manage_typ=t01.thrd_manage_typ 
    and t01.shop_cd=t03.shop_cd
)

,temp8 as (
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
    ,t04.tz_rate_qy
    ,case when temp3.thrd_manage_typ is null then null else temp3.prd_cnt *tz_rate_qy end as prd_cnt1 -- SFA其余-核算数量
    ,case when temp3.thrd_manage_typ is null then null else temp3.prd_amt *tz_rate_qy end as prd_amt1 -- SFA其余-核算金额
from {{ ref('t3_pos_fix_data') }} as temp3  
left join t04 
    on temp3.shop_cd=t04.shop_cd 
    and temp3.thrd_manage_typ=t04.thrd_manage_typ 
    and t04.start_dt= replace('{{ var("start_dt") }}','-','') and t04.end_dt=replace('{{ var("end_dt") }}','-','')
where audit_sts='不通过' or coalesce(audit_sts,'')=''
    and temp3.data_dt between replace('{{ var("start_dt") }}','-','') and replace('{{ var("end_dt") }}','-','')
)

select * from temp8



