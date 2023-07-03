-- temp1
-- 团购提报、零售提报区分开
{{ config(pre_hook="set new_planner_optimize_timeout=60000") }}

with aa as (
select  
    t1.data_dt
    ,t1.shop_cd 
    ,t1.emp_cd 
    ,t7.emp_nam 
    ,t1.product_cd
    ,t3.barcode --国际条码
    ,t3.thrd_manage_typ
    ,coalesce(t1.correct_zcls_cnt,zcls_sbmt_cnt) as qrh_cnt
    ,coalesce(t1.correct_zcls_amt,zcls_sbmt_amt) as qrh_amt
    ,'零售提报' as submit_typ  
from s_ka_ss_shop_emp_prod_d_mild t1 
left join dim_ka_pub_emp_unsens t7 on t1.emp_cd=t7.emp_cd -- 获取人员姓名
left join dim_ka_pub_product_master_data t3 on t1.product_cd=t3.product_cd -- 获取69码
where coalesce(t1.zcls_sbmt_cnt,0)>0

union all 

select  
    t1.data_dt
    ,t1.shop_cd 
    ,t1.emp_cd 
    ,t7.emp_nam 
    ,t1.product_cd
    ,t3.barcode --国际条码
    ,t3.thrd_manage_typ
    ,coalesce(t1.correct_cxtg_cnt,cxtg_sbmt_cnt) as qrh_cnt
    ,coalesce(t1.correct_cxtg_cnt,cxtg_sbmt_amt) as qrh_amt
    ,'团购提报' as submit_typ  
from s_ka_ss_shop_emp_prod_d_mild t1 
left join dim_ka_pub_emp_unsens t7 on t1.emp_cd=t7.emp_cd -- 获取人员姓名
left join dim_ka_pub_product_master_data t3 on t1.product_cd=t3.product_cd -- 获取69码
where coalesce(t1.cxtg_sbmt_cnt,0)>0
) 

-- 按照日期+门店维度，同一日期下，(if(t4 is not null , t4, aa))若aa的门店存在t4表，则aa表该日期该门店的数据替换为t4表的数据，否则保留aa表的数据
,temp1 as (
select 
    aa.data_dt
    ,aa.shop_cd
    ,aa.emp_cd
    ,aa.emp_nam 
    ,aa.product_cd
    ,aa.barcode
    ,aa.thrd_manage_typ
    ,aa.qrh_cnt as prd_cnt
    ,aa.qrh_amt as prd_amt
    ,aa.submit_typ
from aa 
where not exists (select * from dim_ka_ss_sbmt_sales_fix t4 where aa.data_dt =substr(t4.data_dt, 1, 8) and t4.shop_cd=regexp_replace(aa.shop_cd ,'^[0]+','0'))
-- Currently only subquery of the Select type are supported
-- 不支持not exists + union all 的方式,  改写为 join 方式
-- 不能写 not (1=1) 方式, 会有三值的null问题
-- left join dim_ka_ss_sbmt_sales_fix t4 on aa.data_dt = substr(t4.data_dt, 1, 8) and aa.shop_cd=regexp_replace(t4.shop_cd ,'^[0]+','0')
-- where substr(t4.data_dt, 1, 8) is null

union all 

select 
     substr(t4.data_dt, 1, 8) as data_dt
    ,lpad(t4.shop_cd,10,'0') as shop_cd -- 不足10位补10位
    ,t4.emp_cd
    ,t4.emp_nam
    ,t4.product_cd
    ,t3.barcode
    ,t3.thrd_manage_typ
    ,t4.sales_qty as qrh_cnt
    ,t4.sales_amt as qrh_amt 
    ,'零售提报' as  submit_typ
from dim_ka_ss_sbmt_sales_fix t4 
left join dim_ka_pub_product_master_data t3 on t4.product_cd=t3.product_cd 
where exists (select * from aa where aa.data_dt =substr(t4.data_dt, 1, 8) and t4.shop_cd=regexp_replace(aa.shop_cd ,'^[0]+','0'))
)

-- temp2
, temp2 as (
select  
    t2.data_date as data_dt 
    ,lpad(t2.shipper_code,10,'0') as shop_cd
    ,t2.created_by as emp_cd 
    ,t2.created_name as emp_nam 
    ,case when t2.state='无效团购' then '无效' else t2.audit_state end as audit_sts -- 审核结果
    ,t2.product_id  as product_cd
    ,t2.amount as prd_cnt -- 数量
    ,case when t2.is_group_buy ='是' then '团购提报' else '零售提报' end as submit_typ  -- 提报类型
from res_ka_cxx_group_by_audit t2
where not exists (select * from dim_ka_ss_sbmt_sales_fix t4 where t2.data_date =substr(t4.data_dt, 1, 8) and t2.shipper_code=t4.shop_cd)
)

-- select * from temp2 limit 100 ;
-- temp3
, temp3 as (
select
    a1.data_dt
    ,a1.shop_cd
    ,a1.emp_cd
    ,a1.product_cd
    ,a1.barcode
    ,a1.thrd_manage_typ
    ,a2.audit_sts
    ,a1.prd_cnt
    ,a1.prd_amt
    ,a1.submit_typ
    ,case when a1.submit_typ='团购提报' and a2.product_cd is not null  then '促销团购提报' 
        when a1.submit_typ='零售提报' and a2.product_cd is not null then '大数提报' 
        else '正常活动业绩' end as kpi_typ
from temp1 a1 
left join temp2 a2 
    on a1.data_dt=a2.data_dt 
    and a1.shop_cd=a2.shop_cd 
    and a1.emp_cd=a2.emp_cd 
    and a1.product_cd=a2.product_cd 
    and a1.submit_typ=a2.submit_typ 
    and a1.prd_cnt=a2.prd_cnt
)

select * from temp3 
