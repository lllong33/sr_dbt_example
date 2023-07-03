/********************************************************************************************************
数据库表: res_ka_pos_leave_hs
内容描述: POS-总核算剩余表
主键：start_dt,end_dt,shop_cd,thrd_manage_typ
---------------------------------------------------------------------------------------------------------
上线版本     上线日期              修改人    修改内容                  
v1.0         2023-05-31           lf        新增

文档地址: 
---------------------------------------------------------------------------------------------------------
********************************************************************************************************/
/*依赖表
select 's_ka_ss_shop_emp_prod_d_mild' flag, max(etl_datetime )from s_ka_ss_shop_emp_prod_d_mild union all 	    -- 现代渠道门店促销商品日轻度汇总	是	day_shop_prd_cnt_amt
select 'res_ka_cxx_group_by_audit' flag, max(etl_datetime )from res_ka_cxx_group_by_audit union all 	        -- 促销线团购审核明细表	否	t2
select 'dim_ka_pub_product_master_data' flag, max(etl_datetime )from dim_ka_pub_product_master_data union all  -- 现代渠道商品主数据表	否	t3
select 'dim_ka_ss_sbmt_sales_fix' flag, max(etl_datetime )from dim_ka_ss_sbmt_sales_fix union all 	            -- 提报销量修正	否	t4
select 'dim_ka_ss_pos_elim' flag, max(etl_datetime )from dim_ka_ss_pos_elim union all 	                        -- POS剔除	否	t5
select 'dim_ka_pub_emp_unsens' flag, max(etl_datetime )from dim_ka_pub_emp_unsens union all 	                -- 员工基本信息维表（非敏感字段）	否	t6
select 'fact_ka_sales_inven_itv_dtl' flag, max(etl_datetime )from fact_ka_sales_inven_itv_dtl union all 	    -- KA零售-POS机业务导入（段明细）	否	t7
select 'fact_ka_sales_pos_sum' flag, max(etl_datetime )from fact_ka_sales_pos_sum union all 	                -- KA零售-POS汇总（先导入后爬虫）	否	t71
select 'dim_ka_ss_zz_version' flag, max(etl_datetime )from dim_ka_ss_zz_version union all 	                    -- 至尊版本变更表	否	t8
select 'dim_ka_ss_sbmt_sales_elim' flag, max(etl_datetime )from dim_ka_ss_sbmt_sales_elim 	                    -- sfa剔除表	否	t9
*/


-------------------------------
-- 目标表1	计算 POS-总核算剩余	res_ka_pos_leave_hs	POS-总核算剩余表
-------------------------------
-- POS剔除数量金额处理为负数，与原始POS合并，按门店编码-调整分类匹配【POS剩余-调整率计算】中调整率，
-- 计算剩余POS数量IIf([调整分类] Is Null,Null,[零售数量]*[调整率])，与剩余POS金额IIf([调整分类] Is Null,Null,[零售金额]*[调整率])

with day_shop_prd_cnt_amt as (
-- 数据工作台 + POS导入 + POS剔除
-- 期间pos开始结束日期需包含于参数的开始结束日期
select  
     replace('{{ var("start_dt") }}','-','') as start_dt 
    ,replace('{{ var("end_dt") }}','-','') as end_dt
    ,store_code as shop_cd 
    ,t7.prd_code as product_cd
    ,sum(t7.prd_qty) as prd_cnt
    ,sum(t7.prd_amt) as prd_amt 
from fact_ka_sales_inven_itv_dtl t7
where t7.start_date>=replace('{{ var("start_dt") }}','-','') and t7.end_date<= replace('{{ var("end_dt") }}','-','')
    and t7.prd_qty<>0 
    and t7.store_code not in ('00000000仓库','0000000未新增','0000000便利店','未新增','仓库')
    and t7.check_status='7' -- 取审核通过
group by 1,2,3,4

union all 

select 
    replace('{{ var("start_dt") }}','-','') as start_dt 
    ,replace('{{ var("end_dt") }}','-','') as end_dt
    ,t71.store_code as shop_cd
    ,t71.prd_code as product_cd
    ,sum(t71.prd_qty) as prd_cnt
    ,sum(t71.prd_amt) as prd_amt
from fact_ka_sales_pos_sum t71 
where t71.data_date between replace('{{ var("start_dt") }}','-','') and replace('{{ var("end_dt") }}','-','')
    and t71.prd_qty<>0 and t71.store_code not in ('0000000未新增','未新增')
    and t71.data_source = 'POS导入' -- 门店POS日明细取POS导入
    and t71.check_status='7' -- 取审核通过
group by 1,2,3,4

union all 

select  
    replace('{{ var("start_dt") }}','-','') as start_dt 
    ,replace('{{ var("end_dt") }}','-','') as end_dt
    ,lpad(t5.shop_cd,10,'0') as shop_cd 
    ,t5.product_cd
    ,sum(-abs(t5.elim_qty)) as prd_cnt 
    ,sum(-abs(t5.elim_amt)) as prd_amt 
from dim_ka_ss_pos_elim t5 
where t5.start_dt>=replace('{{ var("start_dt") }}','-','') and t5.end_dt<= replace('{{ var("end_dt") }}','-','')
group by 1,2,3,4
) 

, day_shop_prd_thrd_cnt_amt as (
select 
     t1.start_dt 
    ,t1.end_dt
    ,t1.shop_cd 
    ,t3.thrd_manage_typ -- 部分品没有三级分类, 即''
    ,t1.product_cd
    ,sum(prd_cnt) as prd_cnt
    ,sum(prd_amt) as prd_amt
from day_shop_prd_cnt_amt as t1
left join dim_ka_pub_product_master_data t3 on t1.product_cd=t3.product_cd
group by 1,2,3,4,5
)

,adjust_metric as ( -- 调整指标值 
select 
     hs.start_dt 
    ,hs.end_dt
    ,hs.shop_cd 
    ,hs.thrd_manage_typ
    ,hs.product_cd
    ,hs.prd_cnt as prd_cnt_raw
    ,hs.prd_amt as prd_amt_raw
    ,case when coalesce(hs.thrd_manage_typ,'')='' then null else hs.prd_cnt*coalesce(t9.tz_rate, 1) end as prd_cnt -- POS-总核算剩余数量
    ,case when coalesce(hs.thrd_manage_typ,'')='' then null else hs.prd_amt*coalesce(t9.tz_rate, 1) end as prd_amt -- POS-总核算剩余金额
from day_shop_prd_thrd_cnt_amt as hs 
left join {{ ref('t9_sfa_residue_audit_adjust_ratio') }} as t9 
    on hs.shop_cd=t9.shop_cd 
    and hs.thrd_manage_typ=t9.thrd_manage_typ 
    and hs.start_dt=t9.start_dt
    and hs.end_dt=t9.end_dt
)

select * from adjust_metric
-- where shop_cd='0005000049'




