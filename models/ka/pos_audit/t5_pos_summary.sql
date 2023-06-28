-- 按照开始结束日期汇总pos:数据来源期间pos\pos日明细\pos剔除
select 
     tt.start_dt 
    ,tt.end_dt
    ,tt.shop_cd 
    ,t3.thrd_manage_typ 
    ,sum(prd_cnt) as prd_cnt
    ,sum(prd_amt) as prd_amt
from (
    -- 期间pos开始结束日期需包含于参数的开始结束日期
    select  
        replace('{{ var("start_dt") }}','-','') as start_dt 
        ,replace('{{ var("end_dt") }}','-','') as end_dt
        ,store_code as shop_cd 
        ,t7.prd_code as product_cd
        ,sum(t7.prd_qty) as prd_cnt
        ,sum(t7.prd_amt) as prd_amt 
	from fact_ka_sales_inven_itv_dtl t7
    where t7.start_date >= replace('{{ var("start_dt") }}','-','') and t7.end_date<= replace('{{ var("end_dt") }}','-','')
        and t7.prd_qty<>0 and t7.store_code not in ('00000000仓库','0000000未新增','0000000便利店','未新增','仓库')
        and t7.check_status='7' -- 取审核通过
    group by 
        replace('{{ var("start_dt") }}','-','')
        ,replace('{{ var("end_dt") }}','-','') 
        ,store_code 
        ,t7.prd_code 

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
    group by 
	    replace('{{ var("start_dt") }}','-','') 
	    ,replace('{{ var("end_dt") }}','-','') 
	    ,t71.store_code 
	    ,t71.prd_code

    union all 

    select  
        replace('{{ var("start_dt") }}','-','') as start_dt 
        ,replace('{{ var("end_dt") }}','-','') as end_dt
        ,lpad(t5.shop_cd,10,'0') as shop_cd 
        ,t5.product_cd
        ,sum(-abs(t5.elim_qty)) as prd_cnt 
        ,sum(-abs(t5.elim_amt)) as prd_amt 
--         ,null as check_status
--         ,null as data_source
    from dim_ka_ss_pos_elim t5 
    where t5.start_dt>=replace('{{ var("start_dt") }}','-','') and t5.end_dt<= replace('{{ var("end_dt") }}','-','')
    group by replace('{{ var("start_dt") }}','-','') 
        ,replace('{{ var("end_dt") }}','-','')
        ,lpad(t5.shop_cd,10,'0') 
        ,t5.product_cd
) tt 
left join dim_ka_pub_product_master_data t3 on tt.product_cd=t3.product_cd
group by 
    tt.start_dt 
    ,tt.end_dt
    ,tt.shop_cd 
    ,t3.thrd_manage_typ