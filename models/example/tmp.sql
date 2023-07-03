/**********************************
接口地址： /sfa/salesView/orgSalesListDef   获取下级员工组织零售数据列表_默认																																	
上线版本        创建日期      修改人    修改内容
--v1.0.0     20230420   hsj    新增

数据库：生产StarRock

参数：
p_org_code	string	登录人员组织编号
p_stat_dt	string	开始日期(yyyymmdd)
p_end_date	string	结束日期(yyyymmdd)
p_date_type	string	日期类型(d-日/w-周/m-月)

SELECT  *   from ads_ka.dim_ka_bcp_emp_pstn_privilege_corr			t3  		-- 人员编制权限配置表
SELECT  *   from ads_ka.res_ka_ss_jjjg_org_d						t7  		-- 零售主题组织日汇总结果表
SELECT  *   from ads_ka.res_ka_ss_jjjg_org_m						t8  		-- 零售主题组织月日汇总结果表
SELECT  *   from ads_ka.res_ka_ss_jjjg_org_w						t9 			-- 零售主题组织周日汇总结果表
SELECT  *   from ads_ka.dim_ka_pub_rel_pstn_org_emp					t10  		-- 编制组织关系表
SELECT  *   from ads_ka.dim_ka_sfa_order_org						t21 		-- 订单视图组织范围配置表
********************************/

with  temp0 as (
-- 获取登录人所在组织的一级下级组织
SELECT   
t10.org_cd
,case when coalesce(t10.org_nam,'') = '' then 'TBC' else t10.org_nam end as org_nam 
,t10.emp_cd
,case when coalesce(t10.emp_nam,'') = '' then 'TBC' else t10.emp_nam end  as emp_nam
,t10.org_level

,t21.view_nam    -- 测试临时条件 调试完后隐藏
,t10.parorg_org_cd    -- 测试临时条件 调试完后隐藏
from ads_ka.dim_ka_pub_rel_pstn_org_emp t10  -- 编制组织关系表
left  join ads_ka.dim_ka_sfa_order_org		t21 		-- 订单视图组织范围配置表 
on t10.org_level= t21.org_level
and t10.org_nam like  concat('%',t21.key_nam,'%') 
and t21.view_nam = '零售'   

where t10.lvl2_org_cd  in ('O00007257','O00008060')   -- 限制 门店和推广 两条架构 

and t10.parorg_org_cd ='30065106'     --  登录人组织编码 入参    所在组织上级组织编码

and t10.is_charge ='1'  -- 获取组织 负责人

and (
case when t10.org_level=2 then 1
when t10.org_nam  like  concat('%',t21.key_nam,'%') then 1  else 0  end )=1  -- 限制3级以及以下组织范围

group by 
t10.org_cd
,t10.org_nam
,t10.emp_cd
,t10.emp_nam
,t10.org_level
,t21.view_nam    		-- 测试临时条件 调试完后隐藏
,t10.parorg_org_cd    	-- 测试临时条件 调试完后隐藏
)



,res as (
select 
	temp0.emp_nam	as	employee_name	--	员工姓名		varchar
,	temp0.emp_cd	as	employee_code	--	员工编码		varchar
,	regexp_replace(temp0.org_nam,'经营委员会|孵化组|门店孵化组|小组|门店小组','') 	as	org_name	--	组织名称		varchar
,	temp0.org_cd	as	org_code	--	组织编码		varchar
,	case when temp0.org_level<=4  then 1  else 0  end 	as	is_next_lev	  -- 是否可下钻至下一层级(1-是,0-否)		varchar
,	temp1.actual_sales_amt		as	all_retail_amount		--	全品零售			当天全品整合零售金额(元)	numeric(18,0)  
,	temp1.new_sales_amt			as	new_retail_amount		--	新品零售金额	 	当天新品整合零售金额(元)	numeric(18,0)
,	temp1.zz_sales_amt			as	supreme_retail_amount	--	至尊零售金额		当天至尊整合零售金额(元)	numeric(18,0)
,	temp1.zlp_sales_amt			as	strategy_amount			--	战略品零售金额	当天战略品整合零售金额(元)	numeric(18,0)
,	case when temp1.actual_sales_amt=0 then 0 else coalesce(temp1.new_sales_amt,0)/temp1.actual_sales_amt end
	as	new_retail_percent	--	新品零售占比		numeric(18,0)
,	case when temp1.actual_sales_amt=0 then 0 else coalesce(temp1.zz_sales_amt,0)/temp1.actual_sales_amt end
	as	supreme_retail_percent	--	至尊零售占比		numeric(18,0)
,	case when temp1.actual_sales_amt=0 then 0 else coalesce(temp1.zlp_sales_amt,0)/temp1.actual_sales_amt end
	as	strategy_amount_percent	--	战略品零售占比		numeric(18,0)
from  temp0   -- 获取登录人所在组织的一级下级组织
left join  ads_ka.res_ka_ss_jjjg_org_d	temp1  		-- 零售主题组织日汇总结果表      
	on temp1.org_cd=temp0.org_cd
	and temp1.stat_dt = '20230401'      									-- 日数据 开始日期  入参
-- and temp1.stat_mo =substr('20230401',1,6)               				-- 开始日期   月数据  入参
-- and '20230401' between temp1.week_start_dt and temp1.week_end_dt      	-- 开始日期  周数据  入参
)

SELECT   
	employee_name			as	employee_name	--	员工姓名		varchar
,	employee_code			as	employee_code	--	员工编码		varchar
,	org_name				as	org_name	--	组织名称		varchar
,	org_code				as	org_code	--	组织编码		varchar
,	is_next_lev				as	is_next_lev	--	是否可下钻至下一层级(1-是,0-否)		varchar
,	round(all_retail_amount			,0)as	all_retail_amount	--	全品零售		numeric(18,0)
,	round(new_retail_amount			,0)as	new_retail_amount	--	新品零售金额		numeric(18,0)
,	round(supreme_retail_amount		,0)as	supreme_retail_amount	--	至尊零售金额		numeric(18,0)
,	round(strategy_amount			,0)as	strategy_amount	--	战略品零售金额		numeric(18,0)
,	round(new_retail_percent*100		,0)as	new_retail_percent	--	新品零售占比		numeric(18,0)
,	round(supreme_retail_percent*100	,0)as	supreme_retail_percent	--	至尊零售占比		numeric(18,0)
,	round(strategy_amount_percent*100	,0)as	strategy_amount_percent	--	战略品零售占比		numeric(18,0)
from  res
order by all_retail_amount desc nulls last  ,org_code desc     -- 排序字段  排序规则 入参


