with inc as(
select 
    distinct t.this_day as stat_dt
from dim_day  t
where t.this_day >= '20230506'
    and t.this_day <= DATE_FORMAT(date_trunc('day', days_sub(NOW(), 45)), '%Y%m%d')
    -- and t.this_day <= DATE_FORMAT(date_trunc('day', days_sub(NOW(), -45)), '%Y%m%d') 
)


select 
     b.stat_dt  
    ,b.org_code as org_cd
    ,b.charge_flag
    ,b.charge_flag_org_dt
    ,b.pstn_code    as  pstn_cd                     -- 编制编码	
    ,b.pstn_type    as  pstn_typ_cd                 -- 编制类型编码	
    ,c.user_id      as  emp_cd                      -- 员工编码	
    ,c.user_name    as  emp_nam                     -- 员工姓名	
    ,c.user_tag     as  emp_tag_cd                  -- 人员标记编码	
    ,dt4.dict_name  as dt4_dict_name
    ,dt3.dict_name  as dt3_dict_name
    -- debug info
	,concat(
		'b.stat_dt',coalesce(b.stat_dt, '')
		,'b.pstn_code',coalesce(b.pstn_code, '')
	) as b_k 
	,concat(
		'c.stat_dt',coalesce(c.stat_dt, '')
		,'c.pstn_code',coalesce(c.pstn_code, '')
	) as c_k 
    --	count(*), count(distinct stat_dt, org_code)
from {{ ref('d_ihd_bcp_org_position') }} as b -- pk=[stat_dt,pstn_code]
left join {{ ref('d_ihd_bcp_org_user') }} as c -- pk=[stat_dt,pstn_code]
    on b.pstn_code=c.pstn_code   	-- 人员信息    
    and b.stat_dt=c.stat_dt
left join {{ ref('d_ihd_hr_hrx_sys_dict_entry') }} dt3 
    on b.pstn_type=dt3.dict_id 
    and dt3.dict_type_id='IHD_ORG_PSTN_TYPE' and dt3.dist_status='A'   --编制类型字典值
left join {{ ref('d_ihd_hr_hrx_sys_dict_entry') }} dt4 
    on c.user_tag=dt4.dict_id 
    and dt4.dict_type_id='IHD_ORG_USER_TAG' and dt4.dist_status='A'   --人员标记字典值    
;



drop table if exists temp.dim_ihd_pub_rel_pstn_org_emp_zip_org_position_user;
create table temp.dim_ihd_pub_rel_pstn_org_emp_zip_org_position_user STORED AS parquet as 

select 
     t1.stat_dt
    ,t1.org_cd
    ,t1.charge_flag
    ,t1.charge_flag_org_dt
    ,t1.pstn_cd
    ,t1.pstn_typ_cd
    ,t1.emp_cd
    ,t1.emp_nam
    ,t1.emp_tag_cd
    ,t1.dt4_dict_name
    ,t1.dt3_dict_name
    ,t2.pstn_cd as lead_pstn_cd
    ,t2.pstn_typ_cd as lead_pstn_typ_cd
    ,t2.emp_cd as lead_emp_cd
    ,t2.emp_nam as lead_emp_nam
    ,t2.emp_tag_cd as lead_emp_tag_cd
    ,t2.dt4_dict_name as lead_dt4_dict_name
    ,t2.dt3_dict_name as lead_dt3_dict_name
from temp.dim_ihd_pub_rel_pstn_org_emp_zip_org_position_user_p1 as t1 
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_org_position_user_p1 as t2 -- 负责人信息
    on t1.stat_dt = t2.stat_dt 
    and t1.org_cd = t2.org_cd 
    and t2.charge_flag = '1'
;


drop table if exists temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1;
create table temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 stored as parquetfile as
-- A.组织编码范围
--找出每一天的非包含组织&组织链路
select --pk=[stat_dt,org_code]
    a.stat_dt 
    ,a.org_code as org_cd 
    ,a.org_name as org_nam
    ,a.org_link
    ,a.org_level
    ,a.parent_org_code as parent_org_cd
    ,a.org_sort as org_sort
from temp.dim_ihd_pub_rel_pstn_org_emp_zip_org a -- 组织信息
where a.org_form<>'1' -- 剔除：组织形态=包含组织
;


drop table if exists temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org2;
create table temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org2 stored as parquetfile as
-- B.包含组织编码范围
--找出每一天的包含组织&它的上级组织
select --pk=[stat_dt,org_code]
    a.stat_dt
    ,a.parent_org_code as parent_org_cd
    ,a.org_code as org_cd 
    ,a.org_name as org_nam
from temp.dim_ihd_pub_rel_pstn_org_emp_zip_org a -- 组织信息
where a.org_form='1' -- 组织形态=包含组织
;


drop table if exists temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org3_p1;
create table temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org3_p1 stored as parquetfile as
-- C.组织信息
select -- pk=[stat_dt,org_code]
    a.stat_dt
    ,a.org_code as org_cd
    ,a.org_type
    ,tt1.dict_id as org_typ_cd
    ,tt1.dict_name as org_typ_nam
    ,a.org_form
    ,dt2.dict_id as org_form_cd
    ,dt2.dict_name as org_form_nam
    ,'debug_info'
    ,concat(
        'a.org_type: ',coalesce(a.org_type,'')
        ,'__tt1.g_dict_id:',coalesce(tt1.g_dict_id,'')
						,'__a.org_type1: ', split_part(a.org_type,';',1)
						,'__a.org_type2: ', split_part(a.org_type,';',2)
					,'__parent_dict_id: ', coalesce(tt1.parent_dict_id,'')
					,'__dict_id: ', coalesce(tt1.dict_id,'')
    )
from temp.dim_ihd_pub_rel_pstn_org_emp_zip_org a -- 组织信息
left join(
     select 
         concat(trim(dt1.parent_dict_id),'/',trim(dt1.dict_id)) as g_dict_id
        ,dt1.dict_id
        ,dt1.dict_name
        ,dt1.parent_dict_id
        ,dt1.dict_seq
        -- ,dt1.parent_dict_id
        ,dt1.dist_status
    from o_hrmhc_hr_hrx_sys_dict_entry dt1
    where dt1.dict_seq like '%!IHD_ORG_TYPE!%'  
        and dt1.dist_status='A'
)tt1 
	on (tt1.dict_id = split_part(a.org_type,';',1)  or tt1.dict_id = split_part(a.org_type,';',2) or a.org_type = tt1.g_dict_id)			
left join o_hrmhc_hr_hrx_sys_dict_entry dt2 
    on a.org_form=dt2.dict_id 
    and dt2.dict_type_id='IHD_ORG_FORM' 
    and dt2.dist_status='A'
-- where -- unit_test 
--     a.org_code in ('O10001157','O10002164')
;


drop table if exists temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org3;
create table temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org3 stored as parquetfile as
select 
    stat_dt
    ,org_cd
    ,org_type
    ,org_form
    ,org_form_cd
    ,org_form_nam
    ,group_concat(org_typ_cd,';') as org_typ_cd
    ,group_concat(org_typ_nam,';') as org_typ_nam
from temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org3_p1
group by 1,2,3,4,5,6
;


-- V_FROM_SQL
-- drop table if exists dim_ihd_pub_rel_pstn_org_emp_zip;
-- create table dim_ihd_pub_rel_pstn_org_emp_zip stored as parquetfile as
select 
     oz1.stat_dt
    ,oz1.org_cd                       -- 组织编码	
    ,oz1.org_nam                      -- 组织名称	
    ,oz13.org_typ_cd                  -- 组织类型编码	
    ,oz13.org_typ_nam                 -- 组织类型名称	
    ,oz13.org_form_cd                  -- 组织形态编码	
    ,oz13.org_form_nam                 -- 组织形态名称	
    ,case when b1.charge_flag_org_dt >= 1 then b1.lead_emp_cd -- 1.优先取编制负责人 2.取不到再取组织负责人
        else a1.charge_id
        end as org_chrg_emp_cd              -- 组织负责人-编码	
    ,case when b1.charge_flag_org_dt >= 1 then b1.lead_emp_nam
        else a1.charge_name
        end as org_chrg_emp_nam             -- 组织负责人-姓名	
    ,case when b1.charge_flag_org_dt >= 1 then b1.lead_emp_tag_cd 
        else '53'
        end as org_chrg_emp_tag_cd                  -- 组织负责人-人员标记编码		
    ,b1.lead_dt4_dict_name as org_chrg_emp_tag_nam        -- 组织负责人-人员标记名称	
    ,'undefined' as inner_org_cd                 -- 包含下级组织编码	
    ,cast(null as string) as inner_org_nam                -- 包含下级组织名称	
    ,cast(null as string) as inner_org_typ_cd            -- 包含下级组织类型编码	
    ,cast(null as string) as inner_org_typ_nam           -- 包含下级组织类型名称	
    ,cast(null as string) as inner_org_form_cd            -- 包含下级组织形态编码	
    ,cast(null as string) as inner_org_form_nam           -- 包含下级组织形态名称	
    ,cast(null as string) as inner_org_chrg_emp_cd        -- 包含下级组织负责人-编码	
    ,cast(null as string) as inner_org_chrg_emp_nam       -- 包含下级组织负责人-姓名	
    ,cast(null as string) as inner_org_chrg_emp_tag_cd    -- 包含下级组织负责人-人员标记编码	
    ,cast(null as string) as inner_org_chrg_emp_tag_nam   -- 包含下级组织负责人-人员标记名称	
    -- 包含_start
    ,coalesce(b1.pstn_cd, 'undefined') as pstn_cd                     -- 编制编码
    ,b1.pstn_typ_cd                 -- 编制类型编码	
    ,b1.dt3_dict_name  as  pstn_typ_nam -- 编制类型名称	
    ,b1.emp_cd                      -- 员工编码
    ,b1.emp_nam                     -- 员工姓名	
    ,b1.emp_tag_cd                  -- 人员标记编码	
    ,b1.dt4_dict_name  as  emp_tag_nam                  -- 人员标记名称	
    -- 包含end
    ,split_part(oz1.org_link,'.',2) as lvl0_org_cd      -- 0级组织编码	
    ,lvl0.org_nam as lvl0_org_nam     -- 0级组织名称
    ,split_part(oz1.org_link,'.',3)  as lvl1_org_cd                  -- 1级组织编码	
    ,lvl1.org_nam  as lvl1_org_nam                 -- 1级组织名称	
    ,split_part(oz1.org_link,'.',4)  as lvl2_org_cd                  -- 2级组织编码	
    ,lvl2.org_nam  as lvl2_org_nam                 -- 2级组织名称	
    ,split_part(oz1.org_link,'.',5)  as lvl3_org_cd                  -- 3级组织编码	
    ,lvl3.org_nam  as lvl3_org_nam                 -- 3级组织名称	
    ,split_part(oz1.org_link,'.',6)  as lvl4_org_cd                  -- 4级组织编码	
    ,lvl4.org_nam  as lvl4_org_nam                 -- 4级组织名称	
    ,split_part(oz1.org_link,'.',7)  as lvl5_org_cd                  -- 5级组织编码	
    ,lvl5.org_nam  as lvl5_org_nam                 -- 5级组织名称	
    ,split_part(oz1.org_link,'.',8)  as lvl6_org_cd                  -- 6级组织编码	
    ,lvl6.org_nam  as lvl6_org_nam                 -- 6级组织名称	
    ,split_part(oz1.org_link,'.',9)  as lvl7_org_cd                  -- 7级组织编码	
    ,lvl7.org_nam  as lvl7_org_nam                 -- 7级组织名称	
    ,split_part(oz1.org_link,'.',10)  as lvl8_org_cd                  -- 8级组织编码	
    ,lvl8.org_nam  as lvl8_org_nam                 -- 8级组织名称	
    ,split_part(oz1.org_link,'.',11)  as lvl9_org_cd                  -- 9级组织编码	
    ,lvl9.org_nam  as lvl9_org_nam                 -- 9级组织名称	
    ,split_part(oz1.org_link,'.',12)  as lvl10_org_cd                 -- 10级组织编码	
    ,lvl10.org_nam  as lvl10_org_nam                -- 10级组织名称	
    ,split_part(oz1.org_link,'.',13)  as lvl11_org_cd                 -- 11级组织编码	
    ,lvl11.org_nam  as lvl11_org_nam                -- 11级组织名称	
    ,'p1'  as etl_lvl
    ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')   as etl_datetime  -- ETL时间
    ,if(case when b1.charge_flag_org_dt >= 1 then b1.lead_emp_cd  -- 1.优先取编制负责人 2.取不到再取组织负责人
            when a1.charge_id is not null and b1.pstn_cd is null then '-99999'  -- 平凡, 没有对应人员编码和编制;
            else a1.charge_id
            end = coalesce(b1.emp_cd, '-99999'), '1', '0')  as is_charg -- 是否负责人
    ,oz1.org_level
    ,oz1.parent_org_cd
    ,oz1.org_sort
from temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as oz1
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org3 as oz13 on oz1.stat_dt=oz13.stat_dt and oz1.org_cd=oz13.org_cd 
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_org a1 on oz1.stat_dt=a1.stat_dt and oz1.org_cd=a1.org_cd   -- 组织信息
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_org_position_user b1 on oz1.stat_dt=b1.stat_dt and oz1.org_cd=b1.org_cd 	-- 编制信息 -- 粒度=[stat_dt, pstn_cd] 扩散到org_code
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl0 on oz1.stat_dt = lvl0.stat_dt and split_part(oz1.org_link,'.',2) = lvl0.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl1 on oz1.stat_dt = lvl1.stat_dt and split_part(oz1.org_link,'.',3) = lvl1.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl2 on oz1.stat_dt = lvl2.stat_dt and split_part(oz1.org_link,'.',4) = lvl2.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl3 on oz1.stat_dt = lvl3.stat_dt and split_part(oz1.org_link,'.',5) = lvl3.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl4 on oz1.stat_dt = lvl4.stat_dt and split_part(oz1.org_link,'.',6) = lvl4.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl5 on oz1.stat_dt = lvl5.stat_dt and split_part(oz1.org_link,'.',7) = lvl5.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl6 on oz1.stat_dt = lvl6.stat_dt and split_part(oz1.org_link,'.',8) = lvl6.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl7 on oz1.stat_dt = lvl7.stat_dt and split_part(oz1.org_link,'.',9) = lvl7.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl8 on oz1.stat_dt = lvl8.stat_dt and split_part(oz1.org_link,'.',10) = lvl8.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl9 on oz1.stat_dt = lvl9.stat_dt and split_part(oz1.org_link,'.',11) = lvl9.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl10 on oz1.stat_dt = lvl10.stat_dt and split_part(oz1.org_link,'.',12) = lvl10.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl11 on oz1.stat_dt = lvl11.stat_dt and split_part(oz1.org_link,'.',13) = lvl11.org_cd


union all 

select 
     oz1.stat_dt
    ,oz1.org_cd                       -- 组织编码	
    ,oz1.org_nam                      -- 组织名称	
    ,oz13.org_typ_cd                  -- 组织类型编码	
    ,oz13.org_typ_nam                 -- 组织类型名称	
    ,oz13.org_form_cd                  -- 组织形态编码	
    ,oz13.org_form_nam                 -- 组织形态名称	
    ,case when b1.charge_flag_org_dt >= 1 then b1.lead_emp_cd -- 1.优先取编制负责人 2.取不到再取组织负责人
        else a1.charge_id
        end as org_chrg_emp_cd              -- 组织负责人-编码	
    ,case when b1.charge_flag_org_dt >= 1 then b1.lead_emp_nam
        else a1.charge_name
        end as org_chrg_emp_nam             -- 组织负责人-姓名	
    ,case when b1.charge_flag_org_dt >= 1 then b1.lead_emp_tag_cd 
        else '53'
        end as org_chrg_emp_tag_cd                  -- 组织负责人-人员标记编码		
    ,b1.lead_dt4_dict_name as org_chrg_emp_tag_nam        -- 组织负责人-人员标记名称
    ,coalesce(oz2.org_cd,'undefined')        as inner_org_cd                 -- 包含下级组织编码	
    ,oz2.org_nam       as inner_org_nam                -- 包含下级组织名称	
    ,oz23.org_typ_cd   as inner_org_typ_cd            -- 包含下级组织类型编码	
    ,oz23.org_typ_nam  as inner_org_typ_nam           -- 包含下级组织类型名称	
    ,oz23.org_form_cd   as inner_org_form_cd            -- 包含下级组织形态编码	
    ,oz23.org_form_nam  as inner_org_form_nam           -- 包含下级组织形态名称	
    ,case when b2.charge_flag_org_dt >= 1 then b2.emp_cd
        else a2.charge_id
        end as inner_org_chrg_emp_cd        -- 包含下级组织负责人-编码	
    ,case when b2.charge_flag_org_dt >= 1 then b2.emp_nam
        else a2.charge_name
        end as inner_org_chrg_emp_nam       -- 包含下级组织负责人-姓名	
    ,case when oz2.org_cd is not null then  
            case when b2.charge_flag_org_dt >= 1 then b2.emp_tag_cd 
            else '53' 
            end 
        end as inner_org_chrg_emp_tag_cd    -- 包含下级组织负责人-人员标记编码	
    ,b2.dt4_dict_name as inner_org_chrg_emp_tag_nam   -- 包含下级组织负责人-人员标记名称	
    -- 包含_start
    -- TODO 组织没有包含组织, 所以编制也为null了, 置为 undefined
    ,coalesce(b2.pstn_cd, 'undefined')  as pstn_cd                    -- 编制编码
    ,b2.pstn_typ_cd                 -- 编制类型编码	
    ,b2.dt3_dict_name  as  pstn_typ_nam                 -- 编制类型名称	
    ,b2.emp_cd                      -- 员工编码
    ,b2.emp_nam                     -- 员工姓名	
    ,b2.emp_tag_cd                  -- 人员标记编码	
    ,b2.dt4_dict_name  as  emp_tag_nam                  -- 人员标记名称	
    -- 包含end
    ,split_part(oz1.org_link,'.',2) as lvl0_org_cd      -- 0级组织编码	
    ,lvl0.org_nam as lvl0_org_nam     -- 0级组织名称
    ,split_part(oz1.org_link,'.',3)  as lvl1_org_cd                  -- 1级组织编码	
    ,lvl1.org_nam  as lvl1_org_nam                 -- 1级组织名称	
    ,split_part(oz1.org_link,'.',4)  as lvl2_org_cd                  -- 2级组织编码	
    ,lvl2.org_nam  as lvl2_org_nam                 -- 2级组织名称	
    ,split_part(oz1.org_link,'.',5)  as lvl3_org_cd                  -- 3级组织编码	
    ,lvl3.org_nam  as lvl3_org_nam                 -- 3级组织名称	
    ,split_part(oz1.org_link,'.',6)  as lvl4_org_cd                  -- 4级组织编码	
    ,lvl4.org_nam  as lvl4_org_nam                 -- 4级组织名称	
    ,split_part(oz1.org_link,'.',7)  as lvl5_org_cd                  -- 5级组织编码	
    ,lvl5.org_nam  as lvl5_org_nam                 -- 5级组织名称	
    ,split_part(oz1.org_link,'.',8)  as lvl6_org_cd                  -- 6级组织编码	
    ,lvl6.org_nam  as lvl6_org_nam                 -- 6级组织名称	
    ,split_part(oz1.org_link,'.',9)  as lvl7_org_cd                  -- 7级组织编码	
    ,lvl7.org_nam  as lvl7_org_nam                 -- 7级组织名称	
    ,split_part(oz1.org_link,'.',10)  as lvl8_org_cd                  -- 8级组织编码	
    ,lvl8.org_nam  as lvl8_org_nam                 -- 8级组织名称	
    ,split_part(oz1.org_link,'.',11)  as lvl9_org_cd                  -- 9级组织编码	
    ,lvl9.org_nam  as lvl9_org_nam                 -- 9级组织名称	
    ,split_part(oz1.org_link,'.',12)  as lvl10_org_cd                 -- 10级组织编码	
    ,lvl10.org_nam  as lvl10_org_nam                -- 10级组织名称	
    ,split_part(oz1.org_link,'.',13)  as lvl11_org_cd                 -- 11级组织编码	
    ,lvl11.org_nam  as lvl11_org_nam                -- 11级组织名称	
    ,'p2'  as etl_lvl
    ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')   as etl_datetime  -- ETL时间
    ,if(case when b2.charge_flag_org_dt >= 1 then b2.lead_emp_cd
        when a2.charge_id is not null and b2.pstn_cd is null then '-99999'  -- 平凡, 没有对应人员编码和编制;
        else a2.charge_id
        end = coalesce(b2.emp_cd, '-99999'), '1', '0')  as is_charg -- 是否负责人
    ,oz1.org_level
    ,oz1.parent_org_cd 
    ,oz1.org_sort

    -- debug_info 
    -- ,b2.charge_flag_org_dt
    -- ,b2.lead_emp_cd
    -- ,a2.charge_id
    -- ,b2.pstn_cd
    -- ,b2.emp_cd
    -- ,b2.emp_nam
from temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as oz1
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org2 as oz2 on oz1.stat_dt=oz2.stat_dt and oz1.org_cd=oz2.parent_org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org3 as oz13 on oz1.stat_dt=oz13.stat_dt and oz1.org_cd=oz13.org_cd 
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org3 as oz23 on oz2.stat_dt=oz23.stat_dt and oz2.org_cd=oz23.org_cd -- 拿到包含组织对应信息
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_org a1 on oz1.stat_dt=a1.stat_dt and oz1.org_cd=a1.org_cd   -- 组织信息
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_org a2 on oz2.stat_dt=a2.stat_dt and oz2.org_cd=a2.org_cd   -- 组织信息
left join (
    select -- 包含组织,仅看 stat_dt, org_cd 维度关联, 不看pstn_cd, 加上会导致b2扩散
        distinct 
            stat_dt
            ,org_cd
            ,charge_flag_org_dt
            ,lead_emp_cd
            ,lead_emp_nam
            ,lead_emp_tag_cd
            ,lead_dt4_dict_name
    from temp.dim_ihd_pub_rel_pstn_org_emp_zip_org_position_user
) b1 on oz1.stat_dt=b1.stat_dt and oz1.org_cd=b1.org_cd 	-- 编制信息
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_org_position_user b2 on oz2.stat_dt=b2.stat_dt and oz2.org_cd=b2.org_cd  	-- 编制信息
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl0 on oz1.stat_dt = lvl0.stat_dt and split_part(oz1.org_link,'.',2) = lvl0.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl1 on oz1.stat_dt = lvl1.stat_dt and split_part(oz1.org_link,'.',3) = lvl1.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl2 on oz1.stat_dt = lvl2.stat_dt and split_part(oz1.org_link,'.',4) = lvl2.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl3 on oz1.stat_dt = lvl3.stat_dt and split_part(oz1.org_link,'.',5) = lvl3.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl4 on oz1.stat_dt = lvl4.stat_dt and split_part(oz1.org_link,'.',6) = lvl4.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl5 on oz1.stat_dt = lvl5.stat_dt and split_part(oz1.org_link,'.',7) = lvl5.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl6 on oz1.stat_dt = lvl6.stat_dt and split_part(oz1.org_link,'.',8) = lvl6.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl7 on oz1.stat_dt = lvl7.stat_dt and split_part(oz1.org_link,'.',9) = lvl7.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl8 on oz1.stat_dt = lvl8.stat_dt and split_part(oz1.org_link,'.',10) = lvl8.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl9 on oz1.stat_dt = lvl9.stat_dt and split_part(oz1.org_link,'.',11) = lvl9.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl10 on oz1.stat_dt = lvl10.stat_dt and split_part(oz1.org_link,'.',12) = lvl10.org_cd
left join temp.dim_ihd_pub_rel_pstn_org_emp_zip_t_org1 as lvl11 on oz1.stat_dt = lvl11.stat_dt and split_part(oz1.org_link,'.',13) = lvl11.org_cd
where oz2.org_cd is not null



