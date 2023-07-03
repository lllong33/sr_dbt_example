# pymysql 这个也可以
import pymysql



# 2013 (HY000): Lost connection to MySQL server during query
# 复现这个报错

# Create a database connection
# kwargs = {'host': '192.168.235.101', 'username': 'ads_ka_rd', 'password': 'ka_rd_7b65e6_dev', 'database': 'ads_ka', 'port': 9030} # test
kwargs = {'host': '192.168.39.104', 'user': 'ads_ka_rd', 'passwd': 'ads_ka_rd@13b705', 'database': 'ads_ka',
          'port': 9030}  # prod
cnx = pymysql.connect(**kwargs)

try:
    # Start a transaction
    # cnx.start_transaction()
    # cursor = cnx.cursor()
    # cursor.execute("begin")
    # print("begin", cursor.fetchall())

    cursor = cnx.cursor()
    # 2013 (HY000): Lost connection to MySQL server during query
    # sql="""
    # select
    #   null as "database",
    #   table_name as name,
    #   table_schema as "schema",
    #   case when table_type = 'BASE TABLE' then 'table'
    #        when table_type = 'VIEW' then 'view'
    #        else table_type end as table_type
    # from information_schema.tables
    # where table_schema = 'ads_ka'
    # """

    # 可执行
    # sql = 'select 1'

    # ransaction rolled back due to: 2013 (HY000): Lost connection to MySQL server during query
    sql = 'select * from res_ka_co_org_d_bak20230210 limit 10'
    cursor.execute(sql)
    print(cursor.fetchall())

    # cursor = cnx.cursor()
    # sql = "rollback"
    # cursor.execute(sql)
    # print(cursor.fetchall())

except Exception as error:
    print(f"Transaction rolled back due to: {error}")

finally:
    cursor.close()
    cnx.close()