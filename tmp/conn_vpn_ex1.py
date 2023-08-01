
import mysql.connector

sql = """
    select
      null as "database",
      table_name as name,
      table_schema as "schema",
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type end as table_type
    from information_schema.tables
    where table_schema = 'ads_ct'
"""

import yaml
import sys 

# 如何是windows，需要使用绝对路径
if sys.platform == 'win32':
    url = r"C:\Users\admin\.dbt\profiles.yml"
else:
    url = "~/.dbt/profiles.yml"

with open(url) as f:
    data = yaml.load(f, Loader=yaml.FullLoader)    
    target = data["dbt_sr_example"]["target"]
    ct_dev ={
    "host": data["dbt_sr_example"]['outputs'][target]['host'],
    "port": data["dbt_sr_example"]['outputs'][target]['port'],
    "schema": data["dbt_sr_example"]['outputs'][target]['schema'],
    "username": data["dbt_sr_example"]['outputs'][target]['username'],
    "password": data["dbt_sr_example"]['outputs'][target]['password'],
    "type": data["dbt_sr_example"]['outputs'][target]['type'],
    }

cnx = mysql.connector.connect(
    host=ct_dev["host"],
    port=ct_dev["port"],
    user=ct_dev["username"],
    password=ct_dev["password"],
)

cur = cnx.cursor()


cur.execute("select version()")
row = cur.fetchone()
print("Current date is: {0}".format(row[0]))

print("=====================================")
cur.execute("select current_version()")
row = cur.fetchone()
print("Current version is: {0}".format(row[0]))

print("=====================================")
cur.execute(sql)
rows = cur.fetchall()
print(rows)




