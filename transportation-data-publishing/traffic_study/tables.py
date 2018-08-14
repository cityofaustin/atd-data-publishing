"""
Create Traffic Study Tables
"""
import pdb
import sys

import psycopg2

import _setpath
from config.secrets import TRAFFIC_STUDY_DB
import table_config

dbname = TRAFFIC_STUDY_DB["dbname"]
user = TRAFFIC_STUDY_DB["user"]
password = TRAFFIC_STUDY_DB["password"]
host = TRAFFIC_STUDY_DB["host"]
port = 5432

pdb.set_trace()
conn = psycopg2.connect(
    dbname=dbname, user=user, host=host, password=password, port=port
)
cursor = conn.cursor()


def create_table_query(tbl_cfg):
    sql = ""
    for field in tbl_cfg["fields"]:
        sql = sql + '"{}" {}, \n'.format(field["name"], field["type"])
    return sql


for table in tables:
    sql = create_table_query(table)
    cursor.execute(sql)

sql = """
    SELECT * from information_schema.tables
"""
cursor.execute(sql)
res = cursor.fetchall()

for t in res:
    if t[2] in tables:
        print(t)

conn.close()
