# Databricks notebook source
dbutils.widgets.text("query_sql", "", "SQL to execute")
dbutils.widgets.text("query_sql_file", "", "SQL to execute from a file")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse Id")
dbutils.widgets.text("host_uri", "adb-984752964297111.11.azuredatabricks.net", "Workspace Host Uri")
dbutils.widgets.text("max_download_threads", "20", "Maximum threads to use for download if use_cloud_fetch is true")
dbutils.widgets.text("use_cloud_fetch", "False", "Use Cloud Fetch to retrieve query results")

# COMMAND ----------

import re

# COMMAND ----------

host_uri = dbutils.widgets.get("host_uri")
warehouse_id = dbutils.widgets.get("warehouse_id")
query_sql = dbutils.widgets.get("query_sql")
query_sql_file = dbutils.widgets.get("query_sql_file")
max_download_threads = dbutils.widgets.get("max_download_threads")
if max_download_threads is not None and max_download_threads != "":
  max_download_threads = int(max_download_threads)
else:
  max_download_threads = 20
use_cloud_fetch = dbutils.widgets.get("use_cloud_fetch")

use_cloud_fetch = use_cloud_fetch.lower() in ['true', '1', 't', 'y', 'yes']

# COMMAND ----------

if query_sql_file is not None and query_sql_file != "":
    query_sql_from_file = dbutils.fs.head(query_sql_file)
else:
    query_sql_from_file = ""

# COMMAND ----------

sql_arr = [s for s in [query_sql, query_sql_from_file] if s is not None and s != ""]
query_sql = ";".join(sql_arr)

# COMMAND ----------

query_sqls = re.findall("((?:(?:'[^']*')|[^;])*);", query_sql) # split by semicolon

# COMMAND ----------

from databricks import sql

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
with sql.connect(server_hostname = host_uri,
                 http_path       = f"/sql/1.0/warehouses/{warehouse_id}",
                 access_token    = token,
                 max_download_threads = max_download_threads,
                 use_cloud_fetch = use_cloud_fetch) as connection:

  with connection.cursor() as cursor:
    for s in query_sqls:
      cursor.execute(s)
    results = cursor.fetchall_arrow()
    print(results)
