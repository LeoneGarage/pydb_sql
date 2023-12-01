# Databricks notebook source
# %pip install databricks-sql-connector

# COMMAND ----------

# MAGIC %pip install --no-index -f ./sql-connector databricks-sql-connector

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("query_sql", "", "SQL to execute")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse Id")
dbutils.widgets.text("host_uri", "adb-984752964297111.11.azuredatabricks.net", "Workspace Host Uri")
dbutils.widgets.text("max_download_threads", "20", "Maximum threads to use for download if use_cloud_fetch is true")
dbutils.widgets.text("use_cloud_fetch", "False", "Use Cloud Fetch to retrieve query results")

# COMMAND ----------

host_uri = dbutils.widgets.get("host_uri")
warehouse_id = dbutils.widgets.get("warehouse_id")
query_sql = dbutils.widgets.get("query_sql")
max_download_threads = dbutils.widgets.get("max_download_threads")
if max_download_threads is not None and max_download_threads != "":
  max_download_threads = int(max_download_threads)
use_cloud_fetch = dbutils.widgets.get("use_cloud_fetch")

use_cloud_fetch = use_cloud_fetch.lower() in ['true', '1', 't', 'y', 'yes']

# COMMAND ----------

from databricks import sql

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
with sql.connect(server_hostname = host_uri,
                 http_path       = f"/sql/1.0/warehouses/{warehouse_id}",
                 access_token    = token,
                 max_download_threads = max_download_threads,
                 use_cloud_fetch = use_cloud_fetch) as connection:

  with connection.cursor() as cursor:
    cursor.execute(query_sql)
    results = cursor.fetchall_arrow()
    print(results)
