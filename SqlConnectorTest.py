# Databricks notebook source
# MAGIC %pip install databricks-sql-connector

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks import sql
import os

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
with sql.connect(server_hostname = "adb-984752964297111.11.azuredatabricks.net",
                 http_path       = "/sql/1.0/warehouses/<warehouse id>",
                 access_token    = token,
                 max_download_threads = 20,
                 use_cloud_fetch = True) as connection:

  with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM <catalog>.<schema>.<table>")
    results = cursor.fetchall_arrow()

    for row in results[0:10]:
      print(row)

# COMMAND ----------


