# Databricks notebook source
import time
from sql.statement import Statement
from urllib3 import request

api_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
statement = Statement(api_url = api_url, token = token)
pdf = statement.execute(
  warehouse_id = "11b188d521392342", # SQL Warehouse Id. Get it from SQL Warehouse Overview Tab in UI
  sql_statement = "SELECT * FROM <catalog>.<schema>.<table>" # Your query
)

# COMMAND ----------

df = spark.createDataFrame(pdf)
display(df)

# COMMAND ----------

df.count()
