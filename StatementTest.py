# Databricks notebook source
import time
from sql.statement import Statement
from urllib3 import request

start_time = time.time()

api_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
statement = Statement(api_url = api_url, token = token)
pdf = statement.execute(warehouse_id = "11b188d521392342", sql_statement = "SELECT * FROM leone_catalog.leon_eller_db.insurance_claims_features")

end_time = time.time()

print(f"Elapsed time {end_time - start_time} seconds")

# COMMAND ----------

df = spark.createDataFrame(pdf)
display(df)

# COMMAND ----------

df.count()
