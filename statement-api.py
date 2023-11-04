import time
from sql.statement import Statement

start_time = time.time()

statement = Statement(
  api_url = "https://adb-984752964297111.11.azuredatabricks.net",
  token = "<token>" # Yoir PAT Token
)

pdf = statement.execute(
  warehouse_id = "11b188d521392342", # SQL Warehouse Id. Get it from SQL Warehouse Overview Tab in UI
  sql_statement = "SELECT * FROM <catalog>.<schema>.<table>" # Your query
)

end_time = time.time()

print(pdf)
print(f"Elapsed time {end_time - start_time} seconds")
