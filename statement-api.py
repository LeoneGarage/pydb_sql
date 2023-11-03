import time
from sql.statement import Statement

start_time = time.time()

statement = Statement(api_url = "https://adb-984752964297111.11.azuredatabricks.net", token = "
")
df = statement.execute(warehouse_id = "37f83781be24d90a", sql_statement = "SELECT * FROM leone_catalog.leon_eller_db.insurance_claims_features")

end_time = time.time()

print(df)
print(f"Elapsed time {end_time - start_time} seconds")
