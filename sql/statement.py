import time
import json
import concurrent.futures
from io import BytesIO
import pyarrow as pa
import pandas as pd
import gc
import urllib3

class Statement:
  __api_url = None
  __token = None

  def __init__(self, api_url, token):
    self.__api_url = api_url
    self.__token = token
  
  def __send_sql_request(self, action, request_func):
    response = request_func(f'{self.__api_url}/api/2.0/sql/{action}', {"Authorization": f"Bearer {self.__token}"})
    if response.status != 200:
      js = json.loads(response.data)
      raise Exception("Error: %s: %s" % (js["error_code"], js["message"]))
    return json.loads(response.data)

  def __download_chunk(self, http, statement_id, c):
    response = self.__send_sql_request(f'statements/{statement_id}/result/chunks/{c}', lambda u, h: http.request(method="GET", url=u, headers=h))
    response = http.request(method="GET", url=response["external_links"][0]["external_link"])
    df = None
    if response.status == 200:
        with pa.ipc.open_stream(response.data) as reader:
            df = reader.read_pandas()
    else:
      raise Exception(f"HTTP Error {response.status}")
    return df

  def execute(self, warehouse_id, sql_statement, max_download_threads = 20):
    with urllib3.PoolManager(num_pools=1) as http:
      payload = {
          "warehouse_id": warehouse_id,
          "statement": sql_statement,
          "wait_timeout": "0s",
          "disposition": "EXTERNAL_LINKS",
          "format": "ARROW_STREAM"
      }
      payload = json.dumps(payload).encode('utf-8')
      response = self.__send_sql_request('statements', lambda u, h: http.request(method="POST", url=f'{u}', body=payload, headers=h))
      while response["status"]["state"] == "PENDING" or response["status"]["state"] == "RUNNING":
        response = self.__send_sql_request(f'statements/{response["statement_id"]}', lambda u, h: http.request(method="GET", url=u, headers=h))
        time.sleep(1)
      statement_id = response["statement_id"]

      total_chunk_count = response["manifest"]["total_chunk_count"]
      with concurrent.futures.ThreadPoolExecutor(max_workers=max_download_threads) as executor:
        runs = [executor.submit(self.__download_chunk, http, statement_id, c) for c in range(0, total_chunk_count)]
        results = [r.result() for r in runs]

      df = pd.concat(results, ignore_index=True)
      return df
