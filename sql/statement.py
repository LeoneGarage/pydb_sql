import time
import requests
import json
import concurrent.futures
from io import BytesIO
import pyarrow as pa
import pandas as pd

class Statement:
  api_url = None
  token = None

  def __init__(self, api_url, token):
    self.api_url = api_url
    self.token = token
  
  def __send_sql_request(self, action, request_func):
    response = request_func(f'{self.api_url}/api/2.0/sql/{action}', {"Authorization": f"Bearer {self.token}"})
    if response.status_code != 200:
      raise Exception("Error: %s: %s" % (response.json()["error_code"], response.json()["message"]))
    return response.json()

  def __download_chunk(self, statement_id, c):
    response = self.__send_sql_request(f'statements/{statement_id}/result/chunks/{c}', lambda u, h: requests.get(u, headers=h))
    response = requests.get(response["external_links"][0]["external_link"], stream=True)
    df = None
    if response.status_code == 200:
        with pa.ipc.open_stream(response.raw) as reader:
            df = reader.read_pandas()
    return df

  def execute(self, warehouse_id, sql_statement, max_download_threads = 20):
    payload = {
        "warehouse_id": warehouse_id,
        "statement": sql_statement,
        "wait_timeout": "0s",
        "disposition": "EXTERNAL_LINKS",
        "format": "ARROW_STREAM"
    }
    response = self.__send_sql_request('statements', lambda u, h: requests.post(f'{u}', json=payload, headers=h))
    while response["status"]["state"] == "PENDING" or response["status"]["state"] == "RUNNING":
      response = self.__send_sql_request(f'statements/{response["statement_id"]}', lambda u, h: requests.get(u, headers=h))
      time.sleep(1)
    statement_id = response["statement_id"]

    total_chunk_count = response["manifest"]["total_chunk_count"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_download_threads) as executor:
      runs = [executor.submit(self.__download_chunk, statement_id, c) for c in range(0, total_chunk_count)]
      results = [r.result() for r in runs]

    df = pd.concat(results, ignore_index=True)
    return df
