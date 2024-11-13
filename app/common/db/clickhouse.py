import clickhouse_connect
from common.config import CLICKHOUSE_DSN

class ClickhouseClient:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.client = self._init_client()
        
    def _init_client(self):
        return clickhouse_connect.get_client(dsn=self.dsn)
        
    def execute(self, query: str, params: dict = None):
        return self.client.execute(query, parameters=params)
        
    def insert_dataframe(self, table: str, df):
        return self.client.insert_df(table, df) 