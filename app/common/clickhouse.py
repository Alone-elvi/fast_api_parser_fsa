from clickhouse_driver import Client
from common.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DB,
)


class ClickhouseClient:
    _instance = None
    
    def __new__(cls, dsn: str = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.dsn = dsn
            cls._instance.client = cls._instance._init_client()
        return cls._instance
        
    def _init_client(self):
        return clickhouse_connect.get_client(dsn=self.dsn) 
