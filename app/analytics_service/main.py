from fastapi import FastAPI
from common.clickhouse import ClickhouseClient
from common.postgres import PostgresClient

app = FastAPI(
    title="Analytics Service",
    description="Service for providing analytics and viewing records",
    version="1.0.0",
)

clickhouse_client = ClickhouseClient()
postgres_client = PostgresClient()


@app.get("/analytics", tags=["Analytics"])
async def get_analytics():
    """
    Получение аналитических данных из ClickHouse.
    """
    result = clickhouse_client.execute("SELECT * FROM data_table LIMIT 10")
    return result


@app.get("/records", tags=["Records"])
async def get_records():
    """
    Получение записей из PostgreSQL.
    """
    result = postgres_client.execute("SELECT * FROM records LIMIT 10")
    return result
