# processing_service/main.py
from confluent_kafka import Consumer
from common.clickhouse import ClickhouseClient
from common.postgres import PostgresClient
from common.config import KAFKA_BROKER_URL
import json

# Настройка Kafka Consumer
kafka_consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER_URL,
    'group.id': 'processing_group',
    'auto.offset.reset': 'earliest'
})
kafka_consumer.subscribe(['parsed_data'])

# Настройка клиентов Clickhouse и PostgreSQL
clickhouse_client = ClickhouseClient()
postgres_client = PostgresClient()

try:
    while True:
        msg = kafka_consumer.poll(1.0)  # Пытаемся получить сообщение в течение 1 секунды
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        data = json.loads(msg.value().decode('utf-8'))

        # Обработка данных и запись в Clickhouse
        clickhouse_client.execute(
            "INSERT INTO data_table (field1, field2, field3) VALUES", 
            (data["field1"], data["field2"], data["field3"])
        )

        # Запись данных в PostgreSQL
        postgres_client.execute(
            "INSERT INTO records (field1, field2, field3) VALUES (%s, %s, %s)", 
            (data["field1"], data["field2"], data["field3"])
        )
finally:
    # Закрытие Kafka Consumer
    kafka_consumer.close()
    # Закрытие подключения к PostgreSQL
    postgres_client.close()
