# processing_service/main.py
from confluent_kafka import Consumer, KafkaError
from common.clickhouse import ClickhouseClient
from common.config import KAFKA_BROKER_URL, CLICKHOUSE_DSN
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.kafka_consumer = Consumer({
            'bootstrap.servers': 'kafka:29092',
            'group.id': 'processing_group',
            'auto.offset.reset': 'earliest'
        })
        self.clickhouse_client = ClickhouseClient(CLICKHOUSE_DSN)
        self.kafka_consumer.subscribe(['parsed_data'])

    def process_message(self, data):
        try:
            # Подготовка данных для вставки в ClickHouse
            clickhouse_data = {
                'id': data['certificate_id'],
                'eaeu_product_group': data['eaeu_product_group'],
                'registration_number': data['registration_number'],
                'status': data['status'],
                'registration_date': data['registration_date'],
                'expiration_date': data['expiration_date'],
                'termination_date': data['termination_date'],
                'applicant_inn': data['applicant_inn'],
                'manufacturer_inn': data['manufacturer_inn'],
                'certification_body_ral': data['certification_body_ral'],
                'has_violations': 1 if any([data.get('certificate_violations'),
                                            data.get('lab_violations'),
                                            data.get('certification_body_violations'),
                                            data.get('qms_violations')]) else 0,
                'created_at': datetime.now()
            }

            # Вставка данных в ClickHouse
            self.clickhouse_client.execute("""
                INSERT INTO certificates_analytics
                (id, eaeu_product_group, registration_number, status, registration_date,
                expiration_date, termination_date, applicant_inn, manufacturer_inn,
                certification_body_ral, has_violations, created_at)
                VALUES
            """, [clickhouse_data])

            logger.info(f"Successfully processed message for certificate_id: {data['certificate_id']}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            raise

    def run(self):
        try:
            while True:
                msg = self.kafka_consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    self.process_message(data)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        finally:
            self.kafka_consumer.close()

if __name__ == "__main__":
    processor = DataProcessor()
    processor.run()
