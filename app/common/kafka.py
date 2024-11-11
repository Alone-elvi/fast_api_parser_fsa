from confluent_kafka import Producer, Consumer
import json


class KafkaClient:
    def __init__(self, broker_url):
        self.producer = Producer({"bootstrap.servers": broker_url})

    def send(self, topic, message):
        self.producer.produce(topic, json.dumps(message).encode("utf-8"))
        self.producer.flush()

    @staticmethod
    def get_consumer(broker_url, group_id, topic):
        consumer = Consumer(
            {
                "bootstrap.servers": broker_url,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([topic])
        return consumer
