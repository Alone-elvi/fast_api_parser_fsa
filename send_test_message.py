try:
    from confluent_kafka import Producer
except ImportError:
    print("Please install confluent-kafka first:")
    print("pip install confluent-kafka")
    exit(1)

import json
import sys

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

print("Sending test message to Kafka...")

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'test_producer'
}

try:
    producer = Producer(conf)

    test_message = {
        "file_id": "test123",
        "records": [
            {
                "id": 1,
                "name": "Test Record",
                "timestamp": "2024-03-13T12:00:00"
            }
        ]
    }

    print("Connecting to Kafka...")
    producer.produce(
        'validated_certificates',
        value=json.dumps(test_message).encode('utf-8'),
        callback=delivery_report
    )
    
    print("Flushing messages...")
    producer.flush(timeout=10)

except Exception as e:
    print(f"Error sending message: {e}")
    sys.exit(1) 