from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, KafkaError
import json
import time
import sys

def create_topic(admin_client, topic_name, num_partitions=1, replication_factor=1):
    """Создание топика если он не существует"""
    try:
        new_topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        futures = admin_client.create_topics([new_topic])
        
        for topic, future in futures.items():
            try:
                future.result()
                print(f"Topic '{topic}' created successfully")
                return True
            except Exception as e:
                if "already exists" in str(e):
                    print(f"Topic '{topic}' already exists")
                    return True
                print(f"Failed to create topic '{topic}': {e}")
                return False
    except Exception as e:
        print(f"Error creating topic: {e}")
        return False

def list_topics(admin_client):
    """Получение списка топиков"""
    try:
        topics = admin_client.list_topics(timeout=10)
        print("\nAvailable topics:")
        for topic in topics.topics.keys():
            print(f"- {topic}")
        return topics.topics.keys()
    except Exception as e:
        print(f"Error listing topics: {e}")
        return []

print("Starting Kafka checker... Press Ctrl+C to exit")

# Конфигурация для AdminClient и Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',  # Используем внешний порт
    'group.id': 'checker_group',
    'auto.offset.reset': 'earliest'
}

try:
    # Создаем AdminClient для проверки топиков
    print("\nConnecting to Kafka...")
    admin = AdminClient(conf)
    
    # Получаем список топиков
    topics = list_topics(admin)
    
    if 'validated_certificates' not in topics:
        print("\nCreating topic 'validated_certificates'...")
        if not create_topic(admin, 'validated_certificates'):
            print("Failed to create topic")
            sys.exit(1)

    # Создаем consumer
    print("\nCreating consumer...")
    consumer = Consumer(conf)
    consumer.subscribe(['validated_certificates'])
    print("Subscribed to 'validated_certificates'")

    print("\nWaiting for messages... (Press Ctrl+C to exit)")
    message_count = 0
    
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            print(".", end="", flush=True)
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("\nReached end of partition")
            else:
                print(f"\nConsumer error: {msg.error()}")
            continue
            
        try:
            value = msg.value().decode('utf-8')
            data = json.loads(value)
            message_count += 1
            print(f"\nMessage #{message_count}:")
            print(json.dumps(data, indent=2))
        except Exception as e:
            print(f"\nError processing message: {e}")
            print(f"Raw message: {msg.value()}")

except KeyboardInterrupt:
    print("\nShutting down by user request...")
except Exception as e:
    print(f"\nUnexpected error: {e}")
finally:
    try:
        consumer.close()
        print("Consumer closed")
    except:
        pass 