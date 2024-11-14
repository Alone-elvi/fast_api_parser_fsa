import pytest
from unittest.mock import Mock, patch
from ingestion_service.kafka_producer import KafkaProducer
from confluent_kafka import KafkaError

@pytest.fixture
def mock_producer():
    with patch('confluent_kafka.Producer') as mock:
        yield mock

def test_kafka_producer_init(mock_producer):
    producer = KafkaProducer('localhost:9092')
    assert producer._producer is not None

def test_kafka_producer_send_success(mock_producer):
    producer = KafkaProducer('localhost:9092')
    
    # Мокаем успешную отправку
    future = Mock()
    future.get.return_value = None
    mock_producer.return_value.produce.return_value = future
    
    result = producer.send('test-topic', 'test-message')
    assert result is True

def test_kafka_producer_send_failure(mock_producer):
    producer = KafkaProducer('localhost:9092')
    
    # Мокаем ошибку отправки
    mock_producer.return_value.produce.side_effect = KafkaError('Test error')
    
    result = producer.send('test-topic', 'test-message')
    assert result is False

def test_kafka_producer_close(mock_producer):
    producer = KafkaProducer('localhost:9092')
    producer.close()
    mock_producer.return_value.flush.assert_called_once()

@pytest.mark.asyncio
async def test_kafka_producer_async_send_success(mock_producer):
    producer = KafkaProducer('localhost:9092')
    
    # Мокаем успешную асинхронную отправку
    future = Mock()
    future.get.return_value = None
    mock_producer.return_value.produce.return_value = future
    
    result = await producer.async_send('test-topic', 'test-message')
    assert result is True

@pytest.mark.asyncio
async def test_kafka_producer_async_send_failure(mock_producer):
    producer = KafkaProducer('localhost:9092')
    
    # Мокаем ошибку асинхронной отправки
    mock_producer.return_value.produce.side_effect = KafkaError('Test error')
    
    result = await producer.async_send('test-topic', 'test-message')
    assert result is False 