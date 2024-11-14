import pytest
from unittest.mock import Mock, patch
from confluent_kafka import KafkaError
import json
from ingestion_service.base_routes import send_records_batch

@pytest.fixture
def mock_producer():
    producer = Mock()
    producer.produce.return_value = None
    producer.flush.return_value = 0
    return producer

def test_send_records_batch(mock_producer):
    records = [
        {
            "cert_number": "TEST-123",
            "cert_status": "Действует",
            "applicant_type": "Юридическое лицо"
        }
    ]
    file_id = "test-file-id"
    
    send_records_batch(mock_producer, 'test-topic', records, file_id)
    
    mock_producer.produce.assert_called_once()
    mock_producer.flush.assert_called_once()

def test_send_records_batch_error(mock_producer):
    mock_producer.produce.side_effect = KafkaError("Test error")
    
    records = [{"cert_number": "TEST-123"}]
    file_id = "test-file-id"
    
    with pytest.raises(Exception):
        send_records_batch(mock_producer, 'test-topic', records, file_id) 