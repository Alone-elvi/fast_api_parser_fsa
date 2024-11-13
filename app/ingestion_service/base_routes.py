from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from io import BytesIO
import logging
import uuid
import json
import traceback
from datetime import date
from typing import Dict, Any, List
from confluent_kafka import Producer
from common.config import KAFKA_BOOTSTRAP_SERVERS

from .models import Certificate
from .schemas import CertificateData, FileProcessingResponse, ProcessingError
from .constants import FIELD_MAPPING
from .utils import clean_dataframe, process_excel_record

logger = logging.getLogger(__name__)

router = APIRouter()

# Конфигурация Kafka producer
producer_config = {
    'bootstrap.servers': 'kafka:29092',  # Используем внутренний адрес и порт
    'client.id': 'ingestion_service',
    'message.max.bytes': 52428800,  # 50MB
    'compression.type': 'gzip'  # Добавляем сжатие
}

# Инициализация Kafka producer
producer = Producer(producer_config)

def delivery_report(err, msg):
    """Callback для отслеживания доставки сообщений"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating)):
            return int(obj) if isinstance(obj, np.integer) else float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif pd.isna(obj):
            return None
        elif isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)

BATCH_SIZE = 1000  # Размер пакета для отправки

def send_records_batch(producer: Producer, topic: str, records: List[Dict], file_id: str):
    """Отправка пакета записей в Kafka"""
    batch_data = {
        "file_id": file_id,
        "records": records
    }
    message = json.dumps(batch_data, cls=CustomJSONEncoder)
    
    try:
        producer.produce(
            topic,
            value=message.encode('utf-8'),
            callback=delivery_report
        )
        producer.flush()
        logger.info(f"Sent batch of {len(records)} records to Kafka")
    except Exception as e:
        logger.error(f"Error sending batch to Kafka: {str(e)}")
        raise

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_id = str(uuid.uuid4())
        logger.info(f"Processing file with ID: {file_id}")
        
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
        logger.info(f"File read successfully. Shape: {df.shape}")
        
        df = clean_dataframe(df)
        records = df.to_dict('records')
        logger.info(f"Converted to {len(records)} records")
        
        validated_records = []
        errors = []
        
        for idx, record in enumerate(records, 1):
            try:
                if all(pd.isna(v) for v in record.values()):
                    continue
                
                record = {k: None if pd.isna(v) else v for k, v in record.items()}
                
                mapped_record = process_excel_record(record)
                logger.debug(f"Mapped record {idx}: {mapped_record}")
                
                validated_record = CertificateData(**mapped_record)
                validated_records.append(validated_record.dict())
                logger.debug(f"Record {idx} validated successfully")
                
            except Exception as e:
                logger.error(f"Error processing record {idx}: {str(e)}")
                errors.append({
                    "row": idx,
                    "error": str(e),
                    "data": {k: None if pd.isna(v) else v for k, v in record.items()}
                })
        
        # Отправляем записи пакетами
        for i in range(0, len(validated_records), BATCH_SIZE):
            batch = validated_records[i:i + BATCH_SIZE]
            send_records_batch(producer, 'validated_certificates', batch, file_id)
            
        response_data = {
            "message": "File processed successfully",
            "file_id": file_id,
            "total_rows": len(df),
            "processed_rows": len(validated_records),
            "error_count": len(errors)
        }
        
        if errors:
            response_data["errors"] = errors[:5]
            
        json_str = json.dumps(response_data, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        logger.error(traceback.format_exc())
        error_response = {"error": f"Error processing file: {str(e)}"}
        json_str = json.dumps(error_response, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

@router.get("/health")
async def health_check():
    return {"status": "healthy"} 