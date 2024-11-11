from fastapi import FastAPI, UploadFile
from common.kafka import KafkaClient
from common.config import KAFKA_BROKER_URL
import pandas as pd
import uuid

app = FastAPI(
    title="Ingestion Service",
    description="Service for uploading and processing Excel files",
    version="1.0.0",
)

kafka_client = KafkaClient(KAFKA_BROKER_URL)


@app.post("/upload", tags=["File Upload"])
async def upload_file(file: UploadFile):
    """
    Загрузка файла Excel и отправка данных в Kafka.
    """
    df = pd.read_excel(file.file)
    for _, row in df.iterrows():
        kafka_client.send("parsed_data", row.to_dict())
    return {"message": "File processed successfully", "file_id": str(uuid.uuid4())}
