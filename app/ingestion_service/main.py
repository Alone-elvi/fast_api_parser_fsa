from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from io import BytesIO
from .validators import CertificateData
import logging
import uuid
from typing import Dict, Any
import traceback
import json
from fastapi.responses import JSONResponse
from datetime import date, datetime
from .constants import FIELD_MAPPING, DEFAULT_VALUES
from .models import CertificateData

# Настройка логирования
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает и преобразует данные DataFrame."""
    try:
        # Заменяем NaN на None
        df = df.replace({np.nan: None})
        
        # Очищаем строковые колонки
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        
        # Преобразуем даты
        date_columns = ['Дата регистрации сертификата', 
                       'Дата окончания действия сертификата',
                       'Дата прекращения сертификата']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                # Заменяем NaT на None
                df[col] = df[col].where(pd.notnull(df[col]), None)
        
        logger.debug(f"DataFrame cleaned. Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error in clean_dataframe: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def process_excel_record(record: Dict[str, Any]) -> Dict[str, Any]:
    try:
        processed_data = {}
        
        for model_field, excel_field in FIELD_MAPPING.items():
            value = record.get(excel_field)
            
            # Преобразование числовых полей в строки
            if model_field in ['applicant_inn', 'applicant_ogrn', 'manufacturer_inn', 'certification_body_ogrn']:
                if isinstance(value, (int, float)):
                    value = str(int(value))
                elif value is None:
                    value = "Отсутствует"
            
            # Обработка обязательных строковых полей
            elif model_field in ['applicant_name', 'manufacturer_name']:
                if value is None:
                    value = "Не указано"
                else:
                    value = str(value)
            
            processed_data[model_field] = value
            
        return CertificateData(**processed_data).dict()
        
    except Exception as e:
        logger.error(f"Ошибка обработки записи: {str(e)}")
        logger.error(f"Исходные данные: {record}")
        raise

def preprocess_certificate_data(data: dict) -> dict:
    # Обработк строковых полей
    if data.get('Статус сертификата') is None:
        data['Статус сертификата'] = ''
        
    if data.get('Вид заявителя') is None:
        data['Вид заявителя'] = ''
        
    # Обработка даты
    if data.get('Дата окончания/ приостановки аккредитации') == 'Отсутствует':
        data['Дата окончания/ приостановки аккредитации'] = None
        
    return data

def preprocess_product_group(data: dict) -> dict:
    if data.get('Группа продукции ЕАЭС') is None:
        # Варианты обработки:
        data['Группа продукции ЕАЭС'] = 'Не указана'  # Значение по умолчанию
        # или
        # data['Группа продукции ЕАЭС'] = ''  # Пустая строка
    return data

def preprocess_accreditation_date(data: dict) -> dict:
    date_field = 'Дата окончания/ приостановки аккредитации'
    
    if data.get(date_field) == 'Отсутствует':
        data[date_field] = None
        
    return data

@app.post("/upload")
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
                
                # Заменяем NaN на None в записи
                record = {k: None if pd.isna(v) else v for k, v in record.items()}
                
                mapped_record = process_excel_record(record)
                logger.debug(f"Mapped record {idx}: {mapped_record}")
                
                validated_record = CertificateData(**mapped_record)
                validated_records.append(validated_record)
                logger.debug(f"Record {idx} validated successfully")
                
            except Exception as e:
                logger.error(f"Error processing record {idx}: {str(e)}")
                errors.append({
                    "row": idx,
                    "error": str(e),
                    "data": {k: None if pd.isna(v) else v for k, v in record.items()}
                })
        
        response_data = {
            "message": "File processed successfully",
            "file_id": file_id,
            "total_rows": len(df),
            "processed_rows": len(validated_records),
            "error_count": len(errors)
        }
        
        if errors:
            response_data["errors"] = errors[:5]
            
        # Сериализуем данные с помощью нашего CustomJSONEncoder
        json_str = json.dumps(response_data, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        logger.error(traceback.format_exc())
        error_response = {"error": f"Error processing file: {str(e)}"}
        json_str = json.dumps(error_response, cls=CustomJSONEncoder)
        return JSONResponse(content=json.loads(json_str))

@app.get("/health")
async def health_check():
    return {"status": "healthy"}
