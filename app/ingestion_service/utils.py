import pandas as pd
import numpy as np
import logging
from typing import Dict, Any
from .schemas import CertificateData
from .constants import FIELD_MAPPING

logger = logging.getLogger(__name__)

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Очищает и преобразует данные DataFrame."""
    try:
        df = df.replace({np.nan: None})
        
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        
        date_columns = ['Дата регистрации сертификата', 
                       'Дата окончания действия сертификата',
                       'Дата прекращения сертификата']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                df[col] = df[col].where(pd.notnull(df[col]), None)
        
        logger.debug(f"DataFrame cleaned. Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error in clean_dataframe: {str(e)}")
        raise

def process_excel_record(record: Dict[str, Any]) -> Dict[str, Any]:
    try:
        processed_data = {}
        
        for model_field, excel_field in FIELD_MAPPING.items():
            value = record.get(excel_field)
            
            if model_field in ['applicant_inn', 'applicant_ogrn', 'manufacturer_inn', 'certification_body_ogrn']:
                if isinstance(value, (int, float)):
                    value = str(int(value))
                elif value is None:
                    value = "Отсутствует"
            
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
    if data.get('Статус сертификата') is None:
        data['Статус сертификата'] = ''
    if data.get('Вид заявителя') is None:
        data['Вид заявителя'] = ''
    if data.get('Дата окончания/ приостановки аккредитации') == 'Отсутствует':
        data['Дата окончания/ приостановки аккредитации'] = None
    return data

def preprocess_product_group(data: dict) -> dict:
    if data.get('Группа продукции ЕАЭС') is None:
        data['Группа продукции ЕАЭС'] = 'Не указана'
    return data

def preprocess_accreditation_date(data: dict) -> dict:
    date_field = 'Дата окончания/ приостановки аккредитации'
    if data.get(date_field) == 'Отсутствует':
        data[date_field] = None
    return data 