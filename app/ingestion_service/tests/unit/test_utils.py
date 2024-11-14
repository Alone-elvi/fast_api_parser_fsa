import pytest
from datetime import date
import pandas as pd
import numpy as np
from ingestion_service.utils import clean_dataframe, process_excel_record
from ingestion_service.constants import FIELD_MAPPING

def test_clean_dataframe():
    # Тест базовой очистки
    data = {
        'Номер': [1, 2, np.nan],
        'Название': ['a', 'b', 'c']
    }
    df = pd.DataFrame(data)
    cleaned_df = clean_dataframe(df)
    
    assert len(cleaned_df) == 3
    assert 'Номер' in cleaned_df.columns
    assert 'Название' in cleaned_df.columns

def test_process_excel_record():
    # Создаем тестовые данные с обязательными полями из схемы
    record = {
        FIELD_MAPPING['cert_number']: 'TEST-123',
        FIELD_MAPPING['cert_status']: 'Действует',
        FIELD_MAPPING['certification_scheme']: 'Схема 1с',
        FIELD_MAPPING['product_name']: 'Тестовый продукт',
        FIELD_MAPPING['applicant_type']: 'Юридическое лицо',
        FIELD_MAPPING['applicant_address']: 'ул. Тестовая, 1',
        FIELD_MAPPING['manufacturer_address']: 'ул. Производственная, 1',
        FIELD_MAPPING['certification_body_number']: 'RA.RU.11АБ87',
        FIELD_MAPPING['certification_body_name']: 'ООО "Тест-Сертификация"',
        FIELD_MAPPING['cert_expiration_date']: '2024-02-14'
    }
    
    processed = process_excel_record(record)
    
    # Проверяем базовую структуру
    assert isinstance(processed, dict)
    assert processed['cert_number'] == 'TEST-123'
    assert processed['cert_status'] == 'Действует'
    assert processed['cert_expiration_date'] == date(2024, 2, 14)

def test_process_excel_record_with_missing_fields():
    # Тест обработки записи с отсутствующими полями
    record = {
        FIELD_MAPPING['cert_number']: 'TEST-123'
    }
    
    processed = process_excel_record(record)
    
    assert processed['cert_number'] == 'TEST-123'
    assert processed.get('applicant_type') == 'Не указано'
    assert processed.get('cert_expiration_date') is None