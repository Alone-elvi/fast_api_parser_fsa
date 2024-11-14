import pytest
from ingestion_service.schemas import Certificate

def test_certificate_validation():
    # Тест валидации сертификата с корректными данными
    valid_data = {
        "cert_number": "ЕАЭС RU C-RU.АБ07.В.00001/19",
        "cert_status": "Действует",
        "applicant_type": "Юридическое лицо",
        "applicant_name": "ООО Тест",
        "applicant_inn": "1234567890",
        "applicant_ogrn": "1234567890123",
        "product_name": "Тестовый продукт",
        "manufacturer_name": "Производитель",
        "manufacturer_address": "ул. Тестовая, 1"
    }
    
    cert = Certificate(**valid_data)
    assert cert.cert_number == valid_data["cert_number"]
    assert cert.cert_status == valid_data["cert_status"]

def test_certificate_validation_invalid_data():
    # Тест валидации с некорректными данными
    invalid_data = {
        "cert_number": "invalid",
        "cert_status": "invalid",
        "applicant_type": "invalid",
        "applicant_inn": "invalid",
        "applicant_ogrn": "invalid"
    }
    
    with pytest.raises(ValueError):
        Certificate(**invalid_data) 