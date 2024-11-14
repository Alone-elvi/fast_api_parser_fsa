import pytest
from ingestion_service.schemas import Certificate, CertificateData

def test_certificate_model():
    data = {
        "cert_number": "ЕАЭС RU C-RU.АБ07.В.00001/19",
        "cert_status": "Действует",
        "applicant_type": "Юридическое лицо",
        "applicant_name": "ООО Тест",
        "product_name": "Тестовый продукт"
    }
    
    cert = Certificate(**data)
    assert cert.cert_number == data["cert_number"]
    assert cert.cert_status == data["cert_status"]

def test_certificate_data_model():
    data = {
        "cert_number": "ЕАЭС RU C-RU.АБ07.В.00001/19",
        "cert_status": "Действует",
        "applicant_type": "Юридическое лицо",
        "applicant_name": "ООО Тест",
        "product_name": "Тестовый продукт",
        "manufacturer_name": "Производитель",
        "manufacturer_address": "ул. Тестовая, 1"
    }
    
    cert_data = CertificateData(**data)
    assert cert_data.cert_number == data["cert_number"]
    assert cert_data.cert_status == data["cert_status"] 