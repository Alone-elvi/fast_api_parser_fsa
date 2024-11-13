from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import date

class CertificateData(BaseModel):
    # Основные данные о сертификате
    eaeu_product_group: str
    has_violations: bool = False
    url: Optional[str] = None
    registration_number: str = Field(..., description="Номер сертификата")
    certificate_type: Optional[str] = None
    status: str
    registration_date: date
    expiration_date: date
    termination_date: Optional[date] = None
    
    # Данные заявителя
    applicant_inn: Optional[str] = None
    applicant_type: str
    applicant_ogrn: Optional[str] = None
    applicant_name: Optional[str] = None
    applicant_short_name: Optional[str] = None
    applicant_address: str
    
    # Данные производителя
    manufacturer_inn: Optional[str] = None
    manufacturer_name: Optional[str] = None
    manufacturer_address: str
    manufacturer_ogrn: Optional[str] = Field(None, pattern=r'^\d{13}|\d{15}$')
    manufacturer_production_site: Optional[str] = None
    
    # Данные о продукции
    product_name: str
    product_document_name: Optional[str] = None
    test_standards: Optional[str] = None
    
    # Данные испытательной лаборатории
    testing_lab_ral: str = Field(..., min_length=1)
    testing_lab_name: str
    testing_lab_violations: Optional[str] = None
    
    # Данные о СМК
    qms_certificate_number: Optional[str] = None
    qms_certification_body_number: Optional[str] = None
    qms_certification_body_name: Optional[str] = None
    qms_violations: Optional[str] = None
    
    # Данные органа сертификации
    certification_body_ral: str = Field(..., min_length=1)
    certification_body_name: str
    certification_body_ogrn: Optional[str] = Field(None, pattern=r'^\d{13}|\d{15}$')
    certification_body_status: Optional[str] = None
    certification_body_violations: Optional[str] = None

    @validator('applicant_inn')
    def validate_inn(cls, v):
        if v is None:
            return v
        if isinstance(v, (int, float)):
            v = str(int(v))
        # Дополняем нулями слева до 10 или 12 знаков
        if len(v) < 10:
            v = v.zfill(10)
        elif len(v) < 12:
            v = v.zfill(12)
        return v

    @validator('applicant_ogrn')
    def validate_ogrn(cls, v):
        if v is None:
            return v
        if isinstance(v, (int, float)):
            v = str(int(v))
        return v

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "eaeu_product_group": "Строительные материалы",
                "has_violations": True,
                "registration_number": "RU С-RU.НВ77.В.0006124",
                "status": "Действует",
                "registration_date": "2024-11-05",
                "expiration_date": "2029-11-04",
                "applicant_inn": "6670183223",
                "applicant_type": "Изготовитель",
                "applicant_name": "ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ ВСЕ У НАС",
                "manufacturer_inn": "6670183223",
                "manufacturer_name": "ООО ВСЕ У НАС",
                "certification_body_ral": "RA.RU.11НВ77"
            }
        }