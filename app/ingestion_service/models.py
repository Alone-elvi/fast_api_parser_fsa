from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import date

class CertificateData(BaseModel):
    # Поля сертификата
    cert_number: str
    cert_status: Optional[str] = ''
    cert_registration_date: Optional[date] = None
    cert_expiration_date: Optional[date] = None
    cert_termination_date: Optional[date] = None
    cert_scheme: str
    
    # Поля заявителя
    applicant_inn: Optional[str] = "Отсутствует"
    applicant_type: Optional[str] = ''
    applicant_ogrn: Optional[str] = "Отсутствует"
    applicant_name: Optional[str] = "Не указано"
    applicant_short_name: Optional[str] = None
    applicant_address: str
    
    # Поля изготовителя
    manufacturer_inn: Optional[str] = "Отсутствует"
    manufacturer_name: Optional[str] = "Не указано"
    manufacturer_address: str
    manufacturer_production_sites: str
    
    # Поля продукции
    product_group: Optional[str] = None
    product_name: str
    product_doc_name: Optional[str] = None
    product_doc_number: Optional[str] = None
    
    # Поля испытательной лаборатории
    testing_lab_1_ral: str
    testing_lab_1_name: str
    testing_lab_2_ral: Optional[str] = Field(default='Отсутствует')
    testing_lab_2_name: Optional[str] = Field(default='Отсутствует')
    testing_lab_3_ral: Optional[str] = Field(default='Отсутствует')
    testing_lab_3_name: Optional[str] = Field(default='Отсутствует')
    
    # Поля органа по сертификации
    certification_body_ral: str
    certification_body_name: Optional[str] = None
    certification_body_ogrn: Optional[str] = "Отсутствует"
    certification_body_accreditation_status: str
    certification_body_accreditation_end_date: Optional[date] = None
    
    # Поля СМК
    qms_cert_number: Optional[str] = Field(default='Отсутствует')
    qms_certification_body_number: Optional[str] = Field(default='Отсутствует')
    qms_certification_body_name: Optional[str] = Field(default='Отсутствует')
    
    # Поля нарушений
    violation_certificate: Optional[str] = None
    violation_laboratory: Optional[str] = None
    violation_certification_body: Optional[str] = None
    violation_qms: Optional[str] = None

    @validator('certification_body_accreditation_end_date', pre=True)
    def parse_date(cls, value):
        if value == 'Отсутствует' or value is None:
            return None
        return value

    class Config:
        from_attributes = True 