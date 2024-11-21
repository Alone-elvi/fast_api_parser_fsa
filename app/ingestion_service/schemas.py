from pydantic import BaseModel, Field, field_validator
from datetime import date
from typing import Optional, List, Any


class CertificateData(BaseModel):
    # Поля с датами теперь принимают date напрямую
    cert_registration_date: Optional[date] = None
    cert_expiration_date: Optional[date] = None
    cert_termination_date: Optional[date] = None
    certification_body_accreditation_end_date: Optional[date] = None

    # Основные поля сертификата
    cert_number: str = Field(default="Не указано")
    cert_status: str = Field(default="Не указано")
    termination_reason: Optional[str] = Field(default=None)
    certification_scheme: str = Field(default="Не указано")
    product_name: str = Field(default="Не указано")
    product_group: Optional[str] = Field(default=None)

    # Поля заявителя
    applicant_type: str = Field(default="Не указано")
    applicant_name: str = Field(default="Не указано")
    applicant_inn: Optional[str] = Field(default=None)
    applicant_ogrn: Optional[str] = Field(default=None)
    applicant_address: str = Field(default="Не указано")

    # Поля производителя
    manufacturer_name: str = Field(default="Не указано")
    manufacturer_inn: Optional[str] = Field(default=None)
    manufacturer_address: str = Field(default="Не указано")

    # Поля органа сертификации
    certification_body_number: str = Field(default="Не указано")
    certification_body_name: str = Field(default="Не указано")
    certification_body_ogrn: Optional[str] = Field(default=None)
    certification_body_accreditation_status: str = Field(default="Не указано")

    @field_validator(
        "cert_registration_date",
        "cert_expiration_date",
        "cert_termination_date",
        "certification_body_accreditation_end_date",
        mode="before",
    )
    @classmethod
    def validate_dates(cls, value: Any) -> Optional[date]:
        if isinstance(value, date):
            return value
        if (
            value is None
            or value == "Не указано"
            or value == ""
            or value == "Отсутствует"
        ):
            return None
        try:
            if isinstance(value, str):
                return date.fromisoformat(value)
        except ValueError:
            return None
        return value

    @field_validator("*", mode="before")
    @classmethod
    def replace_none_with_default(cls, value: Any, info) -> Any:
        # Пропускаем поля с датами и опциональные поля
        if info.field_name in [
            "cert_registration_date",
            "cert_expiration_date",
            "cert_termination_date",
            "certification_body_accreditation_end_date",
            "termination_reason",
            "product_group",
            "applicant_inn",
            "applicant_ogrn",
            "manufacturer_inn",
            "certification_body_ogrn"
        ]:
            return value
        if value is None:
            return "Не указано"
        return value

    model_config = {"from_attributes": True}


class ProcessingError(BaseModel):
    row: int
    error: str
    data: dict


class FileProcessingResponse(BaseModel):
    message: str
    file_id: str
    total_rows: int
    processed_rows: int
    error_count: int
    errors: Optional[List[ProcessingError]] = None
