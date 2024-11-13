from pydantic import BaseModel, Field, field_validator
from datetime import date
from typing import Optional, List, Any


class CertificateData(BaseModel):
    # Поля с датами теперь принимают date напрямую
    cert_registration_date: Optional[date] = None
    cert_expiration_date: Optional[date] = None
    cert_termination_date: Optional[date] = None
    certification_body_accreditation_end_date: Optional[date] = None

    # Остальные поля
    cert_number: str = Field(default="Не указано")
    cert_status: str = Field(default="Не указано")
    certification_scheme: str = Field(default="Не указано")
    product_name: str = Field(default="Не указано")
    applicant_type: str = Field(default="Не указано")
    applicant_address: str = Field(default="Не указано")
    manufacturer_address: str = Field(default="Не указано")
    certification_body_number: str = Field(default="Не указано")
    certification_body_name: str = Field(default="Не указано")
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
        # Пропускаем поля с датами
        if info.field_name in [
            "cert_registration_date",
            "cert_expiration_date",
            "cert_termination_date",
            "certification_body_accreditation_end_date",
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
