from datetime import date
from typing import Optional


# Базовые модели данных
class Certificate:
    def __init__(
        self,
        cert_number: str,
        cert_status: str,
        cert_registration_date: Optional[date] = None,
        cert_expiration_date: Optional[date] = None,
        cert_termination_date: Optional[date] = None,
        termination_reason: Optional[str] = None,
        certification_scheme: str = "",
        product_name: str = "",
        product_group: Optional[str] = None,
        applicant_type: str = "",
        applicant_name: str = "",
        applicant_inn: Optional[str] = None,
        applicant_ogrn: Optional[str] = None,
        applicant_address: str = "",
        manufacturer_name: str = "",
        manufacturer_inn: Optional[str] = None,
        manufacturer_address: str = "",
        certification_body_number: str = "",
        certification_body_name: str = "",
        certification_body_ogrn: Optional[str] = None,
        certification_body_accreditation_status: str = "",
        certification_body_accreditation_end_date: Optional[date] = None,
    ):
        self.cert_number = cert_number
        self.cert_status = cert_status
        self.cert_registration_date = cert_registration_date
        self.cert_expiration_date = cert_expiration_date
        self.cert_termination_date = cert_termination_date
        self.termination_reason = termination_reason
        self.certification_scheme = certification_scheme
        self.product_name = product_name
        self.product_group = product_group
        self.applicant_type = applicant_type
        self.applicant_name = applicant_name
        self.applicant_inn = applicant_inn
        self.applicant_ogrn = applicant_ogrn
        self.applicant_address = applicant_address
        self.manufacturer_name = manufacturer_name
        self.manufacturer_inn = manufacturer_inn
        self.manufacturer_address = manufacturer_address
        self.certification_body_number = certification_body_number
        self.certification_body_name = certification_body_name
        self.certification_body_ogrn = certification_body_ogrn
        self.certification_body_accreditation_status = (
            certification_body_accreditation_status
        )
        self.certification_body_accreditation_end_date = (
            certification_body_accreditation_end_date
        )
