from typing import Dict

# Маппинг полей из Excel в поля модели
FIELD_MAPPING: Dict[str, str] = {
    # Поля сертификата
    'cert_number': 'Регистрационный номер сертификата',
    'cert_status': 'Статус сертификата',
    'cert_registration_date': 'Дата регистрации сертификата',
    'cert_expiration_date': 'Дата окончания действия сертификата',
    'cert_termination_date': 'Дата прекращения сертификата',
    'termination_reason': 'Причина прекращения',
    'certification_scheme': 'Схема сертификации',
    
    # Поля заявителя
    'applicant_inn': 'ИНН заявителя',
    'applicant_type': 'Вид заявителя',
    'applicant_ogrn': 'Основной государственный регистрационный номер юридического лица',
    'applicant_name': 'Полное наименование',
    'applicant_short_name': 'Сокращенное наименование',
    'applicant_address': 'Адрес места нахождения',
    
    # Поля изготовителя
    'manufacturer_inn': 'ИНН изготовителя',
    'manufacturer_name': 'Полное наименование2',
    'manufacturer_address': 'Адрес места нахождения2',
    'manufacturer_production_sites': 'Производственные площадки',
    
    # Поля продукции
    'product_group': 'Группа продукции ЕАЭС',
    'product_name': 'Общее наименование продукции',
    'product_doc_name': 'Наименование документа_',
    'product_doc_number': 'Номер документа_',
    
    # Поля испытательной лаборатории
    'testing_lab_1_ral': 'Номер записи в РАЛ испытательной лаборатории_1',
    'testing_lab_1_name': 'Наименование испытательной лаборатории_1',
    'testing_lab_2_ral': 'Номер записи в РАЛ испытательной лаборатории_2',
    'testing_lab_2_name': 'Наименование испытательной лаборатории_2',
    'testing_lab_3_ral': 'Номер записи в РАЛ испытательной лаборатории_3',
    'testing_lab_3_name': 'Наименование испытательной лаборатории_3',
    
    # Поля органа по сертификации
    'certification_body_ral': 'Номер записи в РАЛ органа по сертификации',
    'certification_body_name': 'Полное наименование органа по сертификации',
    'certification_body_ogrn': 'ОГРН_ОС',
    'certification_body_accreditation_status': 'Статус аккредитации на момент проверки',
    'certification_body_accreditation_end_date': 'Дата окончания/ приостановки аккредитации',
    'certification_body_number': 'Номер записи в РАЛ органа по сертификации',
    
    # Поля СМК
    'qms_cert_number': 'Номер_СС_СМК',
    'qms_certification_body_number': 'Номер_ОС_СМК',
    'qms_certification_body_name': 'Наименование_ОС_СМК',
    
    # Поля нарушений
    'violation_certificate': 'Нарушение в сертификате',
    'violation_laboratory': 'Нарушение у лаборатории',
    'violation_certification_body': 'Нарушение у ОС',
    'violation_qms': 'Нарушения по СМК'
}

# Значения по умолчанию для отсутствующих полей
DEFAULT_VALUES = {
    'cert_termination_date': None,
    'applicant_short_name': None,
    'testing_lab_2_ral': 'Отсутствует',
    'testing_lab_2_name': 'Отсутствует',
    'testing_lab_3_ral': 'Отсутствует',
    'testing_lab_3_name': 'Отсутствует',
    'qms_cert_number': 'Отсутствует',
    'qms_certification_body_number': 'Отсутствует',
    'qms_certification_body_name': 'Отсутствует'
}