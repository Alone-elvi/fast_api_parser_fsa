-- Таблица заявителей
CREATE TABLE IF NOT EXISTS applicants (
    id SERIAL PRIMARY KEY,
    inn VARCHAR(12),
    type VARCHAR(100),  -- Вид заявителя
    ogrn VARCHAR(15),
    full_name TEXT,
    short_name VARCHAR(255),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица изготовителей
CREATE TABLE IF NOT EXISTS manufacturers (
    id SERIAL PRIMARY KEY,
    inn VARCHAR(12),
    full_name TEXT,
    address TEXT,
    ogrn VARCHAR(15),
    production_sites TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица органов сертификации
CREATE TABLE IF NOT EXISTS certification_bodies (
    id SERIAL PRIMARY KEY,
    ral_number VARCHAR(100),  -- Номер записи в РАЛ
    full_name TEXT,
    ogrn VARCHAR(15),
    accreditation_status VARCHAR(50),
    accreditation_end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица испытательных лабораторий
CREATE TABLE IF NOT EXISTS testing_labs (
    id SERIAL PRIMARY KEY,
    ral_number VARCHAR(100),
    name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Основная таблица сертификатов
CREATE TABLE IF NOT EXISTS certificates (
    id SERIAL PRIMARY KEY,
    eaeu_product_group TEXT,  -- Группа продукции ЕАЭС
    apts_comment TEXT,        -- Комментарий АПТС
    certificate_link TEXT,    -- Ссылка на сертификат
    registration_number VARCHAR(100),
    certification_scheme VARCHAR(50),
    status VARCHAR(50),
    registration_date DATE,
    expiration_date DATE,
    termination_date DATE,
    termination_reason TEXT,
    
    -- Внешние ключи
    applicant_id INTEGER REFERENCES applicants(id),
    manufacturer_id INTEGER REFERENCES manufacturers(id),
    certification_body_id INTEGER REFERENCES certification_bodies(id),
    
    -- Информация о продукции
    product_name TEXT,
    document_name TEXT,
    document_number VARCHAR(100),
    
    -- Информация о СМК
    qms_certificate_number VARCHAR(100),
    qms_certification_body_number VARCHAR(100),
    qms_certification_body_name TEXT,
    
    -- Документы и нарушения
    submitted_documents TEXT,
    certificate_violations TEXT,
    lab_violations TEXT,
    certification_body_violations TEXT,
    qms_violations TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Связующая таблица для сертификатов и лабораторий
CREATE TABLE IF NOT EXISTS certificate_labs (
    certificate_id INTEGER REFERENCES certificates(id),
    lab_id INTEGER REFERENCES testing_labs(id),
    PRIMARY KEY (certificate_id, lab_id)
);

-- Индексы
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_certificates_registration_date') THEN
        CREATE INDEX idx_certificates_registration_date ON certificates(registration_date);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_certificates_status') THEN
        CREATE INDEX idx_certificates_status ON certificates(status);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_applicants_inn') THEN
        CREATE INDEX idx_applicants_inn ON applicants(inn);
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_manufacturers_inn') THEN
        CREATE INDEX idx_manufacturers_inn ON manufacturers(inn);
    END IF;
END $$;