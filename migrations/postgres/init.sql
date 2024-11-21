\c fsa_parser_db;

-- Создаем схему и расширения
CREATE SCHEMA IF NOT EXISTS public;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Создаем таблицы с ограничениями сразу
CREATE TABLE IF NOT EXISTS manufacturers (
    id SERIAL PRIMARY KEY,
    manufacturer_name VARCHAR(255) NOT NULL,
    manufacturer_inn VARCHAR(20) UNIQUE,
    manufacturer_address VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS applicants (
    id SERIAL PRIMARY KEY,
    applicant_name VARCHAR(255),
    applicant_type VARCHAR(50),
    applicant_inn VARCHAR(20) UNIQUE,
    applicant_ogrn VARCHAR(20),
    applicant_address VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS certification_bodies (
    id SERIAL PRIMARY KEY,
    certification_body_number VARCHAR(50) NOT NULL UNIQUE,
    certification_body_name VARCHAR(255),
    certification_body_ogrn VARCHAR(20),
    certification_body_accreditation_status VARCHAR(50),
    certification_body_accreditation_end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS certificates (
    id SERIAL PRIMARY KEY,
    cert_number VARCHAR(255) NOT NULL UNIQUE,
    product_group VARCHAR(255),
    certification_scheme VARCHAR(255),
    cert_status VARCHAR(50),
    product_name TEXT,
    cert_registration_date DATE,
    cert_expiration_date DATE,
    cert_termination_date DATE,
    termination_reason VARCHAR(255),
    manufacturer_id INTEGER REFERENCES manufacturers(id),
    applicant_id INTEGER REFERENCES applicants(id),
    certification_body_id INTEGER REFERENCES certification_bodies(id),
    file_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS certificate_hashes (
    id SERIAL PRIMARY KEY,
    certificate_id INTEGER REFERENCES certificates(id),
    hash_value VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS validation_failures (
    id SERIAL PRIMARY KEY,
    registration_number VARCHAR(255),
    error_type VARCHAR(50),
    error_details JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS operation_logs (
    id SERIAL PRIMARY KEY,
    operation_type VARCHAR(50) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id INTEGER NOT NULL,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Комментарии к таблицам
COMMENT ON TABLE manufacturers IS 'Производители сертифицированной продукции';
COMMENT ON COLUMN manufacturers.name IS 'Наименование производителя';
COMMENT ON COLUMN manufacturers.inn IS 'ИНН производителя';
COMMENT ON COLUMN manufacturers.address IS 'Адрес производителя';
COMMENT ON COLUMN manufacturers.ogrn IS 'ОГРН производителя';

-- Создаем индексы для оптимизии
CREATE INDEX IF NOT EXISTS manufacturers_ogrn_idx ON manufacturers(ogrn);
CREATE INDEX IF NOT EXISTS manufacturers_address_idx ON manufacturers(address);
CREATE INDEX IF NOT EXISTS applicants_ogrn_idx ON applicants(ogrn);
CREATE INDEX IF NOT EXISTS applicants_type_idx ON applicants(type);
CREATE INDEX IF NOT EXISTS certificates_dates_idx ON certificates(cert_registration_date, cert_expiration_date);
CREATE INDEX IF NOT EXISTS certificates_status_idx ON certificates(cert_status);
CREATE INDEX IF NOT EXISTS manufacturers_name_idx ON manufacturers(manufacturer_name);

-- Создаем триггеры
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_manufacturers_updated_at
    BEFORE UPDATE ON manufacturers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

ALTER TABLE certificates ADD CONSTRAINT unique_cert_number UNIQUE (cert_number);
ALTER TABLE manufacturers ADD CONSTRAINT unique_manufacturer_name UNIQUE (manufacturer_name);
ALTER TABLE manufacturers ADD CONSTRAINT unique_manufacturer_inn UNIQUE (manufacturer_inn);

CREATE UNIQUE INDEX manufacturers_inn_idx ON manufacturers(manufacturer_inn);

ALTER TABLE manufacturers DROP CONSTRAINT manufacturers_manufacturer_name_key;
