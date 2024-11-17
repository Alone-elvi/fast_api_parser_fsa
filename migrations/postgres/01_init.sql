\c fsa_parser_db;

CREATE TABLE IF NOT EXISTS manufacturers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS applicants (
    id SERIAL PRIMARY KEY,
    address VARCHAR(255),
    inn VARCHAR(20),
    ogrn VARCHAR(20),
    full_name VARCHAR(255),
    type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(address, full_name)
);

CREATE TABLE IF NOT EXISTS certification_bodies (
    id SERIAL PRIMARY KEY,
    ral_number VARCHAR(50) NOT NULL,
    full_name VARCHAR(255),
    ogrn VARCHAR(20),
    accreditation_status VARCHAR(50),
    accreditation_end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ral_number)
);

CREATE TABLE IF NOT EXISTS certificates (
    id SERIAL PRIMARY KEY,
    registration_number VARCHAR(255) NOT NULL,
    eaeu_product_group VARCHAR(255),
    certification_scheme VARCHAR(255),
    status VARCHAR(50),
    product_name TEXT,
    registration_date DATE,
    expiration_date DATE,
    termination_date DATE,
    termination_reason VARCHAR(255),
    manufacturer_id INTEGER REFERENCES manufacturers(id),
    applicant_id INTEGER REFERENCES applicants(id),
    certification_body_id INTEGER REFERENCES certification_bodies(id),
    file_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(registration_number)
);