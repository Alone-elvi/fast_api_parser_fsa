-- Создаем таблицу для хешей
CREATE TABLE IF NOT EXISTS certificate_hashes (
    id SERIAL PRIMARY KEY,
    certificate_id INTEGER REFERENCES certificates(id),
    hash_value VARCHAR(64) NOT NULL,  -- для SHA-256
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(hash_value)  -- Индекс для быстрого поиска дубликатов
);

-- Индекс для быстрого поиска по сертификату
CREATE INDEX IF NOT EXISTS idx_cert_hashes_cert_id ON certificate_hashes(certificate_id); 