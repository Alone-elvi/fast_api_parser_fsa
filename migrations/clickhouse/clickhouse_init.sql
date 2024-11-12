-- Создаем базу если её нет
CREATE DATABASE IF NOT EXISTS analytics_db;

-- Используем базу
USE analytics_db;

-- Основная таблица для аналитики
CREATE TABLE IF NOT EXISTS certificates_analytics
(
    id UUID DEFAULT generateUUIDv4(),
    eaeu_product_group String,
    registration_number String,
    status String,
    registration_date Date,
    expiration_date Date,
    termination_date Nullable(Date),
    applicant_inn String,
    manufacturer_inn String,
    certification_body_ral String,
    has_violations UInt8,
    year UInt16 MATERIALIZED toYear(registration_date),
    month UInt8 MATERIALIZED toMonth(registration_date),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (registration_date, id)
PARTITION BY toYYYYMM(registration_date);

-- Материализованное представление для агрегированной статистики
CREATE MATERIALIZED VIEW IF NOT EXISTS certificates_monthly_stats
ENGINE = SummingMergeTree()
ORDER BY (year, month, eaeu_product_group)
AS SELECT
    year,
    month,
    eaeu_product_group,
    count() as total_certificates,
    countIf(has_violations = 1) as certificates_with_violations,
    uniqExact(applicant_inn) as unique_applicants
FROM certificates_analytics
GROUP BY year, month, eaeu_product_group; 