-- Добавляем UNIQUE ограничения для существующих таблиц
ALTER TABLE applicants 
    ADD CONSTRAINT applicants_inn_unique UNIQUE (inn);

ALTER TABLE manufacturers 
    ADD CONSTRAINT manufacturers_inn_unique UNIQUE (inn);

ALTER TABLE certification_bodies 
    ADD CONSTRAINT certification_bodies_ral_unique UNIQUE (ral_number);

ALTER TABLE certificates 
    ADD CONSTRAINT certificates_registration_number_unique UNIQUE (registration_number);

ALTER TABLE certificate_hashes 
    ADD CONSTRAINT certificate_hashes_hash_value_unique UNIQUE (hash_value); 