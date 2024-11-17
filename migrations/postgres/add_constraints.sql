-- Проверяем существование ограничений перед созданием
DO $$
BEGIN
    -- Для таблицы applicants
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'applicants_inn_unique'
    ) THEN
        ALTER TABLE applicants 
        ADD CONSTRAINT applicants_inn_unique UNIQUE (inn);
    END IF;

    -- Для таблицы manufacturers
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'manufacturers_inn_unique'
    ) THEN
        ALTER TABLE manufacturers 
        ADD CONSTRAINT manufacturers_inn_unique UNIQUE (inn);
    END IF;

    -- Для таблицы certification_bodies
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'certification_bodies_ral_unique'
    ) THEN
        ALTER TABLE certification_bodies 
        ADD CONSTRAINT certification_bodies_ral_unique UNIQUE (ral_number);
    END IF;

    -- Для таблицы certificates
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'certificates_registration_number_unique'
    ) THEN
        ALTER TABLE certificates 
        ADD CONSTRAINT certificates_registration_number_unique UNIQUE (registration_number);
    END IF;

END $$; 