-- Слои хранилища
CREATE SCHEMA IF NOT EXISTS stg;  -- сырые ответы API (роль 1)
CREATE SCHEMA IF NOT EXISTS dds;  -- нормализованная модель + SCD2 (роль 2)
CREATE SCHEMA IF NOT EXISTS cdm;  -- витрины под дашборд (роль 3)
