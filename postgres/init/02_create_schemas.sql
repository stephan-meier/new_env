-- Schemas fuer die Datenschichten im Data Vault
\c demo;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS raw_vault;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE SCHEMA IF NOT EXISTS psa;

-- Berechtigungen
GRANT ALL ON SCHEMA raw TO demo_user;
GRANT ALL ON SCHEMA staging TO demo_user;
GRANT ALL ON SCHEMA raw_vault TO demo_user;
GRANT ALL ON SCHEMA mart TO demo_user;
GRANT ALL ON SCHEMA psa TO demo_user;
