-- Airflow-Metadatenbank (demo DB wird automatisch via POSTGRES_DB erstellt)
CREATE DATABASE airflow;

-- Berechtigungen
GRANT ALL PRIVILEGES ON DATABASE airflow TO demo_user;
GRANT ALL PRIVILEGES ON DATABASE demo TO demo_user;
