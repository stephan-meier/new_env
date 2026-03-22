-- ============================================================
-- Schema: dq (Data Quality)
-- Speichert dbt-Testergebnisse fuer BI-Dashboards (Metabase)
-- ============================================================

\connect demo;

CREATE SCHEMA IF NOT EXISTS dq;

-- Testergebnisse aus run_results.json
CREATE TABLE IF NOT EXISTS dq.test_results (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(100),           -- dbt invocation_id
    run_at          TIMESTAMP,              -- generated_at aus Metadaten
    domain          VARCHAR(50),            -- klassisch, domain_master, domain_orders, consumption, psa
    test_name       VARCHAR(500),           -- lesbarer Testname
    model_name      VARCHAR(200),           -- getestetes Modell
    layer           VARCHAR(50),            -- staging, raw_vault, mart
    test_type       VARCHAR(50),            -- standard, dbt-expectations
    status          VARCHAR(20),            -- pass, fail, warn, error
    failures        INTEGER DEFAULT 0,
    execution_time  FLOAT,
    message         TEXT
);

-- Index fuer typische Dashboard-Abfragen
CREATE INDEX IF NOT EXISTS idx_test_results_run_at ON dq.test_results(run_at);
CREATE INDEX IF NOT EXISTS idx_test_results_status ON dq.test_results(status);
CREATE INDEX IF NOT EXISTS idx_test_results_layer  ON dq.test_results(layer);

-- Source-Freshness-Ergebnisse
CREATE TABLE IF NOT EXISTS dq.source_freshness (
    id              SERIAL PRIMARY KEY,
    checked_at      TIMESTAMP,
    source_name     VARCHAR(200),
    table_name      VARCHAR(200),
    max_loaded_at   TIMESTAMP,
    status          VARCHAR(20),            -- pass, warn, error
    age_seconds     FLOAT
);

GRANT USAGE ON SCHEMA dq TO demo_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dq TO demo_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dq TO demo_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA dq GRANT ALL ON TABLES TO demo_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA dq GRANT USAGE, SELECT ON SEQUENCES TO demo_user;
