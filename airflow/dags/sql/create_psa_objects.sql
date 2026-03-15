-- =================================================================
-- NG Generator Output (vereinfacht): PSA-Objekte fuer customers
-- Schema: psa
-- Konzept: Interface -> Clean (Dedup + Hash) -> PSA (SCD2) -> Views
-- =================================================================

CREATE SCHEMA IF NOT EXISTS psa;

-- -----------------------------------------------------------------
-- 1. Interface-View: liest 1:1 aus raw, castet Typen explizit
-- -----------------------------------------------------------------
CREATE OR REPLACE VIEW psa.v_customers_ifc AS
SELECT
    id::BIGINT          AS id,
    last_name::VARCHAR  AS last_name,
    first_name::VARCHAR AS first_name,
    email::VARCHAR      AS email,
    company::VARCHAR    AS company,
    phone::VARCHAR      AS phone,
    address1::VARCHAR   AS address1,
    address2::VARCHAR   AS address2,
    city::VARCHAR       AS city,
    state::VARCHAR      AS state,
    postal_code::VARCHAR AS postal_code,
    country::VARCHAR    AS country,
    change_date::TIMESTAMP AS change_date,
    ROW_NUMBER() OVER (
        PARTITION BY id, last_name, first_name, email, change_date
        ORDER BY change_date
    ) AS ng_row_number
FROM raw.customers
WHERE 1 = 1;

-- -----------------------------------------------------------------
-- 2. Clean-View: Duplikate entfernt + Content-Hash berechnet
-- -----------------------------------------------------------------
CREATE OR REPLACE VIEW psa.v_customers_cln AS
SELECT
    id,
    last_name,
    first_name,
    email,
    company,
    phone,
    address1,
    address2,
    city,
    state,
    postal_code,
    country,
    change_date,
    ng_row_number,
    encode(sha256(
        CONCAT_WS('|',
            COALESCE(last_name, ''),
            COALESCE(first_name, ''),
            COALESCE(email, ''),
            COALESCE(company, ''),
            COALESCE(phone, ''),
            COALESCE(address1, ''),
            COALESCE(address2, ''),
            COALESCE(city, ''),
            COALESCE(state, ''),
            COALESCE(postal_code, ''),
            COALESCE(country, '')
        )::bytea
    ), 'hex') AS ng_rowhash
FROM psa.v_customers_ifc
WHERE ng_row_number = 1;

-- -----------------------------------------------------------------
-- 3. PSA-Tabelle: SCD2-historisiert
-- -----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS psa.customers_psa (
    customers_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Quell-Attribute
    id BIGINT NOT NULL,
    last_name VARCHAR(50),
    first_name VARCHAR(50),
    email VARCHAR(50),
    company VARCHAR(50),
    phone VARCHAR(25),
    address1 VARCHAR(150),
    address2 VARCHAR(150),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(15),
    country VARCHAR(50),
    change_date TIMESTAMP,
    -- NG Meta-Attribute
    ng_rowhash VARCHAR(64) NOT NULL,
    ng_valid_from TIMESTAMP NOT NULL,
    ng_valid_to TIMESTAMP,
    ng_is_current INTEGER NOT NULL DEFAULT 1,
    ng_is_deleted INTEGER NOT NULL DEFAULT 0,
    ng_insert_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ng_update_date TIMESTAMP,
    ng_source VARCHAR DEFAULT 'raw.customers'
);

-- Index fuer SCD2-Lookups
CREATE INDEX IF NOT EXISTS idx_customers_psa_bk
    ON psa.customers_psa (id, ng_is_current);

-- -----------------------------------------------------------------
-- 4. Current-View: nur aktuelle, nicht geloeschte Versionen
-- -----------------------------------------------------------------
CREATE OR REPLACE VIEW psa.v_customers_cur AS
SELECT
    id,
    last_name,
    first_name,
    email,
    company,
    phone,
    address1,
    address2,
    city,
    state,
    postal_code,
    country,
    change_date,
    customers_sk,
    ng_rowhash,
    ng_valid_from,
    ng_valid_to,
    ng_is_current,
    ng_is_deleted,
    ng_insert_date,
    ng_update_date,
    ng_source
FROM psa.customers_psa
WHERE ng_is_current = 1
  AND ng_is_deleted = 0;

-- -----------------------------------------------------------------
-- 5. Full-History-View: alle Versionen
-- -----------------------------------------------------------------
CREATE OR REPLACE VIEW psa.v_customers_fhi AS
SELECT
    id,
    last_name,
    first_name,
    email,
    company,
    phone,
    address1,
    address2,
    city,
    state,
    postal_code,
    country,
    change_date,
    customers_sk,
    ng_rowhash,
    ng_valid_from,
    ng_valid_to,
    ng_is_current,
    ng_is_deleted,
    ng_insert_date,
    ng_update_date,
    ng_source
FROM psa.customers_psa;

-- -----------------------------------------------------------------
-- 6. Deleted-View: geloeschte Records
-- -----------------------------------------------------------------
CREATE OR REPLACE VIEW psa.v_customers_del AS
SELECT *
FROM psa.customers_psa
WHERE ng_is_deleted = 1;
