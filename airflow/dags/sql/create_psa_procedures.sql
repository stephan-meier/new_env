-- =================================================================
-- NG Generator Output (vereinfacht): PSA-Procedures fuer customers
-- Schema: psa
-- Vereinfachte 2-Schritt-SCD2-Logik (statt MERGE):
--   Schritt 1: UPDATE bestehende aktuelle Zeilen bei Hash-Aenderung
--   Schritt 2: INSERT neue Versionen + brandneue Keys
-- =================================================================

-- -----------------------------------------------------------------
-- SCD2-Load: Historisierung via Content-Hash-Vergleich
-- -----------------------------------------------------------------
DROP PROCEDURE IF EXISTS psa.run_customers_psa_load();

CREATE OR REPLACE PROCEDURE psa.run_customers_psa_load()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Schritt 1: Bestehende aktuelle Zeilen schliessen,
    --            deren Content-Hash sich geaendert hat
    UPDATE psa.customers_psa t
    SET
        ng_valid_to    = CURRENT_TIMESTAMP,
        ng_is_current  = 0,
        ng_update_date = CURRENT_TIMESTAMP
    FROM psa.v_customers_cln s
    WHERE t.id = s.id
      AND t.ng_is_current = 1
      AND t.ng_rowhash <> s.ng_rowhash;

    -- Schritt 2: Neue Versionen einfuegen fuer:
    --   a) Geaenderte Keys (geschlossen in Schritt 1, neue Version noetig)
    --   b) Brandneue Keys (noch nicht in PSA vorhanden)
    INSERT INTO psa.customers_psa (
        id, last_name, first_name, email, company, phone,
        address1, address2, city, state, postal_code, country,
        change_date, ng_rowhash,
        ng_valid_from, ng_valid_to, ng_is_current, ng_is_deleted,
        ng_insert_date, ng_update_date, ng_source
    )
    SELECT
        s.id, s.last_name, s.first_name, s.email, s.company, s.phone,
        s.address1, s.address2, s.city, s.state, s.postal_code, s.country,
        s.change_date, s.ng_rowhash,
        CURRENT_TIMESTAMP,      -- ng_valid_from
        NULL,                   -- ng_valid_to (offen = aktuell)
        1,                      -- ng_is_current
        0,                      -- ng_is_deleted
        CURRENT_TIMESTAMP,      -- ng_insert_date
        NULL,                   -- ng_update_date
        'raw.customers'         -- ng_source
    FROM psa.v_customers_cln s
    WHERE NOT EXISTS (
        -- Kein INSERT wenn bereits aktuelle Zeile mit gleichem Hash existiert
        SELECT 1
        FROM psa.customers_psa t
        WHERE t.id = s.id
          AND t.ng_is_current = 1
          AND t.ng_rowhash = s.ng_rowhash
    );
END;
$$;

-- -----------------------------------------------------------------
-- Delete Detection: Records die in der Quelle nicht mehr vorhanden sind
-- -----------------------------------------------------------------
DROP PROCEDURE IF EXISTS psa.run_customers_delete_detection();

CREATE OR REPLACE PROCEDURE psa.run_customers_delete_detection()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE psa.customers_psa t
    SET
        ng_valid_to    = CURRENT_TIMESTAMP,
        ng_is_current  = 0,
        ng_is_deleted  = 1,
        ng_update_date = CURRENT_TIMESTAMP
    WHERE t.ng_is_current = 1
      AND NOT EXISTS (
        SELECT 1
        FROM psa.v_customers_cln s
        WHERE s.id = t.id
    );
END;
$$;
