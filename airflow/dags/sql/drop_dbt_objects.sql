-- Alle von dbt verwalteten Objekte aufraumen.
-- Wird von init_raw_data ausgefuehrt, damit nach einem Raw-Reset
-- keine Vault-Tabellen mit veralteten Spaltentypen zurueckbleiben.
-- Schemas selbst bleiben bestehen (werden in 02_create_schemas.sql angelegt).

DROP TABLE IF EXISTS raw_vault.pit_order CASCADE;
DROP TABLE IF EXISTS raw_vault.pit_customer CASCADE;
DROP TABLE IF EXISTS raw_vault.sat_customer CASCADE;
DROP TABLE IF EXISTS raw_vault.sat_customer_psa CASCADE;
DROP TABLE IF EXISTS raw_vault.sat_employee CASCADE;
DROP TABLE IF EXISTS raw_vault.sat_order CASCADE;
DROP TABLE IF EXISTS raw_vault.sat_order_detail CASCADE;
DROP TABLE IF EXISTS raw_vault.sat_product CASCADE;
DROP TABLE IF EXISTS raw_vault.lnk_order_customer CASCADE;
DROP TABLE IF EXISTS raw_vault.lnk_order_employee CASCADE;
DROP TABLE IF EXISTS raw_vault.lnk_order_product CASCADE;
DROP TABLE IF EXISTS raw_vault.hub_customer CASCADE;
DROP TABLE IF EXISTS raw_vault.hub_customer_psa CASCADE;
DROP TABLE IF EXISTS raw_vault.hub_employee CASCADE;
DROP TABLE IF EXISTS raw_vault.hub_order CASCADE;
DROP TABLE IF EXISTS raw_vault.hub_product CASCADE;

-- Staging (Views, werden von dbt rebuild)
DROP VIEW IF EXISTS staging.stg_customers CASCADE;
DROP VIEW IF EXISTS staging.stg_customers_psa CASCADE;
DROP VIEW IF EXISTS staging.stg_employees CASCADE;
DROP VIEW IF EXISTS staging.stg_orders CASCADE;
DROP VIEW IF EXISTS staging.stg_order_details CASCADE;
DROP VIEW IF EXISTS staging.stg_products CASCADE;

-- Marts
DROP TABLE IF EXISTS mart.mart_revenue_per_customer CASCADE;
DROP TABLE IF EXISTS mart.mart_order_overview CASCADE;
DROP TABLE IF EXISTS mart.mart_product_sales CASCADE;
