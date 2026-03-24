-- Read-only User fuer MCP-Zugriff (Claude Desktop / Claude Code)
-- Wird beim ersten Stack-Start automatisch ausgefuehrt.
-- Zweck: MCP-Server sollen nie als Superuser (demo_user) auf die DBs zugreifen.

CREATE ROLE mcp_reader WITH LOGIN PASSWORD 'mcp_readonly';

-- Zugriff auf demo DB (alle fachlichen Schemas)
GRANT CONNECT ON DATABASE demo TO mcp_reader;

\connect demo

GRANT USAGE ON SCHEMA public, raw, raw_vault, staging, psa, mart, dq, dbt_test__audit TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_vault TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA psa TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA dq TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA dbt_test__audit TO mcp_reader;

-- Kuenftige Tabellen (dbt erstellt diese spaeter) automatisch freigeben
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT SELECT ON TABLES TO mcp_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_vault GRANT SELECT ON TABLES TO mcp_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO mcp_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA psa GRANT SELECT ON TABLES TO mcp_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart GRANT SELECT ON TABLES TO mcp_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA dq GRANT SELECT ON TABLES TO mcp_reader;

-- Zugriff auf airflow Metadaten-DB (read-only fuer DAG Runs, Task Instances, etc.)
\connect airflow

GRANT CONNECT ON DATABASE airflow TO mcp_reader;
GRANT USAGE ON SCHEMA public TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO mcp_reader;
