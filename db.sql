CREATE TABLE my_hosts (
  id              INTEGER PRIMARY KEY,
  host_ref        TEXT NOT NULL,
  last_connected  DATETIME
);

CREATE INDEX idx_my_hosts_connected  ON my_hosts (last_connected);
CREATE UNIQUE INDEX unq_my_hosts_ref ON my_hosts (host_ref);

CREATE TABLE my_databases (
  id              INTEGER PRIMARY KEY,
  db_ref          TEXT NOT NULL,
  last_connected  DATETIME,
  success         INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_my_databases_connected   ON my_databases (last_connected);
CREATE UNIQUE INDEX unq_my_databases_ref ON my_databases (db_ref);

CREATE TABLE my_tables (
  id              INTEGER PRIMARY KEY,
  db_ref          TEXT NOT NULL,
  schema_name     TEXT NOT NULL,
  table_name      TEXT NOT NULL,
  column_name     TEXT NOT NULL,
  column_type     TEXT NOT NULL,
  column_nullable INTEGER NOT NULL DEFAULT 0,
  column_attrs    TEXT
);

CREATE INDEX idx_my_tables_db_ref      ON my_tables (db_ref);
CREATE INDEX idx_my_tables_schema_name ON my_tables (schema_name);
CREATE INDEX idx_my_tables_table_name  ON my_tables (table_name);
CREATE INDEX idx_my_tables_column_name ON my_tables (column_name);
CREATE UNIQUE INDEX unq_my_tables_db_schema_table_col ON my_tables (db_ref, schema_name, table_name, column_name);
