CREATE TABLE my_tables (
  db_ref          TEXT NOT NULL,
  schema_name     TEXT NOT NULL,
  table_name      TEXT NOT NULL,
  column_name     TEXT NOT NULL,
  column_type     TEXT NOT NULL,
  column_nullable INTEGER NOT NULL,
  column_attrs    TEXT,
  PRIMARY KEY (db_ref, schema_name, table_name, column_name)
);

CREATE INDEX idx_db_ref      ON my_tables (db_ref);
CREATE INDEX idx_schema_name ON my_tables (schema_name);
CREATE INDEX idx_table_name  ON my_tables (table_name);
CREATE INDEX idx_column_name ON my_tables (column_name);
