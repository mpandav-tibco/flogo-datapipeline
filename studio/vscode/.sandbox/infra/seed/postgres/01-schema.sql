-- Seed schema + data for flogo-datapipeline Studio live tests.

CREATE TABLE IF NOT EXISTS customers (
  id          INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  email       TEXT,
  region      TEXT,
  updated_at  TIMESTAMP DEFAULT now()
);

INSERT INTO customers (id, name, email, region, updated_at) VALUES
  (1, 'Alice',  'alice@acme.io',  'emea', '2026-07-01 10:00:00'),
  (2, 'Bob',    'bob@acme.io',    'amer', '2026-07-02 11:00:00'),
  (3, 'Carol',  'carol@acme.io',  'apac', '2026-07-03 12:00:00'),
  (4, 'Dave',   NULL,             'emea', '2026-07-04 13:00:00'),
  (5, 'Erin',   'erin@acme.io',   'amer', '2026-07-05 14:00:00')
ON CONFLICT (id) DO NOTHING;

-- Target table for the CSV/JDBC-sink tests (created empty; auto-migrate may also create it).
CREATE TABLE IF NOT EXISTS orders (
  order_id    TEXT PRIMARY KEY,
  customer_id TEXT,
  amount      NUMERIC,
  status      TEXT
);

-- DLQ target for validate-route tests.
CREATE TABLE IF NOT EXISTS events_rejected (
  user_id     TEXT,
  event_type  TEXT,
  amount      NUMERIC,
  _error      TEXT
);
