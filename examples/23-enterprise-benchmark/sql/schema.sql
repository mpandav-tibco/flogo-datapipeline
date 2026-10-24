-- Warehouse schema for the enterprise EL/ETL/ELT benchmark.
-- The engine does NOT auto-create tables, so setup-infra.sh applies this first.
--
--   orders_raw     : source-schema landing table (E+L target, ELT source)
--   orders_sink    : source-schema sink for EL / JDBC-sink cases
--   orders_clean   : mapped-schema sink for ETL cases (region_code, net_amount)
--   orders_rejects : dead-letter table for the DLQ enterprise scenario
--
-- Columns are typed (quantity INTEGER, prices NUMERIC) even though the CSV/Parquet
-- sources emit all-string values: PostgreSQL coerces valid text literals on
-- insert, so the same tables accept both typed and string-sourced rows.

CREATE TABLE IF NOT EXISTS orders_raw (
    order_id    text,
    customer_id text,
    region      text,
    product     text,
    category    text,
    quantity    integer,
    unit_price  numeric(12, 2),
    status      text,
    order_date  text,
    email       text
);

-- The jdbc-source cases page with keyset pagination on order_id
-- (WHERE order_id > $last ORDER BY order_id LIMIT n); without this index every
-- page is a full scan + sort of the 53M-row table.
CREATE INDEX IF NOT EXISTS orders_raw_order_id_idx ON orders_raw (order_id);

CREATE TABLE IF NOT EXISTS orders_sink (
    order_id    text,
    customer_id text,
    region      text,
    product     text,
    category    text,
    quantity    integer,
    unit_price  numeric(12, 2),
    status      text,
    order_date  text,
    email       text
);

CREATE TABLE IF NOT EXISTS orders_clean (
    order_id    text,
    customer_id text,
    region_code text,
    product     text,
    category    text,
    quantity    integer,
    net_amount  numeric(14, 2),
    status      text,
    order_date  text,
    email       text
);

-- Dead-letter / reject table for the DLQ enterprise scenario (ent01): rows that
-- fail validation are routed here (queryable) while good rows continue to load.
CREATE TABLE IF NOT EXISTS orders_rejects (
    order_id    text,
    customer_id text,
    region      text,
    product     text,
    category    text,
    quantity    integer,
    unit_price  numeric(12, 2),
    status      text,
    order_date  text,
    email       text
);
