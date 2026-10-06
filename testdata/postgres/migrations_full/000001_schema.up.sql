-- The kinds of object a PostgreSQL schema holds: types, tables with indexes and foreign
-- keys, views, routines, a sequence, a partitioned table and a second schema.
CREATE TYPE product_status AS ENUM ('draft', 'active', 'retired');

CREATE DOMAIN money_amount AS NUMERIC(12, 2) CHECK (VALUE >= 0);

CREATE TYPE address AS (street TEXT, city TEXT);

CREATE TABLE products (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  price money_amount NOT NULL,
  status product_status NOT NULL DEFAULT 'draft',
  category TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Full-text search over the name and description
CREATE INDEX products_search ON products USING GIN (to_tsvector('english', name || ' ' || coalesce(description, '')));

CREATE TABLE categories (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  parent_id TEXT REFERENCES categories (id)
);

CREATE INDEX categories_name ON categories (name);

CREATE TABLE orders (
  id TEXT PRIMARY KEY,
  product_id TEXT NOT NULL,
  quantity INTEGER NOT NULL,
  total_price money_amount NOT NULL,
  ship_to address,
  order_date TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT fk_orders_products FOREIGN KEY (product_id) REFERENCES products (id)
);

CREATE INDEX orders_product_id ON orders (product_id);
CREATE INDEX orders_order_date ON orders (order_date DESC);

-- A view and a materialized view over the orders and products
CREATE VIEW order_summary AS
SELECT o.id AS order_id, p.name AS product_name, o.quantity, o.total_price, o.order_date
FROM orders o
JOIN products p ON o.product_id = p.id;

CREATE MATERIALIZED VIEW product_counts AS
SELECT category, count(*) AS products
FROM products
GROUP BY category;

-- A trigger function and a procedure
CREATE FUNCTION set_order_date() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.order_date := now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER orders_order_date BEFORE INSERT ON orders FOR EACH ROW EXECUTE FUNCTION set_order_date();

CREATE PROCEDURE archive_orders() LANGUAGE sql AS $$
DELETE FROM orders;
$$;

-- A sequence of its own
CREATE SEQUENCE order_numbers START 1000;

-- Events, partitioned by month, with the first partition
CREATE TABLE events (
  id BIGINT GENERATED ALWAYS AS IDENTITY,
  occurred_at TIMESTAMPTZ NOT NULL,
  kind TEXT NOT NULL,
  PRIMARY KEY (id, occurred_at)
) PARTITION BY RANGE (occurred_at);

CREATE TABLE events_2026_01 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

-- A second schema with a table of its own; IF NOT EXISTS, since MigrateDropSchema leaves
-- the schema in place
CREATE SCHEMA IF NOT EXISTS reporting;

CREATE TABLE reporting.daily_sales (
  day DATE PRIMARY KEY,
  total money_amount NOT NULL
);
