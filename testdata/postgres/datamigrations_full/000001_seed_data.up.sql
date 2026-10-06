-- Insert categories
INSERT INTO categories (id, name, parent_id) VALUES ('cat-001', 'Electronics', NULL);
INSERT INTO categories (id, name, parent_id) VALUES ('cat-002', 'Computers', 'cat-001');
INSERT INTO categories (id, name, parent_id) VALUES ('cat-003', 'Phones', 'cat-001');

-- Insert products
INSERT INTO products (id, name, description, price, status, category)
VALUES ('prod-001', 'Laptop Pro 15', 'High-performance laptop with 16GB RAM and 512GB SSD storage', 1299.99, 'active', 'Computers');

INSERT INTO products (id, name, description, price, status, category)
VALUES ('prod-002', 'Smartphone X', 'Latest smartphone with advanced camera and long battery life', 899.99, 'active', 'Phones');

INSERT INTO products (id, name, description, price, status, category)
VALUES ('prod-003', 'Wireless Headphones', 'Premium noise-cancelling wireless headphones with 30-hour battery', 349.99, 'draft', 'Electronics');

-- Count the products per category
REFRESH MATERIALIZED VIEW product_counts;
