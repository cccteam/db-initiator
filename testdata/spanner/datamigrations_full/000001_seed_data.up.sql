-- Insert categories
INSERT INTO Categories (Id, Name, ParentId) VALUES ('cat-001', 'Electronics', NULL);
INSERT INTO Categories (Id, Name, ParentId) VALUES ('cat-002', 'Computers', 'cat-001');
INSERT INTO Categories (Id, Name, ParentId) VALUES ('cat-003', 'Phones', 'cat-001');

-- Insert products
INSERT INTO Products (Id, Name, Description, Price, Category, CreatedAt) 
VALUES ('prod-001', 'Laptop Pro 15', 'High-performance laptop with 16GB RAM and 512GB SSD storage', 1299.99, 'Computers', PENDING_COMMIT_TIMESTAMP());

INSERT INTO Products (Id, Name, Description, Price, Category, CreatedAt) 
VALUES ('prod-002', 'Smartphone X', 'Latest smartphone with advanced camera and long battery life', 899.99, 'Phones', PENDING_COMMIT_TIMESTAMP());

INSERT INTO Products (Id, Name, Description, Price, Category, CreatedAt) 
VALUES ('prod-003', 'Wireless Headphones', 'Premium noise-cancelling wireless headphones with 30-hour battery', 349.99, 'Electronics', PENDING_COMMIT_TIMESTAMP());
