INSERT INTO Customers (Id, Name, CreatedAt) VALUES ('c1', 'Ada', PENDING_COMMIT_TIMESTAMP());
INSERT INTO Customers (Id, Name, CreatedAt) VALUES ('c2', 'Grace', PENDING_COMMIT_TIMESTAMP());
INSERT INTO Orders (Id, CustomerId, Total) VALUES ('o1', 'c1', NUMERIC '19.99');
