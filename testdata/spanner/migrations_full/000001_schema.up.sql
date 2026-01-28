-- Create Products table with tokenized columns for full-text search
CREATE TABLE Products (
  Id STRING(36) NOT NULL,
  Name STRING(255) NOT NULL,
  Description STRING(MAX),
  Price FLOAT64 NOT NULL,
  Category STRING(100),
  CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  Name_Tokens TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
  Description_Tokens TOKENLIST AS (TOKENIZE_FULLTEXT(Description)) HIDDEN,
) PRIMARY KEY(Id);

-- Create search index for full-text search on Products
CREATE SEARCH INDEX ProductsSearchIndex ON Products(Name_Tokens, Description_Tokens);

-- Create Categories table
CREATE TABLE Categories (
  Id STRING(36) NOT NULL,
  Name STRING(100) NOT NULL,
  ParentId STRING(36),
) PRIMARY KEY(Id);

-- Create index on Categories
CREATE INDEX Categories_Name ON Categories(Name);

-- Create Orders table with foreign key
CREATE TABLE Orders (
  Id STRING(36) NOT NULL,
  ProductId STRING(36) NOT NULL,
  Quantity INT64 NOT NULL,
  TotalPrice FLOAT64 NOT NULL,
  OrderDate TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  CONSTRAINT FK_Orders_Products FOREIGN KEY (ProductId) REFERENCES Products(Id),
) PRIMARY KEY(Id);

-- Create index on Orders
CREATE INDEX Orders_ProductId ON Orders(ProductId);
CREATE INDEX Orders_OrderDate ON Orders(OrderDate DESC);

-- Create a view for order summaries
CREATE VIEW OrderSummary SQL SECURITY INVOKER AS
SELECT 
  o.Id AS OrderId,
  p.Name AS ProductName,
  o.Quantity,
  o.TotalPrice,
  o.OrderDate
FROM Orders o
JOIN Products p ON o.ProductId = p.Id;
