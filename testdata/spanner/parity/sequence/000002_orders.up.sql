CREATE TABLE Orders (
  Id STRING(36) NOT NULL,
  CustomerId STRING(36) NOT NULL,
  Total NUMERIC NOT NULL,
  CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId) REFERENCES Customers (Id),
) PRIMARY KEY (Id);

DROP INDEX CustomersByName;
CREATE INDEX CustomersByNameAndId ON Customers(Name, Id);
