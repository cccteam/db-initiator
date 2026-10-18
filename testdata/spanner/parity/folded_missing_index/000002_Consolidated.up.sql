-- A fold that lost the index the sequence ends with.
CREATE TABLE Customers (
  Id STRING(36) NOT NULL,
  Name STRING(MAX) NOT NULL,
  CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (Id);

CREATE TABLE Orders (
  Id STRING(36) NOT NULL,
  CustomerId STRING(36) NOT NULL,
  Total NUMERIC NOT NULL,
  CONSTRAINT FK_Orders_Customers FOREIGN KEY (CustomerId) REFERENCES Customers (Id),
) PRIMARY KEY (Id);
