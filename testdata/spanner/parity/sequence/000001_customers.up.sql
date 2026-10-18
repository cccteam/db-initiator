-- The customers, with a commit timestamp the seed fills.
CREATE TABLE Customers (
  Id STRING(36) NOT NULL,
  Name STRING(MAX) NOT NULL,
  CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (Id);

-- An index the next migration drops: created and dropped in the history.
CREATE INDEX CustomersByName ON Customers(Name);
