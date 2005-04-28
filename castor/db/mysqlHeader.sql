-- This file contains SQL code that is not generated automatically
-- and is inserted at the beginning of the generated code

-- SQL statements for object types */
DROP INDEX I_Id2Type_type ON Id2Type;
DROP INDEX I_Id2Type_full ON Id2Type;
DROP TABLE IF EXIST Id2Type;
CREATE TABLE Id2Type (id BIGINT, type INTEGER, PRIMARY KEY(id));
CREATE INDEX I_Id2Type_full ON Id2Type (id, type);
CREATE INDEX I_Id2Type_type ON Id2Type (type);

-- Sequence for indices: not available in MySQL => using an AUTO_INCREMENT field on a dedicated table
DROP TABLE IF EXIST Sequence;
CREATE TABLE Sequence (value BIGINT AUTO_INCREMENTT, PRIMARY KEY(value));

CREATE PROCEDURE seqNextVal(OUT value BIGINT)
BEGIN
  LOCK TABLES Sequence;
  INSERT INTO Sequence VALUES (NULL);
  SELECT LAST_INSERT_ID() INTO value;
  DELETE FROM Sequence;
  UNLOCK TABLES;
END;

-- SQL statements for requests status
DROP INDEX I_newRequests_type ON newRequests;
DROP TABLE IF EXIST newRequests;
CREATE TABLE newRequests (id INTEGER AUTO_INCREMENT, type INTEGER, creation DATE, PRIMARY KEY(id));
CREATE INDEX I_newRequests_type on newRequests (type);
