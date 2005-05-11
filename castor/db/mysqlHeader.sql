-- This file contains SQL code that is not generated automatically
-- and is inserted at the beginning of the generated code

-- SQL statements for object types */
DROP INDEX I_Id2Type_type ON Id2Type;
DROP INDEX I_Id2Type_full ON Id2Type;
DROP TABLE IF EXIST Id2Type;
CREATE TABLE Id2Type (id BIGINT, type INTEGER, PRIMARY KEY(id));
CREATE INDEX I_Id2Type_full ON Id2Type (id, type);
CREATE INDEX I_Id2Type_type ON Id2Type (type);

-- SQL statements for requests status
DROP INDEX I_newRequests_type ON newRequests;
DROP TABLE IF EXIST newRequests;
CREATE TABLE newRequests (id INTEGER AUTO_INCREMENT, type INTEGER, creation DATE, PRIMARY KEY(id));
CREATE INDEX I_newRequests_type on newRequests (type);

