/* This file contains SQL code that is not generated automatically */
/* and is inserted at the beginning of the generated code           */

/* SQL statements for object types */
DROP INDEX I_Id2Type_type;
DROP INDEX I_Id2Type_full;
DROP TABLE Id2Type;
CREATE TABLE Id2Type (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_Id2Type_full on Id2Type (id, type);
CREATE INDEX main.I_Id2Type_type ON Id2Type(type);

/* Sequence for indices */
DROP SEQUENCE ids_seq;
CREATE SEQUENCE ids_seq;

/* SQL statements for requests status */
DROP INDEX I_newRequests_type;
DROP TABLE newRequests;
CREATE TABLE newRequests (id INTEGER PRIMARY KEY, type INTEGER, creation DATE);
CREATE INDEX I_newRequests_type on newRequests (type);
