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
DROP INDEX I_REQUESTSSTATUS_FULL;
DROP TABLE REQUESTSSTATUS;
CREATE TABLE REQUESTSSTATUS (id INTEGER PRIMARY KEY, status CHAR(20), creation DATE, lastChange DATE);
CREATE INDEX I_REQUESTSSTATUS_FULL on REQUESTSSTATUS (id, status);

/* PL/SQL procedure for getting the next request to handle */
CREATE OR REPLACE PROCEDURE getNRStatement(reqid OUT INTEGER) AS
BEGIN
  SELECT ID INTO reqid FROM requestsStatus WHERE status = 'NEW' AND rownum <=1;
  UPDATE requestsStatus SET status = 'RUNNING', lastChange = SYSDATE WHERE ID = reqid;
END;

