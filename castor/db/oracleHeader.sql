/* This file contains SQL code that is not generated automatically */
/* and is inserted at the beginning of the generated code           */

/* SQL statements for object types */
DROP INDEX I_RH_ID2TYPE_FULL;
DROP TABLE RH_ID2TYPE;
CREATE TABLE RH_ID2TYPE (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_RH_ID2TYPE_FULL on RH_ID2TYPE (id, type);

/* SQL statements for indices */
DROP INDEX I_RH_INDICES_FULL;
DROP TABLE RH_INDICES;
CREATE TABLE RH_INDICES (name CHAR(8), value NUMBER);
CREATE INDEX I_RH_INDICES_FULL on RH_INDICES (name, value);
INSERT INTO RH_INDICES (name, value) VALUES ('next_id', 1);

/* SQL statements for requests status */
DROP INDEX I_RH_REQUESTSSTATUS_FULL;
DROP TABLE RH_REQUESTSSTATUS;
CREATE TABLE RH_REQUESTSSTATUS (id INTEGER PRIMARY KEY, status CHAR(8), creation DATE, lastChange DATE);
CREATE INDEX I_RH_REQUESTSSTATUS_FULL on RH_REQUESTSSTATUS (id, status);

/* PL/SQL procedure for getting the next request to handle */
CREATE OR REPLACE PROCEDURE getNRStatement(reqid OUT INTEGER) AS
BEGIN
  SELECT ID INTO reqid FROM rh_requestsStatus WHERE status = 'NEW' AND rownum <=1;
  UPDATE rh_requestsStatus SET status = 'RUNNING', lastChange = SYSDATE WHERE ID = reqid;
END;

