/* This file contains SQL code that is not generated automatically */
/* and is inserted at the beginning of the generated code           */

/* SQL statements for object types */
DROP INDEX I_ID2TYPE_FULL;
DROP TABLE ID2TYPE;
CREATE TABLE ID2TYPE (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_ID2TYPE_FULL on ID2TYPE (id, type);

/* SQL statements for indices */
DROP INDEX I_INDICES_FULL;
DROP TABLE INDICES;
CREATE TABLE INDICES (name CHAR(8), value NUMBER);
CREATE INDEX I_INDICES_FULL on INDICES (name, value);
INSERT INTO INDICES (name, value) VALUES ('next_id', 1);

/* SQL statements for requests status */
DROP INDEX I_REQUESTSSTATUS_FULL;
DROP TABLE REQUESTSSTATUS;
CREATE TABLE REQUESTSSTATUS (id INTEGER PRIMARY KEY, status CHAR(8), creation DATE, lastChange DATE);
CREATE INDEX I_REQUESTSSTATUS_FULL on REQUESTSSTATUS (id, status);

/* PL/SQL procedure for id retrieval */
CREATE OR REPLACE PROCEDURE getId(nb IN INTEGER, id OUT INTEGER) AS
BEGIN
 UPDATE indices
  SET value = value + nb
  WHERE name='next_id'
  RETURNING value INTO id;
END;

/* PL/SQL procedure for getting the next request to handle */
CREATE OR REPLACE PROCEDURE getNRStatement(reqid OUT INTEGER) AS
BEGIN
  SELECT ID INTO reqid FROM requestsStatus WHERE status = 'NEW' AND rownum <=1;
  UPDATE requestsStatus SET status = 'RUNNING', lastChange = SYSDATE WHERE ID = reqid;
END;

