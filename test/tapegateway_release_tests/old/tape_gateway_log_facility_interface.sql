/*******************************************************************
 *
 * @(#)$RCSfile$ $Revision$ $Date$ $Author$
 *
 * This file contains is an SQL script used to test polices of the tape gateway
 * on a disposable stager schema, which is supposed to be initially empty.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/
 
 /*  W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    */
 /*
  * D O   N O    U S E    O N    A   P R O D   O R   D E V    S T A G E R
  *
  * Only intended for throwaway schema on devdb10
  */
   /*  W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    */

DROP PACKAGE CastorStatus;
DROP PACKAGE TG_LogFacility;
DROP TRIGGER TG_LogFacility_insert_tr;
DROP TABLE TG_LogFacility_logs;
DROP SEQUENCE TG_LogFacility_seq;
CREATE TABLE TG_LogFacility_logs (id NUMBER, Line VARCHAR2(2048));
CREATE SEQUENCE TG_LogFacility_seq START WITH 1 INCREMENT BY 1;

CREATE OR REPLACE  
TRIGGER TG_LogFacility_insert_tr 
BEFORE INSERT ON TG_LogFacility_logs 
FOR EACH ROW
BEGIN
  SELECT TG_LogFacility_seq.nextval INTO :new.id FROM DUAL;
END; /* TRIGGER TG_test_policies_logs_insert */
/

CREATE OR REPLACE PACKAGE TG_LogFacility AS
	PROCEDURE ClearLog;
	PROCEDURE LogLine (Line IN VARCHAR2);
END TG_LogFacility;
/
