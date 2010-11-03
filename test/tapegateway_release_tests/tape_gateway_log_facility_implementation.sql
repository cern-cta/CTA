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
   
CREATE OR REPLACE PACKAGE BODY TG_LogFacility AS

	/* Procedure to insert a log line. Log line id implicitely added by a trigger */
	PROCEDURE LogLine ( Line IN VARCHAR2) IS
		PRAGMA AUTONOMOUS_TRANSACTION;
	BEGIN
		INSERT INTO TG_LogFacility_logs (Line) VALUES (Line); COMMIT;
	END;

	/* Flush the logs */
	PROCEDURE ClearLog IS
		PRAGMA AUTONOMOUS_TRANSACTION;
	BEGIN
		DELETE FROM TG_LogFacility_logs;
		COMMIT;
	END;
END; /* PACKAGE BODY TG_POLICY_TESTING; */
/
