/*******************************************************************
 *
 * @(#)$RCSfile: oracleTrailer.sql,v $ $Revision: 1.1 $ $Release$ $Date: 2007/11/28 11:06:20 $ $Author: itglp $
 *
 * This file contains SQL code that is not generated automatically
 * and is inserted at the end of the generated code
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* A small table used to cross check code and DB versions */
UPDATE CastorVersion SET schemaVersion = '2_1_6_0';

/* Sequence for indices */
CREATE SEQUENCE ids_seq CACHE 200;

/* SQL statements for object types */
CREATE TABLE Id2Type (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_Id2Type_typeId on Id2Type (type, id);

/* get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
  ret NUMBER;
BEGIN
  SELECT (SYSDATE - to_date('01-jan-1970 01:00:00','dd-mon-yyyy HH:MI:SS')) * (24*60*60) INTO ret FROM DUAL;
  RETURN ret;
END;

/* PL/SQL code for castorVdqm package */
CREATE OR REPLACE PACKAGE castorVdqm AS
  TYPE Drive2Req IS RECORD (
        tapeDrive NUMBER,
        tapeRequest NUMBER);
  TYPE Drive2Req_Cur IS REF CURSOR RETURN Drive2Req;
	TYPE TapeDrive_Cur IS REF CURSOR RETURN TapeDrive%ROWTYPE;
	TYPE TapeRequest_Cur IS REF CURSOR RETURN TapeRequest%ROWTYPE;
END castorVdqm;


/**
 * PL/SQL method to dedicate a tape to a tape drive.
 * First it checks the preconditions that a tapeDrive must meet in order to be
 * assigned. The couples (drive,requests) are then orderd by the priorityLevel 
 * and by the modification time and processed one by one to verify
 * if any dedication exists and has to be applied.
 * Returns the relevant IDs if a couple was found, (0,0) otherwise.
 */  
CREATE OR REPLACE PROCEDURE matchTape2TapeDrive
 (tapeDriveID OUT NUMBER, tapeRequestID OUT NUMBER) AS
  d2rCur castorVdqm.Drive2Req_Cur;
  d2r castorVdqm.Drive2Req;
  countDed INTEGER;
BEGIN
  tapeDriveID := 0;
  tapeRequestID := 0;
  
  -- Check all preconditions a tape drive must meet in order to be used by pending tape requests
  OPEN d2rCur FOR
  SELECT TapeDrive.id, TapeRequest.id
    FROM TapeDrive, TapeRequest, TapeDrive2TapeDriveComp, TapeDriveCompatibility, TapeServer
   WHERE TapeDrive.status = 0  -- UNIT_UP
     AND TapeDrive.runningTapeReq = 0  -- not associated with tapeReq
     AND TapeDrive.tapeServer = TapeServer.id 
     AND TapeServer.actingMode = 0  -- ACTIVE
     AND TapeRequest.tapeDrive = 0
     AND (TapeRequest.requestedSrv = TapeServer.id OR TapeRequest.requestedSrv = 0)
     AND TapeDrive2TapeDriveComp.parent = TapeDrive.id 
     AND TapeDrive2TapeDriveComp.child = TapeDriveCompatibility.id 
     AND TapeDriveCompatibility.tapeAccessSpecification = TapeRequest.tapeAccessSpecification
     AND TapeDrive.deviceGroupName = TapeRequest.deviceGroupName
     /*
     AND TapeDrive.deviceGroupName = tapeDriveDgn.id 
     AND TapeRequest.deviceGroupName = tapeRequestDgn.id 
     AND tapeDriveDgn.libraryName = tapeRequestDgn.libraryName 
         -- in case we want to match by libraryName only
     */
     ORDER BY TapeDriveCompatibility.priorityLevel ASC, 
              TapeRequest.modificationTime ASC;

  LOOP
    -- For each candidate couple, verify that the dedications (if any) are met
    FETCH d2rCur INTO d2r;
    EXIT WHEN d2rCur%NOTFOUND;

    SELECT count(*) INTO countDed
      FROM TapeDriveDedication
     WHERE tapeDrive = d2r.tapeDrive
       AND getTime() BETWEEN startTime AND endTime;
    IF countDed = 0 THEN    -- no dedications valid for this TapeDrive
      tapeDriveID := d2r.tapeDrive;   -- fine, we can assign it
      tapeRequestID := d2r.tapeRequest;
      EXIT;
    END IF;

    -- We must check if the request matches the dedications for this tape drive
    SELECT count(*) INTO countDed
      FROM TapeDriveDedication tdd, Tape, TapeRequest, ClientIdentification, 
           TapeAccessSpecification, TapeDrive
     WHERE tdd.tapeDrive = d2r.tapeDrive
       AND getTime() BETWEEN startTime AND endTime
       AND tdd.clientHost(+) = ClientIdentification.machine
       AND tdd.euid(+) = ClientIdentification.euid
       AND tdd.egid(+) = ClientIdentification.egid
       AND tdd.vid(+) = Tape.vid
       AND tdd.accessMode(+) = TapeAccessSpecification.accessMode
       AND TapeRequest.id = d2r.tapeRequest
       AND TapeRequest.tape = Tape.id
       AND TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
       AND TapeRequest.client = ClientIdentification.id;
    IF countDed > 0 THEN  -- there's a matching dedication for at least a criterium
      tapeDriveID := d2r.tapeDrive;   -- fine, we can assign it
      tapeRequestID := d2r.tapeRequest;
      EXIT;
    END IF;
    -- else the tape drive is dedicated to other request(s) and we can't use it, go on
  END LOOP;
  -- if the loop has been fully completed without assignment,
  -- no free tape drive has been found. 
  CLOSE d2rCur;
END;


/**
 * This is the main select statement to dedicate a tape to a tape drive.
 * It respects the old and of course the new way to select a tape for a 
 * tape drive.
 * The old way is to look, if the tapeDrive and the tapeRequest have the same
 * dgn.
 * The new way is to look if the TapeAccessSpecification can be served by a 
 * specific tapeDrive. The tape Request are then orderd by the priorityLevel (for 
 * the new way) and by the modification time.
 * Returns 1 if a couple was found, 0 otherwise.
 *
CREATE PROCEDURE matchTape2TapeDrive
 (tapeDriveID OUT NUMBER, tapeRequestID OUT NUMBER) AS
BEGIN
  SELECT TapeDrive.id, TapeRequest.id INTO tapeDriveID, tapeRequestID
    FROM TapeDrive, TapeRequest, TapeServer, DeviceGroupName tapeDriveDgn, DeviceGroupName tapeRequestDgn
    WHERE TapeDrive.status=0
          AND TapeDrive.runningTapeReq=0
          AND TapeDrive.tapeServer=TapeServer.id
          AND TapeRequest.tape NOT IN (SELECT tape FROM TapeDrive WHERE status!=0)
          AND TapeServer.actingMode=0
          AND TapeRequest.tapeDrive IS NULL
          AND (TapeRequest.requestedSrv=TapeDrive.tapeServer OR TapeRequest.requestedSrv=0)
          AND (TapeDrive.deviceGroupName=TapeRequest.deviceGroupName)
          AND rownum < 2
          ORDER BY TapeRequest.modificationTime ASC; 
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
    tapeDriveID := 0;
    tapeRequestID := 0;
END;
*/


/*
 * PL/SQL method implementing the select from tapereq queue
 */
CREATE OR REPLACE PROCEDURE selectTapeRequestQueue
 (dgn IN VARCHAR2, server IN VARCHAR2, tapeRequests OUT castorVdqm.TapeRequest_Cur) AS
BEGIN
  IF dgn IS NULL AND server IS NULL THEN
    OPEN tapeRequests FOR SELECT * FROM TapeRequest
		  ORDER BY TapeRequest.modificationTime ASC;
  ELSIF dgn IS NULL THEN
    OPEN tapeRequests FOR SELECT TapeRequest.* FROM TapeRequest, TapeServer
      WHERE TapeServer.serverName = server
            AND TapeServer.id = TapeRequest.requestedSrv
			ORDER BY TapeRequest.modificationTime ASC;
  ELSIF server IS NULL THEN
    OPEN tapeRequests FOR SELECT TapeRequest.* FROM TapeRequest, DeviceGroupName
      WHERE DeviceGroupName.dgName = dgn
            AND DeviceGroupName.id = TapeRequest.deviceGroupName
			ORDER BY TapeRequest.modificationTime ASC;
  ELSE 
    OPEN tapeRequests FOR SELECT TapeRequest.* FROM TapeRequest, DeviceGroupName, TapeServer
      WHERE DeviceGroupName.dgName = dgn
            AND DeviceGroupName.id = TapeRequest.deviceGroupName
            AND TapeServer.serverName = server
            AND TapeServer.id = TapeRequest.requestedSrv
			ORDER BY TapeRequest.modificationTime ASC;
  END IF;
END;


/*
 * PL/SQL method implementing the select from tapedrive queue
 */
CREATE OR REPLACE PROCEDURE selectTapeDriveQueue
 (dgn IN VARCHAR2, server IN VARCHAR2, tapeDrives OUT castorVdqm.TapeDrive_Cur) AS
BEGIN
  IF dgn IS NULL AND server IS NULL THEN
    OPEN tapeDrives FOR SELECT * FROM TapeDrive
		  ORDER BY TapeDrive.driveName ASC;
  ELSIF dgn IS NULL THEN
    OPEN tapeDrives FOR SELECT TapeDrive.* FROM TapeDrive, TapeServer
      WHERE TapeServer.serverName = server
            AND TapeServer.id = TapeDrive.tapeServer
			ORDER BY TapeDrive.driveName ASC;
  ELSIF server IS NULL THEN
    OPEN tapeDrives FOR SELECT TapeDrive.* FROM TapeDrive, DeviceGroupName
      WHERE DeviceGroupName.dgName = dgn
            AND DeviceGroupName.id = TapeDrive.deviceGroupName
			ORDER BY TapeDrive.driveName ASC;
  ELSE 
    OPEN tapeDrives FOR SELECT TapeDrive.* FROM TapeDrive, DeviceGroupName, TapeServer
      WHERE DeviceGroupName.dgName = dgn
            AND DeviceGroupName.id = TapeDrive.deviceGroupName
            AND TapeServer.serverName = server
            AND TapeServer.id = TapeDrive.tapeServer
			ORDER BY TapeDrive.driveName ASC;
  END IF;
END;

