/*******************************************************************
 *
 * @(#)$RCSfile: oracleTrailer.sql,v $ $Revision: 1.18 $ $Release$ $Date: 2008/02/23 21:58:26 $ $Author: murrayc3 $
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
 * This view shows candidate "free tape drive to pending tape request"
 * allocations.
 */
CREATE OR REPLACE VIEW CANDIDATEDRIVEALLOCATIONS_VIEW
AS SELECT
  TapeDrive.id as tapeDriveID, TapeRequest.id as tapeRequestID
FROM
  TapeRequest
INNER JOIN
  TapeDrive
ON
  TapeRequest.deviceGroupName = TapeDrive.deviceGroupName
INNER JOIN
  TapeServer
ON
     TapeRequest.requestedSrv = TapeServer.id
  OR TapeRequest.requestedSrv = 0
WHERE
      TapeDrive.status=0 -- UNIT_UP
  AND TapeDrive.runningTapeReq=0
  -- Exclude a request if its tape is already in a drive, such a request
  -- will be considered upon the release of the drive in question
  -- (cf. TapeDriveStatusHandler)
  AND NOT EXISTS (
    SELECT
      'x'
    FROM
      TapeDrive TapeDrive2
    WHERE
      TapeDrive2.tape = TapeRequest.tape
  )
  AND TapeServer.actingMode=0 -- ACTIVE
  AND TapeRequest.tapeDrive=0
ORDER BY
  TapeRequest.modificationTime ASC;


/**
 * View used for generating the list of requests when replying to the
 * showqueues command
 */
create or replace view
  TAPEREQUESTSSHOWQUEUES_VIEW
as select
  TAPEREQUEST.ID,
  TAPEDRIVE.DRIVENAME,
  TAPEDRIVE.ID as TAPEDRIVEID,
  TAPEREQUEST.PRIORITY,
  CLIENTIDENTIFICATION.PORT as CLIENTPORT,
  CLIENTIDENTIFICATION.EUID as CLIENTEUID,
  CLIENTIDENTIFICATION.EGID as CLIENTEGID,
  TAPEACCESSSPECIFICATION.ACCESSMODE,
  TAPEREQUEST.MODIFICATIONTIME,
  CLIENTIDENTIFICATION.MACHINE AS CLIENTMACHINE,
  VDQMTAPE.VID,
  TAPESERVER.SERVERNAME as TAPESERVER,
  DEVICEGROUPNAME.DGNAME,
  CLIENTIDENTIFICATION.USERNAME as CLIENTUSERNAME
from
  TAPEREQUEST
left outer join
  TAPEDRIVE
on
  TAPEREQUEST.TAPEDRIVE = TAPEDRIVE.ID
left outer join
  CLIENTIDENTIFICATION
on
  TAPEREQUEST.CLIENT = CLIENTIDENTIFICATION.ID
inner join
  TAPEACCESSSPECIFICATION
on
  TAPEREQUEST.TAPEACCESSSPECIFICATION = TAPEACCESSSPECIFICATION.ID
left outer join
  VDQMTAPE
on
  TAPEREQUEST.TAPE = VDQMTAPE.ID
left outer join
  TAPESERVER
on
  TAPEREQUEST.REQUESTEDSRV = TAPESERVER.ID
left outer join
  DEVICEGROUPNAME
on
  TAPEREQUEST.DEVICEGROUPNAME = DEVICEGROUPNAME.ID;


/**
 * View used for generating the list of drives when replying to the showqueues
 * command
 */
create or replace view
  TAPEDRIVESHOWQUEUES_VIEW
as with
  TAPEDRIVE2MODEL
as
(
  select
    TAPEDRIVE2TAPEDRIVECOMP.PARENT as TAPEDRIVE,
    max(TAPEDRIVECOMPATIBILITY.TAPEDRIVEMODEL) as DRIVEMODEL
  from
    TAPEDRIVE2TAPEDRIVECOMP
  inner join
    TAPEDRIVECOMPATIBILITY
  on
    TAPEDRIVE2TAPEDRIVECOMP.CHILD = TAPEDRIVECOMPATIBILITY.ID
  group by
    parent
)
select
  TAPEDRIVE.STATUS, TAPEDRIVE.ID, TAPEDRIVE.RUNNINGTAPEREQ, TAPEDRIVE.JOBID,
  TAPEDRIVE.MODIFICATIONTIME, TAPEDRIVE.RESETTIME, TAPEDRIVE.USECOUNT,
  TAPEDRIVE.ERRCOUNT, TAPEDRIVE.TRANSFERREDMB, TAPEDRIVE.TAPEACCESSMODE,
  TAPEDRIVE.TOTALMB, TAPESERVER.SERVERNAME, VDQMTAPE.VID, TAPEDRIVE.DRIVENAME,
  DEVICEGROUPNAME.DGNAME, TAPEDRIVE2MODEL.DRIVEMODEL
from
  TAPEDRIVE
left outer join
  TAPESERVER
on
  TAPEDRIVE.TAPESERVER = TAPESERVER.ID
left outer join
  VDQMTAPE
on
  TAPEDRIVE.TAPE = VDQMTAPE.ID
left outer join
  DEVICEGROUPNAME
on
  TAPEDRIVE.DEVICEGROUPNAME = DEVICEGROUPNAME.ID
inner join
  TAPEDRIVE2MODEL
on
  TAPEDRIVE.ID = TAPEDRIVE2MODEL.TAPEDRIVE;


/**
 * COMMENTED OUT
 * PL/SQL method to dedicate a tape to a tape drive.
 * First it checks the preconditions that a tapeDrive must meet in order to be
 * assigned. The couples (drive,requests) are then orderd by the priorityLevel 
 * and by the modification time and processed one by one to verify
 * if any dedication exists and has to be applied.
 * Returns the relevant IDs if a couple was found, (0,0) otherwise.
 */  
/*
CREATE OR REPLACE PROCEDURE allocateDrive
 (ret OUT NUMBER) AS
  d2rCur castorVdqm.Drive2Req_Cur;
  d2r castorVdqm.Drive2Req;
  countDed INTEGER;
BEGIN
  ret := 0;
  
  -- Check all preconditions a tape drive must meet in order to be used by pending tape requests
  OPEN d2rCur FOR
  SELECT FreeTD.id, TapeRequest.id
    FROM TapeDrive FreeTD, TapeRequest, TapeDrive2TapeDriveComp, TapeDriveCompatibility, TapeServer
   WHERE FreeTD.status = 0  -- UNIT_UP
     AND FreeTD.runningTapeReq = 0  -- not associated with a tape request
     AND FreeTD.tape = 0 -- not associated with a tape
     AND FreeTD.tapeServer = TapeServer.id 
     AND TapeServer.actingMode = 0  -- ACTIVE
     AND TapeRequest.tapeDrive = 0
     AND (TapeRequest.requestedSrv = TapeServer.id OR TapeRequest.requestedSrv = 0)
     AND TapeDrive2TapeDriveComp.parent = FreeTD.id 
     AND TapeDrive2TapeDriveComp.child = TapeDriveCompatibility.id 
     AND TapeDriveCompatibility.tapeAccessSpecification = TapeRequest.tapeAccessSpecification
     AND FreeTD.deviceGroupName = TapeRequest.deviceGroupName
     AND NOT EXISTS (
       -- we explicitly exclude requests whose tape has already been assigned to a drive;
       -- those requests will be considered upon drive release (cf. TapeDriveStatusHandler)
       SELECT 'x'
         FROM TapeDrive UsedTD
        WHERE UsedTD.tape = TapeRequest.tape
       )
     -- AND TapeDrive.deviceGroupName = tapeDriveDgn.id 
     -- AND TapeRequest.deviceGroupName = tapeRequestDgn.id 
     -- AND tapeDriveDgn.libraryName = tapeRequestDgn.libraryName 
         -- in case we want to match by libraryName only
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
      UPDATE TapeDrive SET
        status = 1, -- UNIT_STARTING = 1
        jobID = 0,
        modificationTime = getTime(),
        runningTapeReq = d2r.tapeRequest
        WHERE id = d2r.tapeDrive;
      UPDATE TapeRequest SET
        status = 1, -- MATCHED
        tapeDrive = d2r.tapeDrive,
        modificationTime = getTime()
        WHERE id = d2r.tapeRequest;
      ret := 1;
      CLOSE d2rCur;
      RETURN;
    END IF;

    -- We must check if the request matches the dedications for this tape drive
    SELECT count(*) INTO countDed
      FROM TapeDriveDedication tdd, VdqmTape, TapeRequest,
           ClientIdentification, TapeAccessSpecification, TapeDrive
     WHERE tdd.tapeDrive = d2r.tapeDrive
       AND getTime() BETWEEN startTime AND endTime
       AND tdd.clientHost(+) = ClientIdentification.machine
       AND tdd.euid(+) = ClientIdentification.euid
       AND tdd.egid(+) = ClientIdentification.egid
       AND tdd.vid(+) = VdqmTape.vid
       AND tdd.accessMode(+) = TapeAccessSpecification.accessMode
       AND TapeRequest.id = d2r.tapeRequest
       AND TapeRequest.tape = VdqmTape.id
       AND TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
       AND TapeRequest.client = ClientIdentification.id;
    IF countDed > 0 THEN  -- there's a matching dedication for at least a criterium
      UPDATE TapeDrive SET
        status = 1, -- UNIT_STARTING = 1
        jobID = 0,
        modificationTime = getTime(),
        runningTapeReq = d2r.tapeRequest
        WHERE id = d2r.tapeDrive;
      UPDATE TapeRequest SET
        status = 1, -- MATCHED
        tapeDrive = d2r.tapeDrive,
        modificationTime = getTime()
        WHERE id = d2r.tapeRequest;
      ret := 1;
      CLOSE d2rCur;
      RETURN;
    END IF;
    -- else the tape drive is dedicated to other request(s) and we can't use it, go on
  END LOOP;
  -- if the loop has been fully completed without assignment,
  -- no free tape drive has been found. 
  CLOSE d2rCur;
END;
*/


/**
 * PL/SQL procedure which tries to allocate a free tape drive to a pending tape
 * request.
 *
 * This method returns the ID of the corresponding tape request if the method
 * successfully allocates a drive, else 0.
 */
CREATE OR REPLACE
PROCEDURE allocateDrive
 (ret OUT NUMBER) AS
 tapeDriveID_var   NUMBER := 0;
 tapeRequestID_var NUMBER := 0;
BEGIN
  ret := 0;

  SELECT
    tapeDriveID,
    tapeRequestID
  INTO
    tapeDriveID_var, tapeRequestID_var
  FROM
    CANDIDATEDRIVEALLOCATIONS_VIEW
  WHERE
    rownum < 2;

  -- If there is a free drive which can be allocated to a pending request
  IF tapeDriveID_var != 0 AND tapeRequestID_var != 0 THEN

    UPDATE TapeDrive SET
      status           = 1, -- UNIT_STARTING
      jobID            = 0,
      modificationTime = getTime(),
      runningTapeReq   = tapeRequestID_var
    WHERE
      id = tapeDriveID_var;

    UPDATE TapeRequest SET
      status           = 1, -- MATCHED
      tapeDrive        = tapeDriveID_var,
      modificationTime = getTime()
    WHERE
      id = tapeRequestID_var;

    RET := 1;
  END IF;

EXCEPTION
  -- Do nothing if there was no free tape drive which could be allocated to a
  -- pending request
  WHEN NO_DATA_FOUND THEN NULL;
END;


/**
 * PL/SQL method to get a new matched request to be submitted to rtcpd
 */
CREATE OR REPLACE PROCEDURE requestToSubmit(tapeReqId OUT NUMBER) AS
LockError EXCEPTION;
PRAGMA EXCEPTION_INIT (LockError, -54);
CURSOR c IS
   SELECT id
     FROM TapeRequest
    WHERE status = 1  -- MATCHED
    FOR UPDATE SKIP LOCKED;
BEGIN
  tapeReqId := 0;
  OPEN c;
  FETCH c INTO tapeReqId;
  UPDATE TapeRequest SET status = 2 WHERE id = tapeReqId;  -- BEINGSUBMITTED
  CLOSE c;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- just return reqId = 0, nothing to do
  NULL;
WHEN LockError THEN
  -- We have observed ORA-00054 errors (resource busy and acquire with NOWAIT) even with
  -- the SKIP LOCKED clause. This is a workaround to ignore the error until we understand
  -- what to do, another thread will pick up the request so we don't do anything.
  NULL;
END;


/**
 * PL/SQL method to check and reuse a tape allocation, that is a tape-tape
 * drive match
 */
CREATE OR REPLACE PROCEDURE reuseTapeAllocation(tapeId IN NUMBER,
  tapeDriveId IN NUMBER, tapeReqId OUT NUMBER) AS
BEGIN
  tapeReqId := 0;
  UPDATE TapeRequest
     SET status = 1,  -- MATCHED
         tapeDrive = tapeDriveId,
         modificationTime = getTime()
   WHERE tapeDrive = 0
     AND status = 0  -- PENDING
     AND tape = tapeId
     AND ROWNUM < 2
  RETURNING id INTO tapeReqId;
  IF tapeReqId > 0 THEN -- If a tape request was found
    UPDATE TapeDrive
       SET status = 1, -- UNIT_STARTING
           jobID = 0,
           runningTapeReq = tapeReqId,
           modificationTime = getTime()
     WHERE id = tapeDriveId;
  END IF;
END;
