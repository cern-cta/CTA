/*******************************************************************
 *
 * @(#)$RCSfile: oracleTrailer.sql,v $ $Revision: 1.91 $ $Release$ $Date: 2008/04/19 15:05:24 $ $Author: murrayc3 $
 *
 * This file contains SQL code that is not generated automatically
 * and is inserted at the end of the generated code
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* Update the schema version number */
UPDATE CastorVersion SET schemaVersion = '2_1_7_4';

/* Sequence used to generate unique indentifies */
CREATE SEQUENCE ids_seq CACHE 200;

/* SQL statements for object types */
CREATE TABLE Id2Type (
  id   INTEGER,
  type NUMBER,
  CONSTRAINT PK_Id2Type_id PRIMARY KEY (id));
CREATE INDEX I_Id2Type_typeId on Id2Type (type, id);

/* Enumerations */
CREATE TABLE TapeServerStatusCodes (
  id   NUMBER,
  name VARCHAR2(30),
  CONSTRAINT PK_TapeServerStatusCodes_id PRIMARY KEY (id));
INSERT INTO TapeServerStatusCodes VALUES (0, 'TAPESERVER_ACTIVE');
INSERT INTO TapeServerStatusCodes VALUES (1, 'TAPESERVER_INACTIVE');
COMMIT;

CREATE TABLE TapeDriveStatusCodes (
  id   NUMBER,
  name VARCHAR2(30),
  CONSTRAINT PK_TapeDriveStatusCodes_id PRIMARY KEY (id));
INSERT INTO TapeDriveStatusCodes VALUES (0, 'UNIT_UP');
INSERT INTO TapeDriveStatusCodes VALUES (1, 'UNIT_STARTING');
INSERT INTO TapeDriveStatusCodes VALUES (2, 'UNIT_ASSIGNED');
INSERT INTO TapeDriveStatusCodes VALUES (3, 'VOL_MOUNTED');
INSERT INTO TapeDriveStatusCodes VALUES (4, 'FORCED_UNMOUNT');
INSERT INTO TapeDriveStatusCodes VALUES (5, 'UNIT_DOWN');
INSERT INTO TapeDriveStatusCodes VALUES (6, 'WAIT_FOR_UNMOUNT');
INSERT INTO TapeDriveStatusCodes VALUES (7, 'STATUS_UNKNOWN');
COMMIT;

CREATE TABLE TapeRequestStatusCodes (
  id NUMBER,
  name VARCHAR2(30),
  CONSTRAINT PK_TapeRequestStatusCodes_id PRIMARY KEY (id));
INSERT INTO TapeRequestStatusCodes VALUES (0, 'REQUEST_PENDING');
INSERT INTO TapeRequestStatusCodes VALUES (1, 'REQUEST_MATCHED');
INSERT INTO TapeRequestStatusCodes VALUES (2, 'REQUEST_BEINGSUBMITTED');
INSERT INTO TapeRequestStatusCodes VALUES (3, 'REQUEST_SUBMITTED');
INSERT INTO TapeRequestStatusCodes VALUES (4, 'REQUEST_FAILED');
COMMIT;

/* Not null column constraints */
ALTER TABLE CLIENTIDENTIFICATION MODIFY (EGID NOT NULL);
ALTER TABLE CLIENTIDENTIFICATION MODIFY (EUID NOT NULL);
ALTER TABLE CLIENTIDENTIFICATION MODIFY (MAGIC NOT NULL);
ALTER TABLE CLIENTIDENTIFICATION MODIFY (PORT NOT NULL);
ALTER TABLE ID2TYPE MODIFY (TYPE NOT NULL);
ALTER TABLE TAPEACCESSSPECIFICATION MODIFY (ACCESSMODE NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (DEVICEGROUPNAME NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (ERRCOUNT NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (JOBID NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (MODIFICATIONTIME NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (RESETTIME NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (RUNNINGTAPEREQ NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (STATUS NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (TAPE NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (TAPESERVER NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (TOTALMB NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (TRANSFERREDMB NOT NULL);
ALTER TABLE TAPEDRIVE MODIFY (USECOUNT NOT NULL);
ALTER TABLE TAPEDRIVE2TAPEDRIVECOMP MODIFY (CHILD NOT NULL);
ALTER TABLE TAPEDRIVE2TAPEDRIVECOMP MODIFY (PARENT NOT NULL);
ALTER TABLE TAPEDRIVECOMPATIBILITY MODIFY (PRIORITYLEVEL NOT NULL);
ALTER TABLE TAPEDRIVECOMPATIBILITY MODIFY (TAPEACCESSSPECIFICATION NOT NULL);
ALTER TABLE TAPEDRIVEDEDICATION MODIFY (TAPEDRIVE NOT NULL);
ALTER TABLE TAPEREQUEST MODIFY (CLIENT NOT NULL);
ALTER TABLE TAPEREQUEST MODIFY (CREATIONTIME NOT NULL);
ALTER TABLE TAPEREQUEST MODIFY (DEVICEGROUPNAME NOT NULL);
ALTER TABLE TAPEREQUEST MODIFY (ERRORCODE NOT NULL);
ALTER TABLE TAPEREQUEST MODIFY (MODIFICATIONTIME NOT NULL);
ALTER TABLE TAPEREQUEST MODIFY (PRIORITY NOT NULL);
ALTER TABLE TAPEREQUEST MODIFY (REQUESTEDSRV NOT NULL);
ALTER TABLE TAPEREQUEST MODIFY (STATUS NOT NULL);
ALTER TABLE TAPEREQUEST MODIFY (TAPE NOT NULL);
ALTER TABLE TAPEREQUEST MODIFY (TAPEACCESSSPECIFICATION NOT NULL);
ALTER TABLE TAPEREQUEST MODIFY (TAPEDRIVE NOT NULL);
ALTER TABLE TAPESERVER MODIFY (ACTINGMODE NOT NULL);

/* Unique constraints */
-- A client host can only be dedicated to one drive
--ALTER TABLE TapeDriveDedication
--  ADD CONSTRAINT I_U_TapeDrvDedic_clientHost
--    UNIQUE (clientHost);

-- A vid can only be dedicated to one drive
--ALTER TABLE TapeDriveDedication
--  ADD CONSTRAINT I_U_TapeDrvDedic_vid
--    UNIQUE (vid);

-- A drive can only have one or no access mode dedication
ALTER TABLE TapeDriveDedication
  ADD CONSTRAINT I_U_TapeDrvDedic_tapeDrv_mode
    UNIQUE (tapeDrive, accessMode);

-- Tape VIDs are unique
ALTER TABLE VdqmTape
  ADD CONSTRAINT I_U_VdqmTape_vid
    UNIQUE (VID);


/* Check constraints */
-- The accessMode column of the TapeAccessSpecification table has 2 possible
-- values: 0=write-disabled or 1=write-enabled
ALTER TABLE TapeAccessSpecification
  ADD CONSTRAINT CH_TapeAccessSpec_accessMode
    CHECK ((accessMode=0) OR (accessMode=1));


/* Foreign key constraints with an index for each */
ALTER TABLE ClientIdentification
  ADD CONSTRAINT FK_ClientIdentification_id
    FOREIGN KEY (id)
    REFERENCES Id2Type (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE;

ALTER TABLE DeviceGroupName
  ADD CONSTRAINT FK_DeviceGroupName_id
    FOREIGN KEY (id)
    REFERENCES Id2Type (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE;

ALTER TABLE TapeAccessSpecification
  ADD CONSTRAINT FK_TapeAccessSpecification_id
    FOREIGN KEY (id)
    REFERENCES Id2Type (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE;

ALTER TABLE TapeDrive
  ADD CONSTRAINT FK_TapeDrive_id
    FOREIGN KEY (id)
    REFERENCES Id2Type (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
  ADD CONSTRAINT FK_TapeDrive_tape
    FOREIGN KEY (tape)
    REFERENCES VdqmTape (id)
    DEFERRABLE
    INITIALLY DEFERRED
    DISABLE
  ADD CONSTRAINT FK_TapeDrive_runningTapeReq
    FOREIGN KEY (runningTapeReq)
    REFERENCES TapeRequest (id)
    DEFERRABLE
    INITIALLY DEFERRED
    DISABLE
  ADD CONSTRAINT FK_TapeDrive_deviceGroupName
    FOREIGN KEY (deviceGroupName)
    REFERENCES DeviceGroupName (id)
    DEFERRABLE
    INITIALLY DEFERRED
    DISABLE
  ADD CONSTRAINT FK_TapeDrive_status
    FOREIGN KEY (status)
    REFERENCES TapeDriveStatusCodes (id)
    DEFERRABLE
    INITIALLY IMMEDIATE
    ENABLE
  ADD CONSTRAINT FK_TapeDrive_tapeServer
    FOREIGN KEY (tapeServer)
    REFERENCES TapeServer (id)
    DEFERRABLE
    INITIALLY DEFERRED
    DISABLE;
CREATE INDEX I_FK_TapeDrive_tape            ON TapeDrive (tape);
CREATE INDEX I_FK_TapeDrive_runningTapeReq  ON TapeDrive (runningTapeReq);
CREATE INDEX I_FK_TapeDrive_deviceGroupName ON TapeDrive (deviceGroupName);
CREATE INDEX I_FK_TapeDrive_status          ON TapeDrive (status);
CREATE INDEX I_FK_TapeDrive_tapeServer      ON TapeDrive (tapeServer);

ALTER TABLE TapeDriveCompatibility
  ADD CONSTRAINT FK_TapeDriveCompatibility_id
    FOREIGN KEY (id)
    REFERENCES Id2Type (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
  ADD CONSTRAINT FK_TapeDriveComp_accessSpec
    FOREIGN KEY (tapeAccessSpecification)
    REFERENCES TapeAccessSpecification (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE;
CREATE INDEX I_FK_TapeDriveComp_accessSpec ON TapeDriveCompatibility (tapeAccessSpecification);

ALTER TABLE TapeDriveDedication
  ADD CONSTRAINT FK_TapeDriveDedication_id
    FOREIGN KEY (id)
    REFERENCES Id2Type (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
  ADD CONSTRAINT FK_TapeDriveDedic_tapeDrive
    FOREIGN KEY (tapeDrive)
    REFERENCES TapeDrive (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE;
CREATE INDEX I_FK_TapeDriveDedic_tapeDrive ON TapeDriveDedication (tapeDrive);

ALTER TABLE TapeRequest
  ADD CONSTRAINT FK_TapeRequest_id
    FOREIGN KEY (id)
    REFERENCES Id2Type (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
  ADD CONSTRAINT FK_TapeRequest_tape
    FOREIGN KEY (tape)
    REFERENCES VdqmTape (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
  ADD CONSTRAINT FK_TapeRequest_accessSpec
    FOREIGN KEY (tapeAccessSpecification)
    REFERENCES TapeAccessSpecification (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
  ADD CONSTRAINT FK_TapeRequest_requestedSrv
    FOREIGN KEY (requestedSrv)
    REFERENCES TapeServer (id)
    DEFERRABLE
    INITIALLY DEFERRED
    DISABLE
  ADD CONSTRAINT FK_TapeRequest_tapeDrive
    FOREIGN KEY (tapeDrive)
    REFERENCES TapeDrive (id)
    DEFERRABLE
    INITIALLY DEFERRED
    DISABLE
  ADD CONSTRAINT FK_TapeRequest_dgn
    FOREIGN KEY (deviceGroupName)
    REFERENCES DeviceGroupName (id)
    DEFERRABLE
    INITIALLY DEFERRED
    DISABLE
  ADD CONSTRAINT FK_TapeRequest_status
    FOREIGN KEY (status)
    REFERENCES TapeRequestStatusCodes (id)
    DEFERRABLE
    INITIALLY IMMEDIATE
    ENABLE
  ADD CONSTRAINT FK_TapeRequest_client
    FOREIGN KEY (client)
    REFERENCES ClientIdentification (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE;
CREATE INDEX I_FK_TapeRequest_tape         ON TapeRequest (tape);
CREATE INDEX I_FK_TapeRequest_accessSpec   ON TapeRequest (tapeAccessSpecification);
CREATE INDEX I_FK_TapeRequest_requestedSrv ON TapeRequest (requestedSrv);
CREATE INDEX I_FK_TapeRequest_tapeDrive    ON TapeRequest (tapeDrive);
CREATE INDEX I_FK_TapeRequest_dgn          ON TapeRequest (deviceGroupName);
CREATE INDEX I_FK_TapeRequest_status       ON TapeRequest (status);
CREATE INDEX I_FK_TapeRequest_client       ON TapeRequest (client);

ALTER TABLE TapeServer
  ADD CONSTRAINT FK_TapeServer_id
    FOREIGN KEY (id)
    REFERENCES Id2Type (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
  ADD CONSTRAINT FK_TapeServer_actingMode
    FOREIGN KEY (actingMode)
    REFERENCES TapeServerStatusCodes (id)
    DEFERRABLE
    INITIALLY IMMEDIATE
    ENABLE;
CREATE INDEX I_FK_TapeServer_actingMode ON TapeServer (actingMode);

ALTER TABLE VdqmTape
  ADD CONSTRAINT FK_VdqmTape_id
    FOREIGN KEY (id)
    REFERENCES Id2Type (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE;

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
 * PL/SQL trigger responsible for setting the modification time of a tape
 * drive when it is inserted into the TapeDrive table.
 */
CREATE OR REPLACE TRIGGER TR_I_TapeDrive
  BEFORE INSERT ON TapeDrive
FOR EACH ROW
BEGIN
  -- Set modification time
  :NEW.modificationTime := getTime();
END;


/**
 * PL/SQL trigger responsible for updating the modification time of a tape
 * drive.
 */
CREATE OR REPLACE TRIGGER TR_U_TapeDrive
  BEFORE UPDATE OF modificationTime, status ON TapeDrive 
FOR EACH ROW 
WHEN
  ((NEW.modificationTime != OLD.modificationTime) OR (NEW.status != OLD.status))
BEGIN
  -- Update the modification time
  :NEW.modificationTime := getTime();
END;


/**
 * PL/SQL trigger responsible for setting the creation time and initial
 * modification time of a tape request when it is inserted into the
 * TapeRequest table.
 */
CREATE OR REPLACE TRIGGER TR_I_TapeRequest
  BEFORE INSERT ON TapeRequest
FOR EACH ROW
DECLARE
  timeVar NUMBER := getTime();
BEGIN
  -- Set creation time
  :NEW.creationTime := timeVar;

  -- Set modification time
  :NEW.modificationTime := timeVar;
END;


/**
 * PL/SQL trigger responsible for updating the modification time of a tape
 * request.
 */
CREATE OR REPLACE TRIGGER TR_U_TapeRequest
  BEFORE UPDATE OF modificationTime, status ON TapeRequest
FOR EACH ROW
WHEN
  ((NEW.modificationTime != OLD.modificationTime) OR (NEW.status != OLD.status))
BEGIN
  -- Update the modification time
  :NEW.modificationTime := getTime();
END;


/**
 * This PL/SQL function returns 1 if the specified drive passes its mode
 * dedication if there is one, else it returns 0.
 *
 * @param driveIdVar    the ID of the drive
 * @param accessModeVar the access mode of the volume request
 */
CREATE OR REPLACE FUNCTION passesModeDedication(
  driveIdVar    IN NUMBER,
  accessModeVar IN NUMBER)
  RETURN NUMBER AS
  nbModeDedicationsVar NUMBER;
BEGIN
  -- Count the number of mode dedications for the drive
  -- (there should only be one)
  SELECT COUNT(*) INTO nbModeDedicationsVar
    FROM TapeDriveDedication
    WHERE tapeDrive = driveIdVar AND accessMode IS NOT NULL;

  -- Drive passes if there are no access mode dedications for it
  IF nbModeDedicationsVar = 0 THEN
    RETURN 1;
  END IF;

  -- Drive has a mode dedication

  -- Count the number of matching vid dedications
  -- (there should be a maximum of one)
  SELECT COUNT(*) INTO nbModeDedicationsVar
    FROM TapeDriveDedication
    WHERE tapeDrive = driveIdVar AND accessMode = accessModeVar;

  -- As there is a mode dedication for the drive, the drive only passes if it
  -- matches
  IF nbModeDedicationsVar > 0 THEN
    RETURN 1;
  ELSE
    RETURN 0;
  END IF;
END;


/**
 * This PL/SQL function returns 1 if the specified drive passes its host
 * dedications, else it returns 0.
 *
 * @param driveIdVar    the ID of the drive
 * @param clientHostVar the client host of the volume request
 */
CREATE OR REPLACE FUNCTION passesHostDedications(
  driveIdVar    IN NUMBER,
  clientHostVar IN VARCHAR2)
  RETURN NUMBER AS
  nbHostDedicationsVar NUMBER;
BEGIN
/*
  -- Determine if the host is dedicated to another drive
  SELECT COUNT(*) INTO nbHostDedicationsVar
    FROM TapeDriveDedication
    WHERE tapeDrive != driveIdVar AND clientHost = clientHostVar;

  -- Drive does not pass if the host is dedicated to another drive
  IF nbHostDedicationsVar > 0 THEN
    RETURN 0;
  END IF;
*/
  -- Count the number of host dedications for the drive
  SELECT COUNT(*) INTO nbHostDedicationsVar
    FROM TapeDriveDedication
    WHERE tapeDrive = driveIdVar AND clientHost IS NOT NULL;

  -- Drive passes if there are no host dedications for it
  IF nbHostDedicationsVar = 0 THEN
    RETURN 1;
  END IF;

  -- Drive has one or more host dedications

  -- Count the number of matching host dedications
  SELECT COUNT(*) INTO nbHostDedicationsVar
    FROM TapeDriveDedication
    WHERE
          tapeDrive = driveIdVar
      AND clientHost like '%' || clientHostVar || '%';

  -- As there are host dedications for the drive, it only passes if at least
  -- one matches
  IF nbHostDedicationsVar > 0 THEN
    RETURN 1;
  ELSE
    RETURN 0;
  END IF;

END;


/**
 * This PL/SQL function returns 1 if the specified drive passes its vid
 * dedications, else it returns 0.
 *
 * @param driveIdVar the ID of the drive
 * @param vidVar     the vid of the volume request
 */
CREATE OR REPLACE FUNCTION passesVidDedications(
  driveIdVar IN NUMBER,
  vidVar     IN VARCHAR2)
  RETURN NUMBER AS
  nbVidDedicationsVar NUMBER;
BEGIN
/*
  -- Determine if the vid is dedicated to another drive
  SELECT COUNT(*) INTO nbVidDedicationsVar
    FROM TapeDriveDedication
    WHERE tapeDrive != driveIdVar AND vid = vidVar;

  -- Drive does not pass if the vid is dedicated to another drive
  IF nbVidDedicationsVar > 0 THEN
    RETURN 0;
  END IF;
*/
  -- Count the number of vid dedications for the drive
  SELECT COUNT(*) INTO nbVidDedicationsVar
    FROM TapeDriveDedication
    WHERE tapeDrive = driveIdVar AND vid IS NOT NULL;

  -- Drive passes if there are no vid dedications for it
  IF nbVidDedicationsVar = 0 THEN
    RETURN 1;
  END IF;

  -- Drive has one or more vid dedications

  -- Count the number of matching vid dedications
  SELECT COUNT(*) INTO nbVidDedicationsVar
    FROM TapeDriveDedication
    WHERE tapeDrive = driveIdVar AND vid = vidVar;

  -- As there are vid dedications for the drive, it only passes if at least
  -- one matches
  IF nbVidDedicationsVar > 0 THEN
    RETURN 1;
  ELSE
    RETURN 0;
  END IF;

END;


/**
 * This PL/SQL function returns 1 if the specified drive passes all of its
 * dedications, else it returns 0.
 *
 * @param driveIdVar the ID of the drive
 * @param vidVar     the vid of the volume request
 */
CREATE OR REPLACE FUNCTION passesDedications(
  driveIdVar    IN NUMBER,
  clientHostVar IN VARCHAR2,
  accessModeVar IN NUMBER,
  vidVar        IN VARCHAR2)
  RETURN NUMBER AS
  nbVidDedicationsVar NUMBER;
BEGIN
  IF passesHostDedications(driveIdVar, clientHostVar) = 0 THEN
    RETURN 0;
  END IF;

  IF passesModeDedication(driveIdVar, accessModeVar) = 0 THEN
    RETURN 0;
  END IF;

  IF passesVidDedications(driveIdVar, vidVar) = 0 THEN
    RETURN 0;
  END IF;

  -- Drive has passed all of its dedications
  RETURN 1;
END;


/**
 * This view shows candidate "free tape drive to pending tape request"
 * allocations.
 */
CREATE OR REPLACE VIEW CandidateDriveAllocations_VIEW
AS SELECT UNIQUE
  TapeDrive.id as tapeDriveId,
  TapeRequest.id as tapeRequestId,
  TapeAccessSpecification.accessMode,
  TapeRequest.modificationTime
FROM
  TapeRequest
INNER JOIN TapeAccessSpecification ON
  TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
INNER JOIN VdqmTape ON
  TapeRequest.tape = VdqmTape.id
INNER JOIN ClientIdentification ON
  TapeRequest.client = ClientIdentification.id
INNER JOIN TapeDrive ON
  TapeRequest.deviceGroupName = TapeDrive.deviceGroupName
INNER JOIN TapeServer ON
     TapeRequest.requestedSrv = TapeServer.id
  OR TapeRequest.requestedSrv = 0
WHERE
      TapeDrive.status=0 -- UNIT_UP
  -- Exclude a request if its tape is associated with an on-going request
  AND NOT EXISTS (
    SELECT
      'x'
    FROM
      TapeRequest TapeRequest2
    WHERE
          TapeRequest2.tape = TapeRequest.tape
      AND TapeRequest2.tapeDrive != 0
  )
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
  AND TapeServer.actingMode=0 -- TAPE_SERVER_ACTIVE
  AND TapeRequest.status=0 -- REQUEST_PENDING
  AND passesDedications(tapeDrive.id, ClientIdentification.machine,
        TapeAccessSpecification.accessMode, VdqmTape.vid)=1
ORDER BY
  TapeAccessSpecification.accessMode DESC,
  TapeRequest.modificationTime ASC;


/**
 * This view shows candidate drive allocations that will reuse a current drive
 * allocation.
 */
CREATE OR REPLACE VIEW DriveAllocationsForReuse_VIEW
AS SELECT UNIQUE
  TapeDrive.id as tapeDriveId,
  TapeDrive.tape as tapeId,
  TapeRequest.id as tapeRequestId,
  TapeAccessSpecification.accessMode,
  TapeRequest.modificationTime
FROM
  TapeRequest
INNER JOIN TapeAccessSpecification ON
  TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
INNER JOIN VdqmTape ON
  TapeRequest.tape = VdqmTape.id
INNER JOIN ClientIdentification ON
  TapeRequest.client = ClientIdentification.id
INNER JOIN TapeDrive ON
  TapeRequest.tape = TapeDrive.tape -- Request is for the tape in the drive
INNER JOIN TapeServer ON
     TapeRequest.requestedSrv = TapeServer.id
  OR TapeRequest.requestedSrv = 0
WHERE
      TapeServer.actingMode=0 -- ACTIVE
  AND TapeRequest.tapeDrive=0 -- Request has not already been allocated a drive
  AND passesDedications(tapeDrive.id, ClientIdentification.machine,
        TapeAccessSpecification.accessMode, VdqmTape.vid)=1
ORDER BY
  TapeAccessSpecification.accessMode DESC,
  TapeRequest.modificationTime ASC;


/**
 * View used for generating the list of requests when replying to the
 * showqueues command
 */
CREATE OR REPLACE VIEW TapeRequestShowqueues_VIEW
AS WITH TimezoneOffset AS (
  SELECT
    (EXTRACT(TIMEZONE_HOUR FROM CURRENT_TIMESTAMP) - 1) * 3600 as value
  FROM DUAL)
SELECT
  TapeRequest.id,
  TapeDrive.driveName,
  TapeDrive.id AS tapeDriveId,
  TapeRequest.priority,
  ClientIdentification.port AS clientPort,
  ClientIdentification.euid AS clientEuid,
  ClientIdentification.egid AS clientEgid,
  TapeAccessSpecification.accessMode,
  TapeRequest.creationTime -
    (SELECT TimezoneOffset.value FROM TimezoneOffset) AS creationTime,
  ClientIdentification.machine AS clientMachine,
  VdqmTape.vid,
  TapeServer.serverName AS tapeServer,
  DeviceGroupName.dgName,
  ClientIdentification.username AS clientUsername
FROM
  TapeRequest
LEFT OUTER JOIN TapeDrive ON
  TapeRequest.tapeDrive = TapeDrive.id
LEFT OUTER JOIN ClientIdentification ON
  TapeRequest.client = ClientIdentification.id
INNER JOIN TapeAccessSpecification ON
  TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
LEFT OUTER JOIN VdqmTape ON
  TapeRequest.tape = VdqmTape.id
LEFT OUTER JOIN TapeServer ON
  TapeRequest.requestedSrv = TapeServer.id
LEFT OUTER JOIN DeviceGroupName ON
  TapeRequest.deviceGroupName = DeviceGroupName.id
ORDER BY
  TapeRequest.creationTime;


/**
 * Returns the dedications of the specified drive in the format required by
 * the showqueues command-line tool.
 */
CREATE OR REPLACE FUNCTION getVdqmDedicate(driveIdVar IN NUMBER)
  RETURN VARCHAR2
IS
  buf VARCHAR2(1024);
BEGIN
  FOR accessModeDedication IN (
    SELECT accessMode
      FROM TAPEDRIVEDEDICATION
      WHERE tapeDrive = driveIdVar AND accessMode IS NOT NULL
      ORDER BY accessMode)
  LOOP
    -- Add a comma if there is already a dedication in the buffer
    IF LENGTH(buf) > 0 THEN
      buf := buf || ',';
    END IF;

    -- Add dedication to buffer
    buf := buf || 'mode=' || accessModeDedication.accessMode;
  END LOOP;

  FOR clientHostDedication IN (
    SELECT clientHost
      FROM TAPEDRIVEDEDICATION
      WHERE tapeDrive = driveIdVar AND clientHost IS NOT NULL
      ORDER BY clientHost)
  LOOP
    -- Add a comma if there is already a dedication in the buffer
    IF LENGTH(buf) > 0 THEN
      buf := buf || ',';
    END IF;

    -- Add dedication to buffer
    buf := buf || 'host=' || clientHostDedication.clientHost;
  END LOOP;

  FOR vidDedication IN (
    SELECT vid
      FROM TAPEDRIVEDEDICATION
      WHERE tapeDrive = driveIdVar AND vid IS NOT NULL
      ORDER BY vid)
  LOOP
    -- Add a comma if there is already a dedication in the buffer
    IF LENGTH(buf) > 0 THEN
      buf := buf || ',';
    END IF;

    -- Add dedication to buffer
    buf := buf || 'vid=' || vidDedication.vid;
  END LOOP;

  RETURN buf;
END;


/**
 * View used for generating the list of drives when replying to the showqueues
 * command
 */
create or replace view TAPEDRIVESHOWQUEUES_VIEW
as with TIMEZONEOFFSET as (
  select
    (extract(timezone_hour from current_timestamp) - 1) * 3600 as VALUE
  from DUAL)
select
  TAPEDRIVE.STATUS, TAPEDRIVE.ID, TAPEDRIVE.RUNNINGTAPEREQ, TAPEDRIVE.JOBID,
  TAPEDRIVE.MODIFICATIONTIME -
    (select TIMEZONEOFFSET.VALUE from TIMEZONEOFFSET) as MODIFICATIONTIME,
  TAPEDRIVE.RESETTIME, TAPEDRIVE.USECOUNT, TAPEDRIVE.ERRCOUNT,
  TAPEDRIVE.TRANSFERREDMB, TAPEACCESSSPECIFICATION.ACCESSMODE as TAPEACCESSMODE,
  TAPEDRIVE.TOTALMB, TAPESERVER.SERVERNAME, VDQMTAPE.VID, TAPEDRIVE.DRIVENAME,
  DEVICEGROUPNAME.DGNAME, getVdqmDedicate(TAPEDRIVE.ID) as DEDICATE
from
  TAPEDRIVE
left outer join TAPESERVER on
  TAPEDRIVE.TAPESERVER = TAPESERVER.ID
left outer join VDQMTAPE on
  TAPEDRIVE.TAPE = VDQMTAPE.ID
left outer join DEVICEGROUPNAME on
  TAPEDRIVE.DEVICEGROUPNAME = DEVICEGROUPNAME.ID
left outer join TAPEDRIVEDEDICATION on
  TAPEDRIVE.ID = TAPEDRIVEDEDICATION.TAPEDRIVE
left outer join TAPEREQUEST on
  TAPEDRIVE.RUNNINGTAPEREQ = TAPEREQUEST.ID
left outer join TAPEACCESSSPECIFICATION on
  TAPEREQUEST.TAPEACCESSSPECIFICATION = TAPEACCESSSPECIFICATION.ID
order by
  DRIVENAME ASC;


/**
 * PL/SQL procedure which tries to allocate a free tape drive to a pending tape
 * request.
 *
 * @param returnVar has a value of 1 if a free drive was successfully allocated
 * to a pending request, 0 if no possible allocation could be found or -1 if an
 * allocation was found but was invalidated by other threads before the
 * appropriate locks could be taken.
 * @param tapeDriveIdVar if a free drive was successfully allocated then the
 * value of this parameter will be the ID of the allocated tape drive, else the
 * value of this parameter will be undefined.
 * @param tapeDriveIdVar if a free drive was successfully allocated then the
 * value of this parameter will be the name of the allocated tape drive, else
 * the value of this parameter will be undefined.
 * @param tapeRequestIdVar if a free drive was successfully allocated then the
 * value of this parameter will be the ID of the pending request, else the
 * value of this parameter will be undefined.
 * @param tapeRequestVidVar if a free drive was successfully allocated then the
 * value of this parameter will be the VID of the pending request, else the
 * value of this parameter will be undefined.
 */
CREATE OR REPLACE PROCEDURE allocateDrive(
  returnVar         OUT NUMBER,
  tapeDriveIdVar    OUT NUMBER,
  tapeDriveNameVar  OUT NOCOPY VARCHAR2,
  tapeRequestIdVar  OUT NUMBER,
  tapeRequestVidVar OUT NOCOPY VARCHAR2
  ) AS

  tapeDriveStatusVar   NUMBER;
  tapeRequestStatusVar NUMBER;

BEGIN
  returnVar         := 0; -- No possible allocation could be found
  tapeDriveIdVar    := 0;
  tapeDriveNameVar  := '';
  tapeRequestIdVar  := 0;
  tapeRequestVidVar := '';

  SELECT
    tapeDriveId,
    tapeRequestId
  INTO
    tapeDriveIdVar, tapeRequestIdVar
  FROM
    CandidateDriveAllocations_VIEW
  WHERE
    rownum < 2;

  -- If there is a possible drive allocation
  IF tapeDriveIdVar != 0 AND tapeRequestIdVar != 0 THEN

    -- The status of the drives and requests maybe modified by other scheduler
    -- threads.  The status of the drives may be modified by threads handling
    -- drive request messages.  The status of the requests may be modified by
    -- threads handling tape request messages.  Therefore get a lock on the
    -- corresponding drive and request rows and retrieve their statuses to see
    -- if the drive allocation is still valid
    SELECT TapeDrive.status INTO TapeDriveStatusVar
    FROM TapeDrive
    WHERE TapeDrive.id = tapeDriveIdVar
    FOR UPDATE;
    SELECT TapeRequest.status INTO TapeRequestStatusVar
    FROM TapeRequest
    WHERE TapeRequest.id = tapeRequestIdVar
    FOR UPDATE;

    -- Get the drive name (used for logging)
    SELECT TapeDrive.driveName INTO TapeDriveNameVar
    FROM TapeDrive
    WHERE TapeDrive.id = tapeDriveIdVar;
  
    -- Get the VID of the pending request (used for logging)
    SELECT VdqmTape.vid INTO tapeRequestVidVar
    FROM TapeRequest
    INNER JOIN VdqmTape ON TapeRequest.tape = VdqmTape.id
    WHERE TapeRequest.id = tapeRequestIdVar;

    -- If the drive allocation is still valid, i.e. drive status is UNIT_UP and
    -- request status is REQUEST_PENDING
    IF(TapeDriveStatusVar = 0) AND (TapeRequestStatusVar = 0) THEN

      -- Allocate the free drive to the pending request
      UPDATE TapeDrive SET
        status           = 1, -- UNIT_STARTING
        jobId            = 0,
        modificationTime = getTime(),
        runningTapeReq   = tapeRequestIdVar
      WHERE
        id = tapeDriveIdVar;
      UPDATE TapeRequest SET
        status           = 1, -- MATCHED
        tapeDrive        = tapeDriveIdVar,
        modificationTime = getTime()
      WHERE
        id = tapeRequestIdVar;

      -- A free drive has been allocated to a pending request
      returnVar := 1;

   -- Else the drive allocation is no longer valid
   ELSE

     -- A possible drive allocation was found but was invalidated by other
     -- threads before the appropriate locks could be taken
     returnVar := -1;

   END IF; -- If the drive allocation is still valid

  END IF; -- If there is a possible drive allocation

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
 * PL/SQL procedure which tries to reuse a drive allocation.
 *
 * @param tapeIdVar the ID of the tape of the current drive allocation.
 * @param tapeDriveIdVar the ID of the drive of the current drive allocation.
 * @param returnVar has a value of 1 if the specified drive allocation was
 * successfully reused, 0 if no possible reuse was found or -1 if a possible
 * reuse was found but was invalidated by other threads before the appropriate
 * locks could be taken.
 * @param tapeRequestIdVar if the drive allocation was successfully reused then
 * the value of this parameter will be the ID of the newly assigned request,
 * else the value of this parameter will be undefined.
 */
CREATE OR REPLACE PROCEDURE reuseDriveAllocation(
  tapeIdVar          IN NUMBER,
  tapeDriveIdVar     IN NUMBER,
  returnVar         OUT NUMBER,
  tapeRequestIdVar  OUT NUMBER)
AS
  tapeDriveStatusVar   NUMBER;
  mountedTapeIdVar     NUMBER;
  tapeRequestStatusVar NUMBER;
BEGIN
  returnVar         := 0; -- No possible reuse was found
  tapeRequestIdVar  := 0;

  -- Try to find a candidate volume request that can reuse the current drive
  -- allocation
  SELECT tapeRequestId INTO tapeRequestIdVar
    FROM DriveAllocationsForReuse_VIEW
    WHERE
          DriveAllocationsForReuse_VIEW.TapeDriveId = tapeDriveIdVar
      AND DriveAllocationsForReuse_VIEW.TapeId      = tapeIdVar
      AND rownum < 2;

  -- A candidate was found, because a NO_DATA_FOUND exception was not raised

  -- The status of the drives including which tapes may be mounted in them,
  -- may be modified by other threads handling drive request messages.  The
  -- status of the requests may be modified by threads handling tape request
  -- messages.  Therefore get a lock on the corresponding drive and request
  -- rows and retrieve their statuses and mounted tape in the case of the
  -- drive, to see if the reuse of the drive allocation is still valid.
  SELECT TapeDrive.status, TapeDrive.tape
  INTO TapeDriveStatusVar, mountedTapeIdVar
  FROM TapeDrive
  WHERE TapeDrive.id = tapeDriveIdVar
  FOR UPDATE;
  SELECT TapeRequest.status
  INTO TapeRequestStatusVar
  FROM TapeRequest
  WHERE TapeRequest.id = tapeRequestIdVar
  FOR UPDATE; 

  -- If the reuse of the drive allocation is still valid, i.e. the drive's
  -- status is VOL_MOUNTED and the correct tape is mounted and the tape
  -- request's status is REQUEST_PENDING
  IF(tapeDriveStatusVar = 3) AND(mountedTapeIdVar = tapeIdVar) AND
    (tapeRequestStatusVar = 0) THEN

    -- Reuse the drive allocation with the pending request
    UPDATE TapeRequest SET
      status           = 1,  -- MATCHED
      tapeDrive        = tapeDriveIdVar,
      modificationTime = getTime()
    WHERE id = tapeRequestIdVar;
    UPDATE TapeDrive SET
      status           = 1, -- UNIT_STARTING
      jobId            = 0,
      runningTapeReq   = tapeRequestIdVar,
      modificationTime = getTime()
    WHERE id = tapeDriveIdVar;

    -- The drive allocation was reused
    returnVar := 1;

   -- Else the reuse of the drive allocation is no longer valid
   ELSE

     -- A possible reuse of the drive allocation was found but was invalidated
     -- by other threads before the appropriate locks could be taken
     returnVar := -1;

  END IF; -- If the reuse of the drive allocation is still valid

EXCEPTION
  -- Return a tape request ID of 0 if there was no candidate volume request
  -- which would have reused the current drive allocation
  WHEN NO_DATA_FOUND THEN NULL;
END;


/**
 * PL/SQL procedure to insert the specified drive dedications into the database.
 *
 * @param driveNameVar the name of the tape drive to be dedicated
 * @param serverNameVar the name of the tape server of the tape drive
 * @param dgNameVar the name of the device group of the tape drive
 * @param accessModeVar the access mode dedication
 * @param clientHostVar the client host dedication
 * @param vidVar the vid dedication
 * @param resultVar is 0 if the dedications were successfully inserted into the
 * database, -1 if the specified tape drive does not exist, -2 if the
 * specified tape server is not associated with the specified tape drive and -3
 * if the specified dgn is not associated with the specified tape drive
 */
CREATE OR REPLACE PROCEDURE dedicateDrive
( driveNameVar  IN  VARCHAR2
, serverNameVar IN  VARCHAR2
, dgNameVar     IN  VARCHAR2
, accessModeVar IN  NUMBER
, clientHostVar IN  VARCHAR2
, vidVar        IN  VARCHAR2
, resultVar     OUT INTEGER
) AS
  driveIdVar           NUMBER;
  dgnIdVar             NUMBER;
  serverIdVar          NUMBER;
  nbMatchingServersVar NUMBER;
  nbMatchingDgnsVar    NUMBER;
  TYPE dedicationList_t IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
  dedicationsToDelete  dedicationList_t;
  dedicationIdVar      NUMBER;
BEGIN
  resultVar := 0;

  -- Lock the tape drive row
  BEGIN
    SELECT
      TapeDrive.id, TapeDrive.deviceGroupName, TapeDrive.tapeServer
      INTO driveIdVar, dgnIdVar, serverIdVar
      FROM TapeDrive
      INNER JOIN TapeServer ON
        TapeDrive.tapeServer = TapeServer.id
      WHERE
        TapeDrive.driveName = driveNameVar
      FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    resultVar := -1; -- No such tape drive
    RETURN;
  END;

  -- Check the specified tape drive is associated with the tape server
  SELECT count(*) INTO nbMatchingServersVar
    FROM TapeServer
    WHERE
          TapeServer.id         = serverIdVar
      AND TapeServer.serverName = serverNameVar;

  IF nbMatchingServersVar = 0 THEN
    resultVar := -2; -- Tape server is not associated with tape drive
    RETURN;
  END IF;

  -- Check the specified dgn is associated with the tape server
  SELECT count(*) INTO nbMatchingDgnsVar
    FROM DeviceGroupName
    WHERE
          DeviceGroupName.id     = dgnIdVar
      AND DeviceGroupName.dgName = dgNameVar;

  IF nbMatchingDgnsVar = 0 THEN
    resultVar := -3; -- Tape server is not associated with dgn
    RETURN;
  END IF;

  -- Delete all existing dedications associated with tape drive
  SELECT id BULK COLLECT INTO dedicationsToDelete
    FROM TapeDriveDedication
    WHERE TapeDriveDedication.tapeDrive = driveIdVar;

  IF dedicationsToDelete.COUNT > 0 THEN
    FOR i IN dedicationsToDelete.FIRST .. dedicationsToDelete.LAST LOOP
      DELETE FROM TapeDriveDedication
        WHERE TapeDriveDedication.id = dedicationsToDelete(i);
      DELETE FROM Id2Type
        WHERE Id2Type.id = dedicationsToDelete(i);
    END LOOP;
  END IF;

  -- Insert new dedications
  IF accessModeVar = 0 THEN
    INSERT INTO TapeDriveDedication(id, tapeDrive, accessMode)
      VALUES(ids_seq.nextval, driveIdVar, accessModeVar)
    RETURNING id INTO dedicationIdVar;
    INSERT INTO Id2Type (id, type)
      VALUES (dedicationIdVar, 90);
  END IF;
  IF (clientHostVar IS NOT NULL) AND (clientHostVar != '.*') THEN
    INSERT INTO TapeDriveDedication(id, tapeDrive, clientHost)
      VALUES(ids_seq.nextval, driveIdVar, clientHostVar)
    RETURNING id INTO dedicationIdVar;
    INSERT INTO Id2Type (id, type)
      VALUES (dedicationIdVar, 90);
  END IF;
  IF (vidVar IS NOT NULL) AND (vidVar != '.*') THEN
    INSERT INTO TapeDriveDedication(id, tapeDrive, vid)
      VALUES(ids_seq.nextval, driveIdVar, vidVar)
    RETURNING id INTO dedicationIdVar;
    INSERT INTO Id2Type (id, type)
      VALUES (dedicationIdVar, 90);
  END IF;

END;


/**
 * PL/SQL procedure to delete the specified drive from the database.
 *
 * @param driveNameVar the name of the tape drive to be delete
 * @param serverNameVar the name of the tape server of the tape drive
 * @param dgNameVar the name of the device group of the tape drive
 * @param resultVar is 0 if the deletion was successful. -1 if the specified
 * tape server does not exist, -2 if the specified device group name does not
 * exist, -3 if the specified tape drive does not exist and -4 if the specified
 * drive has a job assigned.
 */
CREATE OR REPLACE PROCEDURE deleteDrive
( driveNameVar  IN  VARCHAR2
, serverNameVar IN  VARCHAR2
, dgNameVar     IN  VARCHAR2
, resultVar     OUT INTEGER
) AS
  dgnIdVar        NUMBER;
  tapeServerIdVar NUMBER;
  driveIdVar      NUMBER;
  driveStatusVar  NUMBER;
BEGIN
  resultVar := 0;

  BEGIN
    SELECT id INTO tapeServerIdVar FROM TapeServer
      WHERE
        serverName = serverNameVar;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    resultVar := -1; -- Tape server does not exist
    RETURN;
  END;
  
  BEGIN
    SELECT id INTO dgnIdVar FROM DeviceGroupName WHERE dgName = dgNameVar;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    resultVar := -2; -- DGN does not exits
    RETURN;
  END;
  
  BEGIN
    SELECT id, status INTO driveIdVar, driveStatusVar FROM TapeDrive
      WHERE
            deviceGroupName = dgnIdVar
        AND tapeServer = tapeServerIdVar
        AND driveName = driveNameVar
      FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    resultVar := -3; -- Tape drive does not exist
    RETURN;
  END;
  
  -- Not UNIT_UP and not UNIT_DOWN
  IF driveStatusVar != 0 AND driveStatusVar != 5 THEN
    resultVar := -4; -- Drive has a job assigned
    RETURN;
  END IF;
  
  DELETE From TapeDrive2TapeDriveComp WHERE parent = driveIdVar;
  DELETE FROM TapeDrive WHERE id = driveIdVar;
  DELETE FROM Id2Type WHERE id = driveIdVar;
END;


/**
 * PL/SQL procedure which tries to write to the database the fact that a
 * successful RTCPD job submission has occured.  This update of the database
 * may not be possible if the corresponding drive and tape request states have
 * been modified by other threads.  For example a thread handling a tape drive
 * request message may have put the drive into the down state.  The RTCPD job
 * submission should be ignored in this case.
 *
 * @param tapeDriveIdVar the ID of the drive
 * @param tapeRequestIdVar the ID of the tape request
 * @param returnVar has a value of 1 if the occurance of the RTCPD job
 * submission was successfully written to the database, else 0.
 */
CREATE OR REPLACE PROCEDURE writeRTPCDJobSubmission(
  tapeDriveIdVar    IN NUMBER,
  tapeRequestIdVar  IN NUMBER,
  returnVar        OUT NUMBER)
AS
  tapeDriveStatusVar      NUMBER;
  runningTapeRequestIdVar NUMBER;
  tapeRequestStatusVar    NUMBER;
BEGIN
  returnVar := 0; -- RTCPD job submission not written to database

  -- The status of the tape requests may be modified by threads handling tape
  -- request messages.  The status of the drives may be modified by threads
  -- handling drive request messages.  One such modification may be the
  -- bringing down of the drive in question, which would make it meaningless to
  -- record a successful RTPCD job submission.  Therefore get a lock on the
  -- corresponding request and drive rows and retrieve their statuses to see
  -- whether or not the success of the RTPCD job submission should be recorded
  -- in the database.
  SELECT TapeDrive.status, TapeDrive.runningTapeReq
  INTO tapeDriveStatusVar, runningTapeRequestIdVar
  FROM TapeDrive
  WHERE TapeDrive.id = tapeDriveIdVar
  FOR UPDATE;
  SELECT TapeRequest.status
  INTO tapeRequestStatusVar
  FROM TapeRequest
  WHERE TapeRequest.id = tapeRequestIdVar
  FOR UPDATE;

  -- If recording of successfull RTCPD job submission is permitted,
  -- i.e. the status of the drive is UNIT_STARTING and the drive is still
  -- paired with the tape request and the status of the tape request is
  -- REQUEST_BEINGSUBMITTED
  IF (tapeDriveStatusVar = 1) AND (runningTapeRequestIdVar = tapeRequestIdVar)
    AND (tapeRequestStatusVar = 2) THEN

    -- Set the state of the tape request to REQUEST_SUBMITTED (3)
    UPDATE TapeRequest
    SET TapeRequest.status = 3
    WHERE TapeRequest.id = tapeRequestIdVar;

    returnVar := 1; -- RTCPD job submission written to database
  END IF;

EXCEPTION

  -- Do nothing if either the drive or request no longer exist in the database
  WHEN NO_DATA_FOUND THEN NULL;

END;


/**
 * PL/SQL procedure which tries to write to the database the fact that a
 * failed RTCPD job submission has occured.  This update of the database
 * may not be possible if the corresponding drive and tape request states have
 * been modified by other threads.  For example a thread handling a tape drive
 * request message may have put the drive into the down state.  The failed
 * RTCPD job submission should be ignored in this case.
 *
 * @param tapeDriveIdVar the ID of the drive
 * @param tapeRequestIdVar the ID of the tape request
 * @param returnVar has a value of 1 if the occurance of the failed RTCPD job
 * submission was successfully written to the database, else 0.
 */
CREATE OR REPLACE PROCEDURE writeFailedRTPCDJobSubmission(
  tapeDriveIdVar    IN NUMBER,
  tapeRequestIdVar  IN NUMBER,
  returnVar        OUT NUMBER)
AS
  tapeDriveStatusVar      NUMBER;
  runningTapeRequestIdVar NUMBER;
  tapeRequestStatusVar    NUMBER;
BEGIN
  returnVar := 0; -- Failed RTCPD job submission not written to database

  -- The status of the tape requests may be modified by threads handling tape
  -- request messages.  The status of the drives may be modified by threads
  -- handling drive request messages.  One such modification may be the
  -- bringing down of the drive in question, which would make it meaningless to
  -- record a successful RTPCD job submission.  Therefore get a lock on the
  -- corresponding request and drive rows and retrieve their statuses to see
  -- whether or not the success of the RTPCD job submission should be recorded
  -- in the database.
  SELECT TapeDrive.status, TapeDrive.runningTapeReq
  INTO tapeDriveStatusVar, runningTapeRequestIdVar
  FROM TapeDrive
  WHERE TapeDrive.id = tapeDriveIdVar
  FOR UPDATE;
  SELECT TapeRequest.status
  INTO tapeRequestStatusVar
  FROM TapeRequest
  WHERE TapeRequest.id = tapeRequestIdVar
  FOR UPDATE;

  -- If recording of failed RTCPD job submission is permitted,
  -- i.e. the status of the drive is UNIT_STARTING and the drive is still
  -- paired with the tape request and the status of the tape request is
  -- REQUEST_BEINGSUBMITTED
  IF (tapeDriveStatusVar = 1) AND (runningTapeRequestIdVar = tapeRequestIdVar)
    AND (tapeRequestStatusVar = 2) THEN

    -- Unlink the tape drive from the tape request
    UPDATE TapeDrive
    SET TapeDrive.runningTapeReq = 0
    WHERE TapeDrive.id = tapeDriveIdVar;
    UPDATE TapeRequest
    SET TapeRequest.tapeDrive = 0
    WHERE TapeRequest.id = tapeRequestIdVar;

    -- set the state of the tape drive to STATUS_UNKNOWN (7)
    UPDATE TapeDrive
    SET TapeDrive.status = 7
    WHERE TapeDrive.id = tapeDriveIdVar;

    -- Set the state of the tape request to REQUEST_PENDING (0)
    UPDATE TapeRequest
    SET TapeRequest.status = 0
    WHERE TapeRequest.id = tapeRequestIdVar;

    returnVar := 1; -- Failed RTCPD job submission written to database
  END IF;

EXCEPTION

  -- Do nothing if either the drive or request no longer exist in the database
  WHEN NO_DATA_FOUND THEN NULL;

END;
