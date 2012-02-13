/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE;

/* SQL statements for type TapeAccessSpecification */
CREATE TABLE TapeAccessSpecification (accessMode NUMBER, density VARCHAR2(2048), tapeModel VARCHAR2(2048), id INTEGER CONSTRAINT PK_TapeAccessSpecification_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeServer */
CREATE TABLE TapeServer (serverName VARCHAR2(2048), id INTEGER CONSTRAINT PK_TapeServer_Id PRIMARY KEY, actingMode INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeRequest */
CREATE TABLE TapeRequest (priority NUMBER, modificationTime INTEGER, creationTime INTEGER, errorCode NUMBER, errorMessage VARCHAR2(2048), remoteCopyType VARCHAR2(2048), id INTEGER CONSTRAINT PK_TapeRequest_Id PRIMARY KEY, tape INTEGER, tapeAccessSpecification INTEGER, requestedSrv INTEGER, tapeDrive INTEGER, deviceGroupName INTEGER, status INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeDrive */
CREATE TABLE TapeDrive (jobID NUMBER, modificationTime INTEGER, resettime INTEGER, usecount NUMBER, errcount NUMBER, transferredMB NUMBER, totalMB INTEGER, driveName VARCHAR2(2048), id INTEGER CONSTRAINT PK_TapeDrive_Id PRIMARY KEY, tape INTEGER, runningTapeReq INTEGER, deviceGroupName INTEGER, status INTEGER, tapeServer INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE TapeDrive2TapeDriveComp (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_TapeDrive2TapeDriveComp_C on TapeDrive2TapeDriveComp (child);
CREATE INDEX I_TapeDrive2TapeDriveComp_P on TapeDrive2TapeDriveComp (parent);

/* SQL statements for type TapeDriveDedication */
CREATE TABLE TapeDriveDedication (clientHost VARCHAR2(2048), euid NUMBER, egid NUMBER, vid VARCHAR2(2048), accessMode NUMBER, startTime INTEGER, endTime INTEGER, reason VARCHAR2(2048), id INTEGER CONSTRAINT PK_TapeDriveDedication_Id PRIMARY KEY, tapeDrive INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeDriveCompatibility */
CREATE TABLE TapeDriveCompatibility (tapeDriveModel VARCHAR2(2048), priorityLevel NUMBER, id INTEGER CONSTRAINT PK_TapeDriveCompatibility_Id PRIMARY KEY, tapeAccessSpecification INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type DeviceGroupName */
CREATE TABLE DeviceGroupName (dgName VARCHAR2(2048), libraryName VARCHAR2(2048), id INTEGER CONSTRAINT PK_DeviceGroupName_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type VdqmTape */
CREATE TABLE VdqmTape (vid VARCHAR2(2048), id INTEGER CONSTRAINT PK_VdqmTape_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type ClientIdentification */
CREATE TABLE ClientIdentification (machine VARCHAR2(2048), userName VARCHAR2(2048), port NUMBER, euid NUMBER, egid NUMBER, magic NUMBER, id INTEGER CONSTRAINT PK_ClientIdentification_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type VolumePriority */
CREATE TABLE VolumePriority (priority NUMBER, clientUID NUMBER, clientGID NUMBER, clientHost VARCHAR2(2048), vid VARCHAR2(2048), tpMode NUMBER, lifespanType NUMBER, creationTime INTEGER, modificationTime INTEGER, id INTEGER CONSTRAINT PK_VolumePriority_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Tape2DriveDedication */
CREATE TABLE Tape2DriveDedication (vid VARCHAR2(2048), creationTime INTEGER, modificationTime INTEGER, id INTEGER CONSTRAINT PK_Tape2DriveDedication_Id PRIMARY KEY, tapeDrive INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for constraints on TapeDrive */
ALTER TABLE TapeDrive2TapeDriveComp
  ADD CONSTRAINT FK_TapeDrive2TapeDriveComp_P FOREIGN KEY (Parent) REFERENCES TapeDrive (id)
  ADD CONSTRAINT FK_TapeDrive2TapeDriveComp_C FOREIGN KEY (Child) REFERENCES TapeDriveCompatibility (id);


/* SQL statements for table UpgradeLog */
CREATE TABLE UpgradeLog (Username VARCHAR2(64) DEFAULT sys_context('USERENV', 'OS_USER') CONSTRAINT NN_UpgradeLog_Username NOT NULL, SchemaName VARCHAR2(64) DEFAULT 'VDQM' CONSTRAINT NN_UpgradeLog_SchemaName NOT NULL, Machine VARCHAR2(64) DEFAULT sys_context('USERENV', 'HOST') CONSTRAINT NN_UpgradeLog_Machine NOT NULL, Program VARCHAR2(48) DEFAULT sys_context('USERENV', 'MODULE') CONSTRAINT NN_UpgradeLog_Program NOT NULL, StartDate TIMESTAMP(6) WITH TIME ZONE DEFAULT sysdate, EndDate TIMESTAMP(6) WITH TIME ZONE, FailureCount NUMBER DEFAULT 0, Type VARCHAR2(20) DEFAULT 'NON TRANSPARENT', State VARCHAR2(20) DEFAULT 'INCOMPLETE', SchemaVersion VARCHAR2(20) CONSTRAINT NN_UpgradeLog_SchemaVersion NOT NULL, Release VARCHAR2(20) CONSTRAINT NN_UpgradeLog_Release NOT NULL);

/* SQL statements for check constraints on the UpgradeLog table */
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_State
  CHECK (state IN ('COMPLETE', 'INCOMPLETE'));
  
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_Type
  CHECK (type IN ('TRANSPARENT', 'NON TRANSPARENT'));

/* SQL statement to populate the intial release value */
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('-', '2_1_12_2');

/* SQL statement to create the CastorVersion view */
CREATE OR REPLACE VIEW CastorVersion
AS
  SELECT decode(type, 'TRANSPARENT', schemaVersion,
           decode(state, 'INCOMPLETE', state, schemaVersion)) schemaVersion,
         decode(type, 'TRANSPARENT', release,
           decode(state, 'INCOMPLETE', state, release)) release,
         schemaName
    FROM UpgradeLog
   WHERE startDate =
     (SELECT max(startDate) FROM UpgradeLog);

/*******************************************************************
 * This file contains SQL code that is not generated automatically
 * and is inserted at the end of the generated code
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_12_0';

/* Sequence used to generate unique indentifies */
CREATE SEQUENCE ids_seq CACHE 200;

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

/**
 * Lock table used to serialise the drive scheduler algorithm.
 */
CREATE TABLE VdqmLock(
  id INTEGER,
  name VARCHAR2(128) CONSTRAINT NN_VdqmLock_name NOT NULL,
  CONSTRAINT PK_VdqmLock_id PRIMARY KEY(id),
  CONSTRAINT U_VdqmLock_name UNIQUE(name));

/**
 * Insert the lock used to serialise the databse scheduler.
 */
INSERT INTO VdqmLock(id, name) VALUES(1, 'SCHEDULER');
COMMIT;

/* Not null column constraints */
ALTER TABLE ClientIdentification MODIFY
  (egid CONSTRAINT NN_ClientIdentification_egid NOT NULL);
ALTER TABLE ClientIdentification MODIFY
  (euid CONSTRAINT NN_ClientIdentification_euid NOT NULL);
ALTER TABLE ClientIdentification MODIFY
  (magic CONSTRAINT NN_ClientIdentification_magic NOT NULL);
ALTER TABLE ClientIdentification MODIFY
  (port CONSTRAINT NN_ClientIdentification_port NOT NULL);
ALTER TABLE TapeAccessSpecification MODIFY
  (accessMode CONSTRAINT NN_TapeAccessSpec_accessMode NOT NULL);
ALTER TABLE TapeDrive MODIFY
 (deviceGroupName CONSTRAINT NN_TapeDrive_deviceGroupName NOT NULL);
ALTER TABLE TapeDrive MODIFY
 (errCount CONSTRAINT NN_TapeDrive_errCount NOT NULL);
ALTER TABLE TapeDrive MODIFY
 (jobId CONSTRAINT NN_TapeDrive_jobId NOT NULL);
ALTER TABLE TapeDrive MODIFY
 (modificationTime CONSTRAINT NN_TapeDrive_modificationTime NOT NULL);
ALTER TABLE TapeDrive MODIFY
 (resetTime CONSTRAINT NN_TapeDrive_resetTime NOT NULL);
ALTER TABLE TapeDrive MODIFY
 (status CONSTRAINT NN_TapeDrive_status NOT NULL);
ALTER TABLE TapeDrive MODIFY
 (tapeServer CONSTRAINT NN_TapeDrive_tapeServer NOT NULL);
ALTER TABLE TapeDrive MODIFY
 (totalMB CONSTRAINT NN_TapeDrive_totalMB NOT NULL);
ALTER TABLE TapeDrive MODIFY
 (transferredMB CONSTRAINT NN_TapeDrive_transferredMB NOT NULL);
ALTER TABLE TapeDrive MODIFY
 (useCount CONSTRAINT NN_TapeDrive_useCount NOT NULL);
ALTER TABLE TapeDrive2TapeDriveComp MODIFY
 (child CONSTRAINT NN_TapeDrv2TapeDrvComp_child NOT NULL);
ALTER TABLE TapeDrive2TapeDriveComp MODIFY
 (parent CONSTRAINT NN_TapeDrv2TapeDrvComp_parent NOT NULL);
ALTER TABLE TapeDriveCompatibility MODIFY
 (priorityLevel CONSTRAINT NN_TapeDriveComp_priorityLevel NOT NULL);
ALTER TABLE TapeDriveCompatibility MODIFY
 (tapeAccessSpecification CONSTRAINT NN_TapeDriveComp_accessSpec NOT NULL);
ALTER TABLE TapeDriveDedication MODIFY
 (tapeDrive CONSTRAINT NN_TapeDrvDedic_tapeDrive NOT NULL);
ALTER TABLE TapeRequest MODIFY
 (client CONSTRAINT NN_TapeRequest_client NOT NULL);
ALTER TABLE TapeRequest MODIFY
 (creationTime CONSTRAINT NN_TapeRequest_creationTime NOT NULL);
ALTER TABLE TapeRequest MODIFY
 (deviceGroupName CONSTRAINT NN_TapeRequest_deviceGroupName NOT NULL);
ALTER TABLE TapeRequest MODIFY
 (errorCode CONSTRAINT NN_TapeRequest_errorCode NOT NULL);
ALTER TABLE TapeRequest MODIFY
 (modificationTime CONSTRAINT NN_TapeRequest_modTime NOT NULL);
ALTER TABLE TapeRequest MODIFY
 (priority CONSTRAINT NN_TapeRequest_priority NOT NULL);
ALTER TABLE TapeRequest MODIFY
 (remoteCopyType CONSTRAINT NN_TapeRequest_remoteCopyType NOT NULL);
ALTER TABLE TapeRequest MODIFY
 (status CONSTRAINT NN_TapeRequest_status NOT NULL);
ALTER TABLE TapeRequest MODIFY
 (tape CONSTRAINT NN_TapeRequest_tape NOT NULL);
ALTER TABLE TapeRequest MODIFY
 (tapeAccessSpecification CONSTRAINT NN_TapeRequest_accessSpec NOT NULL);
ALTER TABLE TapeServer MODIFY
 (actingMode CONSTRAINT NN_TapeServer_actingMode NOT NULL);
ALTER TABLE TapeServer MODIFY
 (serverName CONSTRAINT NN_TapeServer_serverName NOT NULL);
ALTER TABLE VolumePriority MODIFY
 (priority CONSTRAINT NN_VolumePriority_priority NOT NULL);
ALTER TABLE VolumePriority MODIFY
 (clientUID CONSTRAINT NN_VolumePriority_clientUID NOT NULL);
ALTER TABLE VolumePriority MODIFY
 (clientGID CONSTRAINT NN_VolumePriority_clientGID NOT NULL);
ALTER TABLE VolumePriority MODIFY
 (clientHost CONSTRAINT NN_VolumePriority_clientHost NOT NULL);
ALTER TABLE VolumePriority MODIFY
 (vid CONSTRAINT NN_VolumePriority_vid NOT NULL);
ALTER TABLE VolumePriority MODIFY
 (tpMode CONSTRAINT NN_VolumePriority_tpMode NOT NULL);
ALTER TABLE VolumePriority MODIFY
 (lifespanType CONSTRAINT NN_VolumePriority_lifespanType NOT NULL);
ALTER TABLE Tape2DriveDedication MODIFY
  (vid CONSTRAINT NN_Tp2DrvDedic_vid NOT NULL);
ALTER TABLE Tape2DriveDedication MODIFY
  (tapeDrive CONSTRAINT NN_Tp2DrvDedic_tapeDrive NOT NULL);

/* Unique constraints */
-- A client host can only be dedicated to one drive
--ALTER TABLE TapeDriveDedication
--  ADD CONSTRAINT I_U_TapeDrvDedic_clientHost
--    UNIQUE (clientHost);

-- A vid can only be dedicated to one drive
--ALTER TABLE TapeDriveDedication
--  ADD CONSTRAINT I_U_TapeDrvDedic_vid
--    UNIQUE (vid);

-- Tape VIDs are unique
ALTER TABLE VdqmTape
  ADD CONSTRAINT I_U_VdqmTape_vid
    UNIQUE (vid);

-- A volume priority is identified by vid, tapeMode and lifespanType
ALTER TABLE VolumePriority
  ADD CONSTRAINT I_U_VolPriority_vid_mode_life
    UNIQUE (vid, tpMode, lifespanType);

-- A tape drive is identified by driveName and tapeServer
ALTER TABLE TapeDrive
  ADD CONSTRAINT I_U_TapeDrive_name_server 
  UNIQUE (driveName, tapeServer);

-- A tape server is identified by its name
ALTER TABLE TapeServer
  ADD CONSTRAINT I_U_TapeServer_serverName
  UNIQUE (serverName);

-- A tape dedication is made unique by its VID and tape drive
ALTER TABLE Tape2DriveDedication
  ADD CONSTRAINT I_U_Tp2DrvDedic_vid_tapeDrive
    UNIQUE (vid, tapeDrive);

/* Check constraints */
-- The accessMode column of the TapeAccessSpecification table has 2 possible
-- values: 0=write-disabled or 1=write-enabled
ALTER TABLE TapeAccessSpecification
  ADD CONSTRAINT CH_TapeAccessSpec_accessMode
    CHECK ((accessMode=0) OR (accessMode=1));

-- The remoteCopyType column of the TapeRequest table has 3 possible
-- values: 'RTCPD', 'AGGREGATOR' or 'TAPEBRIDGE'
-- The value 'AGGREGATOR' is deprecated by the value 'TAPEBRIDGE'
ALTER TABLE TapeRequest
  ADD CONSTRAINT CH_TapeRequest_remoteCopyType
    CHECK (
      (remoteCopyType='RTCPD')      OR
      (remoteCopyType='AGGREGATOR') OR
      (remoteCopyType='TAPEBRIDGE'));

-- The tpMode column of the VolumePriority table has 2 possible
-- values: 0=write-disabled or 1=write-enabled
ALTER TABLE VolumePriority
  ADD CONSTRAINT CH_VolumePriority_tpMode
    CHECK ((tpMode=0) OR (tpMode=1));

-- The lifespanType column of the VolumePriority table has 2 possible
-- values: 0=single-shot or 1=unlimited
ALTER TABLE VolumePriority
  ADD CONSTRAINT CH_VolumePriority_lifespanType
    CHECK ((lifespanType=0) OR (lifespanType=1));


/* Foreign key constraints with an index for each */
ALTER TABLE TapeDrive
  ADD CONSTRAINT FK_TapeDrive_tape
    FOREIGN KEY (tape)
    REFERENCES VdqmTape (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
  ADD CONSTRAINT FK_TapeDrive_runningTapeReq
    FOREIGN KEY (runningTapeReq)
    REFERENCES TapeRequest (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
  ADD CONSTRAINT FK_TapeDrive_deviceGroupName
    FOREIGN KEY (deviceGroupName)
    REFERENCES DeviceGroupName (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
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
    ENABLE;
CREATE INDEX I_FK_TapeDrive_tape            ON TapeDrive (tape);
CREATE INDEX I_FK_TapeDrive_runningTapeReq  ON TapeDrive (runningTapeReq);
CREATE INDEX I_FK_TapeDrive_deviceGroupName ON TapeDrive (deviceGroupName);
CREATE INDEX I_FK_TapeDrive_status          ON TapeDrive (status);
CREATE INDEX I_FK_TapeDrive_tapeServer      ON TapeDrive (tapeServer);

ALTER TABLE TapeDriveCompatibility
  ADD CONSTRAINT FK_TapeDriveComp_accessSpec
    FOREIGN KEY (tapeAccessSpecification)
    REFERENCES TapeAccessSpecification (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE;
CREATE INDEX I_FK_TapeDriveComp_accessSpec ON TapeDriveCompatibility (tapeAccessSpecification);

ALTER TABLE TapeDriveDedication
  ADD CONSTRAINT FK_TapeDriveDedic_tapeDrive
    FOREIGN KEY (tapeDrive)
    REFERENCES TapeDrive (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE;
CREATE INDEX I_FK_TapeDriveDedic_tapeDrive ON TapeDriveDedication (tapeDrive);

ALTER TABLE TapeRequest
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
    ENABLE
  ADD CONSTRAINT FK_TapeRequest_tapeDrive
    FOREIGN KEY (tapeDrive)
    REFERENCES TapeDrive (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
  ADD CONSTRAINT FK_TapeRequest_dgn
    FOREIGN KEY (deviceGroupName)
    REFERENCES DeviceGroupName (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE
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
  ADD CONSTRAINT FK_TapeServer_actingMode
    FOREIGN KEY (actingMode)
    REFERENCES TapeServerStatusCodes (id)
    DEFERRABLE
    INITIALLY IMMEDIATE
    ENABLE;
CREATE INDEX I_FK_TapeServer_actingMode ON TapeServer (actingMode);

ALTER TABLE Tape2DriveDedication
  ADD CONSTRAINT FK_Tp2DrvDedic_tapeDrive
    FOREIGN KEY (tapeDrive)
    REFERENCES TapeDrive (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE;
CREATE INDEX I_FK_Tp2DrvDedic_tapeDrive ON Tape2DriveDedication (tapeDrive);


/**
 * This package puts all of the exception declarations of the castorVdqm...
 * packages in one place.
 */
CREATE OR REPLACE PACKAGE castorVdqmException AS

  not_implemented EXCEPTION;
  not_implemented_cd CONSTANT PLS_INTEGER := -20001;
  PRAGMA EXCEPTION_INIT(not_implemented, -20001);

  null_is_invalid EXCEPTION;
  null_is_invalid_cd CONSTANT PLS_INTEGER := -20002;
  PRAGMA EXCEPTION_INIT(null_is_invalid, -20002);

  invalid_drive_dedicate EXCEPTION;
  invalid_drive_dedicate_cd CONSTANT PLS_INTEGER := -20003;
  PRAGMA EXCEPTION_INIT(invalid_drive_dedicate, -20003);

  drive_not_found EXCEPTION;
  drive_not_found_cd CONSTANT PLS_INTEGER := -20004;
  PRAGMA EXCEPTION_INIT(drive_not_found, -20004);

  drive_server_not_found EXCEPTION;
  drive_server_not_found_cd CONSTANT PLS_INTEGER := -20005;
  PRAGMA EXCEPTION_INIT(drive_server_not_found, -20005);

  drive_dgn_not_found EXCEPTION;
  drive_dgn_not_found_cd CONSTANT PLS_INTEGER := -20006;
  PRAGMA EXCEPTION_INIT(drive_dgn_not_found, -20006);

  invalid_regexp_host EXCEPTION;
  invalid_regexp_host_cd CONSTANT PLS_INTEGER := -20007;
  PRAGMA EXCEPTION_INIT(invalid_regexp_host, -20007);

  invalid_regexp_vid EXCEPTION;
  invalid_regexp_vid_cd CONSTANT PLS_INTEGER := -20008;
  PRAGMA EXCEPTION_INIT(invalid_regexp_vid, -20008);

  PROCEDURE throw(exceptionNumberVar IN PLS_INTEGER,
    messageVar IN VARCHAR2 DEFAULT NULL);

END castorVdqmException;
/


/**
 * See the castorVdqmException package specification for dcoumentation.
 */
CREATE OR REPLACE PACKAGE BODY castorVdqmException AS

  TYPE Varchar256List IS TABLE OF VARCHAR2(256) INDEX BY BINARY_INTEGER;
  errorMessagesVar Varchar256List;

  PROCEDURE throw(exceptionNumberVar IN PLS_INTEGER,
    messageVar IN VARCHAR2 DEFAULT NULL) AS
  BEGIN
    IF exceptionNumberVar IS NULL THEN
      RAISE_APPLICATION_ERROR(null_is_invalid_cd,
        'castorVdqmException.throw called with NULL exceptionNumberVar');
    END IF;

    IF messageVar IS NULL AND errorMessagesVar.EXISTS(exceptionNumberVar) THEN
      RAISE_APPLICATION_ERROR(exceptionNumberVar,
        errorMessagesVar(exceptionNumberVar));
    ELSE
      RAISE_APPLICATION_ERROR(exceptionNumberVar, messageVar);
    END IF;
  END throw;

-- Package initialization section
BEGIN

  errorMessagesVar(not_implemented_cd) :=
    'Feature not implemented.';
  errorMessagesVar(null_is_invalid_cd) :=
    'A NULL value is invalid.';
  errorMessagesVar(invalid_drive_dedicate_cd) :=
    'Invalid drive dedicate string.';
  errorMessagesVar(drive_not_found_cd) :=
    'Drive not found.';
  errorMessagesVar(drive_server_not_found_cd) :=
    'Drive and server combination not found.';
  errorMessagesVar(drive_dgn_not_found_cd) :=
    'Drive and DGN combination not found.';
  errorMessagesVar(invalid_regexp_host_cd) :=
    'Client host value is not a valid regular expression.';
  errorMessagesVar(invalid_regexp_vid_cd) :=
    'VID value is not a valid regular expression.';

END castorVdqmException;
/


/**
 * This package contains common code used by the other VDQM packages.
 */
CREATE OR REPLACE PACKAGE castorVdqmCommon AS

  /**
   * Datatype for a list of VARCHAR2(256).
   */
  TYPE Varchar256List IS TABLE OF VARCHAR2(256) INDEX BY BINARY_INTEGER;

  /**
   * Tokenizes the specified string buffer using the specified delimiter.
   *
   * @param bufferVar The buffer string to be tokenized.
   * @param delimiterVar The delimiter used to separate the tpkens.
   * @return the tokens.
   */
  FUNCTION tokenize(bufferVar IN VARCHAR2, delimiterVar IN VARCHAR2)
    RETURN Varchar256List;

  /**
   * This function returns the number of seconds since the EPOCH UTC + 1.
   *
   * @return number of seconds since the Epoch (00:00:00 UTC, January 1, 1970).
   */
  FUNCTION getTime RETURN NUMBER;

END castorVdqmCommon;
/


/**
 * See the castorVdqmCommon package specification for documentation.
 */
CREATE OR REPLACE PACKAGE BODY castorVdqmCommon AS

  /**
   * See the castorVdqm package specification for documentation.
   */
  FUNCTION tokenize(bufferVar IN VARCHAR2, delimiterVar IN VARCHAR2)
    RETURN Varchar256List AS
    currentPosVAR   NUMBER := 1;
    nextPosVar      NUMBER := 1;
    delimiterPosVar NUMBER := 1;
    lengthVar       NUMBER := 0;
    tokensVar       Varchar256List;
  BEGIN
    -- While the end of the buffer has not been reached
    WHILE currentPosVar <= LENGTH(bufferVar) LOOP
      -- Try to find the next delimiter
      delimiterPosVar := INSTR(bufferVar, delimiterVar, currentPosVar);

      -- If a delimiter was found
      IF delimiterPosVar > 0 THEN
        lengthVar  := delimiterPosVar - currentPosVar;
        nextPosVar := delimiterPosVar + 1;
      -- Else no delimiter was found
      ELSE
        lengthVar  := LENGTH(bufferVar) - currentPosVar + 1;
        nextPosVar := LENGTH(bufferVar) + 1;
      END IF;

      -- Add token to list
      tokensVar(tokensVar.count + 1) :=
        SUBSTR(bufferVar, currentPosVar, lengthVar);

      -- Move past the token in the buffer
      currentPosVar := nextPosVar;
    END LOOP;

    RETURN tokensVar;
  END tokenize;


  /**
   * See the castorVdqm package specification for documentation.
   */
  FUNCTION getTime RETURN NUMBER AS
    epoch            TIMESTAMP WITH TIME ZONE;
    now              TIMESTAMP WITH TIME ZONE;
    interval         INTERVAL DAY(9) TO SECOND;
    interval_days    NUMBER;
    interval_hours   NUMBER;
    interval_minutes NUMBER;
    interval_seconds NUMBER;
  BEGIN
    epoch := TO_TIMESTAMP_TZ('01-JAN-1970 00:00:00 00:00',
      'DD-MON-YYYY HH24:MI:SS TZH:TZM');
    now := SYSTIMESTAMP AT TIME ZONE '00:00';
    interval         := now - epoch;
    interval_days    := EXTRACT(DAY    FROM (interval));
    interval_hours   := EXTRACT(HOUR   FROM (interval));
    interval_minutes := EXTRACT(MINUTE FROM (interval));
    interval_seconds := EXTRACT(SECOND FROM (interval));

    RETURN interval_days * 24 * 60 * 60 + interval_hours * 60 * 60 +
      interval_minutes * 60 + interval_seconds;
  END getTime;

END castorVdqmCommon;
/


/**
 * This package contains functions used by some of the VDQM database views.
 */
CREATE OR REPLACE PACKAGE castorVdqmView AS

  /**
   * This function returns the dedications of the specified drive in the format
   * required by the showqueues command-line tool.
   *
   * @param driveIdVar the ID of the drive.
   * @return the dedications of the specified drive in the format required by
   * the showqueues command-line tool.
   */
  FUNCTION getVdqmDedicate(driveIdVar IN NUMBER) RETURN VARCHAR2;

END castorVdqmView;
/


/**
 * See the castorVdqmView package specification for documentation.
 */
CREATE OR REPLACE PACKAGE BODY castorVdqmView AS

  /**
   * See the castorVdqmView package specification for documentation.
   */
  FUNCTION getVdqmDedicate(driveIdVar IN NUMBER)
    RETURN VARCHAR2
  IS
    buf VARCHAR2(1024);
  BEGIN
    FOR accessModeDedication IN (
      SELECT accessMode
        FROM TapeDriveDedication
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
        FROM TapeDriveDedication
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

    FOR egidDedication IN (
      SELECT egid
        FROM TapeDriveDedication
        WHERE tapeDrive = driveIdVar AND egid IS NOT NULL
        ORDER BY egid)
    LOOP
      -- Add a comma if there is already a dedication in the buffer
      IF LENGTH(buf) > 0 THEN
        buf := buf || ',';
      END IF;

      -- Add dedication to buffer
      buf := buf || 'gid=' || egidDedication.egid;
    END LOOP;

    FOR euidDedication IN (
      SELECT euid
        FROM TapeDriveDedication
        WHERE tapeDrive = driveIdVar AND euid IS NOT NULL
        ORDER BY euid)
    LOOP
      -- Add a comma if there is already a dedication in the buffer
      IF LENGTH(buf) > 0 THEN
        buf := buf || ',';
      END IF;

      -- Add dedication to buffer
      buf := buf || 'uid=' || euidDedication.euid;
    END LOOP;

    FOR vidDedication IN (
      SELECT vid
        FROM TapeDriveDedication
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

    FOR vid2DrvDedication IN (
      SELECT vid
        FROM Tape2DriveDedication
        WHERE tapeDrive = driveIdVar
        ORDER BY vid)
    LOOP
      -- Add a comma if there is already a dedication in the buffer
      IF LENGTH(buf) > 0 THEN
        buf := buf || ',';
      END IF;

      -- Add dedication to buffer
      buf := buf || 'vid2drv=' || vid2DrvDedication.vid;
    END LOOP;

    RETURN buf;
  END getVdqmDedicate;

END castorVdqmView;
/


/**
 * This view shows the effective volume priorities.  This view is required
 * because there can be two priorities for a given VID and tape access mode,
 * a single-shot lifespan priority from the RecallHandler and an unlimited
 * lifespan priority from a tape operator.  The unlimited priority from the
 * tape operator would be the effective priority in such a case, in other
 * words, operator priorities overrule RecallHandler priorities.
 */
CREATE OR REPLACE VIEW EffectiveVolumePriority_VIEW
  AS WITH EffectiveLifespanType AS (
    SELECT
      VolumePriority.vid,
      VolumePriority.tpMode,
      MAX(VolumePriority.lifespanType) AS lifespanType
    FROM VolumePriority
    GROUP BY VolumePriority.vid, tpMode)
SELECT
  VolumePriority.priority,
  VolumePriority.clientUID,
  VolumePriority.clientGID,
  VolumePriority.clientHost,
  VolumePriority.vid,
  VolumePriority.tpMode,
  VolumePriority.lifespanType,
  VolumePriority.creationTime,
  VolumePriority.modificationTime,
  VolumePriority.id
FROM
  VolumePriority
INNER JOIN EffectiveLifespanType ON
      VolumePriority.vid          = EffectiveLifespanType.vid
  AND VolumePriority.tpMode       = EffectiveLifespanType.tpMode
  AND VolumePriority.lifespanType = EffectiveLifespanType.lifespanType;


/**
 * This view shows candidate tape drive allocations before any dedications
 * have been taken into account.
 */
CREATE OR REPLACE VIEW PotentialMounts_VIEW AS SELECT UNIQUE
  TapeDrive.id as driveId,
  TapeDrive.driveName,
  TapeRequest.id as tapeRequestId,
  ClientIdentification.euid as clientEuid,
  ClientIdentification.egid as clientEgid,
  ClientIdentification.machine as clientMachine,
  TapeAccessSpecification.accessMode,
  VdqmTape.vid,
  TapeRequest.modificationTime,
  TapeRequest.creationTime,
  NVL(EffectiveVolumePriority_VIEW.priority,0) AS volumePriority
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
  AND (
    TapeRequest.requestedSrv IS NULL
    OR TapeRequest.requestedSrv = TapeDrive.tapeServer
  )
INNER JOIN TapeServer ON
  TapeDrive.tapeServer = TapeServer.id
LEFT OUTER JOIN EffectiveVolumePriority_VIEW ON
  VdqmTape.vid = EffectiveVolumePriority_VIEW.vid
  AND TapeAccessSpecification.accessMode = EffectiveVolumePriority_VIEW.tpMode
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
      AND TapeRequest2.tapeDrive IS NOT NULL
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
ORDER BY
  TapeAccessSpecification.accessMode DESC,
  VolumePriority DESC,
  TapeRequest.creationTime ASC,
  DBMS_RANDOM.value;


/**
 * This view shows candidate drive allocations that will reuse a current drive
 * allocation before any dedicatioins have been taken into account.
 */
CREATE OR REPLACE VIEW PotentialReusableMounts_VIEW AS SELECT UNIQUE
  TapeRequest.id as tapeRequestId,
  TapeRequest.modificationTime,
  ClientIdentification.euid as clientEuid,
  ClientIdentification.egid as clientEgid,
  ClientIdentification.machine as clientMachine,
  TapeAccessSpecification.accessMode,
  VdqmTape.id as tapeId,
  VdqmTape.vid,
  TapeDrive.id as driveId,
  TapeServer.actingMode
FROM
  TapeRequest
INNER JOIN ClientIdentification ON
  TapeRequest.client = ClientIdentification.id
INNER JOIN TapeAccessSpecification ON
  TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
INNER JOIN VdqmTape ON
  TapeRequest.tape = VdqmTape.id
INNER JOIN TapeDrive ON
  TapeRequest.tape = TapeDrive.tape -- Request is for the tape in the drive
  AND (
    TapeRequest.requestedSrv IS NULL
    OR TapeRequest.requestedSrv = TapeDrive.tapeServer
  )
INNER JOIN TapeServer ON
  TapeDrive.tapeServer = TapeServer.id
WHERE
      TapeServer.actingMode=0 -- ACTIVE
  AND TapeRequest.tapeDrive IS NULL -- Request has not already been allocated
                                    -- a drive
ORDER BY
  TapeAccessSpecification.accessMode DESC,
  TapeRequest.modificationTime ASC;


/**
 * View used for generating the list of drives when replying to the showqueues
 * command
 */
CREATE OR REPLACE VIEW TapeDriveShowqueues_VIEW AS SELECT
  TapeDrive.status,
  TapeDrive.id,
  TapeDrive.runningTapeReq,
  TapeDrive.jobId,
  TapeDrive.modificationTime,
  TapeDrive.resetTime,
  TapeDrive.useCount,
  TapeDrive.errCount,
  TapeDrive.transferredMB,
  TapeAccessSpecification.accessMode AS tapeAccessMode,
  TapeDrive.totalMB,
  TapeServer.serverName,
  VdqmTape.vid,
  TapeDrive.driveName,
  DeviceGroupName.dgName,
  castorVdqmView.getVdqmDedicate(TapeDrive.Id) AS dedicate
FROM
  TapeDrive
LEFT OUTER JOIN TapeServer ON
  TapeDrive.tapeServer = TapeServer.id
LEFT OUTER JOIN VdqmTape ON
  TapeDrive.tape = VDQMTAPE.ID
LEFT OUTER JOIN DEVICEGROUPNAME ON
  TapeDrive.deviceGroupName = DeviceGroupName.id
LEFT OUTER JOIN TapeRequest ON
  TapeDrive.RunningTapeReq = TapeRequest.id
LEFT OUTER JOIN TapeAccessSpecification ON
  TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
ORDER BY
  dgName ASC,
  driveName ASC;


/**
 * View used for generating the list of requests when replying to the
 * showqueues command
 */
CREATE OR REPLACE VIEW TapeRequestShowqueues_VIEW AS SELECT
  TapeRequest.id,
  TapeDrive.driveName,
  TapeDrive.id AS tapeDriveId,
  TapeRequest.priority,
  ClientIdentification.port AS clientPort,
  ClientIdentification.euid AS clientEuid,
  ClientIdentification.egid AS clientEgid,
  TapeAccessSpecification.accessMode,
  TapeRequest.creationTime,
  ClientIdentification.machine AS clientMachine,
  VdqmTape.vid,
  TapeServer.serverName AS tapeServer,
  DeviceGroupName.dgName,
  ClientIdentification.username AS clientUsername,
  NVL(EffectiveVolumePriority_VIEW.priority,0) AS volumePriority,
  TapeRequest.remoteCopyType
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
LEFT OUTER JOIN EffectiveVolumePriority_VIEW ON
      VdqmTape.vid = EffectiveVolumePriority_VIEW.vid
  AND TapeAccessSpecification.accessMode = EffectiveVolumePriority_VIEW.tpMode;


/**
 * This package provides the API to be used by the VDQM server application.
 */
CREATE OR REPLACE PACKAGE castorVdqm AS

  /**
   * This procedure tries to allocate a free tape drive to a pending tape
   * request.
   *
   * @param returnVar has a value of 1 if a free drive was successfully
   * allocated to a pending request, 0 if no possible allocation could be found
   * or -1 if an allocation was found but was invalidated by other threads
   * before the appropriate locks could be taken.
   * @param tapeDriveIdVar if a free drive was successfully allocated then the
   * value of this parameter will be the ID of the allocated tape drive, else
   * the value of this parameter will be undefined.
   * @param tapeDriveIdVar if a free drive was successfully allocated then the
   * value of this parameter will be the name of the allocated tape drive, else
   * the value of this parameter will be undefined.
   * @param tapeRequestIdVar if a free drive was successfully allocated then the
   * value of this parameter will be the ID of the pending request, else the
   * value of this parameter will be undefined.
   * @param tapeRequestVidVar if a free drive was successfully allocated then
   * the value of this parameter will be the VID of the pending request, else
   * the value of this parameter will be undefined.
   */
  PROCEDURE allocateDrive(returnVar OUT NUMBER, tapeDriveIdVar OUT NUMBER,
    tapeDriveNameVar  OUT NOCOPY VARCHAR2, tapeRequestIdVar  OUT NUMBER,
    tapeRequestVidVar OUT NOCOPY VARCHAR2);

  /**
   * This procedure gets a new matched request to be submitted to rtcpd.
   *
   * @tapeReqId the ID of the matched request.
   */
  PROCEDURE getRequestToSubmit(tapeReqId OUT NUMBER);

  /**
   * This procedure tries to reuse a drive allocation.
   *
   * @param tapeIdVar the ID of the tape of the current drive allocation.
   * @param tapeDriveIdVar the ID of the drive of the current drive allocation.
   * @param returnVar has a value of 1 if the specified drive allocation was
   * successfully reused, 0 if no possible reuse was found or -1 if a possible
   * reuse was found but was invalidated by other threads before the appropriate
   * locks could be taken.
   * @param tapeRequestIdVar if the drive allocation was successfully reused
   * then the value of this parameter will be the ID of the newly assigned
   * request, else the value of this parameter will be undefined.
   */
  PROCEDURE reuseDriveAllocation(tapeIdVar IN NUMBER, tapeDriveIdVar IN NUMBER,
    accessModeVar IN NUMBER, returnVar OUT NUMBER, tapeRequestIdVar OUT NUMBER);

  /**
   * This procedure inserts the specified drive dedications into the database.
   *
   * @param driveNameVar the name of the tape drive to be dedicated
   * @param serverNameVar the name of the tape server of the tape drive
   * @param dgNameVar the name of the device group of the tape drive
   * @param dedicateVar the dedication string
   * @exception castorVdqmException.invalid_drive_dedicate if an error is
   * detected in the drive dedicate string.
   * @exception castorVdqmException.drive_not_found_application if the
   * specified tape drive cannot be found in the database.
   * @exception castorVdqmException.drive_server_not_found if the specified
   * tape drive and tape server combination cannot be found in the database.
   * @exception castorVdqmException.drive_dgn_not_found if the specified tape
   * drive and DGN combination cannot be found in the database.
   */
  PROCEDURE dedicateDrive(driveNameVar IN VARCHAR2, serverNameVar IN VARCHAR2,
    dgNameVar IN VARCHAR2, dedicateVar IN VARCHAR2);

  /**
   * This procedure inserts the specified tape dedication into the database.
   * If a tape drive is not specified, i.e. driveNAmeVar, serverNAmeVar and
   * dgNameVar are set to NULL, then all dedications for the specified tape
   * will be removed from the database.
   *
   * @param driveNameVar the name of the tape drive
   * @param serverNameVar the name of the tape server of the tape drive
   * @param dgNameVar the name of the device group of the tape drive
   * @param vidVar the VID of the tape to be dedicated
   * @exception castorVdqmException.invalid_drive_dedicate if an error is
   * detected in the drive dedicate string.
   * @exception castorVdqmException.drive_not_found_application if the
   * specified tape drive cannot be found in the database.
   * @exception castorVdqmException.drive_server_not_found if the specified
   * tape drive and tape server combination cannot be found in the database.
   * @exception castorVdqmException.drive_dgn_not_found if the specified tape
   * drive and DGN combination cannot be found in the database.
   */
  PROCEDURE dedicateTape(vidVar IN VARCHAR2, driveNameVar IN VARCHAR2,
    serverNameVar IN VARCHAR2, dgNameVar IN VARCHAR2);

  /**
   * This procedure deletes the specified drive from the database.
   *
   * @param driveNameVar the name of the tape drive to be delete
   * @param serverNameVar the name of the tape server of the tape drive
   * @param dgNameVar the name of the device group of the tape drive
   * @param resultVar is 0 if the deletion was successful. -1 if the specified
   * tape server does not exist, -2 if the specified device group name does not
   * exist, -3 if the specified tape drive does not exist and -4 if the
   * specified drive has a job assigned.
   */
  PROCEDURE deleteDrive(driveNameVar IN VARCHAR2, serverNameVar IN VARCHAR2,
    dgNameVar IN VARCHAR2, resultVar OUT INTEGER);

  /**
   * This procedure tries to move a the state of the specified request from
   * REQUEST_BEINGSUBMITTED to REQUEST_SUBMITTED.
   *
   * This state transition may not be possible if the corresponding drive and
   * tape request states have been modified by other threads.  For example a
   * thread handling a tape drive request message may have put the drive into
   * the down state.
   *
   * If the state of the request cannot be moved to REQUEST_SUBMITTED, then
   * this procedure will do nothing.
   *
   * All the details of the actions taken by this procedure are reflected in
   * the xxxxBeforeVar and xxxxAfterVar OUT parameters of this procedure.
   *
   * Note that except for database ids, a xxxxBefore or a xxxxAfter
   * parameter will contain the value -1 if the actual value is unknown.
   * In the case of database ids, a value of NULL may mean unknown or
   * NULL.
   *
   * @param driveIdVar the ID of the drive
   * @param requestIdVar the ID of the tape request
   * @param returnVar has a value of 1 if the state of the request was
   * successfully moved to REQUEST_SUBMITTED, else 0
   * @param driveExistsVar -1 = unknown, 0 = no drive, 1 = drive exists
   * @param driveStatusBeforeVar -1 = unknown, else drive status before this
   * procedure
   * @param driveStatusAfterVar -1 = unknown, else drive status after this
   * procedure
   * @param runningRequestIdBeforeVar the id of the drive's running request
   * before this procedure
   * @param runningRequestIdAfterVar the id of the drive's running request
   * after this procedure
   * @param requestExistsVar -1 = unknown, 0 = no request, 1 request exists
   * @param requestStatusBeforeVar -1 = unknown, else the request status before
   * this procedure
   * @param requestStatusAfterVar -1 = unknown, else the request status after
   * this procedure
   * @param requestDriveIdBeforeVar the id of the request's drive before this
   * procedure
   * @param requestDriveIdAfterVar the id of the request's drive after this
   * procedure
   */
  PROCEDURE requestSubmitted(
    driveIdVar                IN  NUMBER,
    requestIdVar              IN  NUMBER,
    returnVar                 OUT NUMBER,
    driveExistsVar            OUT NUMBER,
    driveStatusBeforeVar      OUT NUMBER,
    driveStatusAfterVar       OUT NUMBER,
    runningRequestIdBeforeVar OUT NUMBER,
    runningRequestIdAfterVar  OUT NUMBER,
    requestExistsVar          OUT NUMBER,
    requestStatusBeforeVar    OUT NUMBER,
    requestStatusAfterVar     OUT NUMBER,
    requestDriveIdBeforeVar   OUT NUMBER,
    requestDriveIdAfterVar    OUT NUMBER);

  /**
   * Resets the specified drive and request.
   *
   * The status of the drive is set to REQUEST_PENDING and its running request
   * is set to NULL.
   *
   * The status of the request is set to REQUEST_PENDING and its drive is set
   * to NULL.
   *
   * All the details of the actions taken by this procedure are reflected in
   * the xxxxBeforeVar and xxxxAfterVar OUT parameters of this procedure.
   *
   * Note that except for database ids, a xxxxBefore or a xxxxAfter
   * parameter will contain the value -1 if the actual value is unknown.
   * In the case of database ids, a value of NULL may mean unknown or
   * NULL.
   *
   * @param tapeDriveIdVar the ID of the drive
   * @param tapeRequestIdVar the ID of the tape request
   * @param driveExistsVar -1 = unknown, 0 = no drive, 1 = drive exists
   * @param driveStatusBeforeVar -1 = unknown, else drive status before this
   * procedure
   * @param driveStatusAfterVar -1 = unknown, else drive status after this
   * procedure
   * @param runningRequestIdBeforeVar the id of the drive's running request
   * before this procedure
   * @param runningRequestIdAfterVar the id of the drive's running request
   * after this procedure
   * @param requestExistsVar -1 = unknown, 0 = no request, 1 request exists
   * @param requestStatusBeforeVar -1 = unknown, else the request status before
   * this procedure
   * @param requestStatusAfterVar -1 = unknown, else the request status after
   * this procedure
   * @param requestDriveIdBeforeVar the id of the request's drive before this
   * procedure
   * @param requestDriveIdAfterVar the id of the request's drive after this
   * procedure
   */
  PROCEDURE resetDriveAndRequest(
    driveIdVar                IN  NUMBER,
    requestIdVar              IN  NUMBER,
    driveExistsVar            OUT NUMBER,
    driveStatusBeforeVar      OUT NUMBER,
    driveStatusAfterVar       OUT NUMBER,
    runningRequestIdBeforeVar OUT NUMBER,
    runningRequestIdAfterVar  OUT NUMBER,
    requestExistsVar          OUT NUMBER,
    requestStatusBeforeVar    OUT NUMBER,
    requestStatusAfterVar     OUT NUMBER,
    requestDriveIdBeforeVar   OUT NUMBER,
    requestDriveIdAfterVar    OUT NUMBER);

  /**
   * This procedure sets the priority of a volume.
   *
   * @param priority the priority where 0 is the lowest priority and INT_MAX is
   * the highest.
   * @param clientUIDVar the user id of the client.
   * @param clientGIDVar the group id fo the client.
   * @param clientHostVar the host of the client.
   * @param vidVar the visual identifier of the volume.
   * @param tpModeVar the tape access mode.  Valid values are either 0 meaning
   * write-disabled or 1 meaning write-enabled.
   * @param lifespanTypeVar the type of lifespan to be assigned to the priority
   * setting.  Valid values are either 0 meaning single-shot or 1 meaning
   * unlimited.
   */
  PROCEDURE setVolPriority(priorityVar IN NUMBER, clientUIDVar IN NUMBER,
    clientGIDVar IN NUMBER, clientHostVar IN VARCHAR2, vidVar IN VARCHAR2,
    tpModeVar IN NUMBER, lifespanTypeVar IN NUMBER);

  /**
   * This procedure deletes the specified volume priority if it exist, else
   * does nothing.
   *
   * @param vidVar the visual identifier of the volume.
   * @param tpModeVar the tape access mode.  Valid values are either 0 meaning
   * write-disabled or 1 meaning write-enabled.
   * @param lifespanTypeVar the type of lifespan to be assigned to the priority
   * setting.  Valid values are either 0 meaning single-shot or 1 meaning
   * unlimited.
   * @param returnVar the ID id of the volume priority row if one was deleted,
   * else 0.
   * @param priorityVar the priority of the deleted volume priority if one was
   * deleted, else undefined.
   * @param clientUIDVar the client UID of the deleted volume priority if one
   * was deleted, else undefined.
   * @param clientGIDVar the client GID of the deleted volume priority if one
   * was deleted, else undefined.
   * @param clientHostVar the client host of the deleted volume priority if one
   * was deleted, else undefined.
   */
  PROCEDURE deleteVolPriority(vidVar IN VARCHAR2, tpModeVar IN NUMBER,
    lifespanTypeVar IN NUMBER, returnVar OUT NUMBER, priorityVar OUT NUMBER,
    clientUIDVar OUT NUMBER, clientGIDVar OUT NUMBER,
    clientHostVar OUT NOCOPY VARCHAR2);

  /**
   * This procedure deletes old single-mount volume priorities.
   *
   * @param maxAgeVar the maximum age of a volume priority in seconds.
   * @param prioritiesDeletedVar the number of volume priorities deleted.
   */
  PROCEDURE deleteOldVolPriorities(maxAgeVar IN NUMBER,
    prioritiesDeletedVar OUT NUMBER);

END castorVdqm;
/


/**
 * See the castorVdqm package specification for documentation.
 */
CREATE OR REPLACE PACKAGE BODY castorVdqm AS

  /**
   * Determines whether or not the specified drive and GID pass the dedications
   * of the drive.
   *
   * @param driveIdVar the ID of the drive.
   * @param gidVar     the GID of the client.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesGidDriveDedications(
    driveIdVar IN NUMBER,
    gidVar     IN NUMBER)
    RETURN NUMBER AS
    nbGidDedicationsVar NUMBER;
  BEGIN
    -- Count the number of GID dedications for the drive
    -- (there should only be one)
    SELECT COUNT(*) INTO nbGidDedicationsVar
      FROM TapeDriveDedication
      WHERE tapeDrive = driveIdVar AND egid IS NOT NULL;

    -- Drive passes if there are no GID dedications for it
    IF nbGidDedicationsVar = 0 THEN
      RETURN 1;
    END IF;

    -- Drive has one or more GID dedications

    -- Count the number of matching GID dedications
    SELECT COUNT(*) INTO nbGidDedicationsVar
      FROM TapeDriveDedication
      WHERE
            TapeDriveDedication.tapeDrive = driveIdVar
        AND gidVar = TapeDriveDedication.egid;

    -- As there are GID dedications for the drive, it only passes if at least
    -- one matches
    IF nbGidDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesGidDriveDedications;


  /**
   * Determines whether or not the specified drive and access mode pass the
   * dedications of the drive.
   *
   * @param driveIdVar    the ID of the drive.
   * @param accessModeVar the access mode of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesModeDriveDedication(
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
      WHERE
            TapeDriveDedication.tapeDrive = driveIdVar
        AND TapeDriveDedication.accessMode = accessModeVar;

    -- As there is a mode dedication for the drive, the drive only passes if it
    -- matches
    IF nbModeDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesModeDriveDedication;


  /**
   * Determines whether or not the specified drive and host pass the
   * dedications of the drive.
   *
   * @param driveIdVar    the ID of the drive.
   * @param clientHostVar the client host of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesHostDriveDedications(
    driveIdVar    IN NUMBER,
    clientHostVar IN VARCHAR2)
    RETURN NUMBER AS
    nbHostDedicationsVar NUMBER;
  BEGIN
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
            TapeDriveDedication.tapeDrive = driveIdVar
        AND REGEXP_LIKE(clientHostVar, TapeDriveDedication.clientHost);

    -- As there are host dedications for the drive, it only passes if at least
    -- one matches
    IF nbHostDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesHostDriveDedications;


  /**
   * Determines whether or not the specified drive and UID pass the dedications
   * of the drive.
   *
   * @param driveIdVar the ID of the drive.
   * @param uidVar     the UID of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesUidDriveDedications(
    driveIdVar IN NUMBER,
    uidVar     IN NUMBER)
    RETURN NUMBER AS
    nbUidDedicationsVar NUMBER;
  BEGIN
    -- Count the number of UID dedications for the drive
    -- (there should only be one)
    SELECT COUNT(*) INTO nbUidDedicationsVar
      FROM TapeDriveDedication
      WHERE tapeDrive = driveIdVar AND euid IS NOT NULL;

    -- Drive passes if there are no UID dedications for it
    IF nbUidDedicationsVar = 0 THEN
      RETURN 1;
    END IF;

    -- Drive has one or more UID dedications

    -- Count the number of matching UID dedications
    SELECT COUNT(*) INTO nbUidDedicationsVar
      FROM TapeDriveDedication
      WHERE
            TapeDriveDedication.tapeDrive = driveIdVar
        AND uidVar = TapeDriveDedication.euid;

    -- As there are UID dedications for the drive, it only passes if at least
    -- one matches
    IF nbUidDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesUidDriveDedications;


  /**
   * Determines whether or not the specified drive and VID pass the dedications
   * of the drive.
   *
   * @param driveIdVar the ID of the drive.
   * @param vidVar     the vid of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesVidDriveDedications(
    driveIdVar IN NUMBER,
    vidVar     IN VARCHAR2)
    RETURN NUMBER AS
    nbVidDedicationsVar NUMBER;
  BEGIN
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
      WHERE
            TapeDriveDedication.tapeDrive = driveIdVar
        AND REGEXP_LIKE(vidVar, TapeDriveDedication.vid);

    -- As there are vid dedications for the drive, it only passes if at least
    -- one matches
    IF nbVidDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesVidDriveDedications;


  /**
   * Determines whether or not the specified drive and VID pass the
   * tape-to-drive dedications.
   *
   * @param driveIdVar the ID of the drive.
   * @param vidVar     the vid of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesTape2DriveDedications(
    driveIdVar IN NUMBER,
    vidVar     IN VARCHAR2)
    RETURN NUMBER AS
    nbDedicationsVar NUMBER;
  BEGIN
    -- Count the number of dedications for the tape
    SELECT COUNT(*) INTO nbDedicationsVar
      FROM Tape2DriveDedication
      WHERE Tape2DriveDedication.vid = vidVar;

    -- Dedications passed if there are no dedications for the tape
    IF nbDedicationsVar = 0 THEN
      RETURN 1;
    END IF;

    -- Tape has one or more dedications

    -- Count the number of matching dedications
    SELECT COUNT(*) INTO nbDedicationsVar
      FROM Tape2DriveDedication
      WHERE
        Tape2DriveDedication.vid = vidVar
        AND Tape2DriveDedication.tapeDrive = driveIdVar;

    -- As there are tape dedications, they are only passed if at least one
    -- matches
    IF nbDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesTape2DriveDedications;


  /**
   * This function determines if the specified drive and usage pass all drive
   * and tape dedications.
   *
   * @param driveIdVar the ID of the drive.
   * @param gidVar     the gid of the client.
   * @param hostVar    the host of the client.
   * @param modeVar    the tape access mode.
   * @param uidVar     the uid of the client.
   * @param vidVar     the vid of the volume request.
   * @return 1 if all dedications are passed, else 0.
   */
  FUNCTION passesDedications(
    driveIdVar IN NUMBER,
    gidVar     IN NUMBER,
    hostVar    IN VARCHAR2,
    modeVar    IN NUMBER,
    uidVar     IN NUMBER,
    vidVar     IN VARCHAR2)
    RETURN NUMBER AS
    nbVidDedicationsVar NUMBER;
  BEGIN
    IF passesGidDriveDedications(driveIdVar, gidVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesHostDriveDedications(driveIdVar, hostVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesModeDriveDedication(driveIdVar, modeVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesUidDriveDedications(driveIdVar, uidVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesVidDriveDedications(driveIdVar, vidVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesTape2DriveDedications(driveIdVar, vidVar) = 0 THEN
      RETURN 0;
    END IF;

    -- Drive has passed all of its dedications
    RETURN 1;
  END passesDedications;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE allocateDrive(
    returnVar         OUT NUMBER,
    tapeDriveIdVar    OUT NUMBER,
    tapeDriveNameVar  OUT NOCOPY VARCHAR2,
    tapeRequestIdVar  OUT NUMBER,
    tapeRequestVidVar OUT NOCOPY VARCHAR2
    ) AS

    schedulerLockIdVar   INTEGER;
    tapeDriveStatusVar   NUMBER;
    tapeRequestStatusVar NUMBER;

  BEGIN
    returnVar         := 0; -- No possible allocation could be found
    tapeDriveIdVar    := 0;
    tapeDriveNameVar  := '';
    tapeRequestIdVar  := 0;
    tapeRequestVidVar := '';

    -- Take the scheduler lock which is responsible for serializing the
    -- scheduler algorithm
    SELECT id INTO schedulerLockIdVar
      FROM VdqmLock
      WHERE id = 1;

    -- For each potential drive allocation, i.e. a drive allocation for which
    -- no dedications have been taken into account
    FOR potentialMount IN (
      SELECT driveId, driveName, tapeRequestId, clientEuid, clientEgid,
        clientMachine, accessMode, vid
      FROM PotentialMounts_VIEW
    ) LOOP
      -- The status of the drives and requests maybe modified by other scheduler
      -- threads.  The status of the drives may be modified by threads handling
      -- drive request messages.  The status of the requests may be modified by
      -- threads handling tape request messages.  Therefore get a lock on the
      -- corresponding drive and request rows and retrieve their statuses to see
      -- if the drive allocation is still valid
      BEGIN
        SELECT TapeDrive.status INTO TapeDriveStatusVar
        FROM TapeDrive
        WHERE TapeDrive.id = potentialMount.driveId
        FOR UPDATE;
        SELECT TapeRequest.status INTO TapeRequestStatusVar
        FROM TapeRequest
        WHERE TapeRequest.id = potentialMount.tapeRequestId
        FOR UPDATE;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          -- A possible drive allocation was found but was invalidated by other
          -- threads before the appropriate locks could be taken
          returnVar := -1;
          RETURN;
      END;

      -- If the drive allocation is no longer valid, i.e. drive status is not
      -- UNIT_UP or request status is not REQUEST_PENDING
      IF(TapeDriveStatusVar != 0) OR (TapeRequestStatusVar != 0) THEN
        -- A possible drive allocation was found but was invalidated by other
        -- threads before the appropriate locks could be taken
        returnVar := -1;
        RETURN;
      END IF;

      IF passesDedications(
        potentialMount.driveId,
        potentialMount.clientEgid,
        potentialMount.clientMachine,
        potentialMount.accessMode,
        potentialMount.clientEuid,
        potentialMount.vid) = 1 THEN

        -- Set the drive id for logging
        tapeDriveIdVar := potentialMount.driveId;

        -- Set the drive name for logging
        TapeDriveNameVar := potentialMount.driveName;

        -- Set the request id for logging
        tapeRequestIdVar := potentialMount.tapeRequestId;

        -- Set the VID of the pending request for logging
        tapeRequestVidVar := potentialMount.vid;

        -- Allocate the free drive to the pending request
        UPDATE TapeDrive SET
          status           = 1, -- UNIT_STARTING
          jobId            = 0,
          modificationTime = castorVdqmCommon.getTime(),
          runningTapeReq   = potentialMount.tapeRequestId
        WHERE
          id = potentialMount.driveId;
        UPDATE TapeRequest SET
          status           = 1, -- MATCHED
          tapeDrive        = potentialMount.driveId,
          modificationTime = castorVdqmCommon.getTime()
        WHERE
          id = potentialMount.tapeRequestId;

        returnVar := 1; -- A free drive has been allocated to a pending request
        RETURN;

      END IF; -- passesDedications

    END LOOP; -- For each potential drive allocation

  END allocateDrive;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE getRequestToSubmit(tapeReqId OUT NUMBER) AS
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
    -- We have observed ORA-00054 errors (resource busy and acquire with
    -- NOWAIT) even with the SKIP LOCKED clause. This is a workaround to ignore
    -- the error until we understand what to do, another thread will pick up
    -- the request so we don't do anything.
    NULL;
  END getRequestToSubmit;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE reuseDriveAllocation(
    tapeIdVar          IN NUMBER,
    tapeDriveIdVar     IN NUMBER,
    accessModeVar      IN NUMBER,
    returnVar         OUT NUMBER,
    tapeRequestIdVar  OUT NUMBER)
  AS
    tapeDriveStatusCheckVar   NUMBER;
    mountedTapeIdCheckVar     NUMBER;
    tapeRequestStatusCheckVar NUMBER;
    tapeAccessSpecIdCheckVar  NUMBER;
    accessModeCheckVar        NUMBER;
  BEGIN
    returnVar        := 0; -- No possible reuse was found
    tapeRequestIdVar := 0;

    -- For each potential reuse of the current mount
    FOR potentialReuse IN (
      SELECT tapeRequestId, clientEuid, clientEgid, clientMachine, accessMode,
        tapeId, vid, driveId
        FROM PotentialReusableMounts_VIEW
        WHERE
              PotentialReusableMounts_VIEW.driveId    = tapeDriveIdVar
          AND PotentialReusableMounts_VIEW.tapeId     = tapeIdVar
          AND PotentialReusableMounts_VIEW.accessMode = accessModeVar
    ) LOOP

      -- The status of the drives including which tapes may be mounted in them,
      -- may be modified by other threads handling drive request messages.  The
      -- status of the requests may be modified by threads handling tape request
      -- messages.  Therefore get a lock on the corresponding drive and request
      -- rows and retrieve their statuses and mounted tape in the case of the
      -- drive, to see if the reuse of the mount is still valid.
      BEGIN
        SELECT TapeDrive.status, TapeDrive.tape
        INTO tapeDriveStatusCheckVar, mountedTapeIdCheckVar
        FROM TapeDrive
        WHERE TapeDrive.id = potentialReuse.driveId
        FOR UPDATE;
        SELECT TapeRequest.status, TapeRequest.tapeAccessSpecification
        INTO tapeRequestStatusCheckVar, tapeAccessSpecIdCheckVar
        FROM TapeRequest
        WHERE TapeRequest.id = potentialReuse.tapeRequestId
        FOR UPDATE;
        SELECT TapeAccessSpecification.accessMode
        INTO accessModeCheckVar
        FROM TapeAccessSpecification
        WHERE TapeAccessSpecification.id = tapeAccessSpecIdCheckVar
        FOR UPDATE;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          -- A possible reuse of the mount was found but was invalidated by
          -- other threads before the appropriate locks could be taken
          returnVar        := -1;
          RETURN;
      END;

      -- If the reuse of the mount is no longer valid, i.e. the drive's status
      -- is not VOL_MOUNTED or the correct tape is not mounted or the tape
      -- request's status is not REQUEST_PENDING or the access mode no longer
      -- matches
      IF
        (tapeDriveStatusCheckVar   !=             3) OR
        (mountedTapeIdCheckVar     !=     tapeIdVar) OR
        (tapeRequestStatusCheckVar !=             0) OR
        (accessModeCheckVar        != accessModeVar) THEN
          -- A possible reuse of the mount was found but was invalidated by
          -- other threads before the appropriate locks could be taken
          returnVar        := -1;
          RETURN;
      END IF;

      IF passesDedications(
        potentialReuse.driveId,
        potentialReuse.clientEgid,
        potentialReuse.clientMachine,
        potentialReuse.accessMode,
        potentialReuse.clientEuid,
        potentialReuse.vid) = 1 THEN

        -- Reuse the mount with the pending request
        UPDATE TapeRequest SET
          status           = 1, -- MATCHED
          tapeDrive        = tapeDriveIdVar,
          modificationTime = castorVdqmCommon.getTime()
        WHERE id = potentialReuse.tapeRequestId;
        UPDATE TapeDrive SET
          status           = 1, -- UNIT_STARTING
          jobId            = 0,
          runningTapeReq   = potentialReuse.tapeRequestId,
          modificationTime = castorVdqmCommon.getTime()
        WHERE id = tapeDriveIdVar;

        -- The drive allocation was reused
        tapeRequestIdVar := potentialReuse.tapeRequestId;
        returnVar        := 1;
        RETURN;
      END IF;

    END LOOP; -- For each potential reuse of the current mount

  END reuseDriveAllocation;


  /**
   * This procedure parses the specified dedicate string for gid, host, mode,
   * uid and VID dedications.  This procedure raises an application error with
   * an error number of -20001 if it detects an error in the dedicate string.
   *
   * @param dedicateStrVar the dedicate string to be parsed.
   * @param gidVar the extracted gid if there was one, else NULL.
   * @param hostVar the extracted host if there was one, else NULL.
   * @param modeVar the extracted mode if there was one, else NULL.
   * @param uidVar the extracted uid if there was one, else NULL.
   * @param vidVar the extracted VID if there was one, else NULL
   * @exception castorVdqmException.invalid_drive_dedicate if an error is
   * detected in the drive dedicate string.
   */
  PROCEDURE parseDedicateStr(
    dedicateStrVar  IN VARCHAR2,
    gidVar         OUT NUMBER,
    hostVar        OUT NOCOPY VARCHAR2,
    modeVar        OUT NUMBER,
    uidVar         OUT NUMBER,
    vidVar         OUT NOCOPY VARCHAR2
    ) AS
    dedicationStrsVar castorVdqmCommon.Varchar256List;
    nameValueListVar  castorVdqmCommon.Varchar256List;
    TYPE NameValue is RECORD (name VARCHAR2(256), val VARCHAR2(256));
    nameValueVar      NameValue;
    TYPE NameValueList IS TABLE OF NameValue INDEX BY BINARY_INTEGER;
    dedicationsVar    NameValueList;
    indexVar          NUMBER := 1;
    dummyNumVar       NUMBER := NULL;
    numAgeVar         NUMBER := 0; -- Number of     age dedications found so far
    numDateStrVar     NUMBER := 0; -- Number of dateStr dedications found so far
    numGidVar         NUMBER := 0; -- Number of     GID dedications found so far
    numHostVar        NUMBER := 0; -- Number of    host dedications found so far
    numModeVar        NUMBER := 0; -- Number of    mode dedications found so far
    numNameVar        NUMBER := 0; -- Number of    name dedications found so far
    numTimeStrVar     NUMBER := 0; -- Number of timeStr dedications found so far
    numUidVar         NUMBER := 0; -- Number of     UID dedications found so far
    numVidVar         NUMBER := 0; -- Number of     VID dedications found so far
  BEGIN
    gidVar  := NULL;
    hostVar := NULL;
    modeVar := NULL;
    uidVar  := NULL;
    vidVar  := NULL;

    -- Return if the dedicate string is NULL or empty
    IF dedicateStrVar IS NULL OR dedicateStrVar = '' THEN
      RETURN;
    END IF;

    -- Split the dedicate string into the individual dedications
    dedicationStrsVar := castorVdqmCommon.tokenize(dedicateStrVar, ',');

    -- Split each individual dedication into a name value pair
    FOR indexVar IN dedicationStrsVar.FIRST .. dedicationStrsVar.LAST LOOP
      nameValueListVar :=
        castorVdqmCommon.tokenize(dedicationStrsVar(indexVar), '=');

      -- Raise an application error if the dedication is not a name value pair
      IF nameValueListVar.count != 2 THEN
        castorVdqmException.throw(castorVdqmException.invalid_drive_dedicate_cd,
          'Invalid drive dedicate string.  ' ||
          'Dedication is not a valid name value pair ''' ||
          dedicationStrsVar(indexVar) || '''');
      END IF;

      nameValueVar.name := LOWER(LTRIM(RTRIM(nameValueListVar(1))));
      nameValueVar.val  := LTRIM(RTRIM(nameValueListVar(2)));

      dedicationsVar(dedicationsVar.count + 1) := nameValueVar;
    END LOOP;

    -- Extract the specific types of dedication (gid, host, etc.), raising an
    -- application error if there is an invalid dedication
    FOR indexVar IN dedicationsVar.FIRST .. dedicationsVar.LAST LOOP
      CASE
        WHEN dedicationsVar(indexVar).name = 'age' THEN
          IF numAgeVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one age dedication. ''');
          END IF;
          numAgeVar := numAgeVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || ' ''age'' dedications are not supported');
          END IF;
        WHEN dedicationsVar(indexVar).name = 'datestr' THEN
          IF numDateStrVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one datestr dedication. ''');
          END IF;
          numDateStrVar := numDateStrVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || ' ''datestr'' dedications are not supported');
          END IF;
        WHEN dedicationsVar(indexVar).name = 'gid' THEN
          IF numGidVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one gid dedication. ''');
          END IF;
          numGidVar := numGidVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            BEGIN
              gidVar := TO_NUMBER(dedicationsVar(indexVar).val);
            EXCEPTION
              WHEN VALUE_ERROR THEN
                castorVdqmException.throw(
                  castorVdqmException.invalid_drive_dedicate_cd,
                  'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
                  || '  Invalid gid dedication ''' ||
                  dedicationsVar(indexVar).val || ''' ' || SQLERRM);
            END;
          END IF;
        WHEN dedicationsVar(indexVar).name = 'host' THEN
          IF numhostVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one host dedication.');
          END IF;
          numHostVar := numHostVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            BEGIN
              SELECT COUNT(*) INTO dummyNumVar
                FROM DUAL WHERE REGEXP_LIKE(dummy,dedicationsVar(indexVar).val);
              hostVar := dedicationsVar(indexVar).val;
            EXCEPTION
              WHEN OTHERS THEN
                castorVdqmException.throw(
                  castorVdqmException.invalid_drive_dedicate_cd,
                  'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
                  || '  Invalid host dedication ''' ||
                  dedicationsVar(indexVar).val || ''' ' || SQLERRM);
            END;
          END IF;
        WHEN dedicationsVar(indexVar).name = 'mode' THEN
          IF numModeVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one mode dedication.');
          END IF;
          numModeVar := numModeVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            BEGIN
              modeVar := TO_NUMBER(dedicationsVar(indexVar).val);
            EXCEPTION
              WHEN VALUE_ERROR THEN
                castorVdqmException.throw(
                  castorVdqmException.invalid_drive_dedicate_cd,
                  'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
                  || '  Invalid mode dedication ''' ||
                  dedicationsVar(indexVar).val || ''' ' || SQLERRM);
            END;
          END IF;
        WHEN dedicationsVar(indexVar).name = 'name' THEN
          IF numNameVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one name dedication.');
          END IF;
          numNameVar := numNameVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || ' ''name'' dedications are not supported');
          END IF;
        WHEN dedicationsVar(indexVar).name = 'timestr' THEN
          IF numTimeStrVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one timestr dedication.');
          END IF;
          numTimeStrVar := numTimeStrVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || ' ''timestr'' dedications are not supported');
          END IF;
        WHEN dedicationsVar(indexVar).name = 'uid' THEN
          IF numUidVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one uid dedication.');
          END IF;
          numUidVar := numUidVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            BEGIN
              uidVar := TO_NUMBER(dedicationsVar(indexVar).val);
            EXCEPTION
              WHEN VALUE_ERROR THEN
                castorVdqmException.throw(
                  castorVdqmException.invalid_drive_dedicate_cd,
                  'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
                  || '  Invalid uid dedication ''' ||
                  dedicationsVar(indexVar).val || ''' ' || SQLERRM);
            END;
          END IF;
        WHEN dedicationsVar(indexVar).name = 'vid' THEN
          IF numVidVar > 0 THEN
            castorVdqmException.throw(
              castorVdqmException.invalid_drive_dedicate_cd,
              'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
              || '  More than one vid dedication.');
          END IF;
          numVidVar := numVidVar + 1;

          IF dedicationsVar(indexVar).val != '.*' THEN
            BEGIN
              SELECT COUNT(*) INTO dummyNumVar
                FROM DUAL WHERE REGEXP_LIKE(dummy,dedicationsVar(indexVar).val);
              vidVar := dedicationsVar(indexVar).val;
            EXCEPTION
              WHEN OTHERS THEN
                castorVdqmException.throw(
                castorVdqmException.invalid_drive_dedicate_cd,
                'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
                || '  Invalid vid dedication ''' || dedicationsVar(indexVar).val
                || ''' ' || SQLERRM);
            END;
          END IF;
        ELSE
          castorVdqmException.throw(
            castorVdqmException.invalid_drive_dedicate_cd,
            'Invalid drive dedicate string ''' || dedicateStrVar || '''.'
            || '  Uknown dedication type ''' || dedicationsVar(indexVar).name
            || '''');
      END CASE;
    END LOOP;

  END parseDedicateStr;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE dedicateDrive(driveNameVar IN VARCHAR2, serverNameVar IN  VARCHAR2,
    dgNameVar IN  VARCHAR2, dedicateVar IN  VARCHAR2) AS
    gidVar               NUMBER;
    hostVar              VARCHAR2(256);
    modeVar              NUMBER;
    uidVar               NUMBER;
    vidVar               VARCHAR2(256);
    driveIdVar           NUMBER;
    TYPE dedicationList_t IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    dedicationsToDelete  dedicationList_t;
  BEGIN

    -- Get the id of the tape drive
    BEGIN
      SELECT
        TapeDrive.id
        INTO driveIdVar
        FROM TapeDrive
        INNER JOIN TapeServer ON
          TapeDrive.tapeServer = TapeServer.id
        INNER JOIN DeviceGroupName ON
          TapeDrive.deviceGroupName = DeviceGroupName.id
        WHERE
              TapeDrive.driveName    = driveNameVar
          AND TapeServer.serverName  = serverNameVar
          AND DeviceGroupName.dgName = dgNameVar;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      castorVdqmException.throw(castorVdqmException.drive_not_found_cd,
        'Tape drive ''' || dgNameVar || ' ' || driveNameVar || '@' ||
        serverNameVar || ''' not found in database.');
    END;

    -- Lock the tape drive row
    BEGIN
      SELECT
        TapeDrive.id
        INTO driveIdVar
        FROM TapeDrive
        WHERE
          TapeDrive.id = driveIdVar
        FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      castorVdqmException.throw(castorVdqmException.drive_not_found_cd,
        'Tape drive ''' || dgNameVar || ' ' || driveNameVar || '@' ||
        serverNameVar || ''' not found in database.');
    END;

    -- Parse the dedication string raising an application error with an error
    -- number of -20001 if there is an error in the dedicate string
    parseDedicateStr(dedicateVar, gidVar, hostVar, modeVar, uidVar, vidVar);

    -- Delete all existing dedications associated with tape drive
    SELECT id BULK COLLECT INTO dedicationsToDelete
      FROM TapeDriveDedication
      WHERE TapeDriveDedication.tapeDrive = driveIdVar;

    IF dedicationsToDelete.COUNT > 0 THEN
      FOR i IN dedicationsToDelete.FIRST .. dedicationsToDelete.LAST LOOP
        DELETE FROM TapeDriveDedication
          WHERE TapeDriveDedication.id = dedicationsToDelete(i);
      END LOOP;
    END IF;

    -- Insert new dedications
    IF gidVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, egid)
        VALUES(ids_seq.nextval, driveIdVar, gidVar);
    END IF;
    IF hostVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, clientHost)
        VALUES(ids_seq.nextval, driveIdVar, hostVar);
    END IF;
    IF modeVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, accessMode)
        VALUES(ids_seq.nextval, driveIdVar, modeVar);
    END IF;
    IF uidVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, euid)
        VALUES(ids_seq.nextval, driveIdVar, uidVar);
    END IF;
    IF vidVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, vid)
        VALUES(ids_seq.nextval, driveIdVar, vidVar);
    END IF;

  END dedicateDrive;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE dedicateTape(vidVar IN VARCHAR2, driveNameVar IN VARCHAR2,
    serverNameVar IN  VARCHAR2, dgNameVar IN  VARCHAR2) AS
    driveIdVar           NUMBER;
    TYPE dedicationList_t IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    dedicationsToDelete  dedicationList_t;
    dedicationIdVar      NUMBER;
  BEGIN
    -- If no tape drive is specified then remove all dedications for the
    -- specified tape and return
    IF driveNameVar IS NULL AND serverNameVar IS NULL AND dgNameVar IS NULL
      THEN
      -- Delete all existing dedications for the tape
      SELECT id BULK COLLECT INTO dedicationsToDelete
        FROM Tape2DriveDedication
        WHERE Tape2DriveDedication.vid = vidVar;
      IF dedicationsToDelete.COUNT > 0 THEN
        FOR i IN dedicationsToDelete.FIRST .. dedicationsToDelete.LAST LOOP
          DELETE FROM Tape2DriveDedication
            WHERE Tape2DriveDedication.id = dedicationsToDelete(i);
        END LOOP;
      END IF;

      RETURN;
    END IF;

    -- Get the id of the tape drive
    BEGIN
      SELECT
        TapeDrive.id
        INTO driveIdVar
        FROM TapeDrive
        INNER JOIN TapeServer ON
          TapeDrive.tapeServer = TapeServer.id
        INNER JOIN DeviceGroupName ON
          TapeDrive.deviceGroupName = DeviceGroupName.id
        WHERE
              TapeDrive.driveName    = driveNameVar
          AND TapeServer.serverName  = serverNameVar
          AND DeviceGroupName.dgName = dgNameVar;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      castorVdqmException.throw(castorVdqmException.drive_not_found_cd,
        'Tape drive ''' || dgNameVar || ' ' || driveNameVar || '@' ||
        serverNameVar || ''' not found in database.');
    END;

    -- Lock the tape drive row
    BEGIN
      SELECT
        TapeDrive.id
        INTO driveIdVar
        FROM TapeDrive
        WHERE
          TapeDrive.id = driveIdVar
        FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      castorVdqmException.throw(castorVdqmException.drive_not_found_cd,
        'Tape drive ''' || dgNameVar || ' ' || driveNameVar || '@' ||
        serverNameVar || ''' not found in database.');
    END;

    -- Delete all existing dedications for the tape
    SELECT id BULK COLLECT INTO dedicationsToDelete
      FROM Tape2DriveDedication
      WHERE Tape2DriveDedication.vid = vidVar;
    IF dedicationsToDelete.COUNT > 0 THEN
      FOR i IN dedicationsToDelete.FIRST .. dedicationsToDelete.LAST LOOP
        DELETE FROM Tape2DriveDedication
          WHERE Tape2DriveDedication.id = dedicationsToDelete(i);
      END LOOP;
    END IF;

    -- Insert new dedication
    INSERT INTO Tape2DriveDedication(id, vid, tapeDrive)
      VALUES(ids_seq.nextval, vidVar, driveIdVar);

  END dedicateTape;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE deleteDrive
  ( driveNameVar  IN  VARCHAR2
  , serverNameVar IN  VARCHAR2
  , dgNameVar     IN  VARCHAR2
  , resultVar     OUT INTEGER
  ) AS
    dgnIdVar          NUMBER;
    tapeServerIdVar   NUMBER;
    driveIdVar        NUMBER;
    runningTapeReqVar NUMBER;
  BEGIN
    resultVar := 0;

    BEGIN
      SELECT id
        INTO tapeServerIdVar
        FROM TapeServer
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
      SELECT id, runningTapeReq
        INTO driveIdVar, runningTapeReqVar
        FROM TapeDrive
        WHERE
              deviceGroupName = dgnIdVar
          AND tapeServer = tapeServerIdVar
          AND driveName = driveNameVar
        FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      resultVar := -3; -- Tape drive does not exist
      RETURN;
    END;
  
    IF runningTapeReqVar IS NOT NULL THEN
      resultVar := -4; -- Drive has a job assigned
      RETURN;
    END IF;
  
    DELETE FROM TapeDrive2TapeDriveComp WHERE parent    = driveIdVar;
    DELETE FROM TapeDriveDedication     WHERE tapeDrive = driveIdVar;
    DELETE FROM TapeDrive               WHERE id        = driveIdVar;

  END deleteDrive;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE requestSubmitted(
    driveIdVar                IN  NUMBER,
    requestIdVar              IN  NUMBER,
    returnVar                 OUT NUMBER,
    driveExistsVar            OUT NUMBER,
    driveStatusBeforeVar      OUT NUMBER,
    driveStatusAfterVar       OUT NUMBER,
    runningRequestIdBeforeVar OUT NUMBER,
    runningRequestIdAfterVar  OUT NUMBER,
    requestExistsVar          OUT NUMBER,
    requestStatusBeforeVar    OUT NUMBER,
    requestStatusAfterVar     OUT NUMBER,
    requestDriveIdBeforeVar   OUT NUMBER,
    requestDriveIdAfterVar    OUT NUMBER)
  AS
  BEGIN
    returnVar                 :=    0; -- Nothing has been done
    driveExistsVar            :=   -1; -- Unknown
    driveStatusBeforeVar      :=   -1; -- Unknown
    driveStatusAfterVar       :=   -1; -- Unknown
    runningRequestIdBeforeVar := NULL; -- Unknown
    runningRequestIdAfterVar  := NULL; -- Unknown
    requestExistsVar          :=   -1; -- Unknown
    requestStatusBeforeVar    :=   -1; -- Unknown
    requestStatusAfterVar     :=   -1; -- Unknown
    requestDriveIdBeforeVar   := NULL; -- Unknown
    requestDriveIdAfterVar    := NULL; -- Unknown

    -- Try to get a lock on and the status of the corresponding drive row
    BEGIN
      SELECT TapeDrive.status    , TapeDrive.runningTapeReq
        INTO driveStatusBeforeVar, runningRequestIdBeforeVar
        FROM TapeDrive
        WHERE TapeDrive.id = driveIdVar
        FOR UPDATE;

      driveExistsVar           := 1;                         -- Drive exists
      driveStatusAfterVar      := driveStatusBeforeVar;      -- No change yet
      runningRequestIdAfterVar := runningRequestIdBeforeVar; -- No change yet
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        driveExistsVar := 0; -- Drive does not exist
    END;

    -- If the drive no longer exists, then do nothing
    IF driveExistsVar = 0 THEN
      RETURN;
    END IF;

    -- Try to get a lock on and the status of the request row
    BEGIN
      SELECT TapeRequest.status    , TapeRequest.tapeDrive
        INTO requestStatusBeforeVar, requestDriveIdBeforeVar
        FROM TapeRequest
        WHERE TapeRequest.id = requestIdVar
        FOR UPDATE;

      requestExistsVar       := 1;                       -- Request exists
      requestStatusAfterVar  := requestStatusBeforeVar;  -- No change yet
      requestDriveIdAfterVar := requestDriveIdBeforeVar; -- No change yet
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        requestExistsVar := 0; -- Request does not exist
    END;

    -- If the request no longer exists, then do nothing
    IF requestExistsVar = 0 THEN
      RETURN;
    END IF;

    -- If the state of the request cannot be moved to REQUEST_SUBMITTED, then
    -- do nothing
    IF driveStatusBeforeVar      != 1            OR -- 1=UNIT_STARTING
       requestStatusBeforeVar    != 2            OR -- 2=REQUEST_BEINGSUBMITTED
       runningRequestIdBeforeVar != requestIdVar OR
       requestDriveIdBeforeVar   != driveIdVar
      THEN
      RETURN;
    END IF;

    -- Reaching this points means the transition to REQUEST_SUBMITTED is valid

    -- Set the state of the request to REQUEST_SUBMITTED
    UPDATE TapeRequest
      SET TapeRequest.status = 3 -- REQUEST_SUBMITTED = 3
      WHERE TapeRequest.id = requestIdVar;

    -- Set out parameters
    requestStatusAfterVar := 3; -- REQUEST_SUBMITTED = 3
    returnVar             := 1; -- Succeeded

  END requestSubmitted;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE resetDriveAndRequest(
    driveIdVar                IN  NUMBER,
    requestIdVar              IN  NUMBER,
    driveExistsVar            OUT NUMBER,
    driveStatusBeforeVar      OUT NUMBER,
    driveStatusAfterVar       OUT NUMBER,
    runningRequestIdBeforeVar OUT NUMBER,
    runningRequestIdAfterVar  OUT NUMBER,
    requestExistsVar          OUT NUMBER,
    requestStatusBeforeVar    OUT NUMBER,
    requestStatusAfterVar     OUT NUMBER,
    requestDriveIdBeforeVar   OUT NUMBER,
    requestDriveIdAfterVar    OUT NUMBER)
  AS
  BEGIN
    driveExistsVar            :=   -1; -- Unknown
    driveStatusBeforeVar      :=   -1; -- Unknown
    driveStatusAfterVar       :=   -1; -- Unknown
    runningRequestIdBeforeVar := NULL; -- Unknown
    runningRequestIdAfterVar  := NULL; -- Unknown
    requestExistsVar          :=   -1; -- Unknown
    requestStatusBeforeVar    :=   -1; -- Unknown
    requestStatusAfterVar     :=   -1; -- Unknown
    requestDriveIdBeforeVar   := NULL; -- Unknown
    requestDriveIdAfterVar    := NULL; -- Unknown

    -- Try to get a lock on and the status of the corresponding drive row
    BEGIN
      SELECT TapeDrive.status    , TapeDrive.runningTapeReq
        INTO driveStatusBeforeVar, runningRequestIdBeforeVar
        FROM TapeDrive
        WHERE TapeDrive.id = driveIdVar
        FOR UPDATE;

      driveExistsVar           := 1;                         -- Drive exists
      driveStatusAfterVar      := driveStatusBeforeVar;      -- No change yet
      runningRequestIdAfterVar := runningRequestIdBeforeVar; -- No change yet
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        driveExistsVar := 0; -- Drive row does not exist
    END;

    -- Try to get a lock on and the status of the request row
    BEGIN
      SELECT TapeRequest.status    , TapeRequest.tapeDrive
        INTO requestStatusBeforeVar, RequestDriveIdBeforeVar
        FROM TapeRequest
        WHERE TapeRequest.id = requestIdVar
        FOR UPDATE;

      requestExistsVar       := 1;                       -- Request exists
      requestStatusAfterVar  := requestStatusBeforeVar;  -- No change yet
      requestDriveIdAfterVar := requestDriveIdBeforeVar; -- No change yet
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        requestExistsVar := 0; -- Request does not exist
    END;

    -- If the drive exists
    IF driveExistsVar = 1 THEN
      UPDATE TapeDrive
        SET
          TapeDrive.status         = 7,   -- STATUS_UNKNOWN = 7
          TapeDrive.runningTapeReq = NULL -- No running request
        WHERE TapeDrive.id = driveIdVar;

      driveStatusAfterVar      := 7;    -- STATUS_UNKNOWN = 7
      runningRequestIDAfterVar := NULL; -- No running request
    END IF;

    -- If the request exists
    IF requestExistsVar = 1 THEN
      UPDATE TapeRequest
        SET
          TapeRequest.status    = 0,   -- REQUEST_PENDING = 0
          TapeRequest.tapeDrive = NULL -- No drive
        WHERE TapeRequest.id = requestIdVar;

      requestStatusAfterVar  := 0;    -- REQUEST_PENDING = 0
      requestDriveIdAfterVar := NULL; -- No drive
    END IF;
  END resetDriveAndRequest;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE setVolPriority(
    priorityVar     IN NUMBER,
    clientUIDVar    IN NUMBER,
    clientGIDVar    IN NUMBER,
    clientHostVar   IN VARCHAR2,
    vidVar          IN VARCHAR2,
    tpModeVar       IN NUMBER,
    lifespanTypeVar IN NUMBER)
  AS
    priorityIdVar NUMBER;
  BEGIN
    -- Try to select and get a lock on the row representing the priority if one
    -- already exists
    BEGIN
      SELECT id INTO priorityIdVar FROM VolumePriority WHERE
            VolumePriority.vid          = vidVar
        AND VolumePriority.tpMode       = tpModeVar
        AND VolumePriority.lifespanType = lifespanTypeVar
        FOR UPDATE;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        priorityIdVar := NULL;
    END;

    -- If a row for the priority already exists
    IF priorityIdVar IS NOT NULL THEN

      -- Update it if it has an equal or lower priority
      UPDATE VolumePriority SET
        VolumePriority.priority   = priorityVar,
        VolumePriority.clientUID  = clientUIDVar,
        VolumePriority.clientGID  = clientGIDVar,
        VolumePriority.clientHost = clientHostVar
      WHERE
            VolumePriority.id = priorityIdVar
        AND VolumePriority.priority <= priorityVar;

    -- Else a row for the priority does not yet exist
    ELSE

      -- Try to create one
      BEGIN
        INSERT INTO VolumePriority(id, priority, clientUID, clientGID,
          clientHost, vid, tpMode, lifespanType)
        VALUES(ids_seq.nextval, priorityVar, clientUIDVar, clientGIDVar,
          clientHostVar, vidVar, tpModeVar, lifespanTypeVar)
        RETURNING id INTO priorityIdVar;
      EXCEPTION
        WHEN OTHERS THEN
          -- If another competing thread created the row first
          IF SQLCODE = -1 THEN -- ORA-00001: unique constraint violated

            -- Ensure priorityIdVar is NULL
            priorityIdVar := NULL;

          -- Else an unforeseen error has occured
          ELSE

            -- Reraise the current exception
            RAISE;

          END IF;
      END;

      -- If a row for the priority was not successfully created then
      -- a competing thread created the row first
      IF priorityIdVar IS NULL THEN
        -- Select and get a lock on the row
        SELECT id INTO priorityIdVar FROM VolumePriority WHERE
            VolumePriority.vid          = vidVar
        AND VolumePriority.tpMode       = tpModeVar
        AND VolumePriority.lifespanType = lifespanTypeVar
        FOR UPDATE;

        -- Update the row if it has an equal or lower priority
        UPDATE VolumePriority SET
          priority   = priorityVar,
          clientUID  = clientUIDVar,
          clientGID  = clientGIDVar,
          clientHost = clientHostVar
        WHERE
              VolumePriority.id = priorityIdVar
          AND VolumePriority.priority <= priorityVar;
      END IF;

    END IF; -- If a row for the priority already exists

  END setVolPriority;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE deleteVolPriority(
    vidVar           IN VARCHAR2,
    tpModeVar        IN NUMBER,
    lifespanTypeVar  IN NUMBER,
    returnVar       OUT NUMBER,
    priorityVar     OUT NUMBER,
    clientUIDVar    OUT NUMBER,
    clientGIDVar    OUT NUMBER,
    clientHostVar   OUT NOCOPY VARCHAR2)
  AS
    priorityIdVar NUMBER;
  BEGIN
    returnVar := 0;

    -- Try to get the ID of the volume priority row
    BEGIN
      SELECT id, priority, clientUID, clientGID, clientHost
        INTO priorityIdVar, priorityVar, clientUIDVar, clientGIDVar,
          clientHostVar
        FROM VolumePriority WHERE
            VolumePriority.vid          = vidVar
        AND VolumePriority.tpMode       = tpModeVar
        AND VolumePriority.lifespanType = lifespanTypeVar
        FOR UPDATE;
    EXCEPTION
      -- Do nothing if the specified volume priority row does not exist
      WHEN NO_DATA_FOUND THEN RETURN;
    END;

    -- Delete the volume priority row
    DELETE FROM VolumePriority WHERE VolumePriority.id = priorityIdVar;

    -- Give the ID of the volume priority row that was deleted
    returnVar := priorityIdVar;
  END deleteVolPriority;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE deleteOldVolPriorities(
    maxAgeVar             IN NUMBER,
    prioritiesDeletedVar OUT NUMBER)
  AS
    nowVar NUMBER;
  BEGIN
    prioritiesDeletedVar := 0;

    nowVar := castorVdqmCommon.getTime();

    DELETE FROM VolumePriority
    WHERE
          VolumePriority.lifespanType = 0 -- Single-mount
      AND (nowVar - VolumePriority.modificationTime) > maxAgeVar;

    prioritiesDeletedVar := SQL%ROWCOUNT;
  END deleteOldVolPriorities;

END castorVdqm;
/


/**
 * Sets the modification time of a tape drive when it is inserted into the
 * TapeDrive table.
 * Converts an inserted runningtape request id of 0 to NULL.
 * Converts an inserted tape id of 0 to NULL.
 */
CREATE OR REPLACE TRIGGER TR_I_TapeDrive
  BEFORE INSERT ON TapeDrive
FOR EACH ROW
BEGIN
  -- Set modification time
  :NEW.modificationTime := castorVdqmCommon.getTime();

  -- Convert a running tape request id of 0 to NULL
  IF :NEW.runningTapeReq = 0 THEN
    :NEW.runningTapeReq := NULL;
  END IF;

  -- Convert a tape id of 0 to NULL
  IF :NEW.tape = 0 THEN
    :NEW.tape := NULL;
  END IF;
END;
/


/**
 * Updates the modification time of a tape drive.
 * Converts an updated runningtape request id of 0 to NULL.
 * Converts an updated tape id of 0 to NULL.
 */
CREATE OR REPLACE TRIGGER TR_U_TapeDrive
  BEFORE UPDATE ON TapeDrive 
FOR EACH ROW 
BEGIN
  -- Update the modification time
  :NEW.modificationTime := castorVdqmCommon.getTime();

  -- Convert a running tape request id of 0 to NULL
  IF :NEW.runningTapeReq = 0 THEN
    :NEW.runningTapeReq := NULL;
  END IF;

  -- Convert a tape id of 0 to NULL
  IF :NEW.tape = 0 THEN
    :NEW.tape := NULL;
  END IF;
END;
/


/**
 * Sets the creation time and initial modification time of a tape request when
 * it is inserted into the TapeRequest table.
 * Converts an inserted requested server id of 0 to NULL.
 * Converts an inserted tape drive id of 0 to NULL.
 */
CREATE OR REPLACE TRIGGER TR_I_TapeRequest
  BEFORE INSERT ON TapeRequest
FOR EACH ROW
DECLARE
  timeVar NUMBER := castorVdqmCommon.getTime();
BEGIN
  -- Set creation time
  :NEW.creationTime := timeVar;

  -- Set modification time
  :NEW.modificationTime := timeVar;

  -- Convert a requested server id of 0 to NULL
  IF :NEW.requestedSrv = 0 THEN
    :NEW.requestedSrv := NULL;
  END IF;

  -- Convert a tape drive id of 0 to NULL
  IF :NEW.tapeDrive = 0 THEN
    :NEW.tapeDrive := NULL;
  END IF;
END;
/


/**
 * Updates the modification time of a tape request.
 * Converts an updated requested server id of 0 to NULL.
 * Converts an updated tape drive id of 0 to NULL.
 */
CREATE OR REPLACE TRIGGER TR_U_TapeRequest
  BEFORE UPDATE ON TapeRequest
FOR EACH ROW
BEGIN
  -- Update the modification time
  IF (:NEW.modificationTime != :OLD.modificationTime) OR
    (:NEW.status != :OLD.status) THEN
    :NEW.modificationTime := castorVdqmCommon.getTime();
  END IF;

  -- Convert requested server id of 0 to NULL
  IF :NEW.requestedSrv = 0 THEN
    :NEW.requestedSrv := NULL;
  END IF;

  -- Convert a tape drive id of 0 to NULL
  IF :NEW.tapeDrive = 0 THEN
    :NEW.tapeDrive := NULL;
  END IF;
END;
/


/**
 * PL/SQL trigger responsible for setting the creation time and initial
 * modification time of a volume priority when it is inserted into the
 * VolumePriority table.
 */
CREATE OR REPLACE TRIGGER TR_I_VolumePriority
  BEFORE INSERT ON VolumePriority
FOR EACH ROW
DECLARE
  timeVar NUMBER := castorVdqmCommon.getTime();
BEGIN
  -- Set creation time
  :NEW.creationTime := timeVar;

  -- Set modification time
  :NEW.modificationTime := timeVar;
END;
/


/**
 * Updates the modification time of a volume priority when the priority is
 * updated.
 */
CREATE OR REPLACE TRIGGER TR_U_VolumePriority
  BEFORE UPDATE OF priority, clientUID, clientGID, clientHost, modificationTime
    ON VolumePriority
FOR EACH ROW
WHEN
  (   (NEW.priority   != OLD.priority)
   OR (NEW.clientUID  != OLD.clientGID)
   OR (NEW.clientGID  != OLD.clientGID)
   OR (NEW.clientHost != OLD.clientHost)
   OR (NEW.modificationTime != OLD.modificationTime))
BEGIN
  -- Update the modification time
  :NEW.modificationTime := castorVdqmCommon.getTime();
END;
/


/**
 * Enforces two integrity constraints of the TapeDriveDedication table.  A
 * client host entry must be a valid regular expression and a VID entry must
 * also be a valid regular expression.
 *
 * @exception invalid_regexp_host if the client host value is not a valid
 * regular expression.
 * @exception invalid_regexp_vid if the VID value is not a valid regular
 * expression.
 */
CREATE OR REPLACE TRIGGER TR_I_TapeDriveDedication
  BEFORE INSERT ON TapeDriveDedication
FOR EACH ROW
DECLARE
  dummyVar NUMBER;
BEGIN
  BEGIN
    SELECT COUNT(*) INTO dummyVar
      FROM DUAL
      WHERE REGEXP_LIKE(dummy, :NEW.clientHost);
  EXCEPTION
    WHEN OTHERS THEN
      castorVdqmException.throw(castorVdqmException.invalid_regexp_host_cd,
        'Client host value is not a valid regular expression ''' ||
        :NEW.clientHost || '''');
  END;

  BEGIN
    SELECT COUNT(*) INTO dummyVar
      FROM DUAL
      WHERE REGEXP_LIKE(dummy, :NEW.vid);
  EXCEPTION
    WHEN OTHERS THEN
      castorVdqmException.throw(castorVdqmException.invalid_regexp_vid_cd,
        'VID value is not a valid regular expression ''' || :NEW.vid || '''');
  END;
END;
/


/**
 * Sets the creation time and initial modification time of a tape-to-drive
 * dedication when it is inserted into the Tape2DriveDedication table.
 * Converts an updated tape drive id of 0 to NULL.
 */
CREATE OR REPLACE TRIGGER TR_I_Tape2DriveDedication
  BEFORE INSERT ON Tape2DriveDedication
FOR EACH ROW
DECLARE
  timeVar NUMBER := castorVdqmCommon.getTime();
BEGIN
  -- Set creation time
  :NEW.creationTime := timeVar;

  -- Set modification time
  :NEW.modificationTime := timeVar;

  -- Convert a tape drive id of 0 to NULL
  IF :NEW.tapeDrive = 0 THEN
    :NEW.tapeDrive := NULL;
  END IF;
END;
/


/**
 * Updates the modification time of a tape-to-drive dedication.
 * Converts an updated tape drive id of 0 to NULL.
 */
CREATE OR REPLACE TRIGGER TR_U_Tape2DriveDedication
  BEFORE UPDATE ON Tape2DriveDedication
FOR EACH ROW
BEGIN
  -- Update the modification time
  :NEW.modificationTime := castorVdqmCommon.getTime();

  -- Convert a tape drive id of 0 to NULL
  IF :NEW.tapeDrive = 0 THEN
    :NEW.tapeDrive := NULL;
  END IF;
END;
/

/* Flag the schema creation as COMPLETE */
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE';
COMMIT;
