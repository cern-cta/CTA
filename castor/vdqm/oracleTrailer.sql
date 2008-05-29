/*******************************************************************
 *
 * @(#)$RCSfile: oracleTrailer.sql,v $ $Revision: 1.120 $ $Release$ $Date: 2008/05/29 13:36:04 $ $Author: murrayc3 $
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
ALTER TABLE ClientIdentification MODIFY (egid NOT NULL);
ALTER TABLE ClientIdentification MODIFY (euid NOT NULL);
ALTER TABLE ClientIdentification MODIFY (magic NOT NULL);
ALTER TABLE ClientIdentification MODIFY (port NOT NULL);
ALTER TABLE Id2Type MODIFY (type NOT NULL);
ALTER TABLE TapeAccessSpecification MODIFY (accessMode NOT NULL);
ALTER TABLE TapeDrive MODIFY (deviceGroupName NOT NULL);
ALTER TABLE TapeDrive MODIFY (errCount NOT NULL);
ALTER TABLE TapeDrive MODIFY (jobId NOT NULL);
ALTER TABLE TapeDrive MODIFY (modificationTime NOT NULL);
ALTER TABLE TapeDrive MODIFY (resetTime NOT NULL);
ALTER TABLE TapeDrive MODIFY (runningTapeReq NOT NULL);
ALTER TABLE TapeDrive MODIFY (status NOT NULL);
ALTER TABLE TapeDrive MODIFY (tape NOT NULL);
ALTER TABLE TapeDrive MODIFY (tapeServer NOT NULL);
ALTER TABLE TapeDrive MODIFY (totalMB NOT NULL);
ALTER TABLE TapeDrive MODIFY (transferredMB NOT NULL);
ALTER TABLE TapeDrive MODIFY (useCount NOT NULL);
ALTER TABLE TapeDrive2TapeDriveComp MODIFY (child NOT NULL);
ALTER TABLE TapeDrive2TapeDriveComp MODIFY (parent NOT NULL);
ALTER TABLE TapeDriveCompatibility MODIFY (priorityLevel NOT NULL);
ALTER TABLE TapeDriveCompatibility MODIFY (tapeAccessSpecification NOT NULL);
ALTER TABLE TapeDriveDedication MODIFY (TAPEDRIVE NOT NULL);
ALTER TABLE TapeRequest MODIFY (client NOT NULL);
ALTER TABLE TapeRequest MODIFY (creationTime NOT NULL);
ALTER TABLE TapeRequest MODIFY (deviceGroupName NOT NULL);
ALTER TABLE TapeRequest MODIFY (errorCode NOT NULL);
ALTER TABLE TapeRequest MODIFY (modificationTime NOT NULL);
ALTER TABLE TapeRequest MODIFY (priority NOT NULL);
ALTER TABLE TapeRequest MODIFY (requestedSrv NOT NULL);
ALTER TABLE TapeRequest MODIFY (status NOT NULL);
ALTER TABLE TapeRequest MODIFY (tape NOT NULL);
ALTER TABLE TapeRequest MODIFY (tapeAccessSpecification NOT NULL);
ALTER TABLE TapeRequest MODIFY (tapeDrive NOT NULL);
ALTER TABLE TapeServer MODIFY (actingMode NOT NULL);
ALTER TABLE VolumePriority MODIFY (priority NOT NULL);
ALTER TABLE VolumePriority MODIFY (clientUID NOT NULL);
ALTER TABLE VolumePriority MODIFY (clientGID NOT NULL);
ALTER TABLE VolumePriority MODIFY (clientHost NOT NULL);
ALTER TABLE VolumePriority MODIFY (vid NOT NULL);
ALTER TABLE VolumePriority MODIFY (tpMode NOT NULL);
ALTER TABLE VolumePriority MODIFY (lifespanType NOT NULL);

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


/* Check constraints */
-- The accessMode column of the TapeAccessSpecification table has 2 possible
-- values: 0=write-disabled or 1=write-enabled
ALTER TABLE TapeAccessSpecification
  ADD CONSTRAINT CH_TapeAccessSpec_accessMode
    CHECK ((accessMode=0) OR (accessMode=1));

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

ALTER TABLE VolumePriority
  ADD CONSTRAINT FK_VolumePriority_id
    FOREIGN KEY (id)
    REFERENCES Id2Type (id)
    DEFERRABLE
    INITIALLY DEFERRED
    ENABLE;


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
   * @return number of seconds since the EPOCH UTC + 1.
   */
  FUNCTION getTime RETURN NUMBER;

END castorVdqmCommon;


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
    ret NUMBER;
  BEGIN
    SELECT (SYSDATE - to_date('01-jan-1970 01:00:00','dd-mon-yyyy HH:MI:SS')) *
      (24*60*60) INTO ret FROM DUAL;
    RETURN ret;
  END getTime;

END castorVdqmCommon;


/**
 * This package contains functions used by some of the VDQM database views.
 */
CREATE OR REPLACE PACKAGE castorVdqmView AS

  /**
   * This function determines if the specified drive and usage pass all of the
   * dedications of the drive.
   *
   * @param driveIdVar the ID of the drive.
   * @param gidVar     the gid of the client.
   * @param hostVar    the host of the client.
   * @param modeVar    the tape access mode.
   * @param uidVar     the uid of the client.
   * @param vidVar     the vid of the volume request.
   * @return 1 if the specified drive passes all of its dedications, else 0.
   */
  FUNCTION passesDedications(driveIdVar IN NUMBER, gidVar IN NUMBER,
    hostVar IN VARCHAR2, modeVar IN NUMBER, uidVar IN NUMBER,
    vidVar IN VARCHAR2) RETURN NUMBER;

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


/**
 * See the castorVdqmView package specification for documentation.
 */
CREATE OR REPLACE PACKAGE BODY castorVdqmView AS

  /**
   * This private function determines whether or not the specified drive and
   * GID pass the dedications of the drive.
   *
   * @param driveIdVar the ID of the drive.
   * @param gidVar     the GID of the client.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesGidDedications(
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
  END passesGidDedications;


  /**
   * This private function determines whether or not the specified drive and
   * access mode pass the dedications of the drive.
   *
   * @param driveIdVar    the ID of the drive.
   * @param accessModeVar the access mode of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesModeDedication(
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
  END passesModeDedication;


  /**
   * This private function determines whether or not the specified drive and
   * host pass the dedications of the drive.
   *
   * @param driveIdVar    the ID of the drive.
   * @param clientHostVar the client host of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesHostDedications(
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
            TapeDriveDedication.tapeDrive = driveIdVar
        AND REGEXP_LIKE(clientHostVar, TapeDriveDedication.clientHost);

    -- As there are host dedications for the drive, it only passes if at least
    -- one matches
    IF nbHostDedicationsVar > 0 THEN
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END passesHostDedications;


  /**
   * This private function determines whether or not the specified drive and
   * UID pass the dedications of the drive.
   *
   * @param driveIdVar the ID of the drive.
   * @param uidVar     the UID of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesUidDedications(
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
  END passesUidDedications;


  /**
   * This private function determines whether or not the specified drive and
   * VID pass the dedications of the drive.
   *
   * @param driveIdVar the ID of the drive.
   * @param vidVar     the vid of the volume request.
   * @return 1 if the dedications are passed, else 0.
   */
  FUNCTION passesVidDedications(
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
  END passesVidDedications;


  /**
   * See the castorVdqmView package specification for documentation.
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
    IF passesGidDedications(driveIdVar, gidVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesHostDedications(driveIdVar, hostVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesModeDedication(driveIdVar, modeVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesUidDedications(driveIdVar, uidVar) = 0 THEN
      RETURN 0;
    END IF;

    IF passesVidDedications(driveIdVar, vidVar) = 0 THEN
      RETURN 0;
    END IF;

    -- Drive has passed all of its dedications
    RETURN 1;
  END passesDedications;


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

    RETURN buf;
  END getVdqmDedicate;

END castorVdqmView;


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
 * This view shows candidate "free tape drive to pending tape request"
 * allocations.
 */
CREATE OR REPLACE VIEW CandidateDriveAllocations_VIEW AS SELECT UNIQUE
  TapeDrive.id as tapeDriveId,
  TapeRequest.id as tapeRequestId,
  TapeAccessSpecification.accessMode,
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
INNER JOIN TapeServer ON
     TapeRequest.requestedSrv = TapeServer.id
  OR TapeRequest.requestedSrv = 0
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
  AND castorVdqmView.passesDedications(tapeDrive.id, ClientIdentification.egid,
    ClientIdentification.machine, TapeAccessSpecification.accessMode,
    ClientIdentification.euid, VdqmTape.vid)=1
ORDER BY
  TapeAccessSpecification.accessMode DESC,
  VolumePriority DESC,
  TapeRequest.creationTime ASC;


/**
 * This view shows candidate drive allocations that will reuse a current drive
 * allocation.
 */
CREATE OR REPLACE VIEW DriveAllocationsForReuse_VIEW AS SELECT UNIQUE
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
  AND castorVdqmView.passesDedications(tapeDrive.id, ClientIdentification.egid,
    ClientIdentification.machine, TapeAccessSpecification.accessMode,
    ClientIdentification.euid, VdqmTape.vid)=1
ORDER BY
  TapeAccessSpecification.accessMode DESC,
  TapeRequest.modificationTime ASC;


/**
 * View used for generating the list of drives when replying to the showqueues
 * command
 */
CREATE OR REPLACE VIEW tapeDriveShowqueues_VIEW
AS WITH TimeZoneOffset AS (
  SELECT
    (EXTRACT(TIMEZONE_HOUR FROM CURRENT_TIMESTAMP) - 1) * 3600 AS value
  FROM DUAL)
SELECT
  TapeDrive.status, TapeDrive.id, TapeDrive.runningTapeReq, TapeDrive.jobId,
  TapeDrive.modificationTime -
    (SELECT TimeZoneOffset.value FROM TimeZoneOffset) AS modificationTime,
  TapeDrive.resetTime, TapeDrive.useCount, TapeDrive.errCount,
  TapeDrive.transferredMB, TapeAccessSpecification.accessMode AS tapeAccessMode,
  TapeDrive.totalMB, TapeServer.serverName, VdqmTape.vid, TapeDrive.driveName,
  DeviceGroupName.dgName, castorVdqmView.getVdqmDedicate(TapeDrive.Id) AS
    dedicate
FROM
  TapeDrive
LEFT OUTER JOIN TapeServer ON
  TapeDrive.tapeServer = TapeServer.id
LEFT OUTER JOIN VdqmTape ON
  TapeDrive.tape = VDQMTAPE.ID
LEFT OUTER JOIN DEVICEGROUPNAME ON
  TapeDrive.deviceGroupName = DeviceGroupName.id
LEFT OUTER JOIN TapeDriveDedication ON
  TapeDrive.id = TapeDriveDedication.tapeDrive
LEFT OUTER JOIN TapeRequest ON
  TapeDrive.RunningTapeReq = TapeRequest.id
LEFT OUTER JOIN TapeAccessSpecification ON
  TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
ORDER BY
  DriveName ASC;


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
  ClientIdentification.username AS clientUsername,
  NVL(EffectiveVolumePriority_VIEW.priority,0) AS volumePriority
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
  AND TapeAccessSpecification.accessMode = EffectiveVolumePriority_VIEW.tpMode
ORDER BY
  TapeRequest.creationTime ASC;


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
    returnVar OUT NUMBER, tapeRequestIdVar  OUT NUMBER);

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
   * This procedure tries to write to the database the fact that a successful
   * RTCPD job submission has occured.  This update of the database may not be
   * possible if the corresponding drive and tape request states have been
   * modified by other threads.  For example a thread handling a tape drive
   * request message may have put the drive into the down state.  The RTCPD job
   * submission should be ignored in this case.
   *
   * @param tapeDriveIdVar the ID of the drive
   * @param tapeRequestIdVar the ID of the tape request
   * @param returnVar has a value of 1 if the occurance of the RTCPD job
   * submission was successfully written to the database, else 0.
   */
  PROCEDURE writeRTPCDJobSubmission(tapeDriveIdVar IN NUMBER,
    tapeRequestIdVar IN NUMBER, returnVar OUT NUMBER);

  /**
   * This procedure tries to write to the database the fact that a failed RTCPD
   * job submission has occured.  This update of the database may not be
   * possible if the corresponding drive and tape request states have been
   * modified by other threads.  For example a thread handling a tape drive
   * request message may have put the drive into the down state.  The failed
   * RTCPD job submission should be ignored in this case.
   *
   * @param tapeDriveIdVar the ID of the drive
   * @param tapeRequestIdVar the ID of the tape request
   * @param returnVar has a value of 1 if the occurance of the failed RTCPD job
   * submission was successfully written to the database, else 0.
   */
  PROCEDURE writeFailedRTPCDJobSubmission(tapeDriveIdVar IN NUMBER,
    tapeRequestIdVar IN NUMBER, returnVar OUT NUMBER);

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

END castorVdqm;


/**
 * See the castorVdqm package specification for documentation.
 */
CREATE OR REPLACE PACKAGE BODY castorVdqm AS

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

      -- If the drive allocation is still valid, i.e. drive status is UNIT_UP 
      -- and request status is REQUEST_PENDING
      IF(TapeDriveStatusVar = 0) AND (TapeRequestStatusVar = 0) THEN

        -- Allocate the free drive to the pending request
        UPDATE TapeDrive SET
          status           = 1, -- UNIT_STARTING
          jobId            = 0,
          modificationTime = castorVdqmCommon.getTime(),
          runningTapeReq   = tapeRequestIdVar
        WHERE
          id = tapeDriveIdVar;
        UPDATE TapeRequest SET
          status           = 1, -- MATCHED
          tapeDrive        = tapeDriveIdVar,
          modificationTime = castorVdqmCommon.getTime()
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
        modificationTime = castorVdqmCommon.getTime()
      WHERE id = tapeRequestIdVar;
      UPDATE TapeDrive SET
        status           = 1, -- UNIT_STARTING
        jobId            = 0,
        runningTapeReq   = tapeRequestIdVar,
        modificationTime = castorVdqmCommon.getTime()
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
    dgnIdVar             NUMBER;
    serverIdVar          NUMBER;
    nbMatchingServersVar NUMBER;
    nbMatchingDgnsVar    NUMBER;
    TYPE dedicationList_t IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    dedicationsToDelete  dedicationList_t;
    dedicationIdVar      NUMBER;
  BEGIN
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
      castorVdqmException.throw(castorVdqmException.drive_not_found_cd,
        'Tape drive ''' || driveNameVar || ''' not found in database.');
    END;

    -- Check the specified tape drive is associated with the specified tape
    -- server
    SELECT count(*) INTO nbMatchingServersVar
      FROM TapeServer
      WHERE
            TapeServer.id         = serverIdVar
        AND TapeServer.serverName = serverNameVar;

    IF nbMatchingServersVar = 0 THEN
      castorVdqmException.throw(castorVdqmException.drive_server_not_found_cd,
        'Tape drive ''' || driveNameVar || ''', server ''' || serverNameVar ||
        ''' combination not found in database.');
    END IF;

    -- Check the specified tape drive is associated with the specified dgn
    SELECT count(*) INTO nbMatchingDgnsVar
      FROM DeviceGroupName
      WHERE
            DeviceGroupName.id     = dgnIdVar
        AND DeviceGroupName.dgName = dgNameVar;

    IF nbMatchingDgnsVar = 0 THEN
      castorVdqmException.throw(castorVdqmException.drive_dgn_not_found_cd,
        'Tape drive ''' || driveNameVar || ''', DGN ''' || dgNameVar ||
        ''' combination not found in database.');
      RETURN;
    END IF;

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
        DELETE FROM Id2Type
          WHERE Id2Type.id = dedicationsToDelete(i);
      END LOOP;
    END IF;

    -- Insert new dedications
    IF gidVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, egid)
        VALUES(ids_seq.nextval, driveIdVar, gidVar)
      RETURNING id INTO dedicationIdVar;
      INSERT INTO Id2Type (id, type)
        VALUES (dedicationIdVar, 90);
    END IF;
    IF hostVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, clientHost)
        VALUES(ids_seq.nextval, driveIdVar, hostVar)
      RETURNING id INTO dedicationIdVar;
      INSERT INTO Id2Type (id, type)
        VALUES (dedicationIdVar, 90);
    END IF;
    IF modeVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, accessMode)
        VALUES(ids_seq.nextval, driveIdVar, modeVar)
      RETURNING id INTO dedicationIdVar;
      INSERT INTO Id2Type (id, type)
        VALUES (dedicationIdVar, 90);
    END IF;
    IF uidVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, euid)
        VALUES(ids_seq.nextval, driveIdVar, uidVar)
      RETURNING id INTO dedicationIdVar;
      INSERT INTO Id2Type (id, type)
        VALUES (dedicationIdVar, 90);
    END IF;
    IF vidVar IS NOT NULL THEN
      INSERT INTO TapeDriveDedication(id, tapeDrive, vid)
        VALUES(ids_seq.nextval, driveIdVar, vidVar)
      RETURNING id INTO dedicationIdVar;
      INSERT INTO Id2Type (id, type)
        VALUES (dedicationIdVar, 90);
    END IF;

  END dedicateDrive;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE deleteDrive
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
  
    DELETE FROM TapeDrive2TapeDriveComp WHERE parent = driveIdVar;
    DELETE FROM TapeDrive WHERE id = driveIdVar;
    DELETE FROM Id2Type WHERE id = driveIdVar;
  END deleteDrive;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE writeRTPCDJobSubmission(
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
    -- bringing down of the drive in question, which would make it meaningless
    -- to record a successful RTPCD job submission.  Therefore get a lock on the
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

  END writeRTPCDJobSubmission;


  /**
   * See the castorVdqm package specification for documentation.
   */
  PROCEDURE writeFailedRTPCDJobSubmission(
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
    -- bringing down of the drive in question, which would make it meaningless
    -- to record a successful RTPCD job submission.  Therefore get a lock on the
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

  END writeFailedRTPCDJobSubmission;


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

      -- Update it
      UPDATE VolumePriority SET
        VolumePriority.priority   = priorityVar,
        VolumePriority.clientUID  = clientUIDVar,
        VolumePriority.clientGID  = clientGIDVar,
        VolumePriority.clientHost = clientHostVar
      WHERE
        VolumePriority.id = priorityIdVar;

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

      -- If a row for the priority was successfully created then
      IF priorityIdVar IS NOT NULL THEN

        -- Update Id2Type
        INSERT INTO Id2Type (id, type)
        VALUES (priorityIdVar, 151); -- 151=OBJ_VolumePriority

      -- Else a competing thread created the row first
      ELSE

        -- Select and get a lock on the row
        SELECT id INTO priorityIdVar FROM VolumePriority WHERE
            VolumePriority.vid          = vidVar
        AND VolumePriority.tpMode       = tpModeVar
        AND VolumePriority.lifespanType = lifespanTypeVar
        FOR UPDATE;

        -- Update the row
        UPDATE VolumePriority SET
          priority   = priorityVar,
          clientUID  = clientUIDVar,
          clientGID  = clientGIDVar,
          clientHost = clientHostVar;
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
    DELETE FROM Id2Type        WHERE Id2Type.id        = priorityIdVar;

    -- Give the ID of the volume priority row that was deleted
    returnVar := priorityIdVar;
  END deleteVolPriority;

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
  :NEW.modificationTime := castorVdqmCommon.getTime();
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
  :NEW.modificationTime := castorVdqmCommon.getTime();
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
  timeVar NUMBER := castorVdqmCommon.getTime();
BEGIN
  -- Set creation time
  :NEW.creationTime := timeVar;

  -- Set modification time
  :NEW.modificationTime := timeVar;
END;


/**
 * Updates the modification time of a tape request.
 */
CREATE OR REPLACE TRIGGER TR_U_TapeRequest
  BEFORE UPDATE OF modificationTime, status ON TapeRequest
FOR EACH ROW
WHEN
  ((NEW.modificationTime != OLD.modificationTime) OR (NEW.status != OLD.status))
BEGIN
  -- Update the modification time
  :NEW.modificationTime := castorVdqmCommon.getTime();
END;


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
