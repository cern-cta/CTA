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

