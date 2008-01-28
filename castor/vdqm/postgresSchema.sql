/* SQL statements for type TapeAccessSpecification */
CREATE TABLE TapeAccessSpecification (accessMode INT4, density VARCHAR(2048), tapeModel VARCHAR(2048), id INT8 CONSTRAINT I_TapeAccessSpecification_Id PRIMARY KEY);

/* SQL statements for type TapeServer */
CREATE TABLE TapeServer (serverName VARCHAR(2048), id INT8 CONSTRAINT I_TapeServer_Id PRIMARY KEY, actingMode INTEGER);

/* SQL statements for type TapeRequest */
CREATE TABLE TapeRequest (priority INT4, modificationTime INT8, creationTime INT8, id INT8 CONSTRAINT I_TapeRequest_Id PRIMARY KEY, tape INTEGER, tapeAccessSpecification INTEGER, requestedSrv INTEGER, tapeDrive INTEGER, deviceGroupName INTEGER, client INTEGER);

/* SQL statements for type TapeDrive */
CREATE TABLE TapeDrive (jobID INT4, modificationTime INT8, resettime INT8, usecount INT4, errcount INT4, transferredMB INT4, totalMB INT8, driveName VARCHAR(2048), tapeAccessMode INT4, id INT8 CONSTRAINT I_TapeDrive_Id PRIMARY KEY, tape INTEGER, runningTapeReq INTEGER, deviceGroupName INTEGER, status INTEGER, tapeServer INTEGER);
CREATE TABLE TapeDrive2TapeDriveComp (Parent INTEGER, Child INTEGER);
CREATE INDEX I_TapeDrive2TapeDriveComp_C on TapeDrive2TapeDriveComp (child);
CREATE INDEX I_TapeDrive2TapeDriveComp_P on TapeDrive2TapeDriveComp (parent);

/* SQL statements for type ErrorHistory */
CREATE TABLE ErrorHistory (errorMessage VARCHAR(2048), timeStamp INT8, id INT8 CONSTRAINT I_ErrorHistory_Id PRIMARY KEY, tapeDrive INTEGER, tape INTEGER);

/* SQL statements for type TapeDriveDedication */
CREATE TABLE TapeDriveDedication (clientHost VARCHAR(2048), euid INT4, egid INT4, vid VARCHAR(2048), accessMode INT4, startTime INT8, endTime INT8, reason VARCHAR(2048), id INT8 CONSTRAINT I_TapeDriveDedication_Id PRIMARY KEY, tapeDrive INTEGER);

/* SQL statements for type TapeDriveCompatibility */
CREATE TABLE TapeDriveCompatibility (tapeDriveModel VARCHAR(2048), priorityLevel INT4, id INT8 CONSTRAINT I_TapeDriveCompatibility_Id PRIMARY KEY, tapeAccessSpecification INTEGER);

/* SQL statements for type DeviceGroupName */
CREATE TABLE DeviceGroupName (dgName VARCHAR(2048), libraryName VARCHAR(2048), id INT8 CONSTRAINT I_DeviceGroupName_Id PRIMARY KEY);

/* SQL statements for type VdqmTape */
CREATE TABLE VdqmTape (vid VARCHAR(2048), side INT4, tpmode INT4, errMsgTxt VARCHAR(2048), errorCode INT4, severity INT4, vwAddress VARCHAR(2048), id INT8 CONSTRAINT I_VdqmTape_Id PRIMARY KEY, status INTEGER);

/* SQL statements for type ClientIdentification */
CREATE TABLE ClientIdentification (machine VARCHAR(2048), userName VARCHAR(2048), port INT4, euid INT4, egid INT4, magic INT4, id INT8 CONSTRAINT I_ClientIdentification_Id PRIMARY KEY);

ALTER TABLE TapeDrive2TapeDriveComp
  ADD CONSTRAINT fk_TapeDrive2TapeDriveComp_P FOREIGN KEY (Parent) REFERENCES TapeDrive (id)
  ADD CONSTRAINT fk_TapeDrive2TapeDriveComp_C FOREIGN KEY (Child) REFERENCES TapeDriveCompatibility (id);
