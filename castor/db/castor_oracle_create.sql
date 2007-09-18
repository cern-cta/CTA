/* SQL statements for type BaseAddress */
CREATE TABLE BaseAddress (objType NUMBER, cnvSvcName VARCHAR2(2048), cnvSvcType NUMBER, target INTEGER, id INTEGER CONSTRAINT I_BaseAddress_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Client */
CREATE TABLE Client (ipAddress NUMBER, port NUMBER, version NUMBER, id INTEGER CONSTRAINT I_Client_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type ClientIdentification */
CREATE TABLE ClientIdentification (machine VARCHAR2(2048), userName VARCHAR2(2048), port NUMBER, euid NUMBER, egid NUMBER, magic NUMBER, id INTEGER CONSTRAINT I_ClientIdentification_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Disk2DiskCopyDoneRequest */
CREATE TABLE Disk2DiskCopyDoneRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskCopyId INTEGER, sourceDiskCopyId INTEGER, id INTEGER CONSTRAINT I_Disk2DiskCopyDoneRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type GetUpdateDone */
CREATE TABLE GetUpdateDone (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, id INTEGER CONSTRAINT I_GetUpdateDone_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type GetUpdateFailed */
CREATE TABLE GetUpdateFailed (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, id INTEGER CONSTRAINT I_GetUpdateFailed_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type PutFailed */
CREATE TABLE PutFailed (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, id INTEGER CONSTRAINT I_PutFailed_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Files2Delete */
CREATE TABLE Files2Delete (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskServer VARCHAR2(2048), id INTEGER CONSTRAINT I_Files2Delete_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type FilesDeleted */
CREATE TABLE FilesDeleted (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_FilesDeleted_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type FilesDeletionFailed */
CREATE TABLE FilesDeletionFailed (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_FilesDeletionFailed_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type GCFile */
CREATE TABLE GCFile (diskCopyId INTEGER, id INTEGER CONSTRAINT I_GCFile_Id PRIMARY KEY, request INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type GCLocalFile */
CREATE TABLE GCLocalFile (fileName VARCHAR2(2048), diskCopyId INTEGER, fileId INTEGER, nsHost VARCHAR2(2048), id INTEGER CONSTRAINT I_GCLocalFile_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type MoverCloseRequest */
CREATE TABLE MoverCloseRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, fileSize INTEGER, timeStamp INTEGER, id INTEGER CONSTRAINT I_MoverCloseRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type PutStartRequest */
CREATE TABLE PutStartRequest (subreqId INTEGER, diskServer VARCHAR2(2048), fileSystem VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_PutStartRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type PutDoneStart */
CREATE TABLE PutDoneStart (subreqId INTEGER, diskServer VARCHAR2(2048), fileSystem VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_PutDoneStart_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type GetUpdateStartRequest */
CREATE TABLE GetUpdateStartRequest (subreqId INTEGER, diskServer VARCHAR2(2048), fileSystem VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_GetUpdateStartRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type QueryParameter */
CREATE TABLE QueryParameter (value VARCHAR2(2048), id INTEGER CONSTRAINT I_QueryParameter_Id PRIMARY KEY, query INTEGER, queryType INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StagePrepareToGetRequest */
CREATE TABLE StagePrepareToGetRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StagePrepareToGetRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StagePrepareToPutRequest */
CREATE TABLE StagePrepareToPutRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StagePrepareToPutRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StagePrepareToUpdateRequest */
CREATE TABLE StagePrepareToUpdateRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StagePrepareToUpdateReque_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StagePutRequest */
CREATE TABLE StagePutRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StagePutRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageUpdateRequest */
CREATE TABLE StageUpdateRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StageUpdateRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageRmRequest */
CREATE TABLE StageRmRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StageRmRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StagePutDoneRequest */
CREATE TABLE StagePutDoneRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, parentUuid VARCHAR2(2048), id INTEGER CONSTRAINT I_StagePutDoneRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER, parent INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageFileQueryRequest */
CREATE TABLE StageFileQueryRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, fileName VARCHAR2(2048), id INTEGER CONSTRAINT I_StageFileQueryRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageRequestQueryRequest */
CREATE TABLE StageRequestQueryRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StageRequestQueryRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageFindRequestRequest */
CREATE TABLE StageFindRequestRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StageFindRequestRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type SubRequest */
CREATE TABLE SubRequest (retryCounter NUMBER, fileName VARCHAR2(2048), protocol VARCHAR2(2048), xsize INTEGER, priority NUMBER, subreqId VARCHAR2(2048), flags NUMBER, modeBits NUMBER, creationTime INTEGER, lastModificationTime INTEGER, answered NUMBER, errorCode NUMBER, errorMessage VARCHAR2(2048), requestedFileSystems VARCHAR2(2048), id INTEGER CONSTRAINT I_SubRequest_Id PRIMARY KEY, diskcopy INTEGER, castorFile INTEGER, parent INTEGER, status INTEGER, request INTEGER, getNextStatus INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageReleaseFilesRequest */
CREATE TABLE StageReleaseFilesRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StageReleaseFilesRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageAbortRequest */
CREATE TABLE StageAbortRequest (parentUuid VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StageAbortRequest_Id PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageGetNextRequest */
CREATE TABLE StageGetNextRequest (parentUuid VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StageGetNextRequest_Id PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StagePutNextRequest */
CREATE TABLE StagePutNextRequest (parentUuid VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StagePutNextRequest_Id PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageUpdateNextRequest */
CREATE TABLE StageUpdateNextRequest (parentUuid VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StageUpdateNextRequest_Id PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Tape */
CREATE TABLE Tape (vid VARCHAR2(2048), side NUMBER, tpmode NUMBER, errMsgTxt VARCHAR2(2048), errorCode NUMBER, severity NUMBER, vwAddress VARCHAR2(2048), id INTEGER CONSTRAINT I_Tape_Id PRIMARY KEY, stream INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Segment */
CREATE TABLE Segment (fseq NUMBER, offset INTEGER, bytes_in INTEGER, bytes_out INTEGER, host_bytes INTEGER, segmCksumAlgorithm VARCHAR2(2048), segmCksum NUMBER, errMsgTxt VARCHAR2(2048), errorCode NUMBER, severity NUMBER, blockId0 INTEGER, blockId1 INTEGER, blockId2 INTEGER, blockId3 INTEGER, id INTEGER CONSTRAINT I_Segment_Id PRIMARY KEY, tape INTEGER, copy INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapePool */
CREATE TABLE TapePool (name VARCHAR2(2048), id INTEGER CONSTRAINT I_TapePool_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeCopy */
CREATE TABLE TapeCopy (copyNb NUMBER, id INTEGER CONSTRAINT I_TapeCopy_Id PRIMARY KEY, castorFile INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type CastorFile */
CREATE TABLE CastorFile (fileId INTEGER, nsHost VARCHAR2(2048), fileSize INTEGER, creationTime INTEGER, lastAccessTime INTEGER, nbAccesses NUMBER, lastKnownFileName VARCHAR2(2048), lastUpdateTime INTEGER, id INTEGER CONSTRAINT I_CastorFile_Id PRIMARY KEY, svcClass INTEGER, fileClass INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type DiskCopy */
CREATE TABLE DiskCopy (path VARCHAR2(2048), gcWeight float, creationTime INTEGER, id INTEGER CONSTRAINT I_DiskCopy_Id PRIMARY KEY, fileSystem INTEGER, castorFile INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type FileSystem */
CREATE TABLE FileSystem (free INTEGER, mountPoint VARCHAR2(2048), minFreeSpace float, minAllowedFreeSpace float, maxFreeSpace float, spaceToBeFreed INTEGER, totalSize INTEGER, readRate INTEGER, writeRate INTEGER, nbReadStreams NUMBER, nbWriteStreams NUMBER, nbReadWriteStreams NUMBER, nbMigratorStreams NUMBER, nbRecallerStreams NUMBER, id INTEGER CONSTRAINT I_FileSystem_Id PRIMARY KEY, diskPool INTEGER, diskserver INTEGER, status INTEGER, adminStatus INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type SvcClass */
CREATE TABLE SvcClass (nbDrives NUMBER, name VARCHAR2(2048), defaultFileSize INTEGER, maxReplicaNb NUMBER, replicationPolicy VARCHAR2(2048), gcPolicy VARCHAR2(2048), migratorPolicy VARCHAR2(2048), recallerPolicy VARCHAR2(2048), hasDiskOnlyBehavior NUMBER, forcedFileClass VARCHAR2(2048), streamPolicy VARCHAR2(2048), id INTEGER CONSTRAINT I_SvcClass_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE SvcClass2TapePool (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_SvcClass2TapePool_C on SvcClass2TapePool (child);
CREATE INDEX I_SvcClass2TapePool_P on SvcClass2TapePool (parent);

/* SQL statements for type DiskPool */
CREATE TABLE DiskPool (name VARCHAR2(2048), id INTEGER CONSTRAINT I_DiskPool_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE DiskPool2SvcClass (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_DiskPool2SvcClass_C on DiskPool2SvcClass (child);
CREATE INDEX I_DiskPool2SvcClass_P on DiskPool2SvcClass (parent);

/* SQL statements for type Stream */
CREATE TABLE Stream (initialSizeToTransfer INTEGER, lastFileSystemChange INTEGER, id INTEGER CONSTRAINT I_Stream_Id PRIMARY KEY, tape INTEGER, lastFileSystemUsed INTEGER, lastButOneFileSystemUsed INTEGER, tapePool INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE Stream2TapeCopy (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_Stream2TapeCopy_C on Stream2TapeCopy (child);
CREATE INDEX I_Stream2TapeCopy_P on Stream2TapeCopy (parent);

/* SQL statements for type FileClass */
CREATE TABLE FileClass (name VARCHAR2(2048), minFileSize INTEGER, maxFileSize INTEGER, nbCopies NUMBER, id INTEGER CONSTRAINT I_FileClass_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type DiskServer */
CREATE TABLE DiskServer (name VARCHAR2(2048), readRate INTEGER, writeRate INTEGER, nbReadStreams NUMBER, nbWriteStreams NUMBER, nbReadWriteStreams NUMBER, nbMigratorStreams NUMBER, nbRecallerStreams NUMBER, id INTEGER CONSTRAINT I_DiskServer_Id PRIMARY KEY, status INTEGER, adminStatus INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type SetFileGCWeight */
CREATE TABLE SetFileGCWeight (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, weight float, id INTEGER CONSTRAINT I_SetFileGCWeight_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageRepackRequest */
CREATE TABLE StageRepackRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, repackVid VARCHAR2(2048), id INTEGER CONSTRAINT I_StageRepackRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageGetRequest */
CREATE TABLE StageGetRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_StageGetRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeAccessSpecification */
CREATE TABLE TapeAccessSpecification (accessMode NUMBER, density VARCHAR2(2048), tapeModel VARCHAR2(2048), id INTEGER CONSTRAINT I_TapeAccessSpecification_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeServer */
CREATE TABLE TapeServer (serverName VARCHAR2(2048), id INTEGER CONSTRAINT I_TapeServer_Id PRIMARY KEY, actingMode INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeRequest */
CREATE TABLE TapeRequest (priority NUMBER, modificationTime INTEGER, creationTime INTEGER, id INTEGER CONSTRAINT I_TapeRequest_Id PRIMARY KEY, tape INTEGER, tapeAccessSpecification INTEGER, requestedSrv INTEGER, tapeDrive INTEGER, deviceGroupName INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeDrive */
CREATE TABLE TapeDrive (jobID NUMBER, modificationTime INTEGER, resettime INTEGER, usecount NUMBER, errcount NUMBER, transferredMB NUMBER, totalMB INTEGER, driveName VARCHAR2(2048), tapeAccessMode NUMBER, id INTEGER CONSTRAINT I_TapeDrive_Id PRIMARY KEY, tape INTEGER, runningTapeReq INTEGER, deviceGroupName INTEGER, status INTEGER, tapeServer INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE TapeDrive2TapeDriveComp (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_TapeDrive2TapeDriveComp_C on TapeDrive2TapeDriveComp (child);
CREATE INDEX I_TapeDrive2TapeDriveComp_P on TapeDrive2TapeDriveComp (parent);

/* SQL statements for type ErrorHistory */
CREATE TABLE ErrorHistory (errorMessage VARCHAR2(2048), timeStamp INTEGER, id INTEGER CONSTRAINT I_ErrorHistory_Id PRIMARY KEY, tapeDrive INTEGER, tape INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeDriveDedication */
CREATE TABLE TapeDriveDedication (clientHost VARCHAR2(2048), euid NUMBER, egid NUMBER, vid VARCHAR2(2048), accessMode NUMBER, startTime INTEGER, endTime INTEGER, reason VARCHAR2(2048), id INTEGER CONSTRAINT I_TapeDriveDedication_Id PRIMARY KEY, tapeDrive INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeDriveCompatibility */
CREATE TABLE TapeDriveCompatibility (tapeDriveModel VARCHAR2(2048), priorityLevel NUMBER, id INTEGER CONSTRAINT I_TapeDriveCompatibility_Id PRIMARY KEY, tapeAccessSpecification INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type DeviceGroupName */
CREATE TABLE DeviceGroupName (dgName VARCHAR2(2048), libraryName VARCHAR2(2048), id INTEGER CONSTRAINT I_DeviceGroupName_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type DiskPoolQuery */
CREATE TABLE DiskPoolQuery (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskPoolName VARCHAR2(2048), id INTEGER CONSTRAINT I_DiskPoolQuery_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type VersionQuery */
CREATE TABLE VersionQuery (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT I_VersionQuery_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

ALTER TABLE SvcClass2TapePool
  ADD CONSTRAINT fk_SvcClass2TapePool_P FOREIGN KEY (Parent) REFERENCES SvcClass (id)
  ADD CONSTRAINT fk_SvcClass2TapePool_C FOREIGN KEY (Child) REFERENCES TapePool (id);
ALTER TABLE DiskPool2SvcClass
  ADD CONSTRAINT fk_DiskPool2SvcClass_P FOREIGN KEY (Parent) REFERENCES DiskPool (id)
  ADD CONSTRAINT fk_DiskPool2SvcClass_C FOREIGN KEY (Child) REFERENCES SvcClass (id);
ALTER TABLE Stream2TapeCopy
  ADD CONSTRAINT fk_Stream2TapeCopy_P FOREIGN KEY (Parent) REFERENCES Stream (id)
  ADD CONSTRAINT fk_Stream2TapeCopy_C FOREIGN KEY (Child) REFERENCES TapeCopy (id);
ALTER TABLE TapeDrive2TapeDriveComp
  ADD CONSTRAINT fk_TapeDrive2TapeDriveComp_P FOREIGN KEY (Parent) REFERENCES TapeDrive (id)
  ADD CONSTRAINT fk_TapeDrive2TapeDriveComp_C FOREIGN KEY (Child) REFERENCES TapeDriveCompatibility (id);

CREATE TABLE CastorVersion (schemaVersion VARCHAR2(20), release VARCHAR2(20));
INSERT INTO CastorVersion VALUES ('-', '2_1_4_5');

/*******************************************************************
 *
 * @(#)RCSfile: oracleTrailer.sql,v  Revision: 1.502  Date: 2007/09/18 06:51:22  Author: waldron 
 *
 * This file contains SQL code that is not generated automatically
 * and is inserted at the end of the generated code
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* A small table used to cross check code and DB versions */
UPDATE CastorVersion SET schemaVersion = '2_1_4_3';

/* Sequence for indices */
CREATE SEQUENCE ids_seq CACHE 300;

/* SQL statements for object types */
CREATE TABLE Id2Type (id INTEGER CONSTRAINT I_Id2Type_Id PRIMARY KEY, type NUMBER);

/* SQL statements for requests status */
/* Partitioning enables faster response (more than indexing) for the most frequent queries - credits to Nilo Segura */
CREATE TABLE newRequests (type NUMBER(38) NOT NULL, id NUMBER(38) NOT NULL, creation DATE NOT NULL, CONSTRAINT I_NewRequests_Type_Id PRIMARY KEY (type, id))
organization index
compress
partition by list (type)
 (
  partition type_16 values (16)  tablespace stager_data,
  partition type_21 values (21)  tablespace stager_data,
  partition type_33 values (33)  tablespace stager_data,
  partition type_34 values (34)  tablespace stager_data,
  partition type_35 values (35)  tablespace stager_data,
  partition type_36 values (36)  tablespace stager_data,
  partition type_37 values (37)  tablespace stager_data,
  partition type_38 values (38)  tablespace stager_data,
  partition type_39 values (39)  tablespace stager_data,
  partition type_40 values (40)  tablespace stager_data,
  partition type_41 values (41)  tablespace stager_data,
  partition type_42 values (42)  tablespace stager_data,
  partition type_43 values (43)  tablespace stager_data,
  partition type_44 values (44)  tablespace stager_data,
  partition type_45 values (45)  tablespace stager_data,
  partition type_46 values (46)  tablespace stager_data,
  partition type_48 values (48)  tablespace stager_data,
  partition type_49 values (49)  tablespace stager_data,
  partition type_50 values (50)  tablespace stager_data,
  partition type_51 values (51)  tablespace stager_data,
  partition type_60 values (60)  tablespace stager_data,
  partition type_64 values (64)  tablespace stager_data,
  partition type_65 values (65)  tablespace stager_data,
  partition type_66 values (66)  tablespace stager_data,
  partition type_67 values (67)  tablespace stager_data,
  partition type_78 values (78)  tablespace stager_data,
  partition type_79 values (79)  tablespace stager_data,
  partition type_80 values (80)  tablespace stager_data,
  partition type_84 values (84)  tablespace stager_data,
  partition type_90 values (90)  tablespace stager_data,
  partition notlisted values (default) tablespace stager_data
 );

/* Redefinition of table SubRequest to make it partitioned by status */
/* Unfortunately it has already been defined, so we drop and recreate it */
/* Note that if the schema changes, this part has to be updated manually! */
DROP TABLE SubRequest;
CREATE TABLE SubRequest
  (retryCounter NUMBER, fileName VARCHAR2(2048), protocol VARCHAR2(2048),
   xsize INTEGER, priority NUMBER, subreqId VARCHAR2(2048), flags NUMBER,
   modeBits NUMBER, creationTime INTEGER, lastModificationTime INTEGER,
   answered NUMBER, errorCode NUMBER, errorMessage VARCHAR2(2048), id NUMBER NOT NULL,
   diskcopy INTEGER, castorFile INTEGER, parent INTEGER, status INTEGER,
   request INTEGER, getNextStatus INTEGER, requestedFileSystems VARCHAR2(2048)
  )
  PCTFREE 50 PCTUSED 40 INITRANS 50
  ENABLE ROW MOVEMENT
  PARTITION BY LIST (STATUS)
   (
    PARTITION P_STATUS_0_1_2   VALUES (0, 1, 2),      -- *START
    PARTITION P_STATUS_3_13_14 VALUES (3, 13, 14),    -- *SCHED
    PARTITION P_STATUS_4       VALUES (4),
    PARTITION P_STATUS_5       VALUES (5),
    PARTITION P_STATUS_6       VALUES (6),
    PARTITION P_STATUS_7_9_10  VALUES (7, 9, 10),     -- FAILED*
    PARTITION P_STATUS_8       VALUES (8),
    PARTITION P_STATUS_11      VALUES (11),
    PARTITION P_STATUS_12      VALUES (12),
    PARTITION P_STATUS_OTHER   VALUES (DEFAULT)
   );

ALTER TABLE SUBREQUEST ADD CONSTRAINT I_SUBREQUEST_PK PRIMARY KEY (ID);

/* Indexes related to most used entities */
CREATE UNIQUE INDEX I_DiskServer_name on DiskServer (name);

CREATE UNIQUE INDEX I_CastorFile_FileIdNsHost on CastorFile (fileId, nsHost);
CREATE INDEX I_CastorFile_LastKnownFileName on CastorFile (lastKnownFileName);
CREATE INDEX I_CastorFile_SvcClass on CastorFile (svcClass);

CREATE INDEX I_DiskCopy_Castorfile on DiskCopy (castorFile);
CREATE INDEX I_DiskCopy_FileSystem on DiskCopy (fileSystem);
CREATE INDEX I_DiskCopy_Status on DiskCopy (status);
CREATE INDEX I_DiskCopy_FS_Status_10 on DiskCopy (fileSystem,decode(status,10,status,NULL));

CREATE INDEX I_TapeCopy_Castorfile on TapeCopy (castorFile);
CREATE INDEX I_TapeCopy_Status on TapeCopy (status);
CREATE INDEX I_TapeCopy_CF_Status_2 on TapeCopy (castorFile,decode(status,2,status,null));

CREATE INDEX I_FileSystem_DiskPool on FileSystem (diskPool);
CREATE INDEX I_FileSystem_DiskServer on FileSystem (diskServer);

CREATE INDEX I_SubRequest_Castorfile on SubRequest (castorFile);
CREATE INDEX I_SubRequest_DiskCopy on SubRequest (diskCopy);
CREATE INDEX I_SubRequest_Request on SubRequest (request);
CREATE INDEX I_SubRequest_Parent on SubRequest (parent);
CREATE INDEX I_SubRequest_SubReqId on SubRequest (subReqId);

CREATE INDEX I_StagePTGRequest_ReqId on StagePrepareToGetRequest (reqId);
CREATE INDEX I_StagePTPRequest_ReqId on StagePrepareToPutRequest (reqId);
CREATE INDEX I_StagePTURequest_ReqId on StagePrepareToUpdateRequest (reqId);
CREATE INDEX I_StageGetRequest_ReqId on StageGetRequest (reqId);
CREATE INDEX I_StagePutRequest_ReqId on StagePutRequest (reqId);
CREATE INDEX I_StageRepackRequest_ReqId on StageRepackRequest (reqId);

/* A primary key index for better scan of Stream2TapeCopy */
CREATE UNIQUE INDEX I_pk_Stream2TapeCopy ON Stream2TapeCopy (parent, child);

/* Some index on the GCFile table to speed up garbage collection */
CREATE INDEX I_GCFILE_REQUEST ON GCFILE(REQUEST);

/* Indexing segments by Tape */
CREATE INDEX I_SEGMENT_TAPE ON SEGMENT (TAPE);

/* some constraints */
ALTER TABLE FileSystem ADD CONSTRAINT diskserver_fk FOREIGN KEY (diskServer) REFERENCES DiskServer(id);
ALTER TABLE FileSystem MODIFY (status NOT NULL);
ALTER TABLE FileSystem MODIFY (diskServer NOT NULL);
ALTER TABLE DiskServer MODIFY (status NOT NULL);
ALTER TABLE SvcClass MODIFY (name NOT NULL);

/* an index to speed up queries in FileQueryRequest, FindRequestRequest, RequestQueryRequest */
CREATE INDEX I_QueryParameter_Query on QueryParameter (query);

/* an index to speed the queries on Segments by copy */
CREATE INDEX I_Segment_Copy on Segment (copy);

/* Constraint on FileClass name */
ALTER TABLE FileClass ADD CONSTRAINT I_FileClass_Name UNIQUE (name); 

/* Add unique constraint on tapes */
ALTER TABLE Tape ADD CONSTRAINT I_Tape_VIDSideTpMode UNIQUE (VID, side, tpMode);

/* Add unique constraint on svcClass name */
ALTER TABLE SvcClass ADD CONSTRAINT I_SvcClass_Name UNIQUE (NAME); 

/* the primary key in this table allows for materialized views */
ALTER TABLE DiskPool2SvcClass ADD CONSTRAINT I_DiskPool2SvcCla_ParentChild PRIMARY KEY (parent, child);


/* get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
  ret NUMBER;
BEGIN
  SELECT (SYSDATE - to_date('01-jan-1970 01:00:00','dd-mon-yyyy HH:MI:SS')) * (24*60*60) INTO ret FROM DUAL;
  RETURN ret;
END;

/* Global temporary table to handle output of the filesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE FilesDeletedProcOutput
  (fileid NUMBER, nshost VARCHAR2(2048))
  ON COMMIT PRESERVE ROWS;

/* Global temporary table to help selectFiles2Delete procedure */
CREATE GLOBAL TEMPORARY TABLE SelectFiles2DeleteProcHelper
  (id NUMBER, path VARCHAR2(2048))
  ON COMMIT DELETE ROWS;

/* Global temporary tables for the cleanup procedures */
CREATE GLOBAL TEMPORARY TABLE ArchivedRequestCleaning
  (id NUMBER NOT NULL ENABLE, type NUMBER NOT NULL ENABLE)
  ON COMMIT PRESERVE ROWS;

CREATE GLOBAL TEMPORARY TABLE OutOfDateRequestCleaning
  (id NUMBER NOT NULL ENABLE, type NUMBER NOT NULL ENABLE)
  ON COMMIT PRESERVE ROWS;

/**
  * Black and while list mechanism
  * In order to be able to enter a request for a given service class, you need :
  *   - to be in the white list for this service class
  *   - to not be in the black list for this services class
  * Being in a list means :
  *   - either that your uid,gid is explicitely in the list
  *   - or that your gid is in the list with null uid (that is group wildcard)
  *   - or there is an entry with null uid and null gid (full wild card)
  * The permissions can also have a request type. Default is null, that is everything.
  * By default anybody can do anything
  */
CREATE TABLE WhiteList (svcClass VARCHAR2(2048), euid NUMBER, egid NUMBER, reqType NUMBER);
CREATE TABLE BlackList (svcClass VARCHAR2(2048), euid NUMBER, egid NUMBER, reqType NUMBER);
BEGIN
  FOR sc IN (SELECT name FROM SvcClass) LOOP
    INSERT INTO WhiteList VALUES (sc.name, NULL, NULL, NULL);
  END LOOP;
  -- extra one specific to queries on all service classes
  INSERT INTO WhiteList VALUES ('*', NULL, NULL, NULL);  
  COMMIT;
END;


/****************************************************************/
/* NbTapeCopiesInFS to work around ORACLE missing optimizations */
/****************************************************************/

/* This table keeps track of the number of TapeCopy waiting for migration
 * which have a diskCopy on this fileSystem. This table only exist
 * because Oracle is not able to optimize the following query :
 * SELECT max(A) from A, B where A.pk = B.fk;
 * Such a query is needed in bestTapeCopyForStream in order to
 * select filesystems effectively having tapecopies.
 * As a work around, we keep track of the tapecopies for each
 * filesystem. The cost is an increase of complexity and especially
 * of the number of triggers ensuring consistency of the whole database */
CREATE TABLE NbTapeCopiesInFS (FS NUMBER, Stream NUMBER, NbTapeCopies NUMBER);
CREATE UNIQUE INDEX I_NbTapeCopiesInFS_FSStream on NbTapeCopiesInFS(FS, Stream);
CREATE INDEX I_NbTapeCopiesInFS_Stream on NbTapeCopiesInFS(Stream);


/*******************************************************************/
/* LockTable to implement proper locking for bestTapeCopyForStream */
/*******************************************************************/

/* This table is needed to insure that bestTapeCopyForStream works Ok.
 * It basically serializes the queries ending to the same diskserver.
 * This is only needed because of lack of funstionnality in ORACLE.
 * The original goal was to lock the selected filesystem in the first
 * query of bestTapeCopyForStream. But a SELECT FOR UPDATE was not enough
 * because it does not revalidate the inner select and we were thus selecting
 * n times the same filesystem when n queries were processed in parallel.
 * (take care, they were processed sequentially due to the lock, but they
 * were still locking the same filesystem). Thus an UPDATE was needed but
 * UPDATE cannot use joins. Thus it was impossible to lock DiskServer
 * and FileSystem at the same time (we need to avoid the DiskServer to
 * be chosen again (with a different filesystem) before we update the
 * weight of all filesystems). Locking the diskserver only was fine but
 * was introducing a possible deadlock with a place where the FileSystem
 * is locked before the DiskServer. Thus this table..... */
CREATE TABLE LockTable (DiskServerId NUMBER PRIMARY KEY, TheLock NUMBER);
INSERT INTO LockTable SELECT id, id FROM DiskServer;


/*********************/
/* FileSystem rating */
/*********************/

/* Computes a 'rate' for the filesystem which is an agglomeration
   of weight and fsDeviation. The goal is to be able to classify
   the fileSystems using a single value and to put an index on it */
CREATE OR REPLACE FUNCTION FileSystemRate
(readRate IN NUMBER,
 writeRate IN NUMBER,
 nbReadStreams IN NUMBER,
 nbWriteStreams IN NUMBER,
 nbReadWriteStreams IN NUMBER,
 nbMigratorStreams IN NUMBER,
 nbRecallerStreams IN NUMBER)
RETURN NUMBER DETERMINISTIC IS
BEGIN
  RETURN - nbReadStreams - nbWriteStreams - nbReadWriteStreams - nbMigratorStreams - nbRecallerStreams;
END;


/* FileSystem index based on the rate. */
CREATE INDEX I_FileSystem_Rate
    ON FileSystem(FileSystemRate(readRate, writeRate,
	          nbReadStreams,nbWriteStreams, nbReadWriteStreams, nbMigratorStreams, nbRecallerStreams));


/*******************************************/
/*                                         */
/*   Triggers and Procedures definitions   */
/*                                         */
/*******************************************/

/* PL/SQL method deleting tapecopies (and segments) of a castorfile */
CREATE OR REPLACE PROCEDURE deleteTapeCopies(cfId NUMBER) AS
BEGIN
  -- loop over the tapecopies
  FOR t IN (SELECT id FROM TapeCopy WHERE castorfile = cfId) LOOP
    FOR s IN (SELECT id FROM Segment WHERE copy = t.id) LOOP
    -- Delete the segment(s)
      DELETE FROM Id2Type WHERE id = s.id;
      DELETE FROM Segment WHERE id = s.id;
    END LOOP;
    -- Delete from Stream2TapeCopy
    DELETE FROM Stream2TapeCopy WHERE child = t.id;
    -- Delete the TapeCopy
    DELETE FROM Id2Type WHERE id = t.id;
    DELETE FROM TapeCopy WHERE id = t.id;
  END LOOP;
END;


/* Checks consistency of DiskCopies when a FileSystem comes
 * back in production after a period spent in DRAINING or
 * DISABLED status.
 * Current checks/fixes include :
 *   - canceling recalls for files that are STAGED/CANBEMIGR
 *     on the fileSystem that comes back.
 */
CREATE OR REPLACE PROCEDURE checkFSBackInProd(fsId NUMBER) AS
  srId NUMBER;
BEGIN
  -- Look for recalls concerning files that are STAGED/CANBEMIGR
  -- on the filesystem coming back to life
  FOR cf IN (SELECT UNIQUE d.castorfile, e.id
               FROM DiskCopy d, DiskCopy e 
              WHERE d.castorfile = e.castorfile
                AND d.fileSystem = fsId
                AND d.status IN (0, 10)
                AND e.status = 2) LOOP
    -- cancel the recall
    deleteTapeCopies(cf.castorfile);
    -- Delete the DiskCopy
    UPDATE DiskCopy
       SET status = 7  -- INVALID
     WHERE id = cf.castorfile;
    -- Look for request associated to the recall and restart
    -- it and all the waiting ones
    UPDATE SubRequest SET status = 1 -- RESTART
     WHERE diskCopy = cf.id RETURNING id INTO srId;
    UPDATE SubRequest
       SET status = 1, parent = 0 -- RESTART
     WHERE status = 5 -- WAITSUBREQ
       AND parent = srId
       AND castorfile = cf.castorfile;
  END LOOP;
END;


/* Used to create a row INTO NbTapeCopiesInFS whenever a new
   FileSystem is created */
CREATE OR REPLACE TRIGGER tr_FileSystem_Insert
BEFORE INSERT ON FileSystem
FOR EACH ROW
BEGIN
  FOR item in (SELECT id FROM Stream) LOOP
    INSERT INTO NbTapeCopiesInFS (FS, Stream, NbTapeCopies) VALUES (:new.id, item.id, 0);
  END LOOP;
END;


/* Used to delete rows IN NbTapeCopiesInFS whenever a
   FileSystem is deleted */
CREATE OR REPLACE TRIGGER tr_FileSystem_Delete
BEFORE DELETE ON FileSystem
FOR EACH ROW
BEGIN
  DELETE FROM NbTapeCopiesInFS WHERE FS = :old.id;
END;


/* Used to check consistency of diskcopies when a filesytem
   comes back to production after having been disabled for
   some time */
CREATE OR REPLACE TRIGGER tr_FileSystem_Update
BEFORE UPDATE of status ON FileSystem
FOR EACH ROW
WHEN (old.status IN (1, 2) AND -- DRAINING, DISABLED
      new.status = 0) -- PRODUCTION
DECLARE
  s NUMBER;
BEGIN
  SELECT status INTO s FROM DiskServer WHERE id = :old.diskServer;
  IF (s = 0) THEN
    checkFSBackInProd(:old.id);
  END IF;
END;


/* Used to create a row INTO NbTapeCopiesInFS whenever a new
   Stream is created */
CREATE OR REPLACE TRIGGER tr_Stream_Insert
BEFORE INSERT ON Stream
FOR EACH ROW
BEGIN
  FOR item in (SELECT id FROM FileSystem) LOOP
    INSERT INTO NbTapeCopiesInFS (FS, Stream, NbTapeCopies) VALUES (item.id, :new.id, 0);
  END LOOP;
END;


/* Used to delete rows IN NbTapeCopiesInFS whenever a
   Stream is deleted */
CREATE OR REPLACE TRIGGER tr_Stream_Delete
BEFORE DELETE ON Stream
FOR EACH ROW
BEGIN
  DELETE FROM NbTapeCopiesInFS WHERE Stream = :old.id;
END;


/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a TapeCopy is linked to a Stream */
CREATE OR REPLACE TRIGGER tr_stream2tapecopy_insert
AFTER INSERT ON STREAM2TAPECOPY
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
  cfSize NUMBER;
BEGIN
  -- added this lock because of several copies of different file systems
  -- from different streams which can cause deadlock
  LOCK TABLE NbTapeCopiesInFS IN ROW SHARE MODE;
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies + 1
   WHERE FS IN (SELECT DiskCopy.FileSystem
                  FROM DiskCopy, TapeCopy
                 WHERE DiskCopy.CastorFile = TapeCopy.castorFile
                   AND TapeCopy.id = :new.child
                   AND DiskCopy.status = 10) -- CANBEMIGR
     AND Stream = :new.parent;
END;


/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a TapeCopy is unlinked from a Stream */
CREATE OR REPLACE TRIGGER tr_stream2tapecopy_delete
BEFORE DELETE ON STREAM2TAPECOPY
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE cfSize NUMBER;
BEGIN  
  -- added this lock because of several copies of different file systems 
  -- from different streams which can cause deadlock
  LOCK TABLE NbTapeCopiesInFS IN ROW SHARE MODE;
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies - 1
   WHERE FS IN (SELECT DiskCopy.FileSystem
                  FROM DiskCopy, TapeCopy
                 WHERE DiskCopy.CastorFile = TapeCopy.castorFile
                   AND TapeCopy.id = :old.child
                   AND DiskCopy.status = 10) -- CANBEMIGR
     AND Stream = :old.parent;
END;


/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a DiskCopy has been replicated and the new one
   is put into CANBEMIGR status from the
   WAITDISK2DISKCOPY status */
CREATE OR REPLACE TRIGGER tr_DiskCopy_Update
AFTER UPDATE of status ON DiskCopy
FOR EACH ROW
WHEN (old.status = 1 AND -- WAITDISK2DISKCOPY
      new.status = 10) -- CANBEMIGR
BEGIN
-- added this lock because of severaval copies of different file systems 
--  from different streams which can cause deadlock
  LOCK TABLE  NbTapeCopiesInFS IN ROW SHARE MODE;
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies + 1
   WHERE FS = :new.fileSystem
     AND Stream IN (SELECT Stream2TapeCopy.parent
                      FROM Stream2TapeCopy, TapeCopy
                     WHERE TapeCopy.castorFile = :new.castorFile
                       AND Stream2TapeCopy.child = TapeCopy.id
                       AND TapeCopy.status = 2); -- WAITINSTREAMS
END;


/***************************************/
/* Some triggers to prevent dead locks */
/***************************************/

/* Used to avoid LOCK TABLE TapeCopy whenever someone wants
   to deal with the tapeCopies on a CastorFile.
   Due to this trigger, locking the CastorFile is enough
   to be safe */
CREATE OR REPLACE TRIGGER tr_TapeCopy_CastorFile
BEFORE INSERT OR UPDATE OF castorFile ON TapeCopy
FOR EACH ROW WHEN (new.castorFile > 0)
DECLARE
  unused CastorFile%ROWTYPE;
BEGIN
  SELECT * INTO unused FROM CastorFile
   WHERE id = :new.castorFile FOR UPDATE;
END;


/* Used to avoid LOCK TABLE TapeCopy whenever someone wants
   to deal with the tapeCopies on a CastorFile.
   Due to this trigger, locking the CastorFile is enough
   to be safe */
CREATE OR REPLACE TRIGGER tr_DiskCopy_CastorFile
BEFORE INSERT OR UPDATE OF castorFile ON DiskCopy
FOR EACH ROW WHEN (new.castorFile > 0)
DECLARE
  unused CastorFile%ROWTYPE;
BEGIN
  SELECT * INTO unused FROM CastorFile
   WHERE id = :new.castorFile FOR UPDATE;
END;


/* PL/SQL method to get the next SubRequest to do according to the given service */
/* the service parameter is not used now, it will with the new stager */
CREATE OR REPLACE PROCEDURE subRequestToDo(service IN VARCHAR2,
                                           srId OUT INTEGER, srRetryCounter OUT INTEGER, srFileName OUT VARCHAR2,
                                           srProtocol OUT VARCHAR2, srXsize OUT INTEGER, srPriority OUT INTEGER,
                                           srStatus OUT INTEGER, srModeBits OUT INTEGER, srFlags OUT INTEGER,
                                           srSubReqId OUT VARCHAR2) AS
 firstRow VARCHAR2(18);
 CURSOR c IS
  SELECT rowidtochar(rowid) FROM SubRequest                                                                                  
   WHERE status in (0,1,2)    -- START, RESTART, RETRY                                                                       
     AND EXISTS                                                                                                              
       (SELECT /*+ index(a I_Id2Type_id) */ 'x'                                                                                
         FROM Id2Type a 
        WHERE a.type in (35,36,37,38,39,40,42,44,95,119)                                                                       
          AND a.id = SubRequest.request);
        --AND Id2Type.type = Type2Obj.type
        --AND Type2Obj.svcHandler = service;
BEGIN
  OPEN c;
  FETCH c INTO firstRow;
  UPDATE subrequest SET status = 3 WHERE rowid = CHARTOROWID(firstRow)   -- SUBREQUEST_WAITSCHED
    RETURNING id, retryCounter, fileName, protocol, xsize, priority, status, modeBits, flags, subReqId
    INTO srId, srRetryCounter, srFileName, srProtocol, srXsize, srPriority, srStatus, srModeBits, srFlags, srSubReqId;
  CLOSE c;
END;


/* PL/SQL method to get the next failed SubRequest to do according to the given service */
/* the service parameter is not used now, it will with the new stager */
CREATE OR REPLACE PROCEDURE subRequestFailedToDo(srId OUT INTEGER, srRetryCounter OUT INTEGER, srFileName OUT VARCHAR2,
                                                 srProtocol OUT VARCHAR2, srXsize OUT INTEGER, srPriority OUT INTEGER,
                                                 srStatus OUT INTEGER, srModeBits OUT INTEGER, srFlags OUT INTEGER,
                                                 srSubReqId OUT VARCHAR2, srErrorCode OUT NUMBER,
                                                 srErrorMessage OUT VARCHAR2) AS
 firstRow VARCHAR2(18);
 CURSOR c IS
  SELECT rowidtochar(rowid) FROM SubRequest WHERE status = 7;
BEGIN
  OPEN c;
  FETCH c INTO firstRow;
  UPDATE subrequest SET status = 10 WHERE rowid = CHARTOROWID(firstRow)   -- SUBREQUEST_FAILED_ANSWERING
    RETURNING id, retryCounter, fileName, protocol, xsize, priority, status,
              modeBits, flags, subReqId, errorCode, errorMessage
    INTO srId, srRetryCounter, srFileName, srProtocol, srXsize, srPriority, srStatus,
              srModeBits, srFlags, srSubReqId, srErrorCode, srErrorMessage;
  CLOSE c;
END;


/* PL/SQL method to get the next request to do according to the given service */
/*
PROCEDURE requestToDo(service IN VARCHAR2, rId OUT INTEGER) AS
BEGIN
 DELETE FROM NewRequests 
  WHERE type IN (
    SELECT Id2Type.type FROM Id2Type, Type2Obj
     WHERE Id2Type.type = Type2Obj.type
       AND Type2Obj.svcHandler = service
    )
  AND ROWNUM < 2 RETURNING id INTO rId;
END;
*/

/* PL/SQL method to make a SubRequest wait on another one, linked to the given DiskCopy */
CREATE OR REPLACE PROCEDURE makeSubRequestWait(srId IN INTEGER, dci IN INTEGER) AS
BEGIN
  -- all wait on the original one
  UPDATE SubRequest
     SET parent = (SELECT SubRequest.id
                     FROM SubRequest, DiskCopy
                    WHERE SubRequest.diskCopy = DiskCopy.id
                      AND DiskCopy.id = dci
                      AND SubRequest.parent = 0
                      AND DiskCopy.status IN (1, 2, 5, 6, 11) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT, WAITFS_SCHEDULING
                      AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6)), -- START, RESTART, RETRY, WAIT*, READY
        status = 5, -- WAITSUBREQ
        lastModificationTime = getTime()
  WHERE SubRequest.id = srId;
END;


/*  PL/SQL method to archive a SubRequest */
CREATE OR REPLACE PROCEDURE archiveSubReq(srId IN INTEGER) AS
  rid INTEGER;
  nb INTEGER;
  cfId NUMBER;
BEGIN
  UPDATE SubRequest
     SET status = 8, parent = 0 -- FINISHED
   WHERE id = srId
  RETURNING request, castorFile INTO rid, cfId;

  -- Try to see whether another subrequest in the same
  -- request is still processing
  SELECT count(*) INTO nb FROM SubRequest
   WHERE request = rid AND status <> 8;  -- all but FINISHED

  -- Archive request if all subrequests have finished
  IF nb = 0 THEN
    UPDATE SubRequest SET status=11 WHERE request=rid and status=8;  -- ARCHIVED 
  END IF;

  -- Check that we don't have too many requests for the file in the DB
  SELECT count(request) INTO nb FROM SubRequest WHERE castorFile = cfId AND status IN (9, 11);
  IF nb > 100  THEN
    FOR sr IN (SELECT request INTO rid
                 FROM (SELECT request FROM SubRequest
                        WHERE castorFile = cfId AND status IN (9, 11)
                        ORDER BY creationTime ASC)
                WHERE ROWNUM < 2) LOOP
      deleteRequest(rid);
    END LOOP;
  END IF;
END;


/* PL/SQL method to delete one single request */
CREATE OR REPLACE PROCEDURE deleteRequest(rId IN INTEGER) AS
  rtype INTEGER;
  rclient INTEGER;
BEGIN  -- delete Request, Client and SubRequests
  -- delete request from Id2Type
  DELETE FROM Id2Type WHERE id = rId RETURNING type INTO rtype;
 
  -- delete request and get client id
  IF rtype = 35 THEN -- StageGetRequest
    DELETE FROM StageGetRequest WHERE id = rId RETURNING client into rclient;
  ELSIF rtype = 40 THEN -- StagePutRequest
    DELETE FROM StagePutRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 44 THEN -- StageUpdateRequest
    DELETE FROM StageUpdateRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 39 THEN -- StagePutDoneRequest
    DELETE FROM StagePutDoneRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 42 THEN -- StageRmRequest
    DELETE FROM StageRmRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 51 THEN -- StageReleaseFilesRequest
    DELETE FROM StageReleaseFilesRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 36 THEN -- StagePrepareToGetRequest
    DELETE FROM StagePrepareToGetRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 37 THEN -- StagePrepareToPutRequest
    DELETE FROM StagePrepareToPutRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 38 THEN -- StagePrepareToUpdateRequest
    DELETE FROM StagePrepareToUpdateRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 119 THEN -- StageRepackRequest
    DELETE FROM StageRepackRequest WHERE id = rId RETURNING client INTO rclient;
  END IF;

  -- Delete Client
  DELETE FROM Id2Type WHERE id = rclient;
  DELETE FROM Client WHERE id = rclient;
  
  -- Delete SubRequests
  DELETE FROM Id2Type WHERE id IN
    (SELECT id FROM SubRequest WHERE request = rId);
  DELETE FROM SubRequest WHERE request = rId;
END;


/* A little generic method to delete efficiently */
CREATE OR REPLACE PROCEDURE bulkDelete(sel IN VARCHAR2, tab IN VARCHAR2) AS
BEGIN
  EXECUTE IMMEDIATE
  'DECLARE
    cursor s IS '||sel||'
    ids "numList";
  BEGIN
    OPEN s;
    LOOP
      FETCH s BULK COLLECT INTO ids LIMIT 10000;
      DELETE FROM '||tab||' WHERE id in (SELECT * FROM TABLE(ids));
      COMMIT;
      EXIT WHEN s%NOTFOUND;
    END LOOP;
  END;';
END;


/* PL/SQL method to delete a group of requests from  */
CREATE OR REPLACE PROCEDURE deleteRequests
  (tab IN VARCHAR2, typ IN VARCHAR2, cleanTab IN VARCHAR2) AS
BEGIN
  -- delete client id2type
  bulkDelete(
    'SELECT client FROM '||tab||', '||cleanTab||'
       WHERE '||tab||'.id = '||cleanTab||'.id;',
    'Id2Type');
  -- delete client itself
  bulkDelete(
    'SELECT client FROM '||tab||', '||cleanTab||'
       WHERE '||tab||'.id = '||cleanTab||'.id;',
    'Client');
  -- delete request id2type
  bulkDelete(
    'SELECT id FROM '||cleanTab||' WHERE type = '||typ||';',
    'Id2Type');
  -- delete request itself
  bulkDelete(
    'SELECT id FROM '||cleanTab||' WHERE type = '||typ||';',
    tab);
END;


/* internal procedure to efficiently delete all requests in the cleanTab table */
CREATE OR REPLACE PROCEDURE internalCleaningProc(cleanTab IN VARCHAR) AS
BEGIN
  -- Delete SubRequests
    -- Starting with id
  bulkDelete(
     'SELECT SubRequest.id FROM SubRequest, '||cleanTab||'
       WHERE SubRequest.request = '||cleanTab||'.id;',
     'Id2Type');
    -- Then the subRequests
  bulkDelete(
     'SELECT SubRequest.id FROM SubRequest, '||cleanTab||'
       WHERE SubRequest.request = '||cleanTab||'.id;',
     'SubRequest');
  -- Delete Request + Clients 
    ---- Get ----
  deleteRequests('StageGetRequest', '35', cleanTab);
    ---- Put ----
  deleteRequests('StagePutRequest', '40', cleanTab);
    ---- Update ----
  deleteRequests('StageUpdateRequest', '44', cleanTab);
    ---- Rm ----
  deleteRequests('StageRmRequest', '42', cleanTab);
    ---- PutDone ----
  deleteRequests('StagePutDoneRequest', '39', cleanTab);
    ---- PrepareToGet -----
  deleteRequests('StagePrepareToGetRequest', '36', cleanTab);
    ---- PrepareToPut ----
  deleteRequests('StagePrepareToPutRequest', '37', cleanTab);
  EXECUTE IMMEDIATE 'TRUNCATE TABLE '||cleanTab;
END;


/* Search and delete too old archived subrequests and their requests */
CREATE OR REPLACE PROCEDURE deleteArchivedRequests(timeOut IN NUMBER) AS
BEGIN
  INSERT /*+ APPEND */ INTO ArchivedRequestCleaning
    SELECT UNIQUE request, type 
      FROM SubRequest, id2type
     WHERE subrequest.request = id2type.id
     GROUP BY request, type
    HAVING min(status) = 11
       AND max(status) = 11 
       AND max(lastModificationTime) < getTime() - timeOut;
  COMMIT;
  internalCleaningProc('ArchivedRequestCleaning');
END;

/* Search and delete "out of date" subrequests and their requests */
CREATE OR REPLACE PROCEDURE deleteOutOfDateRequests(timeOut IN NUMBER) AS
BEGIN
  INSERT /*+ APPEND */ INTO OutOfDateRequestCleaning
    SELECT UNIQUE request, type 
      FROM SubRequest, id2type
     WHERE subrequest.request = id2type.id
     GROUP BY request, type
    HAVING min(status) >= 8
       AND max(status) <= 11 
       AND max(lastModificationTime) < getTime() - timeOut;
  COMMIT;
  internalCleaningProc('OutOfDateRequestCleaning');
END;


/* PL/SQL method implementing anyTapeCopyForStream.
 * This implementation is not the original one. It uses NbTapeCopiesInFS
 * because a join on the tables between DiskServer and Stream2TapeCopy
 * costs too much. It should actually not be the case but ORACLE is unable
 * to optimize correctly queries having a ROWNUM clause. It processes the
 * the query without it (yes !!!) and applies the clause afterwards.
 * Here is the previous select in case ORACLE improves some day :
 * SELECT \/*+ FIRST_ROWS *\/ TapeCopy.id INTO unused
 * FROM DiskServer, FileSystem, DiskCopy, CastorFile, TapeCopy, Stream2TapeCopy
 *  WHERE DiskServer.id = FileSystem.diskserver
 *   AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
 *   AND FileSystem.id = DiskCopy.filesystem
 *   AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
 *   AND DiskCopy.castorfile = CastorFile.id
 *   AND TapeCopy.castorfile = Castorfile.id
 *   AND Stream2TapeCopy.child = TapeCopy.id
 *   AND Stream2TapeCopy.parent = streamId
 *   AND TapeCopy.status = 2 -- WAITINSTREAMS
 *   AND ROWNUM < 2;  */
CREATE OR REPLACE PROCEDURE anyTapeCopyForStream(streamId IN INTEGER, res OUT INTEGER) AS
  unused INTEGER;
BEGIN
  SELECT NbTapeCopiesInFS.NbTapeCopies INTO unused
    FROM NbTapeCopiesInFS, FileSystem, DiskServer
   WHERE NbTapeCopiesInFS.stream = streamId
     AND NbTapeCopiesInFS.NbTapeCopies > 0
     AND FileSystem.id = NbTapeCopiesInFS.FS
     AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
     AND DiskServer.id = FileSystem.DiskServer
     AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
     AND ROWNUM < 2;
  res := 1;
EXCEPTION
 WHEN NO_DATA_FOUND THEN
  res := 0;
END;


/* PL/SQL method to update FileSystem weight for new streams */
CREATE OR REPLACE PROCEDURE updateFsFileOpened
(ds IN INTEGER, fs IN INTEGER, fileSize IN INTEGER) AS
BEGIN
  /* We lock first the diskserver in order to lock all the
     filesystems of this DiskServer in an atomical way */
  UPDATE DiskServer SET nbReadWriteStreams = nbReadWriteStreams + 1 WHERE id = ds;
  UPDATE FileSystem SET nbReadWriteStreams = nbReadWriteStreams + 1,
                        free = free - fileSize   -- just an evaluation, monitoring will update it
   WHERE id = fs;
END;


CREATE OR REPLACE PROCEDURE updateFsMigratorOpened
(ds IN INTEGER, fs IN INTEGER, fileSize IN INTEGER) AS
BEGIN
  /* We lock first the diskserver in order to lock all the
     filesystems of this DiskServer in an atomical way */
  UPDATE DiskServer SET nbMigratorStreams = nbMigratorStreams + 1 WHERE id = ds;
  UPDATE FileSystem SET nbMigratorStreams = nbMigratorStreams + 1 WHERE id = fs;
END;


CREATE OR REPLACE PROCEDURE updateFsRecallerOpened
(ds IN INTEGER, fs IN INTEGER, fileSize IN INTEGER) AS
BEGIN
  /* We lock first the diskserver in order to lock all the
     filesystems of this DiskServer in an atomical way */
  UPDATE DiskServer SET nbRecallerStreams = nbRecallerStreams + 1 WHERE id = ds;
  UPDATE FileSystem SET nbRecallerStreams = nbRecallerStreams + 1,
                        free = free - fileSize   -- just an evaluation, monitoring will update it
   WHERE id = fs;
END;


/* PL/SQL method to update FileSystem nb of streams when files are closed */
CREATE OR REPLACE PROCEDURE updateFsFileClosed(fs IN INTEGER) AS
  ds INTEGER;
BEGIN
  /* We lock first the diskserver in order to lock all the
     filesystems of this DiskServer in an atomical way */
  SELECT DiskServer INTO ds FROM FileSystem WHERE id = fs;
  UPDATE DiskServer SET nbReadWriteStreams = decode(sign(nbReadWriteStreams-1),-1,0,nbReadWriteStreams-1) WHERE id = ds;
  /* now we can safely go */
  UPDATE FileSystem SET nbReadWriteStreams = decode(sign(nbReadWriteStreams-1),-1,0,nbReadWriteStreams-1)
  WHERE id = fs;
END;


/* Used to create a row INTO LockTable whenever a new
   DiskServer is created */
CREATE OR REPLACE TRIGGER tr_DiskServer_Insert
BEFORE INSERT ON DiskServer
FOR EACH ROW
BEGIN
  INSERT INTO LockTable (DiskServerId, TheLock) VALUES (:new.id, 0);
END;


/* Used to delete rows IN LockTable whenever a
   DiskServer is deleted */
CREATE OR REPLACE TRIGGER tr_DiskServer_Delete
BEFORE DELETE ON DiskServer
FOR EACH ROW
BEGIN
  DELETE FROM LockTable WHERE DiskServerId = :old.id;
END;


/* Used to check consistency of diskcopies when filesytems
   comes back to production after having been disabled for
   some time */
CREATE OR REPLACE TRIGGER tr_DiskServer_Update
BEFORE UPDATE of status ON DiskServer
FOR EACH ROW
WHEN (old.status IN (1, 2) AND -- DRAINING, DISABLED
      new.status = 0) -- PRODUCTION
BEGIN
  FOR fs IN (SELECT id, status FROM FileSystem WHERE diskServer = :old.id) LOOP
    IF (fs.status = 0) THEN
      checkFSBackInProd(fs.id);
    END IF;
  END LOOP;
END;


/* PL/SQL method implementing updateFileSystemForJob */
CREATE OR REPLACE PROCEDURE updateFileSystemForJob
(fs IN VARCHAR2, ds IN VARCHAR2,
 fileSize IN NUMBER) AS
  fsID NUMBER;
  dsId NUMBER;
  unused NUMBER;
BEGIN
  SELECT FileSystem.id, DiskServer.id INTO fsId, dsId
    FROM FileSystem, DiskServer
   WHERE FileSystem.diskServer = DiskServer.id
     AND FileSystem.mountPoint = fs
     AND DiskServer.name = ds;
  -- We have to lock the DiskServer in the LockTable TABLE if we want
  -- to avoid dead locks with bestTapeCopyForStream. See the definition
  -- of the table for a complete explanation on why it exists
  SELECT TheLock INTO unused FROM LockTable WHERE DiskServerId = dsId FOR UPDATE;
  updateFsFileOpened(dsId, fsId, fileSize);
END;


/* PL/SQL method implementing bestTapeCopyForStream */
CREATE OR REPLACE PROCEDURE bestTapeCopyForStream(streamId IN INTEGER,
                                                  diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                  path OUT VARCHAR2, dci OUT INTEGER,
                                                  castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                  nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                  tapeCopyId OUT INTEGER, optimized IN INTEGER) AS
  fileSystemId INTEGER := 0;
  dsid NUMBER;
  fsDiskServer NUMBER;
  lastFSChange NUMBER;
  lastFSUsed NUMBER;
  lastButOneFSUsed NUMBER;
  findNewFS NUMBER := 1;
  nbMigrators NUMBER := 0;
BEGIN
  -- First try to see whether we should resuse the same filesystem as last time
  SELECT lastFileSystemChange, lastFileSystemUsed, lastButOneFileSystemUsed
    INTO lastFSChange, lastFSUsed, lastButOneFSUsed
    FROM Stream WHERE id = streamId;
  IF getTime() < lastFSChange + 900 THEN
    SELECT (SELECT count(*) FROM stream WHERE lastFileSystemUsed = lastButOneFSUsed) +
           (SELECT count(*) FROM stream WHERE lastButOneFileSystemUsed = lastButOneFSUsed)
      INTO nbMigrators FROM DUAL;
    -- only go if we are the only migrator on the box
    IF nbMigrators = 1 THEN
      BEGIN
        -- check states of the diskserver and filesystem and get mountpoint and diskserver name
	SELECT name, mountPoint, FileSystem.id INTO diskServerName, mountPoint, fileSystemId
          FROM FileSystem, DiskServer
         WHERE FileSystem.diskServer = DiskServer.id
           AND FileSystem.id = lastButOneFSUsed
           AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
           AND DiskServer.status IN (0, 1); -- PRODUCTION, DRAINING
        -- we are within the time range, so we try to reuse the filesystem
        SELECT P.path, P.diskcopy_id, P.castorfile,
             C.fileId, C.nsHost, C.fileSize, P.id
        INTO path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId
        FROM (SELECT /*+ ORDERED USE_NL(D T) INDEX(T I_TAPECOPY_CF_STATUS_2) INDEX(ST I_PK_STREAM2TAPECOPY) */
              D.path, D.diskcopy_id, D.castorfile, T.id
                FROM (SELECT /*+ INDEX(DK I_DISKCOPY_FS_STATUS_10) */
                             DiskCopy.path path, DiskCopy.id diskcopy_id, DiskCopy.castorfile
                        FROM DiskCopy
                       WHERE decode(DiskCopy.status,10,DiskCopy.status,null) = 10 -- CANBEMIGR
                         AND DiskCopy.filesystem = lastButOneFSUsed) D,
                      TapeCopy T, Stream2TapeCopy ST
               WHERE T.castorfile = D.castorfile
                 AND ST.child = T.id
                 AND ST.parent = streamId
                 AND decode(T.status,2,T.status,null) = 2 -- WAITINSTREAMS
                 AND ROWNUM < 2) P, castorfile C
         WHERE P.castorfile = C.id;
        -- we found one, no need to go for new filesystem
        findNewFS := 0;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- found no tapecopy or diskserver, filesystem are down. We'll go through the normal selection
        NULL;
      END;
    END IF;
  END IF;
  IF findNewFS = 1 THEN
    -- We lock here a given DiskServer. See the comment for the creation of the LockTable
    -- table for a full explanation of why we need such a stupid UPDATE statement
    UPDATE LockTable SET theLock = 1
     WHERE diskServerId = (
       SELECT DS.diskserver_id
         FROM (
           -- The double level of selects is due to the fact that ORACLE is unable
           -- to use ROWNUM and ORDER BY at the same time. Thus, we have to first compute
           -- the maxRate and then select on it.
           SELECT diskserver.id diskserver_id
             FROM FileSystem FS, NbTapeCopiesInFS, DiskServer
            WHERE FS.id = NbTapeCopiesInFS.FS
              AND NbTapeCopiesInFS.NbTapeCopies > 0
              AND NbTapeCopiesInFS.Stream = StreamId
              AND FS.status IN (0, 1)         -- PRODUCTION, DRAINING
              AND DiskServer.id = FS.diskserver
              AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
              -- Ignore diskservers where a migrator already exists
              AND DiskServer.id NOT IN (
                SELECT DISTINCT(FileSystem.diskServer)
                  FROM FileSystem, Stream
                 WHERE FileSystem.id = Stream.lastfilesystemused
                   AND Stream.status IN (3)   -- SELECTED
                   AND optimized = 1
              )
            ORDER BY FileSystemRate(FS.readRate, FS.writeRate, FS.nbReadStreams, FS.nbWriteStreams, FS.nbReadWriteStreams, FS.nbMigratorStreams, FS.nbRecallerStreams) DESC, dbms_random.value
            ) DS
        WHERE ROWNUM < 2)
    RETURNING diskServerId INTO dsid;
    -- Now we got our Diskserver but we lost all other data (due to the fact we had
    -- to do an update for the lock and we could not do a join in the update).
    -- So here we select all we need
    SELECT FN.name, FN.mountPoint, FN.diskserver, FN.id
      INTO diskServerName, mountPoint, fsDiskServer, fileSystemId
      FROM (
        SELECT DiskServer.name, FS.mountPoint, FS.diskserver, FS.id
          FROM FileSystem FS, NbTapeCopiesInFS, Diskserver
         WHERE FS.id = NbTapeCopiesInFS.FS
           AND DiskServer.id = FS.diskserver
           AND NbTapeCopiesInFS.NbTapeCopies > 0
           AND NbTapeCopiesInFS.Stream = StreamId
           AND FS.status IN (0, 1)    -- PRODUCTION, DRAINING
           AND FS.diskserver = dsId
         ORDER BY FileSystemRate(FS.readRate, FS.writeRate, FS.nbReadStreams, FS.nbWriteStreams, FS.nbReadWriteStreams, FS.nbMigratorStreams, FS.nbRecallerStreams) DESC, dbms_random.value
         ) FN
     WHERE ROWNUM < 2;
    SELECT P.path, P.diskcopy_id, P.castorfile,
           C.fileId, C.nsHost, C.fileSize, P.id
      INTO path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId
      FROM (SELECT /*+ ORDERED USE_NL(D T) INDEX(T I_TAPECOPY_CF_STATUS_2) INDEX(ST I_PK_STREAM2TAPECOPY) */
            D.path, D.diskcopy_id, D.castorfile, T.id
              FROM (SELECT /*+ INDEX(DK I_DISKCOPY_FS_STATUS_10) */
                           DiskCopy.path path, DiskCopy.id diskcopy_id, DiskCopy.castorfile
                      FROM DiskCopy
                     WHERE decode(DiskCopy.status,10,DiskCopy.status,null) = 10 -- CANBEMIGR
                       AND DiskCopy.filesystem = fileSystemId) D,
                    TapeCopy T, Stream2TapeCopy ST
             WHERE T.castorfile = D.castorfile
               AND ST.child = T.id
               AND ST.parent = streamId
               AND decode(T.status,2,T.status,null) = 2 -- WAITINSTREAMS
               AND ROWNUM < 2) P, castorfile C
     WHERE P.castorfile = C.id;
  END IF;
  -- update status of selected tapecopy and stream
  UPDATE TapeCopy SET status = 3 -- SELECTED
   WHERE id = tapeCopyId;
  UPDATE Stream
     SET status = 3, -- RUNNING
         lastFileSystemUsed = fileSystemId, lastButOneFileSystemUsed = lastFileSystemUsed
   WHERE id = streamId;
  IF findNewFS = 1 THEN
    -- time only if we changed FS
    UPDATE Stream SET lastFileSystemChange = getTime() WHERE id = streamId;
  END IF;
  -- detach the tapecopy from the stream now that it is SELECTED
  -- the triggers will update NbTapeCopiesInFS accordingly
  DELETE FROM Stream2TapeCopy
   WHERE child = tapeCopyId;

  -- Update Filesystem state
  updateFSMigratorOpened(fsDiskServer, fileSystemId, 0);

  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- If the procedure was called with optimization enabled,
    -- rerun it again with optimization disabled to make sure
    -- there is really nothing to migrate!! Why? optimization
    -- excludes filesystems as being migration candidates if
    -- a migration stream is already running there. This allows
    -- us to maximise bandwidth to tape and not to saturate a
    -- diskserver with too many streams. However, in small disk
    -- pools this behaviour results in mounting, writing one
    -- file and dismounting of tapes as the tpdaemon reads ahead
    -- many files at a time! (#28097)
    IF optimized = 1 THEN
      bestTapeCopyForStream(streamId,
                            diskServerName,
                            mountPoint,
                            path,
                            dci,
                            castorFileId,
                            fileId,
                            nsHost,
                            fileSize,
                            tapeCopyId,
			    0);
      -- Since we recursively called ourselves, we should not do
      -- any update in the outer call
      RETURN;
    END IF;
    -- Reset last filesystems used
    UPDATE Stream
       SET lastFileSystemUsed = 0, lastButOneFileSystemUsed = 0
     WHERE id = streamId;
    -- No data found means the selected filesystem has no
    -- tapecopies to be migrated. Thus we go to next one
    -- However, we reset the NbTapeCopiesInFS row that failed
    -- This is not 100% safe but is far better than retrying
    -- in the same conditions
    IF 0 != fileSystemId THEN
      UPDATE NbTapeCopiesInFS
         SET NbTapeCopies = 0
       WHERE Stream = StreamId
         AND NbTapeCopiesInFS.FS = fileSystemId;
      bestTapeCopyForStream(streamId,
                            diskServerName,
                            mountPoint,
                            path,
                            dci,
                            castorFileId,
                            fileId,
                            nsHost,
                            fileSize,
                            tapeCopyId,
			    optimized);
    END IF;
END;


/* PL/SQL method implementing bestFileSystemForSegment */
CREATE OR REPLACE PROCEDURE bestFileSystemForSegment(segmentId IN INTEGER, diskServerName OUT VARCHAR2,
                                                     rmountPoint OUT VARCHAR2, rpath OUT VARCHAR2,
                                                     dci OUT INTEGER, optimized IN INTEGER) AS
  fileSystemId NUMBER := 0;
  fsDiskServer NUMBER;
  dcid NUMBER;
  cfid NUMBER;
  nb NUMBER := 0;
BEGIN
  -- First get the DiskCopy and see whether it already has a fileSystem
  -- associated (case of a multi segment file)
  -- We also select on the DiskCopy status since we know it is
  -- in WAITTAPERECALL status and there may be other ones
  -- INVALID, GCCANDIDATE, DELETED, etc...
  SELECT DiskCopy.fileSystem, DiskCopy.path, DiskCopy.id, DiskCopy.CastorFile
    INTO fileSystemId, rpath, dcid, cfid
    FROM TapeCopy, Segment, DiskCopy
   WHERE Segment.id = segmentId
     AND Segment.copy = TapeCopy.id
     AND DiskCopy.castorfile = TapeCopy.castorfile
     AND DiskCopy.status = 2; -- WAITTAPERECALL
  -- Check if the DiskCopy had a FileSystem associated
  IF fileSystemId > 0 THEN
    BEGIN
      -- It had one, force filesystem selection, unless it was disabled.
      SELECT DiskServer.name, DiskServer.id, FileSystem.mountPoint
        INTO diskServerName, fsDiskServer, rmountPoint
        FROM DiskServer, FileSystem
       WHERE FileSystem.id = fileSystemId
         AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
         AND DiskServer.id = FileSystem.diskServer
         AND DiskServer.status = 0; -- DISKSERVER_PRODUCTION
      updateFsRecallerOpened(fsDiskServer, fileSystemId, 0);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Error, the filesystem or the machine was probably disabled in between
      raise_application_error(-20101, 'In a multi-segment file, FileSystem or Machine was disabled before all segments were recalled');
    END;
  ELSE
   -- The DiskCopy had no FileSystem assoicated with it which indicates that
   -- This is a new recall. We try and select a good FileSystem for it!
   FOR a IN (SELECT DiskServer.name, FileSystem.mountPoint, FileSystem.id,
                    FileSystem.diskserver, CastorFile.fileSize
               FROM DiskServer, FileSystem, DiskPool2SvcClass,
                    (SELECT id, svcClass from StageGetRequest UNION ALL
                     SELECT id, svcClass from StagePrepareToGetRequest UNION ALL
                     SELECT id, svcClass from StageRepackRequest UNION ALL
                     SELECT id, svcClass from StageGetNextRequest UNION ALL
                     SELECT id, svcClass from StageUpdateRequest UNION ALL
                     SELECT id, svcClass from StagePrepareToUpdateRequest UNION ALL
                     SELECT id, svcClass from StageUpdateNextRequest) Request,
                     SubRequest, CastorFile
              WHERE CastorFile.id = cfid
                AND SubRequest.castorfile = cfid
                AND Request.id = SubRequest.request
                AND Request.svcclass = DiskPool2SvcClass.child
                AND FileSystem.diskpool = DiskPool2SvcClass.parent
                AND FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > CastorFile.fileSize
                AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
                AND DiskServer.id = FileSystem.diskServer
                AND DiskServer.status = 0 -- DISKSERVER_PRODUCTION
                -- Ignore diskservers where a recaller already exists
                AND DiskServer.id NOT IN (
                  SELECT DISTINCT(FileSystem.diskServer)
                    FROM FileSystem
                   WHERE nbRecallerStreams != 0
                     AND optimized = 1
                )
           ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams, FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC, dbms_random.value)
   LOOP
     -- Check that we don't already have a copy of this file on this filesystem.
     -- This will never happen in normal operations but may be the case if a filesystem
     -- was disabled and did come back while the tape recall was waiting.
     -- Even if we could optimize by cancelling remaining unneeded tape recalls when a
     -- fileSystem comes backs, the ones running at the time of the come back will have
     SELECT count(*) INTO nb
       FROM DiskCopy
      WHERE fileSystem = fileSystemId
        AND castorfile = cfid
        AND status = 0; -- STAGED
     IF nb = 0 THEN
       raise_application_error(-20103, 'Recaller could not find a FileSystem in production in the requested SvcClass and without copies of this file');
     END IF;
   END LOOP;  
  END IF;
  -- Set the diskcopy's filesystem
  UPDATE DiskCopy 
     SET fileSystem = fileSystemId
   WHERE id = dcid;
      
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Just like in bestTapeCopyForStream if we were called with optimization enabled
    -- and nothing was found, rerun the procedure again with optimization disabled to
    -- truly make sure nothing is found!
    IF optimized = 1 THEN
      bestFileSystemForSegment(segmentId, diskServerName, rmountPoint, rpath, dci, 0);
      -- Since we recursively called ourselves, we should not do
      -- any update in the outer call
      RETURN;
    END IF;
END;


/* PL/SQL method implementing fileRecallFailed */
CREATE OR REPLACE PROCEDURE fileRecallFailed(tapecopyId IN INTEGER) AS
 SubRequestId NUMBER;
 dci NUMBER;
 fsId NUMBER;
 fileSize NUMBER;
BEGIN
  SELECT SubRequest.id, DiskCopy.id, CastorFile.filesize
    INTO SubRequestId, dci, fileSize
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2; -- DISKCOPY_WAITTAPERECALL
  UPDATE DiskCopy SET status = 4 WHERE id = dci RETURNING fileSystem into fsid; -- DISKCOPY_FAILED
  IF SubRequestId IS NOT NULL THEN
    UPDATE SubRequest SET status = 7, -- SUBREQUEST_FAILED
                          getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED (not strictly correct but the request is over anyway)
                          lastModificationTime = getTime()
     WHERE id = SubRequestId;
    UPDATE SubRequest SET status = 7, -- SUBREQUEST_FAILED
                          getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
                          lastModificationTime = getTime(),
                          parent = 0
     WHERE parent = SubRequestId;
  END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
END;


/* PL/SQL method implementing castor package */
CREATE OR REPLACE PACKAGE castor AS
  TYPE DiskCopyCore IS RECORD (
        id INTEGER,
        path VARCHAR2(2048),
        status NUMBER,
        fsWeight NUMBER,
        mountPoint VARCHAR2(2048),
        diskServer VARCHAR2(2048));
  TYPE DiskCopy_Cur IS REF CURSOR RETURN DiskCopyCore;
  TYPE TapeCopy_Cur IS REF CURSOR RETURN TapeCopy%ROWTYPE;
  TYPE Segment_Cur IS REF CURSOR RETURN Segment%ROWTYPE;
  TYPE StreamCore IS RECORD (
        id INTEGER,
        initialSizeToTransfer INTEGER,
        status NUMBER,
        tapePoolId NUMBER,
        tapePoolName VARCHAR2(2048));
  TYPE Stream_Cur IS REF CURSOR RETURN StreamCore;
  TYPE "strList" IS TABLE OF VARCHAR2(2048) index by binary_integer;
  TYPE "cnumList" IS TABLE OF NUMBER index by binary_integer;
  TYPE QueryLine IS RECORD (
        fileid INTEGER,
        nshost VARCHAR2(2048),
        diskCopyId INTEGER,
        diskCopyPath VARCHAR2(2048),
        filesize INTEGER,
        diskCopyStatus INTEGER,
        diskServerName VARCHAR2(2048),
        fileSystemMountPoint VARCHAR2(2048),
        nbaccesses INTEGER,
        lastKnownFileName VARCHAR2(2048));
  TYPE QueryLine_Cur IS REF CURSOR RETURN QueryLine;
  TYPE FileList_Cur IS REF CURSOR RETURN FilesDeletedProcOutput%ROWTYPE;
  TYPE DiskPoolQueryLine IS RECORD (
        isDP INTEGER,
        isDS INTEGER,
        diskServerName VARCHAR(2048),
        diskServerStatus INTEGER,
        fileSystemmountPoint VARCHAR(2048),
        fileSystemfreeSpace INTEGER,
        fileSystemtotalSpace INTEGER,
        fileSystemminfreeSpace INTEGER,
        fileSystemmaxFreeSpace INTEGER,
        fileSystemStatus INTEGER);
  TYPE DiskPoolQueryLine_Cur IS REF CURSOR RETURN DiskPoolQueryLine;
  TYPE DiskPoolsQueryLine IS RECORD (
        isDP INTEGER,
        isDS INTEGER,
        diskPoolName VARCHAR(2048),
        diskServerName VARCHAR(2048),
        diskServerStatus INTEGER,
        fileSystemmountPoint VARCHAR(2048),
        fileSystemfreeSpace INTEGER,
        fileSystemtotalSpace INTEGER,
        fileSystemminfreeSpace INTEGER,
        fileSystemmaxFreeSpace INTEGER,
        fileSystemStatus INTEGER);
  TYPE DiskPoolsQueryLine_Cur IS REF CURSOR RETURN DiskPoolsQueryLine;
  TYPE IDRecord IS RECORD (id INTEGER);
  TYPE IDRecord_Cur IS REF CURSOR RETURN IDRecord;
END castor;


CREATE OR REPLACE TYPE "numList" IS TABLE OF INTEGER;


/* PL/SQL method checking whether a given service class
   is declared disk only and had only full diskpools.
   Returns 1 in such a case, 0 else */
CREATE OR REPLACE FUNCTION checkFailJobsWhenNoSpace(svcClassId NUMBER)
RETURN NUMBER AS
  failJobFlag NUMBER;
  unused NUMBER;
BEGIN
  SELECT hasDiskOnlyBehavior INTO failJobFlag FROM SvcClass WHERE id = svcClassId;
  IF (failJobFlag = 1) THEN
    SELECT count(*) INTO unused
      FROM diskpool2svcclass, FileSystem, DiskServer
     WHERE diskpool2svcclass.child = svcClassId
       AND diskpool2svcclass.parent = FileSystem.diskPool
       AND FileSystem.diskServer = DiskServer.id
       AND FileSystem.status = 0 -- PRODUCTION
       AND DiskServer.status = 0 -- PRODUCTION
       AND totalSize * minAllowedFreeSpace < free;
    IF (unused = 0) THEN
      RETURN 1;
    END IF;
  END IF;
  RETURN 0;
END;


/* PL/SQL method implementing streamsToDo */
CREATE OR REPLACE PROCEDURE streamsToDo(res OUT castor.Stream_Cur) AS
  streams "numList";
BEGIN
  SELECT UNIQUE Stream.id BULK COLLECT INTO streams
    FROM Stream, NbTapeCopiesInFS, FileSystem, DiskServer
   WHERE Stream.status = 0 --PENDING
     AND NbTapeCopiesInFS.stream = Stream.id
     AND NbTapeCopiesInFS.NbTapeCopies > 0
     AND FileSystem.id = NbTapeCopiesInFS.FS
     AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
     AND DiskServer.id = FileSystem.DiskServer
     AND DiskServer.status IN (0, 1); -- PRODUCTION, DRAINING
  UPDATE Stream SET status = 1 -- WAITDRIVE
   WHERE id MEMBER OF streams;
  OPEN res FOR
    SELECT Stream.id, Stream.InitialSizeToTransfer, Stream.status,
           TapePool.id, TapePool.name
      FROM Stream, TapePool
     WHERE Stream.id MEMBER OF streams
       AND Stream.TapePool = TapePool.id;
END;


/* PL/SQL method implementing isSubRequestToSchedule */
CREATE OR REPLACE PROCEDURE isSubRequestToSchedule
        (rsubreqId IN INTEGER, result OUT INTEGER,
         sources OUT castor.DiskCopy_Cur) AS
  stat "numList";
  dci "numList";
  svcClassId NUMBER;
  reqId NUMBER;
  cfId NUMBER;
  reqType NUMBER;
BEGIN
 ---- retrieve the castorfile and the request id --- 
   SELECT SubRequest.castorfile, SubRequest.request
    INTO cfId, reqId
    FROM SubRequest
   WHERE SubRequest.id = rsubreqId;
  --- lock the castor file to be safe in case of two concurrent subrequest
   SELECT id into cfId FROM CastorFile where CastorFile.id=cfId FOR UPDATE;
  
  -- First see whether we should wait on an ongoing request
  SELECT DiskCopy.status, DiskCopy.id
    BULK COLLECT INTO stat, dci
    FROM DiskCopy, FileSystem, DiskServer, Id2Type
   WHERE reqId = Id2Type.id  -- Avoid that PutDone waits
     AND Id2Type.type != 39               -- on the prepareToPut
     AND cfId = DiskCopy.castorfile
     AND FileSystem.id(+) = DiskCopy.fileSystem
     AND nvl(FileSystem.status, 0) = 0 -- PRODUCTION
     AND DiskServer.id(+) = FileSystem.diskServer
     AND nvl(DiskServer.status, 0) = 0 -- PRODUCTION
     AND DiskCopy.status IN (1, 2); -- WAITDISK2DISKCOPY, WAITTAPERECALL

  IF stat.COUNT > 0 THEN
    -- Only DiskCopy is in WAIT*, make SubRequest wait on previous subrequest and do not schedule
    makeSubRequestWait(rsubreqId, dci(1));
    result := 0;  -- no schedule
    COMMIT;
    RETURN;
  END IF;
  -- Get the svcclass and the reqId for this subrequest
  SELECT Request.id, Request.svcclass, SubRequest.castorfile
    INTO reqId, svcClassId, cfId
    FROM (SELECT id, svcClass from StageGetRequest UNION ALL
          SELECT id, svcClass from StagePrepareToGetRequest UNION ALL
          SELECT id, svcClass from StageRepackRequest UNION ALL
          SELECT id, svcClass from StageUpdateRequest UNION ALL
          SELECT id, svcClass from StagePrepareToUpdateRequest UNION ALL
          SELECT id, svcClass from StagePutDoneRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = rsubreqId;
     
  -- PutDone processing is different from now on, handle it separately
  SELECT type INTO reqType FROM Id2Type WHERE id = reqId;
  IF reqType = 39 THEN -- PutDone
    -- Try to see whether we have available DiskCopies.
    -- Here we look on all FileSystems regardless the status
    -- so that a putDone on a disabled one goes through as there's
    -- no real IO activity involved.
    SELECT DiskCopy.status, DiskCopy.id
      BULK COLLECT INTO stat, dci
      FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
     WHERE DiskCopy.castorfile = cfId
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskpool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = svcClassId
       AND FileSystem.diskserver = DiskServer.id
       AND DiskCopy.status = 6; -- STAGEOUT
    IF stat.COUNT > 0 THEN
      -- Check that there is no put going on
      -- If any, we'll wait on one of them
      DECLARE
        putSubReq NUMBER;
      BEGIN
        -- Try to find a running put
        SELECT subrequest.id INTO putSubReq
          FROM SubRequest, Id2Type
         WHERE SubRequest.castorfile = cfId
           AND SubRequest.request = Id2Type.id
           AND Id2Type.type = 40 -- Put
           AND SubRequest.status IN (0, 1, 2, 3, 6, 13, 14) -- START, RESTART, RETRY, WAITSCHED, READY, READYFORSCHED, BEINGSCHED
           AND ROWNUM < 2;
        -- we've found one, putDone will have to wait
        UPDATE SubRequest
           SET parent = putSubReq,
               status = 5, -- WAITSUBREQ
               lastModificationTime = getTime()
         WHERE id = rsubreqId;
        result := 0;  -- no schedule
        COMMIT;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- no put waiting, we can continue
        result := 1;
        OPEN sources
          FOR SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, 0,   -- fs rate does not apply here
                     FileSystem.mountPoint, DiskServer.name
                FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
               WHERE DiskCopy.castorfile = cfId
                 AND FileSystem.diskpool = DiskPool2SvcClass.parent
                 AND DiskPool2SvcClass.child = svcClassId
                 AND DiskCopy.status = 6 -- STAGEOUT
                 AND FileSystem.id = DiskCopy.fileSystem
                 AND DiskServer.id = FileSystem.diskServer;
      END;
    ELSE
      -- This is a PutDone without a put (otherwise we would have found
      -- a DiskCopy on a FileSystem). We answer 1 without sources,
      -- the stager correctly handles this case.
      result := 1;
    END IF;
  
  ELSE
    -- All other requests: try to see whether we have available DiskCopies
    SELECT DiskCopy.status, DiskCopy.id
      BULK COLLECT INTO stat, dci
      FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
     WHERE DiskCopy.castorfile = cfId
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskpool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = svcClassId
       AND FileSystem.status = 0 -- PRODUCTION
       AND FileSystem.diskserver = DiskServer.id
       AND DiskServer.status = 0 -- PRODUCTION
       AND DiskCopy.status IN (0, 6, 10); -- STAGED, STAGEOUT, CANBEMIGR
    IF stat.COUNT > 0 THEN
      -- Yes, we should schedule and give a list of sources.
      -- However, in case of PrepareToGet/Update this is a no-op,
      -- so we answer no
      IF reqType in (36, 38) THEN
        result := 0;
      ELSE
        result := 1;
        OPEN sources
          FOR SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, 0,   -- fs rate does not apply here
                     FileSystem.mountPoint, DiskServer.name
                FROM DiskCopy, SubRequest, FileSystem, DiskServer, DiskPool2SvcClass
               WHERE SubRequest.id = rsubreqId
                 AND SubRequest.castorfile = DiskCopy.castorfile
                 AND FileSystem.diskpool = DiskPool2SvcClass.parent
                 AND DiskPool2SvcClass.child = svcClassId
                 AND DiskCopy.status IN (0, 6, 10) -- STAGED, STAGEOUT, CANBEMIGR
                 AND FileSystem.id = DiskCopy.fileSystem
                 AND FileSystem.status = 0 -- PRODUCTION
                 AND DiskServer.id = FileSystem.diskServer
                 AND DiskServer.status = 0; -- PRODUCTION
      END IF;
    ELSE
      -- No diskcopies available for this service class;
      -- check whether we are a disk only pool that is already full.
      -- In such a case, we should fail the request with an ENOSPACE error
      IF (checkFailJobsWhenNoSpace(svcClassId) = 1) THEN
        result := 4; -- no schedule
        UPDATE SubRequest
           SET status = 7, -- FAILED
               errorCode = 28, -- ENOSPC
               errorMessage = 'File creation canceled since diskPool is full'
         WHERE id = rsubreqId;
        COMMIT;
        RETURN;
      END IF;  
      -- check whether there are any diskcopies available for a disk2disk copy
      DECLARE
        unused NUMBER;
      BEGIN
        SELECT DiskCopy.id INTO unused 
          FROM DiskCopy, FileSystem, DiskServer
         WHERE DiskCopy.castorfile = cfId
           AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
           AND FileSystem.id = DiskCopy.fileSystem
           AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
           AND DiskServer.id = FileSystem.diskserver
           AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
           AND ROWNUM < 2;
        -- We found at least one, therefore we schedule anywhere  
        -- forcing a disk2disk copy from the existing diskcopy
        -- not available to this svcclass
        result := 3;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- We found no diskcopies at all. We should not schedule
        -- and make a tape recall... except ... in 2 cases :
        --   - if there is some temporarily unavailable diskcopy
        --     that is in CANBEMIGR or STAGEOUT
        -- in such a case, what we have is an existing file, that
	-- was migrated, then overwritten but never migrated again.
        -- So the unavailable diskCopy is the only copy that is valid.
	-- We will tell the client that the file is unavailable
        -- and he/she will retry later
        --   - if we have an available STAGEOUT copy. This can happen
        -- when the copy is in a given svcclass and we were looking
        -- in another one. Since disk to disk copy is impossible in this
        -- case, the file is declared BUSY.
        --   - if we have an available WAITFS, WAITFSSCHEDULING copy in such
        -- a case, we tell the client that the file is BUSY
        DECLARE
          dcStatus NUMBER;
          fsStatus NUMBER;
          dsStatus NUMBER;
        BEGIN
          SELECT DiskCopy.status, nvl(FileSystem.status, 0), nvl(DiskServer.status, 0)
            INTO dcStatus, fsStatus, dsStatus
            FROM DiskCopy, FileSystem, DiskServer
           WHERE DiskCopy.castorfile = cfId
             AND DiskCopy.status IN (5, 6, 10, 11) -- WAITFS, STAGEOUT, CANBEMIGR, WAITFSSCHEDULING
             AND FileSystem.id(+) = DiskCopy.fileSystem
             AND DiskServer.id(+) = FileSystem.diskserver
             AND ROWNUM < 2;
          -- We are in one of the special cases. Don't schedule, don't recall
          result := 4; -- no schedule
          UPDATE SubRequest
             SET status = 7, -- FAILED
                 errorCode = CASE
                   WHEN dcStatus IN (5,11) THEN 16 -- WAITFS, WAITFSSCHEDULING, EBUSY
                   WHEN dcStatus = 6 AND fsStatus = 0 and dsStatus = 0 THEN 16 -- STAGEOUT, PRODUCTION, PRODUCTION, EBUSY
                   ELSE 1718 -- ESTNOTAVAIL
                 END,
                 errorMessage = CASE
                   WHEN dcStatus IN (5, 11) THEN -- WAITFS, WAITFSSCHEDULING
                     'File is being (re)created right now by another user'
                   WHEN dcStatus = 6 AND fsStatus = 0 and dsStatus = 0 THEN -- STAGEOUT, PRODUCTION, PRODUCTION
                     'File is being written to in another SvcClass'
                   ELSE
                     'All copies of this file are unavailable for now. Please retry later'
                 END
           WHERE id = rsubreqId;
          COMMIT;
        EXCEPTION WHEN NO_DATA_FOUND THEN
	  -- We did not found the very special case, go for recall
          result := 2;
        END;
      END;
    END IF;
  END IF;   -- IF type = PutDone
END;


/* Build diskCopy path from fileId */
CREATE OR REPLACE PROCEDURE buildPathFromFileId(fid IN INTEGER,
                                                nsHost IN VARCHAR2,
                                                dcid IN INTEGER,
                                                path OUT VARCHAR2) AS
BEGIN
  path := CONCAT(CONCAT(CONCAT(CONCAT(TO_CHAR(MOD(fid,100),'FM09'), '/'),
                        CONCAT(TO_CHAR(fid), '@')),
                 nsHost), CONCAT('.', TO_CHAR(dcid)));
END;


/* TO BE REMOVED. Temporary hack to avoid losing file modifications in case of update */
CREATE OR REPLACE PROCEDURE fixUpdateRequestProblem(dcid IN INTEGER,
                                                    cfid IN INTEGER,
                                                    srid IN INTEGER) AS
  nbres NUMBER;
  unused NUMBER;
BEGIN
  -- we only want to do something for updates
  BEGIN
    SELECT r.id INTO unused
      FROM stageUpdateRequest r, SubRequest s
     WHERE s.request = r.id
       AND s.id = srId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RETURN;
  END;
  -- First check that the file is not busy, i.e. that we are not
  -- in the middle of migrating it. If we are, just stop and raise
  -- a user exception
  SELECT count(*) INTO nbRes FROM TapeCopy
    WHERE status = 3 -- TAPECOPY_SELECTED
    AND castorFile = cfId;
  IF nbRes > 0 THEN
    raise_application_error(-20106, 'Trying to update a busy file');
  END IF;
  -- invalidate all diskcopies
  UPDATE DiskCopy SET status = 7 -- INVALID 
   WHERE castorFile  = cfId
     AND status IN (0, 10);
  -- except the one we are dealing with that goes to STAGEOUT
  UPDATE DiskCopy SET status = 6 -- STAGEOUT
   WHERE id = dcid;
  -- Suppress all Tapecopies (avoid migration of previous version of the file)
  deleteTapeCopies(cfId);
END;


/* PL/SQL method implementing getUpdateStart */
CREATE OR REPLACE PROCEDURE getUpdateStart
        (srId IN INTEGER, fileSystemId IN INTEGER,
         dci OUT INTEGER, rpath OUT VARCHAR2,
         rstatus OUT NUMBER, sources OUT castor.DiskCopy_Cur,
         reuid OUT INTEGER, regid OUT INTEGER) AS
  cfid INTEGER;
  fid INTEGER;
  nh VARCHAR2(2048);
  realFileSize INTEGER;
  srSvcClass INTEGER;
BEGIN
  -- Get and uid, gid
  SELECT euid, egid, svcClass
    INTO reuid, regid, srSvcClass
    FROM SubRequest,
        (SELECT id, euid, egid, svcClass from StageGetRequest UNION ALL
         SELECT id, euid, egid, svcClass from StagePrepareToGetRequest UNION ALL
         SELECT id, euid, egid, svcClass from StageRepackRequest UNION ALL
         SELECT id, euid, egid, svcClass from StageUpdateRequest UNION ALL
         SELECT id, euid, egid, svcClass from StagePrepareToUpdateRequest) Request
   WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
  -- Take a lock on the CastorFile. Associated with triggers,
  -- this guarantee we are the only ones dealing with its copies
  SELECT CastorFile.fileSize, CastorFile.id
    INTO realFileSize, cfId
    FROM CastorFile, SubRequest
   WHERE CastorFile.id = SubRequest.castorFile
     AND SubRequest.id = srId FOR UPDATE;
  -- Try to find local DiskCopy
  dci := 0;
  SELECT /*+ INDEX(DISKCOPY I_DISKCOPY_CASTORFILE) */ DiskCopy.id, DiskCopy.path, DiskCopy.status
    INTO dci, rpath, rstatus
    FROM DiskCopy
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.filesystem = fileSystemId
     AND DiskCopy.status IN (0, 1, 2, 5, 6, 10, 11); -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
  -- If found local one, check whether to wait on it
  IF rstatus IN (1, 2, 5, 11) THEN -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, WAITFS_SCHEDULING, Make SubRequest Wait
    makeSubRequestWait(srId, dci);
    dci := 0;
    rpath := '';
  END IF;
  -- No wait, so we are settled and we'll use the local copy. However,
  -- in case of an update, we should update it to STAGEOUT and make the
  -- other copies INVALID.
  -- We should NOT do so, and we should wait for the first byte to be
  -- written in the file to do so, but for now, we do it now...
  -- Note that we do the same in Disk2DiskCopyDone for the case of
  -- replication due to the update
  fixUpdateRequestProblem(dci, cfId, srId);
  -- It could be that we end up here, while we were supposed to do
  -- a file replication, i.e. a disk2diskcopy. The reason is that
  -- the selected filesytem in such a case can be any, including
  -- the one(s) already having a valid diskcopy. We have to detect that
  -- and call updateFSFileClose in such a case.
  DECLARE
    maxNbRepl NUMBER;
    repPolicy VARCHAR2(2048);
    curNbRepl NUMBER;
  BEGIN
    IF rstatus IN (0, 10) THEN
      -- see maxNbReplicas for the svcclass
      SELECT maxReplicaNb, replicationPolicy INTO maxNbRepl, repPolicy
        FROM SvcClass WHERE id = srSvcClass;
      -- only deal with replication
      IF maxNbRepl = 0 OR maxNbRepl > 1 THEN
        -- see current nb of copies
        SELECT count(*) INTO curNbRepl
          FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
         WHERE DiskCopy.castorfile = cfId
           AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
           AND FileSystem.id = DiskCopy.fileSystem
           AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
           AND DiskServer.id = FileSystem.diskServer
           AND DiskServer.status IN (0, 1)
           AND DiskPool2SvcClass.parent = FileSystem.diskPool
           AND DiskPool2SvcClass.child = srSvcClass;
        -- compare the 2
        IF curNbRepl < maxNbRepl OR
           (maxNbRepl = 0 AND repPolicy IS NULL) THEN
          -- update filesystem status
          updateFSFileClosed(fileSystemId);
        END IF;
      END IF;
    END IF;
  END;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- No disk copy found on selected FileSystem, look in others
    BEGIN
      -- Try to see whether we should wait on some DiskCopy
        SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status
          INTO dci, rpath, rstatus
          FROM DiskCopy, FileSystem, DiskServer
         WHERE DiskCopy.castorfile = cfId
           AND DiskCopy.status IN (1, 2, 5, 6, 11) -- WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, WAITFS_SCHEDULING, STAGEOUT
           AND FileSystem.id(+) = DiskCopy.fileSystem
           AND FileSystem.status(+) = 0 -- PRODUCTION
           AND DiskServer.id(+) = FileSystem.diskserver
           AND DiskServer.status(+) = 0 -- PRODUCTION
           AND ROWNUM < 2;
      -- Found a DiskCopy, we have to wait on it
      makeSubRequestWait(srId, dci);
      dci := 0;
      rpath := '';
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- No disk copy found in WAIT*, we don't have to wait
      BEGIN
        -- Check whether there are any diskcopy available
        SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status
          INTO dci, rpath, rstatus
          FROM DiskCopy, FileSystem, DiskServer
         WHERE DiskCopy.castorfile = cfId
           AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
           AND FileSystem.id = DiskCopy.fileSystem
           AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
           AND DiskServer.id = FileSystem.diskserver
           AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
           AND ROWNUM < 2;
        -- We found at least a DiskCopy. Let's list all of them
        OPEN sources
          FOR SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status,
                     FileSystemRate(FileSystem.readRate, FileSystem.WriteRate,
                                    FileSystem.nbReadStreams, FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams, 
	      	                    FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams),
                     FileSystem.mountPoint,
                     DiskServer.name
                FROM DiskCopy, FileSystem, DiskServer
               WHERE DiskCopy.castorfile = cfId
                 AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
                 AND FileSystem.id = DiskCopy.fileSystem
                 AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
                 AND DiskServer.id = FileSystem.diskServer
                 AND DiskServer.status IN (0, 1); -- PRODUCTION, DRAINING
        -- create DiskCopy for Disk to Disk copy
        UPDATE SubRequest SET diskCopy = ids_seq.nextval,
                              lastModificationTime = getTime() WHERE id = srId
        RETURNING castorFile, diskCopy INTO cfid, dci;
        SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfid;
        buildPathFromFileId(fid, nh, dci, rpath);
        INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime)
             VALUES (rpath, dci, fileSystemId, cfid, 1, getTime()); -- status WAITDISK2DISKCOPY
        INSERT INTO Id2Type (id, type) VALUES (dci, 5); -- OBJ_DiskCopy
        rstatus := 1; -- status WAITDISK2DISKCOPY
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- No disk copy found on any FileSystem. This is an error because
        -- in case of tape recall the job should not have started.
        -- Raise error to the Job service
        raise_application_error(-20105, 'No valid DiskCopy found on any filesystem for this job');
    END;
  END;
END;


/* PL/SQL method implementing putStart */
CREATE OR REPLACE PROCEDURE putStart
        (srId IN INTEGER, fileSystemId IN INTEGER,
         rdcId OUT INTEGER, rdcStatus OUT INTEGER,
         rdcPath OUT VARCHAR2) AS
  srStatus INTEGER;
  prevFsId INTEGER;
BEGIN
  -- Get diskCopy id
  SELECT diskCopy, SubRequest.status, fileSystem INTO rdcId, srStatus, prevFsId
    FROM SubRequest, DiskCopy
   WHERE SubRequest.diskcopy = Diskcopy.id
     AND SubRequest.id = srId;
  -- Check that we did not cancel the SubRequest in the mean time
  IF srStatus IN (7, 9, 10) THEN -- FAILED, FAILED_FINISHED, FAILED_ANSWERING
    raise_application_error(-20104, 'SubRequest cancelled while queuing in scheduler. Giving up.');
  END IF;
  IF prevFsId > 0 AND prevFsId <> fileSystemId THEN
    -- this could happen if LSF schedules the same job twice!
    -- (see bug report #14358 for the whole story)
    raise_application_error(-20107, 'This job has already started for this DiskCopy. Giving up.');
  END IF;
  
  -- Get older castorFiles with the same name and drop their lastKnownFileName
  UPDATE /*+ INDEX (castorFile) */ CastorFile SET lastKnownFileName = TO_CHAR(id)
   WHERE id IN (
    SELECT /*+ INDEX (cfOld) */ cfOld.id FROM CastorFile cfOld, CastorFile cfNew, SubRequest
     WHERE cfOld.lastKnownFileName = cfNew.lastKnownFileName
       AND cfOld.fileid <> cfNew.fileid
       AND cfNew.id = SubRequest.castorFile
       AND SubRequest.id = srId);
  -- In case the DiskCopy was in WAITFS_SCHEDULING, PUT the
  -- waiting SubRequests in RESTART
  UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0 -- SUBREQUEST_RESTART
   WHERE parent = srId;
  -- link DiskCopy and FileSystem and update DiskCopyStatus
  UPDATE DiskCopy SET status = 6, -- DISKCOPY_STAGEOUT
                      fileSystem = fileSystemId
   WHERE id = rdcId
   RETURNING status, path
   INTO rdcStatus, rdcPath;
END;


/* PL/SQL method implementing updateAndCheckSubRequest */
CREATE OR REPLACE PROCEDURE updateAndCheckSubRequest(srId IN INTEGER, newStatus IN INTEGER, result OUT INTEGER) AS
  reqId INTEGER;
BEGIN
 -- Lock the access to the Request
 SELECT Id2Type.id INTO reqId
  FROM SubRequest, Id2Type
  WHERE SubRequest.id = srId
  AND Id2Type.id = SubRequest.request
  FOR UPDATE;
 -- Update Status
 UPDATE SubRequest SET status = newStatus,
                       answered = 1,
                       lastModificationTime = getTime() WHERE id = srId;
 IF newStatus = 6 THEN  -- READY
   UPDATE SubRequest SET getNextStatus = 1 -- GETNEXTSTATUS_FILESTAGED
    WHERE id = srId;
 END IF;
 -- Check whether it was the last subrequest in the request
 SELECT id INTO result FROM SubRequest
  WHERE request = reqId
    AND status IN (0, 1, 2, 3, 4, 5)   -- START, RESTART, RETRY, WAITSCHED, WAITTAPERECALL, WAITSUBREQ
    AND answered = 0
    AND ROWNUM < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN -- No data found means we were last
  result := 0;
END;


/* PL/SQL method implementing disk2DiskCopyDone */
CREATE OR REPLACE PROCEDURE disk2DiskCopyDone
(dcId IN INTEGER, srcDcId IN INTEGER) AS
  srId INTEGER;
  cfId INTEGER;
  fsId INTEGER;
  maxRepl INTEGER;
  repl INTEGER;
BEGIN
  -- update DiskCopy
  UPDATE DiskCopy
     SET status = (SELECT status FROM DiskCopy WHERE id = srcDcId)
   WHERE id = dcId
  RETURNING CastorFile, FileSystem INTO cfId, fsId;
  -- update SubRequest
  UPDATE SubRequest set status = 6, -- SUBREQUEST_READY
                        getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
                        lastModificationTime = getTime()
   WHERE diskCopy = dcId RETURNING id INTO srId;
  -- If the maxReplicaNb is exceeded, update one of the diskcopies in a 
  -- DRAINING filesystem to INVALID.
  -- We do the check only for Get requests as for the update requests we still
  -- have the fixUpdateRequestProblem procedure...
  BEGIN
    SELECT maxReplicaNb into maxRepl
      FROM SvcClass, (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
                      SELECT id, svcClass FROM StageGetRequest) Request,
           SubRequest
     WHERE SubRequest.id = srId
       AND SubRequest.request = Request.id
       AND Request.svcClass = SvcClass.id;
    SELECT count(*) into repl FROM DiskCopy
     WHERE castorFile = cfId
       AND status in (0, 10);
    IF repl > maxRepl THEN
      -- We did replicate only because of the DRAINING filesystem.
      -- Invalidate one of the original diskcopies, not all of them for fault resiliency purposes
      UPDATE DiskCopy set status = 7
       WHERE status in (0, 10)
         AND castorFile = cfId
         AND fileSystem in (SELECT id FROM FileSystem WHERE status = 1)  -- DRAINING
         AND ROWNUM < 2;
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- this is not a Get request, ignore
    NULL;
  END;
  
  -- In case of an update, we should update the new copy to STAGEOUT,
  -- independently of the original status make the other copies INVALID.
  -- We should NOT do so, and we should wait for the first byte to be
  -- written in the file to do so, but for now, we do it now...
  -- Note that we do the same in GetUpdateStart for the case with no
  -- replication due to the update request.
  fixUpdateRequestProblem(dcId, cfId, srId);
  -- Wake up waiting subrequests
  UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0
   WHERE parent = srId; -- SUBREQUEST_RESTART
  -- update filesystem status
  updateFsFileClosed(fsId);
END;


/* PL/SQL method implementing disk2DiskCopyFailed */
CREATE OR REPLACE PROCEDURE disk2DiskCopyFailed
(dcId IN INTEGER) AS
  srId INTEGER;
  fsId INTEGER;
BEGIN
  -- Delete the DiskCopy
  -- Since it can not be the only one, don't try to delete
  -- the Castorfile
  DELETE FROM Id2Type WHERE Id = dcId;
  DELETE FROM DiskCopy WHERE Id = dcId
    RETURNING FileSystem INTO fsId;
  -- Fail the corresponding subrequest. Answer was sent by the job
  UPDATE SubRequest SET status = 9, -- SUBREQUEST_FAILEDFINISHED
                        lastModificationTime = getTime()
   WHERE diskCopy = dcId RETURNING id INTO srId;
  -- Wake up other subrequests waiting on it
  UPDATE SubRequest SET status = 1, -- SUBREQUEST_RESTART
                        lastModificationTime = getTime(),
                        parent = 0
   WHERE parent = srId;
  -- update filesystem status
  updateFsFileClosed(fsId);
END;


/* PL/SQL method implementing recreateCastorFile */
CREATE OR REPLACE PROCEDURE recreateCastorFile(cfId IN INTEGER,
                                               srId IN INTEGER,
                                               dcId OUT INTEGER,
                                               rstatus OUT INTEGER,
                                               rmountPoint OUT VARCHAR2,
                                               rdiskServer OUT VARCHAR2) AS
  rpath VARCHAR2(2048);
  nbRes INTEGER;
  fid INTEGER;
  nh VARCHAR2(2048);
  unused INTEGER;
  contextPIPP INTEGER;
BEGIN
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO unused FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Determine the context (Put inside PrepareToPut ?)
  BEGIN
    -- check that we are a Put
    SELECT StagePutRequest.id INTO unused
      FROM StagePutRequest, SubRequest
     WHERE SubRequest.id = srId
       AND StagePutRequest.id = SubRequest.request;
    BEGIN
      -- check that there is a PrepareToPut going on
      SELECT SubRequest.diskCopy INTO dcId
        FROM StagePrepareToPutRequest, SubRequest
       WHERE SubRequest.CastorFile = cfId
         AND StagePrepareToPutRequest.id = SubRequest.request
         AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 12, 13, 14);  -- All but FINISHED, FAILED_FINISHED, ARCHIVED
      -- if we got here, we are a Put inside a PrepareToPut
      contextPIPP := 1;
    EXCEPTION WHEN TOO_MANY_ROWS THEN
      -- this means we are a PrepareToPut and another PrepareToPut
      -- is already running. This is forbidden
      archiveSubReq(srId);
      RAISE;
    END;   
  EXCEPTION WHEN NO_DATA_FOUND THEN
    contextPIPP := 0;
  END;
  IF contextPIPP = 0 THEN
    -- check if there is space in the diskpool in case of
    -- disk only pool
    DECLARE
      svcClassId NUMBER;
    BEGIN
      -- get the svcclass
      SELECT svcClass INTO svcClassId
        FROM Subrequest,
             (SELECT id, svcClass from StagePutRequest UNION ALL
              SELECT id, svcClass from StagePrepareToPutRequest) Request
       WHERE SubRequest.id = srId
         AND Request.id = SubRequest.request;
      IF (checkFailJobsWhenNoSpace(svcClassId) = 1) THEN
        -- The svcClass is declared disk only and has no space
        -- thus we cannot recreate the file
        dcId := 0;
        UPDATE SubRequest
           SET status = 7, -- FAILED
               errorCode = 28, -- ENOSPC
               errorMessage = 'File creation canceled since diskPool is full'
         WHERE id = srId;
        COMMIT;
        RETURN;
      END IF;
    END;
    -- check if recreation is possible for TapeCopies
    SELECT count(*) INTO nbRes FROM TapeCopy
     WHERE status = 3 -- TAPECOPY_SELECTED
      AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot recreate
      dcId := 0;
      UPDATE SubRequest
         SET status = 7, -- FAILED
             errorCode = 16, -- EBUSY
             errorMessage = 'File recreation canceled since file is being migrated'
        WHERE id = srId;
      COMMIT;
      RETURN;
    END IF;
    -- check if recreation is possible for DiskCopies
    SELECT count(*) INTO nbRes FROM DiskCopy
     WHERE status IN (1, 2, 5, 6) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT
       AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot recreate
      dcId := 0;
      UPDATE SubRequest
         SET status = 7, -- FAILED
             errorCode = 16, -- EBUSY
             errorMessage = 'File recreation canceled since file is either being recalled, or replicated or created by another user'
       WHERE id = srId;
      COMMIT;
      RETURN;
    END IF;
    -- delete all tapeCopies
    deleteTapeCopies(cfId);
    -- set DiskCopies to INVALID
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE castorFile = cfId AND status IN (0, 10); -- STAGED, CANBEMIGR
    -- create new DiskCopy
    SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfId;
    SELECT ids_seq.nextval INTO dcId FROM DUAL;
    buildPathFromFileId(fid, nh, dcId, rpath);
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime)
         VALUES (rpath, dcId, 0, cfId, 5, getTime()); -- status WAITFS
    rstatus := 5; -- WAITFS
    rmountPoint := '';
    rdiskServer := '';
    INSERT INTO Id2Type (id, type) VALUES (dcId, 5); -- OBJ_DiskCopy
    COMMIT;
  ELSE
    DECLARE
      fsId INTEGER;
      dsId INTEGER;
    BEGIN
      -- Retrieve the infos about the DiskCopy to be used
      SELECT fileSystem, status INTO fsId, rstatus FROM DiskCopy WHERE id = dcId;
      -- retrieve mountpoint and filesystem if any
      IF fsId = 0 THEN
        rmountPoint := '';
        rdiskServer := '';
      ELSE
        SELECT mountPoint, diskServer INTO rmountPoint, dsId
          FROM FileSystem WHERE FileSystem.id = fsId;
        SELECT name INTO rdiskServer FROM DiskServer WHERE id = dsId;
      END IF;
      -- See whether we should wait on the previous Put Request
      IF rstatus = 11 THEN -- WAITFS_SCHEDULING
        makeSubRequestWait(srId, dcId);
      END IF;
    END;
  END IF; 
  -- link SubRequest and DiskCopy
  UPDATE SubRequest SET diskCopy = dcId,
                        lastModificationTime = getTime() WHERE id = srId;
  COMMIT;
END;


/* PL/SQL method internalPutDoneFunc, used by fileRecalled, putDoneFunc
   checks for diskcopies in STAGEOUT and creates the tapecopies for migration
 */
CREATE OR REPLACE PROCEDURE internalPutDoneFunc (cfId IN INTEGER,
                                                 fs IN INTEGER,
                                                 context IN INTEGER,
                                                 nbTC IN INTEGER) AS
  tcId INTEGER;
  dcId INTEGER;

BEGIN
  -- if no tape copy or 0 length file, no migration
  -- so we go directly to status STAGED
  IF nbTC = 0 OR fs = 0 THEN
    UPDATE DiskCopy SET status = 0 -- STAGED
     WHERE castorFile = cfId AND status = 6; -- STAGEOUT
  ELSE
    -- update the DiskCopy status to CANBEMIGR
    UPDATE DiskCopy SET status = 10 -- CANBEMIGR
     WHERE castorFile = cfId AND status = 6 -- STAGEOUT
     RETURNING id INTO dcId;
    -- Create TapeCopies
    FOR i IN 1..nbTC LOOP
      INSERT INTO TapeCopy (id, copyNb, castorFile, status)
           VALUES (ids_seq.nextval, i, cfId, 0) -- TAPECOPY_CREATED
      RETURNING id INTO tcId;
      INSERT INTO Id2Type (id, type) VALUES (tcId, 30); -- OBJ_TapeCopy
    END LOOP;
    -- Restart any waiting Disk2DiskCopies
      UPDATE /*+ INDEX (SubRequest) */ SubRequest -- SUBREQUEST_RESTART
         SET status = 1, lastModificationTime = getTime(), parent = 0
       WHERE status = 5 -- WAIT_SUBREQ
         AND parent IN (SELECT id from SubRequest WHERE diskCopy = dcId AND status IN (3, 6));
  END IF;
  -- If we are a real PutDone (and not a put outside of a prepareToPut/Update)
  -- then we have to archive the original prepareToPut/Update subRequest
  IF context = 2 THEN
    DECLARE
      srId NUMBER;
    BEGIN
      SELECT SubRequest.id INTO srId
        FROM SubRequest, 
         (SELECT id from StagePrepareToPutRequest UNION ALL
          SELECT id from StagePrepareToUpdateRequest) Request
       WHERE SubRequest.castorFile = cfId
         AND SubRequest.request = Request.id
         AND SubRequest.status = 6;
      archiveSubReq(srId);
      EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL; --Ignore the missing subrequest
    END;
  END IF;
END;


/* PL/SQL method putDoneFunc */
CREATE OR REPLACE PROCEDURE putDoneFunc (cfId IN INTEGER,
                                         fs IN INTEGER,
                                         context IN INTEGER) AS
  nc INTEGER;
BEGIN
  -- get number of TapeCopies to create
  SELECT nbCopies INTO nc FROM FileClass, CastorFile
   WHERE CastorFile.id = cfId AND CastorFile.fileClass = FileClass.id;
  -- and execute the internal putDoneFunc with the number of TapeCopies to be created
  internalPutDoneFunc(cfId, fs, context, nc);
END;


/* PL/SQL method implementing prepareForMigration */
CREATE OR REPLACE PROCEDURE prepareForMigration (srId IN INTEGER,
                                                 fs IN INTEGER,
                                                 ts IN NUMBER,
                                                 fId OUT NUMBER,
                                                 nh OUT VARCHAR2,
                                                 userId OUT INTEGER,
                                                 groupId OUT INTEGER,
                                                 errorCode OUT INTEGER) AS
  cfId INTEGER;
  dcId INTEGER;
  fsId INTEGER;
  scId INTEGER;
  realFileSize INTEGER;
  unused INTEGER;
  contextPIPP INTEGER;
BEGIN
  errorCode := 0;
  -- get CastorFile
  SELECT castorFile, diskCopy INTO cfId, dcId FROM SubRequest where id = srId;
  -- Determine the context (Put inside PrepareToPut or not)
  BEGIN
    -- check that we are a Put or an Update
    SELECT Request.id INTO unused
      FROM SubRequest,
         (SELECT id FROM StagePutRequest UNION ALL
          SELECT id FROM StageUpdateRequest) Request
     WHERE SubRequest.id = srId
       AND Request.id = SubRequest.request;
    BEGIN
      -- check that there is a PrepareToPut going on
      SELECT SubRequest.diskCopy INTO unused
        FROM SubRequest,
         (SELECT id FROM StagePrepareToPutRequest UNION ALL
          SELECT id FROM StagePrepareToUpdateRequest) Request
       WHERE SubRequest.CastorFile = cfId
         AND Request.id = SubRequest.request
         AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 12, 13, 14);  -- All but FINISHED, FAILED_FINISHED, ARCHIVED
      -- if we got here, we are a Put inside a PrepareToPut
      contextPIPP := 0;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- here we are a standalone Put
      contextPIPP := 1;
    END;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- here we are a PutDone
    contextPIPP := 2;
  END;
  
  IF contextPIPP != 2 THEN
    -- Check whether the diskCopy is still in STAGEOUT. If not, the file
    -- was deleted via stageRm while being written to. Thus, we should just give up
    BEGIN
      SELECT status INTO unused
        FROM DiskCopy WHERE id = dcId AND status = 6; -- STAGEOUT
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- so we are in the case, we give up
      errorCode := 1;
      -- but we still would like to have the fileId and nameserver
      -- host for logging reasons
      SELECT fileId, nsHost INTO fId, nh
        FROM CastorFile WHERE id = cfId;
      RETURN;
    END;
    -- now we can safely update CastorFile's file size
    UPDATE CastorFile SET fileSize = fs, lastUpdateTime = ts 
     WHERE id = cfId AND (lastUpdateTime is NULL or ts > lastUpdateTime); 
    -- if ts < lastUpdateTime, we were late and another job already updated the
    -- CastorFile. But we still need to call updateFsFileClosed() so we go on
  END IF;
  -- Take a lock on the CastorFile. Together with triggers, this insures that
  -- we are the only ones to deal with its copies.
  -- We also get here the real file size too because fs = 0 for a PutDone
  SELECT fileId, nsHost, fileSize INTO fId, nh, realFileSize
    FROM CastorFile
    WHERE id = cfId
    FOR UPDATE;
  -- get uid, gid and svcclass from Request
  SELECT euid, egid, svcClass INTO userId, groupId, scId
    FROM SubRequest,
      (SELECT euid, egid, id, svcClass from StagePutRequest UNION ALL
       SELECT euid, egid, id, svcClass from StageUpdateRequest UNION ALL
       SELECT euid, egid, id, svcClass from StagePutDoneRequest) Request
   WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
  
  IF contextPIPP != 2 THEN   
    -- any Put, either standalone or inside PrepareToPut
    SELECT fileSystem into fsId from DiskCopy
     WHERE castorFile = cfId AND status = 6;
    updateFsFileClosed(fsId);
  END IF;  

  -- archive Subrequest
  archiveSubReq(srId);
  -- If not a put inside a PrepareToPut/Update, create TapeCopies
  -- and update DiskCopy status
  IF contextPIPP != 0 THEN
    putDoneFunc(cfId, realFileSize, contextPIPP);
  END IF;
  -- If put inside PrepareToPut/Update, restart any PutDone currently
  -- waiting on this put/update
  IF contextPIPP = 0 THEN
    UPDATE SubRequest SET status = 1, parent = 0 -- RESTART
     WHERE id IN
      (SELECT SubRequest.id FROM SubRequest, Id2Type
        WHERE SubRequest.request = Id2Type.id
          AND Id2Type.type = 39       -- PutDone
          AND SubRequest.castorFile = cfId
          AND SubRequest.status = 5); -- WAITSUBREQ
  END IF;
  COMMIT;
END;


/* PL/SQL method implementing fileRecalled */
CREATE OR REPLACE PROCEDURE fileRecalled(tapecopyId IN INTEGER) AS
  SubRequestId NUMBER;
  dci NUMBER;
  fsId NUMBER;
  reqType NUMBER;
  nbTC NUMBER(2);
  cfid NUMBER;
  fcnbcopies NUMBER; --number of tapecopies in fileclass
BEGIN
  SELECT SubRequest.id, DiskCopy.id, CastorFile.id, FileClass.nbcopies
    INTO SubRequestId, dci, cfid, fcnbcopies
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile, FileClass
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND FileClass.id = Castorfile.fileclass
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2;
   
  SELECT type INTO reqType FROM Id2Type WHERE id =
    (SELECT request FROM SubRequest WHERE id = subRequestId);

  UPDATE DiskCopy SET status = decode(reqType, 119,6, 0)  -- DISKCOPY_STAGEOUT if OBJ_StageRepackRequest, else DISKCOPY_STAGED 
   WHERE id = dci RETURNING fileSystem INTO fsid;
  -- delete any previous failed diskcopy for this castorfile (due to failed recall attempts for instance)
  DELETE FROM Id2Type WHERE id IN (SELECT id FROM DiskCopy WHERE castorFile = cfId AND status = 4);
  DELETE FROM DiskCopy WHERE castorFile = cfId AND status = 4;   -- FAILED
  -- mark as invalid any previous diskcopy in disabled filesystems, which may have triggered this recall
  UPDATE DiskCopy SET status = 7  -- INVALID
   WHERE fileSystem IN (SELECT id FROM FileSystem WHERE status = 2)  -- DISABLED
     AND castorFile = cfId
     AND status = 0;  -- STAGED
  
  -- Repack handling:
  -- create the number of tapecopies for waiting subrequests and update their diskcopy.
  IF reqType = 119 THEN      -- OBJ_StageRepackRequest
    -- how many do we have to create ?
     SELECT count(StageRepackRequest.repackvid) INTO nbTC FROM subrequest, StageRepackRequest 
      WHERE subrequest.castorfile = cfid
        AND SubRequest.request = StageRepackRequest.id
        AND subrequest.status in (4,5,6);
	
     -- we are not allowed to create more Tapecopies than in the fileclass specified
     IF fcnbcopies < nbTC THEN 
       nbTC := fcnbcopies;
     END IF;
	
     -- we need to update other subrequests with the diskcopy
     UPDATE SubRequest SET diskcopy = dci 
      WHERE subrequest.castorfile = cfid
        AND subrequest.status in (4,5,6)
        AND SubRequest.request in 
       		(SELECT id FROM StageRepackRequest); 
	   
     -- create the number of tapecopies for the files
    internalPutDoneFunc(cfid, fsId, 0, nbTC);
    /** to avoid additional scheduling of the waiting subrequest(s) 
        (because it is now in 1), we do it
     */
    UPDATE subrequest SET status = 12 -- SUBREQUEST_REPACK
     WHERE subrequest.castorfile = cfid
       AND subrequest.status IN (1,4); -- SUBREQUEST_RESTART
  ELSE 
    IF subRequestId IS NOT NULL THEN
      UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
       WHERE id = SubRequestId; 
      UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
       WHERE parent = SubRequestId;
    END IF;
  END IF;
  updateFsFileClosed(fsId);
END;


/* PL/SQL method implementing selectCastorFile */
CREATE OR REPLACE PROCEDURE selectCastorFile (fId IN INTEGER,
                                              nh IN VARCHAR2,
                                              sc IN INTEGER,
                                              fc IN INTEGER,
                                              fs IN INTEGER,
                                              fn IN VARCHAR2,
                                              rid OUT INTEGER,
                                              rfs OUT INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  BEGIN
    -- try to find an existing file
    SELECT id, fileSize INTO rid, rfs FROM CastorFile
      WHERE fileId = fid AND nsHost = nh;
    -- update lastAccess time
    UPDATE CastorFile SET LastAccessTime = getTime(),
                          nbAccesses = nbAccesses + 1,
                          lastKnownFileName = fn
      WHERE id = rid;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- insert new row
    INSERT INTO CastorFile (id, fileId, nsHost, svcClass, fileClass, fileSize,
                            creationTime, lastAccessTime, nbAccesses, lastKnownFileName)
      VALUES (ids_seq.nextval, fId, nh, sc, fc, fs, getTime(), getTime(), 1, fn)
      RETURNING id, fileSize INTO rid, rfs;
    INSERT INTO Id2Type (id, type) VALUES (rid, 2); -- OBJ_CastorFile
  END;
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  -- retry the select since a creation was done in between
  SELECT id, fileSize INTO rid, rfs FROM CastorFile
    WHERE fileId = fid AND nsHost = nh;
END;

/* PL/SQL method implementing resetStream */
CREATE OR REPLACE PROCEDURE resetStream (sid IN INTEGER) AS
  nbRes NUMBER;
  unused Stream%ROWTYPE;
BEGIN
  BEGIN
    -- First lock the stream
    SELECT * INTO unused from Stream where id = sid FOR UPDATE;
    -- Selecting any column with hint FIRST_ROW and relying
    -- on the exception mechanism in case nothing is found is
    -- far better than issuing a SELECT count(*) because ORACLE
    -- will then ignore the FIRST_ROWS and take ages...
    SELECT /*+ FIRST_ROWS */ Tapecopy.id INTO nbRes
      FROM Stream2TapeCopy, TapeCopy
     WHERE Stream2TapeCopy.Parent = sid
       AND Stream2TapeCopy.Child = TapeCopy.id
       AND status = 2 -- TAPECOPY_WAITINSTREAMS
       AND ROWNUM < 2;
    -- We'we found one, update stream status
    UPDATE Stream SET status = 0, tape = 0, lastFileSystemChange = 0
     WHERE id = sid; -- STREAM_PENDING
  EXCEPTION  WHEN NO_DATA_FOUND THEN
    -- We've found nothing, delete stream
    DELETE FROM Stream2TapeCopy WHERE Parent = sid;
    DELETE FROM Id2Type WHERE id = sid;
    DELETE FROM Stream WHERE id = sid;
  END;
  -- in any case, unlink tape and stream
  UPDATE Tape SET Stream = 0 WHERE Stream = sid;
END;


/* PL/SQL method implementing anySegmentsForTape */
CREATE OR REPLACE PROCEDURE anySegmentsForTape
(tapeId IN INTEGER, nb OUT INTEGER) AS
BEGIN
  SELECT count(*) INTO nb FROM Segment
  WHERE Segment.tape = tapeId
    AND Segment.status = 0;
  IF nb > 0 THEN
    UPDATE Tape SET status = 3 -- WAITMOUNT
    WHERE id = tapeId;
  END IF;
END;


/* PL/SQL method implementing segmentsForTape */
CREATE OR REPLACE PROCEDURE segmentsForTape
(tapeId IN INTEGER, segments OUT castor.Segment_Cur) AS
  segs "numList";
BEGIN
  SELECT Segment.id BULK COLLECT INTO segs FROM Segment
   WHERE Segment.tape = tapeId
     AND Segment.status = 0 FOR UPDATE;
  IF segs.COUNT > 0 THEN
    UPDATE Tape SET status = 4 -- MOUNTED
     WHERE id = tapeId;
    UPDATE Segment set status = 7 -- SELECTED
     WHERE id MEMBER OF segs;
  END IF;
  OPEN segments FOR SELECT * FROM Segment 
                     where id MEMBER OF segs;
END;


/* PL/SQL method implementing getUpdateDone */
CREATE OR REPLACE PROCEDURE getUpdateDoneProc
(subReqId IN NUMBER) AS
BEGIN
  archiveSubReq(subReqId);
END;


/* PL/SQL method implementing getUpdateFailed */
CREATE OR REPLACE PROCEDURE getUpdateFailedProc
(srId IN NUMBER) AS
BEGIN
  UPDATE SubRequest SET status = 7 -- FAILED
   WHERE id = srId;
END;


/* PL/SQL method implementing failedSegments */
CREATE OR REPLACE PROCEDURE failedSegments
(segments OUT castor.Segment_Cur) AS
BEGIN
  OPEN segments FOR SELECT * FROM Segment
                     WHERE Segment.status = 6; -- SEGMENT_FAILED
END;


/* PL/SQL method implementing stageRelease */
CREATE OR REPLACE PROCEDURE stageRelease (fid IN INTEGER,
                                          nh IN VARCHAR2,
                                          ret OUT INTEGER) AS
  cfId INTEGER;
  nbRes INTEGER;
BEGIN
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO cfId FROM CastorFile
   WHERE fileId = fid AND nsHost = nh FOR UPDATE;
  -- check if removal is possible for TapeCopies
  SELECT count(*) INTO nbRes FROM TapeCopy
   WHERE status = 3 -- TAPECOPY_SELECTED
     AND castorFile = cfId;
  IF nbRes > 0 THEN
    -- We found something, thus we cannot recreate
    ret := 1;
    RETURN;
  END IF;
  -- check if recreation is possible for SubRequests
  SELECT count(*) INTO nbRes FROM SubRequest
   WHERE status != 11 AND castorFile = cfId;   -- ARCHIVED
  IF nbRes > 0 THEN
    -- We found something, thus we cannot recreate
    ret := 2;
    RETURN;
  END IF;
  -- set DiskCopies to GCCANDIDATE
  UPDATE DiskCopy SET status = 7 -- INVALID
   WHERE castorFile = cfId AND status = 0; -- STAGED
  ret := 0;
END;


/* PL/SQL method implementing stageRm */
CREATE OR REPLACE PROCEDURE stageRm (fid IN INTEGER,
                                     nh IN VARCHAR2,
                                     svcClassId IN INTEGER,
                                     ret OUT INTEGER) AS
  cfId INTEGER;
  scId INTEGER;
  nbRes INTEGER;
  dcsToRm "numList";
BEGIN
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO cfId FROM CastorFile
   WHERE fileId = fid AND nsHost = nh FOR UPDATE;
  -- First select involved diskCopies
  scId := svcClassId;
  IF scId > 0 THEN
    SELECT DC.id BULK COLLECT INTO dcsToRm
      FROM DiskCopy DC, FileSystem, DiskPool2SvcClass DP2SC
     WHERE DC.castorFile = cfId
       AND DC.status IN (0, 6, 10)  -- STAGED, STAGEOUT, CANBEMIGR
       AND DC.fileSystem = FileSystem.id
       AND FileSystem.diskPool = DP2SC.parent
       AND DP2SC.child = scId;
    -- Check whether something else is left: if not, do as
    -- we are performing a stageRm everywhere
    SELECT count(*) INTO nbRes FROM DiskCopy
     WHERE castorFile = cfId
       AND status in (0, 6, 10)  -- STAGED, STAGEOUT, CANBEMIGR
       AND id not in (SELECT * FROM TABLE(dcsToRm));
    IF nbRes = 0 THEN
      scId := 0;
    END IF;
  END IF;
  IF scId = 0 THEN
    SELECT id BULK COLLECT INTO dcsToRm
      FROM DiskCopy
     WHERE castorFile = cfId
       AND status IN (0, 5, 6, 10, 11);  -- STAGED, WAITFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
  END IF;
  -- Now perform the stageRm. scId is used hereafter as flag
  -- to determine whether to perform full cleanup or not
  IF scId = 0 THEN
    -- check if removal is possible for Migration
    SELECT count(*) INTO nbRes FROM TapeCopy
     WHERE status IN (0, 1, 2, 3) -- CREATED, TOBEMIGRATED, WAITINSTREAMS, SELECTED
       AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot remove
      ret := 1;
      RETURN;
    END IF;
    -- check if removal is possible for Disk2DiskCopy
    SELECT count(*) INTO nbRes FROM DiskCopy
    WHERE status = 1 -- DISKCOPY_WAITDISK2DISKCOPY
      AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot remove
      ret := 2;
      RETURN;
    END IF;
  END IF;
  -- mark all get/put requests for those diskcopies as failed
  -- so that clients eventually get an answer;
  -- don't touch recalls for the moment
  FOR sr IN (SELECT id, status FROM SubRequest
              WHERE diskcopy IN (SELECT * FROM TABLE(dcsToRm))) LOOP
    IF sr.status IN (0, 1, 2, 3, 5, 6, 7, 10, 13, 14) THEN  -- All but FINISHED, FAILED_FINISHED, ARCHIVED
      UPDATE SubRequest SET status = 7, parent = 0 WHERE id = sr.id;  -- FAILED
    END IF;
  END LOOP;
  -- Set selected DiskCopies to INVALID. In any case keep
  -- WAITTAPERECALL diskcopies so that recalls can continue.
  -- Note that WAITFS and WAITFS_SCHEDULING DiskCopies don't exist on disk
  -- so they will only be taken by the cleaning daemon for the failed DCs.
  UPDATE DiskCopy SET status = 7 -- INVALID
   WHERE id IN (SELECT * FROM TABLE(dcsToRm));
  ret := 0;
  IF scId = 0 THEN
    -- Stop ongoing recalls if stageRm either everywhere or the only available diskcopy.
    -- This is not entirely clean: a proper operation here should be to
    -- drop the SubRequest waiting for recall but keep the recall if somebody
    -- else is doing it, and taking care of other WAITSUBREQ requests as well...
    -- but it's fair enough, provided that the last stageRm will cleanup everything.
    DECLARE
      segId INTEGER;
      unusedIds "numList";
    BEGIN
      -- First lock all segments for the file
      SELECT segment.id BULK COLLECT INTO unusedIds
        FROM Segment, TapeCopy
       WHERE TapeCopy.castorfile = cfId
         AND TapeCopy.id = Segment.copy
      FOR UPDATE;
      -- Check whether we have any segment in SELECTED
      SELECT segment.id INTO segId
        FROM Segment, TapeCopy
       WHERE TapeCopy.castorfile = cfId
         AND TapeCopy.id = Segment.copy
         AND Segment.status = 7 -- SELECTED
         AND ROWNUM < 2;
      -- Something is running, so give up
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Nothing running
      deleteTapeCopies(cfId);
      -- Delete the DiskCopies
      UPDATE DiskCopy
         SET status = 7  -- INVALID
       WHERE status = 2  -- WAITTAPERECALL
         AND castorFile = cfId;
      -- Mark the 'recall' SubRequests as failed
      -- so that clients get an answer
      UPDATE SubRequest SET status = 7   -- FAILED
       WHERE castorFile = cfId and status = 4;
    END;
  END IF;
END;

CREATE OR REPLACE PACKAGE castorGC AS
  TYPE GCItem IS RECORD (dcId INTEGER, fileSize NUMBER, utility NUMBER);
  TYPE GCItem_Table is TABLE OF GCItem;
  TYPE GCItem_Cur IS REF CURSOR RETURN GCItem;
  --TYPE GCItem_CurTable is VARRAY(10) OF GCItem_Cur;
  TYPE policiesList IS TABLE OF VARCHAR2(2048);
  TYPE SelectFiles2DeleteLine IS RECORD (
        path VARCHAR2(2048),
        id NUMBER,
	fileId NUMBER,
	nsHost VARCHAR2(2048));
  TYPE SelectFiles2DeleteLine_Cur IS REF CURSOR RETURN SelectFiles2DeleteLine;
END castorGC;


/* PL/SQL method implementing selectFiles2Delete */
CREATE OR REPLACE PROCEDURE selectFiles2Delete
(diskServerName IN VARCHAR2,
 files OUT castorGC.SelectFiles2DeleteLine_Cur) AS
BEGIN
  INSERT INTO SelectFiles2DeleteProcHelper
    SELECT FileSystem.id, FileSystem.mountPoint
      FROM FileSystem, DiskServer
     WHERE FileSystem.diskServer = DiskServer.id
       AND DiskServer.name = diskServerName;
  OPEN files FOR
    SELECT SelectFiles2DeleteProcHelper.path||DiskCopy.path, DiskCopy.id,
	   Castorfile.fileid, Castorfile.nshost
      FROM DiskCopy, SelectFiles2DeleteProcHelper, Castorfile
     WHERE (DiskCopy.status = 8
           OR DiskCopy.status = 9 AND DiskCopy.creationTime < getTime() - 1800)
           -- for failures recovery we also take all DiskCopies which were
           -- already selected but got stuck somehow and didn't get removed
           -- after 30 mins. The creationTime field is actually updated
           -- for this purpose when status changes to 9 in updateFiles2Delete.
       AND DiskCopy.fileSystem = SelectFiles2DeleteProcHelper.id
       AND DiskCopy.castorfile = Castorfile.id
       FOR UPDATE;
END;


/* PL/SQL method implementing updateFiles2Delete */
CREATE OR REPLACE PROCEDURE updateFiles2Delete
(dcIds IN castor."cnumList") AS
BEGIN
  FOR i in dcIds.FIRST .. dcIds.LAST LOOP
    UPDATE DiskCopy
       set status = 9, -- BEING_DELETED
           creationTime = getTime()
           -- note that this is an over-use of the field, but it's needed in selectFiles2Delete
     WHERE id = dcIds(i);
  END LOOP;
END;


/* PL/SQL method to delete a CastorFile only when no Disk|TapeCopies are left for it */
/* Internally used in filesDeletedProc, putFailedProc and deleteOutOfDateDiskCopies */
CREATE OR REPLACE PROCEDURE deleteCastorFile(cfId IN NUMBER) AS
  nb NUMBER;
BEGIN
  -- See whether the castorfile has no other DiskCopy
  SELECT count(*) INTO nb FROM DiskCopy
   WHERE castorFile = cfId;
  -- If any DiskCopy, give up
  IF nb = 0 THEN
    -- See whether the castorfile has any TapeCopy
    SELECT count(*) INTO nb FROM TapeCopy
     WHERE castorFile = cfId AND status != 6; -- FAILED
    -- If any TapeCopy, give up
    IF nb = 0 THEN
      -- See whether the castorfile has any pending SubRequest
      SELECT count(*) INTO nb FROM SubRequest
       WHERE castorFile = cfId
         AND status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 13, 14);   -- All but FINISHED, FAILED_FINISHED, ARCHIVED
      -- If any SubRequest, give up
      IF nb = 0 THEN
        DECLARE
          fid NUMBER;
          fc NUMBER;
          nsh VARCHAR2(2048);
        BEGIN
          -- Delete the failed TapeCopies
          deleteTapeCopies(cfId);
          -- Delete the CastorFile
          DELETE FROM id2Type WHERE id = cfId;
          DELETE FROM CastorFile WHERE id = cfId
            RETURNING fileId, nsHost, fileClass
            INTO fid, nsh, fc;
          -- check whether this file potentially had TapeCopies
          SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
          IF nb = 0 THEN
            -- This castorfile was created with no TapeCopy
            -- So removing it from the stager means erasing
            -- it completely. We should thus also remove it
            -- from the name server
            INSERT INTO FilesDeletedProcOutput VALUES (fid, nsh);
          END IF;
        END;
      END IF;
    END IF;
  END IF;
END;


/* Search and delete old diskCopies in bad states */
CREATE OR REPLACE PROCEDURE deleteOutOfDateDiskCopies(timeOut IN NUMBER) AS
  dcIds "numList";
  cfIds "numList";
  ct INTEGER;
  c INTEGER;
BEGIN
  -- select INVALID diskcopies without filesystem (they can exist after a
  -- stageRm that came before the diskcopy had been created on disk) and ALL FAILED
  -- ones (coming from failed recalls or failed removals from the gcDaemon).
  -- Note that we don't select INVALID diskcopies from recreation of files
  -- because they are taken by the standard GC as they physically exist on disk.
  SELECT id
    BULK COLLECT INTO dcIds 
    FROM DiskCopy
   WHERE (status = 4 OR (status = 7 AND fileSystem = 0))
     AND creationTime < getTime() - timeOut;
  SELECT UNIQUE castorFile
    BULK COLLECT INTO cfIds
    FROM DiskCopy
   WHERE id IN (SELECT * FROM TABLE(dcIds));
  -- drop the DiskCopies
  DELETE FROM Id2Type WHERE id IN (SELECT * FROM TABLE(dcIds));
  DELETE FROM DiskCopy WHERE id IN (SELECT * FROM TABLE(dcIds));
  COMMIT;
  -- maybe delete the CastorFiles if nothing is left for them
  IF cfIds.COUNT > 0 THEN
    ct := 0;
    FOR c IN cfIds.FIRST..cfIds.LAST LOOP
      deleteCastorFile(cfIds(c));
      ct := ct + 1;
      IF ct = 1000 THEN
        -- commit every 1000, don't pause
        ct := 0;
        COMMIT;
      END IF;
    END LOOP;
    COMMIT;
  END IF;
END;


/*
 * PL/SQL method implementing filesDeleted
 * Note that we don't increase the freespace of the fileSystem.
 * This is done by the monitoring daemon, that knows the
 * exact amount of free space. However, we decrease the
 * spaceToBeFreed counter so that a next GC knows the status
 * of the FileSystem.
 * THIS PROCEDURE SHOULD ONLY BE CALLED FOR DiskCopies
 * THAT ALL BELONG TO THE SAME DISKSERVER.
 * Otherwise, deadlocks will be created. Between 2 calls
 * for different diskservers, a commit should be done.
 * dcIds gives the list of diskcopies to delete.
 * fileIds returns the list of castor files to be removed
 * from the name server
 */
CREATE OR REPLACE PROCEDURE filesDeletedProc
(dcIds IN castor."cnumList",
 fileIds OUT castor.FileList_Cur) AS
  cfId NUMBER;
  fsId NUMBER;
  fsize NUMBER;
  isFirst NUMBER;
BEGIN
  isFirst := 1;
  IF dcIds.COUNT > 0 THEN
  -- Loop over the deleted files
  FOR i in dcIds.FIRST .. dcIds.LAST LOOP
    SELECT castorFile, fileSystem INTO cfId, fsId
      FROM DiskCopy WHERE id = dcIds(i);
    LOOP
      DECLARE
        LockError EXCEPTION;
        PRAGMA EXCEPTION_INIT (LockError, -54);
      BEGIN
        -- Try to lock the Castor File and retrieve size
        SELECT fileSize INTO fsize FROM CastorFile where id = cfID FOR UPDATE NOWAIT;
        -- we got the lock, exit the loop
        EXIT;
      EXCEPTION WHEN LockError THEN
        -- then commit what we did to remove the dead lock
        COMMIT;
        -- and try again after some time
        dbms_lock.sleep(0.2);
      END;
    END LOOP;
    -- delete the DiskCopy
    DELETE FROM Id2Type WHERE id = dcIds(i);
    DELETE FROM DiskCopy WHERE id = dcIds(i);
    -- against deadlock
    -- I need a lock on the diskserver if I need more than one filesystem on it
    IF isFirst = 1 THEN
      DECLARE
        dsId NUMBER;
        unused NUMBER;
      BEGIN
        SELECT diskServer INTO dsId FROM FileSystem WHERE id = fsId;
        SELECT id INTO unused FROM DiskServer WHERE id=dsId FOR UPDATE;
        isFirst := 0;	
      END;
    END IF; 
    -- update the FileSystem
    UPDATE FileSystem
       SET spaceToBeFreed = spaceToBeFreed - fsize
     WHERE id = fsId;
    -- clean the CastorFile
    deleteCastorFile(cfId);
  END LOOP;
 END IF;
 OPEN fileIds FOR SELECT * FROM FilesDeletedProcOutput;
END;


/*
 * PL/SQL method removing completely a file from the stager
 * including all its related objects (diskcopy, tapecopy, segments...)
 * The given files are supposed to already have been removed from the
 * name server
 * Note that we don't increase the freespace of the fileSystem.
 * This is done by the monitoring daemon, that knows the
 * exact amount of free space. However, we decrease the
 * spaceToBeFreed counter so that a next GC knows the status
 * of the FileSystem
 * THIS PROCEDURE SHOULD ONLY BE CALLED FOR DiskCopies
 * THAT ALL BELONG TO THE SAME DISKSERVER.
 * Otherwise, deadlocks will be created. Between 2 calls
 * for different diskservers, a commit should be done.
 * fsIds gives the list of files to delete.
 */
CREATE OR REPLACE PROCEDURE filesClearedProc
(cfIds IN castor."cnumList") AS
  fsize NUMBER;
  fsid NUMBER;
  isFirst NUMBER;
BEGIN
  isFirst := 1;
  -- Loop over the deleted files
  FOR i in cfIds.FIRST .. cfIds.LAST LOOP
    -- Lock the Castor File and retrieve size
    SELECT fileSize INTO fsize FROM CastorFile WHERE id = cfIds(i);
    -- delete the DiskCopies
    FOR d in (SELECT id FROM Diskcopy WHERE castorfile = cfIds(i)) LOOP
      DELETE FROM Id2Type WHERE id = d.id;
      DELETE FROM DiskCopy WHERE id = d.id
        RETURNING fileSystem INTO fsId;
      -- against deadlock
      -- I need a lock on the diskserver if I need more than one filesystem on it
      IF isFirst = 1 THEN
        DECLARE
            dsId NUMBER;
          unused NUMBER;
        BEGIN
          SELECT diskServer INTO dsId FROM FileSystem WHERE id = fsId;
          SELECT id INTO unused FROM DiskServer WHERE id=dsId FOR UPDATE;
          isFirst := 0;	
        END;
      END IF; 
      -- update the FileSystem
      UPDATE FileSystem
         SET spaceToBeFreed = spaceToBeFreed - fsize
       WHERE id = fsId;
    END LOOP;
    -- put SubRequests into FAILED (for non FINISHED ONES)
    UPDATE SubRequest SET status = 7, parent = 0 WHERE castorfile = cfIds(i) AND status < 7;
    -- TapeCopy part
    deleteTapeCopies(cfIds(i));
    -- Castorfile
    DELETE FROM Id2Type WHERE id = cfIds(i);
    DELETE FROM CastorFile WHERE id = cfIds(i);
  END LOOP;
END;


/* PL/SQL method implementing filesDeletionFailedProc
 * THIS PROCEDURE SHOULD ONLY BE CALLED FOR DiskCopies
 * THAT ALL BELONG TO THE SAME DISKSERVER.
 * Otherwise, deadlocks will be created. Between 2 calls
 * for different diskservers, a commit should be done.
 */
CREATE OR REPLACE PROCEDURE filesDeletionFailedProc
(dcIds IN castor."cnumList") AS
  cfId NUMBER;
  fsId NUMBER;
  fsize NUMBER;
  isFirst NUMBER;
BEGIN
  isFirst := 1;
  IF dcIds.COUNT > 0 THEN
  -- Loop over the deleted files
  FOR i in dcIds.FIRST .. dcIds.LAST LOOP
    -- set status of DiskCopy to FAILED
    UPDATE DiskCopy SET status = 4 -- FAILED
     WHERE id = dcIds(i)
    RETURNING fileSystem, castorFile INTO fsId, cfId;
    -- Retrieve the file size
    SELECT fileSize INTO fsize FROM CastorFile where id = cfId;
    -- against deadlock
    -- I need a lock on the diskserver if I need more than one filesystem on it
    IF isFirst = 1 THEN
      DECLARE
        dsId NUMBER;
        unused NUMBER;
      BEGIN
        SELECT diskServer INTO dsId FROM FileSystem WHERE id = fsId;
        SELECT id INTO unused FROM DiskServer WHERE id=dsId FOR UPDATE;
        isFirst := 0;	
      END;
    END IF; 
    -- update the FileSystem
    UPDATE FileSystem
       SET spaceToBeFreed = spaceToBeFreed - fsize
     WHERE id = fsId;
  END LOOP;
 END IF;
END;


/* PL/SQL method implementing putFailedProc */
CREATE OR REPLACE PROCEDURE putFailedProc(srId IN NUMBER) AS
  dcId INTEGER;
  fsId INTEGER;
  cfId INTEGER;
  unused INTEGER;
BEGIN
  -- Set SubRequest in FAILED status
  UPDATE SubRequest
     SET status = 7 -- FAILED
   WHERE id = srId
  RETURNING diskCopy, castorFile
    INTO dcId, cfId;
  SELECT fileSystem INTO fsId FROM DiskCopy WHERE id = dcId;
  -- take file closing into account
  IF fsId > 0 THEN
    updateFsFileClosed(fsId);
  END IF;
  -- Determine the context (Put inside PrepareToPut ?)
  BEGIN
    -- check that there is a PrepareToPut going on
    SELECT SubRequest.id INTO unused
      FROM StagePrepareToPutRequest, SubRequest
     WHERE SubRequest.CastorFile = cfId
       AND StagePrepareToPutRequest.id = SubRequest.request
       AND SubRequest.status = 6; -- READY
    -- the select worked out, so we have a prepareToPut
    -- In such a case, we do not cleanup DiskCopy and CastorFile
    -- but we have to wake up a potential waiting putDone
    UPDATE SubRequest SET status = 1, parent = 0 -- RESTART
     WHERE id IN
      (SELECT SubRequest.id
        FROM StagePutDoneRequest, SubRequest
       WHERE SubRequest.CastorFile = cfId
         AND StagePutDoneRequest.id = SubRequest.request
         AND SubRequest.status = 5); -- WAITSUBREQ
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- this means we are a standalone put
    -- thus cleanup DiskCopy and maybe the CastorFile
    -- (the physical file is dropped by the job)
    DELETE FROM DiskCopy WHERE id = dcId;
    DELETE FROM Id2Type WHERE id = dcId;
    deleteCastorFile(cfId);
  END;
END;


/*
 * GC policy that mimic the old GC :
 * a mix of oldest first and biggest first
 */
CREATE OR REPLACE FUNCTION defaultGCPolicy
(fsId INTEGER, garbageSize INTEGER)
  RETURN castorGC.GCItem_Cur AS
  result castorGC.GCItem_Cur;
BEGIN
  OPEN result FOR
    SELECT /*+ INDEX(CF) INDEX(DC) */ DC.id, CF.fileSize,
           getTime() - CF.lastAccessTime + greatest(0,86400*ln((CF.fileSize+1)/1024))
           - nvl(DC.gcWeight, 0)   -- optional weight used by SRM2 for advisory pinning
      FROM DiskCopy DC, CastorFile CF
     WHERE CF.id = DC.castorFile
       AND DC.fileSystem = fsId
       AND DC.status = 0  -- STAGED
       AND NOT EXISTS (
         SELECT 'x' FROM SubRequest 
          WHERE DC.status = 0 AND diskcopy = DC.id 
            AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 12, 13, 14))   -- All but FINISHED, FAILED_FINISHED, ARCHIVED
    ORDER BY 3 DESC;
  RETURN result;
END;


/*
 * GC policy that mimic the old GC and takes into account
 * the number of accesses to the file. Namely we garbage
 * collect the oldest and biggest file that had the less
 * accesses but not 0, as a file having 0 accesses will
 * probably be read soon !
 */
CREATE OR REPLACE FUNCTION nopinGCPolicy
(fsId INTEGER, garbageSize INTEGER)
  RETURN castorGC.GCItem_Cur AS
  result castorGC.GCItem_Cur;
BEGIN
  OPEN result FOR
    SELECT /*+ INDEX(CF) INDEX(DC) */ DC.id, CF.fileSize,
           getTime() - CF.LastAccessTime -- oldest first
           + GREATEST(0,86400*LN((CF.fileSize+1)/1024)) -- biggest first
           + CASE CF.nbAccesses
               WHEN 0 THEN 86400 -- non accessed last
               ELSE 20000 * CF.nbAccesses -- most accessed last
             END
           - nvl(DC.gcWeight, 0)   -- optional weight used by SRM2 for advisory pinning
      FROM DiskCopy DC, CastorFile CF
     WHERE CF.id = DC.castorFile
       AND DC.fileSystem = fsId
       AND DC.status = 0 -- STAGED
       AND NOT EXISTS (
         SELECT 'x' FROM SubRequest 
          WHERE DC.status = 0 AND diskcopy = DC.id 
            AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 12, 13, 14))   -- All but FINISHED, FAILED_FINISHED, ARCHIVED
     ORDER BY 3 DESC;
  RETURN result;
END;


/*
 * Runs Garbage collection on the specified FileSystem
 */
CREATE OR REPLACE PROCEDURE garbageCollectFS(fsId INTEGER) AS
  toBeFreed INTEGER;
  freed INTEGER := 0;
  dpID INTEGER;
  policies castorGC.policiesList;
  --cursors castor.castorGC.GCItem_CurTable;
  cur castorGC.GCItem_Cur;
  cur1 castorGC.GCItem_Cur;
  cur2 castorGC.GCItem_Cur;
  cur3 castorGC.GCItem_Cur;
  nextItems castorGC.GCItem_Table := castorGC.GCItem_Table();
  bestCandidate INTEGER;
  bestValue NUMBER;
BEGIN
  -- List policies to be applied
  SELECT UNIQUE svcClass.gcPolicy
         BULK COLLECT INTO policies
    FROM SvcClass, DiskPool2SvcClass, FileSystem
   WHERE FileSystem.id = fsId
     AND DiskPool2SvcClass.Parent = FileSystem.diskPool
     AND SvcClass.Id = DiskPool2SvcClass.Child
     AND length(SvcClass.gcPolicy) IS NOT NULL; -- strange way ORACLE has to deal with empty strings...
  IF policies.COUNT = 0 THEN
    -- no policy defined, nothing to do
    RETURN;
  END IF;

  -- Now get the DiskPool and the maxFree space we want to achieve
  SELECT diskPool, maxFreeSpace * totalSize - free - spaceToBeFreed
    INTO dpId, toBeFreed
    FROM FileSystem
   WHERE FileSystem.id = fsId
     FOR UPDATE;
  -- Don't do anything if toBeFreed <= 0
  IF toBeFreed <= 0 THEN
    COMMIT;
    RETURN;
  END IF;
  UPDATE FileSystem
     SET spaceToBeFreed = spaceToBeFreed + toBeFreed
   WHERE FileSystem.id = fsId;
  -- release the filesystem lock so that other threads can go on
  COMMIT;

  -- Get candidates for each policy
  nextItems.EXTEND(policies.COUNT);
  bestCandidate := -1;
  bestValue := 100000000000;
  IF policies.COUNT > 0 THEN
    EXECUTE IMMEDIATE 'BEGIN :1 := '||policies(policies.FIRST)||'(:2, :3); END;'
      USING OUT cur1, IN fsId, IN toBeFreed;
    FETCH cur1 INTO nextItems(1);
    IF policies.COUNT > 1 THEN
      EXECUTE IMMEDIATE 'BEGIN :1 := '||policies(policies.FIRST+1)||'(:2, :3); END;'
        USING OUT cur2, IN fsId, IN toBeFreed;
      FETCH cur2 INTO nextItems(2);
      IF policies.COUNT > 2 THEN
        EXECUTE IMMEDIATE 'BEGIN :1 := '||policies(policies.FIRST+2)||'(:2, :3); END;'
          USING OUT cur3, IN fsId, IN toBeFreed;
        FETCH cur3 INTO nextItems(3);
      END IF;    
    END IF;    
  END IF;    
  FOR i IN policies.FIRST .. policies.LAST LOOP
    -- maximum 3 policies allowed
    IF i > policies.FIRST+2 THEN EXIT; END IF;
    IF nextItems(i).utility < bestValue THEN
      bestCandidate := i;
      bestValue := nextItems(i).utility;
    END IF;
  END LOOP;
  
  IF bestCandidate <> -1 THEN
    -- Now extract the diskcopies that will be garbaged and
    -- mark them GCCandidate
    LOOP
      -- Mark the DiskCopy
      UPDATE DISKCOPY SET status = 8 -- GCCANDIDATE
       WHERE id = nextItems(bestCandidate).dcId;
      -- Update toBeFreed
      freed := freed + nextItems(bestCandidate).fileSize;
      -- Shall we continue ?
      IF toBeFreed > freed THEN
        IF 1 = bestCandidate THEN
          cur := cur1;
        ELSIF 2 = bestCandidate THEN
          cur := cur2;
        ELSE
          cur := cur3;
        END IF;
        FETCH cur INTO nextItems(bestCandidate);
        EXIT WHEN cur%NOTFOUND;
        -- find next candidate
        bestValue := 100000000000;
        FOR i IN nextItems.FIRST .. nextItems.LAST LOOP
          IF nextItems(i).utility < bestValue THEN
            bestCandidate := i;
            bestValue := nextItems(i).utility;
          END IF;
        END LOOP;
      ELSE
        -- enough space freed
        EXIT;
      END IF;
    END LOOP;
  END IF;

  -- Update Filesystem toBeFreed space to the exact value
  UPDATE FileSystem
     SET spaceToBeFreed = spaceToBeFreed + freed - toBeFreed
   WHERE FileSystem.id = fsId;
  -- Close cursors
  IF policies.COUNT > 0 THEN
    CLOSE cur1;
    IF policies.COUNT > 1 THEN
      CLOSE cur2;
      IF policies.COUNT > 2 THEN
        CLOSE cur3;
      END IF;
    END IF;
  END IF;
  -- commit everything
  COMMIT;
END;


/*
 * Runs Garbage collection of invalid DiskCopies on the given FileSystem
 */
CREATE OR REPLACE PROCEDURE garbageCollectInvalidDC(fsId INTEGER) AS
  sumSize NUMBER;
  free NUMBER;
  minFree NUMBER;
  dcIds "numList";
  fileSizes "numList";
BEGIN
  -- GC the INVALID unused diskCopies on this filesystem
  SELECT DC.id, CF.fileSize
    BULK COLLECT INTO dcIds, fileSizes
    FROM DiskCopy DC, CastorFile CF
   WHERE DC.castorFile = CF.id
     AND DC.fileSystem = fsId
     AND DC.status = 7  -- INVALID
     AND NOT EXISTS (
       SELECT 'x' FROM SubRequest
        WHERE SubRequest.diskcopy = DC.id
          AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 12, 13, 14));  -- All but FINISHED, FAILED_FINISHED, ARCHIVED

  IF dcIds.COUNT > 0 THEN
    UPDATE DiskCopy SET status = 8  -- GCCANDIDATE
     WHERE id in (SELECT * FROM Table(dcIds));
    -- compute and update the filesystem's freed space
    sumSize := 0;
    FOR i in fileSizes.FIRST .. fileSizes.LAST LOOP
      sumSize := sumSize + fileSizes(i);
    END LOOP;
    UPDATE FileSystem
       SET spaceToBeFreed = spaceToBeFreed + sumSize
     WHERE id = fsId;
    -- commit the cleanup of this filesystem 
    COMMIT;
  END IF;
END;


/*
 * Runs Garbage collection anywhere needed.
 * This is continuously run as a DBMS JOB.
 */
CREATE OR REPLACE PROCEDURE garbageCollect AS
BEGIN
  FOR fs IN (SELECT id FROM FileSystem) LOOP
    BEGIN
      -- run the GCs
      garbageCollectInvalidDC(fs.id);
      garbageCollectFS(fs.id);
      -- yield to other jobs/transactions
      DBMS_LOCK.sleep(seconds => 1.0);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;            -- ignore and go on
    END;
  END LOOP;
END;


DECLARE
  nb NUMBER;
BEGIN
  -- if the job already exists delete it
  SELECT count(*) INTO nb FROM user_scheduler_jobs WHERE job_name = 'GARBAGECOLLECTJOB';
  IF nb != 0 THEN
    DBMS_SCHEDULER.DROP_JOB(JOB_NAME => 'GARBAGECOLLECTJOB', FORCE => TRUE);
  END IF;
  -- creates a db job to be run every 15 mins for the garbage collector.
  -- Such interval allows for ~300 filesystem to be processed for each round
  -- under normal conditions.
  -- XXX In case more are present, jobs will overlap during their run
  DBMS_SCHEDULER.CREATE_JOB (
      JOB_NAME        => 'garbageCollectJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN garbageCollect(); END;',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=15',
      ENABLED         => TRUE,
      COMMENTS        => 'Castor Garbage Collector');
END;


/*
 * PL/SQL method implementing the core part of stage queries
 * It takes a list of castorfile ids as input
 */
CREATE OR REPLACE PROCEDURE internalStageQuery
 (cfs IN "numList",
  svcClassId IN NUMBER,
  result OUT castor.QueryLine_Cur) AS
BEGIN
  IF svcClassId = 0 THEN
    OPEN result FOR
      -- Here we get the status for each cf as follows: if a valid diskCopy is found,
      -- its status is returned, else if a (prepareTo)Get request is found and no diskCopy is there,
      -- WAITTAPERECALL is returned, else -1 (INVALID) is returned
      SELECT fileId, nsHost, dcId, path, fileSize, status, machine, mountPoint, nbAccesses, lastKnownFileName
         FROM (SELECT
             -- we need to give these hints to the optimizer otherwise it goes for a full table scan (!)
             UNIQUE CastorFile.id, CastorFile.fileId, CastorFile.nsHost, DC.id as dcId,
             DC.path, CastorFile.fileSize,
             CASE WHEN DC.id IS NULL THEN
                 (SELECT UNIQUE decode(SubRequest.status, 0,2, 3,2, -1)
                    FROM SubRequest,
                        (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
                         SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
                         SELECT id, svcClass FROM StagePrepareToUpdateRequest UNION ALL
                         SELECT id, svcClass FROM StageRepackRequest UNION ALL
                         SELECT id, svcClass FROM StageGetRequest) Req
                          WHERE SubRequest.CastorFile = CastorFile.id
                            AND request = Req.id)
                  ELSE DC.status END as status,
             DC.machine, DC.mountPoint,
             CastorFile.nbaccesses, CastorFile.lastKnownFileName
        FROM CastorFile,
             (SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, DiskServer.name as machine, FileSystem.mountPoint,
                     DiskPool2SvcClass.child as dcSvcClass, DiskCopy.filesystem, DiskCopy.CastorFile
                FROM FileSystem, DiskServer, DiskPool2SvcClass, 
                     (SELECT id, status, filesystem, castorFile, path FROM DiskCopy
                       WHERE CastorFile IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
                         AND status IN (0, 1, 2, 4, 5, 6, 7, 10, 11)
                             -- search for diskCopies not GCCANDIDATE or BEINGDELETED
                       GROUP BY (id, status, filesystem, castorfile, path)) DiskCopy
               WHERE FileSystem.id(+) = DiskCopy.fileSystem
                 AND nvl(FileSystem.status, 0) = 0 -- PRODUCTION
                 AND DiskServer.id(+) = FileSystem.diskServer
                 AND nvl(DiskServer.status, 0) = 0 -- PRODUCTION
                 AND DiskPool2SvcClass.parent(+) = FileSystem.diskPool) DC
       WHERE CastorFile.id IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
         AND CastorFile.id = DC.castorFile(+))    -- search for valid diskcopy
    WHERE status != -1 -- when no diskcopy and no subrequest available, ignore garbage
    ORDER BY fileid, nshost;
  ELSE
    OPEN result FOR
      -- Here we get the status for each cf as follows: if a valid diskCopy is found,
      -- its status is returned, else if a (prepareTo)Get request is found and no diskCopy is there,
      -- WAITTAPERECALL is returned, else -1 (INVALID) is returned
      SELECT fileId, nsHost, dcId, path, fileSize, srStatus, machine, mountPoint, nbAccesses, lastKnownFileName
        FROM (SELECT
            -- we need to give these hints to the optimizer otherwise it goes for a full table scan (!)
            UNIQUE CastorFile.id, CastorFile.fileId, CastorFile.nsHost, DC.id as dcId,
            DC.path, CastorFile.fileSize, DC.status,
            CASE WHEN DC.dcSvcClass = svcClassId THEN DC.status
                 WHEN DC.fileSystem = 0 THEN
                  (SELECT UNIQUE decode(nvl(SubRequest.status, -1), -1, -1, dc.status)
                   FROM SubRequest,
                       (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
                        SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
                        SELECT id, svcClass FROM StagePrepareToUpdateRequest UNION ALL
                        SELECT id, svcClass FROM StageRepackRequest UNION ALL
                        SELECT id, svcClass FROM StageGetRequest) Req
                         WHERE SubRequest.CastorFile = CastorFile.id
                           AND request = Req.id
                           AND svcClass = svcClassId)
                 WHEN DC.id IS NULL THEN
                (SELECT UNIQUE decode(SubRequest.status, 0,2, 3,2, -1)
                   FROM SubRequest,
                       (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
                        SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
                        SELECT id, svcClass FROM StagePrepareToUpdateRequest UNION ALL
                        SELECT id, svcClass FROM StageRepackRequest UNION ALL
                        SELECT id, svcClass FROM StageGetRequest) Req
                         WHERE SubRequest.CastorFile = CastorFile.id
                           AND request = Req.id
                           AND svcClass = svcClassId) END as srStatus,
            DC.machine, DC.mountPoint,
            CastorFile.nbaccesses, CastorFile.lastKnownFileName
       FROM CastorFile,
            (SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, DiskServer.name as machine, FileSystem.mountPoint,
                    DiskPool2SvcClass.child as dcSvcClass, DiskCopy.filesystem, DiskCopy.CastorFile
               FROM FileSystem, DiskServer, DiskPool2SvcClass, 
                    (SELECT id, status, filesystem, castorFile, path FROM DiskCopy
                      WHERE CastorFile IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
                        AND status IN (0, 1, 2, 4, 5, 6, 7, 10, 11)
                            -- search for diskCopies not GCCANDIDATE or BEINGDELETED
                      GROUP BY (id, status, filesystem, castorfile, path)) DiskCopy
              WHERE FileSystem.id(+) = DiskCopy.fileSystem
                AND nvl(FileSystem.status, 0) = 0 -- PRODUCTION
                AND DiskServer.id(+) = FileSystem.diskServer
                AND nvl(DiskServer.status, 0) = 0 -- PRODUCTION
                AND DiskPool2SvcClass.parent(+) = FileSystem.diskPool) DC
      WHERE CastorFile.id IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
        AND CastorFile.id = DC.castorFile(+))    -- search for valid diskcopy
      WHERE srStatus != -1
      ORDER BY fileid, nshost;
   END IF;
END;


/*
 * PL/SQL method implementing the stager_qry based on file name
 */
CREATE OR REPLACE PROCEDURE fileNameStageQuery
 (fn IN VARCHAR2,
  svcClassId IN INTEGER,
  maxNbResponses IN INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
  IF substr(fn, 1, 7) = 'regexp:' THEN  -- posix-syle regular expressions
    SELECT id BULK COLLECT INTO cfs
      FROM CastorFile
     WHERE REGEXP_LIKE(lastKnownFileName,substr(fn, 8)) 
       AND ROWNUM <= maxNbResponses + 1;
  ELSIF substr(fn, -1, 1) = '/' THEN    -- files in a 'subdirectory'
    SELECT /*+ INDEX(CastorFile I_CastorFile_LastKnownFileName) */ id BULK COLLECT INTO cfs
      FROM CastorFile 
     WHERE lastKnownFileName LIKE fn||'%'
       AND ROWNUM <= maxNbResponses + 1;
  ELSE                                  -- exact match
    SELECT /*+ INDEX(CastorFile I_CastorFile_LastKnownFileName) */ id BULK COLLECT INTO cfs
      FROM CastorFile 
     WHERE lastKnownFileName = fn;
  END IF;
  IF cfs.COUNT > maxNbResponses THEN
    -- We have too many rows, we just give up
    raise_application_error(-20102, 'Too many matching files');
  END IF;
  internalStageQuery(cfs, svcClassId, result);
END;


/*
 * PL/SQL method implementing the stager_qry based on file id
 */
CREATE OR REPLACE PROCEDURE fileIdStageQuery
 (fid IN NUMBER,
  nh IN VARCHAR2,
  svcClassId IN INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
  SELECT id BULK COLLECT INTO cfs FROM CastorFile WHERE fileId = fid AND nshost = nh;
  internalStageQuery(cfs, svcClassId, result);
END;


/*
 * PL/SQL method implementing the stager_qry based on request id
 */
CREATE OR REPLACE PROCEDURE reqIdStageQuery
 (rid IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
  SELECT sr.castorfile BULK COLLECT INTO cfs
    FROM SubRequest sr,
         (SELECT id
            FROM StagePreparetogetRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StagePreparetoputRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM stageGetRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM stagePutRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StageRepackRequest
           WHERE reqid LIKE rid) reqlist
   WHERE sr.request = reqlist.id;
  IF cfs.COUNT > 0 THEN
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;


/*
 * PL/SQL method implementing the stager_qry based on user tag
 */
CREATE OR REPLACE PROCEDURE userTagStageQuery
 (tag IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
  SELECT sr.castorfile BULK COLLECT INTO cfs
    FROM SubRequest sr,
         (SELECT id
            FROM StagePreparetogetRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM StagePreparetoputRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM stageGetRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM stagePutRequest
           WHERE userTag LIKE tag) reqlist
   WHERE sr.request = reqlist.id;
  IF cfs.COUNT > 0 THEN
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;


/*
 * PL/SQL method implementing the LastRecalls stager_qry based on request id
 */
CREATE OR REPLACE PROCEDURE reqIdLastRecallsStageQuery
 (rid IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
  reqs "numList";
BEGIN
  SELECT id BULK COLLECT INTO reqs
    FROM (SELECT id
            FROM StagePreparetogetRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StageRepackRequest
           WHERE reqid LIKE rid
          );
  IF reqs.COUNT > 0 THEN
    UPDATE SubRequest
       SET getNextStatus = 2  -- GETNEXTSTATUS_NOTIFIED
     WHERE getNextStatus = 1  -- GETNEXTSTATUS_FILESTAGED
       AND request in (SELECT * FROM Table(reqs))
    RETURNING castorfile BULK COLLECT INTO cfs;
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;


/*
 * PL/SQL method implementing the LastRecalls stager_qry based on user tag
 */
CREATE OR REPLACE PROCEDURE userTagLastRecallsStageQuery
 (tag IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
  reqs "numList";
BEGIN
  SELECT id BULK COLLECT INTO reqs
    FROM (SELECT id
            FROM StagePreparetogetRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE userTag LIKE tag
          );
  IF reqs.COUNT > 0 THEN
    UPDATE SubRequest
       SET getNextStatus = 2  -- GETNEXTSTATUS_NOTIFIED
     WHERE getNextStatus = 1  -- GETNEXTSTATUS_FILESTAGED
       AND request in (SELECT * FROM Table(reqs))
    RETURNING castorfile BULK COLLECT INTO cfs;
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;


/*
 * PL/SQL method implementing the diskPoolQuery when listing pools
 */
CREATE OR REPLACE PROCEDURE describeDiskPools
(svcClassName IN VARCHAR2,
 result OUT castor.DiskPoolsQueryLine_Cur) AS
BEGIN
  -- We use here analytic functions and the grouping sets
  -- functionnality to get both the list of filesystems
  -- and a summary per diskserver and per diskpool
  -- The grouping analytic function also allows to mark
  -- the summary lines for easy detection in the C++ code
  OPEN result FOR
    SELECT grouping(ds.name) as IsDSGrouped,
           grouping(fs.mountPoint) as IsFSGrouped,
           dp.name,
           ds.name, ds.status, fs.mountPoint,
           sum(fs.free - fs.minAllowedFreeSpace * fs.totalSize) as freeSpace,
           sum(fs.totalSize),
           fs.minFreeSpace, fs.maxFreeSpace, fs.status
      FROM FileSystem fs, DiskServer ds, DiskPool dp,
           DiskPool2SvcClass d2s, SvcClass sc
     WHERE (sc.name = svcClassName OR svcClassName is NULL)
       AND sc.id = d2s.child
       AND d2s.parent = dp.id
       AND dp.id = fs.diskPool
       AND ds.id = fs.diskServer
       group by grouping sets(
           (dp.name, ds.name, ds.status, fs.mountPoint,
             fs.free - fs.minAllowedFreeSpace * fs.totalSize,
             fs.totalSize,
             fs.minFreeSpace, fs.maxFreeSpace, fs.status),
           (dp.name, ds.name, ds.status),
           (dp.name)
          )
       order by dp.name, IsDSGrouped desc, ds.name, IsFSGrouped desc, fs.mountpoint;
END;


/*
 * PL/SQL method implementing the diskPoolQuery for a given pool
 */
CREATE OR REPLACE PROCEDURE describeDiskPool
(diskPoolName IN VARCHAR2,
 result OUT castor.DiskPoolQueryLine_Cur) AS
BEGIN
  -- We use here analytic functions and the grouping sets
  -- functionnality to get both the list of filesystems
  -- and a summary per diskserver and per diskpool
  -- The grouping analytic function also allows to mark
  -- the summary lines for easy detection in the C++ code
  OPEN result FOR 
    SELECT grouping(ds.name) as IsDSGrouped,
           grouping(fs.mountPoint) as IsGrouped,
           ds.name, ds.status, fs.mountPoint,
           sum(fs.free - fs.minAllowedFreeSpace * fs.totalSize) as freeSpace,
           sum(fs.totalSize),
           fs.minFreeSpace, fs.maxFreeSpace, fs.status
      FROM FileSystem fs, DiskServer ds, DiskPool dp
     WHERE dp.id = fs.diskPool
       AND dp.name = diskPoolName
       AND ds.id = fs.diskServer
       group by grouping sets(
           (ds.name, ds.status, fs.mountPoint,
             fs.free - fs.minAllowedFreeSpace * fs.totalSize,
             fs.totalSize,
             fs.minFreeSpace, fs.maxFreeSpace, fs.status),
           (ds.name, ds.status),
           (dp.name)
          )
       order by IsDSGrouped desc, ds.name, IsGrouped desc, fs.mountpoint;
END;


/* PL/SQL procedure which is executed whenever a files has been written to tape by the migrator to
 * check, whether the file information has to be added to the NameServer or to replace an entry 
 * (repack case)
 */
CREATE OR REPLACE PROCEDURE checkFileForRepack(fid IN INTEGER, ret OUT VARCHAR2) AS
  sreqid NUMBER;
BEGIN
  BEGIN
    ret := NULL;
    -- Get the repackvid field from the existing request (if none, then we are not in a repack process)
    SELECT SubRequest.id, StageRepackRequest.repackvid 
      INTO sreqid, ret
      FROM SubRequest, DiskCopy, CastorFile, StageRepackRequest
     WHERE stagerepackrequest.id = subrequest.request
       AND diskcopy.id = subrequest.diskcopy
       AND diskcopy.status = 10 -- CANBEMIGR
       AND subrequest.status = 12 -- SUBREQUEST_REPACK
       AND diskcopy.castorfile = castorfile.id
       AND castorfile.fileid = fid
       AND ROWNUM < 2;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL;
  END;
  IF ret IS NOT NULL THEN
    UPDATE SubRequest set status = 11
    WHERE SubRequest.id = sreqid;
  END IF;
END;


/* PL/SQL Procedure to find migration candidates (TapeCopies) for the passed svcClass.
 * It checks for the TapeCopies in CREATED and TOBEMIGRATED if there is a RepackRequest
 * going on, otherwise it looks for the svcClass from the CastorFile table.
 * In any case, it returns for the requested svcclass the Migration candidates.
 */
CREATE OR REPLACE PROCEDURE selectTapeCopiesForMigration
(svcclassId IN NUMBER, result OUT castor.TapeCopy_Cur) AS
  tcIds "numList";
BEGIN
  -- we look first for repack condidates for this svcclass
  SELECT TapeCopy.id BULK COLLECT INTO tcIds
    FROM TapeCopy, SubRequest, StageRepackRequest
   WHERE StageRepackRequest.svcclass = svcclassId
     AND SubRequest.request = StageRepackRequest.id
     AND SubRequest.status = 12  --SUBREQUEST_REPACK
     AND TapeCopy.castorfile = SubRequest.castorfile
     AND TapeCopy.status IN (0, 1)  -- CREATED / TOBEMIGRATED
     FOR UPDATE;
  -- if we didn't find anything, we look 
  -- the usual svcclass from castorfile table.
  IF tcIds.count = 0 THEN
    SELECT TapeCopy.id BULK COLLECT INTO tcIds 
      FROM TapeCopy, CastorFile 
     WHERE TapeCopy.castorFile = CastorFile.id 
       AND CastorFile.svcClass = svcclassId
       AND TapeCopy.status IN (0, 1) -- CREATED / TOBEMIGRATED
       FOR UPDATE;
  END IF;
  -- atomically update the status to WAITINSTREAMS (we got a lock
  -- above with the FOR UPDATE clause)
  UPDATE TapeCopy
     SET status = 2   -- WAITINSTREAMS
   WHERE id MEMBER OF tcIds;
  -- release the lock
  COMMIT;
  -- return the full resultset
  OPEN result FOR
    SELECT * FROM TapeCopy
    WHERE id MEMBER OF tcIds;
END;


/* PL/SQL method implementing syncClusterStatus */
CREATE OR REPLACE PROCEDURE storeClusterStatus
(machines IN castor."strList",
 fileSystems IN castor."strList",
 machineValues IN castor."cnumList",
 fileSystemValues IN castor."cnumList") AS
 ind NUMBER;
 machine NUMBER := 0;
 fs NUMBER := 0;
BEGIN
  -- First Update Machines
  FOR i in machines.FIRST .. machines.LAST LOOP
    ind := machineValues.FIRST + 9 * (i - machines.FIRST);
    IF machineValues(ind + 1) = 3 THEN -- ADMIN DELETED
      BEGIN
        SELECT id INTO machine FROM DiskServer WHERE name = machines(i);
        DELETE FROM id2Type WHERE id = machine;
        DELETE FROM id2Type WHERE id IN (SELECT id FROM FileSystem WHERE diskServer = machine);
        DELETE FROM FileSystem WHERE diskServer = machine;
        DELETE FROM DiskServer WHERE name = machines(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- Fine, was already deleted
        NULL;
      END;
    ELSE
      BEGIN
        SELECT id INTO machine FROM DiskServer WHERE name = machines(i);
        UPDATE DiskServer
           SET status = machineValues(ind),
               adminStatus = machineValues(ind + 1),
               readRate = machineValues(ind + 2),
               writeRate = machineValues(ind + 3),
               nbReadStreams = machineValues(ind + 4),
               nbWriteStreams = machineValues(ind + 5),
               nbReadWriteStreams = machineValues(ind + 6),
               nbMigratorStreams = machineValues(ind + 7),
               nbRecallerStreams = machineValues(ind + 8)
         WHERE name = machines(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        DECLARE
          mid INTEGER;
        BEGIN
          -- we should insert a new machine here
          SELECT ids_seq.nextval INTO mId FROM DUAL;
          INSERT INTO DiskServer (name, id, status, adminStatus, readRate, writeRate, nbReadStreams,
                   nbWriteStreams, nbReadWriteStreams, nbMigratorStreams, nbRecallerStreams)
           VALUES (machines(i), mid, machineValues(ind),
                   machineValues(ind + 1), machineValues(ind + 2),
                   machineValues(ind + 3), machineValues(ind + 4),
                   machineValues(ind + 5), machineValues(ind + 6),
                   machineValues(ind + 7), machineValues(ind + 8));
          INSERT INTO Id2Type (id, type) VALUES (mid, 8); -- OBJ_DiskServer
        END;
      END;
    END IF;
  END LOOP;
  -- And then FileSystems
  ind := fileSystemValues.FIRST;
  FOR i in fileSystems.FIRST .. fileSystems.LAST LOOP
    IF fileSystems(i) NOT LIKE ('/%') THEN
      SELECT id INTO machine FROM DiskServer WHERE name = fileSystems(i);
    ELSE
      IF fileSystemValues(ind + 1) = 3 THEN -- ADMIN DELETED
        BEGIN
          SELECT id INTO fs FROM FileSystem WHERE mountPoint = fileSystems(i) AND diskServer = machine;
          DELETE FROM id2Type WHERE id = fs;
          DELETE FROM FileSystem WHERE id = fs;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- Fine, was already deleted
          NULL;
        END;
      ELSE
        BEGIN
          SELECT diskServer INTO machine FROM FileSystem
           WHERE mountPoint = fileSystems(i) AND diskServer = machine;
          UPDATE FileSystem
             SET status = fileSystemValues(ind),
                 adminStatus = fileSystemValues(ind + 1),
                 readRate = fileSystemValues(ind + 2),
                 writeRate = fileSystemValues(ind + 3),
                 nbReadStreams = fileSystemValues(ind + 4),
                 nbWriteStreams = fileSystemValues(ind + 5),
                 nbReadWriteStreams = fileSystemValues(ind + 6),
                 nbMigratorStreams = fileSystemValues(ind + 7),
                 nbRecallerStreams = fileSystemValues(ind + 8),
                 free = fileSystemValues(ind + 9),
                 totalSize = fileSystemValues(ind + 10),
                 minFreeSpace = fileSystemValues(ind + 11),
                 maxFreeSpace = fileSystemValues(ind + 12),
                 minAllowedFreeSpace = fileSystemValues(ind + 13)
           WHERE mountPoint = fileSystems(i)
             AND diskServer = machine;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          DECLARE
            fsid INTEGER;
          BEGIN
            -- we should insert a new filesystem here
            SELECT ids_seq.nextval INTO fsId FROM DUAL;
            INSERT INTO FileSystem (free, mountPoint,
                   minFreeSpace, minAllowedFreeSpace, maxFreeSpace,
                   spaceToBeFreed, totalSize, readRate, writeRate, nbReadStreams,
                   nbWriteStreams, nbReadWriteStreams, nbMigratorStreams, nbRecallerStreams,
                   id, diskPool, diskserver, status, adminStatus)
            VALUES (fileSystemValues(ind + 9), fileSystems(i), fileSystemValues(ind+11),
                    fileSystemValues(ind+13), fileSystemValues(ind+12),
                    0, fileSystemValues(ind + 10), fileSystemValues(ind + 2),
                    fileSystemValues(ind + 3), fileSystemValues(ind + 4),
                    fileSystemValues(ind + 5), fileSystemValues(ind + 6),
                    fileSystemValues(ind + 7), fileSystemValues(ind + 8),
                    fsid, 0, machine, 2, 1); -- FILESYSTEM_DISABLED, ADMIN_FORCE
            INSERT INTO Id2Type (id, type) VALUES (fsid, 12); -- OBJ_FileSystem
          END;
        END;
      END IF;
      ind := ind + 14;
    END IF;
  END LOOP;
END;


/* PL/SQL method to know the number of tapecopies attached to a specific stream */
CREATE OR REPLACE PROCEDURE getNumFilesByStream (streamId IN NUMBER, numFiles OUT NUMBER) AS
BEGIN
  SELECT count(*) INTO numFiles 
    FROM stream2tapecopy 
   WHERE stream2tapecopy.parent = streamId;
END;


/* PL/SQL method implementing failSchedulerJob */
CREATE OR REPLACE PROCEDURE failSchedulerJob(srSubReqId IN VARCHAR2, srErrorCode IN NUMBER, srErrorMessage IN VARCHAR2, res OUT INTEGER) AS
BEGIN
  -- Update the subrequest status putting the request into a SUBREQUEST_FAILED
  -- status. We only concern ourselves in the termnination of a job waiting to
  -- start i.e. in a SUBREQUEST_STAGEOUT status. Requests in other statuses are
  -- left unaltered!
  res := 0;
  UPDATE SubRequest
     SET status = 7,      -- SUBREQUEST_FAILED
         errorCode = srErrorCode,
         errorMessage = case srErrorCode
           when 1718 then -- ESTNOTAVAIL
             'All copies of this file are unavailable for now. Please retry later'
           when 1719 then -- ESTJOBKILLED
             'Job killed by service administrator'
           when 1720 then -- ESTJOBTIMEDOUT
             'Job timed out while waiting to be scheduled'
           when 1033 then -- SEUSERUNKN
             'Username unknown to scheduler'
           when 1708 then -- ESTUSER
             'Job not scheduled as recorded username does not match uid'
           when 1706 then -- ESTGROUP
             'User group invalid for given user'
           when 1015 then -- SEINTERNAL
             'Internal error'
           else srErrorMessage
         end,
         lastModificationTime = getTime()
   WHERE subreqid = srSubReqId
     AND status IN (6, 14) -- SUBREQUEST_STAGEOUT, SUBREQUEST_BEINGSCHED
  RETURNING id INTO res;
END;


/* PL/SQL method implementing jobToSchedule */
CREATE OR REPLACE
PROCEDURE jobToSchedule (srId OUT INTEGER, srSubReqId OUT VARCHAR2, srProtocol OUT VARCHAR2, 
			 srXsize OUT INTEGER, srRequestedFileSystems OUT VARCHAR2, 
			 reqId OUT VARCHAR2, cfFileId OUT INTEGER, cfNsHost OUT VARCHAR2, 
			 sSvcClass OUT VARCHAR2, reqType OUT INTEGER, reqEuid OUT INTEGER,
			 reqEgid OUT INTEGER, reqUsername OUT VARCHAR2, direction OUT VARCHAR2,
			 cIp OUT INTEGER, cPort OUT INTEGER, cVersion OUT INTEGER, cType OUT INTEGER) AS
BEGIN
    -- Get the next subrequest to be scheduled.
    UPDATE SubRequest 
       SET status = 14, lastModificationTime = getTime() -- SUBREQUEST_BEINGSCHED
     WHERE status = 13 -- SUBREQUEST_READYFORSCHED
       AND rownum < 2
 RETURNING id, subReqId, protocol, xsize, requestedFileSystems
      INTO srId, srSubReqId, srProtocol, srXsize, srRequestedFileSystems;

    -- Extract the rest of the information required to submit a job into
    -- the scheduler through the job manager.
    SELECT CastorFile.fileId, CastorFile.nsHost, SvcClass.name, Id2type.type,
           Request.reqId, Request.euid, Request.egid, Request.username, Request.direction,
	   Client.ipAddress, Client.port, Client.version,
	   (SELECT type 
              FROM Id2type 
             WHERE id = Client.id) clientType
      INTO cfFileId, cfNsHost, sSvcClass, reqType, reqId, reqEuid, reqEgid, reqUsername, 
           direction, cIp, cPort, cVersion, cType
      FROM SubRequest, CastorFile, SvcClass, Id2type, Client,
           (SELECT id, username, euid, egid, reqid, client, 'w' direction 
	      FROM StagePutRequest 
             UNION ALL
            SELECT id, username, euid, egid, reqid, client, 'r' direction 
              FROM StageGetRequest 
             UNION ALL
            SELECT id, username, euid, egid, reqid, client, 'o' direction 
              FROM StageUpdateRequest) Request
     WHERE SubRequest.id = srId
       AND SubRequest.castorFile = CastorFile.id
       AND CastorFile.svcClass = SvcClass.id
       AND Id2type.id = SubRequest.request
       AND Request.id = SubRequest.request
       AND Request.client = Client.id;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL;
END;


/* Check permissions */
CREATE OR REPLACE PROCEDURE checkPermission(isvcClass IN VARCHAR2,
                                            ieuid IN NUMBER,
                                            iegid IN NUMBER,
                                            ireqType IN NUMBER,
                                            res OUT NUMBER) AS
  unused NUMBER;
BEGIN
  SELECT reqType
    INTO unused
    FROM WhiteList
   WHERE (svcClass = isvcClass OR svcClass IS NULL
          OR (length(isvcClass) IS NULL AND svcClass = 'default'))
     AND (egid = iegid OR egid IS NULL)
     AND (euid = ieuid OR euid IS NULL)
     AND (reqType = ireqType OR reqType IS NULL);
  BEGIN
    SELECT reqType
      INTO unused
      FROM BlackList
     WHERE (svcClass = isvcClass OR svcClass IS NULL
            OR (length(isvcClass) IS NULL AND svcClass = 'default'))
       AND (egid = iegid OR egid IS NULL)
       AND (euid = ieuid OR euid IS NULL)
       AND (reqType = ireqType OR reqType IS NULL);
    -- found in Black list -> no access
    res := -1;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Not Found in Black list -> access
    res := 0;
  END;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Not found in White list -> no access
  res := -1;
END;


/* Useful functions for debugging, returning diskcopies' useful
 * information for any castorfile or diskcopy
 */

/* Drop the objects and collection types first as an
 * ORA-02303: cannot drop or replace a type with type or table dependents
 * will be raised if you try and upgrade them
 */
BEGIN
  -- Drop all objects (ignore monitoring ones!)
  FOR rec IN (SELECT object_name, object_type FROM user_objects
              WHERE  object_name LIKE '%_%_DEBUG'
              AND    object_type = 'TYPE')
  LOOP
    EXECUTE IMMEDIATE 'DROP TYPE "'||rec.object_name||'" FORCE';
  END LOOP;
END;


/* First the objects and collection types */
CREATE OR REPLACE TYPE O_DiskCopy_Debug AS OBJECT(
  id INTEGER,
  diskPool VARCHAR2(2048),
  location VARCHAR2(2048),
  status NUMBER,
  creationtime DATE
);

CREATE OR REPLACE TYPE T_DiskCopy_Debug AS TABLE OF O_DiskCopy_Debug;


/* It would be nice if we could use SubRequest%RowType here!! */
CREATE OR REPLACE TYPE O_SubRequest_Debug AS OBJECT (
  retryCounter NUMBER, 
  fileName VARCHAR2(2048), 
  protocol VARCHAR2(2048),
  xsize INTEGER, 
  priority NUMBER, 
  subreqId VARCHAR2(2048), 
  flags NUMBER,
  modeBits NUMBER, 
  creationTime INTEGER, 
  lastModificationTime INTEGER,
  answered NUMBER,  
  errorCode NUMBER, 
  errorMessage VARCHAR2(2048), 
  id NUMBER,
  diskcopy INTEGER, 
  castorFile INTEGER, 
  parent INTEGER, 
  status INTEGER,
  request INTEGER, 
  getNextStatus INTEGER, 
  requestedFileSystems VARCHAR2(2048)
);

CREATE OR REPLACE TYPE T_SubRequest_Debug AS TABLE OF O_SubRequest_Debug;


/* Return the castor file id associated with the reference number */
CREATE OR REPLACE FUNCTION getCF(ref NUMBER) RETURN NUMBER AS
  t NUMBER;
  cfId NUMBER;
BEGIN
  SELECT type INTO t FROM id2Type WHERE id = ref;
  IF (t = 2) THEN -- CASTORFILE
    RETURN ref;
  ELSIF (t = 5) THEN -- DiskCopy
    SELECT castorFile INTO cfId FROM DiskCopy WHERE id = ref;
  ELSIF (t = 27) THEN -- SubRequest
    SELECT castorFile INTO cfId FROM SubRequest WHERE id = ref;
  ELSIF (t = 30) THEN -- TapeCopy
    SELECT castorFile INTO cfId FROM TapeCopy WHERE id = ref;
  END IF;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- fileid ?
  SELECT id INTO cfId FROM CastorFile WHERE fileId = ref;
  RETURN cfId;
END;


/* Get the diskcopys associated with the reference number */
CREATE OR REPLACE FUNCTION getDCs(ref number) RETURN T_DiskCopy_Debug PIPELINED AS
BEGIN
  FOR d IN (SELECT diskCopy.id,
                   diskPool.name as diskpool,
                   diskServer.name || ':' || fileSystem.mountPoint || diskCopy.path as location,
                   diskCopy.status as status,
                   to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationtime as creationtime
              FROM DiskCopy, FileSystem, DiskServer, DiskPool
             WHERE DiskCopy.fileSystem = FileSystem.id
               AND FileSystem.diskServer = diskServer.id
               AND DiskPool.id = fileSystem.diskPool
               AND DiskCopy.castorfile = getCF(ref)) LOOP
     PIPE ROW(O_DiskCopy_Debug(d.id, d.diskPool, d.location, d.status, d.creationtime));
  END LOOP;
END;


/* Get the subrequest associated with the reference number. (By castorfile/diskcopy/
 * subrequest/tapecopy or fileid
 */
CREATE OR REPLACE FUNCTION getSRs(ref number) RETURN T_SubRequest_Debug PIPELINED AS
BEGIN
  FOR d IN (SELECT * FROM SubRequest WHERE castorfile = getCF(ref)) LOOP
     PIPE ROW(O_SubRequest_Debug(d.retryCounter,   d.fileName, 
                                 d.protocol,       d.xsize, 
                                 d.priority,       d.subreqId, 
                                 d.flags,          d.modeBits, 
                                 d.creationTime,   d.lastModificationTime,
                                 d.answered,       d.errorCode, 
                                 d.errorMessage,   d.id,
                                 d.diskcopy,       d.castorFile, 
                                 d.parent,         d.status,
                                 d.request,        d.getNextStatus, 
                                 d.requestedFileSystems));
  END LOOP;
END;
