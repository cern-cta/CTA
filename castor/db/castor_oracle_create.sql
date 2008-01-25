/* SQL statements for type BaseAddress */
CREATE TABLE BaseAddress (objType NUMBER, cnvSvcName VARCHAR2(2048), cnvSvcType NUMBER, target INTEGER, id INTEGER CONSTRAINT I_BaseAddress_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Client */
CREATE TABLE Client (ipAddress NUMBER, port NUMBER, version NUMBER, secure NUMBER, id INTEGER CONSTRAINT I_Client_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

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
CREATE TABLE Segment (fseq NUMBER, offset INTEGER, bytes_in INTEGER, bytes_out INTEGER, host_bytes INTEGER, segmCksumAlgorithm VARCHAR2(2048), segmCksum NUMBER, errMsgTxt VARCHAR2(2048), errorCode NUMBER, severity NUMBER, blockId0 INTEGER, blockId1 INTEGER, blockId2 INTEGER, blockId3 INTEGER, creationTime INTEGER, id INTEGER CONSTRAINT I_Segment_Id PRIMARY KEY, tape INTEGER, copy INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

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
CREATE TABLE SvcClass (nbDrives NUMBER, name VARCHAR2(2048), defaultFileSize INTEGER, maxReplicaNb NUMBER, replicationPolicy VARCHAR2(2048), gcPolicy VARCHAR2(2048), migratorPolicy VARCHAR2(2048), recallerPolicy VARCHAR2(2048), hasDiskOnlyBehavior NUMBER, streamPolicy VARCHAR2(2048), id INTEGER CONSTRAINT I_SvcClass_Id PRIMARY KEY, forcedFileClass INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
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

/* SQL statements for type StageDiskCopyReplicaRequest */
CREATE TABLE StageDiskCopyReplicaRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, sourceDiskCopyId INTEGER, destDiskCopyId INTEGER, id INTEGER CONSTRAINT I_StageDiskCopyReplicaReque_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type NsFilesDeleted */
CREATE TABLE NsFilesDeleted (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, nsHost VARCHAR2(2048), id INTEGER CONSTRAINT I_NsFilesDeleted_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Disk2DiskCopyStartRequest */
CREATE TABLE Disk2DiskCopyStartRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskCopyId INTEGER, sourceDiskCopyId INTEGER, destSvcClass VARCHAR2(2048), diskServer VARCHAR2(2048), mountPoint VARCHAR2(2048), id INTEGER CONSTRAINT I_Disk2DiskCopyStartRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type FirstByteWritten */
CREATE TABLE FirstByteWritten (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, id INTEGER CONSTRAINT I_FirstByteWritten_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

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
INSERT INTO CastorVersion VALUES ('-', '2_1_6_8');

/* Fill Type2Obj metatable */
CREATE TABLE Type2Obj (type INTEGER PRIMARY KEY NOT NULL, object VARCHAR2(100) NOT NULL, svcHandler VARCHAR2(100));
INSERT INTO Type2Obj (type, object) VALUES (0, 'INVALID');
INSERT INTO Type2Obj (type, object) VALUES (1, 'Ptr');
INSERT INTO Type2Obj (type, object) VALUES (2, 'CastorFile');
INSERT INTO Type2Obj (type, object) VALUES (3, 'OldClient');
INSERT INTO Type2Obj (type, object) VALUES (4, 'Cuuid');
INSERT INTO Type2Obj (type, object) VALUES (5, 'DiskCopy');
INSERT INTO Type2Obj (type, object) VALUES (6, 'DiskFile');
INSERT INTO Type2Obj (type, object) VALUES (7, 'DiskPool');
INSERT INTO Type2Obj (type, object) VALUES (8, 'DiskServer');
INSERT INTO Type2Obj (type, object) VALUES (10, 'FileClass');
INSERT INTO Type2Obj (type, object) VALUES (12, 'FileSystem');
INSERT INTO Type2Obj (type, object) VALUES (13, 'IClient');
INSERT INTO Type2Obj (type, object) VALUES (14, 'MessageAck');
INSERT INTO Type2Obj (type, object) VALUES (16, 'ReqIdRequest');
INSERT INTO Type2Obj (type, object) VALUES (17, 'Request');
INSERT INTO Type2Obj (type, object) VALUES (18, 'Segment');
INSERT INTO Type2Obj (type, object) VALUES (21, 'StageGetNextRequest');
INSERT INTO Type2Obj (type, object) VALUES (26, 'Stream');
INSERT INTO Type2Obj (type, object) VALUES (27, 'SubRequest');
INSERT INTO Type2Obj (type, object) VALUES (28, 'SvcClass');
INSERT INTO Type2Obj (type, object) VALUES (29, 'Tape');
INSERT INTO Type2Obj (type, object) VALUES (30, 'TapeCopy');
INSERT INTO Type2Obj (type, object) VALUES (31, 'TapePool');
INSERT INTO Type2Obj (type, object) VALUES (33, 'StageFileQueryRequest');
INSERT INTO Type2Obj (type, object) VALUES (34, 'StageFindRequestRequest');
INSERT INTO Type2Obj (type, object) VALUES (35, 'StageGetRequest');
INSERT INTO Type2Obj (type, object) VALUES (36, 'StagePrepareToGetRequest');
INSERT INTO Type2Obj (type, object) VALUES (37, 'StagePrepareToPutRequest');
INSERT INTO Type2Obj (type, object) VALUES (38, 'StagePrepareToUpdateRequest');
INSERT INTO Type2Obj (type, object) VALUES (39, 'StagePutDoneRequest');
INSERT INTO Type2Obj (type, object) VALUES (40, 'StagePutRequest');
INSERT INTO Type2Obj (type, object) VALUES (41, 'StageRequestQueryRequest');
INSERT INTO Type2Obj (type, object) VALUES (42, 'StageRmRequest');
INSERT INTO Type2Obj (type, object) VALUES (43, 'StageUpdateFileStatusRequest');
INSERT INTO Type2Obj (type, object) VALUES (44, 'StageUpdateRequest');
INSERT INTO Type2Obj (type, object) VALUES (45, 'FileRequest');
INSERT INTO Type2Obj (type, object) VALUES (46, 'QryRequest');
INSERT INTO Type2Obj (type, object) VALUES (48, 'StagePutNextRequest');
INSERT INTO Type2Obj (type, object) VALUES (49, 'StageUpdateNextRequest');
INSERT INTO Type2Obj (type, object) VALUES (50, 'StageAbortRequest');
INSERT INTO Type2Obj (type, object) VALUES (51, 'StageReleaseFilesRequest');
INSERT INTO Type2Obj (type, object) VALUES (58, 'DiskCopyForRecall');
INSERT INTO Type2Obj (type, object) VALUES (59, 'TapeCopyForMigration');
INSERT INTO Type2Obj (type, object) VALUES (60, 'GetUpdateStartRequest');
INSERT INTO Type2Obj (type, object) VALUES (62, 'BaseAddress');
INSERT INTO Type2Obj (type, object) VALUES (64, 'Disk2DiskCopyDoneRequest');
INSERT INTO Type2Obj (type, object) VALUES (65, 'MoverCloseRequest');
INSERT INTO Type2Obj (type, object) VALUES (66, 'StartRequest');
INSERT INTO Type2Obj (type, object) VALUES (67, 'PutStartRequest');
INSERT INTO Type2Obj (type, object) VALUES (69, 'IObject');
INSERT INTO Type2Obj (type, object) VALUES (70, 'IAddress');
INSERT INTO Type2Obj (type, object) VALUES (71, 'QueryParameter');
INSERT INTO Type2Obj (type, object) VALUES (72, 'DiskCopyInfo');
INSERT INTO Type2Obj (type, object) VALUES (73, 'Files2Delete');
INSERT INTO Type2Obj (type, object) VALUES (74, 'FilesDeleted');
INSERT INTO Type2Obj (type, object) VALUES (76, 'GCLocalFile');
INSERT INTO Type2Obj (type, object) VALUES (78, 'GetUpdateDone');
INSERT INTO Type2Obj (type, object) VALUES (79, 'GetUpdateFailed');
INSERT INTO Type2Obj (type, object) VALUES (80, 'PutFailed');
INSERT INTO Type2Obj (type, object) VALUES (81, 'GCFile');
INSERT INTO Type2Obj (type, object) VALUES (82, 'GCFileList');
INSERT INTO Type2Obj (type, object) VALUES (83, 'FilesDeletionFailed');
INSERT INTO Type2Obj (type, object) VALUES (84, 'TapeRequest');
INSERT INTO Type2Obj (type, object) VALUES (85, 'ClientIdentification');
INSERT INTO Type2Obj (type, object) VALUES (86, 'TapeServer');
INSERT INTO Type2Obj (type, object) VALUES (87, 'TapeDrive');
INSERT INTO Type2Obj (type, object) VALUES (88, 'DeviceGroupName');
INSERT INTO Type2Obj (type, object) VALUES (89, 'ErrorHistory');
INSERT INTO Type2Obj (type, object) VALUES (90, 'TapeDriveDedication');
INSERT INTO Type2Obj (type, object) VALUES (91, 'TapeAccessSpecification');
INSERT INTO Type2Obj (type, object) VALUES (92, 'TapeDriveCompatibility');
INSERT INTO Type2Obj (type, object) VALUES (93, 'PutDoneStart');
INSERT INTO Type2Obj (type, object) VALUES (95, 'SetFileGCWeight');
INSERT INTO Type2Obj (type, object) VALUES (96, 'RepackRequest');
INSERT INTO Type2Obj (type, object) VALUES (97, 'RepackSubRequest');
INSERT INTO Type2Obj (type, object) VALUES (98, 'RepackSegment');
INSERT INTO Type2Obj (type, object) VALUES (99, 'RepackAck');
INSERT INTO Type2Obj (type, object) VALUES (101, 'DiskServerDescription');
INSERT INTO Type2Obj (type, object) VALUES (102, 'FileSystemDescription');
INSERT INTO Type2Obj (type, object) VALUES (103, 'DiskPoolQuery');
INSERT INTO Type2Obj (type, object) VALUES (104, 'EndResponse');
INSERT INTO Type2Obj (type, object) VALUES (105, 'FileResponse');
INSERT INTO Type2Obj (type, object) VALUES (106, 'StringResponse');
INSERT INTO Type2Obj (type, object) VALUES (107, 'Response');
INSERT INTO Type2Obj (type, object) VALUES (108, 'IOResponse');
INSERT INTO Type2Obj (type, object) VALUES (109, 'AbortResponse');
INSERT INTO Type2Obj (type, object) VALUES (110, 'RequestQueryResponse');
INSERT INTO Type2Obj (type, object) VALUES (111, 'FileQueryResponse');
INSERT INTO Type2Obj (type, object) VALUES (112, 'FindReqResponse');
INSERT INTO Type2Obj (type, object) VALUES (113, 'GetUpdateStartResponse');
INSERT INTO Type2Obj (type, object) VALUES (114, 'BasicResponse');
INSERT INTO Type2Obj (type, object) VALUES (115, 'StartResponse');
INSERT INTO Type2Obj (type, object) VALUES (116, 'GCFilesResponse');
INSERT INTO Type2Obj (type, object) VALUES (117, 'FileQryResponse');
INSERT INTO Type2Obj (type, object) VALUES (118, 'DiskPoolQueryResponse');
INSERT INTO Type2Obj (type, object) VALUES (119, 'StageRepackRequest');
INSERT INTO Type2Obj (type, object) VALUES (120, 'DiskServerStateReport');
INSERT INTO Type2Obj (type, object) VALUES (121, 'DiskServerMetricsReport');
INSERT INTO Type2Obj (type, object) VALUES (122, 'FileSystemStateReport');
INSERT INTO Type2Obj (type, object) VALUES (123, 'FileSystemMetricsReport');
INSERT INTO Type2Obj (type, object) VALUES (124, 'DiskServerAdminReport');
INSERT INTO Type2Obj (type, object) VALUES (125, 'FileSystemAdminReport');
INSERT INTO Type2Obj (type, object) VALUES (126, 'StreamReport');
INSERT INTO Type2Obj (type, object) VALUES (127, 'FileSystemStateAck');
INSERT INTO Type2Obj (type, object) VALUES (128, 'MonitorMessageAck');
INSERT INTO Type2Obj (type, object) VALUES (129, 'Client');
INSERT INTO Type2Obj (type, object) VALUES (130, 'JobSubmissionRequest');
INSERT INTO Type2Obj (type, object) VALUES (131, 'VersionQuery');
INSERT INTO Type2Obj (type, object) VALUES (132, 'VersionResponse');
INSERT INTO Type2Obj (type, object) VALUES (133, 'StageDiskCopyReplicaRequest');
INSERT INTO Type2Obj (type, object) VALUES (134, 'RepackResponse');
INSERT INTO Type2Obj (type, object) VALUES (135, 'RepackFileQry');
INSERT INTO Type2Obj (type, object) VALUES (136, 'CnsInfoMigrationPolicy');
INSERT INTO Type2Obj (type, object) VALUES (137, 'DbInfoMigrationPolicy');
INSERT INTO Type2Obj (type, object) VALUES (138, 'CnsInfoRecallPolicy');
INSERT INTO Type2Obj (type, object) VALUES (139, 'DbInfoRecallPolicy');
INSERT INTO Type2Obj (type, object) VALUES (140, 'DbInfoStreamPolicy');
INSERT INTO Type2Obj (type, object) VALUES (141, 'PolicyObj');
INSERT INTO Type2Obj (type, object) VALUES (142, 'NsFilesDeleted');
INSERT INTO Type2Obj (type, object) VALUES (143, 'NsFilesDeletedResponse');
INSERT INTO Type2Obj (type, object) VALUES (144, 'Disk2DiskCopyStartRequest');
INSERT INTO Type2Obj (type, object) VALUES (145, 'Disk2DiskCopyStartResponse');
INSERT INTO Type2Obj (type, object) VALUES (146, 'ThreadNotification');
INSERT INTO Type2Obj (type, object) VALUES (147, 'FirstByteWritten');

/*******************************************************************
 *
 * @(#)RCSfile: oracleTrailer.sql,v  Revision: 1.616  Date: 2008/01/25 15:30:16  Author: sponcec3 
 *
 * This file contains SQL code that is not generated automatically
 * and is inserted at the end of the generated code
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* A small table used to cross check code and DB versions */
UPDATE CastorVersion SET schemaVersion = '2_1_6_0';

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
  partition type_144 values (144)  tablespace stager_data,
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
    PARTITION P_STATUS_7       VALUES (7),
    PARTITION P_STATUS_8       VALUES (8),
    PARTITION P_STATUS_9_10    VALUES (9, 10),        -- FAILED_*
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

/* Some constraints */
ALTER TABLE FileSystem ADD CONSTRAINT diskserver_fk FOREIGN KEY (diskServer) REFERENCES DiskServer(id);
ALTER TABLE FileSystem MODIFY (status NOT NULL);
ALTER TABLE FileSystem MODIFY (diskServer NOT NULL);
ALTER TABLE DiskServer MODIFY (status NOT NULL);
ALTER TABLE SvcClass MODIFY (name NOT NULL);

/* An index to speed up queries in FileQueryRequest, FindRequestRequest, RequestQueryRequest */
CREATE INDEX I_QueryParameter_Query on QueryParameter (query);

/* An index to speed the queries on Segments by copy */
CREATE INDEX I_Segment_Copy on Segment (copy);

/* Constraint on FileClass name */
ALTER TABLE FileClass ADD CONSTRAINT I_FileClass_Name UNIQUE (name); 

/* Add unique constraint on tapes */
ALTER TABLE Tape ADD CONSTRAINT I_Tape_VIDSideTpMode UNIQUE (VID, side, tpMode);

/* Add unique constraint on svcClass name */
ALTER TABLE SvcClass ADD CONSTRAINT I_SvcClass_Name UNIQUE (NAME); 

/* The primary key in this table allows for materialized views */
ALTER TABLE DiskPool2SvcClass ADD CONSTRAINT I_DiskPool2SvcCla_ParentChild PRIMARY KEY (parent, child);

/* Global temporary table to handle output of the filesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE FilesDeletedProcOutput
  (fileid NUMBER, nshost VARCHAR2(2048))
  ON COMMIT PRESERVE ROWS;

/* Global temporary table to help selectFiles2Delete procedure */
CREATE GLOBAL TEMPORARY TABLE SelectFiles2DeleteProcHelper
  (id NUMBER, path VARCHAR2(2048))
  ON COMMIT DELETE ROWS;

/* Global temporary table to handle output of the nsFilesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE NsFilesDeletedOrphans
  (fileid NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary tables for the cleanup procedures */
CREATE GLOBAL TEMPORARY TABLE ArchivedRequestCleaning
  (id NUMBER NOT NULL ENABLE, type NUMBER NOT NULL ENABLE)
  ON COMMIT PRESERVE ROWS;

CREATE GLOBAL TEMPORARY TABLE OutOfDateRequestCleaning
  (id NUMBER NOT NULL ENABLE, type NUMBER NOT NULL ENABLE)
  ON COMMIT PRESERVE ROWS;

/* Global temporary table for dropped out of date StageOut diskCopies */
CREATE GLOBAL TEMPORARY TABLE OutOfDateStageOutDropped
  (fileId NUMBER, nsHost VARCHAR2(2048))
  ON COMMIT DELETE ROWS;

/* Global temporary table for out of date StageOut diskCopies
   where putdone was issued */
CREATE GLOBAL TEMPORARY TABLE OutOfDateStageOutPutDone
  (fileId NUMBER, nsHost VARCHAR2(2048))
  ON COMMIT DELETE ROWS;

/* Global temporary table for dropped out of date Recalls */
CREATE GLOBAL TEMPORARY TABLE OutOfDateRecallDropped
  (fileId NUMBER, nsHost VARCHAR2(2048))
  ON COMMIT DELETE ROWS;

/* Temporary table to handle removing of priviledges */
CREATE GLOBAL TEMPORARY TABLE removePrivilegeTmpTable
  (svcCLass NUMBER,
   euid NUMBER,
   egid NUMBER,
   reqType NUMBER)
  ON COMMIT DELETE ROWS;


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

/* Define the service handlers for the appropriate sets of stage request objects */
UPDATE Type2Obj SET svcHandler = 'JobReqSvc' WHERE type in (35, 40, 44);
UPDATE Type2Obj SET svcHandler = 'PrepReqSvc' WHERE type in (36, 37, 38, 119);
UPDATE Type2Obj SET svcHandler = 'StageReqSvc' WHERE type in (39, 42, 95);
UPDATE Type2Obj SET svcHandler = 'QueryReqSvc' WHERE type in (33, 34, 41, 103, 131);
UPDATE Type2Obj SET svcHandler = 'JobSvc' WHERE type in (60, 64, 65, 67, 78, 79, 80, 93, 144, 147);
UPDATE Type2Obj SET svcHandler = 'GCSvc' WHERE type in (73, 74, 83, 142);

/* Set default values for the StageDiskCopyReplicaRequest table */
ALTER TABLE StageDiskCopyReplicaRequest MODIFY flags DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY euid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY egid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY mask DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY pid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY machine DEFAULT 'stager';

COMMIT;

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
CREATE OR REPLACE FUNCTION fileSystemRate
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
    ON FileSystem(fileSystemRate(readRate, writeRate,
	          nbReadStreams,nbWriteStreams, nbReadWriteStreams, nbMigratorStreams, nbRecallerStreams));


/*******************************************/
/*                                         */
/*   Triggers and Procedures definitions   */
/*                                         */
/*******************************************/

/* Get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
  ret NUMBER;
BEGIN
  SELECT (SYSDATE - to_date('01-jan-1970 01:00:00','dd-mon-yyyy HH:MI:SS')) * (24*60*60) INTO ret FROM DUAL;
  RETURN ret;
END;

/* Generate a universally unique id (UUID) */
CREATE OR REPLACE FUNCTION uuidGen RETURN VARCHAR2 IS
  ret VARCHAR2(36);
BEGIN
  -- Note: the guid generator provided by ORACLE produces sequential uuid's, not
  -- random ones. The reason for this is because random uuid's are not good for
  -- indexing!
  SELECT lower(regexp_replace(sys_guid(), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5'))
    INTO ret FROM Dual;
  RETURN ret;
END;

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

/* PL/SQL method canceling a given recall */
CREATE OR REPLACE PROCEDURE cancelRecall
(cfId NUMBER, dcId NUMBER, newSubReqStatus NUMBER) AS
  srId NUMBER;
BEGIN
  -- cancel the recall
  deleteTapeCopies(cfId);
  -- Delete the DiskCopy
  UPDATE DiskCopy SET status = 7 WHERE id = dcId; -- INVALID
  -- Look for request associated to the recall and fail
  -- it and all the waiting ones
  UPDATE SubRequest SET status = newSubReqStatus
   WHERE diskCopy = dcId RETURNING id INTO srId;
  UPDATE SubRequest
     SET status = newSubReqStatus, parent = 0 -- FAILED
   WHERE status = 5 -- WAITSUBREQ
     AND parent = srId
     AND castorfile = cfId;
END;

/* Checks consistency of DiskCopies when a FileSystem comes
 * back in production after a period spent in DRAINING or
 * DISABLED status.
 * Current checks/fixes include :
 *   - canceling recalls for files that are STAGED/CANBEMIGR
 *     on the fileSystem that comes back.
 *   - dealing with file that are STAGEOUT on the fileSystem
 *     coming back but already exist on another one
 */
CREATE OR REPLACE PROCEDURE checkFSBackInProd(fsId NUMBER) AS
  srId NUMBER;
  srIds "numList";
BEGIN
  -- Look for recalls concerning files that are STAGED/CANBEMIGR
  -- on the filesystem coming back to life
  FOR cf IN (SELECT UNIQUE d.castorfile, e.id
               FROM DiskCopy d, DiskCopy e 
              WHERE d.castorfile = e.castorfile
                AND d.fileSystem = fsId
                AND d.status IN (0, 10)
                AND e.status = 2) LOOP
    -- cancel recall and restart subrequests
    cancelRecall(cf.castorfile, cf.id, 1); -- RESTART
  END LOOP;
  -- Look for files that are STAGEOUT on the filesystem
  -- coming back to life but already STAGED/CANBEMIGR/
  -- WAITTAPERECALL/WAITFS/STAGEOUT/WAITFS_SCHEDULING
  -- somewhere else
  FOR cf IN (SELECT UNIQUE d.castorfile, d.id
               FROM DiskCopy d, DiskCopy e 
              WHERE d.castorfile = e.castorfile
                AND d.fileSystem = fsId
                AND e.fileSystem != fsId
                AND d.status = 6 -- STAGEOUT
                AND e.status IN (0,10,2,5,6,11)) LOOP -- STAGED/CANBEMIGR/WAITTAPERECALL/WAITFS/STAGEOUT/WAITFS_SCHEDULING
    -- Delete the DiskCopy
    UPDATE DiskCopy
       SET status = 7  -- INVALID
     WHERE id = cf.id;
    -- Look for request associated to the diskCopy and restart
    -- it and all the waiting ones
    UPDATE SubRequest SET status = 7 -- FAILED
     WHERE diskCopy = cf.id RETURNING id BULK COLLECT INTO srIds;
    UPDATE SubRequest
       SET status = 7, parent = 0 -- FAILED
     WHERE status = 5 -- WAITSUBREQ
       AND parent MEMBER OF srIds
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
  unused NUMBER;
BEGIN
  SELECT id INTO unused FROM CastorFile
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
  unused NUMBER;
BEGIN
  SELECT id INTO unused FROM CastorFile
   WHERE id = :new.castorFile FOR UPDATE;
END;


/* PL/SQL method to get the next SubRequest to do according to the given service */
CREATE OR REPLACE PROCEDURE subRequestToDo(service IN VARCHAR2,
                                           srId OUT INTEGER, srRetryCounter OUT INTEGER, srFileName OUT VARCHAR2,
                                           srProtocol OUT VARCHAR2, srXsize OUT INTEGER, srPriority OUT INTEGER,
                                           srStatus OUT INTEGER, srModeBits OUT INTEGER, srFlags OUT INTEGER,
                                           srSubReqId OUT VARCHAR2) AS
LockError EXCEPTION;
PRAGMA EXCEPTION_INIT (LockError, -54);
CURSOR c IS
   SELECT /*+ USE_NL */ id
     FROM SubRequest
    WHERE status in (0,1,2)    -- START, RESTART, RETRY
      AND EXISTS
         (SELECT /*+ index(a I_Id2Type_id) */ 'x'
            FROM Id2Type a, Type2Obj
           WHERE a.id = SubRequest.request
             AND a.type = Type2Obj.type
             AND Type2Obj.svcHandler = service)
    FOR UPDATE SKIP LOCKED;
BEGIN
  srId := 0;
  OPEN c;
  FETCH c INTO srId;
  UPDATE SubRequest SET status = 3, subReqId = uuidGen() WHERE id = srId  -- WAITSCHED
    RETURNING retryCounter, fileName, protocol, xsize, priority, status, modeBits, flags, subReqId
    INTO srRetryCounter, srFileName, srProtocol, srXsize, srPriority, srStatus, srModeBits, srFlags, srSubReqId;
  CLOSE c;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- just return srId = 0, nothing to do
  NULL;
WHEN LockError THEN
  -- We have observed ORA-00054 errors (resource busy and acquire with NOWAIT) even with
  -- the SKIP LOCKED clause. This is a workaround to ignore the error until we understand
  -- what to do, another thread will pick up the request so we don't do anything.
  NULL;
END;

/* PL/SQL method to get the next failed SubRequest to do according to the given service */
/* the service parameter is not used now, it will with the new stager */
CREATE OR REPLACE PROCEDURE subRequestFailedToDo(srId OUT INTEGER, srRetryCounter OUT INTEGER, srFileName OUT VARCHAR2,
                                                 srProtocol OUT VARCHAR2, srXsize OUT INTEGER, srPriority OUT INTEGER,
                                                 srStatus OUT INTEGER, srModeBits OUT INTEGER, srFlags OUT INTEGER,
                                                 srSubReqId OUT VARCHAR2, srErrorCode OUT NUMBER,
                                                 srErrorMessage OUT VARCHAR2) AS
CURSOR c IS
   SELECT /*+ USE_NL */ id
     FROM SubRequest
    WHERE status = 7  -- FAILED
    FOR UPDATE SKIP LOCKED;
BEGIN
  srId := 0;
  OPEN c;
  FETCH c INTO srId;
  UPDATE subrequest SET status = 10 WHERE id = srId   -- SUBREQUEST_FAILED_ANSWERING
    RETURNING retryCounter, fileName, protocol, xsize, priority, status,
              modeBits, flags, subReqId, errorCode, errorMessage
    INTO srRetryCounter, srFileName, srProtocol, srXsize, srPriority, srStatus,
         srModeBits, srFlags, srSubReqId, srErrorCode, srErrorMessage;
  CLOSE c;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- just return srId = 0, nothing to do
  NULL;
END;


/* PL/SQL method to get the next request to do according to the given service */
CREATE OR REPLACE PROCEDURE requestToDo(service IN VARCHAR2, rId OUT INTEGER) AS
BEGIN
  DELETE FROM NewRequests 
   WHERE type IN (
     SELECT type FROM Type2Obj
      WHERE svcHandler = service
     )
   AND ROWNUM < 2 RETURNING id INTO rId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  rId := 0;   -- nothing to do
END;

/* PL/SQL method to make a SubRequest wait on another one, linked to the given DiskCopy */
CREATE OR REPLACE PROCEDURE makeSubRequestWait(srId IN INTEGER, dci IN INTEGER) AS
BEGIN
  -- all wait on the original one; also prevent to wait on a PrepareToPut, for the
  -- case when Updates and Puts come after a PrepareToPut and they need to wait on
  -- the first Update|Put to complete.
  -- Cf. recreateCastorFile and the DiskCopy statuses 5 and 11
  UPDATE SubRequest
     SET parent = (SELECT SubRequest.id
                     FROM SubRequest, DiskCopy, Id2Type
                    WHERE SubRequest.diskCopy = DiskCopy.id
                      AND DiskCopy.id = dci
                      AND SubRequest.request = Id2Type.id
                      AND Id2Type.type <> 37  -- OBJ_PrepareToPut
                      AND SubRequest.parent = 0
                      AND DiskCopy.status IN (1, 2, 5, 6, 11) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT, WAITFS_SCHEDULING
                      AND SubRequest.status IN (0, 1, 2, 4, 13, 14, 6)), -- START, RESTART, RETRY, WAITTAPERECALL, WAITFORSCHED, BEINGSCHED, READY
        status = 5, -- WAITSUBREQ
        lastModificationTime = getTime()
  WHERE SubRequest.id = srId;
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
    DELETE FROM StageGetRequest WHERE id = rId RETURNING client INTO rclient;
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
  ELSIF rtype = 95 THEN -- SetFileGCWeightRequest
    DELETE FROM SetFileGCWeight WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 119 THEN -- StageRepackRequest
    DELETE FROM StageRepackRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 133 THEN -- StageDiskCopyReplicaRequest
    DELETE FROM StageDiskCopyReplicaRequest WHERE id = rId RETURNING client INTO rclient;
  END IF;

  -- Delete Client
  DELETE FROM Id2Type WHERE id = rclient;
  DELETE FROM Client WHERE id = rclient;
  
  -- Delete SubRequests
  DELETE FROM Id2Type WHERE id IN
    (SELECT id FROM SubRequest WHERE request = rId);
  DELETE FROM SubRequest WHERE request = rId;
END;


/*  PL/SQL method to archive a SubRequest */
CREATE OR REPLACE PROCEDURE archiveSubReq(srId IN INTEGER) AS
  rId INTEGER;
  nb INTEGER;
  nbReqs INTEGER;
  cfId NUMBER;
  rtype INTEGER;
BEGIN
  UPDATE SubRequest
     SET status = 8, parent = 0 -- FINISHED
   WHERE id = srId
  RETURNING request, castorFile INTO rId, cfId;

  -- Try to see whether another subrequest in the same
  -- request is still processing
  SELECT count(*) INTO nb FROM SubRequest
   WHERE request = rid AND status <> 8;  -- all but FINISHED

  IF nb = 0 THEN
    -- all subrequests have finished: archive or delete depending on the request type
    SELECT type INTO rtype FROM Id2Type WHERE id = rId;
    IF rtype IN (39, 42, 51, 95, 133) THEN   -- PutDone, Rm, Release, SetFileGCWeight, DiskCopyReplica
      deleteRequest(rId);
    ELSE
      UPDATE SubRequest SET status = 11 WHERE request = rId;  -- ARCHIVED 
    END IF;
  END IF;

  -- Check that we don't have too many requests for this file in the DB
  -- XXX dropped for the time being as it introduced deadlocks with itself and with selectFiles2Delete!
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


/* PL/SQL method to delete a group of requests */
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
    ---- PrepareToGet -----
  deleteRequests('StagePrepareToGetRequest', '36', cleanTab);
    ---- PrepareToPut ----
  deleteRequests('StagePrepareToPutRequest', '37', cleanTab);
    ---- PrepareToUpdate ----
  deleteRequests('StagePrepareToUpdateRequest', '38', cleanTab);
    ---- PutDone ----
  deleteRequests('StagePutDoneRequest', '39', cleanTab);
    ---- Rm ----
  deleteRequests('StageRmRequest', '42', cleanTab);
    ---- Repack ----
  deleteRequests('StageRepackRequest', '119', cleanTab);
    ---- DiskCopyReplica ----
  deleteRequests('StageDiskCopyReplicaRequest', '133', cleanTab);
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
       AND max(status) = 11  -- only ARCHIVED requests are taken into account here
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
       AND max(status) <= 12 -- repack 
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
                                                     dcid OUT INTEGER, optimized IN INTEGER) AS
  fileSystemId NUMBER;
  cfid NUMBER;
  fsDiskServer NUMBER;
  fileSize NUMBER;
  nb NUMBER;
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
    fileSystemId := 0;
    -- The DiskCopy had no FileSystem associated with it which indicates that
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
                 AND SubRequest.status = 4 
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
      diskServerName := a.name;
      rmountPoint    := a.mountPoint;
      fileSystemId   := a.id;
      -- Check that we don't already have a copy of this file on this filesystem.
      -- This will never happen in normal operations but may be the case if a filesystem
      -- was disabled and did come back while the tape recall was waiting.
      -- Even if we could optimize by cancelling remaining unneeded tape recalls when a
      -- fileSystem comes backs, the ones running at the time of the come back will have     
      SELECT count(*) INTO nb
        FROM DiskCopy
       WHERE fileSystem = a.id
         AND castorfile = cfid
         AND status = 0; -- STAGED
      IF nb != 0 THEN
        raise_application_error(-20103, 'Recaller could not find a FileSystem in production in the requested SvcClass and without copies of this file');
      END IF;
      -- Set the diskcopy's filesystem
      UPDATE DiskCopy 
         SET fileSystem = a.id 
       WHERE id = dcid;
      updateFsRecallerOpened(a.diskServer, a.id, a.fileSize);
      RETURN;
    END LOOP;  

    -- If we didn't find a filesystem rerun with optimization disabled
    IF fileSystemId = 0 THEN
      IF optimized = 1 THEN
        bestFileSystemForSegment(segmentId, diskServerName, rmountPoint, rpath, dcid, 0);
	RETURN;
      ELSE
        RAISE NO_DATA_FOUND; -- we did not find any suitable FS, even without optimization
      END IF;
    END IF;
  END IF;

  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Just like in bestTapeCopyForStream if we were called with optimization enabled
    -- and nothing was found, rerun the procedure again with optimization disabled to
    -- truly make sure nothing is found!
    IF optimized = 1 THEN
      bestFileSystemForSegment(segmentId, diskServerName, rmountPoint, rpath, dcid, 0);
      -- Since we recursively called ourselves, we should not do
      -- any update in the outer call
      RETURN;
    ELSE
      RAISE;
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
  UPDATE DiskCopy SET status = 4 WHERE id = dci RETURNING fileSystem INTO fsid; -- DISKCOPY_FAILED
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
  TYPE DiskServerName IS RECORD (diskServer VARCHAR(2048));
  TYPE DiskServerList_Cur IS REF CURSOR RETURN DiskServerName;
  TYPE SchedulerResourceLine IS RECORD (
	diskServerName VARCHAR(2048),
	diskServerStatus INTEGER,
	diskServerAdminStatus INTEGER,
	fileSystemMountPoint VARCHAR(2048),
	fileSystemStatus INTEGER,
	fileSystemAdminStatus INTEGER,
	fileSystemDiskPoolName VARCHAR(2048),
	fileSystemSvcClassName VARCHAR(2048));
  TYPE SchedulerResources_Cur IS REF CURSOR RETURN SchedulerResourceLine;	
  TYPE FileEntry IS RECORD (
        fileid INTEGER,
        nshost VARCHAR2(2048));
  TYPE FileEntry_Cur IS REF CURSOR RETURN FileEntry; 
  TYPE DbMigrationInfo IS RECORD (
	id NUMBER,
	copyNb NUMBER,
	fileName VARCHAR2(2048),
	nsHost VARCHAR2(2048),
	fileId NUMBER,
	fileSize NUMBER);
  TYPE DbMigrationInfo_Cur IS REF CURSOR RETURN DbMigrationInfo;
  TYPE DbStreamInfo IS RECORD (
	id NUMBER,
	status INTEGER,
	numFile NUMBER,
	byteVolume NUMBER);
  TYPE DbStreamInfo_Cur IS REF CURSOR RETURN DbStreamInfo;
  TYPE DbRecallInfo IS RECORD ( 
	vid VARCHAR2(2048),
	tapeId NUMBER,
	dataVolume NUMBER,
	numbFiles NUMBER,
	expireTime NUMBER);
  TYPE DbRecallInfo_Cur IS REF CURSOR RETURN DbRecallInfo;
  TYPE StreamPolicyInfo IS RECORD (
	id INTEGER,
	status NUMBER,
	numFile NUMBER,
	byteVolume NUMBER);
  TYPE StreamPolicyInfo_Cur IS REF CURSOR RETURN StreamPolicyInfo;
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
   WHERE Stream.status = 0 -- PENDING
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


/* PL/SQL method implementing checkForD2DCopyOrRecall */
/* dcId is the DiskCopy id of the best candidate for replica, 0 if none is found (tape recall), -1 in case of user error */
/* Internally used by getDiskCopiesForJob and processPrepareRequest */
CREATE OR REPLACE PROCEDURE checkForD2DCopyOrRecall
                            (cfId IN NUMBER, srId IN NUMBER, svcClassId IN NUMBER,
                             dcId OUT NUMBER) AS
BEGIN
  -- First check whether we are a disk only pool that is already full.
  -- In such a case, we should fail the request with an ENOSPACE error
  IF (checkFailJobsWhenNoSpace(svcClassId) = 1) THEN
    dcId := -1;
    UPDATE SubRequest
       SET status = 7, -- FAILED
           errorCode = 28, -- ENOSPC
           errorMessage = 'File creation canceled since diskPool is full'
     WHERE id = srId;
    COMMIT;
    RETURN;
  END IF;
  -- Check whether there are any diskcopies available for a disk2disk copy
  SELECT id INTO dcId 
    FROM (
      SELECT DiskCopy.id
        FROM DiskCopy, FileSystem, DiskServer
       WHERE DiskCopy.castorfile = cfId
         AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
         AND FileSystem.id = DiskCopy.fileSystem
         AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.id = FileSystem.diskserver
         AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskCopy.id NOT IN (
           -- don't select source diskcopies which already failed more than 10 times
           SELECT sourceDiskCopyId FROM (
             SELECT count(*) c, R.sourceDiskCopyId 
               FROM StageDiskCopyReplicaRequest R, SubRequest
              WHERE SubRequest.request = R.id
                AND SubRequest.status = 9 -- FAILED
              GROUP BY sourceDiskCopyId)
           WHERE c >= 10)
         ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                                 FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams,
                                 FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC
         )
     WHERE ROWNUM < 2;
  -- We found at least one, therefore we schedule a disk2disk
  -- copy from the existing diskcopy not available to this svcclass
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
    dcId := -1;
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
     WHERE id = srId;
    COMMIT;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- We did not find the very special case, go for recall
    dcId := 0;
  END;
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

/* PL/SQL method implementing createDiskCopyReplicaRequest */
CREATE OR REPLACE PROCEDURE createDiskCopyReplicaRequest
(sourceSrId IN INTEGER, sourceDcId IN INTEGER, destSvcId IN INTEGER) AS
  srId NUMBER;
  cfId NUMBER;
  destDcId NUMBER;
  reqId NUMBER;  
  clientId NUMBER;
  fileName VARCHAR2(2048);
  fileSize NUMBER;
  fileId NUMBER;
  nsHost VARCHAR2(2048);
  rpath VARCHAR2(2048);
BEGIN
  -- Extract the castorfile associated with the request, this is needed to 
  -- create the StageDiskCopyReplicaRequest's diskcopy and subrequest entries. 
  -- Also for disk2disk copying across service classes make the originating 
  -- subrequest wait on the completion of the transfer.
  IF sourceSrId > 0 THEN
    UPDATE SubRequest
       SET status = 5, parent = ids_seq.nextval -- WAITSUBREQ
     WHERE id = sourceSrId
    RETURNING castorFile, parent INTO cfId, srId;
  ELSE
    SELECT castorfile INTO cfId FROM DiskCopy WHERE id = sourceDcId;
    SELECT ids_seq.nextval INTO srId FROM Dual;
  END IF;

  SELECT fileid, nshost, filesize, lastknownfilename 
    INTO fileId, nsHost, fileSize, fileName
    FROM CastorFile WHERE id = cfId;

  -- Create the Client entry. 
  INSERT INTO Client (ipaddress, port, id, version, secure)
    VALUES (0, 0, ids_seq.nextval, 0, 0)
  RETURNING id INTO clientId;
  INSERT INTO Id2Type (id, type) VALUES (clientId, 129);  -- OBJ_Client
  
  -- Create the StageDiskCopyReplicaRequest. The euid and egid values default to
  -- 0 here, this indicates the request came from the user root. When the
  -- jobManager encounters the StageDiskCopyReplicaRequest it will automatically
  -- use the stage:st account for submission into LSF.
  SELECT ids_seq.nextval INTO destDcId FROM Dual;
  INSERT INTO StageDiskCopyReplicaRequest 
    (svcclassname, reqid, creationtime, lastmodificationtime, id, svcclass, 
     client, sourcediskcopyid, destdiskcopyid)
  VALUES ((SELECT name FROM SvcClass WHERE id = destSvcId), uuidgen(), gettime(), 
     gettime(), ids_seq.nextval, destSvcId, clientId, sourceDcId,  destDcId)
  RETURNING id INTO reqId;
  INSERT INTO Id2Type (id, type) VALUES (reqId, 133);  -- OBJ_StageDiskCopyReplicaRequest;
 
  -- Create the SubRequest setting the initial status to READYFORSCHED for
  -- immediate dispatching i.e no stager processing by the jobManager.
  INSERT INTO SubRequest
    (retrycounter, filename, protocol, xsize, priority, subreqid, flags, modebits,
     creationtime, lastmodificationtime, answered, id, diskcopy, castorfile, parent,
     status, request, getnextstatus, errorcode)
  VALUES (0, fileName, 'rfio', fileSize, 0, uuidgen(), 0, 0, gettime(), gettime(),
     0, srId, destDcId, cfId, 0, 13, reqId, 0, 0);
  INSERT INTO Id2Type (id, type) VALUES (srId, 27);  -- OBJ_SubRequest
  
  -- Create the DiskCopy without filesystem
  buildPathFromFileId(fileId, nsHost, destDcId, rpath);
  INSERT INTO DiskCopy (path, id, filesystem, castorfile, status, creationtime)
    VALUES (rpath, destDcId, 0, cfId, 1, gettime());  -- WAITDISK2DISKCOPY  
  INSERT INTO Id2Type (id, type) VALUES (destDcId, 5);  -- OBJ_DiskCopy
  COMMIT;
END;

/* PL/SQL method implementing getDiskCopiesForJob */
/* the result output is a DiskCopy status for STAGED, DISK2DISKCOPY, RECALL or WAITFS
   -1 for user failure, -2 for subrequest put in WAITSUBREQ */
CREATE OR REPLACE PROCEDURE getDiskCopiesForJob
        (srId IN INTEGER, result OUT INTEGER,
         sources OUT castor.DiskCopy_Cur) AS
  nbDCs INTEGER;
  nbDSs INTEGER;
  upd INTEGER;
  dcIds "numList";
  svcClassId NUMBER;
  cfId NUMBER;
  srcDcId NUMBER;
BEGIN
  -- retrieve the castorFile and the svcClass for this subrequest
  SELECT SubRequest.castorFile, Request.svcClass, Request.upd
    INTO cfId, svcClassId, upd
    FROM (SELECT id, svcClass, 0 upd from StageGetRequest UNION ALL
          SELECT id, svcClass, 1 upd from StageUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = srId;
  -- lock the castorFile to be safe in case of concurrent subrequests
  SELECT id into cfId FROM CastorFile where id = cfId FOR UPDATE;
  
  -- First see whether we should wait on an ongoing request
  SELECT DiskCopy.id BULK COLLECT INTO dcIds
    FROM DiskCopy, FileSystem, DiskServer
   WHERE cfId = DiskCopy.castorFile
     AND FileSystem.id(+) = DiskCopy.fileSystem
     AND nvl(FileSystem.status, 0) = 0 -- PRODUCTION
     AND DiskServer.id(+) = FileSystem.diskServer
     AND nvl(DiskServer.status, 0) = 0 -- PRODUCTION
     AND DiskCopy.status IN (1, 2, 11); -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS_SCHEDULING
  IF dcIds.COUNT > 0 THEN
    -- DiskCopy is in WAIT*, make SubRequest wait on previous subrequest and do not schedule
    makeSubRequestWait(srId, dcIds(1));
    result := -2;
    COMMIT;
    RETURN;
  END IF;
     
  -- Look for available diskcopies
  SELECT COUNT(DiskCopy.id) INTO nbDCs
    FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = svcClassId
     AND FileSystem.status = 0 -- PRODUCTION
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.status = 0 -- PRODUCTION
     AND DiskCopy.status IN (0, 6, 10);  -- STAGED, STAGEOUT, CANBEMIGR
  IF nbDCs = 0 AND upd = 1 THEN
    -- we may be the first update inside a prepareToPut, and this is allowed
    SELECT COUNT(DiskCopy.id) INTO nbDCs
      FROM DiskCopy
     WHERE DiskCopy.castorfile = cfId
       AND DiskCopy.status = 5;  -- WAITFS
    IF nbDCs = 1 THEN
      result := 5;  -- DISKCOPY_WAITFS, try recreation
      RETURN;
      -- note that we don't do here all the needed consistency checks,
      -- but recreateCastorFile takes care of all cases and will fail
      -- the subrequest or make it wait if needed.
    END IF;    
  END IF;
  
  IF nbDCs > 0 THEN
    -- Yes, we have some
    -- List available diskcopies for job scheduling
    OPEN sources
      FOR SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status,
                 FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                                FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams, 
                                FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) fsRate,
                 FileSystem.mountPoint, DiskServer.name
            FROM DiskCopy, SubRequest, FileSystem, DiskServer, DiskPool2SvcClass
           WHERE SubRequest.id = srId
             AND SubRequest.castorfile = DiskCopy.castorfile
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND DiskPool2SvcClass.child = svcClassId
             AND DiskCopy.status IN (0, 6, 10) -- STAGED, STAGEOUT, CANBEMIGR
             AND FileSystem.id = DiskCopy.fileSystem
             AND FileSystem.status = 0  -- PRODUCTION
             AND DiskServer.id = FileSystem.diskServer
             AND DiskServer.status = 0  -- PRODUCTION
           ORDER BY fsRate DESC;
    -- Give an hint to the stager whether internal replication can happen or not:
    -- count the number of diskservers which DON'T contain a diskCopy for this castorFile
    -- and are hence eligible for replication should it need to be done
    SELECT COUNT(DISTINCT(DiskServer.name)) INTO nbDSs 
      FROM DiskServer, FileSystem, DiskPool2SvcClass
     WHERE FileSystem.diskServer = DiskServer.id
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = svcClassId
       AND FileSystem.status = 0 -- PRODUCTION
       AND DiskServer.status = 0 -- PRODUCTION
       AND DiskServer.id NOT IN ( 
         SELECT DISTINCT(DiskServer.id)
           FROM DiskCopy, FileSystem, DiskServer
          WHERE DiskCopy.castorfile = cfId
            AND DiskCopy.fileSystem = FileSystem.id
            AND FileSystem.diskserver = DiskServer.id
            AND DiskCopy.status IN (0, 10));  -- STAGED, CANBEMIGR
    IF nbDSs = 0 THEN
      -- no room for replication
      result := 0;  -- DISKCOPY_STAGED
    ELSE
      -- we have some diskservers, the stager will ultimately decide whether to replicate
      result := 1;  -- DISKCOPY_WAITDISK2DISKCOPY
    END IF;
  ELSE
    -- No diskcopies available for this service class:
    -- we may have to schedule a disk to disk copy or trigger a recall
    checkForD2DCopyOrRecall(cfId, srId, svcClassId, srcDcId);
    IF srcDcId > 0 THEN
      -- create DiskCopyReplica request and make this subRequest wait on it
      createDiskCopyReplicaRequest(srId, srcDcId, svcClassId);
      result := -2;
    ELSIF srcDcId = 0 THEN
      -- no diskcopy found at all, go for recall
      result := 2;  -- DISKCOPY_WAITTAPERECALL
    ELSE
      -- user error 
      result := -1;
    END IF;
  END IF;
END;

/* PL/SQL method internalPutDoneFunc, used by fileRecalled and putDoneFunc.
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
  END IF;
  -- If we are a real PutDone (and not a put outside of a prepareToPut/Update)
  -- then we have to archive the original prepareToPut/Update subRequest
  IF context = 2 THEN
    DECLARE
      srId NUMBER;
    BEGIN
      -- There can be only a single PrepareTo request: any subsequent PPut would be
      -- rejected and any subsequent PUpdate would be directly archived (cf. processPrepareRequest).
      SELECT SubRequest.id INTO srId
        FROM SubRequest, 
         (SELECT id FROM StagePrepareToPutRequest UNION ALL
          SELECT id FROM StagePrepareToUpdateRequest) Request
       WHERE SubRequest.castorFile = cfId
         AND SubRequest.request = Request.id
         AND SubRequest.status = 6;  -- READY
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


/* PL/SQL procedure implementing startRepackMigration */
CREATE OR REPLACE PROCEDURE startRepackMigration(srId IN INTEGER, cfId IN INTEGER, dcId IN INTEGER) AS
  nbTC NUMBER(2);
  nbTCInFC NUMBER(2);
  fsId NUMBER;
BEGIN
  UPDATE DiskCopy SET status = 6  -- DISKCOPY_STAGEOUT 
   WHERE id = dcId RETURNING fileSystem INTO fsId;
  -- how many do we have to create ?
  -- select subrequest in status 3 in case of repack with staged diskcopy
  SELECT count(StageRepackRequest.repackVid) INTO nbTC
    FROM SubRequest, StageRepackRequest 
   WHERE subrequest.castorfile = cfId
     AND SubRequest.request = StageRepackRequest.id
     AND SubRequest.status IN (3, 4, 5, 6);  -- SUBREQUEST_WAITSCHED, WAITTAPERECALL, WAITSUBREQ, READY
  SELECT nbCopies INTO nbTCInFC
    FROM FileClass, CastorFile
   WHERE CastorFile.id = cfId
     AND FileClass.id = Castorfile.fileclass;
  -- we are not allowed to create more TapeCopies than in the FileClass specified
  IF nbTCInFC < nbTC THEN
    nbTC := nbTCInFC;
  END IF;
  
  -- we need to update other subrequests with the diskcopy
  -- we also update status so that we don't reschedule them
  UPDATE SubRequest 
     SET diskCopy = dcId, status = 12  -- SUBREQUEST_REPACK
   WHERE SubRequest.castorFile = cfId
     AND SubRequest.status IN (4, 5, 6)  -- SUBREQUEST_WAITTAPERECALL, WAITSUBREQ, READY
     AND SubRequest.request IN
       (SELECT id FROM StageRepackRequest); 
  
  -- create the required number of tapecopies for the files
  internalPutDoneFunc(cfId, fsId, 0, nbTC);
  -- set svcClass in the CastorFile for the migration
  UPDATE CastorFile SET svcClass = 
    (SELECT R.svcClass 
       FROM StageRepackRequest R, SubRequest
      WHERE SubRequest.request = R.id
        AND SubRequest.id = srId)
   WHERE id = cfId;
   
  -- update remaining STAGED diskcopies to CANBEMIGR too
  -- we may have them as result of disk2disk copies, and so far
  -- we only dealt with dcId
  UPDATE DiskCopy SET status = 10  -- DISKCOPY_CANBEMIGR
   WHERE castorFile = cfId AND status = 0;  -- DISKCOPY_STAGED
END;


/* PL/SQL method implementing processPrepareRequest */
/* the result output is a DiskCopy status for STAGED, DISK2DISKCOPY or RECALL,
   -1 for user failure, -2 for subrequest put in WAITSUBREQ */
CREATE OR REPLACE PROCEDURE processPrepareRequest
        (srId IN INTEGER, result OUT INTEGER) AS
  nbDCs INTEGER;
  svcClassId NUMBER;
  repack INTEGER;
  cfId NUMBER;
  srcDcId NUMBER;
  recSvcClass NUMBER;
  recDcId NUMBER;
BEGIN
  -- retrieve the castorfile, the svcclass and the reqId for this subrequest
  SELECT SubRequest.castorFile, Request.svcClass, Request.repack
    INTO cfId, svcClassId, repack
    FROM (SELECT id, svcClass, 0 repack FROM StagePrepareToGetRequest UNION ALL
          SELECT id, svcClass, 1 repack FROM StageRepackRequest UNION ALL
          SELECT id, svcClass, 0 repack FROM StagePrepareToUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = srId;
  -- lock the castor file to be safe in case of two concurrent subrequest
  SELECT id into cfId FROM CastorFile where id = cfId FOR UPDATE;

  -- Look for available diskcopies. Note that we never wait on other requests
  -- and we include WAITDISK2DISKCOPY as they are going to be available.
  SELECT COUNT(DiskCopy.id) INTO nbDCs
    FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = svcClassId
     AND FileSystem.status = 0 -- PRODUCTION
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.status = 0 -- PRODUCTION
     AND DiskCopy.status IN (0, 1, 6, 10);  -- STAGED, WAITDISK2DISKCOPY, STAGEOUT, CANBEMIGR
  IF nbDCs > 0 THEN
    -- Yes, we have some
    result := 0;  -- DISKCOPY_STAGED
    IF repack = 1 THEN
      BEGIN
        -- In case of Repack, start the repack migration on one diskCopy
        SELECT DiskCopy.id INTO srcDcId
          FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
         WHERE DiskCopy.castorfile = cfId
           AND DiskCopy.fileSystem = FileSystem.id
           AND FileSystem.diskpool = DiskPool2SvcClass.parent
           AND DiskPool2SvcClass.child = svcClassId
           AND FileSystem.status = 0 -- PRODUCTION
           AND FileSystem.diskserver = DiskServer.id
           AND DiskServer.status = 0 -- PRODUCTION
           AND DiskCopy.status = 0  -- STAGED
           AND ROWNUM < 2;
        startRepackMigration(srId, cfId, srcDcId);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- no data found here means that either the file
        -- is being written/migrated or there's a disk-to-disk
        -- copy going on: for this case we should actually wait
        BEGIN
          SELECT DiskCopy.id INTO srcDcId
            FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
           WHERE DiskCopy.castorfile = cfId
             AND DiskCopy.fileSystem = FileSystem.id
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND DiskPool2SvcClass.child = svcClassId
             AND FileSystem.status = 0 -- PRODUCTION
             AND FileSystem.diskserver = DiskServer.id
             AND DiskServer.status = 0 -- PRODUCTION
             AND DiskCopy.status = 1  -- WAITDISK2DISKCOPY
             AND ROWNUM < 2;
          -- found it, we wait on it
          makeSubRequestWait(srId, srcDcId);
          result := -2;
        EXCEPTION WHEN NO_DATA_FOUND THEN
           -- the file is being written/migrated, fail the request
           UPDATE SubRequest
              SET status = 7,  -- FAILED
                  errorCode = 16,  -- EBUSY
                  errorMessage = 'File is currently being written or migrated'
            WHERE id = srId;
           COMMIT;
           result := -1;  -- user error
         END;
       END;
    END IF;
    RETURN;
  END IF;
  
  -- No diskcopies available for this service class:
  -- we may have to schedule a disk to disk copy or trigger a recall
  checkForD2DCopyOrRecall(cfId, srId, svcClassId, srcDcId);
  IF srcDcId > 0 THEN  -- disk to disk copy
    IF repack = 1 THEN
      createDiskCopyReplicaRequest(srId, srcDcId, svcClassId);
      result := -2;  -- Repack waits on the disk to disk copy
    ELSE
      createDiskCopyReplicaRequest(0, srcDcId, svcClassId);
      result := 1;  -- DISKCOPY_WAITDISK2DISKCOPY, for logging purposes
    END IF;
  ELSIF srcDcId = 0 THEN  -- recall
    BEGIN
      -- check whether there's already a recall, and get its svcClass
      SELECT Request.svcClass, DiskCopy.id
        INTO recSvcClass, recDcId
        FROM (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
              SELECT id, svcClass FROM StageGetRequest UNION ALL
              SELECT id, svcClass FROM StageRepackRequest UNION ALL
              SELECT id, svcClass FROM StageUpdateRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToUpdateRequest) Request,
             SubRequest, DiskCopy
       WHERE SubRequest.request = Request.id
         AND SubRequest.castorFile = cfId
         AND DiskCopy.castorFile = cfId
         AND DiskCopy.status = 2  -- WAITTAPERECALL
         AND SubRequest.status = 4;  -- WAITTAPERECALL 
      -- we found one: we need to wait if either we are in a different svcClass
      -- so that afterwards a disk-to-disk copy is triggered, or in case of
      -- Repack so to trigger the repack migration. Note that Repack never
      -- sends a double repack request on the same file.
      IF svcClassId <> recSvcClass OR repack = 1 THEN
        -- make this request wait on the existing WAITTAPERECALL diskcopy
        makeSubRequestWait(srId, recDcId);
        result := -2;
      ELSE
        -- this is a PrepareToGet, and another request is recalling the file
        -- on our svcClass, so we can archive this one
        result := 0;  -- DISKCOPY_STAGED
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- let the stager trigger the recall
      result := 2;  -- DISKCOPY_WAITTAPERECALL
    END;
  ELSE
    result := -1;  -- user error
  END IF;
END;


/* PL/SQL method implementing processPutDoneRequest */
CREATE OR REPLACE PROCEDURE processPutDoneRequest
        (rsubreqId IN INTEGER, result OUT INTEGER) AS
  svcClassId NUMBER;
  cfId NUMBER;
  fs NUMBER;
  nbDCs INTEGER;
  putSubReq NUMBER;
BEGIN
  -- Get the svcClass and the castorFile for this subrequest
  SELECT Req.svcclass, SubRequest.castorfile
    INTO svcClassId, cfId
    FROM SubRequest, StagePutDoneRequest Req
   WHERE Subrequest.request = Req.id
     AND Subrequest.id = rsubreqId;
  -- lock the castor file to be safe in case of two concurrent subrequest
  SELECT id, fileSize INTO cfId, fs 
    FROM CastorFile 
   WHERE CastorFile.id = cfId FOR UPDATE;
  
  -- Check whether there is a Put|Update going on
  -- If any, we'll wait on one of them
  BEGIN
    SELECT subrequest.id INTO putSubReq
      FROM SubRequest, Id2Type
     WHERE SubRequest.castorfile = cfId
       AND SubRequest.request = Id2Type.id
       AND Id2Type.type IN (40, 44)  -- Put, Update
       AND SubRequest.status IN (0, 1, 2, 3, 6, 13, 14) -- START, RESTART, RETRY, WAITSCHED, READY, READYFORSCHED, BEINGSCHED
       AND ROWNUM < 2;
    -- we've found one, we wait
    UPDATE SubRequest
       SET parent = putSubReq,
           status = 5, -- WAITSUBREQ
           lastModificationTime = getTime()
     WHERE id = rsubreqId;
    result := -1;  -- no go, request in wait
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- No put waiting, look now for available DiskCopies.
    -- Here we look on all FileSystems in our svcClass
    -- regardless their status, accepting Disabled ones
    -- as there's no real IO activity involved. However the
    -- risk is that the file might not come back and it's lost!
    SELECT COUNT(DiskCopy.id) INTO nbDCs
      FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
     WHERE DiskCopy.castorfile = cfId
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskpool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = svcClassId
       AND FileSystem.diskserver = DiskServer.id
       AND DiskCopy.status = 6;  -- STAGEOUT
    IF nbDCs > 0 THEN
      -- we have it
      result := 1;
      putDoneFunc(cfId, fs, 2);   -- context = PutDone
    ELSE
      -- This is a PutDone without a put (otherwise we would have found
      -- a DiskCopy on a FileSystem), so we fail the subrequest.
      UPDATE SubRequest SET
        status = 7,
        errorCode = 1,  -- EPERM
        errorMessage = 'putDone without a put, or wrong service class'
      WHERE id = rsubReqId;
      result := 0;  -- no go
      COMMIT;
    END IF;
  END;
END;


/* This method should be called when the first byte is written to a file
 * opened with an update. This will kind of convert the update from a
 * get to a put behavior.
 */
CREATE OR REPLACE PROCEDURE firstByteWrittenProc(srId IN INTEGER) AS
  dcId NUMBER;
  cfId NUMBER;
  nbres NUMBER;
  unused NUMBER;
  stat NUMBER;
BEGIN
  -- lock the Castorfile
  SELECT castorfile, diskCopy INTO cfId, dcId
    FROM SubRequest WHERE id = srId;
  SELECT id INTO unused FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Check that the file is not busy, i.e. that we are not
  -- in the middle of migrating it. If we are, just stop and raise
  -- a user exception
  SELECT count(*) INTO nbRes FROM TapeCopy
    WHERE status = 3 -- TAPECOPY_SELECTED
    AND castorFile = cfId;
  IF nbRes > 0 THEN
    raise_application_error(-20106, 'Trying to update a busy file (ongoing migration)');
  END IF;
  -- Check that we can indeed open the file in write mode
  -- 2 criteria have to be met :
  --   - we are not using a INVALID copy (we should not update an old version)
  --   - there is no other update/put going on or there is a prepareTo and we are
  --     dealing with the same copy.
  SELECT status INTO stat FROM DiskCopy WHERE id = dcId;
  -- First the invalid case
  IF stat = 7 THEN -- INVALID
    raise_application_error(-20106, 'Trying to update an invalid copy of a file (file has been modified by somebody else concurrently)');
  END IF;
  -- if not invalid, either we are alone or we are on the right copy and we
  -- only have to check that there is a prepareTo statement. We do the check
  -- only when needed, that is STAGEOUT case
  IF stat = 6 THEN -- STAGEOUT
    BEGIN
      -- do we are other ongoing requests ?
      SELECT count(*) INTO unused FROM SubRequest WHERE diskCopy = dcId AND id != srId;
      IF (unused > 0) THEN
        -- do we have a prepareTo Request ? There can be only a single one
        -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
        -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
        SELECT SubRequest.id INTO unused
          FROM (SELECT id FROM StagePrepareToPutRequest UNION ALL
                SELECT id FROM StagePrepareToUpdateRequest) PrepareRequest,
              SubRequest
          WHERE SubRequest.CastorFile = cfId
           AND PrepareRequest.id = SubRequest.request
           AND SubRequest.status = 6;  -- READY
      END IF;
      -- we do have a prepareTo, so eveything is fine
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- No prepareTo, so prevent the writing
      raise_application_error(-20106, 'Trying to update a file already open for write (and no prepareToPut/Update context found)');
    END;
  ELSE
    -- If we are not having a STAGEOUT diskCopy, we are the only ones to write,
    -- so we have to setup everything
    -- invalidate all diskcopies
    UPDATE DiskCopy SET status = 7 -- INVALID 
     WHERE castorFile  = cfId
       AND status IN (0, 10);
    -- except the one we are dealing with that goes to STAGEOUT
    UPDATE DiskCopy SET status = 6 -- STAGEOUT
     WHERE id = dcid;
    -- Suppress all Tapecopies (avoid migration of previous version of the file)
    deleteTapeCopies(cfId);
  END IF;
END;

/* Checks whether the protocol used is supporting updates and when not
 * calls firstByteWrittenProc as if the file was already modified */
CREATE OR REPLACE PROCEDURE handleProtoNoUpd
(srId IN INTEGER, protocol VARCHAR2) AS
BEGIN
  IF protocol != 'rfio' THEN
    firstByteWrittenProc(srId);
  END IF;
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
    raise_application_error(-20104, 'SubRequest canceled while queuing in scheduler. Giving up.');
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
  -- In case the DiskCopy was in WAITFS_SCHEDULING,
  -- restart the waiting SubRequests
  UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0 -- SUBREQUEST_RESTART
   WHERE parent = srId;
  -- link DiskCopy and FileSystem and update DiskCopyStatus
  UPDATE DiskCopy SET status = 6, -- DISKCOPY_STAGEOUT
                      fileSystem = fileSystemId
   WHERE id = rdcId
   RETURNING status, path
   INTO rdcStatus, rdcPath;
END;

/* PL/SQL method implementing getUpdateStart */
CREATE OR REPLACE PROCEDURE getUpdateStart
        (srId IN INTEGER, fileSystemId IN INTEGER,
         dci OUT INTEGER, rpath OUT VARCHAR2,
         rstatus OUT NUMBER,
         reuid OUT INTEGER, regid OUT INTEGER) AS
  cfid INTEGER;
  fid INTEGER;
  dcIds "numList";
  dcIdInReq INTEGER;
  nh VARCHAR2(2048);
  fileSize INTEGER;
  srSvcClass INTEGER;
  proto VARCHAR2(2048);
  isUpd NUMBER;
BEGIN
  -- Get data
  SELECT euid, egid, svcClass, upd, diskCopy
    INTO reuid, regid, srSvcClass, isUpd, dcIdInReq
    FROM SubRequest,
        (SELECT id, euid, egid, svcClass, 0 as upd from StageGetRequest UNION ALL
         SELECT id, euid, egid, svcClass, 1 as upd from StageUpdateRequest) Request
   WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
  -- Take a lock on the CastorFile. Associated with triggers,
  -- this guarantees we are the only ones dealing with its copies
  SELECT CastorFile.fileSize, CastorFile.id
    INTO fileSize, cfId
    FROM CastorFile, SubRequest
   WHERE CastorFile.id = SubRequest.castorFile
     AND SubRequest.id = srId FOR UPDATE;
  -- Try to find local DiskCopy
  SELECT /*+ INDEX(DISKCOPY I_DISKCOPY_CASTORFILE) */ id BULK COLLECT INTO dcIds
    FROM DiskCopy
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.filesystem = fileSystemId
     AND DiskCopy.status IN (0, 6, 10); -- STAGED, STAGEOUT, CANBEMIGR
  IF dcIds.COUNT > 0 THEN
    -- We found it, so we are settled and we'll use the local copy.
    -- It might happen that we have more than one, because LSF may have
    -- scheduled a replication on a fileSystem which already had a previous diskcopy.
    -- We don't care and we randomly take the first one.
    SELECT id, path, status
      INTO dci, rpath, rstatus
      FROM DiskCopy
     WHERE id = dcIds(1);
    -- Let's update the SubRequest and set the link with the DiskCopy
    UPDATE SubRequest SET DiskCopy = dci WHERE id = srId RETURNING protocol INTO proto;
    -- In case of an update, if the protocol used does not support
    -- updates properly (via firstByteWritten call), we should
    -- call firstByteWritten now and consider that the file is being
    -- modified.
    IF isUpd = 1 THEN
      handleProtoNoUpd(srId, proto);
    END IF;
  ELSE
    -- No disk copy found on selected FileSystem. This can happen in 3 cases :
    --  + either a diskcopy was available and got disabled before this job
    --    was scheduled. Bad luck, we restart from scratch
    --  + or we are an update creating a file and there is a diskcopy in WAITFS
    --    or WAITFS_SCHEDULING associated to us. Then we have to call putStart
    --  + or we are recalling a 0-size file
    -- So we first check the update hypothesis
    IF isUpd = 1 AND dcIdInReq IS NOT NULL THEN
      DECLARE
        stat NUMBER;
      BEGIN
        SELECT status INTO stat FROM DiskCopy WHERE id = dcIdInReq;
        IF stat IN (5, 11) THEN -- WAITFS, WAITFS_SCHEDULING
          -- it is an update creating a file, let's call putStart
          putStart(srId, fileSystemId, dci, rstatus, rpath);
          RETURN;
        END IF;
      END;
    END IF;
    -- Now we check the 0-size file hypothesis
    -- XXX this is currently broken, to be fixed later
    -- It was not an update creating a file, so we restart
    UPDATE SubRequest SET status = 1 WHERE id = srId;
    dci := 0;
    rpath := '';
  END IF;
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
  IF newStatus IN (6, 11) THEN  -- READY, ARCHIVED
    UPDATE SubRequest SET getNextStatus = 1 -- GETNEXTSTATUS_FILESTAGED
     WHERE id = srId;
  END IF;
  -- Check whether it was the last subrequest in the request
  result := 1;
  SELECT id INTO result FROM SubRequest
   WHERE request = reqId
     AND status IN (0, 1, 2, 3, 4, 5, 7, 10)   -- START, RESTART, RETRY, WAITSCHED, WAITTAPERECALL, WAITSUBREQ, FAILED, FAILED_ANSWERING
     AND answered = 0
     AND ROWNUM < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN -- No data found means we were last
  result := 0;
END;


/* PL/SQL method implementing disk2DiskCopyStart */
CREATE OR REPLACE
PROCEDURE disk2DiskCopyStart
(dcId IN INTEGER, srcDcId IN INTEGER, destSvcClass IN VARCHAR2, 
 destdiskServer IN VARCHAR2, destMountPoint IN VARCHAR2, destPath OUT VARCHAR2,
 srcDiskServer OUT VARCHAR2, srcMountPoint OUT VARCHAR2, srcPath OUT VARCHAR2,
 srcSvcClass OUT VARCHAR2) AS
  fsId NUMBER;
  cfId NUMBER;
  res  NUMBER;
  cfNsHost VARCHAR2(2048); 
BEGIN
  -- Check to see if the proposed diskserver and filesystem selected by the
  -- scheduler to run the destination end of disk2disk copy transfer is in the
  -- correct service class. I.e the service class of the original request. This
  -- is done to prevent files being written to an incorrect service class when
  -- diskservers/filesystems are moved.
  BEGIN
    SELECT FileSystem.id INTO fsId
      FROM DiskServer, FileSystem, DiskPool2SvcClass, DiskPool, SvcClass
     WHERE FileSystem.diskserver = DiskServer.id
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = SvcClass.id
       AND DiskPool2SvcClass.parent = DiskPool.id
       AND SvcClass.name = destSvcClass
       AND DiskServer.name = destDiskServer
       AND FileSystem.mountPoint = destMountPoint;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20108, 'The destination job has been scheduled to the wrong service class.');
  END;
  -- Check whether the source diskcopy is still available. It may no longer be
  -- the case if it got disabled before the job started.
  BEGIN
    SELECT DiskServer.name, FileSystem.mountPoint, DiskCopy.path, 
           CastorFile.fileId, CastorFile.nsHost, SvcClass.name
      INTO srcDiskServer, srcMountPoint, srcPath, cfId, cfNsHost, srcSvcClass
      FROM DiskCopy, CastorFile, DiskServer, FileSystem, DiskPool2SvcClass, 
           DiskPool, SvcClass
     WHERE DiskCopy.id = srcDcId
       AND DiskCopy.castorfile = Castorfile.id
       AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
       AND FileSystem.id = DiskCopy.filesystem
       AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = SvcClass.id
       AND DiskPool2SvcClass.parent = DiskPool.id
       AND DiskServer.id = FileSystem.diskserver
       AND DiskServer.status IN (0, 1); -- PRODUCTION, DRAINING
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20109, 'The source DiskCopy to be replicated is no longer available.');
  END;
  -- Update the filesystem of the destination diskcopy. If the update fails
  -- either the diskcopy doesn't exist anymore indicating the cancellation of
  -- the subrequest or another transfer has already started for it.
  UPDATE DiskCopy SET filesystem = fsId
   WHERE id = dcId 
     AND filesystem = 0
     AND status = 1 -- WAITDISK2DISKCOPY
  RETURNING path INTO destPath;
  IF destPath IS NULL THEN
    raise_application_error(-20110, 'A transfer has already started for this DiskCopy.');
  END IF;
END;


/* PL/SQL method implementing disk2DiskCopyDone */
CREATE OR REPLACE PROCEDURE disk2DiskCopyDone
(dcId IN INTEGER, srcDcId IN INTEGER) AS
  srId INTEGER;
  cfId INTEGER;
  fsId INTEGER;
  maxRepl INTEGER;
  repl INTEGER;
  srcStatus INTEGER;
  proto VARCHAR2(2048);
  reqId NUMBER;
  svcClassId NUMBER;
BEGIN
  -- try to get the source status
  SELECT status INTO srcStatus FROM DiskCopy WHERE id = srcDcId;
  -- In case the status is null, it means that the source has been GCed
  -- while the disk to disk copy was processed. We will invalidate the
  -- brand new copy as we don't know in which status is should be
  IF srcStatus IS NULL THEN
    UPDATE DiskCopy SET status = 7 WHERE id = dcId -- INVALID
    RETURNING CastorFile, FileSystem INTO cfId, fsId;
    -- restart the SubRequests waiting for the copy
    UPDATE SubRequest set status = 1, -- SUBREQUEST_RESTART
                          lastModificationTime = getTime()
     WHERE diskCopy = dcId RETURNING id INTO srId;
    UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0
     WHERE parent = srId; -- SUBREQUEST_RESTART
    -- update filesystem status
    updateFsFileClosed(fsId);
    -- archive the diskCopy replica request
    archiveSubReq(srId);
    RETURN;
  END IF;
  -- otherwise, we can validate the new diskcopy
  UPDATE DiskCopy
     SET status = srcStatus
   WHERE id = dcId
  RETURNING CastorFile, FileSystem INTO cfId, fsId;
  -- update SubRequest
  UPDATE SubRequest set status = 6, -- SUBREQUEST_READY
                        getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
                        lastModificationTime = getTime()
   WHERE diskCopy = dcId RETURNING id, protocol, request INTO srId, proto, reqId;
  -- If the maxReplicaNb is exceeded, update one of the diskcopies in a 
  -- DRAINING filesystem to INVALID.
  SELECT maxReplicaNb, SvcClass.id INTO maxRepl, svcClassId
    FROM SvcClass, StageDiskCopyReplicaRequest Req, SubRequest
   WHERE SubRequest.id = srId
     AND SubRequest.request = Req.id
     AND Req.svcClass = SvcClass.id;
  SELECT count(*) into repl 
    FROM DiskCopy, FileSystem, DiskPool2SvcClass DP2SC
   WHERE castorFile = cfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskPool = DP2SC.parent
     AND DP2SC.child = svcClassId
     AND DiskCopy.status in (0, 10);  -- STAGED, CANBEMIGR
  IF repl > maxRepl THEN
    -- We did replicate only because of a DRAINING filesystem.
    -- Invalidate one of the original diskcopies, not all of them for fault resiliency purposes
    UPDATE DiskCopy set status = 7
     WHERE status in (0, 10)  -- STAGED, CANBEMIGR
       AND castorFile = cfId
       AND fileSystem in (
         SELECT FileSystem.id 
           FROM FileSystem, DiskPool2SvcClass DP2SC
          WHERE FileSystem.diskPool = DP2SC.parent
            AND DP2SC.child = svcClassId
            AND status = 1)  -- DRAINING 
       AND ROWNUM < 2;
  END IF;
  
  -- archive the diskCopy replica request
  archiveSubReq(srId);
  -- Wake up waiting subrequests
  UPDATE SubRequest SET status = 1,  -- SUBREQUEST_RESTART
         lastModificationTime = getTime(), parent = 0
   WHERE parent = srId;
  -- update filesystem status
  updateFsFileClosed(fsId);
END;


/* PL/SQL method implementing disk2DiskCopyFailed */
CREATE OR REPLACE
PROCEDURE disk2DiskCopyFailed
(dcId IN INTEGER, srId OUT INTEGER) AS
  fsId INTEGER;
  dcStatus INTEGER;
BEGIN
  -- Check that the DiskCopy still exists this could be an attempt from the
  -- jobManager to fail the request for the second time i.e. multiple daemons
  -- managing the jobs
  BEGIN
    SELECT status, filesystem INTO dcStatus, fsId FROM DiskCopy
     WHERE id = dcId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    srId := 0;
    RETURN;
  END;
  -- If the DiskCopy is STAGED then this is not a failure
  IF dcStatus = 0 THEN
    RETURN;
  END IF;
  -- Delete the DiskCopy, Since it can not be the only one, don't try to delete
  -- the Castorfile
  DELETE FROM Id2Type WHERE Id = dcId;
  DELETE FROM DiskCopy WHERE Id = dcId;
  -- Fail the corresponding subrequest
  UPDATE SubRequest 
     SET status = 9, -- SUBREQUEST_FAILED_FINISHED
         lastModificationTime = getTime()
   WHERE diskCopy = dcId 
     AND status IN (6, 14) -- SUBREQUEST_STAGEOUT, SUBREQUEST_BEINGSHCED
   RETURNING id INTO srId;
  -- Wake up other subrequests waiting on it
  UPDATE SubRequest SET status = 1, -- SUBREQUEST_RESTART
                        lastModificationTime = getTime(),
                        parent = 0
   WHERE parent = srId;
  -- update filesystem status
  IF fsId > 0 THEN
    updateFsFileClosed(fsId);
  END IF;
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
  putSC INTEGER;
  pputSC INTEGER;
  contextPIPP INTEGER;
BEGIN
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO unused FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Determine the context (Put inside PrepareToPut ?)
  BEGIN
    -- check that we are a Put/Update
    SELECT Request.svcClass INTO putSC
      FROM (SELECT id, svcClass FROM StagePutRequest UNION ALL
            SELECT id, svcClass FROM StageUpdateRequest) Request, SubRequest
     WHERE SubRequest.id = srId
       AND Request.id = SubRequest.request;
    BEGIN
      -- check that there is a PrepareToPut/Update going on. There can be only a single one
      -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
      -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
      SELECT SubRequest.diskCopy, PrepareRequest.svcClass INTO dcId, pputSC
        FROM (SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
       WHERE SubRequest.CastorFile = cfId
         AND PrepareRequest.id = SubRequest.request
         AND SubRequest.status = 6;  -- READY
      -- if we got here, we are a Put/Update inside a PrepareToPut
      -- however, are we in the same service class ?
      IF putSC != pputSC THEN
        -- No, this means we are a Put/Update and another PrepareToPut
        -- is already running in a different service class. This is forbidden
        dcId := 0;
        UPDATE SubRequest
           SET status = 7, -- FAILED
               errorCode = 16, -- EBUSY
               errorMessage = 'A prepareToPut is running in another service class for this file'
         WHERE id = srId;
        COMMIT;
        RETURN;
      END IF;
      -- if we got here, we are a Put/Update inside a PrepareToPut
      -- both running in the same service class
      contextPIPP := 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- if we got here, we are a standalone Put/Update
      contextPIPP := 0;
    END;   
  EXCEPTION WHEN NO_DATA_FOUND THEN
    DECLARE
      nbPReqs NUMBER;
    BEGIN
      -- we are either a prepareToPut, or a prepareToUpdate and it's the only one (file is being created).
      -- In case of prepareToPut we need to check that we are we the only one
      SELECT count(SubRequest.diskCopy) INTO nbPReqs
        FROM (SELECT id FROM StagePrepareToPutRequest UNION ALL
              SELECT id FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
       WHERE SubRequest.castorFile = cfId
         AND PrepareRequest.id = SubRequest.request
         AND SubRequest.status = 6;  -- READY
      -- Note that we did not select ourselves (we are in status 3)
      IF nbPReqs > 0 THEN
        -- this means we are a PrepareToPut and another PrepareToPut/Update
        -- is already running. This is forbidden
        dcId := 0;
        UPDATE SubRequest
           SET status = 7, -- FAILED
               errorCode = 16, -- EBUSY
               errorMessage = 'Another prepareToPut/Update is ongoing for this file'
         WHERE id = srId;
        COMMIT;
        RETURN;
      END IF;
      -- Everything is ok then
      contextPIPP := 0;
    END;
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
             (SELECT id, svcClass FROM StagePutRequest UNION ALL
              SELECT id, svcClass FROM StageUpdateRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToUpdateRequest) Request
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
      -- See whether we should wait on another concurrent Put|Update request
      IF rstatus = 11 THEN  -- WAITFS_SCHEDULING
        -- another Put|Update request was faster than us, so we have to wait on it
        -- to be scheduled; we will be restarted at PutStart of that one
        DECLARE
          srParent NUMBER;
        BEGIN
          -- look for it
          SELECT SubRequest.id INTO srParent
            FROM SubRequest, Id2Type
           WHERE request = Id2Type.id
             AND type IN (40, 44)  -- Put, Update
             AND diskCopy = dcId 
             AND status IN (13, 14, 6)  -- READYFORSCHED, BEINGSCHED, READY
             AND ROWNUM < 2;   -- if we have more than one just take one of them
          UPDATE SubRequest
             SET status = 5, parent = srParent  -- WAITSUBREQ
           WHERE id = srId;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- we didn't find that request: let's assume it failed, we override the status 11
          rstatus := 5;  -- WAITFS
        END;
      ELSE
        -- we are the first, we change status as we are about to go to the scheduler
        UPDATE DiskCopy SET status = 11  -- WAITFS_SCHEDULING
         WHERE castorFile = cfId
           AND status = 5;  -- WAITFS
        -- and we still return 5 = WAITFS to the stager so to go on
      END IF;
    END;
  END IF; 
  -- link SubRequest and DiskCopy
  UPDATE SubRequest SET diskCopy = dcId,
                        lastModificationTime = getTime()                        
   WHERE id = srId;
  -- we don't commit here, the stager will do that when
  -- the subRequest status will be updated to 6
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
  -- check that we are a Put or an Update
  SELECT Request.id INTO unused
    FROM SubRequest,
       (SELECT id FROM StagePutRequest UNION ALL
        SELECT id FROM StageUpdateRequest) Request
   WHERE SubRequest.id = srId
     AND Request.id = SubRequest.request;
  BEGIN
    -- check that there is a PrepareToPut/Update going on. There can be only a single one
    -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
    -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
    SELECT SubRequest.diskCopy INTO unused
      FROM SubRequest,
       (SELECT id FROM StagePrepareToPutRequest UNION ALL
        SELECT id FROM StagePrepareToUpdateRequest) Request
     WHERE SubRequest.CastorFile = cfId
       AND Request.id = SubRequest.request
       AND SubRequest.status = 6;  -- READY
    -- if we got here, we are a Put inside a PrepareToPut
    contextPIPP := 0;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- here we are a standalone Put
    contextPIPP := 1;
  END;
  
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
  -- If ts < lastUpdateTime, we were late and another job already updated the
  -- CastorFile. So we nevertheless retrieve the real file size, and
  -- we take a lock on the CastorFile. Together with triggers, this insures that
  -- we are the only ones to deal with its copies.
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
  
  SELECT fileSystem into fsId from DiskCopy
   WHERE castorFile = cfId AND status = 6;
  updateFsFileClosed(fsId);

  IF contextPIPP != 0 THEN
    -- If not a put inside a PrepareToPut/Update, create TapeCopies
    -- and update DiskCopy status
    putDoneFunc(cfId, realFileSize, contextPIPP);
  ELSE
    -- If put inside PrepareToPut/Update, restart any PutDone currently
    -- waiting on this put/update
    UPDATE SubRequest SET status = 1, parent = 0 -- RESTART
     WHERE id IN
      (SELECT SubRequest.id FROM SubRequest, Id2Type
        WHERE SubRequest.request = Id2Type.id
          AND Id2Type.type = 39       -- PutDone
          AND SubRequest.castorFile = cfId
          AND SubRequest.status = 5); -- WAITSUBREQ
  END IF;
  -- archive Subrequest
  archiveSubReq(srId);
  COMMIT;
END;


/* PL/SQL method implementing fileRecalled */
CREATE OR REPLACE PROCEDURE fileRecalled(tapecopyId IN INTEGER) AS
  subRequestId NUMBER;
  dci NUMBER;
  reqType NUMBER;
  cfId NUMBER;
  fsId NUMBER;
BEGIN
  SELECT SubRequest.id, DiskCopy.id, CastorFile.id, DiskCopy.fileSystem
    INTO subRequestId, dci, cfid, fsId
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2;  -- DISKCOPY_WAITTAPERECALL
   
  -- cleanup:
  -- delete any previous failed diskcopy for this castorfile (due to failed recall attempts for instance)
  DELETE FROM Id2Type WHERE id IN (SELECT id FROM DiskCopy WHERE castorFile = cfId AND status = 4);
  DELETE FROM DiskCopy WHERE castorFile = cfId AND status = 4;   -- FAILED
  -- mark as invalid any previous diskcopy in disabled filesystems, which may have triggered this recall
  UPDATE DiskCopy SET status = 7  -- INVALID
   WHERE fileSystem IN (SELECT id FROM FileSystem WHERE status = 2)  -- DISABLED
     AND castorFile = cfId
     AND status = 0;  -- STAGED
  
  SELECT type INTO reqType FROM Id2Type WHERE id =
    (SELECT request FROM SubRequest WHERE id = subRequestId);
  -- Repack handling:
  -- create the tapecopies for waiting subrequests and update diskcopies
  IF reqType = 119 THEN      -- OBJ_StageRepackRequest
    startRepackMigration(subRequestId, cfId, dci);
  ELSE
    UPDATE DiskCopy SET status = 0 WHERE id = dci;  -- DISKCOPY_STAGED
    -- restart this subrequest if it's not a repack one
    UPDATE SubRequest
       SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
     WHERE id = subRequestId;
  END IF;
  -- restart other requests waiting on this recall
  UPDATE SubRequest
     SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
   WHERE parent = subRequestId;
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
                          lastKnownFileName = REGEXP_REPLACE(fn,'(/){2,}','/')
      WHERE id = rid;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- insert new row
    INSERT INTO CastorFile (id, fileId, nsHost, svcClass, fileClass, fileSize,
                            creationTime, lastAccessTime, nbAccesses, lastKnownFileName)
      VALUES (ids_seq.nextval, fId, nh, sc, fc, fs, getTime(), getTime(), 1, REGEXP_REPLACE(fn,'(/){2,}','/'))
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
  unused NUMBER;
BEGIN
  BEGIN
    -- First lock the stream
    SELECT id INTO unused FROM Stream WHERE id = sid FOR UPDATE;
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


/* PL/SQL method implementing segmentsForTape */

CREATE OR REPLACE PROCEDURE segmentsForTape (tapeId IN INTEGER, segments
OUT castor.Segment_Cur) AS
  segs "numList";
  rows PLS_INTEGER := 500;
  CURSOR c1 IS
    SELECT Segment.id FROM Segment
    WHERE Segment.tape = tapeId AND Segment.status = 0 
    FOR UPDATE;
BEGIN
  OPEN c1;
  FETCH c1 BULK COLLECT INTO segs LIMIT rows;
  CLOSE c1;

  IF segs.COUNT > 0 THEN
     UPDATE Tape SET status = 4 -- MOUNTED
       WHERE id = tapeId;
    FORALL j in segs.FIRST..segs.LAST -- bulk update with the forall..
--much faster
       UPDATE Segment set status = 7 -- SELECTED
       WHERE id = segs(j);
  END IF;

  OPEN segments FOR SELECT fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm, segmCksum, errMsgTxt, errorCode, severity, blockId0, blockId1, blockId2, blockId3, creationTime, id, tape, copy, status FROM Segment
                     where id in (select * from table(segs));
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
  UPDATE SubRequest SET status = 9 -- FAILED_FINISHED
   WHERE id = srId;
  UPDATE SubRequest SET status = 1 -- RESTART
   WHERE parent = srId;
END;


/* PL/SQL method implementing failedSegments */
CREATE OR REPLACE PROCEDURE failedSegments
(segments OUT castor.Segment_Cur) AS
BEGIN
  OPEN segments FOR SELECT fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm, segmCksum, errMsgTxt, errorCode, severity, blockId0, blockId1, blockId2, blockId3, creationTime, id, tape, copy, status FROM Segment
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

/* PL/SQL method implementing stageForcedRm */
CREATE OR REPLACE PROCEDURE stageForcedRm (fid IN INTEGER,
                                           nh IN VARCHAR2) AS
  cfId INTEGER;
  nbRes INTEGER;
  dcsToRm "numList";
BEGIN
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO cfId FROM CastorFile
   WHERE fileId = fid AND nsHost = nh FOR UPDATE;
  -- list diskcopies
  SELECT id BULK COLLECT INTO dcsToRm
    FROM DiskCopy
   WHERE castorFile = cfId
     AND status IN (0, 5, 6, 10, 11);  -- STAGED, WAITFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
  -- Stop ongoing recalls
  deleteTapeCopies(cfId);
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  FOR sr IN (SELECT id, status FROM SubRequest
              WHERE diskcopy IN (SELECT * FROM TABLE(dcsToRm))) LOOP
    IF sr.status IN (0, 1, 2, 3, 5, 6, 10, 13, 14) THEN  -- All but FAILED, FINISHED, FAILED_FINISHED, ARCHIVED
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 4,  -- EINTR
             errorMessage = 'Canceled by another user request',
             parent = 0
       WHERE (id = sr.id) OR (parent = sr.id);
    END IF;
  END LOOP;
  -- Set selected DiskCopies to INVALID
  UPDATE DiskCopy SET status = 7 -- INVALID
   WHERE id IN (SELECT * FROM TABLE(dcsToRm));
END;

/* PL/SQL method implementing stageRm */
CREATE OR REPLACE PROCEDURE stageRm (srId IN INTEGER,
                                     fid IN INTEGER,
                                     nh IN VARCHAR2,
                                     svcClassId IN INTEGER,
                                     ret OUT INTEGER) AS
  cfId INTEGER;
  scId INTEGER;
  nbRes INTEGER;
  dcsToRm "numList";
BEGIN
  ret := 0;
  BEGIN
    -- Lock the access to the CastorFile
    -- This, together with triggers will avoid new TapeCopies
    -- or DiskCopies to be added
    SELECT id INTO cfId FROM CastorFile
     WHERE fileId = fid AND nsHost = nh FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Nothing to be done, this file does not exist in the stager catalog
    NULL;
  END;
  -- First select involved diskCopies
  scId := svcClassId;
  IF scId > 0 THEN
    SELECT id BULK COLLECT INTO dcsToRm FROM (
      -- first physical diskcopies
      SELECT DC.id
        FROM DiskCopy DC, FileSystem, DiskPool2SvcClass DP2SC
       WHERE DC.castorFile = cfId
         AND DC.status IN (0, 6, 10)  -- STAGED, STAGEOUT, CANBEMIGR
         AND DC.fileSystem = FileSystem.id
         AND FileSystem.diskPool = DP2SC.parent
         AND DP2SC.child = scId)
      UNION ALL (
      -- and then diskcopies resulting from PrepareToPut|Update requests
      SELECT DC.id
        FROM (SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToUpdateRequest) PrepareRequest,
             SubRequest, DiskCopy DC
       WHERE SubRequest.diskCopy = DC.id
         AND PrepareRequest.id = SubRequest.request
         AND PrepareRequest.svcClass = scId
         AND DC.castorFile = cfId
         AND DC.status IN (5, 11)  -- WAITFS, WAITFS_SCHEDULING
      );
    IF dcsToRm.COUNT = 0 THEN
      -- We didn't find anything on this svcClass, fail and return
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 2,  -- ENOENT
             errorMessage = 'File not found on this service class'
       WHERE id = srId;
      RETURN;
    END IF;
    -- Check whether something else is left: if not, do as
    -- we are performing a stageRm everywhere
    SELECT count(*) INTO nbRes FROM DiskCopy
     WHERE castorFile = cfId
       AND status IN (0, 5, 6, 10, 11)  -- STAGED, STAGEOUT, CANBEMIGR, WAITFS, WAITFS_SCHEDULING
       AND id NOT IN (SELECT * FROM TABLE(dcsToRm));
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
  
  IF scId = 0 THEN
    -- full cleanup is to be performed, do all necessary checks beforehand
    DECLARE
      segId INTEGER;
      unusedIds "numList";
    BEGIN
      -- check if removal is possible for migration
      SELECT count(*) INTO nbRes FROM TapeCopy
       WHERE status IN (0, 1, 2, 3) -- CREATED, TOBEMIGRATED, WAITINSTREAMS, SELECTED
         AND castorFile = cfId;
      IF nbRes > 0 THEN
        -- We found something, thus we cannot remove
        UPDATE SubRequest
           SET status = 7,  -- FAILED
               errorCode = 16,  -- EBUSY
               errorMessage = 'The file is not yet migrated'
         WHERE id = srId;
        RETURN;
      END IF;
      -- check if removal is possible for Disk2DiskCopy
      SELECT count(*) INTO nbRes FROM DiskCopy
       WHERE status = 1 -- DISKCOPY_WAITDISK2DISKCOPY
         AND castorFile = cfId;
      IF nbRes > 0 THEN
        -- We found something, thus we cannot remove
        UPDATE SubRequest
           SET status = 7,  -- FAILED
               errorCode = 16,  -- EBUSY
               errorMessage = 'The file is being replicated'
         WHERE id = srId;
        RETURN;
      END IF;
      -- Stop ongoing recalls if stageRm either everywhere or the only available diskcopy.
      -- This is not entirely clean: a proper operation here should be to
      -- drop the SubRequest waiting for recall but keep the recall if somebody
      -- else is doing it, and taking care of other WAITSUBREQ requests as well...
      -- but it's fair enough, provided that the last stageRm will cleanup everything.
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
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 16,  -- EBUSY
             errorMessage = 'The file is being recalled from tape'
       WHERE id = srId;
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Nothing running
      deleteTapeCopies(cfId);
      -- Delete the DiskCopies
      UPDATE DiskCopy
         SET status = 7  -- INVALID
       WHERE status = 2  -- WAITTAPERECALL
         AND castorFile = cfId;
      -- Mark the 'recall' SubRequests as failed
      -- so that clients eventually get an answer
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 16,  -- EBUSY
             errorMessage = 'Recall canceled by another user request'
       WHERE castorFile = cfId and status IN (4, 5);   -- WAITTAPERECALL, WAITSUBREQ
    END;
  END IF;

  -- Now perform the remove:
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  FOR sr IN (SELECT id, status FROM SubRequest
              WHERE diskcopy IN (SELECT * FROM TABLE(dcsToRm))) LOOP
    IF sr.status IN (0, 1, 2, 3, 5, 6, 7, 10, 13, 14) THEN  -- All but FINISHED, FAILED_FINISHED, ARCHIVED
      UPDATE SubRequest 
         SET status = 7, parent = 0,  -- FAILED
             errorCode = 4,  -- EINTR
             errorMessage = 'Canceled by another user request'
       WHERE (id = sr.id) OR (parent = sr.id);             
    END IF;
  END LOOP;
  -- Set selected DiskCopies to INVALID. In any case keep
  -- WAITTAPERECALL diskcopies so that recalls can continue.
  -- Note that WAITFS and WAITFS_SCHEDULING DiskCopies don't exist on disk
  -- so they will only be taken by the cleaning daemon for the failed DCs.
  UPDATE DiskCopy SET status = 7 -- INVALID
   WHERE id IN (SELECT * FROM TABLE(dcsToRm));
  ret := 1;  -- ok
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
CREATE OR REPLACE PROCEDURE deleteFailedDiskCopies(timeOut IN NUMBER) AS
  dcIds "numList";
  cfIds "numList";
  ct INTEGER;
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

/* Deal with old diskCopies in STAGEOUT */
CREATE OR REPLACE PROCEDURE deleteOutOfDateStageOutDcs
(timeOut IN NUMBER,
 dropped OUT castor.FileEntry_Cur,
 putDones OUT castor.FileEntry_Cur) AS
BEGIN
  -- Deal with old DiskCopies in STAGEOUT/WAITFS that have a
  -- prepareToPut. The rule is to drop the ones with 0 fileSize
  -- and issue a putDone for the others
  FOR cf IN (SELECT c.filesize, c.id, c.fileId, c.nsHost, d.fileSystem, d.id AS dcid
               FROM DiskCopy d, SubRequest s, StagePrepareToPutRequest r, Castorfile c
              WHERE d.castorfile = s.castorfile
                AND s.request = r.id
                AND c.id = d.castorfile
                AND d.creationtime < getTime() - timeOut
                AND d.status IN (5, 6, 11)) LOOP -- WAITFS, STAGEOUT, WAITFS_SCHEDULING
    IF 0 = cf.fileSize THEN
      -- here we invalidate the diskcopy and let the GC run
      UPDATE DiskCopy SET status = 7 WHERE id = cf.dcid;
      INSERT INTO OutOfDateStageOutDropped VALUES (cf.fileId, cf.nsHost);
    ELSE
      -- here we issue a putDone
      putDoneFunc(cf.id, cf.fileSystem, 2); -- context 2 : real putDone
      INSERT INTO OutOfDateStageOutPutDone VALUES (cf.fileId, cf.nsHost);
    END IF;
  END LOOP;
  OPEN dropped FOR SELECT * FROM OutOfDateStageOutDropped;
  OPEN putDones FOR SELECT * FROM OutOfDateStageOutPutDone;
END;

/* Deal with old diskCopies in WAITTAPERECALL */
CREATE OR REPLACE PROCEDURE deleteOutOfDateRecallDcs
(timeOut IN NUMBER,
 dropped OUT castor.FileEntry_Cur) AS
BEGIN
  FOR cf IN (SELECT c.filesize, c.id, c.fileId, c.nsHost, d.fileSystem, d.id AS dcid
               FROM DiskCopy d, SubRequest s, StagePrepareToPutRequest r, Castorfile c
              WHERE d.castorfile = s.castorfile
                AND s.request = r.id
                AND c.id = d.castorfile
                AND d.creationtime < getTime() - timeOut
                AND d.status = 2) LOOP -- WAITTAPERECALL
    -- cancel recall and fail subrequests
    cancelRecall(cf.id, cf.dcId, 7); -- FAILED
    INSERT INTO OutOfDateRecallDropped VALUES (cf.fileId, cf.nsHost);
  END LOOP;
  OPEN dropped FOR SELECT * FROM OutOfDateRecallDropped;
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
  IF cfIds.COUNT <= 0 THEN
    RETURN;
  END IF;
  -- Loop over the deleted files
  FOR i in cfIds.FIRST .. cfIds.LAST LOOP
    -- Lock the Castor File and retrieve size
    SELECT fileSize INTO fsize FROM CastorFile WHERE id = cfIds(i);
    -- delete the DiskCopies
    FOR d in (SELECT id FROM Diskcopy WHERE castorfile = cfIds(i)) LOOP
      DELETE FROM Id2Type WHERE id = d.id;
      DELETE FROM DiskCopy WHERE id = d.id
        RETURNING fileSystem INTO fsId;
      if fsId != 0 THEN
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
      END IF;
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
  IF dcId > 0 THEN
    SELECT fileSystem INTO fsId FROM DiskCopy WHERE id = dcId;
    -- take file closing into account
    IF fsId > 0 THEN
      updateFsFileClosed(fsId);
    END IF;
  -- ELSE the subRequest is not linked to a diskCopy: should never happen,
  -- but we observed NO DATA FOUND errors and the above SELECT is the only
  -- query that could trigger them! Anyway it's fine to ignore the FS update,
  -- it will be properly updated by the monitoring.
  END IF;
  -- Determine the context (Put inside PrepareToPut/Update ?)
  BEGIN
    -- check that there is a PrepareToPut/Update going on. There can be only a single one
    -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
    -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
    SELECT SubRequest.id INTO unused
      FROM (SELECT id FROM StagePrepareToPutRequest UNION ALL
            SELECT id FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
     WHERE SubRequest.castorFile = cfId
       AND PrepareRequest.id = SubRequest.request
       AND SubRequest.status = 6; -- READY
    -- the select worked out, so we have a prepareToPut/Update
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


/*
 * Runs a table shrink operation for maintenance purposes 
 */
CREATE OR REPLACE PROCEDURE tableShrink AS
BEGIN
  -- Loop over all tables which support row movement and recover space from 
  -- the object and all dependant objects. We deliberately ignore tables 
  -- with function based indexes here as the 'shrink space' option is not 
  -- supported.
  FOR a IN (SELECT table_name FROM user_tables
             WHERE row_movement = 'ENABLED'
               AND table_name NOT IN (
                 SELECT table_name FROM user_indexes
                  WHERE index_type LIKE 'FUNCTION-BASED%'))
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||' SHRINK SPACE CASCADE';
  END LOOP;
END;


/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR a IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('GARBAGECOLLECTJOB', 'TABLESHRINKJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(a.job_name, TRUE);
  END LOOP;

  -- Creates a db job to be run every 15 mins for the garbage collector.
  -- Such interval allows for ~300 filesystem to be processed for each round
  -- under normal conditions.
  -- XXX In case more are present, jobs will overlap during their run
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'garbageCollectJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN garbageCollect(); END;',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=15',
      ENABLED         => TRUE,
      COMMENTS        => 'Castor Garbage Collector');

  -- Creates a db job to be run every day executing the tableShrink procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'tableShrinkJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN tableShrink(); END;',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=DAILY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => '');
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
                 nvl((SELECT UNIQUE decode(SubRequest.status, 0,2, 3,2, -1)
                    FROM SubRequest,
                        (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
                         SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
                         SELECT id, svcClass FROM StagePrepareToUpdateRequest UNION ALL
                         SELECT id, svcClass FROM StageRepackRequest UNION ALL
                         SELECT id, svcClass FROM StageGetRequest) Req
                          WHERE SubRequest.CastorFile = CastorFile.id
                            AND Subrequest.status in (0,3)
                            AND request = Req.id), -1)
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
     WHERE lastKnownFileName = REGEXP_REPLACE(fn,'(/){2,}','/');
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
  UPDATE SubRequest set status = 11  -- SUBREQUEST_ARCHIVED
   WHERE SubRequest.id = sreqid;
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
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
    SELECT /*+ USE_NL(A B)
             INDEX(A I_TAPECOPY_STATUS)
             INDEX(B I_CASTORFILE_ID)
          */
           A.id BULK COLLECT INTO tcIds
      FROM TapeCopy A, CastorFile B
     WHERE A.castorFile = B.id
       AND B.svcClass = svcclassId
       AND A.status IN (0, 1) -- CREATED / TOBEMIGRATED
       FOR UPDATE;
  END IF;
  -- atomically update the status to WAITINSTREAMS (we got a lock
  -- above with the FOR UPDATE clause)
  -- using a FORALL to bulk update... forcing the use of unique index path via PK
  FORALL i in tcIds.FIRST..tcIDs.LAST
    UPDATE TAPECOPY SET STATUS = 2 WHERE id = tcIds(i);
  -- release the lock
  COMMIT;
  -- return the full resultset
  OPEN result FOR
    SELECT /*+ INDEX(b I_TAPECOPY_ID) */ * FROM TapeCopy b
    WHERE b.id in (SELECT /*+ CARDINALITY(a 5) */ *  FROM TABLE(tcIds) a);
END;


/* PL/SQL method implementing storeClusterStatus */
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


/* PL/SQL method implementing getSchedulerResources */
CREATE OR REPLACE 
PROCEDURE getSchedulerResources(resources OUT castor.SchedulerResources_Cur) AS
BEGIN
  -- Provide a list of all scheduler resources. For the moment this is just
  -- the status of all diskservers, their associated filesystems and the
  -- service class they are in.
  OPEN resources
  FOR SELECT DiskServer.name, DiskServer.status, DiskServer.adminstatus,
	     Filesystem.mountpoint, FileSystem.status, FileSystem.adminstatus,
	     DiskPool.name, SvcClass.name
        FROM DiskServer, FileSystem, DiskPool2SvcClass, DiskPool, SvcClass
       WHERE FileSystem.diskServer = DiskServer.id
         AND FileSystem.diskPool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
	 AND DiskPool2SvcClass.parent = DiskPool.id;
END;


/* PL/SQL method implementing failSchedulerJob */
CREATE OR REPLACE 
PROCEDURE failSchedulerJob(srSubReqId IN VARCHAR2, srErrorCode IN NUMBER, res OUT INTEGER) AS
  reqType NUMBER;
  reqId NUMBER;
  srId NUMBER;
  dcId NUMBER;
BEGIN
  res := 0;
  -- Get the necessary information needed about the request and subrequest
  SELECT SubRequest.request, SubRequest.id, Id2Type.type
    INTO reqId, srId, reqType
    FROM SubRequest, Id2Type
   WHERE SubRequest.subreqid = srSubReqId
     AND SubRequest.request = Id2Type.id;

  -- If the request type is a PrepareToGetRequest or PrepareToUpdateRequest 
  -- put the subrequest into SUBREQUEST_FAILED_FINISHED not SUBREQUEST_FAILED
  -- as SUBREQUEST_FAILED will trigger a reply to the client which is no 
  -- longer there.
  IF reqType = 36 OR reqType = 38 THEN
    UPDATE SubRequest 
       SET status = 9, lastModificationTime = getTime()
     WHERE id = srId     
       AND status IN (6, 14) -- SUBREQUEST_STAGEOUT, SUBREQUEST_BEINGSHCED
    RETURNING id INTO res;

  -- For StageDiskCopyReplicaRequests call disk2DiskCopyFailed. We don't restart
  -- requests waiting on the failed one as this is done in disk2DiskCopyFailed
  ELSIF reqType = 133 THEN
    SELECT diskcopy INTO dcId FROM SubRequest WHERE id = srId;
    disk2DiskCopyFailed(dcId, res);
    RETURN;
  ELSE
    -- Update the subrequest status putting the request into a SUBREQUEST_FAILED
    -- status. We only concern ourselves in the termination of a job waiting to
    -- start i.e. in a SUBREQUEST_STAGEOUT status. Requests in other statuses are
    -- left unaltered!
    res := 0;
    UPDATE SubRequest
       SET status = 7,      -- SUBREQUEST_FAILED
           errorCode = srErrorCode,
           lastModificationTime = getTime()
     WHERE id = srId
       AND status IN (6, 14) -- SUBREQUEST_STAGEOUT, SUBREQUEST_BEINGSCHED
    RETURNING id INTO res;
  END IF;

  -- In both cases, restart requests waiting on the failed one
  IF res IS NOT NULL THEN
    UPDATE SubRequest
       SET status = 1,      -- SUBREQUEST_RESTART
           parent = 0
     WHERE parent = res AND status = 5;
  END IF;
END;


/* PL/SQL method implementing jobToSchedule */
CREATE OR REPLACE
PROCEDURE jobToSchedule(srId OUT INTEGER, srSubReqId OUT VARCHAR2, srProtocol OUT VARCHAR2,
                        srXsize OUT INTEGER, srRfs OUT VARCHAR2, reqId OUT VARCHAR2,
                        cfFileId OUT INTEGER, cfNsHost OUT VARCHAR2, reqSvcClass OUT VARCHAR2,
                        reqType OUT INTEGER, reqEuid OUT INTEGER, reqEgid OUT INTEGER,
                        reqUsername OUT VARCHAR2, srOpenFlags OUT VARCHAR2, clientIp OUT INTEGER,
                        clientPort OUT INTEGER, clientVersion OUT INTEGER, clientType OUT INTEGER,
                        reqSourceDiskCopyId OUT INTEGER, reqDestDiskCopyId OUT INTEGER, 
                        askedHosts OUT castor.DiskServerList_Cur, clientSecure OUT INTEGER) AS
  dsId INTEGER;                   
BEGIN
  -- Get the next subrequest to be scheduled.
  UPDATE SubRequest 
     SET status = 14, lastModificationTime = getTime() -- SUBREQUEST_BEINGSCHED
   WHERE status = 13 -- SUBREQUEST_READYFORSCHED
     AND rownum < 2
 RETURNING id, subReqId, protocol, xsize, requestedFileSystems
    INTO srId, srSubReqId, srProtocol, srXsize, srRfs;

  -- Extract the rest of the information required to submit a job into the
  -- scheduler through the job manager.
  SELECT CastorFile.fileId, CastorFile.nsHost, SvcClass.name, Id2type.type,
         Request.reqId, Request.euid, Request.egid, Request.username, 
	 Request.direction, Request.sourceDiskCopyId, Request.destDiskCopyId,
	 Client.ipAddress, Client.port, Client.version,
	 (SELECT type 
            FROM Id2type 
           WHERE id = Client.id) clientType, Client.secure
    INTO cfFileId, cfNsHost, reqSvcClass, reqType, reqId, reqEuid, reqEgid, reqUsername, 
         srOpenFlags, reqSourceDiskCopyId, reqDestDiskCopyId, clientIp, clientPort, 
         clientVersion, clientType,  clientSecure
    FROM SubRequest, CastorFile, SvcClass, Id2type, Client,
         (SELECT id, username, euid, egid, reqid, client, 
                 'w' direction, svcClass, NULL sourceDiskCopyId, NULL destDiskCopyId
            FROM StagePutRequest 
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, 
                 'r' direction, svcClass, NULL sourceDiskCopyId, NULL destDiskCopyId
            FROM StageGetRequest 
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, 
                 'r' direction, svcClass, NULL sourceDiskCopyId, NULL destDiskCopyId
            FROM StagePrepareToGetRequest
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, 
                 'o' direction, svcClass, NULL sourceDiskCopyId, NULL destDiskCopyId
            FROM StageUpdateRequest
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, 
                 'o' direction, svcClass, NULL sourceDiskCopyId, NULL destDiskCopyId
            FROM StagePrepareToUpdateRequest
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, 
                 'r' direction, svcClass, NULL sourceDiskCopyId, NULL destDiskCopyId
            FROM StageRepackRequest
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, 
                 'w' direction, svcClass, sourceDiskCopyId, destDiskCopyId
            FROM StageDiskCopyReplicaRequest) Request
   WHERE SubRequest.id = srId
     AND SubRequest.castorFile = CastorFile.id
     AND Request.svcClass = SvcClass.id
     AND Id2type.id = SubRequest.request
     AND Request.id = SubRequest.request
     AND Request.client = Client.id;

  -- Extract additional information required for StageDiskCopyReplicaRequest's
  IF reqType = 133 THEN
    -- Set the requested filesystem for the job to the mountpoint of the 
    -- source disk copy. The scheduler plugin needs this information to correctly
    -- schedule access to the filesystem.
    SELECT CONCAT(CONCAT(DiskServer.name, ':'), FileSystem.mountpoint), DiskServer.id
      INTO srRfs, dsId
      FROM DiskServer, FileSystem, DiskCopy
     WHERE DiskCopy.id = reqSourceDiskCopyId
       AND DiskCopy.filesystem = FileSystem.id
       AND FileSystem.diskserver = DiskServer.id;
  
    -- Provide the job manager with a potential list of hosts where the
    -- destination disk2disk copy transfer could run. This is all the diskservers
    -- in the service class excluding the source and diskservers where a diskcopy
    -- of this file already resides. We restrict the number of returned 
    -- diskservers because of LSF limitations
    OPEN askedHosts
    FOR
      SELECT * FROM (
	SELECT DISTINCT(DiskServer.name)
          FROM DiskServer, FileSystem, DiskPool2SvcClass, SvcClass
         WHERE FileSystem.diskServer = DiskServer.id
           AND FileSystem.diskPool = DiskPool2SvcClass.parent
           AND DiskPool2SvcClass.child = SvcClass.id
           AND SvcClass.name = reqSvcClass
           AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
           AND DiskServer.status = 0 -- DISKSERVER_PRODUCTION
           AND FileSystem.diskserver <> dsId
           AND FileSystem.diskserver NOT IN
             (SELECT DiskServer.id
                FROM DiskServer, FileSystem, DiskCopy
               WHERE FileSystem.diskServer = DiskServer.id
                 AND DiskCopy.fileSystem = FileSystem.id
                 AND DiskCopy.castorFile = cfFileId)
           ORDER BY dbms_random.value
      ) WHERE rownum < 20;
  END IF;
END;


/* Check permissions */
CREATE OR REPLACE PROCEDURE checkPermission(isvcClass IN VARCHAR2,
                                            ieuid IN NUMBER,
                                            iegid IN NUMBER,
                                            ireqType IN NUMBER,
                                            res OUT NUMBER) AS
  unused NUMBER;
BEGIN
  SELECT count(*) INTO unused
    FROM WhiteList
   WHERE (svcClass = isvcClass OR svcClass IS NULL
          OR (length(isvcClass) IS NULL AND svcClass = 'default'))
     AND (egid = iegid OR egid IS NULL)
     AND (euid = ieuid OR euid IS NULL)
     AND (reqType = ireqType OR reqType IS NULL);
  IF unused = 0 THEN
    -- Not found in White list -> no access
    res := -1;
  ELSE
    SELECT count(*) INTO unused
      FROM BlackList
     WHERE (svcClass = isvcClass OR svcClass IS NULL
            OR (length(isvcClass) IS NULL AND svcClass = 'default'))
       AND (egid = iegid OR egid IS NULL)
       AND (euid = ieuid OR euid IS NULL)
       AND (reqType = ireqType OR reqType IS NULL);
    IF unused = 0 THEN
      -- Not Found in Black list -> access
      res := 0;
    ELSE
      -- found in Black list -> no access
      res := -1;
    END IF;
  END IF;
END;


/* PL/SQL method implementing nsFilesDeletedProc
 */
CREATE OR REPLACE PROCEDURE nsFilesDeletedProc
(nsh IN VARCHAR2,
 fileIds IN castor."cnumList",
 orphans OUT castor.IdRecord_Cur) AS
  unused NUMBER;
BEGIN
  IF fileIds.COUNT <= 0 THEN
    RETURN;
  END IF;
  -- Loop over the deleted files and split the orphan ones
  -- from the normal ones
  FOR fid in fileIds.FIRST .. fileIds.LAST LOOP
    BEGIN
      SELECT id INTO unused FROM CastorFile
       WHERE fileid = fileIds(fid) AND nsHost = nsh;
      stageForcedRm(fileIds(fid), nsh);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- this file was dropped from nameServer AND stager
      -- and still exists on disk. We put it into the list
      -- of orphan fileids to return
      INSERT INTO NsFilesDeletedOrphans VALUES(fileIds(fid));
    END;
  END LOOP;
  -- return orphan ones
  OPEN orphans FOR SELECT * FROM NsFilesDeletedOrphans;
END;


/* PL/SQL method to get all the information needed from the db to call the stream policy */
CREATE OR REPLACE PROCEDURE basicInputForStreamPolicy (svcId IN NUMBER, 
                                                       dbInfo OUT castor.StreamPolicyInfo_Cur, 
                                                       runningStreams OUT NUMBER) 
AS 
  tpId NUMBER; -- used in the loop 
  tcId NUMBER; -- used in the loop 
  streamId NUMBER; -- stream attached to the tapepool 
BEGIN 
  SELECT count(*) INTO runningStreams FROM Stream, SvcClass2TapePool 
   WHERE Stream.status = 3 -- RUNNING
     AND Stream.tapepool = SvcClass2TapePool.child 
     AND SvcClass2TapePool.parent = svcId; 
  
  OPEN dbInfo FOR 
    SELECT Stream.id, Stream.status, count(distinct stream2tapecopy.child), sum(castorfile.filesize) 
      FROM Stream2TapeCopy, TapeCopy, castorFile, Stream 
     WHERE Stream.id(+) = Stream2TapeCopy.parent 
       AND Stream2TapeCopy.child = TapeCopy.id 
       AND TapeCopy.castorfile = CastorFile.id 
       AND Stream.status IN (3, 4, 5, 6) 
       AND castorfile.svcclass = svcId -- RUNNING, STOPPED 
     GROUP BY Stream.id, Stream.status; 
END; 

/** Functions for the MigHunterDaemon **/

/* Get input for python migration policy */
CREATE OR REPLACE PROCEDURE inputForMigrationPolicy
(svcclassName IN VARCHAR2,
 policyName OUT VARCHAR2,
 svcId OUT NUMBER,
 dbInfo OUT castor.DbMigrationInfo_Cur) AS
 tcIds "numList";
BEGIN
  -- do the same operation of getMigrCandidate and return the dbInfoMigrationPolicy
  -- we look first for repack condidates for this svcclass
  SELECT TapeCopy.id BULK COLLECT INTO tcIds
    FROM TapeCopy, SubRequest, StageRepackRequest, SvcClass
   WHERE StageRepackRequest.svcclass = SvcClass.id
     AND SvcClass.name= svcclassName 
     AND SubRequest.request = StageRepackRequest.id
     AND SubRequest.status = 12  --SUBREQUEST_REPACK
     AND TapeCopy.castorfile = SubRequest.castorfile
     AND TapeCopy.status IN (0, 1)  -- CREATED / TOBEMIGRATED
     FOR UPDATE;
  -- if we didn't find anything, we look 
  -- the usual svcclass from castorfile table.
  IF tcIds.count = 0 THEN
    SELECT TapeCopy.id BULK COLLECT INTO tcIds 
      FROM TapeCopy, CastorFile, svcclass 
     WHERE TapeCopy.castorFile = CastorFile.id 
       AND CastorFile.svcClass = SvcClass.id
       AND SvcClass.name = svcclassName 
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
  -- return for Policy
  SELECT SvcClass.migratorpolicy, SvcClass.id INTO policyName, svcId 
    FROM SvcClass 
   WHERE SvcClass.name = svcClassName;
  -- return the full resultset
  OPEN dbInfo FOR
    SELECT TapeCopy.id, TapeCopy.copyNb, CastorFile.lastknownfilename, 
           CastorFile.nsHost, CastorFile.fileid, CastorFile.filesize 
      FROM Tapecopy,CastorFile
     WHERE CastorFile.id = TapeCopy.castorfile 
       AND TapeCopy.id MEMBER OF tcIds;
END;

/* Get input for python Stream Policy */

CREATE OR REPLACE PROCEDURE inputForStreamPolicy
(svcClassName IN VARCHAR2,
 policyName OUT VARCHAR2,
 runningStreams OUT INTEGER,
 maxStream OUT INTEGER,
 dbInfo OUT castor.DbStreamInfo_Cur)
AS
  tpId NUMBER; -- used in the loop
  tcId NUMBER; -- used in the loop
  streamId NUMBER; -- stream attached to the tapepool
  svcId NUMBER; -- id for the svcclass
BEGIN	
  -- info for policy
  SELECT streamPolicy, nbDrives, id INTO policyName, maxStream, svcId 
    FROM SvcClass WHERE SvcClass.name = svcClassName;
  SELECT count(*) INTO runningStreams FROM Stream,SvcClass2TapePool WHERE Stream.TapePool = SvcClass2TapePool.child AND SvcClass2TapePool.parent=svcId AND Stream.status = 3; 
  --- return for policy
  OPEN dbInfo FOR
    SELECT Stream.id, Stream.status, count(distinct Stream2TapeCopy.child), sum(CastorFile.filesize)
      FROM Stream2TapeCopy, TapeCopy, CastorFile, Stream, SvcClass2TapePool
     WHERE Stream2TapeCopy.child = TapeCopy.id(+)  
       AND TapeCopy.castorfile = CastorFile.id(+) 
       AND Stream.id = Stream2TapeCopy.parent(+)
       AND Stream.Tapepool=SvcClass2TapePool.child
       AND SvcClass2TapePool.parent=svcId 
     GROUP BY Stream.id, Stream.status;
END;

/* createOrUpdateStream */

CREATE OR REPLACE PROCEDURE createOrUpdateStream
(svcClassName IN VARCHAR2,
 initialSizeToTransfer IN NUMBER, -- total initialSizeToTransfer for the svcClass
 volumeThreashold IN NUMBER, -- parameter given by -V
 initialSizeCeiling IN NUMBER, -- to calculate the initialSizeToTransfer per stream
 doClone IN INTEGER,
 nbMigrationCandidate IN INTEGER,
 retCode OUT INTEGER) -- all candidate before applying the policy
AS
  nbOldStream NUMBER; -- stream for the specific svcclass
  nbDrives NUMBER; -- drives associated to the svcclass
  initSize NUMBER; --  the initialSizeToTransfer per stream
  tpId NUMBER; -- tape pool id
  strId NUMBER; -- stream id
  streamToClone NUMBER; -- stream id to clone
  svcId NUMBER; --svcclass id
  tcId NUMBER; -- tape copy id
BEGIN
  retCode := 0;
  -- get streamFromSvcClass
  BEGIN
    SELECT id INTO svcId FROM SvcClass 
     WHERE name = svcClassName AND ROWNUM <2;
    SELECT count(Stream.id) INTO nbOldStream 
      FROM Stream, SvcClass2TapePool
     WHERE SvcClass2TapePool.child = Stream.tapepool 
       AND SvcClass2TapePool.parent = svcId;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
    -- RTCPCLD_MSG_NOTPPOOLS
    -- restore candidate 
    retCode := -1;
    RETURN;
  END; 
    
  IF nbOldStream <= 0 AND initialSizeToTransfer < volumeThreashold THEN
    -- restore WAITINSTREAM to TOBEMIGRATED, not enough data
    retCode :=-2 ; -- RTCPCLD_MSG_DATALIMIT
    RETURN;
  END IF;
    
  IF nbOldStream >=0 AND (doClone = 1 OR nbMigrationCandidate > 0) THEN
    -- stream creator
    SELECT SvcClass.nbDrives INTO nbDrives FROM SvcClass WHERE id = svcId;
    IF nbDrives = 0 THEN
    	retCode :=-3 ; -- RESTORE NEEDED
    	RETURN;
    END IF;
    -- get the initialSizeToTransfer to associate to the stream
    IF initialSizeToTransfer/nbDrives > initialSizeCeiling THEN
      initSize := initialSizeCeiling;
    ELSE 
      initSize := initialSizeToTransfer/nbDrives;
    END IF;

    -- loop until the max number of stream
    IF nbOldStream < nbDrives THEN
      LOOP   
        -- get the tape pool with less stream
        BEGIN        	   
          SELECT TapePool.id INTO tpId FROM TapePool,SvcClass2TapePool  
           WHERE TapePool.id NOT IN (SELECT TapePool FROM Stream) AND TapePool.id= SvcClass2TapePool.child
	       AND  SvcClass2TapePool.parent=svcId AND ROWNUM <2;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- at least one stream foreach tapepool
           SELECT tapepool INTO tpId 
            FROM (SELECT tapepool, count(*) AS c FROM Stream WHERE tapepool IN
	    (SELECT SvcClass2TapePool.child FROM SvcClass2TapePool WHERE SvcClass2TapePool.parent=svcId) GROUP BY tapepool ORDER BY c ASC)
           WHERE ROWNUM < 2;  	         
	END;
	           
        -- STREAM_CREATED
        INSERT INTO Stream 
          (id, initialsizetotransfer, lastFileSystemChange, tape, lastFileSystemUsed, 
           lastButOneFileSystemUsed, tapepool, status)  
        VALUES (ids_seq.nextval, initSize, 0, 0, 0, 0, tpId, 5) RETURN id INTO strId;
        INSERT INTO Id2Type (id, type) values (strId,26); -- Stream type
    	IF doClone = 1 THEN
	  -- clone the new stream with one from the same tapepool
	  SELECT id INTO streamToClone 
            FROM Stream WHERE tapepool = tpId AND ROWNUM < 2; 
          FOR tcId IN (SELECT child FROM Stream2TapeCopy 
                        WHERE Stream2TapeCopy.parent = streamToClone) 
          LOOP
            -- a take the first one, they are supposed to be all the same
            INSERT INTO stream2tapecopy (parent, child) VALUES (strId, tpId); 
          END LOOP;
        END IF;
        nbOldStream := nbOldStream + 1;
        EXIT WHEN nbOldStream >= nbDrives;
      END LOOP;
    END IF;
  END IF;
END;


/* attach tapecopy to stream */

CREATE OR REPLACE PROCEDURE attachTapeCopiesToStreams
(tapeCopyIds IN castor."cnumList",
 tapePoolIds IN castor."cnumList")
AS
  streamId NUMBER; -- stream attached to the tapepool
BEGIN
  -- add choosen tapecopies to all Streams associated to the tapepool used by the policy 
  FOR i IN tapeCopyIds.FIRST .. tapeCopyIds.LAST LOOP
    FOR streamId IN (SELECT id FROM Stream WHERE Stream.tapepool= tapePoolIds(i)) LOOP
      DECLARE CONSTRAINT_VIOLATED EXCEPTION;
      PRAGMA EXCEPTION_INIT (CONSTRAINT_VIOLATED,-1);
      BEGIN 
      	INSERT INTO stream2tapecopy (parent ,child) VALUES (streamId.id, tapeCopyIds(i));
      EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      	UPDATE tapecopy set status=1 where id=tapeCopyIds(i);
      END;
    END LOOP;
  END LOOP;
END;

/* start choosen stream */
CREATE OR REPLACE PROCEDURE startChosenStreams
        (streamIds IN castor."cnumList",
	initSize IN NUMBER) AS
BEGIN	
  FOR i in streamIds.FIRST .. streamIds.LAST LOOP
    UPDATE Stream SET initialSizeToTransfer = initSize -- PENDING
     WHERE Stream.status IN (4,5,6) -- CREATED WAITSPACE STOPPED
       AND Stream.initialSizeToTransfer = 0 -- I overwrite it only if it is NULL
       AND id=streamIds(i);
		 		
    UPDATE Stream SET Stream.status = 0 -- PENDING
     WHERE Stream.status IN (4,5,6) -- CREATED WAITSPACE STOPPED
       AND id=streamIds(i);
  END LOOP;	
  UPDATE Stream SET Stream.Status = 6 WHERE Stream.Status IN (4,5,6);  -- CREATED WAITSPACE STOPPED (the one not changed as pending)
  COMMIT;
END;

/* stop chosen stream */
CREATE OR REPLACE PROCEDURE stopChosenStreams
        (streamIds IN castor."cnumList") AS
BEGIN	
  FOR i in streamIds.FIRST .. streamIds.LAST LOOP
    UPDATE Stream SET Stream.status = 6 -- PENDING
     WHERE id = streamIds(i);
  END LOOP;	
  COMMIT;
END;

/* resurrect Candidates */
CREATE OR REPLACE PROCEDURE resurrectCandidates
(migrationCandidates IN castor."cnumList") -- all candidate before applying the policy
AS
BEGIN
  FOR i IN migrationCandidates.FIRST .. migrationCandidates.LAST LOOP
    UPDATE TapeCopy SET status = 1 WHERE status = 2 AND id = migrationCandidates(i);
  END LOOP;
  COMMIT;
END;

/** Functions for the RecHandlerDaemon **/

/* Get input for python recall policy */
CREATE OR REPLACE PROCEDURE inputForRecallPolicy(dbInfo OUT castor.DbRecallInfo_Cur) AS
  svcId NUMBER;
BEGIN  
  OPEN dbInfo FOR 
    SELECT tape.id,tape.vid, sum(castorfile.filesize), count(distinct segment.id), 
           min(segment.creationtime)- gettime() 
      FROM TapeCopy, CastorFile, Segment, Tape
     WHERE Tape.id = Segment.tape(+) 
       AND TapeCopy.id(+) = Segment.copy
       AND CastorFile.id(+) = TapeCopy.castorfile 
       AND Tape.status=8
     GROUP BY Tape.id, Tape.vid;
END;

/* resurrect tapes */
CREATE OR REPLACE PROCEDURE resurrectTapes 
(tapeIds IN castor."cnumList") -- all candidate before applying the policy
AS
BEGIN
  FOR i IN tapeIds.FIRST .. tapeIds.LAST LOOP
    UPDATE Tape SET status = 1 WHERE status = 8 AND id = tapeIds(i);
  END LOOP;
  COMMIT;
END;

/**
  * Black and while list mechanism
  * In order to be able to enter a request for a given service class, you need :
  *   - to be in the white list for this service class
  *   - to not be in the black list for this services class
  * Being in a list means :
  *   - either that your uid,gid is explicitely in the list
  *   - or that your gid is in the list with null uid (that is group wildcard)
  *   - or there is an entry with null uid and null gid (full wild card)
  * The permissions can also have a request type. Default is null, that is everything
  */
CREATE OR REPLACE PACKAGE castorBW AS
  TYPE Privilege IS RECORD (
    svcCLass NUMBER,
    euid NUMBER,
    egid NUMBER,
    reqType NUMBER);
  -- Intersection of 2 priviledges
  -- raises -20109, "Empty privilege" in case the intersection is empty
  FUNCTION intersection(p1 IN Privilege, p2 IN Privilege) RETURN Privilege;
  -- Does one priviledge P1 contain another one P2 ?
  FUNCTION contains(p1 Privilege, p2 Privilege) RETURN Boolean;
  -- Intersection of a priviledge P with the WhiteList
  -- The result is stored in the temporary table removePrivilegeTmpTable
  -- that is cleaned up when the procedure starts
  PROCEDURE intersectionWithWhiteList(p Privilege);
  -- Difference between priviledge P1 and priviledge P2
  -- raises -20108, "Invalid privilege intersection" in case the difference
  -- can not be computed
  -- raises -20109, "Empty privilege" in case the difference is empty
  FUNCTION diff(P1 Privilege, P2 Privilege) RETURN Privilege;
  -- remove priviledge P from list L
  PROCEDURE removePrivilegeFromBlackList(p Privilege);
  -- Add priviledge P to WhiteList
  PROCEDURE addPrivilegeToWL(p Privilege);
  -- Add priviledge P to BlackList
  PROCEDURE addPrivilegeToBL(p Privilege);
  -- cleanup BlackList after privileges were removed from the whitelist
  PROCEDURE cleanupBL;
  -- Add priviledge P
  PROCEDURE addPrivilege(P Privilege);
  -- Remove priviledge P
  PROCEDURE removePrivilege(P Privilege);
END castorBW;

CREATE OR REPLACE PACKAGE BODY castorBW AS

  -- Intersection of 2 priviledges
  FUNCTION intersection(p1 IN Privilege, p2 IN Privilege)
  RETURN Privilege AS
    res Privilege;
  BEGIN
    IF p1.euid IS NULL OR p1.euid = p2.euid THEN
      res.euid := p2.euid;
    ELSIF p2.euid IS NULL THEN
      res.euid := p1.euid;
    ELSE
      raise_application_error(-20109, 'Empty privilege');
    END IF;
    IF p1.egid IS NULL OR p1.egid = p2.egid THEN
      res.egid := p2.egid;
    ELSIF p2.egid IS NULL THEN
      res.egid := p1.egid;
    ELSE
      raise_application_error(-20109, 'Empty privilege');
    END IF;
    IF p1.svcClass IS NULL OR p1.svcClass = p2.svcClass THEN
      res.svcClass := p2.svcClass;
    ELSIF p2.svcClass IS NULL THEN
      res.svcClass := p1.svcClass;
    ELSE
      raise_application_error(-20109, 'Empty privilege');
    END IF;
    IF p1.reqType IS NULL OR p1.reqType = p2.reqType THEN
      res.reqType := p2.reqType;
    ELSIF p2.reqType IS NULL THEN
      res.reqType := p1.reqType;
    ELSE
      raise_application_error(-20109, 'Empty privilege');
    END IF;
    RETURN res;
  END;
  
  -- Does one priviledge P1 contain another one P2 ?
  FUNCTION contains(p1 Privilege, p2 Privilege) RETURN Boolean AS
  BEGIN
    IF p1.euid IS NOT NULL -- p1 NULL means it contains everything !
       AND (p2.euid IS NULL OR p1.euid != p2.euid) THEN
      RETURN FALSE;
    END IF;
    IF p1.egid IS NOT NULL -- p1 NULL means it contains everything !
       AND (p2.egid IS NULL OR p1.egid != p2.egid) THEN
      RETURN FALSE;
    END IF;
    IF p1.svcClass IS NOT NULL -- p1 NULL means it contains everything !
       AND (p2.svcClass IS NULL OR p1.svcClass != p2.svcClass) THEN
      RETURN FALSE;
    END IF;
    IF p1.reqType IS NOT NULL -- p1 NULL means it contains everything !
       AND (p2.reqType IS NULL OR p1.reqType != p2.reqType) THEN
      RETURN FALSE;
    END IF;
    RETURN TRUE;
  END;
  
  -- Intersection of a priviledge P with the WhiteList
  -- The result is stored in the temporary table removePrivilegeTmpTable
  PROCEDURE intersectionWithWhiteList(p Privilege) AS
    wlr Privilege;
    tmp Privilege;
    empty_privilege EXCEPTION;
    PRAGMA EXCEPTION_INIT(empty_privilege, -20109);
  BEGIN
    DELETE FROM removePrivilegeTmpTable;
    FOR r IN (SELECT * FROM WhiteList) LOOP
      BEGIN
        wlr.svcClass := r.svcClass;
        wlr.euid := r.euid;
        wlr.egid := r.egid;
        wlr.reqType := r.reqType;
        tmp := intersection(wlr, p);
        INSERT INTO removePrivilegeTmpTable
        VALUES (tmp.svcClass, tmp.euid, tmp.egid, tmp.reqType);
      EXCEPTION WHEN empty_privilege THEN
        NULL;
      END;
    END LOOP;
  END;

  -- Difference between priviledge P1 and priviledge P2
  FUNCTION diff(P1 Privilege, P2 Privilege) RETURN Privilege AS
    empty_privilege EXCEPTION;
    PRAGMA EXCEPTION_INIT(empty_privilege, -20109);
    unused Privilege;
  BEGIN
    IF contains(P1, P2) THEN
      IF P1.euid = P2.euid OR P1.egid = P2.egid OR P1.svcClass = P2.svcClass OR P1.reqType = P2.reqType THEN
        raise_application_error(-20109, 'Empty privilege');
      ELSE
        raise_application_error(-20108, 'Invalid privilege intersection');
      END IF;
    ELSE
      BEGIN
        unused := intersection(P1, P2);
        -- no exception, so the intersection is not empty.
        -- we don't know how to handle such a case
        raise_application_error(-20108, 'Invalid privilege intersection');
      EXCEPTION WHEN empty_privilege THEN
      -- P1 and P2 do not intersect, the diff is thus P1
        RETURN P1;
      END;
    END IF;
  END;
  
  -- remove priviledge P from list L
  PROCEDURE removePrivilegeFromBlackList(p Privilege) AS
    blr Privilege;
    tmp Privilege;
    empty_privilege EXCEPTION;
    PRAGMA EXCEPTION_INIT(empty_privilege, -20109);
  BEGIN
    FOR r IN (SELECT * FROM BlackList) LOOP
      BEGIN
        blr.svcClass := r.svcClass;
        blr.euid := r.euid;
        blr.egid := r.egid;
        blr.reqType := r.reqType;
        tmp := diff(blr, p);
      EXCEPTION WHEN empty_privilege THEN
        -- diff raised an exception saying that the diff is empty
        -- thus we drop the line
        DELETE FROM BlackList
         WHERE  nvl(svcClass, -1) = nvl(r.svcClass, -1) AND
               nvl(euid, -1) = nvl(r.euid, -1) AND
               nvl(egid, -1) = nvl(r.egid, -1) AND
               nvl(reqType, -1) = nvl(r.reqType, -1);
      END;
    END LOOP;
  END;
  
  -- Add priviledge P to list L :
  PROCEDURE addPrivilegeToWL(p Privilege) AS
    wlr Privilege;
    extended boolean := FALSE;
  BEGIN
    FOR r IN (SELECT * FROM WhiteList) LOOP
      wlr.svcClass := r.svcClass;
      wlr.euid := r.euid;
      wlr.egid := r.egid;
      wlr.reqType := r.reqType;
      -- check if we extend a privilege
      IF contains(p, wlr) THEN
        IF extended THEN
          -- drop this row, it merged into the new one
          DELETE FROM WhiteList
           WHERE nvl(svcClass, -1) = nvl(wlr.svcClass, -1) AND
                 nvl(euid, -1) = nvl(wlr.euid, -1) AND
                 nvl(egid, -1) = nvl(wlr.egid, -1) AND
                 nvl(reqType, -1) = nvl(wlr.reqType, -1);
        ELSE
          -- replace old row with new one
          UPDATE WhiteList
             SET svcClass = p.svcClass,
                 euid = p.euid,
                 egid = p.egid,
                 reqType = p.reqType
           WHERE nvl(svcClass, -1) = nvl(wlr.svcClass, -1) AND
                 nvl(euid, -1) = nvl(wlr.euid, -1) AND
                 nvl(egid, -1) = nvl(wlr.egid, -1) AND
                 nvl(reqType, -1) = nvl(wlr.reqType, -1);
          extended := TRUE;
        END IF;
      END IF;
      -- check if privilege is there
      IF contains(wlr, p) THEN RETURN; END IF;
    END LOOP;
    IF NOT extended THEN
      INSERT INTO WhiteList VALUES p;
    END IF;
  END;
  
  -- Add priviledge P to list L :
  PROCEDURE addPrivilegeToBL(p Privilege) AS
    blr Privilege;
    extended boolean := FALSE;
  BEGIN
    FOR r IN (SELECT * FROM BlackList) LOOP
      blr.svcClass := r.svcClass;
      blr.euid := r.euid;
      blr.egid := r.egid;
      blr.reqType := r.reqType;
      -- check if privilege is there
      IF contains(blr, p) THEN RETURN; END IF;
      -- check if we extend a privilege
      IF contains(p, blr) THEN
        IF extended THEN
          -- drop this row, it merged into the new one
          DELETE FROM BlackList
           WHERE nvl(svcClass, -1) = nvl(blr.svcClass, -1) AND
                 nvl(euid, -1) = nvl(blr.euid, -1) AND
                 nvl(egid, -1) = nvl(blr.egid, -1) AND
                 nvl(reqType, -1) = nvl(blr.reqType, -1);
        ELSE
          -- replace old row with new one
          UPDATE BlackList
             SET svcClass = p.svcClass,
                 euid = p.euid,
                 egid = p.egid,
                 reqType = p.reqType
           WHERE nvl(svcClass, -1) = nvl(blr.svcClass, -1) AND
                 nvl(euid, -1) = nvl(blr.euid, -1) AND
                 nvl(egid, -1) = nvl(blr.egid, -1) AND
                 nvl(reqType, -1) = nvl(blr.reqType, -1);
          extended := TRUE;
        END IF;
      END IF;
    END LOOP;
    IF NOT extended THEN
      INSERT INTO BlackList VALUES p;
    END IF;
  END;
  
  -- cleanup BlackList when a privilege was removed from the whitelist
  PROCEDURE cleanupBL AS
    blr Privilege;
    c NUMBER;
  BEGIN
    FOR r IN (SELECT * FROM BlackList) LOOP
      blr.svcClass := r.svcClass;
      blr.euid := r.euid;
      blr.egid := r.egid;
      blr.reqType := r.reqType;
      intersectionWithWhiteList(blr);
      SELECT COUNT(*) INTO c FROM removePrivilegeTmpTable;
      IF c = 0 THEN
        -- we can safely drop this line
        DELETE FROM BlackList
         WHERE  nvl(svcClass, -1) = nvl(r.svcClass, -1) AND
               nvl(euid, -1) = nvl(r.euid, -1) AND
               nvl(egid, -1) = nvl(r.egid, -1) AND
               nvl(reqType, -1) = nvl(r.reqType, -1);
      END IF;
    END LOOP;
  END;

  -- Add priviledge P
  PROCEDURE addPrivilege(P Privilege) AS
  BEGIN
    removePrivilegeFromBlackList(P);
    addPrivilegeToWL(P);
    COMMIT;
  EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
  END;
  
  -- Remove priviledge P
  PROCEDURE removePrivilege(P Privilege) AS
    c NUMBER;
    wlr Privilege;
  BEGIN
    -- Check first whether there is something to remove
    intersectionWithWhiteList(P);
    SELECT COUNT(*) INTO c FROM removePrivilegeTmpTable;
    IF c = 0 THEN RETURN; END IF;
    -- Remove effectively what can be removed
    FOR r IN (SELECT * FROM WHITELIST) LOOP
      wlr.svcClass := r.svcClass;
      wlr.euid := r.euid;
      wlr.egid := r.egid;
      wlr.reqType := r.reqType;
      IF contains(P, wlr) THEN
        DELETE FROM WhiteList
         WHERE nvl(svcClass, -1) = nvl(wlr.svcClass, -1) AND
               nvl(euid, -1) = nvl(wlr.euid, -1) AND
               nvl(egid, -1) = nvl(wlr.egid, -1) AND
               nvl(reqType, -1) = nvl(wlr.reqType, -1);
      END IF;
    END LOOP;
    -- cleanup blackList
    cleanUpBL();
    -- check what remains
    intersectionWithWhiteList(P);
    SELECT COUNT(*) INTO c FROM removePrivilegeTmpTable;
    IF c = 0 THEN RETURN; END IF;
    -- If any, add them to blackList
    FOR q IN (SELECT * FROM removePrivilegeTmpTable) LOOP
      addPrivilegeToBL(q);
    END LOOP;
  END;
    
END castorBW;
/*******************************************************************
 *
 * @(#)RCSfile: oracleDebug.sql,v  Revision: 1.4  Date: 2008/01/23 11:36:01  Author: itglp 
 *
 * Some SQL code to ease support and debugging
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

CREATE OR REPLACE PACKAGE castor_debug AS
  TYPE DiskCopyDebug_typ IS RECORD (
    id INTEGER,
    diskPool VARCHAR2(2048),
    location VARCHAR2(2048),
    status NUMBER,
    creationtime DATE);
  TYPE DiskCopyDebug IS TABLE OF DiskCopyDebug_typ;
  TYPE SubRequestDebug IS TABLE OF SubRequest%ROWTYPE;
  TYPE RequestDebug_typ IS RECORD (
    creationtime DATE,
    SubReqId NUMBER,
    SubReqParentId NUMBER,
    Status NUMBER,
    username VARCHAR2(2048),
    machine VARCHAR2(2048),
    svcClassName VARCHAR2(2048),
    ReqId NUMBER,
    ReqType VARCHAR2(20));
  TYPE RequestDebug IS TABLE OF RequestDebug_typ;
  TYPE TapeCopyDebug_typ IS RECORD (
    TCId NUMBER,
    TCStatus NUMBER,
    SegId NUMBER,
    SegStatus NUMBER,
    SegErrCode NUMBER,
    VID VARCHAR2(2048),
    tpMode NUMBER,
    TapeStatus NUMBER,
    nbStreams NUMBER,
    SegErr VARCHAR2(2048));
  TYPE TapeCopyDebug IS TABLE OF TapeCopyDebug_typ;
END;


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
  ELSIF (t = 18) THEN -- Segment
    SELECT castorFile INTO cfId FROM TapeCopy, Segment WHERE Segment.id = ref AND TapeCopy.id = Segment.copy;
  END IF;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- fileid ?
  SELECT id INTO cfId FROM CastorFile WHERE fileId = ref;
  RETURN cfId;
END;


/* Get the diskcopys associated with the reference number */
CREATE OR REPLACE FUNCTION getDCs(ref number) RETURN castor_debug.DiskCopyDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT diskCopy.id,
                   diskPool.name as diskpool,
                   diskServer.name || ':' || fileSystem.mountPoint || diskCopy.path as location,
                   diskCopy.status as status,
                   to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationtime as creationtime
              FROM DiskCopy, FileSystem, DiskServer, DiskPool
             WHERE DiskCopy.fileSystem = FileSystem.id(+)
               AND FileSystem.diskServer = diskServer.id(+)
               AND DiskPool.id(+) = fileSystem.diskPool
               AND DiskCopy.castorfile = getCF(ref)) LOOP
     PIPE ROW(d);
  END LOOP;
END;


/* Get the tapecopys, segments and streams associated with the reference number */
CREATE OR REPLACE FUNCTION getTCs(ref number) RETURN castor_debug.TapeCopyDebug PIPELINED AS
BEGIN
  FOR t IN (SELECT TapeCopy.id as TCId, TapeCopy.status as TCStatus,
                   Segment.Id, Segment.status as SegStatus, Segment.errorCode as SegErrCode,
                   Tape.vid as VID, Tape.tpMode as tpMode, Tape.Status as TapeStatus,
                   count(*) as nbStreams, Segment.errMsgTxt as SegErr
              FROM TapeCopy, Segment, Tape, Stream2TapeCopy
             WHERE TapeCopy.id = Segment.copy(+)
               AND Segment.tape = Tape.id(+)
               AND TapeCopy.castorfile = getCF(ref)
               AND Stream2TapeCopy.child = TapeCopy.id
              GROUP BY TapeCopy.id, TapeCopy.status, Segment.id, Segment.status, Segment.errorCode,
                       Tape.vid, Tape.tpMode, Tape.Status, Segment.errMsgTxt) LOOP
     PIPE ROW(t);
  END LOOP;
END;


/* Get the subrequests associated with the reference number. (By castorfile/diskcopy/
 * subrequest/tapecopy or fileid
 */
CREATE OR REPLACE FUNCTION getSRs(ref number) RETURN castor_debug.SubRequestDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT * FROM SubRequest WHERE castorfile = getCF(ref)) LOOP
     PIPE ROW(d);
  END LOOP;
END;


/* Get the requests associated with the reference number. (By castorfile/diskcopy/
 * subrequest/tapecopy or fileid
 */
CREATE OR REPLACE FUNCTION getRs(ref number) RETURN castor_debug.RequestDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationtime as creationtime,
                   SubRequest.id as SubReqId, SubRequest.parent as SubReqParentId, SubRequest.Status,
                   username, machine, svcClassName, Request.id as ReqId, Request.type as ReqType
              FROM SubRequest,
                    (SELECT id, username, machine, svcClassName, 'Get' as type from StageGetRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'PGet' as type from StagePrepareToGetRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'Put' as type from StagePutRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'PPut' as type from StagePrepareToPutRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'Upd' as type from StageUpdateRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'PUpd' as type from StagePrepareToUpdateRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'Repack' as type from StageRepackRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'GetNext' as type from StageGetNextRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'UpdNext' as type from StageUpdateNextRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'PutDone' as type from StagePutDoneRequest UNION ALL
                     SELECT id, username, machine, svcClassName, 'DCRepl' as type from StageDiskCopyReplicaRequest) Request
             WHERE castorfile = getCF(ref)
               AND Request.id = SubRequest.request) LOOP
     PIPE ROW(d);
  END LOOP;
END;
