/* SQL statements for type BaseAddress */
CREATE TABLE BaseAddress (objType NUMBER, cnvSvcName VARCHAR2(2048), cnvSvcType NUMBER, target INTEGER, id INTEGER CONSTRAINT I_BaseAddress_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Client */
CREATE TABLE Client (ipAddress NUMBER, port NUMBER, version NUMBER, id INTEGER CONSTRAINT I_Client_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type ClientIdentification */
CREATE TABLE ClientIdentification (machine VARCHAR2(2048), userName VARCHAR2(2048), port NUMBER, euid NUMBER, egid NUMBER, magic NUMBER, id INTEGER CONSTRAINT I_ClientIdentification_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Disk2DiskCopyDoneRequest */
CREATE TABLE Disk2DiskCopyDoneRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskCopyId INTEGER, status NUMBER, id INTEGER CONSTRAINT I_Disk2DiskCopyDoneRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

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
CREATE TABLE GCLocalFile (fileName VARCHAR2(2048), diskCopyId INTEGER, id INTEGER CONSTRAINT I_GCLocalFile_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

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
CREATE TABLE Stream (initialSizeToTransfer INTEGER, lastFileSystemChange INTEGER, byteVolume INTEGER, id INTEGER CONSTRAINT I_Stream_Id PRIMARY KEY, tape INTEGER, lastFileSystemUsed INTEGER, lastButOneFileSystemUsed INTEGER, tapePool INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
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
