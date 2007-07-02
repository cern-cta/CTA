/* SQL statements for type BaseAddress */
CREATE TABLE BaseAddress (objType INT4, cnvSvcName VARCHAR(2048), cnvSvcType INT4, target INT8, id INT8 CONSTRAINT I_BaseAddress_Id PRIMARY KEY);

/* SQL statements for type Client */
CREATE TABLE Client (ipAddress INT4, port INT4, id INT8 CONSTRAINT I_Client_Id PRIMARY KEY);

/* SQL statements for type ClientIdentification */
CREATE TABLE ClientIdentification (machine VARCHAR(2048), userName VARCHAR(2048), port INT4, euid INT4, egid INT4, magic INT4, id INT8 CONSTRAINT I_ClientIdentification_Id PRIMARY KEY);

/* SQL statements for type Disk2DiskCopyDoneRequest */
CREATE TABLE Disk2DiskCopyDoneRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, diskCopyId INT8, status INT4, id INT8 CONSTRAINT I_Disk2DiskCopyDoneRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type GetUpdateDone */
CREATE TABLE GetUpdateDone (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, subReqId INT8, id INT8 CONSTRAINT I_GetUpdateDone_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type GetUpdateFailed */
CREATE TABLE GetUpdateFailed (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, subReqId INT8, id INT8 CONSTRAINT I_GetUpdateFailed_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type PutFailed */
CREATE TABLE PutFailed (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, subReqId INT8, id INT8 CONSTRAINT I_PutFailed_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type Files2Delete */
CREATE TABLE Files2Delete (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, diskServer VARCHAR(2048), id INT8 CONSTRAINT I_Files2Delete_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type FilesDeleted */
CREATE TABLE FilesDeleted (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_FilesDeleted_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type FilesDeletionFailed */
CREATE TABLE FilesDeletionFailed (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_FilesDeletionFailed_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type GCFile */
CREATE TABLE GCFile (diskCopyId INT8, id INT8 CONSTRAINT I_GCFile_Id PRIMARY KEY, request INTEGER);

/* SQL statements for type GCLocalFile */
CREATE TABLE GCLocalFile (fileName VARCHAR(2048), diskCopyId INT8, id INT8 CONSTRAINT I_GCLocalFile_Id PRIMARY KEY);

/* SQL statements for type MoverCloseRequest */
CREATE TABLE MoverCloseRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, subReqId INT8, fileSize INT8, timeStamp INT8, id INT8 CONSTRAINT I_MoverCloseRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type PutStartRequest */
CREATE TABLE PutStartRequest (subreqId INT8, diskServer VARCHAR(2048), fileSystem VARCHAR(2048), flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_PutStartRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type PutDoneStart */
CREATE TABLE PutDoneStart (subreqId INT8, diskServer VARCHAR(2048), fileSystem VARCHAR(2048), flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_PutDoneStart_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type GetUpdateStartRequest */
CREATE TABLE GetUpdateStartRequest (subreqId INT8, diskServer VARCHAR(2048), fileSystem VARCHAR(2048), flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_GetUpdateStartRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type QueryParameter */
CREATE TABLE QueryParameter (value VARCHAR(2048), id INT8 CONSTRAINT I_QueryParameter_Id PRIMARY KEY, query INTEGER, queryType INTEGER);

/* SQL statements for type StagePrepareToGetRequest */
CREATE TABLE StagePrepareToGetRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StagePrepareToGetRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StagePrepareToPutRequest */
CREATE TABLE StagePrepareToPutRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StagePrepareToPutRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StagePrepareToUpdateRequest */
CREATE TABLE StagePrepareToUpdateRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StagePrepareToUpdateReque_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageGetRequest */
CREATE TABLE StageGetRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StageGetRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StagePutRequest */
CREATE TABLE StagePutRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StagePutRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageUpdateRequest */
CREATE TABLE StageUpdateRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StageUpdateRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageRmRequest */
CREATE TABLE StageRmRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StageRmRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StagePutDoneRequest */
CREATE TABLE StagePutDoneRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, parentUuid VARCHAR(2048), id INT8 CONSTRAINT I_StagePutDoneRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER, parent INTEGER);

/* SQL statements for type StageFileQueryRequest */
CREATE TABLE StageFileQueryRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, fileName VARCHAR(2048), id INT8 CONSTRAINT I_StageFileQueryRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageRequestQueryRequest */
CREATE TABLE StageRequestQueryRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StageRequestQueryRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageFindRequestRequest */
CREATE TABLE StageFindRequestRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StageFindRequestRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type SubRequest */
CREATE TABLE SubRequest (retryCounter INT4, fileName VARCHAR(2048), protocol VARCHAR(2048), xsize INT8, priority INT4, subreqId VARCHAR(2048), flags INT4, modeBits INT4, creationTime INT8, lastModificationTime INT8, answered INT4, errorCode INT4, errorMessage VARCHAR(2048), id INT8 CONSTRAINT I_SubRequest_Id PRIMARY KEY, diskcopy INTEGER, castorFile INTEGER, parent INTEGER, status INTEGER, request INTEGER, getNextStatus INTEGER);

/* SQL statements for type StageReleaseFilesRequest */
CREATE TABLE StageReleaseFilesRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StageReleaseFilesRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageAbortRequest */
CREATE TABLE StageAbortRequest (parentUuid VARCHAR(2048), flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StageAbortRequest_Id PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageGetNextRequest */
CREATE TABLE StageGetNextRequest (parentUuid VARCHAR(2048), flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StageGetNextRequest_Id PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER);

/* SQL statements for type StagePutNextRequest */
CREATE TABLE StagePutNextRequest (parentUuid VARCHAR(2048), flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StagePutNextRequest_Id PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageUpdateNextRequest */
CREATE TABLE StageUpdateNextRequest (parentUuid VARCHAR(2048), flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, id INT8 CONSTRAINT I_StageUpdateNextRequest_Id PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER);

/* SQL statements for type Tape */
CREATE TABLE Tape (vid VARCHAR(2048), side INT4, tpmode INT4, errMsgTxt VARCHAR(2048), errorCode INT4, severity INT4, vwAddress VARCHAR(2048), id INT8 CONSTRAINT I_Tape_Id PRIMARY KEY, stream INTEGER, status INTEGER);

/* SQL statements for type Segment */
CREATE TABLE Segment (fseq INT4, offset INT8, bytes_in INT8, bytes_out INT8, host_bytes INT8, segmCksumAlgorithm VARCHAR(2048), segmCksum INT4, errMsgTxt VARCHAR(2048), errorCode INT4, severity INT4, blockId0 INT4, blockId1 INT4, blockId2 INT4, blockId3 INT4, id INT8 CONSTRAINT I_Segment_Id PRIMARY KEY, tape INTEGER, copy INTEGER, status INTEGER);

/* SQL statements for type TapePool */
CREATE TABLE TapePool (name VARCHAR(2048), id INT8 CONSTRAINT I_TapePool_Id PRIMARY KEY);

/* SQL statements for type TapeCopy */
CREATE TABLE TapeCopy (copyNb INT4, id INT8 CONSTRAINT I_TapeCopy_Id PRIMARY KEY, castorFile INTEGER, status INTEGER);

/* SQL statements for type CastorFile */
CREATE TABLE CastorFile (fileId INT8, nsHost VARCHAR(2048), fileSize INT8, creationTime INT8, lastAccessTime INT8, nbAccesses INT4, lastKnownFileName VARCHAR(2048), lastUpdateTime INT8, id INT8 CONSTRAINT I_CastorFile_Id PRIMARY KEY, svcClass INTEGER, fileClass INTEGER);

/* SQL statements for type DiskCopy */
CREATE TABLE DiskCopy (path VARCHAR(2048), gcWeight float, creationTime INT8, id INT8 CONSTRAINT I_DiskCopy_Id PRIMARY KEY, fileSystem INTEGER, castorFile INTEGER, status INTEGER);

/* SQL statements for type FileSystem */
CREATE TABLE FileSystem (free INT8, mountPoint VARCHAR(2048), minFreeSpace float, minAllowedFreeSpace float, maxFreeSpace float, spaceToBeFreed INT8, totalSize INT8, readRate INT8, writeRate INT8, nbReadStreams INT4, nbWriteStreams INT4, nbReadWriteStreams INT4, nbMigratorStreams INT4, nbRecallerStreams INT4, id INT8 CONSTRAINT I_FileSystem_Id PRIMARY KEY, diskPool INTEGER, diskserver INTEGER, status INTEGER, adminStatus INTEGER);

/* SQL statements for type SvcClass */
CREATE TABLE SvcClass (nbDrives INT4, name VARCHAR(2048), defaultFileSize INT8, maxReplicaNb INT4, replicationPolicy VARCHAR(2048), gcPolicy VARCHAR(2048), migratorPolicy VARCHAR(2048), recallerPolicy VARCHAR(2048), id INT8 CONSTRAINT I_SvcClass_Id PRIMARY KEY);
CREATE TABLE SvcClass2TapePool (Parent INTEGER, Child INTEGER);
CREATE INDEX I_SvcClass2TapePool_C on SvcClass2TapePool (child);
CREATE INDEX I_SvcClass2TapePool_P on SvcClass2TapePool (parent);

/* SQL statements for type DiskPool */
CREATE TABLE DiskPool (name VARCHAR(2048), id INT8 CONSTRAINT I_DiskPool_Id PRIMARY KEY);
CREATE TABLE DiskPool2SvcClass (Parent INTEGER, Child INTEGER);
CREATE INDEX I_DiskPool2SvcClass_C on DiskPool2SvcClass (child);
CREATE INDEX I_DiskPool2SvcClass_P on DiskPool2SvcClass (parent);

/* SQL statements for type Stream */
CREATE TABLE Stream (initialSizeToTransfer INT8, lastFileSystemChange INT8, id INT8 CONSTRAINT I_Stream_Id PRIMARY KEY, tape INTEGER, lastFileSystemUsed INTEGER, lastButOneFileSystemUsed INTEGER, tapePool INTEGER, status INTEGER);
CREATE TABLE Stream2TapeCopy (Parent INTEGER, Child INTEGER);
CREATE INDEX I_Stream2TapeCopy_C on Stream2TapeCopy (child);
CREATE INDEX I_Stream2TapeCopy_P on Stream2TapeCopy (parent);

/* SQL statements for type FileClass */
CREATE TABLE FileClass (name VARCHAR(2048), minFileSize INT8, maxFileSize INT8, nbCopies INT4, id INT8 CONSTRAINT I_FileClass_Id PRIMARY KEY);

/* SQL statements for type DiskServer */
CREATE TABLE DiskServer (name VARCHAR(2048), readRate INT8, writeRate INT8, nbReadStreams INT4, nbWriteStreams INT4, nbReadWriteStreams INT4, nbMigratorStreams INT4, nbRecallerStreams INT4, id INT8 CONSTRAINT I_DiskServer_Id PRIMARY KEY, status INTEGER, adminStatus INTEGER);

/* SQL statements for type SetFileGCWeight */
CREATE TABLE SetFileGCWeight (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, weight float, id INT8 CONSTRAINT I_SetFileGCWeight_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageRepackRequest */
CREATE TABLE StageRepackRequest (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, repackVid VARCHAR(2048), id INT8 CONSTRAINT I_StageRepackRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

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

/* SQL statements for type DiskPoolQuery */
CREATE TABLE DiskPoolQuery (flags INT8, userName VARCHAR(2048), euid INT4, egid INT4, mask INT4, pid INT4, machine VARCHAR(2048), svcClassName VARCHAR(2048), userTag VARCHAR(2048), reqId VARCHAR(2048), creationTime INT8, lastModificationTime INT8, diskPoolName VARCHAR(2048), id INT8 CONSTRAINT I_DiskPoolQuery_Id PRIMARY KEY, svcClass INTEGER, client INTEGER);

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
