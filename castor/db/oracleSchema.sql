/* Type2Obj metatable definition */
CREATE TABLE Type2Obj (type INTEGER CONSTRAINT PK_Type2Obj_Type PRIMARY KEY, object VARCHAR2(100) CONSTRAINT NN_Type2Obj_Object NOT NULL, svcHandler VARCHAR2(100));

/* ObjStatus metatable definition */
CREATE TABLE ObjStatus (object VARCHAR2(100) CONSTRAINT NN_ObjStatus_object NOT NULL, field VARCHAR2(100) CONSTRAINT NN_ObjStatus_field NOT NULL, statusCode INTEGER CONSTRAINT NN_ObjStatus_statusCode NOT NULL, statusName VARCHAR2(100) CONSTRAINT NN_ObjStatus_statusName NOT NULL, CONSTRAINT UN_ObjStatus_objectFieldCode UNIQUE (object, field, statusCode));

/* SQL statements for type BaseAddress */
CREATE TABLE BaseAddress (objType NUMBER, cnvSvcName VARCHAR2(2048), cnvSvcType NUMBER, target INTEGER, id INTEGER CONSTRAINT PK_BaseAddress_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Client */
CREATE TABLE Client (ipAddress NUMBER, port NUMBER, version NUMBER, secure NUMBER, id INTEGER CONSTRAINT PK_Client_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Disk2DiskCopyDoneRequest */
CREATE TABLE Disk2DiskCopyDoneRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskCopyId INTEGER, sourceDiskCopyId INTEGER, fileId INTEGER, nsHost VARCHAR2(2048), replicaFileSize INTEGER, id INTEGER CONSTRAINT PK_Disk2DiskCopyDoneRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type GetUpdateDone */
CREATE TABLE GetUpdateDone (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, fileId INTEGER, nsHost VARCHAR2(2048), id INTEGER CONSTRAINT PK_GetUpdateDone_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type GetUpdateFailed */
CREATE TABLE GetUpdateFailed (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, fileId INTEGER, nsHost VARCHAR2(2048), id INTEGER CONSTRAINT PK_GetUpdateFailed_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type PutFailed */
CREATE TABLE PutFailed (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, fileId INTEGER, nsHost VARCHAR2(2048), id INTEGER CONSTRAINT PK_PutFailed_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Files2Delete */
CREATE TABLE Files2Delete (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskServer VARCHAR2(2048), id INTEGER CONSTRAINT PK_Files2Delete_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type FilesDeleted */
CREATE TABLE FilesDeleted (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_FilesDeleted_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type FilesDeletionFailed */
CREATE TABLE FilesDeletionFailed (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_FilesDeletionFailed_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type GCFile */
CREATE TABLE GCFile (diskCopyId INTEGER, id INTEGER CONSTRAINT PK_GCFile_Id PRIMARY KEY, request INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type GCLocalFile */
CREATE TABLE GCLocalFile (fileName VARCHAR2(2048), diskCopyId INTEGER, fileId INTEGER, nsHost VARCHAR2(2048), lastAccessTime INTEGER, nbAccesses NUMBER, gcWeight NUMBER, gcTriggeredBy VARCHAR2(2048), svcClassName VARCHAR2(2048), id INTEGER CONSTRAINT PK_GCLocalFile_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type MoverCloseRequest */
CREATE TABLE MoverCloseRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, fileSize INTEGER, timeStamp INTEGER, fileId INTEGER, nsHost VARCHAR2(2048), csumType VARCHAR2(2048), csumValue VARCHAR2(2048), id INTEGER CONSTRAINT PK_MoverCloseRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type PutStartRequest */
CREATE TABLE PutStartRequest (subreqId INTEGER, diskServer VARCHAR2(2048), fileSystem VARCHAR2(2048), fileId INTEGER, nsHost VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_PutStartRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type GetUpdateStartRequest */
CREATE TABLE GetUpdateStartRequest (subreqId INTEGER, diskServer VARCHAR2(2048), fileSystem VARCHAR2(2048), fileId INTEGER, nsHost VARCHAR2(2048), flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_GetUpdateStartRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type QueryParameter */
CREATE TABLE QueryParameter (value VARCHAR2(2048), id INTEGER CONSTRAINT PK_QueryParameter_Id PRIMARY KEY, query INTEGER, queryType INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('QueryParameter', 'queryType', 0, 'REQUESTQUERYTYPE_FILENAME');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('QueryParameter', 'queryType', 1, 'REQUESTQUERYTYPE_REQID');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('QueryParameter', 'queryType', 2, 'REQUESTQUERYTYPE_USERTAG');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('QueryParameter', 'queryType', 3, 'REQUESTQUERYTYPE_FILEID');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('QueryParameter', 'queryType', 4, 'REQUESTQUERYTYPE_REQID_GETNEXT');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('QueryParameter', 'queryType', 5, 'REQUESTQUERYTYPE_USERTAG_GETNEXT');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('QueryParameter', 'queryType', 6, 'REQUESTQUERYTYPE_FILENAME_ALLSC');

/* SQL statements for type StagePrepareToGetRequest */
CREATE TABLE StagePrepareToGetRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_StagePrepareToGetRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StagePrepareToPutRequest */
CREATE TABLE StagePrepareToPutRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_StagePrepareToPutRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StagePrepareToUpdateRequest */
CREATE TABLE StagePrepareToUpdateRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_StagePrepareToUpdateRequ_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StagePutRequest */
CREATE TABLE StagePutRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_StagePutRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageUpdateRequest */
CREATE TABLE StageUpdateRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_StageUpdateRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageRmRequest */
CREATE TABLE StageRmRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_StageRmRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StagePutDoneRequest */
CREATE TABLE StagePutDoneRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, parentUuid VARCHAR2(2048), id INTEGER CONSTRAINT PK_StagePutDoneRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER, parent INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageFileQueryRequest */
CREATE TABLE StageFileQueryRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, fileName VARCHAR2(2048), id INTEGER CONSTRAINT PK_StageFileQueryRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type SubRequest */
CREATE TABLE SubRequest (retryCounter NUMBER, fileName VARCHAR2(2048), protocol VARCHAR2(2048), xsize INTEGER, priority NUMBER, subreqId VARCHAR2(2048), flags NUMBER, modeBits NUMBER, creationTime INTEGER, lastModificationTime INTEGER, answered NUMBER, errorCode NUMBER, errorMessage VARCHAR2(2048), requestedFileSystems VARCHAR2(2048), svcHandler VARCHAR2(2048), id INTEGER CONSTRAINT PK_SubRequest_Id PRIMARY KEY, diskcopy INTEGER, castorFile INTEGER, parent INTEGER, status INTEGER, request INTEGER, getNextStatus INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 0, 'SUBREQUEST_START');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 1, 'SUBREQUEST_RESTART');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 2, 'SUBREQUEST_RETRY');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 3, 'SUBREQUEST_WAITSCHED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 4, 'SUBREQUEST_WAITTAPERECALL');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 5, 'SUBREQUEST_WAITSUBREQ');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 6, 'SUBREQUEST_READY');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 7, 'SUBREQUEST_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 8, 'SUBREQUEST_FINISHED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 9, 'SUBREQUEST_FAILED_FINISHED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 10, 'SUBREQUEST_FAILED_ANSWERING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 11, 'SUBREQUEST_ARCHIVED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 12, 'SUBREQUEST_REPACK');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 13, 'SUBREQUEST_READYFORSCHED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'status', 14, 'SUBREQUEST_BEINGSCHED');

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'getNextStatus', 0, 'GETNEXTSTATUS_NOTAPPLICABLE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'getNextStatus', 1, 'GETNEXTSTATUS_FILESTAGED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('SubRequest', 'getNextStatus', 2, 'GETNEXTSTATUS_NOTIFIED');

/* SQL statements for type Tape */
CREATE TABLE Tape (vid VARCHAR2(2048), side NUMBER, tpmode NUMBER, errMsgTxt VARCHAR2(2048), errorCode NUMBER, severity NUMBER, vwAddress VARCHAR2(2048), dgn VARCHAR2(2048), label VARCHAR2(2048), density VARCHAR2(2048), devtype VARCHAR2(2048), id INTEGER CONSTRAINT PK_Tape_Id PRIMARY KEY, stream INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 0, 'TAPE_UNUSED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 1, 'TAPE_PENDING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 2, 'TAPE_WAITDRIVE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 3, 'TAPE_WAITMOUNT');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 4, 'TAPE_MOUNTED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 5, 'TAPE_FINISHED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 6, 'TAPE_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 7, 'TAPE_UNKNOWN');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 8, 'TAPE_WAITPOLICY');

/* SQL statements for type Segment */
CREATE TABLE Segment (fseq NUMBER, offset INTEGER, bytes_in INTEGER, bytes_out INTEGER, host_bytes INTEGER, segmCksumAlgorithm VARCHAR2(2048), segmCksum NUMBER, errMsgTxt VARCHAR2(2048), errorCode NUMBER, severity NUMBER, blockId0 INTEGER, blockId1 INTEGER, blockId2 INTEGER, blockId3 INTEGER, creationTime INTEGER, priority INTEGER, id INTEGER CONSTRAINT PK_Segment_Id PRIMARY KEY, copy INTEGER, status INTEGER, tape INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Segment', 'status', 0, 'SEGMENT_UNPROCESSED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Segment', 'status', 5, 'SEGMENT_FILECOPIED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Segment', 'status', 6, 'SEGMENT_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Segment', 'status', 7, 'SEGMENT_SELECTED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Segment', 'status', 8, 'SEGMENT_RETRIED');

/* SQL statements for type TapePool */
CREATE TABLE TapePool (name VARCHAR2(2048), migrSelectPolicy VARCHAR2(2048), id INTEGER CONSTRAINT PK_TapePool_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type TapeCopy */
CREATE TABLE TapeCopy (copyNb NUMBER, errorCode NUMBER, nbRetry NUMBER, missingCopies NUMBER, id INTEGER CONSTRAINT PK_TapeCopy_Id PRIMARY KEY, castorFile INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeCopy', 'status', 0, 'TAPECOPY_CREATED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeCopy', 'status', 1, 'TAPECOPY_TOBEMIGRATED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeCopy', 'status', 2, 'TAPECOPY_WAITINSTREAMS');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeCopy', 'status', 3, 'TAPECOPY_SELECTED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeCopy', 'status', 4, 'TAPECOPY_TOBERECALLED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeCopy', 'status', 5, 'TAPECOPY_STAGED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeCopy', 'status', 6, 'TAPECOPY_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeCopy', 'status', 7, 'TAPECOPY_WAITPOLICY');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeCopy', 'status', 8, 'TAPECOPY_REC_RETRY');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('TapeCopy', 'status', 9, 'TAPECOPY_MIG_RETRY');

/* SQL statements for type CastorFile */
CREATE TABLE CastorFile (fileId INTEGER, nsHost VARCHAR2(2048), fileSize INTEGER, creationTime INTEGER, lastAccessTime INTEGER, lastKnownFileName VARCHAR2(2048), lastUpdateTime INTEGER, id INTEGER CONSTRAINT PK_CastorFile_Id PRIMARY KEY, svcClass INTEGER, fileClass INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type DiskCopy */
CREATE TABLE DiskCopy (path VARCHAR2(2048), gcWeight NUMBER, creationTime INTEGER, lastAccessTime INTEGER, diskCopySize INTEGER, nbCopyAccesses NUMBER, owneruid NUMBER, ownergid NUMBER, id INTEGER CONSTRAINT PK_DiskCopy_Id PRIMARY KEY, gcType INTEGER, fileSystem INTEGER, castorFile INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'gcType', 0, 'GCTYPE_AUTO');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'gcType', 1, 'GCTYPE_USER');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'gcType', 2, 'GCTYPE_TOO_MANY_REPLICAS');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'gcType', 3, 'GCTYPE_DRAINING_FS');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'gcType', 4, 'GCTYPE_NS_SYNCH');

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'status', 0, 'DISKCOPY_STAGED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'status', 1, 'DISKCOPY_WAITDISK2DISKCOPY');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'status', 2, 'DISKCOPY_WAITTAPERECALL');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'status', 3, 'DISKCOPY_DELETED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'status', 4, 'DISKCOPY_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'status', 5, 'DISKCOPY_WAITFS');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'status', 6, 'DISKCOPY_STAGEOUT');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'status', 7, 'DISKCOPY_INVALID');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'status', 9, 'DISKCOPY_BEINGDELETED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'status', 10, 'DISKCOPY_CANBEMIGR');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskCopy', 'status', 11, 'DISKCOPY_WAITFS_SCHEDULING');

/* SQL statements for type FileSystem */
CREATE TABLE FileSystem (free INTEGER, mountPoint VARCHAR2(2048), minFreeSpace NUMBER, minAllowedFreeSpace NUMBER, maxFreeSpace NUMBER, totalSize INTEGER, readRate INTEGER, writeRate INTEGER, nbReadStreams NUMBER, nbWriteStreams NUMBER, nbReadWriteStreams NUMBER, nbMigratorStreams NUMBER, nbRecallerStreams NUMBER, id INTEGER CONSTRAINT PK_FileSystem_Id PRIMARY KEY, diskPool INTEGER, diskserver INTEGER, status INTEGER, adminStatus INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('FileSystem', 'status', 0, 'FILESYSTEM_PRODUCTION');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('FileSystem', 'status', 1, 'FILESYSTEM_DRAINING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('FileSystem', 'status', 2, 'FILESYSTEM_DISABLED');

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('FileSystem', 'adminStatus', 0, 'ADMIN_NONE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('FileSystem', 'adminStatus', 1, 'ADMIN_FORCE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('FileSystem', 'adminStatus', 2, 'ADMIN_RELEASE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('FileSystem', 'adminStatus', 3, 'ADMIN_DELETED');

/* SQL statements for type SvcClass */
CREATE TABLE SvcClass (nbDrives NUMBER, name VARCHAR2(2048), defaultFileSize INTEGER, maxReplicaNb NUMBER, migratorPolicy VARCHAR2(2048), recallerPolicy VARCHAR2(2048), streamPolicy VARCHAR2(2048), gcPolicy VARCHAR2(2048), disk1Behavior NUMBER, replicateOnClose NUMBER, failJobsWhenNoSpace NUMBER, id INTEGER CONSTRAINT PK_SvcClass_Id PRIMARY KEY, forcedFileClass INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE SvcClass2TapePool (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_SvcClass2TapePool_C on SvcClass2TapePool (child);
CREATE INDEX I_SvcClass2TapePool_P on SvcClass2TapePool (parent);

/* SQL statements for type DiskPool */
CREATE TABLE DiskPool (name VARCHAR2(2048), id INTEGER CONSTRAINT PK_DiskPool_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE DiskPool2SvcClass (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_DiskPool2SvcClass_C on DiskPool2SvcClass (child);
CREATE INDEX I_DiskPool2SvcClass_P on DiskPool2SvcClass (parent);

/* SQL statements for type Stream */
CREATE TABLE Stream (initialSizeToTransfer INTEGER, lastFileSystemChange INTEGER, id INTEGER CONSTRAINT PK_Stream_Id PRIMARY KEY, tape INTEGER, lastFileSystemUsed INTEGER, lastButOneFileSystemUsed INTEGER, tapePool INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE Stream2TapeCopy (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_Stream2TapeCopy_C on Stream2TapeCopy (child);
CREATE INDEX I_Stream2TapeCopy_P on Stream2TapeCopy (parent);

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Stream', 'status', 0, 'STREAM_PENDING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Stream', 'status', 1, 'STREAM_WAITDRIVE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Stream', 'status', 2, 'STREAM_WAITMOUNT');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Stream', 'status', 3, 'STREAM_RUNNING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Stream', 'status', 4, 'STREAM_WAITSPACE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Stream', 'status', 5, 'STREAM_CREATED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Stream', 'status', 6, 'STREAM_STOPPED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Stream', 'status', 7, 'STREAM_WAITPOLICY');

/* SQL statements for type FileClass */
CREATE TABLE FileClass (name VARCHAR2(2048), nbCopies NUMBER, id INTEGER CONSTRAINT PK_FileClass_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type DiskServer */
CREATE TABLE DiskServer (name VARCHAR2(2048), readRate INTEGER, writeRate INTEGER, nbReadStreams NUMBER, nbWriteStreams NUMBER, nbReadWriteStreams NUMBER, nbMigratorStreams NUMBER, nbRecallerStreams NUMBER, id INTEGER CONSTRAINT PK_DiskServer_Id PRIMARY KEY, status INTEGER, adminStatus INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskServer', 'status', 0, 'DISKSERVER_PRODUCTION');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskServer', 'status', 1, 'DISKSERVER_DRAINING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskServer', 'status', 2, 'DISKSERVER_DISABLED');

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskServer', 'adminStatus', 0, 'ADMIN_NONE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskServer', 'adminStatus', 1, 'ADMIN_FORCE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskServer', 'adminStatus', 2, 'ADMIN_RELEASE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskServer', 'adminStatus', 3, 'ADMIN_DELETED');

/* SQL statements for type SetFileGCWeight */
CREATE TABLE SetFileGCWeight (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, weight NUMBER, id INTEGER CONSTRAINT PK_SetFileGCWeight_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageRepackRequest */
CREATE TABLE StageRepackRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, repackVid VARCHAR2(2048), id INTEGER CONSTRAINT PK_StageRepackRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageDiskCopyReplicaRequest */
CREATE TABLE StageDiskCopyReplicaRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_StageDiskCopyReplicaRequ_Id PRIMARY KEY, svcClass INTEGER, client INTEGER, sourceSvcClass INTEGER, destDiskCopy INTEGER, sourceDiskCopy INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type NsFilesDeleted */
CREATE TABLE NsFilesDeleted (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, nsHost VARCHAR2(2048), id INTEGER CONSTRAINT PK_NsFilesDeleted_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Disk2DiskCopyStartRequest */
CREATE TABLE Disk2DiskCopyStartRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskCopyId INTEGER, sourceDiskCopyId INTEGER, diskServer VARCHAR2(2048), mountPoint VARCHAR2(2048), fileId INTEGER, nsHost VARCHAR2(2048), id INTEGER CONSTRAINT PK_Disk2DiskCopyStartReques_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type FirstByteWritten */
CREATE TABLE FirstByteWritten (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, subReqId INTEGER, fileId INTEGER, nsHost VARCHAR2(2048), id INTEGER CONSTRAINT PK_FirstByteWritten_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageGetRequest */
CREATE TABLE StageGetRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_StageGetRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StgFilesDeleted */
CREATE TABLE StgFilesDeleted (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, nsHost VARCHAR2(2048), id INTEGER CONSTRAINT PK_StgFilesDeleted_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type StageAbortRequest */
CREATE TABLE StageAbortRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, parentUuid VARCHAR2(2048), id INTEGER CONSTRAINT PK_StageAbortRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER, parent INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type NsFileId */
CREATE TABLE NsFileId (fileid INTEGER, nsHost VARCHAR2(2048), id INTEGER CONSTRAINT PK_NsFileId_Id PRIMARY KEY, request INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type VersionQuery */
CREATE TABLE VersionQuery (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_VersionQuery_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type DiskPoolQuery */
CREATE TABLE DiskPoolQuery (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskPoolName VARCHAR2(2048), id INTEGER CONSTRAINT PK_DiskPoolQuery_Id PRIMARY KEY, svcClass INTEGER, client INTEGER, queryType INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskPoolQuery', 'queryType', 0, 'DISKPOOLQUERYTYPE_DEFAULT');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskPoolQuery', 'queryType', 1, 'DISKPOOLQUERYTYPE_AVAILABLE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('DiskPoolQuery', 'queryType', 2, 'DISKPOOLQUERYTYPE_TOTAL');

/* SQL statements for type ChangePrivilege */
CREATE TABLE ChangePrivilege (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, isGranted NUMBER, id INTEGER CONSTRAINT PK_ChangePrivilege_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type BWUser */
CREATE TABLE BWUser (euid NUMBER, egid NUMBER, id INTEGER CONSTRAINT PK_BWUser_Id PRIMARY KEY, request INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type RequestType */
CREATE TABLE RequestType (reqType NUMBER, id INTEGER CONSTRAINT PK_RequestType_Id PRIMARY KEY, request INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type ListPrivileges */
CREATE TABLE ListPrivileges (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, userId NUMBER, groupId NUMBER, requestType NUMBER, id INTEGER CONSTRAINT PK_ListPrivileges_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for constraints on SvcClass */
ALTER TABLE SvcClass2TapePool
  ADD CONSTRAINT FK_SvcClass2TapePool_P FOREIGN KEY (Parent) REFERENCES SvcClass (id)
  ADD CONSTRAINT FK_SvcClass2TapePool_C FOREIGN KEY (Child) REFERENCES TapePool (id);

/* SQL statements for constraints on DiskPool */
ALTER TABLE DiskPool2SvcClass
  ADD CONSTRAINT FK_DiskPool2SvcClass_P FOREIGN KEY (Parent) REFERENCES DiskPool (id)
  ADD CONSTRAINT FK_DiskPool2SvcClass_C FOREIGN KEY (Child) REFERENCES SvcClass (id);

/* SQL statements for constraints on Stream */
ALTER TABLE Stream2TapeCopy
  ADD CONSTRAINT FK_Stream2TapeCopy_P FOREIGN KEY (Parent) REFERENCES Stream (id)
  ADD CONSTRAINT FK_Stream2TapeCopy_C FOREIGN KEY (Child) REFERENCES TapeCopy (id);

/* Fill Type2Obj metatable */
INSERT INTO Type2Obj (type, object) VALUES (0, 'INVALID');
INSERT INTO Type2Obj (type, object) VALUES (1, 'Ptr');
INSERT INTO Type2Obj (type, object) VALUES (2, 'CastorFile');
INSERT INTO Type2Obj (type, object) VALUES (4, 'Cuuid');
INSERT INTO Type2Obj (type, object) VALUES (5, 'DiskCopy');
INSERT INTO Type2Obj (type, object) VALUES (6, 'DiskFile');
INSERT INTO Type2Obj (type, object) VALUES (7, 'DiskPool');
INSERT INTO Type2Obj (type, object) VALUES (8, 'DiskServer');
INSERT INTO Type2Obj (type, object) VALUES (10, 'FileClass');
INSERT INTO Type2Obj (type, object) VALUES (12, 'FileSystem');
INSERT INTO Type2Obj (type, object) VALUES (13, 'IClient');
INSERT INTO Type2Obj (type, object) VALUES (14, 'MessageAck');
INSERT INTO Type2Obj (type, object) VALUES (17, 'Request');
INSERT INTO Type2Obj (type, object) VALUES (18, 'Segment');
INSERT INTO Type2Obj (type, object) VALUES (26, 'Stream');
INSERT INTO Type2Obj (type, object) VALUES (27, 'SubRequest');
INSERT INTO Type2Obj (type, object) VALUES (28, 'SvcClass');
INSERT INTO Type2Obj (type, object) VALUES (29, 'Tape');
INSERT INTO Type2Obj (type, object) VALUES (30, 'TapeCopy');
INSERT INTO Type2Obj (type, object) VALUES (31, 'TapePool');
INSERT INTO Type2Obj (type, object) VALUES (33, 'StageFileQueryRequest');
INSERT INTO Type2Obj (type, object) VALUES (35, 'StageGetRequest');
INSERT INTO Type2Obj (type, object) VALUES (36, 'StagePrepareToGetRequest');
INSERT INTO Type2Obj (type, object) VALUES (37, 'StagePrepareToPutRequest');
INSERT INTO Type2Obj (type, object) VALUES (38, 'StagePrepareToUpdateRequest');
INSERT INTO Type2Obj (type, object) VALUES (39, 'StagePutDoneRequest');
INSERT INTO Type2Obj (type, object) VALUES (40, 'StagePutRequest');
INSERT INTO Type2Obj (type, object) VALUES (42, 'StageRmRequest');
INSERT INTO Type2Obj (type, object) VALUES (44, 'StageUpdateRequest');
INSERT INTO Type2Obj (type, object) VALUES (45, 'FileRequest');
INSERT INTO Type2Obj (type, object) VALUES (46, 'QryRequest');
INSERT INTO Type2Obj (type, object) VALUES (50, 'StageAbortRequest');
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
INSERT INTO Type2Obj (type, object) VALUES (90, 'TapeDriveDedication');
INSERT INTO Type2Obj (type, object) VALUES (91, 'TapeAccessSpecification');
INSERT INTO Type2Obj (type, object) VALUES (92, 'TapeDriveCompatibility');
INSERT INTO Type2Obj (type, object) VALUES (95, 'SetFileGCWeight');
INSERT INTO Type2Obj (type, object) VALUES (96, 'RepackRequest');
INSERT INTO Type2Obj (type, object) VALUES (97, 'RepackSubRequest');
INSERT INTO Type2Obj (type, object) VALUES (98, 'RepackSegment');
INSERT INTO Type2Obj (type, object) VALUES (99, 'RepackAck');
INSERT INTO Type2Obj (type, object) VALUES (101, 'DiskServerDescription');
INSERT INTO Type2Obj (type, object) VALUES (102, 'FileSystemDescription');
INSERT INTO Type2Obj (type, object) VALUES (103, 'DiskPoolQueryOld');
INSERT INTO Type2Obj (type, object) VALUES (104, 'EndResponse');
INSERT INTO Type2Obj (type, object) VALUES (105, 'FileResponse');
INSERT INTO Type2Obj (type, object) VALUES (106, 'StringResponse');
INSERT INTO Type2Obj (type, object) VALUES (107, 'Response');
INSERT INTO Type2Obj (type, object) VALUES (108, 'IOResponse');
INSERT INTO Type2Obj (type, object) VALUES (109, 'AbortResponse');
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
INSERT INTO Type2Obj (type, object) VALUES (148, 'VdqmTape');
INSERT INTO Type2Obj (type, object) VALUES (149, 'StgFilesDeleted');
INSERT INTO Type2Obj (type, object) VALUES (150, 'StgFilesDeletedResponse');
INSERT INTO Type2Obj (type, object) VALUES (151, 'VolumePriority');
INSERT INTO Type2Obj (type, object) VALUES (152, 'ChangePrivilege');
INSERT INTO Type2Obj (type, object) VALUES (153, 'BWUser');
INSERT INTO Type2Obj (type, object) VALUES (154, 'RequestType');
INSERT INTO Type2Obj (type, object) VALUES (155, 'ListPrivileges');
INSERT INTO Type2Obj (type, object) VALUES (156, 'Privilege');
INSERT INTO Type2Obj (type, object) VALUES (157, 'ListPrivilegesResponse');
INSERT INTO Type2Obj (type, object) VALUES (158, 'PriorityMap');
INSERT INTO Type2Obj (type, object) VALUES (159, 'VectorAddress');
INSERT INTO Type2Obj (type, object) VALUES (160, 'Tape2DriveDedication');
INSERT INTO Type2Obj (type, object) VALUES (161, 'TapeRecall');
INSERT INTO Type2Obj (type, object) VALUES (162, 'FileMigratedNotification');
INSERT INTO Type2Obj (type, object) VALUES (163, 'FileRecalledNotification');
INSERT INTO Type2Obj (type, object) VALUES (164, 'FileToMigrateRequest');
INSERT INTO Type2Obj (type, object) VALUES (165, 'FileToMigrate');
INSERT INTO Type2Obj (type, object) VALUES (166, 'FileToRecallRequest');
INSERT INTO Type2Obj (type, object) VALUES (167, 'FileToRecall');
INSERT INTO Type2Obj (type, object) VALUES (168, 'VolumeRequest');
INSERT INTO Type2Obj (type, object) VALUES (169, 'Volume');
INSERT INTO Type2Obj (type, object) VALUES (170, 'TapeGatewayRequest');
INSERT INTO Type2Obj (type, object) VALUES (171, 'DbInfoRetryPolicy');
INSERT INTO Type2Obj (type, object) VALUES (172, 'EndNotification');
INSERT INTO Type2Obj (type, object) VALUES (173, 'NoMoreFiles');
INSERT INTO Type2Obj (type, object) VALUES (174, 'NotificationAcknowledge');
INSERT INTO Type2Obj (type, object) VALUES (175, 'FileErrorReport');
INSERT INTO Type2Obj (type, object) VALUES (176, 'BaseFileInfo');
INSERT INTO Type2Obj (type, object) VALUES (178, 'RmMasterReport');
INSERT INTO Type2Obj (type, object) VALUES (179, 'EndNotificationErrorReport');
INSERT INTO Type2Obj (type, object) VALUES (180, 'TapeGatewaySubRequest');
INSERT INTO Type2Obj (type, object) VALUES (181, 'GatewayMessage');
INSERT INTO Type2Obj (type, object) VALUES (182, 'DumpNotification');
INSERT INTO Type2Obj (type, object) VALUES (183, 'PingNotification');
INSERT INTO Type2Obj (type, object) VALUES (184, 'DumpParameters');
INSERT INTO Type2Obj (type, object) VALUES (185, 'DumpParametersRequest');
INSERT INTO Type2Obj (type, object) VALUES (186, 'RecallPolicyElement');
INSERT INTO Type2Obj (type, object) VALUES (187, 'MigrationPolicyElement');
INSERT INTO Type2Obj (type, object) VALUES (188, 'StreamPolicyElement');
INSERT INTO Type2Obj (type, object) VALUES (189, 'RetryPolicyElement');
INSERT INTO Type2Obj (type, object) VALUES (190, 'VdqmTapeGatewayRequest');
INSERT INTO Type2Obj (type, object) VALUES (191, 'StageQueryResult');
INSERT INTO Type2Obj (type, object) VALUES (192, 'NsFileId');
INSERT INTO Type2Obj (type, object) VALUES (193, 'BulkRequestResult');
INSERT INTO Type2Obj (type, object) VALUES (194, 'FileResult');
INSERT INTO Type2Obj (type, object) VALUES (195, 'DiskPoolQuery');
COMMIT;

