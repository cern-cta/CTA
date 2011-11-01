/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE;

/* Type2Obj metatable definition */
CREATE TABLE Type2Obj (type INTEGER CONSTRAINT PK_Type2Obj_Type PRIMARY KEY, object VARCHAR2(100) CONSTRAINT NN_Type2Obj_Object NOT NULL, svcHandler VARCHAR2(100), CONSTRAINT UN_Type2Obj_typeObject UNIQUE (type, object));

/* ObjStatus metatable definition */
CREATE TABLE ObjStatus (object VARCHAR2(100) CONSTRAINT NN_ObjStatus_object NOT NULL, field VARCHAR2(100) CONSTRAINT NN_ObjStatus_field NOT NULL, statusCode INTEGER CONSTRAINT NN_ObjStatus_statusCode NOT NULL, statusName VARCHAR2(100) CONSTRAINT NN_ObjStatus_statusName NOT NULL, CONSTRAINT UN_ObjStatus_objectFieldCode UNIQUE (object, field, statusCode));

/* SQL statements for type BaseAddress */
CREATE TABLE BaseAddress (objType NUMBER, cnvSvcName VARCHAR2(2048), cnvSvcType NUMBER, target INTEGER, id INTEGER CONSTRAINT PK_BaseAddress_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Client */
CREATE TABLE Client (ipAddress NUMBER, port NUMBER, version NUMBER, secure NUMBER, id INTEGER CONSTRAINT PK_Client_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type SubRequest */
CREATE TABLE SubRequest (retryCounter NUMBER, fileName VARCHAR2(2048), protocol VARCHAR2(2048), xsize INTEGER, priority NUMBER, subreqId VARCHAR2(2048), flags NUMBER, modeBits NUMBER, creationTime INTEGER, lastModificationTime INTEGER, answered NUMBER, errorCode NUMBER, errorMessage VARCHAR2(2048), requestedFileSystems VARCHAR2(2048), svcHandler VARCHAR2(2048), reqType NUMBER, id INTEGER CONSTRAINT PK_SubRequest_Id PRIMARY KEY, diskcopy INTEGER, castorFile INTEGER, parent INTEGER, status INTEGER, request INTEGER, getNextStatus INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

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

/* SQL statements for type Disk2DiskCopyDoneRequest */
CREATE TABLE Disk2DiskCopyDoneRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, diskCopyId INTEGER, sourceDiskCopyId INTEGER, fileId INTEGER, nsHost VARCHAR2(2048), replicaFileSize INTEGER, id INTEGER CONSTRAINT PK_Disk2DiskCopyDoneRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type FileClass */
CREATE TABLE FileClass (name VARCHAR2(2048), nbCopies NUMBER, classId INTEGER, id INTEGER CONSTRAINT PK_FileClass_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

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

/* SQL statements for type StageFileQueryRequest */
CREATE TABLE StageFileQueryRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, fileName VARCHAR2(2048), id INTEGER CONSTRAINT PK_StageFileQueryRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Tape */
CREATE TABLE Tape (vid VARCHAR2(2048), side NUMBER, tpmode NUMBER, errMsgTxt VARCHAR2(2048), errorCode NUMBER, severity NUMBER, vwAddress VARCHAR2(2048), dgn VARCHAR2(2048), label VARCHAR2(2048), density VARCHAR2(2048), devtype VARCHAR2(2048), startTime NUMBER, lastVdqmPingTime NUMBER, vdqmVolReqId NUMBER, lastFseq NUMBER, tapeGatewayRequestId NUMBER, id INTEGER CONSTRAINT PK_Tape_Id PRIMARY KEY, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 0, 'TAPE_UNUSED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 1, 'TAPE_PENDING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 2, 'TAPE_WAITDRIVE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 3, 'TAPE_WAITMOUNT');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 4, 'TAPE_MOUNTED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 5, 'TAPE_FINISHED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 6, 'TAPE_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 7, 'TAPE_UNKNOWN');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 8, 'TAPE_WAITPOLICY');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Tape', 'status', 9, 'TAPE_ATTACHEDTOSTREAM');

/* SQL statements for type Segment */
CREATE TABLE Segment (fseq NUMBER, offset INTEGER, bytes_in INTEGER, bytes_out INTEGER, host_bytes INTEGER, segmCksumAlgorithm VARCHAR2(2048), segmCksum NUMBER, errMsgTxt VARCHAR2(2048), errorCode NUMBER, severity NUMBER, blockId0 INTEGER, blockId1 INTEGER, blockId2 INTEGER, blockId3 INTEGER, creationTime INTEGER, priority INTEGER, id INTEGER CONSTRAINT PK_Segment_Id PRIMARY KEY, copy INTEGER, status INTEGER, tape INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Segment', 'status', 0, 'SEGMENT_UNPROCESSED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Segment', 'status', 5, 'SEGMENT_FILECOPIED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Segment', 'status', 6, 'SEGMENT_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Segment', 'status', 7, 'SEGMENT_SELECTED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('Segment', 'status', 8, 'SEGMENT_RETRIED');

/* SQL statements for type RecallJob */
CREATE TABLE RecallJob (copyNb NUMBER, errorCode NUMBER, nbRetry NUMBER, missingCopies NUMBER, fseq NUMBER, tapeGatewayRequestId NUMBER, vid VARCHAR2(2048), fileTransactionId NUMBER, id INTEGER CONSTRAINT PK_RecallJob_Id PRIMARY KEY, castorFile INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 3, 'RECALLJOB_SELECTED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 4, 'RECALLJOB_TOBERECALLED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 5, 'RECALLJOB_STAGED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 6, 'RECALLJOB_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 8, 'RECALLJOB_RETRY');

/* SQL statements for type CastorFile */
CREATE TABLE CastorFile (fileId INTEGER, nsHost VARCHAR2(2048), fileSize INTEGER, creationTime INTEGER, lastAccessTime INTEGER, lastKnownFileName VARCHAR2(2048), lastUpdateTime INTEGER, id INTEGER CONSTRAINT PK_CastorFile_Id PRIMARY KEY, fileClass INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

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
CREATE TABLE SvcClass (name VARCHAR2(2048), defaultFileSize INTEGER, maxReplicaNb NUMBER, recallerPolicy VARCHAR2(2048), gcPolicy VARCHAR2(2048), disk1Behavior NUMBER, replicateOnClose NUMBER, failJobsWhenNoSpace NUMBER, lastEditor VARCHAR2(2048), lastEditionTime INTEGER, id INTEGER CONSTRAINT PK_SvcClass_Id PRIMARY KEY, forcedFileClass INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type DiskPool */
CREATE TABLE DiskPool (name VARCHAR2(2048), id INTEGER CONSTRAINT PK_DiskPool_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE DiskPool2SvcClass (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_DiskPool2SvcClass_C on DiskPool2SvcClass (child);
CREATE INDEX I_DiskPool2SvcClass_P on DiskPool2SvcClass (parent);

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
CREATE TABLE StageRepackRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, repackVid VARCHAR2(2048), id INTEGER CONSTRAINT PK_StageRepackRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 0, 'REPACK_STARTING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 1, 'REPACK_ONGOING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 2, 'REPACK_FINISHED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 3, 'REPACK_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 4, 'REPACK_ABORTING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 5, 'REPACK_ABORTED');

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

/* SQL statements for type StagePutDoneRequest */
CREATE TABLE StagePutDoneRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, parentUuid VARCHAR2(2048), id INTEGER CONSTRAINT PK_StagePutDoneRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER, parent INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

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

/* SQL statements for constraints on DiskPool */
ALTER TABLE DiskPool2SvcClass
  ADD CONSTRAINT FK_DiskPool2SvcClass_P FOREIGN KEY (Parent) REFERENCES DiskPool (id)
  ADD CONSTRAINT FK_DiskPool2SvcClass_C FOREIGN KEY (Child) REFERENCES SvcClass (id);

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
INSERT INTO Type2Obj (type, object) VALUES (27, 'SubRequest');
INSERT INTO Type2Obj (type, object) VALUES (28, 'SvcClass');
INSERT INTO Type2Obj (type, object) VALUES (29, 'Tape');
INSERT INTO Type2Obj (type, object) VALUES (30, 'RecallJob');
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
INSERT INTO Type2Obj (type, object) VALUES (196, 'EndNotificationFileErrorReport');
INSERT INTO Type2Obj (type, object) VALUES (197, 'FileMigrationReportList');
INSERT INTO Type2Obj (type, object) VALUES (198, 'FileRecallReportList');
INSERT INTO Type2Obj (type, object) VALUES (199, 'FilesToMigrateList');
INSERT INTO Type2Obj (type, object) VALUES (200, 'FilesToMigrateListRequest');
INSERT INTO Type2Obj (type, object) VALUES (201, 'FilesToRecallListRequest');
INSERT INTO Type2Obj (type, object) VALUES (202, 'FileErrorReportStruct');
INSERT INTO Type2Obj (type, object) VALUES (203, 'FileMigratedNotificationStruct');
INSERT INTO Type2Obj (type, object) VALUES (204, 'FileRecalledNotificationStruct');
INSERT INTO Type2Obj (type, object) VALUES (205, 'FilesToRecallList');
INSERT INTO Type2Obj (type, object) VALUES (206, 'FileToMigrateStruct');
INSERT INTO Type2Obj (type, object) VALUES (207, 'FileToRecallStruct');
INSERT INTO Type2Obj (type, object) VALUES (208, 'FilesListRequest');
COMMIT;


/* SQL statements for table UpgradeLog */
CREATE TABLE UpgradeLog (Username VARCHAR2(64) DEFAULT sys_context('USERENV', 'OS_USER') CONSTRAINT NN_UpgradeLog_Username NOT NULL, SchemaName VARCHAR2(64) DEFAULT 'STAGER' CONSTRAINT NN_UpgradeLog_SchemaName NOT NULL, Machine VARCHAR2(64) DEFAULT sys_context('USERENV', 'HOST') CONSTRAINT NN_UpgradeLog_Machine NOT NULL, Program VARCHAR2(48) DEFAULT sys_context('USERENV', 'MODULE') CONSTRAINT NN_UpgradeLog_Program NOT NULL, StartDate TIMESTAMP(6) WITH TIME ZONE DEFAULT sysdate, EndDate TIMESTAMP(6) WITH TIME ZONE, FailureCount NUMBER DEFAULT 0, Type VARCHAR2(20) DEFAULT 'NON TRANSPARENT', State VARCHAR2(20) DEFAULT 'INCOMPLETE', SchemaVersion VARCHAR2(20) CONSTRAINT NN_UpgradeLog_SchemaVersion NOT NULL, Release VARCHAR2(20) CONSTRAINT NN_UpgradeLog_Release NOT NULL);

/* SQL statements for check constraints on the UpgradeLog table */
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_State
  CHECK (state IN ('COMPLETE', 'INCOMPLETE'));
  
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_Type
  CHECK (type IN ('TRANSPARENT', 'NON TRANSPARENT'));

/* SQL statement to populate the intial release value */
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('-', '2_1_12_0');

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
 *
 * @(#)RCSfile: oracleCommon.schema.sql,v  Revision: 1.19  Date: 2009/08/10 15:30:12  Author: itglp 
 *
 * This file contains all schema definitions which are not generated automatically.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_12_0';

/* Sequence for indices */
CREATE SEQUENCE ids_seq CACHE 300;

/* SQL statements for requests status */
/* Partitioning enables faster response (more than indexing) for the most frequent queries - credits to Nilo Segura */
CREATE TABLE newRequests (type NUMBER(38) CONSTRAINT NN_NewRequests_Type NOT NULL, id NUMBER(38) CONSTRAINT NN_NewRequests_Id NOT NULL, creation DATE CONSTRAINT NN_NewRequests_Creation NOT NULL, CONSTRAINT PK_NewRequests_Type_Id PRIMARY KEY (type, id))
ORGANIZATION INDEX
COMPRESS
PARTITION BY LIST (type)
 (
  PARTITION type_16 VALUES (16)  TABLESPACE stager_data,
  PARTITION type_21 VALUES (21)  TABLESPACE stager_data,
  PARTITION type_33 VALUES (33)  TABLESPACE stager_data,
  PARTITION type_34 VALUES (34)  TABLESPACE stager_data,
  PARTITION type_35 VALUES (35)  TABLESPACE stager_data,
  PARTITION type_36 VALUES (36)  TABLESPACE stager_data,
  PARTITION type_37 VALUES (37)  TABLESPACE stager_data,
  PARTITION type_38 VALUES (38)  TABLESPACE stager_data,
  PARTITION type_39 VALUES (39)  TABLESPACE stager_data,
  PARTITION type_40 VALUES (40)  TABLESPACE stager_data,
  PARTITION type_41 VALUES (41)  TABLESPACE stager_data,
  PARTITION type_42 VALUES (42)  TABLESPACE stager_data,
  PARTITION type_43 VALUES (43)  TABLESPACE stager_data,
  PARTITION type_44 VALUES (44)  TABLESPACE stager_data,
  PARTITION type_45 VALUES (45)  TABLESPACE stager_data,
  PARTITION type_46 VALUES (46)  TABLESPACE stager_data,
  PARTITION type_48 VALUES (48)  TABLESPACE stager_data,
  PARTITION type_49 VALUES (49)  TABLESPACE stager_data,
  PARTITION type_50 VALUES (50)  TABLESPACE stager_data,
  PARTITION type_51 VALUES (51)  TABLESPACE stager_data,
  PARTITION type_60 VALUES (60)  TABLESPACE stager_data,
  PARTITION type_64 VALUES (64)  TABLESPACE stager_data,
  PARTITION type_65 VALUES (65)  TABLESPACE stager_data,
  PARTITION type_66 VALUES (66)  TABLESPACE stager_data,
  PARTITION type_67 VALUES (67)  TABLESPACE stager_data,
  PARTITION type_78 VALUES (78)  TABLESPACE stager_data,
  PARTITION type_79 VALUES (79)  TABLESPACE stager_data,
  PARTITION type_80 VALUES (80)  TABLESPACE stager_data,
  PARTITION type_84 VALUES (84)  TABLESPACE stager_data,
  PARTITION type_90 VALUES (90)  TABLESPACE stager_data,
  PARTITION type_142 VALUES (142)  TABLESPACE stager_data,
  PARTITION type_144 VALUES (144)  TABLESPACE stager_data,
  PARTITION type_147 VALUES (147)  TABLESPACE stager_data,
  PARTITION type_149 VALUES (149)  TABLESPACE stager_data,
  PARTITION notlisted VALUES (default) TABLESPACE stager_data
 );

/* Redefinition of table SubRequest to make it partitioned by status */
/* Unfortunately it has already been defined, so we drop and recreate it */
/* Note that if the schema changes, this part has to be updated manually! */
DROP TABLE SubRequest;
CREATE TABLE SubRequest
  (retryCounter NUMBER, fileName VARCHAR2(2048), protocol VARCHAR2(2048),
   xsize INTEGER, priority NUMBER, subreqId VARCHAR2(2048), flags NUMBER,
   modeBits NUMBER, creationTime INTEGER CONSTRAINT NN_SubRequest_CreationTime 
   NOT NULL, lastModificationTime INTEGER, answered NUMBER, errorCode NUMBER, 
   errorMessage VARCHAR2(2048), id NUMBER CONSTRAINT NN_SubRequest_Id NOT NULL,
   diskcopy INTEGER, castorFile INTEGER, parent INTEGER, status INTEGER,
   request INTEGER, getNextStatus INTEGER, requestedFileSystems VARCHAR2(2048),
   svcHandler VARCHAR2(2048) CONSTRAINT NN_SubRequest_SvcHandler NOT NULL,
   reqType INTEGER CONSTRAINT NN_SubRequest_reqType NOT NULL
  )
  PCTFREE 50 PCTUSED 40 INITRANS 50
  ENABLE ROW MOVEMENT
  PARTITION BY LIST (STATUS)
   (
    PARTITION P_STATUS_0_1_2 VALUES (0, 1, 2),      -- *START
    PARTITION P_STATUS_3     VALUES (3),
    PARTITION P_STATUS_4     VALUES (4),
    PARTITION P_STATUS_5     VALUES (5),
    PARTITION P_STATUS_6     VALUES (6),
    PARTITION P_STATUS_7     VALUES (7),
    PARTITION P_STATUS_8     VALUES (8),
    PARTITION P_STATUS_9_10  VALUES (9, 10),        -- FAILED_*
    PARTITION P_STATUS_11    VALUES (11),
    PARTITION P_STATUS_12    VALUES (12),
    PARTITION P_STATUS_13_14 VALUES (13, 14),       -- *SCHED
    PARTITION P_STATUS_OTHER VALUES (DEFAULT)
   );

/* SQL statements for constraints on the SubRequest table */
ALTER TABLE SubRequest
  ADD CONSTRAINT PK_SubRequest_Id PRIMARY KEY (ID);
CREATE INDEX I_SubRequest_RT_CT_ID ON SubRequest(svcHandler, creationTime, id) LOCAL
 (PARTITION P_STATUS_0_1_2,
  PARTITION P_STATUS_3,
  PARTITION P_STATUS_4,
  PARTITION P_STATUS_5,
  PARTITION P_STATUS_6,
  PARTITION P_STATUS_7,
  PARTITION P_STATUS_8,
  PARTITION P_STATUS_9_10,
  PARTITION P_STATUS_11,
  PARTITION P_STATUS_12,
  PARTITION P_STATUS_13_14,
  PARTITION P_STATUS_OTHER);

CREATE INDEX I_SubRequest_CT_ID ON SubRequest(creationTime, id) LOCAL
 (PARTITION P_STATUS_0_1_2,
  PARTITION P_STATUS_3,
  PARTITION P_STATUS_4,
  PARTITION P_STATUS_5,
  PARTITION P_STATUS_6,
  PARTITION P_STATUS_7,
  PARTITION P_STATUS_8,
  PARTITION P_STATUS_9_10,
  PARTITION P_STATUS_11,
  PARTITION P_STATUS_12,
  PARTITION P_STATUS_13_14,
  PARTITION P_STATUS_OTHER);

/* Redefinition of table RecallJob to make it partitioned by status */
DROP TABLE RecallJob;
CREATE TABLE RecallJob(copyNb NUMBER,
                       errorCode NUMBER,
                       nbRetry NUMBER,
                       missingCopies NUMBER, 
                       fseq NUMBER,
                       tapeGatewayRequestId NUMBER,
                       vid VARCHAR2(2048), 
                       fileTransactionId NUMBER,
                       id INTEGER CONSTRAINT PK_RecallJob_Id PRIMARY KEY CONSTRAINT NN_RecallJob_Id NOT NULL, 
                       castorFile INTEGER,
                       status INTEGER) 
INITRANS 50 PCTUSED 40 PCTFREE 50 ENABLE ROW MOVEMENT
PARTITION BY LIST (STATUS) (
  PARTITION P_STATUS_0_1   VALUES (0, 1),
  PARTITION P_STATUS_OTHER VALUES (DEFAULT)
);
/* Add index to allow fast lookup by VID (use for preventing 2 tape copies on the same tape.) */
CREATE INDEX I_RecallJob_VID ON RecallJob(VID);
CREATE INDEX I_RecallJob_Castorfile ON RecallJob (castorFile) LOCAL;
CREATE INDEX I_RecallJob_Status ON RecallJob (status) LOCAL;
/* This transaction id is the mean to track a migration, so it obviously needs to be unique */
ALTER TABLE RecallJob ADD CONSTRAINT UN_RECALLJOB_FILETRID 
  UNIQUE (FileTransactionId) USING INDEX;

/* Create sequence for the File request IDs. */
CREATE SEQUENCE TG_FILETRID_SEQ START WITH 1 INCREMENT BY 1;

/* Definition of the TapePool table
 *   name : the name of the TapePool
 *   minAmountDataForMount : the minimum amount of data needed to trigger a new mount, in bytes
 *   minNbFilesForMount : the minimum number of files needed to trigger a new mount
 *   maxFileAgeBeforeMount : the maximum file age before a tape in mounted, in seconds
 *   lastEditor : the login from which the tapepool was last modified
 *   lastEditionTime : the time at which the tapepool was last modified
 * Note that a mount is attempted as soon as one of the three criterias is reached.
 */
CREATE TABLE TapePool (name VARCHAR2(2048) CONSTRAINT NN_TapePool_Name NOT NULL,
                       nbDrives INTEGER CONSTRAINT NN_TapePool_NbDrives NOT NULL,
                       minAmountDataForMount INTEGER CONSTRAINT NN_TapePool_MinAmountData NOT NULL,
                       minNbFilesForMount INTEGER CONSTRAINT NN_TapePool_MinNbFiles NOT NULL,
                       maxFileAgeBeforeMount INTEGER CONSTRAINT NN_TapePool_MaxFileAge NOT NULL,
                       lastEditor VARCHAR2(2048) CONSTRAINT NN_TapePool_LastEditor NOT NULL,
                       lastEditionTime NUMBER CONSTRAINT NN_TapePool_LastEdTime NOT NULL,
                       id INTEGER CONSTRAINT PK_TapePool_Id PRIMARY KEY CONSTRAINT NN_TapePool_Id NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* Definition of the MigrationMount table
 *   vdqmVolReqId : 
 *   tapeGatewayRequestId : 
 *   VID : tape currently mounted (when applicable)
 *   label : label (i.e. format) of the currently mounted tape (when applicable)
 *   density : density of the currently mounted tape (when applicable)
 *   lastFseq : position of the last file written on the tape
 *   lastVDQMPingTime : last time we've pinged VDQM
 *   tapePool : tapepool used by this migration
 *   status : current status of the migration
 */
CREATE TABLE MigrationMount (vdqmVolReqId INTEGER CONSTRAINT UN_MigrationMount_VDQM UNIQUE USING INDEX,
                             tapeGatewayRequestId INTEGER CONSTRAINT NN_MigrationMount_tgRequestId NOT NULL,
                             id INTEGER CONSTRAINT PK_MigrationMount_Id PRIMARY KEY
                                        CONSTRAINT NN_MigrationMount_Id NOT NULL,
                             startTime NUMBER CONSTRAINT NN_MigrationMount_startTime NOT NULL,
                             VID VARCHAR2(2048) CONSTRAINT UN_MigrationMount_VID UNIQUE USING INDEX,
                             label VARCHAR2(2048),
                             density VARCHAR2(2048),
                             lastFseq INTEGER,
                             full INTEGER,
                             lastVDQMPingTime NUMBER CONSTRAINT NN_MigrationMount_lastVDQMPing NOT NULL,
                             tapePool INTEGER CONSTRAINT NN_MigrationMount_TapePool NOT NULL,
                             status INTEGER CONSTRAINT NN_MigrationMount_Status NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE INDEX I_MigrationMount_TapePool ON MigrationMount(tapePool); 
ALTER TABLE MigrationMount ADD CONSTRAINT FK_MigrationMount_TapePool
   FOREIGN KEY (tapePool) REFERENCES TapePool(id);
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationMount', 'status', 0, 'MIGRATIONMOUNT_WAITTAPE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationMount', 'status', 1, 'MIGRATIONMOUNT_SEND_TO_VDQM');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationMount', 'status', 2, 'MIGRATIONMOUNT_WAITDRIVE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationMount', 'status', 3, 'MIGRATIONMOUNT_MIGRATING');


/* Definition of the MigrationJob table
 *   fileSize : size of the file to be migrated, in bytes
 *   VID : tape on which the file is being migrated (when applicable)
 *   creationTime : time of creation of this MigrationJob, in seconds since the epoch
 *   castorFile : the file to migrate
 *   copyNb : the number of the copy of the file to migrate
 *   tapePool : the tape pool where to migrate
 *   nbRetry : the number of retries we already went through
 *   errorcode : the error we got on last try (if any)
 *   tapeGatewayRequestId : an identifier for the migration session that is handling this job (when applicable)
 *   fileTransactionId : an identifier for this migration job
 *   fSeq : the file sequence of the copy created on tape for this job (when applicable)
 *   status : the status of the migration job
 */
CREATE TABLE MigrationJob (fileSize INTEGER CONSTRAINT NN_MigrationJob_FileSize NOT NULL,
                           VID VARCHAR2(2048),
                           creationTime NUMBER CONSTRAINT NN_MigrationJob_CreationTime NOT NULL,
                           castorFile INTEGER CONSTRAINT NN_MigrationJob_CastorFile NOT NULL,
                           copyNb INTEGER CONSTRAINT NN_MigrationJob_copyNb NOT NULL,
                           tapePool INTEGER CONSTRAINT NN_MigrationJob_TapePool NOT NULL,
                           nbRetry INTEGER CONSTRAINT NN_MigrationJob_nbRetry NOT NULL,
                           errorcode INTEGER,
                           tapeGatewayRequestId INTEGER,
                           fileTransactionId INTEGER CONSTRAINT UN_MigrationJob_FileTrId UNIQUE USING INDEX,
                           fSeq INTEGER,
                           status INTEGER CONSTRAINT NN_MigrationJob_Status NOT NULL,
                           id INTEGER CONSTRAINT PK_MigrationJob_Id PRIMARY KEY 
                                      CONSTRAINT NN_MigrationJob_Id NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE INDEX I_MigrationJob_CFVID ON MigrationJob(CastorFile, VID);
CREATE INDEX I_MigrationJob_TapePoolSize ON MigrationJob(tapePool, fileSize);
CREATE INDEX I_MigrationJob_TPStatusCFId ON MigrationJob(tapePool, status, castorFile, id);
CREATE INDEX I_MigrationJob_CFCopyNb ON MigrationJob(CastorFile, copyNb);
ALTER TABLE MigrationJob ADD CONSTRAINT UN_MigrationMount_CopyNb UNIQUE (castorFile, copyNb) USING INDEX I_MigrationJob_CFCopyNb;
ALTER TABLE MigrationJob ADD CONSTRAINT FK_MigrationJob_CastorFile
   FOREIGN KEY (castorFile) REFERENCES CastorFile(id);
ALTER TABLE MigrationJob ADD CONSTRAINT FK_MigrationJob_TapePool
   FOREIGN KEY (tapePool) REFERENCES TapePool(id);
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationJob', 'status', 0, 'MIGRATIONJOB_PENDING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationJob', 'status', 1, 'MIGRATIONJOB_SELECTED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationJob', 'status', 2, 'MIGRATIONJOB_MIGRATED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationJob', 'status', 6, 'MIGRATIONJOB_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationJob', 'status', 8, 'MIGRATIONJOB_RETRY');


/* Definition of the MigrationRouting table. Each line is a routing rule for migration jobs
 *   isSmallFile : whether this routing rule applies to small files. Null means it applies to all files
 *   copyNb : the copy number the routing rule applies to
 *   svcClass : the service class the routing rule applies to
 *   fileClass : the file class the routing rule applies to
 *   lastEditor : name of the last one that modified this routing rule.
 *   lastEditionTime : last time this routing rule was edited, in seconds since the epoch
 *   tapePool : the tape pool where to migrate files matching the above criteria
 */
CREATE TABLE MigrationRouting (isSmallFile INTEGER,
                               copyNb INTEGER CONSTRAINT NN_MigrationRouting_CopyNb NOT NULL,
                               svcClass INTEGER CONSTRAINT NN_MigrationRouting_SvcClass NOT NULL,
                               fileClass INTEGER CONSTRAINT NN_MigrationRouting_FileClass NOT NULL,
                               lastEditor VARCHAR2(2048) CONSTRAINT NN_MigrationRouting_LastEditor NOT NULL,
                               lastEditionTime NUMBER CONSTRAINT NN_MigrationRouting_LastEdTime NOT NULL,
                               tapePool INTEGER CONSTRAINT NN_MigrationRouting_TapePool NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE INDEX I_MigrationRouting_Rules ON MigrationRouting(svcClass, fileClass, copyNb, isSmallFile);
ALTER TABLE MigrationRouting ADD CONSTRAINT UN_MigrationRouting_Rules UNIQUE (svcClass, fileClass, copyNb, isSmallFile) USING INDEX I_MigrationRouting_Rules;
ALTER TABLE MigrationRouting ADD CONSTRAINT FK_MigrationRouting_SvcClass
   FOREIGN KEY (svcClass) REFERENCES SvcClass(id);
ALTER TABLE MigrationRouting ADD CONSTRAINT FK_MigrationRouting_FileClass
   FOREIGN KEY (fileClass) REFERENCES FileClass(id);
ALTER TABLE MigrationRouting ADD CONSTRAINT FK_MigrationRouting_TapePool
   FOREIGN KEY (tapePool) REFERENCES TapePool(id);
 
/* Indexes related to most used entities */
CREATE UNIQUE INDEX I_DiskServer_name ON DiskServer (name);

CREATE UNIQUE INDEX I_CastorFile_FileIdNsHost ON CastorFile (fileId, nsHost);
CREATE UNIQUE INDEX I_CastorFile_LastKnownFileName ON CastorFile (lastKnownFileName);

CREATE INDEX I_DiskCopy_Castorfile ON DiskCopy (castorFile);
CREATE INDEX I_DiskCopy_FileSystem ON DiskCopy (fileSystem);
CREATE INDEX I_DiskCopy_Status ON DiskCopy (status);
CREATE INDEX I_DiskCopy_Status_9 ON DiskCopy (decode(status,9,status,NULL));
CREATE INDEX I_DiskCopy_FS_GCW ON DiskCopy (filesystem, status, gcweight, ID, castorFile);

CREATE INDEX I_FileSystem_DiskPool ON FileSystem (diskPool);
CREATE INDEX I_FileSystem_DiskServer ON FileSystem (diskServer);

CREATE INDEX I_SubRequest_Castorfile ON SubRequest (castorFile);
CREATE INDEX I_SubRequest_DiskCopy ON SubRequest (diskCopy);
CREATE INDEX I_SubRequest_Request ON SubRequest (request);
CREATE INDEX I_SubRequest_Parent ON SubRequest (parent);
CREATE INDEX I_SubRequest_SubReqId ON SubRequest (subReqId);
CREATE INDEX I_SubRequest_LastModTime ON SubRequest (lastModificationTime) LOCAL;

CREATE INDEX I_StagePTGRequest_ReqId ON StagePrepareToGetRequest (reqId);
CREATE INDEX I_StagePTPRequest_ReqId ON StagePrepareToPutRequest (reqId);
CREATE INDEX I_StagePTURequest_ReqId ON StagePrepareToUpdateRequest (reqId);
CREATE INDEX I_StageGetRequest_ReqId ON StageGetRequest (reqId);
CREATE INDEX I_StagePutRequest_ReqId ON StagePutRequest (reqId);
CREATE INDEX I_StageRepackRequest_ReqId ON StageRepackRequest (reqId);

/* Improve query execution in the checkFailJobsWhenNoSpace function */
CREATE INDEX I_StagePutRequest_SvcClass ON StagePutRequest (svcClass);

/* Indexing GCFile by Request */
CREATE INDEX I_GCFile_Request ON GCFile (request);

/* Indexing Tape by Status */
CREATE INDEX I_Tape_Status ON Tape (status);

/* Indexing Segments by Tape */
CREATE INDEX I_Segment_Tape ON Segment (tape);

/* Indexing Segments by Tape and Status */
CREATE INDEX I_Segment_TapeStatus ON Segment (tape, status);

/* FileSystem constraints */
ALTER TABLE FileSystem ADD CONSTRAINT FK_FileSystem_DiskServer 
  FOREIGN KEY (diskServer) REFERENCES DiskServer(id);

ALTER TABLE FileSystem MODIFY
  (status     CONSTRAINT NN_FileSystem_Status NOT NULL,
   diskServer CONSTRAINT NN_FileSystem_DiskServer NOT NULL,
   mountPoint CONSTRAINT NN_FileSystem_MountPoint NOT NULL);

ALTER TABLE FileSystem ADD CONSTRAINT UN_FileSystem_DSMountPoint
   UNIQUE (diskServer, mountPoint);

/* DiskServer constraints */
ALTER TABLE DiskServer MODIFY
  (status CONSTRAINT NN_DiskServer_Status NOT NULL,
   name CONSTRAINT NN_DiskServer_Name NOT NULL);

ALTER TABLE DiskServer ADD CONSTRAINT UN_DiskServer_Name UNIQUE (name);

/* An index to speed up queries in FileQueryRequest, FindRequestRequest, RequestQueryRequest */
CREATE INDEX I_QueryParameter_Query ON QueryParameter (query);

/* An index to speed the queries on Segments by copy */
CREATE INDEX I_Segment_Copy ON Segment (copy);

/* Constraint on FileClass name */
ALTER TABLE FileClass ADD CONSTRAINT UN_FileClass_Name UNIQUE (name);

/* Add unique constraint on tapes */
ALTER TABLE Tape ADD CONSTRAINT UN_Tape_VIDSideTpMode UNIQUE (VID, side, tpMode);

/* Add unique constraint on svcClass name */
ALTER TABLE SvcClass ADD CONSTRAINT UN_SvcClass_Name UNIQUE (name);

/* Custom type to handle int arrays */
CREATE OR REPLACE TYPE "numList" IS TABLE OF INTEGER;
/

/* Custom type to handle strings returned by pipelined functions */
CREATE OR REPLACE TYPE strListTable AS TABLE OF VARCHAR2(2048);
/

/* SvcClass constraints */
ALTER TABLE SvcClass
  MODIFY (name CONSTRAINT NN_SvcClass_Name NOT NULL);

ALTER TABLE SvcClass 
  MODIFY (forcedFileClass CONSTRAINT NN_SvcClass_ForcedFileClass NOT NULL);

ALTER TABLE SvcClass MODIFY (gcPolicy DEFAULT 'default');

ALTER TABLE SvcClass MODIFY (lastEditor CONSTRAINT NN_SvcClass_LastEditor NOT NULL);

ALTER TABLE SvcClass MODIFY (lastEditionTime CONSTRAINT NN_SvcClass_LastEditionTime NOT NULL);

/* DiskCopy constraints */
ALTER TABLE DiskCopy MODIFY (nbCopyAccesses DEFAULT 0);

ALTER TABLE DiskCopy MODIFY (gcType DEFAULT NULL);

ALTER TABLE DiskCopy ADD CONSTRAINT FK_DiskCopy_CastorFile
  FOREIGN KEY (castorFile) REFERENCES CastorFile (id)
  INITIALLY DEFERRED DEFERRABLE;

ALTER TABLE DiskCopy
  MODIFY (status CONSTRAINT NN_DiskCopy_Status NOT NULL);

/* CastorFile constraints */
ALTER TABLE CastorFile ADD CONSTRAINT FK_CastorFile_FileClass
  FOREIGN KEY (fileClass) REFERENCES FileClass (id)
  INITIALLY DEFERRED DEFERRABLE;

ALTER TABLE CastorFile ADD CONSTRAINT UN_CastorFile_LKFileName UNIQUE (LastKnownFileName);

ALTER TABLE CastorFile MODIFY (LastKnownFileName CONSTRAINT NN_CastorFile_LKFileName NOT NULL);

ALTER TABLE Tape MODIFY(lastVdqmPingTime CONSTRAINT NN_Tape_lastVdqmPingTime NOT NULL);

/* DiskPool2SvcClass constraints */
ALTER TABLE DiskPool2SvcClass ADD CONSTRAINT PK_DiskPool2SvcClass_PC
  PRIMARY KEY (parent, child);

/* Global temporary table to handle output of the filesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE FilesDeletedProcOutput
  (fileId NUMBER, nsHost VARCHAR2(2048))
  ON COMMIT PRESERVE ROWS;

/* Global temporary table to store castor file ids temporarily in the filesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE FilesDeletedProcHelper
  (cfId NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to handle output of the nsFilesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE NsFilesDeletedOrphans
  (fileid NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to handle output of the stgFilesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE StgFilesDeletedOrphans
  (diskCopyId NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to handle output of the processBulkAbortForGet procedure */
CREATE GLOBAL TEMPORARY TABLE ProcessBulkAbortFileReqsHelper
  (srId NUMBER, cfId NUMBER, fileId NUMBER, nsHost VARCHAR2(2048), uuid VARCHAR(2048))
  ON COMMIT PRESERVE ROWS;
ALTER TABLE ProcessBulkAbortFileReqsHelper
  ADD CONSTRAINT PK_ProcessBulkAbortFileRe_SrId PRIMARY KEY (srId);

/* Global temporary table to handle output of the processBulkRequest procedure */
CREATE GLOBAL TEMPORARY TABLE ProcessBulkRequestHelper
  (fileId NUMBER, nsHost VARCHAR2(2048), errorCode NUMBER, errorMessage VARCHAR2(2048))
  ON COMMIT PRESERVE ROWS;

/* Global temporary table to handle bulk update of subrequests in processBulkAbortForRepack */
CREATE GLOBAL TEMPORARY TABLE ProcessRepackAbortHelperSR (srId NUMBER) ON COMMIT DELETE ROWS;
/* Global temporary table to handle bulk update of diskCopies in processBulkAbortForRepack */
CREATE GLOBAL TEMPORARY TABLE ProcessRepackAbortHelperDCrec (cfId NUMBER) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMPORARY TABLE ProcessRepackAbortHelperDCmigr (cfId NUMBER) ON COMMIT DELETE ROWS;

/* Tables to log the activity performed by the cleanup job */
CREATE TABLE CleanupJobLog
  (fileId NUMBER CONSTRAINT NN_CleanupJobLog_FileId NOT NULL, 
   nsHost VARCHAR2(2048) CONSTRAINT NN_CleanupJobLog_NsHost NOT NULL,
   operation INTEGER CONSTRAINT NN_CleanupJobLog_Operation NOT NULL);
 
/* Temporary table to handle removing of priviledges */
CREATE GLOBAL TEMPORARY TABLE RemovePrivilegeTmpTable
  (svcClass VARCHAR2(2048),
   euid NUMBER,
   egid NUMBER,
   reqType NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to store ids temporarily in the bulkCreateObj procedures */
CREATE GLOBAL TEMPORARY TABLE BulkSelectHelper
  (objId NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to store the information on diskcopyies which need to
 * processed to see if too many replicas are online. This temporary table is
 * required to solve the error: `ORA-04091: table is mutating, trigger/function`
 */
CREATE GLOBAL TEMPORARY TABLE TooManyReplicasHelper
  (svcClass NUMBER, castorFile NUMBER)
  ON COMMIT DELETE ROWS;

ALTER TABLE TooManyReplicasHelper 
  ADD CONSTRAINT UN_TooManyReplicasHelp_SVC_CF UNIQUE (svcClass, castorFile);

/* Global temporary table to store subRequest and castorFile ids for cleanup operations.
   See the deleteTerminatedRequest procedure for more details.
 */
CREATE GLOBAL TEMPORARY TABLE DeleteTermReqHelper
  (srId NUMBER, cfId NUMBER)
  ON COMMIT PRESERVE ROWS;

/* Global temporary table used in streamsToDo to temporarily
 * store interesting streams.
 */
CREATE GLOBAL TEMPORARY TABLE StreamsToDoHelper (id NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to handle output of the processBulkRequest procedure */
CREATE GLOBAL TEMPORARY TABLE getFileIdsForSrsHelper (rowno NUMBER, fileId NUMBER, nsHost VARCHAR(2048))
  ON COMMIT DELETE ROWS;

/* SQL statements for table PriorityMap */
CREATE TABLE PriorityMap (euid INTEGER, egid INTEGER, priority INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
ALTER TABLE PriorityMap ADD CONSTRAINT UN_Priority_euid_egid UNIQUE (euid, egid);

/*
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

/* Create the AdminUsers table */
CREATE TABLE AdminUsers (euid NUMBER, egid NUMBER);
ALTER TABLE AdminUsers ADD CONSTRAINT UN_AdminUsers_euid_egid UNIQUE (euid, egid);
INSERT INTO AdminUsers VALUES (0, 0);   -- root/root, to be removed
INSERT INTO AdminUsers VALUES (-1, -1); -- internal requests

/* Prompt for stage:st account */
PROMPT Configuration of the admin part of the B/W list
UNDEF stageUid
ACCEPT stageUid NUMBER PROMPT 'Enter the stage user id: ';
UNDEF stageGid
ACCEPT stageGid NUMBER PROMPT 'Enter the st group id: ';
INSERT INTO AdminUsers VALUES (&stageUid, &stageGid);

/* Prompt for additional administrators */
PROMPT In order to define admins that will be exempt of B/W list checks,
PROMPT (e.g. c3 group at CERN), please give a space separated list of
PROMPT <userid>:<groupid> pairs. userid can be empty, meaning any user
PROMPT in the specified group.
UNDEF adminList
ACCEPT adminList CHAR PROMPT 'List of admins: ';

/* Define the service handlers for the appropriate sets of stage request objects */
UPDATE Type2Obj SET svcHandler = 'JobReqSvc' WHERE type IN (35, 40, 44);
UPDATE Type2Obj SET svcHandler = 'PrepReqSvc' WHERE type IN (36, 37, 38);
UPDATE Type2Obj SET svcHandler = 'StageReqSvc' WHERE type IN (39, 42, 95);
UPDATE Type2Obj SET svcHandler = 'QueryReqSvc' WHERE type IN (33, 34, 41, 103, 131, 152, 155, 195);
UPDATE Type2Obj SET svcHandler = 'JobSvc' WHERE type IN (60, 64, 65, 67, 78, 79, 80, 93, 144, 147);
UPDATE Type2Obj SET svcHandler = 'GCSvc' WHERE type IN (73, 74, 83, 142, 149);
UPDATE Type2Obj SET svcHandler = 'BulkStageReqSvc' WHERE type IN (50, 119);

/* Set default values for the StageDiskCopyReplicaRequest table */
ALTER TABLE StageDiskCopyReplicaRequest MODIFY flags DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY euid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY egid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY mask DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY pid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY machine DEFAULT 'stager';

/* Indexing StageDiskCopyReplicaRequest by source and destination diskcopy id */
CREATE INDEX I_StageDiskCopyReplic_SourceDC 
  ON StageDiskCopyReplicaRequest (sourceDiskCopy);
CREATE INDEX I_StageDiskCopyReplic_DestDC 
  ON StageDiskCopyReplicaRequest (destDiskCopy);

/* Define a table for some configuration key-value pairs and populate it */
CREATE TABLE CastorConfig
  (class VARCHAR2(2048) CONSTRAINT NN_CastorConfig_Class NOT NULL, 
   key VARCHAR2(2048) CONSTRAINT NN_CastorConfig_Key NOT NULL, 
   value VARCHAR2(2048) CONSTRAINT NN_CastorConfig_Value NOT NULL, 
   description VARCHAR2(2048));

ALTER TABLE CastorConfig ADD CONSTRAINT UN_CastorConfig_class_key UNIQUE (class, key);

/* Prompt for the value of the general/instance options */
UNDEF instanceName
ACCEPT instanceName DEFAULT castor_stager PROMPT 'Enter the name of the castor instance: (default: castor_stager, example: castoratlas) '
SET VER OFF

INSERT INTO CastorConfig
  VALUES ('general', 'instance', '&instanceName', 'Name of this Castor instance');

/* Prompt for the value of the stager/nsHost option */
UNDEF stagerNsHost
ACCEPT stagerNsHost DEFAULT undefined PROMPT 'Enter the name of the stager/nsHost: (default: undefined, example: castorns) '

INSERT INTO CastorConfig
  VALUES ('stager', 'nsHost', '&stagerNsHost', 'The name of the name server host to set in the CastorFile table overriding the CNS/HOST option defined in castor.conf');


INSERT INTO CastorConfig
  VALUES ('general', 'owner', sys_context('USERENV', 'CURRENT_USER'), 'The database owner of the schema');
INSERT INTO CastorConfig
  VALUES ('cleaning', 'terminatedRequestsTimeout', '120', 'Maximum timeout for successful and failed requests in hours');
INSERT INTO CastorConfig
  VALUES ('cleaning', 'outOfDateStageOutDCsTimeout', '72', 'Timeout for STAGEOUT diskCopies in hours');
INSERT INTO CastorConfig
  VALUES ('cleaning', 'failedDCsTimeout', '72', 'Timeout for failed diskCopies in hours');
INSERT INTO CastorConfig
  VALUES ('Repack', 'Protocol', 'rfio', 'The protocol that repack should use for writing files to disk');
INSERT INTO CastorConfig
  VALUES ('Repack', 'MaxNbConcurrentClients', '5', 'The maximum number of repacks clients that are able to run concurrently. This are either clients atrting repacks or aborting running repacks. Providing that each of them will take a DB core, this number should roughtly be ~60% of the number of cores of the stager DB server');
COMMIT;


/*********************************************************************/
/* FileSystemsToCheck used to optimise the processing of filesystems */
/* when they change status                                           */
/*********************************************************************/
CREATE TABLE FileSystemsToCheck (FileSystem NUMBER CONSTRAINT PK_FSToCheck_FS PRIMARY KEY, ToBeChecked NUMBER);
INSERT INTO FileSystemsToCheck SELECT id, 0 FROM FileSystem;


/**************/
/* Accounting */
/**************/

/* WARNING!!!! Changing this to a materialized view which is refresh at a set
 * frequency causes problems with the disk server draining tools.
 */
CREATE TABLE Accounting (euid INTEGER CONSTRAINT NN_Accounting_Euid NOT NULL, 
                         fileSystem INTEGER CONSTRAINT NN_Accounting_Filesystem NOT NULL,
                         nbBytes INTEGER);
ALTER TABLE Accounting 
ADD CONSTRAINT PK_Accounting_EuidFs PRIMARY KEY (euid, fileSystem);

/* SQL statement for the creation of the AccountingSummary view */
CREATE OR REPLACE VIEW AccountingSummary
AS
  SELECT (SELECT cast(last_start_date AS DATE) 
            FROM dba_scheduler_jobs
           WHERE job_name = 'ACCOUNTINGJOB'
             AND owner = 
              (SELECT value FROM CastorConfig
                WHERE class = 'general' AND key = 'owner')) timestamp,
         3600 interval, SvcClass.name SvcClass, Accounting.euid, 
         sum(Accounting.nbbytes) totalBytes
    FROM Accounting, FileSystem, DiskPool2SvcClass, svcclass
   WHERE Accounting.filesystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = SvcClass.id
   GROUP BY SvcClass.name, Accounting.euid
   ORDER BY SvcClass.name, Accounting.euid;


/************************************/
/* Garbage collection related table */
/************************************/

/* A table storing the Gc policies and detailing there configuration
 * For each policy, identified by a name, parameters are :
 *   - userWeight : the name of the PL/SQL function to be called to
 *     precompute the GC weight when a file is written by the user.
 *   - recallWeight : the name of the PL/SQL function to be called to
 *     precompute the GC weight when a file is recalled
 *   - copyWeight : the name of the PL/SQL function to be called to
 *     precompute the GC weight when a file is disk to disk copied
 *   - firstAccessHook : the name of the PL/SQL function to be called
 *     when the file is accessed for the first time. Can be NULL.
 *   - accessHook : the name of the PL/SQL function to be called
 *     when the file is accessed (except for the first time). Can be NULL.
 *   - userSetGCWeight : the name of the PL/SQL function to be called
 *     when a setFileGcWeight user request is processed can be NULL.
 * All functions return a number that is the new gcWeight.
 * In general, here are the signatures :
 *   userWeight(fileSize NUMBER, DiskCopyStatus NUMBER)
 *   recallWeight(fileSize NUMBER)
 *   copyWeight(fileSize NUMBER, DiskCopyStatus NUMBER, sourceWeight NUMBER))
 *   firstAccessHook(oldGcWeight NUMBER, creationTime NUMBER)
 *   accessHook(oldGcWeight NUMBER, creationTime NUMBER, nbAccesses NUMBER)
 *   userSetGCWeight(oldGcWeight NUMBER, userDelta NUMBER)
 * Few notes :
 *   diskCopyStatus can be STAGED(0) or CANBEMIGR(10)
 */
CREATE TABLE GcPolicy (name VARCHAR2(2048) CONSTRAINT NN_GcPolicy_Name NOT NULL CONSTRAINT PK_GcPolicy_Name PRIMARY KEY,
                       userWeight VARCHAR2(2048) CONSTRAINT NN_GcPolicy_UserWeight NOT NULL,
                       recallWeight VARCHAR2(2048) CONSTRAINT NN_GcPolicy_RecallWeight NOT NULL,
                       copyWeight VARCHAR2(2048) CONSTRAINT NN_GcPolicy_CopyWeight NOT NULL,
                       firstAccessHook VARCHAR2(2048) DEFAULT NULL,
                       accessHook VARCHAR2(2048) DEFAULT NULL,
                       userSetGCWeight VARCHAR2(2048) DEFAULT NULL);

/* Default policy, mainly based on file sizes */
INSERT INTO GcPolicy VALUES ('default',
                             'castorGC.sizeRelatedUserWeight',
                             'castorGC.sizeRelatedRecallWeight',
                             'castorGC.sizeRelatedCopyWeight',
                             'castorGC.dayBonusFirstAccessHook',
                             'castorGC.halfHourBonusAccessHook',
                             'castorGC.cappedUserSetGCWeight');
INSERT INTO GcPolicy VALUES ('FIFO',
                             'castorGC.creationTimeUserWeight',
                             'castorGC.creationTimeRecallWeight',
                             'castorGC.creationTimeCopyWeight',
                             NULL,
                             NULL,
                             NULL);
INSERT INTO GcPolicy VALUES ('LRU',
                             'castorGC.creationTimeUserWeight',
                             'castorGC.creationTimeRecallWeight',
                             'castorGC.creationTimeCopyWeight',
                             'castorGC.LRUFirstAccessHook',
                             'castorGC.LRUAccessHook',
                             NULL);


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
/

/* FileSystem index based on the rate. */
CREATE INDEX I_FileSystem_Rate
    ON FileSystem(fileSystemRate(readRate, writeRate,
	          nbReadStreams,nbWriteStreams, nbReadWriteStreams, nbMigratorStreams, nbRecallerStreams));


/************/
/* Aborting */
/************/

CREATE TABLE TransfersToAbort (uuid VARCHAR2(2048)
  CONSTRAINT NN_TransfersToAbort_Uuid NOT NULL);

/*******************************/
/* running job synchronization */
/*******************************/

CREATE GLOBAL TEMPORARY TABLE SyncRunningTransfersHelper(subreqId VARCHAR2(2048)) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMPORARY TABLE SyncRunningTransfersHelper2
(subreqId VARCHAR2(2048), reqId VARCHAR2(2048),
 fileid NUMBER, nsHost VARCHAR2(2048),
 errorCode NUMBER, errorMsg VARCHAR2(2048))
ON COMMIT PRESERVE ROWS;

/***************************************************/
/* rmMaster main lock, only used to elect a master */
/***************************************************/

CREATE TABLE RmMasterLock (unused NUMBER);


/**********/
/* Repack */
/**********/

/* DB link to the nameserver db */
PROMPT Configuration of the database link to the CASTOR name space
UNDEF cnsUser
ACCEPT cnsUser CHAR DEFAULT 'castorns' PROMPT 'Enter the nameserver db username (default castorns): ';
UNDEF cnsPasswd
ACCEPT cnsPasswd CHAR PROMPT 'Enter the nameserver db password: ';
UNDEF cnsDbName
ACCEPT cnsDbName CHAR DEFAULT PROMPT 'Enter the nameserver db TNS name: ';
CREATE DATABASE LINK remotens
  CONNECT TO &cnsUser IDENTIFIED BY &cnsPasswd USING '&cnsDbName';

/* temporary table used for listing segments of a tape */
/* efficiently via DB link when repacking              */
CREATE GLOBAL TEMPORARY TABLE RepackTapeSegments (s_fileId NUMBER, blockid RAW(4), fseq NUMBER, segSize NUMBER, copyno NUMBER, fileClass NUMBER) ON COMMIT PRESERVE ROWS;
/*******************************************************************
 * @(#)RCSfile: oracleDrain.schema.sql,v  Revision: 1.4  Date: 2009/07/05 13:49:08  Author: waldron 
 * Schema creation code for Draining FileSystems Logic
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement for the creation of the DrainingFileSystem table */
CREATE TABLE DrainingFileSystem
  (userName       VARCHAR2(30) CONSTRAINT NN_DrainingFs_UserName NOT NULL,
   machine        VARCHAR2(500) CONSTRAINT NN_DrainingFs_Machine NOT NULL,
   creationTime   NUMBER DEFAULT 0,
   startTime      NUMBER DEFAULT 0,
   lastUpdateTime NUMBER DEFAULT 0,
   fileSystem     NUMBER CONSTRAINT NN_DrainingFs_FileSystem NOT NULL,
   /* Current state of the draining process, one of:
    *   0 -- CREATED
    *   1 -- INITIALIZING
    *   2 -- RUNNING
    *   3 -- INTERRUPTED
    *   4 -- FAILED
    *   5 -- COMPLETED
    *   6 -- DELETING
    *   7 -- RESTART
    */
   status         NUMBER DEFAULT 0,
   svcClass       NUMBER CONSTRAINT NN_DrainingFs_SvcClass NOT NULL,
   /* Flag to indicate whether files should be invalidated so that they can be
    * removed by the garbage collection process after a file is replicated to
    * another diskserver.
    */
   autoDelete     NUMBER DEFAULT 0,
   /* Column to indicate which files should be replicated. Valid values are:
    *   0 -- STAGED,
    *   1 -- CANBEMIGR
    *   2 -- ALL
    */
   fileMask       NUMBER DEFAULT 1,
   /* The maximum number of current transfers (job slots) available for draining
    * the filesystem.
    */
   maxTransfers   NUMBER DEFAULT 50,
   totalFiles     NUMBER DEFAULT 0,
   totalBytes     NUMBER DEFAULT 0,
   comments       VARCHAR2(50) DEFAULT 'N/A' CONSTRAINT NN_DrainingFs_Comments NOT NULL)
  /* Allow shrink operations */
  ENABLE ROW MOVEMENT;

/* SQL statement for primary key constraint on DrainingFileSystem */
ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT PK_DrainingFs_FileSystem
  PRIMARY KEY (fileSystem);

/* SQL statements for check constraints on the DrainingFileSystem table */
ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT CK_DrainingFs_Status
  CHECK (status IN (0, 1, 2, 3, 4, 5, 6, 7));

ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT CK_DrainingFs_FileMask
  CHECK (fileMask IN (0, 1, 2));

ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT CK_DrainingFs_AutoDelete
  CHECK (autoDelete IN (0, 1));

ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT CK_DrainingFs_MaxTransfers
  CHECK (maxTransfers > 0);

/* SQL statements for foreign key constraints on DrainingFileSystem */
ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT FK_DrainingFs_SvcClass
  FOREIGN KEY (svcClass)
  REFERENCES SvcClass (id);

ALTER TABLE DrainingFileSystem
  ADD CONSTRAINT FK_DrainingFs_FileSystem
  FOREIGN KEY (fileSystem)
  REFERENCES FileSystem (id);

/* SQL statements for indexes on DrainingFileSystem table */
CREATE INDEX I_DrainingFileSystem_SvcClass
  ON DrainingFileSystem (svcClass);


/* SQL statements for the creation of the DrainingDiskCopy table
 *
 * The way the logic for draining a filesystems works is to essentially create
 * a list of all the files that need to be replicated to other diskservers and
 * to process that list until all files have been replicated.
 *
 * This list/queue could have been done with Oracle Advanced Queuing (AQ).
 * However, due to the complexities of setting it up and the lack of prior
 * experience on behalf of the CASTOR developers and CERN DBA's we create a
 * simple queue using a standard table.
 */
CREATE TABLE DrainingDiskCopy
  (fileSystem     NUMBER CONSTRAINT NN_DrainingDCs_FileSystem NOT NULL,
   /* Status of the diskcopy to be replicated. Note: this is not the same as
    * the status of the diskcopy i.e. STAGED, CANBEMIGR. It is an internal
    * status assigned to each diskcopy (file) as a means of tracking how far the
    * file is in the lifecycle of draining a filesystem.
    * The status can be one of:
    *   0 -- CREATED
    *   1 -- RESTARTED
    *   2 -- PROCESSING    (Transient state)
    *   3 -- WAITD2D
    *   4 -- FAILED
    */
   status         NUMBER DEFAULT 0 CONSTRAINT NN_DrainingDCs_Status NOT NULL,
   /* A link to the diskcopy. Note: this is deliberately not enforced with a
    * foreign key constraint!!!
    */
   diskCopy       NUMBER CONSTRAINT NN_DrainingDCs_DiskCopy NOT NULL,
   parent         NUMBER DEFAULT 0 CONSTRAINT NN_DrainingDCs_Parent NOT NULL,
   creationTime   NUMBER DEFAULT 0,
   priority       NUMBER DEFAULT 0,
   fileSize       NUMBER DEFAULT 0 CONSTRAINT NN_DrainingDCs_FileSize NOT NULL,
   comments       VARCHAR2(2048) DEFAULT NULL)
  /* Allow shrink operations */
  ENABLE ROW MOVEMENT;

/* SQL statement for primary key constraint on DrainingDiskCopy */
ALTER TABLE DrainingDiskCopy
  ADD CONSTRAINT PK_DrainingDCs_DiskCopy
  PRIMARY KEY (diskCopy);

/* SQL statement for check constraints on the DrainingDiskCopy table */
ALTER TABLE DrainingDiskCopy
  ADD CONSTRAINT CK_DrainingDCs_Status
  CHECK (status IN (0, 1, 2, 3, 4));

/* SQL statement for foreign key constraints on DrainingDiskCopy */
ALTER TABLE DrainingDiskCopy
  ADD CONSTRAINT FK_DrainingDCs_FileSystem
  FOREIGN KEY (fileSystem)
  REFERENCES DrainingFileSystem (fileSystem);

/* SQL statements for indexes on DrainingDiskCopy table */
CREATE INDEX I_DrainingDCs_FileSystem
  ON DrainingDiskCopy (fileSystem);

CREATE INDEX I_DrainingDCs_Status
  ON DrainingDiskCopy (status);

/* This index is essentially the same as the one on the SubRequest table which
 * allows us to process entries in order. In this case by priority and
 * creationTime.
 */
CREATE INDEX I_DrainingDCs_PC
  ON DrainingDiskCopy (priority, creationTime);

CREATE INDEX I_DrainingDCs_Parent
  ON DrainingDiskCopy (parent);
/*******************************************************************
 * @(#)RCSfile: oracleMonitoring.schema.sql,v  Revision: 1.6  Date: 2009/07/05 13:46:14  Author: waldron 
 * Schema creation code for Monitoring tables
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement for table MonDiskCopyStats */
CREATE TABLE MonDiskCopyStats
  (timestamp DATE, interval NUMBER, diskServer VARCHAR2(255), mountPoint VARCHAR2(255), dsStatus VARCHAR2(50), fsStatus VARCHAR2(50), available VARCHAR2(2), status VARCHAR2(100), totalFileSize NUMBER, nbFiles NUMBER);

/* SQL statement for table MonWaitTapeMigrationStats */
CREATE TABLE MonWaitTapeMigrationStats
  (timestamp DATE, interval NUMBER, svcClass VARCHAR2(255), status VARCHAR2(10), minWaitTime NUMBER, maxWaitTime NUMBER, avgWaitTime NUMBER, minFileSize NUMBER, maxFileSize NUMBER, avgFileSize NUMBER, bin_LT_1 NUMBER, bin_1_To_6 NUMBER, bin_6_To_12 NUMBER, bin_12_To_24 NUMBER, bin_24_To_48 NUMBER, bin_GT_48 NUMBER, totalFileSize NUMBER, nbFiles NUMBER);

/* SQL statement for table MonWaitTapeRecallStats  */
CREATE TABLE MonWaitTapeRecallStats
  (timestamp DATE, interval NUMBER, svcClass VARCHAR2(255), minWaitTime NUMBER, maxWaitTime NUMBER, avgWaitTime NUMBER, minFileSize NUMBER, maxFileSize NUMBER, avgFileSize NUMBER, bin_LT_1 NUMBER, bin_1_To_6 NUMBER, bin_6_To_12 NUMBER, bin_12_To_24 NUMBER, bin_24_To_48 NUMBER, bin_GT_48 NUMBER, totalFileSize NUMBER, nbFiles NUMBER);

/******************************************************************************
 *                 castor/db/oracleTapeConstants.sql
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Nicola Bessone and Steven Murray
 *****************************************************************************/

/**
 * Package containing the definition of all tape-archieve related PL/SQL 
 * constants.
 */
CREATE OR REPLACE PACKAGE tconst
AS

  -- TPMODE
  WRITE_DISABLE CONSTANT PLS_INTEGER :=  0;
  WRITE_ENABLE  CONSTANT PLS_INTEGER :=  1;

  SEGMENT_UNPROCESSED CONSTANT PLS_INTEGER := 0;
  SEGMENT_FILECOPIED  CONSTANT PLS_INTEGER := 5;
  SEGMENT_FAILED      CONSTANT PLS_INTEGER := 6;
  SEGMENT_SELECTED    CONSTANT PLS_INTEGER := 7;
  SEGMENT_RETRIED     CONSTANT PLS_INTEGER := 8;

  MIGRATIONMOUNT_WAITTAPE  CONSTANT PLS_INTEGER := 0;
  MIGRATIONMOUNT_SEND_TO_VDQM CONSTANT PLS_INTEGER := 1;
  MIGRATIONMOUNT_WAITDRIVE CONSTANT PLS_INTEGER := 2;
  MIGRATIONMOUNT_MIGRATING CONSTANT PLS_INTEGER := 3;

  TAPE_UNUSED     CONSTANT PLS_INTEGER := 0;
  TAPE_PENDING    CONSTANT PLS_INTEGER := 1;
  TAPE_WAITDRIVE  CONSTANT PLS_INTEGER := 2;
  TAPE_WAITMOUNT  CONSTANT PLS_INTEGER := 3;
  TAPE_MOUNTED    CONSTANT PLS_INTEGER := 4;
  TAPE_FINISHED   CONSTANT PLS_INTEGER := 5;
  TAPE_FAILED     CONSTANT PLS_INTEGER := 6;
  TAPE_UNKNOWN    CONSTANT PLS_INTEGER := 7;
  TAPE_WAITPOLICY CONSTANT PLS_INTEGER := 8;
  TAPE_ATTACHEDTOSTREAM CONSTANT PLS_INTEGER := 9;
  
  TPMODE_READ     CONSTANT PLS_INTEGER := 0;
  TPMODE_WRITE    CONSTANT PLS_INTEGER := 1;

  RECALLJOB_SELECTED      CONSTANT PLS_INTEGER := 3;
  RECALLJOB_TOBERECALLED  CONSTANT PLS_INTEGER := 4;
  RECALLJOB_STAGED        CONSTANT PLS_INTEGER := 5;
  RECALLJOB_FAILED        CONSTANT PLS_INTEGER := 6;
  RECALLJOB_RETRY         CONSTANT PLS_INTEGER := 8;

  MIGRATIONJOB_PENDING   CONSTANT PLS_INTEGER := 0;
  MIGRATIONJOB_SELECTED  CONSTANT PLS_INTEGER := 1;
  MIGRATIONJOB_MIGRATED  CONSTANT PLS_INTEGER := 2;
  MIGRATIONJOB_FAILED    CONSTANT PLS_INTEGER := 6;
  MIGRATIONJOB_RETRY     CONSTANT PLS_INTEGER := 8;

  REPACK_STARTING        CONSTANT PLS_INTEGER := 0;
  REPACK_ONGOING         CONSTANT PLS_INTEGER := 1;
  REPACK_FINISHED        CONSTANT PLS_INTEGER := 2;
  REPACK_FAILED          CONSTANT PLS_INTEGER := 3;
  REPACK_ABORTING        CONSTANT PLS_INTEGER := 4;
  REPACK_ABORTED         CONSTANT PLS_INTEGER := 5;

END tconst;
/


/**
 * Package containing the definition of all disk related PL/SQL constants.
 */
CREATE OR REPLACE PACKAGE dconst
AS

  DISKCOPY_STAGED            CONSTANT PLS_INTEGER :=  0;
  DISKCOPY_WAITDISK2DISKCOPY CONSTANT PLS_INTEGER :=  1;
  DISKCOPY_WAITTAPERECALL    CONSTANT PLS_INTEGER :=  2;
  DISKCOPY_DELETED           CONSTANT PLS_INTEGER :=  3;
  DISKCOPY_FAILED            CONSTANT PLS_INTEGER :=  4;
  DISKCOPY_WAITFS            CONSTANT PLS_INTEGER :=  5;
  DISKCOPY_STAGEOUT          CONSTANT PLS_INTEGER :=  6;
  DISKCOPY_INVALID           CONSTANT PLS_INTEGER :=  7;
  DISKCOPY_BEINGDELETED      CONSTANT PLS_INTEGER :=  9;
  DISKCOPY_CANBEMIGR         CONSTANT PLS_INTEGER := 10;
  DISKCOPY_WAITFS_SCHEDULING CONSTANT PLS_INTEGER := 11;

  DISKSERVER_PRODUCTION CONSTANT PLS_INTEGER := 0;
  DISKSERVER_DRAINING   CONSTANT PLS_INTEGER := 1;
  DISKSERVER_DISABLED   CONSTANT PLS_INTEGER := 2;

  FILESYSTEM_PRODUCTION CONSTANT PLS_INTEGER := 0;
  FILESYSTEM_DRAINING   CONSTANT PLS_INTEGER := 1;
  FILESYSTEM_DISABLED   CONSTANT PLS_INTEGER := 2;

  SUBREQUEST_START            CONSTANT PLS_INTEGER :=  0;
  SUBREQUEST_RESTART          CONSTANT PLS_INTEGER :=  1;
  SUBREQUEST_RETRY            CONSTANT PLS_INTEGER :=  2;
  SUBREQUEST_WAITSCHED        CONSTANT PLS_INTEGER :=  3;
  SUBREQUEST_WAITTAPERECALL   CONSTANT PLS_INTEGER :=  4;
  SUBREQUEST_WAITSUBREQ       CONSTANT PLS_INTEGER :=  5;
  SUBREQUEST_READY            CONSTANT PLS_INTEGER :=  6;
  SUBREQUEST_FAILED           CONSTANT PLS_INTEGER :=  7;
  SUBREQUEST_FINISHED         CONSTANT PLS_INTEGER :=  8;
  SUBREQUEST_FAILED_FINISHED  CONSTANT PLS_INTEGER :=  9;
  SUBREQUEST_FAILED_ANSWERING CONSTANT PLS_INTEGER := 10;
  SUBREQUEST_ARCHIVED         CONSTANT PLS_INTEGER := 11;
  SUBREQUEST_REPACK           CONSTANT PLS_INTEGER := 12;
  SUBREQUEST_READYFORSCHED    CONSTANT PLS_INTEGER := 13;
  SUBREQUEST_BEINGSCHED       CONSTANT PLS_INTEGER := 14;

  GETNEXTSTATUS_NOTAPPLICABLE CONSTANT PLS_INTEGER :=  0;
  GETNEXTSTATUS_FILESTAGED    CONSTANT PLS_INTEGER :=  1;
  GETNEXTSTATUS_NOTIFIED      CONSTANT PLS_INTEGER :=  2;

  DISKPOOLQUERYTYPE_DEFAULT   CONSTANT PLS_INTEGER :=  0;
  DISKPOOLQUERYTYPE_AVAILABLE CONSTANT PLS_INTEGER :=  1;
  DISKPOOLQUERYTYPE_TOTAL     CONSTANT PLS_INTEGER :=  2;

  DISKPOOLSPACETYPE_FREE     CONSTANT PLS_INTEGER :=  0;
  DISKPOOLSPACETYPE_CAPACITY CONSTANT PLS_INTEGER :=  1;

END dconst;
/

/*******************************************************************
 *
 * @(#)RCSfile: oracleCommon.sql,v  Revision: 1.697  Date: 2009/08/13 14:11:33  Author: itglp 
 *
 * This file contains some common PL/SQL utilities for the stager database.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* Get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
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
END;
/

/* Generate a universally unique id (UUID) */
CREATE OR REPLACE FUNCTION uuidGen RETURN VARCHAR2 IS
  ret VARCHAR2(36);
BEGIN
  -- Note: the guid generator provided by ORACLE produces sequential uuid's, not
  -- random ones. The reason for this is because random uuid's are not good for
  -- indexing!
  RETURN lower(regexp_replace(sys_guid(), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5'));
END;
/

/* Function to check if a service class exists by name. This function can return
 * the id of the named service class or raise an application error if it does
 * not exist.
 * @param svcClasName The name of the service class (Note: can be NULL)
 * @param allowNull   Flag to indicate whether NULL or '' service class names are
 *                    permitted.
 * @param raiseError  Flag to indicate whether the function should raise an
 *                    application error when the service class doesn't exist or
 *                    return a boolean value of 0.
 */
CREATE OR REPLACE FUNCTION checkForValidSvcClass
(svcClassName VARCHAR2, allowNull NUMBER, raiseError NUMBER) RETURN NUMBER IS
  ret NUMBER;
BEGIN
  -- Check if the service class name is allowed to be NULL. This is quite often
  -- the case if the calling function supports '*' (null) to indicate that all
  -- service classes are being targeted. Nevertheless, in such a case we
  -- return the id of the default one.
  IF svcClassName IS NULL OR length(svcClassName) IS NULL THEN
    IF allowNull = 1 THEN
      SELECT id INTO ret FROM SvcClass WHERE name = 'default';
      RETURN ret;
    END IF;
  END IF;
  -- We do accept '*' as being valid, as it is the wildcard
  IF svcClassName = '*' THEN
    RETURN 0;
  END IF;
  -- Check to see if service class exists by name and return its id
  BEGIN
    SELECT id INTO ret FROM SvcClass WHERE name = svcClassName;
    RETURN ret;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- If permitted to do so raise an application error if the service class does
    -- not exist
    IF raiseError = 1 THEN
      raise_application_error(-20113, 'Invalid service class ''' || svcClassName || '''');
    END IF;
    RETURN 0;
  END;
END;
/

/* Function to return a comma separate list of service classes that a
 * filesystem belongs to.
 */
CREATE OR REPLACE FUNCTION getSvcClassList(fsId NUMBER) RETURN VARCHAR2 IS
  svcClassList VARCHAR2(4000) := NULL;
  c INTEGER := 0;
BEGIN
  FOR a IN (SELECT Distinct(SvcClass.name)
              FROM FileSystem, DiskPool2SvcClass, SvcClass
             WHERE FileSystem.id = fsId
               AND FileSystem.diskpool = DiskPool2SvcClass.parent
               AND DiskPool2SvcClass.child = SvcClass.id
             ORDER BY SvcClass.name)
  LOOP
    svcClassList := svcClassList || ',' || a.name;
    c := c + 1;
    IF c = 5 THEN
      svcClassList := svcClassList || ',...';
      EXIT;
    END IF;
  END LOOP;
  RETURN ltrim(svcClassList, ',');
END;
/

/* Function to extract a configuration option from the castor config
 * table.
 */
CREATE OR REPLACE FUNCTION getConfigOption
(className VARCHAR2, optionName VARCHAR2, defaultValue VARCHAR2) 
RETURN VARCHAR2 IS
  returnValue VARCHAR2(2048) := defaultValue;
BEGIN
  SELECT value INTO returnValue
    FROM CastorConfig
   WHERE class = className
     AND key = optionName
     AND value != 'undefined';
  RETURN returnValue;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RETURN returnValue;
END;
/

/* Function to tokenize a string using a specified delimiter. If no delimiter
 * is specified the default is ','. The results are returned as a table e.g.
 * SELECT * FROM TABLE (strTokenizer(inputValue, delimiter))
 */
CREATE OR REPLACE FUNCTION strTokenizer(p_list VARCHAR2, p_del VARCHAR2 := ',')
  RETURN strListTable pipelined IS
  l_idx   INTEGER;
  l_list  VARCHAR2(32767) := p_list;
  l_value VARCHAR2(32767);
BEGIN
  LOOP
    l_idx := instr(l_list, p_del);
    IF l_idx > 0 THEN
      PIPE ROW(ltrim(rtrim(substr(l_list, 1, l_idx - 1))));
      l_list := substr(l_list, l_idx + length(p_del));
    ELSE
      PIPE ROW(ltrim(rtrim(l_list)));
      EXIT;
    END IF;
  END LOOP;
  RETURN;
END;
/

/* Function to normalize a filepath, i.e. to drop multiple '/'s and resolve any '..' */
CREATE OR REPLACE FUNCTION normalizePath(path IN VARCHAR2) RETURN VARCHAR2 IS
  buf VARCHAR2(2048);
  ret VARCHAR2(2048);
BEGIN
  -- drop '.'s and multiple '/'s
  ret := replace(regexp_replace(path, '[/]+', '/'), '/./', '/');
  LOOP
    buf := ret;
    -- a path component is by definition anything between two slashes, except
    -- the '..' string itself. This is not taken into account, resulting in incorrect
    -- parsing when relative paths are used (see bug #49002). We're not concerned by
    -- those cases; however this code could be fixed and improved by using string
    -- tokenization as opposed to expensive regexp parsing.
    ret := regexp_replace(buf, '/[^/]+/\.\./', '/');
    EXIT WHEN ret = buf;
  END LOOP;
  RETURN ret;
END;
/

/* Function to check if a diskserver and its given mountpoints have any files
 * attached to them.
 */
CREATE OR REPLACE
FUNCTION checkIfFilesExist(diskServerName IN VARCHAR2, mountPointName IN VARCHAR2)
RETURN NUMBER AS
  rtn NUMBER;
BEGIN
  SELECT count(*) INTO rtn
    FROM DiskCopy, FileSystem, DiskServer
   WHERE DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.name = diskServerName
     AND (FileSystem.mountpoint = mountPointName 
      OR  length(mountPointName) IS NULL)
     AND rownum = 1;
  RETURN rtn;
END;
/

/* PL/SQL method deleting migration jobs of a castorfile */
CREATE OR REPLACE PROCEDURE deleteMigrationJobs(cfId NUMBER) AS
BEGIN
  DELETE FROM MigrationJob WHERE castorfile = cfId;
END;
/

/* PL/SQL method deleting recall jobs (and segments) of a castorfile */
CREATE OR REPLACE PROCEDURE deleteRecallJobs(cfId NUMBER) AS
BEGIN
  -- Loop over the recall jobs
  FOR t IN (SELECT id FROM RecallJob WHERE castorfile = cfId) LOOP
    DELETE FROM Segment WHERE copy = t.id;
    DELETE FROM RecallJob WHERE id = t.id;
  END LOOP;
END;
/

/* PL/SQL method canceling a given recall */
CREATE OR REPLACE PROCEDURE cancelRecall
(cfId NUMBER, dcId NUMBER, newSubReqStatus NUMBER) AS
  srIds "numList";
  unused NUMBER;
BEGIN
  -- Lock the CastorFile
  SELECT id INTO unused FROM CastorFile
   WHERE id = cfId FOR UPDATE;
  -- Cancel the recall
  deleteRecallJobs(cfId);
  -- Invalidate the DiskCopy
  UPDATE DiskCopy SET status = 7 WHERE id = dcId; -- INVALID
  -- Look for request associated to the recall and fail
  -- it and all the waiting ones
  UPDATE /*+ INDEX(Subrequest I_Subrequest_Diskcopy)*/ SubRequest
     SET status = newSubReqStatus
   WHERE diskCopy = dcId RETURNING id BULK COLLECT INTO srIds;
  UPDATE /*+ INDEX(Subrequest I_Subrequest_Parent)*/ SubRequest
     SET status = newSubReqStatus, parent = 0 -- FAILED
   WHERE status = 5 -- WAITSUBREQ
     AND parent IN
       (SELECT /*+ CARDINALITY(sridTable 5) */ *
          FROM TABLE(srIds) sridTable)
     AND castorfile = cfId;
END;
/

/* PL/SQL method to delete a CastorFile only when no DiskCopy, no MigrationJob and no RecallJob are left for it */
/* Internally used in filesDeletedProc, putFailedProc and deleteOutOfDateDiskCopies */
CREATE OR REPLACE PROCEDURE deleteCastorFile(cfId IN NUMBER) AS
  nb NUMBER;
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
BEGIN
  -- First try to lock the castorFile
  SELECT id INTO nb FROM CastorFile
   WHERE id = cfId FOR UPDATE NOWAIT;
  -- See whether it has any DiskCopy
  SELECT count(*) INTO nb FROM DiskCopy
   WHERE castorFile = cfId;
  -- If any DiskCopy, give up
  IF nb = 0 THEN
    -- See whether it has any RecallJob
    SELECT count(*) INTO nb FROM RecallJob
     WHERE castorFile = cfId AND status != tconst.RECALLJOB_FAILED;
    -- If any RecallJob, give up
    IF nb = 0 THEN
      -- See whether it has any MigrationJob
      SELECT count(*) INTO nb FROM MigrationJob
       WHERE castorFile = cfId AND status != tconst.RECALLJOB_FAILED;
      -- If any MigrationJob, give up
      IF nb = 0 THEN
        -- See whether pending SubRequests exist
        SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
          FROM SubRequest
         WHERE castorFile = cfId
           AND status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 12, 13, 14);   -- All but FINISHED, FAILED_FINISHED, ARCHIVED
        -- If any SubRequest, give up
        IF nb = 0 THEN
          DECLARE
            fid NUMBER;
            fc NUMBER;
            nsh VARCHAR2(2048);
          BEGIN
            -- Delete the failed Recalls
            deleteRecallJobs(cfId);
            -- Delete the failed Migrations
            deleteMigrationJobs(cfId);
            -- Delete the CastorFile
            DELETE FROM CastorFile WHERE id = cfId
              RETURNING fileId, nsHost, fileClass
              INTO fid, nsh, fc;
            -- check whether this file potentially had copies on tape
            SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
            IF nb = 0 THEN
              -- This castorfile was created with no copy on tape
              -- So removing it from the stager means erasing
              -- it completely. We should thus also remove it
              -- from the name server
              INSERT INTO FilesDeletedProcOutput VALUES (fid, nsh);
            END IF;
          END;
        END IF;
      END IF;
    END IF;
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- ignore, this means that the castorFile did not exist.
  -- There is thus no way to find out whether to remove the
  -- file from the nameserver. For safety, we thus keep it
  NULL;
WHEN LockError THEN
  -- ignore, somebody else is dealing with this castorFile, 
  NULL;
END;
/
/*******************************************************************
 *
 * @(#)RCSfile: oraclePerm.sql,v  Revision: 1.655  Date: 2009/03/26 14:11:58  Author: itglp 
 *
 * PL/SQL code for permission and B/W list handling
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* Process the adminList provided by the user in oracleCommon.schema */
DECLARE
  adminUserId NUMBER;
  adminGroupId NUMBER;
  ind NUMBER;
  errmsg VARCHAR(2048);
BEGIN
  -- If the adminList is empty do nothing
  IF '&adminList' IS NULL THEN
    RETURN;
  END IF;
  -- Loop over the adminList
  FOR admin IN (SELECT column_value AS s
                  FROM TABLE(strTokenizer('&adminList',' '))) LOOP
    BEGIN
      ind := INSTR(admin.s, ':');
      IF ind = 0 THEN
        errMsg := 'Invalid <userid>:<groupid> ' || admin.s || ', ignoring';
        RAISE INVALID_NUMBER;
      END IF;
      errMsg := 'Invalid userid ' || SUBSTR(admin.s, 1, ind - 1) || ', ignoring';
      adminUserId := TO_NUMBER(SUBSTR(admin.s, 1, ind - 1));
      errMsg := 'Invalid groupid ' || SUBSTR(admin.s, ind) || ', ignoring';
      adminGroupId := TO_NUMBER(SUBSTR(admin.s, ind+1));
      INSERT INTO AdminUsers VALUES (adminUserId, adminGroupId);
    EXCEPTION WHEN INVALID_NUMBER THEN
      dbms_output.put_line(errMsg);
    END;
  END LOOP;
END;
/


/* PL/SQL method implementing checkPermission
 * The return value can be
 *   0 if access is granted
 *   1 if access denied
 */
CREATE OR REPLACE FUNCTION checkPermission(reqSvcClass IN VARCHAR2,
                                           reqEuid IN NUMBER,
                                           reqEgid IN NUMBER,
                                           reqTypeI IN NUMBER)
RETURN NUMBER AS
  res NUMBER;
  c NUMBER;
BEGIN
  -- Skip access control checks for admin/internal users
  SELECT count(*) INTO c FROM AdminUsers 
   WHERE egid = reqEgid
     AND (euid = reqEuid OR euid IS NULL);
  IF c > 0 THEN
    -- Admin access, just proceed
    RETURN 0;
  END IF;
  -- Perform the check
  SELECT count(*) INTO c
    FROM WhiteList
   WHERE (svcClass = reqSvcClass OR svcClass IS NULL
          OR (length(reqSvcClass) IS NULL AND svcClass = 'default'))
     AND (egid = reqEgid OR egid IS NULL)
     AND (euid = reqEuid OR euid IS NULL)
     AND (reqType = reqTypeI OR reqType IS NULL);
  IF c = 0 THEN
    -- Not found in White list -> no access
    RETURN 1;
  ELSE
    SELECT count(*) INTO c
      FROM BlackList
     WHERE (svcClass = reqSvcClass OR svcClass IS NULL
            OR (length(reqSvcClass) IS NULL AND svcClass = 'default'))
       AND (egid = reqEgid OR egid IS NULL)
       AND (euid = reqEuid OR euid IS NULL)
       AND (reqType = reqTypeI OR reqType IS NULL);
    IF c = 0 THEN
      -- Not Found in Black list -> access
      RETURN 0;
    ELSE
      -- Found in Black list -> no access
      RETURN 1;
    END IF;
  END IF;
END;
/


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
  -- defines a privilege
  TYPE Privilege IS RECORD (
    svcClass VARCHAR2(2048),
    euid NUMBER,
    egid NUMBER,
    reqType NUMBER);
  -- defines a privilege, plus a "direction"
  TYPE PrivilegeExt IS RECORD (
    svcClass VARCHAR2(2048),
    euid NUMBER,
    egid NUMBER,
    reqType NUMBER,
    isGranted NUMBER);
  -- a cursor of privileges
  TYPE PrivilegeExt_Cur IS REF CURSOR RETURN PrivilegeExt;
  -- Intersection of 2 privileges
  -- raises -20109, "Empty privilege" in case the intersection is empty
  FUNCTION intersection(p1 IN Privilege, p2 IN Privilege) RETURN Privilege;
  -- Does one privilege P1 contain another one P2 ?
  FUNCTION contains(p1 Privilege, p2 Privilege) RETURN Boolean;
  -- Intersection of a privilege P with the WhiteList
  -- The result is stored in the temporary table removePrivilegeTmpTable
  -- that is cleaned up when the procedure starts
  PROCEDURE intersectionWithWhiteList(p Privilege);
  -- Difference between privilege P1 and privilege P2
  -- raises -20108, "Invalid privilege intersection" in case the difference
  -- can not be computed
  -- raises -20109, "Empty privilege" in case the difference is empty
  FUNCTION diff(P1 Privilege, P2 Privilege) RETURN Privilege;
  -- remove privilege P from list L
  PROCEDURE removePrivilegeFromBlackList(p Privilege);
  -- Add privilege P to WhiteList
  PROCEDURE addPrivilegeToWL(p Privilege);
  -- Add privilege P to BlackList
  PROCEDURE addPrivilegeToBL(p Privilege);
  -- cleanup BlackList after privileges were removed from the whitelist
  PROCEDURE cleanupBL;
  -- Add privilege P
  PROCEDURE addPrivilege(P Privilege);
  -- Remove privilege P
  PROCEDURE removePrivilege(P Privilege);
  -- Add privilege(s)
  PROCEDURE addPrivilege(svcClassName VARCHAR2, euid NUMBER, egid NUMBER, reqType NUMBER);
  -- Remove privilege(S)
  PROCEDURE removePrivilege(svcClassName VARCHAR2, euid NUMBER, egid NUMBER, reqType NUMBER);
  -- List privilege(s)
  PROCEDURE listPrivileges(svcClassName IN VARCHAR2, ieuid IN NUMBER,
                           iegid IN NUMBER, ireqType IN NUMBER,
                           plist OUT PrivilegeExt_Cur);
END castorBW;
/

CREATE OR REPLACE PACKAGE BODY castorBW AS

  -- Intersection of 2 privileges
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

  -- Does one privilege P1 contain another one P2 ?
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

  -- Intersection of a privilege P with the WhiteList
  -- The result is stored in the temporary table removePrivilegeTmpTable
  PROCEDURE intersectionWithWhiteList(p Privilege) AS
    wlr Privilege;
    tmp Privilege;
    empty_privilege EXCEPTION;
    PRAGMA EXCEPTION_INIT(empty_privilege, -20109);
  BEGIN
    DELETE FROM RemovePrivilegeTmpTable;
    FOR r IN (SELECT * FROM WhiteList) LOOP
      BEGIN
        wlr.svcClass := r.svcClass;
        wlr.euid := r.euid;
        wlr.egid := r.egid;
        wlr.reqType := r.reqType;
        tmp := intersection(wlr, p);
        INSERT INTO RemovePrivilegeTmpTable
        VALUES (tmp.svcClass, tmp.euid, tmp.egid, tmp.reqType);
      EXCEPTION WHEN empty_privilege THEN
        NULL;
      END;
    END LOOP;
  END;

  -- Difference between privilege P1 and privilege P2
  FUNCTION diff(P1 Privilege, P2 Privilege) RETURN Privilege AS
    empty_privilege EXCEPTION;
    PRAGMA EXCEPTION_INIT(empty_privilege, -20109);
    unused Privilege;
  BEGIN
    IF contains(P1, P2) THEN
      IF (P1.euid = P2.euid OR (P1.euid IS NULL AND P2.euid IS NULL)) AND
         (P1.egid = P2.egid OR (P1.egid IS NULL AND P2.egid IS NULL)) AND
         (P1.svcClass = P2.svcClass OR (P1.svcClass IS NULL AND P2.svcClass IS NULL)) AND
         (P1.reqType = P2.reqType OR (P1.reqType IS NULL AND P2.reqType IS NULL)) THEN
        raise_application_error(-20109, 'Empty privilege');
      ELSE
        raise_application_error(-20108, 'Invalid privilege intersection');
      END IF;
    ELSIF contains(P2, P1) THEN
      raise_application_error(-20109, 'Empty privilege');
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

  -- remove privilege P from list L
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
         WHERE nvl(svcClass, -1) = nvl(r.svcClass, -1) AND
               nvl(euid, -1) = nvl(r.euid, -1) AND
               nvl(egid, -1) = nvl(r.egid, -1) AND
               nvl(reqType, -1) = nvl(r.reqType, -1);
      END;
    END LOOP;
  END;

  -- Add privilege P to list L :
  PROCEDURE addPrivilegeToWL(p Privilege) AS
    wlr Privilege;
    extended boolean := FALSE;
    ret NUMBER;
  BEGIN
    -- check if the service class exists
    ret := checkForValidSvcClass(p.svcClass, 1, 1);

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

  -- Add privilege P to list L :
  PROCEDURE addPrivilegeToBL(p Privilege) AS
    blr Privilege;
    extended boolean := FALSE;
    ret NUMBER;
  BEGIN
    -- check if the service class exists
    ret := checkForValidSvcClass(p.svcClass, 1, 1);

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
      SELECT COUNT(*) INTO c FROM RemovePrivilegeTmpTable;
      IF c = 0 THEN
        -- we can safely drop this line
        DELETE FROM BlackList
         WHERE nvl(svcClass, -1) = nvl(r.svcClass, -1) AND
               nvl(euid, -1) = nvl(r.euid, -1) AND
               nvl(egid, -1) = nvl(r.egid, -1) AND
               nvl(reqType, -1) = nvl(r.reqType, -1);
      END IF;
    END LOOP;
  END;

  -- Add privilege P
  PROCEDURE addPrivilege(P Privilege) AS
  BEGIN
    removePrivilegeFromBlackList(P);
    addPrivilegeToWL(P);
  END;

  -- Remove privilege P
  PROCEDURE removePrivilege(P Privilege) AS
    c NUMBER;
    wlr Privilege;
  BEGIN
    -- Check first whether there is something to remove
    intersectionWithWhiteList(P);
    SELECT COUNT(*) INTO c FROM RemovePrivilegeTmpTable;
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
    FOR q IN (SELECT * FROM RemovePrivilegeTmpTable) LOOP
      wlr.svcClass := q.svcClass;
      wlr.euid := q.euid;
      wlr.egid := q.egid;
      wlr.reqType := q.reqType;
      addPrivilegeToBL(wlr);
    END LOOP;
  END;

  -- Add privilege
  PROCEDURE addPrivilege(svcClassName VARCHAR2, euid NUMBER, egid NUMBER, reqType NUMBER) AS
    p castorBW.Privilege;
  BEGIN
    p.svcClass := svcClassName;
    p.euid := euid;
    p.egid := egid;
    p.reqType := reqType;
    /* This line is a deprecated work around the issue of having changed the magic number of
     * DiskPoolQuery status from 103 to 195. It should be dropped as soon as all clients
     * are 2.1.10-1 or newer */
    IF p.reqType = 103 THEN p.reqType := 195; END IF; -- DiskPoolQuery fix
    addPrivilege(p);
  END;

  -- Remove privilege
  PROCEDURE removePrivilege(svcClassName VARCHAR2, euid NUMBER, egid NUMBER, reqType NUMBER) AS
    p castorBW.Privilege;
  BEGIN
    p.svcClass := svcClassName;
    p.euid := euid;
    p.egid := egid;
    p.reqType := reqType;
    /* This line is a deprecated work around the issue of having changed the magic number of
     * DiskPoolQuery status from 103 to 195. It should be dropped as soon as all clients
     * are 2.1.10-1 or newer */
    IF p.reqType = 103 THEN p.reqType := 195; END IF; -- DiskPoolQuery fix
    removePrivilege(p);
  END;

  -- List privileges
  PROCEDURE listPrivileges(svcClassName IN VARCHAR2, ieuid IN NUMBER,
                           iegid IN NUMBER, ireqType IN NUMBER,
                           plist OUT PrivilegeExt_Cur) AS
    ireqTypeFixed NUMBER;
  BEGIN
    /* ireqTypeFixed is a deprecated work around the issue of having changed the magic number of
     * DiskPoolQuery status from 103 to 195. It should be dropped as soon as all clients
     * are 2.1.10-1 or newer */
    ireqTypeFixed := ireqType;
    IF ireqTypeFixed = 103 THEN ireqTypeFixed := 195; END IF; -- DiskPoolQuery fix
    OPEN plist FOR
      SELECT decode(svcClass, NULL, '*', '*', '''*''', svcClass),
             euid, egid, reqType, 1
        FROM WhiteList
       WHERE (WhiteList.svcClass = svcClassName OR WhiteList.svcClass IS  NULL OR svcClassName IS NULL)
         AND (WhiteList.euid = ieuid OR WhiteList.euid IS NULL OR ieuid = -1)
         AND (WhiteList.egid = iegid OR WhiteList.egid IS NULL OR iegid = -1)
         AND (WhiteList.reqType = ireqTypeFixed OR WhiteList.reqType IS NULL OR ireqTypeFixed = 0)
    UNION
      SELECT decode(svcClass, NULL, '*', '*', '''*''', svcClass),
             euid, egid, reqType, 0
        FROM BlackList
       WHERE (BlackList.svcClass = svcClassName OR BlackList.svcClass IS  NULL OR svcClassName IS NULL)
         AND (BlackList.euid = ieuid OR BlackList.euid IS NULL OR ieuid = -1)
         AND (BlackList.egid = iegid OR BlackList.egid IS NULL OR iegid = -1)
         AND (BlackList.reqType = ireqTypeFixed OR BlackList.reqType IS NULL OR ireqTypeFixed = 0);
  END;

END castorBW;
/

/*******************************************************************
 *
 * @(#)RCSfile: oracleStager.sql,v  Revision: 1.752  Date: 2009/08/17 15:08:33  Author: sponcec3 
 *
 * PL/SQL code for the stager and resource monitoring
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* PL/SQL declaration for the castor package */
CREATE OR REPLACE PACKAGE castor AS
  TYPE DiskCopyCore IS RECORD (
    id INTEGER,
    path VARCHAR2(2048),
    status NUMBER,
    fsWeight NUMBER,
    mountPoint VARCHAR2(2048),
    diskServer VARCHAR2(2048));
  TYPE DiskCopy_Cur IS REF CURSOR RETURN DiskCopyCore;
  TYPE FailedMigrationJob IS RECORD (
    id NUMBER,
    errorCode NUMBER,
    nbRetry NUMBER);
  TYPE FailedMigrationJob_Cur IS REF CURSOR RETURN FailedMigrationJob;
  TYPE FailedRecallJob IS RECORD (
    id NUMBER,
    errorCode NUMBER,
    nbRetry NUMBER);
  TYPE FailedRecallJob_Cur IS REF CURSOR RETURN FailedRecallJob;
  TYPE Segment_Rec IS RECORD (
    fseq NUMBER,
    offset INTEGER,
    bytes_in INTEGER,
    bytes_out INTEGER,
    host_bytes INTEGER,
    segmCksumAlgorithm VARCHAR2(2048),
    segmCksum NUMBER,
    errMsgTxt VARCHAR2(2048),
    errorCode NUMBER,
    severity NUMBER,
    blockId0 INTEGER,
    blockId1 INTEGER,
    blockId2 INTEGER,
    blockId3 INTEGER,
    creationTime INTEGER,
    id INTEGER,
    tape INTEGER,
    copy INTEGER,
    status INTEGER,
    priority INTEGER);
  TYPE Segment_Cur IS REF CURSOR RETURN Segment_Rec;
  TYPE "strList" IS TABLE OF VARCHAR2(2048) index BY binary_integer;
  TYPE "cnumList" IS TABLE OF NUMBER index BY binary_integer;
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
    lastKnownFileName VARCHAR2(2048),
    creationTime INTEGER,
    svcClass VARCHAR2(2048),
    lastAccessTime INTEGER,
    hwStatus INTEGER);
  TYPE QueryLine_Cur IS REF CURSOR RETURN QueryLine;
  TYPE FileList IS RECORD (
    fileId NUMBER,
    nsHost VARCHAR2(2048));
  TYPE FileList_Cur IS REF CURSOR RETURN FileList;
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
  TYPE UUIDRecord IS RECORD (uuid VARCHAR(2048));
  TYPE UUIDRecord_Cur IS REF CURSOR RETURN UUIDRecord;
  TYPE UUIDPairRecord IS RECORD (uuid1 VARCHAR(2048), uuid2 VARCHAR(2048));
  TYPE UUIDPairRecord_Cur IS REF CURSOR RETURN UUIDPairRecord;
  TYPE TransferRecord IS RECORD (subreId VARCHAR(2048), resId VARCHAR(2048), fileId NUMBER, nsHost VARCHAR2(2048));
  TYPE TransferRecord_Cur IS REF CURSOR RETURN TransferRecord;
  TYPE DiskServerName IS RECORD (diskServer VARCHAR(2048));
  TYPE DiskServerList_Cur IS REF CURSOR RETURN DiskServerName;
  TYPE FileEntry IS RECORD (
    fileid INTEGER,
    nshost VARCHAR2(2048));
  TYPE FileEntry_Cur IS REF CURSOR RETURN FileEntry;
  TYPE TapeAccessPriority IS RECORD (
    euid INTEGER,
    egid INTEGER,
    priority INTEGER);
  TYPE TapeAccessPriority_Cur IS REF CURSOR RETURN TapeAccessPriority;
  TYPE StreamReport IS RECORD (
   diskserver VARCHAR2(2048),
   mountPoint VARCHAR2(2048));
  TYPE StreamReport_Cur IS REF CURSOR RETURN StreamReport;  
  TYPE FileResult IS RECORD (
   fileid INTEGER,
   nshost VARCHAR2(2048),
   errorcode INTEGER,
   errormessage VARCHAR2(2048));
  TYPE FileResult_Cur IS REF CURSOR RETURN FileResult;  
END castor;
/

/* Used to create a row in FileSystemsToCheck
   whenever a new FileSystem is created */
CREATE OR REPLACE TRIGGER tr_FileSystem_Insert
BEFORE INSERT ON FileSystem
FOR EACH ROW
BEGIN
  INSERT INTO FileSystemsToCheck (FileSystem, ToBeChecked) VALUES (:new.id, 0);
END;
/

/* Used to delete rows in FileSystemsToCheck
   whenever a FileSystem is deleted */
CREATE OR REPLACE TRIGGER tr_FileSystem_Delete
BEFORE DELETE ON FileSystem
FOR EACH ROW
BEGIN
  DELETE FROM FileSystemsToCheck WHERE FileSystem = :old.id;
END;
/

/* Checks consistency of DiskCopies when a FileSystem comes  	 
 * back in production after a period spent in a DRAINING or a 	 
 * DISABLED status. 	 
 * Current checks/fixes include : 	 
 *   - Canceling recalls for files that are STAGED or CANBEMIGR 	 
 *     on the fileSystem that comes back. (Scheduled for bulk 	 
 *     operation) 	 
 *   - Dealing with files that are STAGEOUT on the fileSystem 	 
 *     coming back but already exist on another one 	 
 */ 	 
CREATE OR REPLACE PROCEDURE checkFSBackInProd(fsId NUMBER) AS 	 
   srIds "numList"; 	 
BEGIN 	 
  -- Flag the filesystem for processing in a bulk operation later. 	 
  -- We need to do this because some operations are database intensive 	 
  -- and therefore it is often better to process several filesystems 	 
  -- simultaneous with one query as opposed to one by one. Especially 	 
  -- where full table scans are involved. 	 
  UPDATE FileSystemsToCheck SET toBeChecked = 1 	 
   WHERE fileSystem = fsId; 	 
  -- Look for files that are STAGEOUT on the filesystem coming back to life 	 
  -- but already STAGED/CANBEMIGR/WAITTAPERECALL/WAITFS/STAGEOUT/ 	 
  -- WAITFS_SCHEDULING somewhere else 	 
  FOR cf IN (SELECT UNIQUE d.castorfile, d.id 	 
               FROM DiskCopy d, DiskCopy e 	 
              WHERE d.castorfile = e.castorfile 	 
                AND d.fileSystem = fsId 	 
                AND e.fileSystem != fsId 	 
                AND d.status = 6 -- STAGEOUT 	 
                AND e.status IN (0, 10, 2, 5, 6, 11)) LOOP -- STAGED/CANBEMIGR/WAITTAPERECALL/WAITFS/STAGEOUT/WAITFS_SCHEDULING 	 
    -- Invalidate the DiskCopy 	 
    UPDATE DiskCopy 	 
       SET status = 7  -- INVALID 	 
     WHERE id = cf.id;
  END LOOP; 	 
END; 	 
/

/* PL/SQL method implementing bulkCheckFSBackInProd for processing
 * filesystems in one bulk operation to optimise database performance
 */
CREATE OR REPLACE PROCEDURE bulkCheckFSBackInProd AS
  fsIds "numList";
BEGIN
  -- Extract a list of filesystems which have been scheduled to be
  -- checked in a bulk operation on the database.
  UPDATE FileSystemsToCheck SET toBeChecked = 0
   WHERE toBeChecked = 1
  RETURNING fileSystem BULK COLLECT INTO fsIds;
  -- Nothing found, return
  IF fsIds.COUNT = 0 THEN
    RETURN;
  END IF;
  -- Look for recalls concerning files that are STAGED or CANBEMIGR
  -- on all filesystems scheduled to be checked, and restart their
  -- subrequests (reconsidering the recall source).
  FOR cf IN (SELECT /*+ USE_NL(E D)
                     INDEX_RS_ASC(E I_DiskCopy_Status)
                     INDEX_RS_ASC(D I_DiskCopy_CastorFile) */
                    UNIQUE D.castorfile, E.id
               FROM DiskCopy D, DiskCopy E
              WHERE D.castorfile = E.castorfile
                AND D.fileSystem IN
                  (SELECT /*+ CARDINALITY(fsidTable 5) */ *
                     FROM TABLE(fsIds) fsidTable)
                AND D.status IN (0, 10)
                AND E.status = 2) LOOP
    -- Cancel recall and restart subrequests
    cancelRecall(cf.castorfile, cf.id, 1); -- RESTART
  END LOOP;
END;
/


/* SQL statement for the update trigger on the FileSystem table */
CREATE OR REPLACE TRIGGER tr_FileSystem_Update
BEFORE UPDATE OF status ON FileSystem
FOR EACH ROW WHEN (old.status != new.status)
BEGIN
  -- If the filesystem is coming back into PRODUCTION, initiate a consistency
  -- check for the diskcopies which reside on the filesystem.
  IF :old.status IN (1, 2) AND  -- DRAINING, DISABLED
     :new.status = 0 THEN       -- PRODUCTION
    checkFsBackInProd(:old.id);
  END IF;
  -- Cancel any ongoing draining operations if the filesystem is not in a
  -- DRAINING state
  IF :new.status != 1 THEN  -- DRAINING
    UPDATE DrainingFileSystem
       SET status = 3  -- INTERRUPTED
     WHERE fileSystem = :new.id
       AND status IN (0, 1, 2, 7);  -- CREATED, INITIALIZING, RUNNING, RESTART
  END IF;
END;
/


/* SQL statement for the update trigger on the DiskServer table */
CREATE OR REPLACE TRIGGER tr_DiskServer_Update
BEFORE UPDATE OF status ON DiskServer
FOR EACH ROW WHEN (old.status != new.status)
BEGIN
  -- If the diskserver is coming back into PRODUCTION, initiate a consistency
  -- check for all the diskcopies on its associated filesystems which are in
  -- a PRODUCTION.
  IF :old.status IN (1, 2) AND  -- DRAINING, DISABLED
     :new.status = 0 THEN       -- PRODUCTION
    FOR fs IN (SELECT id FROM FileSystem
                WHERE diskServer = :old.id
                  AND status = 0)  -- PRODUCTION
    LOOP
      checkFsBackInProd(fs.id);
    END LOOP;
  END IF;
  -- Cancel all draining operations if the diskserver is disabled.
  IF :new.status = 2 THEN  -- DISABLED
    UPDATE DrainingFileSystem
       SET status = 3  -- INTERRUPTED
     WHERE fileSystem IN
       (SELECT FileSystem.id FROM FileSystem
         WHERE FileSystem.diskServer = :new.id)
       AND status IN (0, 1, 2, 7);  -- CREATED, INITIALIZING, RUNNING, RESTART
  END IF;
  -- If the diskserver is in PRODUCTION cancel the draining operation of 
  -- filesystems not in DRAINING.
  IF :new.status = 0 THEN  -- PRODUCTION
    UPDATE DrainingFileSystem
       SET status = 3  -- INTERRUPTED
     WHERE fileSystem IN
       (SELECT FileSystem.id FROM FileSystem
         WHERE FileSystem.diskServer = :new.ID
           AND FileSystem.status != 1)  -- DRAINING
       AND status IN (0, 1, 2, 7);  -- CREATED, INITIALIZING, RUNNING, RESTART
  END IF; 
END;
/


/* Trigger used to check if the maxReplicaNb has been exceeded
 * after a diskcopy has changed its status to STAGED or CANBEMIGR
 */
CREATE OR REPLACE TRIGGER tr_DiskCopy_Stmt_Online
AFTER UPDATE OF STATUS ON DISKCOPY
DECLARE
  maxReplicaNb NUMBER;
  unused NUMBER;
  nbFiles NUMBER;
BEGIN
  -- Loop over the diskcopies to be processed
  FOR a IN (SELECT * FROM TooManyReplicasHelper)
  LOOP
    -- Lock the castorfile. This shouldn't be necessary as the procedure that
    -- caused the trigger to be executed should already have the lock.
    -- Nevertheless, we make sure!
    SELECT id INTO unused FROM CastorFile
     WHERE id = a.castorfile FOR UPDATE;
    -- Get the max replica number of the service class
    SELECT maxReplicaNb INTO maxReplicaNb
      FROM SvcClass WHERE id = a.svcclass;
    -- Produce a list of diskcopies to invalidate should too many replicas be
    -- online.
    FOR b IN (SELECT id FROM (
                SELECT rownum ind, id FROM (
                  SELECT DiskCopy.id
                    FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass,
                         DiskServer
                   WHERE DiskCopy.filesystem = FileSystem.id
                     AND FileSystem.diskpool = DiskPool2SvcClass.parent
                     AND FileSystem.diskserver = DiskServer.id
                     AND DiskPool2SvcClass.child = SvcClass.id
                     AND DiskCopy.castorfile = a.castorfile
                     AND DiskCopy.status IN (0, 10)  -- STAGED, CANBEMIGR
                     AND SvcClass.id = a.svcclass
                   -- Select DISABLED or DRAINING hardware first
                   ORDER BY decode(FileSystem.status, 0,
                            decode(DiskServer.status, 0, 0, 1), 1) ASC,
                            DiskCopy.gcWeight DESC))
               WHERE ind > maxReplicaNb)
    LOOP
      -- Sanity check, make sure that the last copy is never dropped!
      SELECT count(*) INTO nbFiles
        FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass, DiskServer
       WHERE DiskCopy.filesystem = FileSystem.id
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND FileSystem.diskserver = DiskServer.id
         AND DiskPool2SvcClass.child = SvcClass.id
         AND DiskCopy.castorfile = a.castorfile
         AND DiskCopy.status IN (0, 10)  -- STAGED, CANBEMIGR
         AND SvcClass.id = a.svcclass;
      IF nbFiles = 1 THEN
        EXIT;  -- Last file, so exit the loop
      END IF;
      -- Invalidate the diskcopy
      UPDATE DiskCopy
         SET status = 7,  -- INVALID
             gcType = 2   -- Too many replicas
       WHERE id = b.id;
    END LOOP;
  END LOOP;
  -- cleanup the table so that we do not accumulate lines. This would trigger
  -- a n^2 behavior until the next commit.
  DELETE FROM TooManyReplicasHelper;
END;
/


/* Trigger used to provide input to the statement level trigger 
 * defined above
 */
CREATE OR REPLACE TRIGGER tr_DiskCopy_Online
AFTER UPDATE OF status ON DiskCopy
FOR EACH ROW
WHEN ((old.status != 10) AND    -- !CANBEMIGR -> {STAGED, CANBEMIGR}
      (new.status = 0 OR new.status = 10))     
DECLARE
  svcId  NUMBER;
  unused NUMBER;
  -- Trap `ORA-00001: unique constraint violated` errors
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
BEGIN
  -- Insert the information about the diskcopy being processed into
  -- the TooManyReplicasHelper. This information will be used later
  -- on the DiskCopy AFTER UPDATE statement level trigger. We cannot
  -- do the work of that trigger here as it would result in
  -- `ORA-04091: table is mutating, trigger/function` errors
  BEGIN
    SELECT SvcClass.id INTO svcId
      FROM FileSystem, DiskPool2SvcClass, SvcClass
     WHERE FileSystem.diskpool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = SvcClass.id
       AND FileSystem.id = :new.filesystem;
  EXCEPTION WHEN TOO_MANY_ROWS THEN
    -- The filesystem belongs to multiple service classes which is not
    -- supported by the replica management trigger.
    RETURN;
  END;
  -- Insert an entry into the TooManyReplicasHelper table.
  BEGIN
    INSERT INTO TooManyReplicasHelper
    VALUES (svcId, :new.castorfile);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    RETURN;  -- Entry already exists!
  END;
END;
/


/***************************************/
/* Some triggers to prevent dead locks */
/***************************************/

CREATE OR REPLACE TRIGGER tr_Tape_Insert
  BEFORE INSERT ON Tape
FOR EACH ROW
/**
 * Converts an inserted lastVdqmPingTime of NULL to the current time.
 */
BEGIN
  IF :NEW.lastVdqmPingTime IS NULL THEN
    :NEW.lastVdqmPingTime := getTime();
  END IF;
END;
/

/* PL/SQL method to get the next SubRequest to do according to the given service */
CREATE OR REPLACE PROCEDURE subRequestToDo(service IN VARCHAR2,
                                           srId OUT INTEGER, srRetryCounter OUT INTEGER, srFileName OUT VARCHAR2,
                                           srProtocol OUT VARCHAR2, srXsize OUT INTEGER,
                                           srModeBits OUT INTEGER, srFlags OUT INTEGER,
                                           srSubReqId OUT VARCHAR2, srAnswered OUT INTEGER, srReqType OUT INTEGER,
                                           rId OUT INTEGER, rFlags OUT INTEGER, rUsername OUT VARCHAR2, rEuid OUT INTEGER,
                                           rEgid OUT INTEGER, rMask OUT INTEGER, rPid OUT INTEGER, rMachine OUT VARCHAR2,
                                           rSvcClassName OUT VARCHAR2, rUserTag OUT VARCHAR2, rReqId OUT VARCHAR2,
                                           rCreationTime OUT INTEGER, rLastModificationTime OUT INTEGER,
                                           rRepackVid OUT VARCHAR2, rGCWeight OUT INTEGER,
                                           clIpAddress OUT INTEGER, clPort OUT INTEGER, clVersion OUT INTEGER) AS
  CURSOR SRcur IS SELECT /*+ FIRST_ROWS(10) INDEX(SR I_SubRequest_RT_CT_ID) */ SR.id
                    FROM SubRequest PARTITION (P_STATUS_0_1_2) SR
                   WHERE SR.svcHandler = service
                   ORDER BY SR.creationTime ASC;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  varSrId NUMBER;
  varRName VARCHAR2(100);
  varClientId NUMBER;
BEGIN
  OPEN SRcur;
  -- Loop on candidates until we can lock one
  LOOP
    -- Fetch next candidate
    FETCH SRcur INTO varSrId;
    EXIT WHEN SRcur%NOTFOUND;
    BEGIN
      -- Try to take a lock on the current candidate, and revalidate its status
      SELECT /*+ INDEX(SR PK_SubRequest_ID) */ id INTO varSrId
        FROM SubRequest PARTITION (P_STATUS_0_1_2) SR
       WHERE id = varSrId FOR UPDATE NOWAIT;
      -- Since we are here, we got the lock. We have our winner, let's update it
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_WAITSCHED, subReqId = nvl(subReqId, uuidGen())
       WHERE id = varSrId
      RETURNING id, retryCounter, fileName, protocol, xsize, modeBits, flags, subReqId,
        answered, reqType, request, (SELECT object FROM Type2Obj WHERE type = reqType) 
        INTO srId, srRetryCounter, srFileName, srProtocol, srXsize, srModeBits, srFlags, srSubReqId,
        srAnswered, srReqType, rId, varRName;
      -- XXX This could be done in a single EXECUTE IMMEDIATE statement, but to make it
      -- XXX efficient we implement a CASE construct. At a later time the FileRequests should
      -- XXX be merged in a single table (partitioned by reqType) to avoid the following block.
      CASE
        WHEN varRName = 'StagePrepareToPutRequest' THEN 
          SELECT flags, username, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, client
            INTO rFlags, rUsername, rEuid, rEgid, rMask, rPid, rMachine, rSvcClassName, rUserTag, rReqId, rCreationTime, rLastModificationTime, varClientId
            FROM StagePrepareToPutRequest WHERE id = rId;
        WHEN varRName = 'StagePrepareToGetRequest' THEN 
          SELECT flags, username, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, client
            INTO rFlags, rUsername, rEuid, rEgid, rMask, rPid, rMachine, rSvcClassName, rUserTag, rReqId, rCreationTime, rLastModificationTime, varClientId
            FROM StagePrepareToGetRequest WHERE id = rId;
        WHEN varRName = 'StagePrepareToUpdateRequest' THEN 
          SELECT flags, username, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, client
            INTO rFlags, rUsername, rEuid, rEgid, rMask, rPid, rMachine, rSvcClassName, rUserTag, rReqId, rCreationTime, rLastModificationTime, varClientId
            FROM StagePrepareToUpdateRequest WHERE id = rId;
        WHEN varRName = 'StageRepackRequest' THEN 
          SELECT flags, username, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, repackVid, client
            INTO rFlags, rUsername, rEuid, rEgid, rMask, rPid, rMachine, rSvcClassName, rUserTag, rReqId, rCreationTime, rLastModificationTime, rRepackVid, varClientId
            FROM StageRepackRequest WHERE id = rId;
        WHEN varRName = 'StagePutRequest' THEN 
          SELECT flags, username, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, client
            INTO rFlags, rUsername, rEuid, rEgid, rMask, rPid, rMachine, rSvcClassName, rUserTag, rReqId, rCreationTime, rLastModificationTime, varClientId
            FROM StagePutRequest WHERE id = rId;
        WHEN varRName = 'StageGetRequest' THEN 
          SELECT flags, username, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, client
            INTO rFlags, rUsername, rEuid, rEgid, rMask, rPid, rMachine, rSvcClassName, rUserTag, rReqId, rCreationTime, rLastModificationTime, varClientId
            FROM StageGetRequest WHERE id = rId;
        WHEN varRName = 'StageUpdateRequest' THEN 
          SELECT flags, username, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, client
            INTO rFlags, rUsername, rEuid, rEgid, rMask, rPid, rMachine, rSvcClassName, rUserTag, rReqId, rCreationTime, rLastModificationTime, varClientId
            FROM StageUpdateRequest WHERE id = rId;
        WHEN varRName = 'StagePutDoneRequest' THEN 
          SELECT flags, username, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, client
            INTO rFlags, rUsername, rEuid, rEgid, rMask, rPid, rMachine, rSvcClassName, rUserTag, rReqId, rCreationTime, rLastModificationTime, varClientId
            FROM StagePutDoneRequest WHERE id = rId;
        WHEN varRName = 'StageRmRequest' THEN 
          SELECT flags, username, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, client
            INTO rFlags, rUsername, rEuid, rEgid, rMask, rPid, rMachine, rSvcClassName, rUserTag, rReqId, rCreationTime, rLastModificationTime, varClientId
            FROM StageRmRequest WHERE id = rId;
        WHEN varRName = 'SetFileGCWeight' THEN 
          SELECT flags, username, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, weight, client
            INTO rFlags, rUsername, rEuid, rEgid, rMask, rPid, rMachine, rSvcClassName, rUserTag, rReqId, rCreationTime, rLastModificationTime, rGcWeight, varClientId
            FROM SetFileGCWeight WHERE id = rId;
      END CASE;
      SELECT ipAddress, port, version
        INTO clIpAddress, clPort, clVersion
        FROM Client WHERE id = varClientId;
      EXIT;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        -- Got to next candidate, this subrequest was processed already and its status changed
        NULL;
      WHEN SrLocked THEN
        -- Go to next candidate, this subrequest is being processed by another thread
        NULL;
    END;
  END LOOP;
  CLOSE SRcur;
END;
/

/* PL/SQL method to process bulk abort on a given get/prepareToGet request */
CREATE OR REPLACE PROCEDURE processAbortForGet(sr processBulkAbortFileReqsHelper%ROWTYPE) AS
  abortedSRstatus NUMBER;
BEGIN
  -- note the revalidation of the status and even of the existence of the subrequest
  -- as it may have changed before we got the lock on the Castorfile in processBulkAbortFileReqs
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ status
    INTO abortedSRstatus
    FROM SubRequest
   WHERE id = sr.srId;
  CASE
    WHEN abortedSRstatus = dconst.SUBREQUEST_START
      OR abortedSRstatus = dconst.SUBREQUEST_RESTART
      OR abortedSRstatus = dconst.SUBREQUEST_RETRY
      OR abortedSRstatus = dconst.SUBREQUEST_WAITSCHED
      OR abortedSRstatus = dconst.SUBREQUEST_WAITSUBREQ
      OR abortedSRstatus = dconst.SUBREQUEST_READY
      OR abortedSRstatus = dconst.SUBREQUEST_REPACK
      OR abortedSRstatus = dconst.SUBREQUEST_READYFORSCHED
      OR abortedSRstatus = dconst.SUBREQUEST_BEINGSCHED THEN
      -- standard case, we only have to fail the subrequest
      UPDATE SubRequest SET status = 7 WHERE id = sr.srId;
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 0, '');
    WHEN abortedSRstatus = dconst.SUBREQUEST_WAITTAPERECALL THEN
      -- recall case, let's see whether we can cancel the recall
      DECLARE
        segId INTEGER;
        unusedIds "numList";
      BEGIN
        -- Check whether we have any segment in SELECTED
        SELECT segment.id INTO segId
          FROM Segment, RecallJob
         WHERE RecallJob.castorfile = sr.cfId
           AND RecallJob.id = Segment.copy
           AND Segment.status = tconst.SEGMENT_SELECTED
           AND ROWNUM < 2;
        -- Something is running, so give up
        INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 16, 'Cannot abort ongoing recall'); -- EBUSY
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- Nothing running, we can cancel the recall  
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET status = 7 WHERE id = sr.srId;
        deleteRecallJobs(sr.cfId);
        UPDATE DiskCopy SET status = dconst.DISKCOPY_FAILED
         WHERE castorfile = sr.cfid AND status = dconst.DISKCOPY_WAITTAPERECALL;
        INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 0, '');
      END;
    WHEN abortedSRstatus = dconst.SUBREQUEST_FAILED
      OR abortedSRstatus = dconst.SUBREQUEST_FAILED_FINISHED
      OR abortedSRstatus = dconst.SUBREQUEST_FAILED_ANSWERING THEN
      -- subrequest has failed, nothing to abort
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 22, 'Cannot abort failed subRequest'); -- EINVAL
    WHEN abortedSRstatus = dconst.SUBREQUEST_FINISHED
      OR abortedSRstatus = dconst.SUBREQUEST_ARCHIVED THEN
      -- subrequest is over, nothing to abort
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 22, 'Cannot abort completed subRequest'); -- EINVAL
    ELSE
      -- unknown status !
      INSERT INTO ProcessBulkRequestHelper
      VALUES (sr.fileId, sr.nsHost, 1015, 'Found unknown status for request : ' || TO_CHAR(abortedSRstatus)); -- SEINTERNAL
  END CASE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- subRequest was deleted in the mean time !
  INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 2, 'Targeted SubRequest has just been deleted'); -- ENOENT
END;
/

/* PL/SQL method to process bulk abort on a given put/prepareToPut request */
CREATE OR REPLACE PROCEDURE processAbortForPut(sr processBulkAbortFileReqsHelper%ROWTYPE) AS
  abortedSRstatus NUMBER;
BEGIN
  -- note the revalidation of the status and even of the existence of the subrequest
  -- as it may have changed before we got the lock on the Castorfile in processBulkAbortFileReqs
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ status INTO abortedSRstatus FROM SubRequest WHERE id = sr.srId;
  CASE
    WHEN abortedSRstatus = dconst.SUBREQUEST_START
      OR abortedSRstatus = dconst.SUBREQUEST_RESTART
      OR abortedSRstatus = dconst.SUBREQUEST_RETRY
      OR abortedSRstatus = dconst.SUBREQUEST_WAITSCHED
      OR abortedSRstatus = dconst.SUBREQUEST_WAITSUBREQ
      OR abortedSRstatus = dconst.SUBREQUEST_READY
      OR abortedSRstatus = dconst.SUBREQUEST_REPACK
      OR abortedSRstatus = dconst.SUBREQUEST_READYFORSCHED
      OR abortedSRstatus = dconst.SUBREQUEST_BEINGSCHED THEN
      -- standard case, we only have to fail the subrequest
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_FAILED
       WHERE id = sr.srId;
      UPDATE DiskCopy SET status = dconst.DISKCOPY_FAILED
       WHERE castorfile = sr.cfid AND status IN (dconst.DISKCOPY_STAGEOUT,
                                                 dconst.DISKCOPY_WAITFS,
                                                 dconst.DISKCOPY_WAITFS_SCHEDULING);
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 0, '');
    WHEN abortedSRstatus = dconst.SUBREQUEST_FAILED
      OR abortedSRstatus = dconst.SUBREQUEST_FAILED_FINISHED
      OR abortedSRstatus = dconst.SUBREQUEST_FAILED_ANSWERING THEN
      -- subrequest has failed, nothing to abort
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 22, 'Cannot abort failed subRequest'); -- EINVAL
    WHEN abortedSRstatus = dconst.SUBREQUEST_FINISHED
      OR abortedSRstatus = dconst.SUBREQUEST_ARCHIVED THEN
      -- subrequest is over, nothing to abort
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 22, 'Cannot abort completed subRequest'); -- EINVAL
    ELSE
      -- unknown status !
      INSERT INTO ProcessBulkRequestHelper
      VALUES (sr.fileId, sr.nsHost, 1015, 'Found unknown status for request : ' || TO_CHAR(abortedSRstatus)); -- SEINTERNAL
  END CASE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- subRequest was deleted in the mean time !
  INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 2, 'Targeted SubRequest has just been deleted'); -- ENOENT
END;
/

/* PL/SQL method to process bulk abort on a given Repack request */
CREATE OR REPLACE PROCEDURE processBulkAbortForRepack(abortReqId IN INTEGER, origReqId IN INTEGER) AS
  abortedSRstatus NUMBER;
  srsToUpdate "numList";
  dcrecsToUpdate "numList";
  dcmigrsToUpdate "numList";
  nbItems NUMBER;
  nbItemsDone NUMBER := 0;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  unused NUMBER;
  firstOne BOOLEAN := TRUE;
  commitWork BOOLEAN := FALSE;
BEGIN
  -- Gather the list of subrequests to abort
  INSERT INTO ProcessBulkAbortFileReqsHelper (
    SELECT /*+ INDEX(Subrequest I_Subrequest_CastorFile)*/
           SubRequest.id, CastorFile.id, CastorFile.fileId, CastorFile.nsHost, SubRequest.subreqId
      FROM SubRequest, CastorFile
     WHERE SubRequest.castorFile = CastorFile.id
       AND request = origReqId
       AND status IN (dconst.SUBREQUEST_RESTART, dconst.SUBREQUEST_RETRY,
                      dconst.SUBREQUEST_WAITSUBREQ, dconst.SUBREQUEST_WAITTAPERECALL,
                      dconst.SUBREQUEST_REPACK));
  SELECT COUNT(*) INTO nbItems FROM processBulkAbortFileReqsHelper;
  -- handle aborts in bulk while avoiding deadlocks
  WHILE nbItems > 0 LOOP
    FOR sr IN (SELECT srId, cfId, fileId, nsHost, uuid FROM processBulkAbortFileReqsHelper) LOOP
      BEGIN
        IF firstOne THEN
          -- on the first item, we take a blocking lock as we are sure that we will not
          -- deadlock and we would like to process at least one item to not loop endlessly
          SELECT id INTO unused FROM CastorFile WHERE id = sr.cfId FOR UPDATE;
          firstOne := FALSE;
        ELSE
          -- on the other items, we go for a non blocking lock. If we get it, that's
          -- good and we process this extra subrequest within the same session. If
          -- we do not get the lock, then we close the session here and go for a new
          -- one. This will prevent dead locks while ensuring that a minimal number of
          -- commits is performed.
          SELECT id INTO unused FROM CastorFile WHERE id = sr.cfId FOR UPDATE NOWAIT;
        END IF;
        -- note the revalidation of the status and even of the existence of the subrequest
        -- as it may have changed before we got the lock on the Castorfile in processBulkAbortFileReqs
        SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ status
          INTO abortedSRstatus
          FROM SubRequest
         WHERE id = sr.srId;
        CASE
          WHEN abortedSRstatus = dconst.SUBREQUEST_RESTART
            OR abortedSRstatus = dconst.SUBREQUEST_RETRY
            OR abortedSRstatus = dconst.SUBREQUEST_WAITSUBREQ THEN
            -- easy case, we only have to fail the subrequest
            INSERT INTO ProcessRepackAbortHelperSR VALUES (sr.srId);
          WHEN abortedSRstatus = dconst.SUBREQUEST_WAITTAPERECALL THEN
            -- recall case, let's see whether we can cancel the recall
            DECLARE
              segId INTEGER;
              unusedIds "numList";
            BEGIN
              -- Check whether we have any segment in SELECTED
              SELECT segment.id INTO segId
                FROM Segment, RecallJob
               WHERE RecallJob.castorfile = sr.cfId
                 AND RecallJob.id = Segment.copy
                 AND Segment.status = tconst.SEGMENT_SELECTED
                 AND ROWNUM < 2;
              -- Something is running, so give up
              INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 16, 'Cannot abort ongoing recall'); -- EBUSY
            EXCEPTION WHEN NO_DATA_FOUND THEN
              -- Nothing running, we can cancel the recall  
              INSERT INTO ProcessRepackAbortHelperSR VALUES (sr.srId);
              deleteRecallJobs(sr.cfId);
              INSERT INTO ProcessRepackAbortHelperDCrec VALUES (sr.cfId);
            END;
          WHEN abortedSRstatus = dconst.SUBREQUEST_REPACK THEN
            -- stop the migration and put back the file to STAGED
            INSERT INTO ProcessRepackAbortHelperSR VALUES (sr.srId);
            deleteMigrationJobs(sr.cfId);
            INSERT INTO ProcessRepackAbortHelperDCmigr VALUES (sr.cfId);
        END CASE;
        DELETE FROM processBulkAbortFileReqsHelper WHERE srId = sr.srId;
        nbItemsDone := nbItemsDone + 1;
      EXCEPTION WHEN SrLocked THEN
        commitWork := TRUE;
      END;
      -- commit anyway from time to time, to avoid too long redo logs
      IF commitWork OR nbItemsDone >= 1000 THEN
        -- exit the current loop and restart a new one, in order to commit without getting invalid ROWID errors
        EXIT;
      END IF;
    END LOOP;
    -- do the bulk updates
    SELECT srId BULK COLLECT INTO srsToUpdate FROM ProcessRepackAbortHelperSR;
    FORALL i IN srsToUpdate.FIRST .. srsToUpdate.LAST
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET parent = NULL, diskCopy = NULL, lastModificationTime = getTime(),
             status = 9, errorCode = 1701, errorMessage='Aborted explicitely'
       WHERE id = srsToUpdate(i);
    SELECT cfId BULK COLLECT INTO dcrecsToUpdate FROM ProcessRepackAbortHelperDCrec;
    FORALL i IN dcrecsToUpdate.FIRST .. dcrecsToUpdate.LAST
      UPDATE DiskCopy SET status = dconst.DISKCOPY_FAILED
       WHERE castorfile = dcrecsToUpdate(i) AND status = dconst.DISKCOPY_WAITTAPERECALL;
    SELECT cfId BULK COLLECT INTO dcmigrsToUpdate FROM ProcessRepackAbortHelperDCmigr;
    FORALL i IN dcmigrsToUpdate.FIRST .. dcmigrsToUpdate.LAST
      UPDATE DiskCopy SET status = dconst.DISKCOPY_STAGED
       WHERE castorfile = dcmigrsToUpdate(i) AND status = dconst.DISKCOPY_CANBEMIGR;
    -- commit
    COMMIT;
    -- reset all counters
    nbItems := nbItems - nbItemsDone;
    nbItemsDone := 0;
    firstOne := TRUE;
    commitWork := FALSE;
  END LOOP;
END;
/

/* PL/SQL method to process bulk abort on files related requests */
CREATE OR REPLACE PROCEDURE processBulkAbortFileReqs
(abortReqId IN INTEGER, origReqId IN INTEGER,
 fileIds IN "numList", nsHosts IN strListTable, reqType IN NUMBER) AS
  nbItems NUMBER;
  nbItemsDone NUMBER := 0;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  unused NUMBER;
  firstOne BOOLEAN := TRUE;
  commitWork BOOLEAN := FALSE;
BEGIN
  -- Gather the list of subrequests to abort
  IF fileIds.count() = 0 THEN
    -- handle the case of an empty request, meaning that all files should be aborted
    INSERT INTO processBulkAbortFileReqsHelper (
      SELECT /*+ INDEX(Subrequest I_Subrequest_Request)*/
             SubRequest.id, CastorFile.id, CastorFile.fileId, CastorFile.nsHost, SubRequest.subreqId
        FROM SubRequest, CastorFile
       WHERE SubRequest.castorFile = CastorFile.id
         AND request = origReqId);
  ELSE
    -- handle the case of selective abort
    FOR i IN fileIds.FIRST .. fileIds.LAST LOOP
      DECLARE
        srId NUMBER;
        cfId NUMBER;
        srUuid VARCHAR(2048);
      BEGIN
        SELECT /*+ INDEX(Subrequest I_Subrequest_CastorFile)*/
               SubRequest.id, CastorFile.id, SubRequest.subreqId INTO srId, cfId, srUuid
          FROM SubRequest, CastorFile
         WHERE request = origReqId
           AND SubRequest.castorFile = CastorFile.id
           AND CastorFile.fileid = fileIds(i)
           AND CastorFile.nsHost = nsHosts(i);
        INSERT INTO processBulkAbortFileReqsHelper VALUES (srId, cfId, fileIds(i), nsHosts(i), srUuid);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- this fileid/nshost did not exist in the request, send an error back
        INSERT INTO ProcessBulkRequestHelper
        VALUES (fileIds(i), nsHosts(i), 2, 'No subRequest found for this fileId/nsHost'); -- ENOENT
      END;
    END LOOP;
  END IF;
  SELECT COUNT(*) INTO nbItems FROM processBulkAbortFileReqsHelper;
  -- handle aborts in bulk while avoiding deadlocks
  WHILE nbItems > 0 LOOP
    FOR sr IN (SELECT srId, cfId, fileId, nsHost, uuid FROM processBulkAbortFileReqsHelper) LOOP
      BEGIN
        IF firstOne THEN
          -- on the first item, we take a blocking lock as we are sure that we will not
          -- deadlock and we would like to process at least one item to not loop endlessly
          SELECT id INTO unused FROM CastorFile WHERE id = sr.cfId FOR UPDATE;
          firstOne := FALSE;
        ELSE
          -- on the other items, we go for a non blocking lock. If we get it, that's
          -- good and we process this extra subrequest within the same session. If
          -- we do not get the lock, then we close the session here and go for a new
          -- one. This will prevent dead locks while ensuring that a minimal number of
          -- commits is performed.
          SELECT id INTO unused FROM CastorFile WHERE id = sr.cfId FOR UPDATE NOWAIT;
        END IF;
        -- we got the lock on the Castorfile, we can handle the abort for this subrequest
        CASE reqType
          WHEN 1 THEN processAbortForGet(sr);
          WHEN 2 THEN processAbortForPut(sr);
        END CASE;
        DELETE FROM processBulkAbortFileReqsHelper WHERE srId = sr.srId;
        -- make the scheduler aware so that it can remove the transfer from the queues if needed
        INSERT INTO TransfersToAbort VALUES (sr.uuid);
        nbItemsDone := nbItemsDone + 1;
      EXCEPTION WHEN SrLocked THEN
        commitWork := TRUE;
      END;
      -- commit anyway from time to time, to avoid too long redo logs
      IF commitWork OR nbItemsDone >= 1000 THEN
        -- exit the current loop and restart a new one, in order to commit without getting invalid ROWID errors
        EXIT;
      END IF;
    END LOOP;
    -- commit
    COMMIT;
    -- wake up the scheduler so that it can remove the transfer from the queues
    DBMS_ALERT.SIGNAL('transfersToAbort', '');
    -- reset all counters
    nbItems := nbItems - nbItemsDone;
    nbItemsDone := 0;
    firstOne := TRUE;
    commitWork := FALSE;
  END LOOP;
END;
/

/* PL/SQL method to process bulk abort requests */
CREATE OR REPLACE PROCEDURE processBulkAbort(abortReqId IN INTEGER, rIpAddress OUT INTEGER,
                                             rport OUT INTEGER, rReqUuid OUT VARCHAR2) AS
  clientId NUMBER;
  reqType NUMBER;
  requestId NUMBER;
  abortedReqUuid VARCHAR(2048);
  fileIds "numList";
  nsHosts strListTable;
  ids "numList";
  nsHostName VARCHAR2(2048);
BEGIN
  -- get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', '');
  -- get request and client informations and drop them from the DB
  DELETE FROM StageAbortRequest WHERE id = abortReqId
    RETURNING reqId, parentUuid, client INTO rReqUuid, abortedReqUuid, clientId;
  DELETE FROM Client WHERE id = clientId
    RETURNING ipAddress, port INTO rIpAddress, rport;
  -- list fileids to process and drop them from the DB; override the
  -- nsHost in case it is defined in the configuration
  SELECT fileid, decode(nsHostName, '', nsHost, nsHostName), id
    BULK COLLECT INTO fileIds, nsHosts, ids
    FROM NsFileId WHERE request = abortReqId;
  FORALL i IN ids.FIRST .. ids.LAST DELETE FROM NsFileId WHERE id = ids(i);
  -- dispatch actual processing depending on request type
  BEGIN
    SELECT rType, id INTO reqType, requestId FROM
      (SELECT /*+ INDEX(StageGetRequest I_StageGetRequest_ReqId) */
              reqId, id, 1 as rtype from StageGetRequest UNION ALL
       SELECT /*+ INDEX(StagePrepareToGetRequest I_StagePTGRequest_ReqId) */
              reqId, id, 1 as rtype from StagePrepareToGetRequest UNION ALL
       SELECT /*+ INDEX(stagePutRequest I_stagePutRequest_ReqId) */
              reqId, id, 2 as rtype from StagePutRequest UNION ALL
       SELECT /*+ INDEX(StagePrepareToPutRequest I_StagePTPRequest_ReqId) */
              reqId, id, 2 as rtype from StagePrepareToPutRequest UNION ALL
       SELECT /*+ INDEX(StageRepackRequest I_RepackRequest_ReqId) */
              reqId, id, 3 as rtype from StageRepackRequest)
     WHERE reqId = abortedReqUuid;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- abort on non supported request type
    INSERT INTO ProcessBulkRequestHelper VALUES (0, '', 2, 'Request not found, or abort not supported for this request type'); -- ENOENT
    RETURN;
  END;
  IF reqType IN (1,2) THEN
    processBulkAbortFileReqs(abortReqId, requestId, fileIds, nsHosts, reqType);
  ELSE
    processBulkAbortForRepack(abortReqId, requestId);
  END IF;
END;
/

/* PL/SQL method to process bulk abort requests */
CREATE OR REPLACE PROCEDURE abortRepackRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   clientIP IN INTEGER,
   parentUUID IN VARCHAR2,
   rSubResults OUT castor.FileResult_Cur) AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  rIpAddress INTEGER;
  rport INTEGER;
  rReqUuid VARCHAR2(2048);
BEGIN
  -- mark the repack request as being aborted
  UPDATE StageRepackRequest SET status = tconst.REPACK_ABORTING WHERE reqId = parentUUID;
  COMMIT;
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  INSERT INTO StageAbortRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
  VALUES (0,userName,euid,egid,0,pid,machine,'','',uuidgen(),creationTime,creationTime,parentUUID,reqId,0,clientId);
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,0,0,0,clientId);
  -- process the abort
  processBulkAbort(reqId, rIpAddress, rport, rReqUuid);
  -- mark the repack request as ABORTED
  UPDATE StageRepackRequest SET status = tconst.REPACK_ABORTED WHERE reqId = parentUUID;
  -- return all results
  OPEN rSubResults FOR
    SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
END;
/

/* PL/SQL method to process bulk requests */
CREATE OR REPLACE PROCEDURE processBulkRequest(service IN VARCHAR2, requestId OUT INTEGER,
                                               rtype OUT INTEGER, rIpAddress OUT INTEGER,
                                               rport OUT INTEGER, rReqUuid OUT VARCHAR2,
                                               reuid OUT INTEGER, regid OUT INTEGER,
                                               freeParam OUT VARCHAR2,
                                               rSubResults OUT castor.FileResult_Cur) AS
  CURSOR Rcur IS SELECT /*+ FIRST_ROWS(10) */ id
                   FROM NewRequests
                  WHERE type IN (
                    SELECT type FROM Type2Obj
                     WHERE svcHandler = service
                       AND svcHandler IS NOT NULL);
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
BEGIN
  -- in case we do not find anything, rtype should be 0
  rType := 0;
  OPEN Rcur;
  -- Loop on candidates until we can lock one
  LOOP
    -- Fetch next candidate
    FETCH Rcur INTO requestId;
    EXIT WHEN Rcur%NOTFOUND;
    BEGIN
      -- Try to take a lock on the current candidate
      SELECT type INTO rType FROM NewRequests WHERE id = requestId FOR UPDATE NOWAIT;
      -- Since we are here, we got the lock. We have our winner,
      DELETE FROM NewRequests WHERE id = requestId;
      -- Clear the temporary table for subresults
      DELETE FROM ProcessBulkRequestHelper;
      -- dispatch actual processing depending on request type
      CASE rType
        WHEN 50 THEN -- Abort Request
          processBulkAbort(requestId, rIpAddress, rport, rReqUuid);
          reuid := -1;  -- not used
          regid := -1;  -- not used
      END CASE;
      -- open cursor on results
      OPEN rSubResults FOR
        SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
      -- and exit the loop
      EXIT;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        -- Got to next candidate, this request was processed already and disappeared
        NULL;
      WHEN SrLocked THEN
        -- Go to next candidate, this request is being processed by another thread
        NULL;
    END;
  END LOOP;
  CLOSE Rcur;
END;
/

/* PL/SQL method to handle a repack request */
CREATE OR REPLACE PROCEDURE handleRepackRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   clientIP IN INTEGER,
   reqVID IN VARCHAR2,
   rSubResults OUT castor.FileResult_Cur) AS
  varReqId INTEGER;
  clientId INTEGER;
  creationTime NUMBER;
  svcClassId INTEGER;
  varSubreqId INTEGER;
  cfId INTEGER;
  dcId INTEGER;
  tapeId INTEGER;
  tapeStatus INTEGER;
  repackProtocol VARCHAR2(2048);
  recallPolicy VARCHAR2(2048);
  result NUMBER;
  nsHostName VARCHAR2(2048);
  lastKnownFileName VARCHAR2(2048);
  priority INTEGER;
  unused INTEGER;
  firstCF boolean := True;
  nbFilesProcessed NUMBER := 0;
  nbFilesFailed NUMBER := 0;
  isOngoing boolean := False;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, 119);
  -- insertion time
  creationTime := getTime();
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,0,0,0,ids_seq.nextval)
  RETURNING id INTO clientId;
  -- insert the request itself
  INSERT INTO StageRepackRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, repackVid, id, svcClass, client,status)
  VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',uuidgen(),creationTime,creationTime,reqVID,ids_seq.nextval,svcClassId,clientId,tconst.REPACK_STARTING)
  RETURNING id INTO varReqId;
  -- Clear the temporary table for subresults
  DELETE FROM ProcessBulkRequestHelper;
  -- Check which protocol should be used for writing files to disk
  repackProtocol := getConfigOption('Repack', 'Protocol', 'rfio');
  -- name server host name
  nsHostName := getConfigOption('stager', 'nsHost', '');
  -- check and create the tape if needed. Its status depends on the presence of a recallerPolicy
  -- Note that selectTapeForRecall uses autonomous transaction so that we do not keep a lock
  SELECT recallerPolicy INTO recallPolicy FROM SvcClass WHERE id = svcClassId;
  IF recallPolicy IS NULL THEN
    tapeStatus := tconst.TAPE_PENDING;
  ELSE
    tapeStatus := tconst.TAPE_WAITPOLICY;
  END IF;
  tapeId := selectTapeForRecall(reqVID, tapeStatus);
  -- compute the priority of this request
  BEGIN
    SELECT priority INTO priority FROM PriorityMap
     WHERE euid = euid AND egid = egid AND ROWNUM < 2;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    priority := 0;
  END;
  -- Get the list of files to repack from the NS DB via DBLink and store them in memory
  -- in a temporary table. We do that so that we do not keep an open cursor for too long
  -- in the nameserver DB
  INSERT INTO RepackTapeSegments (SELECT s_fileid, blockid, fseq, segSize, copyno, fileclass
                                    FROM Cns_Seg_Metadata@remotens, Cns_File_Metadata@remotens
                                   WHERE vid = reqVID
                                     AND s_fileid = fileid
                                     AND s_status = '-'
                                     AND status != 'D');
  FOR segment IN (SELECT * FROM RepackTapeSegments) LOOP
    -- Commit from time to time
    IF nbFilesProcessed = 1000 THEN
      COMMIT;
      firstCF := TRUE;
      nbFilesProcessed := 0;
    END IF;
    -- lastKnownFileName we will have in the DB
    lastKnownFileName := CONCAT('Repack_', TO_CHAR(segment.s_fileid));
    -- find the Castorfile (and take a lock on it)
    DECLARE
      locked EXCEPTION;
      PRAGMA EXCEPTION_INIT (locked, -54);
    BEGIN
      -- This may raise a SrLocked exception as we do not want to wait for locks (except on first file).
      -- In such a case, we commit what we've done so far and retry this file, this time waiting for the lock.
      -- The reason for such a complex code is to avoid commiting each file separately, as it would be
      -- too heavy. On the other hand, we still need to avoid dead locks.
      -- Note that we pass 0 for the subrequest id, thus the subrequest will not be attached to the
      -- CastorFile. We actually attach it when we create it.
      selectCastorFileInternal(segment.s_fileid, nsHostName, segment.fileclass,
                               segment.segSize, lastKnownFileName, 0, creationTime, firstCF, cfid, unused);
      firstCF := FALSE;
    EXCEPTION WHEN locked THEN
      -- commit what we've done so far
      COMMIT;
      -- And lock the castorfile (waiting this time)
      selectCastorFileInternal(segment.s_fileid, nsHostName, segment.fileclass,
                               segment.segSize, lastKnownFileName, 0, creationTime, TRUE, cfid, unused);
    END;
    nbFilesProcessed := nbFilesProcessed + 1;
    -- create  subrequest for this file.
    -- Note that the svcHandler is set to PrepReqSvc. Most of the time, it will not be used,
    -- as the rest of this procedure will handle the subrequests itself. The only case where it is
    -- needed is when some subrequests are put to WAITSUBREQ. The the PrepReqSvc will be
    -- in charge of handling them one by one when they can wake up
    INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, parent, status, request, getNextStatus, reqType)
    VALUES (0, lastKnownFileName, repackProtocol, segment.segSize, 0, uuidGen(), 0, 0, creationTime, creationTime, 0, 0, '', NULL, 'PrepReqSvc', ids_seq.nextval, 0, cfId, NULL, dconst.SUBREQUEST_START, varReqId, 0, 119)
    RETURNING id INTO varSubreqId;
    -- find out whether this file is already staged or not
    processPrepareRequest(varSubreqId, result);
    CASE
      WHEN result = -2 OR result = 0 OR result = 1 THEN
        -- SubRequest has been put in WAITSUBREQ, WAITDISK2DISKCOPY or directly to REPACK. Nothing to do
        isOngoing := True;
      WHEN result = -1 THEN
        -- SubRequest has been failed, log the error
        INSERT INTO ProcessBulkRequestHelper
          (SELECT segment.s_fileid, nsHostName, errorCode, errorMessage
             FROM SubRequest WHERE id = varSubreqId);
        nbFilesFailed := nbFilesFailed + 1;
      WHEN result = 2 THEN
        -- standard case : we need to log and to trigger the recall
        triggerRepackRecall(varSubreqId, cfId, tapeId, segment.s_fileid, nsHostName, segment.blockid, segment.fseq, segment.copyno, priority, euid, egid);
        isOngoing := True;
    END CASE;
  END LOOP;
  -- cleanup RepackTapeSegments
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RepackTapeSegments';
  -- update status of the RepackRequest
  IF isOngoing THEN
    UPDATE StageRepackRequest SET status = tconst.REPACK_ONGOING WHERE StageRepackRequest.id = varReqId;
  ELSE
    IF nbFilesFailed > 0 THEN
      UPDATE StageRepackRequest SET status = tconst.REPACK_FAILED WHERE StageRepackRequest.id = varReqId;
    ELSE
      -- CASE of an 'empty' repack : the tape had no files at all
      UPDATE StageRepackRequest SET status = tconst.REPACK_FINISHED WHERE StageRepackRequest.id = varReqId;
    END IF;
  END IF;
  -- open cursor on results
  OPEN rSubResults FOR
    SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
END;
/

/* PL/SQL method to handle a repack subrequest.
 * Repack requests are normally handled as a whole via the handleRepackRequest procedure.
 * However, some subrequests may be put in WAITSUBREQ on that processing.
 * They are further processed here, when the wait is over.
 */
CREATE OR REPLACE PROCEDURE handleRepackSubRequest(srId IN INTEGER, rIpAddress OUT INTEGER,
                                                   rport OUT INTEGER, rReqUuid OUT VARCHAR2,
                                                   rSubResults OUT castor.FileResult_Cur) AS
  nsHostName VARCHAR2(2048);
  varFileid NUMBER;
  varReqid NUMBER;
  varCfid NUMBER;
  varReqvid VARCHAR2(2048);
  varEuid INTEGER;
  varEgid INTEGER;
  varPriority INTEGER;
  varBlockid RAW(4);
  varFseq INTEGER;
  varCopyno INTEGER;
  varTapeId INTEGER;
  varTapeStatus INTEGER;
  varSvcClassId INTEGER;
  varRecallPolicy VARCHAR2(2048);
  varResult NUMBER;
BEGIN
  -- name server host name
  nsHostName := getConfigOption('stager', 'nsHost', '');
  -- Get the request id and the fileid of the file
  SELECT fileid, request, CastorFile.id
    INTO varFileid, varReqid, varCfId
    FROM SubRequest, CastorFile
   WHERE SubRequest.id = srId
     AND SubRequest.castorFile = CastorFile.id;
  -- get the repack request's content
  SELECT repackVid, ipAddress, port, reqId, euid, egid, svcClass
    INTO varReqvid, rIpAddress, rport, rReqUuid, varEuid, varEgid, varSvcClassId
    FROM StageRepackRequest, Client
   WHERE StageRepackRequest.id = varReqid
     AND StageRepackRequest.client = Client.id;
  -- find out whether this file is already staged or not
  processPrepareRequest(srId, varResult);
  CASE
    WHEN varResult = -2 OR varResult = 0 THEN
      -- SubRequest has been put in WAITSUBREQ or directly to REPACK. Nothing to do, only log
      INSERT INTO ProcessBulkRequestHelper VALUES (varFileid, nsHostName, 0, '');
    WHEN varResult = -1 THEN
      -- SubRequest has been failed, log the error
      INSERT INTO ProcessBulkRequestHelper
        (SELECT varFileid, nsHostName, errorCode, errorMessage
           FROM SubRequest WHERE id = srId);
    ELSE
      -- standard case : we need to log and to trigger the recall
      INSERT INTO ProcessBulkRequestHelper VALUES (varFileid, nsHostName, 0, '');
      -- compute the priority of this request
      BEGIN
        SELECT priority INTO varPriority FROM PriorityMap
         WHERE euid = varEuid AND egid = varEgid AND ROWNUM < 2;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        varPriority := 0;
      END;
      -- check and create the tape if needed. Its status depends on the presence of a recallerPolicy
      -- Note that selectTapeForRecall uses autonomous transaction so that we do not keep a lock
      SELECT recallerPolicy INTO varRecallPolicy FROM SvcClass WHERE id = varSvcClassId;
      IF varRecallPolicy IS NULL THEN
        varTapeStatus := tconst.TAPE_PENDING;
      ELSE
        varTapeStatus := tconst.TAPE_WAITPOLICY;
      END IF;
      varTapeId := selectTapeForRecall(varReqvid, varTapeStatus);
      -- get all data we need for trigering a recall
      SELECT blockid, fseq, copyno
        INTO varBlockid, varFseq, varCopyno
        FROM Cns_Seg_Metadata@remotens, Cns_File_Metadata@remotens
       WHERE s_status = '-'
         AND status != 'D'
         AND vid = varReqvid
         AND s_fileid = varFileid;
      -- trigger the recall
      triggerRepackRecall(srId, varCfid, varTapeId, varFileid, nsHostName, varBlockid, varFseq, varCopyno, varPriority, varEuid, varEgid);
  END CASE;
  -- open cursor on results
  OPEN rSubResults FOR
    SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
END;
/

/* PL/SQL method to get the next failed SubRequest to do according to the given service */
/* the service parameter is not used now, it will with the new stager */
CREATE OR REPLACE PROCEDURE subRequestFailedToDo(srId OUT NUMBER, srFileName OUT VARCHAR2, srSubReqId OUT VARCHAR2,
                                                 srErrorCode OUT INTEGER, srErrorMessage OUT VARCHAR2, rReqId OUT VARCHAR2,
                                                 clIpAddress OUT INTEGER, clPort OUT INTEGER, clVersion OUT INTEGER,
                                                 srFileId OUT NUMBER) AS
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  CURSOR c IS
     SELECT /*+ FIRST_ROWS(10) INDEX(SR I_SubRequest_RT_CT_ID) */ SR.id
       FROM SubRequest PARTITION (P_STATUS_7) SR; -- FAILED
  varSRId NUMBER;
  varCFId NUMBER;
  varRId NUMBER;
  varSrAnswered INTEGER;
  varRName VARCHAR2(100);
  varClientId NUMBER;
BEGIN
  OPEN c;
  LOOP
    FETCH c INTO varSRId;
    EXIT WHEN c%NOTFOUND;
    BEGIN
      SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ answered INTO varSrAnswered
        FROM SubRequest PARTITION (P_STATUS_7) 
       WHERE id = varSRId FOR UPDATE NOWAIT;
      IF varSrAnswered = 1 THEN
        -- already answered, ignore it
        archiveSubReq(varSRId, dconst.SUBREQUEST_FAILED_FINISHED);
      ELSE
        -- we got our subrequest, select all relevant data
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ subrequest
           SET status = dconst.SUBREQUEST_FAILED_ANSWERING
         WHERE id = varSRId
        RETURNING fileName, subReqId, errorCode, errorMessage,
          (SELECT object FROM Type2Obj WHERE type = reqType), request, castorFile
        INTO srFileName, srSubReqId, srErrorCode, srErrorMessage, varRName, varRId, varCFId;
        srId := varSRId;
        srFileId := 0;
        BEGIN
          CASE
            WHEN varRName = 'StagePrepareToPutRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StagePrepareToPutRequest WHERE id = varRId;
            WHEN varRName = 'StagePrepareToGetRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StagePrepareToGetRequest WHERE id = varRId;
            WHEN varRName = 'StagePrepareToUpdateRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StagePrepareToUpdateRequest WHERE id = varRId;
            WHEN varRName = 'StageRepackRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StageRepackRequest WHERE id = varRId;
            WHEN varRName = 'StagePutRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StagePutRequest WHERE id = varRId;
            WHEN varRName = 'StageGetRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StageGetRequest WHERE id = varRId;
            WHEN varRName = 'StageUpdateRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StageUpdateRequest WHERE id = varRId;
            WHEN varRName = 'StagePutDoneRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StagePutDoneRequest WHERE id = varRId;
            WHEN varRName = 'StageRmRequest' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM StageRmRequest WHERE id = varRId;
            WHEN varRName = 'SetFileGCWeight' THEN
              SELECT reqId, client
                INTO rReqId, varClientId
                FROM SetFileGCWeight WHERE id = varRId;
          END CASE;
          SELECT ipAddress, port, version
            INTO clIpAddress, clPort, clVersion
            FROM Client WHERE id = varClientId;
          IF varCFId > 0 THEN
            SELECT fileId INTO srFileId FROM CastorFile WHERE id = varCFId;
          END IF;
          EXIT;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- This should never happen, we have an orphaned subrequest.
          -- As we couldn't get the client, we just archive and move on.
          srId := 0;
          archiveSubReq(varSRId, dconst.SUBREQUEST_FAILED_FINISHED);
        END;
      END IF;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        -- Go to next candidate, this subrequest was processed already and its status changed
        NULL;
      WHEN SrLocked THEN
        -- Go to next candidate, this subrequest is being processed by another thread
        NULL;
    END;
  END LOOP;
  CLOSE c;
END;
/


/* PL/SQL method to get the next request to do according to the given service */
CREATE OR REPLACE PROCEDURE requestToDo(service IN VARCHAR2, rId OUT INTEGER, rType OUT INTEGER) AS
BEGIN
  DELETE FROM NewRequests
   WHERE type IN (
     SELECT type FROM Type2Obj
      WHERE svcHandler = service
        AND svcHandler IS NOT NULL
     )
   AND ROWNUM < 2 RETURNING id, type INTO rId, rType;
EXCEPTION WHEN NO_DATA_FOUND THEN
  rId := 0;   -- nothing to do
  rType := 0;
END;
/


/* PL/SQL method to make a SubRequest wait on another one, linked to the given DiskCopy */
CREATE OR REPLACE PROCEDURE makeSubRequestWait(srId IN INTEGER, dci IN INTEGER) AS
BEGIN
  -- all wait on the original one; also prevent to wait on a PrepareToPut, for the
  -- case when Updates and Puts come after a PrepareToPut and they need to wait on
  -- the first Update|Put to complete.
  -- Cf. recreateCastorFile and the DiskCopy statuses 5 and 11
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET parent = (SELECT SubRequest.id
                     FROM SubRequest, DiskCopy
                    WHERE SubRequest.diskCopy = DiskCopy.id
                      AND DiskCopy.id = dci
                      AND SubRequest.reqType <> 37  -- OBJ_PrepareToPut
                      AND SubRequest.parent = 0
                      AND DiskCopy.status IN (1, 2, 5, 6, 11) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT, WAITFS_SCHEDULING
                      AND SubRequest.status IN (0, 1, 2, 4, 13, 14, 6)), -- START, RESTART, RETRY, WAITTAPERECALL, READYFORSCHED, BEINGSCHED, READY
        status = 5, -- WAITSUBREQ
        lastModificationTime = getTime()
  WHERE SubRequest.id = srId;
END;
/


/* PL/SQL method to archive a SubRequest */
CREATE OR REPLACE PROCEDURE archiveSubReq(srId IN INTEGER, finalStatus IN INTEGER) AS
  unused INTEGER;
  rId INTEGER;
  rName VARCHAR2(100);
  rType NUMBER := 0;
  clientId INTEGER;
BEGIN
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
     SET parent = NULL, diskCopy = NULL,  -- unlink this subrequest as it's dead now
         lastModificationTime = getTime(),
         status = finalStatus
   WHERE id = srId
   RETURNING request, reqType, (SELECT object FROM Type2Obj WHERE type = reqType) INTO rId, rType, rName;
  BEGIN
    -- Try to see whether another subrequest in the same
    -- request is still being processed
    SELECT id INTO unused FROM SubRequest
     WHERE request = rId AND status NOT IN (8, 9) AND ROWNUM < 2;  -- all but {FAILED_,}FINISHED
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- All subrequests have finished, we can archive:
    -- take lock on the request (it is enough the following be
    -- executed serially, multiple executions are idempotent)
    -- and drop the associated Client entity
    EXECUTE IMMEDIATE
      'BEGIN SELECT client INTO :clientId FROM '|| rName ||' WHERE id = :rId FOR UPDATE; END;'
      USING OUT clientId, IN rId;
    DELETE FROM Client WHERE id = clientId;
    -- archive the successful subrequests
    UPDATE /*+ INDEX(SubRequest I_SubRequest_Request) */ SubRequest
       SET status = 11    -- ARCHIVED
     WHERE request = rId
       AND status = 8;  -- FINISHED
    -- in case of repack, change the status of the request
    IF rType = 119 THEN
      DECLARE
        nbfailures NUMBER;
      BEGIN
        SELECT count(*) INTO nbfailures FROM SubRequest
         WHERE request = rId
           AND status = dconst.SUBREQUEST_FAILED_FINISHED
           AND ROWNUM < 2;
        UPDATE StageRepackRequest
           SET status = CASE nbfailures WHEN 1 THEN tconst.REPACK_FAILED ELSE tconst.REPACK_FINISHED END
         WHERE id = rId;
      END;
    END IF;
  END;
END;
/


/* PL/SQL method checking whether a given service class
 * is declared disk only and had only full diskpools.
 * Returns 1 in such a case, 0 else
 */
CREATE OR REPLACE FUNCTION checkFailJobsWhenNoSpace(svcClassId NUMBER)
RETURN NUMBER AS
  failJobsFlag NUMBER;
  defFileSize NUMBER;
  c NUMBER;
  availSpace NUMBER;
  reservedSpace NUMBER;
BEGIN
  -- Determine if the service class is D1 and the default
  -- file size. If the default file size is 0 we assume 2G
  SELECT failJobsWhenNoSpace,
         decode(defaultFileSize, 0, 2000000000, defaultFileSize)
    INTO failJobsFlag, defFileSize
    FROM SvcClass
   WHERE id = svcClassId;
  -- Check that the pool has space, taking into account current
  -- availability and space reserved by Put requests in the queue
  IF (failJobsFlag = 1) THEN
    SELECT count(*), sum(free - totalSize * minAllowedFreeSpace) 
      INTO c, availSpace
      FROM DiskPool2SvcClass, FileSystem, DiskServer
     WHERE DiskPool2SvcClass.child = svcClassId
       AND DiskPool2SvcClass.parent = FileSystem.diskPool
       AND FileSystem.diskServer = DiskServer.id
       AND FileSystem.status = 0 -- PRODUCTION
       AND DiskServer.status = 0 -- PRODUCTION
       AND totalSize * minAllowedFreeSpace < free - defFileSize;
    IF (c = 0) THEN
      RETURN 1;
    END IF;
  END IF;
  RETURN 0;
END;
/

/* PL/SQL method checking whether we have an existing routing for this service class and file class.
 * Returns 1 in case we do not have such a routing, 0 else
 */
CREATE OR REPLACE FUNCTION checkNoTapeRouting(svcClassId NUMBER, fileClassId NUMBER)
RETURN NUMBER AS
  nbTCs INTEGER;
  varTpId INTEGER;
BEGIN
  -- get number of copies on tape requested by this file
  SELECT nbCopies INTO nbTCs
    FROM FileClass WHERE id = fileClassId;
  -- loop over the copies and check the routing of each of them
  FOR i IN 1..nbTCs LOOP
    SELECT tapePool INTO varTpId FROM MigrationRouting
     WHERE fileClass = fileClassId
       AND svcClass = svcClassId
       AND copyNb = i
       AND ROWNUM < 2;
  END LOOP;
  -- all routes could be found. Everything is ok
  RETURN 0;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no route for at least one copy
  RETURN 1;
END;
/


/* PL/SQL method implementing getBestDiskCopyToReplicate. */
CREATE OR REPLACE PROCEDURE getBestDiskCopyToReplicate
  (cfId IN NUMBER, reuid IN NUMBER, regid IN NUMBER, internal IN NUMBER,
   destSvcClassId IN NUMBER, dcId OUT NUMBER, srcSvcClassId OUT NUMBER) AS
  destSvcClass VARCHAR2(2048);
BEGIN
  -- Select the best diskcopy available to replicate and for which the user has
  -- access too.
  SELECT id, srcSvcClassId INTO dcId, srcSvcClassId
    FROM (
      SELECT DiskCopy.id, SvcClass.id srcSvcClassId
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
       WHERE DiskCopy.castorfile = cfId
         AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
         AND FileSystem.id = DiskCopy.fileSystem
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
         AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.id = FileSystem.diskserver
         AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
         -- If this is an internal replication request make sure that the diskcopy
         -- is in the same service class as the source.
         AND (SvcClass.id = destSvcClassId OR internal = 0)
         -- Check that the user has the necessary access rights to replicate a
         -- file from the source service class. Note: instead of using a
         -- StageGetRequest type here we use a StagDiskCopyReplicaRequest type
         -- to be able to distinguish between and read and replication requst.
         AND checkPermission(SvcClass.name, reuid, regid, 133) = 0
         AND NOT EXISTS (
           -- Don't select source diskcopies which already failed more than 10 times
           SELECT /*+ INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */ 'x'
             FROM StageDiskCopyReplicaRequest R, SubRequest
            WHERE SubRequest.request = R.id
              AND R.sourceDiskCopy = DiskCopy.id
              AND SubRequest.status = 9 -- FAILED_FINISHED
           HAVING COUNT(*) >= 10)
       ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                               FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams,
                               FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC,
                DBMS_Random.value)
   WHERE ROWNUM < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RAISE; -- No diskcopy found that could be replicated
END;
/


/* PL/SQL method implementing getBestDiskCopyToRead used to return the
 * best location of a file based on monitoring information. This is
 * useful for xrootd so that it can avoid scheduling reads
 */
CREATE OR REPLACE PROCEDURE getBestDiskCopyToRead(cfId IN NUMBER,
                                                  svcClassId IN NUMBER,
                                                  diskServer OUT VARCHAR2,
                                                  filePath OUT VARCHAR2) AS
BEGIN
  -- Select best diskcopy
  SELECT name, path INTO diskServer, filePath FROM (
    SELECT DiskServer.name, FileSystem.mountpoint || DiskCopy.path AS path
      FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
     WHERE DiskCopy.castorfile = cfId
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskpool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = svcClassId
       AND FileSystem.status = 0 -- PRODUCTION
       AND FileSystem.diskserver = DiskServer.id
       AND DiskServer.status = 0 -- PRODUCTION
       AND DiskCopy.status IN (0, 6, 10)  -- STAGED, STAGEOUT, CANBEMIGR
     ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                             FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams,
                             FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC,
              DBMS_Random.value)
   WHERE rownum < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RAISE; -- No file found to be read
END;
/


/* PL/SQL method implementing checkForD2DCopyOrRecall
 * dcId is the DiskCopy id of the best candidate for replica, 0 if none is found (tape recall), -1 in case of user error
 * Internally used by getDiskCopiesForJob and processPrepareRequest
 */
CREATE OR REPLACE
PROCEDURE checkForD2DCopyOrRecall(cfId IN NUMBER, srId IN NUMBER, reuid IN NUMBER, regid IN NUMBER,
                                  svcClassId IN NUMBER, dcId OUT NUMBER, srcSvcClassId OUT NUMBER) AS
  destSvcClass VARCHAR2(2048);
  userid NUMBER := reuid;
  groupid NUMBER := regid;
BEGIN
  -- First check whether we are a disk only pool that is already full.
  -- In such a case, we should fail the request with an ENOSPACE error
  IF (checkFailJobsWhenNoSpace(svcClassId) = 1) THEN
    dcId := -1;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 7, -- FAILED
           errorCode = 28, -- ENOSPC
           errorMessage = 'File creation canceled since diskPool is full'
     WHERE id = srId;
    RETURN;
  END IF;
  -- Resolve the destination service class id to a name
  SELECT name INTO destSvcClass FROM SvcClass WHERE id = svcClassId;
  -- Determine if there are any copies of the file in the same service class
  -- on DISABLED or DRAINING hardware. If we found something then set the user
  -- and group id to -1 this effectively disables the later privilege checks
  -- to see if the user can trigger a d2d or recall. (#55745)
  BEGIN
    SELECT -1, -1 INTO userid, groupid
      FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
     WHERE DiskCopy.fileSystem = FileSystem.id
       AND DiskCopy.castorFile = cfId
       AND DiskCopy.status IN (0, 10)  -- STAGED, CANBEMIGR
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = svcClassId
       AND FileSystem.diskServer = DiskServer.id
       AND (DiskServer.status != 0  -- !PRODUCTION
        OR  FileSystem.status != 0) -- !PRODUCTION
       AND ROWNUM < 2;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL;  -- Nothing
  END; 
  -- If we are in this procedure then we did not find a copy of the
  -- file in the target service class that could be used. So, we check
  -- to see if the user has the rights to create a file in the destination
  -- service class. I.e. check for StagePutRequest access rights
  IF checkPermission(destSvcClass, userid, groupid, 40) != 0 THEN
    -- Fail the subrequest and notify the client
    dcId := -1;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 7, -- FAILED
           errorCode = 13, -- EACCES
           errorMessage = 'Insufficient user privileges to trigger a tape recall or file replication to the '''||destSvcClass||''' service class'
     WHERE id = srId;
    RETURN;
  END IF;
  -- Try to find a diskcopy to replicate
  getBestDiskCopyToReplicate(cfId, userid, groupid, 0, svcClassId, dcId, srcSvcClassId);
  -- We found at least one, therefore we schedule a disk2disk
  -- copy from the existing diskcopy not available to this svcclass
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- We found no diskcopies at all. We should not schedule
  -- and make a tape recall... except ... in 3 cases :
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
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 7, -- FAILED
           errorCode = CASE
             WHEN dcStatus IN (5, 11) THEN 16 -- WAITFS, WAITFSSCHEDULING, EBUSY
             WHEN dcStatus = 6 AND fsStatus = 0 AND dsStatus = 0 THEN 16 -- STAGEOUT, PRODUCTION, PRODUCTION, EBUSY
             ELSE 1718 -- ESTNOTAVAIL
           END,
           errorMessage = CASE
             WHEN dcStatus IN (5, 11) THEN -- WAITFS, WAITFSSCHEDULING
               'File is being (re)created right now by another user'
             WHEN dcStatus = 6 AND fsStatus = 0 and dsStatus = 0 THEN -- STAGEOUT, PRODUCTION, PRODUCTION
               'File is being written to in another service class'
             ELSE
               'All copies of this file are unavailable for now. Please retry later'
           END
     WHERE id = srId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Check whether the user has the rights to issue a tape recall to
    -- the destination service class.
    IF checkPermission(destSvcClass, userid, groupid, 161) != 0 THEN
      -- Fail the subrequest and notify the client
      dcId := -1;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7, -- FAILED
             errorCode = 13, -- EACCES
             errorMessage = 'Insufficient user privileges to trigger a tape recall to the '''||destSvcClass||''' service class'
       WHERE id = srId;
    ELSE
      -- We did not find the very special case so trigger a tape recall.
      dcId := 0;
    END IF;
  END;
END;
/


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
/


/* PL/SQL method implementing createDiskCopyReplicaRequest */
CREATE OR REPLACE PROCEDURE createDiskCopyReplicaRequest
(sourceSrId IN INTEGER, sourceDcId IN INTEGER, sourceSvcId IN INTEGER,
 destSvcId IN INTEGER, ouid IN INTEGER, ogid IN INTEGER) AS
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
  rfs VARCHAR(2048);
BEGIN
  -- Extract the castorfile associated with the request, this is needed to
  -- create the StageDiskCopyReplicaRequest's diskcopy and subrequest entries.
  -- Also for disk2disk copying across service classes make the originating
  -- subrequest wait on the completion of the transfer.
  IF sourceSrId > 0 THEN
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 5, parent = ids_seq.nextval -- WAITSUBREQ
     WHERE id = sourceSrId
    RETURNING castorFile, parent INTO cfId, srId;
  ELSE
    SELECT castorfile INTO cfId FROM DiskCopy WHERE id = sourceDcId;
    SELECT ids_seq.nextval INTO srId FROM Dual;
  END IF;

  -- Extract CastorFile information
  SELECT fileid, nshost, filesize, lastknownfilename
    INTO fileId, nsHost, fileSize, fileName
    FROM CastorFile WHERE id = cfId;

  -- Create the Client entry.
  INSERT INTO Client (ipaddress, port, id, version, secure)
    VALUES (0, 0, ids_seq.nextval, 0, 0)
  RETURNING id INTO clientId;

  -- Create the StageDiskCopyReplicaRequest. The euid and egid values default to
  -- 0 here, this indicates the request came from the user root.
  SELECT ids_seq.nextval INTO destDcId FROM Dual;
  INSERT INTO StageDiskCopyReplicaRequest
    (svcclassname, reqid, creationtime, lastmodificationtime, id, svcclass,
     client, sourcediskcopy, destdiskcopy, sourceSvcClass)
  VALUES ((SELECT name FROM SvcClass WHERE id = destSvcId), uuidgen(), gettime(),
     gettime(), ids_seq.nextval, destSvcId, clientId, sourceDcId, destDcId, sourceSvcId)
  RETURNING id INTO reqId;

  -- Determine the requested filesystem value
  SELECT DiskServer.name || ':' || FileSystem.mountpoint INTO rfs
    FROM DiskCopy, FileSystem, DiskServer
   WHERE DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskServer = DiskServer.id
     AND DiskCopy.id = sourceDcId;

  -- Create the SubRequest setting the initial status to READYFORSCHED for
  -- immediate dispatching i.e no stager processing
  INSERT INTO SubRequest
    (retrycounter, filename, protocol, xsize, priority, subreqid, flags, modebits,
     creationtime, lastmodificationtime, answered, id, diskcopy, castorfile, parent,
     status, request, getnextstatus, errorcode, requestedfilesystems, svcHandler, reqType)
  VALUES (0, fileName, 'rfio', fileSize, 0, uuidgen(), 0, 0, gettime(), gettime(),
     0, srId, destDcId, cfId, 0, 13, reqId, 0, 0, rfs, 'NotNullNeeded', 133); -- OBJ_StageDiskCopyReplicaRequest;

  -- Create the DiskCopy without filesystem
  buildPathFromFileId(fileId, nsHost, destDcId, rpath);
  INSERT INTO DiskCopy
    (path, id, filesystem, castorfile, status, creationTime, lastAccessTime,
     gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
  VALUES (rpath, destDcId, 0, cfId, 1, getTime(), getTime(), 0, fileSize, 0, ouid, ogid);  -- WAITDISK2DISKCOPY
END;
/

/* PL/SQL method implementing createEmptyFile */
CREATE OR REPLACE PROCEDURE createEmptyFile
(cfId IN NUMBER, fileId IN NUMBER, nsHost IN VARCHAR2,
 srId IN INTEGER, schedule IN INTEGER) AS
  dcPath VARCHAR2(2048);
  gcw NUMBER;
  gcwProc VARCHAR(2048);
  fsId NUMBER;
  dcId NUMBER;
  svcClassId NUMBER;
  ouid INTEGER;
  ogid INTEGER;
  fsPath VARCHAR2(2048);
BEGIN
  -- update filesize overriding any previous value
  UPDATE CastorFile SET fileSize = 0 WHERE id = cfId;
  -- get an id for our new DiskCopy
  SELECT ids_seq.nextval INTO dcId FROM DUAL;
  -- compute the DiskCopy Path
  buildPathFromFileId(fileId, nsHost, dcId, dcPath);
  -- find a fileSystem for this empty file
  SELECT id, svcClass, euid, egid, name || ':' || mountpoint
    INTO fsId, svcClassId, ouid, ogid, fsPath
    FROM (SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
                 FileSystem.id, Request.svcClass, Request.euid, Request.egid, DiskServer.name, FileSystem.mountpoint
            FROM DiskServer, FileSystem, DiskPool2SvcClass,
                 (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                         id, svcClass, euid, egid from StageGetRequest UNION ALL
                  SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */
                         id, svcClass, euid, egid from StagePrepareToGetRequest UNION ALL
                  SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                         id, svcClass, euid, egid from StageUpdateRequest UNION ALL
                  SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                         id, svcClass, euid, egid from StagePrepareToUpdateRequest) Request,
                  SubRequest
           WHERE SubRequest.id = srId
             AND Request.id = SubRequest.request
             AND Request.svcclass = DiskPool2SvcClass.child
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
             AND DiskServer.id = FileSystem.diskServer
             AND DiskServer.status = 0 -- DISKSERVER_PRODUCTION
        ORDER BY -- first prefer DSs without concurrent migrators/recallers
                 DiskServer.nbRecallerStreams ASC, FileSystem.nbMigratorStreams ASC,
                 -- then order by rate as defined by the function
                 fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                                FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams,
                                FileSystem.nbRecallerStreams) DESC,
                 -- finally use randomness to avoid preferring always the same FS
                 DBMS_Random.value)
   WHERE ROWNUM < 2;
  -- compute it's gcWeight
  gcwProc := castorGC.getRecallWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(0); END;'
    USING OUT gcw;
  -- then create the DiskCopy
  INSERT INTO DiskCopy
    (path, id, filesystem, castorfile, status,
     creationTime, lastAccessTime, gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
  VALUES (dcPath, dcId, fsId, cfId, 0,   -- STAGED
          getTime(), getTime(), GCw, 0, 0, ouid, ogid);
  -- link to the SubRequest and schedule an access if requested
  IF schedule = 0 THEN
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET diskCopy = dcId WHERE id = srId;
  ELSE
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET diskCopy = dcId,
           requestedFileSystems = fsPath,
           xsize = 0, status = 13, -- READYFORSCHED
           getNextStatus = 1 -- FILESTAGED
     WHERE id = srId;    
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  raise_application_error(-20115, 'No suitable filesystem found for this empty file');
END;
/

/* PL/SQL method implementing replicateOnClose */
CREATE OR REPLACE PROCEDURE replicateOnClose(cfId IN NUMBER, ouid IN INTEGER, ogid IN INTEGER) AS
  unused NUMBER;
  srcDcId NUMBER;
  srcSvcClassId NUMBER;
  ignoreSvcClass NUMBER;
BEGIN
  -- Lock the castorfile
  SELECT id INTO unused FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Loop over all service classes where replication is required
  FOR a IN (SELECT SvcClass.id FROM (
              -- Determine the number of copies of the file in all service classes
              SELECT * FROM (
                SELECT SvcClass.id, count(*) available
                  FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
                 WHERE DiskCopy.filesystem = FileSystem.id
                   AND DiskCopy.castorfile = cfId
                   AND FileSystem.diskpool = DiskPool2SvcClass.parent
                   AND DiskPool2SvcClass.child = SvcClass.id
                   AND DiskCopy.status IN (0, 1, 10)  -- STAGED, WAITDISK2DISKCOPY, CANBEMIGR
                   AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
                   AND DiskServer.id = FileSystem.diskserver
                   AND DiskServer.status IN (0, 1)  -- PRODUCTION, DRAINING
                 GROUP BY SvcClass.id)
             ) results, SvcClass
             -- Join the results with the service class table and determine if
             -- additional copies need to be created
             WHERE results.id = SvcClass.id
               AND SvcClass.replicateOnClose = 1
               AND results.available < SvcClass.maxReplicaNb)
  LOOP
    -- Ignore this replication if there is already a pending replication request
    -- for the given castorfile in the target/destination service class. Once
    -- the replication has gone through, this procedure will be called again.
    -- This insures that only one replication request is active at any given time
    -- and that we don't create too many replication requests that may exceed
    -- the maxReplicaNb defined on the service class
    BEGIN
      SELECT /*+ INDEX(StageDiskCopyReplicaRequest I_StageDiskCopyReplic_DestDC) */ DiskCopy.id INTO unused
        FROM DiskCopy, StageDiskCopyReplicaRequest
       WHERE StageDiskCopyReplicaRequest.destdiskcopy = DiskCopy.id
         AND StageDiskCopyReplicaRequest.svcclass = a.id
         AND DiskCopy.castorfile = cfId
         AND DiskCopy.status = 1  -- WAITDISK2DISKCOPY
         AND rownum < 2;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- Select the best diskcopy to be replicated. Note: we force that the
        -- replication must happen internally within the service class. This
        -- prevents D2 style activities from impacting other more controlled
        -- service classes. E.g. D2 replication should not impact CDR
        getBestDiskCopyToReplicate(cfId, -1, -1, 1, a.id, srcDcId, srcSvcClassId);
        -- Trigger a replication request.
        createDiskCopyReplicaRequest(0, srcDcId, srcSvcClassId, a.id, ouid, ogid);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        NULL;  -- No copies to replicate from
      END;
    END;
  END LOOP;
END;
/


/* PL/SQL method implementing getDiskCopiesForJob */
/* the result output is a DiskCopy status for STAGED, DISK2DISKCOPY, RECALL or WAITFS
   -1 for user failure, -2 or -3 for subrequest put in WAITSUBREQ */
CREATE OR REPLACE PROCEDURE getDiskCopiesForJob
        (srId IN INTEGER, result OUT INTEGER,
         sources OUT castor.DiskCopy_Cur) AS
  nbDCs INTEGER;
  nbDSs INTEGER;
  maxDCs INTEGER;
  upd INTEGER;
  dcIds "numList";
  dcStatus INTEGER;
  svcClassId NUMBER;
  srcSvcClassId NUMBER;
  cfId NUMBER;
  srcDcId NUMBER;
  d2dsrId NUMBER;
  reuid NUMBER;
  regid NUMBER;
BEGIN
  -- retrieve the castorFile and the svcClass for this subrequest
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         SubRequest.castorFile, Request.euid, Request.egid, Request.svcClass, Request.upd
    INTO cfId, reuid, regid, svcClassId, upd
    FROM (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                 id, euid, egid, svcClass, 0 upd from StageGetRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                 id, euid, egid, svcClass, 1 upd from StageUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = srId;
  -- lock the castorFile to be safe in case of concurrent subrequests
  SELECT id INTO cfId FROM CastorFile WHERE id = cfId FOR UPDATE;

  -- First see whether we should wait on an ongoing request
  SELECT DiskCopy.id BULK COLLECT INTO dcIds
    FROM DiskCopy, FileSystem, DiskServer
   WHERE cfId = DiskCopy.castorFile
     AND FileSystem.id(+) = DiskCopy.fileSystem
     AND nvl(FileSystem.status, 0) = 0 -- PRODUCTION
     AND DiskServer.id(+) = FileSystem.diskServer
     AND nvl(DiskServer.status, 0) = 0 -- PRODUCTION
     AND DiskCopy.status IN (2, 11); -- WAITTAPERECALL, WAITFS_SCHEDULING
  IF dcIds.COUNT > 0 THEN
    -- DiskCopy is in WAIT*, make SubRequest wait on previous subrequest and do not schedule
    makeSubRequestWait(srId, dcIds(1));
    result := -2;
    RETURN;
  END IF;

  -- Look for available diskcopies. The status is needed for the
  -- internal replication processing, and only if count = 1, hence
  -- the min() function does not represent anything here.
  SELECT COUNT(DiskCopy.id), min(DiskCopy.status) INTO nbDCs, dcStatus
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
      FOR SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ DiskCopy.id, DiskCopy.path, DiskCopy.status,
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
    -- Internal replication processing
    IF upd = 1 OR (nbDCs = 1 AND dcStatus = 6) THEN -- DISKCOPY_STAGEOUT
      -- replication forbidden
      result := 0;  -- DISKCOPY_STAGED
    ELSE
      -- check whether there's already an ongoing replication
      BEGIN
        SELECT /*+ INDEX(StageDiskCopyReplicaRequest I_StageDiskCopyReplic_DestDC) */ DiskCopy.id INTO srcDcId
          FROM DiskCopy, StageDiskCopyReplicaRequest
         WHERE DiskCopy.id = StageDiskCopyReplicaRequest.destDiskCopy
           AND StageDiskCopyReplicaRequest.svcclass = svcClassId
           AND DiskCopy.castorfile = cfId
           AND DiskCopy.status = dconst.DISKCOPY_WAITDISK2DISKCOPY;
        -- found an ongoing replication, we don't trigger another one
        result := dconst.DISKCOPY_STAGED;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- ok, we can replicate; do we need to? compare total current
        -- # of diskcopies, regardless hardware availability, against maxReplicaNb.
        SELECT COUNT(*), max(maxReplicaNb) INTO nbDCs, maxDCs FROM (
          SELECT DiskCopy.id, maxReplicaNb
            FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass
           WHERE DiskCopy.castorfile = cfId
             AND DiskCopy.fileSystem = FileSystem.id
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND DiskPool2SvcClass.child = SvcClass.id
             AND SvcClass.id = svcClassId
             AND DiskCopy.status IN (dconst.DISKCOPY_STAGED, dconst.DISKCOPY_CANBEMIGR));
        IF nbDCs < maxDCs OR maxDCs = 0 THEN
          -- we have to replicate. Do it only if we have enough
          -- available diskservers.
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
                  AND DiskCopy.status IN (dconst.DISKCOPY_STAGED, dconst.DISKCOPY_CANBEMIGR));
          IF nbDSs > 0 THEN
            BEGIN
              -- yes, we can replicate. Select the best candidate for replication
              srcDcId := 0;
              getBestDiskCopyToReplicate(cfId, -1, -1, 1, svcClassId, srcDcId, svcClassId);
              -- and create a replication request without waiting on it.
              createDiskCopyReplicaRequest(0, srcDcId, svcClassId, svcClassId, reuid, regid);
              -- result is different for logging purposes
              result := dconst.DISKCOPY_WAITDISK2DISKCOPY;
            EXCEPTION WHEN NO_DATA_FOUND THEN
              -- replication failed. We still go ahead with the access
              result := dconst.DISKCOPY_STAGED;  
            END;
          ELSE
            -- no replication to be done
            result := dconst.DISKCOPY_STAGED;
          END IF;
        ELSE
          -- no replication to be done
          result := dconst.DISKCOPY_STAGED;
        END IF;
      END;
    END IF;   -- end internal replication processing
  ELSE
    -- No diskcopies available for this service class:
    -- first check whether there's already a disk to disk copy going on
    BEGIN
      SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile) INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */ SubRequest.id INTO d2dsrId
        FROM StageDiskCopyReplicaRequest Req, SubRequest
       WHERE SubRequest.request = Req.id
         AND Req.svcClass = svcClassId    -- this is the destination service class
         AND status IN (13, 14, 6)  -- WAITINGFORSCHED, BEINGSCHED, READY
         AND castorFile = cfId;
      -- found it, wait on it
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET parent = d2dsrId, status = 5  -- WAITSUBREQ
       WHERE id = srId;
      result := -2;
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- not found, we may have to schedule a disk to disk copy or trigger a recall
      checkForD2DCopyOrRecall(cfId, srId, reuid, regid, svcClassId, srcDcId, srcSvcClassId);
      IF srcDcId > 0 THEN
        -- create DiskCopyReplica request and make this subRequest wait on it
        createDiskCopyReplicaRequest(srId, srcDcId, srcSvcClassId, svcClassId, reuid, regid);
        result := -3; -- return code is different here for logging purposes
      ELSIF srcDcId = 0 THEN
        -- no diskcopy found at all, go for recall
        result := 2;  -- DISKCOPY_WAITTAPERECALL
      ELSE
        -- user error
        result := -1;
      END IF;
    END;
  END IF;
END;
/

/*** initMigration ***/
CREATE OR REPLACE PROCEDURE initMigration(cfId IN INTEGER, datasize IN INTEGER, nbTC IN INTEGER, scId IN INTEGER) AS
  varTpId INTEGER;
  varSizeThreshold INTEGER;
BEGIN
  varSizeThreshold := TO_NUMBER(getConfigOption('tape', 'sizeThreshold', '300000000'));
  FOR i IN 1..nbTC LOOP
    -- Find routing
    BEGIN
      SELECT tapePool INTO varTpId FROM MigrationRouting MR, CastorFile
       WHERE MR.fileClass = CastorFile.fileClass
         AND CastorFile.id = cfId
         AND MR.svcClass = scId
         AND MR.copyNb = i
         AND (MR.isSmallFile = (CASE WHEN datasize < varSizeThreshold THEN 1 ELSE 0 END) OR MR.isSmallFile IS NULL);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- No routing rule found means a user-visible error on the putDone or on the file close operation
      raise_application_error(-20100, 'Cannot find an appropriate tape routing for this file in the current service class, aborting tape migration');
    END;
    -- Create tape copy and attach to the appropriate tape pool
    -- XXX TODO tapeGatewayRequestId should be renamed migrationMount and made Foreign Key
    INSERT INTO MigrationJob (fileSize, creationTime, castorFile, copyNb, tapePool, nbRetry, status, id, tapeGatewayRequestId)
      VALUES (datasize, getTime(), cfId, i, varTpId, 0, tconst.MIGRATIONJOB_PENDING, ids_seq.nextval, NULL);
  END LOOP;
END;
/

/* PL/SQL method internalPutDoneFunc, used by putDoneFunc.
   checks for diskcopies in STAGEOUT and creates the migration jobs
 */
CREATE OR REPLACE PROCEDURE internalPutDoneFunc (cfId IN INTEGER,
                                                 fs IN INTEGER,
                                                 context IN INTEGER,
                                                 nbTC IN INTEGER,
                                                 svcClassId IN INTEGER) AS
  tcId INTEGER;
  dcId INTEGER;
  gcwProc VARCHAR2(2048);
  gcw NUMBER;
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  -- get function to use for computing the gc weight of the brand new diskCopy
  gcwProc := castorGC.getUserWeight(svcClassId);
  -- if no tape copy or 0 length file, no migration
  -- so we go directly to status STAGED
  IF nbTC = 0 OR fs = 0 THEN
    EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:fs, 0); END;'
      USING OUT gcw, IN fs;
    UPDATE DiskCopy
       SET status = 0, -- STAGED
           lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
           gcWeight = gcw,
	   diskCopySize = fs
     WHERE castorFile = cfId AND status = 6 -- STAGEOUT
     RETURNING owneruid, ownergid INTO ouid, ogid;
  ELSE
    EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:fs, 10); END;'
      USING OUT gcw, IN fs;
    -- update the DiskCopy status to CANBEMIGR
    dcId := 0;
    UPDATE DiskCopy
       SET status = 10, -- CANBEMIGR
           lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
           gcWeight = gcw,
	   diskCopySize = fs
     WHERE castorFile = cfId AND status = 6 -- STAGEOUT
     RETURNING id, owneruid, ownergid INTO dcId, ouid, ogid;
    IF dcId > 0 THEN
      -- Only if we really found the relevant diskcopy, create migration jobs
      -- This is an extra sanity check, see also the deleteOutOfDateStageOutDCs procedure
      initMigration(cfId, fs, nbTC, svcClassId);
    END IF;
  END IF;
  -- If we are a real PutDone (and not a put outside of a prepareToPut/Update)
  -- then we have to archive the original prepareToPut/Update subRequest
  IF context = 2 THEN
    -- There can be only a single PrepareTo request: any subsequent PPut would be
    -- rejected and any subsequent PUpdate would be directly archived (cf. processPrepareRequest).
    DECLARE
      srId NUMBER;
    BEGIN
      SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest.id INTO srId
        FROM SubRequest,
         (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id
            FROM StagePrepareToPutRequest UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id
            FROM StagePrepareToUpdateRequest) Request
       WHERE SubRequest.castorFile = cfId
         AND SubRequest.request = Request.id
         AND SubRequest.status = 6;  -- READY
      archiveSubReq(srId, 8);  -- FINISHED
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;   -- ignore the missing subrequest
    END;
  END IF;
  -- Trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(cfId, ouid, ogid);
END;
/


/* PL/SQL method implementing putDoneFunc */
CREATE OR REPLACE PROCEDURE putDoneFunc (cfId IN INTEGER,
                                         fs IN INTEGER,
                                         context IN INTEGER,
                                         svcClassId IN INTEGER) AS
  nc INTEGER;
BEGIN
  -- get number of migration jobs to create
  SELECT nbCopies INTO nc FROM FileClass, CastorFile
   WHERE CastorFile.id = cfId AND CastorFile.fileClass = FileClass.id;
  -- and execute the internal putDoneFunc with the number of migration jobs to be created
  internalPutDoneFunc(cfId, fs, context, nc, svcClassId);
END;
/


/* PL/SQL procedure implementing startRepackMigration */
CREATE OR REPLACE PROCEDURE startRepackMigration
(srId IN INTEGER, cfId IN INTEGER, dcId IN INTEGER,
 reuid OUT INTEGER, regid OUT INTEGER) AS
  nbTC NUMBER(2);
  nbTCInFC NUMBER(2);
  fs NUMBER;
  svcClassId NUMBER;
BEGIN
  UPDATE DiskCopy SET status = 6  -- DISKCOPY_STAGEOUT
   WHERE id = dcId RETURNING diskCopySize INTO fs;
  -- how many do we have to create ?
  SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */
         count(StageRepackRequest.repackVid) INTO nbTC
    FROM SubRequest, StageRepackRequest
   WHERE SubRequest.request = StageRepackRequest.id
     AND (SubRequest.id = srId
       OR (SubRequest.castorFile = cfId
         AND SubRequest.status IN (4, 5)));  -- WAITTAPERECALL, WAITSUBREQ
  SELECT nbCopies INTO nbTCInFC
    FROM FileClass, CastorFile
   WHERE CastorFile.id = cfId
     AND FileClass.id = Castorfile.fileclass;
  -- we are not allowed to create more migration jobs than specified in the FileClass
  IF nbTCInFC < nbTC THEN
    nbTC := nbTCInFC;
  END IF;
  -- update the Repack subRequest for this file. The status REPACK
  -- stays until the migration to the new tape is over.
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET diskCopy = dcId, status = 12  -- REPACK
   WHERE id = srId;   
  -- get the service class, uid and gid
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id) INDEX(R PK_StageRepackRequest_Id) */ R.svcClass, euid, egid
    INTO svcClassId, reuid, regid
    FROM StageRepackRequest R, SubRequest
   WHERE SubRequest.request = R.id
     AND SubRequest.id = srId;
  -- create the required number of migration jobs for the files
  -- XXX For the time being, nbTC will be 1 for sure until we're able
  -- XXX to handle repacking of dual-copy files 
  internalPutDoneFunc(cfId, fs, 0, nbTC, svcClassId);
  -- update remaining STAGED diskcopies to CANBEMIGR too
  -- we may have them as result of disk2disk copies, and so far
  -- we only dealt with dcId
  UPDATE DiskCopy SET status = 10  -- DISKCOPY_CANBEMIGR
   WHERE castorFile = cfId AND status = 0;  -- DISKCOPY_STAGED
END;
/

/* PL/SQL procedure implementing selectTapeForRecall
 * get the given tape or create it
 * Note that we run in an autonomous transaction.
 */
CREATE OR REPLACE FUNCTION selectTapeForRecall(vid IN VARCHAR2, newStatus IN INTEGER)
RETURN INTEGER AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
  tapeId INTEGER;
  status INTEGER;
BEGIN
  -- try to get the tape, assuming it exists
  -- Note the hardcoded side, as this column is deprecated
  SELECT id, status INTO tapeId, status FROM Tape WHERE vid = vid AND tpmode = tconst.TPMODE_READ AND side = 0;
  IF status = tconst.TAPE_UNUSED OR status = tconst.TAPE_FINISHED OR
     status = tconst.TAPE_FAILED OR status = tconst.TAPE_UNKNOWN THEN
    UPDATE Tape SET status = newStatus WHERE id = tapeId;
    COMMIT;
  END IF;
  RETURN tapeId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no tape found, create it
  BEGIN
    SELECT ids_seq.nextval INTO tapeId FROM DUAL;
    INSERT INTO TAPE (id, vid, side, tpmode, status)
    VALUES (tapeId, vid, 0, tconst.TPMODE_READ, newStatus);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    -- insertion failed with constraint violated.
    -- This means that somebody else was faster.
    -- So go back to select
    SELECT id INTO tapeId FROM Tape WHERE vid = vid AND tpmode = tconst.TPMODE_READ AND side = 0;
    IF status = tconst.TAPE_UNUSED OR status = tconst.TAPE_FINISHED OR
       status = tconst.TAPE_FAILED OR status = tconst.TAPE_UNKNOWN THEN
      UPDATE Tape SET status = newStatus WHERE id = tapeId;
    END IF;
  END;
  COMMIT;
  RETURN tapeId;
END;
/

/* PL/SQL procedure implementing triggerRepackRecall
 * this creates all rows needed to recall a given file in the repack context
 * that is Tape (if needed), Segment, RecallJob, DiskCopy and updates
 * the subrequest.
 */
CREATE OR REPLACE PROCEDURE triggerRepackRecall
(srId IN INTEGER, cfId IN INTEGER, tapeId IN INTEGER, fileId IN INTEGER, nsHost IN VARCHAR2, block IN RAW,
 fseq IN INTEGER, copynb IN INTEGER, priority IN INTEGER, euid IN INTEGER, egid IN INTEGER) AS
  segId INTEGER;
  tcId INTEGER;
  dcId INTEGER;
  recallPolicy VARCHAR2(2048);
  dcpath VARCHAR2(2048);
  hexblock VARCHAR2(8);
  block0 INTEGER;
  block1 INTEGER;
  block2 INTEGER;
  block3 INTEGER;
BEGIN
  -- get needed ids
  SELECT ids_seq.nextval INTO segId FROM DUAL;
  SELECT ids_seq.nextval INTO tcId FROM DUAL;
  SELECT ids_seq.nextval INTO dcId FROM DUAL;
  -- insert a Segment
  hexblock := RAWTOHEX(block);
  block0 := TO_NUMBER(SUBSTR(hexblock,1,2),'XX');
  block1 := TO_NUMBER(SUBSTR(hexblock,3,2),'XX');
  block2 := TO_NUMBER(SUBSTR(hexblock,5,2),'XX');
  block3 := TO_NUMBER(SUBSTR(hexblock,7,2),'XX');
  INSERT INTO Segment (id, blockId0, blockId1, blockId2, blockId3, fseq, creationTime, status, copy, tape, priority)
  VALUES (segId, block0, block1, block2, block3, fseq, getTime(), tconst.SEGMENT_UNPROCESSED, tcId, tapeId, priority);
  -- insert a RecallJob
  INSERT INTO RecallJob (id, copyNb, status, castorFile, fseq)
  VALUES (tcId, copynb, tconst.RECALLJOB_TOBERECALLED, cfId, fseq);
  -- insert a DiskCopy
  buildPathFromFileId(fileId, nsHost, dcId, dcPath);
  INSERT INTO DiskCopy (id, status, creationTime, castorFile, ownerUid, ownerGid, path)
  VALUES (dcId, dconst.DISKCOPY_WAITTAPERECALL, getTime(), cfId, euid, egid, dcPath);
  -- udpate SubRequest
  UPDATE SubRequest SET diskcopy = dcId, status = dconst.SUBREQUEST_WAITTAPERECALL WHERE id = srId;
END;
/

/* PL/SQL method implementing processPrepareRequest */
/* the result output is a DiskCopy status for STAGED, DISK2DISKCOPY or RECALL,
   -1 for user failure, -2 for subrequest put in WAITSUBREQ */
CREATE OR REPLACE PROCEDURE processPrepareRequest
        (srId IN INTEGER, result OUT INTEGER) AS
  nbDCs INTEGER;
  svcClassId NUMBER;
  srvSvcClassId NUMBER;
  repack INTEGER;
  cfId NUMBER;
  srcDcId NUMBER;
  recSvcClass NUMBER;
  recDcId NUMBER;
  recRepack INTEGER;
  reuid NUMBER;
  regid NUMBER;
BEGIN
  -- retrieve the castorfile, the svcclass and the reqId for this subrequest
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         SubRequest.castorFile, Request.euid, Request.egid, Request.svcClass, Request.repack
    INTO cfId, reuid, regid, svcClassId, repack
    FROM (SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */
                 id, euid, egid, svcClass, 0 repack FROM StagePrepareToGetRequest UNION ALL
          SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */
                 id, euid, egid, svcClass, 1 repack FROM StageRepackRequest UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                 id, euid, egid, svcClass, 0 repack FROM StagePrepareToUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = srId;
  -- lock the castor file to be safe in case of two concurrent subrequest
  SELECT id INTO cfId FROM CastorFile WHERE id = cfId FOR UPDATE;

  -- Look for available diskcopies. Note that we never wait on other requests
  -- and we include WAITDISK2DISKCOPY as they are going to be available.
  -- For those ones, the filesystem link in the diskcopy table is set to 0,
  -- hence it is not possible to determine the service class via the
  -- filesystem -> diskpool -> svcclass relationship and the replication
  -- request is used.
  SELECT COUNT(*) INTO nbDCs FROM (
    SELECT DiskCopy.id
      FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
     WHERE DiskCopy.castorfile = cfId
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskpool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = svcClassId
       AND FileSystem.status = 0 -- PRODUCTION
       AND FileSystem.diskserver = DiskServer.id
       AND DiskServer.status = 0 -- PRODUCTION
       AND DiskCopy.status IN (dconst.DISKCOPY_STAGED, dconst.DISKCOPY_STAGEOUT, dconst.DISKCOPY_CANBEMIGR)
     UNION ALL
    SELECT /*+ INDEX(StageDiskCopyReplicaRequest I_StageDiskCopyReplic_DestDC) */ DiskCopy.id
      FROM DiskCopy, StageDiskCopyReplicaRequest
     WHERE DiskCopy.id = StageDiskCopyReplicaRequest.destDiskCopy
       AND StageDiskCopyReplicaRequest.svcclass = svcClassId
       AND DiskCopy.castorfile = cfId
       AND DiskCopy.status = 1); -- WAITDISK2DISKCOPY
  
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
        startRepackMigration(srId, cfId, srcDcId, reuid, regid);
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
          -- the file is being written/migrated. This may happen in two cases:
          -- either there's another repack going on for the same file, or another
          -- user is overwriting the file.
          -- In the first case, if this request comes for a tape other
          -- than the one being repacked, i.e. the file has a double tape copy,
          -- then we should make the request wait on the first repack (it may be
          -- for a different service class than the one being used right now).
          -- In the second case, we just have to fail this request.
          -- However at the moment the tapegateway does not handle double repacks,
          -- so we simply fail this repack and rely on Repack to submit
          -- such double tape repacks one by one.
          UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
             SET status = 9,  -- FAILED
                 errorCode = 16,  -- EBUSY
                 errorMessage = 'File is currently being written or migrated'
           WHERE id = srId;
          result := -1;  -- user error
        END;
      END;
    END IF;
    RETURN;
  END IF;

  -- No diskcopies available for this service class:
  -- we may have to schedule a disk to disk copy or trigger a recall
  checkForD2DCopyOrRecall(cfId, srId, reuid, regid, svcClassId, srcDcId, srvSvcClassId);
  IF srcDcId > 0 THEN  -- disk to disk copy
    createDiskCopyReplicaRequest(srId, srcDcId, srvSvcClassId, svcClassId, reuid, regid);
    result := 1;  -- DISKCOPY_WAITDISK2DISKCOPY, for logging purposes
  ELSIF srcDcId = 0 THEN  -- recall
    BEGIN
      -- check whether there's already a recall, and get its svcClass
      SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ Request.svcClass, DiskCopy.id, repack
        INTO recSvcClass, recDcId, recRepack
        FROM (SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ 
                     id, svcClass, 0 repack FROM StagePrepareToGetRequest UNION ALL
              SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                     id, svcClass, 0 repack FROM StageGetRequest UNION ALL
              SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */
                     id, svcClass, 1 repack FROM StageRepackRequest UNION ALL
              SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                     id, svcClass, 0 repack FROM StageUpdateRequest UNION ALL
              SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                     id, svcClass, 0 repack FROM StagePrepareToUpdateRequest) Request,
             SubRequest, DiskCopy
       WHERE SubRequest.request = Request.id
         AND SubRequest.castorFile = cfId
         AND DiskCopy.castorFile = cfId
         AND DiskCopy.status = 2  -- WAITTAPERECALL
         AND SubRequest.status = 4;  -- WAITTAPERECALL
      -- we found one: we need to wait if either we are in a different svcClass
      -- so that afterwards a disk-to-disk copy is triggered, or in case of
      -- Repack so to trigger the repack migration. We also protect ourselves
      -- from a double repack request on the same file.
      IF repack = 1 AND recRepack = 1 THEN
        -- we are not able to handle a double repack, see the detailed comment above
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
           SET status = 7,  -- FAILED
               errorCode = 16,  -- EBUSY
               errorMessage = 'File is currently being repacked'
         WHERE id = srId;
        result := -1;  -- user error
        RETURN;
      END IF;
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
/


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
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id) INDEX(Req PK_StagePutDoneRequest_Id) */
         Req.svcclass, SubRequest.castorfile
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
    SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ id INTO putSubReq
      FROM SubRequest
     WHERE castorfile = cfId
       AND reqType IN (40, 44)  -- Put, Update
       AND status IN (0, 1, 2, 3, 6, 13, 14) -- START, RESTART, RETRY, WAITSCHED, READY, READYFORSCHED, BEINGSCHED
       AND ROWNUM < 2;
    -- we've found one, we wait
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
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
    IF nbDCs = 0 THEN
      -- This is a PutDone without a put (otherwise we would have found
      -- a DiskCopy on a FileSystem), so we fail the subrequest.
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET
        status = 7,  -- FAILED
        errorCode = 1,  -- EPERM
        errorMessage = 'putDone without a put, or wrong service class'
      WHERE id = rsubReqId;
      result := 0;  -- no go
      RETURN;
    END IF;
    -- All checks have been completed, let's do it
    putDoneFunc(cfId, fs, 2, svcClassId);   -- context = PutDone
    result := 1;
  END;
END;
/


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
  fclassId INTEGER;
  sclassId INTEGER;
  putSC INTEGER;
  pputSC INTEGER;
  contextPIPP INTEGER;
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  -- Get data and lock access to the CastorFile
  -- This, together with triggers will avoid new migration/recall jobs
  -- or DiskCopies to be added
  SELECT fileclass INTO fclassId FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Determine the context (Put inside PrepareToPut ?)
  BEGIN
    -- check that we are a Put/Update
    SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ Request.svcClass INTO putSC
      FROM (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                   id, svcClass FROM StagePutRequest UNION ALL
            SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                   id, svcClass FROM StageUpdateRequest) Request, SubRequest
     WHERE SubRequest.id = srId
       AND Request.id = SubRequest.request;
    BEGIN
      -- check that there is a PrepareToPut/Update going on. There can be only a single one
      -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
      -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
      SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest.diskCopy,
             PrepareRequest.svcClass INTO dcId, pputSC
        FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */
                     id, svcClass FROM StagePrepareToPutRequest UNION ALL
              SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                     id, svcClass FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
       WHERE SubRequest.CastorFile = cfId
         AND PrepareRequest.id = SubRequest.request
         AND SubRequest.status = 6;  -- READY
      -- if we got here, we are a Put/Update inside a PrepareToPut
      -- however, are we in the same service class ?
      IF putSC != pputSC THEN
        -- No, this means we are a Put/Update and another PrepareToPut
        -- is already running in a different service class. This is forbidden
        dcId := 0;
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
           SET status = 7, -- FAILED
               errorCode = 16, -- EBUSY
               errorMessage = 'A prepareToPut is running in another service class for this file'
         WHERE id = srId;
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
      -- In case of prepareToPut we need to check that we are the only one
      SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ count(SubRequest.diskCopy) INTO nbPReqs
        FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id
                FROM StagePrepareToPutRequest UNION ALL
              SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id
                FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
       WHERE SubRequest.castorFile = cfId
         AND PrepareRequest.id = SubRequest.request
         AND SubRequest.status = 6;  -- READY
      -- Note that we did not select ourselves (we are in status 3)
      IF nbPReqs > 0 THEN
        -- this means we are a PrepareToPut and another PrepareToPut/Update
        -- is already running. This is forbidden
        dcId := 0;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 7, -- FAILED
               errorCode = 16, -- EBUSY
               errorMessage = 'Another prepareToPut/Update is ongoing for this file'
         WHERE id = srId;
        RETURN;
      END IF;
      -- Everything is ok then
      contextPIPP := 0;
    END;
  END;
  -- check if there is space in the diskpool in case of
  -- disk only pool
  -- First get the svcclass and the user
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ svcClass, euid, egid INTO sclassId, ouid, ogid
    FROM Subrequest,
         (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                 id, svcClass, euid, egid FROM StagePutRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                 id, svcClass, euid, egid FROM StageUpdateRequest UNION ALL
          SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */
                 id, svcClass, euid, egid FROM StagePrepareToPutRequest UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                 id, svcClass, euid, egid FROM StagePrepareToUpdateRequest) Request
   WHERE SubRequest.id = srId
     AND Request.id = SubRequest.request;
  IF checkFailJobsWhenNoSpace(sclassId) = 1 THEN
    -- The svcClass is declared disk only and has no space
    -- thus we cannot recreate the file
    dcId := 0;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 7, -- FAILED
           errorCode = 28, -- ENOSPC
           errorMessage = 'File creation canceled since disk pool is full'
     WHERE id = srId;
    RETURN;
  END IF;
  IF contextPIPP = 0 THEN
    -- Puts inside PrepareToPuts don't need the following checks
    -- check if the file can be routed to tape
    IF checkNoTapeRouting(sclassId, fclassId) = 1 THEN
      -- We could not route the file to tape, so let's fail the opening
      dcId := 0;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7, -- FAILED
             errorCode = 1727, -- ESTNOTAPEROUTE
             errorMessage = 'File recreation canceled since the file cannot be routed to tape'
       WHERE id = srId;
      RETURN;
    END IF;
    -- check if recreation is possible for TapeCopies
    SELECT count(*) INTO nbRes FROM MigrationJob
     WHERE status = tconst.MIGRATIONJOB_SELECTED
      AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot recreate
      dcId := 0;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7, -- FAILED
             errorCode = 16, -- EBUSY
             errorMessage = 'File recreation canceled since file is being migrated'
        WHERE id = srId;
      RETURN;
    END IF;
    -- check if recreation is possible for DiskCopies
    SELECT count(*) INTO nbRes FROM DiskCopy
     WHERE status IN (1, 2, 5, 6, 11) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT, WAITFS_SCHEDULING
       AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot recreate
      dcId := 0;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7, -- FAILED
             errorCode = 16, -- EBUSY
             errorMessage = 'File recreation canceled since file is either being recalled, or replicated or created by another user'
       WHERE id = srId;
      RETURN;
    END IF;
    -- delete ongoing recalls
    deleteRecallJobs(cfId);
    -- delete ongoing migrations
    deleteMigrationJobs(cfId);
    -- set DiskCopies to INVALID
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE castorFile = cfId AND status IN (0, 10); -- STAGED, CANBEMIGR
    -- create new DiskCopy
    SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfId;
    SELECT ids_seq.nextval INTO dcId FROM DUAL;
    buildPathFromFileId(fid, nh, dcId, rpath);
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime, lastAccessTime, gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
         VALUES (rpath, dcId, 0, cfId, 5, getTime(), getTime(), 0, 0, 0, ouid, ogid); -- status WAITFS
    rstatus := 5; -- WAITFS
    rmountPoint := '';
    rdiskServer := '';
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
          SELECT /*+ INDEX(Subrequest I_Subrequest_DiskCopy)*/ id INTO srParent
            FROM SubRequest
           WHERE reqType IN (40, 44)  -- Put, Update
             AND diskCopy = dcId
             AND status IN (13, 14, 6)  -- READYFORSCHED, BEINGSCHED, READY
             AND ROWNUM < 2;   -- if we have more than one just take one of them
          UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
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
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET diskCopy = dcId,
         lastModificationTime = getTime()
   WHERE id = srId;
  -- we don't commit here, the stager will do that when
  -- the subRequest status will be updated to 6
END;
/

/* This procedure resets the lastKnownFileName the CastorFile that has a given name
   inside an autonomous transaction. This should be called before creating/renaming any
   CastorFile so that lastKnownFileName stays unique */
CREATE OR REPLACE PROCEDURE dropReusedLastKnownFileName(fileName IN VARCHAR2) AS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  UPDATE /*+ INDEX (I_CastorFile_lastKnownFileName) */ CastorFile
     SET lastKnownFileName = TO_CHAR(id)
   WHERE lastKnownFileName = normalizePath(fileName);
  COMMIT;
END;
/

/* PL/SQL method implementing selectCastorFile
 * This is only a wrapper on selectCastorFileInternal
 */
CREATE OR REPLACE PROCEDURE selectCastorFile (fId IN INTEGER,
                                              nh IN VARCHAR2,
                                              fc IN INTEGER,
                                              fs IN INTEGER,
                                              fn IN VARCHAR2,
                                              srId IN NUMBER,
                                              lut IN NUMBER,
                                              rid OUT INTEGER,
                                              rfs OUT INTEGER) AS
  nsHostName VARCHAR2(2048);
BEGIN
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- call internal method
  selectCastorFileInternal(fId, nsHostName, fc, fs, fn, srId, lut, TRUE, rid, rfs);
END;
/

/* PL/SQL method implementing selectCastorFile */
CREATE OR REPLACE PROCEDURE selectCastorFileInternal (fId IN INTEGER,
                                                      nh IN VARCHAR2,
                                                      fc IN INTEGER,
                                                      fs IN INTEGER,
                                                      fn IN VARCHAR2,
                                                      srId IN NUMBER,
                                                      lut IN NUMBER,
                                                      waitForLock IN BOOLEAN,
                                                      rid OUT INTEGER,
                                                      rfs OUT INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  previousLastKnownFileName VARCHAR2(2048);
  fcId NUMBER;
BEGIN
  -- Resolve the fileclass
  BEGIN
    SELECT id INTO fcId FROM FileClass WHERE classId = fc;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR (-20010, 'File class '|| fc ||' not found in database');
  END;
  BEGIN
    -- try to find an existing file
    SELECT id, fileSize, lastKnownFileName
      INTO rid, rfs, previousLastKnownFileName
      FROM CastorFile
     WHERE fileId = fid AND nsHost = nh;
    -- In case its filename has changed, take care that the new name is
    -- not already the lastKnownFileName of another file, that was also
    -- renamed but for which the lastKnownFileName has not been updated
    -- We actually reset the lastKnownFileName of such a file if needed
    -- Note that this procedure will run in an autonomous transaction so that
    -- no dead lock can result from taking a second lock within this transaction
    IF fn != previousLastKnownFileName THEN
      dropReusedLastKnownFileName(fn);
    END IF;
    -- take a lock on the file. Note that the file may have disappeared in the
    -- meantime, this is why we first select (potentially having a NO_DATA_FOUND
    -- exception) before we update.
    IF waitForLock THEN
      SELECT id INTO rid FROM CastorFile WHERE id = rid FOR UPDATE;
    ELSE
      SELECT id INTO rid FROM CastorFile WHERE id = rid FOR UPDATE NOWAIT;
    END IF;
    -- The file is still there, so update lastAccessTime and lastKnownFileName.
    UPDATE CastorFile SET lastAccessTime = getTime(),
                          lastKnownFileName = normalizePath(fn)
     WHERE id = rid;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET castorFile = rid
     WHERE id = srId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- we did not find the file, let's create a new one
    -- take care that the name of the new file is not already the lastKnownFileName
    -- of another file, that was renamed but for which the lastKnownFileName has
    -- not been updated.
    -- We actually reset the lastKnownFileName of such a file if needed
    -- Note that this procedure will run in an autonomous transaction so that
    -- no dead lock can result from taking a second lock within this transaction
    dropReusedLastKnownFileName(fn);
    -- insert new row
    INSERT INTO CastorFile (id, fileId, nsHost, fileClass, fileSize,
                            creationTime, lastAccessTime, lastUpdateTime, lastKnownFileName)
      VALUES (ids_seq.nextval, fId, nh, fcId, fs, getTime(), getTime(), lut, normalizePath(fn))
      RETURNING id, fileSize INTO rid, rfs;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET castorFile = rid
     WHERE id = srId;
  END;
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  -- the violated constraint indicates that the file was created by another client
  -- while we were trying to create it ourselves. We can thus use the newly created file
  IF waitForLock THEN
    SELECT id, fileSize INTO rid, rfs FROM CastorFile
      WHERE fileId = fid AND nsHost = nh FOR UPDATE;
  ELSE
    SELECT id, fileSize INTO rid, rfs FROM CastorFile
      WHERE fileId = fid AND nsHost = nh FOR UPDATE NOWAIT;
  END IF;
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET castorFile = rid
   WHERE id = srId;
END;
/

/* PL/SQL method implementing stageForcedRm */
CREATE OR REPLACE PROCEDURE stageForcedRm (fid IN INTEGER,
                                           nh IN VARCHAR2,
                                           inGcType IN INTEGER DEFAULT NULL) AS
  cfId INTEGER;
  nbRes INTEGER;
  dcsToRm "numList";
  nsHostName VARCHAR2(2048);
BEGIN
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new migration/recall jobs
  -- or DiskCopies to be added
  SELECT id INTO cfId FROM CastorFile
   WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
  -- list diskcopies
  SELECT id BULK COLLECT INTO dcsToRm
    FROM DiskCopy
   WHERE castorFile = cfId
     AND status IN (0, 5, 6, 10, 11);  -- STAGED, WAITFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
  -- Stop ongoing recalls
  deleteRecallJobs(cfId);
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  FOR sr IN (SELECT /*+ INDEX(Subrequest I_Subrequest_DiskCopy)*/ id, status FROM SubRequest
              WHERE diskcopy IN
                (SELECT /*+ CARDINALITY(dcidTable 5) */ *
                   FROM TABLE(dcsToRm) dcidTable)
                AND status IN (0, 1, 2, 5, 6, 12, 13)) LOOP   -- START, RESTART, RETRY, WAITSUBREQ, READY, READYFORSCHED
    UPDATE SubRequest
       SET status = 7,  -- FAILED
           errorCode = 4,  -- EINTR
           errorMessage = 'Canceled by another user request',
           parent = 0
     WHERE (id = sr.id) OR (parent = sr.id);
  END LOOP;
  -- Set selected DiskCopies to INVALID
  FORALL i IN dcsToRm.FIRST .. dcsToRm.LAST
    UPDATE DiskCopy
       SET status = 7, -- INVALID
           gcType = inGcType
     WHERE id = dcsToRm(i);
END;
/


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
  srIds "numList";
  dcStatus INTEGER;
  nsHostName VARCHAR2(2048);
  migSvcClass NUMBER;
BEGIN
  ret := 0;
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  BEGIN
    -- Lock the access to the CastorFile
    -- This, together with triggers will avoid new migration/recall jobs
    -- or DiskCopies to be added
    SELECT id INTO cfId FROM CastorFile
     WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- This file does not exist in the stager catalog
    -- so we just fail the request
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 7,  -- FAILED
           errorCode = 2,  -- ENOENT
           errorMessage = 'File not found on disk cache'
     WHERE id = srId;
    RETURN;
  END;
  -- First select involved diskCopies
  scId := svcClassId;
  dcStatus := 0;
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
      -- and then diskcopies resulting from ongoing requests, for which the previous
      -- query wouldn't return any entry because of e.g. missing filesystem
      SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ DC.id
        FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id
                FROM StagePrepareToPutRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id
                FROM StagePrepareToGetRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id
                FROM StagePrepareToUpdateRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id
                FROM StageRepackRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ id
                FROM StagePutRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id
                FROM StageGetRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id
                FROM StageUpdateRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */ id
                FROM StageDiskCopyReplicaRequest WHERE svcClass = scId) Request,
             SubRequest, DiskCopy DC
       WHERE SubRequest.diskCopy = DC.id
         AND Request.id = SubRequest.request
         AND DC.castorFile = cfId
         AND DC.status IN (1, 2, 5, 11)  -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, WAITFS_SCHEDULING
      );
    IF dcsToRm.COUNT = 0 THEN
      -- We didn't find anything on this svcClass, fail and return
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7,  -- FAILED
             errorCode = 2,  -- ENOENT
             errorMessage = 'File not found on this service class'
       WHERE id = srId;
      RETURN;
    END IF;
    -- Select current status of the diskCopies
    SELECT status INTO dcStatus
      FROM DiskCopy
     WHERE id = dcsToRm(1);
    IF dcStatus != dconst.DISKCOPY_CANBEMIGR THEN
      -- Check whether something else is left: if not, do as
      -- if we are performing a stageRm everywhere.
      SELECT count(*) INTO nbRes FROM DiskCopy
         WHERE castorFile = cfId
           AND status IN (0, 2, 5, 6, 10, 11)  -- STAGED, WAITTAPERECALL, STAGEOUT, CANBEMIGR, WAITFS, WAITFS_SCHEDULING
           AND id NOT IN (SELECT /*+ CARDINALITY(dcidTable 5) */ *
                            FROM TABLE(dcsToRm) dcidTable);
      IF nbRes = 0 THEN
        -- nothing found, so we're dropping the last copy; then
        -- we need to perform all the checks to make sure we can
        -- allow the removal.
        scId := 0;
      END IF;
    END IF;
  END IF;

  IF scId = 0 THEN
    -- full cleanup is to be performed, check for migrations beforehand
    SELECT count(*) INTO nbRes FROM DiskCopy
     WHERE status = 10 -- DISKCOPY_CANBEMIGR
       AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot remove
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7,  -- FAILED
             errorCode = 16,  -- EBUSY
             errorMessage = 'The file is not yet migrated'
       WHERE id = srId;
      RETURN;
    END IF;
    -- No migration running. Let's check if we have the file at all
    SELECT count(*) INTO nbRes FROM DiskCopy
     WHERE castorFile = cfId
       AND status NOT IN (4, 7, 9);  -- anything but FAILED, INVALID, BEINGDELETED
    IF nbRes = 0 THEN
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7,  -- FAILED
             errorCode = 2,  -- ENOENT
             errorMessage = 'File not found on disk cache'
       WHERE id = srId;
      RETURN;
    END IF;
    -- nothing running and we have the file, let's cancel recalls
    deleteRecallJobs(cfId);
    -- Reselect what needs to be removed
    SELECT id BULK COLLECT INTO dcsToRm
      FROM DiskCopy
     WHERE castorFile = cfId
       AND status IN (0, 1, 2, 5, 6, 10, 11);  -- STAGED, WAIT*, STAGEOUT, CANBEMIGR
  END IF;

  -- Now perform the remove:
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  SELECT /*+ INDEX(SR I_SubRequest_DiskCopy) */ id
    BULK COLLECT INTO srIds
    FROM SubRequest SR
   WHERE diskcopy IN
     (SELECT /*+ CARDINALITY(dcidTable 5) */ * 	 
        FROM TABLE(dcsToRm) dcidTable) 	 
     AND status IN (0, 1, 2, 4, 5, 6, 13); -- START, RESTART, RETRY, WAITTAPERECALL, WAITSUBREQ, READY, READYFORSCHED
  IF srIds.COUNT > 0 THEN
    DECLARE
      srUuid VARCHAR(2048);
    BEGIN
      FOR i IN srIds.FIRST .. srIds.LAST LOOP
        SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ subreqId INTO srUuid
          FROM SubRequest
         WHERE SubRequest.id = srIds(i);
        UPDATE SubRequest
           SET status = CASE
                 WHEN status IN (6, 13) AND reqType = 133 THEN 9 ELSE 7
               END,  -- FAILED_FINISHED for DiskCopyReplicaRequests in status READYFORSCHED or READY, otherwise FAILED
               -- this so that user requests in status WAITSUBREQ are always marked FAILED even if they wait on a replication
               errorCode = 4,  -- EINTR
               errorMessage = 'Canceled by another user request'
         WHERE id = srIds(i) OR parent = srIds(i);
        -- make the scheduler aware so that it can remove the transfer from the queues if needed
        INSERT INTO TransfersToAbort VALUES (srUuid);
      END LOOP;
    END;
    -- wake up the scheduler so that it can remove the transfer from the queues now
    DBMS_ALERT.SIGNAL('transfersToAbort', '');
  END IF;
  -- Set selected DiskCopies to either INVALID or FAILED. We deliberately
  -- ignore WAITDISK2DISKCOPY's (see bug #78826)
  FOR i IN dcsToRm.FIRST .. dcsToRm.LAST LOOP
    SELECT status INTO dcStatus
      FROM DiskCopy
     WHERE id = dcsToRm(i);
    UPDATE DiskCopy
       -- WAITTAPERECALL,WAITFS[_SCHED] -> FAILED, others -> INVALID
       SET status = decode(status, 2,4, 5,4, 11,4, 7)
     WHERE id = dcsToRm(i);
  END LOOP;
  ret := 1;  -- ok
END;
/


/* PL/SQL method implementing a setFileGCWeight request */
CREATE OR REPLACE PROCEDURE setFileGCWeightProc
(fid IN NUMBER, nh IN VARCHAR2, svcClassId IN NUMBER, weight IN FLOAT, ret OUT INTEGER) AS
  CURSOR dcs IS
  SELECT DiskCopy.id, gcWeight
    FROM DiskCopy, CastorFile
   WHERE castorFile.id = diskcopy.castorFile
     AND fileid = fid
     AND nshost = getConfigOption('stager', 'nsHost', nh)
     AND fileSystem IN (
       SELECT FileSystem.id
         FROM FileSystem, DiskPool2SvcClass D2S
        WHERE FileSystem.diskPool = D2S.parent
          AND D2S.child = svcClassId);
  gcwProc VARCHAR(2048);
  gcw NUMBER;
BEGIN
  ret := 0;
  -- get gc userSetGCWeight function to be used, if any
  gcwProc := castorGC.getUserSetGCWeight(svcClassId);
  -- loop over diskcopies and update them
  FOR dc in dcs LOOP
    gcw := dc.gcWeight;
    -- compute actual gc weight to be used
    IF gcwProc IS NOT NULL THEN
      EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:oldGcw, :delta); END;'
        USING OUT gcw, IN gcw, weight;
    END IF;
    -- update DiskCopy
    UPDATE DiskCopy SET gcWeight = gcw WHERE id = dc.id;
    ret := 1;   -- some diskcopies found, ok
  END LOOP;
END;
/


/* PL/SQL method implementing updateAndCheckSubRequest */
CREATE OR REPLACE PROCEDURE updateAndCheckSubRequest(srId IN INTEGER, newStatus IN INTEGER, result OUT INTEGER) AS
  reqId INTEGER;
  rName VARCHAR2(100);
BEGIN
  -- Update Status
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET status = newStatus,
         answered = 1,
         lastModificationTime = getTime(),
         getNextStatus = decode(newStatus, 6, 1, 8, 1, 9, 1, 0)  -- READY, FINISHED or FAILED_FINISHED -> GETNEXTSTATUS_FILESTAGED
   WHERE id = srId
   RETURNING request, (SELECT object FROM Type2Obj WHERE type = reqType) INTO reqId, rName;
  -- Lock the access to the Request
  EXECUTE IMMEDIATE
    'BEGIN SELECT id INTO :reqId FROM '|| rName ||' WHERE id = :reqId FOR UPDATE; END;'
    USING IN OUT reqId;
  -- Check whether it was the last subrequest in the request
  SELECT /*+ INDEX(Subrequest I_Subrequest_Request)*/ id INTO result FROM SubRequest
   WHERE request = reqId
     AND status IN (0, 1, 2, 3, 4, 5, 7, 10, 12, 13, 14)   -- all but FINISHED, FAILED_FINISHED, ARCHIVED
     AND answered = 0
     AND ROWNUM < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN
  result := 0;
  -- No data found means we were last; check whether we have to archive
  IF newStatus IN (8, 9) THEN
    archiveSubReq(srId, newStatus);
  END IF;
END;
/

/* PL/SQL function to elect a rmmaster master */
CREATE OR REPLACE FUNCTION isMonitoringMaster RETURN NUMBER IS
  locked EXCEPTION;
  PRAGMA EXCEPTION_INIT (locked, -54);
BEGIN
  LOCK TABLE RmMasterLock IN EXCLUSIVE MODE NOWAIT;
  RETURN 1;
EXCEPTION WHEN locked THEN
  RETURN 0;
END;
/

/* PL/SQL method implementing storeClusterStatus */
CREATE OR REPLACE PROCEDURE storeClusterStatus
(machines IN castor."strList",
 fileSystems IN castor."strList",
 machineValues IN castor."cnumList",
 fileSystemValues IN castor."cnumList") AS
 found   NUMBER;
 ind     NUMBER;
 dsId    NUMBER := 0;
 fs      NUMBER := 0;
 fsIds   "numList";
BEGIN
  -- Sanity check
  IF machines.COUNT = 0 OR fileSystems.COUNT = 0 THEN
    RETURN;
  END IF;
  -- First Update Machines
  FOR i IN machines.FIRST .. machines.LAST LOOP
    ind := machineValues.FIRST + 9 * (i - machines.FIRST);
    IF machineValues(ind + 1) = 3 THEN -- ADMIN DELETED
      BEGIN
        -- Resolve the machine name to its id
        SELECT id INTO dsId FROM DiskServer
         WHERE name = machines(i);
        -- If any of the filesystems belonging to the diskserver are currently
        -- in the process of being drained then do not delete the diskserver or
        -- its associated filesystems. Why? Because of unique constraint
        -- violations between the FileSystem and DrainingDiskCopy table.
        SELECT fileSystem BULK COLLECT INTO fsIds
          FROM DrainingFileSystem DFS, FileSystem FS
         WHERE DFS.fileSystem = FS.id
           AND FS.diskServer = dsId;
        IF fsIds.COUNT > 0 THEN
          -- Entries found so flag the draining process as DELETING
          UPDATE DrainingFileSystem
             SET status = 6  -- DELETING
           WHERE fileSystem IN
             (SELECT /*+ CARDINALITY(fsIdTable 5) */ *
                FROM TABLE (fsIds) fsIdTable);
        ELSE
          -- There is no outstanding process to drain the diskservers
          -- filesystems so we can now delete it.
          DELETE FROM FileSystem WHERE diskServer = dsId;
          DELETE FROM DiskServer WHERE name = machines(i);
        END IF;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        NULL;  -- Already deleted
      END;
    ELSE
      BEGIN
        SELECT id INTO dsId FROM DiskServer
         WHERE name = machines(i);
        UPDATE DiskServer
           SET status             = machineValues(ind),
               adminStatus        = machineValues(ind + 1),
               readRate           = machineValues(ind + 2),
               writeRate          = machineValues(ind + 3),
               nbReadStreams      = machineValues(ind + 4),
               nbWriteStreams     = machineValues(ind + 5),
               nbReadWriteStreams = machineValues(ind + 6),
               nbMigratorStreams  = machineValues(ind + 7),
               nbRecallerStreams  = machineValues(ind + 8)
         WHERE name = machines(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- We should insert a new machine here
        INSERT INTO DiskServer (name, id, status, adminStatus, readRate,
                                writeRate, nbReadStreams, nbWriteStreams,
                                nbReadWriteStreams, nbMigratorStreams,
                                nbRecallerStreams)
         VALUES (machines(i), ids_seq.nextval, machineValues(ind),
                 machineValues(ind + 1), machineValues(ind + 2),
                 machineValues(ind + 3), machineValues(ind + 4),
                 machineValues(ind + 5), machineValues(ind + 6),
                 machineValues(ind + 7), machineValues(ind + 8));
      END;
    END IF;
    -- Release the lock on the DiskServer as soon as possible to prevent
    -- deadlocks with other activities e.g. recaller
    COMMIT;
  END LOOP;
  -- And then FileSystems
  ind := fileSystemValues.FIRST;
  FOR i in fileSystems.FIRST .. fileSystems.LAST LOOP
    IF fileSystems(i) NOT LIKE ('/%') THEN
      SELECT id INTO dsId FROM DiskServer
       WHERE name = fileSystems(i);
    ELSE
      IF fileSystemValues(ind + 1) = 3 THEN -- ADMIN DELETED
        BEGIN
          -- Resolve the mountpoint name to its id
          SELECT id INTO fs
            FROM FileSystem
           WHERE mountPoint = fileSystems(i)
             AND diskServer = dsId;
          -- Check to see if the filesystem is currently in the process of
          -- being drained. If so, we flag it for deletion.
          found := 0;
          UPDATE DrainingFileSystem
             SET status = 6  -- DELETING
           WHERE fileSystem = fs
          RETURNING fs INTO found;
          -- No entry found so delete the filesystem.
          IF found = 0 THEN
            DELETE FROM FileSystem WHERE id = fs;
          END IF;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          NULL;  -- Already deleted
        END;
      ELSE
        BEGIN
          SELECT diskServer INTO dsId FROM FileSystem
           WHERE mountPoint = fileSystems(i) AND diskServer = dsId;
          UPDATE FileSystem
             SET status              = fileSystemValues(ind),
                 adminStatus         = fileSystemValues(ind + 1),
                 readRate            = fileSystemValues(ind + 2),
                 writeRate           = fileSystemValues(ind + 3),
                 nbReadStreams       = fileSystemValues(ind + 4),
                 nbWriteStreams      = fileSystemValues(ind + 5),
                 nbReadWriteStreams  = fileSystemValues(ind + 6),
                 nbMigratorStreams   = fileSystemValues(ind + 7),
                 nbRecallerStreams   = fileSystemValues(ind + 8),
                 free                = fileSystemValues(ind + 9),
                 totalSize           = fileSystemValues(ind + 10),
                 minFreeSpace        = fileSystemValues(ind + 11),
                 maxFreeSpace        = fileSystemValues(ind + 12),
                 minAllowedFreeSpace = fileSystemValues(ind + 13)
           WHERE mountPoint          = fileSystems(i)
             AND diskServer          = dsId;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- we should insert a new filesystem here
          INSERT INTO FileSystem (free, mountPoint, minFreeSpace,
                                  minAllowedFreeSpace, maxFreeSpace, totalSize,
                                  readRate, writeRate, nbReadStreams,
                                  nbWriteStreams, nbReadWriteStreams,
                                  nbMigratorStreams, nbRecallerStreams, id,
                                  diskPool, diskserver, status, adminStatus)
          VALUES (fileSystemValues(ind + 9), fileSystems(i), fileSystemValues(ind+11),
                  fileSystemValues(ind + 13), fileSystemValues(ind + 12),
                  fileSystemValues(ind + 10), fileSystemValues(ind + 2),
                  fileSystemValues(ind + 3), fileSystemValues(ind + 4),
                  fileSystemValues(ind + 5), fileSystemValues(ind + 6),
                  fileSystemValues(ind + 7), fileSystemValues(ind + 8),
                  ids_seq.nextval, 0, dsId, 2, 1); -- FILESYSTEM_DISABLED, ADMIN_FORCE
        END;
      END IF;
      ind := ind + 14;
    END IF;
    -- Release the lock on the FileSystem as soon as possible to prevent
    -- deadlocks with other activities e.g. recaller
    COMMIT;
  END LOOP;
END;
/


/* PL/SQL method implementing selectPriority */
CREATE OR REPLACE PROCEDURE selectPriority(
  inUid IN INTEGER,
  inGid IN INTEGER,
  inPriority IN INTEGER,
  dbInfo OUT castor.TapeAccessPriority_Cur) AS
BEGIN
  OPEN dbInfo FOR
    SELECT euid, egid, priority FROM PriorityMap
     WHERE (euid = inUid OR inUid = -1)
       AND (egid = inGid OR inGid = -1)
       AND (priority = inPriority OR inPriority = -1);
END;
/

/* PL/SQL method implementing enterPriority
   it can raise constraint violation exception */
CREATE OR REPLACE PROCEDURE enterPriority(
  inUid IN NUMBER,
  inGid IN NUMBER,
  inPriority IN INTEGER) AS
BEGIN
  INSERT INTO PriorityMap (euid, egid, priority)
  VALUES (inUid, inGid, inPriority);
END;
/


/* PL/SQL method implementing deletePriority */
CREATE OR REPLACE PROCEDURE deletePriority(
  inUid IN INTEGER,
  inGid IN INTEGER) AS
BEGIN
  DELETE FROM PriorityMap
   WHERE (euid = inUid OR inUid = -1)
     AND (egid = inGid OR inGid = -1);
END;
/
/*******************************************************************
 *
 * @(#)RCSfile: oracleJob.sql,v  Revision: 1.687  Date: 2009/07/31 15:17:04  Author: waldron 
 *
 * PL/SQL code for scheduling and job handling
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/


/* This method should be called when the first byte is written to a file
 * opened with an update. This will kind of convert the update from a
 * get to a put behavior.
 */
CREATE OR REPLACE PROCEDURE firstByteWrittenProc(srId IN INTEGER) AS
  dcId NUMBER;
  cfId NUMBER;
  nbres NUMBER;
  stat NUMBER;
  fclassId NUMBER;
  sclassId NUMBER;
BEGIN
  -- Get data and lock the CastorFile
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ castorfile, diskCopy
    INTO cfId, dcId
    FROM SubRequest WHERE id = srId;
  SELECT fileclass INTO fclassId FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Check that the file is not busy, i.e. that we are not
  -- in the middle of migrating it. If we are, just stop and raise
  -- a user exception
  SELECT count(*) INTO nbRes FROM MigrationJob
    WHERE status = tconst.MIGRATIONJOB_SELECTED
    AND castorFile = cfId;
  IF nbRes > 0 THEN
    raise_application_error(-20106, 'Trying to update a busy file (ongoing migration)');
  END IF;
  -- Check that we can indeed open the file in write mode
  -- 3 criteria have to be met :
  --   - we are not using a INVALID copy (we should not update an old version)
  --   - we are not in a disk only svcClass and the file class asks for tape copy
  --   - there is no other update/put going on or there is a prepareTo and we are
  --     dealing with the same copy.
  SELECT status INTO stat FROM DiskCopy WHERE id = dcId;
  -- First the invalid case
  IF stat = 7 THEN -- INVALID
    raise_application_error(-20106, 'Trying to update an invalid copy of a file (file has been modified by somebody else concurrently)');
  END IF;
  -- Then the disk only check
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id) INDEX(Request PK_StageUpdateRequest_Id) */ svcClass INTO sclassId
    FROM Subrequest, StageUpdateRequest Request
   WHERE SubRequest.id = srId
     AND Request.id = SubRequest.request;
  IF checkNoTapeRouting(sclassId, fclassId) = 1 THEN
     raise_application_error(-20106, 'File update canceled since the file cannot be routed to tape');
  END IF;
  -- Otherwise, either we are alone or we are on the right copy and we
  -- only have to check that there is a prepareTo statement. We do the check
  -- only when needed, that is STAGEOUT case
  IF stat = 6 THEN -- STAGEOUT
    BEGIN
      -- do we have other ongoing requests ?
      SELECT /*+ INDEX(Subrequest I_Subrequest_DiskCopy)*/ count(*) INTO nbRes
        FROM SubRequest
       WHERE diskCopy = dcId AND id != srId;
      IF (nbRes > 0) THEN
        -- do we have a prepareTo Request ? There can be only a single one
        -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
        -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
        SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest.id INTO nbRes
          FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id FROM StagePrepareToPutRequest UNION ALL
                SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id FROM StagePrepareToUpdateRequest) PrepareRequest,
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
     WHERE castorFile = cfId
       AND status IN (0, 10);
    -- except the one we are dealing with that goes to STAGEOUT
    UPDATE DiskCopy
       SET status = 6 -- STAGEOUT
     WHERE id = dcid;
    -- Suppress all Migration Jobs (avoid migration of previous version of the file)
    deleteMigrationJobs(cfId);
  END IF;
  -- Invalidate any ongoing replications
  UPDATE DiskCopy SET status = 7 -- INVALID
   WHERE castorFile = cfId
     AND status = 1; -- WAITDISK2DISKCOPY
END;
/


/* Checks whether the protocol used is supporting updates and when not
 * calls firstByteWrittenProc as if the file was already modified */
CREATE OR REPLACE PROCEDURE handleProtoNoUpd
(srId IN INTEGER, protocol VARCHAR2) AS
BEGIN
  IF protocol != 'rfio'  AND
     protocol != 'rfio3' AND
     protocol != 'xroot' THEN
    firstByteWrittenProc(srId);
  END IF;
END;
/


/* PL/SQL method implementing putStart */
CREATE OR REPLACE PROCEDURE putStart
        (srId IN INTEGER, selectedDiskServer IN VARCHAR2, selectedMountPoint IN VARCHAR2,
         rdcId OUT INTEGER, rdcStatus OUT INTEGER, rdcPath OUT VARCHAR2) AS
  srStatus INTEGER;
  srSvcClass INTEGER;
  fsId INTEGER;
  prevFsId INTEGER;
BEGIN
  -- Get diskCopy and subrequest related information
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         SubRequest.diskCopy, SubRequest.status, DiskCopy.fileSystem,
         Request.svcClass
    INTO rdcId, srStatus, prevFsId, srSvcClass
    FROM SubRequest, DiskCopy,
         (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ id, svcClass FROM StagePutRequest UNION ALL
          SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClass FROM StageGetRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClass FROM StageUpdateRequest) Request
   WHERE SubRequest.diskcopy = Diskcopy.id
     AND SubRequest.id = srId
     AND SubRequest.request = Request.id;
  -- Check that we did not cancel the SubRequest in the mean time
  IF srStatus IN (7, 9, 10) THEN -- FAILED, FAILED_FINISHED, FAILED_ANSWERING
    raise_application_error(-20104, 'SubRequest canceled while queuing in scheduler. Giving up.');
  END IF;
  -- Get selected filesystem
  SELECT FileSystem.id INTO fsId
    FROM DiskServer, FileSystem
   WHERE FileSystem.diskserver = DiskServer.id
     AND DiskServer.name = selectedDiskServer
     AND FileSystem.mountPoint = selectedMountPoint;
  -- Check that a job has not already started for this diskcopy. Refer to
  -- bug #14358
  IF prevFsId > 0 AND prevFsId <> fsId THEN
    raise_application_error(-20107, 'This job has already started for this DiskCopy. Giving up.');
  END IF;
  -- In case the DiskCopy was in WAITFS_SCHEDULING,
  -- restart the waiting SubRequests
  UPDATE /*+ INDEX(Subrequest I_Subrequest_Parent)*/ SubRequest
     SET status = 1, lastModificationTime = getTime(), parent = 0 -- SUBREQUEST_RESTART
   WHERE parent = srId;
  -- link DiskCopy and FileSystem and update DiskCopyStatus
  UPDATE DiskCopy
     SET status = 6, -- DISKCOPY_STAGEOUT
         fileSystem = fsId,
         nbCopyAccesses = nbCopyAccesses + 1
   WHERE id = rdcId
   RETURNING status, path
   INTO rdcStatus, rdcPath;
END;
/


/* PL/SQL method implementing getUpdateStart */
CREATE OR REPLACE PROCEDURE getUpdateStart
        (srId IN INTEGER, selectedDiskServer IN VARCHAR2, selectedMountPoint IN VARCHAR2,
         dci OUT INTEGER, rpath OUT VARCHAR2, rstatus OUT NUMBER, reuid OUT INTEGER,
         regid OUT INTEGER, diskCopySize OUT NUMBER) AS
  cfid INTEGER;
  fid INTEGER;
  dcId INTEGER;
  fsId INTEGER;
  dcIdInReq INTEGER;
  nh VARCHAR2(2048);
  fileSize INTEGER;
  srSvcClass INTEGER;
  proto VARCHAR2(2048);
  isUpd NUMBER;
  nbAc NUMBER;
  gcw NUMBER;
  gcwProc VARCHAR2(2048);
  cTime NUMBER;
BEGIN
  -- Get data
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ euid, egid, svcClass, upd, diskCopy
    INTO reuid, regid, srSvcClass, isUpd, dcIdInReq
    FROM SubRequest,
        (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, euid, egid, svcClass, 0 AS upd FROM StageGetRequest UNION ALL
         SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, euid, egid, svcClass, 1 AS upd FROM StageUpdateRequest) Request
   WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
  -- Take a lock on the CastorFile. Associated with triggers,
  -- this guarantees we are the only ones dealing with its copies
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ CastorFile.fileSize, CastorFile.id
    INTO fileSize, cfId
    FROM CastorFile, SubRequest
   WHERE CastorFile.id = SubRequest.castorFile
     AND SubRequest.id = srId FOR UPDATE OF CastorFile;
  -- Get selected filesystem
  SELECT FileSystem.id INTO fsId
    FROM DiskServer, FileSystem
   WHERE FileSystem.diskserver = DiskServer.id
     AND DiskServer.name = selectedDiskServer
     AND FileSystem.mountPoint = selectedMountPoint;
  -- Try to find local DiskCopy
  SELECT /*+ INDEX(DiskCopy I_DiskCopy_Castorfile) */ id, nbCopyAccesses, gcWeight, creationTime
    INTO dcId, nbac, gcw, cTime
    FROM DiskCopy
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.filesystem = fsId
     AND DiskCopy.status IN (0, 6, 10) -- STAGED, STAGEOUT, CANBEMIGR
     AND ROWNUM < 2;
  -- We found it, so we are settled and we'll use the local copy.
  -- It might happen that we have more than one, because the scheduling may have
  -- scheduled a replication on a fileSystem which already had a previous diskcopy.
  -- We don't care and we randomly took the first one.
  -- First we will compute the new gcWeight of the diskcopy
  IF nbac = 0 THEN
    gcwProc := castorGC.getFirstAccessHook(srSvcClass);
    IF gcwProc IS NOT NULL THEN
      EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:oldGcw, :cTime); END;'
        USING OUT gcw, IN gcw, IN cTime;
    END IF;
  ELSE
    gcwProc := castorGC.getAccessHook(srSvcClass);
    IF gcwProc IS NOT NULL THEN
      EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:oldGcw, :cTime, :nbAc); END;'
        USING OUT gcw, IN gcw, IN cTime, IN nbac;
    END IF;
  END IF;
  -- Here we also update the gcWeight taking into account the new lastAccessTime
  -- and the weightForAccess from our svcClass: this is added as a bonus to
  -- the selected diskCopy.
  UPDATE DiskCopy
     SET gcWeight = gcw,
         lastAccessTime = getTime(),
         nbCopyAccesses = nbCopyAccesses + 1
   WHERE id = dcId
  RETURNING id, path, status, diskCopySize INTO dci, rpath, rstatus, diskCopySize;
  -- Let's update the SubRequest and set the link with the DiskCopy
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET DiskCopy = dci
   WHERE id = srId RETURNING protocol INTO proto;
  -- In case of an update, if the protocol used does not support
  -- updates properly (via firstByteWritten call), we should
  -- call firstByteWritten now and consider that the file is being
  -- modified.
  IF isUpd = 1 THEN
    handleProtoNoUpd(srId, proto);
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- No disk copy found on selected FileSystem. This can happen in 2 cases :
  --  + either a diskcopy was available and got disabled before this job
  --    was scheduled. Bad luck, we fail the request, the user will have to retry
  --  + or we are an update creating a file and there is a diskcopy in WAITFS
  --    or WAITFS_SCHEDULING associated to us. Then we have to call putStart
  -- So we first check the update hypothesis
  IF isUpd = 1 AND dcIdInReq IS NOT NULL THEN
    DECLARE
      stat NUMBER;
    BEGIN
      SELECT status INTO stat FROM DiskCopy WHERE id = dcIdInReq;
      IF stat IN (5, 11) THEN -- WAITFS, WAITFS_SCHEDULING
        -- it is an update creating a file, let's call putStart
        putStart(srId, selectedDiskServer, selectedMountPoint, dci, rstatus, rpath);
        RETURN;
      END IF;
    END;
  END IF;
  -- It was not an update creating a file, so we fail
  raise_application_error(-20114, 'File invalidated while queuing in the scheduler, please try again');
END;
/


/* PL/SQL method implementing disk2DiskCopyStart */
CREATE OR REPLACE PROCEDURE disk2DiskCopyStart
(dcId IN INTEGER, srcDcId IN INTEGER, destdiskServer IN VARCHAR2,
 destMountPoint IN VARCHAR2, destPath OUT VARCHAR2, destSvcClass OUT VARCHAR2,
 srcDiskServer OUT VARCHAR2, srcMountPoint OUT VARCHAR2, srcPath OUT VARCHAR2,
 srcSvcClass OUT VARCHAR2) AS
  fsId NUMBER;
  cfId NUMBER;
  dsId NUMBER;
  res NUMBER;
  unused NUMBER;
  nbCopies NUMBER;
  cfNsHost VARCHAR2(2048);
BEGIN
  -- Check that we did not cancel the replication request in the mean time
  BEGIN
    SELECT /*+ INDEX(Subrequest I_Subrequest_DiskCopy) INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */
           SubRequest.status, StageDiskCopyReplicaRequest.svcClassName
      INTO unused, destSvcClass
      FROM SubRequest, StageDiskCopyReplicaRequest
     WHERE SubRequest.diskcopy = dcId
       AND SubRequest.request = StageDiskCopyReplicaRequest.id
       AND SubRequest.status = dconst.SUBREQUEST_READY;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20111, 'Replication request canceled while queuing in scheduler. Giving up.');
  END;
  -- Check to see if the proposed diskserver and filesystem selected by the
  -- scheduler to run the destination end of disk2disk copy transfer is in the
  -- correct service class. I.e the service class of the original request. This
  -- is done to prevent files being written to an incorrect service class when
  -- diskservers/filesystems are moved.
  BEGIN
    SELECT FileSystem.id, DiskServer.id INTO fsId, dsId
      FROM DiskServer, FileSystem, DiskPool2SvcClass, SvcClass
     WHERE FileSystem.diskserver = DiskServer.id
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = SvcClass.id
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
           CastorFile.id, CastorFile.nsHost, SvcClass.name
      INTO srcDiskServer, srcMountPoint, srcPath, cfId, cfNsHost, srcSvcClass
      FROM DiskCopy, CastorFile, DiskServer, FileSystem, DiskPool2SvcClass,
           SvcClass, StageDiskCopyReplicaRequest
     WHERE DiskCopy.id = srcDcId
       AND DiskCopy.castorfile = CastorFile.id
       AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
       AND FileSystem.id = DiskCopy.filesystem
       AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = SvcClass.id
       AND DiskServer.id = FileSystem.diskserver
       AND DiskServer.status IN (0, 1)
       -- For diskpools which belong to multiple service classes, make sure
       -- we are checking for the file in the correct service class!
       AND StageDiskCopyReplicaRequest.sourceDiskCopy = DiskCopy.id
       AND StageDiskCopyReplicaRequest.sourceSvcClass = SvcClass.id
       AND StageDiskCopyReplicaRequest.destDiskCopy = dcId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20109, 'The source DiskCopy to be replicated is no longer available.');
  END;
  -- Prevent multiple copies of the file being created on the same diskserver
  SELECT count(*) INTO nbCopies
    FROM DiskCopy, FileSystem
   WHERE DiskCopy.filesystem = FileSystem.id
     AND FileSystem.diskserver = dsId
     AND DiskCopy.castorfile = cfId
     AND DiskCopy.id != dcId
     AND DiskCopy.status IN (0, 1, 2, 10); -- STAGED, DISK2DISKCOPY, WAITTAPERECALL, CANBEMIGR
  IF nbCopies > 0 THEN
    raise_application_error(-20112, 'Multiple copies of this file already found on this diskserver');
  END IF;
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
/


/* PL/SQL method implementing disk2DiskCopyDone */
CREATE OR REPLACE PROCEDURE disk2DiskCopyDone
(dcId IN INTEGER, srcDcId IN INTEGER, replicaFileSize IN INTEGER) AS
  srId       INTEGER;
  cfId       INTEGER;
  srcStatus  INTEGER;
  srcFsId    NUMBER;
  proto      VARCHAR2(2048);
  reqId      NUMBER;
  svcClassId NUMBER;
  gcwProc    VARCHAR2(2048);
  gcw        NUMBER;
  fileSize   NUMBER;
  ouid       INTEGER;
  ogid       INTEGER;
BEGIN
  -- Lock the CastorFile
  SELECT castorFile INTO cfId
    FROM DiskCopy
   WHERE id = dcId;
  SELECT id INTO cfId FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Try to get the source diskcopy status
  srcFsId := -1;
  BEGIN
    SELECT status, gcWeight, diskCopySize, fileSystem
      INTO srcStatus, gcw, fileSize, srcFsId
      FROM DiskCopy
     WHERE id = srcDcId
       AND status IN (0, 10);  -- STAGED, CANBEMIGR
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL;
  END;
  -- If no diskcopy was returned it means that the source has either:
  --   A) Been garbage collected while the copying was taking place OR
  --   B) The diskcopy is no longer in a STAGED or CANBEMIGR state. As
  --      A result we do not know which status to put the new copy in
  --      and/or cannot trust that the file was not modified mid transfer
  --
  -- If a diskcopy was returned but the size of the original file in
  -- comparison to the replica is different then some corruption has
  -- occurred and the new copy should not be kept
  --
  -- In all cases we invalidate the new copy!
  IF (srcFsId = -1) OR
     (srcFsId != -1 AND fileSize != replicaFileSize) THEN
    -- Begin the process of invalidating the file replica
    UPDATE DiskCopy SET status = 7 WHERE id = dcId -- INVALID
    RETURNING CastorFile INTO cfId;
    -- Restart the SubRequests waiting for the copy
    UPDATE /*+ INDEX(Subrequest I_Subrequest_DiskCopy)*/ SubRequest
       SET status = 1, -- SUBREQUEST_RESTART
           lastModificationTime = getTime()
     WHERE diskCopy = dcId RETURNING id INTO srId;
    UPDATE /*+ INDEX(Subrequest I_Subrequest_Parent)*/ SubRequest
       SET status = 1,
           getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
           lastModificationTime = getTime(),
           parent = 0
     WHERE parent = srId; -- SUBREQUEST_RESTART
    -- Archive the diskCopy replica request, status FAILED_FINISHED
    -- for abnormal transfer termination
    IF (srcFsId IS NOT NULL AND fileSize != replicaFileSize) THEN
      archiveSubReq(srId, 9);  -- FAILED_FINISHED
    ELSE
      archiveSubReq(srId, 8);  -- FINISHED
    END IF;
    -- Restart all entries in the snapshot of files to drain that may be
    -- waiting on the replication of the source diskcopy.
    UPDATE DrainingDiskCopy
       SET status = 1,  -- RESTARTED
           parent = 0
     WHERE status = 3  -- RUNNING
       AND (diskCopy = srcDcId
        OR  parent = srcDcId);
    drainFileSystem(srcFsId);
    -- If a file size mismatch has occurred raise an exception which
    -- will be logged by the d2dtransfer mover.
    IF (srcFsId IS NOT NULL AND fileSize != replicaFileSize) THEN
      -- Commit the invalidation of the replica. If we dont the raising of
      -- an application_error will trigger a rollback and the diskcopy will
      -- be stuck in WAITDISK2DISKCOPY
      COMMIT;
      raise_application_error(-20119, 'File replication size mismatch: (original size: '||fileSize||' - replica size: '||replicaFileSize||')');
    END IF;
    RETURN;
  END IF;
  -- The new replica looks OK, so lets keept it!
  -- update SubRequest and get data
  UPDATE /*+ INDEX(Subrequest I_Subrequest_Diskcopy)*/ SubRequest
     SET status = 6, -- SUBREQUEST_READY
         lastModificationTime = getTime()
   WHERE diskCopy = dcId RETURNING id, protocol, request
    INTO srId, proto, reqId;
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id) INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */
         SvcClass.id INTO svcClassId
    FROM SvcClass, StageDiskCopyReplicaRequest Req, SubRequest
   WHERE SubRequest.id = srId
     AND SubRequest.request = Req.id
     AND Req.svcClass = SvcClass.id;
  -- Compute gcWeight
  gcwProc := castorGC.getCopyWeight(svcClassId);
  EXECUTE IMMEDIATE
    'BEGIN :newGcw := ' || gcwProc || '(:size, :status, :gcw); END;'
    USING OUT gcw, IN fileSize, srcStatus, gcw;
  -- Update status
  UPDATE DiskCopy
     SET status = srcStatus,
         gcWeight = gcw
   WHERE id = dcId
  RETURNING castorFile, owneruid, ownergid
    INTO cfId, ouid, ogid;
  -- Wake up waiting subrequests
  UPDATE /*+ INDEX(Subrequest I_Subrequest_Parent)*/ SubRequest
     SET status = 1,  -- SUBREQUEST_RESTART
         getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
         lastModificationTime = getTime(),
         parent = 0
   WHERE parent = srId;
  -- Archive the diskCopy replica request
  archiveSubReq(srId, 8);  -- FINISHED
  -- Trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(cfId, ouid, ogid);
  -- Restart all entries in the snapshot of files to drain that may be
  -- waiting on the replication of the source diskcopy.
  UPDATE DrainingDiskCopy
     SET status = 1,  -- RESTARTED
         parent = 0
   WHERE status = 3  -- RUNNING
     AND (diskCopy = srcDcId
      OR  parent = srcDcId);
  drainFileSystem(srcFsId);
  -- WARNING: previous call to drainFileSystem has a COMMIT inside. So all
  -- locks have been released!!
END;
/


/* PL/SQL method implementing disk2DiskCopyFailed */
CREATE OR REPLACE PROCEDURE disk2DiskCopyFailed
(dcId IN INTEGER, enoent IN INTEGER) AS
  fsId    NUMBER;
  cfId    NUMBER;
  ouid    INTEGER;
  ogid    INTEGER;
  srcDcId NUMBER;
  srcFsId NUMBER;
  srId    NUMBER;
BEGIN
  fsId := 0;
  srcFsId := -1;
  -- Lock the CastorFile
  SELECT id INTO cfId FROM CastorFile
   WHERE id =
    (SELECT castorFile
       FROM DiskCopy
      WHERE id = dcId) FOR UPDATE;
  IF enoent = 1 THEN
    -- Set all diskcopies to FAILED. We're preemptying the NS synchronization here
    UPDATE DiskCopy SET status = 4 -- FAILED
     WHERE castorFile =
       (SELECT castorFile FROM DiskCopy WHERE id = dcId);
  ELSE
    -- Set the diskcopy status to INVALID so that it will be garbage collected
    -- at a later date.
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE status = 1 -- WAITDISK2DISKCOPY
       AND id = dcId
     RETURNING fileSystem, castorFile, owneruid, ownergid
      INTO fsId, cfId, ouid, ogid;
  END IF;
  -- Handle SubRequests
  BEGIN
    -- Get the corresponding subRequest, if it exists
    SELECT /*+ INDEX(Subrequest I_Subrequest_DiskCopy)*/ id INTO srId
      FROM SubRequest
     WHERE diskCopy = dcId
       AND status IN (6, 14); -- READY, BEINGSHCED
    -- Wake up other subrequests waiting on it
    UPDATE /*+ INDEX(Subrequest I_Subrequest_Parent)*/ SubRequest
       SET status = 1, -- RESTART
           parent = 0
     WHERE parent = srId;
    -- Fail it
    archiveSubReq(srId, 9); -- FAILED_FINISHED
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- SubRequest not found, don't trigger replicateOnClose here
    -- as we may have been restarted
    NULL;
  END;
  -- Handle draining logic
  BEGIN
    -- Determine the source diskcopy and filesystem involved in the replication
    SELECT /*+ INDEX(StageDiskCopyReplicaRequest I_StageDiskCopyReplic_DestDC) */ sourceDiskCopy, fileSystem
      INTO srcDcId, srcFsId
      FROM DiskCopy, StageDiskCopyReplicaRequest
     WHERE StageDiskCopyReplicaRequest.sourceDiskCopy = DiskCopy.id
       AND StageDiskCopyReplicaRequest.destDiskCopy = dcId;
    -- Restart all entries in the snapshot of files to drain that may be
    -- waiting on the replication of the source diskcopy.
    UPDATE DrainingDiskCopy
       SET status = 1,  -- RESTARTED
           parent = 0
     WHERE status = 3  -- RUNNING
       AND (diskCopy = srcDcId
        OR  parent = srcDcId);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL;
  END;
  -- Handle replication on close
  BEGIN
    -- Trigger the creation of additional copies of the file, if necessary.
    -- Note: We do this also on failure to be able to recover from transient
    -- errors, e.g. timeouts while waiting to be scheduled, but we don't on ENOENT.
    IF enoent = 0 THEN
      replicateOnClose(cfId, ouid, ogid);
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL;
  END;
  -- If no filesystem was set on the diskcopy then we can safely delete it
  -- without waiting for garbage collection as the transfer was never started
  IF fsId = 0 THEN
    DELETE FROM DiskCopy WHERE id = dcId;
  END IF;
  -- Continue draining process
  drainFileSystem(srcFsId);
  -- WARNING: previous call to drainFileSystem has a COMMIT inside. So all
  -- locks have been released!!
END;
/


/* PL/SQL method implementing prepareForMigration
   returnCode can take 2 values :
    - 0 : Nothing special
    - 1 : The file got deleted while it was being written to
*/
CREATE OR REPLACE PROCEDURE prepareForMigration (srId IN INTEGER,
                                                 fs IN INTEGER,
                                                 ts IN NUMBER,
                                                 fId OUT NUMBER,
                                                 nh OUT VARCHAR2,
                                                 returnCode OUT INTEGER) AS
  cfId INTEGER;
  dcId INTEGER;
  svcId INTEGER;
  realFileSize INTEGER;
  unused INTEGER;
  contextPIPP INTEGER;
BEGIN
  returnCode := 0;
  -- Get CastorFile
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ castorFile, diskCopy INTO cfId, dcId
    FROM SubRequest WHERE id = srId;
  -- Lock the CastorFile and get the fileid and name server host
  SELECT id, fileid, nsHost INTO cfId, fId, nh
    FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Determine the context (Put inside PrepareToPut or not)
  -- check that we are a Put or an Update
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ Request.id INTO unused
    FROM SubRequest,
       (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ id FROM StagePutRequest UNION ALL
        SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id FROM StageUpdateRequest) Request
   WHERE SubRequest.id = srId
     AND Request.id = SubRequest.request;
  BEGIN
    -- Check that there is a PrepareToPut/Update going on. There can be only a
    -- single one or none. If there was a PrepareTo, any subsequent PPut would
    -- be rejected and any subsequent PUpdate would be directly archived (cf.
    -- processPrepareRequest).
    SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest.diskCopy INTO unused
      FROM SubRequest,
       (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id FROM StagePrepareToPutRequest UNION ALL
        SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id FROM StagePrepareToUpdateRequest) Request
     WHERE SubRequest.CastorFile = cfId
       AND Request.id = SubRequest.request
       AND SubRequest.status = 6; -- READY
    -- If we got here, we are a Put inside a PrepareToPut
    contextPIPP := 0;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Here we are a standalone Put
    contextPIPP := 1;
  END;
  -- Check whether the diskCopy is still in STAGEOUT. If not, the file
  -- was deleted via stageRm while being written to. Thus, we should just give
  -- up
  BEGIN
    SELECT status INTO unused
      FROM DiskCopy WHERE id = dcId AND status = 6; -- STAGEOUT
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- So we are in the case, we give up
    returnCode := 1;
    RETURN;
  END;
  -- Now we can safely update CastorFile's file size
  UPDATE CastorFile SET fileSize = fs, lastUpdateTime = ts
   WHERE id = cfId AND (lastUpdateTime IS NULL OR ts >= lastUpdateTime);
  -- If ts < lastUpdateTime, we were late and another job already updated the
  -- CastorFile. So we nevertheless retrieve the real file size.
  SELECT fileSize INTO realFileSize FROM CastorFile WHERE id = cfId;
  -- Get svcclass from Request
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ svcClass INTO svcId
    FROM SubRequest,
      (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ id, svcClass FROM StagePutRequest          UNION ALL
       SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClass FROM StageUpdateRequest UNION ALL
       SELECT /*+ INDEX(StagePutDoneRequest PK_StagePutDoneRequest_Id) */ id, svcClass FROM StagePutDoneRequest) Request
   WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
  IF contextPIPP != 0 THEN
    -- If not a put inside a PrepareToPut/Update, trigger migration
    -- and update DiskCopy status
    putDoneFunc(cfId, realFileSize, contextPIPP, svcId);
  ELSE
    -- If put inside PrepareToPut/Update, restart any PutDone currently
    -- waiting on this put/update
    UPDATE /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest
       SET status = 1, parent = 0 -- RESTART
     WHERE reqType = 39  -- PutDone
       AND castorFile = cfId
       AND status = 5; -- WAITSUBREQ
  END IF;
  -- Archive Subrequest
  archiveSubReq(srId, 8);  -- FINISHED
END;
/


/* PL/SQL method implementing getUpdateDone */
CREATE OR REPLACE PROCEDURE getUpdateDoneProc
(srId IN NUMBER) AS
BEGIN
  archiveSubReq(srId, 8);  -- FINISHED
END;
/


/* PL/SQL method implementing getUpdateFailed */
CREATE OR REPLACE PROCEDURE getUpdateFailedProcExt
(srId IN NUMBER, errno IN NUMBER, errmsg IN VARCHAR2) AS
BEGIN
  -- Fail the subrequest. The stager will try and answer the client
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET status = 7, -- FAILED
         errorCode = errno,
         errorMessage = errmsg
   WHERE id = srId;
  -- Wake up other subrequests waiting on it
  UPDATE /*+ INDEX(Subrequest I_Subrequest_Parent)*/ SubRequest
     SET parent = 0, status = 1, -- RESTART
         lastModificationTime = getTime()
   WHERE parent = srId;
END;
/

CREATE OR REPLACE PROCEDURE getUpdateFailedProc
(srId IN NUMBER) AS
BEGIN
  getUpdateFailedProcExt(srId, 1015, 'Job terminated with failure');  -- SEINTERNAL
END;
/

/* PL/SQL method implementing putFailedProc */
CREATE OR REPLACE PROCEDURE putFailedProcExt(srId IN NUMBER, errno IN NUMBER, errmsg IN VARCHAR2) AS
  dcId INTEGER;
  cfId INTEGER;
  unused INTEGER;
BEGIN
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ diskCopy, castorFile
    INTO dcId, cfId
    FROM SubRequest
   WHERE id = srId;
  -- Fail the subRequest
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET status = 7, -- FAILED
         errorCode = errno,
         errorMessage = errmsg
   WHERE id = srId;
  -- Determine the context (Put inside PrepareToPut/Update ?)
  BEGIN
    -- Check that there is a PrepareToPut/Update going on. There can be only a single one
    -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
    -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
    SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest.id INTO unused
      FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id FROM StagePrepareToPutRequest UNION ALL
            SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
     WHERE SubRequest.castorFile = cfId
       AND PrepareRequest.id = SubRequest.request
       AND SubRequest.status = 6; -- READY
    -- The select worked out, so we have a prepareToPut/Update
    -- In such a case, we do not cleanup DiskCopy and CastorFile
    -- but we have to wake up a potential waiting putDone
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 1, parent = 0 -- RESTART
     WHERE id IN
      (SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile) INDEX(StagePutDoneRequest PK_StagePutDoneRequest_Id) */ SubRequest.id
         FROM StagePutDoneRequest, SubRequest
        WHERE SubRequest.CastorFile = cfId
          AND StagePutDoneRequest.id = SubRequest.request
          AND SubRequest.status = 5); -- WAITSUBREQ
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- This means we are a standalone put
    -- thus cleanup DiskCopy and maybe the CastorFile
    -- (the physical file is dropped by the job)
    DELETE FROM DiskCopy WHERE id = dcId;
    deleteCastorFile(cfId);
  END;
END;
/

/* PL/SQL method implementing putFailedProc */
CREATE OR REPLACE PROCEDURE putFailedProc(srId IN NUMBER) AS
BEGIN
  putFailedProcExt(srId, 1015, 'Job terminated with failure');  -- SEINTERNAL
END;
/

/* PL/SQL method implementing getSchedulerTransfers.
   This method lists all known transfers
   that are started/pending for more than an hour */
CREATE OR REPLACE PROCEDURE getSchedulerTransfers
  (transfers OUT castor.UUIDPairRecord_Cur) AS
BEGIN
  OPEN transfers FOR
    SELECT SR.subReqId, Request.reqid
      FROM SubRequest SR,
        -- Union of all requests that could result in scheduler transfers
        (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                id, svcClass, reqid, 40  AS reqType FROM StagePutRequest             UNION ALL
         SELECT /*+ INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */
                id, svcClass, reqid, 133 AS reqType FROM StageDiskCopyReplicaRequest UNION ALL
         SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                id, svcClass, reqid, 35  AS reqType FROM StageGetRequest             UNION ALL
         SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                id, svcClass, reqid, 44  AS reqType FROM StageUpdateRequest) Request
     WHERE SR.status = 6  -- READY
       AND SR.request = Request.id
       AND SR.lastModificationTime < getTime() - 3600;
END;
/

/* PL/SQL method implementing getSchedulerD2dTransfers.
   This method lists all running D2d transfers */
CREATE OR REPLACE PROCEDURE getSchedulerD2dTransfers
  (transfers OUT castor.UUIDPairRecord_Cur) AS
BEGIN
  OPEN transfers FOR
    SELECT SR.subReqId, Request.reqid
      FROM SubRequest SR, StageDiskCopyReplicaRequest Request
     WHERE SR.status = 6  -- READY
       AND SR.request = Request.id;
END;
/

/* PL/SQL method implementing getFileIdsForSrs.
   This method returns the list of fileids associated to the given list of
   subrequests */
CREATE OR REPLACE PROCEDURE getFileIdsForSrs
  (subReqIds IN castor."strList", fileids OUT castor.FileEntry_Cur) AS
  fid NUMBER;
  nh VARCHAR(2048);
BEGIN
  FOR i IN subReqIds.FIRST .. subReqIds.LAST LOOP
    SELECT /*+ INDEX(Subrequest I_Subrequest_SubreqId)*/ fileid, nsHost INTO fid, nh
      FROM Castorfile, SubRequest
     WHERE SubRequest.subreqId = subReqIds(i)
       AND SubRequest.castorFile = CastorFile.id;
    INSERT INTO getFileIdsForSrsHelper VALUES (i, fid, nh);
  END LOOP;
  OPEN fileids FOR SELECT nh, fileid FROM getFileIdsForSrsHelper ORDER BY rowno;
END;
/

/* PL/SQL method implementing transferFailedSafe, providing bulk termination of file
 * transfers.
 */
CREATE OR REPLACE
PROCEDURE transferFailedSafe(subReqIds IN castor."strList",
                             errnos IN castor."cnumList",
                             errmsg IN castor."strList") AS
  srId  NUMBER;
  dcId  NUMBER;
  cfId  NUMBER;
  rType NUMBER;
BEGIN
  -- give up if nothing to be done
  IF subReqIds.COUNT = 0 THEN RETURN; END IF;
  -- Loop over all transfers to fail
  FOR i IN subReqIds.FIRST .. subReqIds.LAST LOOP
    BEGIN
      -- Get the necessary information needed about the request.
      SELECT /*+ INDEX(Subrequest I_Subrequest_SubreqId)*/ id, diskCopy, reqType, castorFile
        INTO srId, dcId, rType, cfId
        FROM SubRequest
       WHERE subReqId = subReqIds(i)
         AND status IN (6, 14);  -- READY, BEINGSCHED
       -- Lock the CastorFile.
       SELECT id INTO cfId FROM CastorFile
        WHERE id = cfId FOR UPDATE;
       -- Confirm SubRequest status hasn't changed after acquisition of lock
       SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ id INTO srId FROM SubRequest
        WHERE id = srId AND status IN (6, 14);  -- READY, BEINGSCHED
       -- Call the relevant cleanup procedure for the transfer, procedures that
       -- would have been called if the transfer failed on the remote execution host.
       IF rType = 40 THEN      -- StagePutRequest
         putFailedProc(srId);
       ELSIF rType = 133 THEN  -- StageDiskCopyReplicaRequest
         disk2DiskCopyFailed(dcId, 0);
       ELSE                    -- StageGetRequest or StageUpdateRequest
         getUpdateFailedProc(srId);
       END IF;
       -- Update the reason for termination, overriding the error code set above
       UPDATE SubRequest
          SET errorCode = decode(errnos(i), 0, errorCode, errnos(i)),
              errorMessage = decode(errmsg(i), NULL, errorMessage, errmsg(i))
        WHERE id = srId;
       -- Release locks
       COMMIT;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;  -- The SubRequest may have be removed, nothing to be done.
    END;
  END LOOP;
END;
/

/* PL/SQL method implementing transferFailedLockedFile, providing bulk termination of file
 * transfers. in case the castorfile is already locked
 */
CREATE OR REPLACE
PROCEDURE transferFailedLockedFile(subReqId IN castor."strList",
                                   errno IN castor."cnumList",
                                   errmsg IN castor."strList")
AS
  srId  NUMBER;
  dcId  NUMBER;
  rType NUMBER;
BEGIN
  FOR i IN subReqId.FIRST .. subReqId.LAST LOOP
    BEGIN
      -- Get the necessary information needed about the request.
      SELECT id, diskCopy, reqType
        INTO srId, dcId, rType
        FROM SubRequest
       WHERE subReqId = subReqId(i)
         AND status IN (6, 14);  -- READY, BEINGSCHED
       -- Update the reason for termination.
       UPDATE SubRequest
          SET errorCode = decode(errno(i), 0, errorCode, errno(i)),
              errorMessage = decode(errmsg(i), NULL, errorMessage, errmsg(i))
        WHERE id = srId;
       -- Call the relevant cleanup procedure for the transfer, procedures that
       -- would have been called if the transfer failed on the remote execution host.
       IF rType = 40 THEN      -- StagePutRequest
         putFailedProc(srId);
       ELSIF rType = 133 THEN  -- StageDiskCopyReplicaRequest
         disk2DiskCopyFailed(dcId, 0);
       ELSE                    -- StageGetRequest or StageUpdateRequest
         getUpdateFailedProc(srId);
       END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;  -- The SubRequest may have be removed, nothing to be done.
    END;
  END LOOP;
END;
/

CREATE OR REPLACE TRIGGER tr_SubRequest_informSchedReady AFTER UPDATE OF status ON SubRequest
FOR EACH ROW WHEN (new.status = 13) -- SUBREQUEST_READYFORSCHED
BEGIN
  DBMS_ALERT.SIGNAL('transferReadyToSchedule', '');
END;
/

/* PL/SQL method implementing transferToSchedule */
CREATE OR REPLACE
PROCEDURE transferToSchedule(srId OUT INTEGER,              srSubReqId OUT VARCHAR2,
                             srProtocol OUT VARCHAR2,       srXsize OUT INTEGER,
                             srRfs OUT VARCHAR2,            reqId OUT VARCHAR2,
                             cfFileId OUT INTEGER,          cfNsHost OUT VARCHAR2,
                             reqSvcClass OUT VARCHAR2,      reqType OUT INTEGER,
                             reqEuid OUT INTEGER,           reqEgid OUT INTEGER,
                             reqUsername OUT VARCHAR2,      srOpenFlags OUT VARCHAR2,
                             clientIp OUT INTEGER,          clientPort OUT INTEGER,
                             clientVersion OUT INTEGER,     clientType OUT INTEGER,
                             reqSourceDiskCopy OUT INTEGER, reqDestDiskCopy OUT INTEGER,
                             clientSecure OUT INTEGER,      reqSourceSvcClass OUT VARCHAR2,
                             reqCreationTime OUT INTEGER,   reqDefaultFileSize OUT INTEGER,
                             sourceRfs OUT VARCHAR2) AS
  cfId NUMBER;
  -- Cursor to select the next candidate for submission to the scheduler orderd
  -- by creation time.
  CURSOR c IS
    SELECT /*+ FIRST_ROWS(10) INDEX(SR I_SubRequest_CT_ID) */ SR.id
      FROM SubRequest
 PARTITION (P_STATUS_13_14) SR  -- RESTART, READYFORSCHED, BEINGSCHED
     ORDER BY status ASC, SR.creationTime ASC;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  srIntId NUMBER;
  svcClassId NUMBER;
  unusedMessage VARCHAR2(2048);
  unusedStatus INTEGER;
BEGIN
  -- Open a cursor on potential candidates
  OPEN c;
  -- Retrieve the first candidate
  FETCH c INTO srIntId;
  IF c%NOTFOUND THEN
    -- There is no candidate available. Wait for next alert concerning something
    -- to schedule for a maximum of 3 seconds.
    -- We do not wait forever in order to ensure that we will retry from time to
    -- time to dig out candidates that timed out in status BEINGSCHED. Plus we
    -- need to give the control back to the caller daemon in case it should exit
    CLOSE c;
    DBMS_ALERT.WAITONE('transferReadyToSchedule', unusedMessage, unusedStatus, 3);
    -- try again to find something now that we waited
    OPEN c;
    FETCH c INTO srIntId;
    IF c%NOTFOUND THEN
      -- still nothing. We will give back the control to the application
      -- so that it can handle cases like signals and exit. We will probably
      -- be back soon :-)
      RETURN;
    END IF;
  END IF;
  LOOP
    -- we reached this point because we have found at least one candidate
    -- let's loop on the candidates until we find one we can process
    BEGIN
      -- Try to lock the current candidate, verify that the status is valid. A
      -- valid subrequest is either in READYFORSCHED or has been stuck in
      -- BEINGSCHED for more than 1800 seconds (30 mins)
      SELECT /*+ INDEX(SR PK_SubRequest_ID) */ id INTO srIntId
        FROM SubRequest PARTITION (P_STATUS_13_14) SR
       WHERE id = srIntId
         AND status = dconst.SUBREQUEST_READYFORSCHED
         FOR UPDATE NOWAIT;
      -- We have successfully acquired the lock, so we update the subrequest
      -- status and modification time
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_READY,
             lastModificationTime = getTime()
       WHERE id = srIntId
      RETURNING id, subReqId, protocol, xsize, requestedFileSystems
        INTO srId, srSubReqId, srProtocol, srXsize, srRfs;
      -- and we exit the loop on candidates
      EXIT;
    EXCEPTION
      -- Try again, either we failed to accquire the lock on the subrequest or
      -- the subrequest being processed is not the correct state
      WHEN NO_DATA_FOUND THEN
        NULL;
      WHEN SrLocked THEN
        NULL;
    END;
    -- we are here because the current candidate could not be handled
    -- let's go to the next one
    FETCH c INTO srIntId;
    IF c%NOTFOUND THEN
      -- no next one ? then we can return
      RETURN;
    END IF;
  END LOOP;
  CLOSE c;

  -- We finally got a valid candidate, let's process it
  -- Extract the rest of the information required by transfer manager
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         CastorFile.id, CastorFile.fileId, CastorFile.nsHost, SvcClass.name, SvcClass.id,
         Request.type, Request.reqId, Request.euid, Request.egid, Request.username,
         Request.direction, Request.sourceDiskCopy, Request.destDiskCopy,
         Request.sourceSvcClass, Client.ipAddress, Client.port, Client.version,
         129 clientType, Client.secure, Request.creationTime,
         decode(SvcClass.defaultFileSize, 0, 2000000000, SvcClass.defaultFileSize)
    INTO cfId, cfFileId, cfNsHost, reqSvcClass, svcClassId, reqType, reqId, reqEuid, reqEgid,
         reqUsername, srOpenFlags, reqSourceDiskCopy, reqDestDiskCopy, reqSourceSvcClass,
         clientIp, clientPort, clientVersion, clientType, clientSecure, reqCreationTime,
         reqDefaultFileSize
    FROM SubRequest, CastorFile, SvcClass, Client,
         (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                 id, username, euid, egid, reqid, client, creationTime,
                 'w' direction, svcClass, NULL sourceDiskCopy,
                 NULL destDiskCopy, NULL sourceSvcClass, 40 type
            FROM StagePutRequest
           UNION ALL
          SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                 id, username, euid, egid, reqid, client, creationTime,
                 'r' direction, svcClass, NULL sourceDiskCopy,
                 NULL destDiskCopy, NULL sourceSvcClass, 35 type
            FROM StageGetRequest
           UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                 id, username, euid, egid, reqid, client, creationTime,
                 'o' direction, svcClass, NULL sourceDiskCopy,
                 NULL destDiskCopy, NULL sourceSvcClass, 44 type
            FROM StageUpdateRequest
           UNION ALL
          SELECT /*+ INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */
                 id, username, euid, egid, reqid, client, creationTime,
                 'w' direction, svcClass, sourceDiskCopy, destDiskCopy,
                 (SELECT name FROM SvcClass WHERE id = sourceSvcClass), 133 type
            FROM StageDiskCopyReplicaRequest) Request
   WHERE SubRequest.id = srId
     AND SubRequest.castorFile = CastorFile.id
     AND Request.svcClass = SvcClass.id
     AND Request.id = SubRequest.request
     AND Request.client = Client.id;

  -- In case of disk2disk copies, requested filesystems are concerning the sources
  -- destinations are free
  IF reqType = 133 THEN  -- StageDiskCopyReplicaRequest
    sourceRfs := srRfs;
    srRfs := NULL;
  END IF;

  -- Select random filesystems to use if none is already requested
  IF LENGTH(srRfs) IS NULL THEN
    FOR line IN
      (SELECT candidate FROM
         (SELECT UNIQUE FIRST_VALUE (DiskServer.name || ':' || FileSystem.mountPoint)
                   OVER (PARTITION BY DiskServer.id ORDER BY DBMS_Random.value) AS candidate
            FROM DiskServer, FileSystem, DiskPool2SvcClass
           WHERE FileSystem.diskServer = DiskServer.id
             AND FileSystem.diskPool = DiskPool2SvcClass.parent
             AND DiskPool2SvcClass.child = SvcClassId
             AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
             AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
             AND FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > srXsize
             -- this is to avoid disk2diskcopies to create new copies on diskservers already having one
             AND DiskServer.id NOT IN
               (SELECT diskserver FROM DiskCopy, FileSystem
                 WHERE DiskCopy.castorFile = cfId
                   AND DiskCopy.status IN (dconst.DISKCOPY_STAGED, dconst.DISKCOPY_WAITDISK2DISKCOPY,
                                           dconst.DISKCOPY_CANBEMIGR)
                   AND FileSystem.id = DiskCopy.fileSystem)
           ORDER BY DBMS_Random.value)
        WHERE ROWNUM <= 5) LOOP
      IF LENGTH(srRfs) IS NOT NULL THEN srRfs := srRfs || '|'; END IF;
      srRfs := srRfs || line.candidate;
    END LOOP;
  END IF;

END;
/

/* PL/SQL method implementing transfersToAbort */
CREATE OR REPLACE
PROCEDURE transfersToAbortProc(srUuidCur OUT castor.UUIDRecord_Cur) AS
  srUuid VARCHAR2(2048);
  srUuids strListTable;
  unusedMessage VARCHAR2(2048);
  unusedStatus INTEGER;
BEGIN
  BEGIN
    -- find out whether there is something
    SELECT uuid INTO srUuid FROM TransfersToAbort WHERE ROWNUM < 2;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- There is nothing to abort. Wait for next alert concerning something
    -- to abort or at least 3 seconds.
    DBMS_ALERT.WAITONE('transfersToAbort', unusedMessage, unusedStatus, 3);
  END;
  -- we want to delete what we will return but deleting multiple rows is a nice way
  -- to have deadlocks. So we first take locks in NOWAIT mode
  SELECT uuid BULK COLLECT INTO srUuids FROM transfersToAbort FOR UPDATE SKIP LOCKED;
  DELETE FROM transfersToAbort WHERE uuid IN (SELECT * FROM TABLE(srUuids));
  -- Either we found something or we timed out, in both cases
  -- we go back to python so that it can handle cases like signals and exit
  -- We will probably be back soon :-)
  OPEN srUuidCur FOR 
    SELECT * FROM TABLE(srUuids);
END;
/

/* PL/SQL method implementing syncRunningTransfers
 * This is called by the transfer manager daemon on the restart of a disk server manager
 * in order to sync running transfers in the database with the reality of the machine.
 * This is particularly useful to terminate cleanly transfers interupted by a power cut
 */
CREATE OR REPLACE
PROCEDURE syncRunningTransfers(machine IN VARCHAR2,
                               transfers IN castor."strList",
                               killedTransfersCur OUT castor.TransferRecord_Cur) AS
  unused VARCHAR2(2048);
  fileid NUMBER;
  nsHost VARCHAR2(2048);
  reqId VARCHAR2(2048);
  killedTransfers castor."strList";
  errnos castor."cnumList";
  errmsg castor."strList";
BEGIN
  -- cleanup from previous round
  DELETE FROM SyncRunningTransfersHelper2;
  -- insert the list of running transfers into a temporary table for easy access
  FORALL i IN transfers.FIRST .. transfers.LAST
    INSERT INTO SyncRunningTransfersHelper VALUES (transfers(i));
  -- Go through all running transfers from the DB point of view for the given diskserver
  FOR SR IN (SELECT SubRequest.id, SubRequest.subreqId, SubRequest.castorfile, SubRequest.request
               FROM SubRequest, DiskCopy, FileSystem, DiskServer
              WHERE SubRequest.status = dconst.SUBREQUEST_READY
                AND Subrequest.diskCopy = DiskCopy.id
                AND DiskCopy.fileSystem = FileSystem.id
                AND DiskCopy.status = dconst.DISKCOPY_STAGEOUT
                AND FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = machine) LOOP
    BEGIN
      -- check if they are running on the diskserver
      SELECT subReqId INTO unused FROM SyncRunningTransfersHelper
       WHERE subreqId = SR.subreqId;
      -- this one was still running, nothing to do then
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- this transfer is not running anymore although the stager DB believes it is
      -- we first get its reqid and fileid
      SELECT Request.reqId INTO reqId FROM
        (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ reqId, id from StageGetRequest UNION ALL
         SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ reqId, id from StagePutRequest UNION ALL
         SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ reqId, id from StageUpdateRequest UNION ALL
         SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ reqId, id from StageRepackRequest) Request
       WHERE Request.id = SR.request;
      SELECT fileid, nsHost INTO fileid, nsHost FROM CastorFile WHERE id = SR.castorFile;
      -- and we put it in the list of transfers to be failed with code 1015 (SEINTERNAL)
      INSERT INTO SyncRunningTransfersHelper2 VALUES (SR.subreqId, reqId, fileid, nsHost, 1015, 'Transfer has been killed while running');
    END;
  END LOOP;
  -- fail the transfers that are no more running
  SELECT subreqId, errorCode, errorMsg BULK COLLECT
    INTO killedTransfers, errnos, errmsg
    FROM SyncRunningTransfersHelper2;
  -- Note that the next call will commit (even once per transfer to kill)
  -- This is ok as SyncRunningTransfersHelper2 was declared "ON COMMIT PRESERVE ROWS" and
  -- is a temporary table so it's content is only visible to our connection.
  transferFailedSafe(killedTransfers, errnos, errmsg);
  -- and return list of transfers that have been failed, for logging purposes
  OPEN killedTransfersCur FOR SELECT subreqId, reqId, fileid, nsHost FROM SyncRunningTransfersHelper2;
END;
/
/*******************************************************************
 *
 * @(#)RCSfile: oracleQuery.sql,v  Revision: 1.661  Date: 2009/06/17 10:55:44  Author: itglp 
 *
 * PL/SQL code for the stager query service
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/


/*
 * PL/SQL method implementing the core part of stage queries
 * It takes a list of castorfile ids as input
 */
CREATE OR REPLACE PROCEDURE internalStageQuery
 (cfs IN "numList",
  svcClassId IN NUMBER,
  euid IN INTEGER, egid IN INTEGER,
  result OUT castor.QueryLine_Cur) AS
BEGIN
  -- Here we get the status for each castorFile as follows: if a valid diskCopy is found,
  -- or if a request is found and its related diskCopy exists, the diskCopy status
  -- is returned, else -1 (INVALID) is returned.
  -- The case of svcClassId = 0 (i.e. '*') is handled separately for performance reasons
  -- and because it may include a check for read permissions.
  IF svcClassId = 0 THEN
    OPEN result FOR
      SELECT fileId, nsHost, dcId, path, fileSize, status, machine, mountPoint, nbCopyAccesses,
             lastKnownFileName, creationTime, svcClass, lastAccessTime, hwStatus
        FROM (
          SELECT UNIQUE CastorFile.id, CastorFile.fileId, CastorFile.nsHost, DC.id AS dcId,
                 DC.path, CastorFile.fileSize, DC.status,
                 CASE WHEN DC.svcClass IS NULL THEN
                   (SELECT /*+ INDEX(Subrequest I_Subrequest_DiskCopy)*/ UNIQUE Req.svcClassName
                      FROM SubRequest,
                        (SELECT /*+ INDEX(StageGetRequest PK_StagePrepareToGetRequest_Id) */ id, svcClassName FROM StagePrepareToGetRequest    UNION ALL
                         SELECT /*+ INDEX(StageGetRequest PK_StagePrepareToPutRequest_Id) */ id, svcClassName FROM StagePrepareToPutRequest    UNION ALL
                         SELECT /*+ INDEX(StageGetRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcClassName FROM StagePrepareToUpdateRequest UNION ALL
                         SELECT /*+ INDEX(StageGetRequest PK_StageRepackRequest_Id) */ id, svcClassName FROM StageRepackRequest                UNION ALL
                         SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClassName FROM StageGetRequest) Req
                          WHERE SubRequest.diskCopy = DC.id
                            AND request = Req.id)              
                   ELSE DC.svcClass END AS svcClass,
                 DC.machine, DC.mountPoint, DC.nbCopyAccesses, CastorFile.lastKnownFileName,
                 DC.creationTime, DC.lastAccessTime, nvl(decode(DC.hwStatus, 2, 1, DC.hwStatus), -1) hwStatus
            FROM CastorFile,
              (SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, DiskServer.name AS machine, FileSystem.mountPoint,
                      SvcClass.name AS svcClass, DiskCopy.filesystem, DiskCopy.CastorFile, 
                      DiskCopy.nbCopyAccesses, DiskCopy.creationTime, DiskCopy.lastAccessTime,
                      FileSystem.status + DiskServer.status AS hwStatus
                 FROM FileSystem, DiskServer, DiskPool2SvcClass, SvcClass,
                   (SELECT id, status, filesystem, castorFile, path, nbCopyAccesses, creationTime, lastAccessTime
                      FROM DiskCopy
                     WHERE CastorFile IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
                       AND status IN (0, 1, 2, 4, 5, 6, 7, 10, 11) -- search for diskCopies not BEINGDELETED
                     GROUP BY (id, status, filesystem, castorfile, path, nbCopyAccesses, creationTime, lastAccessTime)) DiskCopy
                WHERE FileSystem.id(+) = DiskCopy.fileSystem
                  AND nvl(FileSystem.status, 0) IN (0, 1) -- PRODUCTION, DRAINING
                  AND DiskServer.id(+) = FileSystem.diskServer
                  AND nvl(DiskServer.status, 0) IN (0, 1) -- PRODUCTION, DRAINING
                  AND DiskPool2SvcClass.parent(+) = FileSystem.diskPool
                  AND SvcClass.id(+) = DiskPool2SvcClass.child
                  AND (euid = 0 OR SvcClass.id IS NULL OR   -- if euid > 0 check read permissions for srmLs (see bug #69678)
                       checkPermission(SvcClass.name, euid, egid, 35) = 0)   -- OBJ_StageGetRequest
                 ) DC
           WHERE CastorFile.id = DC.castorFile)
       WHERE status IS NOT NULL    -- search for valid diskcopies
       ORDER BY fileid, nshost;
  ELSE
    OPEN result FOR
      SELECT fileId, nsHost, dcId, path, fileSize, status, machine, mountPoint, nbCopyAccesses,
             lastKnownFileName, creationTime, (SELECT name FROM svcClass WHERE id = svcClassId),
             lastAccessTime, hwStatus
        FROM (
          SELECT UNIQUE CastorFile.id, CastorFile.fileId, CastorFile.nsHost, DC.id AS dcId,
                 DC.path, CastorFile.fileSize,
                 CASE WHEN DC.dcSvcClass = svcClassId THEN DC.status
                      WHEN DC.fileSystem = 0 THEN
                       (SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/
                        UNIQUE decode(nvl(SubRequest.status, -1), -1, -1, DC.status)
                          FROM SubRequest,
                            (SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, svcclass, svcClassName FROM StagePrepareToGetRequest       UNION ALL
                             SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id, svcclass, svcClassName FROM StagePrepareToPutRequest       UNION ALL
                             SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcclass, svcClassName FROM StagePrepareToUpdateRequest UNION ALL
                             SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, svcclass, svcClassName FROM StageRepackRequest                         UNION ALL
                             SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcclass, svcClassName FROM StageGetRequest) Req
                         WHERE SubRequest.CastorFile = CastorFile.id
                           AND SubRequest.request = Req.id
                           AND svcClass = svcClassId)
                      END AS status,
                 DC.machine, DC.mountPoint, DC.nbCopyAccesses, CastorFile.lastKnownFileName,
                 DC.creationTime, DC.lastAccessTime, nvl(decode(DC.hwStatus, 2, 1, DC.hwStatus), -1) hwStatus
            FROM CastorFile,
              (SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, DiskServer.name AS machine, FileSystem.mountPoint,
                      DiskPool2SvcClass.child AS dcSvcClass, DiskCopy.filesystem, DiskCopy.CastorFile, 
                      DiskCopy.nbCopyAccesses, DiskCopy.creationTime, DiskCopy.lastAccessTime,
                      FileSystem.status + DiskServer.status AS hwStatus
                 FROM FileSystem, DiskServer, DiskPool2SvcClass,
                   (SELECT id, status, filesystem, castorFile, path, nbCopyAccesses, creationTime, lastAccessTime
                      FROM DiskCopy
                     WHERE CastorFile IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
                       AND status IN (0, 1, 2, 4, 5, 6, 7, 10, 11)  -- search for diskCopies not GCCANDIDATE or BEINGDELETED
                     GROUP BY (id, status, filesystem, castorfile, path, nbCopyAccesses, creationTime, lastAccessTime)) DiskCopy
                WHERE FileSystem.id(+) = DiskCopy.fileSystem
                  AND nvl(FileSystem.status, 0) IN (0, 1) -- PRODUCTION, DRAINING
                  AND DiskServer.id(+) = FileSystem.diskServer
                  AND nvl(DiskServer.status, 0) IN (0, 1) -- PRODUCTION, DRAINING
                  AND DiskPool2SvcClass.parent(+) = FileSystem.diskPool) DC
                  -- No extra check on read permissions here, it is not relevant
           WHERE CastorFile.id = DC.castorFile)
       WHERE status IS NOT NULL     -- search for valid diskcopies
       ORDER BY fileid, nshost;
   END IF;
END;
/


/*
 * PL/SQL method implementing the stager_qry based on file name for directories
 */
CREATE OR REPLACE PROCEDURE fileNameStageQuery
 (fn IN VARCHAR2,
  svcClassId IN INTEGER,
  euid IN INTEGER,
  egid IN INTEGER,
  maxNbResponses IN INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfIds "numList";
BEGIN
  IF substr(fn, -1, 1) = '/' THEN  -- files in a 'subdirectory'
    SELECT /*+ INDEX(CastorFile I_CastorFile_LastKnownFileName) INDEX(DiskCopy I_DiskCopy_CastorFile) */ 
           CastorFile.id
      BULK COLLECT INTO cfIds
      FROM DiskCopy, FileSystem, DiskPool2SvcClass, CastorFile
     WHERE CastorFile.lastKnownFileName LIKE normalizePath(fn)||'%'
       AND DiskCopy.castorFile = CastorFile.id
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND (DiskPool2SvcClass.child = svcClassId OR svcClassId = 0)
       AND ROWNUM <= maxNbResponses + 1;
  -- ELSE exact match, not implemented here any longer but in fileIdStageQuery 
  END IF;
  IF cfIds.COUNT > maxNbResponses THEN
    -- We have too many rows, we just give up
    raise_application_error(-20102, 'Too many matching files');
  END IF;
  internalStageQuery(cfIds, svcClassId, euid, egid, result);
END;
/


/*
 * PL/SQL method implementing the stager_qry based on file id or single filename
 */
CREATE OR REPLACE PROCEDURE fileIdStageQuery
 (fid IN NUMBER,
  nh IN VARCHAR2,
  svcClassId IN INTEGER,
  euid IN INTEGER,
  egid IN INTEGER,
  fileName IN VARCHAR2,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
  currentFileName VARCHAR2(2048);
  nsHostName VARCHAR2(2048);
BEGIN
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- Extract CastorFile ids from the fileid
  SELECT id BULK COLLECT INTO cfs FROM CastorFile 
   WHERE fileId = fid AND nshost = nsHostName;
  -- Check and fix when needed the LastKnownFileNames
  IF (cfs.COUNT > 0) THEN
    SELECT lastKnownFileName INTO currentFileName
      FROM CastorFile
     WHERE id = cfs(cfs.FIRST);
    IF currentFileName != fileName THEN
      UPDATE CastorFile SET lastKnownFileName = fileName
       WHERE id = cfs(cfs.FIRST);
      COMMIT;
    END IF;
  END IF;
  -- Finally issue the actual query
  internalStageQuery(cfs, svcClassId, euid, egid, result);
END;
/


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
  SELECT /*+ NO_USE_HASH(REQLIST SR) USE_NL(REQLIST SR) 
             INDEX(SR I_SUBREQUEST_REQUEST) */
         sr.castorfile BULK COLLECT INTO cfs
    FROM SubRequest sr,
         (SELECT /*+ INDEX(StagePrepareToGetRequest I_StagePTGRequest_ReqId) */ id
            FROM StagePreparetogetRequest
           WHERE reqid = rid
          UNION ALL
          SELECT /*+ INDEX(StagePrepareToPutRequest I_StagePTPRequest_ReqId) */ id
            FROM StagePreparetoputRequest
           WHERE reqid = rid
          UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest I_StagePTURequest_ReqId) */ id
            FROM StagePreparetoupdateRequest
           WHERE reqid = rid
          UNION ALL
          SELECT /*+ INDEX(StageGetRequest I_StageGetRequest_ReqId) */ id
            FROM stageGetRequest
           WHERE reqid = rid
          UNION ALL
          SELECT /*+ INDEX(stagePutRequest I_stagePutRequest_ReqId) */ id
            FROM stagePutRequest
           WHERE reqid = rid
          UNION ALL
          SELECT /*+ INDEX(StageRepackRequest I_StageRepackRequest_ReqId) */ id
            FROM StageRepackRequest
           WHERE reqid LIKE rid) reqlist
   WHERE sr.request = reqlist.id;
  IF cfs.COUNT > 0 THEN
    internalStageQuery(cfs, svcClassId, 0, 0, result);
  ELSE
    notfound := 1;
  END IF;
END;
/


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
  SELECT /*+ NO_USE_HASH(REQLIST SR) USE_NL(REQLIST SR) 
             INDEX(SR I_SUBREQUEST_REQUEST) */
         sr.castorfile BULK COLLECT INTO cfs
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
    internalStageQuery(cfs, svcClassId, 0, 0, result);
  ELSE
    notfound := 1;
  END IF;
END;
/


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
    FROM (SELECT /*+ INDEX(StagePrepareToGetRequest I_StagePTGRequest_ReqId) */ id
            FROM StagePreparetogetRequest
           WHERE reqid = rid
          UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest I_StagePTURequest_ReqId) */ id
            FROM StagePreparetoupdateRequest
           WHERE reqid = rid
          UNION ALL
          SELECT /*+ INDEX(StageRepackRequest I_StageRepackRequest_ReqId) */ id
            FROM StageRepackRequest
           WHERE reqid = rid
          );
  IF reqs.COUNT > 0 THEN
    UPDATE /*+ INDEX(Subrequest I_Subrequest_Request)*/ SubRequest 
       SET getNextStatus = 2  -- GETNEXTSTATUS_NOTIFIED
     WHERE getNextStatus = 1  -- GETNEXTSTATUS_FILESTAGED
       AND request IN (SELECT * FROM TABLE(reqs))
    RETURNING castorfile BULK COLLECT INTO cfs;
    internalStageQuery(cfs, svcClassId, 0, 0, result);
  ELSE
    notfound := 1;
  END IF;
END;
/


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
    UPDATE /*+ INDEX(Subrequest I_Subrequest_Request)*/ SubRequest 
       SET getNextStatus = 2  -- GETNEXTSTATUS_NOTIFIED
     WHERE getNextStatus = 1  -- GETNEXTSTATUS_FILESTAGED
       AND request IN (SELECT * FROM TABLE(reqs))
    RETURNING castorfile BULK COLLECT INTO cfs;
    internalStageQuery(cfs, svcClassId, 0, 0, result);
  ELSE
    notfound := 1;
  END IF;
END;
/

/* Internal function used by describeDiskPool[s] to return either available
 * (i.e. the space on PRODUCTION status resources) or total space depending on
 * the type of query */
CREATE OR REPLACE FUNCTION getSpace(status IN NUMBER, space IN NUMBER,
                                    queryType IN NUMBER, spaceType IN NUMBER)
RETURN NUMBER IS
BEGIN
  IF space < 0 THEN
    -- over used filesystems may report negative numbers, just cut to 0
    RETURN 0;
  END IF;
  IF (status > 0) AND  -- not in production
     (queryType = dconst.DISKPOOLQUERYTYPE_AVAILABLE OR
      (queryType = dconst.DISKPOOLQUERYTYPE_DEFAULT AND spaceType = dconst.DISKPOOLSPACETYPE_FREE)) THEN
    return 0;
  ELSE
    RETURN space;
  END IF;
END;
/

/*
 * PL/SQL method implementing the diskPoolQuery when listing pools
 */
CREATE OR REPLACE PROCEDURE describeDiskPools
(svcClassName IN VARCHAR2, reqEuid IN INTEGER, reqEgid IN INTEGER, queryType IN INTEGER,
 res OUT NUMBER, result OUT castor.DiskPoolsQueryLine_Cur) AS
BEGIN
  -- We use here analytic functions and the grouping sets functionality to
  -- get both the list of filesystems and a summary per diskserver and per
  -- diskpool. The grouping analytic function also allows to mark the summary
  -- lines for easy detection in the C++ code
  IF svcClassName = '*' THEN
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
             grouping(fs.mountPoint) AS IsFSGrouped,
             dp.name, ds.name, ds.status, fs.mountPoint,
             sum(getSpace(fs.status + ds.status,
                          fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_FREE)) AS freeSpace,
             sum(getSpace(fs.status + ds.status,
                          fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_CAPACITY)) AS totalSize,
             fs.minFreeSpace, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp
       WHERE dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         GROUP BY grouping sets(
             (dp.name, ds.name, ds.status, fs.mountPoint,
              getSpace(fs.status + ds.status,
                       fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_FREE),
              getSpace(fs.status + ds.status,
                       fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_CAPACITY),
              fs.minFreeSpace, fs.maxFreeSpace, fs.status),
             (dp.name, ds.name, ds.status),
             (dp.name)
            )
         ORDER BY dp.name, IsDSGrouped DESC, ds.name, IsFSGrouped DESC, fs.mountpoint;
  ELSE 
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
             grouping(fs.mountPoint) AS IsFSGrouped,
             dp.name, ds.name, ds.status, fs.mountPoint,
             sum(getSpace(fs.status + ds.status,
                          fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_FREE)) AS freeSpace,
             sum(getSpace(fs.status + ds.status,
                          fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_CAPACITY)) AS totalSize,
             fs.minFreeSpace, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp,
             DiskPool2SvcClass d2s, SvcClass sc
       WHERE sc.name = svcClassName
         AND sc.id = d2s.child
         AND checkPermission(sc.name, reqEuid, reqEgid, 195) = 0
         AND d2s.parent = dp.id
         AND dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         GROUP BY grouping sets(
             (dp.name, ds.name, ds.status, fs.mountPoint,
              getSpace(fs.status + ds.status,
                       fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_FREE),
              getSpace(fs.status + ds.status,
                       fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_CAPACITY),
              fs.minFreeSpace, fs.maxFreeSpace, fs.status),
             (dp.name, ds.name, ds.status),
             (dp.name)
            )
         ORDER BY dp.name, IsDSGrouped DESC, ds.name, IsFSGrouped DESC, fs.mountpoint;
  END IF;
  -- If no results are available, check to see if any diskpool exists and if
  -- access to view all the diskpools has been revoked. The information extracted
  -- here will be used to send an appropriate error message to the client.
  IF result%ROWCOUNT = 0 THEN
    SELECT CASE count(*)
           WHEN sum(checkPermission(sc.name, reqEuid, reqEgid, 195)) THEN -1
           ELSE count(*) END
      INTO res
      FROM DiskPool2SvcClass d2s, SvcClass sc
     WHERE d2s.child = sc.id
       AND (sc.name = svcClassName OR svcClassName = '*');
  END IF;
END;
/



/*
 * PL/SQL method implementing the diskPoolQuery for a given pool
 */
CREATE OR REPLACE PROCEDURE describeDiskPool
(diskPoolName IN VARCHAR2, svcClassName IN VARCHAR2, queryType IN INTEGER,
 res OUT NUMBER, result OUT castor.DiskPoolQueryLine_Cur) AS
BEGIN
  -- We use here analytic functions and the grouping sets functionnality to
  -- get both the list of filesystems and a summary per diskserver and per
  -- diskpool. The grouping analytic function also allows to mark the summary
  -- lines for easy detection in the C++ code
  IF svcClassName = '*' THEN
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
             grouping(fs.mountPoint) AS IsGrouped,
             ds.name, ds.status, fs.mountPoint,
             sum(getSpace(fs.status + ds.status,
                          fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_FREE)) AS freeSpace,
             sum(getSpace(fs.status + ds.status,
                          fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_CAPACITY)) AS totalSize,
             fs.minFreeSpace, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp
       WHERE dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         AND dp.name = diskPoolName
         GROUP BY grouping sets(
             (ds.name, ds.status, fs.mountPoint,
              getSpace(fs.status + ds.status,
                       fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_FREE),
              getSpace(fs.status + ds.status,
                       fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_CAPACITY),
              fs.minFreeSpace, fs.maxFreeSpace, fs.status),
             (ds.name, ds.status),
             (dp.name)
            )
         ORDER BY IsDSGrouped DESC, ds.name, IsGrouped DESC, fs.mountpoint;
  ELSE
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
             grouping(fs.mountPoint) AS IsGrouped,
             ds.name, ds.status, fs.mountPoint,
             sum(getSpace(fs.status + ds.status,
                          fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_FREE)) AS freeSpace,
             sum(getSpace(fs.status + ds.status,
                          fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_CAPACITY)) AS totalSize,
             fs.minFreeSpace, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp,
             DiskPool2SvcClass d2s, SvcClass sc
       WHERE sc.name = svcClassName
         AND sc.id = d2s.child
         AND d2s.parent = dp.id
         AND dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         AND dp.name = diskPoolName
         GROUP BY grouping sets(
             (ds.name, ds.status, fs.mountPoint,
              getSpace(fs.status + ds.status,
                       fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_FREE),
              getSpace(fs.status + ds.status,
                       fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_CAPACITY),
              fs.minFreeSpace, fs.maxFreeSpace, fs.status),
             (ds.name, ds.status),
             (dp.name)
            )
         ORDER BY IsDSGrouped DESC, ds.name, IsGrouped DESC, fs.mountpoint;
  END IF;
  -- If no results are available, check to see if any diskpool exists and if
  -- access to view all the diskpools has been revoked. The information extracted
  -- here will be used to send an appropriate error message to the client.
  IF result%ROWCOUNT = 0 THEN
    SELECT count(*) INTO res
      FROM DiskPool dp, DiskPool2SvcClass d2s, SvcClass sc
     WHERE d2s.child = sc.id
       AND d2s.parent = dp.id
       AND dp.name = diskPoolName
       AND (sc.name = svcClassName OR svcClassName = '*');
  END IF;
END;
/
/*******************************************************************	
 * @(#)RCSfile: oracleTape.sql,v  Revision: 1.761  Date: 2009/08/10 15:30:13  Author: itglp 
 *
 * PL/SQL code for the interface to the tape system
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

 /* PL/SQL declaration for the castorTape package */
CREATE OR REPLACE PACKAGE castorTape AS 
   TYPE TapeGatewayRequestExtended IS RECORD (
    accessMode NUMBER,
    id NUMBER,
    starttime NUMBER,
    lastvdqmpingtime NUMBER, 
    vdqmvolreqid NUMBER, 
    vid VARCHAR2(2048));
  TYPE TapeGatewayRequest_Cur IS REF CURSOR RETURN TapeGatewayRequestExtended;
  TYPE TapeGatewayRequestCore IS RECORD (
    tpmode INTEGER,
    side INTEGER,
    vid VARCHAR2(2048),
    tapeRequestId NUMBER);
  TYPE TapeGatewayRequestCore_Cur IS REF CURSOR RETURN TapeGatewayRequestCore;
  TYPE MigrationMountCore IS RECORD (
    id INTEGER,
    tapePoolName VARCHAR2(2048));
  TYPE MigrationMount_Cur IS REF CURSOR RETURN MigrationMountCore; 
  TYPE DbMigrationInfo IS RECORD (
    id NUMBER,
    copyNb NUMBER,
    fileName VARCHAR2(2048),
    nsHost VARCHAR2(2048),
    fileId NUMBER,
    fileSize NUMBER);
  TYPE DbMigrationInfo_Cur IS REF CURSOR RETURN DbMigrationInfo;
  TYPE DbRecallInfo IS RECORD (
    vid VARCHAR2(2048),
    tapeId NUMBER,
    dataVolume NUMBER,
    numbFiles NUMBER,
    expireTime NUMBER,
    priority NUMBER);
  TYPE DbRecallInfo_Cur IS REF CURSOR RETURN DbRecallInfo;
  TYPE RecallMountsForPolicy IS RECORD (
    tapeId             NUMBER,
    vid                VARCHAR2(2048),
    numSegments        NUMBER,
    totalBytes         NUMBER,
    ageOfOldestSegment NUMBER,
    priority           NUMBER,
    status             NUMBER);
  TYPE RecallMountsForPolicy_Cur IS REF CURSOR RETURN RecallMountsForPolicy;
  TYPE FileToRecallCore IS RECORD (
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   diskserver VARCHAR2(2048),
   mountPoint VARCHAR(2048),
   path VARCHAR2(2048),
   fseq INTEGER,
   fileTransactionId NUMBER);
  TYPE FileToRecallCore_Cur IS REF CURSOR RETURN  FileToRecallCore;  
  TYPE FileToMigrateCore IS RECORD (
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   lastModificationTime NUMBER,
   diskserver VARCHAR2(2048),
   mountPoint VARCHAR(2048),
   path VARCHAR2(2048),
   lastKnownFilename VARCHAR2(2048), 
   fseq INTEGER,
   fileSize NUMBER,
   fileTransactionId NUMBER);
  TYPE FileToMigrateCore_Cur IS REF CURSOR RETURN  FileToMigrateCore;  
END castorTape;
/

/* Trigger ensuring validity of VID in state transitions */
CREATE OR REPLACE TRIGGER TR_RecallJob_VID
BEFORE INSERT OR UPDATE OF Status ON RecallJob
FOR EACH ROW
BEGIN
  /* Enforce the state integrity of VID in state transitions */
  
  CASE :new.status
    WHEN  tconst.RECALLJOB_SELECTED THEN
      /* The VID MUST be defined when the RecallJob gets selected */
      IF :new.VID IS NULL THEN
        RAISE_APPLICATION_ERROR(-20119,
          'Moving/creating (in)to RECALLJOB_SELECTED State without a VID (TC.ID: '||
          :new.ID||' VID:'|| :old.VID||'=>'||:new.VID||' Status:'||:old.status||'=>'||:new.status||')');
      END IF;
    WHEN tconst.RECALLJOB_STAGED THEN
       /* The VID MUST be defined when the RecallJob goes to staged */
       IF :new.VID IS NULL THEN
         RAISE_APPLICATION_ERROR(-20119,
          'Moving/creating (in)to RECALLJOB_STAGED State without a VID (TC.ID: '||
          :new.ID||' VID:'|| :old.VID||'=>'||:new.VID||' Status:'||:old.status||'=>'||:new.status||')');
       END IF;
       /* The VID MUST remain the same when going to staged */
       IF :new.VID != :old.VID THEN
         RAISE_APPLICATION_ERROR(-20119,
           'Moving to STAGED State without carrying the VID over');
       END IF;
    ELSE
      /* In all other cases, VID should be NULL */
      IF :new.VID IS NOT NULL THEN
        RAISE_APPLICATION_ERROR(-20119,
          'Moving/creating (in)to RecallJob state where VID makes no sense, yet VID!=NULL (TC.ID: '||
          :new.ID||' VID:'|| :old.VID||'=>'||:new.VID||' Status:'||:old.status||'=>'||:new.status||')');
      END IF;
  END CASE;
END;
/

/* PL/SQL method implementing bestFileSystemForSegment */
CREATE OR REPLACE PROCEDURE bestFileSystemForSegment(segmentId IN INTEGER, diskServerName OUT VARCHAR2,
                                                     rmountPoint OUT VARCHAR2, rpath OUT VARCHAR2,
                                                     dcid OUT INTEGER) AS
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
    FROM RecallJob, Segment, DiskCopy
   WHERE Segment.id = segmentId
     AND Segment.copy = RecallJob.id
     AND DiskCopy.castorfile = RecallJob.castorfile
     AND DiskCopy.status = dconst.DISKCOPY_WAITTAPERECALL;
  -- Check if the DiskCopy had a FileSystem associated
  IF fileSystemId > 0 THEN
    BEGIN
      -- It had one, force filesystem selection, unless it was disabled.
      SELECT DiskServer.name, DiskServer.id, FileSystem.mountPoint
        INTO diskServerName, fsDiskServer, rmountPoint
        FROM DiskServer, FileSystem
       WHERE FileSystem.id = fileSystemId
         AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
         AND DiskServer.id = FileSystem.diskServer
         AND DiskServer.status = dconst.DISKSERVER_PRODUCTION;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Error, the filesystem or the machine was probably disabled in between
      raise_application_error(-20101, 'In a multi-segment file, FileSystem or Machine was disabled before all segments were recalled');
    END;
  ELSE
    fileSystemId := 0;
    -- The DiskCopy had no FileSystem associated with it which indicates that
    -- This is a new recall. We try and select a good FileSystem for it!
    FOR a IN (SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/
                     DiskServer.name, FileSystem.mountPoint, FileSystem.id,
                     FileSystem.diskserver, CastorFile.fileSize
                FROM DiskServer, FileSystem, DiskPool2SvcClass,
                     (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClass from StageGetRequest                            UNION ALL
                      SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, svcClass from StagePrepareToGetRequest UNION ALL
                      SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, svcClass from StageRepackRequest                   UNION ALL
                      SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClass from StageUpdateRequest                   UNION ALL
                      SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcClass from StagePrepareToUpdateRequest) Request,
                      SubRequest, CastorFile
               WHERE CastorFile.id = cfid
                 AND SubRequest.castorfile = cfid
                 AND SubRequest.status = dconst.SUBREQUEST_WAITTAPERECALL
                 AND Request.id = SubRequest.request
                 AND Request.svcclass = DiskPool2SvcClass.child
                 AND FileSystem.diskpool = DiskPool2SvcClass.parent
                 AND FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > CastorFile.fileSize
                 AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
                 AND DiskServer.id = FileSystem.diskServer
                 AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
            ORDER BY -- first prefer DSs without concurrent migrators/recallers
                     DiskServer.nbRecallerStreams ASC, FileSystem.nbMigratorStreams ASC,
                     -- then order by rate as defined by the function
                     fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                                    FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams,
                                    FileSystem.nbRecallerStreams) DESC,
                     -- finally use randomness to avoid preferring always the same FS
                     DBMS_Random.value)
    LOOP
      diskServerName := a.name;
      rmountPoint    := a.mountPoint;
      fileSystemId   := a.id;
      -- Check that we don't already have a copy of this file on this filesystem.
      -- This will never happen in normal operations but may be the case if a filesystem
      -- was disabled and did come back while the tape recall was waiting.
      -- Even if we optimize by cancelling remaining unneeded tape recalls when a
      -- fileSystem comes back, the ones running at the time of the come back will have
      -- the problem.
      SELECT count(*) INTO nb
        FROM DiskCopy
       WHERE fileSystem = a.id
         AND castorfile = cfid
         AND status = dconst.DISKCOPY_STAGED;
      IF nb != 0 THEN
        raise_application_error(-20103, 'Recaller could not find a FileSystem in production in the requested SvcClass and without copies of this file');
      END IF;
      -- Set the diskcopy's filesystem
      UPDATE DiskCopy
         SET fileSystem = a.id
       WHERE id = dcid;
      RETURN;
    END LOOP;

    IF fileSystemId = 0 THEN
      raise_application_error(-20115, 'No suitable filesystem found for this recalled segment');
    END IF;
  END IF;
END;
/

/** Functions for the RecHandlerDaemon **/

/* Get input for python recall policy */
CREATE OR REPLACE PROCEDURE inputForRecallPolicy(dbInfo OUT castorTape.DbRecallInfo_Cur) AS
  svcId NUMBER;
BEGIN
  OPEN dbInfo FOR
    SELECT
       /*+ NO_USE_MERGE(TAPE SEGMENT RECALLJOB CASTORFILE)
           NO_USE_HASH(TAPE SEGMENT RECALLJOB CASTORFILE)
           INDEX_RS_ASC(SEGMENT I_SEGMENT_TAPE)
           INDEX_RS_ASC(TAPE I_TAPE_STATUS)
           INDEX_RS_ASC(RECALLJOB PK_RECALLJOB_ID)
           INDEX_RS_ASC(CASTORFILE PK_CASTORFILE_ID) */
       Tape.id,
       Tape.vid,
       count(distinct segment.id),
       sum(CastorFile.fileSize),
       getTime() - min(Segment.creationTime) age,
       max(Segment.priority)
      FROM RecallJob, CastorFile, Segment, Tape
     WHERE Tape.id = Segment.tape
       AND RecallJob.id = Segment.copy
       AND CastorFile.id = RecallJob.castorfile
       AND Tape.status IN (tconst.TAPE_PENDING, tconst.TAPE_WAITDRIVE, tconst.TAPE_WAITPOLICY)
       AND Segment.status = tconst.SEGMENT_UNPROCESSED
     GROUP BY Tape.id, Tape.vid
     ORDER BY age DESC;
END;
/

CREATE OR REPLACE PROCEDURE tapesAndMountsForRecallPolicy (
  outRecallMounts      OUT castorTape.RecallMountsForPolicy_Cur,
  outNbMountsForRecall OUT NUMBER)
AS
-- Retrieves the input for the rechandler daemon to pass to the
-- rechandler-policy Python-function
--
-- @param outRecallMounts      List of recall-mounts which are either pending,
--                             waiting for a drive or waiting for the
--                             rechandler-policy.
-- @param outNbMountsForRecall The number of tapes currently mounted for recall
--                             for this stager.
BEGIN
  SELECT count(distinct Tape.vid )
    INTO outNbMountsForRecall
    FROM Tape
   WHERE Tape.status = tconst.TAPE_MOUNTED
     AND TPMODE = tconst.TPMODE_READ;

    OPEN outRecallMounts
     FOR SELECT /*+ NO_USE_MERGE(TAPE SEGMENT RECALLJOB CASTORFILE) NO_USE_HASH(TAPE SEGMENT RECALLJOB CASTORFILE) INDEX_RS_ASC(SEGMENT I_SEGMENT_TAPE) INDEX_RS_ASC(TAPE I_TAPE_STATUS) INDEX_RS_ASC(TAPE
COPY PK_RECALLJOB_ID) INDEX_RS_ASC(CASTORFILE PK_CASTORFILE_ID) */ Tape.id,
                Tape.vid,
                count ( distinct segment.id ),
                sum ( CastorFile.fileSize ),
                getTime ( ) - min ( Segment.creationTime ) age,
                max ( Segment.priority ),
                Tape.status
           FROM RecallJob,
                CastorFile,
                Segment,
                Tape
          WHERE Tape.id = Segment.tape
            AND RecallJob.id = Segment.copy
            AND CastorFile.id = RecallJob.castorfile
            AND Tape.status IN (tconst.TAPE_PENDING, tconst.TAPE_WAITDRIVE, tconst.TAPE_WAITPOLICY)
            AND Segment.status = tconst.SEGMENT_UNPROCESSED
          GROUP BY Tape.id, Tape.vid, Tape.status
          ORDER BY age DESC;
    
END tapesAndMountsForRecallPolicy;
/

/* resurrect tapes */
CREATE OR REPLACE PROCEDURE resurrectTapes
(tapeIds IN castor."cnumList")
AS
BEGIN
  FOR i IN tapeIds.FIRST .. tapeIds.LAST LOOP
    UPDATE Tape T
       SET T.TapegatewayRequestId = ids_seq.nextval,
           T.status = tconst.TAPE_PENDING
     WHERE T.status = tconst.TAPE_WAITPOLICY AND T.id = tapeIds(i);
  END LOOP; 
  COMMIT;
END;	
/

/* insert new Migration Mount */
CREATE OR REPLACE PROCEDURE insertMigrationMount(inTapePoolId IN NUMBER) AS
BEGIN 
  INSERT INTO MigrationMount
              (vdqmVolReqId, tapeGatewayRequestId, id, startTime, VID, label, density,
               lastFseq, lastVDQMPingTime, tapePool, status)
       VALUES (NULL, ids_seq.nextval, ids_seq.nextval, gettime(), NULL, NULL, NULL,
               NULL, 0, inTapePoolId, tconst.MIGRATIONMOUNT_WAITTAPE);
END;
/

/* resurrect tapes */
CREATE OR REPLACE PROCEDURE startMigrationMounts AS
  varNbMounts INTEGER;
  varDataAmount INTEGER;
  varNbFiles INTEGER;
  varOldestCreationTime NUMBER;
BEGIN
  -- loop through tapepools
  FOR t IN (SELECT id, nbDrives, minAmountDataForMount,
                   minNbFilesForMount, maxFileAgeBeforeMount
              FROM TapePool) LOOP
    -- get number of mount already running for this tapepool
    SELECT count(*) INTO varNbMounts
      FROM MigrationMount
     WHERE tapePool = t.id;
    -- get the amount of data and number of files to migrate, plus the age of the oldest file
    BEGIN
      SELECT SUM(fileSize), COUNT(*), MIN(creationTime) INTO varDataAmount, varNbFiles, varOldestCreationTime
        FROM MigrationJob
       WHERE tapePool = t.id
       GROUP BY tapePool;
      -- Create as many mounts as needed according to amount of data and number of files
      WHILE (varNbMounts < t.nbDrives) AND
            ((varDataAmount/(varNbMounts+1) >= t.minAmountDataForMount) OR
             (varNbFiles/(varNbMounts+1) >= t.minNbFilesForMount)) LOOP
        insertMigrationMount(t.id);
        varNbMounts := varNbMounts + 1;
      END LOOP;
      -- force creation of a unique mount in case no mount was created at all and some files are too old
      IF varNbMounts = 0 AND t.nbDrives > 0 AND gettime() - varOldestCreationTime > t.maxFileAgeBeforeMount THEN
        insertMigrationMount(t.id);
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- there is no file to migrate, so nothing to do
      NULL;
    END;
    COMMIT;
  END LOOP;
END;
/

/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('MIGRATIONMOUNTSJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every 20 minutes executing the deleteTerminatedRequests procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'MigrationMountsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startMigrationMounts(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Creates MigrationMount entries when new migrations should start');
END;
/

/*******************************************************************
 *
 * @(#)RCSfile: oracleTapeGateway.sql,v  Revision: 1.12  Date: 2009/08/13 15:14:25  Author: gtaur 
 *
 * PL/SQL code for the tape gateway daemon
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* PROCEDURE */

/* tg_findFromTGRequestId */
CREATE OR REPLACE
PROCEDURE tg_findFromTGRequestId (
  inTapeGatewayRequestId IN  INTEGER,
  outTapeId              OUT INTEGER,
  outMountId            OUT INTEGER) AS
BEGIN
  -- Will return a valid ID in either outTapeId or outMountId,
  -- and NULL in the other when finding the object corresponding to
  -- this TGR request ID.
  --
  -- Will throw an exception in case of non-unicity.

  -- Look for read tapes:
  BEGIN
    SELECT id INTO outTapeId
      FROM Tape
     WHERE TapeGatewayRequestId = inTapeGatewayRequestId
       AND tpMode = tconst.TPMODE_READ;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outTapeId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple read tapes for same TapeGatewayRequestId: '|| 
         inTapeGatewayRequestId || ' in tg_findFromTGRequestId');
     -- Let other exceptions fly through.
   END;
   
   -- Look for migration mounts
   BEGIN
     SELECT id INTO outMountId
       FROM MigrationMount
      WHERE TapeGatewayRequestId = inTapeGatewayRequestId;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outMountId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple migration mounts for same TapeGatewayRequestId: '|| 
         inTapeGatewayRequestId || ' in tg_findFromTGRequestId');
     -- Let other exceptions fly through.     
   END;
   
   -- Check for migration mount/tape collision
   IF (outMountId IS NOT NULL AND outTapeId IS NOT NULL) THEN
     RAISE_APPLICATION_ERROR (-20119, 
       'Found both read tape (id='||outTapeId||') and migration mount (id='||
       outMountId||') for TapeGatewayRequestId: '||
       inTapeGatewayRequestId || ' in tg_findFromTGRequestId');
   END IF;
END;
/

/* tg_findFromVDQMReqId */
CREATE OR REPLACE
PROCEDURE tg_findFromVDQMReqId (
  inVDQMReqId IN  INTEGER,
  outTapeId              OUT INTEGER,
  outMountId            OUT INTEGER) AS
BEGIN
  -- Will return a valid ID in either outTapeId or outMountId,
  -- and NULL in the other when finding the object corresponding to
  -- this TGR request ID.
  --
  -- Will throw an exception in case of non-unicity.

  -- Look for read tapes:
  BEGIN
    SELECT id INTO outTapeId
      FROM Tape
     WHERE VDQMVolReqId = inVDQMReqId
       AND tpMode = tconst.TPMODE_READ;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outTapeId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple read tapes for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.
   END;
   
   -- Look for migration mounts
   BEGIN
     SELECT id INTO outMountId
       FROM MigrationMount
      WHERE VDQMVolReqId = inVDQMReqId;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outMountId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple migration mounts for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.     
   END;
   
   -- Check for migration mount/tape collision
   IF (outMountId IS NOT NULL AND outTapeId IS NOT NULL) THEN
     RAISE_APPLICATION_ERROR (-20119, 
       'Found both read tape (id='||outTapeId||') and migration mount (id='||
       outMountId||') for VDQMVolReqId: '||
       inVDQMReqId || ' in tg_findFromVDQMReqId');
   END IF;
END;
/

/* tg_RequestIdFromVDQMReqId */
CREATE OR REPLACE
PROCEDURE tg_RequestIdFromVDQMReqId (
  inVDQMReqId IN  INTEGER,
  outTgrId    OUT INTEGER) AS
  varTapeId       INTEGER;
  varMountId     INTEGER;
BEGIN
  -- Will return a valid tape gateway request Id if one and only one read
  -- tape or migration mount is found with this VDQM request ID.
  --
  -- Will throw an exception in case of non-unicity.
  --
  -- Will return NULL in case of not found.
  outTgrId := NULL;
  -- Look for read tapes:
  BEGIN
    SELECT id, TapeGatewayRequestId INTO varTapeId, outTgrId
      FROM Tape
     WHERE VDQMVolReqId = inVDQMReqId
       AND tpMode = tconst.TPMODE_READ;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL; -- It's OK, could be a migration mount.
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple read tapes for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.
   END;
   
   -- Look for migration mounts
   BEGIN
     SELECT id, TapeGatewayRequestId INTO varMountId, outTgrId
       FROM MigrationMount
      WHERE VDQMVolReqId = inVDQMReqId;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL; -- It's OK, might have been a tape.
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple migration mounts for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.     
   END;
   
   -- Check for migration mount/tape collision
   IF (varMountId IS NOT NULL AND varTapeId IS NOT NULL) THEN
     outTgrId := NULL;
     RAISE_APPLICATION_ERROR (-20119, 
       'Found both read tape (id='||varTapeId||') and migration mount (id='||
       varMountId||') for VDQMVolReqId: '||
       inVDQMReqId || ' in tg_findFromVDQMReqId');
   END IF;
END;
/

/* tg_findVDQMReqFromTGReqId */
CREATE OR REPLACE
PROCEDURE tg_findVDQMReqFromTGReqId (
  inTGReqId     IN  INTEGER,
  outVDQMReqId  OUT INTEGER) AS
  varTapeId         NUMBER;
  varMountId       NUMBER;
BEGIN
  -- Helper function. Wrapper to another helper.
  tg_findFromTGRequestId (inTGReqId, varTapeId, varMountId);
  IF (varTapeId IS NOT NULL) THEN
    SELECT T.vdqmVolReqId INTO outVDQMReqId
      FROM Tape T WHERE T.id = varTapeId;
  ELSIF (varMountId IS NOT NULL) THEN
    SELECT vdqmVolReqId INTO outVDQMReqId
      FROM MigrationMount WHERE id = varMountId;  
  ELSE
    RAISE_APPLICATION_ERROR (-20119, 
         'Could not find migration mount or tape read for TG request Id='|| 
         inTGReqId || ' in tg_findVDQMReqFromTGReqId');
  END IF;
END;
/

/* attach drive request to tape */
CREATE OR REPLACE
PROCEDURE tg_attachDriveReqToTape(
  inTapeRequestId IN NUMBER,
  inVdqmId    IN NUMBER,
  inDgn       IN VARCHAR2,
  inLabel     IN VARCHAR2,
  inDensity   IN VARCHAR2) AS
  varTapeId   INTEGER;
  varMountId INTEGER;
/* Update the status and propoerties of the Tape structure related to
 * a tape request, and the migration mount state in case of a write.
 * All other properties are attached to the tape itself.
 */
BEGIN
  -- Update tape or migration mount, whichever is relevant. First find:
  tg_findFromTGRequestId (inTapeRequestId, varTapeId, varMountId);
  
  -- Process one or the other (we trust the called function to not provide both)
  IF (varTapeId IS NOT NULL) THEN
    -- In the case of a read, only the tape itself is involved
    -- update reading tape which have been submitted to vdqm => WAIT_DRIVE.
    UPDATE Tape T
       SET T.lastvdqmpingtime = gettime(),
           T.starttime        = gettime(),
           T.vdqmvolreqid     = inVdqmId,
           T.Status           = tconst.TAPE_WAITDRIVE,
           T.dgn              = inDgn,
           T.label            = inLabel,
           T.density          = inDensity
     WHERE T.Id = varTapeId;
    COMMIT;
    RETURN;
  ELSIF (varMountId IS NOT NULL) THEN
    -- We have to update the tape as well (potentially, we keep the can-fail
    -- query based update of the previous system.
    SAVEPOINT Tape_Mismatch;
    DECLARE
      varTapeFromMount NUMBER;
      varTp             Tape%ROWTYPE;
    BEGIN
      UPDATE MigrationMount
         SET status = tconst.MIGRATIONMOUNT_WAITDRIVE,
             label = inLabel,
             density = inDensity,
             vdqmvolreqid = inVdqmId
       WHERE id = varMountId;
      COMMIT;
      RETURN;
    END; -- END of local block for varTapeFromMount and varTp
  ELSE RAISE_APPLICATION_ERROR (-20119,
       'Found no migration mount or read tape for TapeRequestId: '||inTapeRequestId);
  END IF;
END;
/
        
/* attach the tapes to the migration mounts  */
CREATE OR REPLACE
PROCEDURE tg_attachTapesToStreams (
  inStartFseqs IN castor."cnumList",
  inMountIds   IN castor."cnumList",
  inTapeVids   IN castor."strList") AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  -- Sanity check
  IF (inStartFseqs.COUNT != inTapeVids.COUNT) THEN
    RAISE_APPLICATION_ERROR (-20119,
       'Size mismatch for arrays: inStartFseqs.COUNT='||inStartFseqs.COUNT
       ||' inTapeVids.COUNT='||inTapeVids.COUNT);
  END IF;
  FORALL i IN inMountIds.FIRST .. inMountIds.LAST
    UPDATE MigrationMount
       SET VID = inTapeVids(i),
           lastFseq = inStartFseqs(i),
           startTime = getTime(),
           status = tconst.MIGRATIONMOUNT_SEND_TO_VDQM
     WHERE id = inMountIds(i);
END;
/

/* update the db when a tape session is ended */
CREATE OR REPLACE
PROCEDURE tg_endTapeSession(inTransId IN NUMBER, inErrorCode IN INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varUnused NUMBER;
  varTpId NUMBER;        -- TapeGateway Taperecall
  varTgrId NUMBER;       -- TapeGatewayRequest ID
  varMountId NUMBER;     -- Migration mount ID
  varSegNum INTEGER;     -- Segment count
  varTcIds "numList";    -- recall/migration job Ids

BEGIN
  -- Prepare to revert changes
  SAVEPOINT MainEndTapeSession;
  -- Find the Tape read or migration mount for this VDQM request
  tg_findFromVDQMReqId (inTransId, varTpId, varMountId);
  -- Pre-process the read and write: find corresponding TapeGatewayRequest Id.
  -- Lock corresponding Tape or MigrationMount. This will bomb if we
  -- don't find exactly ones (which is good).
  varTgrId := NULL;
  IF (varTpId IS NOT NULL) THEN
    -- Find and lock tape
    SELECT TapeGatewayRequestId INTO varTgrId
      FROM Tape
     WHERE id = varTpId
       FOR UPDATE;
  ELSIF (varMountId IS NOT NULL) THEN
    -- Find and lock migration mount
    SELECT TapeGatewayRequestId INTO varTgrId
      FROM MigrationMount
     WHERE id = varMountId
       FOR UPDATE;
  ELSE
    -- Nothing found for the VDQMRequestId: whine and leave.
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'No tape or migration mount found for VDQM ID='|| inTransId);
  END IF;
  -- If we failed to get the TG req Id, no point in going further.
  IF (varTgrId IS NULL) THEN
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'Got NULL TapeGatewayRequestId for tape ID='|| varTpId||
     ' or MigrationMount Id='|| varMountId||' processing VDQM Id='||inTransId||
     ' in tg_endTapeSession.');
  END IF;

  -- Process the read case
  IF (varTpId IS NOT NULL) THEN
    -- find and lock the RecallJobs
    SELECT id BULK COLLECT INTO varTcIds
      FROM RecallJob
     WHERE TapeGatewayRequestId = varTgrId
       FOR UPDATE;
    IF (inErrorCode != 0) THEN
        -- if a failure is reported
        -- fail all the segments
        UPDATE Segment SEG
           SET SEG.status=tconst.SEGMENT_FAILED
         WHERE SEG.copy IN (SELECT * FROM TABLE(varTcIds));
        -- mark RecallJob as RECALLJOB_RETRY
        UPDATE RecallJob
           SET status    = tconst.RECALLJOB_RETRY,
               errorcode = inErrorCode
         WHERE id IN (SELECT * FROM TABLE(varTcIds));
    END IF;
    -- resurrect lost segments
    UPDATE Segment SEG
       SET SEG.status = tconst.SEGMENT_UNPROCESSED
     WHERE SEG.status = tconst.SEGMENT_SELECTED
       AND SEG.tape = varTpId;
    -- check if there is work for this tape
    SELECT count(*) INTO varSegNum
      FROM segment SEG
     WHERE SEG.Tape = varTpId
       AND status = tconst.SEGMENT_UNPROCESSED;
    -- Restart the unprocessed segments' tape if there are any.
    IF varSegNum > 0 THEN
      UPDATE Tape T
         SET T.status = tconst.TAPE_WAITPOLICY -- for rechandler
       WHERE T.id=varTpId;
    ELSE
      UPDATE Tape
         SET status = tconst.TAPE_UNUSED
       WHERE id=varTpId;
     END IF;
  ELSIF (varMountId IS NOT NULL) THEN
    -- find and lock the MigrationJobs.
    SELECT id BULK COLLECT INTO varTcIds
      FROM MigrationJob
     WHERE TapeGatewayRequestId = varTgrId
       FOR UPDATE;
    -- Process the write case
    IF inErrorCode != 0 THEN
      -- if a failure is reported, retry
      UPDATE MigrationJob
         SET status=tconst.MIGRATIONJOB_RETRY,
             VID=NULL,
             TapeGatewayRequestId = NULL,
             errorcode=inErrorCode,
             nbretry=0
       WHERE id IN (SELECT * FROM TABLE(varTcIds));
    ELSE
      -- just resurrect them if they were lost
      UPDATE MigrationJob
         SET status = tconst.MIGRATIONJOB_PENDING,
             VID = NULL,
             TapeGatewayRequestId = NULL
       WHERE id IN (SELECT * FROM TABLE(varTcIds))
         AND status = tconst.MIGRATIONJOB_SELECTED;
    END IF;
    -- delete the migration mount
    DELETE FROM MigrationMount  WHERE tapegatewayrequestid = varMountId;
  ELSE

    -- Small infusion of paranoia ;-) We should never reach that point...
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'No tape or migration mount found on second pass for VDQM ID='|| inTransId ||
     ' in tg_endTapeSession');
  END IF;
  COMMIT;
END;
/

/* mark a migration or recall as failed saving in the db the error code associated with the failure */
CREATE OR REPLACE
PROCEDURE tg_failFileTransfer(
  inTransId      IN NUMBER,    -- The VDQM transaction ID
  inFileId    IN NUMBER,       -- File ID
  inNsHost    IN VARCHAR2,     -- NS Host
  inFseq      IN INTEGER,      -- fSeq of the tape copy
  inErrorCode IN INTEGER)  AS  -- Error Code
  varUnused NUMBER;            -- dummy
  varTgrId NUMBER;             -- Tape Gateway Request Id
  varMountId NUMBER;           -- MigrationMount Id
  varTpId NUMBER;              -- Tape Id
  varTcId NUMBER;              -- recall/migration job Id
BEGIN
  -- Prepare to return everything to its original state in case of problem.
  SAVEPOINT MainFailFileSession;
  
  -- Find related Read tape or migration mount from VDQM Id
  tg_findFromVDQMReqId(inTransId, varTpId, varMountId);
  
  -- Lock related castorfile -- TODO: This should be a procedure-based access to
  -- the disk system.
  SELECT CF.id INTO varUnused 
    FROM CastorFile CF
   WHERE CF.fileid = inFileId
     AND CF.nsHost = inNsHost 
    FOR UPDATE;
  
  -- Case dependent part
  IF (varTpId IS NOT NULL) THEN
    -- We handle a read case
    -- fail the segment on that tape
    UPDATE Segment SEG
       SET SEG.status    = tconst.SEGMENT_FAILED,
           SEG.severity  = inErrorCode,
           SEG.errorCode = -1 
     WHERE SEG.fseq = inFseq 
       AND SEG.tape = varTpId 
    RETURNING SEG.copy INTO varTcId;
    -- mark RecallJob for retry
    UPDATE RecallJob
       SET status    = tconst.RECALLJOB_RETRY,
           errorcode = inErrorCode 
     WHERE id = varTcId;
  ELSIF (varMountId IS NOT NULL) THEN
    -- Write case
    SELECT TapeGatewayRequestId INTO varTgrId
      FROM MigrationMount
     WHERE MigrationMount.id = varMountId;
    -- mark MigrationJob for retry. It should be the migration job with the proper 
    -- TapegatewayRequest + having a matching Fseq.
    UPDATE MigrationJob
       SET status    = tconst.MIGRATIONJOB_RETRY,
           errorcode = inErrorCode,
           vid       = NULL
     WHERE TapegatewayRequestId = varTgrId
       AND fSeq = inFseq; 
  ELSE
    -- Complain in case of failure
    ROLLBACK TO SAVEPOINT MainFailFileSession;
    RAISE_APPLICATION_ERROR (-20119, 
     'No tape or migration mount found on second pass for VDQM ID='|| inTransId||
     ' in tg_failFileTransfer');
  END IF;
EXCEPTION WHEN  NO_DATA_FOUND THEN
  NULL;
END;
/

/* retrieve from the db all the migration jobs that faced a failure */
CREATE OR REPLACE
PROCEDURE tg_getFailedMigrations(outFailedMigrationJob_c OUT castor.FailedMigrationJob_Cur) AS
BEGIN
  OPEN outFailedMigrationJob_c FOR
    SELECT id, errorCode, nbRetry
      FROM MigrationJob
     WHERE MigrationJob.status = tconst.MIGRATIONJOB_RETRY
       AND ROWNUM < 1000 
       FOR UPDATE SKIP LOCKED; 
END;
/


/* retrieve from the db all the recall jobs that faced a failure */
CREATE OR REPLACE
PROCEDURE tg_getFailedRecalls(outFailedRecallJob_c OUT castor.FailedRecallJob_Cur) AS
BEGIN
  OPEN outFailedRecallJob_c FOR
    SELECT id, errorCode, nbRetry
      FROM RecallJob
     WHERE RecallJob.status = tconst.RECALLJOB_RETRY
       AND ROWNUM < 1000 
       FOR UPDATE SKIP LOCKED;
END;
/

/* default migration candidate selection policy */
CREATE OR REPLACE
PROCEDURE tg_defaultMigrSelPolicy(inMountId IN INTEGER,
                                  outDiskServerName OUT NOCOPY VARCHAR2,
                                  outMountPoint OUT NOCOPY VARCHAR2,
                                  outPath OUT NOCOPY VARCHAR2,
                                  outDiskCopyId OUT INTEGER,
                                  outLastKnownFileName OUT NOCOPY VARCHAR2, 
                                  outFileId OUT INTEGER,
                                  outNsHost OUT NOCOPY VARCHAR2, 
                                  outFileSize OUT INTEGER,
                                  outMigJobId OUT INTEGER, 
                                  outLastUpdateTime OUT INTEGER) AS
  /* Find the next file to migrate for a given migration mount.
   *
   * Procedure's input: migration mount id
   * Procedure's output: non-zero MigrationJob ID
   *
   * Lock taken on the migration job if it selects one.
   * Lock taken on the diskserver and FileSystem.
   * 
   * Per policy we should only propose a migration job for a file that does not 
   * already have another migration job attached for or migrated to the same tape.
   * The already migrated migration jobs are kept until the whole set of siblings 
   * have been migrated. Nothing else is guaranteed to be.
   * 
   * From this we can find a list of our potential siblings (by castorfile) from
   * this TapeGatewayRequest, and prevent the selection of migration jobs whose 
   * siblings already live on the same tape.
   */
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
  CURSOR c IS
    SELECT /*+ FIRST_ROWS(1) LEADING(MigrationMount MigrationJob DiskCopy FileSystem DiskServer CastorFile) */
           DiskServer.name, FileSystem.mountPoint, DiskCopy.path, DiskCopy.id, CastorFile.lastKnownFilename,
           CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, MigrationJob.id, CastorFile.lastUpdateTime
      FROM MigrationMount, MigrationJob, DiskCopy, FileSystem, DiskServer, CastorFile
     WHERE MigrationMount.id = inMountId
       AND MigrationJob.tapePool = MigrationMount.tapepool
       AND MigrationJob.status = tconst.MIGRATIONJOB_PENDING
       AND DiskCopy.castorFile = MigrationJob.castorFile
       AND FileSystem.id = DiskCopy.fileSystem
       AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
       AND DiskServer.id = FileSystem.diskServer
       AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
       AND CastorFile.id = MigrationJob.castorFile
       AND MigrationMount.VID NOT IN (SELECT DISTINCT M2.VID FROM MigrationJob M2
                                       WHERE M2.castorFile = MigrationJob.castorfile
                                         AND M2.status IN (tconst.MIGRATIONJOB_SELECTED,
                                                           tconst.MIGRATIONJOB_MIGRATED))
       FOR UPDATE OF MigrationJob.id SKIP LOCKED;
BEGIN
  OPEN c;
  FETCH c INTO outDiskServerName, outMountPoint, outPath, outDiskCopyId, outLastKnownFileName,
               outFileId, outNsHost, outFileSize, outMigJobId, outLastUpdateTime;
  CLOSE c;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Nothing to migrate. Simply return
  CLOSE c;
END;
/

/* get a candidate for migration */
CREATE OR REPLACE
PROCEDURE tg_getFileToMigrate(
  inVDQMtransacId IN  NUMBER,
  outRet           OUT INTEGER,
  outVid        OUT NOCOPY VARCHAR2,
  outputFile    OUT castorTape.FileToMigrateCore_cur) AS
  -- This procedure finds the next file to migrate
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  varMountId NUMBER;
  varDiskServer VARCHAR2(2048);
  varMountPoint VARCHAR2(2048);
  varPath  VARCHAR2(2048);
  varDiskCopyId NUMBER;
  varFileId NUMBER;
  varNsHost VARCHAR2(2048);
  varFileSize  INTEGER;
  varMigJobId  INTEGER;
  varLastUpdateTime NUMBER;
  varLastKnownName VARCHAR2(2048);
  varTgRequestId NUMBER;
  varUnused INTEGER;
BEGIN
  outRet:=0;
  BEGIN
    -- Extract id and tape gateway request and VID from the migration mount
    SELECT id, TapeGatewayRequestId, vid INTO varMountId, varTgRequestId, outVid
      FROM MigrationMount
     WHERE VDQMVolReqId = inVDQMtransacId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    outRet:=-2;   -- migration mount is over
    RETURN;
  END;
  -- default policy is used in all cases in version 2.1.12
  tg_defaultMigrSelPolicy(varMountId,varDiskServer,varMountPoint,varPath,
    varDiskCopyId ,varLastKnownName, varFileId, varNsHost,varFileSize,
    varMigJobId,varLastUpdateTime);
  IF varMigJobId IS NULL THEN
    outRet := -1; -- the migration selection policy didn't find any candidate
    RETURN;
  END IF;

  DECLARE
    varNewFseq NUMBER;
  BEGIN
   -- Atomically increment and read the next FSEQ to be written to. fSeq is held
   -- in the tape structure.
   UPDATE MigrationMount
      SET lastFseq = lastFseq + 1
     WHERE id = varMountId
   RETURNING lastFseq-1 into varNewFseq; -- The previous value is where we'll write

   -- Update the migration job and attach it to a newly created file transaction ID
   UPDATE MigrationJob
      SET status = tconst.MIGRATIONJOB_SELECTED,
          VID = outVID,
          fSeq = varNewFseq,
          tapeGatewayRequestId = varTgRequestId,
          fileTransactionId = TG_FileTrId_Seq.NEXTVAL
    WHERE id = varMigJobId;

   OPEN outputFile FOR
     SELECT varFileId,varNshost,varLastUpdateTime,varDiskServer,varMountPoint,
            varPath,varLastKnownName,fseq,varFileSize,fileTransactionId
       FROM MigrationJob
      WHERE id = varMigJobId;

  END;
  COMMIT;
END;
/

/* get a candidate for recall */
CREATE OR REPLACE
PROCEDURE tg_getFileToRecall (inTransId IN  NUMBER, outRet OUT INTEGER,
  outVid OUT NOCOPY VARCHAR2, outFile OUT castorTape.FileToRecallCore_Cur) AS
  varTgrId         INTEGER; -- TapeGateway Request Id
  varDSName VARCHAR2(2048); -- Disk Server name
  varMPoint VARCHAR2(2048); -- Mount Point
  varPath   VARCHAR2(2048); -- Diskcopy path
  varSegId          NUMBER; -- Segment Id
  varDcId           NUMBER; -- Disk Copy Id
  varRjId           NUMBER; -- Recalljob Id
  varTapeId         NUMBER; -- Tape Id
  varNewFSeq       INTEGER; -- new FSeq
  varUnused         NUMBER;
BEGIN 
  outRet:=0;
  BEGIN
    -- master lock on the tape read
    -- Find tape
    tg_FindFromVDQMReqId (inTransId, varTapeId, varUnused);
    IF (varTapeId IS NULL) THEN 
      outRet := -2;
      RETURN;
    END IF;
    -- Take lock on tape
    SELECT T.TapeGatewayRequestId, T.vid INTO varTgrId, outVid
      FROM Tape T
     WHERE T.id = varTapeId
       FOR UPDATE;
  EXCEPTION WHEN  NO_DATA_FOUND THEN
     outRet:=-2; -- ERROR
     RETURN;
  END; 
  BEGIN
    -- Find the unprocessed segment of this tape with lowest fSeq
    SELECT       id,       fSeq,    Copy 
      INTO varSegId, varNewFSeq, varRjId 
      FROM (SELECT SEG.id id, SEG.fSeq fSeq, SEG.Copy Copy 
              FROM Segment SEG
             WHERE SEG.tape = varTapeId  
               AND SEG.status = tconst.SEGMENT_UNPROCESSED
             ORDER BY SEG.fseq ASC)
     WHERE ROWNUM < 2;
    -- Lock the corresponding castorfile
    SELECT CF.id INTO varUnused 
      FROM Castorfile CF, RecallJob RJ
     WHERE CF.id = RJ.castorfile 
       AND RJ.id = varRjId 
       FOR UPDATE OF CF.id;
  EXCEPTION WHEN NO_DATA_FOUND THEN
     outRet := -1; -- NO MORE FILES
     COMMIT;
     RETURN;
  END;
  DECLARE
    application_error EXCEPTION;
    PRAGMA EXCEPTION_INIT(application_error,-20115);
  BEGIN
    bestFileSystemForSegment(varSegId,varDSName,varMPoint,varPath,varDcId);
  EXCEPTION WHEN application_error  OR NO_DATA_FOUND THEN 
    outRet := -3;
    COMMIT;
    RETURN;
  END;
  -- Update the RecallJob's parameters
  UPDATE RecallJob
     SET fseq = varNewFSeq,
         TapeGatewayRequestId = varTgrId,
         FileTransactionID = TG_FileTrId_Seq.NEXTVAL
   WHERE id = varRjId;
   -- Update the segment's status
  UPDATE Segment SEG 
     SET SEG.status = tconst.SEGMENT_SELECTED
   WHERE SEG.id=varSegId 
     AND SEG.status = tconst.SEGMENT_UNPROCESSED;
  OPEN outFile FOR 
    SELECT CF.fileid, CF.nshost, varDSName, varMPoint, varPath, varNewFSeq , 
           RJ.FileTransactionID
      FROM RecallJob RJ, Castorfile CF
     WHERE RJ.id = varRjId
       AND CF.id = RJ.castorfile;
END;
/

/* get the information from the db for a successful migration */
CREATE OR REPLACE
PROCEDURE tg_getRepackVidAndFileInfo(
  inFileId          IN  NUMBER, 
  inNsHost          IN VARCHAR2,
  inFseq            IN  INTEGER, 
  inTransId         IN  NUMBER, 
  inBytesTransfered IN  NUMBER,
  outRepackVid     OUT NOCOPY VARCHAR2, 
  outVID           OUT NOCOPY VARCHAR2,
  outCopyNb        OUT INTEGER, 
  outLastTime      OUT NUMBER,
  outFileClass     OUT NOCOPY VARCHAR2,
  outRet           OUT INTEGER) AS 
  varCfId              NUMBER;  -- CastorFile Id
  varFileSize          NUMBER;  -- Expected file size
  varTgrId             NUMBER;  -- Tape gateway request Id
BEGIN
  outRepackVid:=NULL;
   -- ignore the repack state
  BEGIN
    SELECT CF.lastupdatetime, CF.id, CF.fileSize,      FC.name 
      INTO outLastTime,     varCfId, varFileSize, outFileClass
      FROM CastorFile CF 
      LEFT OUTER JOIN FileClass FC ON FC.Id = CF.FileClass
     WHERE CF.fileid = inFileId 
       AND CF.nshost = inNsHost;
     IF (outFileClass IS NULL) THEN
       outFileClass := 'UNKNOWN';
     END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR (-20119,
         'Castorfile not found for File ID='||inFileId||' and nshost = '||
           inNsHost||' in tg_getRepackVidAndFileInfo.');
  END;
  IF varFileSize <> inBytesTransfered THEN
  -- fail the file
    tg_failFileTransfer(inTransId,inFileId, inNsHost, inFseq,  1613); -- wrongfilesize
    COMMIT;
    outRet := -1;
    RETURN;
  ELSE
    outRet:=0;
  END IF;
  
  tg_RequestIdFromVDQMReqId(inTransId, varTgrId);
  IF (varTgrId IS NOT NULL) THEN
    BEGIN
      SELECT copyNb INTO outcopynb 
        FROM MigrationJob
       WHERE TapeGatewayRequestId = varTgrId
         AND castorfile = varCfId
         AND fseq= inFseq;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR (-20119,
         'MigrationJob not found for castorfile='||varCFId||'(File ID='||inFileId||' and nshost = '||
           inNsHost||') and fSeq='||inFseq||' in tg_getRepackVidAndFileInfo.');
    END;
    BEGIN
      SELECT MigrationMount.vid
        INTO outVID
        FROM MigrationMount
       WHERE MigrationMount.tapeGatewayRequestId = varTgrId;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR (-20119,
           'Tape not found for tapeGatewayRequestId='||varTgrId||'(File ID='||inFileId||' and nshost = '||
             inNsHost||' and castorfile='||varCFId||'and fSeq='||inFseq||') in tg_getRepackVidAndFileInfo.');
    END;
  END IF;
  
  BEGIN 
   --REPACK case
    -- Get the repackvid field from the existing request (if none, then we are not in a repack process
     SELECT /*+ INDEX(sR I_Subrequest_Castorfile) INDEX(SRR PK_StageRepackRequest_Id) */
            SRR.repackvid INTO outRepackVid
       FROM SubRequest sR, StageRepackRequest SRR
      WHERE SRR.id = SR.request
        AND sR.status = dconst.SUBREQUEST_REPACK
        AND sR.castorFile = varCfId
        AND ROWNUM < 2;
  EXCEPTION WHEN NO_DATA_FOUND THEN
   -- standard migration
    NULL;
  END;
END;
/

/* get the information from the db for a successful recall */
CREATE OR REPLACE
PROCEDURE tg_getSegmentInfo(
  inTransId     IN NUMBER,
  inFseq        IN INTEGER,
  outVid       OUT NOCOPY VARCHAR2,
  outCopyNb    OUT INTEGER ) AS
  varTrId          NUMBER;
  varTapeId        NUMBER;
  varUnused        NUMBER;
BEGIN
  -- We are looking for a recalled tape with this TGR request ID.
  tg_FindFromVDQMReqId (inTransId, varTapeId, varUnused);

  -- Throw an exception in case of not found.
  IF (varTapeId IS NULL) THEN
    outVid := NULL;
    outCopyNb := NULL;
    RAISE_APPLICATION_ERROR (-20119,
         'No tape read found for VDQMReqId'||
         inTransId || ' in tg_getSegmentInfo');
  END IF;

  -- Get the data
  BEGIN
  SELECT T.vid, T.TapeGatewayRequestId  INTO outVid, varTrId
    FROM Tape T
   WHERE T.id=varTapeId;

  SELECT copynb INTO outCopyNb
    FROM RecallJob
   WHERE fseq = inFseq
     AND TapeGateWayRequestId = varTrId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    outVid := NULL;
    outCopyNb := NULL;
    RAISE_APPLICATION_ERROR (-20119,
         'Tailed to find tape or tape copy for VDQM Id='||
         inTransId || ',TapeId='||varTapeId||' TapeGatewayReqId='||varTrId||
         ' in tg_getSegmentInfo');
  END;
END;
/

/* get the migration mounts without any tape associated */
CREATE OR REPLACE
PROCEDURE tg_getMigMountsWithoutTapes(outStrList OUT castorTape.MigrationMount_Cur) AS
BEGIN
  -- get migration mounts  in state WAITTAPE with a non-NULL TapeGatewayRequestId
  OPEN outStrList FOR
    SELECT M.id, TP.name
      FROM MigrationMount M,Tapepool TP
     WHERE M.status = tconst.MIGRATIONMOUNT_WAITTAPE
       AND M.TapeGatewayRequestId IS NOT NULL
       AND M.tapepool=TP.id 
       FOR UPDATE OF M.id SKIP LOCKED;   
END;
/

/* get tape with a pending request in VDQM */
CREATE OR REPLACE
PROCEDURE tg_getTapesWithDriveReqs(
  inTimeLimit     IN  NUMBER,
  outTapeRequest OUT castorTape.tapegatewayrequest_Cur) AS
  varTgrId        "numList";
  varTapeReadIds  "numList";
  varMigMountIds    "numList";
  varNow          NUMBER;
BEGIN 
  -- get requests in WAITING_TAPESERVER and ONGOING
  -- With the tape gateway request table dropping, this boils down to
  -- streams in state STREAM_WAITDRIVE, STREAM_WAITMOUNT, STREAM_RUNNING
  -- or Tape reads in state TAPE_WAITTAPEDRIVE or MOUNTED.
  
  -- In addition, we only look for the tape reads/streams which have a VDQM ping
  -- time older than inTimeLimit.  
  
  -- TODO: The function name should reflect the fact that it's actually dealing
  -- with a timeout mechanism.
  
  -- TODO: I do not have the TAPE_WAITMOUNT state in the lifecycle diagram, but
  -- include it nevertheless. This is a safe option as the select will be limite
  -- to tapes in tape read mode. If the diagram is right, this will have no
  -- effect and if the diagram is wrong, this will lead to cover the
  -- non-decribed case.

  -- No need to query the clock all the time
  varNow := gettime();
  
  -- Find all the tapes and lock
  SELECT T.id BULK COLLECT INTO varTapeReadIds
    FROM Tape T
   WHERE T.status IN ( tconst.TAPE_WAITDRIVE, tconst.TAPE_WAITMOUNT, tconst.TAPE_MOUNTED )
     AND T.tpMode = tconst.TPMODE_READ
     AND T.TapeGatewayRequestId IS NOT NULL
     AND varNow - T.lastVdqmPingTime > inTimeLimit
     FOR UPDATE SKIP LOCKED;
     
  -- Find all the migration mounts and lock
  SELECT id BULK COLLECT INTO varMigMountIds
    FROM MigrationMount
   WHERE status IN ( tconst.MIGRATIONMOUNT_WAITDRIVE, tconst.MIGRATIONMOUNT_MIGRATING )
     AND varNow - lastVdqmPingTime > inTimeLimit
     FOR UPDATE SKIP LOCKED;
     
  -- Update the last VDQM ping time for all of them.
  UPDATE Tape T
     SET T.lastVdqmPingTime = varNow
   WHERE T.id IN ( SELECT /*+ CARDINALITY(trTable 5) */ * 
                     FROM TABLE (varTapeReadIds) );
  UPDATE MigrationMount
     SET lastVdqmPingTime = varNow
   WHERE id IN ( SELECT /*+ CARDINALITY(trTable 5) */ *
                   FROM TABLE (varMigMountIds));
                   
  -- Return them. For VDQM request IT, we have to split the select in 2 and
  -- union in the end, unlike in the previous statement.
  OPEN outTapeRequest FOR
    -- Read case
    SELECT T.TpMode, T.TapeGatewayRequestId, T.startTime,
           T.lastvdqmpingtime, T.vdqmVolReqid, 
           T.vid
      FROM Tape T
     WHERE T.Id IN (SELECT /*+ CARDINALITY(trTable 5) */ *
                    FROM TABLE(varTapeReadIds))
     UNION ALL
    -- Write case
    SELECT tconst.TPMODE_WRITE, tapeGatewayRequestId, startTime,
           lastvdqmpingtime, vdqmVolReqid, VID
      FROM MigrationMount
     WHERE id IN (SELECT /*+ CARDINALITY(trTable 5) */ *
                    FROM TABLE(varMigMountIds));
END;
/

/* get a tape without any drive requests sent to VDQM */
CREATE OR REPLACE
PROCEDURE tg_getTapeWithoutDriveReq(
  outReqId    OUT NUMBER,
  outTapeMode OUT NUMBER,
  outTapeSide OUT INTEGER,
  outTapeVid  OUT NOCOPY VARCHAR2) AS
  varMigMountId     NUMBER;
  varTapeId       NUMBER;
BEGIN
  -- Initially looked for tapegateway request in state SEND_TO_VDQM
  -- Find a tapegateway request id for which there is a tape read in
  -- state TAPE_PENDING or a migration mount in state SEND_TO_VDQM.
  -- This method is called until there are no more pending tapes
  -- We serve writes first and then reads
  BEGIN
    SELECT tapeGatewayRequestId, 1, 0, VID  -- note that the tape side is deprecated and thus hardcoded to 0
      INTO outReqId, outTapeMode, outTapeSide, outTapeVid
      FROM MigrationMount
     WHERE status = tconst.MIGRATIONMOUNT_SEND_TO_VDQM
       AND ROWNUM < 2
     ORDER BY dbms_random.value()
       FOR UPDATE SKIP LOCKED;
    RETURN;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Nothing to be found, we'll just carry on to the reads.
    NULL;
  END;
  BEGIN -- The read casse
    SELECT T.TapeGatewayRequestId,     0,      T.side,      T.vid,      T.id
      INTO outReqId,         outTapeMode, outTapeSide, outTapeVid, varTapeId
      FROM Tape T
     WHERE T.tpMode = tconst.TPMODE_READ
       AND T.status = tconst.TAPE_PENDING
       AND ROWNUM < 2
       FOR UPDATE SKIP LOCKED;
     -- Potential lazy/late definition of the request id
     -- We might be confronted to a not defined request id if the tape was created
     -- by the stager straight into pending state in the absence of a recall policy
     -- otherwise, the tape will go through resurrect tape (rec handler) and all
     -- will be fine.
     -- If we get here, we found a tape so the request id must be defined when leaving.
     IF (outReqId IS NULL OR outReqId = 0) THEN
       SELECT ids_seq.nextval INTO outReqId FROM DUAL;
       UPDATE Tape T SET T.TapeGatewayRequestId = outReqId WHERE T.id = varTapeId;
     END IF; 
  EXCEPTION WHEN NO_DATA_FOUND THEN
    outReqId := 0;
  END;
END;
/


/* get tape to release in VMGR */
CREATE OR REPLACE
PROCEDURE tg_getTapeToRelease(
  inVdqmReqId IN  INTEGER, 
  outVID      OUT NOCOPY VARCHAR2, 
  outMode     OUT INTEGER,
  outFull     OUT INTEGER ) AS
  varMountId      NUMBER;
  varTpId         NUMBER;
BEGIN
  -- Find Tape read or migration mount for this vdqm request
  tg_findFromVDQMReqId(inVdqmReqId, varTpId, varMountId);
  
   IF (varTpId IS NOT NULL) THEN -- read case
     outMode := 0;
     SELECT T.vid INTO outVID 
       FROM Tape T
       WHERE T.id = varTpId; 
   ELSIF (varMountId IS NOT NULL) THEN -- write case
     outMode := 1;
     SELECT vid, full
     INTO outVID, outFull
       FROM MigrationMount
      WHERE id = varMountId;
   END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- already cleaned by the checker
  NULL;
END;
/

/* invalidate a file that it is not possible to recall */
CREATE OR REPLACE
PROCEDURE tg_invalidateFile(
  inTransId   IN NUMBER,
  inFileId    IN NUMBER, 
  inNsHost    IN VARCHAR2,
  inFseq      IN INTEGER,
  inErrorCode IN INTEGER) AS
  varTapeId      NUMBER;
BEGIN
  UPDATE Tape
     SET lastfseq = lastfseq-1
   WHERE VDQMVolReqId = inTransId
     AND tpMode = tconst.TPMODE_READ;
   tg_failfiletransfer(inTransId, inFileId, inNsHost, inFseq, inErrorCode);
END;
/

/* restart taperequest which had problems */
CREATE OR REPLACE
PROCEDURE tg_restartLostReqs(
  trIds IN castor."cnumList") AS
  vdqmId INTEGER;
BEGIN
 FOR  i IN trIds.FIRST .. trIds.LAST LOOP   
   BEGIN
     tg_findVDQMReqFromTGReqId(trIds(i), vdqmId);
     tg_endTapeSession(vdqmId,0);
   EXCEPTION WHEN NO_DATA_FOUND THEN
     NULL;
   END;
 END LOOP;
 COMMIT;
END;
/


/* update the db after a successful migration */
CREATE OR REPLACE
PROCEDURE TG_SetFileMigrated(
  inTransId         IN  NUMBER, 
  inFileId          IN  NUMBER,
  inNsHost          IN  VARCHAR2, 
  inFseq            IN  INTEGER, 
  inFileTransaction IN  NUMBER) AS
  varMigJobCount      INTEGER;
  varCfId               NUMBER;
BEGIN
  -- Lock the CastorFile
  SELECT CF.id INTO varCfId FROM CastorFile CF
   WHERE CF.fileid = inFileId 
     AND CF.nsHost = inNsHost 
     FOR UPDATE;
  -- Update the corresponding migration job
  UPDATE MigrationJob
     SET status = tconst.MIGRATIONJOB_MIGRATED
   WHERE FileTransactionId = inFileTransaction
     AND fSeq = inFseq;
  -- let's check if another migration should be performed, if not, we're done for this file.
  SELECT count(*) INTO varMigJobCount
    FROM MigrationJob
    WHERE castorfile = varCfId  
     AND STATUS != tconst.MIGRATIONJOB_MIGRATED;
  IF varMigJobCount = 0 THEN
     -- Mark all disk copies as staged and delete all migration jobs together.
     UPDATE DiskCopy
        SET status= dconst.DISKCOPY_STAGED
      WHERE castorFile = varCfId
        AND status= dconst.DISKCOPY_CANBEMIGR;
     DELETE FROM MigrationJob
      WHERE castorfile = varCfId; 
  END IF;
  -- archive Repack requests should any be in the db
  FOR i IN (
    SELECT /*+ INDEX(SR I_Subrequest_Castorfile)*/ SR.id FROM SubRequest SR
    WHERE SR.castorfile = varCfId AND
          SR.status = dconst.SUBREQUEST_REPACK
    ) LOOP
    archiveSubReq(i.id, 8); -- SUBREQUEST_FINISHED
  END LOOP;
  COMMIT;
END;
/


/* update the db after a successful recall */
CREATE OR REPLACE
PROCEDURE tg_setFileRecalled(
  inTransId          IN  NUMBER,
  inFileId           IN  NUMBER,
  inNsHost           IN  VARCHAR2,
  inFseq             IN  NUMBER,
  inFileTransaction  IN  NUMBER) AS
  varDcId               NUMBER;         -- DiskCopy Id
  varCfId               NUMBER;         -- CastorFile Id
  varSubrequestId       NUMBER;
  varRequestId          NUMBER;
  varRequestType        NUMBER;
  varFileSize           NUMBER;
  varGcWeight           NUMBER;         -- Garbage collection weight
  varGcWeightProc       VARCHAR(2048);  -- Garbage collection weighting procedure name
  varEuid               INTEGER;        -- Effective user Id
  varEgid               INTEGER;        -- Effective Group Id
  varSvcClassId         NUMBER;         -- Service Class Id
  varMissingCopies      INTEGER;
BEGIN
  -- find and lock castor file for the nsHost/fileID
  SELECT CF.id, CF.fileSize INTO varCfId, varFileSize
    FROM CastorFile CF
   WHERE CF.fileid = inFileId
     AND CF.nsHost = inNsHost
     FOR UPDATE;
  -- Find and lock the RecallJob, plus get the number of missing copies
  SELECT missingCopies INTO varMissingCopies
    FROM Recalljob
   WHERE FileTransactionId = inFileTransaction
     AND fSeq = inFseq;
  -- find and lock the disk copy. There should be only one.
  SELECT DC.id INTO varDcId
    FROM DiskCopy DC
   WHERE DC.castorFile = varCfId
     AND DC.status = dconst.DISKCOPY_WAITTAPERECALL;
  -- delete reacll jobs
  deleteRecallJobs(varCfId);
  -- update diskcopy status, size and gweight
  SELECT /*+ INDEX(SR I_Subrequest_DiskCopy)*/ SR.id, SR.request, SR.reqType
    INTO varSubrequestId, varRequestId, varRequestType
    FROM SubRequest SR
   WHERE SR.diskcopy = varDcId;
  SELECT REQ.svcClass, REQ.euid, REQ.egid INTO varSvcClassId, varEuid, varEgid
    FROM (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClass, euid, egid FROM StageGetRequest                                  UNION ALL
          SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, svcClass, euid, egid FROM StagePrepareToGetRequest       UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClass, euid, egid FROM StageUpdateRequest                         UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcClass, euid, egid FROM StagePrepareToUpdateRequest UNION ALL
          SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, svcClass, euid, egid FROM StageRepackRequest) REQ
    WHERE REQ.id = varRequestId;
  varGcWeightProc := castorGC.getRecallWeight(varSvcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || varGcWeightProc || '(:size); END;'
    USING OUT varGcWeight, IN varFileSize;
  UPDATE DiskCopy DC
    SET DC.status = decode(varMissingCopies,
                           0, dconst.DISKCOPY_STAGED,
                              dconst.DISKCOPY_CANBEMIGR), -- DISKCOPY_STAGED if varMissingCopies = 0, otherwise CANBEMIGR
        DC.lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
        DC.gcWeight = varGcWeight,
        DC.diskCopySize = varFileSize
    WHERE Dc.id = varDcId;
  -- determine the type of the request
  IF varRequestType = 119 THEN  -- OBJ_StageRepackRequest
    startRepackMigration(varSubrequestId, varCfId, varDcId, varEuid, varEgid);
  ELSE  -- restart this subrequest so that the stager can follow it up
    UPDATE /*+ INDEX(SR PK_Subrequest_Id)*/ SubRequest SR
       SET SR.status = dconst.SUBREQUEST_RESTART,
           SR.getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
           SR.lastModificationTime = getTime(), SR.parent = 0
     WHERE SR.id = varSubrequestId;
  END IF;
  -- and trigger new migrations if missing tape copies were detected
  IF varMissingCopies > 0 THEN
    initMigration(varCfId, varFileSize, varMissingCopies, varSvcClassId);
  END IF;
  -- restart other requests waiting on this recall
  UPDATE /*+ INDEX(SR I_Subrequest_Parent)*/ SubRequest SR
     SET SR.status = dconst.SUBREQUEST_RESTART,
         SR.getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
         SR.lastModificationTime = getTime(), SR.parent = 0
   WHERE SR.parent = varSubrequestId;
  -- trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(varCfId, varEuid, varEgid);
  COMMIT;
END;
/

/* save in the db the results returned by the retry policy for migration */
CREATE OR REPLACE
PROCEDURE tg_setMigRetryResult(
  mjToRetry IN castor."cnumList",
  mjToFail  IN castor."cnumList" ) AS
  srId NUMBER;
  cfId NUMBER;

BEGIN
   -- check because oracle cannot handle empty buffer
  IF mjToRetry( mjToRetry.FIRST) != -1 THEN
    
    -- restarted the one to be retried
    FOR i IN mjToRetry.FIRST .. mjToRetry.LAST LOOP
      UPDATE MigrationJob SET
        status = tconst.MIGRATIONJOB_PENDING,
        nbretry = nbretry+1,
        vid = NULL  -- this job will not go to this volume after all, at least not now...
        WHERE id = mjToRetry(i);
    END LOOP;
  END IF;

  -- check because oracle cannot handle empty buffer
  IF mjToFail(mjToFail.FIRST) != -1 THEN
    -- fail the migration jobs
    FORALL i IN mjToFail.FIRST .. mjToFail.LAST
      UPDATE MigrationJob SET
        status = tconst.MIGRATIONJOB_FAILED
      WHERE id = mjToFail(i);

    -- fail repack subrequests
    FOR i IN mjToFail.FIRST .. mjToFail.LAST LOOP
        BEGIN
        -- we don't need a lock on castorfile because we cannot have a parallel migration of the same file using repack
          SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/
                 SubRequest.id, SubRequest.castorfile into srId, cfId
            FROM SubRequest, MigrationJob
            WHERE MigrationJob.id = mjToFail(i)
            AND SubRequest.castorfile = MigrationJob.castorfile
            AND subrequest.status = dconst.SUBREQUEST_REPACK;

          -- STAGED because the copy on disk most probably is valid and the failure of repack happened during the migration

          UPDATE DiskCopy
            SET status = dconst.DISKCOPY_STAGED
            WHERE castorfile = cfId
            AND status=dconst.DISKCOPY_CANBEMIGR; -- otherwise repack will wait forever

          archivesubreq(srId,9);

        EXCEPTION WHEN NO_DATA_FOUND THEN
          NULL;
        END;
     END LOOP;
  END IF;

  COMMIT;
END;
/

/* save in the db the results returned by the retry policy for recall */
CREATE OR REPLACE PROCEDURE tg_setRecRetryResult(
  rjToRetry IN castor."cnumList", 
  rjToFail  IN castor."cnumList"  ) AS
  tapeId NUMBER;
  cfId NUMBER;

BEGIN
  -- I restart the recall that I want to retry
  -- check because oracle cannot handle empty buffer
  IF rjToRetry(rjToRetry.FIRST) != -1 THEN 

    -- recall job => TOBERECALLED
    FORALL i IN rjToRetry.FIRST .. rjToRetry.LAST
      UPDATE RecallJob
        SET status    = tconst.RECALLJOB_TOBERECALLED,
            errorcode = 0,
            nbretry   = nbretry+1 
        WHERE id=rjToRetry(i);
    
    -- segment => UNPROCESSED
    -- tape => PENDING if UNUSED OR FAILED with still segments unprocessed
    FOR i IN rjToRetry.FIRST .. rjToRetry.LAST LOOP
      UPDATE Segment
        SET status = tconst.SEGMENT_UNPROCESSED
        WHERE copy = rjToRetry(i)
        RETURNING tape INTO tapeId;
      UPDATE Tape
        SET status = tconst.TAPE_WAITPOLICY
        WHERE id = tapeId AND
          status IN (tconst.TAPE_UNUSED, tconst.TAPE_FAILED);
    END LOOP;
  END IF;
  
  -- I mark as failed the hopeless recall
  -- check because oracle cannot handle empty buffer
  IF rjToFail(rjToFail.FIRST) != -1 THEN
    FOR i IN rjToFail.FIRST .. rjToFail.LAST  LOOP

      -- lock castorFile
      SELECT castorFile INTO cfId 
        FROM RecallJob, CastorFile
        WHERE RecallJob.id = rjToFail(i) 
        AND CastorFile.id = RecallJob.castorfile 
        FOR UPDATE OF castorfile.id;

      -- fail diskcopy
      UPDATE DiskCopy SET status = dconst.DISKCOPY_FAILED
        WHERE castorFile = cfId 
        AND status = dconst.DISKCOPY_WAITTAPERECALL;
      
      -- Drop recall jobs. Ideally, we should keep some track that
      -- the recall failed in order to prevent future recalls until some
      -- sort of manual intervention. For the time being, as we can't
      -- say whether the failure is fatal or not, we drop everything
      -- and we won't deny a future request for recall.
      deleteRecallJobs(cfId);
      
      -- fail subrequests
      UPDATE /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest 
        SET status = dconst.SUBREQUEST_FAILED,
            getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED, --  (not strictly correct but the request is over anyway)
            lastModificationTime = getTime(),
            errorCode = 1015,  -- SEINTERNAL
            errorMessage = 'File recall from tape has failed, please try again later',
            parent = 0
        WHERE castorFile = cfId 
        AND status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_WAITSUBREQ);
    
    END LOOP;
  END IF;
  COMMIT;
END;
/


/* update the db when a tape session is started */
CREATE OR REPLACE
PROCEDURE  tg_startTapeSession(
  inVdqmReqId    IN  NUMBER,
  outVid         OUT NOCOPY VARCHAR2,
  outAccessMode  OUT INTEGER,
  outRet         OUT INTEGER,
  outDensity     OUT NOCOPY VARCHAR2,
  outLabel       OUT NOCOPY VARCHAR2 ) AS
  varTGReqId         NUMBER;
  varTpId            NUMBER;
  varMountId        NUMBER;
  varUnused          NUMBER;
BEGIN
  outRet:=-2;
  -- set the request to ONGOING
  -- Transition from REQUEST WAITING TAPE SERVER to ONGOING
  -- is equivalent to WAITTAPERIVE to MOUNTED for the tape read
  -- and WAITDRIVE or WAITMOUNT to RUNNING for a migration job.

  -- Step 1, pick the migration mount or tape.
  tg_findFromVDQMReqId(inVdqmReqId, varTpId, varMountId);
  IF (varTpId IS NOT NULL) THEN
    -- Read case
    outAccessMode := 0;
    BEGIN
      SELECT 1 INTO varUnused FROM dual
       WHERE EXISTS (SELECT 'x' FROM Segment S
                      WHERE S.tape = varTpId);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      UPDATE Tape T
         SET T.lastvdqmpingtime=0
       WHERE T.id = varTpId; -- to force the cleanup
      outRet:=-1; --NO MORE FILES
      COMMIT;
      RETURN;
    END;
    UPDATE Tape T
       SET T.status = tconst.TAPE_MOUNTED
     WHERE T.id = varTpId
       AND T.tpmode = tconst.TPMODE_READ
    RETURNING T.vid,  T.label,  T.density
      INTO   outVid, outLabel, outDensity; -- tape is MOUNTED
    outRet:=0;
    COMMIT;
    RETURN;
  ELSIF (varMountId IS NOT NULL) THEN
    -- Write case
    outAccessMode := 1;
    BEGIN
      SELECT 1 INTO varUnused FROM dual
       WHERE EXISTS (SELECT 'x' FROM MigrationJob, MigrationMount
                      WHERE MigrationJob.tapepool = MigrationMount.tapepool
                        AND MigrationMount.id = varMountId);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- no more files
      UPDATE MigrationMount
         SET lastVDQMPingTime = 0
       WHERE id = varMountId;
      outRet:=-1; --NO MORE FILES
      outVid:=NULL;
      COMMIT;
      RETURN;
    END;
    UPDATE MigrationMount
       SET status = tconst.MIGRATIONMOUNT_MIGRATING
     WHERE id = varMountId
     RETURNING VID, label, density INTO outVid, outLabel, outDensity;
    outRet:=0;
    COMMIT;
  ELSE
    -- Not found
    outRet:=-2; -- UNKNOWN request
  END IF;
END;
/

/* delete MigrationMount */
CREATE OR REPLACE
PROCEDURE tg_deleteMigrationMount(inMountId IN NUMBER) AS
BEGIN
  DELETE FROM MigrationMount WHERE id=inMountId;
END;
/

CREATE OR REPLACE
PROCEDURE TG_SetFileStaleInMigration(
  /* When discovering in name server that a file has been changed during its migration,
  we have to finish it and indicate to the stager that the discopy is outdated */
  inTransId         IN  NUMBER,
  inFileId          IN  NUMBER,
  inNsHost          IN  VARCHAR2,
  inFseq            IN  INTEGER,
  inFileTransaction IN  NUMBER) AS
  varCfId               NUMBER;
  varUnused             NUMBER;
BEGIN
  -- Find MigrationMount from vdqm vol req ID and lock it
  SELECT VDQMVolReqId INTO varUnused FROM MigrationMount
   WHERE VDQMVolReqId = inTransId FOR UPDATE;
  -- Lock the CastorFile
  SELECT CF.id INTO varCfId FROM CastorFile CF
   WHERE CF.fileid = inFileId
     AND CF.nsHost = inNsHost
     FOR UPDATE;
  -- XXX This migration job is done. The other ones (if any) will have to live
  -- an independant life without the diskcopy (and fail)
  -- After the schema remake in 09/2011 for 2.1.12, we expect to
  -- fully cover those cases.
  -- As of today,we are supposed to repack a file's tape copies
  -- one by one. So should be exempt of this problem.
  DELETE FROM MigrationJob
   WHERE FileTransactionId = inFileTransaction
     AND fSeq = inFseq;
  -- The disk copy is known stale. Let's invalidate it:
  UPDATE DiskCopy DC
     SET DC.status= dconst.DISKCOPY_INVALID
   WHERE DC.castorFile = varCfId
     AND DC.status= dconst.DISKCOPY_CANBEMIGR;
  -- archive Repack requests should any be in the db
  FOR i IN (
    SELECT /*+ INDEX(SR I_Subrequest_Castorfile)*/ SR.id FROM SubRequest SR
    WHERE SR.castorfile = varCfId AND
          SR.status= dconst.SUBREQUEST_REPACK
    ) LOOP
      archivesubreq(i.id, 8); -- SUBREQUEST_FINISHED
  END LOOP;
END;
/

/* delete taperequest  for not existing tape */
CREATE OR REPLACE
PROCEDURE tg_deleteTapeRequest( inTGReqId IN NUMBER ) AS
  /* The tape gateway request does not exist per se, but 
   * references to its ID should be removed (with needed consequences
   * from the structures pointing to it) */
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varTpReqId NUMBER;     -- Tape Recall (TapeGatewayReequest.TapeRecall)
  varMountId NUMBER;       -- migration mount Id.
  varSegNum INTEGER;
  varCfId NUMBER;        -- CastorFile Id
  varRjIds "numList";    -- recall job IDs
BEGIN
  -- Find the relevant migration mount or reading tape id.
  tg_findFromTGRequestId (inTGReqId, varTpReqId, varMountId);
  -- Find out whether this is a read or a write
  IF (varTpReqId IS NOT NULL) THEN
    -- Lock and reset the tape in case of a read
    UPDATE Tape T
      SET T.status = tconst.TAPE_UNUSED
      WHERE T.id = varTpReqId;
    SELECT SEG.copy BULK COLLECT INTO varRjIds 
      FROM Segment SEG 
     WHERE SEG.tape = varTpReqId;
    FOR i IN varRjIds.FIRST .. varRjIds.LAST  LOOP
      -- lock castorFile
      SELECT RJ.castorFile INTO varCfId 
        FROM RecallJob RJ, CastorFile CF
        WHERE RJ.id = varRjIds(i) 
        AND CF.id = RJ.castorfile 
        FOR UPDATE OF CF.id;
      -- fail diskcopy, drop recall and migration jobs
      UPDATE DiskCopy DC SET DC.status = dconst.DISKCOPY_FAILED
       WHERE DC.castorFile = varCfId 
         AND DC.status = dconst.DISKCOPY_WAITTAPERECALL;
      deleteRecallJobs(varCfId);
      deleteMigrationJobs(varCfId);
      -- Fail the subrequest
      UPDATE /*+ INDEX(SR I_Subrequest_Castorfile)*/ SubRequest SR
         SET SR.status = dconst.SUBREQUEST_FAILED,
             SR.getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED, --  (not strictly correct but the request is over anyway)
             SR.lastModificationTime = getTime(),
             SR.errorCode = 1015,  -- SEINTERNAL
             SR.errorMessage = 'File recall from tape has failed, please try again later',
             SR.parent = 0
       WHERE SR.castorFile = varCfId 
         AND SR.status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_WAITSUBREQ);
    END LOOP;
  ELSIF (varMountId IS NOT NULL) THEN
    -- In case of a write, delete the migration mount
    DELETE FROM MigrationMount  WHERE vdqmVolReqId = varMountId;
  ELSE
    -- Wrong Access Mode encountered. Notify.
    RAISE_APPLICATION_ERROR(-20292, 'tg_deleteTapeRequest: no read tape or '||
      'migration mount found for TapeGatewayRequestId: '|| inTGReqId);
  END IF;
END;
/

/* flag tape as full for a given session */
CREATE OR REPLACE
PROCEDURE tg_flagTapeFull ( inTGReqId IN NUMBER ) AS
  /* The tape gateway request does not exist per se, but 
   * references to its ID should be removed (with needed consequences
   * from the structures pointing to it) */
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varUnused NUMBER;
  varMJId NUMBER;
BEGIN
  -- Find the relevant migration or recall mount id.
  tg_findFromVDQMReqId (inTGReqId, varUnused, varMJId);
  -- Find out whether this is a read or a write
  IF (varMJId IS NOT NULL) THEN
    UPDATE MigrationMount
       SET full = 1
     WHERE id = varMJId;
  ELSE
    -- Wrong Access Mode encountered. Notify.
    RAISE_APPLICATION_ERROR(-20292, 'tg_flagTapeFullForMigrationSession: '||
      'no migration mount found for TapeGatewayRequestId: '|| inTGReqId);
  END IF;
END;
/

/*******************************************************************
 *
 * @(#)RCSfile: oracleGC.sql,v  Revision: 1.698  Date: 2009/08/17 15:08:33  Author: sponcec3 
 *
 * PL/SQL code for stager cleanup and garbage collecting
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* PL/SQL declaration for the castorGC package */
CREATE OR REPLACE PACKAGE castorGC AS
  TYPE SelectFiles2DeleteLine IS RECORD (
        path VARCHAR2(2048),
        id NUMBER,
        fileId NUMBER,
        nsHost VARCHAR2(2048),
        lastAccessTime INTEGER,
        nbAccesses NUMBER,
        gcWeight NUMBER,
        gcTriggeredBy VARCHAR2(2048),
        svcClassName VARCHAR2(2048));
  TYPE SelectFiles2DeleteLine_Cur IS REF CURSOR RETURN SelectFiles2DeleteLine;
  TYPE JobLogEntry IS RECORD (
    fileid NUMBER,
    nshost VARCHAR2(2048),
    operation INTEGER);
  TYPE JobLogEntry_Cur IS REF CURSOR RETURN JobLogEntry;
  -- find out a gc function to be used from a given serviceClass
  FUNCTION getUserWeight(svcClassId NUMBER) RETURN VARCHAR2;
  FUNCTION getRecallWeight(svcClassId NUMBER) RETURN VARCHAR2;
  FUNCTION getCopyWeight(svcClassId NUMBER) RETURN VARCHAR2;
  FUNCTION getFirstAccessHook(svcClassId NUMBER) RETURN VARCHAR2;
  FUNCTION getAccessHook(svcClassId NUMBER) RETURN VARCHAR2;
  FUNCTION getUserSetGCWeight(svcClassId NUMBER) RETURN VARCHAR2;
  -- compute gcWeight from size
  FUNCTION size2GCWeight(s NUMBER) RETURN NUMBER;
  -- Default gc policy
  FUNCTION sizeRelatedUserWeight(fileSize NUMBER, DiskCopyStatus NUMBER) RETURN NUMBER;
  FUNCTION sizeRelatedRecallWeight(fileSize NUMBER) RETURN NUMBER;
  FUNCTION sizeRelatedCopyWeight(fileSize NUMBER, DiskCopyStatus NUMBER, sourceWeight NUMBER) RETURN NUMBER;
  FUNCTION dayBonusFirstAccessHook(oldGcWeight NUMBER, creationTime NUMBER) RETURN NUMBER;
  FUNCTION halfHourBonusAccessHook(oldGcWeight NUMBER, creationTime NUMBER, nbAccesses NUMBER) RETURN NUMBER;
  FUNCTION cappedUserSetGCWeight(oldGcWeight NUMBER, userDelta NUMBER) RETURN NUMBER;
  -- FIFO gc policy
  FUNCTION creationTimeUserWeight(fileSize NUMBER, DiskCopyStatus NUMBER) RETURN NUMBER;
  FUNCTION creationTimeRecallWeight(fileSize NUMBER) RETURN NUMBER;
  FUNCTION creationTimeCopyWeight(fileSize NUMBER, DiskCopyStatus NUMBER, sourceWeight NUMBER) RETURN NUMBER;
  -- LRU gc policy
  FUNCTION LRUFirstAccessHook(oldGcWeight NUMBER, creationTime NUMBER) RETURN NUMBER;
  FUNCTION LRUAccessHook(oldGcWeight NUMBER, creationTime NUMBER, nbAccesses NUMBER) RETURN NUMBER;
END castorGC;
/

CREATE OR REPLACE PACKAGE BODY castorGC AS

  FUNCTION getUserWeight(svcClassId NUMBER) RETURN VARCHAR2 AS
    ret VARCHAR2(2048);
  BEGIN
    SELECT userWeight INTO ret
      FROM SvcClass, GcPolicy
     WHERE SvcClass.id = svcClassId
       AND SvcClass.gcPolicy = GcPolicy.name;
    RETURN ret;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- we did not get any policy, let's go for the default
    SELECT userWeight INTO ret
      FROM GcPolicy
     WHERE GcPolicy.name = 'default';
    RETURN ret;
  END;

  FUNCTION getRecallWeight(svcClassId NUMBER) RETURN VARCHAR2 AS
    ret VARCHAR2(2048);
  BEGIN
    SELECT recallWeight INTO ret
      FROM SvcClass, GcPolicy
     WHERE SvcClass.id = svcClassId
       AND SvcClass.gcPolicy = GcPolicy.name;
    RETURN ret;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- we did not get any policy, let's go for the default
    SELECT recallWeight INTO ret
      FROM GcPolicy
     WHERE GcPolicy.name = 'default';
    RETURN ret;
  END;

  FUNCTION getCopyWeight(svcClassId NUMBER) RETURN VARCHAR2 AS
    ret VARCHAR2(2048);
  BEGIN
    SELECT copyWeight INTO ret
      FROM SvcClass, GcPolicy
     WHERE SvcClass.id = svcClassId
       AND SvcClass.gcPolicy = GcPolicy.name;
    RETURN ret;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- we did not get any policy, let's go for the default
    SELECT copyWeight INTO ret
      FROM GcPolicy
     WHERE GcPolicy.name = 'default';
    RETURN ret;
  END;

  FUNCTION getFirstAccessHook(svcClassId NUMBER) RETURN VARCHAR2 AS
    ret VARCHAR2(2048);
  BEGIN
    SELECT firstAccessHook INTO ret
      FROM SvcClass, GcPolicy
     WHERE SvcClass.id = svcClassId
       AND SvcClass.gcPolicy = GcPolicy.name;
    RETURN ret;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RETURN NULL;
  END;

  FUNCTION getAccessHook(svcClassId NUMBER) RETURN VARCHAR2 AS
    ret VARCHAR2(2048);
  BEGIN
    SELECT accessHook INTO ret
      FROM SvcClass, GcPolicy
     WHERE SvcClass.id = svcClassId
       AND SvcClass.gcPolicy = GcPolicy.name;
    RETURN ret;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RETURN NULL;
  END;

  FUNCTION getUserSetGCWeight(svcClassId NUMBER) RETURN VARCHAR2 AS
    ret VARCHAR2(2048);
  BEGIN
    SELECT userSetGCWeight INTO ret
      FROM SvcClass, GcPolicy
     WHERE SvcClass.id = svcClassId
       AND SvcClass.gcPolicy = GcPolicy.name;
    RETURN ret;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RETURN NULL;
  END;

  FUNCTION size2GCWeight(s NUMBER) RETURN NUMBER IS
  BEGIN
    IF s < 1073741824 THEN
      RETURN 1073741824/(s+1)*86400 + getTime();  -- 1GB/filesize (days) + current time as lastAccessTime
    ELSE
      RETURN 86400 + getTime();  -- the value for 1G file. We do not make any difference for big files and privilege FIFO
    END IF;
  END;

  FUNCTION sizeRelatedUserWeight(fileSize NUMBER, DiskCopyStatus NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN size2GCWeight(fileSize);
  END;

  FUNCTION sizeRelatedRecallWeight(fileSize NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN size2GCWeight(fileSize);
  END;

  FUNCTION sizeRelatedCopyWeight(fileSize NUMBER, DiskCopyStatus NUMBER, sourceWeight NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN size2GCWeight(fileSize);
  END;

  FUNCTION dayBonusFirstAccessHook(oldGcWeight NUMBER, creationTime NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN oldGcWeight - 86400;
  END;

  FUNCTION halfHourBonusAccessHook(oldGcWeight NUMBER, creationTime NUMBER, nbAccesses NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN oldGcWeight + 1800;
  END;

  FUNCTION cappedUserSetGCWeight(oldGcWeight NUMBER, userDelta NUMBER) RETURN NUMBER AS
  BEGIN
    IF userDelta >= 18000 THEN -- 5h max
      RETURN oldGcWeight + 18000;
    ELSE
      RETURN oldGcWeight + userDelta;
    END IF;
  END;

  -- FIFO gc policy
  FUNCTION creationTimeUserWeight(fileSize NUMBER, DiskCopyStatus NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN getTime();
  END;

  FUNCTION creationTimeRecallWeight(fileSize NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN getTime();
  END;

  FUNCTION creationTimeCopyWeight(fileSize NUMBER, DiskCopyStatus NUMBER, sourceWeight NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN getTime();
  END;

  -- LRU gc policy
  FUNCTION LRUFirstAccessHook(oldGcWeight NUMBER, creationTime NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN getTime();
  END;

  FUNCTION LRUAccessHook(oldGcWeight NUMBER, creationTime NUMBER, nbAccesses NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN getTime();
  END;

END castorGC;
/

/* PL/SQL method implementing selectFiles2Delete
   This is the standard garbage collector: it sorts STAGED
   diskcopies by gcWeight and selects them for deletion up to
   the desired free space watermark */
CREATE OR REPLACE
PROCEDURE selectFiles2Delete(diskServerName IN VARCHAR2,
                             files OUT castorGC.SelectFiles2DeleteLine_Cur) AS
  dcIds "numList";
  freed INTEGER;
  deltaFree INTEGER;
  toBeFreed INTEGER;
  dontGC INTEGER;
  totalCount INTEGER;
  unused INTEGER;
  backoff INTEGER;
  CastorFileLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (CastorFileLocked, -54);
BEGIN
  -- First of all, check if we are in a Disk1 pool
  dontGC := 0;
  FOR sc IN (SELECT disk1Behavior
               FROM SvcClass, DiskPool2SvcClass D2S, DiskServer, FileSystem
              WHERE SvcClass.id = D2S.child
                AND D2S.parent = FileSystem.diskPool
                AND FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = diskServerName) LOOP
    -- If any of the service classes to which we belong (normally a single one)
    -- say this is Disk1, we don't GC STAGED files.
    IF sc.disk1Behavior = 1 THEN
      dontGC := 1;
      EXIT;
    END IF;
  END LOOP;

  -- Loop on all concerned fileSystems in a random order.
  totalCount := 0;
  FOR fs IN (SELECT FileSystem.id
               FROM FileSystem, DiskServer
              WHERE FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = diskServerName
             ORDER BY dbms_random.value) LOOP

    -- Count the number of diskcopies on this filesystem that are in a
    -- BEINGDELETED state. These need to be reselected in any case.
    freed := 0;
    SELECT totalCount + count(*), nvl(sum(DiskCopy.diskCopySize), 0)
      INTO totalCount, freed
      FROM DiskCopy
     WHERE DiskCopy.fileSystem = fs.id
       AND decode(DiskCopy.status, 9, DiskCopy.status, NULL) = 9; -- BEINGDELETED

    -- estimate the number of GC running the "long" query, that is the one dealing with the GCing of
    -- STAGED files.
    SELECT COUNT(*) INTO backoff
      FROM v$session s, v$sqltext t
     WHERE s.sql_id = t.sql_id AND t.sql_text LIKE '%I_DiskCopy_FS_GCW%';

    -- Process diskcopies that are in an INVALID state.
    UPDATE DiskCopy
       SET status = 9, -- BEINGDELETED
           gcType = decode(gcType, NULL, 1, gcType)
     WHERE fileSystem = fs.id
       AND status = 7  -- INVALID
       AND NOT EXISTS
         -- Ignore diskcopies with active subrequests
         (SELECT /*+ INDEX(SubRequest I_SubRequest_DiskCopy) */ 'x'
            FROM SubRequest
           WHERE SubRequest.diskcopy = DiskCopy.id
             AND SubRequest.status IN (4, 5, 6, 12, 13, 14)) -- being processed (WAIT*, READY, *SCHED)
       AND NOT EXISTS
         -- Ignore diskcopies with active replications
         (SELECT 'x' FROM StageDiskCopyReplicaRequest, DiskCopy D
           WHERE StageDiskCopyReplicaRequest.destDiskCopy = D.id
             AND StageDiskCopyReplicaRequest.sourceDiskCopy = DiskCopy.id
             AND D.status = 1)  -- WAITD2D
       AND rownum <= 10000 - totalCount
    RETURNING id BULK COLLECT INTO dcIds;
    COMMIT;

    -- If we have more than 10,000 files to GC, exit the loop. There is no point
    -- processing more as the maximum sent back to the client in one call is
    -- 10,000. This protects the garbage collector from being overwhelmed with
    -- requests and reduces the stager DB load. Furthermore, if too much data is
    -- sent back to the client, the transfer time between the stager and client
    -- becomes very long and the message may timeout or may not even fit in the
    -- clients receive buffer!
    totalCount := totalCount + dcIds.COUNT();
    EXIT WHEN totalCount >= 10000;

    -- Continue processing but with STAGED files, only in case we are not already loaded
    IF dontGC = 0 AND backoff < 4 THEN
      -- Do not delete STAGED files from non production hardware
      BEGIN
        SELECT FileSystem.id INTO unused
          FROM DiskServer, FileSystem
         WHERE FileSystem.id = fs.id
           AND FileSystem.status = 0  -- PRODUCTION
           AND FileSystem.diskserver = DiskServer.id
           AND DiskServer.status = 0; -- PRODUCTION
      EXCEPTION WHEN NO_DATA_FOUND THEN
        EXIT;
      END;
      -- Calculate the amount of space that would be freed on the filesystem
      -- if the files selected above were to be deleted.
      IF dcIds.COUNT > 0 THEN
        SELECT freed + sum(diskCopySize) INTO freed
          FROM DiskCopy
         WHERE DiskCopy.id IN
             (SELECT /*+ CARDINALITY(fsidTable 5) */ *
                FROM TABLE(dcIds) dcidTable);
      END IF;
      -- Get the amount of space to be liberated
      SELECT decode(sign(maxFreeSpace * totalSize - free), -1, 0, maxFreeSpace * totalSize - free)
        INTO toBeFreed
        FROM FileSystem
       WHERE id = fs.id;
      -- If space is still required even after removal of INVALID files, consider
      -- removing STAGED files until we are below the free space watermark
      IF freed < toBeFreed THEN
        -- Loop on file deletions
        FOR dc IN (SELECT /*+ INDEX(DiskCopy I_DiskCopy_FS_GCW) */ id, castorFile FROM DiskCopy
                    WHERE fileSystem = fs.id
                      AND status = 0 -- STAGED
                      AND NOT EXISTS (
                        SELECT /*+ INDEX(SubRequest I_SubRequest_DiskCopy) */ 'x'
                          FROM SubRequest
                         WHERE SubRequest.diskcopy = DiskCopy.id
                           AND SubRequest.status IN (4, 5, 6, 12, 13, 14)) -- being processed (WAIT*, READY, *SCHED)
                      AND NOT EXISTS
                        -- Ignore diskcopies with active replications
                        (SELECT /*+ INDEX(DCRR I_StageDiskCopyReplic_DestDC) */ 'x'
                           FROM StageDiskCopyReplicaRequest DCRR, DiskCopy DD
                          WHERE DCRR.destDiskCopy = DD.id
                            AND DCRR.sourceDiskCopy = DiskCopy.id
                            AND DD.status = 1)  -- WAITD2D
                      ORDER BY gcWeight ASC) LOOP
          BEGIN
            -- Lock the CastorFile
            SELECT id INTO unused FROM CastorFile
             WHERE id = dc.castorFile FOR UPDATE NOWAIT;
            -- Mark the DiskCopy as being deleted
            UPDATE DiskCopy
               SET status = 9, -- BEINGDELETED
                   gcType = 0  -- GCTYPE_AUTO
             WHERE id = dc.id RETURNING diskCopySize INTO deltaFree;
            totalCount := totalCount + 1;
            -- Update freed space
            freed := freed + deltaFree;
            -- Shall we continue ?
            IF toBeFreed <= freed THEN
              EXIT;
            END IF;
            IF totalCount >= 10000 THEN
              EXIT;
            END IF;           
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              -- The file no longer exists or has the wrong state
              NULL;
            WHEN CastorFileLocked THEN
              -- Go to the next candidate, processing is taking place on the
              -- file
              NULL;
          END;
          COMMIT;
        END LOOP;
      END IF;
    END IF;
    -- We have enough files to exit the loop ?
    EXIT WHEN totalCount >= 10000;
  END LOOP;

  -- Now select all the BEINGDELETED diskcopies in this diskserver for the GC daemon
  OPEN files FOR
    SELECT /*+ INDEX(CastorFile PK_CastorFile_ID) */ FileSystem.mountPoint || DiskCopy.path,
           DiskCopy.id,
           Castorfile.fileid, Castorfile.nshost,
           DiskCopy.lastAccessTime, DiskCopy.nbCopyAccesses, DiskCopy.gcWeight,
           CASE WHEN DiskCopy.gcType = 0 THEN 'Automatic'
                WHEN DiskCopy.gcType = 1 THEN 'User Requested'
                WHEN DiskCopy.gcType = 2 THEN 'Too many replicas'
                WHEN DiskCopy.gcType = 3 THEN 'Draining filesystem'
                WHEN DiskCopy.gcType = 4 THEN 'NS synchronization'
                ELSE 'Unknown' END,
           getSvcClassList(FileSystem.id)
      FROM CastorFile, DiskCopy, FileSystem, DiskServer
     WHERE decode(DiskCopy.status, 9, DiskCopy.status, NULL) = 9 -- BEINGDELETED
       AND DiskCopy.castorfile = CastorFile.id
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskServer = DiskServer.id
       AND DiskServer.name = diskServerName
       AND rownum <= 10000;
END;
/


/*
 * PL/SQL method implementing filesDeleted
 * Note that we don't increase the freespace of the fileSystem.
 * This is done by the monitoring daemon, that knows the
 * exact amount of free space.
 * dcIds gives the list of diskcopies to delete.
 * fileIds returns the list of castor files to be removed
 * from the name server
 */
CREATE OR REPLACE PROCEDURE filesDeletedProc
(dcIds IN castor."cnumList",
 fileIds OUT castor.FileList_Cur) AS
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- List the castorfiles to be cleaned up afterwards
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      INSERT INTO filesDeletedProcHelper VALUES
           ((SELECT castorFile FROM DiskCopy
              WHERE id = dcIds(i)));
    -- Loop over the deleted files; first use FORALL for bulk operation
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      DELETE FROM DiskCopy WHERE id = dcIds(i);
    -- Then use a normal loop to clean castorFiles. Note: We order the list to
    -- prevent a deadlock
    FOR cf IN (SELECT DISTINCT(cfId)
                 FROM filesDeletedProcHelper
                ORDER BY cfId ASC) LOOP
      DECLARE
        nb NUMBER;
      BEGIN
        -- First try to lock the castorFile
        SELECT id INTO nb FROM CastorFile
         WHERE id = cf.cfId FOR UPDATE;
        -- See whether it has any DiskCopy
        SELECT count(*) INTO nb FROM DiskCopy
         WHERE castorFile = cf.cfId;
        -- If any DiskCopy, give up
        IF nb = 0 THEN
          -- Delete the migrations and recalls
          deleteMigrationJobs(cf.cfId);
          deleteRecallJobs(cf.cfId);
          -- See whether pending SubRequests exist
          SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
            FROM SubRequest
           WHERE castorFile = cf.cfId
             AND status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 12, 13, 14);  -- All but FINISHED, FAILED_FINISHED, ARCHIVED
          IF nb = 0 THEN
            -- No Subrequest, delete the CastorFile
            DECLARE
              fid NUMBER;
              fc NUMBER;
              nsh VARCHAR2(2048);
            BEGIN
              -- Delete the CastorFile
              DELETE FROM CastorFile WHERE id = cf.cfId
              RETURNING fileId, nsHost, fileClass
                INTO fid, nsh, fc;
              -- Check whether this file potentially had copies on tape
              SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
              IF nb = 0 THEN
                -- This castorfile was created with no copy on tape
                -- So removing it from the stager means erasing
                -- it completely. We should thus also remove it
                -- from the name server
                INSERT INTO FilesDeletedProcOutput VALUES (fid, nsh);
              END IF;
            END;
          ELSE
            -- SubRequests exist, fail them
            UPDATE /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest
               SET status = 7 -- FAILED
             WHERE castorFile = cf.cfId
               AND status IN (0, 1, 2, 3, 4, 5, 6, 12, 13, 14);
          END IF;
        END IF;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- Ignore, this means that the castorFile did not exist.
        -- There is thus no way to find out whether to remove the
        -- file from the nameserver. For safety, we thus keep it
        NULL;
      END;
    END LOOP;
  END IF;
  OPEN fileIds FOR
    SELECT fileId, nsHost FROM FilesDeletedProcOutput;
END;
/

/* PL/SQL method implementing filesDeletionFailedProc */
CREATE OR REPLACE PROCEDURE filesDeletionFailedProc
(dcIds IN castor."cnumList") AS
  cfId NUMBER;
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- Loop over the files
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      UPDATE DiskCopy SET status = 4 -- FAILED
       WHERE id = dcIds(i);
  END IF;
END;
/



/* PL/SQL method implementing nsFilesDeletedProc */
CREATE OR REPLACE PROCEDURE nsFilesDeletedProc
(nh IN VARCHAR2,
 fileIds IN castor."cnumList",
 orphans OUT castor.IdRecord_Cur) AS
  unused NUMBER;
  nsHostName VARCHAR2(2048);
BEGIN
  IF fileIds.COUNT <= 0 THEN
    RETURN;
  END IF;
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- Loop over the deleted files and split the orphan ones
  -- from the normal ones
  FOR fid in fileIds.FIRST .. fileIds.LAST LOOP
    BEGIN
      SELECT id INTO unused FROM CastorFile
       WHERE fileid = fileIds(fid) AND nsHost = nsHostName;
      stageForcedRm(fileIds(fid), nsHostName, 4);  -- GCTYPE_NS_SYNCH
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
/


/* PL/SQL method implementing stgFilesDeletedProc */
CREATE OR REPLACE PROCEDURE stgFilesDeletedProc
(dcIds IN castor."cnumList",
 stgOrphans OUT castor.IdRecord_Cur) AS
  unused NUMBER;
BEGIN
  -- Nothing to do
  IF dcIds.COUNT <= 0 THEN
    RETURN;
  END IF;
  -- Insert diskcopy ids into a temporary table
  FORALL i IN dcIds.FIRST..dcIds.LAST
   INSERT INTO StgFilesDeletedOrphans VALUES(dcIds(i));
  -- Return a list of diskcopy ids which no longer exist
  OPEN stgOrphans FOR
    SELECT diskCopyId FROM StgFilesDeletedOrphans
     WHERE NOT EXISTS (
        SELECT 'x' FROM DiskCopy
         WHERE id = diskCopyId);
END;
/


/** Cleanup job **/

/* A little generic method to delete efficiently */
CREATE OR REPLACE PROCEDURE bulkDelete(sel IN VARCHAR2, tab IN VARCHAR2) AS
BEGIN
  EXECUTE IMMEDIATE
  'DECLARE
    CURSOR s IS '||sel||'
    ids "numList";
  BEGIN
    LOOP
      OPEN s;
      FETCH s BULK COLLECT INTO ids LIMIT 100000;
      EXIT WHEN ids.count = 0;
      FORALL i IN ids.FIRST..ids.LAST
        DELETE FROM '||tab||' WHERE id = ids(i);
      CLOSE s;
      COMMIT;
    END LOOP;
  END;';
END;
/

/* A generic method to delete requests of a given type */
CREATE OR REPLACE Procedure bulkDeleteRequests(reqType IN VARCHAR) AS
BEGIN
  bulkDelete('SELECT id FROM '|| reqType ||' R WHERE
    NOT EXISTS (SELECT 1 FROM SubRequest WHERE request = R.id);',
    reqType);
END;
/

/* Search and delete old archived/failed subrequests and their requests */
CREATE OR REPLACE PROCEDURE deleteTerminatedRequests AS
  timeOut INTEGER;
  rate INTEGER;
  srIds "numList";
  ct NUMBER;
BEGIN
  -- select requested timeout from configuration table
  timeout := 3600*TO_NUMBER(getConfigOption('cleaning', 'terminatedRequestsTimeout', '120'));
  -- get a rough estimate of the current request processing rate
  SELECT count(*) INTO rate
    FROM SubRequest
   WHERE status IN (9, 11)  -- FAILED_FINISHED, ARCHIVED
     AND lastModificationTime > getTime() - 1800;
  IF rate > 0 AND (500000 / rate * 1800) < timeOut THEN
    timeOut := 500000 / rate * 1800;  -- keep 500k requests max
  END IF;
  
  -- delete castorFiles if nothing is left for them. Here we use
  -- a temporary table as we need to commit every ~1000 operations
  -- and keeping a cursor opened on the original select may take
  -- too long, leading to ORA-01555 'snapshot too old' errors.
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DeleteTermReqHelper';
  INSERT /*+ APPEND */ INTO DeleteTermReqHelper
    (SELECT id, castorFile FROM SubRequest
      WHERE status IN (9, 11)
        AND lastModificationTime < getTime() - timeOut);
  COMMIT;
  ct := 0;
  FOR cf IN (SELECT UNIQUE cfId FROM DeleteTermReqHelper) LOOP
    deleteCastorFile(cf.cfId);
    ct := ct + 1;
    IF ct = 1000 THEN
      COMMIT;
      ct := 0;
    END IF;
  END LOOP;

  -- now delete all old subRequest. We reuse here the
  -- temporary table, which serves as a snapshot of the
  -- entries to be deleted, and we use the FORALL logic
  -- (cf. bulkDelete) instead of a simple DELETE ...
  -- WHERE id IN (SELECT srId FROM DeleteTermReqHelper)
  -- for efficiency reasons. Moreover, we don't risk
  -- here the ORA-01555 error keeping the cursor open
  -- between commits as we are selecting on our
  -- temporary table.
  DECLARE
    CURSOR s IS
      SELECT srId FROM DeleteTermReqHelper;
    ids "numList";
  BEGIN
    OPEN s;
    LOOP
      FETCH s BULK COLLECT INTO ids LIMIT 10000;
      EXIT WHEN ids.count = 0;
      FORALL i IN ids.FIRST..ids.LAST
        DELETE FROM SubRequest WHERE id = ids(i);
      COMMIT;
    END LOOP;
    CLOSE s;
  END;
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DeleteTermReqHelper';

  -- and then related Requests
    ---- Get ----
  bulkDeleteRequests('StageGetRequest');
    ---- Put ----
  bulkDeleteRequests('StagePutRequest');
    ---- Update ----
  bulkDeleteRequests('StageUpdateRequest');
    ---- PrepareToGet -----
  bulkDeleteRequests('StagePrepareToGetRequest');
    ---- PrepareToPut ----
  bulkDeleteRequests('StagePrepareToPutRequest');
    ---- PrepareToUpdate ----
  bulkDeleteRequests('StagePrepareToUpdateRequest');
    ---- PutDone ----
  bulkDeleteRequests('StagePutDoneRequest');
    ---- Rm ----
  bulkDeleteRequests('StageRmRequest');
    ---- Repack ----
  bulkDeleteRequests('StageRepackRequest');
    ---- DiskCopyReplica ----
  bulkDeleteRequests('StageDiskCopyReplicaRequest');
    ---- SetGCWeight ----
  bulkDeleteRequests('SetFileGCWeight');
END;
/

/* Search and delete old diskCopies in bad states */
CREATE OR REPLACE PROCEDURE deleteFailedDiskCopies(timeOut IN NUMBER) AS
  dcIds "numList";
  cfIds "numList";
  ct INTEGER;
BEGIN
  -- select INVALID diskcopies without filesystem (they can exist after a
  -- stageRm that came before the diskcopy had been created on disk) and ALL FAILED
  -- ones (coming from failed recalls or failed removals from the GC daemon).
  -- Note that we don't select INVALID diskcopies from recreation of files
  -- because they are taken by the standard GC as they physically exist on disk.
  SELECT id
    BULK COLLECT INTO dcIds
    FROM DiskCopy
   WHERE (status = 4 OR (status = 7 AND fileSystem = 0))
     AND creationTime < getTime() - timeOut;
  SELECT /*+ INDEX(DC PK_DiskCopy_ID) */ UNIQUE castorFile
    BULK COLLECT INTO cfIds
    FROM DiskCopy DC
   WHERE id IN (SELECT /*+ CARDINALITY(ids 5) */ * FROM TABLE(dcIds) ids);
  -- drop the DiskCopies
  FORALL i IN dcIds.FIRST..dcIds.LAST
    DELETE FROM DiskCopy WHERE id = dcIds(i);
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
/

/* Deal with old diskCopies in STAGEOUT */
CREATE OR REPLACE PROCEDURE deleteOutOfDateStageOutDCs(timeOut IN NUMBER) AS
  srId NUMBER;
BEGIN
  -- Deal with old DiskCopies in STAGEOUT/WAITFS. The rule is to drop
  -- the ones with 0 fileSize and issue a putDone for the others
  FOR f IN (SELECT /*+ USE_NL(D C) INDEX(D I_DISKCOPY_STATUS) */ c.filesize, c.id,
                   c.fileId, c.nsHost, d.fileSystem, d.id AS dcId, d.status AS dcStatus
              FROM DiskCopy d, Castorfile c
             WHERE c.id = d.castorFile
               AND d.creationTime < getTime() - timeOut
               AND d.status IN (5, 6, 11) -- WAITFS, STAGEOUT, WAITFS_SCHEDULING
               AND NOT EXISTS (
                 SELECT 'x'
                   FROM SubRequest
                  WHERE castorFile = c.id
                    AND status IN (0, 1, 2, 3, 5, 6, 13, 14) -- all active
                    AND reqType NOT IN (37, 38))) LOOP -- ignore PrepareToPut, PrepareToUpdate
    IF (0 = f.fileSize) OR (f.dcStatus <> 6) THEN
      -- here we invalidate the diskcopy and let the GC run
      UPDATE DiskCopy SET status = 7  -- INVALID
       WHERE id = f.dcid;
      -- and we also fail the correspondent prepareToPut/Update request if it exists
      BEGIN
        SELECT /*+ INDEX(Subrequest I_Subrequest_Diskcopy)*/ id
          INTO srId   -- there can only be one outstanding PrepareToPut/Update, if any
          FROM SubRequest
         WHERE status = 6 AND diskCopy = f.dcid;
        archiveSubReq(srId, 9);  -- FAILED_FINISHED
      EXCEPTION WHEN NO_DATA_FOUND THEN
        NULL;
      END;
      INSERT INTO CleanupJobLog VALUES (f.fileId, f.nsHost, 0);
    ELSE
      -- here we issue a putDone
      -- context 2 : real putDone. Missing PPut requests are ignored.
      -- svcClass 0 since we don't know it. This will trigger a
      -- default behavior in the putDoneFunc
      putDoneFunc(f.id, f.fileSize, 2, 0);
      INSERT INTO CleanupJobLog VALUES (f.fileId, f.nsHost, 1);
    END IF;
  END LOOP;
  COMMIT;
END;
/

/* Runs cleanup operations */
CREATE OR REPLACE PROCEDURE cleanup AS
  t INTEGER;
BEGIN
  -- First perform some cleanup of old stuff:
  -- for each, read relevant timeout from configuration table
  t := TO_NUMBER(getConfigOption('cleaning', 'outOfDateStageOutDCsTimeout', '72'));
  deleteOutOfDateStageOutDCs(t*3600);
  t := TO_NUMBER(getConfigOption('cleaning', 'failedDCsTimeout', '72'));
  deleteFailedDiskCopies(t*3600);
END;
/


/* PL/SQL method used by the stager to log what it has been done by the cleanup job */
CREATE OR REPLACE PROCEDURE dumpCleanupLogs(jobLog OUT castorGC.JobLogEntry_Cur) AS
  unused NUMBER;
BEGIN
  SELECT fileid INTO unused FROM CleanupJobLog WHERE ROWNUM < 2;
  -- if we got here, we have something in the log table, let's lock it and dump it
  LOCK TABLE CleanupJobLog IN EXCLUSIVE MODE;
  OPEN jobLog FOR
    SELECT * FROM CleanupJobLog;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- nothing to do
  NULL;
END;
/


/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('HOUSEKEEPINGJOB',
                                'CLEANUPJOB',
                                'BULKCHECKFSBACKINPRODJOB',
                                'ACCOUNTINGJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every 20 minutes executing the deleteTerminatedRequests procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'houseKeepingJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN deleteTerminatedRequests(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=20',
      ENABLED         => TRUE,
      COMMENTS        => 'Cleaning of terminated requests');

  -- Create a db job to be run twice a day executing the cleanup procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'cleanupJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN cleanup(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=HOURLY; INTERVAL=12',
      ENABLED         => TRUE,
      COMMENTS        => 'Database maintenance');

  -- Create a db job to be run every 5 minutes executing the bulkCheckFSBackInProd procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'bulkCheckFSBackInProdJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN bulkCheckFSBackInProd(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'Bulk operation to processing filesystem state changes');

  -- Create a db job to be run every hour that generates the accounting information
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'accountingJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN 
                            DELETE FROM Accounting;
                            INSERT INTO Accounting (euid, fileSystem, nbBytes)
                              (SELECT owneruid, fileSystem, sum(diskCopySize)
                                 FROM DiskCopy
                                WHERE DiskCopy.status IN (0, 10)
                                  AND DiskCopy.owneruid IS NOT NULL
                                  AND DiskCopy.ownergid IS NOT NULL
                                GROUP BY owneruid, fileSystem);
                          END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=60',
      ENABLED         => TRUE,
      COMMENTS        => 'Generation of accounting information');
END;
/

/*******************************************************************
 * @(#)RCSfile: oracleDrain.sql,v  Revision: 1.7  Date: 2009/08/13 14:11:04  Author: itglp 
 * PL/SQL code for Draining FileSystems Logic
 *
 * Additional procedures modified to support the DrainingFileSystems
 * logic include: disk2DiskCopyDone, disk2DiskCopyFailed,
 * selectFiles2Delete, storeClusterStatus tr_DiskServer_Update and
 * tr_FileSystem_Update
 *
 * Note: the terms filesystem and mountpoint are used interchangeably
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* Function to convert seconds into a time string using the format:
 * DD-MON-YYYY HH24:MI:SS. If seconds is not defined then the current time
 * will be returned.
 */
CREATE OR REPLACE FUNCTION getTimeString
(seconds IN NUMBER DEFAULT NULL,
 format  IN VARCHAR2 DEFAULT 'DD-MON-YYYY HH24:MI:SS')
RETURN VARCHAR2 AS
BEGIN
  RETURN (to_char(to_date('01-JAN-1970', 'DD-MON-YYYY') +
          nvl(seconds, getTime()) / (60 * 60 * 24), format));
END;
/


/* Function used to convert bytes into a human readable string using powers
 * of 1000 (SI units). E.g 2000 = 2k.
 */
CREATE OR REPLACE FUNCTION sizeOfFmtSI(bytes IN NUMBER) RETURN VARCHAR2 AS
  unit VARCHAR2(2);
  num  NUMBER := bytes;
BEGIN
  IF bytes > 1000000000000000 THEN  -- PETA
    num := trunc(bytes / 1000000000000000, 2);
    unit := 'P';
  ELSIF bytes > 1000000000000 THEN  -- TERA
    num := trunc(bytes / 1000000000000, 2);
    unit := 'T';
  ELSIF bytes > 1000000000 THEN     -- GIGA
    num := trunc(bytes / 1000000000, 2);
    unit := 'G';
  ELSIF bytes > 1000000 THEN        -- MEGA
    num := trunc(bytes / 1000000, 2);
    unit := 'M';
  ELSIF bytes > 1000 THEN           -- KILO
    num := trunc(bytes / 1000, 2);
    unit := 'k';
  END IF;

  RETURN (to_char(num, 'FM99900.009') || unit);
END;
/


/* Function used to covert elapsed time in seconds into a human readable
 * string. For example, 3600 seconds = '00 01:00:00'
 */
CREATE OR REPLACE FUNCTION getInterval(startTime IN NUMBER, endTime IN NUMBER)
RETURN VARCHAR2 AS
  elapsed NUMBER;
  ret     VARCHAR2(2048);
BEGIN
  -- If the elapsed time is negative or greater than 99 days return an error
  elapsed := ceil(endTime) - floor(startTime);
  IF elapsed > 86400 * 99 OR elapsed < 0 THEN
    RETURN '## ##:##:##';
  END IF;
  -- Convert the elapsed time in seconds to an interval string
  -- e.g. +000000011 13:46:40.
  ret := rtrim(numtodsinterval(elapsed, 'SECOND'), 0);
  -- Remove the trailing '.' and leading '+'
  ret := substr(ret, 2, length(ret) - 2);
  -- Remove the leading '0'
  RETURN substr(ret, + 8);
END;
/


/* SQL statement for the creation of the DrainingOverview view */
CREATE OR REPLACE VIEW DrainingOverview
AS
  SELECT DS.name DiskServer,
         FS.mountPoint MountPoint,
         -- Determine the status of the filesystem being drained. If it's in a
         -- PRODUCTION status then the status of the diskserver is taken.
         decode(
           decode(FS.status, 0, DS.status, FS.status),
                  0, 'PRODUCTION',
                  1, 'DRAINING',
                  2, 'DISABLED', 'UNKNOWN') FileSystemStatus,
         DFS.username Username,
         DFS.machine Machine,
         DFS.comments Comments,
         getTimeString(ceil(DFS.creationTime)) Created,
         DFS.maxTransfers MaxTransfers,
         DFS.totalFiles TotalFiles,
         sizeOfFmtSI(DFS.totalBytes) TotalSize,
         -- Translate the fileMask value to string
         decode(DFS.fileMask,
                0, 'STAGED',
                1, 'CANBEMIGR',
                2, 'ALL', 'UNKNOWN') FileMask,
         -- Translate the autoDelete value to a string
         decode(DFS.autoDelete, 0, 'NO', 1, 'YES', 'UNKNOWN') AutoDelete,
         -- The target service class
         SVC.name SvcClass,
         -- Determine the status of the draining process. If the last update
         -- time is more than 60 minutes ago the process is considered to be
         -- STALLED.
         decode(DFS.status, 0, 'CREATED', 1, 'INITIALIZING', 2,
           decode(sign((getTime() - 3600) - DFS.lastUpdateTime),
                  -1, 'RUNNING', 'STALLED'),
                   3, 'INTERRUPTED',
                   4, 'FAILED',
                   5, 'COMPLETED',
                   6, 'DELETING',
                   7, 'RESTART', 'UNKNOWN') Status,
         nvl(DDCS.filesRemaining, 0) FilesRemaining,
         sizeOfFmtSI(nvl(DDCS.bytesRemaining, 0)) SizeRemaining,
         nvl(DDCS.running, 0) Running,
         nvl(DDCS.failed, 0) Failed,
         -- Calculate how long the process for draining the filesystem has been
         -- running. If the process is in a CREATED or INITIALIZING status
         -- 00 00:00:00 will be returned.
         decode(DFS.status, 0, '00 00:00:00', 1, '00 00:00:00',
           decode(sign(DFS.status - 3), -1,
             getInterval(DFS.startTime, gettime()),
               getInterval(DFS.startTime, DFS.lastUpdateTime))) RunTime,
         -- Calculate how far the process has gotten as a percentage of the data
         -- already transferred. If the process is in a CREATED, INITIALIZING,
         -- DELETING or RESTART status N/A will be returned.
         decode(DFS.status, 0, 'N/A', 1, 'N/A', 6, 'N/A', 7, 'N/A',
           decode(DFS.totalBytes, 0, '100%',
             concat(to_char(
               floor(((DFS.totalBytes - nvl(DDCS.bytesRemaining, 0)) /
                       DFS.totalBytes) * 100)), '%'))) Progress,
         -- Calculate the estimated time to completion in seconds if the process
         -- is in a RUNNING status and more than 10% of the data has already by
         -- transferred.
         decode(DFS.status, 2, decode(DFS.totalBytes, 0, 'N/A',
           decode(sign((getTime() - 3600) - DFS.lastUpdateTime), -1,
             decode(sign((((DFS.totalBytes - nvl(DDCS.bytesRemaining, 0)) /
                            DFS.totalBytes) * 100) - 10), -1, 'N/A',
               getInterval(0, trunc(DDCS.bytesRemaining / ((DFS.totalBytes -
                           nvl(DDCS.bytesRemaining, 0)) /
                           (getTime() - DFS.startTime))))), 'N/A')), 'N/A') ETC
    FROM (
      SELECT fileSystem,
             max(decode(status, 3, nbFiles, 0)) Running,
             max(decode(status, 4, nbFiles, 0)) Failed,
             sum(nbFiles) FilesRemaining,
             sum(totalFileSize) BytesRemaining
        FROM (
          SELECT fileSystem, status, count(*) nbFiles,
                 sum(fileSize) totalFileSize
            FROM DrainingDiskCopy
           GROUP BY fileSystem, status
        )
       GROUP BY fileSystem
    ) DDCS
   RIGHT JOIN DrainingFileSystem DFS
      ON DFS.fileSystem = DDCS.fileSystem
   INNER JOIN FileSystem FS
      ON DFS.fileSystem = FS.id
   INNER JOIN DiskServer DS
      ON FS.diskServer = DS.id
   INNER JOIN SvcClass SVC
      ON DFS.svcClass = SVC.id
   ORDER BY DS.name, FS.mountPoint;


/* SQL statement for the creation of the DrainingFailures view */
CREATE OR REPLACE VIEW DrainingFailures
AS
  SELECT DS.name DiskServer,
  	 FS.mountPoint MountPoint,
         regexp_replace
           ((FS.MountPoint || '/' || DC.path),'(/){2,}','/') Path,
         nvl(DDC.comments, 'Unknown') Comments
    FROM DrainingDiskCopy DDC, DiskCopy DC, FileSystem FS, DiskServer DS
   WHERE DDC.diskCopy = DC.id
     AND DDC.status = 4  -- FAILED
     AND DC.fileSystem = FS.id
     AND FS.diskServer = DS.id
   ORDER BY DS.name, FS.mountPoint, DC.path;



/* SQL statement for the removeFailedDrainingTransfers procedure */
CREATE OR REPLACE PROCEDURE removeFailedDrainingTransfers(fsId IN NUMBER)
AS
  CURSOR c IS
    SELECT STDCRR.id, SR.id subrequest, STDCRR.client
      FROM StageDiskCopyReplicaRequest STDCRR, DrainingDiskCopy DDC,
           SubRequest SR
     WHERE STDCRR.sourceDiskCopy = DDC.diskCopy
       AND SR.request = STDCRR.id
       AND decode(DDC.status, 4, DDC.status, NULL) = 4  -- FAILED
       AND SR.status = 9  -- FAILED_FINISHED
       AND DDC.fileSystem = fsId;
  reqIds    "numList";
  clientIds "numList";
  srIds     "numList";
BEGIN
  -- Remove failed transfers requests from the StageDiskCopyReplicaRequest
  -- table. If we do not do this files which failed due to "Maximum number of
  -- attempts exceeded" cannot be resubmitted to the system.
  -- (see getBestDiskCopyToReplicate)
  LOOP
    OPEN c;
    FETCH c BULK COLLECT INTO reqIds, srIds, clientIds LIMIT 10000;
    -- Break out of the loop when the cursor returns no results
    EXIT WHEN reqIds.count = 0;
    -- Delete data
    FORALL i IN clientIds.FIRST .. clientIds.LAST
      DELETE FROM Client WHERE id = clientIds(i);
    FORALL i IN srIds.FIRST .. srIds.LAST
      DELETE FROM SubRequest WHERE id = srIds(i);
    CLOSE c;
  END LOOP;
  -- Delete all data related to the filesystem from the draining diskcopy table
  DELETE FROM DrainingDiskCopy
   WHERE fileSystem = fsId;
END;
/


/* Procedure responsible for stopping the draining process for a diskserver
 * or filesystem. In no filesystem is specified then all filesystems
 * associated to the diskserver will be stopped.
 */
CREATE OR REPLACE PROCEDURE stopDraining(inNodeName   IN VARCHAR,
                                         inMountPoint IN VARCHAR2 DEFAULT NULL,
                                         inRestart    IN NUMBER DEFAULT 0)
AS
  fsIds  "numList";
  unused NUMBER;
  mntPnt VARCHAR2(2048);
BEGIN
  -- Check that the nodename and mountpoint input options are valid
  SELECT FileSystem.id BULK COLLECT INTO fsIds
    FROM FileSystem, DiskServer
   WHERE FileSystem.diskServer = DiskServer.id
     AND (FileSystem.mountPoint = inMountPoint
      OR inMountPoint IS NULL)
     AND DiskServer.name = inNodeName;
  IF fsIds.COUNT = 0 THEN
    IF inMountPoint IS NULL THEN
      raise_application_error
        (-20019, 'Diskserver does not exist or has no mountpoints');
    ELSE
      raise_application_error
        (-20015, 'Diskserver and mountpoint does not exist');
    END IF;
  END IF;
  -- Update the filesystem entries to DELETING or RESTART depending in the
  -- inRestart option. The drainManager job will take care of actually doing
  -- the work.
  FOR i IN fsIds.FIRST .. fsIds.LAST
  LOOP
    -- Check to see if the mountpoint and diskserver combination allow for a
    -- draining operation to begin.
    BEGIN
      SELECT mountPoint INTO mntPnt
        FROM FileSystem WHERE id = fsIds(i);
      -- If restarting verify that the diskserver and filesystem are in a 
      -- valid state, code copied from startDraining.
      IF inRestart = 1 THEN
        SELECT FS.diskPool INTO unused
          FROM FileSystem FS, DiskServer DS
         WHERE FS.diskServer = DS.id
           AND FS.id = fsIds(i)
           AND (FS.status = 1 OR DS.status = 1)
           AND FS.status != 2
           AND DS.status != 2;
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      raise_application_error
        (-20116, 'Mountpoint: Restart not possible '||mntPnt||' is not in a DRAINING state or diskserver is DISABLED');
    END;
    UPDATE DrainingFileSystem
       SET status = decode(inRestart, 1, 7, 6)
     WHERE fileSystem = fsIds(i);
  END LOOP;
END;
/


/* Procedure responsible for starting the draining process for a diskserver
 * or filesystem. If no filesystem is specified then all filesystems
 * associated to the diskserver will be add to the list of filesystems to be
 * drained.
 */
CREATE OR REPLACE
PROCEDURE startDraining(inNodeName     IN VARCHAR,
                        inMountPoint   IN VARCHAR DEFAULT NULL,
                        inSvcClass     IN VARCHAR DEFAULT NULL,
                        inFileMask     IN NUMBER DEFAULT 1,
                        inAutoDelete   IN NUMBER DEFAULT 0,
                        inMaxTransfers IN NUMBER DEFAULT 50,
                        inComments     IN VARCHAR DEFAULT 'N/A')
AS
  ret    NUMBER;
  fsIds  "numList";
  svcId  NUMBER;
  unused NUMBER;
  mntPnt VARCHAR2(2048);
  maxCommentLen NUMBER;
BEGIN
  -- Check that the comment is not too long.
  SELECT char_length INTO maxCommentLen
    FROM user_tab_columns
   WHERE table_name = 'DRAININGFILESYSTEM'
     AND column_name = 'COMMENTS';
  IF length(inComments) > maxCommentLen THEN
    raise_application_error
      (-20020, 'Comment exceeds maximum length of '|| maxCommentLen ||' bytes');
  END IF;
  -- Check that the nodename and mountpoint input options are valid.
  SELECT FileSystem.id BULK COLLECT INTO fsIds
    FROM FileSystem, DiskServer
   WHERE FileSystem.diskServer = Diskserver.id
     AND (FileSystem.mountPoint = inMountPoint
      OR inMountPoint IS NULL)
     AND DiskServer.name = inNodeName;
  IF fsIds.COUNT = 0 THEN
    IF inMountPoint IS NULL THEN
      raise_application_error
        (-20019, 'Diskserver does not exist or has no mountpoints');
    ELSE
      raise_application_error
        (-20015, 'Diskserver and mountpoint does not exist');
    END IF;
  END IF;
  -- Loop over the mountpoints extracted above and validate that the service
  -- class and status of the mountpoint are correct.
  FOR i IN fsIds.FIRST .. fsIds.LAST
  LOOP
    SELECT mountPoint INTO mntPnt
      FROM FileSystem WHERE id = fsIds(i);
    -- If no service class option is defined attempt to automatically find one.
    -- Note: this only works if the filesystem belongs to one and only one
    -- service class. If this is not the case the user must explicit provide
    -- one on the command line using the --svcclass option to the
    -- draindiskserver tool.
    IF inSvcClass IS NULL THEN
      BEGIN
        SELECT SvcClass.id INTO svcId
          FROM FileSystem, DiskPool2SvcClass, SvcClass
         WHERE FileSystem.diskPool = DiskPool2SvcClass.parent
           AND DiskPool2SvcClass.child = SvcClass.id
           AND FileSystem.id = fsIds(i);
      EXCEPTION
        WHEN TOO_MANY_ROWS THEN
          raise_application_error
            (-20101, 'Mountpoint: '||mntPnt||' belongs to multiple service classes, please specify which service class to use, using the --svcclass option');
        WHEN NO_DATA_FOUND THEN
          raise_application_error
            (-20120, 'Mountpoint: '||mntPnt||' does not belong to any service class');
      END;
    ELSE
      -- Check if the user supplied service class name exists
      ret := checkForValidSvcClass(inSvcClass, 0, 1);
      -- Check that the mountpoint belongs to the service class provided by the
      -- user. This check is necessary as we do not support the draining of a
      -- mountpoint to a service class which it is not already a member of.
      BEGIN
        SELECT SvcClass.id INTO SvcId
          FROM FileSystem, DiskPool2SvcClass, SvcClass
         WHERE FileSystem.diskPool = DiskPool2SvcClass.parent
           AND DiskPool2SvcClass.child = SvcClass.id
           AND FileSystem.id = fsIds(i)
           AND SvcClass.name = inSvcClass;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        raise_application_error
          (-20117, 'Mountpoint: '||mntPnt||' does not belong to the '''||inSvcClass||''' service class');
      END;
    END IF;
    -- Check to see if the mountpoint and diskserver combination allow for a
    -- draining operation to begin.
    BEGIN
      SELECT FS.diskPool INTO unused
        FROM FileSystem FS, DiskServer DS
       WHERE FS.diskServer = DS.id
         AND FS.id = fsIds(i)
         AND (FS.status = 1 OR DS.status = 1)
         AND FS.status != 2
         AND DS.status != 2;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      raise_application_error
        (-20116, 'Mountpoint: '||mntPnt||' is not in a DRAINING state or diskserver is DISABLED');
    END;
    -- Check to see if the mountpoint is already being drained. Note: we do not
    -- allow the resubmission of a mountpoint without a prior DELETION unless
    -- the previous process was not in a FAILED or COMPLETED state. An exception
    -- is made for the cases where no mountpoint was given and the whole box
    -- should be drained
    BEGIN
      SELECT fileSystem INTO unused
        FROM DrainingFileSystem
       WHERE fileSystem = fsIds(i)
         AND status NOT IN (4, 5);  -- FAILED, COMPLETED
      IF inMountPoint IS NOT NULL THEN
        raise_application_error
          (-20118, 'Mountpoint: '||mntPnt||' is already being drained');
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Cleanup
      removeFailedDrainingTransfers(fsIds(i));
      DELETE FROM DrainingFileSystem
       WHERE fileSystem = fsIds(i);
      -- Insert the new mountpoint into the list of those to be drained. The
      -- drainManager job will later pick it up and start the draining process.
      INSERT INTO DrainingFileSystem
        (userName, machine, creationTime, fileSystem, svcClass, autoDelete,
         fileMask, maxTransfers, comments)
      VALUES
        (-- For the time being the draindiskserver command is distributed with
         -- the castor-dbtools package and uses the /etc/castor/ORASTAGERCONFIG
         -- password to connect to the database. As the file is only readable
         -- by the root user and the st group, the OS_USER will either be root
         -- or stage. This is not very interesting!!
         sys_context('USERENV', 'OS_USER'),
         sys_context('USERENV', 'HOST'),
         getTime(), fsIds(i), svcId, inAutoDelete, inFileMask, inMaxTransfers,
         inComments);
    END;
  END LOOP;
END;
/


/* Procedure responsible for processing the snapshot of files that need to be
 * replicated in order to drain a filesystem.
 */
CREATE OR REPLACE
PROCEDURE drainFileSystem(fsId IN NUMBER)
AS
  unused     NUMBER;
  res        NUMBER;
  dcId       NUMBER;
  cfId       NUMBER;
  svcId      NUMBER;
  ouid       NUMBER;
  ogid       NUMBER;
  autoDelete NUMBER;
  fileMask   NUMBER;
BEGIN
  -- As this procedure is called recursively we release the previous calls
  -- locks. This prevents the procedure from locking too many castorfile
  -- entries which could result in service degradation.
  COMMIT;
  -- Update the filesystems entry in the DrainingFileSystem table to indicate
  -- that activity is ongoing. If no entry exist then the filesystem is not
  -- under the control of the draining logic.
  svcId := 0;
  UPDATE DrainingFileSystem
     SET lastUpdateTime = getTime()
   WHERE fileSystem = fsId
     AND status = 2  -- RUNNING
  RETURNING svcclass, autoDelete, fileMask INTO svcId, autoDelete, fileMask;
  IF svcId = 0 THEN
    RETURN;  -- Do nothing
  END IF;
  -- Extract the next diskcopy to be processed.
  dcId := 0;
  UPDATE DrainingDiskCopy
     SET status = 2  -- PROCESSING
   WHERE diskCopy = (
     SELECT diskCopy FROM (
       SELECT DDC.diskCopy
         FROM DrainingDiskCopy DDC
        WHERE DDC.fileSystem = fsId
          AND DDC.status IN (0, 1)  -- CREATED, RESTARTED
        ORDER BY DDC.priority DESC, DDC.creationTime ASC)
     WHERE rownum < 2)
  RETURNING diskCopy INTO dcId;
  IF dcId = 0 THEN
    -- If there are no transfers outstanding related to the draining process we
    -- can update the filesystem entry in the DrainingFileSystem table for the
    -- last time.
    UPDATE DrainingFileSystem
       SET status = decode((SELECT count(*)
                              FROM DrainingDiskCopy
                             WHERE status = 4  -- FAILED
                               AND fileSystem = fsId), 0, 5, 4),
           lastUpdateTime = getTime()
     WHERE fileSystem = fsId
       -- Check to make sure there are no transfers outstanding.
       AND (SELECT count(*) FROM DrainingDiskCopy
             WHERE status = 3  -- RUNNING
               AND fileSystem = fsId) = 0;
    RETURN;  -- Out of work!
  END IF;
  -- The DrainingDiskCopy table essentially contains a snapshot of the work
  -- that needs to be done at the time the draining process was initiated for a
  -- filesystem. As a result, the diskcopy id previously extracted may no longer
  -- be valid as the snapshot information is outdated. Here we simply verify
  -- that the diskcopy is still what we expected.
  BEGIN
    -- Determine the castorfile id
    SELECT castorFile INTO cfId FROM DiskCopy WHERE id = dcId;
    -- Lock the castorfile
    SELECT id INTO cfId FROM CastorFile
     WHERE id = cfId FOR UPDATE;
    -- Check that the status of the diskcopy matches what was specified in the
    -- filemask for the filesystem. If the diskcopy is not in the expected
    -- status then it is no longer a candidate to be replicated.
    SELECT ownerUid, ownerGid INTO ouid, ogid
      FROM DiskCopy
     WHERE id = dcId
       AND ((status = 0        AND fileMask = 0)    -- STAGED
        OR  (status = 10       AND fileMask = 1)    -- CANBEMIGR
        OR  (status IN (0, 10) AND fileMask = 2));  -- ALL
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- The diskcopy no longer exists or is not of interest so we delete it from
    -- the snapshot of files to be processed.
    DELETE FROM DrainingDiskCopy
      WHERE diskCopy = dcId AND fileSystem = fsId;
    drainFileSystem(fsId);
    RETURN;  -- No replication required
  END;
  -- Just because the file was listed in the snapshot doesn't mean that it must
  -- be replicated! Here we check to see if the file is available on another
  -- diskserver in the target service class and if enough copies of the file
  -- are available.
  --
  -- The decode logic used here essentially states:
  --   If replication on close is enabled and there are enough copies available
  --   to satisfy the maxReplicaNb on the service class, then do not replicate.
  -- Otherwise,
  --   If replication on close is disabled and a copy exists elsewhere, then do
  --   do not replicate. All other cases result in triggering replication.
  --
  -- Notes:
  -- - The check to see if we have enough copies online when replication on
  --   close is enabled will effectively rebalance the pool!
  SELECT decode(replicateOnClose, 1,
         decode((available - maxReplicaNb) -
                 abs(available - maxReplicaNb), 0, 0, 1),
         decode(sign(available), 1, 0, 1)) INTO res
    FROM (
      SELECT count(*) available
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
       WHERE DiskCopy.fileSystem = FileSystem.id
         AND DiskCopy.castorFile = cfId
         AND DiskCopy.id != dcId
         AND DiskCopy.status IN (0, 10)  -- STAGED, CANBEMIGR
         AND FileSystem.diskPool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = svcId
         AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
         AND FileSystem.diskServer = DiskServer.id
         AND DiskServer.status IN (0, 1)  -- PRODUCTION, DRAINING
     ) results, SvcClass
   WHERE SvcClass.id = svcId;
  IF res = 0 THEN
    -- No replication is required, there are enough copies of the file to
    -- satisfy the requirements of the target service class.
    IF autoDelete = 1 THEN
      -- Invalidate the diskcopy so that it can be garbage collected.
      UPDATE DiskCopy
         SET status = 7,  -- INVALID
             gctype = 3   -- Draining filesystem
       WHERE id = dcId AND fileSystem = fsId;
    END IF;
    -- Delete the diskcopy from the snapshot as no action is required.
    DELETE FROM DrainingDiskCopy
     WHERE diskCopy = dcId AND fileSystem = fsId;
    drainFileSystem(fsId);
    RETURN;
  END IF;
  -- Check to see if there is already an outstanding request to replicate the
  -- castorfile to the target service class. If so, we wait on that replication
  -- to complete. This avoids having multiple requests to replicate the same
  -- file to the same target service class multiple times.
  BEGIN
    SELECT /*+ INDEX(StageDiskCopyReplicaRequest I_StageDiskCopyReplic_DestDC) */ DiskCopy.id INTO res
      FROM DiskCopy, StageDiskCopyReplicaRequest
     WHERE StageDiskCopyReplicaRequest.destDiskCopy = DiskCopy.id
       AND StageDiskCopyReplicaRequest.svcclass = svcId
       AND DiskCopy.castorFile = cfId
       AND DiskCopy.status = 1  -- WAITDISK2DISKCOPY
       AND rownum < 2;
    IF res > 0 THEN
      -- Wait on another replication to complete.
      UPDATE DrainingDiskCopy
         SET status = 3,  -- WAITD2D
             parent = res
       WHERE diskCopy = dcId AND fileSystem = fsId;
       RETURN;
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL;  -- No pending replications running
  END;
  -- If we have attempted to replicate the file more than 10 times already then
  -- give up! The error will be exposed later to an administrator for manual
  -- corrective action.
  SELECT /*+ INDEX(R I_StageDiskCopyReplic_SourceDC) */ count(*) INTO res
    FROM StageDiskCopyReplicaRequest R, SubRequest
   WHERE SubRequest.request = R.id
     AND R.sourceDiskCopy = dcId
     AND SubRequest.status = 9; -- FAILED_FINISHED
  IF res >= 10 THEN
    UPDATE DrainingDiskCopy
       SET status = 4,
           comments = 'Maximum number of attempts exceeded'
     WHERE diskCopy = dcId AND fileSystem = fsId;
    drainFileSystem(fsId);
    RETURN;  -- Try again
  END IF;
  -- Trigger a replication request for the file
  createDiskCopyReplicaRequest(0, dcId, svcId, svcId, ouid, ogid);
  -- Update the status of the file
  UPDATE DrainingDiskCopy SET status = 3  -- WAITD2D
   WHERE diskCopy = dcId AND fileSystem = fsId;
END;
/


/* SQL statement for DBMS_SCHEDULER job creation */
BEGIN
  -- Remove jobs related to the draining logic before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('DRAINMANAGERJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create the drain manager job to be executed every minute
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'drainManagerJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN DrainManager(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 5/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Database job to manage the draining process');
END;
/


/* Procedure responsible for managing the draining process */
CREATE OR REPLACE PROCEDURE drainManager AS
  fsIds "numList";
  dcIds "numList";
BEGIN
  -- Delete (and restart if necessary) the filesystems which are:
  --  A) in a DELETING state and have no transfers pending in the scheduler.
  --  B) are COMPLETED and older than 7 days.
  FOR A IN (SELECT fileSystem, status FROM DrainingFileSystem
             WHERE (NOT EXISTS
               (SELECT 'x' FROM DrainingDiskCopy
                 WHERE fileSystem = DrainingFileSystem.fileSystem
                   AND status = 3)  -- WAITD2D
               AND status IN (6, 7))  -- DELETING, RESTART
                OR (status = 5 AND lastUpdateTime < getTime() - (7 * 86400)))
  LOOP
    -- If the status is RESTART, reset the draining filesystem entry to
    -- its default values and set its status to CREATED, otherwise delete it!
    removeFailedDrainingTransfers(a.fileSystem);
    IF a.status = 7 THEN
      UPDATE DrainingFileSystem
         SET creationTime = getTime(), startTime = 0, lastUpdateTime = 0,
             status = 0, totalFiles = 0, totalBytes = 0
       WHERE fileSystem = a.fileSystem;        
    ELSE
      DELETE FROM DrainingFileSystem
       WHERE fileSystem = a.fileSystem;
    END IF;
  END LOOP;
  -- If the draining filesystem is in a DELETING state and there are still
  -- running transfers for which the diskcopy they were waiting on has
  -- disappeared delete them. SR #112993
  SELECT DDC.diskCopy BULK COLLECT INTO dcIds
    FROM DrainingDiskCopy DDC, DrainingFileSystem DFS
   WHERE DDC.fileSystem = DFS.fileSystem
     AND DDC.status = 3  -- WAITD2D
     AND DFS.status = 6  -- DELETING
     AND NOT EXISTS
       (SELECT 'x' FROM DiskCopy WHERE id = DDC.diskCopy);
  DELETE FROM DrainingDiskCopy
   WHERE status = 3
     AND diskCopy IN
       (SELECT /*+ CARDINALITY(dcIdTable 5) */ *
          FROM TABLE (dcIds) dcIdTable);
  -- Process filesystems which in a CREATED state
  UPDATE DrainingFileSystem
     SET status = 1  -- INITIALIZING
   WHERE status = 0  -- CREATED
  RETURNING fileSystem BULK COLLECT INTO fsIds;
  -- Commit, this isn't really necessary but its done so that the user gets
  -- feedback when listing the filesystems which are being drained.
  COMMIT;
  IF fsIds.COUNT = 0 THEN
    -- Shrink the DrainingDiskCopy to keep the number of blocks small
    EXECUTE IMMEDIATE 'ALTER TABLE DrainingDiskCopy SHRINK SPACE CASCADE';
    RETURN;  -- No results
  END IF;
  -- Create the DrainingDiskCopy snapshot
  INSERT /*+ APPEND */ INTO DrainingDiskCopy
    (fileSystem, diskCopy, creationTime, priority, fileSize)
      (SELECT /*+ index(DC I_DiskCopy_FileSystem) */
              DC.fileSystem, DC.id, DC.creationTime, DC.status,
              DC.diskCopySize
         FROM DiskCopy DC, DrainingFileSystem DFS
        WHERE DFS.fileSystem = DC.fileSystem
          AND DFS.fileSystem IN
            (SELECT /*+ CARDINALITY(fsIdTable 5) */ *
               FROM TABLE (fsIds) fsIdTable)
          AND ((DC.status = 0         AND DFS.fileMask = 0)    -- STAGED
           OR  (DC.status = decode(DC.status, 10, DC.status, NULL)
                AND DFS.fileMask = 1)                          -- CANBEMIGR
           OR  (DC.status IN (0, 10)  AND DFS.fileMask = 2))); -- ALL
  -- Regenerate the statistics for the DrainingDiskCopy table
  DBMS_STATS.GATHER_TABLE_STATS
    (OWNNAME          => sys_context('USERENV', 'CURRENT_SCHEMA'),
     TABNAME          =>'DRAININGDISKCOPY',
     ESTIMATE_PERCENT => 100,
     METHOD_OPT       => 'FOR ALL COLUMNS SIZE 100',
     NO_INVALIDATE    => FALSE,
     FORCE            => TRUE);
  -- Update the DrainingFileSystem counters
  FOR a IN (SELECT fileSystem, count(*) files, sum(DDC.fileSize) bytes
              FROM DrainingDiskCopy DDC
             WHERE DDC.fileSystem IN
               (SELECT /*+ CARDINALITY(fsIdTable 5) */ *
                  FROM TABLE (fsIds) fsIdTable)
               AND DDC.status = 0  -- CREATED
             GROUP BY DDC.fileSystem)
  LOOP
    UPDATE DrainingFileSystem
       SET totalFiles = a.files,
           totalBytes = a.bytes
     WHERE fileSystem = a.fileSystem;
  END LOOP;
  -- Update the filesystem entries to RUNNING
  UPDATE DrainingFileSystem
     SET status = 2,  -- RUNNING
         startTime = getTime(),
         lastUpdateTime = getTime()
   WHERE fileSystem IN
     (SELECT /*+ CARDINALITY(fsIdTable 5) */ *
        FROM TABLE (fsIds) fsIdTable);
  -- Start the process of draining the filesystems. For an explanation of the
  -- query refer to: "Creating N Copies of a Row":
  -- http://forums.oracle.com/forums/message.jspa?messageID=1953433#1953433
  FOR a IN ( SELECT fileSystem
               FROM DrainingFileSystem
              WHERE fileSystem IN
                (SELECT /*+ CARDINALITY(fsIdTable 5) */ *
                   FROM TABLE (fsIds) fsIdTable)
            CONNECT BY CONNECT_BY_ROOT fileSystem = fileSystem
                AND LEVEL <= maxTransfers
                AND PRIOR sys_guid() IS NOT NULL
              ORDER BY totalBytes ASC, fileSystem)
  LOOP
    drainFileSystem(a.fileSystem);
  END LOOP;
END;
/
/*******************************************************************
 *
 * @(#)RCSfile: oracleDebug.sql,v  Revision: 1.14  Date: 2009/08/13 13:34:16  Author: itglp 
 *
 * Some SQL code to ease support and debugging
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* PL/SQL declaration for the castorDebug package */
CREATE OR REPLACE PACKAGE castorDebug AS
  TYPE DiskCopyDebug_typ IS RECORD (
    id INTEGER,
    diskPool VARCHAR2(2048),
    location VARCHAR2(2048),
    available CHAR(1),
    status NUMBER,
    creationtime VARCHAR2(2048),
    diskCopySize NUMBER,
    castorFileSize NUMBER,
    gcWeight NUMBER);
  TYPE DiskCopyDebug IS TABLE OF DiskCopyDebug_typ;
  TYPE SubRequestDebug IS TABLE OF SubRequest%ROWTYPE;
  TYPE MigrationJobDebug IS TABLE OF MigrationJob%ROWTYPE;
  TYPE RequestDebug_typ IS RECORD (
    creationtime VARCHAR2(2048),
    SubReqId NUMBER,
    SubReqParentId NUMBER,
    Status NUMBER,
    username VARCHAR2(2048),
    machine VARCHAR2(2048),
    svcClassName VARCHAR2(2048),
    ReqId NUMBER,
    ReqType VARCHAR2(20));
  TYPE RequestDebug IS TABLE OF RequestDebug_typ;
  TYPE RecallJobDebug_typ IS RECORD (
    RJId NUMBER,
    RJStatus NUMBER,
    RJMissing NUMBER,
    RJNbRetry NUMBER,
    SegId NUMBER,
    SegStatus NUMBER,
    SegErrCode NUMBER,
    VID VARCHAR2(2048),
    tpMode NUMBER,
    TapeStatus NUMBER,
    SegErr VARCHAR2(2048));
  TYPE RecallJobDebug IS TABLE OF RecallJobDebug_typ;
END;
/


/* Return the castor file id associated with the reference number */
CREATE OR REPLACE FUNCTION getCF(ref NUMBER) RETURN NUMBER AS
  t NUMBER;
  cfId NUMBER;
BEGIN
  SELECT id INTO cfId FROM CastorFile WHERE id = ref OR fileId = ref;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- DiskCopy?
BEGIN
  SELECT castorFile INTO cfId FROM DiskCopy WHERE id = ref;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- SubRequest?
BEGIN
  SELECT castorFile INTO cfId FROM SubRequest WHERE id = ref;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- RecallJob?
BEGIN
  SELECT castorFile INTO cfId FROM RecallJob WHERE id = ref;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- MigrationJob?
BEGIN
  SELECT castorFile INTO cfId FROM MigrationJob WHERE id = ref;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- Segment?
BEGIN
  SELECT castorFile INTO cfId FROM RecallJob, Segment
   WHERE Segment.id = ref AND RecallJob.id = Segment.copy;
  RETURN cfId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- nothing found
  RAISE_APPLICATION_ERROR (-20000, 'Could not find any CastorFile, SubRequest, DiskCopy, MigrationJob, RecallJob or Segment with id = ' || ref);
END; END; END; END; END; END;
/


/* Get the diskcopys associated with the reference number */
CREATE OR REPLACE FUNCTION getDCs(ref number) RETURN castorDebug.DiskCopyDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT DiskCopy.id,
                   DiskPool.name AS diskpool,
                   DiskServer.name || ':' || FileSystem.mountPoint || DiskCopy.path AS location,
                   decode(DiskServer.status, 2, 'N', decode(FileSystem.status, 2, 'N', 'Y')) AS available,
                   DiskCopy.status AS status,
                   getTimeString(DiskCopy.creationtime) AS creationtime,
                   DiskCopy.diskCopySize AS diskcopysize,
                   CastorFile.fileSize AS castorfilesize,
                   trunc(DiskCopy.gcWeight, 2) AS gcweight
              FROM DiskCopy, FileSystem, DiskServer, DiskPool, CastorFile
             WHERE DiskCopy.fileSystem = FileSystem.id(+)
               AND FileSystem.diskServer = diskServer.id(+)
               AND DiskPool.id(+) = fileSystem.diskPool
               AND DiskCopy.castorFile = getCF(ref)
               AND DiskCopy.castorFile = CastorFile.id) LOOP
     PIPE ROW(d);
  END LOOP;
END;
/


/* Get the recalljobs, segments and tapes associated with the reference number */
CREATE OR REPLACE FUNCTION getRJs(ref number) RETURN castorDebug.RecallJobDebug PIPELINED AS
BEGIN
  FOR t IN (SELECT RecallJob.id AS RJId, RecallJob.status AS RJStatus,
                   RecallJob.missingCopies AS RJmissing, RecallJob.nbRetry AS RJNbRetry,
                   Segment.Id, Segment.status AS SegStatus, Segment.errorCode AS SegErrCode,
                   Tape.vid AS VID, Tape.tpMode AS tpMode, Tape.Status AS TapeStatus,
                   Segment.errMsgTxt AS SegErr
              FROM RecallJob, Segment, Tape
             WHERE RecallJob.id = Segment.copy(+)
               AND Segment.tape = Tape.id(+)
               AND RecallJob.castorfile = getCF(ref)) LOOP
     PIPE ROW(t);
  END LOOP;
END;
/


/* Get the migration jobs associated with the reference number */
CREATE OR REPLACE FUNCTION getMJs(ref number) RETURN castorDebug.MigrationJobDebug PIPELINED AS
BEGIN
  FOR t IN (SELECT *
              FROM MigrationJob
             WHERE castorfile = getCF(ref)) LOOP
     PIPE ROW(t);
  END LOOP;
END;
/


/* Get the subrequests associated with the reference number. */
CREATE OR REPLACE FUNCTION getSRs(ref number) RETURN castorDebug.SubRequestDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT * FROM SubRequest WHERE castorfile = getCF(ref)) LOOP
     PIPE ROW(d);
  END LOOP;
END;
/


/* Get the requests associated with the reference number. */
CREATE OR REPLACE FUNCTION getRs(ref number) RETURN castorDebug.RequestDebug PIPELINED AS
BEGIN
  FOR d IN (SELECT getTimeString(creationtime) AS creationtime,
                   SubRequest.id AS SubReqId, SubRequest.parent AS SubReqParentId, SubRequest.Status,
                   username, machine, svcClassName, Request.id AS ReqId, Request.type AS ReqType
              FROM SubRequest,
                    (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, username, machine, svcClassName, 'Get' AS type FROM StageGetRequest UNION ALL
                     SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, username, machine, svcClassName, 'PGet' AS type FROM StagePrepareToGetRequest UNION ALL
                     SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ id, username, machine, svcClassName, 'Put' AS type FROM StagePutRequest UNION ALL
                     SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id, username, machine, svcClassName, 'PPut' AS type FROM StagePrepareToPutRequest UNION ALL
                     SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, username, machine, svcClassName, 'Upd' AS type FROM StageUpdateRequest UNION ALL
                     SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, username, machine, svcClassName, 'PUpd' AS type FROM StagePrepareToUpdateRequest UNION ALL
                     SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, username, machine, svcClassName, 'Repack' AS type FROM StageRepackRequest UNION ALL
                     SELECT /*+ INDEX(StagePutDoneRequest PK_StagePutDoneRequest_Id) */ id, username, machine, svcClassName, 'PutDone' AS type FROM StagePutDoneRequest UNION ALL
                     SELECT /*+ INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */ id, username, machine, svcClassName, 'DCRepl' AS type FROM StageDiskCopyReplicaRequest UNION ALL
                     SELECT /*+ INDEX(SetFileGCWeight PK_SetFileGCWeight_Id) */ id, username, machine, svcClassName, 'SetGCW' AS type FROM SetFileGCWeight) Request
             WHERE castorfile = getCF(ref)
               AND Request.id = SubRequest.request) LOOP
     PIPE ROW(d);
  END LOOP;
END;
/
/*******************************************************************
 * @(#)RCSfile: oracleMonitoring.sql,v  Revision: 1.8  Date: 2009/07/05 13:46:14  Author: waldron 
 * PL/SQL code for stager monitoring
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* CastorMon Package Specification */
CREATE OR REPLACE PACKAGE CastorMon AS

  /**
   * This procedure generates statistics on the status of all diskcopies. The
   * results are grouped by DiskServer, MountPoint and DiskCopy status.
   *
   * @param interval The frequency at which the data is refreshed.
   */
  PROCEDURE diskCopyStats(interval IN NUMBER);

  /**
   * This procedure generates statistics on files which are waiting to be
   * migrated to tape.
   * @param interval The frequency at which the data is refreshed.
   */
  PROCEDURE waitTapeMigrationStats(interval IN NUMBER);

  /**
   * This procedure generates statistics on files which are waiting to be
   * recalled from tape.
   * @param interval The frequency at which the data is refreshed.
   */
  PROCEDURE waitTapeRecallStats(interval IN NUMBER);

END CastorMon;
/


/* CastorMon Package Body */
CREATE OR REPLACE PACKAGE BODY CastorMon AS

  /**
   * PL/SQL method implementing diskCopyStats
   * See the castorMon package specification for documentation.
   */
  PROCEDURE diskCopyStats(interval IN NUMBER) AS
  BEGIN
    -- Truncate the MonDiskCopyStats table
    EXECUTE IMMEDIATE 'DELETE FROM MonDiskCopyStats';
    -- Populate the MonDiskCopyStats table
    INSERT INTO MonDiskCopyStats
      (timestamp, interval, diskServer, mountPoint, dsStatus, fsStatus,
       available, status, totalFileSize, nbFiles)
      -- Gather data
      SELECT sysdate timestamp, interval, a.name diskServer, a.mountPoint,
             a.dsStatus, a.fsStatus, a.available, a.statusName status,
             nvl(b.totalFileSize, 0) totalFileSize, nvl(b.nbFiles, 0) nbFiles
        FROM (
          -- Produce a matrix of all possible diskservers, filesystems and
          -- diskcopy states.
          SELECT DiskServer.name, FileSystem.mountPoint, FileSystem.id,
                 (SELECT statusName FROM ObjStatus
                   WHERE object = 'DiskServer'
                     AND field = 'status'
                     AND statusCode = DiskServer.status) dsStatus,
                 (SELECT statusName FROM ObjStatus
                   WHERE object = 'FileSystem'
                     AND field = 'status'
                     AND statusCode = FileSystem.status) fsStatus,
                 decode(DiskServer.status, 2, 'N',
                   decode(FileSystem.status, 2, 'N', 'Y')) available,
                 ObjStatus.statusName
            FROM DiskServer, FileSystem, ObjStatus
           WHERE FileSystem.diskServer = DiskServer.id
             AND ObjStatus.object(+) = 'DiskCopy'
             AND ObjStatus.field = 'status'
             AND ObjStatus.statusCode IN (0, 4, 5, 6, 7, 9, 10, 11)) a
        -- Attach the aggregation information for all filesystems to the results
        -- extracted above.
        LEFT JOIN (
          SELECT DiskCopy.fileSystem, ObjStatus.statusName,
                 sum(DiskCopy.diskCopySize) totalFileSize, count(*) nbFiles
            FROM DiskCopy, ObjStatus
           WHERE DiskCopy.status = ObjStatus.statusCode
             AND ObjStatus.object = 'DiskCopy'
             AND ObjStatus.field = 'status'
             AND DiskCopy.status IN (0, 4, 5, 6, 7, 9, 10, 11)
           GROUP BY DiskCopy.fileSystem, ObjStatus.statusName) b
          ON (a.id = b.fileSystem AND a.statusName = b.statusName)
       ORDER BY a.name, a.mountPoint, a.statusName;
  END diskCopyStats;


  /**
   * PL/SQL method implementing waitTapeMigrationStats
   * See the castorMon package specification for documentation.
   */
  PROCEDURE waitTapeMigrationStats(interval IN NUMBER) AS
  BEGIN
    -- Truncate the MonWaitTapeMigrationStats table
    EXECUTE IMMEDIATE 'DELETE FROM MonWaitTapeMigrationStats';
    -- Populate the MonWaitTapeMigrationStats table
    INSERT INTO MonWaitTapeMigrationStats
      (timestamp, interval, svcClass, status, minWaitTime, maxWaitTime,
       avgWaitTime, minFileSize, maxFileSize, avgFileSize, bin_LT_1,
       bin_1_To_6, bin_6_To_12, bin_12_To_24, bin_24_To_48, bin_GT_48,
       totalFileSize, nbFiles)
      -- Gather data
      SELECT sysdate timestamp, interval, b.svcClass, nvl(b.status, '-') status,
             -- File age statistics
             round(nvl(min(a.waitTime), 0), 0) minWaitTime,
             round(nvl(max(a.waitTime), 0), 0) maxWaitTime,
             round(nvl(avg(a.waitTime), 0), 0) avgWaitTime,
             -- File size statistics
             round(nvl(min(a.diskCopySize), 0), 0) minFileSize,
             round(nvl(max(a.diskCopySize), 0), 0) maxFileSize,
             round(nvl(avg(a.diskCopySize), 0), 0) avgFileSize,
             -- Wait time stats
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 1     AND 3600
                      THEN 1 ELSE 0 END) BIN_LT_1,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 3600  AND 21600
                      THEN 1 ELSE 0 END) BIN_1_TO_6,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 21600 AND 43200
                      THEN 1 ELSE 0 END) BIN_6_TO_12,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 43200 AND 86400
                      THEN 1 ELSE 0 END) BIN_12_TO_24,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 86400 AND 172800
                      THEN 1 ELSE 0 END) BIN_24_TO_48,
             sum(CASE WHEN nvl(a.waitTime, 0) > 172800
                      THEN 1 ELSE 0 END) BIN_GT_48,
             -- Summary values
             nvl(sum(a.diskCopySize), 0) totalFileSize, nvl(sum(a.found), 0) nbFiles
        FROM (
          -- Determine the service class of all tapecopies and their associated
          -- status (in a summarized form)
          -- In castor 2.1.11:
          -- Output is 'Pending' for 'PENDING' or 'TOBEMIGRATED',
          --           'Selected' for 'WAITINSTREAMS' or 'SELECTED'
          --           'Failed' for 'FAILED'
          -- Other states are ignored ('TOBERECALLED', 'STAGED', 'WAITPOLICY', 'REC_RETRY', 'MIG_RETRY')
          -- New version (2.1.12) will be:
          --          'Pending' for 'PENDING'
          --          'Selected' for 'SELECTED'
          --          'Failed' for 'FAILED'
          --          'MIGRATED' and 'RETRY' will be ignored.
          SELECT svcClass, status, waitTime, diskCopySize, found FROM (
            SELECT /*+ USE_NL(MigrationJob DiskCopy CastorFile) */
                   SvcClass.name svcClass,
                   decode(MigrationJob.status, tconst.MIGRATIONJOB_PENDING, 'PENDING',
                      decode(MigrationJob.status, tconst.MIGRATIONJOB_SELECTED, 'SELECTED', 
                          decode(MigrationJob.status, tconst.MIGRATIONJOB_FAILED, 'FAILED','UNKNOWN')
                       )
                   ) status,
                   (getTime() - DiskCopy.creationTime) waitTime,
                   DiskCopy.diskCopySize, 1 found, RANK() OVER (PARTITION BY
                   DiskCopy.castorFile ORDER BY DiskCopy.id ASC) rank
              FROM DiskCopy, CastorFile, MigrationJob, SvcClass
             WHERE DiskCopy.castorFile = CastorFile.id
               AND CastorFile.id = MigrationJob.castorFile
               AND CastorFile.svcClass = SvcClass.id
               AND decode(DiskCopy.status, 10, DiskCopy.status, NULL) = 10  -- CANBEMIGR
               AND MigrationJob.status IN (tconst.MIGRATIONJOB_PENDING, 
                                           tconst.MIGRATIONJOB_SELECTED,
                                           tconst.MIGRATIONJOB_FAILED))
           WHERE rank = 1
        ) a
        -- Attach a list of all service classes and possible states (PENDING,
        -- SELECTED) to the results above.
        RIGHT JOIN (
          SELECT SvcClass.name svcClass, a.status
            FROM SvcClass,
             (SELECT 'PENDING'  status FROM Dual UNION ALL
              SELECT 'SELECTED' status FROM Dual UNION ALL
              SELECT 'FAILED'   status FROM Dual) a) b
           ON (a.svcClass = b.svcClass AND a.status = b.status)
       GROUP BY GROUPING SETS (b.svcClass, b.status), (b.svcClass)
       ORDER BY b.svcClass, b.status;
  END waitTapeMigrationStats;


  /**
   * PL/SQL method implementing waitTapeRecallStats
   * See the castorMon package specification for documentation.
   */
  PROCEDURE waitTapeRecallStats(interval IN NUMBER) AS
  BEGIN
    -- Truncate the MonWaitTapeRecallStats table
    EXECUTE IMMEDIATE 'DELETE FROM MonWaitTapeRecallStats';
    -- Populate the MonWaitTapeRecallStats table
    INSERT INTO MonWaitTapeRecallStats
      (timestamp, interval, svcClass, minWaitTime, maxWaitTime, avgWaitTime,
       minFileSize, maxFileSize, avgFileSize, bin_LT_1, bin_1_To_6, bin_6_To_12,
       bin_12_To_24, bin_24_To_48, bin_GT_48, totalFileSize, nbFiles)
      -- Gather data
      SELECT sysdate timestamp, interval, SvcClass.name svcClass,
             -- File age statistics
             round(nvl(min(a.waitTime), 0), 0) minWaitTime,
             round(nvl(max(a.waitTime), 0), 0) maxWaitTime,
             round(nvl(avg(a.waitTime), 0), 0) avgWaitTime,
             -- File size statistics
             round(nvl(min(a.fileSize), 0), 0) minFileSize,
             round(nvl(max(a.fileSize), 0), 0) maxFileSize,
             round(nvl(avg(a.fileSize), 0), 0) avgFileSize,
             -- Wait time stats
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 1     AND 3600
                      THEN 1 ELSE 0 END) BIN_LT_1,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 3600  AND 21600
                      THEN 1 ELSE 0 END) BIN_1_TO_6,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 21600 AND 43200
                      THEN 1 ELSE 0 END) BIN_6_TO_12,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 43200 AND 86400
                      THEN 1 ELSE 0 END) BIN_12_TO_24,
             sum(CASE WHEN nvl(a.waitTime, 0) BETWEEN 86400 AND 172800
                      THEN 1 ELSE 0 END) BIN_24_TO_48,
             sum(CASE WHEN nvl(a.waitTime, 0) > 172800
                      THEN 1 ELSE 0 END) BIN_GT_48,
             -- Summary values
             nvl(sum(a.fileSize), 0) totalFileSize, nvl(sum(a.found), 0) nbFiles
        FROM (
          -- Determine the list of ongoing a. We need to join with the
          -- SubRequest and Request tables here to work out the service class
          -- as the DiskCopy filesystem pointer is 0 until the file is
          -- successfully recalled.
          SELECT Request.svcClassName svcClass, CastorFile.fileSize,
                 (getTime() - DiskCopy.creationTime) waitTime, 1 found
            FROM DiskCopy, SubRequest, CastorFile,
              (SELECT id, svcClassName FROM StageGetRequest UNION ALL
               SELECT id, svcClassName FROM StagePrepareToGetRequest UNION ALL
               SELECT id, svcClassName FROM StageUpdateRequest UNION ALL
               SELECT id, svcClassName FROM StageRepackRequest) Request
           WHERE DiskCopy.id = SubRequest.diskCopy
             AND DiskCopy.status = 2  -- DISKCOPY_WAITTAPERECALL
             AND DiskCopy.castorFile = CastorFile.id
             AND SubRequest.status = 4  -- SUBREQUEST_WAITTAPERECALL
             AND SubRequest.parent = 0
             AND SubRequest.request = Request.id
        ) a
        -- Attach a list of all service classes
        RIGHT JOIN SvcClass
           ON SvcClass.name = a.svcClass
        GROUP BY SvcClass.name
        ORDER BY SvcClass.name;
  END waitTapeRecallStats;

END CastorMon;
/


/* Database jobs */
BEGIN
  -- Drop all monitoring jobs
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name LIKE ('MONITORINGJOB_%'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Recreate monitoring jobs
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'monitoringJob_60mins',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'DECLARE
                            interval NUMBER := 3600;
                          BEGIN
                            castorMon.diskCopyStats(interval);
                            castorMon.waitTapeMigrationStats(interval);
                            castorMon.waitTapeRecallStats(interval);
                          END;',
      JOB_CLASS       => 'CASTOR_MON_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=60',
      ENABLED         => TRUE,
      COMMENTS        => 'Generation of monitoring information');
END;
/

/******************************************************************************
 *                   oracleRH.sql
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * PL/SQL code dedicated to the Request Handler part of CASTOR
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* prechecks common to all insert*Request methods*/
CREATE OR REPLACE FUNCTION insertPreChecks
  (euid IN INTEGER,
   egid IN INTEGER,
   svcClassName IN VARCHAR2,
   reqType IN INTEGER) RETURN NUMBER AS
BEGIN
  -- Check permissions
  IF 0 != checkPermission(svcClassName, euid, egid, reqType) THEN
    -- permission denied
    raise_application_error(-20121, 'Permission denied');
  END IF;  
  -- check the validity of the given service class and return its internal id
  RETURN checkForValidSvcClass(svcClassName, 1, 1);
END;
/

/* inserts simple Requests in the stager DB.
 * This handles FirstByteWritten, GetUpdateFailed, GetUpdateDone and PutFailed
 * requests.
 */ 	 
CREATE OR REPLACE PROCEDURE insertSimpleRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   subReqId IN INTEGER,
   fileId IN INTEGER,
   nsHost IN VARCHAR2) AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 78 THEN -- GetUpdateDone
      INSERT INTO GetUpdateDone (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, subReqId, fileId, nsHost, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,subReqId,fileId,nsHost,reqId,svcClassId,clientId);
    WHEN reqType = 79 THEN  -- GetUpdateFailed
      INSERT INTO GetUpdateFailed (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, subReqId, fileId, nsHost, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,subReqId,fileId,nsHost,reqId,svcClassId,clientId);
    WHEN reqType = 80 THEN  -- PutFailed
      INSERT INTO PutFailed (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, subReqId, fileId, nsHost, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,subReqId,fileId,nsHost,reqId,svcClassId,clientId);
    WHEN reqType = 147 THEN -- FirstByteWritten
      INSERT INTO FirstByteWritten (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, subReqId, fileId, nsHost, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,subReqId,fileId,nsHost,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertSimpleRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* inserts file Requests in the stager DB.
 * This handles StageGetRequest, StagePrepareToGetRequest, StagePutRequest,
 * StagePrepareToPutRequest, StageUpdateRequest, StagePrepareToUpdateRequest,
 * StagePutDoneRequest, StagePrepareToUpdateRequest, and StageRmRequest requests.
 */ 	 
CREATE OR REPLACE PROCEDURE insertFileRequest
  (userTag IN VARCHAR2,
   machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   mask IN INTEGER,
   userName IN VARCHAR2,
   flags IN INTEGER,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   inReqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   freeStrParam IN VARCHAR2,
   freeNumParam IN NUMBER,
   srFileNames IN castor."strList",
   srProtocols IN castor."strList",
   srXsizes IN castor."cnumList",
   srFlags IN castor."cnumList",
   srModeBits IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  subreqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  svcHandler VARCHAR2(100);
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, inReqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN inReqType = 35 THEN -- StageGetRequest
      INSERT INTO StageGetRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 36 THEN -- StagePrepareToGetRequest
      INSERT INTO StagePrepareToGetRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 40 THEN -- StagePutRequest
      INSERT INTO StagePutRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 37 THEN -- StagePrepareToPutRequest
      INSERT INTO StagePrepareToPutRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 44 THEN -- StageUpdateRequest
      INSERT INTO StageUpdateRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 38 THEN -- StagePrepareToUpdateRequest
      INSERT INTO StagePrepareToUpdateRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 42 THEN -- StageRmRequest
      INSERT INTO StageRmRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 39 THEN -- StagePutDoneRequest
      INSERT INTO StagePutDoneRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,freeStrParam,reqId,svcClassId,clientId);
    WHEN inReqType = 95 THEN -- SetFileGCWeight
      INSERT INTO SetFileGCWeight (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, weight, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,freeNumParam,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertFileRequest : ' || TO_CHAR(inReqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- get the request's service handler
  SELECT svcHandler INTO svcHandler FROM Type2Obj WHERE type=inReqType;
  -- Loop on subrequests
  FOR i IN srFileNames.FIRST .. srFileNames.LAST LOOP
    -- get unique ids for the subrequest
    SELECT ids_seq.nextval INTO subreqId FROM DUAL;
    -- insert the subrequest
    INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, parent, status, request, getNextStatus, reqType)
    VALUES (0, srFileNames(i), srProtocols(i), srXsizes(i), 0, NULL, srFlags(i), srModeBits(i), creationTime, creationTime, 0, 0, '', NULL, svcHandler, subreqId, NULL, NULL, NULL, 0, reqId, 0, inReqType);
  END LOOP;
END;
/

/* inserts start Requests in the stager DB.
 * This handles PutStartRequest and GetUpdateStartRequest
 * requests.
 */ 	 
CREATE OR REPLACE PROCEDURE insertStartRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   subReqId IN INTEGER,
   diskServer IN VARCHAR2,
   fileSystem IN VARCHAR2,
   fileId IN INTEGER,
   nsHost IN VARCHAR2) AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 67 THEN -- PutStartRequest
      INSERT INTO PutStartRequest (subReqId, diskServer, fileSystem, fileId, nsHost, flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (subReqId,diskServer,fileSystem,fileId,nsHost,0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN reqType = 60 THEN  -- GetUpdateStartRequest
      INSERT INTO GetUpdateStartRequest (subReqId, diskServer, fileSystem, fileId, nsHost, flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (subReqId,diskServer,fileSystem,fileId,nsHost,0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertStartRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* inserts d2d Requests in the stager DB.
 * This handles Disk2DiskCopyDoneRequest and Disk2DiskCopyStartRequest
 * requests.
 */ 	 
CREATE OR REPLACE PROCEDURE insertD2dRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   diskCopyId IN INTEGER,
   srcDiskCopyId IN INTEGER,
   fileSize IN INTEGER,
   diskServerName IN VARCHAR2,
   mountPoint IN VARCHAR2,
   fileId IN INTEGER,
   nsHost IN VARCHAR2) AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 64 THEN -- Disk2DiskCopyDoneRequest
      INSERT INTO Disk2DiskCopyDoneRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, diskCopyId, sourceDiskCopyId, fileId, nsHost, replicaFileSize, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,diskCopyId,srcDiskCopyId,fileId,nsHost,fileSize,reqId,svcClassId,clientId);
    WHEN reqType = 144 THEN  -- Disk2DiskCopyStartRequest
      INSERT INTO Disk2DiskCopyStartRequest  (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, diskCopyId, sourceDiskCopyId, diskServer, mountPoint, fileId, nsHost, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,diskCopyId,srcDiskCopyId,diskServerName,mountPoint,fileId,nsHost,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertD2dRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* inserts VersionQuery requests in the stager DB */ 	 
CREATE OR REPLACE PROCEDURE insertVersionQueryRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER) AS
  svcClassId NUMBER;
  reqId NUMBER;
  queryParamId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 131 THEN -- VersionQuery
      INSERT INTO VersionQuery (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertVersionQueryRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* inserts StageFileQueryRequest requests in the stager DB */ 	 
CREATE OR REPLACE PROCEDURE insertStageFileQueryRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   fileName IN VARCHAR2,
   qpValues IN castor."strList",
   qpTypes IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  queryParamId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 33 THEN -- StageFileQueryRequest
      INSERT INTO StageFileQueryRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, fileName, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,fileName,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertFileQueryRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on query parameters
  FOR i IN qpValues.FIRST .. qpValues.LAST LOOP
    -- get unique ids for the query parameter
    SELECT ids_seq.nextval INTO queryParamId FROM DUAL;
    -- insert the query parameter
    INSERT INTO QueryParameter (value, id, query, querytype)
    VALUES (qpValues(i), queryParamId, reqId, qpTypes(i));
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* inserts DiskPoolQuery requests in the stager DB. */ 	 
CREATE OR REPLACE PROCEDURE insertDiskPoolQueryRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   dpName IN VARCHAR2,
   queryType IN NUMBER) AS
  svcClassId NUMBER;
  reqId NUMBER;
  queryParamId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 195 THEN -- DiskPoolQuery
      INSERT INTO DiskPoolQuery (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, diskPoolName, id, svcClass, client,queryType)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,dpName,reqId,svcClassId,clientId,queryType);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertDiskPoolQueryRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* inserts MoverClose Requests in the stager DB */ 	 
CREATE OR REPLACE PROCEDURE insertMoverCloseRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   subReqId IN INTEGER,
   fileId IN INTEGER,
   nsHost IN VARCHAR2,
   fileSize IN INTEGER,
   timeStamp IN INTEGER,
   checkSumType IN VARCHAR2,
   checkSumValue IN VARCHAR2) AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 65 THEN -- MoverCloseRequest
      INSERT INTO MoverCloseRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, subReqId, fileSize, timeStamp, fileId, nsHost, csumType, csumValue, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,subReqId,fileSize,timeStamp,fileId,nsHost,checkSumType,checkSumValue,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertMoverClose : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* inserts Files2Delete Requests in the stager DB */ 	 
CREATE OR REPLACE PROCEDURE insertFiles2DeleteRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   diskServerName IN VARCHAR2) AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 73 THEN -- Files2Delete
      INSERT INTO Files2Delete (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, diskServer, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,diskServerName,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertFiles2Delete : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* inserts Files2Delete Requests in the stager DB */ 	 
CREATE OR REPLACE PROCEDURE insertListPrivilegesRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   userId IN INTEGER,
   groupId IN INTEGER,
   requestType IN INTEGER) AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 155 THEN -- ListPrivileges
      INSERT INTO ListPrivileges (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, userId, groupId, requestType, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,userId,groupId,requestType,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertListPrivileges : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* inserts StageAbort Request in the stager DB */ 	 
CREATE OR REPLACE PROCEDURE insertStageAbortRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   parentUUID IN VARCHAR2,
   fileids IN castor."cnumList",
   nsHosts IN castor."strList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  fileidId INTEGER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 50 THEN -- StageAbortRequest
      INSERT INTO StageAbortRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,parentUUID,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertStageAbortRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on fileids
  FOR i IN fileids.FIRST .. fileids.LAST LOOP
    -- ignore fake items passed only because ORACLE does not like empty arrays
    IF nsHosts(i) IS NULL THEN EXIT; END IF;
    -- get unique ids for the fileid
    SELECT ids_seq.nextval INTO fileidId FROM DUAL;
    -- insert the fileid
    INSERT INTO NsFileId (fileId, nsHost, id, request)
    VALUES (fileids(i), nsHosts(i), fileidId, reqId);
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* inserts GC Requests in the stager DB
 * This handles NsFilesDeleted, FilesDeleted, FilesDeletionFailed and StgFilesDeleted 
 * requests.
 */
CREATE OR REPLACE PROCEDURE insertGCRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   nsHost IN VARCHAR2,
   diskCopyIds IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  gcFileId INTEGER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 142 THEN -- NsFilesDeleted
      INSERT INTO NsFilesDeleted (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, nsHost, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,nsHost,reqId,svcClassId,clientId);
    WHEN reqType = 74 THEN -- FilesDeleted
      INSERT INTO FilesDeleted (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN reqType = 83 THEN -- FilesDeletionFailed
      INSERT INTO FilesDeletionFailed (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN reqType = 149 THEN -- StgFilesDeleted
      INSERT INTO StgFilesDeleted (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, nsHost, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,nsHost,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertGCRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on diskCopies
  FOR i IN diskCopyIds.FIRST .. diskCopyIds.LAST LOOP
    -- get unique ids for the diskCopy
    SELECT ids_seq.nextval INTO gcFileId FROM DUAL;
    -- insert the fileid
    INSERT INTO GCFile (diskCopyId, id, request)
    VALUES (diskCopyIds(i), gcFileId, reqId);
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* inserts ChangePrivilege Request in the stager DB */ 	 
CREATE OR REPLACE PROCEDURE insertChangePrivilegeRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   isGranted IN INTEGER,
   euids IN castor."cnumList",
   egids IN castor."cnumList",
   reqTypes IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  subobjId INTEGER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 152 THEN -- ChangePrivilege
      INSERT INTO ChangePrivilege (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, isGranted, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,isGranted,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertChangePrivilege : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on request types
  FOR i IN reqTypes.FIRST .. reqTypes.LAST LOOP
    -- get unique ids for the request type
    SELECT ids_seq.nextval INTO subobjId FROM DUAL;
    -- insert the request type
    INSERT INTO RequestType (reqType, id, request)
    VALUES (reqTypes(i), subobjId, reqId);
  END LOOP;
  -- Loop on BWUsers
  FOR i IN euids.FIRST .. euids.LAST LOOP
    -- get unique ids for the request type
    SELECT ids_seq.nextval INTO subobjId FROM DUAL;
    -- insert the BWUser
    INSERT INTO BWUser (euid, egid, id, request)
    VALUES (euids(i), egids(i), subobjId, reqId);
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
END;
/

/* Flag the schema creation as COMPLETE */
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE';
COMMIT;
