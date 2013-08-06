/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE;

/* accessors to ObjStatus table */
CREATE OR REPLACE FUNCTION getObjStatusName(inObject VARCHAR2, inField VARCHAR2, inStatusCode INTEGER)
RETURN VARCHAR2 AS
  varstatusName VARCHAR2(2048);
BEGIN
  SELECT statusName INTO varstatusName
    FROM ObjStatus
   WHERE object = inObject
     AND field = inField
     AND statusCode = inStatusCode;
  RETURN varstatusName;
END;
/

CREATE OR REPLACE PROCEDURE setObjStatusName(inObject VARCHAR2, inField VARCHAR2,
                                             inStatusCode INTEGER, inStatusName VARCHAR2) AS
BEGIN
  INSERT INTO ObjStatus (object, field, statusCode, statusName)
  VALUES (inObject, inField, inStatusCode, inStatusName);
END;
/

/* Type2Obj metatable definition */
CREATE TABLE Type2Obj (type INTEGER CONSTRAINT PK_Type2Obj_Type PRIMARY KEY, object VARCHAR2(100) CONSTRAINT NN_Type2Obj_Object NOT NULL, svcHandler VARCHAR2(100), CONSTRAINT UN_Type2Obj_typeObject UNIQUE (type, object));

/* ObjStatus metatable definition */
CREATE TABLE ObjStatus (object VARCHAR2(100) CONSTRAINT NN_ObjStatus_object NOT NULL, field VARCHAR2(100) CONSTRAINT NN_ObjStatus_field NOT NULL, statusCode INTEGER CONSTRAINT NN_ObjStatus_statusCode NOT NULL, statusName VARCHAR2(100) CONSTRAINT NN_ObjStatus_statusName NOT NULL, CONSTRAINT UN_ObjStatus_objectFieldCode UNIQUE (object, field, statusCode));

/* SQL statements for type BaseAddress */
CREATE TABLE BaseAddress (objType NUMBER, cnvSvcName VARCHAR2(2048), cnvSvcType NUMBER, target INTEGER, id INTEGER CONSTRAINT PK_BaseAddress_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type Client */
CREATE TABLE Client (ipAddress NUMBER, port NUMBER, version NUMBER, secure NUMBER, id INTEGER CONSTRAINT PK_Client_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

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

BEGIN
  setObjStatusName('QueryParameter', 'queryType', 0, 'REQUESTQUERYTYPE_FILENAME');
  setObjStatusName('QueryParameter', 'queryType', 1, 'REQUESTQUERYTYPE_REQID');
  setObjStatusName('QueryParameter', 'queryType', 2, 'REQUESTQUERYTYPE_USERTAG');
  setObjStatusName('QueryParameter', 'queryType', 3, 'REQUESTQUERYTYPE_FILEID');
  setObjStatusName('QueryParameter', 'queryType', 4, 'REQUESTQUERYTYPE_REQID_GETNEXT');
  setObjStatusName('QueryParameter', 'queryType', 5, 'REQUESTQUERYTYPE_USERTAG_GETNEXT');
  setObjStatusName('QueryParameter', 'queryType', 6, 'REQUESTQUERYTYPE_FILENAME_ALLSC');
END;
/

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

/* SQL statements for type SetFileGCWeight */
CREATE TABLE SetFileGCWeight (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, weight NUMBER, id INTEGER CONSTRAINT PK_SetFileGCWeight_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

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

BEGIN
  setObjStatusName('DiskPoolQuery', 'queryType', 0, 'DISKPOOLQUERYTYPE_DEFAULT');
  setObjStatusName('DiskPoolQuery', 'queryType', 1, 'DISKPOOLQUERYTYPE_AVAILABLE');
  setObjStatusName('DiskPoolQuery', 'queryType', 2, 'DISKPOOLQUERYTYPE_TOTAL');
END;
/

/* SQL statements for type ChangePrivilege */
CREATE TABLE ChangePrivilege (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, isGranted NUMBER, id INTEGER CONSTRAINT PK_ChangePrivilege_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type BWUser */
CREATE TABLE BWUser (euid NUMBER, egid NUMBER, id INTEGER CONSTRAINT PK_BWUser_Id PRIMARY KEY, request INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type RequestType */
CREATE TABLE RequestType (reqType NUMBER, id INTEGER CONSTRAINT PK_RequestType_Id PRIMARY KEY, request INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type ListPrivileges */
CREATE TABLE ListPrivileges (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, userId NUMBER, groupId NUMBER, requestType NUMBER, id INTEGER CONSTRAINT PK_ListPrivileges_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* Fill Type2Obj metatable */
INSERT INTO Type2Obj (type, object) VALUES (0, 'INVALID');
INSERT INTO Type2Obj (type, object) VALUES (1, 'Ptr');
INSERT INTO Type2Obj (type, object) VALUES (2, 'CastorFile');
INSERT INTO Type2Obj (type, object) VALUES (4, 'Cuuid');
INSERT INTO Type2Obj (type, object) VALUES (6, 'DiskFile');
INSERT INTO Type2Obj (type, object) VALUES (7, 'DiskPool');
INSERT INTO Type2Obj (type, object) VALUES (10, 'FileClass');
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
INSERT INTO Type2Obj (type, object) VALUES (129, 'Client');
INSERT INTO Type2Obj (type, object) VALUES (130, 'JobSubmissionRequest');
INSERT INTO Type2Obj (type, object) VALUES (131, 'VersionQuery');
INSERT INTO Type2Obj (type, object) VALUES (132, 'VersionResponse');
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
INSERT INTO Type2Obj (type, object) VALUES (171, 'DbInfoRetryPolicy');
INSERT INTO Type2Obj (type, object) VALUES (172, 'EndNotification');
INSERT INTO Type2Obj (type, object) VALUES (173, 'NoMoreFiles');
INSERT INTO Type2Obj (type, object) VALUES (174, 'NotificationAcknowledge');
INSERT INTO Type2Obj (type, object) VALUES (175, 'FileErrorReport');
INSERT INTO Type2Obj (type, object) VALUES (176, 'BaseFileInfo');
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
CREATE TABLE UpgradeLog (Username VARCHAR2(64) DEFAULT sys_context('USERENV', 'OS_USER') CONSTRAINT NN_UpgradeLog_Username NOT NULL, SchemaName VARCHAR2(64) DEFAULT 'STAGER' CONSTRAINT NN_UpgradeLog_SchemaName NOT NULL, Machine VARCHAR2(64) DEFAULT sys_context('USERENV', 'HOST') CONSTRAINT NN_UpgradeLog_Machine NOT NULL, Program VARCHAR2(48) DEFAULT sys_context('USERENV', 'MODULE') CONSTRAINT NN_UpgradeLog_Program NOT NULL, StartDate TIMESTAMP(6) WITH TIME ZONE DEFAULT systimestamp, EndDate TIMESTAMP(6) WITH TIME ZONE, FailureCount NUMBER DEFAULT 0, Type VARCHAR2(20) DEFAULT 'NON TRANSPARENT', State VARCHAR2(20) DEFAULT 'INCOMPLETE', SchemaVersion VARCHAR2(20) CONSTRAINT NN_UpgradeLog_SchemaVersion NOT NULL, Release VARCHAR2(20) CONSTRAINT NN_UpgradeLog_Release NOT NULL);

/* SQL statements for check constraints on the UpgradeLog table */
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_State
  CHECK (state IN ('COMPLETE', 'INCOMPLETE'));
  
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_Type
  CHECK (type IN ('TRANSPARENT', 'NON TRANSPARENT'));

/* SQL statement to populate the intial release value */
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('-', '2_1_14_2');

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

/******************************************************************************
 *                 castor/db/oracleConstants.sql
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
 * @author castor-dev@cern.ch
 *****************************************************************************/

/**
 * Package containing the definition of all tape-archive related PL/SQL
 * constants.
 */
CREATE OR REPLACE PACKAGE tconst
AS
  -- TPMODE
  WRITE_DISABLE CONSTANT PLS_INTEGER :=  0;
  WRITE_ENABLE  CONSTANT PLS_INTEGER :=  1;

  RECALLMOUNT_NEW        CONSTANT PLS_INTEGER := 0;
  RECALLMOUNT_WAITDRIVE  CONSTANT PLS_INTEGER := 1;
  RECALLMOUNT_RECALLING  CONSTANT PLS_INTEGER := 2;

  RECALLJOB_PENDING      CONSTANT PLS_INTEGER := 1;
  RECALLJOB_SELECTED     CONSTANT PLS_INTEGER := 2;
  RECALLJOB_RETRYMOUNT   CONSTANT PLS_INTEGER := 3;

  MIGRATIONMOUNT_WAITTAPE  CONSTANT PLS_INTEGER := 0;
  MIGRATIONMOUNT_SEND_TO_VDQM CONSTANT PLS_INTEGER := 1;
  MIGRATIONMOUNT_WAITDRIVE CONSTANT PLS_INTEGER := 2;
  MIGRATIONMOUNT_MIGRATING CONSTANT PLS_INTEGER := 3;

  MIGRATIONJOB_PENDING   CONSTANT PLS_INTEGER := 0;
  MIGRATIONJOB_SELECTED  CONSTANT PLS_INTEGER := 1;
  MIGRATIONJOB_WAITINGONRECALL CONSTANT PLS_INTEGER := 3;

  REPACK_SUBMITTED       CONSTANT PLS_INTEGER := 6;
  REPACK_STARTING        CONSTANT PLS_INTEGER := 0;
  REPACK_ONGOING         CONSTANT PLS_INTEGER := 1;
  REPACK_FINISHED        CONSTANT PLS_INTEGER := 2;
  REPACK_FAILED          CONSTANT PLS_INTEGER := 3;
  REPACK_ABORTING        CONSTANT PLS_INTEGER := 4;
  REPACK_ABORTED         CONSTANT PLS_INTEGER := 5;

  TAPE_DISABLED          CONSTANT PLS_INTEGER := 1;
  TAPE_EXPORTED          CONSTANT PLS_INTEGER := 2;
  TAPE_BUSY              CONSTANT PLS_INTEGER := 4;
  TAPE_FULL              CONSTANT PLS_INTEGER := 8;
  TAPE_RDONLY            CONSTANT PLS_INTEGER := 16;
  TAPE_ARCHIVED          CONSTANT PLS_INTEGER := 32;
END tconst;
/

CREATE OR REPLACE FUNCTION tapeStatusToString(status IN NUMBER) RETURN VARCHAR2 AS
  res VARCHAR2(2048);
  rebuildValue NUMBER := 0;
BEGIN
  IF status = 0 THEN RETURN 'OK'; END IF;
  IF BITAND(status, tconst.TAPE_DISABLED) != 0 THEN
    res := res || '|DISABLED';
    rebuildValue := rebuildValue + tconst.TAPE_DISABLED;
  END IF;
  IF BITAND(status, tconst.TAPE_EXPORTED) != 0  THEN
    res := res || '|EXPORTED';
    rebuildValue := rebuildValue + tconst.TAPE_EXPORTED;
  END IF;
  IF BITAND(status, tconst.TAPE_BUSY) != 0  THEN
    res := res || '|BUSY';
    rebuildValue := rebuildValue + tconst.TAPE_BUSY;
  END IF;
  IF BITAND(status, tconst.TAPE_FULL) != 0  THEN
    res := res || '|FULL';
    rebuildValue := rebuildValue + tconst.TAPE_FULL;
  END IF;
  IF BITAND(status, tconst.TAPE_RDONLY) != 0  THEN
    res := res || '|RDONLY';
    rebuildValue := rebuildValue + tconst.TAPE_RDONLY;
  END IF;
  IF BITAND(status, tconst.TAPE_ARCHIVED) != 0  THEN
    res := res || '|ARCHIVED';
    rebuildValue := rebuildValue + tconst.TAPE_ARCHIVED;
  END IF;
  IF res IS NULL THEN
    res := 'UNKNOWN:' || TO_CHAR(status);
  ELSE
    res := SUBSTR(res, 2);
    IF rebuildValue != status THEN
      res := res || '|UNKNOWN:' || TO_CHAR(status-rebuildValue);
    END IF;
  END IF;
  RETURN res;
END;
/

/**
 * Package containing the definition of all disk related PL/SQL constants.
 */
CREATE OR REPLACE PACKAGE dconst
AS

  CASTORFILE_NOTONTAPE       CONSTANT PLS_INTEGER :=  0;
  CASTORFILE_ONTAPE          CONSTANT PLS_INTEGER :=  1;
  CASTORFILE_DISKONLY        CONSTANT PLS_INTEGER :=  2;

  DISKCOPY_VALID             CONSTANT PLS_INTEGER :=  0;
  DISKCOPY_FAILED            CONSTANT PLS_INTEGER :=  4;
  DISKCOPY_WAITFS            CONSTANT PLS_INTEGER :=  5;
  DISKCOPY_STAGEOUT          CONSTANT PLS_INTEGER :=  6;
  DISKCOPY_INVALID           CONSTANT PLS_INTEGER :=  7;
  DISKCOPY_BEINGDELETED      CONSTANT PLS_INTEGER :=  9;
  DISKCOPY_WAITFS_SCHEDULING CONSTANT PLS_INTEGER := 11;

  DISKSERVER_PRODUCTION CONSTANT PLS_INTEGER := 0;
  DISKSERVER_DRAINING   CONSTANT PLS_INTEGER := 1;
  DISKSERVER_DISABLED   CONSTANT PLS_INTEGER := 2;
  DISKSERVER_READONLY   CONSTANT PLS_INTEGER := 3;

  FILESYSTEM_PRODUCTION CONSTANT PLS_INTEGER := 0;
  FILESYSTEM_DRAINING   CONSTANT PLS_INTEGER := 1;
  FILESYSTEM_DISABLED   CONSTANT PLS_INTEGER := 2;
  FILESYSTEM_READONLY   CONSTANT PLS_INTEGER := 3;
  
  DRAININGJOB_SUBMITTED    CONSTANT PLS_INTEGER := 0;
  DRAININGJOB_STARTING     CONSTANT PLS_INTEGER := 1;
  DRAININGJOB_RUNNING      CONSTANT PLS_INTEGER := 2;
  DRAININGJOB_FAILED       CONSTANT PLS_INTEGER := 4;
  DRAININGJOB_FINISHED    CONSTANT PLS_INTEGER := 5;

  DRAIN_FILEMASK_NOTONTAPE    CONSTANT PLS_INTEGER := 0;
  DRAIN_FILEMASK_ALL          CONSTANT PLS_INTEGER := 1;
  
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
  SUBREQUEST_ARCHIVED         CONSTANT PLS_INTEGER := 11;
  SUBREQUEST_REPACK           CONSTANT PLS_INTEGER := 12;
  SUBREQUEST_READYFORSCHED    CONSTANT PLS_INTEGER := 13;

  GETNEXTSTATUS_NOTAPPLICABLE CONSTANT PLS_INTEGER :=  0;
  GETNEXTSTATUS_FILESTAGED    CONSTANT PLS_INTEGER :=  1;
  GETNEXTSTATUS_NOTIFIED      CONSTANT PLS_INTEGER :=  2;

  DISKPOOLQUERYTYPE_DEFAULT   CONSTANT PLS_INTEGER :=  0;
  DISKPOOLQUERYTYPE_AVAILABLE CONSTANT PLS_INTEGER :=  1;
  DISKPOOLQUERYTYPE_TOTAL     CONSTANT PLS_INTEGER :=  2;

  DISKPOOLSPACETYPE_FREE     CONSTANT PLS_INTEGER :=  0;
  DISKPOOLSPACETYPE_CAPACITY CONSTANT PLS_INTEGER :=  1;

  GCTYPE_AUTO                CONSTANT PLS_INTEGER :=  0;
  GCTYPE_USER                CONSTANT PLS_INTEGER :=  1;
  GCTYPE_TOOMANYREPLICAS     CONSTANT PLS_INTEGER :=  2;
  GCTYPE_DRAINING            CONSTANT PLS_INTEGER :=  3;
  GCTYPE_NSSYNCH             CONSTANT PLS_INTEGER :=  4;
  GCTYPE_OVERWRITTEN         CONSTANT PLS_INTEGER :=  5;
  GCTYPE_ADMIN               CONSTANT PLS_INTEGER :=  6;
  GCTYPE_FAILEDD2D           CONSTANT PLS_INTEGER :=  7;
  
  DELDC_ENOENT               CONSTANT PLS_INTEGER :=  1;
  DELDC_RECALL               CONSTANT PLS_INTEGER :=  2;
  DELDC_REPLICATION          CONSTANT PLS_INTEGER :=  3;
  DELDC_LOST                 CONSTANT PLS_INTEGER :=  4;
  DELDC_GC                   CONSTANT PLS_INTEGER :=  5;
  DELDC_NOOP                 CONSTANT PLS_INTEGER :=  6;

  DISK2DISKCOPYJOB_PENDING   CONSTANT PLS_INTEGER :=  0;
  DISK2DISKCOPYJOB_SCHEDULED CONSTANT PLS_INTEGER :=  1;
  DISK2DISKCOPYJOB_RUNNING   CONSTANT PLS_INTEGER :=  2;

  REPLICATIONTYPE_USER       CONSTANT PLS_INTEGER :=  0;
  REPLICATIONTYPE_INTERNAL   CONSTANT PLS_INTEGER :=  1;
  REPLICATIONTYPE_DRAINING   CONSTANT PLS_INTEGER :=  2;
  REPLICATIONTYPE_REBALANCE  CONSTANT PLS_INTEGER :=  3;

END dconst;
/

/**
 * Package containing the definition of all DLF levels and messages logged from the SQL-to-DLF API
 */
CREATE OR REPLACE PACKAGE dlf
AS
  /* message levels */
  LVL_EMERGENCY  CONSTANT PLS_INTEGER := 0; /* LOG_EMERG   System is unusable */
  LVL_ALERT      CONSTANT PLS_INTEGER := 1; /* LOG_ALERT   Action must be taken immediately */
  LVL_CRIT       CONSTANT PLS_INTEGER := 2; /* LOG_CRIT    Critical conditions */
  LVL_ERROR      CONSTANT PLS_INTEGER := 3; /* LOG_ERR     Error conditions */
  LVL_WARNING    CONSTANT PLS_INTEGER := 4; /* LOG_WARNING Warning conditions */
  LVL_NOTICE     CONSTANT PLS_INTEGER := 5; /* LOG_NOTICE  Normal but significant condition */
  LVL_USER_ERROR CONSTANT PLS_INTEGER := 5; /* LOG_NOTICE  Normal but significant condition */
  LVL_AUTH       CONSTANT PLS_INTEGER := 5; /* LOG_NOTICE  Normal but significant condition */
  LVL_SECURITY   CONSTANT PLS_INTEGER := 5; /* LOG_NOTICE  Normal but significant condition */
  LVL_SYSTEM     CONSTANT PLS_INTEGER := 6; /* LOG_INFO    Informational */
  LVL_DEBUG      CONSTANT PLS_INTEGER := 7; /* LOG_DEBUG   Debug-level messages */

  /* messages */
  FILE_DROPPED_BY_CLEANING     CONSTANT VARCHAR2(2048) := 'File was dropped by internal cleaning';
  PUTDONE_ENFORCED_BY_CLEANING CONSTANT VARCHAR2(2048) := 'PutDone enforced by internal cleaning';
  
  DBJOB_UNEXPECTED_EXCEPTION   CONSTANT VARCHAR2(2048) := 'Unexpected exception caught in DB job';

  MIGMOUNT_NO_FILE             CONSTANT VARCHAR2(2048) := 'startMigrationMounts: failed migration mount creation due to lack of files';
  MIGMOUNT_AGE_NO_FILE         CONSTANT VARCHAR2(2048) := 'startMigrationMounts: failed migration mount creation base on age due to lack of files';
  MIGMOUNT_NEW_MOUNT           CONSTANT VARCHAR2(2048) := 'startMigrationMounts: created new migration mount';
  MIGMOUNT_NEW_MOUNT_AGE       CONSTANT VARCHAR2(2048) := 'startMigrationMounts: created new migration mount based on age';
  MIGMOUNT_NOACTION            CONSTANT VARCHAR2(2048) := 'startMigrationMounts: no need for new migration mount';

  RECMOUNT_NEW_MOUNT           CONSTANT VARCHAR2(2048) := 'startRecallMounts: created new recall mount';
  RECMOUNT_NOACTION_NODRIVE    CONSTANT VARCHAR2(2048) := 'startRecallMounts: not allowed to start new recall mount. Maximum nb of drives has been reached';
  RECMOUNT_NOACTION_NOCAND     CONSTANT VARCHAR2(2048) := 'startRecallMounts: no candidate found for a mount';

  RECALL_FOUND_ONGOING_RECALL  CONSTANT VARCHAR2(2048) := 'createRecallCandidate: found already running recall';
  RECALL_UNKNOWN_NS_ERROR      CONSTANT VARCHAR2(2048) := 'createRecallCandidate: error when retrieving segments from namespace';
  RECALL_NO_SEG_FOUND          CONSTANT VARCHAR2(2048) := 'createRecallCandidate: no valid segment to recall found';
  RECALL_NO_SEG_FOUND_AT_ALL   CONSTANT VARCHAR2(2048) := 'createRecallCandidate: no segment found for this file. File is probably lost';
  RECALL_INVALID_SEGMENT       CONSTANT VARCHAR2(2048) := 'createRecallCandidate: found unusable segment';
  RECALL_UNUSABLE_TAPE         CONSTANT VARCHAR2(2048) := 'createRecallCandidate: found segment on unusable tape';
  RECALL_CREATING_RECALLJOB    CONSTANT VARCHAR2(2048) := 'createRecallCandidate: created new RecallJob';
  RECALL_MISSING_COPIES        CONSTANT VARCHAR2(2048) := 'createRecallCandidate: detected missing copies on tape';
  RECALL_MISSING_COPIES_NOOP   CONSTANT VARCHAR2(2048) := 'createRecallCandidate: detected missing copies on tape, but migrations ongoing';
  RECALL_MJ_FOR_MISSING_COPY   CONSTANT VARCHAR2(2048) := 'createRecallCandidate: create new MigrationJob to migrate missing copy';
  RECALL_COPY_STILL_MISSING    CONSTANT VARCHAR2(2048) := 'createRecallCandidate: could not find enough valid copy numbers to create missing copy';
  RECALL_MISSING_COPY_NO_ROUTE CONSTANT VARCHAR2(2048) := 'createRecallCandidate: no route to tape defined for missing copy';
  RECALL_MISSING_COPY_ERROR    CONSTANT VARCHAR2(2048) := 'createRecallCandidate: unexpected error when creating missing copy';
  RECALL_CANCEL_BY_VID         CONSTANT VARCHAR2(2048) := 'Canceling tape recall for given VID';
  RECALL_CANCEL_RECALLJOB_VID  CONSTANT VARCHAR2(2048) := 'Canceling RecallJobs for given VID';
  RECALL_FAILING               CONSTANT VARCHAR2(2048) := 'Failing Recall(s)';
  RECALL_FS_NOT_FOUND          CONSTANT VARCHAR2(2048) := 'bestFileSystemForRecall could not find a suitable destination for this recall';
  RECALL_NOT_FOUND             CONSTANT VARCHAR2(2048) := 'setBulkFileRecallResult: unable to identify recall, giving up';
  RECALL_INVALID_PATH          CONSTANT VARCHAR2(2048) := 'setFileRecalled: unable to parse input path, giving up';
  RECALL_COMPLETED_DB          CONSTANT VARCHAR2(2048) := 'setFileRecalled: db updates after full recall completed';
  RECALL_FILE_OVERWRITTEN      CONSTANT VARCHAR2(2048) := 'setFileRecalled: file was overwritten during recall, restarting from scratch or skipping repack';
  RECALL_FILE_DROPPED          CONSTANT VARCHAR2(2048) := 'checkRecallInNS: file was dropped from namespace during recall, giving up';
  RECALL_BAD_CHECKSUM          CONSTANT VARCHAR2(2048) := 'checkRecallInNS: bad checksum detected, will retry if allowed';
  RECALL_CREATED_CHECKSUM      CONSTANT VARCHAR2(2048) := 'checkRecallInNS: created missing checksum in the namespace';
  RECALL_FAILED                CONSTANT VARCHAR2(2048) := 'setBulkFileRecallResult: recall process failed, will retry if allowed';
  RECALL_PERMANENTLY_FAILED    CONSTANT VARCHAR2(2048) := 'setFileRecalled: recall process failed permanently';
  BULK_RECALL_COMPLETED        CONSTANT VARCHAR2(2048) := 'setBulkFileRecallResult: bulk recall completed';
  
  MIGRATION_CANCEL_BY_VID      CONSTANT VARCHAR2(2048) := 'Canceling tape migration for given VID';
  MIGRATION_COMPLETED          CONSTANT VARCHAR2(2048) := 'setFileMigrated: db updates after full migration completed';
  MIGRATION_NOT_FOUND          CONSTANT VARCHAR2(2048) := 'setFileMigrated: unable to identify migration, giving up';
  MIGRATION_RETRY              CONSTANT VARCHAR2(2048) := 'setBulkFilesMigrationResult: migration failed, will retry if allowed';
  MIGRATION_FILE_DROPPED       CONSTANT VARCHAR2(2048) := 'failFileMigration: file was dropped or modified during migration, giving up';
  MIGRATION_SUPERFLUOUS_COPY   CONSTANT VARCHAR2(2048) := 'failFileMigration: file already had enough copies on tape, ignoring new segment';
  MIGRATION_FAILED             CONSTANT VARCHAR2(2048) := 'failFileMigration: migration to tape failed for this file, giving up';
  MIGRATION_FAILED_NOT_FOUND   CONSTANT VARCHAR2(2048) := 'failFileMigration: file not found when failing migration';
  BULK_MIGRATION_COMPLETED     CONSTANT VARCHAR2(2048) := 'setBulkFileMigrationResult: bulk migration completed';

  REPACK_SUBMITTED             CONSTANT VARCHAR2(2048) := 'New Repack request submitted';
  REPACK_ABORTING              CONSTANT VARCHAR2(2048) := 'Aborting Repack request';
  REPACK_ABORTED               CONSTANT VARCHAR2(2048) := 'Repack request aborted';
  REPACK_ABORTED_FAILED        CONSTANT VARCHAR2(2048) := 'Aborting Repack request failed, dropping it';
  REPACK_JOB_ONGOING           CONSTANT VARCHAR2(2048) := 'repackManager: Repack processes still starting, no new ones will be started for this round';
  REPACK_STARTED               CONSTANT VARCHAR2(2048) := 'repackManager: Repack process started';
  REPACK_JOB_STATS             CONSTANT VARCHAR2(2048) := 'repackManager: Repack processes statistics';
  REPACK_UNEXPECTED_EXCEPTION  CONSTANT VARCHAR2(2048) := 'handleRepackRequest: unexpected exception caught';

  DRAINING_JOB_ONGOING         CONSTANT VARCHAR2(2048) := 'drainingManager: Draining jobs still starting, no new ones will be started for this round';
  DRAINING_STARTED             CONSTANT VARCHAR2(2048) := 'drainingManager: Draining process started';

  DELETEDISKCOPY_RECALL        CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy was lost, about to recall from tape';
  DELETEDISKCOPY_REPLICATION   CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy was lost, about to replicate from another pool';
  DELETEDISKCOPY_LOST          CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: file was LOST and is being dropped from the system';
  DELETEDISKCOPY_GC            CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy is being garbage collected';
  DELETEDISKCOPY_NOOP          CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy could not be garbage collected';

  STAGER_GET                   CONSTANT VARCHAR2(2048) := 'Get Request';
  STAGER_PUT                   CONSTANT VARCHAR2(2048) := 'Put Request';
  STAGER_UPDATE                CONSTANT VARCHAR2(2048) := 'Update Request';
  STAGER_PREPARETOGET          CONSTANT VARCHAR2(2048) := 'PrepareToGet Request';
  STAGER_PREPARETOPUT          CONSTANT VARCHAR2(2048) := 'PrepareToPut Request';
  STAGER_PREPARETOUPDATE       CONSTANT VARCHAR2(2048) := 'PrepareToUpdate Request';

  STAGER_D2D_TRIGGERED         CONSTANT VARCHAR2(2048) := 'Triggering DiskCopy replication';
  STAGER_WAITSUBREQ            CONSTANT VARCHAR2(2048) := 'Request moved to Wait';
  STAGER_UNABLETOPERFORM       CONSTANT VARCHAR2(2048) := 'Unable to perform request, notifying user';
  STAGER_RECREATION_IMPOSSIBLE CONSTANT VARCHAR2(2048) := 'Impossible to recreate CastorFile';
  STAGER_CASTORFILE_RECREATION CONSTANT VARCHAR2(2048) := 'Recreating CastorFile';
  STAGER_GET_REPLICATION       CONSTANT VARCHAR2(2048) := 'Triggering internal DiskCopy replication';
  STAGER_GET_REPLICATION_FAIL  CONSTANT VARCHAR2(2048) := 'Triggering internal DiskCopy replication failed';
  STAGER_DISKCOPY_FOUND        CONSTANT VARCHAR2(2048) := 'Available DiskCopy found';

  REPORT_HEART_BEAT_RESUMED    CONSTANT VARCHAR2(2048) := 'Heartbeat resumed for diskserver, status changed to PRODUCTION';
  
  D2D_CREATING_JOB             CONSTANT VARCHAR2(2048) := 'Created new Disk2DiskCopyJob';
  D2D_CANCELED_AT_START        CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Replication request canceled while queuing in scheduler or transfer already started';
  D2D_MULTIPLE_COPIES_ON_DS    CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Multiple copies of this file already found on this diskserver';
  D2D_SOURCE_GONE              CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Source has disappeared while queuing in scheduler, retrying';
  D2D_SRC_DISABLED             CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Source diskserver/filesystem was DISABLED meanwhile';
  D2D_DEST_NOT_PRODUCTION      CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Destination diskserver/filesystem not in PRODUCTION any longer';
  D2D_START_OK                 CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart called and returned successfully';
  D2D_D2DDONE_CANCEL           CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : Invalidating new copy as job was canceled';
  D2D_D2DDONE_BADSIZE          CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : File replication size mismatch';
  D2D_D2DDONE_OK               CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : Replication successful';
  D2D_D2DDONE_RETRIED          CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : Retrying disk to disk copy';
  D2D_D2DDONE_NORETRY          CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : Exhausted retries, giving up';
  D2D_D2DFAILED                CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : replication failed';
  REBALANCING_START            CONSTANT VARCHAR2(2048) := 'rebalancing : starting';
  REBALANCING_STOP             CONSTANT VARCHAR2(2048) := 'rebalancing : stopping';
END dlf;
/

/**
 * Package containing the definition of some relevant (s)errno values and messages.
 */
CREATE OR REPLACE PACKAGE serrno AS
  /* (s)errno values */
  ENOENT          CONSTANT PLS_INTEGER := 2;    /* No such file or directory */
  EINTR           CONSTANT PLS_INTEGER := 4;    /* Interrupted system call */
  EACCES          CONSTANT PLS_INTEGER := 13;   /* Permission denied */
  EBUSY           CONSTANT PLS_INTEGER := 16;   /* Device or resource busy */
  EEXIST          CONSTANT PLS_INTEGER := 17;   /* File exists */
  EISDIR          CONSTANT PLS_INTEGER := 21;   /* Is a directory */
  EINVAL          CONSTANT PLS_INTEGER := 22;   /* Invalid argument */
  ENOSPC          CONSTANT PLS_INTEGER := 28;   /* No space left on device */

  SEINTERNAL      CONSTANT PLS_INTEGER := 1015; /* Internal error */
  SECHECKSUM      CONSTANT PLS_INTEGER := 1037; /* Bad checksum */
  ENSFILECHG      CONSTANT PLS_INTEGER := 1402; /* File has been overwritten, request ignored */
  ENSNOSEG        CONSTANT PLS_INTEGER := 1403; /* Segment had been deleted */
  ENSTOOMANYSEGS  CONSTANT PLS_INTEGER := 1406; /* Too many copies on tape */
  ENSOVERWHENREP  CONSTANT PLS_INTEGER := 1407; /* Cannot overwrite valid segment when replacing */
  ERTWRONGSIZE    CONSTANT PLS_INTEGER := 1613; /* (Recalled) file size incorrect */
  ESTNOTAVAIL     CONSTANT PLS_INTEGER := 1718; /* File is currently not available */
  ESTNOSEGFOUND   CONSTANT PLS_INTEGER := 1723; /* File has no copy on tape or no diskcopies are accessible */
  ESTNOTAPEROUTE  CONSTANT PLS_INTEGER := 1727; /* File recreation canceled since the file cannot be routed to tape */
  
  /* messages */
  ENOENT_MSG          CONSTANT VARCHAR2(2048) := 'No such file or directory';
  EINTR_MSG           CONSTANT VARCHAR2(2048) := 'Interrupted system call';
  EACCES_MSG          CONSTANT VARCHAR2(2048) := 'Permission denied';
  EBUSY_MSG           CONSTANT VARCHAR2(2048) := 'Device or resource busy';
  EEXIST_MSG          CONSTANT VARCHAR2(2048) := 'File exists';
  EISDIR_MSG          CONSTANT VARCHAR2(2048) := 'Is a directory';
  EINVAL_MSG          CONSTANT VARCHAR2(2048) := 'Invalid argument';
  
  SEINTERNAL_MSG      CONSTANT VARCHAR2(2048) := 'Internal error';
  SECHECKSUM_MSG      CONSTANT VARCHAR2(2048) := 'Checksum mismatch between segment and file';
  ENSFILECHG_MSG      CONSTANT VARCHAR2(2048) := 'File has been overwritten, request ignored';
  ENSNOSEG_MSG        CONSTANT VARCHAR2(2048) := 'Segment had been deleted';
  ENSTOOMANYSEGS_MSG  CONSTANT VARCHAR2(2048) := 'Too many copies on tape';
  ENSOVERWHENREP_MSG  CONSTANT VARCHAR2(2048) := 'Cannot overwrite valid segment when replacing';
  ERTWRONGSIZE_MSG    CONSTANT VARCHAR2(2048) := 'Incorrect file size';
  ESTNOSEGFOUND_MSG   CONSTANT VARCHAR2(2048) := 'File has no copy on tape or no diskcopies are accessible';
  ESTNOTAPEROUTE_MSG  CONSTANT VARCHAR2(2048) := 'File recreation canceled since the file cannot be routed to tape';
END serrno;
/
/*******************************************************************
 *
 * This file contains all schema definitions which are not generated automatically.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_14_2';

/* Sequence for indices */
CREATE SEQUENCE ids_seq CACHE 300;

/* Custom type to handle int arrays */
CREATE OR REPLACE TYPE "numList" IS TABLE OF INTEGER;
/

/* Custom type to handle float arrays */
CREATE OR REPLACE TYPE floatList IS TABLE OF NUMBER;
/

/* Custom type to handle strings returned by pipelined functions */
CREATE OR REPLACE TYPE strListTable AS TABLE OF VARCHAR2(2048);
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
      IF l_list IS NOT NULL THEN
        PIPE ROW(ltrim(rtrim(l_list)));
      END IF;
      EXIT;
    END IF;
  END LOOP;
  RETURN;
END;
/

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


/****************/
/* CastorConfig */
/****************/

/* Define a table for some configuration key-value pairs and populate it */
CREATE TABLE CastorConfig
  (class VARCHAR2(2048) CONSTRAINT NN_CastorConfig_class NOT NULL,
   key VARCHAR2(2048) CONSTRAINT NN_CastorConfig_key NOT NULL,
   value VARCHAR2(2048) CONSTRAINT NN_CastorConfig_value NOT NULL,
   description VARCHAR2(2048));

ALTER TABLE CastorConfig ADD CONSTRAINT UN_CastorConfig_class_key UNIQUE (class, key);

/* Prompt for the value of the general/instance option */
UNDEF instanceName
ACCEPT instanceName CHAR DEFAULT castor_stager PROMPT 'Enter the castor instance name (default: castor_stager, example: castoratlas): '
SET VER OFF
INSERT INTO CastorConfig
  VALUES ('general', 'instance', '&instanceName', 'Name of this Castor instance');

/* Prompt for the value of the stager/nsHost option */
UNDEF stagerNsHost
ACCEPT stagerNsHost CHAR PROMPT 'Enter the name of the nameserver host (example: castorns; this value is mandatory): '
INSERT INTO CastorConfig
  VALUES ('stager', 'nsHost', '&stagerNsHost', 'The name of the name server host to set in the CastorFile table overriding the CNS/HOST option defined in castor.conf');

/* DB link to the nameserver db */
PROMPT Configuration of the database link to the CASTOR name space
UNDEF cnsUser
ACCEPT cnsUser CHAR DEFAULT 'castor' PROMPT 'Enter the nameserver db username (default castor): ';
UNDEF cnsPasswd
ACCEPT cnsPasswd CHAR PROMPT 'Enter the nameserver db password: ';
UNDEF cnsDbName
ACCEPT cnsDbName CHAR PROMPT 'Enter the nameserver db TNS name: ';
CREATE DATABASE LINK remotens
  CONNECT TO &cnsUser IDENTIFIED BY &cnsPasswd USING '&cnsDbName';

/* Insert other default values */
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
  VALUES ('Recall', 'MaxNbRetriesWithinMount', '2', 'The maximum number of retries for recalling a file within the same tape mount. When exceeded, the recall may still be retried in another mount. See Recall/MaxNbMount entry');
INSERT INTO CastorConfig
  VALUES ('Recall', 'MaxNbMounts', '2', 'The maximum number of mounts for recalling a given file. When exceeded, the recall will be failed if no other tapecopy can be used. See also Recall/MaxNbRetriesWithinMount entry');
INSERT INTO CastorConfig
  VALUES ('Migration', 'SizeThreshold', '300000000', 'The threshold to consider a file "small" or "large" when routing it to tape');
INSERT INTO CastorConfig
  VALUES ('D2dCopy', 'MaxNbRetries', '2', 'The maximum number of retries for disk to disk copies before it is considered failed. Here 2 means we will do in total 3 attempts.');
INSERT INTO CastorConfig
  VALUES ('DiskServer', 'HeartbeatTimeout', '180', 'The maximum amount of time in seconds that a diskserver can spend without sending any hearbeat before it is automatically set to offline.');
INSERT INTO CastorConfig
  VALUES ('Draining', 'MaxNbSchedD2dPerDrain', '1000', 'The maximum number of disk to disk copies that each draining job should send to the scheduler concurrently.');
INSERT INTO CastorConfig
  VALUES ('Rebalancing', 'Sensibility', '5', 'The rebalancing sensibility (in percent) : if a fileSystem is at least this percentage fuller than the average of the diskpool where is lives, rebalancing will fire.');

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
      INSERT INTO AdminUsers (euid, egid) VALUES (adminUserId, adminGroupId);
    EXCEPTION WHEN INVALID_NUMBER THEN
      dbms_output.put_line(errMsg);
    END;
  END LOOP;
END;
/


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
INSERT INTO GcPolicy VALUES ('LRUpin',
                             'castorGC.creationTimeUserWeight',
                             'castorGC.creationTimeRecallWeight',
                             'castorGC.creationTimeCopyWeight',
                             'castorGC.LRUFirstAccessHook',
                             'castorGC.LRUAccessHook',
                             'castorGC.LRUpinUserSetGCWeight');


/* SQL statements for type SvcClass */
CREATE TABLE SvcClass (name VARCHAR2(2048) CONSTRAINT NN_SvcClass_Name NOT NULL,
                       defaultFileSize INTEGER,
                       maxReplicaNb NUMBER,
                       gcPolicy VARCHAR2(2048) DEFAULT 'default' CONSTRAINT NN_SvcClass_GcPolicy NOT NULL,
                       disk1Behavior NUMBER,
                       replicateOnClose NUMBER,
                       failJobsWhenNoSpace NUMBER,
                       lastEditor VARCHAR2(2048) CONSTRAINT NN_SvcClass_LastEditor NOT NULL,
                       lastEditionTime INTEGER CONSTRAINT NN_SvcClass_LastEditionTime NOT NULL,
                       id INTEGER CONSTRAINT PK_SvcClass_Id PRIMARY KEY,
                       forcedFileClass INTEGER CONSTRAINT NN_SvcClass_ForcedFileClass NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
ALTER TABLE SvcClass ADD CONSTRAINT UN_SvcClass_Name UNIQUE (name);
ALTER TABLE SvcClass ADD CONSTRAINT FK_SvcClass_GCPolicy
  FOREIGN KEY (gcPolicy) REFERENCES GcPolicy (name);
CREATE INDEX I_SvcClass_GcPolicy ON SvcClass (gcPolicy);

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


/* SQL statements for type CastorFile */
CREATE TABLE CastorFile (fileId INTEGER,
                         nsHost VARCHAR2(2048),
                         fileSize INTEGER,
                         creationTime INTEGER,
                         lastAccessTime INTEGER,
                         lastKnownFileName VARCHAR2(2048) CONSTRAINT NN_CastorFile_LKFileName NOT NULL,
                         lastUpdateTime INTEGER,
                         id INTEGER CONSTRAINT PK_CastorFile_Id PRIMARY KEY,
                         fileClass INTEGER,
                         tapeStatus INTEGER, -- can be ONTAPE, NOTONTAPE, DISKONLY or NULL
                         nsOpenTime NUMBER CONSTRAINT NN_CastorFile_NsOpenTime NOT NULL)  -- timestamp given by the Nameserver at Cns_openx()
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
ALTER TABLE CastorFile ADD CONSTRAINT FK_CastorFile_FileClass
  FOREIGN KEY (fileClass) REFERENCES FileClass (id);
CREATE UNIQUE INDEX I_CastorFile_LastKnownFileName ON CastorFile (lastKnownFileName);
ALTER TABLE CastorFile ADD CONSTRAINT UN_CastorFile_LKFileName UNIQUE (lastKnownFileName);
CREATE INDEX I_CastorFile_FileClass ON CastorFile(FileClass);
CREATE UNIQUE INDEX I_CastorFile_FileIdNsHost ON CastorFile (fileId, nsHost);
ALTER TABLE CastorFile
  ADD CONSTRAINT CK_CastorFile_TapeStatus
  CHECK (tapeStatus IN (0, 1, 2));

/* SQL statement for table SubRequest */
CREATE TABLE SubRequest
  (retryCounter NUMBER, fileName VARCHAR2(2048), protocol VARCHAR2(2048),
   xsize INTEGER, priority NUMBER, subreqId VARCHAR2(2048), flags NUMBER,
   modeBits NUMBER, creationTime INTEGER CONSTRAINT NN_SubRequest_CreationTime 
   NOT NULL, lastModificationTime INTEGER, answered NUMBER, errorCode NUMBER, 
   errorMessage VARCHAR2(2048), id NUMBER CONSTRAINT NN_SubRequest_Id NOT NULL,
   diskcopy INTEGER, castorFile INTEGER, status INTEGER,
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

/* this index is dedicated to archivesubreq */
CREATE INDEX I_SubRequest_Req_Stat_no89 ON SubRequest (request, decode(status,8,NULL,9,NULL,status));

CREATE INDEX I_SubRequest_Castorfile ON SubRequest (castorFile);
CREATE INDEX I_SubRequest_DiskCopy ON SubRequest (diskCopy);
CREATE INDEX I_SubRequest_Request ON SubRequest (request);
CREATE INDEX I_SubRequest_SubReqId ON SubRequest (subReqId);
CREATE INDEX I_SubRequest_LastModTime ON SubRequest (lastModificationTime) LOCAL;
ALTER TABLE SubRequest
  ADD CONSTRAINT CK_SubRequest_Status
  CHECK (status IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13));

BEGIN
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_START, 'SUBREQUEST_START');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_RESTART, 'SUBREQUEST_RESTART');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_RETRY, 'SUBREQUEST_RETRY');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_WAITSCHED, 'SUBREQUEST_WAITSCHED');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_WAITTAPERECALL, 'SUBREQUEST_WAITTAPERECALL');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_WAITSUBREQ, 'SUBREQUEST_WAITSUBREQ');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_READY, 'SUBREQUEST_READY');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_FAILED, 'SUBREQUEST_FAILED');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_FINISHED, 'SUBREQUEST_FINISHED');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_FAILED_FINISHED, 'SUBREQUEST_FAILED_FINISHED');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_ARCHIVED, 'SUBREQUEST_ARCHIVED');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_REPACK, 'SUBREQUEST_REPACK');
  setObjStatusName('SubRequest', 'status', dconst.SUBREQUEST_READYFORSCHED, 'SUBREQUEST_READYFORSCHED');
  setObjStatusName('SubRequest', 'getNextStatus', dconst.GETNEXTSTATUS_NOTAPPLICABLE, 'GETNEXTSTATUS_NOTAPPLICABLE');
  setObjStatusName('SubRequest', 'getNextStatus', dconst.GETNEXTSTATUS_FILESTAGED, 'GETNEXTSTATUS_FILESTAGED');
  setObjStatusName('SubRequest', 'getNextStatus', dconst.GETNEXTSTATUS_NOTIFIED, 'GETNEXTSTATUS_NOTIFIED');
END;
/


/**********************************/
/* Recall/Migration related table */
/**********************************/

/* Definition of the RecallGroup table
 *   id : unique id of the RecallGroup
 *   name : the name of the RecallGroup
 *   nbDrives : maximum number of drives that may be concurrently used across all users of this RecallGroup
 *   minAmountDataForMount : the minimum amount of data needed to trigger a new mount, in bytes
 *   minNbFilesForMount : the minimum number of files needed to trigger a new mount
 *   maxFileAgeBeforeMount : the maximum file age before a tape in mounted, in seconds
 *   vdqmPriority : the priority that should be used for VDQM requests
 *   lastEditor : the login from which the tapepool was last modified
 *   lastEditionTime : the time at which the tapepool was last modified
 * Note that a mount is attempted as soon as one of the three criterias is reached.
 */
CREATE TABLE RecallGroup(id INTEGER CONSTRAINT PK_RecallGroup_Id PRIMARY KEY CONSTRAINT NN_RecallGroup_Id NOT NULL, 
                         name VARCHAR2(2048) CONSTRAINT NN_RecallGroup_Name NOT NULL
                                             CONSTRAINT UN_RecallGroup_Name UNIQUE USING INDEX,
                         nbDrives INTEGER CONSTRAINT NN_RecallGroup_NbDrives NOT NULL,
                         minAmountDataForMount INTEGER CONSTRAINT NN_RecallGroup_MinAmountData NOT NULL,
                         minNbFilesForMount INTEGER CONSTRAINT NN_RecallGroup_MinNbFiles NOT NULL,
                         maxFileAgeBeforeMount INTEGER CONSTRAINT NN_RecallGroup_MaxFileAge NOT NULL,
                         vdqmPriority INTEGER DEFAULT 0 CONSTRAINT NN_RecallGroup_VdqmPriority NOT NULL,
                         lastEditor VARCHAR2(2048) CONSTRAINT NN_RecallGroup_LastEditor NOT NULL,
                         lastEditionTime NUMBER CONSTRAINT NN_RecallGroup_LastEdTime NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* Insert the bare minimum to get a working recall:
 * create the default recall group to have a default recall mount traffic shaping.
 */
INSERT INTO RecallGroup (id, name, nbDrives, minAmountDataForMount, minNbFilesForMount,
                         maxFileAgeBeforeMount, vdqmPriority, lastEditor, lastEditionTime)
  VALUES (ids_seq.nextval, 'default', 20, 10*1024*1024*1024, 10, 30*3600, 0, 'Castor 2.1.13 or above installation script', getTime());


/* Definition of the RecallUser table
 *   euid : uid of the recall user
 *   egid : gid of the recall user
 *   recallGroup : the recall group to which this user belongs
 *   lastEditor : the login from which the tapepool was last modified
 *   lastEditionTime : the time at which the tapepool was last modified
 * Note that a mount is attempted as soon as one of the three criterias is reached.
 */
CREATE TABLE RecallUser(euid INTEGER,
                        egid INTEGER CONSTRAINT NN_RecallUser_Egid NOT NULL,
                        recallGroup INTEGER CONSTRAINT NN_RecallUser_RecallGroup NOT NULL,
                        lastEditor VARCHAR2(2048) CONSTRAINT NN_RecallUser_LastEditor NOT NULL,
                        lastEditionTime NUMBER CONSTRAINT NN_RecallUser_LastEdTime NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
-- see comment in the RecallMount table about why we need this index
CREATE INDEX I_RecallUser_RecallGroup ON RecallUser(recallGroup); 
ALTER TABLE RecallUser ADD CONSTRAINT FK_RecallUser_RecallGroup FOREIGN KEY (recallGroup) REFERENCES RecallGroup(id);

/* Definition of the RecallMount table
 *   id : unique id of the RecallGroup
 *   mountTransactionId : the VDQM transaction that this mount is dealing with
 *   vid : the tape mounted or to be mounted
 *   label : the label of the mounted tape
 *   density : the density of the mounted tape
 *   recallGroup : the recall group to which this mount belongs
 *   startTime : the time at which this mount started
 *   status : current status of the RecallMount (NEW, WAITDRIVE or RECALLING)
 *   lastVDQMPingTime : last time we have checked VDQM for this mount
 *   lastProcessedFseq : last fseq that was processed by this mount (-1 if none)
 */
CREATE TABLE RecallMount(id INTEGER CONSTRAINT PK_RecallMount_Id PRIMARY KEY CONSTRAINT NN_RecallMount_Id NOT NULL, 
                         mountTransactionId INTEGER CONSTRAINT UN_RecallMount_TransId UNIQUE USING INDEX,
                         VID VARCHAR2(2048) CONSTRAINT NN_RecallMount_VID NOT NULL
                                            CONSTRAINT UN_RecallMount_VID UNIQUE USING INDEX,
                         label VARCHAR2(2048),
                         density VARCHAR2(2048),
                         recallGroup INTEGER CONSTRAINT NN_RecallMount_RecallGroup NOT NULL,
                         startTime NUMBER CONSTRAINT NN_RecallMount_startTime NOT NULL,
                         status INTEGER CONSTRAINT NN_RecallMount_Status NOT NULL,
                         lastVDQMPingTime NUMBER DEFAULT 0 CONSTRAINT NN_RecallMount_lastVDQMPing NOT NULL,
                         lastProcessedFseq INTEGER DEFAULT -1 CONSTRAINT NN_RecallMount_Fseq NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
-- this index may sound counter productive as we have very few rows and a full table scan will always be faster
-- However, it is needed to avoid a table lock on RecallGroup when taking a row lock on RecallMount,
-- via the existing foreign key. On top, this table lock is also taken in case of an update that does not
-- touch any row while with the index, no row lock is taken at all, as one may expect
CREATE INDEX I_RecallMount_RecallGroup ON RecallMount(recallGroup); 
ALTER TABLE RecallMount ADD CONSTRAINT FK_RecallMount_RecallGroup FOREIGN KEY (recallGroup) REFERENCES RecallGroup(id);
BEGIN
  setObjStatusName('RecallMount', 'status', tconst.RECALLMOUNT_NEW, 'RECALLMOUNT_NEW');
  setObjStatusName('RecallMount', 'status', tconst.RECALLMOUNT_WAITDRIVE, 'RECALLMOUNT_WAITDRIVE');
  setObjStatusName('RecallMount', 'status', tconst.RECALLMOUNT_RECALLING, 'RECALLMOUNT_RECALLING');
END;
/
ALTER TABLE RecallMount
  ADD CONSTRAINT CK_RecallMount_Status
  CHECK (status IN (0, 1, 2));

/* Definition of the RecallJob table
 * id unique identifer of this RecallJob
 * castorFile the file to be recalled
 * copyNb the copy number of the segment that this recalljob is targetting
 * recallGroup the recallGroup that triggered the recall
 * svcClass the service class used when triggering the recall. Will be used to place the file on disk
 * euid the user that triggered the recall
 * egid the group that triggered the recall
 * vid the tape on which the targetted segment resides
 * fseq the file sequence number of the targetted segment on its tape
 * status status of the recallJob
 * filesize size of the segment to be recalled
 * creationTime time when this job was created
 * nbRetriesWithinMount number of times we have tried to read the file within the current tape mount
 * nbMounts number of times we have mounted a tape for this RecallJob
 * blockId blockId of the file
 * fileTransactionId
 */
CREATE TABLE RecallJob(id INTEGER CONSTRAINT PK_RecallJob_Id PRIMARY KEY CONSTRAINT NN_RecallJob_Id NOT NULL, 
                       castorFile INTEGER CONSTRAINT NN_RecallJob_CastorFile NOT NULL,
                       copyNb INTEGER CONSTRAINT NN_RecallJob_CopyNb NOT NULL,
                       recallGroup INTEGER CONSTRAINT NN_RecallJob_RecallGroup NOT NULL,
                       svcClass INTEGER CONSTRAINT NN_RecallJob_SvcClass NOT NULL,
                       euid INTEGER CONSTRAINT NN_RecallJob_Euid NOT NULL,
                       egid INTEGER CONSTRAINT NN_RecallJob_Egid NOT NULL,
                       vid VARCHAR2(2048) CONSTRAINT NN_RecallJob_VID NOT NULL,
                       fseq INTEGER CONSTRAINT NN_RecallJob_Fseq NOT NULL,
                       status INTEGER CONSTRAINT NN_RecallJob_Status NOT NULL,
                       fileSize INTEGER CONSTRAINT NN_RecallJob_FileSize NOT NULL,
                       creationTime INTEGER CONSTRAINT NN_RecallJob_CreationTime NOT NULL,
                       nbRetriesWithinMount NUMBER DEFAULT 0 CONSTRAINT NN_RecallJob_nbRetriesWM NOT NULL,
                       nbMounts NUMBER DEFAULT 0 CONSTRAINT NN_RecallJob_nbMounts NOT NULL,
                       blockId RAW(4) CONSTRAINT NN_RecallJob_blockId NOT NULL,
                       fileTransactionId INTEGER CONSTRAINT UN_RecallJob_FileTrId UNIQUE USING INDEX)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

-- see comment in the RecallMount table about why we need the next 3 indices (although here,
-- the size of the table by itself is asking for one)
CREATE INDEX I_RecallJob_SvcClass ON RecallJob (svcClass);
CREATE INDEX I_RecallJob_RecallGroup ON RecallJob (recallGroup);
CREATE INDEX I_RecallJob_Castorfile_VID ON RecallJob (castorFile, VID);
CREATE INDEX I_RecallJob_VIDFseq ON RecallJob (VID, fseq);

ALTER TABLE RecallJob ADD CONSTRAINT FK_RecallJob_SvcClass FOREIGN KEY (svcClass) REFERENCES SvcClass(id);
ALTER TABLE RecallJob ADD CONSTRAINT FK_RecallJob_RecallGroup FOREIGN KEY (recallGroup) REFERENCES RecallGroup(id);
ALTER TABLE RecallJob ADD CONSTRAINT FK_RecallJob_CastorFile FOREIGN KEY (castorFile) REFERENCES CastorFile(id);

BEGIN
  -- PENDING status is when a RecallJob is created
  -- It is immediately candidate for being recalled by an ongoing recallMount
  setObjStatusName('RecallJob', 'status', tconst.RECALLJOB_PENDING, 'RECALLJOB_PENDING');
  -- SELECTED status is when the file is currently being recalled.
  -- Note all recallJobs of a given file will have this state while the file is being recalled,
  -- even if another copy is being recalled. The recallJob that is effectively used can be identified
  -- by its non NULL fileTransactionId
  setObjStatusName('RecallJob', 'status', tconst.RECALLJOB_SELECTED, 'RECALLJOB_SELECTED');
  -- RETRYMOUNT status is when the file recall has failed and should be retried after remounting the tape
  -- These will be reset to PENDING on RecallMount deletion
  setObjStatusName('RecallJob', 'status', tconst.RECALLJOB_RETRYMOUNT, 'RECALLJOB_RETRYMOUNT');
END;
/
ALTER TABLE RecallJob
  ADD CONSTRAINT CK_RecallJob_Status
  CHECK (status IN (0, 1, 2));

/* Definition of the TapePool table
 *   name : the name of the TapePool
 *   nbDrives : maximum number of drives that may be concurrently used across all users of this TapePool
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
 *   mountTransactionId : the unique identifier of the mount transaction
 *   tapeGatewayRequestId : 
 *   VID : tape currently mounted (when applicable)
 *   label : label (i.e. format) of the currently mounted tape (when applicable)
 *   density : density of the currently mounted tape (when applicable)
 *   lastFseq : position of the last file written on the tape
 *   lastVDQMPingTime : last time we've pinged VDQM
 *   tapePool : tapepool used by this migration
 *   status : current status of the migration
 */
CREATE TABLE MigrationMount (mountTransactionId INTEGER CONSTRAINT UN_MigrationMount_VDQM UNIQUE USING INDEX,
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
BEGIN
  setObjStatusName('MigrationMount', 'status', tconst.MIGRATIONMOUNT_WAITTAPE, 'MIGRATIONMOUNT_WAITTAPE');
  setObjStatusName('MigrationMount', 'status', tconst.MIGRATIONMOUNT_SEND_TO_VDQM, 'MIGRATIONMOUNT_SEND_TO_VDQM');
  setObjStatusName('MigrationMount', 'status', tconst.MIGRATIONMOUNT_WAITDRIVE, 'MIGRATIONMOUNT_WAITDRIVE');
  setObjStatusName('MigrationMount', 'status', tconst.MIGRATIONMOUNT_MIGRATING, 'MIGRATIONMOUNT_MIGRATING');
END;
/
ALTER TABLE MigrationMount
  ADD CONSTRAINT CK_MigrationMount_Status
  CHECK (status IN (0, 1, 2, 3));

/* Definition of the MigratedSegment table
 * This table lists segments existing on tape for the files being
 * migrating. This allows to avoid putting two copies of a given
 * file on the same tape.
 *   castorFile : the file concerned
 *   copyNb : the copy number of this segment
 *   VID : the tape on which this segment resides
 */
CREATE TABLE MigratedSegment(castorFile INTEGER CONSTRAINT NN_MigratedSegment_CastorFile NOT NULL,
                             copyNb INTEGER CONSTRAINT NN_MigratedSegment_CopyNb NOT NULL,
                             VID VARCHAR2(2048) CONSTRAINT NN_MigratedSegment_VID NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE UNIQUE INDEX I_MigratedSegment_CFCopyNbVID ON MigratedSegment(CastorFile, copyNb, VID);
ALTER TABLE MigratedSegment ADD CONSTRAINT FK_MigratedSegment_CastorFile
  FOREIGN KEY (castorFile) REFERENCES CastorFile(id);

/* Definition of the MigrationJob table
 *   fileSize : size of the file to be migrated, in bytes
 *   VID : tape on which the file is being migrated (when applicable)
 *   creationTime : time of creation of this MigrationJob, in seconds since the epoch.
 *                  In case the MigrationJob went through a "WAITINGONRECALL" status,
 *                  time when it (re)entered the "PENDING" state
 *   castorFile : the file to migrate
 *   originalVID :  in case of repack, the VID of the tape where the original copy is leaving
 *   originalCopyNb : in case of repack, the number of the original copy being replaced
 *   destCopyNb : the number of the new copy of the file to migrate to tape
 *   tapePool : the tape pool where to migrate
 *   nbRetry : the number of retries we already went through
 *   errorcode : the error we got on last try (if any)
 *   mountTransactionId : an identifier for the migration session that is handling this job (when applicable)
 *   fileTransactionId : an identifier for this migration job
 *   fSeq : the file sequence of the copy created on tape for this job (when applicable)
 *   status : the status of the migration job
 */
CREATE TABLE MigrationJob (fileSize INTEGER CONSTRAINT NN_MigrationJob_FileSize NOT NULL,
                           VID VARCHAR2(2048),
                           creationTime NUMBER CONSTRAINT NN_MigrationJob_CreationTime NOT NULL,
                           castorFile INTEGER CONSTRAINT NN_MigrationJob_CastorFile NOT NULL,
                           originalVID VARCHAR2(20),
                           originalCopyNb INTEGER,
                           destCopyNb INTEGER CONSTRAINT NN_MigrationJob_destcopyNb NOT NULL,
                           tapePool INTEGER CONSTRAINT NN_MigrationJob_TapePool NOT NULL,
                           nbRetries INTEGER DEFAULT 0 CONSTRAINT NN_MigrationJob_nbRetries NOT NULL,
                           mountTransactionId INTEGER,   -- this is NULL at the beginning
                           fileTransactionId INTEGER CONSTRAINT UN_MigrationJob_FileTrId UNIQUE USING INDEX,
                           fSeq INTEGER,
                           status INTEGER CONSTRAINT NN_MigrationJob_Status NOT NULL,
                           id INTEGER CONSTRAINT PK_MigrationJob_Id PRIMARY KEY 
                                      CONSTRAINT NN_MigrationJob_Id NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
-- see comment in the RecallMount table about why we need this index
CREATE INDEX I_MigrationJob_MountTransId ON MigrationJob(mountTransactionId);
CREATE INDEX I_MigrationJob_CFVID ON MigrationJob(castorFile, VID);
CREATE INDEX I_MigrationJob_TapePoolSize ON MigrationJob(tapePool, fileSize);
CREATE UNIQUE INDEX I_MigrationJob_TPStatusId ON MigrationJob(tapePool, status, id);
CREATE UNIQUE INDEX I_MigrationJob_CFCopyNb ON MigrationJob(castorFile, destCopyNb);
ALTER TABLE MigrationJob ADD CONSTRAINT UN_MigrationJob_CopyNb
  UNIQUE (castorFile, destCopyNb) USING INDEX I_MigrationJob_CFCopyNb;
ALTER TABLE MigrationJob ADD CONSTRAINT FK_MigrationJob_CastorFile
  FOREIGN KEY (castorFile) REFERENCES CastorFile(id);
ALTER TABLE MigrationJob ADD CONSTRAINT FK_MigrationJob_TapePool
  FOREIGN KEY (tapePool) REFERENCES TapePool(id);
ALTER TABLE MigrationJob ADD CONSTRAINT FK_MigrationJob_MigrationMount
  FOREIGN KEY (mountTransactionId) REFERENCES MigrationMount(mountTransactionId);
ALTER TABLE MigrationJob ADD CONSTRAINT CK_MigrationJob_FS_Positive CHECK (fileSize > 0);
BEGIN
  setObjStatusName('MigrationJob', 'status', tconst.MIGRATIONJOB_PENDING, 'MIGRATIONJOB_PENDING');
  setObjStatusName('MigrationJob', 'status', tconst.MIGRATIONJOB_SELECTED, 'MIGRATIONJOB_SELECTED');
  setObjStatusName('MigrationJob', 'status', tconst.MIGRATIONJOB_WAITINGONRECALL, 'MIGRATIONJOB_WAITINGONRECALL');
END;
/
ALTER TABLE MigrationJob
  ADD CONSTRAINT CK_MigrationJob_Status
  CHECK (status IN (0, 1, 3));

/* Definition of the MigrationRouting table. Each line is a routing rule for migration jobs
 *   isSmallFile : whether this routing rule applies to small files. Null means it applies to all files
 *   copyNb : the copy number the routing rule applies to
 *   fileClass : the file class the routing rule applies to
 *   lastEditor : name of the last one that modified this routing rule.
 *   lastEditionTime : last time this routing rule was edited, in seconds since the epoch
 *   tapePool : the tape pool where to migrate files matching the above criteria
 */
CREATE TABLE MigrationRouting (isSmallFile INTEGER,
                               copyNb INTEGER CONSTRAINT NN_MigrationRouting_CopyNb NOT NULL,
                               fileClass INTEGER CONSTRAINT NN_MigrationRouting_FileClass NOT NULL,
                               lastEditor VARCHAR2(2048) CONSTRAINT NN_MigrationRouting_LastEditor NOT NULL,
                               lastEditionTime NUMBER CONSTRAINT NN_MigrationRouting_LastEdTime NOT NULL,
                               tapePool INTEGER CONSTRAINT NN_MigrationRouting_TapePool NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
-- see comment in the RecallMount table about why we need thess indexes
CREATE INDEX I_MigrationRouting_TapePool ON MigrationRouting(tapePool);
CREATE INDEX I_MigrationRouting_Rules ON MigrationRouting(fileClass, copyNb, isSmallFile);
ALTER TABLE MigrationRouting ADD CONSTRAINT UN_MigrationRouting_Rules
  UNIQUE (fileClass, copyNb, isSmallFile) USING INDEX I_MigrationRouting_Rules;
ALTER TABLE MigrationRouting ADD CONSTRAINT FK_MigrationRouting_FileClass
  FOREIGN KEY (fileClass) REFERENCES FileClass(id);
ALTER TABLE MigrationRouting ADD CONSTRAINT FK_MigrationRouting_TapePool
  FOREIGN KEY (tapePool) REFERENCES TapePool(id);

/* Temporary table used to bulk select next candidates for recall and migration */
CREATE GLOBAL TEMPORARY TABLE FilesToRecallHelper
 (fileId NUMBER, nsHost VARCHAR2(100), fileTransactionId NUMBER,
  filePath VARCHAR2(2048), blockId RAW(4), fSeq INTEGER, copyNb INTEGER,
  euid NUMBER, egid NUMBER, VID VARCHAR2(10), fileSize INTEGER, creationTime INTEGER,
  nbRetriesInMount INTEGER, nbMounts INTEGER)
 ON COMMIT DELETE ROWS;

CREATE GLOBAL TEMPORARY TABLE FilesToMigrateHelper
 (fileId NUMBER CONSTRAINT UN_FilesToMigrateHelper_fileId UNIQUE,
  nsHost VARCHAR2(100), lastKnownFileName VARCHAR2(2048), filePath VARCHAR2(2048),
  fileTransactionId NUMBER, fileSize NUMBER, fSeq INTEGER)
 ON COMMIT DELETE ROWS;

/* The following would be a temporary table, except that as it is used through a distributed
   transaction and Oracle does not support temporary tables in such context, it is defined as
   a normal table. See ns_setOrReplaceSegments for more details */
CREATE TABLE FileMigrationResultsHelper
 (reqId VARCHAR2(36), fileId NUMBER, lastModTime NUMBER, copyNo NUMBER, oldCopyNo NUMBER, transfSize NUMBER,
  comprSize NUMBER, vid VARCHAR2(6), fSeq NUMBER, blockId RAW(4), checksumType VARCHAR2(16), checksum NUMBER);
CREATE INDEX I_FileMigResultsHelper_ReqId ON FileMigrationResultsHelper(ReqId);

/* SQL statements for type DiskServer */
CREATE TABLE DiskServer (name VARCHAR2(2048), lastHeartbeatTime NUMBER, id INTEGER CONSTRAINT PK_DiskServer_Id PRIMARY KEY, status INTEGER, hwOnline INTEGER DEFAULT 0) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE UNIQUE INDEX I_DiskServer_name ON DiskServer (name);
ALTER TABLE DiskServer MODIFY
  (status CONSTRAINT NN_DiskServer_Status NOT NULL,
   name CONSTRAINT NN_DiskServer_Name NOT NULL,
   hwOnline CONSTRAINT NN_DiskServer_hwOnline NOT NULL);
ALTER TABLE DiskServer ADD CONSTRAINT UN_DiskServer_Name UNIQUE (name);

BEGIN
  setObjStatusName('DiskServer', 'status', dconst.DISKSERVER_PRODUCTION, 'DISKSERVER_PRODUCTION');
  setObjStatusName('DiskServer', 'status', dconst.DISKSERVER_DRAINING, 'DISKSERVER_DRAINING');
  setObjStatusName('DiskServer', 'status', dconst.DISKSERVER_DISABLED, 'DISKSERVER_DISABLED');
  setObjStatusName('DiskServer', 'status', dconst.DISKSERVER_READONLY, 'DISKSERVER_READONLY');
END;
/
ALTER TABLE DiskServer
  ADD CONSTRAINT CK_DiskServer_Status
  CHECK (status IN (0, 1, 2, 3));

/* SQL statements for type FileSystem */
CREATE TABLE FileSystem (free INTEGER, mountPoint VARCHAR2(2048), minAllowedFreeSpace NUMBER, maxFreeSpace NUMBER, totalSize INTEGER, nbReadStreams NUMBER, nbWriteStreams NUMBER, nbMigratorStreams NUMBER, nbRecallerStreams NUMBER, id INTEGER CONSTRAINT PK_FileSystem_Id PRIMARY KEY, diskPool INTEGER, diskserver INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
ALTER TABLE FileSystem ADD CONSTRAINT FK_FileSystem_DiskServer 
  FOREIGN KEY (diskServer) REFERENCES DiskServer(id);
ALTER TABLE FileSystem MODIFY
  (status     CONSTRAINT NN_FileSystem_Status NOT NULL,
   diskServer CONSTRAINT NN_FileSystem_DiskServer NOT NULL,
   mountPoint CONSTRAINT NN_FileSystem_MountPoint NOT NULL);
ALTER TABLE FileSystem ADD CONSTRAINT UN_FileSystem_DSMountPoint
  UNIQUE (diskServer, mountPoint);
CREATE INDEX I_FileSystem_DiskPool ON FileSystem (diskPool);
CREATE INDEX I_FileSystem_DiskServer ON FileSystem (diskServer);

BEGIN
  setObjStatusName('FileSystem', 'status', dconst.FILESYSTEM_PRODUCTION, 'FILESYSTEM_PRODUCTION');
  setObjStatusName('FileSystem', 'status', dconst.FILESYSTEM_DRAINING, 'FILESYSTEM_DRAINING');
  setObjStatusName('FileSystem', 'status', dconst.FILESYSTEM_DISABLED, 'FILESYSTEM_DISABLED');
  setObjStatusName('FileSystem', 'status', dconst.FILESYSTEM_READONLY, 'FILESYSTEM_READONLY');
END;
/
ALTER TABLE FileSystem
  ADD CONSTRAINT CK_FileSystem_Status
  CHECK (status IN (0, 1, 2, 3));

/* SQL statements for type DiskPool */
CREATE TABLE DiskPool (name VARCHAR2(2048), id INTEGER CONSTRAINT PK_DiskPool_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE DiskPool2SvcClass (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_DiskPool2SvcClass_C on DiskPool2SvcClass (child);
CREATE INDEX I_DiskPool2SvcClass_P on DiskPool2SvcClass (parent);
ALTER TABLE DiskPool2SvcClass
  ADD CONSTRAINT FK_DiskPool2SvcClass_P FOREIGN KEY (Parent) REFERENCES DiskPool (id)
  ADD CONSTRAINT FK_DiskPool2SvcClass_C FOREIGN KEY (Child) REFERENCES SvcClass (id);

/* DiskCopy Table
   - importance : the importance of this DiskCopy. The importance is always negative and the
     algorithm to compute it is -nb_disk_copies-100*at_least_a_tape_copy_exists
*/
CREATE TABLE DiskCopy
 (path VARCHAR2(2048),
  gcWeight NUMBER,
  creationTime INTEGER,
  lastAccessTime INTEGER,
  diskCopySize INTEGER,
  nbCopyAccesses NUMBER,
  owneruid NUMBER,
  ownergid NUMBER,
  id INTEGER CONSTRAINT PK_DiskCopy_Id PRIMARY KEY,
  gcType INTEGER,
  fileSystem INTEGER,
  castorFile INTEGER,
  status INTEGER,
  importance INTEGER CONSTRAINT NN_DiskCopy_Importance NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

CREATE INDEX I_DiskCopy_Castorfile ON DiskCopy (castorFile);
CREATE INDEX I_DiskCopy_FileSystem ON DiskCopy (fileSystem);
CREATE INDEX I_DiskCopy_FS_GCW ON DiskCopy (fileSystem, gcWeight);
-- for queries on active statuses
CREATE INDEX I_DiskCopy_Status_6 ON DiskCopy (decode(status,6,status,NULL));
CREATE INDEX I_DiskCopy_Status_7_FS ON DiskCopy (decode(status,7,status,NULL), fileSystem);
CREATE INDEX I_DiskCopy_Status_9 ON DiskCopy (decode(status,9,status,NULL));
-- to speed up deleteOutOfDateStageOutDCs
CREATE INDEX I_DiskCopy_Status_Open ON DiskCopy (decode(status,6,status,decode(status,5,status,decode(status,11,status,NULL))));
-- to speed up draining manager job
CREATE INDEX I_DiskCopy_FS_ST_Impor_ID_CF_S ON DiskCopy (filesystem, status, importance, id, castorFile, diskCopySize);

/* DiskCopy constraints */
ALTER TABLE DiskCopy MODIFY (nbCopyAccesses DEFAULT 0);
ALTER TABLE DiskCopy MODIFY (gcType DEFAULT NULL);
ALTER TABLE DiskCopy ADD CONSTRAINT FK_DiskCopy_CastorFile
  FOREIGN KEY (castorFile) REFERENCES CastorFile (id);
ALTER TABLE DiskCopy
  MODIFY (status CONSTRAINT NN_DiskCopy_Status NOT NULL);

BEGIN
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_AUTO, 'GCTYPE_AUTO');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_USER, 'GCTYPE_USER');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_TOOMANYREPLICAS, 'GCTYPE_TOOMANYREPLICAS');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_DRAINING, 'GCTYPE_DRAINING');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_NSSYNCH, 'GCTYPE_NSSYNCH');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_OVERWRITTEN, 'GCTYPE_OVERWRITTEN');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_ADMIN, 'GCTYPE_ADMIN');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_FAILEDD2D, 'GCTYPE_FAILEDD2D');
  setObjStatusName('DiskCopy', 'status', dconst.DISKCOPY_VALID, 'DISKCOPY_VALID');
  setObjStatusName('DiskCopy', 'status', dconst.DISKCOPY_FAILED, 'DISKCOPY_FAILED');
  setObjStatusName('DiskCopy', 'status', dconst.DISKCOPY_WAITFS, 'DISKCOPY_WAITFS');
  setObjStatusName('DiskCopy', 'status', dconst.DISKCOPY_STAGEOUT, 'DISKCOPY_STAGEOUT');
  setObjStatusName('DiskCopy', 'status', dconst.DISKCOPY_INVALID, 'DISKCOPY_INVALID');
  setObjStatusName('DiskCopy', 'status', dconst.DISKCOPY_BEINGDELETED, 'DISKCOPY_BEINGDELETED');
  setObjStatusName('DiskCopy', 'status', dconst.DISKCOPY_WAITFS_SCHEDULING, 'DISKCOPY_WAITFS_SCHEDULING');
END;
/
ALTER TABLE DiskCopy
  ADD CONSTRAINT CK_DiskCopy_Status
  CHECK (status IN (0, 4, 5, 6, 7, 9, 10, 11));
ALTER TABLE DiskCopy
  ADD CONSTRAINT CK_DiskCopy_GcType
  CHECK (gcType IN (0, 1, 2, 3, 4, 5, 6, 7));

CREATE INDEX I_StagePTGRequest_ReqId ON StagePrepareToGetRequest (reqId);
CREATE INDEX I_StagePTPRequest_ReqId ON StagePrepareToPutRequest (reqId);
CREATE INDEX I_StagePTURequest_ReqId ON StagePrepareToUpdateRequest (reqId);
CREATE INDEX I_StageGetRequest_ReqId ON StageGetRequest (reqId);
CREATE INDEX I_StagePutRequest_ReqId ON StagePutRequest (reqId);

/* Improve query execution in the checkFailJobsWhenNoSpace function */
CREATE INDEX I_StagePutRequest_SvcClass ON StagePutRequest (svcClass);

/* Indexing GCFile by Request */
CREATE INDEX I_GCFile_Request ON GCFile (request);

/* An index to speed up queries in FileQueryRequest, FindRequestRequest, RequestQueryRequest */
CREATE INDEX I_QueryParameter_Query ON QueryParameter (query);

/* Constraint on FileClass name */
ALTER TABLE FileClass ADD CONSTRAINT UN_FileClass_Name UNIQUE (name);
ALTER TABLE FileClass MODIFY (name CONSTRAINT NN_FileClass_Name NOT NULL);
ALTER TABLE FileClass MODIFY (classId CONSTRAINT NN_FileClass_ClassId NOT NULL);

/* Custom type to handle int arrays */
CREATE OR REPLACE TYPE "numList" IS TABLE OF INTEGER;
/

/* Custom type to handle float arrays */
CREATE OR REPLACE TYPE floatList IS TABLE OF NUMBER;
/

/* Custom type to handle strings returned by pipelined functions */
CREATE OR REPLACE TYPE strListTable AS TABLE OF VARCHAR2(2048);
/

/* SvcClass constraints */
ALTER TABLE SvcClass MODIFY (gcPolicy DEFAULT 'default');

/* DiskCopy constraints */
ALTER TABLE DiskCopy MODIFY (nbCopyAccesses DEFAULT 0);

ALTER TABLE DiskCopy MODIFY (gcType DEFAULT NULL);

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
CREATE GLOBAL TEMPORARY TABLE ProcessRepackAbortHelperDCmigr (cfId NUMBER) ON COMMIT DELETE ROWS;

/* Tables to log the DB activity */
CREATE TABLE DLFLogs
  (timeinfo NUMBER,
   uuid VARCHAR2(2048),
   priority INTEGER CONSTRAINT NN_DLFLogs_Priority NOT NULL,
   msg VARCHAR2(2048) CONSTRAINT NN_DLFLogs_Msg NOT NULL,
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   source VARCHAR2(2048),
   params VARCHAR2(2048));
CREATE GLOBAL TEMPORARY TABLE DLFLogsHelper
  (timeinfo NUMBER,
   uuid VARCHAR2(2048),
   priority INTEGER,
   msg VARCHAR2(2048),
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   source VARCHAR2(2048),
   params VARCHAR2(2048))
ON COMMIT DELETE ROWS;

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

/* Global temporary table to handle output of the processBulkRequest procedure */
CREATE GLOBAL TEMPORARY TABLE getFileIdsForSrsHelper (rowno NUMBER, fileId NUMBER, nsHost VARCHAR(2048))
  ON COMMIT DELETE ROWS;

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

/* Define the service handlers for the appropriate sets of stage request objects */
UPDATE Type2Obj SET svcHandler = 'JobReqSvc' WHERE type IN (35, 40, 44);
UPDATE Type2Obj SET svcHandler = 'PrepReqSvc' WHERE type IN (36, 37, 38);
UPDATE Type2Obj SET svcHandler = 'StageReqSvc' WHERE type IN (39, 42, 95);
UPDATE Type2Obj SET svcHandler = 'QueryReqSvc' WHERE type IN (33, 34, 41, 103, 131, 152, 155, 195);
UPDATE Type2Obj SET svcHandler = 'JobSvc' WHERE type IN (60, 64, 65, 67, 78, 79, 80, 93, 144, 147);
UPDATE Type2Obj SET svcHandler = 'GCSvc' WHERE type IN (73, 74, 83, 142, 149);
UPDATE Type2Obj SET svcHandler = 'BulkStageReqSvc' WHERE type IN (50, 119);

/*********************************************************************/
/* FileSystemsToCheck used to optimise the processing of filesystems */
/* when they change status                                           */
/*********************************************************************/
CREATE TABLE FileSystemsToCheck (FileSystem NUMBER CONSTRAINT PK_FSToCheck_FS PRIMARY KEY, ToBeChecked NUMBER);

/*********************/
/* FileSystem rating */
/*********************/

/* Computes a 'rate' for the filesystem which is an agglomeration
   of weight and fsDeviation. The goal is to be able to classify
   the fileSystems using a single value and to put an index on it */
CREATE OR REPLACE FUNCTION fileSystemRate
(nbReadStreams IN NUMBER,
 nbWriteStreams IN NUMBER)
RETURN NUMBER DETERMINISTIC IS
BEGIN
  RETURN - nbReadStreams - nbWriteStreams;
END;
/

/* FileSystem index based on the rate. */
CREATE INDEX I_FileSystem_Rate
    ON FileSystem(fileSystemRate(nbReadStreams, nbWriteStreams));

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

/* For deleteDiskCopy */
CREATE GLOBAL TEMPORARY TABLE DeleteDiskCopyHelper
  (dcId INTEGER CONSTRAINT PK_DDCHelper_dcId PRIMARY KEY, rc INTEGER)
  ON COMMIT PRESERVE ROWS;

/**********/
/* Repack */
/**********/

/* SQL statements for type StageRepackRequest (not autogenerated any more) */
CREATE TABLE StageRepackRequest
 (flags INTEGER,
  userName VARCHAR2(2048),
  euid NUMBER,
  egid NUMBER,
  mask NUMBER,
  pid NUMBER,
  machine VARCHAR2(2048),
  svcClassName VARCHAR2(2048),
  userTag VARCHAR2(2048),
  reqId VARCHAR2(2048),
  creationTime INTEGER,
  lastModificationTime INTEGER,
  repackVid VARCHAR2(2048) CONSTRAINT NN_StageRepackReq_repackVid NOT NULL,
  id INTEGER CONSTRAINT PK_StageRepackRequest_Id PRIMARY KEY,
  svcClass INTEGER,
  client INTEGER,
  status INTEGER CONSTRAINT NN_StageRepackReq_status NOT NULL,
  fileCount INTEGER CONSTRAINT NN_StageRepackReq_fileCount NOT NULL,
  totalSize INTEGER CONSTRAINT NN_StageRepackReq_totalSize NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

BEGIN
  setObjStatusName('StageRepackRequest', 'status', tconst.REPACK_STARTING, 'REPACK_STARTING');
  setObjStatusName('StageRepackRequest', 'status', tconst.REPACK_ONGOING, 'REPACK_ONGOING');
  setObjStatusName('StageRepackRequest', 'status', tconst.REPACK_FINISHED, 'REPACK_FINISHED');
  setObjStatusName('StageRepackRequest', 'status', tconst.REPACK_FAILED, 'REPACK_FAILED');
  setObjStatusName('StageRepackRequest', 'status', tconst.REPACK_ABORTING, 'REPACK_ABORTING');
  setObjStatusName('StageRepackRequest', 'status', tconst.REPACK_ABORTED, 'REPACK_ABORTED');
  setObjStatusName('StageRepackRequest', 'status', tconst.REPACK_SUBMITTED, 'REPACK_SUBMITTED');
END;
/
ALTER TABLE StageRepackRequest
  ADD CONSTRAINT CK_StageRepackRequest_Status
  CHECK (status IN (0, 1, 2, 3, 4, 5, 6));

CREATE INDEX I_StageRepackRequest_ReqId ON StageRepackRequest (reqId);

/* Temporary table used for listing segments of a tape */
/* efficiently via DB link when repacking              */
CREATE GLOBAL TEMPORARY TABLE RepackTapeSegments
 (fileId NUMBER, blockid RAW(4), fseq NUMBER, segSize NUMBER,
  copyNb NUMBER, fileClass NUMBER, allSegments VARCHAR2(2048))
 ON COMMIT PRESERVE ROWS;

/**********************************/
/* Draining and disk to disk copy */
/**********************************/

/* Creation of the DrainingJob table
 *   - id : unique identifier of the DrainingJob
 *   - userName, euid, egid : identification of the originator of the job
 *   - pid : process id of the originator of the job
 *   - machine : machine where the originator of the job was running
 *   - creationTime : time when the job was created
 *   - lastModificationTime : lest time the job was updated
 *   - fileSystem : id of the concerned filesystem
 *   - status : current status of the job. One of SUBMITTED, STARTING,
 *              RUNNING, FAILED, COMPLETED
 *   - svcClass : the target service class for the draining
 *   - fileMask : indicates which files are concerned by the draining.
 *                One of NOTONTAPE, ALL
 *   - totalFiles, totalBytes : indication of the work to be done. These
 *                numbers are partial and increasing while starting
 *                and then stable while running
 *   - nbFailedBytes/Files, nbSuccessBytes/Files : indication of the
 *                work already done. These counters are updated while running
 *   - userComment : a user comment
 */
CREATE TABLE DrainingJob
  (id             INTEGER CONSTRAINT PK_DrainingJob_Id PRIMARY KEY,
   userName       VARCHAR2(2048) CONSTRAINT NN_DrainingJob_UserName NOT NULL,
   euid           INTEGER CONSTRAINT NN_DrainingJob_Euid NOT NULL,
   egid           INTEGER CONSTRAINT NN_DrainingJob_Egid NOT NULL,
   pid            INTEGER CONSTRAINT NN_DrainingJob_Pid NOT NULL,
   machine        VARCHAR2(2048) CONSTRAINT NN_DrainingJob_Machine NOT NULL,
   creationTime   INTEGER CONSTRAINT NN_DrainingJob_CT NOT NULL,
   lastModificationTime INTEGER CONSTRAINT NN_DrainingJob_LMT NOT NULL,
   status         INTEGER CONSTRAINT NN_DrainingJob_Status NOT NULL,
   fileSystem     INTEGER CONSTRAINT NN_DrainingJob_FileSystem NOT NULL 
                          CONSTRAINT UN_DrainingJob_FileSystem UNIQUE USING INDEX,
   svcClass       INTEGER CONSTRAINT NN_DrainingJob_SvcClass NOT NULL,
   fileMask       INTEGER CONSTRAINT NN_DrainingJob_FileMask NOT NULL,
   totalFiles     INTEGER CONSTRAINT NN_DrainingJob_TotFiles NOT NULL,
   totalBytes     INTEGER CONSTRAINT NN_DrainingJob_TotBytes NOT NULL,
   nbFailedBytes  INTEGER CONSTRAINT NN_DrainingJob_FailedFiles NOT NULL,
   nbFailedFiles  INTEGER CONSTRAINT NN_DrainingJob_FailedBytes NOT NULL,
   nbSuccessBytes INTEGER CONSTRAINT NN_DrainingJob_SuccessBytes NOT NULL,
   nbSuccessFiles INTEGER CONSTRAINT NN_DrainingJob_SuccessFiles NOT NULL,
   userComment    VARCHAR2(2048))
ENABLE ROW MOVEMENT;

BEGIN
  setObjStatusName('DrainingJob', 'status', 0, 'SUBMITTED');
  setObjStatusName('DrainingJob', 'status', 1, 'STARTING');
  setObjStatusName('DrainingJob', 'status', 2, 'RUNNING');
  setObjStatusName('DrainingJob', 'status', 4, 'FAILED');
  setObjStatusName('DrainingJob', 'status', 5, 'FINISHED');
END;
/

ALTER TABLE DrainingJob
  ADD CONSTRAINT FK_DrainingJob_FileSystem
  FOREIGN KEY (fileSystem)
  REFERENCES FileSystem (id);

ALTER TABLE DrainingJob
  ADD CONSTRAINT CK_DrainingJob_Status
  CHECK (status IN (0, 1, 2, 4, 5));

ALTER TABLE DrainingJob
  ADD CONSTRAINT FK_DrainingJob_SvcClass
  FOREIGN KEY (svcClass)
  REFERENCES SvcClass (id);

ALTER TABLE DrainingJob
  ADD CONSTRAINT CK_DrainingJob_FileMask
  CHECK (fileMask IN (0, 1));

CREATE INDEX I_DrainingJob_SvcClass ON DrainingJob (svcClass);

/* Creation of the DrainingErrors table
 *   - drainingJob : identifier of the concerned DrainingJob
 *   - errorMsg : the error that occured
 *   - fileId, nsHost : concerned file
 */
CREATE TABLE DrainingErrors
  (drainingJob  INTEGER CONSTRAINT NN_DrainingErrors_DJ NOT NULL,
   errorMsg     VARCHAR2(2048) CONSTRAINT NN_DrainingErrors_ErrorMsg NOT NULL,
   fileId       INTEGER CONSTRAINT NN_DrainingErrors_FileId NOT NULL,
   nsHost       VARCHAR2(2048) CONSTRAINT NN_DrainingErrors_NsHost NOT NULL)
ENABLE ROW MOVEMENT;

CREATE INDEX I_DrainingErrors_DJ ON DrainingErrors (drainingJob);

ALTER TABLE DrainingErrors
  ADD CONSTRAINT FK_DrainingErrors_DJ
  FOREIGN KEY (drainingJob)
  REFERENCES DrainingJob (id);

/* Definition of the Disk2DiskCopyJob table. Each line is a disk2diskCopy job to process
 *   id : unique DB identifier for this job
 *   transferId : unique identifier for the transfer associated to this job
 *   creationTime : creation time of this item, allows to compute easily waiting times
 *   status : status of the job (PENDING, SCHEDULED, RUNNING) 
 *   retryCounter : number of times the copy was attempted
 *   ouid : the originator user id
 *   ogid : the originator group id
 *   castorFile : the concerned file
 *   nsOpenTime : the nsOpenTime of the castorFile when this job was created
 *                Allows to detect if the file has been overwritten during replication
 *   destSvcClass : the destination service class
 *   replicationType : the type of replication involved (user, internal or draining)
 *   replacedDcId : in case of draining, the replaced diskCopy to be dropped
 *   destDcId : the destination diskCopy
 *   drainingJob : the draining job behind this d2dJob. Not NULL only if replicationType is DRAINING'
 */
CREATE TABLE Disk2DiskCopyJob
  (id NUMBER CONSTRAINT PK_Disk2DiskCopyJob_Id PRIMARY KEY 
             CONSTRAINT NN_Disk2DiskCopyJob_Id NOT NULL,
   transferId VARCHAR2(2048) CONSTRAINT NN_Disk2DiskCopyJob_TId NOT NULL,
   creationTime INTEGER CONSTRAINT NN_Disk2DiskCopyJob_CTime NOT NULL,
   status INTEGER CONSTRAINT NN_Disk2DiskCopyJob_Status NOT NULL,
   retryCounter INTEGER DEFAULT 0 CONSTRAINT NN_Disk2DiskCopyJob_retryCnt NOT NULL,
   ouid INTEGER CONSTRAINT NN_Disk2DiskCopyJob_ouid NOT NULL,
   ogid INTEGER CONSTRAINT NN_Disk2DiskCopyJob_ogid NOT NULL,
   castorFile INTEGER CONSTRAINT NN_Disk2DiskCopyJob_CastorFile NOT NULL,
   nsOpenTime INTEGER CONSTRAINT NN_Disk2DiskCopyJob_NSOpenTime NOT NULL,
   destSvcClass INTEGER CONSTRAINT NN_Disk2DiskCopyJob_dstSC NOT NULL,
   replicationType INTEGER CONSTRAINT NN_Disk2DiskCopyJob_Type NOT NULL,
   replacedDcId INTEGER,
   destDcId INTEGER CONSTRAINT NN_Disk2DiskCopyJob_DCId NOT NULL,
   drainingJob INTEGER)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE INDEX I_Disk2DiskCopyJob_Tid ON Disk2DiskCopyJob(transferId);
CREATE INDEX I_Disk2DiskCopyJob_CfId ON Disk2DiskCopyJob(CastorFile);
CREATE INDEX I_Disk2DiskCopyJob_CT_Id ON Disk2DiskCopyJob(creationTime, id);
CREATE INDEX I_Disk2DiskCopyJob_drainJob ON Disk2DiskCopyJob(drainingJob);
CREATE INDEX I_Disk2DiskCopyJob_SC_type ON Disk2DiskCopyJob(destSvcClass, replicationType);
CREATE INDEX I_Disk2DiskCopyJob_status_CT ON Disk2DiskCopyJob(status, creationTime);
BEGIN
  -- PENDING status is when a Disk2DiskCopyJob is created
  -- It is immediately candidate for being scheduled
  setObjStatusName('Disk2DiskCopyJob', 'status', dconst.DISK2DISKCOPYJOB_PENDING, 'DISK2DISKCOPYJOB_PENDING');
  -- SCHEDULED status is when the Disk2DiskCopyJob has been scheduled and is not yet started
  setObjStatusName('Disk2DiskCopyJob', 'status', dconst.DISK2DISKCOPYJOB_SCHEDULED, 'DISK2DISKCOPYJOB_SCHEDULED');
  -- RUNNING status is when the disk to disk copy is ongoing
  setObjStatusName('Disk2DiskCopyJob', 'status', dconst.DISK2DISKCOPYJOB_RUNNING, 'DISK2DISKCOPYJOB_RUNNING');
  -- USER replication type is when replication is triggered by the user
  setObjStatusName('Disk2DiskCopyJob', 'replicationType', dconst.REPLICATIONTYPE_USER, 'REPLICATIONTYPE_USER');
  -- INTERNAL replication type is when replication is triggered internally (e.g. dual copy disk pools)
  setObjStatusName('Disk2DiskCopyJob', 'replicationType', dconst.REPLICATIONTYPE_INTERNAL, 'REPLICATIONTYPE_INTERNAL');
  -- DRAINING replication type is when replication is triggered by a drain operation
  setObjStatusName('Disk2DiskCopyJob', 'replicationType', dconst.REPLICATIONTYPE_DRAINING, 'REPLICATIONTYPE_DRAINING');
  -- REBALANCE replication type is when replication is triggered by a rebalancing of data on different filesystems
  setObjStatusName('Disk2DiskCopyJob', 'replicationType', dconst.REPLICATIONTYPE_REBALANCE, 'REPLICATIONTYPE_REBALANCE');
END;
/
ALTER TABLE Disk2DiskCopyJob ADD CONSTRAINT FK_Disk2DiskCopyJob_CastorFile
  FOREIGN KEY (castorFile) REFERENCES CastorFile(id);
ALTER TABLE Disk2DiskCopyJob ADD CONSTRAINT FK_Disk2DiskCopyJob_SvcClass
  FOREIGN KEY (destSvcClass) REFERENCES SvcClass(id);
ALTER TABLE Disk2DiskCopyJob ADD CONSTRAINT FK_Disk2DiskCopyJob_DrainJob
  FOREIGN KEY (drainingJob) REFERENCES DrainingJob(id);
ALTER TABLE Disk2DiskCopyJob
  ADD CONSTRAINT CK_Disk2DiskCopyJob_Status
  CHECK (status IN (0, 1, 2));
ALTER TABLE Disk2DiskCopyJob
  ADD CONSTRAINT CK_Disk2DiskCopyJob_type
  CHECK (replicationType IN (0, 1, 2));

/*****************/
/* logon trigger */
/*****************/

/* allows the call of new versions of remote procedures when the signature is matching */
CREATE OR REPLACE TRIGGER tr_RemoteDepSignature AFTER LOGON ON SCHEMA
BEGIN
  EXECUTE IMMEDIATE 'ALTER SESSION SET REMOTE_DEPENDENCIES_MODE=SIGNATURE';
END;
/
/*******************************************************************
 *
 *
 * This file contains some common PL/SQL utilities for the stager database.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* Returns a time interval in seconds */
CREATE OR REPLACE FUNCTION getSecs(startTime IN TIMESTAMP, endTime IN TIMESTAMP) RETURN NUMBER IS
BEGIN
  RETURN TRUNC(EXTRACT(SECOND FROM (endTime - startTime)), 6);
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

/* Function to return the name of a given file class */
CREATE OR REPLACE FUNCTION getFileClassName(fileClassId NUMBER) RETURN VARCHAR2 IS
  varFileClassName VARCHAR2(2048);
BEGIN
  SELECT name INTO varFileClassName FROM FileClass WHERE id = fileClassId;
  RETURN varFileClassName;
END;
/

/* Function to return the name of a given service class */
CREATE OR REPLACE FUNCTION getSvcClassName(svcClassId NUMBER) RETURN VARCHAR2 IS
  varSvcClassName VARCHAR2(2048);
BEGIN
  SELECT name INTO varSvcClassName FROM SvcClass WHERE id = svcClassId;
  RETURN varSvcClassName;
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

/* Function to concatenate values into a string using a specified delimiter.
 * If no delimiter is specified the default is ','.
 */
CREATE OR REPLACE FUNCTION strConcat(p_cursor SYS_REFCURSOR, p_del VARCHAR2 := ',')
RETURN VARCHAR2 IS
  l_value   VARCHAR2(2048);
  l_result  VARCHAR2(2048);
BEGIN
  LOOP
    FETCH p_cursor INTO l_value;
    EXIT WHEN p_cursor%NOTFOUND;
    IF l_result IS NOT NULL THEN
      l_result := l_result || p_del;
    END IF;
    l_result := l_result || l_value;
  END LOOP;
  RETURN l_result;
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
  SELECT /*+ INDEX(DiskCopy I_DiskCopy_FileSystem) */ 1 INTO rtn
    FROM DiskCopy, FileSystem, DiskServer
   WHERE DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.name = diskServerName
     AND (FileSystem.mountpoint = mountPointName 
      OR  length(mountPointName) IS NULL)
     AND rownum < 2;
  RETURN rtn;
END;
/

/* PL/SQL method deleting disk2diskCopyJobs jobs of a castorfile */
CREATE OR REPLACE PROCEDURE deleteDisk2DiskCopyJobs(inCfId INTEGER) AS
BEGIN
  DELETE FROM Disk2DiskCopyJob WHERE castorfile = inCfId;
END;
/

/* PL/SQL method deleting migration jobs of a castorfile */
CREATE OR REPLACE PROCEDURE deleteMigrationJobs(cfId NUMBER) AS
BEGIN
  DELETE /*+ INDEX (MigrationJob I_MigrationJob_CFVID) */
    FROM MigrationJob
   WHERE castorfile = cfId;
  DELETE /*+ INDEX (MigratedSegment I_MigratedSegment_CFCopyNbVID) */
    FROM MigratedSegment
   WHERE castorfile = cfId;
END;
/

/* PL/SQL method deleting migration jobs of a castorfile that was being recalled */
CREATE OR REPLACE PROCEDURE deleteMigrationJobsForRecall(cfId NUMBER) AS
BEGIN
  -- delete migration jobs waiting on this recall
  DELETE /*+ INDEX (MigrationJob I_MigrationJob_CFVID) */
    FROM MigrationJob
   WHERE castorFile = cfId AND status = tconst.MIGRATIONJOB_WAITINGONRECALL;
  -- delete migrated segments if no migration jobs remain
  DECLARE
    unused NUMBER;
  BEGIN
    SELECT /*+ INDEX_RS_ASC(MigrationJob I_MigrationJob_CFCopyNb) */ castorFile INTO unused
      FROM MigrationJob WHERE castorFile = cfId AND ROWNUM < 2;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    DELETE FROM MigratedSegment WHERE castorfile = cfId;
  END;
END;
/

/* PL/SQL method deleting recall jobs of a castorfile */
CREATE OR REPLACE PROCEDURE deleteRecallJobs(cfId NUMBER) AS
BEGIN
  -- Loop over the recall jobs
  DELETE FROM RecallJob WHERE castorfile = cfId;
  -- deal with potential waiting migrationJobs
  deleteMigrationJobsForRecall(cfId);
END;
/

/* PL/SQL method to delete a CastorFile only when no DiskCopy, no MigrationJob and no RecallJob are left for it */
/* Internally used in filesDeletedProc, putFailedProc and deleteOutOfDateDiskCopies */
CREATE OR REPLACE PROCEDURE deleteCastorFile(cfId IN NUMBER) AS
  nb NUMBER;
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -2292);
BEGIN
  -- First try to lock the castorFile
  SELECT id INTO nb FROM CastorFile
   WHERE id = cfId FOR UPDATE NOWAIT;

  -- See whether pending SubRequests exist
  SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
    FROM SubRequest
   WHERE castorFile = cfId
     AND status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 12, 13);   -- All but FINISHED, FAILED_FINISHED, ARCHIVED
  -- If any SubRequest, give up
  IF nb = 0 THEN
    DECLARE
      fid NUMBER;
      fc NUMBER;
      nsh VARCHAR2(2048);
    BEGIN
      -- Delete the CastorFile
      -- this will raise a constraint violation if any DiskCopy, MigrationJob, RecallJob or Disk2DiskCopyJob
      -- still exists for this CastorFile. It is caught and we give up with the deletion in such a case.
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
        INSERT INTO FilesDeletedProcOutput (fileId, nsHost) VALUES (fid, nsh);
      END IF;
    END;
  END IF;
EXCEPTION
  WHEN CONSTRAINT_VIOLATED THEN
    -- ignore, this means we still have some DiskCopy or Job around
    NULL;
  WHEN NO_DATA_FOUND THEN
    -- ignore, this means that the castorFile did not exist.
    -- There is thus no way to find out whether to remove the
    -- file from the nameserver. For safety, we thus keep it
    NULL;
  WHEN LockError THEN
    -- ignore, somebody else is dealing with this castorFile
    NULL;
END;
/

/* automatic logging procedure. The logs are then processed by the stager and sent to the rsyslog streams.
   Note that the log will be commited at the same time as the rest of the transaction */
CREATE OR REPLACE PROCEDURE logToDLFWithTime(logTime NUMBER,
                                             uuid VARCHAR2,
                                             priority INTEGER,
                                             msg VARCHAR2,
                                             fileId NUMBER,
                                             nsHost VARCHAR2,
                                             source VARCHAR2,
                                             params VARCHAR2) AS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  INSERT INTO DLFLogs (timeinfo, uuid, priority, msg, fileId, nsHost, source, params)
         VALUES (logTime, uuid, priority, msg, fileId, nsHost, source, params);
  COMMIT;
END;
/

/* automatic logging procedure. The logs are then processed by the stager and sent to the rsyslog streams.
   Note that the log will be commited at the same time as the rest of the transaction */
CREATE OR REPLACE PROCEDURE logToDLF(uuid VARCHAR2,
                                     priority INTEGER,
                                     msg VARCHAR2,
                                     fileId NUMBER,
                                     nsHost VARCHAR2,
                                     source VARCHAR2,
                                     params VARCHAR2) AS
BEGIN
  logToDLFWithTime(getTime(), uuid, priority, msg, fileId, nsHost, source, params);
END;
/

/* Small utility function to convert an hexadecimal string (8 digits) into a RAW(4) type */
CREATE OR REPLACE FUNCTION strToRaw4(v VARCHAR2) RETURN RAW IS
BEGIN
  RETURN hexToRaw(ltrim(to_char(to_number(v, 'XXXXXXXX'), '0XXXXXXX')));
END;
/

/* A wrapper to run DB jobs and catch+log any exception they may throw */
CREATE OR REPLACE PROCEDURE startDbJob(jobCode VARCHAR2, source VARCHAR2) AS
BEGIN
  EXECUTE IMMEDIATE jobCode;
EXCEPTION WHEN OTHERS THEN
  logToDLF(NULL, dlf.LVL_ALERT, dlf.DBJOB_UNEXPECTED_EXCEPTION, 0, '', source,
    'jobCode="'|| jobCode ||'" errorCode=' || to_char(SQLCODE) ||' errorMessage="' || SQLERRM
    ||'" stackTrace="' || dbms_utility.format_error_backtrace ||'"');
END;
/
/*******************************************************************
 *
 *
 * PL/SQL code for permission and B/W list handling
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/


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
        INSERT INTO RemovePrivilegeTmpTable (svcClass, euid, egid, reqType)
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
 * PL/SQL code for Repack
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/


/* PL/SQL method to process bulk Repack abort requests */
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
  varCreationTime NUMBER;
  rIpAddress INTEGER;
  rport INTEGER;
  rReqUuid VARCHAR2(2048);
  varVID VARCHAR2(10);
  varStartTime NUMBER;
  varOldStatus INTEGER;
BEGIN
  -- lock and mark the repack request as being aborted
  SELECT status, repackVID INTO varOldStatus, varVID
    FROM StageRepackRequest
   WHERE reqId = parentUUID
   FOR UPDATE;
  IF varOldStatus = tconst.REPACK_SUBMITTED THEN
    -- the request was just submitted, simply drop it from the queue
    DELETE FROM StageRepackRequest WHERE reqId = parentUUID;
    logToDLF(parentUUID, dlf.LVL_SYSTEM, dlf.REPACK_ABORTED, 0, '', 'repackd', 'TPVID=' || varVID);
    COMMIT;
    -- and return an empty cursor - not that nice...
    OPEN rSubResults FOR
      SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
    RETURN;
  END IF;
  UPDATE StageRepackRequest SET status = tconst.REPACK_ABORTING
   WHERE reqId = parentUUID;
  COMMIT;  -- so to make the status change visible
  BEGIN
    varStartTime := getTime();
    logToDLF(parentUUID, dlf.LVL_SYSTEM, dlf.REPACK_ABORTING, 0, '', 'repackd', 'TPVID=' || varVID);
    -- get unique ids for the request and the client and get current time
    SELECT ids_seq.nextval INTO reqId FROM DUAL;
    SELECT ids_seq.nextval INTO clientId FROM DUAL;
    varCreationTime := getTime();
    -- insert the request itself
    INSERT INTO StageAbortRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName,
      userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
    VALUES (0, userName, euid, egid, 0, pid, machine, '', '', uuidgen(),
      varCreationTime, varCreationTime, parentUUID, reqId, 0, clientId);
    -- insert the client information
    INSERT INTO Client (ipAddress, port, version, secure, id)
    VALUES (clientIP, 0, 0, 0, clientId);
    -- process the abort
    processBulkAbort(reqId, rIpAddress, rport, rReqUuid);
    -- mark the repack request as ABORTED
    UPDATE StageRepackRequest SET status = tconst.REPACK_ABORTED WHERE reqId = parentUUID;
    logToDLF(parentUUID, dlf.LVL_SYSTEM, dlf.REPACK_ABORTED, 0, '', 'repackd',
      'TPVID=' || varVID || ' elapsedTime=' || to_char(getTime() - varStartTime));
    -- return all results
    OPEN rSubResults FOR
      SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
  EXCEPTION WHEN OTHERS THEN
    -- Something went wrong when aborting: log and fail
    UPDATE StageRepackRequest SET status = tconst.REPACK_FAILED WHERE reqId = parentUUID;
    logToDLF(parentUUID, dlf.LVL_ERROR, dlf.REPACK_ABORTED_FAILED, 0, '', 'repackd', 'TPVID=' || varVID
      || ' errorMessage="' || SQLERRM || '" stackTrace="' || dbms_utility.format_error_backtrace ||'"');
    COMMIT;
    RAISE;
  END;
END;
/


/* PL/SQL method to submit a Repack request. This method returns immediately
   and the processing is done asynchronously by a DB job. */
CREATE OR REPLACE PROCEDURE submitRepackRequest
  (inMachine IN VARCHAR2,
   inEuid IN INTEGER,
   inEgid IN INTEGER,
   inPid IN INTEGER,
   inUserName IN VARCHAR2,
   inSvcClassName IN VARCHAR2,
   clientIP IN INTEGER,
   reqVID IN VARCHAR2) AS
  varClientId INTEGER;
  varSvcClassId INTEGER;
  varReqUUID VARCHAR2(36);
BEGIN
  -- do prechecks and get the service class
  varSvcClassId := insertPreChecks(inEuid, inEgid, inSvcClassName, 119);
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP, 0, 0, 0, ids_seq.nextval)
  RETURNING id INTO varClientId;
  varReqUUID := uuidGen();
  -- insert the request in status SUBMITTED, the SubRequests will be created afterwards
  INSERT INTO StageRepackRequest (reqId, machine, euid, egid, pid, userName, svcClassName, svcClass, client, repackVID,
    userTag, flags, mask, creationTime, lastModificationTime, status, fileCount, totalSize, id)
  VALUES (varReqUUID, inMachine, inEuid, inEgid, inPid, inUserName, inSvcClassName, varSvcClassId, varClientId, reqVID,
    varReqUUID, 0, 0, getTime(), getTime(), tconst.REPACK_SUBMITTED, 0, 0, ids_seq.nextval);
  COMMIT;
  logToDLF(varReqUUID, dlf.LVL_SYSTEM, dlf.REPACK_SUBMITTED, 0, '', 'repackd', 'TPVID=' || reqVID);
END;
/

/* PL/SQL method to handle a Repack request. This is performed as part of a DB job. */
CREATE OR REPLACE PROCEDURE handleRepackRequest(inReqUUID IN VARCHAR2,
                                                outNbFilesProcessed OUT INTEGER, outNbFailures OUT INTEGER) AS
  varEuid INTEGER;
  varEgid INTEGER;
  svcClassId INTEGER;
  varRepackVID VARCHAR2(10);
  varCreationTime NUMBER;
  varReqId INTEGER;
  cfId INTEGER;
  dcId INTEGER;
  repackProtocol VARCHAR2(2048);
  nsHostName VARCHAR2(2048);
  lastKnownFileName VARCHAR2(2048);
  varRecallGroupId INTEGER;
  varRecallGroupName VARCHAR2(2048);
  unused INTEGER;
  firstCF boolean := True;
  isOngoing boolean := False;
  varTotalSize INTEGER := 0;
BEGIN
  UPDATE StageRepackRequest SET status = tconst.REPACK_STARTING
   WHERE reqId = inReqUUID
  RETURNING id, euid, egid, svcClass, repackVID
    INTO varReqId, varEuid, varEgid, svcClassId, varRepackVID;
  -- commit so that the status change is visible, even if the request is empty for the moment
  COMMIT;
  outNbFilesProcessed := 0;
  outNbFailures := 0;
  -- Check which protocol should be used for writing files to disk
  repackProtocol := getConfigOption('Repack', 'Protocol', 'rfio');
  -- name server host name
  nsHostName := getConfigOption('stager', 'nsHost', '');
  -- creation time given to new CastorFile entries
  varCreationTime := getTime();
  -- find out the recallGroup to be used for this request
  getRecallGroup(varEuid, varEgid, varRecallGroupId, varRecallGroupName);
  -- Get the list of files to repack from the NS DB via DBLink and store them in memory
  -- in a temporary table. We do that so that we do not keep an open cursor for too long
  -- in the nameserver DB
  INSERT INTO RepackTapeSegments (fileId, blockId, fseq, segSize, copyNb, fileClass, allSegments)
    (SELECT s_fileid, blockid, fseq, segSize,
            copyno, fileclass,
            (SELECT LISTAGG(TO_CHAR(oseg.copyno)||','||oseg.vid, ',')
             WITHIN GROUP (ORDER BY copyno)
               FROM Cns_Seg_Metadata@remotens oseg
              WHERE oseg.s_fileid = seg.s_fileid
                AND oseg.s_status = '-'
              GROUP BY oseg.s_fileid)
       FROM Cns_Seg_Metadata@remotens seg, Cns_File_Metadata@remotens fileEntry
      WHERE seg.vid = varRepackVID
        AND seg.s_fileid = fileEntry.fileid
        AND seg.s_status = '-'
        AND fileEntry.status != 'D');
  FOR segment IN (SELECT * FROM RepackTapeSegments) LOOP
    DECLARE
      varSubreqId INTEGER;
      varSubreqUUID VARCHAR2(2048);
      varSrStatus INTEGER := dconst.SUBREQUEST_REPACK;
      varMjStatus INTEGER := tconst.MIGRATIONJOB_PENDING;
      varNbCopies INTEGER;
      varSrErrorCode INTEGER := 0;
      varSrErrorMsg VARCHAR2(2048) := NULL;
      varWasRecalled NUMBER;
      varMigrationTriggered BOOLEAN := False;
    BEGIN
      outNbFilesProcessed := outNbFilesProcessed + 1;
      varTotalSize := varTotalSize + segment.segSize;
      -- Commit from time to time
      IF MOD(outNbFilesProcessed, 1000) = 0 THEN
        COMMIT;
        firstCF := TRUE;
      END IF;
      -- lastKnownFileName we will have in the DB
      lastKnownFileName := CONCAT('Repack_', TO_CHAR(segment.fileid));
      -- find the Castorfile (and take a lock on it)
      DECLARE
        locked EXCEPTION;
        PRAGMA EXCEPTION_INIT (locked, -54);
      BEGIN
        -- This may raise a Locked exception as we do not want to wait for locks (except on first file).
        -- In such a case, we commit what we've done so far and retry this file, this time waiting for the lock.
        -- The reason for such a complex code is to avoid commiting each file separately, as it would be
        -- too heavy. On the other hand, we still need to avoid dead locks.
        -- Note that we pass 0 for the subrequest id, thus the subrequest will not be attached to the
        -- CastorFile. We actually attach it when we create it.
        selectCastorFileInternal(segment.fileid, nsHostName, segment.fileclass,
                                 segment.segSize, lastKnownFileName, 0, varCreationTime, firstCF, cfid, unused);
        firstCF := FALSE;
      EXCEPTION WHEN locked THEN
        -- commit what we've done so far
        COMMIT;
        -- And lock the castorfile (waiting this time)
        selectCastorFileInternal(segment.fileid, nsHostName, segment.fileclass,
                                 segment.segSize, lastKnownFileName, 0, varCreationTime, TRUE, cfid, unused);
      END;
      -- create  subrequest for this file.
      -- Note that the svcHandler is not set. It will actually never be used as repacks are handled purely in PL/SQL
      INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, status, request, getNextStatus, reqType)
      VALUES (0, lastKnownFileName, repackProtocol, segment.segSize, 0, uuidGen(), 0, 0, varCreationTime, varCreationTime, 1, 0, '', NULL, 'NotNullNeeded', ids_seq.nextval, 0, cfId, dconst.SUBREQUEST_START, varReqId, 0, 119)
      RETURNING id, subReqId INTO varSubreqId, varSubreqUUID;
      -- if the file is being overwritten, fail
      SELECT count(DiskCopy.id) INTO varNbCopies
        FROM DiskCopy
       WHERE DiskCopy.castorfile = cfId
         AND DiskCopy.status = dconst.DISKCOPY_STAGEOUT;
      IF varNbCopies > 0 THEN
        varSrStatus := dconst.SUBREQUEST_FAILED;
        varSrErrorCode := serrno.EBUSY;
        varSrErrorMsg := 'File is currently being overwritten';
        outNbFailures := outNbFailures + 1;
      ELSE
        -- find out whether this file is already on disk
        SELECT /*+ INDEX(DC I_DiskCopy_CastorFile) */ count(DC.id)
          INTO varNbCopies
          FROM DiskCopy DC, FileSystem, DiskServer
         WHERE DC.castorfile = cfId
           AND DC.fileSystem = FileSystem.id
           AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
           AND FileSystem.diskserver = DiskServer.id
           AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
           AND DiskServer.hwOnline = 1
           AND DC.status = dconst.DISKCOPY_VALID;
        IF varNbCopies = 0 THEN
          -- find out whether this file is already being recalled
          SELECT count(*) INTO varWasRecalled FROM RecallJob WHERE castorfile = cfId AND ROWNUM < 2;
          IF varWasRecalled = 0 THEN
            -- trigger recall
            triggerRepackRecall(cfId, segment.fileid, nsHostName, segment.blockid,
                                segment.fseq, segment.copyNb, varEuid, varEgid,
                                varRecallGroupId, svcClassId, varRepackVID, segment.segSize,
                                segment.fileclass, segment.allSegments, inReqUUID, varSubreqUUID, varRecallGroupName);
          END IF;
          -- file is being recalled
          varSrStatus := dconst.SUBREQUEST_WAITTAPERECALL;
          isOngoing := TRUE;
        END IF;
        -- deal with migrations
        IF varSrStatus = dconst.SUBREQUEST_WAITTAPERECALL THEN
          varMJStatus := tconst.MIGRATIONJOB_WAITINGONRECALL;
        END IF;
        DECLARE
          noValidCopyNbFound EXCEPTION;
          PRAGMA EXCEPTION_INIT (noValidCopyNbFound, -20123);
          noMigrationRoute EXCEPTION;
          PRAGMA EXCEPTION_INIT (noMigrationRoute, -20100);
        BEGIN
          triggerRepackMigration(cfId, varRepackVID, segment.fileid, segment.copyNb, segment.fileclass,
                                 segment.segSize, segment.allSegments, varMJStatus, varMigrationTriggered);
          IF varMigrationTriggered THEN
            -- update CastorFile tapeStatus
            UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_NOTONTAPE WHERE id = cfId;
          END IF;
          isOngoing := True;
        EXCEPTION WHEN noValidCopyNbFound OR noMigrationRoute THEN
          -- cleanup recall part if needed
          IF varWasRecalled = 0 THEN
            DELETE FROM RecallJob WHERE castorFile = cfId;
          END IF;
          -- fail SubRequest
          varSrStatus := dconst.SUBREQUEST_FAILED;
          varSrErrorCode := serrno.EINVAL;
          varSrErrorMsg := SQLERRM;
          outNbFailures := outNbFailures + 1;
        END;
      END IF;
      -- update SubRequest
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
         SET status = varSrStatus,
             errorCode = varSrErrorCode,
             errorMessage = varSrErrorMsg
       WHERE id = varSubreqId;
    EXCEPTION WHEN OTHERS THEN
      -- something went wrong: log "handleRepackRequest: unexpected exception caught"
      outNbFailures := outNbFailures + 1;
      varSrErrorMsg := 'Oracle error caught : ' || SQLERRM;
      logToDLF(NULL, dlf.LVL_ERROR, dlf.REPACK_UNEXPECTED_EXCEPTION, segment.fileId, nsHostName, 'repackd',
        'errorCode=' || to_char(SQLCODE) ||' errorMessage="' || varSrErrorMsg
        ||'" stackTrace="' || dbms_utility.format_error_backtrace ||'"');
      -- cleanup and fail SubRequest
      IF varWasRecalled = 0 THEN
        DELETE FROM RecallJob WHERE castorFile = cfId;
      END IF;
      IF varMigrationTriggered THEN
        DELETE FROM MigrationJob WHERE castorFile = cfId;
        DELETE FROM MigratedSegment WHERE castorFile = cfId;
      END IF;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.SEINTERNAL,
             errorMessage = varSrErrorMsg
       WHERE id = varSubreqId;
    END;
  END LOOP;
  -- cleanup RepackTapeSegments
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RepackTapeSegments';
  -- update status of the RepackRequest
  IF isOngoing THEN
    UPDATE StageRepackRequest
       SET status = tconst.REPACK_ONGOING,
           fileCount = outNbFilesProcessed,
           totalSize = varTotalSize
     WHERE StageRepackRequest.id = varReqId;
  ELSE
    IF outNbFailures > 0 THEN
      UPDATE StageRepackRequest
         SET status = tconst.REPACK_FAILED,
             fileCount = outNbFilesProcessed,
             totalSize = varTotalSize
       WHERE StageRepackRequest.id = varReqId;
    ELSE
      -- CASE of an 'empty' repack : the tape had no files at all
      UPDATE StageRepackRequest
         SET status = tconst.REPACK_FINISHED,
             fileCount = 0,
             totalSize = 0
       WHERE StageRepackRequest.id = varReqId;
    END IF;
  END IF;
  COMMIT;
END;
/

/* PL/SQL procedure implementing triggerRepackMigration */
CREATE OR REPLACE PROCEDURE triggerRepackMigration
(cfId IN INTEGER, vid IN VARCHAR2, fileid IN INTEGER, copyNb IN INTEGER,
 fileclass IN INTEGER, fileSize IN INTEGER, allSegments IN VARCHAR2,
 inMJStatus IN INTEGER, migrationTriggered OUT boolean) AS
  varMjId INTEGER;
  varNb INTEGER;
  varDestCopyNb INTEGER;
  varNbCopies INTEGER;
  varAllSegments strListTable;
BEGIN
  -- check whether we already have a migrationJob for this copy of the file
  SELECT id INTO varMjId
    FROM MigrationJob
   WHERE castorFile = cfId
     AND destCopyNb = copyNb;
  -- we have a migrationJob for this copy ! This means that the file has been overwritten
  -- and the new version is about to go to tape. We thus don't have anything to do
  -- for this file
  migrationTriggered := False;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no migration job for this copyNb, we can proceed
  -- first let's parse the list of all segments
  SELECT * BULK COLLECT INTO varAllSegments
    FROM TABLE(strTokenizer(allSegments));
  DECLARE
    varAllCopyNbs "numList" := "numList"();
    varAllVIDs strListTable := strListTable();
  BEGIN
    FOR i IN varAllSegments.FIRST .. varAllSegments.LAST/2 LOOP
      varAllCopyNbs.EXTEND;
      varAllCopyNbs(i) := TO_NUMBER(varAllSegments(2*i-1));
      varAllVIDs.EXTEND;
      varAllVIDs(i) := varAllSegments(2*i);
    END LOOP;
    -- find the new copy number to be used. This is the minimal one
    -- that is lower than the allowed number of copies and is not
    -- already used by an ongoing migration or by a valid migrated copy
    SELECT nbCopies INTO varNbCopies FROM FileClass WHERE classId = fileclass;
    FOR i IN 1 .. varNbCopies LOOP
      -- if we are reusing the original copy number, it's ok
      IF i = copyNb THEN
        varDestCopyNb := i;
      ELSE
        BEGIN
          -- check whether this copy number is already in use by a valid copy
          SELECT * INTO varNb FROM TABLE(varAllCopyNbs)
           WHERE COLUMN_VALUE = i;
          -- this copy number is in use, go to next one
        EXCEPTION WHEN NO_DATA_FOUND THEN
          BEGIN
            -- check whether this copy number is in use by an ongoing migration
            SELECT destCopyNb INTO varNb FROM MigrationJob WHERE castorFile = cfId AND destCopyNb = i;
            -- this copy number is in use, go to next one
          EXCEPTION WHEN NO_DATA_FOUND THEN
            -- copy number is not in use, we take it
            varDestCopyNb := i;
            EXIT;
          END;
        END;
      END IF;
    END LOOP;
    IF varDestCopyNb IS NULL THEN
      RAISE_APPLICATION_ERROR (-20123,
        'Unable to find a valid copy number for the migration of file ' ||
        TO_CHAR (fileid) || ' in triggerRepackMigration. ' ||
        'The file probably has too many valid copies.');
    END IF;
    -- create new migration
    initMigration(cfId, fileSize, vid, copyNb, varDestCopyNb, inMJStatus);
    -- create migrated segments for the existing segments if there are none
    SELECT /*+ INDEX_RS_ASC (MigratedSegment I_MigratedSegment_CFCopyNbVID) */ count(*) INTO varNb
      FROM MigratedSegment
     WHERE castorFile = cfId;
    IF varNb = 0 THEN
      FOR i IN varAllCopyNbs.FIRST .. varAllCopyNbs.LAST LOOP
        INSERT INTO MigratedSegment (castorFile, copyNb, VID)
        VALUES (cfId, varAllCopyNbs(i), varAllVIDs(i));
      END LOOP;
    END IF;
    -- all is fine, migration was triggered
    migrationTriggered := True;
  END;
END;
/

/* PL/SQL procedure implementing triggerRepackRecall
 * this triggers a recall in the repack context
 */
CREATE OR REPLACE PROCEDURE triggerRepackRecall
(inCfId IN INTEGER, inFileId IN INTEGER, inNsHost IN VARCHAR2, inBlock IN RAW,
 inFseq IN INTEGER, inCopynb IN INTEGER, inEuid IN INTEGER, inEgid IN INTEGER,
 inRecallGroupId IN INTEGER, inSvcClassId IN INTEGER, inVid IN VARCHAR2, inFileSize IN INTEGER,
 inFileClass IN INTEGER, inAllSegments IN VARCHAR2, inReqUUID IN VARCHAR2,
 inSubReqUUID IN VARCHAR2, inRecallGroupName IN VARCHAR2) AS
  varLogParam VARCHAR2(2048);
  varAllCopyNbs "numList" := "numList"();
  varAllVIDs strListTable := strListTable();
  varAllSegments strListTable;
  varFileClassId INTEGER;
BEGIN
  -- create recallJob for the given VID, copyNb, etc.
  INSERT INTO RecallJob (id, castorFile, copyNb, recallGroup, svcClass, euid, egid,
                         vid, fseq, status, fileSize, creationTime, blockId, fileTransactionId)
  VALUES (ids_seq.nextval, inCfId, inCopynb, inRecallGroupId, inSvcClassId,
          inEuid, inEgid, inVid, inFseq, tconst.RECALLJOB_PENDING, inFileSize, getTime(),
          inBlock, NULL);
  -- log "created new RecallJob"
  varLogParam := 'SUBREQID=' || inSubReqUUID || ' RecallGroup=' || inRecallGroupName;
  logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.RECALL_CREATING_RECALLJOB, inFileId, inNsHost, 'stagerd',
           varLogParam || ' fileClass=' || TO_CHAR(inFileClass) || ' copyNb=' || TO_CHAR(inCopynb)
           || ' TPVID=' || inVid || ' fseq=' || TO_CHAR(inFseq) || ' FileSize=' || TO_CHAR(inFileSize));
  -- create missing segments if needed
  SELECT * BULK COLLECT INTO varAllSegments
    FROM TABLE(strTokenizer(inAllSegments));
  FOR i IN 1 .. varAllSegments.COUNT/2 LOOP
    varAllCopyNbs.EXTEND;
    varAllCopyNbs(i) := TO_NUMBER(varAllSegments(2*i-1));
    varAllVIDs.EXTEND;
    varAllVIDs(i) := varAllSegments(2*i);
  END LOOP;
  SELECT id INTO varFileClassId
    FROM FileClass WHERE classId = inFileClass;
  createMJForMissingSegments(inCfId, inFileSize, varFileClassId, varAllCopyNbs,
                             varAllVIDs, inFileId, inNsHost, varLogParam);
END;
/


/* PL/SQL procedure implementing repackManager, the repack job
   that really handles all repack requests */
CREATE OR REPLACE PROCEDURE repackManager AS
  varReqUUID VARCHAR2(36);
  varRepackVID VARCHAR2(10);
  varStartTime NUMBER;
  varTapeStartTime NUMBER;
  varNbRepacks INTEGER;
  varNbFiles INTEGER;
  varNbFailures INTEGER;
BEGIN
  SELECT count(*) INTO varNbRepacks
    FROM StageRepackRequest
   WHERE status = tconst.REPACK_STARTING;
  IF varNbRepacks > 0 THEN
    -- this shouldn't happen, possibly this procedure is running in parallel: give up and log
    -- "repackManager: Repack processes still starting, no new ones will be started for this round"
    logToDLF(NULL, dlf.LVL_NOTICE, dlf.REPACK_JOB_ONGOING, 0, '', 'repackd', '');
    RETURN;
  END IF;
  varStartTime := getTime();
  WHILE TRUE LOOP
    varTapeStartTime := getTime();
    BEGIN
      -- get an outstanding repack to start, and lock to prevent
      -- a concurrent abort (there cannot be anyone else)
      SELECT reqId, repackVID INTO varReqUUID, varRepackVID
        FROM StageRepackRequest
       WHERE id = (SELECT id
                     FROM (SELECT id
                             FROM StageRepackRequest
                            WHERE status = tconst.REPACK_SUBMITTED
                            ORDER BY creationTime ASC)
                    WHERE ROWNUM < 2)
         FOR UPDATE;
      -- start the repack for this request: this can take some considerable time
      handleRepackRequest(varReqUUID, varNbFiles, varNbFailures);
      -- log 'Repack process started'
      logToDLF(varReqUUID, dlf.LVL_SYSTEM, dlf.REPACK_STARTED, 0, '', 'repackd',
        'TPVID=' || varRepackVID || ' nbFiles=' || TO_CHAR(varNbFiles)
        || ' nbFailures=' || TO_CHAR(varNbFailures)
        || ' elapsedTime=' || TO_CHAR(getTime() - varTapeStartTime));
      varNbRepacks := varNbRepacks + 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- if no new repack is found to start, terminate
      EXIT;
    END;
  END LOOP;
  IF varNbRepacks > 0 THEN
    -- log some statistics
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REPACK_JOB_STATS, 0, '', 'repackd',
      'nbStarted=' || TO_CHAR(varNbRepacks) || ' elapsedTime=' || TO_CHAR(TRUNC(getTime() - varStartTime)));
  END IF;
END;
/


/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name = 'REPACKMANAGERJOB')
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every 2 minutes executing the repackManager procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'RepackManagerJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN repackManager(); END;'', ''repackd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=2',
      ENABLED         => TRUE,
      COMMENTS        => 'Database job to manage the repack process');
END;
/
/*******************************************************************
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
  TYPE StringValue IS RECORD (value VARCHAR(2048));
  TYPE StringList_Cur IS REF CURSOR RETURN StringValue;
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
  TYPE DiskCopyResult IS RECORD (
    dcId INTEGER,
    retCode INTEGER);
  TYPE DiskCopyResult_Cur IS REF CURSOR RETURN DiskCopyResult;
  TYPE LogEntry IS RECORD (
    timeinfo NUMBER,
    uuid VARCHAR2(2048),
    priority INTEGER,
    msg VARCHAR2(2048),
    fileId NUMBER,
    nsHost VARCHAR2(2048),
    source VARCHAR2(2048),
    params VARCHAR2(2048));
  TYPE LogEntry_Cur IS REF CURSOR RETURN LogEntry;
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
 *   - Canceling recalls for files that are VALID
 *     on the fileSystem that comes back. (Scheduled for bulk
 *     operation)
 *   - Dealing with files that are STAGEOUT on the fileSystem
 *     coming back but already exist on another one
 */
CREATE OR REPLACE PROCEDURE checkFSBackInProd(fsId NUMBER) AS
BEGIN
  -- Flag the filesystem for processing in a bulk operation later.
  -- We need to do this because some operations are database intensive
  -- and therefore it is often better to process several filesystems
  -- simultaneous with one query as opposed to one by one. Especially
  -- where full table scans are involved.
  UPDATE FileSystemsToCheck SET toBeChecked = 1
   WHERE fileSystem = fsId;
  -- Look for files that are STAGEOUT on the filesystem coming back to life
  -- but already VALID/WAITFS/STAGEOUT/
  -- WAITFS_SCHEDULING somewhere else
  FOR cf IN (SELECT /*+ USE_NL(D E) INDEX(D I_DiskCopy_Status6) */
                    UNIQUE D.castorfile, D.id dcId
               FROM DiskCopy D, DiskCopy E
              WHERE D.castorfile = E.castorfile
                AND D.fileSystem = fsId
                AND E.fileSystem != fsId
                AND decode(D.status,6,D.status,NULL) = dconst.DISKCOPY_STAGEOUT
                AND E.status IN (dconst.DISKCOPY_VALID,
                                 dconst.DISKCOPY_WAITFS, dconst.DISKCOPY_STAGEOUT,
                                 dconst.DISKCOPY_WAITFS_SCHEDULING)) LOOP
    -- Invalidate the DiskCopy
    UPDATE DiskCopy
       SET status = dconst.DISKCOPY_INVALID
     WHERE id = cf.dcId;
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
  -- Look for recalls concerning files that are VALID
  -- on all filesystems scheduled to be checked, and restart their
  -- subrequests (reconsidering the recall source).
  FOR file IN (SELECT UNIQUE DiskCopy.castorFile
               FROM DiskCopy, RecallJob
              WHERE DiskCopy.castorfile = RecallJob.castorfile
                AND DiskCopy.fileSystem IN
                  (SELECT /*+ CARDINALITY(fsidTable 5) */ *
                     FROM TABLE(fsIds) fsidTable)
                AND DiskCopy.status = dconst.DISKCOPY_VALID) LOOP
    -- cancel the recall for that file
    deleteRecallJobs(file.castorFile);
    -- restart subrequests that were waiting on the recall
    UPDATE SubRequest
       SET status = dconst.SUBREQUEST_RESTART
     WHERE castorFile = file.castorFile
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
    -- commit that file
    COMMIT;
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
  IF :old.status != dconst.FILESYSTEM_PRODUCTION AND
     :new.status = dconst.FILESYSTEM_PRODUCTION THEN
    checkFsBackInProd(:old.id);
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
  -- PRODUCTION.
  IF :old.status != dconst.DISKSERVER_PRODUCTION AND
     :new.status = dconst.DISKSERVER_PRODUCTION AND :new.hwOnline = 1 THEN
    FOR fs IN (SELECT id FROM FileSystem
                WHERE diskServer = :old.id
                  AND status = dconst.FILESYSTEM_PRODUCTION)
    LOOP
      checkFsBackInProd(fs.id);
    END LOOP;
  END IF;
END;
/


/* Trigger used to check if the maxReplicaNb has been exceeded
 * after a diskcopy has changed its status to VALID
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
                  SELECT /*+ INDEX (DiskCopy I_DiskCopy_Castorfile) */ DiskCopy.id
                    FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass,
                         DiskServer
                   WHERE DiskCopy.filesystem = FileSystem.id
                     AND FileSystem.diskpool = DiskPool2SvcClass.parent
                     AND FileSystem.diskserver = DiskServer.id
                     AND DiskPool2SvcClass.child = SvcClass.id
                     AND DiskCopy.castorfile = a.castorfile
                     AND DiskCopy.status = dconst.DISKCOPY_VALID
                     AND SvcClass.id = a.svcclass
                   -- Select non-PRODUCTION hardware first
                   ORDER BY decode(FileSystem.status, 0,
                            decode(DiskServer.status, 0, 0, 1), 1) ASC,
                            DiskCopy.gcWeight DESC))
               WHERE ind > maxReplicaNb)
    LOOP
      -- Sanity check, make sure that the last copy is never dropped!
      SELECT /*+ INDEX(DiskCopy I_DiskCopy_CastorFile) */ count(*) INTO nbFiles
        FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass, DiskServer
       WHERE DiskCopy.filesystem = FileSystem.id
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND FileSystem.diskserver = DiskServer.id
         AND DiskPool2SvcClass.child = SvcClass.id
         AND DiskCopy.castorfile = a.castorfile
         AND DiskCopy.status = dconst.DISKCOPY_VALID
         AND SvcClass.id = a.svcclass;
      IF nbFiles = 1 THEN
        EXIT;  -- Last file, so exit the loop
      END IF;
      -- Invalidate the diskcopy
      UPDATE DiskCopy
         SET status = dconst.DISKCOPY_INVALID,
             gcType = dconst.GCTYPE_TOOMANYREPLICAS
       WHERE id = b.id;
      -- update importance of remaining diskcopies
      UPDATE DiskCopy SET importance = importance + 1
       WHERE castorFile = a.castorfile
         AND status = dconst.DISKCOPY_VALID;
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
CREATE OR REPLACE TRIGGER tr_DiskCopy_Created
AFTER INSERT ON DiskCopy
FOR EACH ROW
WHEN (new.status = 0) -- dconst.DISKCOPY_VALID
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
    INSERT INTO TooManyReplicasHelper (svcClass, castorFile)
    VALUES (svcId, :new.castorfile);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    RETURN;  -- Entry already exists!
  END;
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
  CURSOR SRcur IS
    SELECT /*+ FIRST_ROWS_10 INDEX_RS_ASC(SR I_SubRequest_RT_CT_ID) */ SR.id
      FROM SubRequest PARTITION (P_STATUS_0_1_2) SR  -- START, RESTART, RETRY
     WHERE SR.svcHandler = service
     ORDER BY SR.creationTime ASC;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  varSrId NUMBER;
  varRName VARCHAR2(100);
  varClientId NUMBER;
  varUnusedMessage VARCHAR2(2048);
  varUnusedStatus INTEGER;
BEGIN
  -- Open a cursor on potential candidates
  OPEN SRcur;
  -- Retrieve the first candidate
  FETCH SRCur INTO varSrId;
  IF SRCur%NOTFOUND THEN
    -- There is no candidate available. Wait for next alert for a maximum of 3 seconds.
    -- We do not wait forever in order to to give the control back to the
    -- caller daemon in case it should exit.
    CLOSE SRCur;
    DBMS_ALERT.WAITONE('wakeUp'||service, varUnusedMessage, varUnusedStatus, 3);
    -- try again to find something now that we waited
    OPEN SRCur;
    FETCH SRCur INTO varSrId;
    IF SRCur%NOTFOUND THEN
      -- still nothing. We will give back the control to the application
      -- so that it can handle cases like signals and exit. We will probably
      -- be back soon :-)
      RETURN;
    END IF;
  END IF;
  -- Loop on candidates until we can lock one
  LOOP
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
      EXIT;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        -- Got to next candidate, this subrequest was processed already and its status changed
        NULL;
      WHEN SrLocked THEN
        -- Go to next candidate, this subrequest is being processed by another thread
        NULL;
    END;
    -- we are here because the current candidate could not be handled
    -- let's go to the next one
    FETCH SRcur INTO varSrId;
    IF SRcur%NOTFOUND THEN
      -- no next one ? then we can return
      RETURN;
    END IF;
  END LOOP;
  CLOSE SRcur;

  BEGIN
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
  EXCEPTION WHEN OTHERS THEN
    -- Something went really wrong, our subrequest does not have the corresponding request or client,
    -- Just drop it and re-raise exception. Some rare occurrences have happened in the past,
    -- this catch-all logic protects the stager-scheduling system from getting stuck with a single such case.
    archiveSubReq(varSrId, dconst.SUBREQUEST_FAILED_FINISHED);
    COMMIT;
    raise_application_error(-20100, 'Request got corrupted and could not be processed : ' ||
                                    SQLCODE || ' -ERROR- ' || SQLERRM);
  END;
END;
/

/* PL/SQL method to fail a subrequest in WAITTAPERECALL
 * and eventually the recall itself if it's the only subrequest waiting for it
 */
CREATE OR REPLACE PROCEDURE failRecallSubReq(inSrId IN INTEGER, inCfId IN INTEGER) AS
  varNbSRs INTEGER;
BEGIN
  -- recall case. First fail the subrequest
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET status = dconst.SUBREQUEST_FAILED
   WHERE id = inSrId;
  -- check whether there are other subRequests waiting for a recall
  SELECT COUNT(*) INTO varNbSrs
    FROM SubRequest
   WHERE castorFile = inCfId
     AND status = dconst.SUBREQUEST_WAITTAPERECALL;
  IF varNbSrs = 0 THEN
    -- no other subrequests, so drop recalls
    deleteRecallJobs(inCfId);
  END IF;
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
      OR abortedSRstatus = dconst.SUBREQUEST_READYFORSCHED THEN
      -- standard case, we only have to fail the subrequest
      UPDATE SubRequest SET status = dconst.SUBREQUEST_FAILED WHERE id = sr.srId;
      INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
      VALUES (sr.fileId, sr.nsHost, 0, '');
    WHEN abortedSRstatus = dconst.SUBREQUEST_WAITTAPERECALL THEN
        failRecallSubReq(sr.srId, sr.cfId);
        INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
        VALUES (sr.fileId, sr.nsHost, 0, '');
    WHEN abortedSRstatus = dconst.SUBREQUEST_FAILED
      OR abortedSRstatus = dconst.SUBREQUEST_FAILED_FINISHED THEN
      -- subrequest has failed, nothing to abort
      INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
      VALUES (sr.fileId, sr.nsHost, serrno.EINVAL, 'Cannot abort failed subRequest');
    WHEN abortedSRstatus = dconst.SUBREQUEST_FINISHED
      OR abortedSRstatus = dconst.SUBREQUEST_ARCHIVED THEN
      -- subrequest is over, nothing to abort
      INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
      VALUES (sr.fileId, sr.nsHost, serrno.EINVAL, 'Cannot abort completed subRequest');
    ELSE
      -- unknown status !
      INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
      VALUES (sr.fileId, sr.nsHost, serrno.SEINTERNAL, 'Found unknown status for request : ' || TO_CHAR(abortedSRstatus));
  END CASE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- subRequest was deleted in the mean time !
  INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
  VALUES (sr.fileId, sr.nsHost, serrno.ENOENT, 'Targeted SubRequest has just been deleted');
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
      OR abortedSRstatus = dconst.SUBREQUEST_READYFORSCHED THEN
      -- standard case, we only have to fail the subrequest
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_FAILED
       WHERE id = sr.srId;
      UPDATE DiskCopy
         SET status = decode(status, dconst.DISKCOPY_WAITFS, dconst.DISKCOPY_FAILED,
                                     dconst.DISKCOPY_WAITFS_SCHEDULING, dconst.DISKCOPY_FAILED,
                                     dconst.DISKCOPY_INVALID)
       WHERE castorfile = sr.cfid AND status IN (dconst.DISKCOPY_STAGEOUT,
                                                 dconst.DISKCOPY_WAITFS,
                                                 dconst.DISKCOPY_WAITFS_SCHEDULING);
      INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
      VALUES (sr.fileId, sr.nsHost, 0, '');
    WHEN abortedSRstatus = dconst.SUBREQUEST_FAILED
      OR abortedSRstatus = dconst.SUBREQUEST_FAILED_FINISHED THEN
      -- subrequest has failed, nothing to abort
      INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
      VALUES (sr.fileId, sr.nsHost, serrno.EINVAL, 'Cannot abort failed subRequest');
    WHEN abortedSRstatus = dconst.SUBREQUEST_FINISHED
      OR abortedSRstatus = dconst.SUBREQUEST_ARCHIVED THEN
      -- subrequest is over, nothing to abort
      INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
      VALUES (sr.fileId, sr.nsHost, serrno.EINVAL, 'Cannot abort completed subRequest');
    ELSE
      -- unknown status !
      INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
      VALUES (sr.fileId, sr.nsHost, serrno.SEINTERNAL, 'Found unknown status for request : ' || TO_CHAR(abortedSRstatus));
  END CASE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- subRequest was deleted in the mean time !
  INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
  VALUES (sr.fileId, sr.nsHost, serrno.ENOENT, 'Targeted SubRequest has just been deleted');
END;
/

/* PL/SQL method to process bulk abort on a given Repack request */
CREATE OR REPLACE PROCEDURE processBulkAbortForRepack(origReqId IN INTEGER) AS
  abortedSRstatus INTEGER := -1;
  srsToUpdate "numList";
  dcmigrsToUpdate "numList";
  nbItems INTEGER;
  nbItemsDone INTEGER := 0;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  cfId INTEGER;
  srId INTEGER;
  firstOne BOOLEAN := TRUE;
  commitWork BOOLEAN := FALSE;
  varOriginalVID VARCHAR2(2048);
BEGIN
  -- get the VID of the aborted repack request
  SELECT repackVID INTO varOriginalVID FROM StageRepackRequest WHERE id = origReqId;
  -- Gather the list of subrequests to abort
  INSERT INTO ProcessBulkAbortFileReqsHelper (srId, cfId, fileId, nsHost, uuid) (
    SELECT /*+ INDEX(Subrequest I_Subrequest_CastorFile)*/
           SubRequest.id, CastorFile.id, CastorFile.fileId, CastorFile.nsHost, SubRequest.subreqId
      FROM SubRequest, CastorFile
     WHERE SubRequest.castorFile = CastorFile.id
       AND request = origReqId
       AND status IN (dconst.SUBREQUEST_START, dconst.SUBREQUEST_RESTART, dconst.SUBREQUEST_RETRY,
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
          SELECT id INTO cfId FROM CastorFile WHERE id = sr.cfId FOR UPDATE;
          firstOne := FALSE;
        ELSE
          -- on the other items, we go for a non blocking lock. If we get it, that's
          -- good and we process this extra subrequest within the same session. If
          -- we do not get the lock, then we close the session here and go for a new
          -- one. This will prevent dead locks while ensuring that a minimal number of
          -- commits is performed.
          SELECT id INTO cfId FROM CastorFile WHERE id = sr.cfId FOR UPDATE NOWAIT;
        END IF;
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
            OR abortedSRstatus = dconst.SUBREQUEST_WAITSUBREQ THEN
            -- easy case, we only have to fail the subrequest
            INSERT INTO ProcessRepackAbortHelperSR (srId) VALUES (sr.srId);
          WHEN abortedSRstatus = dconst.SUBREQUEST_WAITTAPERECALL THEN
            -- recall case, fail the subRequest and cancel the recall if needed
            failRecallSubReq(sr.srId, sr.cfId);
          WHEN abortedSRstatus = dconst.SUBREQUEST_REPACK THEN
            -- trigger the update the subrequest status to FAILED
            INSERT INTO ProcessRepackAbortHelperSR (srId) VALUES (sr.srId);
            -- delete migration jobs of this repack, hence stopping selectively the migrations
            DELETE FROM MigrationJob WHERE castorfile = sr.cfId AND originalVID = varOriginalVID;
            -- delete migrated segments if no migration jobs remain
            BEGIN
              SELECT id INTO cfId FROM MigrationJob WHERE castorfile = sr.cfId AND ROWNUM < 2;
            EXCEPTION WHEN NO_DATA_FOUND THEN
              DELETE FROM MigratedSegment WHERE castorfile = sr.cfId;
              -- trigger the update of the CastorFile's tapeStatus to ONTAPE as no more migrations remain
              INSERT INTO ProcessRepackAbortHelperDCmigr (cfId) VALUES (sr.cfId);
            END;
           WHEN abortedSRstatus IN (dconst.SUBREQUEST_FAILED,
                                    dconst.SUBREQUEST_FINISHED,
                                    dconst.SUBREQUEST_FAILED_FINISHED,
                                    dconst.SUBREQUEST_ARCHIVED) THEN
             -- nothing to be done here
             NULL;
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
    FORALL i IN 1 .. srsToUpdate.COUNT
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET diskCopy = NULL, lastModificationTime = getTime(),
             status = dconst.SUBREQUEST_FAILED_FINISHED,
             errorCode = 1701, errorMessage = 'Aborted explicitely'  -- ESTCLEARED
       WHERE id = srsToUpdate(i);
    SELECT cfId BULK COLLECT INTO dcmigrsToUpdate FROM ProcessRepackAbortHelperDCmigr;
    FORALL i IN 1 .. dcmigrsToUpdate.COUNT
      UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_ONTAPE WHERE id = dcmigrsToUpdate(i);
    -- commit
    COMMIT;
    -- reset all counters
    nbItems := nbItems - nbItemsDone;
    nbItemsDone := 0;
    firstOne := TRUE;
    commitWork := FALSE;
  END LOOP;
  -- archive the request
  BEGIN
    SELECT id, status INTO srId, abortedSRstatus
      FROM SubRequest
     WHERE request = origReqId
       AND status IN (dconst.SUBREQUEST_FINISHED, dconst.SUBREQUEST_FAILED_FINISHED)
       AND ROWNUM = 1;
    -- This procedure should really be called 'terminateSubReqAndArchiveRequest', and this is
    -- why we call it here: we need to trigger the logic to mark the whole request and all of its subrequests
    -- as ARCHIVED, so that they are cleaned up afterwards. Note that this is effectively
    -- a no-op for the status change of the single fetched SubRequest.
    archiveSubReq(srId, abortedSRstatus);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Should never happen, anyway ignore as there's nothing else to do
    NULL;
  END;
  COMMIT;
END;
/

/* PL/SQL method to process bulk abort on files related requests */
CREATE OR REPLACE PROCEDURE processBulkAbortFileReqs
(origReqId IN INTEGER, fileIds IN "numList", nsHosts IN strListTable, reqType IN NUMBER) AS
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
    INSERT INTO ProcessBulkAbortFileReqsHelper (srId, cfId, fileId, nsHost, uuid) (
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
        INSERT INTO processBulkAbortFileReqsHelper (srId, cfId, fileId, nsHost, uuid)
        VALUES (srId, cfId, fileIds(i), nsHosts(i), srUuid);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- this fileid/nshost did not exist in the request, send an error back
        INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
        VALUES (fileIds(i), nsHosts(i), serrno.ENOENT, 'No subRequest found for this fileId/nsHost');
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
        INSERT INTO TransfersToAbort (uuid) VALUES (sr.uuid);
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
  FORALL i IN 1 .. ids.COUNT DELETE FROM NsFileId WHERE id = ids(i);
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
    INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
    VALUES (0, '', serrno.ENOENT, 'Request not found, or abort not supported for this request type');
    RETURN;
  END;
  IF reqType IN (1,2) THEN
    processBulkAbortFileReqs(requestId, fileIds, nsHosts, reqType);
  ELSE
    processBulkAbortForRepack(requestId);
  END IF;
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
  varUnusedMessage VARCHAR2(2048);
  varUnusedStatus INTEGER;
BEGIN
  -- in case we do not find anything, rtype should be 0
  rType := 0;
  -- Open a cursor on potential candidates
  OPEN Rcur;
  -- Retrieve the first candidate
  FETCH Rcur INTO requestId;
  IF Rcur%NOTFOUND THEN
    -- There is no candidate available. Wait for next alert for a maximum of 3 seconds.
    -- We do not wait forever in order to to give the control back to the
    -- caller daemon in case it should exit.
    CLOSE Rcur;
    DBMS_ALERT.WAITONE('wakeUp'||service, varUnusedMessage, varUnusedStatus, 3);
    -- try again to find something now that we waited
    OPEN Rcur;
    FETCH Rcur INTO requestId;
    IF Rcur%NOTFOUND THEN
      -- still nothing. We will give back the control to the application
      -- so that it can handle cases like signals and exit. We will probably
      -- be back soon :-)
      RETURN;
    END IF;
  END IF;
  -- Loop on candidates until we can lock one
  LOOP
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
    -- we are here because the current candidate could not be handled
    -- let's go to the next one
    FETCH Rcur INTO requestId;
    IF Rcur%NOTFOUND THEN
      -- no next one ? then we can return
      RETURN;
    END IF;
  END LOOP;
  CLOSE Rcur;
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
  varUnusedMessage VARCHAR2(2048);
  varUnusedStatus INTEGER;
BEGIN
  -- Open a cursor on potential candidates
  OPEN c;
  -- Retrieve the first candidate
  FETCH c INTO varSRId;
  IF c%NOTFOUND THEN
    -- There is no candidate available. Wait for next alert for a maximum of 3 seconds.
    -- We do not wait forever in order to to give the control back to the
    -- caller daemon in case it should exit.
    CLOSE c;
    DBMS_ALERT.WAITONE('wakeUpErrorSvc', varUnusedMessage, varUnusedStatus, 3);
    -- try again to find something now that we waited
    OPEN c;
    FETCH c INTO varSRId;
    IF c%NOTFOUND THEN
      -- still nothing. We will give back the control to the application
      -- so that it can handle cases like signals and exit. We will probably
      -- be back soon :-)
      RETURN;
    END IF;
  END IF;
  -- Loop on candidates until we can lock one
  LOOP
    BEGIN
      SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ answered INTO varSrAnswered
        FROM SubRequest PARTITION (P_STATUS_7)
       WHERE id = varSRId FOR UPDATE NOWAIT;
      IF varSrAnswered = 1 THEN
        -- already answered, archive and move on
        archiveSubReq(varSRId, dconst.SUBREQUEST_FAILED_FINISHED);
        -- release the lock on this request as it's completed
        COMMIT;
      ELSE
        -- we got our subrequest, select all relevant data and hold the lock
        SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ fileName, subReqId, errorCode, errorMessage,
          (SELECT object FROM Type2Obj WHERE type = reqType), request, castorFile
          INTO srFileName, srSubReqId, srErrorCode, srErrorMessage, varRName, varRId, varCFId
          FROM SubRequest
         WHERE id = varSRId;
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
            ELSE
              -- Unsupported request type, should never happen
              RAISE NO_DATA_FOUND;
          END CASE;
          SELECT ipAddress, port, version
            INTO clIpAddress, clPort, clVersion
            FROM Client WHERE id = varClientId;
          IF varCFId > 0 THEN
            SELECT fileId INTO srFileId FROM CastorFile WHERE id = varCFId;
          END IF;
          EXIT;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- This should never happen, we have either an orphaned subrequest
          -- or a request with an unsupported type.
          -- As we couldn't get the client, we just archive and move on.
          -- XXX For next version, call logToDLF() instead of silently archive.
          srId := 0;
          archiveSubReq(varSRId, dconst.SUBREQUEST_FAILED_FINISHED);
          COMMIT;
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
    FETCH c INTO varSRId;
    IF c%NOTFOUND THEN
      -- no next one ? then we can return
      RETURN;
    END IF;
  END LOOP;
  CLOSE c;
END;
/


/* PL/SQL method to get the next request to do according to the given service */
CREATE OR REPLACE PROCEDURE requestToDo(service IN VARCHAR2, rId OUT INTEGER, rType OUT INTEGER) AS
  varUnusedMessage VARCHAR2(2048);
  varUnusedStatus INTEGER;
BEGIN
  DELETE /*+ INDEX_RS_ASC(NewRequests PK_NewRequests_Type_Id) LEADING(Type2Obj NewRequests) */ FROM NewRequests
   WHERE type IN (SELECT type FROM Type2Obj
                   WHERE svcHandler = service
                     AND svcHandler IS NOT NULL)
   AND ROWNUM < 2 RETURNING id, type INTO rId, rType;
  IF rId IS NULL THEN
    -- There is no candidate available. Wait for next alert for a maximum of 3 seconds.
    -- We do not wait forever in order to to give the control back to the
    -- caller daemon in case it should exit.
    DBMS_ALERT.WAITONE('wakeUp'||service, varUnusedMessage, varUnusedStatus, 3);
    -- try again to find something now that we waited
    DELETE FROM NewRequests
     WHERE type IN (SELECT type FROM Type2Obj
                     WHERE svcHandler = service
                       AND svcHandler IS NOT NULL)
     AND ROWNUM < 2 RETURNING id, type INTO rId, rType;
    IF rId IS NULL THEN
      rId := 0;   -- nothing to do
      rType := 0;
    END IF;
  END IF;
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
     SET diskCopy = NULL,  -- unlink this subrequest as it's dead now
         lastModificationTime = getTime(),
         status = finalStatus
   WHERE id = srId
   RETURNING request, reqType, (SELECT object FROM Type2Obj WHERE type = reqType) INTO rId, rType, rName;
  -- Try to see whether another subrequest in the same
  -- request is still being processed. For this, we
  -- need a master lock on the request.
  EXECUTE IMMEDIATE
    'BEGIN SELECT client INTO :clientId FROM '|| rName ||' WHERE id = :rId FOR UPDATE; END;'
    USING OUT clientId, IN rId;
  BEGIN
    -- note the decode trick to use the dedicated index I_SubRequest_Req_Stat_no89
    SELECT request INTO unused FROM SubRequest
     WHERE request = rId AND decode(status,8,NULL,9,NULL,status) IS NOT NULL
       AND ROWNUM < 2;  -- all but {FAILED_,}FINISHED
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- All subrequests have finished, we can archive:
    -- drop the associated Client entity
    DELETE FROM Client WHERE id = clientId;
    -- archive the successful subrequests
    UPDATE /*+ INDEX(SubRequest I_SubRequest_Request) */ SubRequest
       SET status = dconst.SUBREQUEST_ARCHIVED
     WHERE request = rId
       AND status = dconst.SUBREQUEST_FINISHED;
    -- in case of repack, change the status of the request
    IF rType = 119 THEN  -- OBJ_StageRepackRequest
      DECLARE
        nbfailures NUMBER;
      BEGIN
        SELECT count(*) INTO nbfailures FROM SubRequest
         WHERE request = rId
           AND status = dconst.SUBREQUEST_FAILED_FINISHED
           AND ROWNUM < 2;
        UPDATE StageRepackRequest
           SET status = CASE nbfailures WHEN 1 THEN tconst.REPACK_FAILED ELSE tconst.REPACK_FINISHED END,
               lastModificationTime = getTime()
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
       AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
       AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
       AND DiskServer.hwOnline = 1
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
CREATE OR REPLACE FUNCTION checkNoTapeRouting(fileClassId NUMBER)
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


/* PL/SQL method implementing findDiskCopyToReplicate. */
CREATE OR REPLACE PROCEDURE findDiskCopyToReplicate
  (cfId IN NUMBER, reuid IN NUMBER, regid IN NUMBER,
   dcId OUT NUMBER, srcSvcClassId OUT NUMBER) AS
  destSvcClass VARCHAR2(2048);
BEGIN
  -- Select the best diskcopy available to replicate and for which the user has
  -- access too.
  SELECT id, srcSvcClassId INTO dcId, srcSvcClassId
    FROM (
      SELECT /*+ INDEX (DiskCopy I_DiskCopy_CastorFile) */ DiskCopy.id, SvcClass.id srcSvcClassId
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
       WHERE DiskCopy.castorfile = cfId
         AND DiskCopy.status = dconst.DISKCOPY_VALID
         AND FileSystem.id = DiskCopy.fileSystem
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
         AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
         AND DiskServer.id = FileSystem.diskserver
         AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
         AND DiskServer.hwOnline = 1
         -- Check that the user has the necessary access rights to replicate a
         -- file from the source service class. Note: instead of using a
         -- StageGetRequest type here we use a StagDiskCopyReplicaRequest type
         -- to be able to distinguish between and read and replication requst.
         AND checkPermission(SvcClass.name, reuid, regid, 133) = 0)
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
       AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
       AND FileSystem.diskserver = DiskServer.id
       AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
       AND DiskServer.hwOnline = 1
       AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
     ORDER BY FileSystemRate(FileSystem.nbReadStreams, FileSystem.nbWriteStreams) DESC,
              DBMS_Random.value)
   WHERE rownum < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RAISE; -- No file found to be read
END;
/


/* PL/SQL method implementing checkForD2DCopyOrRecall
 * dcId is the DiskCopy id of the best candidate for replica, 0 if none is found (tape recall), -1 in case of user error
 */
CREATE OR REPLACE PROCEDURE checkForD2DCopyOrRecall(cfId IN NUMBER, srId IN NUMBER, reuid IN NUMBER, regid IN NUMBER,
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
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOSPC, -- No space left on device
           errorMessage = 'File creation canceled since diskPool is full'
     WHERE id = srId;
    RETURN;
  END IF;
  -- Resolve the destination service class id to a name
  SELECT name INTO destSvcClass FROM SvcClass WHERE id = svcClassId;
  -- Determine if there are any copies of the file in the same service class
  -- on non PRODUCTION hardware. If we found something then set the user
  -- and group id to -1 this effectively disables the later privilege checks
  -- to see if the user can trigger a d2d or recall. (#55745)
  BEGIN
    SELECT /*+ INDEX (DiskCopy I_DiskCopy_CastorFile) */ -1, -1 INTO userid, groupid
      FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
     WHERE DiskCopy.fileSystem = FileSystem.id
       AND DiskCopy.castorFile = cfId
       AND DiskCopy.status = dconst.DISKCOPY_VALID
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = svcClassId
       AND FileSystem.diskServer = DiskServer.id
       AND (DiskServer.status != dconst.DISKSERVER_PRODUCTION
        OR  FileSystem.status != dconst.FILESYSTEM_PRODUCTION)
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
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.EACCES,
           errorMessage = 'Insufficient user privileges to trigger a tape recall or file replication to the '''||destSvcClass||''' service class'
     WHERE id = srId;
    RETURN;
  END IF;
  -- Try to find a diskcopy to replicate
  findDiskCopyToReplicate(cfId, userid, groupid, dcId, srcSvcClassId);
  -- We found at least one, therefore we schedule a disk2disk
  -- copy from the existing diskcopy not available to this svcclass
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- We found no diskcopies at all. We should not schedule
  -- and make a tape recall... except ... in 4 cases :
  --   - if there is some temporarily unavailable diskcopy
  --     that is in STAGEOUT or is VALID but not on tape yet
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
  --   - if we have some temporarily unavailable diskcopy(ies)
  --     that is in status VALID and the file is disk only.
  -- In this case nothing can be recalled and the file is inaccessible
  -- until we have one of the unvailable copies back
  DECLARE
    dcStatus NUMBER;
    fsStatus NUMBER;
    dsStatus NUMBER;
    varNbCopies NUMBER;
  BEGIN
    SELECT DiskCopy.status, nvl(FileSystem.status, dconst.FILESYSTEM_PRODUCTION),
           nvl(DiskServer.status, dconst.DISKSERVER_PRODUCTION)
      INTO dcStatus, fsStatus, dsStatus
      FROM DiskCopy, FileSystem, DiskServer, CastorFile
     WHERE DiskCopy.castorfile = cfId
       AND Castorfile.id = cfId
       AND (DiskCopy.status IN (dconst.DISKCOPY_WAITFS, dconst.DISKCOPY_STAGEOUT,
                                dconst.DISKCOPY_WAITFS_SCHEDULING)
            OR (DiskCopy.status = dconst.DISKCOPY_VALID AND
                CastorFile.tapeStatus = dconst.CASTORFILE_NOTONTAPE))
       AND FileSystem.id(+) = DiskCopy.fileSystem
       AND DiskServer.id(+) = FileSystem.diskserver
       AND ROWNUM < 2;
    -- We are in one of the 3 first special cases. Don't schedule, don't recall
    dcId := -1;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = CASE
             WHEN dcStatus IN (dconst.DISKCOPY_WAITFS, dconst.DISKCOPY_WAITFS_SCHEDULING) THEN serrno.EBUSY
             WHEN dcStatus = dconst.DISKCOPY_STAGEOUT
               AND fsStatus IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
               AND dsStatus IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY) THEN serrno.EBUSY
             ELSE serrno.ESTNOTAVAIL -- File is currently not available
           END,
           errorMessage = CASE
             WHEN dcStatus IN (dconst.DISKCOPY_WAITFS, dconst.DISKCOPY_WAITFS_SCHEDULING) THEN
               'File is being (re)created right now by another user'
             WHEN dcStatus = dconst.DISKCOPY_STAGEOUT
               AND fsStatus IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
               AND dsStatus IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY) THEN
               'File is being written to in another service class'
             ELSE
               'All copies of this file are unavailable for now. Please retry later'
           END
     WHERE id = srId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- we are not in one of the 3 first special cases. Let's check the 4th one
    -- by checking whether the file is diskonly
    SELECT nbCopies INTO varNbCopies
      FROM FileClass, CastorFile
     WHERE FileClass.id = CastorFile.fileClass
       AND CastorFile.id = cfId;
    IF varNbCopies = 0 THEN
      -- we have indeed a disk only file, so fail the request
      dcId := -1;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.ESTNOTAVAIL, -- File is currently not available
             errorMessage = 'All disk copies of this disk-only file are unavailable for now. Please retry later'
       WHERE id = srId;
    ELSE
      -- We did not find the very special case so we should recall from tape.
      -- Check whether the user has the rights to issue a tape recall to
      -- the destination service class.
      IF checkPermission(destSvcClass, userid, groupid, 161) != 0 THEN
        -- Fail the subrequest and notify the client
        dcId := -1;
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
           SET status = dconst.SUBREQUEST_FAILED,
               errorCode = serrno.EACCES, -- Permission denied
               errorMessage = 'Insufficient user privileges to trigger a tape recall to the '''||destSvcClass||''' service class'
         WHERE id = srId;
      ELSE
        -- user has enough rights, green light for the recall
        dcId := 0;
      END IF;
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
  path := TO_CHAR(MOD(fid,100),'FM09') || '/' || TO_CHAR(fid) || '@' || nsHost || '.' || TO_CHAR(dcid);
END;
/

/* parse a path to give back the FileSystem and path */
CREATE OR REPLACE PROCEDURE parsePath(inFullPath IN VARCHAR2,
                                      outFileSystem OUT INTEGER,
                                      outPath OUT VARCHAR2,
                                      outDcId OUT INTEGER) AS
  varPathPos INTEGER;
  varLastDotPos INTEGER;
  varColonPos INTEGER;
  varDiskServerName VARCHAR2(2048);
  varMountPoint VARCHAR2(2048);
BEGIN
  -- path starts after the second '/' from the end
  varPathPos := INSTR(inFullPath, '/', -1, 2);
  outPath := SUBSTR(inFullPath, varPathPos+1);
  -- DcId is the part after the last '.'
  varLastDotPos := INSTR(inFullPath, '.', -1, 1);
  outDcId := TO_NUMBER(SUBSTR(inFullPath, varLastDotPos+1));
  -- the mountPoint is between the ':' and the start of the path
  varColonPos := INSTR(inFullPath, ':', 1, 1);
  varMountPoint := SUBSTR(inFullPath, varColonPos+1, varPathPos-varColonPos);
  -- the diskserver is before the ':
  varDiskServerName := SUBSTR(inFullPath, 1, varColonPos-1);
  -- find out the filesystem Id
  SELECT FileSystem.id INTO outFileSystem
    FROM DiskServer, FileSystem
   WHERE DiskServer.name = varDiskServerName
     AND FileSystem.diskServer = DiskServer.id
     AND FileSystem.mountPoint = varMountPoint;
END;
/

/* PL/SQL method implementing createDisk2DiskCopyJob */
CREATE OR REPLACE PROCEDURE createDisk2DiskCopyJob
(inCfId IN INTEGER, inNsOpenTime IN INTEGER, inDestSvcClassId IN INTEGER,
 inOuid IN INTEGER, inOgid IN INTEGER, inReplicationType IN INTEGER,
 inReplacedDcId IN INTEGER, inDrainingJob IN INTEGER) AS
  varD2dCopyJobId INTEGER;
  varDestDcId INTEGER;
BEGIN
  varD2dCopyJobId := ids_seq.nextval();
  varDestDcId := ids_seq.nextval();
  -- Create the Disk2DiskCopyJob
  INSERT INTO Disk2DiskCopyJob (id, transferId, creationTime, status, retryCounter, ouid, ogid,
                                destSvcClass, castorFile, nsOpenTime, replicationType,
                                replacedDcId, destDcId, drainingJob)
  VALUES (varD2dCopyJobId, uuidgen(), gettime(), dconst.DISK2DISKCOPYJOB_PENDING, 0, inOuid, inOgid,
          inDestSvcClassId, inCfId, inNsOpenTime, inReplicationType,
          inReplacedDcId, varDestDcId, inDrainingJob);

  -- log "Created new Disk2DiskCopyJob"
  DECLARE
    varFileId INTEGER;
    varNsHost VARCHAR2(2048);
  BEGIN
    SELECT fileid, nsHost INTO varFileId, varNsHost FROM CastorFile WHERE id = inCfId;
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_CREATING_JOB, varFileId, varNsHost, 'stagerd',
             'destSvcClass=' || getSvcClassName(inDestSvcClassId) || ' nsOpenTime=' || TO_CHAR(inNsOpenTime) ||
             ' uid=' || TO_CHAR(inOuid) || ' gid=' || TO_CHAR(inOgid) || ' replicationType=' ||
             getObjStatusName('Disk2DiskCopyJob', 'replicationType', inReplicationType) ||
             ' TransferId=' || TO_CHAR(varD2dCopyJobId) || ' replacedDcId=' || TO_CHAR(inReplacedDcId ||
             ' DrainReq=' || TO_CHAR(inDrainingJob)));
  END;
  
  -- wake up transfermanager
  DBMS_ALERT.SIGNAL('d2dReadyToSchedule', '');
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
  dcId := ids_seq.nextval();
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
             AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
             AND DiskServer.id = FileSystem.diskServer
             AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
             AND DiskServer.hwOnline = 1
        ORDER BY -- order by rate as defined by the function
                 fileSystemRate(FileSystem.nbReadStreams, FileSystem.nbWriteStreams) DESC,
                 -- finally use randomness to avoid preferring always the same FS
                 DBMS_Random.value)
   WHERE ROWNUM < 2;
  -- compute it's gcWeight
  gcwProc := castorGC.getRecallWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(0); END;'
    USING OUT gcw;
  -- then create the DiskCopy
  INSERT INTO DiskCopy
    (path, id, filesystem, castorfile, status, importance,
     creationTime, lastAccessTime, gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
  VALUES (dcPath, dcId, fsId, cfId, dconst.CASTORFILE_DISKONLY, -1,
          getTime(), getTime(), GCw, 0, 0, ouid, ogid);
  -- link to the SubRequest and schedule an access if requested
  IF schedule = 0 THEN
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET diskCopy = dcId WHERE id = srId;
  ELSE
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET diskCopy = dcId,
           requestedFileSystems = fsPath,
           xsize = 0, status = dconst.SUBREQUEST_READYFORSCHED,
           getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED
     WHERE id = srId;
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  raise_application_error(-20115, 'No suitable filesystem found for this empty file');
END;
/

/* PL/SQL method implementing replicateOnClose */
CREATE OR REPLACE PROCEDURE replicateOnClose(cfId IN NUMBER, ouid IN INTEGER, ogid IN INTEGER) AS
  varNsOpenTime NUMBER;
  srcDcId NUMBER;
  srcSvcClassId NUMBER;
  ignoreSvcClass NUMBER;
BEGIN
  -- Lock the castorfile and take the nsOpenTime
  SELECT nsOpenTime INTO varNsOpenTime FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Loop over all service classes where replication is required
  FOR a IN (SELECT SvcClass.id FROM (
              -- Determine the number of copies of the file in all service classes
              SELECT * FROM (
                SELECT  /*+ INDEX(DiskCopy I_DiskCopy_CastorFile) */
                       SvcClass.id, count(*) available
                  FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
                 WHERE DiskCopy.filesystem = FileSystem.id
                   AND DiskCopy.castorfile = cfId
                   AND FileSystem.diskpool = DiskPool2SvcClass.parent
                   AND DiskPool2SvcClass.child = SvcClass.id
                   AND DiskCopy.status = dconst.DISKCOPY_VALID
                   AND FileSystem.status IN
                       (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
                   AND DiskServer.id = FileSystem.diskserver
                   AND DiskServer.status IN
                       (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
                 GROUP BY SvcClass.id)
             ) results, SvcClass
             -- Join the results with the service class table and determine if
             -- additional copies need to be created
             WHERE results.id = SvcClass.id
               AND SvcClass.replicateOnClose = 1
               AND results.available < SvcClass.maxReplicaNb)
  LOOP
    BEGIN
      -- Trigger a replication request.
      createDisk2DiskCopyJob(cfId, varNsOpenTime, a.id, ouid, ogid, dconst.REPLICATIONTYPE_USER, NULL, NULL);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;  -- No copies to replicate from
    END;
  END LOOP;
END;
/


/*** initMigration ***/
CREATE OR REPLACE PROCEDURE initMigration
(cfId IN INTEGER, datasize IN INTEGER, originalVID IN VARCHAR2,
 originalCopyNb IN INTEGER, destCopyNb IN INTEGER, inMJStatus IN INTEGER) AS
  varTpId INTEGER;
  varSizeThreshold INTEGER;
BEGIN
  varSizeThreshold := TO_NUMBER(getConfigOption('Migration', 'SizeThreshold', '300000000'));
  -- Find routing
  BEGIN
    SELECT tapePool INTO varTpId FROM MigrationRouting MR, CastorFile
     WHERE MR.fileClass = CastorFile.fileClass
       AND CastorFile.id = cfId
       AND MR.copyNb = destCopyNb
       AND (MR.isSmallFile = (CASE WHEN datasize < varSizeThreshold THEN 1 ELSE 0 END) OR MR.isSmallFile IS NULL);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- No routing rule found means a user-visible error on the putDone or on the file close operation
    raise_application_error(-20100, 'Cannot find an appropriate tape routing for this file, aborting');
  END;
  -- Create tape copy and attach to the appropriate tape pool
  INSERT INTO MigrationJob (fileSize, creationTime, castorFile, originalVID, originalCopyNb, destCopyNb,
                            tapePool, nbRetries, status, mountTransactionId, id)
    VALUES (datasize, getTime(), cfId, originalVID, originalCopyNb, destCopyNb, varTpId, 0,
            inMJStatus, NULL, ids_seq.nextval);
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
  gcwProc VARCHAR2(2048);
  gcw NUMBER;
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  -- compute the gc weight of the brand new diskCopy
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:fs); END;'
    USING OUT gcw, IN fs;
  gcwProc := castorGC.getUserWeight(svcClassId);
  -- update the DiskCopy
  UPDATE DiskCopy
     SET status = dconst.DISKCOPY_VALID,
         lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
         gcWeight = gcw,
         diskCopySize = fs,
         importance = -1              -- we have a single diskcopy for now
   WHERE castorFile = cfId AND status = dconst.DISKCOPY_STAGEOUT
   RETURNING owneruid, ownergid INTO ouid, ogid;
  -- update the CastorFile
  UPDATE Castorfile SET tapeStatus = (CASE WHEN nbTC = 0 OR fs = 0
                                           THEN dconst.CASTORFILE_DISKONLY
                                           ELSE dconst.CASTORFILE_NOTONTAPE
                                       END)
   WHERE id = cfId;
  -- trigger migration when needed
  IF nbTC > 0 AND fs > 0 THEN
    FOR i IN 1..nbTC LOOP
      initMigration(cfId, fs, NULL, NULL, i, tconst.MIGRATIONJOB_PENDING);
    END LOOP;
  END IF;
  -- If we are a real PutDone (and not a put outside of a prepareToPut/Update)
  -- then we have to archive the original prepareToPut/Update subRequest
  IF context = 2 THEN
    -- There can be only a single PrepareTo request: any subsequent PPut would be
    -- rejected and any subsequent PUpdate would be directly archived
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
       AND status IN (0, 1, 2, 3, 6, 13) -- START, RESTART, RETRY, WAITSCHED, READY, READYFORSCHED
       AND ROWNUM < 2;
    -- we've found one, we wait
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_WAITSUBREQ,
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
       AND DiskCopy.status = dconst.DISKCOPY_STAGEOUT;
    IF nbDCs = 0 THEN
      -- This is a PutDone without a put (otherwise we would have found
      -- a DiskCopy on a FileSystem), so we fail the subrequest.
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET
        status = dconst.SUBREQUEST_FAILED,
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
                                                      inNsOpenTime IN NUMBER,
                                                      waitForLock IN BOOLEAN,
                                                      rid OUT INTEGER,
                                                      rfs OUT INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  previousLastKnownFileName VARCHAR2(2048);
  fcId NUMBER;
  varReqType INTEGER;
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
    -- The file is still there, so update timestamps
    UPDATE CastorFile SET lastAccessTime = getTime(),
                          lastKnownFileName = normalizePath(fn),
                          nsOpenTime = inNsOpenTime
     WHERE id = rid;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET castorFile = rid
     WHERE id = srId
     RETURNING reqType INTO varReqType;
    IF varReqType IN (37, 38, 40, 44) THEN  -- all write operations
      -- reset lastUpdateTime: this is used to prevent parallel Put operations from
      -- overtaking each other. As it is still relying on diskservers' timestamps
      -- with seconds precision, we truncate the usec-precision nameserver timestamp
      UPDATE CastorFile SET lastUpdateTime = TRUNC(inNsOpenTime)
       WHERE id = rid;
    ELSE
      -- On pure read operations, we should actually check whether our disk cache is stale,
      -- that is IF CF.nsOpenTime < inNsOpenTime THEN invalidate our diskcopies.
      -- This is pending the full deployment of the 'new open mode' as implemented
      -- in the fix of bug #95189: Time discrepencies between
      -- disk servers and name servers can lead to silent data loss on input.
      -- The problem being that in 'Compatibility' mode inNsOpenTime is the
      -- namespace's mtime, which can be modified by nstouch,
      -- hence nstouch followed by a Get would destroy the data on disk!
      NULL;
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- we did not find the file, let's create a new one:
    -- take care that the name of the new file is not already the lastKnownFileName
    -- of another file, that was renamed but for which the lastKnownFileName has
    -- not been updated.
    -- We actually reset the lastKnownFileName of such a file if needed
    -- Note that this procedure will run in an autonomous transaction so that
    -- no dead lock can result from taking a second lock within this transaction
    dropReusedLastKnownFileName(fn);
    -- insert new row (see above for the TRUNC() operation)
    INSERT INTO CastorFile (id, fileId, nsHost, fileClass, fileSize, creationTime,
                            lastAccessTime, lastUpdateTime, nsOpenTime, lastKnownFileName, tapeStatus)
      VALUES (ids_seq.nextval, fId, nh, fcId, fs, getTime(), getTime(),
              TRUNC(inNsOpenTime), inNsOpenTime, normalizePath(fn), NULL)
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
     AND status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_WAITFS,
                    dconst.DISKCOPY_STAGEOUT, dconst.DISKCOPY_WAITFS_SCHEDULING);
  -- Stop ongoing recalls and migrations
  deleteRecallJobs(cfId);
  deleteMigrationJobs(cfId);
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  FOR sr IN (SELECT /*+ INDEX(Subrequest I_Subrequest_DiskCopy)*/ id, status FROM SubRequest
              WHERE diskcopy IN
                (SELECT /*+ CARDINALITY(dcidTable 5) */ *
                   FROM TABLE(dcsToRm) dcidTable)
                AND status IN (0, 1, 2, 5, 6, 12, 13)) LOOP   -- START, RESTART, RETRY, WAITSUBREQ, READY, READYFORSCHED
    UPDATE SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.EINTR,
           errorMessage = 'Canceled by another user request'
     WHERE (castorfile = cfId AND status = dconst.SUBREQUEST_WAITSUBREQ)
        OR id = sr.id;
  END LOOP;
  -- Set selected DiskCopies to INVALID
  FORALL i IN 1 .. dcsToRm.COUNT
    UPDATE DiskCopy
       SET status = dconst.DISKCOPY_INVALID,
           gcType = inGcType
     WHERE id = dcsToRm(i);
END;
/


/* PL/SQL method implementing renamedFileCleanup */
CREATE OR REPLACE PROCEDURE renamedFileCleanup(inFileName IN VARCHAR2,
                                               inSrId IN INTEGER) AS
  varCfId INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
  varNsPath VARCHAR2(2048);
BEGIN
  -- try to find a file with the right name
  SELECT /*+ INDEX(CastorFile I_CastorFile_LastKnownFileName) */ fileId, nshost, id
    INTO varFileId, varNsHost, varCfId
    FROM CastorFile
   WHERE lastKnownFileName = inFileName;
  -- validate this file against the NameServer
  BEGIN
    SELECT getPathForFileid@remotens(fileId) INTO varNsPath
      FROM Cns_file_metadata@remotens
     WHERE fileid = varFileId;
    -- the nameserver contains a file with this fileid, but
    -- with a different name than the stager. Obviously the
    -- file got renamed and the requested deletion cannot succeed;
    -- anyway we update the stager catalogue with the new name
    DECLARE
      CONSTRAINT_VIOLATED EXCEPTION;
      PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
    BEGIN
      UPDATE CastorFile SET lastKnownFileName = varNsPath
       WHERE id = varCfId;
    EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- we have another file that already uses the new name of this one...
      -- let's fix this, but we won't put the right name there
      UPDATE CastorFile SET lastKnownFileName = TO_CHAR(id)
       WHERE lastKnownFileName = varNsPath;
      UPDATE CastorFile SET lastKnownFileName = varNsPath
       WHERE id = varCfId;
    END;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- the file exists only in the stager db,
    -- execute stageForcedRm (cf. ns synch performed in GC daemon)
    stageForcedRm(varFileId, varNsHost, dconst.GCTYPE_NSSYNCH);
  END;
  -- in all cases we fail the subrequest
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_FAILED, errorCode=serrno.ENOENT,
         errorMessage = 'The file got renamed by another user request'
   WHERE id = inSrId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- No file found with the given name, fail the subrequest with a generic ENOENT
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_FAILED, errorCode=serrno.ENOENT
   WHERE id = inSrId;
END;
/


/* PL/SQL method implementing stageRm */
CREATE OR REPLACE PROCEDURE stageRm (srId IN INTEGER,
                                     fid IN INTEGER,
                                     nh IN VARCHAR2,
                                     svcClassId IN INTEGER,
                                     ret OUT INTEGER) AS
  nsHostName VARCHAR2(2048);
  cfId INTEGER;
  dcsToRm "numList";
  dcsToRmStatus "numList";
  dcsToRmCfStatus "numList";
  nbRJsDeleted INTEGER;
  varNbValidRmed INTEGER;
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
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOENT,
           errorMessage = 'File not found on disk cache'
     WHERE id = srId;
    RETURN;
  END;

  -- select the list of DiskCopies to be deleted
  SELECT id, status, tapeStatus BULK COLLECT INTO dcsToRm, dcsToRmStatus, dcsToRmCfStatus FROM (
    -- first physical diskcopies
    SELECT /*+ INDEX(DC I_DiskCopy_CastorFile) */ DC.id, DC.status, CastorFile.tapeStatus
      FROM DiskCopy DC, FileSystem, DiskPool2SvcClass DP2SC, CastorFile
     WHERE DC.castorFile = cfId
       AND DC.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
       AND DC.fileSystem = FileSystem.id
       AND FileSystem.diskPool = DP2SC.parent
       AND (DP2SC.child = svcClassId OR svcClassId = 0)
       AND CastorFile.id = cfId)
  UNION ALL
    -- and then diskcopies resulting from ongoing requests, for which the previous
    -- query wouldn't return any entry because of e.g. missing filesystem
    SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ DC.id, DC.status, CastorFile.tapeStatus
      FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id
              FROM StagePrepareToPutRequest WHERE svcClass = svcClassId UNION ALL
            SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ id
              FROM StagePutRequest WHERE svcClass = svcClassId) Request,
           SubRequest, DiskCopy DC, CastorFile
     WHERE SubRequest.diskCopy = DC.id
       AND Request.id = SubRequest.request
       AND DC.castorFile = cfId
       AND DC.status IN (dconst.DISKCOPY_WAITFS, dconst.DISKCOPY_WAITFS_SCHEDULING)
       AND CastorFile.id = cfId;

  -- in case we are dropping diskcopies not yet on tape, ensure that we have at least one copy left on disk
  IF dcsToRmStatus.COUNT > 0 THEN
    IF dcsToRmStatus(1) = dconst.DISKCOPY_VALID AND dcsToRmCfStatus(1) = dconst.CASTORFILE_NOTONTAPE THEN
      BEGIN
        SELECT castorFile INTO cfId
          FROM DiskCopy
         WHERE castorFile = cfId
           AND status = dconst.DISKCOPY_VALID
           AND id NOT IN (SELECT /*+ CARDINALITY(dcidTable 5) */ * FROM TABLE(dcsToRm) dcidTable)
           AND ROWNUM < 2;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- nothing left, so we would lose the file. Better to forbid stagerm
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
           SET status = dconst.SUBREQUEST_FAILED,
               errorCode = serrno.EBUSY,
               errorMessage = 'The file is not yet migrated'
         WHERE id = srId;
        RETURN;
      END;
    END IF;

    -- fail diskcopies : WAITFS[_SCHED] -> FAILED, others -> INVALID
    UPDATE DiskCopy
       SET status = decode(status, dconst.DISKCOPY_WAITFS, dconst.DISKCOPY_FAILED,
                                   dconst.DISKCOPY_WAITFS_SCHEDULING, dconst.DISKCOPY_FAILED,
                                   dconst.DISKCOPY_INVALID)
     WHERE id IN (SELECT /*+ CARDINALITY(dcidTable 5) */ * FROM TABLE(dcsToRm) dcidTable)
    RETURNING SUM(decode(status, dconst.DISKCOPY_VALID, 1, 0)) INTO varNbValidRmed;

    -- update importance of remaining DiskCopies, if any
    UPDATE DiskCopy SET importance = importance + varNbValidRmed
     WHERE castorFile = cfId AND status = dconst.DISKCOPY_VALID;

    -- fail the subrequests linked to the deleted diskcopies
    FOR sr IN (SELECT /*+ INDEX(SR I_SubRequest_DiskCopy) */ id, subreqId
                 FROM SubRequest SR
                WHERE diskcopy IN (SELECT /*+ CARDINALITY(dcidTable 5) */ * FROM TABLE(dcsToRm) dcidTable)
                  AND status IN (dconst.SUBREQUEST_START, dconst.SUBREQUEST_RESTART,
                                 dconst.SUBREQUEST_RETRY, dconst.SUBREQUEST_WAITTAPERECALL,
                                 dconst.SUBREQUEST_WAITSUBREQ, dconst.SUBREQUEST_READY,
                                 dconst.SUBREQUEST_READYFORSCHED)) LOOP
      UPDATE SubRequest
         SET status = CASE WHEN status IN (dconst.SUBREQUEST_READY, dconst.SUBREQUEST_READYFORSCHED)
                            AND reqType = 133  -- DiskCopyReplicaRequests
                           THEN dconst.SUBREQUEST_FAILED_FINISHED
                           ELSE dconst.SUBREQUEST_FAILED END,
             -- user requests in status WAITSUBREQ are always marked FAILED
             -- even if they wait on a replication
             errorCode = serrno.EINTR,
             errorMessage = 'Canceled by another user request'
       WHERE id = sr.id
          OR (castorFile = cfId AND status = dconst.SUBREQUEST_WAITSUBREQ);
      -- make the scheduler aware so that it can remove the transfer from the queues if needed
      INSERT INTO TransfersToAbort (uuid) VALUES (sr.subreqId);
    END LOOP;
    -- wake up the scheduler so that it can remove the transfer from the queues now
    DBMS_ALERT.SIGNAL('transfersToAbort', '');
  END IF;

  -- delete RecallJobs that should be canceled
  DELETE FROM RecallJob
   WHERE castorfile = cfId AND (svcClass = svcClassId OR svcClassId = 0);
  nbRJsDeleted := SQL%ROWCOUNT;
  -- in case we've dropped something, check whether we still have recalls ongoing
  IF nbRJsDeleted > 0 THEN
    BEGIN
      SELECT castorFile INTO cfId
        FROM RecallJob
       WHERE castorFile = cfId;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- all recalls are canceled for this file
      -- deal with potential waiting migrationJobs
      deleteMigrationJobsForRecall(cfId);
      -- fail corresponding requests
      UPDATE SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.EINTR,
             errorMessage = 'Canceled by another user request'
       WHERE castorFile = cfId
         AND status = dconst.SUBREQUEST_WAITTAPERECALL;
    END;
  END IF;

  -- In case nothing was dropped at all, complain
  IF dcsToRm.COUNT = 0 AND nbRJsDeleted = 0 THEN
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOENT,
           errorMessage = CASE WHEN svcClassId = 0 THEN 'File not found on disk cache'
                               ELSE 'File not found on this service class' END
     WHERE id = srId;
    RETURN;
  END IF;

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
     AND status IN (0, 1, 2, 3, 4, 5, 7, 10, 12, 13)   -- all but FINISHED, FAILED_FINISHED, ARCHIVED
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

/* PL/SQL method implementing storeReports. This procedure stores new reports
 * and disables nodes for which last report (aka heartbeat) is too old. 
 * For efficiency reasons the input parameters to this method
 * are 2 vectors. The first ones is a list of strings with format :
 *  (diskServerName1, mountPoint1, diskServerName2, mountPoint2, ...)
 * representing a set of mountpoints with the diskservername repeated
 * for each mountPoint.
 * The second vector is a list of numbers with format :
 *  (maxFreeSpace1, minAllowedFreeSpace1, totalSpace1, freeSpace1,
 *   nbReadStreams1, nbWriteStreams1, nbRecalls1, nbMigrations1,
 *   maxFreeSpace2, ...)
 * where 8 values are given for each of the mountPoints in the first vector
 */
CREATE OR REPLACE PROCEDURE storeReports
(inStrParams IN castor."strList",
 inNumParams IN castor."cnumList") AS
 varDsId NUMBER;
 varFsId NUMBER;
 varHeartbeatTimeout NUMBER;
 emptyReport BOOLEAN := False;
BEGIN
  -- quick check of the vector lengths
  IF MOD(inStrParams.COUNT,2) != 0 THEN
    IF inStrParams.COUNT = 1 AND inStrParams(1) = 'Empty' THEN
      -- work around the "PLS-00418: array bind type must match PL/SQL table row type"
      -- error launched by Oracle when empty arrays are passed as parameters
      emptyReport := True;
    ELSE
      RAISE_APPLICATION_ERROR (-20125, 'Invalid call to storeReports : ' ||
                                       '1st vector has odd number of elements (' ||
                                       TO_CHAR(inStrParams.COUNT) || ')');
    END IF;
  END IF;
  IF MOD(inNumParams.COUNT,8) != 0 AND NOT emptyReport THEN
    RAISE_APPLICATION_ERROR (-20125, 'Invalid call to storeReports : ' ||
                             '2nd vector has wrong number of elements (' ||
                             TO_CHAR(inNumParams.COUNT) || ' instead of ' ||
                             TO_CHAR(inStrParams.COUNT*4) || ')');
  END IF;
  IF NOT emptyReport THEN
    -- Go through the concerned filesystems
    FOR i IN 0 .. inStrParams.COUNT/2-1 LOOP
      -- update DiskServer
      varDsId := NULL;
      UPDATE DiskServer
         SET hwOnline = 1,
             lastHeartbeatTime = getTime()
       WHERE name = inStrParams(2*i+1)
      RETURNING id INTO varDsId;
      -- if it did not exist, create it
      IF varDsId IS NULL THEN
        INSERT INTO DiskServer (name, id, status, hwOnline, lastHeartbeatTime)
         VALUES (inStrParams(2*i+1), ids_seq.nextval, dconst.DISKSERVER_DISABLED, 1, getTime())
        RETURNING id INTO varDsId;
      END IF;
      -- update FileSystem
      varFsId := NULL;
      UPDATE FileSystem
         SET maxFreeSpace = inNumParams(8*i+1),
             minAllowedFreeSpace = inNumParams(8*i+2),
             totalSize = inNumParams(8*i+3),
             free = inNumParams(8*i+4),
             nbReadStreams = inNumParams(8*i+5),
             nbWriteStreams = inNumParams(8*i+6),
             nbRecallerStreams = inNumParams(8*i+7),
             nbMigratorStreams = inNumParams(8*i+8)
       WHERE diskServer=varDsId AND mountPoint=inStrParams(2*i+2)
      RETURNING id INTO varFsId;
      -- if it did not exist, create it
      IF varFsId IS NULL THEN
        INSERT INTO FileSystem (mountPoint, maxFreeSpace, minAllowedFreeSpace, totalSize, free,
                                nbReadStreams, nbWriteStreams, nbRecallerStreams, nbMigratorStreams,
                                id, diskPool, diskserver, status)
        VALUES (inStrParams(2*i+2), inNumParams(8*i+1), inNumParams(8*i+2), inNumParams(8*i+3),
                inNumParams(8*i+4), inNumParams(8*i+5), inNumParams(8*i+6), inNumParams(8*i+7),
                inNumParams(8*i+8), ids_seq.nextval, 0, varDsId, dconst.FILESYSTEM_DISABLED);
      END IF;
    END LOOP;
  END IF;

  -- now disable nodes that have too old reports
  varHeartbeatTimeout := TO_NUMBER(getConfigOption('DiskServer', 'HeartbeatTimeout', '180'));
  UPDATE DiskServer
     SET hwOnline = 0
   WHERE lastHeartbeatTime < getTime() - varHeartbeatTimeout
     AND hwOnline = 1;
END;
/

/* PL/SQL method used by the stager to collect the logging made in the DB */
CREATE OR REPLACE PROCEDURE dumpDBLogs(logEntries OUT castor.LogEntry_Cur) AS
  rowIds strListTable;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
BEGIN
  BEGIN
    -- lock whatever we can from the table. This is to prevent deadlocks.
    SELECT ROWID BULK COLLECT INTO rowIds
      FROM DLFLogs FOR UPDATE NOWAIT;
    -- insert data on tmp table and drop selected entries
    INSERT INTO DLFLogsHelper (timeinfo, uuid, priority, msg, fileId, nsHost, SOURCE, params)
     (SELECT timeinfo, uuid, priority, msg, fileId, nsHost, SOURCE, params
      FROM DLFLogs WHERE ROWID IN (SELECT * FROM TABLE(rowIds)));
    DELETE FROM DLFLogs WHERE ROWID IN (SELECT * FROM TABLE(rowIds));
  EXCEPTION WHEN SrLocked THEN
    -- nothing we can lock, as someone else already has the lock.
    -- The logs will be taken by this other guy, so just give up
    NULL;
  END;
  -- return list of entries by opening a cursor on temp table
  OPEN logEntries FOR
    SELECT timeinfo, uuid, priority, msg, fileId, nsHost, source, params FROM DLFLogsHelper;
END;
/

/* PL/SQL method creating MigrationJobs for missing segments of a file if needed */
/* Can throw a -20100 exception when no route to tape is found for the missing segments */
CREATE OR REPLACE PROCEDURE createMJForMissingSegments(inCfId IN INTEGER,
                                                       inFileSize IN INTEGER,
                                                       inFileClassId IN INTEGER,
                                                       inAllCopyNbs IN "numList",
                                                       inAllVIDs IN strListTable,
                                                       inFileId IN INTEGER,
                                                       inNsHost IN VARCHAR2,
                                                       inLogParams IN VARCHAR2) AS
  varExpectedNbCopies INTEGER;
  varCreatedMJs INTEGER := 0;
  varNextCopyNb INTEGER := 1;
  varNb INTEGER;
BEGIN
  -- check whether there are missing segments and whether we should create new ones
  SELECT nbCopies INTO varExpectedNbCopies FROM FileClass WHERE id = inFileClassId;
  IF varExpectedNbCopies > inAllCopyNbs.COUNT THEN
    -- some copies are missing
    DECLARE
      unused INTEGER;
    BEGIN
      -- check presence of migration jobs for this file
      SELECT id INTO unused FROM MigrationJob WHERE castorFile=inCfId AND ROWNUM < 2;
      -- there are MigrationJobs already, so remigrations were already handled. Nothing to be done
      -- we typically are in a situation where the file was already waiting for recall for
      -- another recall group.
      -- log "detected missing copies on tape, but migrations ongoing"
      logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECALL_MISSING_COPIES_NOOP, inFileId, inNsHost, 'stagerd',
               inLogParams || ' nbMissingCopies=' || TO_CHAR(varExpectedNbCopies-inAllCopyNbs.COUNT));
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- we need to remigrate this file
      NULL;
    END;
    -- log "detected missing copies on tape"
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_MISSING_COPIES, inFileId, inNsHost, 'stagerd',
             inLogParams || ' nbMissingCopies=' || TO_CHAR(varExpectedNbCopies-inAllCopyNbs.COUNT));
    -- copies are missing, try to recreate them
    WHILE varExpectedNbCopies > inAllCopyNbs.COUNT + varCreatedMJs AND varNextCopyNb <= varExpectedNbCopies LOOP
      BEGIN
        -- check whether varNextCopyNb is already in use by a valid copy
        SELECT * INTO varNb FROM TABLE(inAllCopyNbs) WHERE COLUMN_VALUE=varNextCopyNb;
        -- this copy number is in use, go to next one
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- copy number is not in use, create a migrationJob using it (may throw exceptions)
        initMigration(inCfId, inFileSize, NULL, NULL, varNextCopyNb, tconst.MIGRATIONJOB_WAITINGONRECALL);
        varCreatedMJs := varCreatedMJs + 1;
        -- log "create new MigrationJob to migrate missing copy"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_MJ_FOR_MISSING_COPY, inFileId, inNsHost, 'stagerd',
                 inLogParams || ' copyNb=' || TO_CHAR(varNextCopyNb));
      END;
      varNextCopyNb := varNextCopyNb + 1;
    END LOOP;
    -- Did we create new copies ?
    IF varExpectedNbCopies > inAllCopyNbs.COUNT + varCreatedMJs THEN
      -- We did not create enough new copies, this means that we did not find enough
      -- valid copy numbers. Odd... Log something !
      logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_COPY_STILL_MISSING, inFileId, inNsHost, 'stagerd',
               inLogParams || ' nbCopiesStillMissing=' ||
               TO_CHAR(varExpectedNbCopies - inAllCopyNbs.COUNT - varCreatedMJs));
    ELSE
      -- Yes, then create migrated segments for the existing segments if there are none
      SELECT count(*) INTO varNb FROM MigratedSegment WHERE castorFile = inCfId;
      IF varNb = 0 THEN
        FOR i IN inAllCopyNbs.FIRST .. inAllCopyNbs.LAST LOOP
          INSERT INTO MigratedSegment (castorFile, copyNb, VID)
          VALUES (inCfId, inAllCopyNbs(i), inAllVIDs(i));
        END LOOP;
      END IF;
    END IF;
  END IF;
END;
/

/* PL/SQL method creating RecallJobs
 * It also creates MigrationJobs for eventually missing segments
 * It returns 0 if successful, else an error code
 */
CREATE OR REPLACE FUNCTION createRecallJobs(inCfId IN INTEGER,
                                            inFileId IN INTEGER,
                                            inNsHost IN VARCHAR2,
                                            inFileSize IN INTEGER,
                                            inFileClassId IN INTEGER,
                                            inRecallGroupId IN INTEGER,
                                            inSvcClassId IN INTEGER,
                                            inEuid IN INTEGER,
                                            inEgid IN INTEGER,
                                            inRequestTime IN NUMBER,
                                            inLogParams IN VARCHAR2) RETURN INTEGER AS
  -- list of all valid segments, whatever the tape status. Used to trigger remigrations
  varAllCopyNbs "numList" := "numList"();
  varAllVIDs strListTable := strListTable();
  -- whether we found a segment at all (valid or not). Used to detect potentially lost files
  varFoundSeg boolean := FALSE;
  varI INTEGER := 1;
  NO_TAPE_ROUTE EXCEPTION;
  PRAGMA EXCEPTION_INIT(NO_TAPE_ROUTE, -20100);
  varErrorMsg VARCHAR2(2048);
BEGIN
  BEGIN
    -- loop over the existing segments
    FOR varSeg IN (SELECT s_fileId as fileId, 0 as lastModTime, copyNo, segSize, 0 as comprSize,
                          Cns_seg_metadata.vid, fseq, blockId, checksum_name, nvl(checksum, 0) as checksum,
                          Cns_seg_metadata.s_status as segStatus, Vmgr_tape_status_view.status as tapeStatus
                     FROM Cns_seg_metadata@remotens, Vmgr_tape_status_view@remotens
                    WHERE Cns_seg_metadata.s_fileid = inFileId
                      AND Vmgr_tape_status_view.VID = Cns_seg_metadata.VID
                    ORDER BY copyno, fsec) LOOP
      varFoundSeg := TRUE;
      -- Is the segment valid
      IF varSeg.segStatus = '-' THEN
        -- remember the copy number and tape
        varAllCopyNbs.EXTEND;
        varAllCopyNbs(varI) := varSeg.copyno;
        varAllVIDs.EXTEND;
        varAllVIDs(varI) := varSeg.vid;
        varI := varI + 1;
        -- Is the segment on a valid tape from recall point of view ?
        IF BITAND(varSeg.tapeStatus, tconst.TAPE_DISABLED) = 0 AND
           BITAND(varSeg.tapeStatus, tconst.TAPE_EXPORTED) = 0 AND
           BITAND(varSeg.tapeStatus, tconst.TAPE_ARCHIVED) = 0 THEN
          -- create recallJob
          INSERT INTO RecallJob (id, castorFile, copyNb, recallGroup, svcClass, euid, egid,
                                 vid, fseq, status, fileSize, creationTime, blockId, fileTransactionId)
          VALUES (ids_seq.nextval, inCfId, varSeg.copyno, inRecallGroupId, inSvcClassId,
                  inEuid, inEgid, varSeg.vid, varSeg.fseq, tconst.RECALLJOB_PENDING, inFileSize, inRequestTime,
                  varSeg.blockId, NULL);
          -- log "created new RecallJob"
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_CREATING_RECALLJOB, inFileId, inNsHost, 'stagerd',
                   inLogParams || ' copyNb=' || TO_CHAR(varSeg.copyno) || ' TPVID=' || varSeg.vid ||
                   ' fseq=' || TO_CHAR(varSeg.fseq || ' FileSize=' || TO_CHAR(inFileSize)));
        ELSE
          -- invalid tape found. Log it.
          -- "createRecallCandidate: found segment on unusable tape"
          logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECALL_UNUSABLE_TAPE, inFileId, inNsHost, 'stagerd',
                   inLogParams || ' segStatus=OK tapeStatus=' || tapeStatusToString(varSeg.tapeStatus));
        END IF;
      ELSE
        -- invalid segment tape found. Log it.
        -- "createRecallCandidate: found unusable segment"
        logToDLF(NULL, dlf.LVL_NOTICE, dlf.RECALL_INVALID_SEGMENT, inFileId, inNsHost, 'stagerd',
                 inLogParams || ' segStatus=' ||
                 CASE varSeg.segStatus WHEN '-' THEN 'OK'
                                       WHEN 'D' THEN 'DISABLED'
                                       ELSE 'UNKNOWN:' || varSeg.segStatus END);
      END IF;
    END LOOP;
  EXCEPTION WHEN OTHERS THEN
    -- log "error when retrieving segments from namespace"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_UNKNOWN_NS_ERROR, inFileId, inNsHost, 'stagerd',
             inLogParams || ' ErrorMessage=' || SQLERRM);
    RETURN serrno.SEINTERNAL;
  END;
  -- If we did not find any valid segment to recall, log a critical error as the file is probably lost
  IF NOT varFoundSeg THEN
    -- log "createRecallCandidate: no segment found for this file. File is probably lost"
    logToDLF(NULL, dlf.LVL_CRIT, dlf.RECALL_NO_SEG_FOUND_AT_ALL, inFileId, inNsHost, 'stagerd', inLogParams);
    RETURN serrno.ESTNOSEGFOUND;
  END IF;
  -- If we found no valid segment (but some disabled ones), log a warning
  IF varAllCopyNbs.COUNT = 0 THEN
    -- log "createRecallCandidate: no valid segment to recall found"
    logToDLF(NULL, dlf.LVL_WARNING, dlf.RECALL_NO_SEG_FOUND, inFileId, inNsHost, 'stagerd', inLogParams);
    RETURN serrno.ESTNOSEGFOUND;
  END IF;
  BEGIN
    -- create missing segments if needed
    createMJForMissingSegments(inCfId, inFileSize, inFileClassId, varAllCopyNbs,
                               varAllVIDs, inFileId, inNsHost, inLogParams);
  EXCEPTION WHEN NO_TAPE_ROUTE THEN
    -- there's at least a missing segment and we cannot recreate it!
    -- log a "no route to tape defined for missing copy" error, but don't fail the recall
    logToDLF(NULL, dlf.LVL_ALERT, dlf.RECALL_MISSING_COPY_NO_ROUTE, inFileId, inNsHost, 'stagerd', inLogParams);
  WHEN OTHERS THEN
    -- some other error happened, log "unexpected error when creating missing copy", but don't fail the recall
    varErrorMsg := 'Oracle error caught : ' || SQLERRM;
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_MISSING_COPY_ERROR, inFileId, inNsHost, 'stagerd',
      'errorCode=' || to_char(SQLCODE) ||' errorMessage="' || varErrorMsg
      ||'" stackTrace="' || dbms_utility.format_error_backtrace ||'"');
  END;
  RETURN 0;
END;
/

/* PL/SQL method that selects the recallGroup to be used */
CREATE OR REPLACE PROCEDURE getRecallGroup(inEuid IN INTEGER,
                                           inEgid IN INTEGER,
                                           outRecallGroup OUT INTEGER,
                                           outRecallGroupName OUT VARCHAR2) AS
BEGIN
  -- try to find a specific group
  SELECT RecallGroup.id, RecallGroup.name
    INTO outRecallGroup, outRecallGroupName
    FROM RecallGroup, RecallUser
   WHERE (RecallUser.euid = inEuid OR RecallUser.euid IS NULL)
     AND RecallUser.egid = inEgid
     AND RecallGroup.id = RecallUser.recallGroup;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- go back to default
  BEGIN
    SELECT id, name INTO outRecallGroup, outRecallGroupName
      FROM RecallGroup
     WHERE name='default';
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- default is not properly defined. Complain !
    RAISE_APPLICATION_ERROR (-20124, 'Configuration error : no default recallGroup defined');
  END;
END;
/

/* PL/SQL method used by the stager to handle recalls
 * note that this method should only be called with a lock on the concerned CastorFile
 */
CREATE OR REPLACE FUNCTION createRecallCandidate(inSrId IN INTEGER) RETURN INTEGER AS
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
  varFileName VARCHAR2(2048);
  varSvcClassId VARCHAR2(2048);
  varFileClassId INTEGER;
  varFileSize INTEGER;
  varCfId INTEGER;
  varRecallGroup INTEGER;
  varRecallGroupName VARCHAR2(2048);
  varSubReqUUID VARCHAR2(2048);
  varEuid INTEGER;
  varEgid INTEGER;
  varReqTime NUMBER;
  varReqUUID VARCHAR2(2048);
  varReqId INTEGER;
  varReqType VARCHAR2(2048);
  varIsBeingRecalled INTEGER;
  varRc INTEGER := 0;
BEGIN
  -- get some useful data from CastorFile, subrequest and request
  SELECT castorFile, request, fileName, SubReqId
    INTO varCfId, varReqId, varFileName, varSubReqUUID
    FROM SubRequest WHERE id = inSrId;
  SELECT fileid, nsHost, fileClass, fileSize INTO varFileId, varNsHost, varFileClassId, varFileSize
    FROM CastorFile WHERE id = varCfId;
  SELECT Request.reqId, Request.svcClass, Request.euid, Request.egid, Request.creationTime, Request.reqtype
    INTO varReqUUID, varSvcClassId, varEuid, varEgid, varReqTime, varReqType
    FROM (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                 id, svcClass, euid, egid, reqId, creationTime, 'StageGetRequest' as reqtype FROM StageGetRequest UNION ALL
          SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */
                 id, svcClass, euid, egid, reqId, creationTime, 'StagePrepareToGetRequest' as reqtype FROM StagePrepareToGetRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                 id, svcClass, euid, egid, reqId, creationTime, 'StageUpdateRequest' as reqtype FROM StageUpdateRequest UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequest_Id) */
                 id, svcClass, euid, egid, reqId, creationTime, 'StagePrepareToUpdateRequest' as reqtype FROM StagePrepareToUpdateRequest) Request
   WHERE Request.id = varReqId;
  -- get the RecallGroup
  getRecallGroup(varEuid, varEgid, varRecallGroup, varRecallGroupName);
  -- check whether this file is already being recalled for this RecallGroup
  -- or being actively recalled by any group
  SELECT count(*) INTO varIsBeingRecalled
    FROM RecallJob
   WHERE castorFile = varCfId
     AND (recallGroup = varRecallGroup OR status = tconst.RECALLJOB_SELECTED);
  -- trigger recall only if recall is not already ongoing
  IF varIsBeingRecalled != 0 THEN
    -- createRecallCandidate: found already running recall
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_FOUND_ONGOING_RECALL, varFileId, varNsHost, 'stagerd',
             'FileName=' || varFileName || ' REQID=' || varReqUUID ||
             ' SUBREQID=' || varSubReqUUID || ' RecallGroup=' || varRecallGroupName ||
             ' RequestType=' || varReqType);
  ELSE
    varRc := createRecallJobs(varCfId, varFileId, varNsHost, varFileSize, varFileClassId,
                              varRecallGroup, varSvcClassId, varEuid, varEgid, varReqTime,
                              'FileName=' || varFileName || ' REQID=' || varReqUUID ||
                              ' SUBREQID=' || varSubReqUUID || ' RecallGroup=' || varRecallGroupName ||
                              ' RequestType=' || varReqType);
  END IF;
  -- update the state of the SubRequest
  IF varRc = 0 THEN
    UPDATE Subrequest SET status = dconst.SUBREQUEST_WAITTAPERECALL WHERE id = inSrId;
    RETURN dconst.SUBREQUEST_WAITTAPERECALL;
  ELSE
    UPDATE Subrequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = varRc
     WHERE id = inSrId;
    RETURN dconst.SUBREQUEST_FAILED;
  END IF;
END;
/

/* PL/SQL method to either force GC of the given diskCopies or delete them when the physical files behind have been lost */
CREATE OR REPLACE PROCEDURE deleteDiskCopies(inDcIds IN castor."cnumList", inFileIds IN castor."cnumList", inForce IN BOOLEAN, inDryRun IN BOOLEAN, outRes OUT castor.DiskCopyResult_Cur, outDiskPool OUT VARCHAR2) AS
  varNsHost VARCHAR2(100);
  varFileName VARCHAR2(2048);
  varCfId INTEGER;
  varNbRemaining INTEGER;
  varStatus INTEGER;
  varFStatus VARCHAR2(1);
  varLogParams VARCHAR2(2048);
BEGIN
  DELETE FROM DeleteDiskCopyHelper;
  FOR i IN 1..inDcIds.COUNT LOOP
    BEGIN
      -- get data and lock
      SELECT castorFile, DiskCopy.status, DiskPool.name
        INTO varCfId, varStatus, outDiskPool
        FROM DiskPool, FileSystem, DiskCopy
       WHERE DiskCopy.id = inDcIds(i)
         AND DiskCopy.fileSystem = FileSystem.id
         AND FileSystem.diskPool = DiskPool.id;
      SELECT nsHost, lastKnownFileName
        INTO varNsHost, varFileName
        FROM CastorFile
       WHERE id = varCfId
         AND fileId = inFileIds(i)
         FOR UPDATE;
      varLogParams := 'FileName="' || varFileName ||'" DiskPool="'|| outDiskPool
        ||'" dcId='|| inDcIds(i) ||' status='
        || getObjStatusName('DiskCopy', 'status', varStatus);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- diskcopy not found in stager
      INSERT INTO DeleteDiskCopyHelper (dcId, rc)
        VALUES (inDcIds(i), dconst.DELDC_ENOENT);
      COMMIT;
      CONTINUE;
    END;
    BEGIN
      -- get the Nameserver status in case we have to also drop the namespace entry
      SELECT status INTO varFStatus FROM Cns_file_metadata@RemoteNS
       WHERE fileid = inFileIds(i);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- not found in the Nameserver means that we can scrap everything and there's no data loss
      -- as we're anticipating the NS synchronization
      varFStatus := 'd';
    END;
    -- count remaining ones
    SELECT count(*) INTO varNbRemaining FROM DiskCopy
     WHERE castorFile = varCfId
       AND status = dconst.DISKCOPY_VALID
       AND id != inDcIds(i);
    -- and update their importance if needed (other copy exists and dropped one was valid)
    IF varNbRemaining > 0 AND varStatus = dconst.DISKCOPY_VALID THEN
      UPDATE DiskCopy SET importance = importance + 1
       WHERE castorFile = varCfId
         AND status = dconst.DISKCOPY_VALID;
    END IF;
    IF inForce = TRUE THEN
      -- the physical diskcopy is deemed lost: delete the diskcopy entry
      -- and potentially drop dangling entities
      IF NOT inDryRun THEN
        DELETE FROM DiskCopy WHERE id = inDcIds(i);
      END IF;
      IF varStatus = dconst.DISKCOPY_STAGEOUT THEN
        -- fail outstanding requests
        UPDATE SubRequest
           SET status = dconst.SUBREQUEST_FAILED,
               errorCode = serrno.SEINTERNAL,
               errorMessage = 'File got lost while being written to'
         WHERE diskCopy = inDcIds(i)
           AND status = dconst.SUBREQUEST_READY;
      END IF;
      -- was it the last active one?
      IF varNbRemaining = 0 THEN
        IF NOT inDryRun THEN
          -- yes, drop the (now bound to fail) migration job(s)
          deleteMigrationJobs(varCfId);
        END IF;
        -- check if the entire castorFile chain can be dropped
        IF NOT inDryRun THEN
          deleteCastorFile(varCfId);
        END IF;
        IF varFStatus = 'm' THEN
          -- file is on tape: let's recall it. This may potentially trigger a new migration
          INSERT INTO DeleteDiskCopyHelper (dcId, rc)
            VALUES (inDcIds(i), dconst.DELDC_RECALL);
          IF NOT inDryRun THEN
            logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_RECALL, inFileIds(i), varNsHost, 'stagerd', varLogParams);
          END IF;
        ELSIF varFStatus = 'd' THEN
          -- file was dropped, report as if we have run a standard GC
          INSERT INTO DeleteDiskCopyHelper (dcId, rc)
            VALUES (inDcIds(i), dconst.DELDC_GC);
          IF NOT inDryRun THEN
            logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_GC, inFileIds(i), varNsHost, 'stagerd', varLogParams);
          END IF;
        ELSE
          -- file is really lost, we'll remove the namespace entry afterwards
          INSERT INTO DeleteDiskCopyHelper (dcId, rc)
            VALUES (inDcIds(i), dconst.DELDC_LOST);
          IF NOT inDryRun THEN
            logToDLF(NULL, dlf.LVL_WARNING, dlf.DELETEDISKCOPY_LOST, inFileIds(i), varNsHost, 'stagerd', varLogParams);
          END IF;
        END IF;
      ELSE
        -- it was not the last valid copy, replicate from another one
        INSERT INTO DeleteDiskCopyHelper (dcId, rc)
          VALUES (inDcIds(i), dconst.DELDC_REPLICATION);
        IF NOT inDryRun THEN
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_REPLICATION, inFileIds(i), varNsHost, 'stagerd', varLogParams);
        END IF;
      END IF;
    ELSE
      -- similarly to stageRm, check that the deletion is allowed:
      -- basically only files on tape may be dropped in case no data loss is provoked,
      -- or files already dropped from the namespace. The rest is forbidden.
      IF (varStatus = dconst.DISKCOPY_VALID AND (varNbRemaining > 0 OR varFStatus = 'm'))
         OR varFStatus = 'd' THEN
        INSERT INTO DeleteDiskCopyHelper (dcId, rc)
          VALUES (inDcIds(i), dconst.DELDC_GC);
        IF NOT inDryRun THEN
          UPDATE DiskCopy
             SET status = dconst.DISKCOPY_INVALID, gcType = dconst.GCTYPE_ADMIN
           WHERE id = inDcIds(i);
           logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_GC, inFileIds(i), varNsHost, 'stagerd', varLogParams);
        END IF;
      ELSE
        -- nothing is done, just record no-action
        INSERT INTO DeleteDiskCopyHelper (dcId, rc)
          VALUES (inDcIds(i), dconst.DELDC_NOOP);
        IF NOT inDryRun THEN
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_NOOP, inFileIds(i), varNsHost, 'stagerd', varLogParams);
        END IF;
        COMMIT;
        CONTINUE;
      END IF;
    END IF;
    COMMIT;   -- release locks file by file
  END LOOP;
  -- return back all results for the python script to post-process them,
  -- including performing all required actions
  OPEN outRes FOR
    SELECT dcId, rc FROM DeleteDiskCopyHelper;
END;
/

/* PL/SQL procedure to handle disk-to-disk copy replication */
CREATE OR REPLACE PROCEDURE handleReplication(inSRId IN INTEGER, inFileId IN INTEGER,
                                              inNsHost IN VARCHAR2, inCfId IN INTEGER,
                                              inNsOpenTime IN INTEGER, inSvcClassId IN INTEGER,
                                              inEuid IN INTEGER, inEGID IN INTEGER,
                                              inReqUUID IN VARCHAR2, inSrUUID IN VARCHAR2) AS
  varUnused INTEGER;
BEGIN
  -- check whether there's already an ongoing replication
  SELECT id INTO varUnused
    FROM Disk2DiskCopyJob
   WHERE castorfile = inCfId;
  -- there is an ongoing replication, so just let it go and do nothing more
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no ongoing replication, let's see whether we need to trigger one
  -- that is compare total current # of diskcopies, regardless hardware availability, against maxReplicaNb
  DECLARE
    varNbDCs INTEGER;
    varMaxDCs INTEGER;
    varNbDss INTEGER;
  BEGIN
    SELECT COUNT(*), max(maxReplicaNb) INTO varNbDCs, varMaxDCs FROM (
      SELECT DiskCopy.id, maxReplicaNb
        FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass
       WHERE DiskCopy.castorfile = inCfId
         AND DiskCopy.fileSystem = FileSystem.id
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
         AND SvcClass.id = inSvcClassId
         AND DiskCopy.status = dconst.DISKCOPY_VALID);
    IF varNbDCs < varMaxDCs OR varMaxDCs = 0 THEN
      -- we have to replicate. But we should ony do it if we have enough available diskservers.
      SELECT COUNT(DISTINCT(DiskServer.name)) INTO varNbDSs
        FROM DiskServer, FileSystem, DiskPool2SvcClass
       WHERE FileSystem.diskServer = DiskServer.id
         AND FileSystem.diskPool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = inSvcClassId
         AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
         AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
         AND DiskServer.hwOnline = 1
         AND DiskServer.id NOT IN (
           SELECT DISTINCT(DiskServer.id)
             FROM DiskCopy, FileSystem, DiskServer
            WHERE DiskCopy.castorfile = inCfId
              AND DiskCopy.fileSystem = FileSystem.id
              AND FileSystem.diskserver = DiskServer.id
              AND DiskCopy.status = dconst.DISKCOPY_VALID);
      IF varNbDSs > 0 THEN
        DECLARE
          varSrcDcId NUMBER;
          varSrcSvcClassId NUMBER;
        BEGIN
          -- yes, we can replicate, create a replication request without waiting on it.
          createDisk2DiskCopyJob(inCfId, inNsOpenTime, varSrcSvcClassId, inEuid, inEgid,
                                 dconst.REPLICATIONTYPE_INTERNAL, NULL, NULL);
          -- log it
          logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_GET_REPLICATION, inFileId, inNsHost, 'stagerd',
                   'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId) ||
                   ' srcDcId=' || TO_CHAR(varSrcDcId) || ' srcSvcClassId=' || getSvcClassName(varSrcSvcClassId) ||
                   ' euid=' || TO_CHAR(inEuid) || ' egid=' || TO_CHAR(inEgid));
        EXCEPTION WHEN NO_DATA_FOUND THEN
          logToDLF(inReqUUID, dlf.LVL_WARNING, dlf.STAGER_GET_REPLICATION_FAIL, inFileId, inNsHost, 'stagerd',
                   'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId) ||
                   ' srcDcId=' || TO_CHAR(varSrcDcId) || ' euid=' || TO_CHAR(inEuid) ||
                   ' egid=' || TO_CHAR(inEgid));
        END;
      END IF;
    END IF;
  END;
END;
/

/* PL/SQL method implementing triggerD2dOrRecall
 * returns 1 if a recall was successfully triggered
 */
CREATE OR REPLACE FUNCTION triggerD2dOrRecall(inCfId IN INTEGER, inNsOpenTime IN INTEGER,  inSrId IN INTEGER,
                                              inFileId IN INTEGER, inNsHost IN VARCHAR2,
                                              inEuid IN INTEGER, inEgid IN INTEGER,
                                              inSvcClassId IN INTEGER, inFileSize IN INTEGER,
                                              inReqUUID IN VARCHAR2, inSrUUID IN VARCHAR2) RETURN INTEGER AS
  varSrcDcId NUMBER;
  varSrcSvcClassId NUMBER;
BEGIN
  -- Check whether we can use disk to disk copy or whether we need to trigger a recall
  checkForD2DCopyOrRecall(inCfId, inSrId, inEuid, inEgid, inSvcClassId, varSrcDcId, varSrcSvcClassId);
  IF varSrcDcId > 0 THEN
    -- create DiskCopyCopyJob and make this subRequest wait on it
    createDisk2DiskCopyJob(inCfId, inNsOpenTime, inSvcClassId, inEuid, inEgid,
                           dconst.REPLICATIONTYPE_USER, NULL, NULL);
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_WAITSUBREQ
     WHERE id = inSrId;
    logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_D2D_TRIGGERED, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID || ' svcClass=' || getSvcClassName(inSvcClassId) ||
             ' srcDcId=' || TO_CHAR(varSrcDcId) || ' euid=' || TO_CHAR(inEuid) ||
             ' egid=' || TO_CHAR(inEgid));
  ELSIF varSrcDcId = 0 THEN
    -- no diskcopy found, no disk to disk copy possibility
    IF inFileSize = 0 THEN
      -- case of a 0 size file, we create it on the fly and schedule it
      createEmptyFile(inCfId, inFileId, inNsHost, inSrId, 1);
    ELSE
      -- regular file, go for a recall
      IF (createRecallCandidate(inSrId) = dconst.SUBREQUEST_FAILED) THEN 
        RETURN 0;
      END IF;
    END IF;
  ELSE
    -- user error
    logToDLF(inReqUUID, dlf.LVL_USER_ERROR, dlf.STAGER_UNABLETOPERFORM, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId));
    RETURN 0;
  END IF;
  RETURN 1;
END;
/

/*
 * handle a raw put/upd (i.e. outside a prepareToPut) or a prepareToPut/Upd
 * return 1 if the client needs to be replied to, else 0
 */
CREATE OR REPLACE FUNCTION handleRawPutOrPPut(inCfId IN INTEGER, inSrId IN INTEGER,
                                              inFileId IN INTEGER, inNsHost IN VARCHAR2,
                                              inFileClassId IN INTEGER, inSvcClassId IN INTEGER,
                                              inEuid IN INTEGER, inEgid IN INTEGER,
                                              inReqUUID IN VARCHAR2, inSrUUID IN VARCHAR2,
                                              inDoSchedule IN BOOLEAN) RETURN INTEGER AS
BEGIN
  -- check if the file can be routed to tape
  IF checkNoTapeRouting(inFileClassId) = 1 THEN
    -- We could not route the file to tape, so we fail the opening
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ESTNOTAPEROUTE,
           errorMessage = 'File recreation canceled since the file cannot be routed to tape'
     WHERE id = inSrId;
    logToDLF(inReqUUID, dlf.LVL_USER_ERROR, dlf.STAGER_RECREATION_IMPOSSIBLE, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId) ||
             ' fileClassId=' || getFileClassName(inFileClassId) || ' reason="no route to tape"');
    RETURN 0;
  END IF;

  -- delete ongoing disk2DiskCopyJobs
  deleteDisk2DiskCopyJobs(inCfId);

  -- delete ongoing recalls
  deleteRecallJobs(inCfId);

  -- fail recall requests pending on the previous file
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_FAILED,
         errorCode = serrno.EINTR,
         errorMessage = 'Canceled by another user request'
   WHERE castorFile = inCfId
     AND status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_REPACK);

  -- delete ongoing migrations
  deleteMigrationJobs(inCfId);

  -- set DiskCopies to INVALID
  UPDATE DiskCopy
     SET status = dconst.DISKCOPY_INVALID,
         gcType = dconst.GCTYPE_OVERWRITTEN
   WHERE castorFile = inCfId
     AND status = dconst.DISKCOPY_VALID;

  -- create new DiskCopy, associate it to SubRequest and schedule
  DECLARE
    varDcId INTEGER;
    varPath VARCHAR2(2048);
  BEGIN
    logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_CASTORFILE_RECREATION, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId));
    -- DiskCopy creation
    varDcId := ids_seq.nextval();
    buildPathFromFileId(inFileId, inNsHost, varDcId, varPath);
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime, lastAccessTime,
                          gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid, importance)
    VALUES (varPath, varDcId, 0, inCfId, dconst.DISKCOPY_WAITFS, getTime(), getTime(), 0, 0, 0, inEuid, inEgid, 0);
    -- update and schedule the subRequest if needed
    IF inDoSchedule THEN
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET diskCopy = varDcId, lastModificationTime = getTime(),
             xsize = CASE WHEN xsize = 0
                          THEN (SELECT defaultFileSize FROM SvcClass WHERE id = inSvcClassId)
                          ELSE xsize
                     END,
             status = dconst.SUBREQUEST_READYFORSCHED,
             getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED
       WHERE id = inSrId;
    ELSE
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET diskCopy = varDcId, lastModificationTime = getTime(),
             status = dconst.SUBREQUEST_READY
       WHERE id = inSrId;
    END IF;
    -- reset the castorfile size as the file was truncated
    UPDATE CastorFile SET fileSize = 0 WHERE id = inCfId;
  END;
  RETURN (CASE WHEN inDoSchedule THEN 0 ELSE 1 END);
END;
/

/* handle a put/upd outside a prepareToPut/Upd */
CREATE OR REPLACE PROCEDURE handlePutInsidePrepareToPut(inCfId IN INTEGER, inSrId IN INTEGER,
                                                        inFileId IN INTEGER, inNsHost IN VARCHAR2,
                                                        inDcId IN INTEGER, inSvcClassId IN INTEGER,
                                                        inReqUUID IN VARCHAR2, inSrUUID IN VARCHAR2) AS
  varFsId INTEGER;
  varStatus INTEGER;
BEGIN
  -- Retrieve the infos about the DiskCopy to be used
  SELECT fileSystem, status INTO varFsId, varStatus FROM DiskCopy WHERE id = inDcId;

  -- handle the case where another concurrent Put|Update request overtook us
  IF varStatus = dconst.DISKCOPY_WAITFS_SCHEDULING THEN
    -- another Put|Update request was faster, subRequest needs to wait on it
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_WAITSUBREQ
     WHERE id = inSrId;
    logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_WAITSUBREQ, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID);
    RETURN;
  ELSE
    IF varStatus = dconst.DISKCOPY_WAITFS THEN
      -- we are the first put/update in the prepare session
      -- we change the diskCopy status to ensure that nobody else schedules it
      UPDATE DiskCopy SET status = dconst.DISKCOPY_WAITFS_SCHEDULING
       WHERE castorFile = inCfId
         AND status = dconst.DISKCOPY_WAITFS;
    END IF;
  END IF;

  -- schedule the request 
  DECLARE
    varReqFileSystem VARCHAR(2048) := '';
  BEGIN
    logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_CASTORFILE_RECREATION, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID);
    -- retrieve requested filesystem if any
    IF varFsId != 0 THEN
      SELECT diskServer.name || ':' || fileSystem.mountPoint INTO varReqFileSystem
        FROM FileSystem, DiskServer
       WHERE FileSystem.id = varFsId
         AND DiskServer.id = FileSystem.diskServer;
    END IF;
    -- update and schedule the subRequest
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET diskCopy = inDcId,
           lastModificationTime = getTime(),
           requestedFileSystems = varReqFileSystem,
           xsize = CASE WHEN xsize = 0
                        THEN (SELECT defaultFileSize FROM SvcClass WHERE id = inSvcClassId)
                        ELSE xsize
                   END,
           status = dconst.SUBREQUEST_READYFORSCHED,
           getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED
     WHERE id = inSrId;
    -- reset the castorfile size as the file was truncated
    UPDATE CastorFile SET fileSize = 0 WHERE id = inCfId;
  END;
END;
/

/* PL/SQL method implementing handlePut */
CREATE OR REPLACE PROCEDURE handlePut(inCfId IN INTEGER, inSrId IN INTEGER,
                                      inFileId IN INTEGER, inNsHost IN VARCHAR2) AS
  varFileClassId INTEGER;
  varSvcClassId INTEGER;
  varEuid INTEGER;
  varEgid INTEGER;
  varReqUUID VARCHAR(2048);
  varSrUUID VARCHAR(2048);
  varHasPrepareReq BOOLEAN;
  varPrepDcid INTEGER;
BEGIN
  -- Get fileClass and lock access to the CastorFile
  SELECT fileclass INTO varFileClassId FROM CastorFile WHERE id = inCfId FOR UPDATE;
  -- Get serviceClass and user data
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         Request.svcClass, euid, egid, Request.reqId, SubRequest.subreqId
    INTO varSvcClassId, varEuid, varEgid, varReqUUID, varSrUUID
    FROM (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                 id, svcClass, euid, egid, reqId FROM StagePutRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                 id, svcClass, euid, egid, reqId FROM StageUpdateRequest) Request, SubRequest
   WHERE SubRequest.id = inSrId
     AND Request.id = SubRequest.request;
  -- log
  logToDLF(varReqUUID, dlf.LVL_DEBUG, dlf.STAGER_PUT, inFileId, inNsHost, 'stagerd', 'SUBREQID=' || varSrUUID);

  -- check whether there is a PrepareToPut/Update going on. There can be only a single one
  -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
  -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
  DECLARE
    varPrepSvcClassId INTEGER;
  BEGIN
    -- look for the (eventual) prepare request and get its service class
    SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ PrepareRequest.svcClass, SubRequest.diskCopy
      INTO varPrepSvcClassId, varPrepDcid
      FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */
                   id, svcClass FROM StagePrepareToPutRequest UNION ALL
            SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                   id, svcClass FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
     WHERE SubRequest.CastorFile = inCfId
       AND PrepareRequest.id = SubRequest.request
       AND SubRequest.status = dconst.SUBREQUEST_READY;
    -- we found a PrepareRequest, but are we in the same service class ?
    IF varSvcClassId != varPrepSvcClassId THEN
      -- No, this means we are a Put/Update and another Prepare request
      -- is already running in a different service class. This is forbidden
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.EBUSY,
             errorMessage = 'A prepareToPut is running in another service class for this file'
       WHERE id = inSrId;
      logToDLF(varReqUUID, dlf.LVL_USER_ERROR, dlf.STAGER_RECREATION_IMPOSSIBLE, inFileId, inNsHost, 'stagerd',
               'SUBREQID=' || varSrUUID || ' reason="A prepareToPut is running in another service class"' ||
               ' svcClassID=' || getFileClassName(varFileClassId) ||
               ' otherSvcClass=' || getSvcClassName(varPrepSvcClassId));
      RETURN;
    END IF;
    -- if we got here, we are a Put/Update inside a Prepare request running in the same service class
    varHasPrepareReq := True;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- if we got here, we are a standalone Put/Update
    varHasPrepareReq := False;
  END;

  -- in case of disk only pool, check if there is space in the diskpool 
  IF checkFailJobsWhenNoSpace(varSvcClassId) = 1 THEN
    -- The svcClass is declared disk only and has no space thus we cannot recreate the file
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOSPC,
           errorMessage = 'File creation canceled since disk pool is full'
     WHERE id = inSrId;
    logToDLF(varReqUUID, dlf.LVL_USER_ERROR, dlf.STAGER_RECREATION_IMPOSSIBLE, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || varSrUUID || ' reason="disk pool is full"' ||
             ' svcClassID=' || getFileClassName(varFileClassId));
    RETURN;
  END IF;

  -- core processing of the request
  IF varHasPrepareReq THEN
    handlePutInsidePrepareToPut(inCfId, inSrId, inFileId, inNsHost, varPrepDcid, varSvcClassId,
                                varReqUUID, varSrUUID);
  ELSE
    DECLARE
      varIgnored INTEGER;
    BEGIN
      varIgnored := handleRawPutOrPPut(inCfId, inSrId, inFileId, inNsHost,
                                       varFileClassId, varSvcClassId,
                                       varEuid, varEgid, varReqUUID, varSrUUID, True);
    END;
  END IF;
END;
/

/* PL/SQL method implementing handleGet */
CREATE OR REPLACE PROCEDURE handleGet(inCfId IN INTEGER, inSrId IN INTEGER,
                                      inFileId IN INTEGER, inNsHost IN VARCHAR2,
                                      inFileSize IN INTEGER) AS
  varNsOpenTime INTEGER;
  varEuid NUMBER;
  varEgid NUMBER;
  varSvcClassId NUMBER;
  varUpd INTEGER;
  varReqUUID VARCHAR(2048);
  varSrUUID VARCHAR(2048);
  varNbDCs INTEGER;
  varDcStatus INTEGER;
BEGIN
  -- lock the castorFile to be safe in case of concurrent subrequests
  SELECT nsOpenTime INTO varNsOpenTime FROM CastorFile WHERE id = inCfId FOR UPDATE;
  -- retrieve the svcClass, user and log data for this subrequest
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         Request.euid, Request.egid, Request.svcClass, Request.upd,
         Request.reqId, SubRequest.subreqId
    INTO varEuid, varEgid, varSvcClassId, varUpd, varReqUUID, varSrUUID
    FROM (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                 id, euid, egid, svcClass, 0 upd, reqId from StageGetRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                 id, euid, egid, svcClass, 1 upd, reqId from StageUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = inSrId;
  -- log
  logToDLF(varReqUUID, dlf.LVL_DEBUG, dlf.STAGER_GET, inFileId, inNsHost, 'stagerd',
           'SUBREQID=' || varSrUUID);

  -- First see whether we should wait on an ongoing request
  DECLARE
    varDcIds "numList";
  BEGIN
    SELECT /*+ INDEX (DiskCopy I_DiskCopy_CastorFile) */ DiskCopy.id BULK COLLECT INTO varDcIds
      FROM DiskCopy, FileSystem, DiskServer
     WHERE inCfId = DiskCopy.castorFile
       AND FileSystem.id(+) = DiskCopy.fileSystem
       AND nvl(FileSystem.status, 0) IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
       AND DiskServer.id(+) = FileSystem.diskServer
       AND nvl(DiskServer.status, 0) IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
       AND nvl(DiskServer.hwOnline, 1) = 1
       AND DiskCopy.status = dconst.DISKCOPY_WAITFS_SCHEDULING;
    IF varDcIds.COUNT > 0 THEN
      -- DiskCopy is in WAIT*, make SubRequest wait on previous subrequest and do not schedule
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_WAITSUBREQ,
             lastModificationTime = getTime()
      WHERE SubRequest.id = inSrId;
      logToDLF(varReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_WAITSUBREQ, inFileId, inNsHost, 'stagerd',
               'SUBREQID=' || varSrUUID || ' svcClassId=' || getSvcClassName(varSvcClassId) ||
               ' reason="ongoing write request"' || ' existingDcId=' || TO_CHAR(varDcIds(1)));
      RETURN;
    END IF;
  END;

  -- Look for available diskcopies. The status is needed for the
  -- internal replication processing, and only if count = 1, hence
  -- the min() function does not represent anything here.
  -- Note that we accept copies in READONLY hardware here as we're processing Get
  -- and Update requests, and we would deny Updates switching to write mode in that case.
  SELECT /*+ INDEX (DiskCopy I_DiskCopy_CastorFile) */
         COUNT(DiskCopy.id), min(DiskCopy.status) INTO varNbDCs, varDcStatus
    FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
   WHERE DiskCopy.castorfile = inCfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = varSvcClassId
     AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
     AND DiskServer.hwOnline = 1
     AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT);

  -- we may be the first update inside a prepareToPut. Check for this case
  IF varNbDCs = 0 AND varUpd = 1 THEN
    SELECT COUNT(DiskCopy.id) INTO varNbDCs
      FROM DiskCopy
     WHERE DiskCopy.castorfile = inCfId
       AND DiskCopy.status = dconst.DISKCOPY_WAITFS;
    IF varNbDCs = 1 THEN
      -- we are the first update, so we actually need to behave as a Put
      handlePut(inCfId, inSrId, inFileId, inNsHost);
      RETURN;
    END IF;
  END IF;

  -- first handle the case where we found diskcopies
  IF varNbDCs > 0 THEN
    -- We may still need to replicate the file (replication on demand)
    IF varUpd = 0 AND (varNbDCs > 1 OR varDcStatus != dconst.DISKCOPY_STAGEOUT) THEN
      handleReplication(inSRId, inFileId, inNsHost, inCfId, varNsOpenTime, varSvcClassId,
                        varEuid, varEgid, varReqUUID, varSrUUID);
    END IF;
    DECLARE
      varDcList VARCHAR2(2048);
    BEGIN
      -- List available diskcopies for job scheduling
      SELECT /*+ INDEX (DiskCopy I_DiskCopy_CastorFile) INDEX(Subrequest PK_Subrequest_Id)*/ 
             LISTAGG(DiskServer.name || ':' || FileSystem.mountPoint, '|')
             WITHIN GROUP (ORDER BY FileSystemRate(FileSystem.nbReadStreams, FileSystem.nbWriteStreams))
        INTO varDcList
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
       WHERE DiskCopy.castorfile = inCfId
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = varSvcClassId
         AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
         AND FileSystem.id = DiskCopy.fileSystem
         AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
         AND DiskServer.id = FileSystem.diskServer
         AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
         AND DiskServer.hwOnline = 1;
      -- mark subrequest for scheduling
      UPDATE SubRequest
         SET requestedFileSystems = varDcList,
             xsize = 0,
             status = dconst.SUBREQUEST_READYFORSCHED,
             getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED
       WHERE id = inSrId;
    END;
  ELSE
    -- No diskcopies available for this service class. We may need to recall or trigger a disk to disk copy
    DECLARE
      varD2dId NUMBER;
    BEGIN
      -- Check whether there's already a disk to disk copy going on
      SELECT id INTO varD2dId
        FROM Disk2DiskCopyJob
       WHERE destSvcClass = varSvcClassId    -- this is the destination service class
         AND castorFile = inCfId;
      -- There is an ongoing disk to disk copy, so let's wait on it
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_WAITSUBREQ
       WHERE id = inSrId;
      logToDLF(varReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_WAITSUBREQ, inFileId, inNsHost, 'stagerd',
               'SUBREQID=' || varSrUUID || ' svcClassId=' || getSvcClassName(varSvcClassId) ||
               ' reason="ongoing replication"' || ' ongoingD2dSubReqId=' || TO_CHAR(varD2dId));
    EXCEPTION WHEN NO_DATA_FOUND THEN
      DECLARE 
        varIgnored INTEGER;
      BEGIN
        -- no ongoing disk to disk copy, trigger one or go for a recall
        varIgnored := triggerD2dOrRecall(inCfId, varNsOpenTime, inSrId, inFileId, inNsHost, varEuid, varEgid,
                                         varSvcClassId, inFileSize, varReqUUID, varSrUUID);
      END;
    END;
  END IF;
END;
/


/* PL/SQL method implementing handlePrepareToGet
 * returns status of the SubRequest : FAILED, FINISHED, WAITTAPERECALL OR WAITDISKTODISKCOPY
 */
CREATE OR REPLACE FUNCTION handlePrepareToGet(inCfId IN INTEGER, inSrId IN INTEGER,
                                              inFileId IN INTEGER, inNsHost IN VARCHAR2,
                                              inFileSize IN INTEGER) RETURN INTEGER AS
  varNsOpenTime INTEGER;
  varEuid NUMBER;
  varEgid NUMBER;
  varSvcClassId NUMBER;
  varReqUUID VARCHAR(2048);
  varSrUUID VARCHAR(2048);
BEGIN
  -- lock the castorFile to be safe in case of concurrent subrequests
  SELECT nsOpenTime INTO varNsOpenTime FROM CastorFile WHERE id = inCfId FOR UPDATE;
  -- retrieve the svcClass, user and log data for this subrequest
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         Request.euid, Request.egid, Request.svcClass,
         Request.reqId, SubRequest.subreqId
    INTO varEuid, varEgid, varSvcClassId, varReqUUID, varSrUUID
    FROM (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                 id, euid, egid, svcClass, reqId from StagePrepareToGetRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                 id, euid, egid, svcClass, reqId from StagePrepareToUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = inSrId;
  -- log
  logToDLF(varReqUUID, dlf.LVL_DEBUG, dlf.STAGER_PREPARETOGET, inFileId, inNsHost, 'stagerd',
           'SUBREQID=' || varSrUUID);

  -- First look for available diskcopies. Note that we never wait on other requests.
  -- and we include Disk2DiskCopyJobs as they are going to produce available DiskCopies.
  DECLARE
    varNbDCs INTEGER;
  BEGIN
    SELECT COUNT(*) INTO varNbDCs FROM (
      SELECT /*+ INDEX (DiskCopy I_DiskCopy_CastorFile) */ DiskCopy.id
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
       WHERE DiskCopy.castorfile = inCfId
         AND DiskCopy.fileSystem = FileSystem.id
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = varSvcClassId
         AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
         AND FileSystem.diskserver = DiskServer.id
         AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
         AND DiskServer.hwOnline = 1
         AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
       UNION ALL
      SELECT id
        FROM Disk2DiskCopyJob
       WHERE destSvcclass = varSvcClassId
         AND castorfile = inCfId);
    IF varNbDCs > 0 THEN
      -- some available diskcopy was found.
      logToDLF(varReqUUID, dlf.LVL_DEBUG, dlf.STAGER_DISKCOPY_FOUND, inFileId, inNsHost, 'stagerd',
              'SUBREQID=' || varSrUUID);
      -- last thing, check whether we should recreate missing copies
      handleReplication(inSrId, inFileId, inNsHost, inCfId, varNsOpenTime, varSvcClassId,
                        varEuid, varEgid, varReqUUID, varSrUUID);
      RETURN dconst.SUBREQUEST_FINISHED;
    END IF;
  END;

  RETURN CASE triggerD2dOrRecall(inCfId, varNsOpenTime, inSrId, inFileId, inNsHost, varEuid, varEgid,
                                 varSvcClassId, inFileSize, varReqUUID, varSrUUID)
         WHEN 0 THEN dconst.SUBREQUEST_FAILED
         ELSE dconst.SUBREQUEST_WAITTAPERECALL
         END;
END;
/

/*
 * handle a prepareToPut/Upd
 * return 1 if the client needs to be replied to, else 0
 */
CREATE OR REPLACE FUNCTION handlePrepareToPut(inCfId IN INTEGER, inSrId IN INTEGER,
                                              inFileId IN INTEGER, inNsHost IN VARCHAR2) RETURN INTEGER AS
  varFileClassId INTEGER;
  varSvcClassId INTEGER;
  varEuid INTEGER;
  varEgid INTEGER;
  varReqUUID VARCHAR(2048);
  varSrUUID VARCHAR(2048);
BEGIN
  -- Get fileClass and lock access to the CastorFile
  SELECT fileclass INTO varFileClassId FROM CastorFile WHERE id = inCfId FOR UPDATE;
  -- Get serviceClass log data
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         Request.svcClass, euid, egid, Request.reqId, SubRequest.subreqId
    INTO varSvcClassId, varEuid, varEgid, varReqUUID, varSrUUID
    FROM (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                 id, svcClass, euid, egid, reqId FROM StagePrepareToPutRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                 id, svcClass, euid, egid, reqId FROM StagePrepareToUpdateRequest) Request, SubRequest
   WHERE SubRequest.id = inSrId
     AND Request.id = SubRequest.request;
  -- log
  logToDLF(varReqUUID, dlf.LVL_DEBUG, dlf.STAGER_PREPARETOPUT, inFileId, inNsHost, 'stagerd',
           'SUBREQID=' || varSrUUID);

  -- check whether there is another PrepareToPut/Update going on. There can be only one
  DECLARE
    varNbPReqs INTEGER;
  BEGIN
    -- Note that we do not select ourselves as we are in status SUBREQUEST_WAITSCHED
    SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/
           count(SubRequest.diskCopy) INTO varNbPReqs
      FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id
              FROM StagePrepareToPutRequest UNION ALL
            SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id
              FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
     WHERE SubRequest.castorFile = inCfId
       AND PrepareRequest.id = SubRequest.request
       AND SubRequest.status = dconst.SUBREQUEST_READY;
    IF varNbPReqs > 0 THEN
      -- this means that another PrepareTo request is already running. This is forbidden
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.EBUSY,
             errorMessage = 'Another prepareToPut/Update is ongoing for this file'
       WHERE id = inSrId;
      RETURN 0;
    END IF;
  END;

  -- in case of disk only pool, check if there is space in the diskpool 
  IF checkFailJobsWhenNoSpace(varSvcClassId) = 1 THEN
    -- The svcClass is declared disk only and has no space thus we cannot recreate the file
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOSPC,
           errorMessage = 'File creation canceled since disk pool is full'
     WHERE id = inSrId;
    logToDLF(varReqUUID, dlf.LVL_USER_ERROR, dlf.STAGER_RECREATION_IMPOSSIBLE, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || varSrUUID || ' reason="disk pool is full"' ||
             ' svcClassID=' || getFileClassName(varFileClassId));
    RETURN 0;
  END IF;

  -- core processing of the request
  RETURN handleRawPutOrPPut(inCfId, inSrId, inFileId, inNsHost, varFileClassId,
                             varSvcClassId, varEuid, varEgid, varReqUUID, varSrUUID, False);
END;
/
/*******************************************************************
 *
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
BEGIN
  -- Get data and lock the CastorFile
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ castorfile, diskCopy
    INTO cfId, dcId
    FROM SubRequest WHERE id = srId;
  SELECT fileclass INTO fclassId FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Check that we can indeed open the file in write mode
  -- 4 criteria have to be met :
  --   - we are not using a INVALID copy (we should not update an old version)
  --   - we are not in a disk only svcClass and the file class asks for tape copy
  --   - the filesystem/diskserver is not in READONLY state
  --   - there is no other update/put going on or there is a prepareTo and we are
  --     dealing with the same copy.
  SELECT status INTO stat FROM DiskCopy WHERE id = dcId;
  -- First the invalid case
  IF stat = dconst.DISKCOPY_INVALID THEN
    raise_application_error(-20106, 'Could not update an invalid copy of a file (file has been modified by somebody else concurrently)');
  END IF;
  -- Then the disk only check
  IF checkNoTapeRouting(fclassId) = 1 THEN
     raise_application_error(-20106, 'File update canceled since the file cannot be routed to tape');
  END IF;
  -- Then the hardware status check
  BEGIN
    SELECT 1 INTO nbres
      FROM DiskServer, FileSystem, DiskCopy
     WHERE DiskCopy.id = dcId
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
       AND FileSystem.diskServer = DiskServer.id
       AND DiskServer.status = dconst.DISKSERVER_PRODUCTION;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- hardware is not in PRODUCTION, prevent the writing
    raise_application_error(-20106, 'Could not update the file, hardware was set to read-only mode by the administrator');
  END;
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
      raise_application_error(-20106, 'Could not update a file already open for write (and no prepareToPut/Update context found)');
    END;
  ELSE
    -- If we are not having a STAGEOUT diskCopy, we are the only ones to write,
    -- so we have to setup everything
    -- invalidate all diskcopies
    UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID
     WHERE castorFile = cfId
       AND status = dconst.DISKCOPY_VALID;
    -- except the one we are dealing with that goes to STAGEOUT
    UPDATE DiskCopy
       SET status = dconst.DISKCOPY_STAGEOUT, importance = 0
     WHERE id = dcid;
    -- Suppress all Migration Jobs (avoid migration of previous version of the file)
    deleteMigrationJobs(cfId);
  END IF;
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
  varCfId INTEGER;
  srStatus INTEGER;
  srSvcClass INTEGER;
  fsId INTEGER;
  fsStatus INTEGER;
  dsStatus INTEGER;
  varHwOnline INTEGER;
  prevFsId INTEGER;
BEGIN
  -- Get diskCopy and subrequest related information
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         SubRequest.castorFile, SubRequest.diskCopy, SubRequest.status, DiskCopy.fileSystem,
         Request.svcClass
    INTO varCfId, rdcId, srStatus, prevFsId, srSvcClass
    FROM SubRequest, DiskCopy,
         (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ id, svcClass FROM StagePutRequest UNION ALL
          SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClass FROM StageGetRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClass FROM StageUpdateRequest) Request
   WHERE SubRequest.diskcopy = Diskcopy.id
     AND SubRequest.id = srId
     AND SubRequest.request = Request.id;
  -- Check that we did not cancel the SubRequest in the mean time
  IF srStatus IN (dconst.SUBREQUEST_FAILED, dconst.SUBREQUEST_FAILED_FINISHED) THEN
    raise_application_error(-20104, 'SubRequest canceled while queuing in scheduler. Giving up.');
  END IF;
  -- Get selected filesystem and its status
  SELECT FileSystem.id, FileSystem.status, DiskServer.status, DiskServer.hwOnline
    INTO fsId, fsStatus, dsStatus, varHwOnline
    FROM DiskServer, FileSystem
   WHERE FileSystem.diskserver = DiskServer.id
     AND DiskServer.name = selectedDiskServer
     AND FileSystem.mountPoint = selectedMountPoint;
  -- Check that a job has not already started for this diskcopy. Refer to
  -- bug #14358
  IF prevFsId > 0 AND prevFsId <> fsId THEN
    raise_application_error(-20104, 'This job has already started for this DiskCopy. Giving up.');
  END IF;
  IF fsStatus != dconst.FILESYSTEM_PRODUCTION OR dsStatus != dconst.DISKSERVER_PRODUCTION OR varHwOnline = 0 THEN
    raise_application_error(-20104, 'The selected diskserver/filesystem is not in PRODUCTION any longer. Giving up.');
  END IF;
  -- In case the DiskCopy was in WAITFS_SCHEDULING,
  -- restart the waiting SubRequests
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_RESTART, lastModificationTime = getTime()
   WHERE status = dconst.SUBREQUEST_WAITSUBREQ
     AND castorFile = varCfId;
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
  fsStatus INTEGER;
  dsStatus INTEGER;
  varHwOnline INTEGER;
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
  -- Get selected filesystem and its status
  SELECT FileSystem.id, FileSystem.status, DiskServer.status, DiskServer.hwOnline INTO fsId, fsStatus, dsStatus, varHwOnline
    FROM DiskServer, FileSystem
   WHERE FileSystem.diskserver = DiskServer.id
     AND DiskServer.name = selectedDiskServer
     AND FileSystem.mountPoint = selectedMountPoint;
  -- Now check that the hardware status is still valid for a Get request
  IF NOT (fsStatus IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY) AND
          dsStatus IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY) AND
          varHwOnline = 1) THEN
    raise_application_error(-20114, 'The selected diskserver/filesystem is not in PRODUCTION or READONLY any longer. Giving up.');
  END IF;
  -- Try to find local DiskCopy
  SELECT /*+ INDEX(DiskCopy I_DiskCopy_Castorfile) */ id, nbCopyAccesses, gcWeight, creationTime
    INTO dcId, nbac, gcw, cTime
    FROM DiskCopy
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.filesystem = fsId
     AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
     AND ROWNUM < 2;
  -- We found it, so we are settled and we'll use the local copy.
  -- For the ROWNUM < 2 condition: it might happen that we have more than one, because
  -- the scheduling may have scheduled a replication on a fileSystem which already had a previous diskcopy.
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
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_FAILED, errorCode = 1725, errorMessage='Request canceled while queuing'
   WHERE id = srId;
  COMMIT;
  raise_application_error(-20114, 'File invalidated while queuing in the scheduler, please try again');
END;
/

/* update a drainingJob at then end of a disk2diskcopy */
CREATE OR REPLACE PROCEDURE updateDrainingJobOnD2dEnd(inDjId IN INTEGER, inFileSize IN INTEGER,
                                                      inHasFailed IN BOOLEAN) AS
  varTotalFiles INTEGER;
  varNbFailedBytes INTEGER;
  varNbSuccessBytes INTEGER;
  varNbFailedFiles INTEGER;
  varNbSuccessFiles INTEGER;
  varStatus INTEGER;
BEGIN
  -- note the locking that insures consistency of the counters
  SELECT status, totalFiles, nbFailedBytes, nbSuccessBytes, nbFailedFiles, nbSuccessFiles
    INTO varStatus, varTotalFiles, varNbFailedBytes, varNbSuccessBytes, varNbFailedFiles, varNbSuccessFiles
    FROM DrainingJob
   WHERE id = inDjId
     FOR UPDATE;
  -- update counters
  IF inHasFailed THEN
    -- case of failures
    varNbFailedBytes := varNbFailedBytes + inFileSize;
    varNbFailedFiles := varNbFailedFiles + 1;
  ELSE
    -- case of success
    varNbSuccessBytes := varNbSuccessBytes + inFileSize;
    varNbSuccessFiles := varNbSuccessFiles + 1;
  END IF;
  -- detect end of draining. Do not touch INTERRUPTED status
  IF varStatus = dconst.DRAININGJOB_RUNNING AND
     varNbSuccessFiles + varNbFailedFiles = varTotalFiles THEN
    IF varNbFailedFiles = 0 THEN
      varStatus := dconst.DRAININGJOB_FINISHED;
    ELSE
      varStatus := dconst.DRAININGJOB_FAILED;
    END IF;
  END IF;
  -- update DrainingJob
  UPDATE DrainingJob
     SET status = varStatus,
         totalFiles = varTotalFiles,
         nbFailedBytes = varNbFailedBytes,
         nbSuccessBytes = varNbSuccessBytes,
         nbFailedFiles = varNbFailedFiles,
         nbSuccessFiles = varNbSuccessFiles
   WHERE id = inDjId;
END;
/

/* PL/SQL method implementing disk2DiskCopyEnded
 * Note that inDestDsName, inDestPath and inReplicaFileSize are not used when inErrorMessage is not NULL
 */
CREATE OR REPLACE PROCEDURE disk2DiskCopyEnded
(inTransferId IN VARCHAR2, inDestDsName IN VARCHAR2, inDestPath IN VARCHAR2,
 inReplicaFileSize IN INTEGER, inErrorMessage IN VARCHAR2) AS
  varCfId INTEGER;
  varUid INTEGER := -1;
  varGid INTEGER := -1;
  varDestDcId INTEGER;
  varDestSvcClass INTEGER;
  varRepType INTEGER;
  varReplacedDcId INTEGER;
  varRetryCounter INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
  varFileSize INTEGER;
  varDestPath VARCHAR2(2048);
  varDestFsId INTEGER;
  varDcGcWeight NUMBER := 0;
  varDcImportance NUMBER := 0;
  varNewDcStatus INTEGER := dconst.DISKCOPY_VALID;
  varLogMsg VARCHAR2(2048);
  varComment VARCHAR2(2048);
  varDrainingJob VARCHAR2(2048);
BEGIN
  varLogMsg := CASE WHEN inErrorMessage IS NULL THEN dlf.D2D_D2DDONE_OK ELSE dlf.D2D_D2DFAILED END;
  BEGIN
    -- Get data from the disk2DiskCopy Job
    SELECT castorFile, ouid, ogid, destDcId, destSvcClass, replicationType,
           replacedDcId, retryCounter, drainingJob
      INTO varCfId, varUid, varGid, varDestDcId, varDestSvcClass, varRepType,
           varReplacedDcId, varRetryCounter, varDrainingJob
      FROM Disk2DiskCopyjob
     WHERE transferId = inTransferId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- disk2diskCopyJob not found. It was probably canceled.
    -- So our brand new copy has to be created as invalid to trigger GC.
    varNewDcStatus := dconst.DISKCOPY_INVALID;
    varLogMsg := dlf.D2D_D2DDONE_CANCEL;
  END;
  -- lock the castor file (and get logging info)
  SELECT fileid, nsHost, fileSize INTO varFileId, varNsHost, varFileSize
    FROM CastorFile
   WHERE id = varCfId
     FOR UPDATE;
  -- check the filesize
  IF inReplicaFileSize != varFileSize THEN
    -- replication went wrong !
    IF varLogMsg = dlf.D2D_D2DDONE_OK THEN
      varLogMsg := dlf.D2D_D2DDONE_BADSIZE;
      varNewDcStatus := dconst.DISKCOPY_INVALID;
    END IF;
  END IF;
  -- Log success or failure of the replication
  varComment := 'transferId=' || inTransferId ||
         ' destSvcClass=' || getSvcClassName(varDestSvcClass) ||
         ' dstDcId=' || TO_CHAR(varDestDcId) || ' destPath=' || inDestDsName || ':' || inDestPath ||
         ' euid=' || TO_CHAR(varUid) || ' egid=' || TO_CHAR(varGid) || 
         ' fileSize=' || TO_CHAR(varFileSize);
  IF inErrorMessage IS NOT NULL THEN
    varComment := varComment || ' replicaFileSize=' || TO_CHAR(inReplicaFileSize) ||
                  ' errorMessage=' || inErrorMessage;
  END IF;
  logToDLF(NULL, dlf.LVL_SYSTEM, varLogMsg, varFileId, varNsHost, 'stagerd', varComment);
  -- if success, create new DiskCopy, restart waiting requests, cleanup and handle replicate on close
  IF inErrorMessage IS NULL THEN
    -- get filesystem of the diskcopy and parse diskcopy path
    SELECT FileSystem.id, SUBSTR(inDestPath, LENGTH(FileSystem.mountPoint)+1)
      INTO varDestFsId, varDestPath
      FROM DiskServer, FileSystem
     WHERE DiskServer.name = inDestDsName
       AND FileSystem.diskServer = DiskServer.id
       AND INSTR(inDestPath, FileSystem.mountPoint) = 1;
    -- compute GcWeight and importance of the new copy
    IF varNewDcStatus = dconst.DISKCOPY_VALID THEN
      DECLARE
        varGcwProc VARCHAR2(2048);
      BEGIN
        varGcwProc := castorGC.getCopyWeight(varFileSize);
        EXECUTE IMMEDIATE
          'BEGIN :newGcw := ' || varGcwProc || '(:size); END;'
          USING OUT varDcGcWeight, IN varFileSize;
        SELECT /*+ INDEX (DiskCopy I_DiskCopy_CastorFile) */
               COUNT(*)+1 INTO varDCImportance FROM DiskCopy
         WHERE castorFile=varCfId AND status = dconst.DISKCOPY_VALID;
      END;
    END IF;
    -- create the new DiskCopy
    INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                          owneruid, ownergid, id, gcType, fileSystem, castorFile, status, importance)
    VALUES (varDestPath, varDcGcWeight, getTime(), getTime(), varFileSize, 0, varUid, varGid, varDestDcId,
            CASE varNewDcStatus WHEN dconst.DISKCOPY_INVALID THEN dconst.GCTYPE_OVERWRITTEN ELSE NULL END,
            varDestFsId, varCfId, varNewDcStatus, varDCImportance);
    -- Wake up waiting subrequests
    UPDATE SubRequest
       SET status = dconst.SUBREQUEST_RESTART,
           getNextStatus = CASE WHEN inErrorMessage IS NULL THEN dconst.GETNEXTSTATUS_FILESTAGED ELSE getNextStatus END,
           lastModificationTime = getTime()
     WHERE status = dconst.SUBREQUEST_WAITSUBREQ
       AND castorfile = varCfId;
    -- delete the disk2diskCopyJob
    DELETE FROM Disk2DiskCopyjob WHERE transferId = inTransferId;
    -- In case of valid new copy
    IF varNewDcStatus = dconst.DISKCOPY_VALID THEN
      -- update importance of other DiskCopies if it's an additional one
      IF varReplacedDcId IS NOT NULL THEN
        UPDATE DiskCopy SET importance = varDCImportance WHERE castorFile=varCfId;
      END IF;
      -- drop source if requested
      UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID WHERE id = varReplacedDcId;
      -- Trigger the creation of additional copies of the file, if any
      replicateOnClose(varCfId, varUid, varGid);
    END IF;
    -- In case of draining, update DrainingJob
    IF varDrainingJob IS NOT NULL THEN
      updateDrainingJobOnD2dEnd(varDrainingJob, varFileSize, False);
    END IF;
  ELSE
    DECLARE
      varMaxNbD2dRetries INTEGER := TO_NUMBER(getConfigOption('D2dCopy', 'MaxNbRetries', 2));
    BEGIN
      -- shall we try again ?
      IF varRetryCounter < varMaxNbD2dRetries THEN
        -- yes, so let's restart the Disk2DiskCopyJob
        UPDATE Disk2DiskCopyJob
           SET status = dconst.DISK2DISKCOPYJOB_PENDING,
               retryCounter = varRetryCounter + 1
         WHERE transferId = inTransferId;
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_D2DDONE_RETRIED, varFileId, varNsHost, 'stagerd', varComment ||
                 ' RetryNb=' || TO_CHAR(varRetryCounter+1) || ' maxNbRetries=' || TO_CHAR(varMaxNbD2dRetries));
      ELSE
        -- no more retries, let's delete the disk to disk job copy
        BEGIN
          DELETE FROM Disk2DiskCopyjob WHERE transferId = inTransferId;
          -- and remember the error in case of draining
          IF varDrainingJob IS NOT NULL THEN
            INSERT INTO DrainingErrors (drainingJob, errorMsg, fileId, nsHost)
            VALUES (varDrainingJob, inErrorMessage, varFileId, varNsHost);
          END IF;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- the Disk2DiskCopyjob was already dropped (e.g. because of an interrupted draining)
          -- in such a case, forget about the error
          NULL;
        END;
        logToDLF(NULL, dlf.LVL_NOTICE, dlf.D2D_D2DDONE_NORETRY, varFileId, varNsHost, 'stagerd', varComment ||
                 ' maxNbRetries=' || TO_CHAR(varMaxNbD2dRetries));
        -- Fail waiting subrequests
        UPDATE SubRequest
           SET status = dconst.SUBREQUEST_FAILED,
               lastModificationTime = getTime(),
               errorCode = serrno.SEINTERNAL,
               errorMessage = 'Disk to disk copy failed after ' || TO_CHAR(varMaxNbD2dRetries) ||
                              'retries. Last error was : ' || inErrorMessage
         WHERE status = dconst.SUBREQUEST_WAITSUBREQ
           AND castorfile = varCfId;
        -- In case of draining, update DrainingJob
        IF varDrainingJob IS NOT NULL THEN
          updateDrainingJobOnD2dEnd(varDrainingJob, varFileSize, True);
        END IF;
      END IF;
    END;
  END IF;
END;
/

/* PL/SQL method implementing disk2DiskCopyStart
 * Note that cfId is only needed for proper logging in case the replication has been canceled.
 */
CREATE OR REPLACE PROCEDURE disk2DiskCopyStart
 (inTransferId IN VARCHAR2, inFileId IN INTEGER, inNsHost IN VARCHAR2,
  inDestDiskServerName IN VARCHAR2, inDestMountPoint IN VARCHAR2,
  inSrcDiskServerName IN VARCHAR2, inSrcMountPoint IN VARCHAR2,
  outDestDcPath OUT VARCHAR2, outSrcDcPath OUT VARCHAR2) AS
  varCfId INTEGER;
  varDestDcId INTEGER;
  varDestDsId INTEGER;
  varSrcDcId INTEGER;
  varSrcFsId INTEGER;
  varNbCopies INTEGER;
  varSrcFsStatus INTEGER;
  varSrcDsStatus INTEGER;
  varSrcHwOnline INTEGER;
  varDestFsStatus INTEGER;
  varDestDsStatus INTEGER;
  varDestHwOnline INTEGER;
BEGIN
  -- check the Disk2DiskCopyJob status and check that it was not canceled in the mean time
  BEGIN
    SELECT castorFile, destDcId INTO varCfId, varDestDcId
      FROM Disk2DiskCopyJob
     WHERE transferId = inTransferId
       AND status = dconst.DISK2DISKCOPYJOB_SCHEDULED;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "disk2DiskCopyStart : Replication request canceled while queuing in scheduler or transfer already started"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.D2D_CANCELED_AT_START, inFileId, inNsHost, 'stagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' destDiskServer=' || inDestDiskServerName ||
             ' destMountPoint=' || inDestMountPoint || ' srcDiskServer=' || inSrcDiskServerName ||
             ' srcMountPoint=' || inSrcMountPoint);
    -- raise exception
    raise_application_error(-20110, dlf.D2D_CANCELED_AT_START || '');
  END;

  -- identify the source DiskCopy and diskserver/filesystem and check that it is still valid
  BEGIN
    SELECT /*+ INDEX(DiskCopy I_DiskCopy_CastorFile) */
           FileSystem.id, DiskCopy.id, FileSystem.status, DiskServer.status, DiskServer.hwOnline
      INTO varSrcFsId, varSrcDcId, varSrcFsStatus, varSrcDsStatus, varSrcHwOnline
      FROM DiskServer, FileSystem, DiskCopy
     WHERE DiskServer.name = inSrcDiskServerName
       AND DiskServer.id = FileSystem.diskServer
       AND FileSystem.mountPoint = inSrcMountPoint
       AND DiskCopy.FileSystem = FileSystem.id
       AND DiskCopy.status = dconst.DISKCOPY_VALID
       AND DiskCopy.castorFile = varCfId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "disk2DiskCopyStart : Source has disappeared while queuing in scheduler, retrying"
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_SOURCE_GONE, inFileId, inNsHost, 'stagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' destDiskServer=' || inDestDiskServerName ||
             ' destMountPoint=' || inDestMountPoint || ' srcDiskServer=' || inSrcDiskServerName ||
             ' srcMountPoint=' || inSrcMountPoint);
    -- end the disktodisk copy (may be retried)
    disk2DiskCopyEnded(inTransferId, '', '', 0, dlf.D2D_SOURCE_GONE);
    COMMIT; -- commit or raise_application_error will roll back for us :-(
    -- raise exception for the scheduling part
    raise_application_error(-20110, dlf.D2D_SOURCE_GONE);
  END;
  IF (varSrcDsStatus = dconst.DISKSERVER_DISABLED OR varSrcFsStatus = dconst.FILESYSTEM_DISABLED
      OR varSrcHwOnline = 0) THEN
    -- log "disk2DiskCopyStart : Source diskserver/filesystem was DISABLED meanwhile"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.D2D_SRC_DISABLED, inFileId, inNsHost, 'stagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' diskServer=' || inSrcDiskServerName ||
             ' fileSystem=' || inSrcMountPoint);
    -- fail d2d transfer
    disk2DiskCopyEnded(inTransferId, '', '', 0, 'Source was disabled');
    COMMIT; -- commit or raise_application_error will roll back for us :-(
    -- raise exception
    raise_application_error(-20110, dlf.D2D_SRC_DISABLED);
  END IF;

  -- get destination diskServer/filesystem and check its status
  SELECT DiskServer.id, DiskServer.status, FileSystem.status, DiskServer.hwOnline
    INTO varDestDsId, varDestDsStatus, varDestFsStatus, varDestHwOnline
    FROM DiskServer, FileSystem
   WHERE DiskServer.name = inDestDiskServerName
     AND FileSystem.mountPoint = inDestMountPoint
     AND FileSystem.diskServer = DiskServer.id;
  IF (varDestDsStatus != dconst.DISKSERVER_PRODUCTION OR varDestFsStatus != dconst.FILESYSTEM_PRODUCTION
      OR varDestHwOnline = 0) THEN
    -- log "disk2DiskCopyStart : Destination diskserver/filesystem not in PRODUCTION any longer"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.D2D_DEST_NOT_PRODUCTION, inFileId, inNsHost, 'stagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' diskServer=' || inDestDiskServerName);
    -- fail d2d transfer
    disk2DiskCopyEnded(inTransferId, '', '', 0, 'Destination not in production');
    COMMIT; -- commit or raise_application_error will roll back for us :-(
    -- raise exception
    raise_application_error(-20110, dlf.D2D_DEST_NOT_PRODUCTION);
  END IF;

  -- Prevent multiple copies of the file to be created on the same diskserver
  SELECT /*+ INDEX(DiskCopy I_DiskCopy_Castorfile) */ count(*) INTO varNbCopies
    FROM DiskCopy, FileSystem
   WHERE DiskCopy.filesystem = FileSystem.id
     AND FileSystem.diskserver = varDestDsId
     AND DiskCopy.castorfile = varCfId
     AND DiskCopy.status = dconst.DISKCOPY_VALID;
  IF varNbCopies > 0 THEN
    -- log "disk2DiskCopyStart : Multiple copies of this file already found on this diskserver"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.D2D_MULTIPLE_COPIES_ON_DS, inFileId, inNsHost, 'stagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' diskServer=' || inDestDiskServerName);
    -- fail d2d transfer
    disk2DiskCopyEnded(inTransferId, '', '', 0, 'Copy found on diskserver');
    COMMIT; -- commit or raise_application_error will roll back for us :-(
    -- raise exception
    raise_application_error(-20110, dlf.D2D_MULTIPLE_COPIES_ON_DS);
  END IF;

  -- update the Disk2DiskCopyJob status and filesystem
  UPDATE Disk2DiskCopyJob
     SET status = dconst.DISK2DISKCOPYJOB_RUNNING
   WHERE transferId = inTransferId;

  -- build full path of destination copy
  buildPathFromFileId(inFileId, inNsHost, varDestDcId, outDestDcPath);
  outDestDcPath := inDestMountPoint || outDestDcPath;

  -- build full path of source copy
  buildPathFromFileId(inFileId, inNsHost, varSrcDcId, outSrcDcPath);
  outSrcDcPath := inSrcDiskServerName || ':' || inSrcMountPoint || outSrcDcPath;

  -- log "disk2DiskCopyStart called and returned successfully"
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_START_OK, inFileId, inNsHost, 'stagerd',
           'TransferId=' || TO_CHAR(inTransferId) || ' srcPath=' || outSrcDcPath ||
           ' destPath=' || outDestDcPath);
END;
/

/* PL/SQL method implementing prepareForMigration
   returnCode can take 2 values :
    - 0 : Nothing special
    - 1 : The file got deleted while it was being written to
*/
CREATE OR REPLACE PROCEDURE prepareForMigration (srId IN INTEGER,
                                                 inFileSize IN INTEGER,
                                                 inNewTimeStamp IN NUMBER,
                                                 outFileId OUT NUMBER,
                                                 outNsHost OUT VARCHAR2,
                                                 outRC OUT INTEGER,
                                                 outNsOpenTime OUT NUMBER) AS
  cfId INTEGER;
  dcId INTEGER;
  svcId INTEGER;
  realFileSize INTEGER;
  unused INTEGER;
  contextPIPP INTEGER;
  lastUpdTime NUMBER;
BEGIN
  outRC := 0;
  -- Get CastorFile
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ castorFile, diskCopy INTO cfId, dcId
    FROM SubRequest WHERE id = srId;
  -- Lock the CastorFile and get the fileid and name server host
  SELECT id, fileid, nsHost, nvl(lastUpdateTime, 0), nsOpenTime
    INTO cfId, outFileId, outNsHost, lastUpdTime, outNsOpenTime
    FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Determine the context (Put inside PrepareToPut or not)
  BEGIN
    -- Check that there is a PrepareToPut/Update going on. There can be only a
    -- single one or none. If there was a PrepareTo, any subsequent PPut would
    -- be rejected and any subsequent PUpdate would be directly archived (cf.
    -- processPrepareRequest).
    SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile) */ SubRequest.id INTO unused
      FROM SubRequest,
       (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id FROM StagePrepareToPutRequest UNION ALL
        SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id FROM StagePrepareToUpdateRequest) Request
     WHERE SubRequest.CastorFile = cfId
       AND Request.id = SubRequest.request
       AND SubRequest.status = dconst.SUBREQUEST_READY;
    -- If we got here, we are a Put inside a PrepareToPut
    contextPIPP := 0;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Here we are a standalone Put
    contextPIPP := 1;
  END;
  -- Check whether the diskCopy is still in STAGEOUT. If not, the file
  -- was deleted via stageRm while being written to. Thus, we should just give up
  BEGIN
    SELECT status INTO unused
      FROM DiskCopy WHERE id = dcId AND status = dconst.DISKCOPY_STAGEOUT;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- So we are in the case, we give up
    outRC := 1;
    ROLLBACK;
    RETURN;
  END;
  -- Check if the timestamps allow us to update
  IF inNewTimeStamp >= lastUpdTime THEN
    -- Now we can safely update CastorFile's file size and time stamps
    UPDATE CastorFile SET fileSize = inFileSize, lastUpdateTime = inNewTimeStamp
     WHERE id = cfId;
  END IF;
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
       SET status = dconst.SUBREQUEST_RESTART
     WHERE reqType = 39  -- PutDone
       AND castorFile = cfId
       AND status = dconst.SUBREQUEST_WAITSUBREQ;
    -- and wake up the stager for processing it
    DBMS_ALERT.SIGNAL('wakeUpStageReqSvc', '');
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
  varCfId INTEGER;
BEGIN
  -- Fail the subrequest. The stager will try and answer the client
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET status = dconst.SUBREQUEST_FAILED,
         errorCode = errno,
         errorMessage = errmsg
   WHERE id = srId
  RETURNING castorFile INTO varCfId;
  -- Wake up other waiting subrequests
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_RESTART,
         lastModificationTime = getTime()
   WHERE castorFile = varCfId
     AND status = dconst.SUBREQUEST_WAITSUBREQ;
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
     SET status = dconst.SUBREQUEST_FAILED,
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
    UPDATE SubRequest
       SET status = dconst.SUBREQUEST_RESTART
     WHERE castorFile = cfId
       AND reqType = 39  -- PutDone
       AND SubRequest.status = dconst.SUBREQUEST_WAITSUBREQ;
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
   that are started/pending for more than 5mn */
CREATE OR REPLACE PROCEDURE getSchedulerTransfers
  (transfers OUT castor.UUIDPairRecord_Cur) AS
BEGIN
  OPEN transfers FOR
    SELECT SR.subReqId, Request.reqid
      FROM SubRequest SR,
        -- Union of all requests that could result in scheduler transfers
        (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                id, svcClass, reqid, 40  AS reqType FROM StagePutRequest             UNION ALL
         SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                id, svcClass, reqid, 35  AS reqType FROM StageGetRequest             UNION ALL
         SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                id, svcClass, reqid, 44  AS reqType FROM StageUpdateRequest) Request
     WHERE SR.status = 6  -- READY
       AND SR.request = Request.id
       AND SR.lastModificationTime < getTime() - 300
     UNION ALL
       SELECT transferId, '' FROM Disk2DiskCopyJob
        WHERE status IN (dconst.DISK2DISKCOPYJOB_SCHEDULED, dconst.DISK2DISKCOPYJOB_RUNNING)
          AND creationTime < getTime() - 300;
END;
/

/* PL/SQL method implementing getSchedulerD2dTransfers.
   This method lists all running D2d transfers */
CREATE OR REPLACE PROCEDURE getSchedulerD2dTransfers
  (transfers OUT castor.UUIDRecord_Cur) AS
BEGIN
  OPEN transfers FOR
    SELECT transferId
      FROM Disk2DiskCopyJob
     WHERE status IN (dconst.DISK2DISKCOPYJOB_SCHEDULED, dconst.DISK2DISKCOPYJOB_RUNNING);
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
    BEGIN
      SELECT /*+ INDEX(Subrequest I_Subrequest_SubreqId)*/ fileid, nsHost INTO fid, nh
        FROM Castorfile, SubRequest
       WHERE SubRequest.subreqId = subReqIds(i)
         AND SubRequest.castorFile = CastorFile.id;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- must be disk to disk copies
      SELECT fileid, nsHost INTO fid, nh
        FROM Castorfile, Disk2DiskCopyJob
       WHERE Disk2DiskCopyJob.transferid = subReqIds(i)
         AND Disk2DiskCopyJob.castorFile = CastorFile.id;
    END;
    INSERT INTO GetFileIdsForSrsHelper (rowno, fileId, nsHost) VALUES (i, fid, nh);
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
                             errmsgs IN castor."strList") AS
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
         AND status = dconst.SUBREQUEST_READY;
      -- Lock the CastorFile.
      SELECT id INTO cfId FROM CastorFile
       WHERE id = cfId FOR UPDATE;
      -- Confirm SubRequest status hasn't changed after acquisition of lock
      SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ id INTO srId FROM SubRequest
       WHERE id = srId AND status = dconst.SUBREQUEST_READY;
      -- Call the relevant cleanup procedure for the transfer, procedures that
      -- would have been called if the transfer failed on the remote execution host.
      IF rType = 40 THEN      -- StagePutRequest
       putFailedProcExt(srId, errnos(i), errmsgs(i));
      ELSE                    -- StageGetRequest or StageUpdateRequest
       getUpdateFailedProcExt(srId, errnos(i), errmsgs(i));
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- try disk2diskCopyJob
        SELECT id into srId FROM Disk2diskCopyJob WHERE transferId = subReqIds(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        CONTINUE;  -- The SubRequest/disk2DiskCopyJob may have be removed, nothing to be done.
      END;
      -- found it, call disk2DiskCopyEnded
      disk2DiskCopyEnded(subReqIds(i), '', '', 0, errmsgs(i));
    END;
    -- Release locks
    COMMIT;
  END LOOP;
END;
/

/* PL/SQL method implementing transferFailedLockedFile, providing bulk termination of file
 * transfers. in case the castorfile is already locked
 */
CREATE OR REPLACE
PROCEDURE transferFailedLockedFile(subReqId IN castor."strList",
                                   errnos IN castor."cnumList",
                                   errmsgs IN castor."strList")
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
         AND status = dconst.SUBREQUEST_READY;
      -- Call the relevant cleanup procedure for the transfer, procedures that
      -- would have been called if the transfer failed on the remote execution host.
      IF rType = 40 THEN      -- StagePutRequest
        putFailedProcExt(srId, errnos(i), errmsgs(i));
      ELSE                    -- StageGetRequest or StageUpdateRequest
        getUpdateFailedProcExt(srId, errnos(i), errmsgs(i));
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- try disk2diskCopyJob
        SELECT id into srId FROM Disk2diskCopyJob WHERE transferId = subReqId(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        CONTINUE;  -- The SubRequest/disk2DiskCopyJob may have be removed, nothing to be done.
      END;
      -- found it, call disk2DiskCopyEnded
      disk2DiskCopyEnded(subReqId(i), '', '', 0, errmsgs(i));
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

CREATE OR REPLACE TRIGGER tr_SubRequest_informError AFTER UPDATE OF status ON SubRequest
FOR EACH ROW WHEN (new.status = 7) -- SUBREQUEST_FAILED
BEGIN
  DBMS_ALERT.SIGNAL('wakeUpErrorSvc', '');
END;
/

CREATE OR REPLACE TRIGGER tr_SubRequest_informRestart AFTER UPDATE OF status ON SubRequest
FOR EACH ROW WHEN (new.status = 1 OR -- SUBREQUEST_RESTART
                   new.status = 2 OR -- SUBREQUEST_RETRY
                   new.status = 0)   -- SUBREQUEST_START
BEGIN
  DBMS_ALERT.SIGNAL('wakeUp'||:new.svcHandler, '');
END;
/

CREATE OR REPLACE FUNCTION selectRandomDestinationFs(inSvcClassId IN INTEGER,
                                                     inMinFreeSpace IN INTEGER,
                                                     inCfId IN INTEGER)
RETURN VARCHAR2 AS
  varResult VARCHAR2(2048) := '';
BEGIN
  -- note that we discard READONLY hardware and filter only the PRODUCTION one.
  FOR line IN
    (SELECT candidate FROM
       (SELECT UNIQUE FIRST_VALUE (DiskServer.name || ':' || FileSystem.mountPoint)
                 OVER (PARTITION BY DiskServer.id ORDER BY DBMS_Random.value) AS candidate
          FROM DiskServer, FileSystem, DiskPool2SvcClass
         WHERE FileSystem.diskServer = DiskServer.id
           AND FileSystem.diskPool = DiskPool2SvcClass.parent
           AND DiskPool2SvcClass.child = inSvcClassId
           AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
           AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
           AND DiskServer.hwOnline = 1
           AND FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > inMinFreeSpace
           AND DiskServer.id NOT IN
               (SELECT /*+ INDEX(DiskCopy I_DiskCopy_Castorfile) */ diskserver FROM DiskCopy, FileSystem
                 WHERE DiskCopy.castorFile = inCfId
                   AND DiskCopy.status = dconst.DISKCOPY_VALID
                   AND FileSystem.id = DiskCopy.fileSystem)
         ORDER BY DBMS_Random.value)
      WHERE ROWNUM <= 3) LOOP
    IF LENGTH(varResult) IS NOT NULL THEN varResult := varResult || '|'; END IF;
    varResult := varResult || line.candidate;
  END LOOP;
  RETURN varResult;
END;
/

CREATE OR REPLACE FUNCTION selectAllSourceFs(inCfId IN INTEGER)
RETURN VARCHAR2 AS
  varResult VARCHAR2(2048) := '';
BEGIN
  -- in this case we take any non DISABLED hardware
  FOR line IN
    (SELECT candidate FROM
       (SELECT /*+ INDEX(DiskCopy I_DiskCopy_Castorfile) */ 
               UNIQUE FIRST_VALUE (DiskServer.name || ':' || FileSystem.mountPoint)
                 OVER (PARTITION BY DiskServer.id ORDER BY DBMS_Random.value) AS candidate
          FROM DiskServer, FileSystem, DiskCopy
         WHERE DiskCopy.castorFile = inCfId
           AND DiskCopy.status = dconst.DISKCOPY_VALID
           AND DiskCopy.fileSystem = FileSystem.id
           AND FileSystem.diskServer = DiskServer.id
           AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
           AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
           AND DiskServer.hwOnline = 1
         ORDER BY DBMS_Random.value)
      WHERE ROWNUM <= 3) LOOP
    IF LENGTH(varResult) IS NOT NULL THEN varResult := varResult || '|'; END IF;
    varResult := varResult || line.candidate;
  END LOOP;
  RETURN varResult;
END;
/

/* PL/SQL method implementing userTransferToSchedule */
CREATE OR REPLACE
PROCEDURE userTransferToSchedule(srId OUT INTEGER,              srSubReqId OUT VARCHAR2,
                                 srProtocol OUT VARCHAR2,       srRfs OUT VARCHAR2,
                                 reqId OUT VARCHAR2,            cfFileId OUT INTEGER,
                                 cfNsHost OUT VARCHAR2,         reqSvcClass OUT VARCHAR2,
                                 reqType OUT INTEGER,           reqEuid OUT INTEGER,
                                 reqEgid OUT INTEGER,           srOpenFlags OUT VARCHAR2,
                                 clientIp OUT INTEGER,          clientPort OUT INTEGER,
                                 clientSecure OUT INTEGER,      reqCreationTime OUT INTEGER) AS
  cfId NUMBER;
  -- Cursor to select the next candidate for submission to the scheduler ordered
  -- by creation time.
  -- Note that the where clause is not strictly needed, but this way Oracle is forced
  -- to use an INDEX RANGE SCAN instead of its preferred (and unstable upon load) FULL SCAN!
  CURSOR c IS
    SELECT /*+ FIRST_ROWS_10 INDEX(SR I_SubRequest_RT_CT_ID) */ SR.id
      FROM SubRequest PARTITION (P_STATUS_13_14) SR  -- READYFORSCHED
     WHERE svcHandler = 'JobReqSvc'
     ORDER BY SR.creationTime ASC;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  varSrId NUMBER;
  svcClassId NUMBER;
  unusedMessage VARCHAR2(2048);
  unusedStatus INTEGER;
  varXsize INTEGER;
BEGIN
  -- Open a cursor on potential candidates
  OPEN c;
  -- Retrieve the first candidate
  FETCH c INTO varSrId;
  IF c%NOTFOUND THEN
    -- There is no candidate available. Wait for next alert concerning something
    -- to schedule for a maximum of 3 seconds.
    -- We do not wait forever in order to to give the control back to the
    -- caller daemon in case it should exit.
    CLOSE c;
    DBMS_ALERT.WAITONE('transferReadyToSchedule', unusedMessage, unusedStatus, 3);
    -- try again to find something now that we waited
    OPEN c;
    FETCH c INTO varSrId;
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
      -- valid subrequest is in status READYFORSCHED
      SELECT /*+ INDEX(SR PK_SubRequest_ID) */ id INTO varSrId
        FROM SubRequest PARTITION (P_STATUS_13_14) SR
       WHERE id = varSrId
         AND status = dconst.SUBREQUEST_READYFORSCHED
         FOR UPDATE NOWAIT;
      -- We have successfully acquired the lock, so we update the subrequest
      -- status and modification time
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_READY,
             lastModificationTime = getTime()
       WHERE id = varSrId
      RETURNING id, subReqId, protocol, xsize, requestedFileSystems
        INTO srId, srSubReqId, srProtocol, varXsize, srRfs;
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
    FETCH c INTO varSrId;
    IF c%NOTFOUND THEN
      -- no next one ? then we can return
      RETURN;
    END IF;
  END LOOP;
  CLOSE c;

  BEGIN
    -- We finally got a valid candidate, let's process it
    -- Extract the rest of the information required by transfer manager
    SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
           CastorFile.id, CastorFile.fileId, CastorFile.nsHost, SvcClass.name, SvcClass.id,
           Request.type, Request.reqId, Request.euid, Request.egid,
           Request.direction, Client.ipAddress, Client.port,
           Client.secure, Request.creationTime
      INTO cfId, cfFileId, cfNsHost, reqSvcClass, svcClassId, reqType, reqId, reqEuid, reqEgid,
           srOpenFlags, clientIp, clientPort, clientSecure, reqCreationTime
      FROM SubRequest, CastorFile, SvcClass, Client,
           (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                   id, euid, egid, reqid, client, creationTime,
                   'w' direction, svcClass, 40 type
              FROM StagePutRequest
             UNION ALL
            SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                   id, euid, egid, reqid, client, creationTime,
                   'r' direction, svcClass, 35 type
              FROM StageGetRequest
             UNION ALL
            SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                   id, euid, egid, reqid, client, creationTime,
                   'o' direction, svcClass, 44 type
              FROM StageUpdateRequest) Request
     WHERE SubRequest.id = srId
       AND SubRequest.castorFile = CastorFile.id
       AND Request.svcClass = SvcClass.id
       AND Request.id = SubRequest.request
       AND Request.client = Client.id;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Something went really wrong, our subrequest does not have the corresponding request or client.
    -- Just drop it and re-raise exception. Some rare occurrences have happened in the past,
    -- this catch-all logic protects the stager-scheduling system from getting stuck with a single such case.
    archiveSubReq(varSrId, dconst.SUBREQUEST_FAILED_FINISHED);
    COMMIT;
    raise_application_error(-20100, 'Request got corrupted and could not be processed');
  END;
  
  -- Select random filesystems to use if none is already requested. This only happens
  -- on Put requests, so we discard READONLY hardware and filter only the PRODUCTION one.
  IF LENGTH(srRfs) IS NULL THEN
    srRFs := selectRandomDestinationFs(svcClassId, varXsize, cfId);
  END IF;
END;
/

/* PL/SQL method implementing D2dTransferToSchedule */
CREATE OR REPLACE
PROCEDURE D2dTransferToSchedule(outTransferId OUT VARCHAR2, outReqId OUT VARCHAR2,
                                outFileId OUT INTEGER, outNsHost OUT VARCHAR2,
                                outEuid OUT INTEGER, outEgid OUT INTEGER,
                                outSvcClassName OUT VARCHAR2, outCreationTime OUT INTEGER,
                                outreplicationType OUT INTEGER,
                                outDestFileSystems OUT VARCHAR2, outSourceFileSystems OUT VARCHAR2) AS
  cfId NUMBER;
  -- Cursor to select the next candidate for submission to the scheduler orderd
  -- by creation time.
  -- Note that the where clause is not strictly needed, but this way Oracle is forced
  -- to use an INDEX RANGE SCAN instead of its preferred (and unstable upon load) FULL SCAN!
  CURSOR c IS
    SELECT /*+ INDEX_RS_ASC(Disk2DiskCopyJob I_Disk2DiskCopyJob_status_CT) */ Disk2DiskCopyJob.id
      FROM Disk2DiskCopyJob
     WHERE status = dconst.DISK2DISKCOPYJOB_PENDING
     ORDER BY creationTime ASC;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  varD2dJId INTEGER;
  varFileSize INTEGER;
  varCfId INTEGER;
  varUnusedMessage VARCHAR2(2048);
  varUnusedStatus INTEGER;
  varSvcClassId INTEGER;
BEGIN
  -- Open a cursor on potential candidates
  OPEN c;
  -- Retrieve the first candidate
  FETCH c INTO varD2dJId;
  IF c%NOTFOUND THEN
    -- There is no candidate available. Wait for next alert concerning something
    -- to schedule for a maximum of 3 seconds.
    -- We do not wait forever in order to to give the control back to the
    -- caller daemon in case it should exit.
    CLOSE c;
    DBMS_ALERT.WAITONE('d2dReadyToSchedule', varUnusedMessage, varUnusedStatus, 3);
    -- try again to find something now that we waited
    OPEN c;
    FETCH c INTO varD2dJId;
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
      -- valid subrequest is in status READYFORSCHED
      SELECT /*+ INDEX(Disk2DiskCopyJob PK_Disk2DiskCopyJob_ID) */ id INTO varD2dJId
        FROM Disk2DiskCopyJob
       WHERE id = varD2dJId
         AND status = dconst.DISK2DISKCOPYJOB_PENDING
         FOR UPDATE NOWAIT;
      -- We have successfully acquired the lock, so we update the Disk2DiskCopyJob
      UPDATE /*+ INDEX(Disk2DiskCopyJob PK_Disk2DiskCopyJob_Id)*/ Disk2DiskCopyJob
         SET status = dconst.DISK2DISKCOPYJOB_SCHEDULED
       WHERE id = varD2dJId
      RETURNING transferId, castorFile, ouid, ogid, creationTime, destSvcClass,
                getSvcClassName(destSvcClass), replicationType
        INTO outTransferId, varCfId, outEuid, outEgid, outCreationTime, varSvcClassId,
             outSvcClassName, outReplicationType;
      -- Extract the rest of the information required by transfer manager
      SELECT fileId, nsHost, fileSize INTO outFileId, outNsHost, varFileSize
        FROM CastorFile
       WHERE CastorFile.id = varCfId;
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
    FETCH c INTO varD2dJId;
    IF c%NOTFOUND THEN
      -- no next one ? then we can return
      RETURN;
    END IF;
  END LOOP;
  CLOSE c;
  -- We finally got a valid candidate, let's select potential sources
  outSourceFileSystems := selectAllSourceFs(varCfId);
  -- Select random filesystems to use as destination.
  outDestFileSystems := selectRandomDestinationFs(varSvcClassId, varFileSize, varCfId);
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
  varFileid NUMBER;
  varNsHost VARCHAR2(2048);
  varReqId VARCHAR2(2048);
  killedTransfers castor."strList";
  errnos castor."cnumList";
  errmsg castor."strList";
BEGIN
  -- cleanup from previous round
  DELETE FROM SyncRunningTransfersHelper2;
  -- insert the list of running transfers into a temporary table for easy access
  FORALL i IN transfers.FIRST .. transfers.LAST
    INSERT INTO SyncRunningTransfersHelper (subreqId) VALUES (transfers(i));
  -- Go through all running transfers from the DB point of view for the given diskserver
  FOR SR IN (SELECT SubRequest.id, SubRequest.subreqId, SubRequest.castorfile, SubRequest.request
               FROM SubRequest, DiskCopy, FileSystem, DiskServer
              WHERE SubRequest.status = dconst.SUBREQUEST_READY
                AND Subrequest.reqType IN (35, 37, 44)  -- StageGet/Put/UpdateRequest
                AND Subrequest.diskCopy = DiskCopy.id
                AND DiskCopy.fileSystem = FileSystem.id
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
      SELECT Request.reqId INTO varReqId FROM
        (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ reqId, id from StageGetRequest UNION ALL
         SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ reqId, id from StagePutRequest UNION ALL
         SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ reqId, id from StageUpdateRequest) Request
       WHERE Request.id = SR.request;
      SELECT fileid, nsHost INTO varFileid, varNsHost FROM CastorFile WHERE id = SR.castorFile;
      -- and we put it in the list of transfers to be failed with code 1015 (SEINTERNAL)
      INSERT INTO SyncRunningTransfersHelper2 (subreqId, reqId, fileid, nsHost, errorCode, errorMsg)
      VALUES (SR.subreqId, varReqId, varFileid, varNsHost, 1015, 'Transfer has been killed while running');
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
  OPEN killedTransfersCur FOR
    SELECT subreqId, reqId, fileid, nsHost FROM SyncRunningTransfersHelper2;
END;
/
/*******************************************************************
 *
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
  -- Hardware status affects the results as follows:
  -- - PRODUCTION and READONLY hardware are the same and don't affect the result
  -- - DRAINING hardware makes any VALID diskcopy be exposed as STAGEABLE
  -- - DISABLED hardware or diskservers with hwOnline flag = 0 are filtered out
  IF svcClassId = 0 THEN
    OPEN result FOR
     SELECT * FROM (
      SELECT fileId, nsHost, dcId, path, fileSize, status, machine, mountPoint, nbCopyAccesses,
             lastKnownFileName, creationTime, svcClass, lastAccessTime, hwStatus
        FROM (
          SELECT UNIQUE CastorFile.id, CastorFile.fileId, CastorFile.nsHost, DC.id AS dcId,
                 DC.path, CastorFile.fileSize,
                 CASE WHEN DC.status = dconst.DISKCOPY_VALID
                      THEN CASE WHEN CastorFile.tapeStatus = dconst.CASTORFILE_NOTONTAPE
                                THEN 10 -- CANBEMIGR
                                ELSE 0  -- STAGED
                                END
                      ELSE DC.status
                      END AS status,
                 CASE WHEN DC.svcClass IS NULL THEN
                   (SELECT /*+ INDEX(Subrequest I_Subrequest_DiskCopy)*/ UNIQUE Req.svcClassName
                      FROM SubRequest,
                        (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id, svcClassName FROM StagePrepareToPutRequest UNION ALL
                         SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcClassName FROM StagePrepareToUpdateRequest) Req
                          WHERE SubRequest.diskCopy = DC.id
                            AND SubRequest.status IN (5, 6, 13)  -- WAITSUBREQ, READY, READYFORSCHED
                            AND request = Req.id)              
                   ELSE DC.svcClass END AS svcClass,
                 DC.machine, DC.mountPoint, DC.nbCopyAccesses, CastorFile.lastKnownFileName,
                 DC.creationTime, DC.lastAccessTime, nvl(decode(DC.hwStatus, 2, dconst.DISKSERVER_DRAINING, DC.hwStatus), -1) hwStatus
            FROM CastorFile,
              (SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, DiskServer.name AS machine, FileSystem.mountPoint,
                      SvcClass.name AS svcClass, DiskCopy.filesystem, DiskCopy.castorFile, 
                      DiskCopy.nbCopyAccesses, DiskCopy.creationTime, DiskCopy.lastAccessTime,
                      decode(FileSystem.status, dconst.DISKSERVER_READONLY, dconst.DISKSERVER_PRODUCTION, FileSystem.status) +  -- READONLY == PRODUCTION
                      decode(DiskServer.status, dconst.FILESYSTEM_READONLY, dconst.FILESYSTEM_PRODUCTION, DiskServer.status) AS hwStatus
                 FROM FileSystem, DiskServer, DiskPool2SvcClass, SvcClass, DiskCopy
                WHERE Diskcopy.castorFile IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
                  AND Diskcopy.status IN (0, 1, 4, 5, 6, 7, 10, 11) -- search for diskCopies not BEINGDELETED
                  AND FileSystem.id(+) = DiskCopy.fileSystem
                  AND nvl(FileSystem.status, 0) IN
                      (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
                  AND DiskServer.id(+) = FileSystem.diskServer
                  AND nvl(DiskServer.status, 0) IN
                      (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
                  AND nvl(DiskServer.hwOnline, 1) = 1
                  AND DiskPool2SvcClass.parent(+) = FileSystem.diskPool
                  AND SvcClass.id(+) = DiskPool2SvcClass.child
                  AND (euid = 0 OR SvcClass.id IS NULL OR   -- if euid > 0 check read permissions for srmLs (see bug #69678)
                       checkPermission(SvcClass.name, euid, egid, 35) = 0)   -- OBJ_StageGetRequest
                 ) DC
           WHERE CastorFile.id = DC.castorFile)
       WHERE status IS NOT NULL    -- search for valid diskcopies
     UNION
      SELECT CastorFile.fileId, CastorFile.nsHost, 0, '', Castorfile.fileSize, 2, -- WAITTAPERECALL
             '', '', 0, CastorFile.lastKnownFileName, Subrequest.creationTime, Req.svcClassName, Subrequest.creationTime, -1
        FROM CastorFile, Subrequest,
             (SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, svcClassName FROM StagePrepareToGetRequest UNION ALL
              SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcClassName FROM StagePrepareToUpdateRequest UNION ALL
              SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClassName FROM StageGetRequest UNION ALL
              SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClassName FROM StageUpdateRequest UNION ALL
              SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, svcClassName FROM StageRepackRequest) Req
       WHERE Castorfile.id IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
         AND Subrequest.CastorFile = Castorfile.id
         AND SubRequest.status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_START, dconst.SUBREQUEST_RESTART)
         AND Req.id = SubRequest.request
     UNION
      SELECT CastorFile.fileId, CastorFile.nsHost, 0, '', Castorfile.fileSize, 1, -- WAITDISK2DISKCOPY
             '', '', 0, CastorFile.lastKnownFileName, Disk2DiskCopyJob.creationTime,
             getSvcClassName(Disk2DiskCopyJob.destSvcClass), Disk2DiskCopyJob.creationTime, -1
        FROM CastorFile, Disk2DiskCopyJob
       WHERE Castorfile.id IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
         AND Disk2DiskCopyJob.CastorFile = Castorfile.id)
    ORDER BY fileid, nshost;
  ELSE
    OPEN result FOR
     SELECT * FROM (
      SELECT fileId, nsHost, dcId, path, fileSize, status, machine, mountPoint, nbCopyAccesses,
             lastKnownFileName, creationTime, (SELECT name FROM svcClass WHERE id = svcClassId),
             lastAccessTime, hwStatus
        FROM (
          SELECT UNIQUE CastorFile.id, CastorFile.fileId, CastorFile.nsHost, DC.id AS dcId,
                 DC.path, CastorFile.fileSize,
                 CASE WHEN DC.dcSvcClass = svcClassId
                      THEN CASE WHEN DC.status = dconst.DISKCOPY_VALID
                                THEN CASE WHEN CastorFile.tapeStatus = dconst.CASTORFILE_NOTONTAPE
                                          THEN 10 -- CANBEMIGR
                                          ELSE 0  -- STAGED
                                          END
                                ELSE DC.status
                                END
                      WHEN DC.fileSystem = 0 THEN
                       (SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/
                        UNIQUE decode(nvl(SubRequest.status, -1), -1, -1, DC.status)
                          FROM SubRequest,
                            (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id, svcclass, svcClassName FROM StagePrepareToPutRequest UNION ALL
                             SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcclass, svcClassName FROM StagePrepareToUpdateRequest) Req
                         WHERE SubRequest.CastorFile = CastorFile.id
                           AND SubRequest.request = Req.id
                           AND SubRequest.status IN (5, 6, 13)  -- WAITSUBREQ, READY, READYFORSCHED
                           AND svcClass = svcClassId)
                      END AS status,
                 DC.machine, DC.mountPoint, DC.nbCopyAccesses, CastorFile.lastKnownFileName,
                 DC.creationTime, DC.lastAccessTime, nvl(decode(DC.hwStatus, 2, dconst.DISKSERVER_DRAINING, DC.hwStatus), -1) hwStatus
            FROM CastorFile,
              (SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, DiskServer.name AS machine, FileSystem.mountPoint,
                      DiskPool2SvcClass.child AS dcSvcClass, DiskCopy.filesystem, DiskCopy.CastorFile, 
                      DiskCopy.nbCopyAccesses, DiskCopy.creationTime, DiskCopy.lastAccessTime,
                      decode(FileSystem.status, dconst.DISKSERVER_READONLY, dconst.DISKSERVER_PRODUCTION, FileSystem.status) +  -- READONLY == PRODUCTION
                      decode(DiskServer.status, dconst.FILESYSTEM_READONLY, dconst.FILESYSTEM_PRODUCTION, DiskServer.status) AS hwStatus
                 FROM FileSystem, DiskServer, DiskPool2SvcClass, DiskCopy
                WHERE Diskcopy.castorFile IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
                  AND DiskCopy.status IN (0, 1, 4, 5, 6, 7, 10, 11)  -- search for diskCopies not GCCANDIDATE or BEINGDELETED
                  AND FileSystem.id(+) = DiskCopy.fileSystem
                  AND nvl(FileSystem.status, 0) IN
                      (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
                  AND DiskServer.id(+) = FileSystem.diskServer
                  AND nvl(DiskServer.status, 0) IN
                      (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
                  AND nvl(DiskServer.hwOnline, 1) = 1
                  AND DiskPool2SvcClass.parent(+) = FileSystem.diskPool) DC
                  -- No extra check on read permissions here, it is not relevant
           WHERE CastorFile.id = DC.castorFile)
       WHERE status IS NOT NULL     -- search for valid diskcopies
     UNION
      SELECT CastorFile.fileId, CastorFile.nsHost, 0, '', Castorfile.fileSize, 2, -- WAITTAPERECALL
             '', '', 0, CastorFile.lastKnownFileName, Subrequest.creationTime, Req.svcClassName, Subrequest.creationTime, -1
        FROM CastorFile, Subrequest,
             (SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, svcClassName, svcClass FROM StagePrepareToGetRequest UNION ALL
              SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcClassName, svcClass FROM StagePrepareToUpdateRequest UNION ALL
              SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClassName, svcClass FROM StageGetRequest UNION ALL
              SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClassName, svcClass FROM StageUpdateRequest UNION ALL
              SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, svcClassName, svcClass FROM StageRepackRequest) Req
       WHERE Castorfile.id IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
         AND Subrequest.CastorFile = Castorfile.id
         AND SubRequest.status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_START, dconst.SUBREQUEST_RESTART)
         AND Req.id = SubRequest.request
         AND Req.svcClass = svcClassId
     UNION
      SELECT CastorFile.fileId, CastorFile.nsHost, 0, '', Castorfile.fileSize, 1, -- WAITDISK2DISKCOPY
             '', '', 0, CastorFile.lastKnownFileName, Disk2DiskCopyJob.creationTime,
             getSvcClassName(Disk2DiskCopyJob.destSvcClass), Disk2DiskCopyJob.creationTime, -1
        FROM CastorFile, Disk2DiskCopyJob
       WHERE Castorfile.id IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
         AND Disk2DiskCopyJob.CastorFile = Castorfile.id
         AND Disk2DiskCopyJob.destSvcClass = svcClassId)
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
CREATE OR REPLACE FUNCTION getSpace(status IN INTEGER, hwOnline IN INTEGER, space IN INTEGER,
                                    queryType IN INTEGER, spaceType IN INTEGER)
RETURN NUMBER IS
BEGIN
  IF space < 0 THEN
    -- over used filesystems may report negative numbers, just cut to 0
    RETURN 0;
  END IF;
  IF ((hwOnline = 0) OR (status > 0 AND status <= 4)) AND  -- either OFFLINE or DRAINING or DISABLED
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
             dp.name, ds.name, max(decode(ds.hwOnline, 0, dconst.DISKSERVER_DISABLED, ds.status)), fs.mountPoint,
             sum(getSpace(fs.status + ds.status, ds.hwOnline,
                          fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_FREE)) AS freeSpace,
             sum(getSpace(fs.status + ds.status, ds.hwOnline,
                          fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_CAPACITY)) AS totalSize,
             0, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp
       WHERE dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         GROUP BY grouping sets(
             (dp.name, ds.name, ds.status, fs.mountPoint,
              getSpace(fs.status + ds.status, ds.hwOnline,
                       fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_FREE),
              getSpace(fs.status + ds.status, ds.hwOnline,
                       fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_CAPACITY),
              fs.maxFreeSpace, fs.status),
             (dp.name, ds.name),
             (dp.name)
            )
         ORDER BY dp.name, IsDSGrouped DESC, ds.name, IsFSGrouped DESC, fs.mountpoint;
  ELSE 
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
             grouping(fs.mountPoint) AS IsFSGrouped,
             dp.name, ds.name, max(decode(ds.hwOnline, 0, dconst.DISKSERVER_DISABLED, ds.status)), fs.mountPoint,
             sum(getSpace(fs.status + ds.status, ds.hwOnline,
                          fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_FREE)) AS freeSpace,
             sum(getSpace(fs.status + ds.status, ds.hwOnline,
                          fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_CAPACITY)) AS totalSize,
             0, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp,
             DiskPool2SvcClass d2s, SvcClass sc
       WHERE sc.name = svcClassName
         AND sc.id = d2s.child
         AND checkPermission(sc.name, reqEuid, reqEgid, 195) = 0
         AND d2s.parent = dp.id
         AND dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         GROUP BY grouping sets(
             (dp.name, ds.name, fs.mountPoint,
              getSpace(fs.status + ds.status, ds.hwOnline,
                       fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_FREE),
              getSpace(fs.status + ds.status, ds.hwOnline,
                       fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_CAPACITY),
              fs.maxFreeSpace, fs.status),
             (dp.name, ds.name),
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
             ds.name, max(decode(ds.hwOnline, 0, dconst.DISKSERVER_DISABLED, ds.status)), fs.mountPoint,
             sum(getSpace(fs.status + ds.status, ds.hwOnline,
                          fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_FREE)) AS freeSpace,
             sum(getSpace(fs.status + ds.status, ds.hwOnline,
                          fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_CAPACITY)) AS totalSize,
             0, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp
       WHERE dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         AND dp.name = diskPoolName
         GROUP BY grouping sets(
             (ds.name, fs.mountPoint,
              getSpace(fs.status + ds.status, ds.hwOnline,
                       fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_FREE),
              getSpace(fs.status + ds.status, ds.hwOnline,
                       fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_CAPACITY),
              fs.maxFreeSpace, fs.status),
             (ds.name),
             (dp.name)
            )
         ORDER BY IsDSGrouped DESC, ds.name, IsGrouped DESC, fs.mountpoint;
  ELSE
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
             grouping(fs.mountPoint) AS IsGrouped,
             ds.name, max(decode(ds.hwOnline, 0, dconst.DISKSERVER_DISABLED, ds.status)), fs.mountPoint,
             sum(getSpace(fs.status + ds.status, ds.hwOnline,
                          fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_FREE)) AS freeSpace,
             sum(getSpace(fs.status + ds.status, ds.hwOnline,
                          fs.totalSize,
                          queryType, dconst.DISKPOOLSPACETYPE_CAPACITY)) AS totalSize,
             0, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp,
             DiskPool2SvcClass d2s, SvcClass sc
       WHERE sc.name = svcClassName
         AND sc.id = d2s.child
         AND d2s.parent = dp.id
         AND dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         AND dp.name = diskPoolName
         GROUP BY grouping sets(
             (ds.name, fs.mountPoint,
              getSpace(fs.status + ds.status, ds.hwOnline,
                       fs.free - fs.minAllowedFreeSpace * fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_FREE),
              getSpace(fs.status + ds.status, ds.hwOnline,
                       fs.totalSize,
                       queryType, dconst.DISKPOOLSPACETYPE_CAPACITY),
              fs.maxFreeSpace, fs.status),
             (ds.name),
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
 *
 * PL/SQL code for the tape gateway daemon
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* PL/SQL declaration for the castorTape package */
CREATE OR REPLACE PACKAGE castorTape AS 
  TYPE TapeGatewayRequest IS RECORD (
    accessMode INTEGER,
    mountTransactionId NUMBER, 
    vid VARCHAR2(2048));
  TYPE TapeGatewayRequest_Cur IS REF CURSOR RETURN TapeGatewayRequest;
  TYPE VIDPriorityRec IS RECORD (vid VARCHAR2(2048), vdqmPriority INTEGER);
  TYPE VIDPriority_Cur IS REF CURSOR RETURN VIDPriorityRec;
  TYPE FileToMigrateCore IS RECORD (
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   lastKnownFileName VARCHAR2(2048),
   filePath VARCHAR2(2048),
   fileTransactionId NUMBER,
   fseq INTEGER,
   fileSize NUMBER);
  TYPE FileToMigrateCore_Cur IS REF CURSOR RETURN  FileToMigrateCore;  
END castorTape;
/

/* attach drive request to a recallMount or a migrationMount */
CREATE OR REPLACE
PROCEDURE tg_attachDriveReq(inVID IN VARCHAR2,
                            inVdqmId IN INTEGER,
                            inMode IN INTEGER,
                            inLabel IN VARCHAR2,
                            inDensity IN VARCHAR2) AS
BEGIN
  IF inMode = tconst.WRITE_DISABLE THEN
    UPDATE RecallMount
       SET lastvdqmpingtime   = gettime(),
           mountTransactionId = inVdqmId,
           status             = tconst.RECALLMOUNT_WAITDRIVE,
           label              = inLabel,
           density            = inDensity
     WHERE VID = inVID;
  ELSE
    UPDATE MigrationMount
       SET lastvdqmpingtime   = gettime(),
           mountTransactionId = inVdqmId,
           status             = tconst.MIGRATIONMOUNT_WAITDRIVE,
           label              = inLabel,
           density            = inDensity
     WHERE VID = inVID;
  END IF;
END;
/

/* attach the tapes to the migration mounts  */
CREATE OR REPLACE
PROCEDURE tg_attachTapesToMigMounts (
  inStartFseqs IN castor."cnumList",
  inMountIds   IN castor."cnumList",
  inTapeVids   IN castor."strList") AS
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
  COMMIT;
END;
/

/* update the db when a tape session is ended */
CREATE OR REPLACE PROCEDURE tg_endTapeSession(inMountTransactionId IN NUMBER,
                                              inErrorCode IN INTEGER) AS
  varMjIds "numList";    -- recall/migration job Ids
  varMountId INTEGER;
BEGIN
  -- Let's assume this is a migration mount
  SELECT id INTO varMountId
    FROM MigrationMount
   WHERE mountTransactionId = inMountTransactionId
   FOR UPDATE;
  -- yes, it's a migration mount: delete it and detach all selected jobs
  UPDATE MigrationJob
     SET status = tconst.MIGRATIONJOB_PENDING,
         VID = NULL,
         mountTransactionId = NULL
   WHERE mountTransactionId = inMountTransactionId
     AND status = tconst.MIGRATIONJOB_SELECTED;
  DELETE FROM MigrationMount
   WHERE id = varMountId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- was not a migration session, let's try a recall one
  DECLARE
    varVID VARCHAR2(2048);
    varRjIds "numList";
  BEGIN
    SELECT vid INTO varVID
      FROM RecallMount
     WHERE mountTransactionId = inMountTransactionId
     FOR UPDATE;
    -- it was a recall mount
    -- find and reset the all RecallJobs of files for this VID
    UPDATE RecallJob
       SET status = tconst.RECALLJOB_PENDING
     WHERE castorFile IN (SELECT castorFile
                            FROM RecallJob
                           WHERE VID = varVID);
    DELETE FROM RecallMount WHERE vid = varVID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Small infusion of paranoia ;-) We should never reach that point...
    ROLLBACK;
    RAISE_APPLICATION_ERROR (-20119, 'endTapeSession: no recall or migration mount found');
  END;
END;
/

/* update the db when a tape session is ended. This autonomous transaction wrapper
 * allow cleanup of leftover sessions when creating new sessions */
CREATE OR REPLACE PROCEDURE tg_endTapeSessionAT(inMountTransactionId IN NUMBER,
                                                inErrorCode IN INTEGER) AS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  tg_endTapeSession(inMountTransactionId, inErrorCode);
  COMMIT;
END;
/

/* find all migration mounts involving a set of tapes */
CREATE OR REPLACE PROCEDURE tg_getMigMountReqsForVids(inVids IN strListTable,
                                                      outBlockingSessions OUT SYS_REFCURSOR) AS
BEGIN
    OPEN  outBlockingSessions FOR
      SELECT vid TPVID, mountTransactionId VDQMREQID
        FROM MigrationMount
       WHERE vid IN (SELECT * FROM TABLE (inVids));
END;
/

/* PL/SQL method implementing bestFileSystemForRecall */
CREATE OR REPLACE PROCEDURE bestFileSystemForRecall(inCfId IN INTEGER, outFilePath OUT VARCHAR2) AS
  varCfId INTEGER;
  varFileSystemId NUMBER := 0;
  nb NUMBER;
BEGIN
  -- try and select a good FileSystem for this recall
  FOR f IN (SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/
                   DiskServer.name ||':'|| FileSystem.mountPoint AS remotePath, FileSystem.id,
                   FileSystem.diskserver, CastorFile.fileSize, CastorFile.fileId, CastorFile.nsHost
              FROM DiskServer, FileSystem, DiskPool2SvcClass,
                   (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClass FROM StageGetRequest                            UNION ALL
                    SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, svcClass FROM StagePrepareToGetRequest UNION ALL
                    SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, svcClass from StageRepackRequest                   UNION ALL
                    SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClass from StageUpdateRequest                   UNION ALL
                    SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcClass FROM StagePrepareToUpdateRequest) Request,
                    SubRequest, CastorFile
             WHERE CastorFile.id = inCfId
               AND SubRequest.castorfile = inCfId
               AND SubRequest.status = dconst.SUBREQUEST_WAITTAPERECALL
               AND Request.id = SubRequest.request
               AND Request.svcclass = DiskPool2SvcClass.child
               AND FileSystem.diskpool = DiskPool2SvcClass.parent
               -- a priori, we want to have enough free space. However, if we don't, we accept to start writing
               -- if we have a minimum of 30GB free and count on gerbage collection to liberate space while writing
               -- We still check that the file fit on the disk, and actually keep a 30% margin so that very recent
               -- files can be kept
               AND (FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > CastorFile.fileSize
                 OR (FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > 30000000000
                 AND FileSystem.totalSize * 0.7 > CastorFile.fileSize))
               AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
               AND DiskServer.id = FileSystem.diskServer
               AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
               AND DiskServer.hwOnline = 1
          ORDER BY -- use randomness to scatter recalls everywhere in the pool. This works unless the pool starts to be overloaded:
                   -- once a hot spot develops, recalls start to take longer and longer and thus tend to accumulate. However,
                   -- until we have a faster feedback system to rank filesystems, the fileSystemRate order has not proven to be better.
                   DBMS_Random.value)
  LOOP
    varFileSystemId := f.id;
    buildPathFromFileId(f.fileId, f.nsHost, ids_seq.nextval, outFilePath);
    outFilePath := f.remotePath || outFilePath;
    -- Check that we don't already have a copy of this file on this filesystem.
    -- This will never happen in normal operations but may be the case if a filesystem
    -- was disabled and did come back while the tape recall was waiting.
    -- Even if we optimize by cancelling remaining unneeded tape recalls when a
    -- fileSystem comes back, the ones running at the time of the come back will have
    -- the problem.
    SELECT count(*) INTO nb
      FROM DiskCopy
     WHERE fileSystem = f.id
       AND castorfile = inCfid
       AND status = dconst.DISKCOPY_VALID;
    IF nb != 0 THEN
      raise_application_error(-20115, 'Recaller could not find a FileSystem in production in the requested SvcClass and without copies of this file');
    END IF;
    RETURN;
  END LOOP;
  IF varFileSystemId = 0 THEN
    raise_application_error(-20115, 'No suitable filesystem found for this recalled file');
  END IF;
END;
/


/* get the migration mounts without any tape associated */
CREATE OR REPLACE
PROCEDURE tg_getMigMountsWithoutTapes(outStrList OUT SYS_REFCURSOR) AS
BEGIN
  -- get migration mounts in state WAITTAPE
  OPEN outStrList FOR
    SELECT M.id, TP.name
      FROM MigrationMount M, Tapepool TP
     WHERE M.status = tconst.MIGRATIONMOUNT_WAITTAPE
       AND M.tapepool = TP.id 
       FOR UPDATE OF M.id SKIP LOCKED;   
END;
/

/* get tape with a pending request in VDQM */
CREATE OR REPLACE
PROCEDURE tg_getTapesWithDriveReqs(
  inTimeLimit     IN  NUMBER,
  outTapeRequest  OUT castorTape.tapegatewayrequest_Cur) AS
  varTgrId        "numList";
  varRecMountIds  "numList";
  varMigMountIds  "numList";
  varNow          NUMBER;
BEGIN 
  -- we only look for the Recall/MigrationMounts which have a VDQM ping
  -- time older than inTimeLimit
  -- No need to query the clock all the time
  varNow := gettime();
  
  -- Find all the recall mounts and lock
  SELECT id BULK COLLECT INTO varRecMountIds
    FROM RecallMount
   WHERE status IN (tconst.RECALLMOUNT_WAITDRIVE, tconst.RECALLMOUNT_RECALLING)
     AND varNow - lastVdqmPingTime > inTimeLimit
     FOR UPDATE SKIP LOCKED;
     
  -- Find all the migration mounts and lock
  SELECT id BULK COLLECT INTO varMigMountIds
    FROM MigrationMount
   WHERE status IN (tconst.MIGRATIONMOUNT_WAITDRIVE, tconst.MIGRATIONMOUNT_MIGRATING)
     AND varNow - lastVdqmPingTime > inTimeLimit
     FOR UPDATE SKIP LOCKED;
     
  -- Update the last VDQM ping time for all of them.
  UPDATE RecallMount
     SET lastVdqmPingTime = varNow
   WHERE id IN (SELECT /*+ CARDINALITY(trTable 5) */ * 
                  FROM TABLE (varRecMountIds));
  UPDATE MigrationMount
     SET lastVdqmPingTime = varNow
   WHERE id IN (SELECT /*+ CARDINALITY(trTable 5) */ *
                  FROM TABLE (varMigMountIds));
                   
  -- Return them
  OPEN outTapeRequest FOR
    -- Read case
    SELECT tconst.WRITE_DISABLE, mountTransactionId, VID
      FROM RecallMount
     WHERE id IN (SELECT /*+ CARDINALITY(trTable 5) */ *
                    FROM TABLE(varRecMountIds))
     UNION ALL
    -- Write case
    SELECT tconst.WRITE_ENABLE, mountTransactionId, VID
      FROM MigrationMount
     WHERE id IN (SELECT /*+ CARDINALITY(trTable 5) */ *
                    FROM TABLE(varMigMountIds));
END;
/

/* get a the list of tapes to be sent to VDQM */
CREATE OR REPLACE
PROCEDURE tg_getTapeWithoutDriveReq(outVID OUT VARCHAR2,
                                    outVdqmPriority OUT INTEGER,
                                    outMode OUT INTEGER) AS
BEGIN
  -- try to find a migration mount
  SELECT VID, 0, 1  -- harcoded priority to 0, mode 1 == WRITE_ENABLE
    INTO outVID, outVdqmPriority, outMode
    FROM MigrationMount
   WHERE status = tconst.MIGRATIONMOUNT_SEND_TO_VDQM
     AND ROWNUM < 2
     FOR UPDATE SKIP LOCKED;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no migration mount to process, try to find a recall mount
  BEGIN
    SELECT RecallMount.VID, RecallGroup.vdqmPriority, 0  -- mode 0 == WRITE_DISABLE
      INTO outVID, outVdqmPriority, outMode
      FROM RecallMount, RecallGroup
     WHERE RecallMount.status = tconst.RECALLMOUNT_NEW
       AND RecallMount.recallGroup = RecallGroup.id
       AND ROWNUM < 2
       FOR UPDATE OF RecallMount.id SKIP LOCKED;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- no recall mount to process either
    outVID := '';
    outVdqmPriority := 0;
    outMode := 0;
  END;
END;
/

/* get tape to release in VMGR */
CREATE OR REPLACE
PROCEDURE tg_getTapeToRelease(
  inMountTransactionId IN  INTEGER, 
  outVID      OUT NOCOPY VARCHAR2, 
  outMode     OUT INTEGER,
  outFull     OUT INTEGER) AS
BEGIN
  -- suppose it's a recall case
  SELECT vid INTO outVID 
    FROM RecallMount
   WHERE mountTransactionId = inMountTransactionId;
  outMode := tconst.WRITE_DISABLE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no a recall, then let's suppose it's a migration
  BEGIN
    SELECT vid, full
    INTO outVID, outFull
      FROM MigrationMount
     WHERE mountTransactionId = inMountTransactionId;
    outMode := tconst.WRITE_ENABLE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- must have been already cleaned by the checker
    NULL;
  END;
END;
/

/* restart taperequest which had problems */
CREATE OR REPLACE
PROCEDURE tg_restartLostReqs(inMountTransactionIds IN castor."cnumList") AS
BEGIN
 FOR i IN inMountTransactionIds.FIRST .. inMountTransactionIds.LAST LOOP   
   tg_endTapeSession(inMountTransactionIds(i), 0);
 END LOOP;
 COMMIT;
END;
/

/* resets a CastorFile, its diskcopies and recall/migrationJobs when it
 * was overwritten in the namespace. This includes :
 *    - updating the CastorFile with the new NS data
 *    - mark current DiskCopies for GC
 *    - restart any pending recalls
 *    - discard any pending migrations
 * XXXX This is a preliminary version of this function that is used only
 * XXXX in the context of overwritten files during recalls. It has to be
 * XXXX completed and tested before any other usage. In particular, is
 * XXXX does not handle the Disk2DiskCopy case
 */
CREATE OR REPLACE PROCEDURE resetOverwrittenCastorFile(inCfId INTEGER,
                                                       inNewLastUpdateTime NUMBER,
                                                       inNewSize INTEGER) AS
BEGIN
  -- update the Castorfile
  UPDATE CastorFile
     SET lastUpdateTime = inNewLastUpdateTime,
         fileSize = inNewSize,
         lastAccessTime = getTime()
   WHERE id = inCfId;
  -- cancel ongoing recalls, if any
  deleteRecallJobs(inCfId);
  -- cancel ongoing migrations, if any
  deleteMigrationJobs(inCfId);
  -- invalidate existing DiskCopies, if any
  UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID
   WHERE castorFile = inCfId
     AND status = dconst.DISKCOPY_VALID;
  -- restart ongoing requests
  -- Note that we reset the "answered" flag of the subrequest. This will potentially lead to
  -- a wrong attempt to answer again the client (but won't harm as the client is gone in that case)
  -- but is needed as the current implementation of the stager also uses this flag to know
  -- whether to archive the subrequest. If we leave it to 1, the subrequests are wrongly
  -- archived when retried, leading e.g. to failing recalls
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_RESTART, answered = 0
   WHERE castorFile = inCfId
     AND status = dconst.SUBREQUEST_WAITTAPERECALL;
END;
/

/* Checks whether repack requests are ongoing for a file and archives them depending on the
 * provided error code.
 * Can be called because of Nameserver errors after recalls or after migrations
 * (cf. failFileMigration and checkRecallInNS).
 */
CREATE OR REPLACE PROCEDURE archiveOrFailRepackSubreq(inCfId INTEGER, inErrorCode INTEGER) AS
  varSrIds "numList";
BEGIN
  -- find and archive any repack subrequest(s)
  SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile) */
         SubRequest.id BULK COLLECT INTO varSrIds
    FROM SubRequest
   WHERE SubRequest.castorfile = inCfId
     AND subrequest.reqType = 119;  -- OBJ_StageRepackRequest
  FOR i IN 1 .. varSrIds.COUNT LOOP
    -- archive: ENOENT and ENSFILECHG are considered as non-errors in a Repack context (#97529)
    archiveSubReq(varSrIds(i), CASE WHEN inErrorCode IN (serrno.ENOENT, serrno.ENSFILECHG)
      THEN dconst.SUBREQUEST_FINISHED ELSE dconst.SUBREQUEST_FAILED_FINISHED END);
    -- for error reporting
    UPDATE SubRequest
       SET errorCode = inErrorCode,
           errorMessage = CASE
             WHEN inErrorCode IN (serrno.ENOENT, serrno.ENSFILECHG) THEN
               ''
             WHEN inErrorCode = serrno.ENSNOSEG THEN
               'Segment was dropped during repack, skipping'
             WHEN inErrorCode = serrno.ENSTOOMANYSEGS THEN
               'File has too many segments on tape, skipping'
             ELSE
               'Migration failed, reached maximum number of retries'
             END
     WHERE id = varSrIds(i);
  END LOOP;
END;
/

/* Checks whether a recall that was reported successful is ok from the namespace
 * point of view. This includes :
 *   - checking that the file still exists
 *   - checking that the file was not overwritten
 *   - checking the checksum, and setting it if there was none
 * In case one of the check fails, appropriate cleaning actions are taken.
 * Returns whether the checks were all ok. If not, the caller should
 * return immediately as all corrective actions were already taken.
 */
CREATE OR REPLACE FUNCTION checkRecallInNS(inCfId IN INTEGER,
                                           inMountTransactionId IN INTEGER,
                                           inVID IN VARCHAR2,
                                           inCopyNb IN INTEGER,
                                           inFseq IN INTEGER,
                                           inFileId IN INTEGER,
                                           inNsHost IN VARCHAR2,
                                           inCksumName IN VARCHAR2,
                                           inCksumValue IN INTEGER,
                                           inLastUpdateTime IN NUMBER,
                                           inReqId IN VARCHAR2,
                                           inLogContext IN VARCHAR2) RETURN BOOLEAN AS
  varNSLastUpdateTime NUMBER;
  varNSSize INTEGER;
  varNSCsumtype VARCHAR2(2048);
  varNSCsumvalue VARCHAR2(2048);
BEGIN
  -- check the namespace
  SELECT mtime, csumtype, csumvalue, filesize
    INTO varNSLastUpdateTime, varNSCsumtype, varNSCsumvalue, varNSSize
    FROM Cns_File_Metadata@remoteNs
   WHERE fileid = inFileId;

  -- was the file overwritten in the meantime ?
  IF varNSLastUpdateTime > inLastUpdateTime THEN
    -- yes ! reset it and thus restart the recall from scratch
    resetOverwrittenCastorFile(inCfId, varNSLastUpdateTime, varNSSize);
    -- in case of repack, just stop and archive the corresponding request(s) as we're not interested
    -- any longer (the original segment disappeared). This potentially stops the entire recall process.
    archiveOrFailRepackSubreq(inCfId, serrno.ENSFILECHG);
    -- log "setFileRecalled : file was overwritten during recall, restarting from scratch or skipping repack"
    logToDLF(inReqId, dlf.LVL_NOTICE, dlf.RECALL_FILE_OVERWRITTEN, inFileId, inNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' NsLastUpdateTime=' || TO_CHAR(varNSLastUpdateTime) ||
             ' CFLastUpdateTime=' || TO_CHAR(inLastUpdateTime) || inLogContext);
    RETURN FALSE;
  END IF;

  -- is the checksum set in the namespace ?
  IF varNSCsumtype IS NULL THEN
    -- no -> let's set it (note that the function called commits in the remote DB)
    setSegChecksumWhenNull@remoteNS(inFileId, inCopyNb, inCksumName, inCksumValue);
    -- log 'checkRecallInNS : created missing checksum in the namespace'
    logToDLF(inReqId, dlf.LVL_SYSTEM, dlf.RECALL_CREATED_CHECKSUM, inFileId, inNsHost, 'nsd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' copyNb=' || TO_CHAR(inCopyNb) ||
             ' TPVID=' || inVID || ' fseq=' || TO_CHAR(inFseq) || ' checksumType='  || inCksumName ||
             ' checksumValue=' || TO_CHAR(inCksumValue));
  ELSE
    -- is the checksum matching ?
    -- note that this is probably useless as it was already checked at transfer time
    IF inCksumName = varNSCsumtype AND TO_CHAR(inCksumValue, 'XXXXXXXX') != varNSCsumvalue THEN
      -- not matching ! log "checkRecallInNS : bad checksum detected, will retry if allowed"
      logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_BAD_CHECKSUM, inFileId, inNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
               ' fseq=' || TO_CHAR(inFseq) || ' copyNb=' || TO_CHAR(inCopyNb) || ' checksumType=' || inCksumName ||
               ' expectedChecksumValue=' || varNSCsumvalue ||
               ' checksumValue=' || TO_CHAR(inCksumValue, 'XXXXXXXX') || inLogContext);
      retryOrFailRecall(inCfId, inVID, inReqId, inLogContext);
      RETURN FALSE;
    END IF;
  END IF;
  RETURN TRUE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- file got dropped from the namespace, recall should be cancelled
  deleteRecallJobs(inCfId);
  -- potentially terminate repack requests
  archiveOrFailRepackSubreq(inCfId, serrno.ENOENT);
  -- and fail remaining requests
  UPDATE SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOENT,
           errorMessage = 'File was removed during recall'
     WHERE castorFile = inCfId
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
  -- log "checkRecallInNS : file was dropped from namespace during recall, giving up"
  logToDLF(inReqId, dlf.LVL_NOTICE, dlf.RECALL_FILE_DROPPED, inFileId, inNsHost, 'tapegatewayd',
           'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
           ' fseq=' || TO_CHAR(inFseq) || ' CFLastUpdateTime=' || TO_CHAR(inLastUpdateTime) || ' ' || inLogContext);
  RETURN FALSE;
END;
/

/* update the db after a successful recall */
CREATE OR REPLACE PROCEDURE tg_setFileRecalled(inMountTransactionId IN INTEGER,
                                               inFseq IN INTEGER,
                                               inFilePath IN VARCHAR2,
                                               inCksumName IN VARCHAR2,
                                               inCksumValue IN INTEGER,
                                               inReqId IN VARCHAR2,
                                               inLogContext IN VARCHAR2) AS
  varFileId         INTEGER;
  varNsHost         VARCHAR2(2048);
  varVID            VARCHAR2(2048);
  varCopyNb         INTEGER;
  varSvcClassId     INTEGER;
  varEuid           INTEGER;
  varEgid           INTEGER;
  varLastUpdateTime INTEGER;
  varCfId           INTEGER;
  varFSId           INTEGER;
  varDCPath         VARCHAR2(2048);
  varDcId           INTEGER;
  varFileSize       INTEGER;
  varFileClassId    INTEGER;
  varNbMigrationsStarted INTEGER;
  varGcWeight       NUMBER;
  varGcWeightProc   VARCHAR2(2048);
  varRecallStartTime NUMBER;
BEGIN
  -- first lock Castorfile, check NS and parse path

  -- Get RecallJob and lock Castorfile
  BEGIN
    SELECT CastorFile.id, CastorFile.fileId, CastorFile.nsHost, CastorFile.lastUpdateTime,
           CastorFile.fileSize, CastorFile.fileClass, RecallMount.VID, RecallJob.copyNb,
           RecallJob.euid, RecallJob.egid
      INTO varCfId, varFileId, varNsHost, varLastUpdateTime, varFileSize, varFileClassId, varVID,
           varCopyNb, varEuid, varEgid
      FROM RecallMount, RecallJob, CastorFile
     WHERE RecallMount.mountTransactionId = inMountTransactionId
       AND RecallJob.vid = RecallMount.vid
       AND RecallJob.fseq = inFseq
       AND RecallJob.status = tconst.RECALLJOB_SELECTED
       AND RecallJob.castorFile = CastorFile.id
       AND ROWNUM < 2
       FOR UPDATE OF CastorFile.id;
    -- the ROWNUM < 2 clause is worth a comment here :
    -- this select will select a single CastorFile and RecallMount, but may select
    -- several RecallJobs "linked" to them. All these recall jobs have the same copyNb
    -- but different uid/gid. They exist because these different uid/gid are attached
    -- to different recallGroups.
    -- In case of several recallJobs present, they are all equally responsible for the
    -- recall, thus we pick the first one as "the" responsible. The only consequence is
    -- that it's uid/gid will be used for the DiskCopy creation
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "setFileRecalled : unable to identify Recall. giving up"
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_NOT_FOUND, 0, '', 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' ' || inLogContext);
    RETURN;
  END;
  -- Check that the file is still there in the namespace (and did not get overwritten)
  -- Note that error handling and logging is done inside the function
  IF NOT checkRecallInNS(varCfId, inMountTransactionId, varVID, varCopyNb, inFseq, varFileId, varNsHost,
                         inCksumName, inCksumValue, varLastUpdateTime, inReqId, inLogContext) THEN
    RETURN;
  END IF;
  -- get diskserver, filesystem and path from full path in input
  BEGIN
    parsePath(inFilePath, varFSId, varDCPath, varDCId);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "setFileRecalled : unable to parse input path. giving up"
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_INVALID_PATH, varFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' ' || inLogContext);
    RETURN;
  END;

  -- Then deal with recalljobs and potential migrationJobs

  -- Find out starting time of oldest recall for logging purposes
  SELECT MIN(creationTime) INTO varRecallStartTime FROM RecallJob WHERE castorFile = varCfId;
  -- Delete recall jobs
  DELETE FROM RecallJob WHERE castorFile = varCfId;
  -- trigger waiting migrations if any
  -- Note that we reset the creation time as if the MigrationJob was created right now
  -- this is because "creationTime" is actually the time of entering the "PENDING" state
  -- in the cases where the migrationJob went through a WAITINGONRECALL state
  UPDATE /*+ INDEX (MigrationJob I_MigrationJob_CFVID) */ MigrationJob
     SET status = tconst.MIGRATIONJOB_PENDING,
         creationTime = getTime()
   WHERE status = tconst.MIGRATIONJOB_WAITINGONRECALL
     AND castorFile = varCfId;
  varNbMigrationsStarted := SQL%ROWCOUNT;

  -- Deal with DiskCopies

  -- compute GC weight of the recalled diskcopy
  -- first get the svcClass
  SELECT Diskpool2SvcClass.child INTO varSvcClassId
    FROM Diskpool2SvcClass, FileSystem
   WHERE FileSystem.id = varFSId
     AND Diskpool2SvcClass.parent = FileSystem.diskPool
     AND ROWNUM < 2;
  -- Again, the ROWNUM < 2 is worth a comment : the diskpool may be attached
  -- to several svcClasses. However, we do not support that these different
  -- SvcClasses have different GC policies (actually the GC policy should be
  -- moved to the DiskPool table in the future). Thus it is safe to take any
  -- SvcClass from the list
  varGcWeightProc := castorGC.getRecallWeight(varSvcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || varGcWeightProc || '(:size); END;'
    USING OUT varGcWeight, IN varFileSize;
  -- create the DiskCopy, after getting how many copies on tape we have, for the importance number
  DECLARE
    varNbCopiesOnTape INTEGER;
  BEGIN
    SELECT nbCopies INTO varNbCopiesOnTape FROM FileClass WHERE id = varFileClassId;
    INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                          ownerUid, ownerGid, id, gcType, fileSystem, castorFile, status, importance)
    VALUES (varDCPath, varGcWeight, getTime(), getTime(), varFileSize, 0,
            varEuid, varEgid, varDCId, NULL, varFSId, varCfId, dconst.DISKCOPY_VALID,
            -1-varNbCopiesOnTape*100);
  END;
  -- in case there are migrations, update CastorFile's tapeStatus to NOTONTAPE
  IF varNbMigrationsStarted > 0 THEN
    UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_NOTONTAPE WHERE id = varCfId;
  END IF;

  -- Finally deal with user requests
  UPDATE SubRequest
     SET status = decode(reqType,
                         119, dconst.SUBREQUEST_REPACK, -- repack case
                         dconst.SUBREQUEST_RESTART),    -- standard case
         getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
         lastModificationTime = getTime()
   WHERE castorFile = varCfId
     AND status = dconst.SUBREQUEST_WAITTAPERECALL;

  -- trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(varCfId, varEuid, varEgid);

  -- log success
  logToDLF(inReqId, dlf.LVL_SYSTEM, dlf.RECALL_COMPLETED_DB, varFileId, varNsHost, 'tapegatewayd',
           'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
           ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' recallTime=' ||
           to_char(trunc(getTime() - varRecallStartTime, 0)) || ' ' || inLogContext);
END;
/

/* Attempt to retry a recall. Fail it in case it should not be retried anymore */
CREATE OR REPLACE PROCEDURE retryOrFailRecall(inCfId IN NUMBER, inVID IN VARCHAR2,
                                              inReqId IN VARCHAR2, inLogContext IN VARCHAR2) AS
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
  varRecallStillAlive INTEGER;
BEGIN
  -- lock castorFile
  SELECT fileId, nsHost INTO varFileId, varNsHost
    FROM CastorFile WHERE id = inCfId FOR UPDATE;
  -- increase retry counters within mount and set recallJob status to NEW
  UPDATE RecallJob
     SET nbRetriesWithinMount = nbRetriesWithinMount + 1,
         status = tconst.RECALLJOB_PENDING
   WHERE castorFile = inCfId
     AND VID = inVID;
  -- detect the RecallJobs with too many retries within this mount
  -- mark them for a retry on next mount
  UPDATE RecallJob
     SET nbRetriesWithinMount = 0,
         nbMounts = nbMounts + 1,
         status = tconst.RECALLJOB_RETRYMOUNT
   WHERE castorFile = inCfId
     AND VID = inVID
     AND nbRetriesWithinMount >= TO_NUMBER(getConfigOption('Recall', 'MaxNbRetriesWithinMount', 2));
  -- stop here if no recallJob was concerned
  IF SQL%ROWCOUNT = 0 THEN RETURN; END IF;
  -- detect RecallJobs with too many mounts
  DELETE RecallJob
   WHERE castorFile = inCfId
     AND VID = inVID
     AND nbMounts >= TO_NUMBER(getConfigOption('Recall', 'MaxNbMounts', 3));
  -- check whether other RecallJobs are still around for this file (other copies on tape)
  SELECT count(*) INTO varRecallStillAlive
    FROM RecallJob
   WHERE castorFile = inCfId
     AND ROWNUM < 2;
  -- if no remaining recallJobs, the subrequests are failed
  IF varRecallStillAlive = 0 THEN
    UPDATE /*+ INDEX(Subrequest I_Subrequest_Castorfile) */ SubRequest 
       SET status = dconst.SUBREQUEST_FAILED,
           lastModificationTime = getTime(),
           errorCode = serrno.SEINTERNAL,
           errorMessage = 'File recall from tape has failed, please try again later'
     WHERE castorFile = inCfId 
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
     -- log 'File recall has permanently failed'
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_PERMANENTLY_FAILED, varFileId, varNsHost,
      'tapegatewayd', ' TPVID=' || inVID ||' '|| inLogContext);
  END IF;
END;
/

/* Attempt to retry a migration. Fail it in case it should not be retried anymore */
CREATE OR REPLACE PROCEDURE retryOrFailMigration(inMountTrId IN NUMBER, inFileId IN VARCHAR2, inNsHost IN VARCHAR2,
                                                 inErrorCode IN NUMBER, inReqId IN VARCHAR2) AS
  varFileTrId NUMBER;
BEGIN
  -- For the time being, we ignore the error code and apply the same policy to any
  -- tape-side error. Note that NS errors like ENOENT are caught at a second stage and never retried.
  -- Check if a new retry is allowed
  UPDATE (
    SELECT nbRetries, status, vid, mountTransactionId, fileTransactionId
      FROM MigrationJob MJ, CastorFile CF
     WHERE mountTransactionId = inMountTrId
       AND MJ.castorFile = CF.id
       AND CF.fileId = inFileId
       AND CF.nsHost = inNsHost
       AND nbRetries <= TO_NUMBER(getConfigOption('Migration', 'MaxNbMounts', 7)))
    SET nbRetries = nbRetries + 1,
        status = tconst.MIGRATIONJOB_PENDING,
        vid = NULL,
        mountTransactionId = NULL
    RETURNING fileTransactionId INTO varFileTrId;
  IF SQL%ROWCOUNT = 0 THEN
    -- Nb of retries exceeded or migration job not found, fail migration
    failFileMigration(inMountTrId, inFileId, inErrorCode, inReqId);
  -- ELSE we have one more retry, which has been logged upstream
  END IF;
END;
/


/* update the db when a tape session is started */
CREATE OR REPLACE
PROCEDURE tg_startTapeSession(inMountTransactionId IN NUMBER,
                              outVid        OUT NOCOPY VARCHAR2,
                              outAccessMode OUT INTEGER,
                              outRet        OUT INTEGER,
                              outDensity    OUT NOCOPY VARCHAR2,
                              outLabel      OUT NOCOPY VARCHAR2) AS
  varUnused   NUMBER;
  varTapePool INTEGER;
BEGIN
  outRet := 0;
  -- try to deal with a read case
  UPDATE RecallMount
     SET status = tconst.RECALLMOUNT_RECALLING
   WHERE mountTransactionId = inMountTransactionId
  RETURNING VID, tconst.WRITE_DISABLE, 0, density, label
    INTO outVid, outAccessMode, outRet, outDensity, outLabel;
  IF SQL%ROWCOUNT > 0 THEN
    -- it is a read case
    -- check whether there is something to do
    BEGIN
      SELECT id INTO varUnused FROM RecallJob WHERE VID=outVID AND ROWNUM < 2;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- no more file to recall. Force the cleanup and return -1
      UPDATE RecallMount
         SET lastvdqmpingtime = 0
       WHERE mountTransactionId = inMountTransactionId;
      outRet:=-1;
    END;
  ELSE
    -- not a read, so it should be a write
    UPDATE MigrationMount
       SET status = tconst.MIGRATIONMOUNT_MIGRATING
     WHERE mountTransactionId = inMountTransactionId
    RETURNING VID, tconst.WRITE_ENABLE, 0, density, label, tapePool
    INTO outVid, outAccessMode, outRet, outDensity, outLabel, varTapePool;
    IF SQL%ROWCOUNT > 0 THEN
      -- it is a write case
      -- check whether there is something to do
      BEGIN
        SELECT id INTO varUnused FROM MigrationJob WHERE tapePool=varTapePool AND ROWNUM < 2;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- no more file to migrate. Force the cleanup and return -1
        UPDATE MigrationMount
           SET lastvdqmpingtime = 0
         WHERE mountTransactionId = inMountTransactionId;
        outRet:=-1;
      END;
    ELSE
      -- it was neither a read nor a write -> not found error.
      outRet:=-2; -- UNKNOWN request
    END IF;
  END IF;
END;
/

/* delete MigrationMount */
CREATE OR REPLACE PROCEDURE tg_deleteMigrationMount(inMountId IN NUMBER) AS
BEGIN
  DELETE FROM MigrationMount WHERE id=inMountId;
END;
/


/* fail recall of a given CastorFile for a non existing tape */
CREATE OR REPLACE PROCEDURE cancelRecallForCFAndVID(inCfId IN INTEGER,
                                                    inVID IN VARCHAR2,
                                                    inErrorCode IN INTEGER,
                                                    inErrorMsg IN VARCHAR2) AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  varNbRecalls INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
BEGIN
  -- lock castorFile, skip if it's missing
  -- (it may have disappeared in the mean time as we held no lock)
  BEGIN
    SELECT fileid, nsHost INTO varFileId, varNsHost
      FROM CastorFile
     WHERE id = inCfId
       FOR UPDATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN;
  END;
  -- log "Canceling RecallJobs for given VID"
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_CANCEL_RECALLJOB_VID, varFileId, varNsHost, 'tapegatewayd',
           'errorCode=' || TO_CHAR(inErrorCode) ||
           ' errorMessage="' || inErrorMsg ||
           '" TPVID=' || inVID);
  -- remove recallJobs that need the non existing tape
  DELETE FROM RecallJob WHERE castorfile = inCfId AND VID=inVID;
  -- check if other recallJobs remain (typically dual copy tapes)
  SELECT COUNT(*) INTO varNbRecalls FROM RecallJob WHERE castorfile = inCfId;
  -- if no remaining recalls, fail requests and cleanup
  IF varNbRecalls = 0 THEN
    -- log "Failing Recall(s)"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_FAILING, varFileId, varNsHost, 'tapegatewayd',
             'errorCode=' || TO_CHAR(inErrorCode) ||
             ' errorMessage="' || inErrorMsg ||
             '" TPVID=' || inVID);
    -- delete potential migration jobs waiting on recalls
    deleteMigrationJobsForRecall(inCfId);
    -- Fail the associated subrequest(s)
    UPDATE /*+ INDEX(SR I_Subrequest_Castorfile)*/ SubRequest SR
       SET SR.status = dconst.SUBREQUEST_FAILED,
           SR.getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED, --  (not strictly correct but the request is over anyway)
           SR.lastModificationTime = getTime(),
           SR.errorCode = serrno.SEINTERNAL,
           SR.errorMessage = 'File recall from tape has failed (tape not available), please try again later'
     WHERE SR.castorFile = inCfId
       AND SR.status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_WAITSUBREQ);
  END IF;
  -- commit
  COMMIT;
END;
/

/* Cancel a tape session before startup e.g. in case of a VMGR errors when checking the tape.
 * Not to be called on a running session as the procedure assumes no running migration/recall job
 * is attached to the session being deleted.
 */
CREATE OR REPLACE PROCEDURE cancelMigrationOrRecall(inMode IN INTEGER,
                                                    inVID IN VARCHAR2,
                                                    inErrorCode IN INTEGER,
                                                    inErrorMsg IN VARCHAR2) AS
BEGIN
  IF inMode = tconst.WRITE_ENABLE THEN
    -- cancel the migration. No job has been attached yet
    DELETE FROM MigrationMount WHERE VID = inVID;
  ELSE
    -- cancel the recall
    DELETE FROM RecallMount WHERE VID = inVID;
    -- fail the recalls of all files that waited for this tape
    FOR file IN (SELECT castorFile FROM RecallJob WHERE VID = inVID) LOOP
      -- note that this call commits
      cancelRecallForCFAndVID(file.castorFile, inVID, inErrorCode, inErrorMsg);
    END LOOP;
  END IF;
END;
/

/* flag tape as full for a given session */
CREATE OR REPLACE PROCEDURE tg_flagTapeFull (inMountTransactionId IN NUMBER) AS
BEGIN
  UPDATE MigrationMount SET full = 1 WHERE mountTransactionId = inMountTransactionId;
END;
/

/* Find the VID of the tape used in a tape session */
CREATE OR REPLACE PROCEDURE tg_getMigrationMountVid (
    inMountTransactionId IN NUMBER,
    outVid          OUT NOCOPY VARCHAR2,
    outTapePool     OUT NOCOPY VARCHAR2) AS
    varMMId         NUMBER;
    varUnused       NUMBER;
BEGIN
  SELECT MigrationMount.vid, TapePool.name
    INTO outVid, outTapePool
    FROM MigrationMount, TapePool
   WHERE TapePool.id = MigrationMount.tapePool
     AND MigrationMount.mountTransactionId = inMountTransactionId;
END;
/


/* insert new Migration Mount */
CREATE OR REPLACE PROCEDURE insertMigrationMount(inTapePoolId IN NUMBER,
                                                 outMountId OUT INTEGER) AS
  varMigJobId INTEGER;
BEGIN
  -- Check that the mount would be honoured by running a dry-run file selection.
  -- This is almost a duplicate of the query in tg_getFilesToMigrate.
  SELECT /*+ FIRST_ROWS_1
             LEADING(MigrationJob CastorFile DiskCopy FileSystem DiskServer)
             USE_NL(MMigrationJob CastorFile DiskCopy FileSystem DiskServer)
             INDEX(CastorFile PK_CastorFile_Id)
             INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile)
             INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusId) */
         MigrationJob.id mjId INTO varMigJobId
    FROM MigrationJob, DiskCopy, FileSystem, DiskServer, CastorFile
   WHERE MigrationJob.tapePool = inTapePoolId
     AND MigrationJob.status = tconst.MIGRATIONJOB_PENDING
     AND CastorFile.id = MigrationJob.castorFile
     AND CastorFile.id = DiskCopy.castorFile
     AND CastorFile.tapeStatus = dconst.CASTORFILE_NOTONTAPE
     AND DiskCopy.status = dconst.DISKCOPY_VALID
     AND FileSystem.id = DiskCopy.fileSystem
     AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
     AND DiskServer.id = FileSystem.diskServer
     AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
     AND DiskServer.hwOnline = 1
     AND ROWNUM < 2;
  -- The select worked out, create a mount for this tape pool
  INSERT INTO MigrationMount
              (mountTransactionId, id, startTime, VID, label, density,
               lastFseq, lastVDQMPingTime, tapePool, status)
    VALUES (NULL, ids_seq.nextval, gettime(), NULL, NULL, NULL,
            NULL, 0, inTapePoolId, tconst.MIGRATIONMOUNT_WAITTAPE)
    RETURNING id INTO outMountId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- No valid candidate found: this could happen e.g. when candidates exist
  -- but reside on non-available hardware. In this case we drop the mount and log
  outMountId := 0;
END;
/


/* DB job to start new migration mounts */
CREATE OR REPLACE PROCEDURE startMigrationMounts AS
  varNbPreExistingMounts INTEGER;
  varTotalNbMounts INTEGER := 0;
  varDataAmount INTEGER;
  varNbFiles INTEGER;
  varOldestCreationTime NUMBER;
  varMountId INTEGER;
BEGIN
  -- loop through tapepools
  FOR t IN (SELECT id, name, nbDrives, minAmountDataForMount,
                   minNbFilesForMount, maxFileAgeBeforeMount
              FROM TapePool) LOOP
    -- get number of mounts already running for this tapepool
    SELECT nvl(count(*), 0) INTO varNbPreExistingMounts
      FROM MigrationMount
     WHERE tapePool = t.id;
    varTotalNbMounts := varNbPreExistingMounts;
    -- get the amount of data and number of files to migrate, plus the age of the oldest file
    SELECT nvl(SUM(fileSize), 0), COUNT(*), nvl(MIN(creationTime), 0)
      INTO varDataAmount, varNbFiles, varOldestCreationTime
      FROM MigrationJob
     WHERE tapePool = t.id
       AND status = tconst.MIGRATIONJOB_PENDING;
    -- Create as many mounts as needed according to amount of data and number of files
    WHILE (varTotalNbMounts < t.nbDrives) AND
          ((varDataAmount/(varTotalNbMounts+1) >= t.minAmountDataForMount) OR
           (varNbFiles/(varTotalNbMounts+1) >= t.minNbFilesForMount)) AND
          (varTotalNbMounts+1 <= varNbFiles) LOOP   -- in case minAmountDataForMount << avgFileSize, stop creating more than one mount per file
      insertMigrationMount(t.id, varMountId);
      varTotalNbMounts := varTotalNbMounts + 1;
      IF varMountId = 0 THEN
        -- log "startMigrationMounts: failed migration mount creation due to lack of files"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NO_FILE, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
        -- no need to continue as we could not find a single file to migrate
        EXIT;
      ELSE
        -- log "startMigrationMounts: created new migration mount"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NEW_MOUNT, 0, '', 'tapegatewayd',
                 'mountId=' || TO_CHAR(varMountId) ||
                 ' tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
      END IF;
    END LOOP;
    -- force creation of a unique mount in case no mount was created at all and some files are too old
    IF varNbFiles > 0 AND varTotalNbMounts = 0 AND t.nbDrives > 0 AND
       gettime() - varOldestCreationTime > t.maxFileAgeBeforeMount THEN
      insertMigrationMount(t.id, varMountId);
      IF varMountId = 0 THEN
        -- log "startMigrationMounts: failed migration mount creation due to lack of files"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_AGE_NO_FILE, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
      ELSE
        -- log "startMigrationMounts: created new migration mount based on age"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NEW_MOUNT_AGE, 0, '', 'tapegatewayd',
                 'mountId=' || TO_CHAR(varMountId) ||
                 ' tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
      END IF;
    ELSE
      IF varTotalNbMounts = varNbPreExistingMounts THEN 
        -- log "startMigrationMounts: no need for new migration mount"
        logToDLF(NULL, dlf.LVL_DEBUG, dlf.MIGMOUNT_NOACTION, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(nvl(varDataAmount,0)) ||
                 ' nbFilesInQueue=' || TO_CHAR(nvl(varNbFiles,0)) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(nvl(varOldestCreationTime,0))));
      END IF;
    END IF;
    COMMIT;
  END LOOP;
END;
/

/* DB job to start new recall mounts */
CREATE OR REPLACE PROCEDURE startRecallMounts AS
   varNbMounts INTEGER;
   varNbExtraMounts INTEGER := 0;
BEGIN
  -- loop through RecallGroups
  FOR rg IN (SELECT id, name, nbDrives, minAmountDataForMount,
                    minNbFilesForMount, maxFileAgeBeforeMount
               FROM RecallGroup
              ORDER BY vdqmPriority DESC) LOOP
    -- get number of mounts already running for this recallGroup
    SELECT COUNT(*) INTO varNbMounts
      FROM RecallMount
     WHERE recallGroup = rg.id;
    -- check whether some tapes should be mounted
    IF varNbMounts < rg.nbDrives THEN
      DECLARE
        varVID VARCHAR2(2048);
        varDataAmount INTEGER;
        varNbFiles INTEGER;
        varOldestCreationTime NUMBER;
      BEGIN
        -- loop over the best candidates until we have enough mounts
        WHILE varNbMounts + varNbExtraMounts < rg.nbDrives LOOP
          SELECT * INTO varVID, varDataAmount, varNbFiles, varOldestCreationTime FROM (
            SELECT vid, SUM(fileSize) dataAmount, COUNT(*) nbFiles, MIN(creationTime)
              FROM RecallJob
             WHERE recallGroup = rg.id
             GROUP BY vid
            HAVING (SUM(fileSize) >= rg.minAmountDataForMount OR
                    COUNT(*) >= rg.minNbFilesForMount OR
                    gettime() - MIN(creationTime) > rg.maxFileAgeBeforeMount)
               AND VID NOT IN (SELECT vid FROM RecallMount)
             ORDER BY MIN(creationTime))
           WHERE ROWNUM < 2;
          -- trigger a new mount
          INSERT INTO RecallMount (id, VID, recallGroup, startTime, status)
          VALUES (ids_seq.nextval, varVid, rg.id, gettime(), tconst.RECALLMOUNT_NEW);
          varNbExtraMounts := varNbExtraMounts + 1;
          -- log "startRecallMounts: created new recall mount"
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECMOUNT_NEW_MOUNT, 0, '', 'tapegatewayd',
                   'recallGroup=' || rg.name ||
                   ' tape=' || varVid ||
                   ' nbExistingMounts=' || TO_CHAR(varNbMounts) ||
                   ' nbNewMountsSoFar=' || TO_CHAR(varNbExtraMounts) ||
                   ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                   ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                   ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
        END LOOP;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- nothing left to recall, just exit nicely
        NULL;
      END;
      IF varNbExtraMounts = 0 THEN
        -- log "startRecallMounts: no candidate found for a mount"
        logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECMOUNT_NOACTION_NOCAND, 0, '',
                 'tapegatewayd', 'recallGroup=' || rg.name);
      END IF;
    ELSE
      -- log "startRecallMounts: not allowed to start new recall mount. Maximum nb of drives has been reached"
      logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECMOUNT_NOACTION_NODRIVE, 0, '',
               'tapegatewayd', 'recallGroup=' || rg.name);
    END IF;
    COMMIT;
  END LOOP;
END;
/

/*** Disk-Tape interface, Migration ***/

/* Get next candidates for a given migration mount.
 * input:  VDQM transaction id, count and total size
 * output: outVid    the target VID,
           outFiles  a cursor for the set of migration candidates. 
 * Locks are taken on the selected migration jobs.
 *
 * We should only propose a migration job for a file that does not
 * already have another copy migrated to the same tape.
 * The already migrated copies are kept in MigratedSegment until the whole set
 * of siblings has been migrated.
 */
CREATE OR REPLACE PROCEDURE tg_getBulkFilesToMigrate(inLogContext IN VARCHAR2,
                                                     inMountTrId IN NUMBER,
                                                     inCount IN INTEGER,
                                                     inTotalSize IN INTEGER,
                                                     outFiles OUT castorTape.FileToMigrateCore_cur) AS
  varMountId NUMBER;
  varCount INTEGER;
  varTotalSize INTEGER;
  varVid VARCHAR2(10);
  varNewFseq INTEGER;
  varFileTrId NUMBER;
  varUnused INTEGER;
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
BEGIN
  BEGIN
    -- Get id, VID and last valid fseq for this migration mount, lock
    SELECT id, vid, lastFSeq INTO varMountId, varVid, varNewFseq
      FROM MigrationMount
     WHERE mountTransactionId = inMountTrId
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- migration mount is over or unknown request: return an empty cursor
    OPEN outFiles FOR
      SELECT fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fseq, fileSize
        FROM FilesToMigrateHelper;
    RETURN;
  END;
  varCount := 0;
  varTotalSize := 0;
  -- Get candidates up to inCount or inTotalSize
  FOR Cand IN (
    SELECT /*+ FIRST_ROWS(100)
               LEADING(MigrationMount MigrationJob CastorFile DiskCopy FileSystem DiskServer)
               USE_NL(MigrationMount MigrationJob CastorFile DiskCopy FileSystem DiskServer)
               INDEX(CastorFile PK_CastorFile_Id)
               INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile)
               INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusId) */
           MigrationJob.id mjId, DiskServer.name || ':' || FileSystem.mountPoint || DiskCopy.path filePath,
           CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, CastorFile.lastKnownFileName,
           MigrationMount.VID, Castorfile.id as castorfile
      FROM MigrationMount, MigrationJob, CastorFile, DiskCopy, FileSystem, DiskServer
     WHERE MigrationMount.id = varMountId
       AND MigrationJob.tapePool = MigrationMount.tapePool
       AND MigrationJob.status = tconst.MIGRATIONJOB_PENDING
       AND CastorFile.id = MigrationJob.castorFile
       AND CastorFile.id = DiskCopy.castorFile
       AND CastorFile.tapeStatus = dconst.CASTORFILE_NOTONTAPE
       AND DiskCopy.status = dconst.DISKCOPY_VALID
       AND FileSystem.id = DiskCopy.fileSystem
       AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
       AND DiskServer.id = FileSystem.diskServer
       AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
       AND DiskServer.hwOnline = 1
       AND NOT EXISTS (SELECT /*+ INDEX_RS_ASC(MigratedSegment I_MigratedSegment_CFCopyNBVID) */ 1
                         FROM MigratedSegment
                        WHERE MigratedSegment.castorFile = MigrationJob.castorfile
                          AND MigratedSegment.copyNb != MigrationJob.destCopyNb
                          AND MigratedSegment.vid = MigrationMount.vid)
       FOR UPDATE OF MigrationJob.id SKIP LOCKED)
  LOOP
    -- last part of the above statement. Could not be part of it as ORACLE insisted on not
    -- optimizing properly the execution plan
    BEGIN
      SELECT /*+ INDEX_RS_ASC(MJ I_MigrationJob_CFVID) */ 1 INTO varUnused
        FROM MigrationJob MJ
       WHERE MJ.castorFile = Cand.castorFile
         AND MJ.vid = Cand.VID
         AND MJ.vid IS NOT NULL;
      -- found one, so skip this candidate
      CONTINUE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- nothing, it's a valid candidate
      NULL;
    END;
    BEGIN
      -- Try to take this candidate on this mount
      INSERT INTO FilesToMigrateHelper (fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fileSize, fseq)
        VALUES (Cand.fileId, Cand.nsHost, Cand.lastKnownFileName, Cand.filePath, ids_seq.NEXTVAL, Cand.fileSize, varNewFseq)
        RETURNING fileTransactionId INTO varFileTrId;
    EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- If we fail here, it means that another copy of this file was already selected for this mount.
      -- Not a big deal, we skip this candidate and keep going.
      CONTINUE;
    END;
    varCount := varCount + 1;
    varTotalSize := varTotalSize + Cand.fileSize;
    UPDATE MigrationJob
       SET status = tconst.MIGRATIONJOB_SELECTED,
           vid = varVid,
           fSeq = varNewFseq,
           mountTransactionId = inMountTrId,
           fileTransactionId = varFileTrId
     WHERE id = Cand.mjId;
    varNewFseq := varNewFseq + 1;    -- we still decide the fseq for each migration candidate
    IF varCount >= inCount OR varTotalSize >= inTotalSize THEN
      -- we have enough candidates for this round, exit loop
      EXIT;
    END IF;
  END LOOP;
  -- Update last fseq
  UPDATE MigrationMount
     SET lastFseq = varNewFseq
   WHERE id = varMountId;
  -- Return all candidates (potentially an empty cursor). Don't commit now, this will be done
  -- in C++ after the results have been collected as the temporary table will be emptied. 
  OPEN outFiles FOR
    SELECT fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fseq, fileSize
      FROM FilesToMigrateHelper;
END;
/

/* Wrapper procedure for the setOrReplaceSegmentsForFiles call in the NS DB. Because we can't
 * pass arrays, and temporary tables are forbidden with distributed transactions, we use standard
 * tables on the Stager DB (while the NS table is still temporary) to pass the data
 * and we wrap everything in an autonomous transaction to isolate the caller.
 */
CREATE OR REPLACE PROCEDURE ns_setOrReplaceSegments(inReqId IN VARCHAR2,
                                                    outNSIsOnlyLogs OUT "numList",
                                                    outNSTimeInfos OUT floatList,
                                                    outNSErrorCodes OUT "numList",
                                                    outNSMsgs OUT strListTable,
                                                    outNSFileIds OUT "numList",
                                                    outNSParams OUT strListTable) AS
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  -- "Bulk" transfer data to the NS DB
  INSERT /*+ APPEND */ INTO SetSegsForFilesInputHelper@RemoteNS
    (reqId, fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
     vid, fseq, blockId, checksumType, checksum) (
    SELECT reqId, fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
           vid, fseq, blockId, checksumType, checksum
      FROM FileMigrationResultsHelper
     WHERE reqId = inReqId);
  DELETE FROM FileMigrationResultsHelper
   WHERE reqId = inReqId;
  COMMIT;  -- commit the remote insertion, otherwise a ORA-01002 (fetch out of sequence) may happen
  -- This call autocommits all successful segments in the NameServer, and reports
  -- successes and errors as entries in the SetSegsForFilesResultsHelper table
  setOrReplaceSegmentsForFiles@RemoteNS(inReqId);
  -- Retrieve results from the NS DB in bulk and clean data
  SELECT isOnlyLog, timeinfo, errorCode, msg, fileId, params
    BULK COLLECT INTO outNSIsOnlyLogs, outNSTimeInfos, outNSErrorCodes, outNSMsgs, outNSFileIds, outNSParams
    FROM SetSegsForFilesResultsHelper@RemoteNS
   WHERE reqId = inReqId;
  DELETE FROM SetSegsForFilesResultsHelper@RemoteNS
   WHERE reqId = inReqId;
  -- this commits the remote deletion
  COMMIT;
END;
/

/* Commit a set of succeeded/failed migration processes to the NS and stager db.
 * Locks are taken on the involved castorfiles one by one, then to the dependent entities.
 */
CREATE OR REPLACE PROCEDURE tg_setBulkFileMigrationResult(inLogContext IN VARCHAR2,
                                                          inMountTrId IN NUMBER,
                                                          inFileIds IN "numList",
                                                          inFileTrIds IN "numList",
                                                          inFseqs IN "numList",
                                                          inBlockIds IN strListTable,
                                                          inChecksumTypes IN strListTable,
                                                          inChecksums IN "numList",
                                                          inComprSizes IN "numList",
                                                          inTransferredSizes IN "numList",
                                                          inErrorCodes IN "numList",
                                                          inErrorMsgs IN strListTable
                                                          ) AS
  varStartTime TIMESTAMP;
  varNsHost VARCHAR2(2048);
  varReqId VARCHAR2(36);
  varNsOpenTime NUMBER;
  varCopyNo NUMBER;
  varOldCopyNo NUMBER;
  varVid VARCHAR2(10);
  varNSIsOnlyLogs "numList";
  varNSTimeInfos floatList;
  varNSErrorCodes "numList";
  varNSMsgs strListTable;
  varNSFileIds "numList";
  varNSParams strListTable;
  varParams VARCHAR2(4000);
  varNbSentToNS INTEGER := 0;
BEGIN
  varStartTime := SYSTIMESTAMP;
  varReqId := uuidGen();
  -- Get the NS host name
  varNsHost := getConfigOption('stager', 'nsHost', '');
  FOR i IN inFileTrIds.FIRST .. inFileTrIds.LAST LOOP
    BEGIN
      -- Collect additional data. Note that this is NOT bulk
      -- to preserve the order in the input arrays.
      SELECT CF.nsOpenTime, nvl(MJ.originalCopyNb, 0), MJ.vid, MJ.destCopyNb
        INTO varNsOpenTime, varOldCopyNo, varVid, varCopyNo
        FROM CastorFile CF, MigrationJob MJ
       WHERE MJ.castorFile = CF.id
         AND CF.fileid = inFileIds(i)
         AND MJ.mountTransactionId = inMountTrId
         AND MJ.fileTransactionId = inFileTrIds(i)
         AND status = tconst.MIGRATIONJOB_SELECTED;
        -- Store in a temporary table, to be transfered to the NS DB.
        -- Note that this is an ON COMMIT DELETE table and we never take locks or commit until
        -- after the NS call: if anything goes bad (including the db link being broken) we bail out
        -- without needing to rollback.
      IF inErrorCodes(i) = 0 THEN
        -- Successful migration
        INSERT INTO FileMigrationResultsHelper
          (reqId, fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
           vid, fseq, blockId, checksumType, checksum)
        VALUES (varReqId, inFileIds(i), varNsOpenTime, varCopyNo, varOldCopyNo,
                inTransferredSizes(i), CASE inComprSizes(i) WHEN 0 THEN 1 ELSE inComprSizes(i) END, varVid, inFseqs(i),
                strtoRaw4(inBlockIds(i)), inChecksumTypes(i), inChecksums(i));
        varNbSentToNS := varNbSentToNS + 1;
      ELSE
        -- Fail/retry this migration, log 'migration failed, will retry if allowed'
        varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' ErrorCode='|| to_char(inErrorCodes(i))
                     ||' ErrorMessage="'|| inErrorMsgs(i) ||'" VID='|| varVid
                     ||' copyNb='|| to_char(varCopyNo) ||' '|| inLogContext;
        logToDLF(varReqid, dlf.LVL_WARNING, dlf.MIGRATION_RETRY, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
        retryOrFailMigration(inMountTrId, inFileIds(i), varNsHost, inErrorCodes(i), varReqId);
        -- here we commit immediately because retryOrFailMigration took a lock on the CastorFile
        COMMIT;
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Log 'unable to identify migration, giving up'
      varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' fileTransactionId='|| to_char(inFileTrIds(i))
        ||' '|| inLogContext;
      logToDLF(varReqid, dlf.LVL_ERROR, dlf.MIGRATION_NOT_FOUND, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
    END;
  END LOOP;
  -- Commit all the entries in FileMigrationResultsHelper so that the next call can take them
  COMMIT;

  DECLARE
    varUnused INTEGER;
  BEGIN
    -- boundary case: if nothing to do, just skip the remote call and the
    -- subsequent FOR loop as it would be useless (and would fail).
    SELECT 1 INTO varUnused FROM FileMigrationResultsHelper
     WHERE reqId = varReqId AND ROWNUM < 2;
    -- The following procedure wraps the remote calls in an autonomous transaction
    ns_setOrReplaceSegments(varReqId, varNSIsOnlyLogs, varNSTimeInfos, varNSErrorCodes, varNSMsgs, varNSFileIds, varNSParams);

    -- Process the results
    FOR i IN 1 .. varNSFileIds.COUNT LOOP
      -- First log on behalf of the NS
      -- We classify the log level based on the error code here.
      -- Current error codes are:
      --   ENOENT, EACCES, EBUSY, EEXIST, EISDIR, EINVAL, SEINTERNAL, SECHECKSUM, ENSFILECHG, ENSNOSEG
      --   ENSTOOMANYSEGS, ENSOVERWHENREP, ERTWRONGSIZE, ESTNOSEGFOUND
      -- default level is ERROR. Some cases can be demoted to warning when it's a normal case
      -- (like file deleted by user in the mean time).
      logToDLFWithTime(varNSTimeinfos(i), varReqid,
                       CASE varNSErrorCodes(i) 
                         WHEN 0                 THEN dlf.LVL_SYSTEM
                         WHEN serrno.ENOENT     THEN dlf.LVL_WARNING
                         WHEN serrno.ENSFILECHG THEN dlf.LVL_WARNING
                         ELSE                        dlf.LVL_ERROR
                       END,
                       varNSMsgs(i), varNSFileIds(i), varNsHost, 'nsd', varNSParams(i));
      -- Now skip pure log entries and process file by file, depending on the result
      IF varNSIsOnlyLogs(i) = 1 THEN CONTINUE; END IF;
      CASE
      WHEN varNSErrorCodes(i) = 0 THEN
        -- All right, commit the migration in the stager
        tg_setFileMigrated(inMountTrId, varNSFileIds(i), varReqId, inLogContext);

      WHEN varNSErrorCodes(i) = serrno.ENOENT
        OR varNSErrorCodes(i) = serrno.ENSNOSEG
        OR varNSErrorCodes(i) = serrno.ENSFILECHG
        OR varNSErrorCodes(i) = serrno.ENSTOOMANYSEGS THEN
        -- The migration was useless because either the file is gone, or it has been modified elsewhere,
        -- or there were already enough copies on tape for it. Fail and update disk cache accordingly.
        failFileMigration(inMountTrId, varNSFileIds(i), varNSErrorCodes(i), varReqId);

      ELSE
        -- Attempt to retry for all other NS errors. To be reviewed whether some of the NS errors are to be considered fatal.
        varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' '|| varNSParams(i) ||' '|| inLogContext;
        logToDLF(varReqid, dlf.LVL_WARNING, dlf.MIGRATION_RETRY, varNSFileIds(i), varNsHost, 'tapegatewayd', varParams);
        retryOrFailMigration(inMountTrId, varNSFileIds(i), varNsHost, varNSErrorCodes(i), varReqId);
      END CASE;
      -- Commit file by file
      COMMIT;
    END LOOP;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Nothing to do after processing the error cases
    NULL;
  END;
  -- Final log, "setBulkFileMigrationResult: bulk migration completed"
  varParams := 'mountTransactionId='|| to_char(inMountTrId)
               ||' NbInputFiles='|| inFileIds.COUNT
               ||' NbSentToNS='|| varNbSentToNS
               ||' NbFilesBackFromNS='|| varNSFileIds.COUNT
               ||' '|| inLogContext
               ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
               ||' AvgProcessingTime='|| trunc(getSecs(varStartTime, SYSTIMESTAMP)/inFileIds.COUNT, 6);
  logToDLF(varReqid, dlf.LVL_SYSTEM, dlf.BULK_MIGRATION_COMPLETED, 0, '', 'tapegatewayd', varParams);
END;
/

/* Commit a successful file migration */
CREATE OR REPLACE PROCEDURE tg_setFileMigrated(inMountTrId IN NUMBER, inFileId IN NUMBER,
                                               inReqId IN VARCHAR2, inLogContext IN VARCHAR2) AS
  varNsHost VARCHAR2(2048);
  varCfId NUMBER;
  varCopyNb INTEGER;
  varVID VARCHAR2(10);
  varOrigVID VARCHAR2(10);
  varMigJobCount INTEGER;
  varParams VARCHAR2(4000);
  varMigStartTime NUMBER;
  varSrId INTEGER;
BEGIN
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Lock the CastorFile
  SELECT CF.id INTO varCfId FROM CastorFile CF
   WHERE CF.fileid = inFileId 
     AND CF.nsHost = varNsHost
     FOR UPDATE;
  -- delete the corresponding migration job, create the new migrated segment
  DELETE FROM MigrationJob
   WHERE castorFile = varCfId
     AND mountTransactionId = inMountTrId
  RETURNING destCopyNb, VID, originalVID, creationTime
    INTO varCopyNb, varVID, varOrigVID, varMigStartTime;
  -- check if another migration should be performed
  SELECT count(*) INTO varMigJobCount
    FROM MigrationJob
   WHERE castorFile = varCfId;
  IF varMigJobCount = 0 THEN
    -- no more migrations, delete all migrated segments 
    DELETE FROM MigratedSegment
     WHERE castorFile = varCfId;
    -- And mark CastorFile as ONTAPE
    UPDATE CastorFile
       SET tapeStatus= dconst.CASTORFILE_ONTAPE
     WHERE id = varCfId;
  ELSE
    -- another migration ongoing, keep track of the one just completed
    INSERT INTO MigratedSegment (castorFile, copyNb, vid)
    VALUES (varCfId, varCopyNb, varVID);
  END IF;
  -- Do we have to deal with a repack ?
  IF varOrigVID IS NOT NULL THEN
    -- Yes we do, then archive the repack subrequest associated
    -- Note that there may be several if we are dealing with old bad tapes
    -- that have 2 copies of the same file on them. Thus we take one at random
    SELECT /*+ INDEX(SR I_Subrequest_CastorFile) */ SR.id INTO varSrId
      FROM SubRequest SR, StageRepackRequest Req
      WHERE SR.castorfile = varCfId
        AND SR.status = dconst.SUBREQUEST_REPACK
        AND SR.request = Req.id
        AND Req.RepackVID = varOrigVID
        AND ROWNUM < 2;
    archiveSubReq(varSrId, dconst.SUBREQUEST_FINISHED);
  END IF;
  -- Log 'db updates after full migration completed'
  varParams := 'TPVID='|| varVID ||' mountTransactionId='|| to_char(inMountTrId) ||
    ' migrationTime=' || to_char(trunc(getTime() - varMigStartTime, 0)) || ' '|| inLogContext;
  logToDLF(inReqid, dlf.LVL_SYSTEM, dlf.MIGRATION_COMPLETED, inFileId, varNsHost, 'tapegatewayd', varParams);
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Log 'file not found, giving up'
  varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' '|| inLogContext;
  logToDLF(inReqid, dlf.LVL_ERROR, dlf.MIGRATION_NOT_FOUND, inFileId, varNsHost, 'tapegatewayd', varParams);
END;
/


/* Fail a file migration, potentially archiving outstanding repack requests */
CREATE OR REPLACE PROCEDURE failFileMigration(inMountTrId IN NUMBER, inFileId IN NUMBER,
                                              inErrorCode IN INTEGER, inReqId IN VARCHAR2) AS
  varNsHost VARCHAR2(2048);
  varCfId NUMBER;
  varNsOpenTime NUMBER;
  varSrIds "numList";
  varOriginalCopyNb NUMBER;
  varMigJobCount NUMBER;
  varErrorCode INTEGER := inErrorCode;
BEGIN
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Lock castor file
  SELECT id, nsOpenTime INTO varCfId, varNsOpenTime
    FROM CastorFile WHERE fileId = inFileId FOR UPDATE;
  -- delete migration job
  DELETE FROM MigrationJob
   WHERE castorFile = varCFId AND mountTransactionId = inMountTrId
  RETURNING originalCopyNb INTO varOriginalCopyNb;
  -- check if another migration should be performed
  SELECT count(*) INTO varMigJobCount
    FROM MigrationJob
   WHERE castorfile = varCfId;
  IF varMigJobCount = 0 THEN
     -- no other migration, delete all migrated segments
     DELETE FROM MigratedSegment
      WHERE castorfile = varCfId;
  END IF;
  -- terminate repack subrequests
  IF varOriginalCopyNb IS NOT NULL THEN
    archiveOrFailRepackSubreq(varCfId, inErrorCode);
    -- set back the CastorFile to ONTAPE otherwise repack will wait forever
    UPDATE CastorFile
       SET tapeStatus = dconst.CASTORFILE_ONTAPE
     WHERE id = varCfId;
  END IF;
  
  IF varErrorCode = serrno.ENOENT THEN
    -- unfortunately, tape servers can throw this error too (see SR #136759), so we have to double check
    -- prior to taking destructive actions on the file: if the file does exist in the Nameserver, then
    -- replace the error code to a generic ETSYS (taped system error), otherwise keep ENOENT
    BEGIN
      SELECT 1902 INTO varErrorCode FROM Dual
       WHERE EXISTS (SELECT 1 FROM Cns_file_metadata@RemoteNS WHERE fileid = inFileId);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;
    END;
  END IF;
  -- Log depending on the error: some are not pathological and have dedicated handling
  IF varErrorCode = serrno.ENOENT OR varErrorCode = serrno.ENSFILECHG OR varErrorCode = serrno.ENSNOSEG THEN
    -- in this case, disk cache is stale
    UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID
     WHERE status = dconst.DISKCOPY_VALID
       AND castorFile = varCfId;
    -- cleanup other migration jobs for that file if any
    DELETE FROM MigrationJob WHERE castorfile = varCfId;
    -- Log 'file was dropped or modified during migration, giving up'
    logToDLF(inReqid, dlf.LVL_NOTICE, dlf.MIGRATION_FILE_DROPPED, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || inMountTrId || ' ErrorCode=' || varErrorCode ||
             ' NsOpenTimeAtStager=' || trunc(varNsOpenTime, 6));
  ELSIF varErrorCode = serrno.ENSTOOMANYSEGS THEN
    -- do as if migration was successful
    UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_ONTAPE WHERE id = varCfId;
    -- Log 'file already had enough copies on tape, ignoring new segment'
    logToDLF(inReqid, dlf.LVL_NOTICE, dlf.MIGRATION_SUPERFLUOUS_COPY, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || inMountTrId);
  ELSE
    -- Any other case, log 'migration to tape failed for this file, giving up'
    logToDLF(inReqid, dlf.LVL_ERROR, dlf.MIGRATION_FAILED, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || inMountTrId || ' LastErrorCode=' || varErrorCode);
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- File was dropped, log 'file not found when failing migration'
  logToDLF(inReqid, dlf.LVL_ERROR, dlf.MIGRATION_FAILED_NOT_FOUND, inFileId, varNsHost, 'tapegatewayd',
           'mountTransactionId=' || inMountTrId || ' LastErrorCode=' || varErrorCode);
END;
/


/*** Disk-Tape interface, Recall ***/

/* Get next candidates for a given recall mount.
 * input:  VDQM transaction id, count and total size
 * output: outFiles, a cursor for the set of recall candidates.
 */
create or replace 
PROCEDURE tg_getBulkFilesToRecall(inLogContext IN VARCHAR2,
                                                    inMountTrId IN NUMBER,
                                                    inCount IN INTEGER,
                                                    inTotalSize IN INTEGER,
                                                    outFiles OUT SYS_REFCURSOR) AS
  varVid VARCHAR2(10);
  varPreviousFseq INTEGER;
  varCount INTEGER;
  varTotalSize INTEGER;
  varPath VARCHAR2(2048);
  varFileTrId INTEGER;
  varNewFseq INTEGER;
  bestFSForRecall_error EXCEPTION;
  PRAGMA EXCEPTION_INIT(bestFSForRecall_error, -20115);
BEGIN
  BEGIN
    -- Get VID and last processed fseq for this recall mount, lock
    SELECT vid, lastProcessedFseq INTO varVid, varPreviousFseq
      FROM RecallMount
     WHERE mountTransactionId = inMountTrId
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- recall is over or unknown request: return an empty cursor
    OPEN outFiles FOR
      SELECT fileId, nsHost, fileTransactionId, filePath, blockId, fseq, copyNb,
             euid, egid, VID, fileSize, creationTime, nbRetriesInMount, nbMounts
        FROM FilesToRecallHelper;
    RETURN;
  END;
  varCount := 0;
  varTotalSize := 0;
  varNewFseq := varPreviousFseq;
  -- Get candidates up to inCount or inTotalSize
  -- Select only the ones further down the tape (fseq > current one) as long as possible
  LOOP
    DECLARE
      varRjId INTEGER;
      varFSeq INTEGER;
      varBlockId RAW(4);
      varFileSize INTEGER;
      varCfId INTEGER;
      varFileId INTEGER;
      varNsHost VARCHAR2(2048);
      varCopyNb NUMBER;
      varEuid NUMBER;
      varEgid NUMBER;
      varCreationTime NUMBER;
      varNbRetriesInMount NUMBER;
      varNbMounts NUMBER;
      CfLocked EXCEPTION;
      PRAGMA EXCEPTION_INIT (CfLocked, -54);
    BEGIN
      -- Find the unprocessed recallJobs of this tape with lowest fSeq
      -- that is above the previous one
      SELECT * INTO varRjId, varFSeq, varBlockId, varFileSize, varCfId, varCopyNb,
                    varEuid, varEgid, varCreationTime, varNbRetriesInMount,
                    varNbMounts
        FROM (SELECT id, fSeq, blockId, fileSize, castorFile, copyNb, eUid, eGid,
                     creationTime, nbRetriesWithinMount,  nbMounts
                FROM RecallJob
               WHERE vid = varVid
                 AND status = tconst.RECALLJOB_PENDING
                 AND fseq > varNewFseq
               ORDER BY fseq ASC)
       WHERE ROWNUM < 2;
      -- lock the corresponding CastorFile, give up if we do not manage as it means that
      -- this file is already being handled by someone else
      -- Note that the giving up is handled by the handling of the CfLocked exception
      SELECT fileId, nsHost INTO varFileId, varNsHost
        FROM CastorFile
       WHERE id = varCfId
         FOR UPDATE NOWAIT;
      -- Now that we have the lock, double check that the RecallJob is still there and
      -- valid (due to race condition, it may have been processed in between our first select
      -- and the takin gof the lock)
      BEGIN
        SELECT id INTO varRjId FROM RecallJob WHERE id = varRJId AND status = tconst.RECALLJOB_PENDING;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- we got the race condition ! So this has already been handled, let's move to next file
        CONTINUE;
      END;
      -- move up last fseq used. Note that it moves up even if bestFileSystemForRecall
      -- (or any other statement) fails and the file is actually not recalled.
      -- The goal is that a potential retry within the same mount only occurs after
      -- we went around the other files on this tape.
      varNewFseq := varFseq;
      -- Find the best filesystem to recall the selected file
      bestFileSystemForRecall(varCfId, varPath);
      varCount := varCount + 1;
      varTotalSize := varTotalSize + varFileSize;
      INSERT INTO FilesToRecallHelper (fileId, nsHost, fileTransactionId, filePath, blockId, fSeq,
                 copyNb, euid, egid, VID, fileSize, creationTime, nbRetriesInMount, nbMounts)
        VALUES (varFileId, varNsHost, ids_seq.nextval, varPath, varBlockId, varFSeq,
                varCopyNb, varEuid, varEgid, varVID, varFileSize, varCreationTime, varNbRetriesInMount,
                varNbMounts)
        RETURNING fileTransactionId INTO varFileTrId;
      -- update RecallJobs of this file. Only the recalled one gets a fileTransactionId
      UPDATE RecallJob
         SET status = tconst.RECALLJOB_SELECTED,
             fileTransactionID = CASE WHEN id = varRjId THEN varFileTrId ELSE NULL END
       WHERE castorFile = varCfId;
      IF varCount >= inCount OR varTotalSize >= inTotalSize THEN
        -- we have enough candidates for this round, exit loop
        EXIT;
      END IF;
    EXCEPTION
      WHEN CfLocked THEN
        -- Go to next candidate, this CastorFile is being processed by another thread
        NULL;
      WHEN bestFSForRecall_error THEN
        -- log 'bestFileSystemForRecall could not find a suitable destination for this recall' and skip it
        logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_FS_NOT_FOUND, varFileId, varNsHost, 'tapegatewayd',
                 'errorMessage="' || SQLERRM || '" mountTransactionId=' || to_char(inMountTrId) ||' '|| inLogContext);
        -- mark the recall job as failed, and maybe retry
        retryOrFailRecall(varCfId, varVID, NULL, inLogContext);
      WHEN NO_DATA_FOUND THEN
        -- nothing found. In case we did not try so far, try to restart with low fseqs
        IF varNewFseq > -1 THEN
          varNewFseq := -1;
        ELSE
          -- low fseqs were tried, we are really out of candidates, so exit the loop
          EXIT;
        END IF;
    END;
  END LOOP;
  -- Record last fseq at the mount level
  UPDATE RecallMount
     SET lastProcessedFseq = varNewFseq
   WHERE vid = varVid;
  -- Return all candidates. Don't commit now, this will be done in C++
  -- after the results have been collected as the temporary table will be emptied.
  OPEN outFiles FOR
    SELECT fileId, nsHost, fileTransactionId, filePath, blockId, fseq,
           copyNb, euid, egid, VID, fileSize, creationTime,
           nbRetriesInMount, nbMounts
      FROM FilesToRecallHelper;
END;
/


/* Commit a set of succeeded/failed recall processes to the NS and stager db.
 * Locks are taken on the involved castorfiles one by one, then to the dependent entities.
 */
CREATE OR REPLACE PROCEDURE tg_setBulkFileRecallResult(inLogContext IN VARCHAR2,
                                                       inMountTrId IN NUMBER,
                                                       inFileIds IN "numList",
                                                       inFileTrIds IN "numList",
                                                       inFilePaths IN strListTable,
                                                       inFseqs IN "numList",
                                                       inChecksumNames IN strListTable,
                                                       inChecksums IN "numList",
                                                       inFileSizes IN "numList",
                                                       inErrorCodes IN "numList",
                                                       inErrorMsgs IN strListTable) AS
  varCfId NUMBER;
  varVID VARCHAR2(10);
  varReqId VARCHAR2(36);
  varStartTime TIMESTAMP;
  varNsHost VARCHAR2(2048);
  varParams VARCHAR2(4000);
BEGIN
  varStartTime := SYSTIMESTAMP;
  varReqId := uuidGen();
  -- Get the NS host name
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Get the current VID
  SELECT VID INTO varVID
    FROM RecallMount
   WHERE mountTransactionId = inMountTrId;
  -- Loop over the input
  FOR i IN inFileIds.FIRST .. inFileIds.LAST LOOP
    BEGIN
      -- Find and lock related castorFile
      SELECT id INTO varCfId
        FROM CastorFile
       WHERE fileid = inFileIds(i)
         AND nsHost = varNsHost
         FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- log "setBulkFileRecallResult : unable to identify Recall. giving up"
      logToDLF(varReqId, dlf.LVL_ERROR, dlf.RECALL_NOT_FOUND, inFileIds(i), varNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTrId) || ' TPVID=' || varVID ||
               ' fseq=' || TO_CHAR(inFseqs(i)) || ' filePath=' || inFilePaths(i) || ' ' || inLogContext);
      CONTINUE;
    END;
    -- Now deal with each recall one by one
    IF inErrorCodes(i) = 0 THEN
      -- Recall successful, check NS and update stager + log
      tg_setFileRecalled(inMountTrId, inFseqs(i), inFilePaths(i), inChecksumNames(i), inChecksums(i),
                         varReqId, inLogContext);
    ELSE
      -- Recall failed at tapeserver level, attempt to retry it
      -- log "setBulkFileRecallResult : recall process failed, will retry if allowed"
      logToDLF(varReqId, dlf.LVL_WARNING, dlf.RECALL_FAILED, inFileIds(i), varNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTrId) || ' TPVID=' || varVID ||
               ' fseq=' || TO_CHAR(inFseqs(i)) || ' errorMessage="' || inErrorMsgs(i) ||'" '|| inLogContext);
      retryOrFailRecall(varCfId, varVID, varReqId, inLogContext);
    END IF;
    COMMIT;
  END LOOP;
  -- log "setBulkFileRecallResult: bulk recall completed"
  varParams := 'mountTransactionId='|| to_char(inMountTrId)
               ||' NbFiles='|| inFileIds.COUNT ||' '|| inLogContext
               ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
               ||' AvgProcessingTime='|| trunc(getSecs(varStartTime, SYSTIMESTAMP)/inFileIds.COUNT, 6);
  logToDLF(varReqid, dlf.LVL_SYSTEM, dlf.BULK_RECALL_COMPLETED, 0, '', 'tapegatewayd', varParams);
END;
/


/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('MIGRATIONMOUNTSJOB', 'RECALLMOUNTSJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every minute executing the startMigrationMounts procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'MigrationMountsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN startMigrationMounts(); END;'', ''tapegatewayd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Creates MigrationMount entries when new migrations should start');

  -- Create a db job to be run every minute executing the startRecallMounts procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'RecallMountsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN startRecallMounts(); END;'', ''tapegatewayd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Creates RecallMount entries when new recalls should start');
END;
/
/*******************************************************************
 *
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
  FUNCTION sizeRelatedUserWeight(fileSize NUMBER) RETURN NUMBER;
  FUNCTION sizeRelatedRecallWeight(fileSize NUMBER) RETURN NUMBER;
  FUNCTION sizeRelatedCopyWeight(fileSize NUMBER) RETURN NUMBER;
  FUNCTION dayBonusFirstAccessHook(oldGcWeight NUMBER, creationTime NUMBER) RETURN NUMBER;
  FUNCTION halfHourBonusAccessHook(oldGcWeight NUMBER, creationTime NUMBER, nbAccesses NUMBER) RETURN NUMBER;
  FUNCTION cappedUserSetGCWeight(oldGcWeight NUMBER, userDelta NUMBER) RETURN NUMBER;
  -- FIFO gc policy
  FUNCTION creationTimeUserWeight(fileSize NUMBER) RETURN NUMBER;
  FUNCTION creationTimeRecallWeight(fileSize NUMBER) RETURN NUMBER;
  FUNCTION creationTimeCopyWeight(fileSize NUMBER) RETURN NUMBER;
  -- LRU gc policy
  FUNCTION LRUFirstAccessHook(oldGcWeight NUMBER, creationTime NUMBER) RETURN NUMBER;
  FUNCTION LRUAccessHook(oldGcWeight NUMBER, creationTime NUMBER, nbAccesses NUMBER) RETURN NUMBER;
  FUNCTION LRUpinUserSetGCWeight(oldGcWeight NUMBER, userDelta NUMBER) RETURN NUMBER;
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

  FUNCTION sizeRelatedUserWeight(fileSize NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN size2GCWeight(fileSize);
  END;

  FUNCTION sizeRelatedRecallWeight(fileSize NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN size2GCWeight(fileSize);
  END;

  FUNCTION sizeRelatedCopyWeight(fileSize NUMBER) RETURN NUMBER AS
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
  FUNCTION creationTimeUserWeight(fileSize NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN getTime();
  END;

  FUNCTION creationTimeRecallWeight(fileSize NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN getTime();
  END;

  FUNCTION creationTimeCopyWeight(fileSize NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN getTime();
  END;

  -- LRU and LRUpin gc policy
  FUNCTION LRUFirstAccessHook(oldGcWeight NUMBER, creationTime NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN getTime();
  END;

  FUNCTION LRUAccessHook(oldGcWeight NUMBER, creationTime NUMBER, nbAccesses NUMBER) RETURN NUMBER AS
  BEGIN
    RETURN getTime();
  END;

  FUNCTION LRUpinUserSetGCWeight(oldGcWeight NUMBER, userDelta NUMBER) RETURN NUMBER AS
  BEGIN
    IF userDelta >= 2592000 THEN -- 30 days max
      RETURN oldGcWeight + 2592000;
    ELSE
      RETURN oldGcWeight + userDelta;
    END IF;
  END;

END castorGC;
/

/* PL/SQL method implementing selectFiles2Delete
   This is the standard garbage collector: it sorts VALID diskcopies
   that do not need to go to tape by gcWeight and selects them for deletion up to
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
    -- say this is Disk1, we don't GC files.
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
       AND decode(status, 9, status, NULL) = 9;  -- BEINGDELETED (decode used to use function-based index)

    -- estimate the number of GC running the "long" query, that is the one dealing with the GCing of
    -- VALID files.
    SELECT COUNT(*) INTO backoff
      FROM v$session s, v$sqltext t
     WHERE s.sql_id = t.sql_id AND t.sql_text LIKE '%I_DiskCopy_FS_GCW%';

    -- Process diskcopies that are in an INVALID state.
    UPDATE /*+ INDEX(DiskCopy I_DiskCopy_Status_7_FS)) */ DiskCopy
       SET status = 9, -- BEINGDELETED
           gcType = decode(gcType, NULL, dconst.GCTYPE_USER, gcType)
     WHERE fileSystem = fs.id
       AND decode(status, 7, status, NULL) = 7  -- INVALID (decode used to use function-based index)
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

    -- Continue processing but with VALID files, only in case we are not already loaded
    IF dontGC = 0 AND backoff < 4 THEN
      -- Do not delete VALID files from non production hardware
      BEGIN
        SELECT FileSystem.id INTO unused
          FROM DiskServer, FileSystem
         WHERE FileSystem.id = fs.id
           AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
           AND FileSystem.diskserver = DiskServer.id
           AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
           AND DiskServer.hwOnline = 1;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        EXIT;
      END;
      -- Calculate the amount of space that would be freed on the filesystem
      -- if the files selected above were to be deleted.
      IF dcIds.COUNT > 0 THEN
        SELECT /*+ INDEX(DiskCopy PK_DiskCopy_Id) */ freed + sum(diskCopySize) INTO freed
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
      -- removing VALID files until we are below the free space watermark
      IF freed < toBeFreed THEN
        -- Loop on file deletions
        FOR dc IN (SELECT /*+ INDEX(DiskCopy I_DiskCopy_FS_GCW) */ DiskCopy.id, castorFile
                     FROM DiskCopy, CastorFile
                    WHERE fileSystem = fs.id
                      AND status = dconst.DISKCOPY_VALID
                      AND CastorFile.id = DiskCopy.castorFile
                      AND CastorFile.tapeStatus IN (dconst.CASTORFILE_DISKONLY, dconst.CASTORFILE_ONTAPE)
                      ORDER BY gcWeight ASC) LOOP
          BEGIN
            -- Lock the CastorFile
            SELECT id INTO unused FROM CastorFile
             WHERE id = dc.castorFile FOR UPDATE NOWAIT;
            -- Mark the DiskCopy as being deleted
            UPDATE DiskCopy
               SET status = dconst.DISKCOPY_BEINGDELETED,
                   gcType = dconst.GCTYPE_AUTO
             WHERE id = dc.id RETURNING diskCopySize INTO deltaFree;
            totalCount := totalCount + 1;
            -- Update freed space
            freed := freed + deltaFree;
            -- update importance of remianing copies of the file if any
            UPDATE DiskCopy
               SET importance = importance + 1
             WHERE castorFile = dc.castorFile
               AND status = dconst.DISKCOPY_VALID;
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
           getObjStatusName('DiskCopy', 'gcType', DiskCopy.gcType),
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
  fid NUMBER;
  fc NUMBER;
  nsh VARCHAR2(2048);
  nb INTEGER;
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- List the castorfiles to be cleaned up afterwards
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      INSERT INTO FilesDeletedProcHelper (cfId) (
        SELECT castorFile FROM DiskCopy
         WHERE id = dcIds(i));
    -- Loop over the deleted files; first use FORALL for bulk operation
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      DELETE FROM DiskCopy WHERE id = dcIds(i);
    -- Then use a normal loop to clean castorFiles. Note: We order the list to
    -- prevent a deadlock
    FOR cf IN (SELECT DISTINCT(cfId)
                 FROM filesDeletedProcHelper
                ORDER BY cfId ASC) LOOP
      BEGIN
        -- Get data and lock the castorFile
        SELECT fileId, nsHost, fileClass
          INTO fid, nsh, fc
          FROM CastorFile
         WHERE id = cf.cfId FOR UPDATE;
        -- Cleanup:
        -- See whether it has any other DiskCopy or any new Recall request:
        -- if so, skip the rest
        SELECT count(*) INTO nb FROM DiskCopy
         WHERE castorFile = cf.cfId;
        IF nb > 0 THEN
          CONTINUE;
        END IF;
        SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
          FROM SubRequest
         WHERE castorFile = cf.cfId
           AND status = dconst.SUBREQUEST_WAITTAPERECALL;
        IF nb > 0 THEN
          CONTINUE;
        END IF;
        -- Nothing found, check for any other subrequests
        SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
          FROM SubRequest
         WHERE castorFile = cf.cfId
           AND status IN (1, 2, 3, 4, 5, 6, 7, 10, 12, 13);  -- all but START, FINISHED, FAILED_FINISHED, ARCHIVED
        IF nb = 0 THEN
          -- Nothing left, delete the CastorFile
          DELETE FROM CastorFile WHERE id = cf.cfId;
        ELSE
          -- Fail existing subrequests for this file
          UPDATE /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest
             SET status = dconst.SUBREQUEST_FAILED
           WHERE castorFile = cf.cfId
             AND status IN (1, 2, 3, 4, 5, 6, 12, 13);  -- same as above
        END IF;
        -- Check whether this file potentially had copies on tape
        SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
        IF nb = 0 THEN
          -- This castorfile was created with no copy on tape
          -- So removing it from the stager means erasing
          -- it completely. We should thus also remove it
          -- from the name server
          INSERT INTO FilesDeletedProcOutput (fileId, nsHost) VALUES (fid, nsh);
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
      stageForcedRm(fileIds(fid), nsHostName, dconst.GCTYPE_NSSYNCH);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- this file was dropped from nameServer AND stager
      -- and still exists on disk. We put it into the list
      -- of orphan fileids to return
      INSERT INTO NsFilesDeletedOrphans (fileId) VALUES (fileIds(fid));
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
   INSERT INTO StgFilesDeletedOrphans (diskCopyId) VALUES (dcIds(i));
  -- Return a list of diskcopy ids which no longer exist
  OPEN stgOrphans FOR
    SELECT diskCopyId FROM StgFilesDeletedOrphans
     WHERE NOT EXISTS (
        SELECT /*+ INDEX(DiskCopy PK_DiskCopy_Id */ 'x' FROM DiskCopy
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
      FORALL i IN 1 .. ids.COUNT
        DELETE FROM '||tab||' WHERE id = ids(i);
      CLOSE s;
      COMMIT;
    END LOOP;
  END;';
END;
/

/* A generic method to delete requests of a given type */
CREATE OR REPLACE Procedure bulkDeleteRequests(reqType IN VARCHAR, timeOut IN INTEGER) AS
BEGIN
  bulkDelete('SELECT id FROM '|| reqType ||' R WHERE
    NOT EXISTS (SELECT 1 FROM SubRequest WHERE request = R.id) AND lastModificationTime < getTime() - '
    || timeOut ||';',
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
  IF rate > 0 AND (1000000 / rate * 1800) < timeOut THEN
    timeOut := 1000000 / rate * 1800;  -- keep 1M requests max
  END IF;
  
  -- delete castorFiles if nothing is left for them. Here we use
  -- a temporary table as we need to commit every ~1000 operations
  -- and keeping a cursor opened on the original select may take
  -- too long, leading to ORA-01555 'snapshot too old' errors.
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DeleteTermReqHelper';
  INSERT /*+ APPEND */ INTO DeleteTermReqHelper (srId, cfId)
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
      FORALL i IN 1 .. ids.COUNT
        DELETE FROM SubRequest WHERE id = ids(i);
      COMMIT;
    END LOOP;
    CLOSE s;
  END;
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DeleteTermReqHelper';

  -- And then related Requests, now orphaned.
  -- The timeout makes sure we keep very recent requests,
  -- even if they have no subrequests. This may actually
  -- be the case only for Repack requests, as they're
  -- created empty and filled after querying the NS.
    ---- Get ----
  bulkDeleteRequests('StageGetRequest', timeOut);
    ---- Put ----
  bulkDeleteRequests('StagePutRequest', timeOut);
    ---- Update ----
  bulkDeleteRequests('StageUpdateRequest', timeOut);
    ---- PrepareToGet -----
  bulkDeleteRequests('StagePrepareToGetRequest', timeOut);
    ---- PrepareToPut ----
  bulkDeleteRequests('StagePrepareToPutRequest', timeOut);
    ---- PrepareToUpdate ----
  bulkDeleteRequests('StagePrepareToUpdateRequest', timeOut);
    ---- PutDone ----
  bulkDeleteRequests('StagePutDoneRequest', timeOut);
    ---- Rm ----
  bulkDeleteRequests('StageRmRequest', timeOut);
    ---- Repack ----
  bulkDeleteRequests('StageRepackRequest', timeOut);
    ---- SetGCWeight ----
  bulkDeleteRequests('SetFileGCWeight', timeOut);
END;
/

/* Search and delete old diskCopies in bad states */
CREATE OR REPLACE PROCEDURE deleteFailedDiskCopies(timeOut IN NUMBER) AS
  dcIds "numList";
  cfIds "numList";
BEGIN
  LOOP
    -- select INVALID diskcopies without filesystem (they can exist after a
    -- stageRm that came before the diskcopy had been created on disk) and ALL FAILED
    -- ones (coming from failed recalls or failed removals from the GC daemon).
    -- Note that we don't select INVALID diskcopies from recreation of files
    -- because they are taken by the standard GC as they physically exist on disk.
    -- go only for 1000 at a time and retry if the limit was reached
    SELECT id
      BULK COLLECT INTO dcIds
      FROM DiskCopy
     WHERE (status = 4 OR (status = 7 AND fileSystem = 0))
       AND creationTime < getTime() - timeOut
       AND ROWNUM <= 1000;
    SELECT /*+ INDEX(DC PK_DiskCopy_ID) */ UNIQUE castorFile
      BULK COLLECT INTO cfIds
      FROM DiskCopy DC
     WHERE id IN (SELECT /*+ CARDINALITY(ids 5) */ * FROM TABLE(dcIds) ids);
    -- drop the DiskCopies
    FORALL i IN 1 .. dcIds.COUNT
      DELETE FROM DiskCopy WHERE id = dcIds(i);
    COMMIT;
    -- maybe delete the CastorFiles if nothing is left for them
    FOR i IN 1 .. cfIds.COUNT LOOP
      deleteCastorFile(cfIds(i));
    END LOOP;
    COMMIT;
    -- exit if we did less than 1000
    IF dcIds.COUNT < 1000 THEN EXIT; END IF;
  END LOOP;
END;
/

/* Deal with old diskCopies in STAGEOUT */
CREATE OR REPLACE PROCEDURE deleteOutOfDateStageOutDCs(timeOut IN NUMBER) AS
  srId NUMBER;
BEGIN
  -- Deal with old DiskCopies in STAGEOUT/WAITFS. The rule is to drop
  -- the ones with 0 fileSize and issue a putDone for the others
  FOR f IN (SELECT /*+ USE_NL(D C S) LEADING(D C S) INDEX(D I_DiskCopy_Status_Open) INDEX(S I_SubRequest_CastorFile) */
                   C.filesize, C.id, C.fileId, C.nsHost, D.fileSystem, D.id AS dcId, D.status AS dcStatus
              FROM DiskCopy D, Castorfile C
             WHERE C.id = D.castorFile
               AND D.creationTime < getTime() - timeOut
               AND decode(D.status,6,D.status,decode(D.status,5,D.status,decode(D.status,11,D.status,NULL))) IS NOT NULL
               AND NOT EXISTS (
                 SELECT 'x'
                   FROM SubRequest
                  WHERE castorFile = C.id
                    AND status IN (0, 1, 2, 3, 5, 6, 13) -- all active
                    AND reqType NOT IN (37, 38))) LOOP -- ignore PrepareToPut, PrepareToUpdate
    IF (0 = f.fileSize) OR (f.dcStatus <> 6) THEN  -- DISKCOPY_STAGEOUT
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
      logToDLF(NULL, dlf.LVL_WARNING, dlf.FILE_DROPPED_BY_CLEANING, f.fileId, f.nsHost, 'stagerd', '');
    ELSE
      -- here we issue a putDone
      -- context 2 : real putDone. Missing PPut requests are ignored.
      -- svcClass 0 since we don't know it. This will trigger a
      -- default behavior in the putDoneFunc
      putDoneFunc(f.id, f.fileSize, 2, 0);
      logToDLF(NULL, dlf.LVL_WARNING, dlf.PUTDONE_ENFORCED_BY_CLEANING, f.fileId, f.nsHost, 'stagerd', '');
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

/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('HOUSEKEEPINGJOB',
                                'CLEANUPJOB',
                                'BULKCHECKFSBACKINPRODJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every 20 minutes executing the deleteTerminatedRequests procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'houseKeepingJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN deleteTerminatedRequests(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=20',
      ENABLED         => TRUE,
      COMMENTS        => 'Cleaning of terminated requests');

  -- Create a db job to be run twice a day executing the cleanup procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'cleanupJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN cleanup(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=HOURLY; INTERVAL=12',
      ENABLED         => TRUE,
      COMMENTS        => 'Database maintenance');

  -- Create a db job to be run every 5 minutes executing the bulkCheckFSBackInProd procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'bulkCheckFSBackInProdJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN bulkCheckFSBackInProd(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'Bulk operation to processing filesystem state changes');
END;
/

/*******************************************************************
 * PL/SQL code for Draining FileSystems Logic
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* deletes a DrainingJob and related objects */
CREATE OR REPLACE PROCEDURE deleteDrainingJob(inDjId IN INTEGER) AS
  varUnused INTEGER;
BEGIN
  -- take a lock on the drainingJob
  SELECT id INTO varUnused FROM DrainingJob WHERE id = inDjId FOR UPDATE;
  -- drop ongoing Disk2DiskCopyJobs
  DELETE FROM Disk2DiskCopyJob WHERE drainingJob = inDjId;
  -- delete associated errors
  DELETE FROM DrainingErrors WHERE drainingJob = inDjId;
  -- finally delete the DrainingJob
  DELETE FROM DrainingJob WHERE id = inDjId;
END;
/

/* handle the creation of the Disk2DiskCopyJobs for the running drainingJobs */
CREATE OR REPLACE PROCEDURE drainRunner AS
  varNbFiles INTEGER := 0;
  varNbBytes INTEGER := 0;
  varNbRunningJobs INTEGER;
  varMaxNbOfSchedD2dPerDrain INTEGER;
BEGIN
  -- get maxNbOfSchedD2dPerDrain
  varMaxNbOfSchedD2dPerDrain := TO_NUMBER(getConfigOption('Draining', 'MaxNbSchedD2dPerDrain', '1000'));
  -- loop over draining jobs
  FOR dj IN (SELECT id, fileSystem, svcClass, fileMask, euid, egid
               FROM DrainingJob WHERE status = dconst.DRAININGJOB_RUNNING) LOOP
    -- check how many disk2DiskCopyJobs are already running for this draining job
    SELECT count(*) INTO varNbRunningJobs FROM Disk2DiskCopyJob WHERE drainingJob = dj.id;
    -- Loop over the creation of Disk2DiskCopyJobs. Select max 1000 files, taking running
    -- ones into account. Also Take the most important jobs first
    FOR F IN (SELECT * FROM
               (SELECT CastorFile.id cfId, Castorfile.nsOpenTime, DiskCopy.id dcId, CastorFile.fileSize
                  FROM DiskCopy, CastorFile
                 WHERE DiskCopy.fileSystem = dj.fileSystem
                   AND CastorFile.id = DiskCopy.castorFile
                   AND ((dj.fileMask = dconst.DRAIN_FILEMASK_NOTONTAPE AND
                         CastorFile.tapeStatus IN (dconst.CASTORFILE_NOTONTAPE, dconst.CASTORFILE_DISKONLY)) OR
                        (dj.fileMask = dconst.DRAIN_FILEMASK_ALL))
                   AND DiskCopy.status = dconst.DISKCOPY_VALID
                   AND NOT EXISTS (SELECT 1 FROM Disk2DiskCopyJob WHERE castorFile=CastorFile.id)
                 ORDER BY DiskCopy.importance DESC)
               WHERE ROWNUM <= varMaxNbOfSchedD2dPerDrain-varNbRunningJobs) LOOP
      createDisk2DiskCopyJob(F.cfId, F.nsOpenTime, dj.svcClass, dj.euid, dj.egid,
                             dconst.REPLICATIONTYPE_DRAINING, F.dcId, dj.id);
      varNbFiles := varNbFiles + 1;
      varNbBytes := varNbBytes + F.fileSize;
    END LOOP;
    -- commit and update counters
    UPDATE DrainingJob
       SET totalFiles = totalFiles + varNbFiles,
           totalBytes = totalBytes + varNbBytes,
           lastModificationTime = getTime()
     WHERE id = dj.id;
    COMMIT;
  END LOOP;
END;
/

/* Procedure responsible for managing the draining process
 * note the locking that makes sure we are not running twice in parallel
 * as this will be restarted regularly by an Oracle job, but may take long to conclude
 */
CREATE OR REPLACE PROCEDURE drainManager AS
  varTFiles INTEGER;
  varTBytes INTEGER;
BEGIN
  -- Delete the COMPLETED jobs older than 7 days
  DELETE FROM DrainingJob
   WHERE status = dconst.DRAININGJOB_FINISHED
     AND lastModificationTime < getTime() - (7 * 86400);
  COMMIT;

  -- Start new DrainingJobs if needed
  FOR dj IN (SELECT id, fileSystem FROM DrainingJob WHERE status = dconst.DRAININGJOB_SUBMITTED) LOOP
    UPDATE DrainingJob SET status = dconst.DRAININGJOB_STARTING WHERE id = dj.id;
    COMMIT;
    SELECT count(*), SUM(diskCopySize) INTO varTFiles, varTBytes
      FROM DiskCopy
     WHERE fileSystem = dj.fileSystem
       AND status = dconst.DISKCOPY_VALID;
    UPDATE DrainingJob
       SET totalFiles=varTFiles, totalBytes=varTBytes, status = dconst.DRAININGJOB_RUNNING
     WHERE id = dj.id;
    COMMIT;
  END LOOP;
END;
/

/* Procedure responsible for rebalancing one given filesystem by moving away
 * the given amount of data */
CREATE OR REPLACE PROCEDURE rebalance(inFsId IN INTEGER, inDataAmount IN INTEGER,
                                      inDestSvcClassId IN INTEGER,
                                      inDiskServerName IN VARCHAR2, inMountPoint IN VARCHAR2) AS
  CURSOR DCcur IS
    SELECT /*+ FIRST_ROWS_10 */
           DiskCopy.id, DiskCopy.diskCopySize, DiskCopy.owneruid, DiskCopy.ownergid,
           CastorFile.id, CastorFile.nsOpenTime
      FROM DiskCopy, CastorFile
     WHERE DiskCopy.fileSystem = inFsId
       AND DiskCopy.status = dconst.DISKCOPY_VALID
       AND CastorFile.id = DiskCopy.castorFile;
  varDcId INTEGER;
  varDcSize INTEGER;
  varEuid INTEGER;
  varEgid INTEGER;
  varCfId INTEGER;
  varNsOpenTime INTEGER;
  varTotalRebalanced INTEGER := 0;
  varNbFilesRebalanced INTEGER := 0;
BEGIN
  -- disk to disk copy files out of this node until we reach inDataAmount
  -- "rebalancing : starting" message
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REBALANCING_START, 0, '', 'stagerd',
           'DiskServer=' || inDiskServerName || ' mountPoint=' || inMountPoint ||
           ' dataTomove=' || TO_CHAR(inDataAmount));
  -- Loop on candidates until we can lock one
  OPEN DCcur;
  LOOP
    -- Fetch next candidate
    FETCH DCcur INTO varDcId, varDcSize, varEuid, varEgid, varCfId, varNsOpenTime;
    varTotalRebalanced := varTotalRebalanced + varDcSize;
    varNbFilesRebalanced := varNbFilesRebalanced + 1;
    -- stop if it would be too much
    IF varTotalRebalanced > inDataAmount THEN RETURN; END IF;
    -- create disk2DiskCopyJob for this diskCopy
    createDisk2DiskCopyJob(varCfId, varNsOpenTime, inDestSvcClassId,
                           varEuid, varEgid, dconst.REPLICATIONTYPE_REBALANCE,
                           varDcId, NULL);
  END LOOP;
  -- "rebalancing : stopping" message
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REBALANCING_STOP, 0, '', 'stagerd',
           'DiskServer=' || inDiskServerName || ' mountPoint=' || inMountPoint ||
           ' dataMoved=' || TO_CHAR(varTotalRebalanced) ||
           ' nbFilesMoved=' || TO_CHAR(varNbFilesRebalanced));
END;
/

/* Procedure responsible for rebalancing of data on nodes within diskpools */
CREATE OR REPLACE PROCEDURE rebalancingManager AS
  varFreeRef NUMBER;
  varSensibility NUMBER;
  varAlreadyRebalancing INTEGER;
BEGIN
  -- go through all service classes
  FOR SC IN (SELECT id FROM SvcClass) LOOP
    -- check if we are already rebalancing
    SELECT count(*) INTO varAlreadyRebalancing
      FROM Disk2DiskCopyJob
     WHERE destSvcClass = SC.id
       AND replicationType = dconst.REPLICATIONTYPE_REBALANCE
       AND ROWNUM < 2;
    -- if yes, do nothing for this round
    IF varAlreadyRebalancing > 0 THEN
      CONTINUE;
    END IF;
    -- compute average filling of filesystems on production machines
    -- note that read only ones are not taken into account as they cannot
    -- be filled anymore
    SELECT AVG(free/totalSize) INTO varFreeRef
      FROM FileSystem, DiskPool2SvcClass, DiskServer
     WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
       AND DiskPool2SvcClass.child = SC.id
       AND DiskServer.id = FileSystem.diskServer
       AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
       AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
       AND DiskServer.hwOnline = 1
     GROUP BY SC.id;
    -- get sensibility of the rebalancing
    varSensibility := TO_NUMBER(getConfigOption('Rebalancing', 'Sensibility', '5'))/100;
    -- for each filesystem too full compared to average, rebalance
    -- note that we take the read only ones into account here
    FOR FS IN (SELECT FileSystem.id, varFreeRef*totalSize-free dataToMove,
                      DiskServer.name ds, FileSystem.mountPoint
                 FROM FileSystem, DiskPool2SvcClass, DiskServer
                WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
                  AND DiskPool2SvcClass.child = SC.id
                  AND varFreeRef - free/totalSize > varSensibility
                  AND DiskServer.id = FileSystem.diskServer
                  AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
                  AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
                  AND DiskServer.hwOnline = 1) LOOP
      rebalance(FS.id, FS.dataToMove, SC.id, FS.ds, FS.mountPoint);
    END LOOP;
  END LOOP;
END;
/

/* SQL statement for DBMS_SCHEDULER job creation */
BEGIN
  -- Remove jobs related to the draining logic before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('DRAINMANAGERJOB', 'DRAINRUNNERJOB', 'REBALANCINGJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;


  -- Create the drain manager job to be executed every minute. This one starts and clean up draining jobs
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'drainManagerJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN drainManager(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Database job to manage the draining process');

  -- Create the drain runner job to be executed every minute. This one checks whether new
  -- disk2diskCopies need to be created for a given draining job
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'drainRunnerJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN drainRunner(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Database job to manage the draining process');

  -- Create the drain manager job to be executed every minute
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'rebalancingJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN rebalancingManager(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 5/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Database job to manage rebalancing of data on nodes within diskpools');
END;
/
/*******************************************************************
 *
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
    Status NUMBER,
    username VARCHAR2(2048),
    machine VARCHAR2(2048),
    svcClassName VARCHAR2(2048),
    ReqId NUMBER,
    ReqType VARCHAR2(20));
  TYPE RequestDebug IS TABLE OF RequestDebug_typ;
  TYPE RecallJobDebug_typ IS RECORD (
    id INTEGER,
    copyNb INTEGER,
    recallGroup VARCHAR(2048),
    svcClass VARCHAR(2048),
    euid INTEGER,
    egid INTEGER,
    vid VARCHAR(2048),
    fseq INTEGER,
    status INTEGER,
    creationTime NUMBER,
    nbRetriesWithinMount INTEGER,
    nbMounts INTEGER);
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
EXCEPTION WHEN NO_DATA_FOUND THEN -- nothing found
  RAISE_APPLICATION_ERROR (-20000, 'Could not find any CastorFile, SubRequest, DiskCopy, MigrationJob or RecallJob with id = ' || ref);
END; END; END; END; END;
/

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


/* Get the recalljobs associated with the reference number */
CREATE OR REPLACE FUNCTION getRJs(ref number) RETURN castorDebug.RecallJobDebug PIPELINED AS
BEGIN
  FOR t IN (SELECT RecallJob.id, RecallJob.copyNb, RecallGroup.name as recallGroupName,
                   SvcClass.name as svcClassName, RecallJob.euid, RecallJob.egid, RecallJob.vid,
                   RecallJob.fseq, RecallJob.status, RecallJob.creationTime,
                   RecallJob.nbRetriesWithinMount, RecallJob.nbMounts
              FROM RecallJob, RecallGroup, SvcClass
             WHERE RecallJob.castorfile = getCF(ref)
               AND RecallJob.recallGroup = RecallGroup.id
               AND RecallJob.svcClass = SvcClass.id) LOOP
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
                   SubRequest.id AS SubReqId, SubRequest.Status,
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
                     SELECT /*+ INDEX(SetFileGCWeight PK_SetFileGCWeight_Id) */ id, username, machine, svcClassName, 'SetGCW' AS type FROM SetFileGCWeight) Request
             WHERE castorfile = getCF(ref)
               AND Request.id = SubRequest.request) LOOP
     PIPE ROW(d);
  END LOOP;
END;
/
/*******************************************************************
 * PL/SQL code for stager monitoring
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* CastorMon Package Specification */
CREATE OR REPLACE PACKAGE CastorMon AS
  -- Backlog statistics for migrations, grouped by TapePool
  PROCEDURE waitTapeMigrationStats;
  -- Backlog statistics for recalls, grouped by SvcClass
  PROCEDURE waitTapeRecallStats;
END CastorMon;
/


/* CastorMon Package Body */
CREATE OR REPLACE PACKAGE BODY CastorMon AS

  /**
   * PL/SQL method implementing waitTapeMigrationStats
   * See the castorMon package specification for documentation.
   */
  PROCEDURE waitTapeMigrationStats AS
    CURSOR rec IS
      SELECT tapePool tapePool,
             nvl(sum(fileSize), 0) totalFileSize, 
             count(fileSize) nbFiles
      FROM (
        SELECT
               TapePool.name tapePool,
               MigrationJob.filesize fileSize
        FROM MigrationJob, TapePool
        WHERE TapePool.id = MigrationJob.tapePool(+) -- Left outer join to have zeroes when there is no migrations
      )
      GROUP BY tapePool;
  BEGIN
    FOR r in rec LOOP
      logToDLF(NULL, dlf.LVL_SYSTEM, 'waitTapeMigrationStats', 0, '', 'stagerd', 
            'TapePool="' || r.tapePool ||
            '" totalFileSize="' || r.totalFileSize || 
            '" nbFiles="' || r.nbFiles || '"');
    END LOOP;
  END waitTapeMigrationStats;

  /**
   * PL/SQL method implementing waitTapeRecallStats
   * See the castorMon package specification for documentation.
   */
  PROCEDURE waitTapeRecallStats AS
    CURSOR rec IS
      SELECT svcClass svcClass,
             nvl(sum(fileSize), 0) totalFileSize,
             count(fileSize) nbFiles
      FROM (
        SELECT
               SvcClass.name svcClass,
               RecallJob.filesize fileSize
        FROM RecallJob, SvcClass
        WHERE SvcClass.id = RecallJob.SvcClass(+) -- Left outer join to have zeroes when there is no recall
      )
      GROUP BY svcClass;
  BEGIN
    FOR r in rec LOOP
      logToDLF(NULL, dlf.LVL_SYSTEM, 'waitTapeRecallStats', 0, '', 'stagerd', 
            'SvcClass="' || r.svcClass ||
            '" totalFileSize="' || r.totalFileSize || 
            '" nbFiles="' || r.nbFiles || '"');
    END LOOP;
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
      JOB_NAME        => 'monitoringJob_1min',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN CastorMon.waitTapeMigrationStats(); CastorMon.waitTapeRecallStats(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/3600,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Generation of monitoring information about migrations and recalls backlog');
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
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpJobSvc', '');
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
    INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, status, request, getNextStatus, reqType)
    VALUES (0, srFileNames(i), srProtocols(i), srXsizes(i), 0, NULL, srFlags(i), srModeBits(i), creationTime, creationTime, 0, 0, '', NULL, svcHandler, subreqId, NULL, NULL, dconst.SUBREQUEST_START, reqId, 0, inReqType);
    -- send an alert to accelerate the processing of the request
    CASE
    WHEN inReqType = 35 OR   -- StageGetRequest
         inReqType = 40 OR   -- StagePutRequest
         inReqType = 44 THEN -- StageUpdateRequest
      DBMS_ALERT.SIGNAL('wakeUpJobReqSvc', '');
    WHEN inReqType = 36 OR   -- StagePrepareToGetRequest
         inReqType = 37 OR   -- StagePrepareToPutRequest
         inReqType = 38 THEN -- StagePrepareToUpdateRequest
      DBMS_ALERT.SIGNAL('wakeUpPrepReqSvc', '');
    WHEN inReqType = 42 OR   -- StageRmRequest
         inReqType = 39 OR   -- StagePutDoneRequest
         inReqType = 95 THEN -- SetFileGCWeight
      DBMS_ALERT.SIGNAL('wakeUpStageReqSvc', '');
    END CASE;
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
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpJobSvc', '');
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
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpQueryReqSvc', '');
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
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpQueryReqSvc', '');
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
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpQueryReqSvc', '');
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
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpJobSvc', '');
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
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpGCSvc', '');
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
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpQueryReqSvc', '');
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
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpBulkStageReqSvc', '');
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
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpGCSvc', '');
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
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpQueryReqSvc', '');
END;
/

/* Flag the schema creation as COMPLETE */
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE';
COMMIT;
