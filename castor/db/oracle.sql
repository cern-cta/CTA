/* This file contains SQL code that is not generated automatically */
/* and is inserted at the beginning of the generated code           */

/* SQL statements for object types */
DROP INDEX I_RH_ID2TYPE_FULL;
DROP TABLE RH_ID2TYPE;
CREATE TABLE RH_ID2TYPE (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_RH_ID2TYPE_FULL on RH_ID2TYPE (id, type);

/* SQL statements for indices */
DROP INDEX I_RH_INDICES_FULL;
DROP TABLE RH_INDICES;
CREATE TABLE RH_INDICES (name CHAR(8), value NUMBER);
CREATE INDEX I_RH_INDICES_FULL on RH_INDICES (name, value);
INSERT INTO RH_INDICES (name, value) VALUES ('next_id', 1);

/* SQL statements for requests status */
DROP INDEX I_RH_REQUESTSSTATUS_FULL;
DROP TABLE RH_REQUESTSSTATUS;
CREATE TABLE RH_REQUESTSSTATUS (id INTEGER PRIMARY KEY, status CHAR(8), creation DATE, lastChange DATE);
CREATE INDEX I_RH_REQUESTSSTATUS_FULL on RH_REQUESTSSTATUS (id, status);

/* PL/SQL procedure for getting the next request to handle */
CREATE OR REPLACE PROCEDURE getNRStatement(reqid OUT INTEGER) AS
BEGIN
  SELECT ID INTO reqid FROM rh_requestsStatus WHERE status = 'NEW' AND rownum <=1;
  UPDATE rh_requestsStatus SET status = 'RUNNING', lastChange = SYSDATE WHERE ID = reqid;
END;

/* SQL statements for type Client */
DROP TABLE rh_Client;
CREATE TABLE rh_Client (ipAddress NUMBER, port NUMBER, id INTEGER PRIMARY KEY, request INTEGER);

/* SQL statements for type StageInRequest */
DROP TABLE rh_StageInRequest;
CREATE TABLE rh_StageInRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), openflags NUMBER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageQryRequest */
DROP TABLE rh_StageQryRequest;
CREATE TABLE rh_StageQryRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageOutRequest */
DROP TABLE rh_StageOutRequest;
CREATE TABLE rh_StageOutRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), openmode NUMBER, id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageClrRequest */
DROP TABLE rh_StageClrRequest;
CREATE TABLE rh_StageClrRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageFilChgRequest */
DROP TABLE rh_StageFilChgRequest;
CREATE TABLE rh_StageFilChgRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type ReqId */
DROP TABLE rh_ReqId;
CREATE TABLE rh_ReqId (value VARCHAR(255), id INTEGER PRIMARY KEY, request INTEGER);

/* SQL statements for type SubRequest */
DROP TABLE rh_SubRequest;
CREATE TABLE rh_SubRequest (retryCounter NUMBER, fileName VARCHAR(255), protocol VARCHAR(255), poolName VARCHAR(255), xsize INTEGER, priority NUMBER, id INTEGER PRIMARY KEY, diskcopy INTEGER, castorFile INTEGER, parent INTEGER, request INTEGER, status INTEGER);

/* SQL statements for type StageUpdcRequest */
DROP TABLE rh_StageUpdcRequest;
CREATE TABLE rh_StageUpdcRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type Tape */
DROP TABLE rh_Tape;
CREATE TABLE rh_Tape (vid VARCHAR(255), side NUMBER, tpmode NUMBER, errMsgTxt VARCHAR(255), errorCode NUMBER, severity NUMBER, vwAddress VARCHAR(255), id INTEGER PRIMARY KEY, stream INTEGER, status INTEGER);

/* SQL statements for type Segment */
DROP TABLE rh_Segment;
CREATE TABLE rh_Segment (blockid CHAR(4), fseq NUMBER, offset INTEGER, bytes_in INTEGER, bytes_out INTEGER, host_bytes INTEGER, segmCksumAlgorithm VARCHAR(255), segmCksum NUMBER, errMsgTxt VARCHAR(255), errorCode NUMBER, severity NUMBER, id INTEGER PRIMARY KEY, tape INTEGER, copy INTEGER, status INTEGER);

/* SQL statements for type TapePool */
DROP TABLE rh_TapePool;
CREATE TABLE rh_TapePool (name VARCHAR(255), id INTEGER PRIMARY KEY);

/* SQL statements for type TapeCopy */
DROP TABLE rh_TapeCopy;
CREATE TABLE rh_TapeCopy (id INTEGER PRIMARY KEY, castorFile INTEGER, status INTEGER);

/* SQL statements for type CastorFile */
DROP TABLE rh_CastorFile;
CREATE TABLE rh_CastorFile (fileId INTEGER, nsHost VARCHAR(255), fileSize INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, fileClass INTEGER);

/* SQL statements for type DiskCopy */
DROP TABLE rh_DiskCopy;
CREATE TABLE rh_DiskCopy (path VARCHAR(255), id INTEGER PRIMARY KEY, fileSystem INTEGER, castorFile INTEGER, status INTEGER);

/* SQL statements for type FileSystem */
DROP TABLE rh_FileSystem;
CREATE TABLE rh_FileSystem (free INTEGER, weight float, fsDeviation float, mountPoint VARCHAR(255), id INTEGER PRIMARY KEY, diskPool INTEGER, diskserver INTEGER);

/* SQL statements for type SvcClass */
DROP TABLE rh_SvcClass;
CREATE TABLE rh_SvcClass (policy VARCHAR(255), nbDrives NUMBER, name VARCHAR(255), id INTEGER PRIMARY KEY);
DROP TABLE rh_SvcClass2TapePool;
CREATE TABLE rh_SvcClass2TapePool (Parent INTEGER, Child INTEGER);

/* SQL statements for type DiskPool */
DROP TABLE rh_DiskPool;
CREATE TABLE rh_DiskPool (name VARCHAR(255), id INTEGER PRIMARY KEY);
DROP TABLE rh_DiskPool2SvcClass;
CREATE TABLE rh_DiskPool2SvcClass (Parent INTEGER, Child INTEGER);

/* SQL statements for type Stream */
DROP TABLE rh_Stream;
CREATE TABLE rh_Stream (initialSizeToTransfer INTEGER, id INTEGER PRIMARY KEY, tape INTEGER, tapePool INTEGER, status INTEGER);
DROP TABLE rh_Stream2TapeCopy;
CREATE TABLE rh_Stream2TapeCopy (Parent INTEGER, Child INTEGER);

/* SQL statements for type FileClass */
DROP TABLE rh_FileClass;
CREATE TABLE rh_FileClass (name VARCHAR(255), minFileSize NUMBER, maxFileSize NUMBER, nbCopies NUMBER, id INTEGER PRIMARY KEY);

/* SQL statements for type DiskServer */
DROP TABLE rh_DiskServer;
CREATE TABLE rh_DiskServer (name VARCHAR(255), id INTEGER PRIMARY KEY, status INTEGER);

/* This file contains SQL code that is not generated automatically */
/* and is inserted at the end of the generated code           */

/* Indexes realted to CastorFiles */
drop index rh_DiskCopyCastorfileIndex;
drop index rh_TapeCopyCastorfileIndex;
drop index rh_SubRequestCastorfileIndex;
create index rh_DiskCopyCastorfileIndex on rh_DiskCopy (castorFile);
create index rh_TapeCopyCastorfileIndex on rh_TapeCopy (castorFile);
create index rh_SubRequestCastorfileIndex on rh_SubRequest (castorFile);

/* PL/SQL method implementing bestTapeCopyForStream */
CREATE OR REPLACE PROCEDURE bestTapeCopyForStream(streamId IN INTEGER, tapeCopyStatus IN NUMBER,
                                                  newTapeCopyStatus IN NUMBER, newStreamStatus IN NUMBER,
                                                  diskServerName OUT VARCHAR, mountPoint OUT VARCHAR,
                                                  path OUT VARCHAR, diskCopyId OUT INTEGER,
                                                  castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                  nsHost OUT VARCHAR, fileSize OUT INTEGER,
                                                  tapeCopyId OUT INTEGER) AS
BEGIN
 SELECT rh_DiskServer.name, rh_FileSystem.mountPoint, rh_DiskCopy.path, rh_DiskCopy.id,
   rh_CastorFile.id, rh_CastorFile.fileId, rh_CastorFile.nsHost, rh_CastorFile.fileSize, rh_TapeCopy.id
  INTO diskServerName, mountPoint, path, diskCopyId, castorFileId, fileId, nsHost, fileSize, tapeCopyId
  FROM rh_DiskServer, rh_FileSystem, rh_DiskCopy, rh_CastorFile, rh_TapeCopy, rh_Stream2TapeCopy
  WHERE rh_DiskServer.id = rh_FileSystem.diskserver
    AND rh_FileSystem.id = rh_DiskCopy.filesystem
    AND rh_DiskCopy.castorfile = rh_CastorFile.id
    AND rh_TapeCopy.castorfile = rh_Castorfile.id
    AND rh_Stream2TapeCopy.parent = rh_TapeCopy.id
    AND rh_Stream2TapeCopy.child = streamId
    AND rh_TapeCopy.status = tapeCopyStatus
    AND ROWNUM < 2
  ORDER by rh_FileSystem.weight DESC;
 UPDATE rh_TapeCopy SET status = newTapeCopyStatus WHERE id = tapeCopyId;
 UPDATE rh_Stream SET status = newStreamStatus WHERE id = streamId;
END;

/* PL/SQL method implementing bestFileSystemForSegment */
CREATE OR REPLACE PROCEDURE bestFileSystemForSegment(segmentId IN INTEGER, diskServerName OUT VARCHAR,
                                                     mountPoint OUT VARCHAR, path OUT VARCHAR,
                                                     diskCopyId OUT INTEGER) AS
 fileSystemId NUMBER;
BEGIN
SELECT rh_DiskServer.name, rh_FileSystem.mountPoint, rh_DiskCopy.path, rh_DiskCopy.id
 INTO diskServerName, mountPoint, path, diskCopyId
 FROM rh_DiskServer, rh_FileSystem, rh_DiskPool2SvcClass, rh_StageInRequest,
      rh_SubRequest, rh_TapeCopy, rh_Segment, rh_DiskCopy, rh_CastorFile
  WHERE rh_Segment.id = segmentId
    AND rh_Segment.copy = rh_TapeCopy.id
    AND rh_SubRequest.castorfile = rh_TapeCopy.castorfile
    AND rh_DiskCopy.castorfile = rh_TapeCopy.castorfile
    AND rh_Castorfile.id = rh_TapeCopy.castorfile
    AND rh_StageInRequest.id = rh_SubRequest.request
    AND rh_StageInRequest.svcclass = rh_DiskPool2SvcClass.child
    AND rh_FileSystem.diskpool = rh_DiskPool2SvcClass.parent
    AND rh_FileSystem.free > rh_CastorFile.fileSize
    AND ROWNUM < 2
  ORDER by rh_FileSystem.weight DESC;
UPDATE rh_DiskCopy SET fileSystem = fileSystemId WHERE id = diskCopyId;
END;
