/* This file contains SQL code that is not generated automatically */
/* and is inserted at the beginning of the generated code           */

/* SQL statements for object types */
DROP INDEX I_ID2TYPE_FULL;
DROP TABLE ID2TYPE;
CREATE TABLE ID2TYPE (id INTEGER PRIMARY KEY, type NUMBER);
CREATE INDEX I_ID2TYPE_FULL on ID2TYPE (id, type);

/* SQL statements for indices */
DROP INDEX I_INDICES_FULL;
DROP TABLE INDICES;
CREATE TABLE INDICES (name CHAR(8), value NUMBER);
CREATE INDEX I_INDICES_FULL on INDICES (name, value);
INSERT INTO INDICES (name, value) VALUES ('next_id', 1);

/* SQL statements for requests status */
DROP INDEX I_REQUESTSSTATUS_FULL;
DROP TABLE REQUESTSSTATUS;
CREATE TABLE REQUESTSSTATUS (id INTEGER PRIMARY KEY, status CHAR(8), creation DATE, lastChange DATE);
CREATE INDEX I_REQUESTSSTATUS_FULL on REQUESTSSTATUS (id, status);

/* PL/SQL procedure for id retrieval */
CREATE OR REPLACE PROCEDURE getId(nb IN INTEGER, id OUT INTEGER) AS
BEGIN
 UPDATE indices
  SET value = value + nb
  WHERE name='next_id'
  RETURNING value INTO id;
END;

/* PL/SQL procedure for getting the next request to handle */
CREATE OR REPLACE PROCEDURE getNRStatement(reqid OUT INTEGER) AS
BEGIN
  SELECT ID INTO reqid FROM requestsStatus WHERE status = 'NEW' AND rownum <=1;
  UPDATE requestsStatus SET status = 'RUNNING', lastChange = SYSDATE WHERE ID = reqid;
END;

/* SQL statements for type Client */
DROP TABLE Client;
CREATE TABLE Client (ipAddress NUMBER, port NUMBER, id INTEGER PRIMARY KEY, request INTEGER);

/* SQL statements for type StagePrepareToGetRequest */
DROP TABLE StagePrepareToGetRequest;
CREATE TABLE StagePrepareToGetRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StagePrepareToPutRequest */
DROP TABLE StagePrepareToPutRequest;
CREATE TABLE StagePrepareToPutRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StagePrepareToUpdateRequest */
DROP TABLE StagePrepareToUpdateRequest;
CREATE TABLE StagePrepareToUpdateRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageGetRequest */
DROP TABLE StageGetRequest;
CREATE TABLE StageGetRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StagePutRequest */
DROP TABLE StagePutRequest;
CREATE TABLE StagePutRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageUpdateRequest */
DROP TABLE StageUpdateRequest;
CREATE TABLE StageUpdateRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageRmRequest */
DROP TABLE StageRmRequest;
CREATE TABLE StageRmRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StagePutDoneRequest */
DROP TABLE StagePutDoneRequest;
CREATE TABLE StagePutDoneRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageUpdateFileStatusRequest */
DROP TABLE StageUpdateFileStatusRequest;
CREATE TABLE StageUpdateFileStatusRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageFileQueryRequest */
DROP TABLE StageFileQueryRequest;
CREATE TABLE StageFileQueryRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageRequestQueryRequest */
DROP TABLE StageRequestQueryRequest;
CREATE TABLE StageRequestQueryRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageFindRequestRequest */
DROP TABLE StageFindRequestRequest;
CREATE TABLE StageFindRequestRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, svcClass INTEGER, client INTEGER);

/* SQL statements for type SubRequest */
DROP TABLE SubRequest;
CREATE TABLE SubRequest (retryCounter NUMBER, fileName VARCHAR(255), protocol VARCHAR(255), poolName VARCHAR(255), xsize INTEGER, priority NUMBER, subreqId VARCHAR(255), id INTEGER PRIMARY KEY, diskcopy INTEGER, castorFile INTEGER, parent INTEGER, status INTEGER, request INTEGER);

/* SQL statements for type StageGetNextRequest */
DROP TABLE StageGetNextRequest;
CREATE TABLE StageGetNextRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER);

/* SQL statements for type StagePutNextRequest */
DROP TABLE StagePutNextRequest;
CREATE TABLE StagePutNextRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER);

/* SQL statements for type StageUpdateNextRequest */
DROP TABLE StageUpdateNextRequest;
CREATE TABLE StageUpdateNextRequest (flags INTEGER, userName VARCHAR(255), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR(255), svcClassName VARCHAR(255), userTag VARCHAR(255), reqId VARCHAR(255), id INTEGER PRIMARY KEY, parent INTEGER, svcClass INTEGER, client INTEGER);

/* SQL statements for type Tape */
DROP TABLE Tape;
CREATE TABLE Tape (vid VARCHAR(255), side NUMBER, tpmode NUMBER, errMsgTxt VARCHAR(255), errorCode NUMBER, severity NUMBER, vwAddress VARCHAR(255), id INTEGER PRIMARY KEY, stream INTEGER, status INTEGER);

/* SQL statements for type Segment */
DROP TABLE Segment;
CREATE TABLE Segment (blockid CHAR(4), fseq NUMBER, offset INTEGER, bytes_in INTEGER, bytes_out INTEGER, host_bytes INTEGER, segmCksumAlgorithm VARCHAR(255), segmCksum NUMBER, errMsgTxt VARCHAR(255), errorCode NUMBER, severity NUMBER, id INTEGER PRIMARY KEY, tape INTEGER, copy INTEGER, status INTEGER);

/* SQL statements for type TapePool */
DROP TABLE TapePool;
CREATE TABLE TapePool (name VARCHAR(255), id INTEGER PRIMARY KEY);

/* SQL statements for type TapeCopy */
DROP TABLE TapeCopy;
CREATE TABLE TapeCopy (copyNb NUMBER, id INTEGER PRIMARY KEY, castorFile INTEGER, status INTEGER);

/* SQL statements for type CastorFile */
DROP TABLE CastorFile;
CREATE TABLE CastorFile (fileId INTEGER, nsHost VARCHAR(255), fileSize INTEGER, id INTEGER PRIMARY KEY, svcClass INTEGER, fileClass INTEGER);

/* SQL statements for type DiskCopy */
DROP TABLE DiskCopy;
CREATE TABLE DiskCopy (path VARCHAR(255), diskcopyId VARCHAR(255), id INTEGER PRIMARY KEY, fileSystem INTEGER, castorFile INTEGER, status INTEGER);

/* SQL statements for type FileSystem */
DROP TABLE FileSystem;
CREATE TABLE FileSystem (free INTEGER, weight float, fsDeviation float, mountPoint VARCHAR(255), id INTEGER PRIMARY KEY, diskPool INTEGER, diskserver INTEGER);

/* SQL statements for type SvcClass */
DROP TABLE SvcClass;
CREATE TABLE SvcClass (policy VARCHAR(255), nbDrives NUMBER, name VARCHAR(255), id INTEGER PRIMARY KEY);
DROP INDEX I_SvcClass2TapePool_Child;
DROP INDEX I_SvcClass2TapePool_Parent;
DROP TABLE SvcClass2TapePool;
CREATE TABLE SvcClass2TapePool (Parent INTEGER, Child INTEGER);
CREATE INDEX I_SvcClass2TapePool_Child on SvcClass2TapePool (child);
CREATE INDEX I_SvcClass2TapePool_Parent on SvcClass2TapePool (parent);

/* SQL statements for type DiskPool */
DROP TABLE DiskPool;
CREATE TABLE DiskPool (name VARCHAR(255), id INTEGER PRIMARY KEY);
DROP INDEX I_DiskPool2SvcClass_Child;
DROP INDEX I_DiskPool2SvcClass_Parent;
DROP TABLE DiskPool2SvcClass;
CREATE TABLE DiskPool2SvcClass (Parent INTEGER, Child INTEGER);
CREATE INDEX I_DiskPool2SvcClass_Child on DiskPool2SvcClass (child);
CREATE INDEX I_DiskPool2SvcClass_Parent on DiskPool2SvcClass (parent);

/* SQL statements for type Stream */
DROP TABLE Stream;
CREATE TABLE Stream (initialSizeToTransfer INTEGER, id INTEGER PRIMARY KEY, tape INTEGER, tapePool INTEGER, status INTEGER);
DROP INDEX I_Stream2TapeCopy_Child;
DROP INDEX I_Stream2TapeCopy_Parent;
DROP TABLE Stream2TapeCopy;
CREATE TABLE Stream2TapeCopy (Parent INTEGER, Child INTEGER);
CREATE INDEX I_Stream2TapeCopy_Child on Stream2TapeCopy (child);
CREATE INDEX I_Stream2TapeCopy_Parent on Stream2TapeCopy (parent);

/* SQL statements for type FileClass */
DROP TABLE FileClass;
CREATE TABLE FileClass (name VARCHAR(255), minFileSize NUMBER, maxFileSize NUMBER, nbCopies NUMBER, id INTEGER PRIMARY KEY);

/* SQL statements for type DiskServer */
DROP TABLE DiskServer;
CREATE TABLE DiskServer (name VARCHAR(255), id INTEGER PRIMARY KEY, status INTEGER);

/* This file contains SQL code that is not generated automatically */
/* and is inserted at the end of the generated code           */

/* Indexes related to CastorFiles */
DROP INDEX I_DiskCopy_Castorfile;
DROP INDEX I_TapeCopy_Castorfile;
DROP INDEX I_SubRequest_Castorfile;
DROP INDEX I_FileSystem_DiskPool;
DROP INDEX I_SubRequest_DiskCopy;
CREATE INDEX I_DiskCopy_Castorfile on DiskCopy (castorFile);
CREATE INDEX I_TapeCopy_Castorfile on TapeCopy (castorFile);
CREATE INDEX I_SubRequest_Castorfile on SubRequest (castorFile);
CREATE INDEX I_FileSystem_DiskPool on FileSystem (diskPool);
CREATE INDEX I_SubRequest_DiskCopy on SubRequest (diskCopy);

/* PL/SQL method to make a SubRequest wait on another one, linked to the given DiskCopy */
CREATE OR REPLACE PROCEDURE makeSubRequestWait(subreqId IN INTEGER, diskCopyId IN INTEGER) AS
BEGIN
 UPDATE SubRequest
  SET parent = (SELECT id FROM SubRequest WHERE diskCopy = diskCopyId), status = 5 -- WAITSUBREQ
  WHERE SubRequest.id = subreqId;
END;

/* PL/SQL method implementing bestTapeCopyForStream */
CREATE OR REPLACE PROCEDURE bestTapeCopyForStream(streamId IN INTEGER, tapeCopyStatus IN NUMBER,
                                                  newTapeCopyStatus IN NUMBER, newStreamStatus IN NUMBER,
                                                  diskServerName OUT VARCHAR, mountPoint OUT VARCHAR,
                                                  path OUT VARCHAR, diskCopyId OUT INTEGER,
                                                  castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                  nsHost OUT VARCHAR, fileSize OUT INTEGER,
                                                  tapeCopyId OUT INTEGER) AS
 fileSystemId INTEGER;
BEGIN
 SELECT DiskServer.name, FileSystem.mountPoint, DiskCopy.path, DiskCopy.id, FileSystem.id,
   CastorFile.id, CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, TapeCopy.id
  INTO diskServerName, mountPoint, path, diskCopyId, fileSystemId, castorFileId, fileId, nsHost, fileSize, tapeCopyId
  FROM DiskServer, FileSystem, DiskCopy, CastorFile, TapeCopy, Stream2TapeCopy
  WHERE DiskServer.id = FileSystem.diskserver
    AND FileSystem.id = DiskCopy.filesystem
    AND DiskCopy.castorfile = CastorFile.id
    AND TapeCopy.castorfile = Castorfile.id
    AND Stream2TapeCopy.child = TapeCopy.id
    AND Stream2TapeCopy.parent = streamId
    AND TapeCopy.status = tapeCopyStatus
    AND ROWNUM < 2
  ORDER by FileSystem.weight DESC;
 UPDATE TapeCopy SET status = newTapeCopyStatus WHERE id = tapeCopyId;
 UPDATE Stream SET status = newStreamStatus WHERE id = streamId;
 UPDATE FileSystem SET weight = weight - fsDeviation WHERE id = fileSystemId;
END;

/* PL/SQL method implementing bestFileSystemForSegment */
CREATE OR REPLACE PROCEDURE bestFileSystemForSegment(segmentId IN INTEGER, diskServerName OUT VARCHAR,
                                                     mountPoint OUT VARCHAR, path OUT VARCHAR,
                                                     diskCopyId OUT INTEGER) AS
 fileSystemId NUMBER;
BEGIN
SELECT DiskServer.name, FileSystem.mountPoint, DiskCopy.path, DiskCopy.id
 INTO diskServerName, mountPoint, path, diskCopyId
 FROM DiskServer, FileSystem, DiskPool2SvcClass, StageInRequest,
      SubRequest, TapeCopy, Segment, DiskCopy, CastorFile
  WHERE Segment.id = segmentId
    AND Segment.copy = TapeCopy.id
    AND SubRequest.castorfile = TapeCopy.castorfile
    AND DiskCopy.castorfile = TapeCopy.castorfile
    AND Castorfile.id = TapeCopy.castorfile
    AND StageInRequest.id = SubRequest.request
    AND StageInRequest.svcclass = DiskPool2SvcClass.child
    AND FileSystem.diskpool = DiskPool2SvcClass.parent
    AND FileSystem.free > CastorFile.fileSize
    AND DiskServer.id = FileSystem.diskServer
    AND ROWNUM < 2
  ORDER by FileSystem.weight DESC;
UPDATE DiskCopy SET fileSystem = fileSystemId WHERE id = diskCopyId;
UPDATE FileSystem SET weight = weight - fsDeviation WHERE id = fileSystemId;
END;

/* PL/SQL method implementing fileRecalled */
CREATE OR REPLACE PROCEDURE fileRecalled(tapecopyId IN INTEGER, SubRequestStatus IN NUMBER,
                                         DiskCopyStatus IN NUMBER) AS
 SubRequestId NUMBER;
 DiskCopyId NUMBER;
BEGIN
SELECT SubRequest.id, DiskCopy.id
 INTO SubRequestId, DiskCopyId
 FROM TapeCopy, SubRequest, DiskCopy
 WHERE TapeCopy.id = 6002
  AND DiskCopy.castorFile = TapeCopy.castorFile
  AND SubRequest.diskcopy = DiskCopy.id;
UPDATE DiskCopy SET status = diskCopyStatus WHERE id = DiskCopyId;
UPDATE SubRequest SET status = SubRequestStatus WHERE id = SubRequestId;
UPDATE SubRequest SET status = SubRequestStatus WHERE parent = SubRequestId;
END;

/* PL/SQL method implementing isSubRequestToSchedule */
CREATE OR REPLACE PROCEDURE isSubRequestToSchedule(subreqId IN INTEGER, result OUT INTEGER) AS
  stat INTEGER;
  diskCopyId INTEGER;
BEGIN
 SELECT DiskCopy.status, DiskCopy.id
  INTO stat, diskCopyId
  FROM DiskCopy, SubRequest
  WHERE SubRequest.id = subreqId
   AND SubRequest.castorfile = DiskCopy.castorfile
   AND DiskCopy.status != 3 -- DISKCCOPY_DELETED
   AND DiskCopy.status != 4 -- DISKCCOPY_FAILED
   AND DiskCopy.status != 7 -- DISKCCOPY_INVALID
   AND ROWNUM < 2;
 IF 2 = stat -- DISKCCOPY_WAITTAPERECALL
 THEN
  -- Only DiskCopy, make SubRequest wait on the recalling one and do not schedule
  update SubRequest SET parent = (SELECT id FROM SubRequest where diskCopy = diskCopyId) WHERE id = subreqId;
  result := 0;
 ELSE
  -- DiskCopies exist that are already recalled, thus schedule for access
  result := 1;
 END IF;
EXCEPTION
 WHEN NO_DATA_FOUND -- In this case, schedule for recall
 THEN result := 1;
END;

/* PL/SQL method implementing scheduleSubRequest */
DECLARE
  TYPE DiskCopy_Cur IS REF CURSOR RETURN SubRequest%ROWTYPE;
BEGIN
  NULL;
END;
CREATE OR REPLACE PACKAGE castor AS
  TYPE DiskCopyCore IS RECORD (id INTEGER, path VARCHAR(255), status NUMBER);
  TYPE DiskCopy_Cur IS REF CURSOR RETURN DiskCopyCore;
END castor;

CREATE OR REPLACE PROCEDURE scheduleSubRequest(subreqId IN INTEGER, fileSystemId IN INTEGER,
                                               diskCopyId OUT INTEGER, path OUT VARCHAR,
                                               status OUT NUMBER, sources OUT castor.DiskCopy_Cur) AS
  castorFileId INTEGER;
  unusedPATH VARCHAR(255);
BEGIN
 diskCopyId := 0;
 SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status
  INTO diskCopyId, path, status
  FROM DiskCopy, SubRequest
  WHERE SubRequest.id = subreqId
    AND SubRequest.castorfile = DiskCopy.castorfile
    AND DiskCopy.filesystem = fileSystemId
    AND DiskCopy.status IN (0, 1, 2, 5, 6); -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT
 IF status IN (2, 5) THEN -- WAITTAPERECALL, WAITFS, Make SubRequest Wait
   dbms_output.put_line('Make SubRequest Wait');
   makeSubRequestWait(subreqId, diskCopyId);
   diskCopyId := 0;
   path := '';
 END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN -- No disk copy found on selected FileSystem, look in others
 OPEN sources FOR SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status
 FROM DiskCopy, SubRequest
 WHERE SubRequest.id = subreqId
   AND SubRequest.castorfile = DiskCopy.castorfile
   AND DiskCopy.status IN (0, 1, 2, 5, 6); -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT
 IF sources%NOTFOUND THEN -- create DiskCopy for recall
   getId(1, diskCopyId);
   UPDATE SubRequest SET diskCopy = diskCopyId, status = 4 -- WAITTAPERECALL
    WHERE id = subreqId RETURNING castorFile INTO castorFileId;
   INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status)
    VALUES ('', diskCopyId, 0, castorFileId, 2); -- status WAITTAPERECALL
   diskCopyId := 0;
   close sources;
 ELSE
   FETCH sources INTO diskCopyId, unusedPath, status;
   IF status IN (2,5) THEN -- WAITTAPERECALL, WAITFS, Make SubRequest Wait
     dbms_output.put_line('We will have to wait');
     makeSubRequestWait(subreqId, diskCopyId);
     diskCopyId := 0;
     close sources;
   ELSE -- create DiskCopy for Disk to Disk copy
     getId(1, diskCopyId);
     UPDATE SubRequest SET diskCopy = diskCopyId WHERE id = subreqId
      RETURNING castorFile INTO castorFileId;
     INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status)
      VALUES ('', diskCopyId, 0, castorFileId, 1); -- status WAITDISK2DISKCOPY
     status := 1; -- status WAITDISK2DISKCOPY
   END IF;
 END IF;
END;
