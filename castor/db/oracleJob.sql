/* This file contains SQL code that is not generated automatically */
/* and is inserted at the end of the generated code           */

/* Indexes related to CastorFiles */
DROP INDEX I_DiskCopy_Castorfile;
DROP INDEX I_TapeCopy_Castorfile;
DROP INDEX I_SubRequest_Castorfile;
DROP INDEX I_FileSystem_DiskPool;
DROP INDEX I_SubRequest_DiskCopy;
DROP INDEX I_CastorFile_fileIdNsHost;
DROP INDEX I_DiskServer_name;
CREATE UNIQUE INDEX I_DiskServer_name on DiskServer (name);
CREATE UNIQUE INDEX I_CastorFile_fileIdNsHost on CastorFile (fileId, nsHost);
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
 FROM DiskServer, FileSystem, DiskPool2SvcClass,
      (SELECT id, svcClass from StageGetRequest UNION
       SELECT id, svcClass from StagePrepareToGetRequest UNION
       SELECT id, svcClass from StageGetNextRequest UNION
       SELECT id, svcClass from StageUpdateRequest UNION
       SELECT id, svcClass from StagePrepareToUpdateRequest UNION
       SELECT id, svcClass from StageUpdateNextRequest) Request,
      SubRequest, TapeCopy, Segment, DiskCopy, CastorFile
  WHERE Segment.id = segmentId
    AND Segment.copy = TapeCopy.id
    AND SubRequest.castorfile = TapeCopy.castorfile
    AND DiskCopy.castorfile = TapeCopy.castorfile
    AND Castorfile.id = TapeCopy.castorfile
    AND Request.id = SubRequest.request
    AND Request.svcclass = DiskPool2SvcClass.child
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
                                         dskCopyStatus IN NUMBER) AS
 SubRequestId NUMBER;
 DiskCopyId NUMBER;
BEGIN
SELECT SubRequest.id, DiskCopy.id
 INTO SubRequestId, DiskCopyId
 FROM TapeCopy, SubRequest, DiskCopy
 WHERE TapeCopy.id = tapecopyId
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
  TYPE DiskCopyCore IS RECORD (id INTEGER, path VARCHAR(2048), status NUMBER);
  TYPE DiskCopy_Cur IS REF CURSOR RETURN DiskCopyCore;
END castor;

/* PL/SQL method implementing scheduleSubRequest */
CREATE OR REPLACE PROCEDURE scheduleSubRequest(srId IN INTEGER, fileSystemId IN INTEGER,
                                               diskCopyId OUT INTEGER, path OUT VARCHAR,
                                               status OUT NUMBER, sources OUT castor.DiskCopy_Cur) AS
  castorFileId INTEGER;
  unusedPATH VARCHAR(2048);
BEGIN
 diskCopyId := 0;
 SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status
  INTO diskCopyId, path, status
  FROM DiskCopy, SubRequest
  WHERE SubRequest.id = srId
    AND SubRequest.castorfile = DiskCopy.castorfile
    AND DiskCopy.filesystem = fileSystemId
    AND DiskCopy.status IN (0, 1, 2, 5, 6); -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT
 IF status IN (2, 5) THEN -- WAITTAPERECALL, WAITFS, Make SubRequest Wait
   makeSubRequestWait(srId, diskCopyId);
   diskCopyId := 0;
   path := '';
 END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN -- No disk copy found on selected FileSystem, look in others
 OPEN sources FOR SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status
 FROM DiskCopy, SubRequest
 WHERE SubRequest.id = srId
   AND SubRequest.castorfile = DiskCopy.castorfile
   AND DiskCopy.status IN (0, 1, 2, 5, 6); -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT
 IF sources%NOTFOUND THEN -- create DiskCopy for recall
   getId(1, diskCopyId);
   UPDATE SubRequest SET diskCopy = diskCopyId, status = 4 -- WAITTAPERECALL
    WHERE id = srId RETURNING castorFile INTO castorFileId;
   INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status)
    VALUES ('', diskCopyId, 0, castorFileId, 2); -- status WAITTAPERECALL
   diskCopyId := 0;
   status := 2; -- WAITTAPERECALL
   close sources;
 ELSE
   FETCH sources INTO diskCopyId, unusedPath, status;
   IF status IN (2,5) THEN -- WAITTAPERECALL, WAITFS, Make SubRequest Wait
     makeSubRequestWait(srId, diskCopyId);
     diskCopyId := 0;
     close sources;
   ELSE -- create DiskCopy for Disk to Disk copy
     getId(1, diskCopyId);
     UPDATE SubRequest SET diskCopy = diskCopyId WHERE id = srId
      RETURNING castorFile INTO castorFileId;
     INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status)
      VALUES ('', diskCopyId, 0, castorFileId, 1); -- status WAITDISK2DISKCOPY
     status := 1; -- status WAITDISK2DISKCOPY
   END IF;
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
 UPDATE SubRequest SET status = newStatus WHERE id = srId;
 -- Check whether it was the last subrequest in the request
 SELECT id INTO result FROM SubRequest
  WHERE request = reqId
    AND status NOT IN (6, 7) -- SUBREQUEST_READY, SUBREQUEST_FAILED
    AND ROWNUM < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN -- No data found means we were last
  result := 0;
END;
