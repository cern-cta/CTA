/* This file contains SQL code that is not generated automatically */
/* and is inserted at the end of the generated code           */

/* Indexes related to CastorFiles */
CREATE UNIQUE INDEX I_DiskServer_name on DiskServer (name);
CREATE UNIQUE INDEX I_CastorFile_fileIdNsHost on CastorFile (fileId, nsHost);
CREATE INDEX I_DiskCopy_Castorfile on DiskCopy (castorFile);
CREATE INDEX I_TapeCopy_Castorfile on TapeCopy (castorFile);
CREATE INDEX I_SubRequest_Castorfile on SubRequest (castorFile);
CREATE INDEX I_FileSystem_DiskPool on FileSystem (diskPool);
CREATE INDEX I_SubRequest_DiskCopy on SubRequest (diskCopy);

/* PL/SQL method to make a SubRequest wait on another one, linked to the given DiskCopy */
CREATE OR REPLACE PROCEDURE makeSubRequestWait(subreqId IN INTEGER, dci IN INTEGER) AS
BEGIN
 UPDATE SubRequest
  SET parent = (SELECT id FROM SubRequest WHERE diskCopy = dci), status = 5 -- WAITSUBREQ
  WHERE SubRequest.id = subreqId;
END;

/* PL/SQL method implementing bestTapeCopyForStream */
CREATE OR REPLACE PROCEDURE bestTapeCopyForStream(streamId IN INTEGER, tapeCopyStatus IN NUMBER,
                                                  newTapeCopyStatus IN NUMBER, newStreamStatus IN NUMBER,
                                                  diskServerName OUT VARCHAR, mountPoint OUT VARCHAR,
                                                  path OUT VARCHAR, dci OUT INTEGER,
                                                  castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                  nsHost OUT VARCHAR, fileSize OUT INTEGER,
                                                  tapeCopyId OUT INTEGER) AS
 fileSystemId INTEGER;
BEGIN
 SELECT DiskServer.name, FileSystem.mountPoint, DiskCopy.path, DiskCopy.id, FileSystem.id,
   CastorFile.id, CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, TapeCopy.id
  INTO diskServerName, mountPoint, path, dci, fileSystemId, castorFileId, fileId, nsHost, fileSize, tapeCopyId
  FROM DiskServer, FileSystem, DiskCopy, CastorFile, TapeCopy, Stream2TapeCopy
  WHERE DiskServer.id = FileSystem.diskserver
    AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
    AND FileSystem.id = DiskCopy.filesystem
    AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
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
                                                     rmountPoint OUT VARCHAR, rpath OUT VARCHAR,
                                                     dci OUT INTEGER) AS
 fileSystemId NUMBER;
BEGIN
SELECT DiskServer.name, FileSystem.mountPoint, FileSystem.id, DiskCopy.path, DiskCopy.id
 INTO diskServerName, rmountPoint, fileSystemId, rpath, dci
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
    AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
    AND DiskServer.id = FileSystem.diskServer
    AND DiskServer.status = 0 -- DISKSERVER_PRODUCTION
    AND ROWNUM < 2
  ORDER by FileSystem.weight DESC;
UPDATE DiskCopy SET fileSystem = fileSystemId WHERE id = dci;
UPDATE FileSystem SET weight = weight - fsDeviation WHERE id = fileSystemId;
END;

/* PL/SQL method implementing fileRecalled */
CREATE OR REPLACE PROCEDURE fileRecalled(tapecopyId IN INTEGER,
                                         uuid IN VARCHAR) AS
 SubRequestId NUMBER;
 dci NUMBER;
BEGIN
SELECT SubRequest.id, DiskCopy.id
 INTO SubRequestId, dci
 FROM TapeCopy, SubRequest, DiskCopy
 WHERE TapeCopy.id = tapecopyId
  AND DiskCopy.castorFile = TapeCopy.castorFile
  AND SubRequest.diskcopy = DiskCopy.id;
UPDATE DiskCopy SET status = 0, diskcopyId = uuid WHERE id = dci; -- DISKCOPY_STAGED
UPDATE SubRequest SET status = 1 WHERE id = SubRequestId; -- SUBREQUEST_RESTART
UPDATE SubRequest SET status = 1 WHERE parent = SubRequestId; -- SUBREQUEST_RESTART
END;

/* PL/SQL method implementing isSubRequestToSchedule */
CREATE OR REPLACE PROCEDURE isSubRequestToSchedule(rsubreqId IN INTEGER, result OUT INTEGER) AS
  stat INTEGER;
  dci INTEGER;
BEGIN
 SELECT DiskCopy.status, DiskCopy.id
  INTO stat, dci
  FROM DiskCopy, SubRequest
  WHERE SubRequest.id = rsubreqId
   AND SubRequest.castorfile = DiskCopy.castorfile
   AND DiskCopy.status != 3 -- DISKCCOPY_DELETED
   AND DiskCopy.status != 4 -- DISKCCOPY_FAILED
   AND DiskCopy.status != 7 -- DISKCCOPY_INVALID
   AND ROWNUM < 2;
 IF 2 = stat -- DISKCCOPY_WAITTAPERECALL
 THEN
  -- Only DiskCopy, make SubRequest wait on the recalling one and do not schedule
  update SubRequest SET parent = (SELECT id FROM SubRequest where diskCopy = dci) WHERE id = rsubreqId;
  result := 0;
 ELSE
  -- DiskCopies exist that are already recalled, thus schedule for access
  result := 1;
 END IF;
EXCEPTION
 WHEN NO_DATA_FOUND -- In this case, schedule for recall
 THEN result := 1;
END;

/* PL/SQL method implementing castor package */
CREATE OR REPLACE PACKAGE castor AS
  TYPE DiskCopyCore IS RECORD (id INTEGER, path VARCHAR(2048), status NUMBER, diskCopyId VARCHAR(2048), fsWeight NUMBER, mountPoint VARCHAR(2048), diskServer VARCHAR(2048));
  TYPE DiskCopy_Cur IS REF CURSOR RETURN DiskCopyCore;
END castor;

/* Build diskCopy path from fileId */
CREATE OR REPLACE PROCEDURE buildPathFromFileId(fid IN INTEGER,
                                                nsHost IN VARCHAR,
                                                path OUT VARCHAR) AS
BEGIN
  path := CONCAT(CONCAT(CONCAT(TO_CHAR(MOD(fid,100),'FM09'), '/'),
                        CONCAT(TO_CHAR(fid), '@')),
                 nsHost);
END;

/* PL/SQL method implementing getUpdateStart */
CREATE OR REPLACE PROCEDURE getUpdateStart
        (srId IN INTEGER, fileSystemId IN INTEGER,
         dci OUT INTEGER, rpath OUT VARCHAR,
         rstatus OUT NUMBER, sources OUT castor.DiskCopy_Cur,
         rclient OUT INTEGER, rDiskCopyId OUT VARCHAR,
         reuid OUT INTEGER, regid OUT INTEGER) AS
  cfid INTEGER;
  fid INTEGER;
  nh VARCHAR(2048);
  rFsWeight NUMBER;
BEGIN
 -- Get IClient and uid, gid
 SELECT client, euid, egid INTO rclient, reuid, regid FROM SubRequest,
      (SELECT client, id, euid, egid from StageGetRequest UNION
       SELECT client, id, euid, egid from StagePrepareToGetRequest UNION
       SELECT client, id, euid, egid from StageUpdateRequest UNION
       SELECT client, id, euid, egid from StagePrepareToUpdateRequest) Request
  WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
 -- Try to find local DiskCopy
 dci := 0;
 SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, DiskCopy.diskcopyId
  INTO dci, rpath, rstatus, rDiskCopyId
  FROM DiskCopy, SubRequest
  WHERE SubRequest.id = srId
    AND SubRequest.castorfile = DiskCopy.castorfile
    AND DiskCopy.filesystem = fileSystemId
    AND DiskCopy.status IN (0, 1, 2, 5, 6); -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT
 -- If found local one, check whether to wait on it
 IF rstatus IN (2, 5) THEN -- WAITTAPERECALL, WAITFS, Make SubRequest Wait
   makeSubRequestWait(srId, dci);
   dci := 0;
   rpath := '';
   rDiskCopyId := '';
 END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN -- No disk copy found on selected FileSystem, look in others
 BEGIN
  -- Try to find remote DiskCopies
  SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status,
         DiskCopy.diskcopyId, FileSystem.weight
  INTO dci, rpath, rstatus, rDiskCopyId, rFsWeight
  FROM DiskCopy, SubRequest, FileSystem
  WHERE SubRequest.id = srId
    AND SubRequest.castorfile = DiskCopy.castorfile
    AND DiskCopy.status IN (0, 1, 2, 5, 6) -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT
    AND FileSystem.id = DiskCopy.fileSystem
    AND ROWNUM < 2;
  -- Found a DiskCopy, Check whether to wait on it
  IF rstatus IN (2,5) THEN -- WAITTAPERECALL, WAITFS, Make SubRequest Wait
    makeSubRequestWait(srId, dci);
    dci := 0;
    rpath := '';
    rDiskCopyId := '';
    close sources;
  ELSE
    OPEN sources
    FOR SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status,
               DiskCopy.diskcopyId, FileSystem.weight,
               FileSystem.mountPoint, DiskServer.name
    FROM DiskCopy, SubRequest, FileSystem, DiskServer
    WHERE SubRequest.id = srId
      AND SubRequest.castorfile = DiskCopy.castorfile
      AND DiskCopy.status IN (0, 1, 2, 5, 6) -- STAGED, WAITDISKTODISKCOPY, WAITTAPERECALL, WAIFS, STAGEOUT
      AND FileSystem.id = DiskCopy.fileSystem
      AND DiskServer.id = FileSystem.diskServer;
    -- create DiskCopy for Disk to Disk copy
    getId(1, dci);
    dci := dci - 1;
    UPDATE SubRequest SET diskCopy = dci WHERE id = srId
     RETURNING castorFile INTO cfid;
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status)
     VALUES (rpath, dci, fileSystemId, cfid, 1); -- status WAITDISK2DISKCOPY
    INSERT INTO Id2Type (id, type) VALUES (dci, 5); -- OBJ_DiskCopy
    rstatus := 1; -- status WAITDISK2DISKCOPY
  END IF;
 EXCEPTION WHEN NO_DATA_FOUND THEN -- No disk copy found on any FileSystem
  -- create one for recall
  getId(1, dci);
  dci := dci - 1;
  UPDATE SubRequest SET diskCopy = dci, status = 4 -- WAITTAPERECALL
   WHERE id = srId RETURNING castorFile INTO cfid;
  SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfid;
  buildPathFromFileId(fid, nh, rpath);
  INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status)
   VALUES (rpath, dci, fileSystemId, cfid, 2); -- status WAITTAPERECALL
  INSERT INTO Id2Type (id, type) VALUES (dci, 5); -- OBJ_DiskCopy
  rstatus := 99; -- WAITTAPERECALL, NEWLY CREATED
 END;
END;

/* PL/SQL method implementing putStart */
CREATE OR REPLACE PROCEDURE putStart
        (srId IN INTEGER, fileSystemId IN INTEGER,
         rclient OUT INTEGER, rdcId OUT INTEGER,
         rdcStatus OUT INTEGER, rdcPath OUT VARCHAR,
         rdcDiskCopyId OUT VARCHAR) AS
BEGIN
 -- Get IClient
 SELECT client, diskCopy INTO rclient, rdcId FROM SubRequest,
      (SELECT client, id from StagePutRequest UNION
       SELECT client, id from StagePrepareToPutRequest) Request
  WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
 -- link DiskCopy and FileSystem and update DiskCopyStatus
 UPDATE DiskCopy SET status = 6, -- DISKCOPY_STAGEOUT
                     fileSystem = fileSystemId
  WHERE id = rdcId
  RETURNING status, path, diskcopyId
  INTO rdcStatus, rdcPath, rdcDiskCopyId;
EXCEPTION WHEN NO_DATA_FOUND THEN -- No data found means we were last
  rclient := 0;
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

/* PL/SQL method implementing disk2DiskCopyDone */
CREATE OR REPLACE PROCEDURE disk2DiskCopyDone(dcId IN INTEGER) AS
BEGIN
  -- update DiskCopy
  UPDATE DiskCopy set status = 0 WHERE id = dcId; -- status DISKCOPY_STAGED
  -- update SubRequest
  UPDATE SubRequest set status = 6 WHERE diskCopy = dcId; -- status SUBREQUEST_READY
END;

/* PL/SQL method implementing recreateCastorFile */
CREATE OR REPLACE PROCEDURE recreateCastorFile(cfId IN INTEGER,
                                               srId IN INTEGER,
                                               dcId OUT INTEGER) AS
  rpath VARCHAR(2048);
  fid INTEGER;
  nh VARCHAR(2048);
BEGIN
 -- Lock the access to the TapeCopies and DiskCopies
 LOCK TABLE TapeCopy in share mode;
 LOCK TABLE DiskCopy in share mode;
 -- check if recreation is possible (exception if not)
 BEGIN
   SELECT id INTO dcId FROM TapeCopy
     WHERE status = 3 -- TAPECOPY_SELECTED
     AND castorFile = cfId;
   -- We found something, thus we cannot recreate
   dcId := 0;
   COMMIT;
   RETURN;
 EXCEPTION WHEN NO_DATA_FOUND THEN
   -- No data found means we can recreate
   NULL;
 END;
 BEGIN
   SELECT id INTO dcId FROM DiskCopy
     WHERE status IN (1, 2, 5, 6) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT
     AND castorFile = cfId;
   -- We found something, thus we cannot recreate
   dcId := 0;
   COMMIT;
   RETURN;
 EXCEPTION WHEN NO_DATA_FOUND THEN
   -- No data found means we can recreate
   NULL;
 END;
 -- delete all tapeCopies
 DELETE from TapeCopy WHERE castorFile = cfId;
 -- set DiskCopies to INVALID
 UPDATE DiskCopy SET status = 7 -- INVALID
  WHERE castorFile = cfId AND status NOT IN (3, 4, 7); -- FAILED, DELETED, INVALID
 -- create new DiskCopy
 getId(1, dcId);
 dcId := dcId - 1;
 SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfId;
 buildPathFromFileId(fid, nh, rpath);
 INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status)
  VALUES (rpath, dcId, 0, cfId, 5); -- status WAITFS
 INSERT INTO Id2Type (id, type) VALUES (dcId, 5); -- OBJ_DiskCopy
 -- link SubRequest and DiskCopy
 UPDATE SubRequest SET diskCopy = dcId WHERE id = srId;
 COMMIT;
END;

/* PL/SQL method implementing prepareForMigration */
CREATE OR REPLACE PROCEDURE prepareForMigration (srId IN INTEGER,
                                                 fs IN INTEGER,
                                                 fId OUT NUMBER,
                                                 nh OUT VARCHAR,
                                                 userId OUT INTEGER,
                                                 groupId OUT INTEGER) AS
  nc INTEGER;
  cfId INTEGER;
  tcId INTEGER;
BEGIN
 -- get CastorFile
 SELECT castorFile INTO cfId FROM SubRequest where id = srId;
 -- update CastorFile
 UPDATE CastorFile set fileSize = fs WHERE id = cfId
  RETURNING fileId, nsHost INTO fId, nh;
 -- get uid, gid from Request
 SELECT euid, egid INTO userId, groupId FROM SubRequest,
      (SELECT euid, egid, id from StagePutRequest UNION
       SELECT euid, egid, id from StagePrepareToPutRequest) Request
  WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
 -- if 0 length file, stop here
 IF fs = 0 THEN
   COMMIT;
   RETURN;
 END IF;
 -- get number of copies to create
 SELECT nbCopies INTO nc FROM FileClass, CastorFile
  WHERE CastorFile.id = cfId AND CastorFile.fileClass = FileClass.id;
 -- Create TapeCopies
 <<TapeCopyCreation>>
 getId(nc, tcId);
 tcId := tcId - nc;
 FOR i IN 1..nc LOOP
  INSERT INTO TapeCopy (id, copyNb, castorFile, status)
   VALUES (tcId, i, cfId, 0); -- TAPECOPY_CREATED
  INSERT INTO Id2Type (id, type) VALUES (tcId, 30); -- OBJ_TapeCopy
  tcId := tcId + 1;
 END LOOP TapeCopyCreation;
 COMMIT;
END;
