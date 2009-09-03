/*******************************************************************	
 * @(#)$RCSfile: oracleTape.sql,v $ $Revision: 1.761 $ $Date: 2009/08/10 15:30:13 $ $Author: itglp $
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
    status NUMBER,
    vid VARCHAR2(2048));
  TYPE TapeGatewayRequest_Cur IS REF CURSOR RETURN TapeGatewayRequestExtended;
  TYPE TapeGatewayRequestCore IS RECORD (
    tpmode INTEGER,
    side INTEGER,
    vid VARCHAR2(2048),
    tapeRequestId NUMBER);
  TYPE TapeGatewayRequestCore_Cur IS REF CURSOR RETURN TapeGatewayRequestCore;
  TYPE StreamCore IS RECORD (
    id INTEGER,
    initialSizeToTransfer INTEGER,
    status NUMBER,
    tapePoolId NUMBER,
    tapePoolName VARCHAR2(2048));
  TYPE Stream_Cur IS REF CURSOR RETURN StreamCore; 
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
    numFile NUMBER,
    byteVolume NUMBER,
    age NUMBER);
  TYPE DbStreamInfo_Cur IS REF CURSOR RETURN DbStreamInfo;
  TYPE DbRecallInfo IS RECORD (
    vid VARCHAR2(2048),
    tapeId NUMBER,
    dataVolume NUMBER,
    numbFiles NUMBER,
    expireTime NUMBER,
    priority NUMBER);
  TYPE DbRecallInfo_Cur IS REF CURSOR RETURN DbRecallInfo;
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

/* PL/SQL methods to update FileSystem weight for new migrator streams */
CREATE OR REPLACE PROCEDURE updateFsMigratorOpened
(ds IN INTEGER, fs IN INTEGER, fileSize IN INTEGER) AS
BEGIN
  /* We lock first the diskserver in order to lock all the
     filesystems of this DiskServer in an atomical way */
  UPDATE DiskServer SET nbMigratorStreams = nbMigratorStreams + 1 WHERE id = ds;
  UPDATE FileSystem SET nbMigratorStreams = nbMigratorStreams + 1 WHERE id = fs;
END;
/

/* PL/SQL methods to update FileSystem weight for new recaller streams */
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
/


/* PL/SQL method implementing anyTapeCopyForStream.*/
CREATE OR REPLACE PROCEDURE anyTapeCopyForStream(streamId IN INTEGER, res OUT INTEGER) AS
  unused INTEGER;
BEGIN
  -- JUST rtcpclientd
  SELECT /*+ FIRST_ROWS */ TapeCopy.id INTO unused
    FROM DiskServer, FileSystem, DiskCopy, TapeCopy, Stream2TapeCopy
   WHERE DiskServer.id = FileSystem.diskserver
     AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
     AND FileSystem.id = DiskCopy.filesystem
     AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
     AND DiskCopy.castorfile = TapeCopy.castorfile
     AND Stream2TapeCopy.child = TapeCopy.id
     AND Stream2TapeCopy.parent = streamId
     AND TapeCopy.status = 2 -- WAITINSTREAMS
     AND ROWNUM < 2; 
  res := 1;
EXCEPTION
 WHEN NO_DATA_FOUND THEN
  res := 0;
END;
/

/* PL/SQL method implementing bestTapeCopyForStream */
CREATE OR REPLACE PROCEDURE bestTapeCopyForStream(streamId IN INTEGER,
                                                  diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                  path OUT VARCHAR2, dci OUT INTEGER,
                                                  castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                  nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                  tapeCopyId OUT INTEGER, lastUpdateTime OUT INTEGER) AS
  policy VARCHAR(2048);
BEGIN
  -- get the policy name
  BEGIN
    SELECT migrSelectPolicy INTO policy
      FROM Stream, TapePool
     WHERE Stream.id = streamId
       AND Stream.tapePool = TapePool.id;
    -- check for NULL value
    IF policy IS NULL THEN
      policy := 'defaultMigrSelPolicy';
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    policy := 'defaultMigrSelPolicy';
  END;
  EXECUTE IMMEDIATE 'BEGIN ' || policy || '(:streamId, :diskServerName, :mountPoint, :path, :dci, :castorFileId, :fileId, :nsHost, :fileSize, :tapeCopyId, :lastUpdateTime); END;'
    USING IN streamId, OUT diskServerName, OUT mountPoint, OUT path, OUT dci, OUT castorFileId, OUT fileId, OUT nsHost, OUT fileSize, OUT tapeCopyId, OUT lastUpdateTime;
END;
/

/* default migration candidate selection policy */
CREATE OR REPLACE PROCEDURE defaultMigrSelPolicy(streamId IN INTEGER,
                                                 diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                 path OUT VARCHAR2, dci OUT INTEGER,
                                                 castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                 nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                 tapeCopyId OUT INTEGER, lastUpdateTime OUT INTEGER) AS
  fileSystemId INTEGER := 0;
  diskServerId NUMBER;
  lastFSChange NUMBER;
  lastFSUsed NUMBER;
  lastButOneFSUsed NUMBER;
  findNewFS NUMBER := 1;
  nbMigrators NUMBER := 0;
  unused NUMBER;
BEGIN
  tapeCopyId := 0;
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
        SELECT /*+ FIRST_ROWS(1)  LEADING(D T ST) */
               D.path, D.id, D.castorfile, T.id
          INTO path, dci, castorFileId, tapeCopyId
          FROM DiskCopy D, TapeCopy T, Stream2TapeCopy ST
         WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
           AND D.filesystem = lastButOneFSUsed
           AND ST.parent = streamId
           AND T.status = 2 -- WAITINSTREAMS
           AND ST.child = T.id
           AND T.castorfile = D.castorfile
           AND ROWNUM < 2;
        SELECT CastorFile.FileId, CastorFile.NsHost, CastorFile.FileSize, CastorFile.lastUpdateTime
          INTO fileId, nsHost, fileSize, lastUpdateTime
          FROM CastorFile
         WHERE Id = castorFileId;
        -- we found one, no need to go for new filesystem
        findNewFS := 0;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- found no tapecopy or diskserver, filesystem are down. We'll go through the normal selection
        NULL;
      END;
    END IF;
  END IF;
  IF findNewFS = 1 THEN
    FOR f IN (
    SELECT FileSystem.id AS FileSystemId, DiskServer.id AS DiskServerId, DiskServer.name, FileSystem.mountPoint
       FROM Stream, SvcClass2TapePool, DiskPool2SvcClass, FileSystem, DiskServer
      WHERE Stream.id = streamId
        AND Stream.TapePool = SvcClass2TapePool.child
        AND SvcClass2TapePool.parent = DiskPool2SvcClass.child
        AND DiskPool2SvcClass.parent = FileSystem.diskPool
        AND FileSystem.diskServer = DiskServer.id
        AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
        AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
      ORDER BY -- first prefer diskservers where no migrator runs and filesystems with no recalls
               DiskServer.nbMigratorStreams ASC, FileSystem.nbRecallerStreams ASC,
               -- then order by rate as defined by the function
               fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams, FileSystem.nbWriteStreams,
                              FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC,
               -- finally use randomness to avoid preferring always the same FS
               DBMS_Random.value) LOOP
       DECLARE
         lock_detected EXCEPTION;
         PRAGMA EXCEPTION_INIT(lock_detected, -54);
       BEGIN
         -- lock the complete diskServer as we will update all filesystems
         SELECT id INTO unused FROM DiskServer WHERE id = f.DiskServerId FOR UPDATE NOWAIT;
         SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
                f.diskServerId, f.name, f.mountPoint, f.fileSystemId, D.path, D.id, D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, C.lastUpdateTime
           INTO diskServerId, diskServerName, mountPoint, fileSystemId, path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
           FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
          WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
            AND D.filesystem = f.fileSystemId
            AND StT.parent = streamId
            AND T.status = 2 -- WAITINSTREAMS
            AND StT.child = T.id
            AND T.castorfile = D.castorfile
            AND C.id = D.castorfile
            AND ROWNUM < 2;
         -- found something on this filesystem, no need to go on
         diskServerId := f.DiskServerId;
         fileSystemId := f.fileSystemId;
         EXIT;
       EXCEPTION WHEN NO_DATA_FOUND OR lock_detected THEN
         -- either the filesystem is already locked or we found nothing,
         -- let's go to the next one
         NULL;
       END;
    END LOOP;
  END IF;

  IF tapeCopyId = 0 THEN
    -- Nothing found, reset last filesystems used and exit
    UPDATE Stream
       SET lastFileSystemUsed = 0, lastButOneFileSystemUsed = 0
     WHERE id = streamId;
    RETURN;
  END IF;

  -- Here we found a tapeCopy and we process it
  -- update status of selected tapecopy and stream
  UPDATE TapeCopy SET status = 3 -- SELECTED
   WHERE id = tapeCopyId;
  IF findNewFS = 1 THEN
    UPDATE Stream
       SET status = 3, -- RUNNING
           lastFileSystemUsed = fileSystemId,
           lastButOneFileSystemUsed = lastFileSystemUsed,
           lastFileSystemChange = getTime()
     WHERE id = streamId AND status IN (2,3);
  ELSE
    -- only update status
    UPDATE Stream
       SET status = 3 -- RUNNING
     WHERE id = streamId AND status IN (2,3);
  END IF;
  -- detach the tapecopy from the stream now that it is SELECTED;
  DELETE FROM Stream2TapeCopy
   WHERE child = tapeCopyId;

  -- Update Filesystem state
  updateFSMigratorOpened(diskServerId, fileSystemId, 0);
END;
/

/* drain disk migration candidate selection policy */
CREATE OR REPLACE PROCEDURE drainDiskMigrSelPolicy(streamId IN INTEGER,
                                                   diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                   path OUT VARCHAR2, dci OUT INTEGER,
                                                   castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                   nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                   tapeCopyId OUT INTEGER, lastUpdateTime OUT INTEGER) AS
  fileSystemId INTEGER := 0;
  diskServerId NUMBER;
  fsDiskServer NUMBER;
  lastFSChange NUMBER;
  lastFSUsed NUMBER;
  lastButOneFSUsed NUMBER;
  findNewFS NUMBER := 1;
  nbMigrators NUMBER := 0;
  unused NUMBER;
BEGIN
  tapeCopyId := 0;
  -- First try to see whether we should resuse the same filesystem as last time
  SELECT lastFileSystemChange, lastFileSystemUsed, lastButOneFileSystemUsed
    INTO lastFSChange, lastFSUsed, lastButOneFSUsed
    FROM Stream WHERE id = streamId;
  IF getTime() < lastFSChange + 1800 THEN
    SELECT (SELECT count(*) FROM stream WHERE lastFileSystemUsed = lastFSUsed)
      INTO nbMigrators FROM DUAL;
    -- only go if we are the only migrator on the box
    IF nbMigrators = 1 THEN
      BEGIN
        -- check states of the diskserver and filesystem and get mountpoint and diskserver name
        SELECT diskserver.id, name, mountPoint, FileSystem.id INTO diskServerId, diskServerName, mountPoint, fileSystemId
          FROM FileSystem, DiskServer
         WHERE FileSystem.diskServer = DiskServer.id
           AND FileSystem.id = lastFSUsed
           AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
           AND DiskServer.status IN (0, 1); -- PRODUCTION, DRAINING
        -- we are within the time range, so we try to reuse the filesystem
        SELECT P.path, P.diskcopy_id, P.castorfile,
             C.fileId, C.nsHost, C.fileSize, P.id, C.lastUpdateTime
        INTO path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
        FROM (SELECT /*+ ORDERED USE_NL(D T) INDEX(T I_TapeCopy_CF_Status_2) INDEX(ST I_Stream2TapeCopy_PC) */
              D.path, D.diskcopy_id, D.castorfile, T.id
                FROM (SELECT /*+ INDEX(DK I_DiskCopy_FS_Status_10) */
                             DK.path path, DK.id diskcopy_id, DK.castorfile
                        FROM DiskCopy DK
                       WHERE decode(DK.status, 10, DK.status, NULL) = 10 -- CANBEMIGR
                         AND DK.filesystem = lastFSUsed) D,
                      TapeCopy T, Stream2TapeCopy ST
               WHERE T.castorfile = D.castorfile
                 AND ST.child = T.id
                 AND ST.parent = streamId
                 AND decode(T.status, 2, T.status, NULL) = 2 -- WAITINSTREAMS
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
    -- We try first to reuse the diskserver of the lastFSUsed, even if we change filesystem
    FOR f IN (
      SELECT FileSystem.id AS FileSystemId, DiskServer.id AS DiskServerId, DiskServer.name, FileSystem.mountPoint
        FROM FileSystem, DiskServer
       WHERE FileSystem.diskServer = DiskServer.id
         AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.id = lastButOneFSUsed) LOOP
       DECLARE
         lock_detected EXCEPTION;
         PRAGMA EXCEPTION_INIT(lock_detected, -54);
       BEGIN
         -- lock the complete diskServer as we will update all filesystems
         SELECT id INTO unused FROM DiskServer WHERE id = f.DiskServerId FOR UPDATE NOWAIT;
         SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
                f.diskServerId, f.name, f.mountPoint, f.fileSystemId, D.path, D.id, D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, C.lastUpdateTime
           INTO diskServerId, diskServerName, mountPoint, fileSystemId, path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
           FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
          WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
            AND D.filesystem = f.fileSystemId
            AND StT.parent = streamId
            AND T.status = 2 -- WAITINSTREAMS
            AND StT.child = T.id
            AND T.castorfile = D.castorfile
            AND C.id = D.castorfile
            AND ROWNUM < 2;
         -- found something on this filesystem, no need to go on
         diskServerId := f.DiskServerId;
         fileSystemId := f.fileSystemId;
         EXIT;
       EXCEPTION WHEN NO_DATA_FOUND OR lock_detected THEN
         -- either the filesystem is already locked or we found nothing,
         -- let's go to the next one
         NULL;
       END;
    END LOOP;
  END IF;
  IF tapeCopyId = 0 THEN
    -- Then we go for all potential filesystems. Note the duplication of code, due to the fact that ORACLE cannot order unions
    FOR f IN (
      SELECT FileSystem.id AS FileSystemId, DiskServer.id AS DiskServerId, DiskServer.name, FileSystem.mountPoint
        FROM Stream, SvcClass2TapePool, DiskPool2SvcClass, FileSystem, DiskServer
       WHERE Stream.id = streamId
         AND Stream.TapePool = SvcClass2TapePool.child
         AND SvcClass2TapePool.parent = DiskPool2SvcClass.child
         AND DiskPool2SvcClass.parent = FileSystem.diskPool
         AND FileSystem.diskServer = DiskServer.id
         AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
       ORDER BY -- first prefer diskservers where no migrator runs and filesystems with no recalls
                DiskServer.nbMigratorStreams ASC, FileSystem.nbRecallerStreams ASC,
                -- then order by rate as defined by the function
                fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams, FileSystem.nbWriteStreams,
                               FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC,
                -- finally use randomness to avoid preferring always the same FS
                DBMS_Random.value) LOOP
       DECLARE
         lock_detected EXCEPTION;
         PRAGMA EXCEPTION_INIT(lock_detected, -54);
       BEGIN
         -- lock the complete diskServer as we will update all filesystems
         SELECT id INTO unused FROM DiskServer WHERE id = f.DiskServerId FOR UPDATE NOWAIT;
         SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
                f.diskServerId, f.name, f.mountPoint, f.fileSystemId, D.path, D.id, D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, C.lastUpdateTime
           INTO diskServerId, diskServerName, mountPoint, fileSystemId, path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
           FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
          WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
            AND D.filesystem = f.fileSystemId
            AND StT.parent = streamId
            AND T.status = 2 -- WAITINSTREAMS
            AND StT.child = T.id
            AND T.castorfile = D.castorfile
            AND C.id = D.castorfile
            AND ROWNUM < 2;
         -- found something on this filesystem, no need to go on
         diskServerId := f.DiskServerId;
         fileSystemId := f.fileSystemId;
         EXIT;
       EXCEPTION WHEN NO_DATA_FOUND OR lock_detected THEN
         -- either the filesystem is already locked or we found nothing,
         -- let's go to the next one
         NULL;
       END;
    END LOOP;
  END IF;

  IF tapeCopyId = 0 THEN
    -- Nothing found, reset last filesystems used and exit
    UPDATE Stream
       SET lastFileSystemUsed = 0, lastButOneFileSystemUsed = 0
     WHERE id = streamId;
    RETURN;
  END IF;

  -- Here we found a tapeCopy and we process it
  -- update status of selected tapecopy and stream
  UPDATE TapeCopy SET status = 3 -- SELECTED
   WHERE id = tapeCopyId;
  IF findNewFS = 1 THEN
    UPDATE Stream
       SET status = 3, -- RUNNING
           lastFileSystemUsed = fileSystemId,
           lastButOneFileSystemUsed = lastFileSystemUsed,
           lastFileSystemChange = getTime()
     WHERE id = streamId AND status IN (2,3);
  ELSE
    -- only update status
    UPDATE Stream
       SET status = 3 -- RUNNING
     WHERE id = streamId AND status IN (2,3);
  END IF;
  -- detach the tapecopy from the stream now that it is SELECTED;
  DELETE FROM Stream2TapeCopy
   WHERE child = tapeCopyId;

  -- Update Filesystem state
  updateFSMigratorOpened(fsDiskServer, fileSystemId, 0);

END;
/

/* repack migration candidate selection policy */
CREATE OR REPLACE PROCEDURE repackMigrSelPolicy(streamId IN INTEGER,
                                                diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                path OUT VARCHAR2, dci OUT INTEGER,
                                                castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                tapeCopyId OUT INTEGER, lastUpdateTime OUT INTEGER) AS
  fileSystemId INTEGER := 0;
  diskServerId NUMBER;
  lastFSChange NUMBER;
  lastFSUsed NUMBER;
  lastButOneFSUsed NUMBER;
  nbMigrators NUMBER := 0;
  unused NUMBER;
BEGIN
  tapeCopyId := 0;
  FOR f IN (
    SELECT FileSystem.id AS FileSystemId, DiskServer.id AS DiskServerId, DiskServer.name, FileSystem.mountPoint
       FROM Stream, SvcClass2TapePool, DiskPool2SvcClass, FileSystem, DiskServer
      WHERE Stream.id = streamId
        AND Stream.TapePool = SvcClass2TapePool.child
        AND SvcClass2TapePool.parent = DiskPool2SvcClass.child
        AND DiskPool2SvcClass.parent = FileSystem.diskPool
        AND FileSystem.diskServer = DiskServer.id
        AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
        AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
      ORDER BY -- first prefer diskservers where no migrator runs and filesystems with no recalls
               DiskServer.nbMigratorStreams ASC, FileSystem.nbRecallerStreams ASC,
               -- then order by rate as defined by the function
               fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams, FileSystem.nbWriteStreams,
                              FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC,
               -- finally use randomness to avoid preferring always the same FS
               DBMS_Random.value) LOOP
    DECLARE
      lock_detected EXCEPTION;
      PRAGMA EXCEPTION_INIT(lock_detected, -54);
    BEGIN
      -- lock the complete diskServer as we will update all filesystems
      SELECT id INTO unused FROM DiskServer WHERE id = f.DiskServerId FOR UPDATE NOWAIT;
      SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
             f.diskServerId, f.name, f.mountPoint, f.fileSystemId, D.path, D.id, D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, C.lastUpdateTime
        INTO diskServerId, diskServerName, mountPoint, fileSystemId, path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
        FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
       WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
         AND D.filesystem = f.fileSystemId
         AND StT.parent = streamId
         AND T.status = 2 -- WAITINSTREAMS
         AND StT.child = T.id
         AND T.castorfile = D.castorfile
         AND C.id = D.castorfile
         AND ROWNUM < 2;
      -- found something on this filesystem, no need to go on
      diskServerId := f.DiskServerId;
      fileSystemId := f.fileSystemId;
      EXIT;
    EXCEPTION WHEN NO_DATA_FOUND OR lock_detected THEN
      -- either the filesystem is already locked or we found nothing,
      -- let's go to the next one
      NULL;
    END;
  END LOOP;

  IF tapeCopyId = 0 THEN
    -- Nothing found, reset last filesystems used and exit
    UPDATE Stream
       SET lastFileSystemUsed = 0, lastButOneFileSystemUsed = 0
     WHERE id = streamId;
    RETURN;
  END IF;

  -- Here we found a tapeCopy and we process it
  -- update status of selected tapecopy and stream
  UPDATE TapeCopy SET status = 3 -- SELECTED
   WHERE id = tapeCopyId;
  UPDATE Stream
     SET status = 3, -- RUNNING
         lastFileSystemUsed = fileSystemId,
         lastButOneFileSystemUsed = lastFileSystemUsed,
         lastFileSystemChange = getTime()
   WHERE id = streamId AND status IN (2,3);
  -- detach the tapecopy from the stream now that it is SELECTED;
  DELETE FROM Stream2TapeCopy
   WHERE child = tapeCopyId;

  -- Update Filesystem state
  updateFSMigratorOpened(diskServerId, fileSystemId, 0);
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
                      SELECT id, svcClass from StageUpdateRequest UNION ALL
                      SELECT id, svcClass from StagePrepareToUpdateRequest) Request,
                      SubRequest, CastorFile
               WHERE CastorFile.id = cfid
                 AND SubRequest.castorfile = cfid
                 AND SubRequest.status = 4 -- SUBREQUEST_WAITTAPERECALL
                 AND Request.id = SubRequest.request
                 AND Request.svcclass = DiskPool2SvcClass.child
                 AND FileSystem.diskpool = DiskPool2SvcClass.parent
                 AND FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > CastorFile.fileSize
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

    IF fileSystemId = 0 THEN
      raise_application_error(-20115, 'No suitable filesystem found for this recalled segment');
    END IF;
  END IF;
END;
/

/* PL/SQL method implementing fileRecallFailed */
CREATE OR REPLACE PROCEDURE fileRecallFailed(tapecopyId IN INTEGER) AS
 cfId NUMBER;
BEGIN
  SELECT castorFile INTO cfId FROM TapeCopy
   WHERE id = tapecopyId;
  UPDATE DiskCopy SET status = 4 -- DISKCOPY_FAILED
   WHERE castorFile = cfId
     AND status = 2; -- DISKCOPY_WAITTAPERECALL
  -- Drop tape copies. Ideally, we should keep some track that
  -- the recall failed in order to prevent future recalls until some
  -- sort of manual intervention. For the time being, as we can't
  -- say whether the failure is fatal or not, we drop everything
  -- and we won't deny a future request for recall.
  deleteTapeCopies(cfId);
  UPDATE SubRequest 
     SET status = 7, -- SUBREQUEST_FAILED
         getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED (not strictly correct but the request is over anyway)
         lastModificationTime = getTime(),
         errorCode = 1015,  -- SEINTERNAL
         errorMessage = 'File recall from tape has failed, please try again later',
         parent = 0
   WHERE castorFile = cfId
     AND status IN (4, 5); -- WAITTAPERECALL, WAITSUBREQ
END;
/


/* PL/SQL method implementing streamsToDo */
CREATE OR REPLACE PROCEDURE streamsToDo(res OUT castorTape.Stream_Cur) AS
  sId NUMBER;
  streams "numList";
BEGIN
   -- JUST rtcpclientd
  FOR s IN (SELECT id FROM Stream WHERE status = 0) LOOP
    BEGIN
      SELECT /*+ LEADING(Stream2TapeCopy TapeCopy DiskCopy FileSystem DiskServer) */
             s.id INTO sId
        FROM Stream2TapeCopy, TapeCopy, DiskCopy, FileSystem, DiskServer
       WHERE Stream2TapeCopy.parent = s.id
         AND Stream2TapeCopy.child = TapeCopy.id
         AND TapeCopy.castorFile = DiskCopy.CastorFile
         AND DiskCopy.fileSystem = FileSystem.id
         AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.id = FileSystem.DiskServer
         AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
         AND ROWNUM < 2;
      INSERT INTO StreamsToDoHelper VALUES (sId);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- just ignore as this stream has no available candidate
      NULL;
    END;
  END LOOP;
  SELECT id BULK COLLECT INTO Streams FROM StreamsToDoHelper;
  FORALL i in streams.FIRST..streams.LAST
    UPDATE Stream SET status = 1 -- WAITDRIVE
     WHERE id = streams(i);
  OPEN res FOR
    SELECT Stream.id, Stream.InitialSizeToTransfer, Stream.status,
           TapePool.id, TapePool.name
      FROM Stream, TapePool
     WHERE Stream.id MEMBER OF streams
       AND Stream.TapePool = TapePool.id;
END;
/


/* PL/SQL method implementing fileRecalled */
CREATE OR REPLACE PROCEDURE fileRecalled(tapecopyId IN INTEGER) AS
  subRequestId NUMBER;
  requestId NUMBER;
  dci NUMBER;
  reqType NUMBER;
  cfId NUMBER;
  fs NUMBER;
  gcw NUMBER;
  gcwProc VARCHAR(2048);
  ouid INTEGER;
  ogid INTEGER;
  svcClassId NUMBER;
  missingTCs INTEGER;
BEGIN
  SELECT SubRequest.id, SubRequest.request, DiskCopy.id,
         CastorFile.id, Castorfile.FileSize, TapeCopy.missingCopies
    INTO subRequestId, requestId, dci, cfId, fs, missingTCs
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2;  -- DISKCOPY_WAITTAPERECALL
  -- delete any previous failed diskcopy for this castorfile (due to failed recall attempts for instance)
  DELETE FROM Id2Type WHERE id IN (SELECT id FROM DiskCopy WHERE castorFile = cfId AND status = 4);
  DELETE FROM DiskCopy WHERE castorFile = cfId AND status = 4;  -- FAILED
  -- update diskcopy size and gweight
  SELECT Request.svcClass, euid, egid INTO svcClassId, ouid, ogid
    FROM (SELECT id, svcClass, euid, egid FROM StageGetRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StagePrepareToGetRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StageUpdateRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StagePrepareToUpdateRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StageRepackRequest) Request
   WHERE Request.id = requestId;
  gcwProc := castorGC.getRecallWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:size); END;'
    USING OUT gcw, IN fs;
  UPDATE DiskCopy
     SET status = 0,  -- DISKCOPY_STAGED
         lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
         gcWeight = gcw,
         diskCopySize = fs
   WHERE id = dci;
  -- determine the type of the request
  SELECT type INTO reqType FROM Id2Type WHERE id =
    (SELECT request FROM SubRequest WHERE id = subRequestId);
  IF reqType = 119 THEN  -- OBJ_StageRepackRequest
    startRepackMigration(subRequestId, cfId, dci, ouid, ogid);
  ELSE
    -- restart this subrequest if it's not a repack one
    UPDATE SubRequest
       SET status = 1, getNextStatus = 1,  -- SUBREQUEST_RESTART, GETNEXTSTATUS_FILESTAGED
           lastModificationTime = getTime(), parent = 0
     WHERE id = subRequestId;
    -- And trigger new migrations if missing tape copies were detected
    IF missingTCs > 0 THEN
      DECLARE
        tcId INTEGER;
      BEGIN
        UPDATE DiskCopy
           SET status = 10  -- DISKCOPY_CANBEMIGR
         WHERE id = dci;
        FOR i IN 1..missingTCs LOOP
          INSERT INTO TapeCopy (id, copyNb, castorFile, status)
          VALUES (ids_seq.nextval, 0, cfId, 0) -- TAPECOPY_CREATED
          RETURNING id INTO tcId;
          INSERT INTO Id2Type (id, type) VALUES (tcId, 30); -- OBJ_TapeCopy
        END LOOP;
      END;
    END IF;
  END IF;
  -- restart other requests waiting on this recall
  UPDATE /*+ INDEX(ST I_SUBREQUEST_PARENT) */ SubRequest ST
       SET status = 1, getNextStatus = 1,  -- SUBREQUEST_RESTART, GETNEXTSTATUS_FILESTAGED
           lastModificationTime = getTime(), parent = 0
   WHERE parent = subRequestId;
  -- Trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(cfId, ouid, ogid);
END;
/


/* PL/SQL method implementing resetStream */

CREATE OR REPLACE PROCEDURE resetStream (sid IN INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
BEGIN  
   DELETE FROM Stream WHERE id = sid;
   DELETE FROM Id2Type WHERE id = sid;
   UPDATE Tape SET status = 0, stream = null WHERE stream = sid;
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  -- constraint violation and we cannot delete the stream
  UPDATE Stream SET status = 6, tape = null, lastFileSystemChange = null WHERE id = sid; 
  UPDATE Tape SET status = 0, stream = null WHERE stream = sid;
END;
/


/* PL/SQL method implementing segmentsForTape */
CREATE OR REPLACE PROCEDURE segmentsForTape (tapeId IN INTEGER, segments
OUT castor.Segment_Cur) AS
  segs "numList";
  rows PLS_INTEGER := 500;
  CURSOR c1 IS
    SELECT Segment.id FROM Segment
    WHERE Segment.tape = tapeId AND Segment.status = 0 ORDER BY Segment.fseq
    FOR UPDATE;
BEGIN
   -- JUST rtcpclientd
  OPEN c1;
  FETCH c1 BULK COLLECT INTO segs LIMIT rows;
  CLOSE c1;

  IF segs.COUNT > 0 THEN
     UPDATE Tape SET status = 4 -- MOUNTED
       WHERE id = tapeId;
     FORALL j IN segs.FIRST..segs.LAST -- bulk update with the forall..
       UPDATE Segment SET status = 7 -- SELECTED
       WHERE id = segs(j);
  END IF;

  OPEN segments FOR
    SELECT fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm, segmCksum,
           errMsgTxt, errorCode, severity, blockId0, blockId1, blockId2, blockId3,
           creationTime, id, tape, copy, status, priority
      FROM Segment
     WHERE id IN (SELECT /*+ CARDINALITY(segsTable 5) */ * FROM TABLE(segs) segsTable);
END;
/


/* PL/SQL method implementing anySegmentsForTape */
CREATE OR REPLACE PROCEDURE anySegmentsForTape
(tapeId IN INTEGER, nb OUT INTEGER) AS
BEGIN
   -- JUST rtcpclientd
  SELECT count(*) INTO nb FROM Segment
  WHERE Segment.tape = tapeId
    AND Segment.status = 0;
  IF nb > 0 THEN
    UPDATE Tape SET status = 3 -- WAITMOUNT
    WHERE id = tapeId;
  END IF;
END;
/


/* PL/SQL method implementing failedSegments */
CREATE OR REPLACE PROCEDURE failedSegments
(segments OUT castor.Segment_Cur) AS
BEGIN
   -- JUST rtcpclientd
  OPEN segments FOR
    SELECT fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm, segmCksum,
           errMsgTxt, errorCode, severity, blockId0, blockId1, blockId2, blockId3,
           creationTime, id, tape, copy, status, priority
      FROM Segment
     WHERE Segment.status = 6; -- SEGMENT_FAILED
END;
/


/* PL/SQL procedure which is executed whenever a files has been written to tape by the migrator to
 * check, whether the file information has to be added to the NameServer or to replace an entry
 * (repack case)
 */
CREATE OR REPLACE PROCEDURE checkFileForRepack(fid IN INTEGER, ret OUT VARCHAR2) AS
  sreqid NUMBER;
BEGIN
   -- JUST rtcpclientd
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
  archiveSubReq(sreqid, 8); -- XXX this step is to be moved after and if the operation has been
                            -- XXX successful, once the migrator is properly rewritten
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
/

/* PL/SQL method implementing rtcpclientdCleanUp */
CREATE OR REPLACE PROCEDURE rtcpclientdCleanUp AS
  tpIds "numList";
BEGIN
  -- JUST rtcpclientd
  -- Deal with Migrations
  -- 1) Ressurect tapecopies for migration
  UPDATE TapeCopy SET status = 1 WHERE status = 3;
  -- 2) Clean up the streams
  UPDATE Stream SET status = 0 
   WHERE status NOT IN (0, 5, 6, 7) --PENDING, CREATED, STOPPED, WAITPOLICY
  RETURNING tape BULK COLLECT INTO tpIds;
  UPDATE Stream SET tape = NULL WHERE tape != 0;
  -- 3) Reset the tape for migration
  FORALL i IN tpIds.FIRST .. tpIds.LAST  
    UPDATE tape SET stream = 0, status = 0 WHERE status IN (2, 3, 4) AND id = tpIds(i);

  -- Deal with Recalls
  UPDATE Segment SET status = 0 WHERE status = 7; -- Resurrect SELECTED segment
  UPDATE Tape SET status = 1 WHERE tpmode = 0 AND status IN (2, 3, 4); -- Resurrect the tapes running for recall
  UPDATE Tape A SET status = 8 
   WHERE status IN (0, 6, 7) AND EXISTS
    (SELECT id FROM Segment WHERE status = 0 AND tape = A.id);
  COMMIT;
END;
/


/** Functions for the MigHunterDaemon **/

/** Cleanup before starting a new MigHunterDaemon **/
CREATE OR REPLACE PROCEDURE migHunterCleanUp(svcName IN VARCHAR2)
AS
  svcId NUMBER;
BEGIN
  SELECT id INTO svcId FROM SvcClass WHERE name = svcName;
  -- clean up tapecopies, WAITPOLICY reset into TOBEMIGRATED
  UPDATE
     /*+ LEADING(TC CF)
         INDEX_RS_ASC(CF PK_CASTORFILE_ID)
         INDEX_RS_ASC(TC I_TAPECOPY_STATUS) */ 
         TapeCopy TC
     SET status = 1
   WHERE status = 7 
     AND EXISTS (
       SELECT 'x' 
         FROM CastorFile CF
        WHERE TC.castorFile = CF.id
          AND CF.svcclass = svcId);
  -- clean up streams, WAITPOLICY reset into CREATED
  UPDATE Stream SET status = 5 WHERE status = 7 AND tapepool IN
   (SELECT svcclass2tapepool.child
      FROM svcclass2tapepool
     WHERE svcId = svcclass2tapepool.parent);
  COMMIT;
END;
/

/* Get input for python migration policy */
CREATE OR REPLACE PROCEDURE inputForMigrationPolicy
(svcclassName IN VARCHAR2,
 policyName OUT NOCOPY VARCHAR2,
 svcId OUT NUMBER,
 dbInfo OUT castorTape.DbMigrationInfo_Cur) AS
 tcIds "numList";
BEGIN
  -- do the same operation of getMigrCandidate and return the dbInfoMigrationPolicy
  -- we look first for repack condidates for this svcclass
  -- we update atomically WAITPOLICY
  SELECT SvcClass.migratorpolicy, SvcClass.id INTO policyName, svcId
    FROM SvcClass
   WHERE SvcClass.name = svcClassName;

  UPDATE
     /*+ LEADING(TC CF)
         INDEX_RS_ASC(CF PK_CASTORFILE_ID)
         INDEX_RS_ASC(TC I_TAPECOPY_STATUS) */ 
         TapeCopy TC 
     SET status = 7
   WHERE status IN (0, 1)
     AND (EXISTS
       (SELECT 'x' FROM SubRequest, StageRepackRequest
         WHERE StageRepackRequest.svcclass = svcId
           AND SubRequest.request = StageRepackRequest.id
           AND SubRequest.status = 12  -- SUBREQUEST_REPACK
           AND TC.castorfile = SubRequest.castorfile
      ) OR EXISTS (
        SELECT 'x'
          FROM CastorFile CF
         WHERE TC.castorFile = CF.id
           AND CF.svcClass = svcId)) AND rownum < 10000
    RETURNING TC.id -- CREATED / TOBEMIGRATED
    BULK COLLECT INTO tcIds;
  COMMIT;
  -- return the full resultset
  OPEN dbInfo FOR
    SELECT TapeCopy.id, TapeCopy.copyNb, CastorFile.lastknownfilename,
           CastorFile.nsHost, CastorFile.fileid, CastorFile.filesize
      FROM Tapecopy,CastorFile
     WHERE CastorFile.id = TapeCopy.castorfile
       AND TapeCopy.id IN 
         (SELECT /*+ CARDINALITY(tcidTable 5) */ * 
            FROM table(tcIds) tcidTable);
END;
/

/* Get input for python Stream Policy */
CREATE OR REPLACE PROCEDURE inputForStreamPolicy
(svcClassName IN VARCHAR2,
 policyName OUT NOCOPY VARCHAR2,
 runningStreams OUT INTEGER,
 maxStream OUT INTEGER,
 dbInfo OUT castorTape.DbStreamInfo_Cur)
AS
  tpId NUMBER; -- used in the loop
  tcId NUMBER; -- used in the loop
  streamId NUMBER; -- stream attached to the tapepool
  svcId NUMBER; -- id for the svcclass
  strIds "numList";
  tcNum NUMBER;
BEGIN
  -- info for policy
  SELECT streamPolicy, nbDrives, id INTO policyName, maxStream, svcId
    FROM SvcClass WHERE SvcClass.name = svcClassName;
  SELECT count(*) INTO runningStreams
    FROM Stream, SvcClass2TapePool
   WHERE Stream.TapePool = SvcClass2TapePool.child
     AND SvcClass2TapePool.parent = svcId
     AND Stream.status = 3;
  UPDATE stream SET status = 7
   WHERE Stream.status IN (4, 5, 6)
     AND Stream.id
      IN (SELECT Stream.id FROM Stream,SvcClass2TapePool
           WHERE Stream.Tapepool = SvcClass2TapePool.child
             AND SvcClass2TapePool.parent = svcId)
  RETURNING Stream.id BULK COLLECT INTO strIds;
  COMMIT;
  
  -- check for overloaded streams
  SELECT count(*) INTO tcNum FROM stream2tapecopy 
   WHERE parent IN (SELECT /*+ CARDINALITY(stridTable 5) */ * FROM TABLE(strIds) stridTable);
  IF (tcnum > 10000 * maxstream) AND (maxstream > 0) THEN
    -- emergency mode
    OPEN dbInfo FOR
      SELECT Stream.id, 10000, 10000, gettime
        FROM Stream
       WHERE Stream.id IN (SELECT /*+ CARDINALITY(stridTable 5) */ * FROM TABLE(strIds) stridTable)
         AND Stream.status = 7
       GROUP BY Stream.id;
  ELSE
  -- return for policy
  OPEN dbInfo FOR
    SELECT /*+ INDEX(CastorFile PK_CastorFile_Id) */ Stream.id,
           count(distinct Stream2TapeCopy.child),
           sum(CastorFile.filesize), gettime() - min(CastorFile.creationtime)
      FROM Stream2TapeCopy, TapeCopy, CastorFile, Stream
     WHERE Stream.id IN
        (SELECT /*+ CARDINALITY(stridTable 5) */ * FROM TABLE(strIds) stridTable)
       AND Stream2TapeCopy.child = TapeCopy.id
       AND TapeCopy.castorfile = CastorFile.id
       AND Stream.id = Stream2TapeCopy.parent
       AND Stream.status = 7
     GROUP BY Stream.id
   UNION ALL
    SELECT Stream.id, 0, 0, 0
      FROM Stream WHERE Stream.id IN
        (SELECT /*+ CARDINALITY(stridTable 5) */ * FROM TABLE(strIds) stridTable)
       AND Stream.status = 7
       AND NOT EXISTS 
        (SELECT 'x' FROM Stream2TapeCopy ST WHERE ST.parent = Stream.ID);
 END IF;         
END;
/

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
  oldSize NUMBER; -- value for a cloned stream
BEGIN
  retCode := 0;
  -- get streamFromSvcClass
  BEGIN
    SELECT id INTO svcId FROM SvcClass
     WHERE name = svcClassName AND ROWNUM < 2;
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
    retCode := -2 ; -- RTCPCLD_MSG_DATALIMIT
    RETURN;
  END IF;

  IF nbOldStream >= 0 AND (doClone = 1 OR nbMigrationCandidate > 0) THEN
    -- stream creator
    SELECT SvcClass.nbDrives INTO nbDrives FROM SvcClass WHERE id = svcId;
    IF nbDrives = 0 THEN
    	retCode := -3; -- RESTORE NEEDED
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
         -- tapepool without stream randomly chosen
          SELECT a INTO tpId
            FROM (
              SELECT TapePool.id AS a FROM TapePool,SvcClass2TapePool
               WHERE TapePool.id NOT IN (SELECT TapePool FROM Stream)
                 AND TapePool.id = SvcClass2TapePool.child
	         AND SvcClass2TapePool.parent = svcId
            ORDER BY dbms_random.value
	    ) WHERE ROWNUM < 2;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- at least one stream foreach tapepool
           SELECT tapepool INTO tpId
             FROM (
               SELECT tapepool, count(*) AS c
                 FROM Stream
                WHERE tapepool IN (
                  SELECT SvcClass2TapePool.child
                    FROM SvcClass2TapePool
                   WHERE SvcClass2TapePool.parent = svcId)
             GROUP BY tapepool
             ORDER BY c ASC, dbms_random.value)
           WHERE ROWNUM < 2;
	END;

        -- STREAM_CREATED
        INSERT INTO Stream
          (id, initialsizetotransfer, lastFileSystemChange, tape, lastFileSystemUsed,
           lastButOneFileSystemUsed, tapepool, status)
        VALUES (ids_seq.nextval, initSize, null, null, null, null, tpId, 5) RETURN id INTO strId;
        INSERT INTO Id2Type (id, type) values (strId,26); -- Stream type
    	IF doClone = 1 THEN
	  BEGIN
	    -- clone the new stream with one from the same tapepool
	    SELECT id, initialsizetotransfer INTO streamToClone, oldSize
              FROM Stream WHERE tapepool = tpId AND id != strId AND ROWNUM < 2;
            FOR tcId IN (SELECT child FROM Stream2TapeCopy
                          WHERE Stream2TapeCopy.parent = streamToClone)
            LOOP
              -- a take the first one, they are supposed to be all the same
              INSERT INTO stream2tapecopy (parent, child) VALUES (strId, tcId.child);
            END LOOP;
            UPDATE Stream SET initialSizeToTransfer = oldSize WHERE id = strId;
           EXCEPTION WHEN NO_DATA_FOUND THEN
  	    -- no stream to clone for this tapepool
  	    NULL;
	   END;
	END IF;
        nbOldStream := nbOldStream + 1;
        EXIT WHEN nbOldStream >= nbDrives;
      END LOOP;
    END IF;
  END IF;
END;
/

/* attach tapecopies to stream */
CREATE OR REPLACE PROCEDURE attachTapeCopiesToStreams
(tapeCopyIds IN castor."cnumList",
 tapePoolIds IN castor."cnumList")
AS
  streamId NUMBER; -- stream attached to the tapepool
  counter NUMBER := 0;
  unused NUMBER;
  nbStream NUMBER;
BEGIN
  -- add choosen tapecopies to all Streams associated to the tapepool used by the policy
  FOR i IN tapeCopyIds.FIRST .. tapeCopyIds.LAST LOOP
    BEGIN
      SELECT count(id) INTO nbStream FROM Stream
       WHERE Stream.tapepool = tapePoolIds(i);
      IF nbStream <> 0 THEN
        -- we have at least a stream for that tapepool
        SELECT id INTO unused
          FROM TapeCopy
         WHERE Status in (2,7) AND id = tapeCopyIds(i) FOR UPDATE;
        -- let's attach it to the different streams
        FOR streamId IN (SELECT id FROM Stream WHERE Stream.tapepool = tapePoolIds(i)) LOOP
          UPDATE TapeCopy SET Status = 2 WHERE Status = 7 AND id = tapeCopyIds(i);
          DECLARE CONSTRAINT_VIOLATED EXCEPTION;
          PRAGMA EXCEPTION_INIT (CONSTRAINT_VIOLATED, -1);
          BEGIN
            INSERT INTO stream2tapecopy (parent ,child) VALUES (streamId.id, tapeCopyIds(i));
          EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
            -- if the stream does not exist anymore
            UPDATE tapecopy SET status = 7 WHERE id = tapeCopyIds(i);
            -- it might also be that the tapecopy does not exist anymore
          END;
        END LOOP; -- stream loop
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Go on the tapecopy has been resurrected or migrated
      NULL;
    END;
    counter := counter + 1;
    IF counter = 100 THEN
      counter := 0;
      COMMIT;
    END IF;
  END LOOP; -- loop tapecopies

  -- resurrect the one never attached
  FORALL i IN tapeCopyIds.FIRST .. tapeCopyIds.LAST
    UPDATE TapeCopy SET Status = 1 WHERE id = tapeCopyIds(i) AND Status = 7;
  COMMIT;
END;
/

/* start choosen stream */
CREATE OR REPLACE PROCEDURE startChosenStreams
        (streamIds IN castor."cnumList", initSize IN NUMBER) AS
BEGIN
  FORALL i IN streamIds.FIRST .. streamIds.LAST
    UPDATE Stream
       SET status = 0, -- PENDING
           -- initialSize overwritten to initSize only if it is 0
           initialSizeToTransfer = decode(initialSizeToTransfer, 0, initSize, initialSizeToTransfer)
     WHERE Stream.status = 7 -- WAITPOLICY
       AND id = streamIds(i);
  COMMIT;
END;
/

/* stop chosen stream */
CREATE OR REPLACE PROCEDURE stopChosenStreams
        (streamIds IN castor."cnumList") AS
  nbTc NUMBER;
BEGIN
  FOR i IN streamIds.FIRST .. streamIds.LAST LOOP
    SELECT count(*) INTO nbTc FROM stream2tapecopy WHERE parent = streamIds(i);
    IF nbTc = 0 THEN
      DELETE FROM Stream WHERE id = streamIds(i);
    ELSE
      UPDATE Stream
         SET status = 6 -- STOPPED
       WHERE Stream.status = 7 -- WAITPOLICY
         AND id = streamIds(i);
    END IF;
    COMMIT;
  END LOOP;
END;
/

/* resurrect Candidates */
CREATE OR REPLACE PROCEDURE resurrectCandidates
(migrationCandidates IN castor."cnumList") -- all candidate before applying the policy
AS
  unused "numList";
BEGIN
  FORALL i IN migrationCandidates.FIRST .. migrationCandidates.LAST
    UPDATE TapeCopy SET Status = 1 WHERE Status = 7 AND id = migrationCandidates(i);
  COMMIT;
END;
/

/* invalidate tape copies */
CREATE OR REPLACE PROCEDURE invalidateTapeCopies
(tapecopyIds IN castor."cnumList") -- tapecopies not in the nameserver
AS
  srId NUMBER;
BEGIN
  -- tapecopies
  FORALL i IN tapecopyIds.FIRST .. tapecopyIds.LAST
    UPDATE TapeCopy SET status = 6 WHERE id = tapecopyIds(i) AND status = 7;

  -- repack subrequests to be archived
  FOR i IN tapecopyIds.FIRST .. tapecopyIds.LAST LOOP
    BEGIN
      SELECT subrequest.id INTO srId FROM subrequest, tapecopy 
       WHERE subrequest.castorfile = tapecopy.castorfile
         AND tapecopy.id = tapecopyIds(i)
         AND subrequest.status = 12;
      archivesubreq(srId,9);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- no repack pending
      NULL;
    END;
  END LOOP;
  COMMIT;
END;
/


/** Functions for the RecHandlerDaemon **/

/* Get input for python recall policy */
CREATE OR REPLACE PROCEDURE inputForRecallPolicy(dbInfo OUT castorTape.DbRecallInfo_Cur) AS
  svcId NUMBER;
BEGIN
  OPEN dbInfo FOR
    SELECT
       /*+ NO_USE_MERGE(TAPE SEGMENT TAPECOPY CASTORFILE)
           NO_USE_HASH(TAPE SEGMENT TAPECOPY CASTORFILE)
           INDEX_RS_ASC(SEGMENT I_SEGMENT_TAPE)
           INDEX_RS_ASC(TAPE I_TAPE_STATUS)
           INDEX_RS_ASC(TAPECOPY PK_TAPECOPY_ID)
           INDEX_RS_ASC(CASTORFILE PK_CASTORFILE_ID) */
       Tape.id, Tape.vid, count(distinct segment.id), sum(CastorFile.fileSize),
       getTime() - min(Segment.creationTime), max(Segment.priority)
      FROM TapeCopy, CastorFile, Segment, Tape
     WHERE Tape.id = Segment.tape
       AND TapeCopy.id = Segment.copy
       AND CastorFile.id = TapeCopy.castorfile
       AND Tape.status IN (1, 2, 8)  -- PENDING, WAITDRIVE, WAITPOLICY
       AND Segment.status = 0  -- SEGMENT_UNPROCESSED
     GROUP BY Tape.id, Tape.vid;
END;
/

/* resurrect tapes */
CREATE OR REPLACE PROCEDURE resurrectTapes
(tapeIds IN castor."cnumList")
AS
BEGIN
  FORALL i IN tapeIds.FIRST .. tapeIds.LAST
    UPDATE Tape SET status = 1 WHERE status = 8 AND id = tapeIds(i);
  COMMIT;
END;
/


/* clean the db for repack, it is used as workaround because of repack abort limitation */
CREATE OR REPLACE PROCEDURE removeAllForRepack (inputVid IN VARCHAR2) AS
  reqId NUMBER;
  srId NUMBER;
  cfIds "numList";
  dcIds "numList";
  tcIds "numList";
  segIds "numList";
  tapeIds "numList";
BEGIN
  -- look for the request. If not found, raise NO_DATA_FOUND error;
  -- note that if the request is over (all in 9,11) or not started (0), nothing is done
  SELECT id INTO reqId 
    FROM StageRepackRequest R 
   WHERE repackVid = inputVid
     AND EXISTS 
       (SELECT 1 FROM SubRequest 
         WHERE request = R.id AND status IN (4, 12));  -- WAITTAPERECALL, REPACK
  -- fail subrequests
  UPDATE Subrequest SET status = 9
   WHERE request = reqId AND status NOT IN (9, 11)
  RETURNING castorFile, diskcopy BULK COLLECT INTO cfIds, dcIds;
  SELECT id INTO srId 
    FROM SubRequest 
   WHERE request = reqId AND ROWNUM = 1;
  archiveSubReq(srId, 9);

  -- fail related diskcopies
  FORALL i IN dcIds.FIRST .. dcids.LAST
    UPDATE DiskCopy
       SET status = decode(status, 2, 4, 7) -- WAITTAPERECALL->FAILED, otherwise INVALID
     WHERE id = dcIds(i);

  -- delete tapecopy from id2type and get the ids
  DELETE FROM id2type WHERE id IN 
   (SELECT id FROM TAPECOPY
     WHERE castorfile IN (SELECT /*+ CARDINALITY(cfIdsTable 5) */ *
                            FROM TABLE(cfIds) cfIdsTable))
  RETURNING id BULK COLLECT INTO tcIds;

  -- detach tapecopies from stream
  FORALL i IN tcids.FIRST .. tcids.LAST
    DELETE FROM stream2tapecopy WHERE child=tcIds(i);

  -- delete tapecopies
  FORALL i IN tcids.FIRST .. tcids.LAST
    DELETE FROM tapecopy WHERE id = tcIds(i);

  -- delete segments using the tapecopy link
  DELETE FROM segment WHERE copy IN
   (SELECT /*+ CARDINALITY(tcIdsTable 5) */ *
      FROM TABLE(tcids) tcIdsTable)
  RETURNING id, tape BULK COLLECT INTO segIds, tapeIds;

  FORALL i IN segIds.FIRST .. segIds.LAST
    DELETE FROM id2type WHERE id = segIds(i);

  -- delete the orphan segments (this should not be necessary)
  DELETE FROM segment WHERE tape IN 
    (SELECT id FROM tape WHERE vid = inputVid) 
  RETURNING id BULK COLLECT INTO segIds;
  FORALL i IN segIds.FIRST .. segIds.LAST
    DELETE FROM id2type WHERE id = segIds(i);

  -- update the tape as not used
  UPDATE tape SET status = 0 WHERE vid = inputVid AND tpmode = 0;
  -- update other tapes which could have been involved
  FORALL i IN tapeIds.FIRST .. tapeIds.LAST
    UPDATE tape SET status = 0 WHERE id = tapeIds(i);
  -- commit the transation
  COMMIT;
END;
/

