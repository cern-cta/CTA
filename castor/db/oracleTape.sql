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
  /**
   * The StreamForPolicy record is used to pass information about a specific
   * stream to the stream-policy Python-function of a service-class.  The
   * Python-function is responsible for deciding whether or not the stream
   * should be started.
   */
  TYPE StreamForPolicy IS RECORD (
    id                  NUMBER,
    numTapeCopies       NUMBER,
    totalBytes          NUMBER,
    ageOfOldestTapeCopy NUMBER,
    tapePool            NUMBER);
  TYPE StreamForPolicy_Cur IS REF CURSOR RETURN StreamForPolicy;
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
CREATE OR REPLACE TRIGGER TR_TapeCopy_VID
BEFORE INSERT OR UPDATE OF Status ON TapeCopy
FOR EACH ROW
BEGIN
  /* Enforce the state integrity of VID in state transitions */
  
  /* rtcpclientd is given full exception, no check */
  IF rtcpclientdIsRunning THEN RETURN; END IF;
  
  CASE :new.status
    WHEN  tconst.TAPECOPY_SELECTED THEN
      /* The VID MUST be defined when the tapecopy gets selected */
      IF :new.VID IS NULL THEN
        RAISE_APPLICATION_ERROR(-20119,
          'Moving/creating (in)to TAPECOPY_SELECTED State without a VID (TC.ID: '||
          :new.ID||' VID:'|| :old.VID||'=>'||:new.VID||' Status:'||:old.status||'=>'||:new.status||')');
      END IF;
    WHEN tconst.TAPECOPY_STAGED THEN
       /* The VID MUST be defined when the tapecopy goes to staged */
       IF :new.VID IS NULL THEN
         RAISE_APPLICATION_ERROR(-20119,
          'Moving/creating (in)to TAPECOPY_STAGED State without a VID (TC.ID: '||
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
          'Moving/creating (in)to TapeCopy state where VID makes no sense, yet VID!=NULL (TC.ID: '||
          :new.ID||' VID:'|| :old.VID||'=>'||:new.VID||' Status:'||:old.status||'=>'||:new.status||')');
      END IF;
  END CASE;
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
     AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
     AND FileSystem.id = DiskCopy.filesystem
     AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
     AND DiskCopy.castorfile = TapeCopy.castorfile
     AND Stream2TapeCopy.child = TapeCopy.id
     AND Stream2TapeCopy.parent = streamId
     AND TapeCopy.status = tconst.TAPECOPY_WAITINSTREAMS
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
                                                 diskServerName OUT NOCOPY VARCHAR2, mountPoint OUT NOCOPY VARCHAR2,
                                                 path OUT NOCOPY VARCHAR2, dci OUT INTEGER,
                                                 castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                 nsHost OUT NOCOPY VARCHAR2, fileSize OUT INTEGER,
                                                 tapeCopyId OUT INTEGER, lastUpdateTime OUT INTEGER) AS
  fileSystemId INTEGER := 0;
  diskServerId NUMBER;
  lastFSChange NUMBER;
  lastFSUsed NUMBER;
  lastButOneFSUsed NUMBER;
  findNewFS NUMBER := 1;
  nbMigrators NUMBER := 0;
  unused NUMBER;
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
BEGIN
  tapeCopyId := 0;
  -- First try to see whether we should reuse the same filesystem as last time
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
           AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
           AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING);
        -- we are within the time range, so we try to reuse the filesystem
        SELECT /*+ FIRST_ROWS(1)  LEADING(D T ST) */
               D.path, D.id, D.castorfile, T.id
          INTO path, dci, castorFileId, tapeCopyId
          FROM DiskCopy D, TapeCopy T, Stream2TapeCopy ST
         WHERE decode(D.status, 10, D.status, NULL) = dconst.DISKCOPY_CANBEMIGR
         -- 10 = dconst.DISKCOPY_CANBEMIGR. Has to be kept as a hardcoded number in order to use a function-based index.
           AND D.filesystem = lastButOneFSUsed
           AND ST.parent = streamId
           AND T.status = tconst.TAPECOPY_WAITINSTREAMS
           AND ST.child = T.id
           AND T.castorfile = D.castorfile
           AND ROWNUM < 2 FOR UPDATE OF t.id NOWAIT;
        SELECT CastorFile.FileId, CastorFile.NsHost, CastorFile.FileSize,
               CastorFile.lastUpdateTime
          INTO fileId, nsHost, fileSize, lastUpdateTime
          FROM CastorFile
         WHERE Id = castorFileId;
        -- we found one, no need to go for new filesystem
        findNewFS := 0;
      EXCEPTION WHEN NO_DATA_FOUND OR LockError THEN
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
        AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
        AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
      ORDER BY -- first prefer diskservers where no migrator runs and filesystems with no recalls
               DiskServer.nbMigratorStreams ASC, FileSystem.nbRecallerStreams ASC,
               -- then order by rate as defined by the function
               fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams, FileSystem.nbWriteStreams,
                              FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC,
               -- finally use randomness to avoid preferring always the same FS
               DBMS_Random.value) LOOP
       BEGIN
         -- lock the complete diskServer as we will update all filesystems
         SELECT id INTO unused FROM DiskServer WHERE id = f.DiskServerId FOR UPDATE NOWAIT;
         SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
                f.diskServerId, f.name, f.mountPoint, f.fileSystemId, D.path, D.id, D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, C.lastUpdateTime
           INTO diskServerId, diskServerName, mountPoint, fileSystemId, path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
           FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
          WHERE decode(D.status, 10, D.status, NULL) = dconst.DISKCOPY_CANBEMIGR
          -- 10 = dconst.DISKCOPY_CANBEMIGR. Has to be kept as a hardcoded number in order to use a function-based index.
            AND D.filesystem = f.fileSystemId
            AND StT.parent = streamId
            AND T.status = tconst.TAPECOPY_WAITINSTREAMS
            AND StT.child = T.id
            AND T.castorfile = D.castorfile
            AND C.id = D.castorfile
            AND ROWNUM < 2;
         -- found something on this filesystem, no need to go on
         diskServerId := f.DiskServerId;
         fileSystemId := f.fileSystemId;
         EXIT;
       EXCEPTION WHEN NO_DATA_FOUND OR lockError THEN
         -- either the filesystem is already locked or we found nothing,
         -- let's go to the next one
         NULL;
       END;
    END LOOP;
  END IF;

  IF tapeCopyId = 0 THEN
    -- Nothing found, reset last filesystems used and exit
    UPDATE Stream
       SET lastFileSystemUsed = 0, lastButOneFileSystemUsed = 0 -- XXX Should this not be NULL?
     WHERE id = streamId;
    RETURN;
  END IF;

  -- Here we found a tapeCopy and we process it
  -- update status of selected tapecopy and stream
  UPDATE TapeCopy SET status = tconst.TAPECOPY_SELECTED
   WHERE id = tapeCopyId;
  IF findNewFS = 1 THEN
    UPDATE Stream
       SET status = tconst.STREAM_RUNNING,
           lastFileSystemUsed = fileSystemId,
           lastButOneFileSystemUsed = lastFileSystemUsed,
           lastFileSystemChange = getTime()
     WHERE id = streamId AND status IN (tconst.STREAM_WAITMOUNT,tconst.STREAM_RUNNING);
  ELSE
    -- only update status
    UPDATE Stream
       SET status = tconst.STREAM_RUNNING
     WHERE id = streamId AND status IN (tconst.STREAM_WAITMOUNT,tconst.STREAM_RUNNING);
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
                                                   diskServerName OUT NOCOPY VARCHAR2, mountPoint OUT NOCOPY VARCHAR2,
                                                   path OUT NOCOPY VARCHAR2, dci OUT INTEGER,
                                                   castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                   nsHost OUT NOCOPY VARCHAR2, fileSize OUT INTEGER,
                                                   tapeCopyId OUT INTEGER, lastUpdateTime OUT INTEGER) AS
  varFSId INTEGER := 0;
  varDSId NUMBER;
  varFsDiskServer NUMBER; /* XXX TODO FIXME This variable is used uninitialized at the end of the function. Was already the case in revision 21389 */
  varLastFSChange NUMBER;
  varLastFSUsed NUMBER;
  varLastButOneFSUsed NUMBER;
  varFindNewFS NUMBER := 1;
  varNbMigrators NUMBER := 0;
  unused NUMBER;
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
BEGIN
  tapeCopyId := 0;
  -- First try to see whether we should reuse the same filesystem as last time
  SELECT lastFileSystemChange, lastFileSystemUsed, lastButOneFileSystemUsed
    INTO varLastFSChange, varLastFSUsed, varLastButOneFSUsed
    FROM Stream WHERE id = streamId;
  IF getTime() < varLastFSChange + 1800 THEN
    SELECT (SELECT count(*) FROM stream WHERE lastFileSystemUsed = varLastFSUsed)
      INTO varNbMigrators FROM DUAL;
    -- only go if we are the only migrator on the box
    IF varNbMigrators = 1 THEN
      BEGIN
        -- check states of the diskserver and filesystem and get mountpoint and diskserver name
        SELECT diskserver.id, name, mountPoint, FileSystem.id
          INTO varDSId, diskServerName, mountPoint, varFSId
          FROM FileSystem, DiskServer
         WHERE FileSystem.diskServer = DiskServer.id
           AND FileSystem.id = varLastFSUsed
           AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
           AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING);
        -- we are within the time range, so we try to reuse the filesystem
        SELECT /*+ ORDERED USE_NL(D T) INDEX(T I_TapeCopy_CF_Status_2)
                   INDEX(ST I_Stream2TapeCopy_PC) */
               D.path, D.diskcopy_id, D.castorfile, T.id
          INTO path, dci, castorFileId, tapeCopyId
          FROM (SELECT /*+ INDEX(DK I_DiskCopy_FS_Status_10) */
                       DK.path path, DK.id diskcopy_id, DK.castorfile
                  FROM DiskCopy DK
                 WHERE decode(DK.status, 10, DK.status, NULL) = dconst.DISKCOPY_CANBEMIGR
                 -- 10 = dconst.DISKCOPY_CANBEMIGR. Has to be kept as a hardcoded number in order to use a function-based index.
                   AND DK.filesystem = varLastFSUsed) D, TapeCopy T, Stream2TapeCopy ST
         WHERE T.castorfile = D.castorfile
           AND ST.child = T.id
           AND ST.parent = streamId
           AND decode(T.status, 2, T.status, NULL) = tconst.TAPECOPY_WAITINSTREAMS
           -- 2 = tconst.TAPECOPY_WAITINSTREAMS. Has to be kept as a hardcoded number in order to use a function-based index.
           AND ROWNUM < 2 FOR UPDATE OF T.id NOWAIT;   
        SELECT C.fileId, C.nsHost, C.fileSize, C.lastUpdateTime
          INTO fileId, nsHost, fileSize, lastUpdateTime
          FROM castorfile C
         WHERE castorfileId = C.id;
        -- we found one, no need to go for new filesystem
        varFindNewFS := 0;
      EXCEPTION WHEN NO_DATA_FOUND OR LockError THEN
        -- found no tapecopy or diskserver, filesystem are down. We'll go through the normal selection
        NULL;
      END;
    END IF;
  END IF;
  IF varFindNewFS = 1 THEN
    -- We try first to reuse the diskserver of the lastFSUsed, even if we change filesystem
    FOR f IN (
      SELECT FileSystem.id AS FileSystemId, DiskServer.id AS DiskServerId, DiskServer.name, FileSystem.mountPoint
        FROM FileSystem, DiskServer
       WHERE FileSystem.diskServer = DiskServer.id
         AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
         AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
         AND DiskServer.id = varLastButOneFSUsed) LOOP
       BEGIN
         -- lock the complete diskServer as we will update all filesystems
         SELECT id INTO unused FROM DiskServer
          WHERE id = f.DiskServerId FOR UPDATE NOWAIT;
         SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
                f.diskServerId, f.name, f.mountPoint, f.fileSystemId, D.path, D.id, D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, C.lastUpdateTime
           INTO varDSId, diskServerName, mountPoint, varFSId, path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
           FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
          WHERE decode(D.status, 10, D.status, NULL) = dconst.DISKCOPY_CANBEMIGR
          -- 10 = dconst.DISKCOPY_CANBEMIGR. Has to be kept as a hardcoded number in order to use a function-based index.
            AND D.filesystem = f.fileSystemId
            AND StT.parent = streamId
            AND T.status = tconst.TAPECOPY_WAITINSTREAMS
            AND StT.child = T.id
            AND T.castorfile = D.castorfile
            AND C.id = D.castorfile
            AND ROWNUM < 2;
         -- found something on this filesystem, no need to go on
         varDSId := f.DiskServerId;
         varFSId := f.fileSystemId;
         EXIT;
       EXCEPTION WHEN NO_DATA_FOUND OR lockError THEN
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
         AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
         AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
       ORDER BY -- first prefer diskservers where no migrator runs and filesystems with no recalls
                DiskServer.nbMigratorStreams ASC, FileSystem.nbRecallerStreams ASC,
                -- then order by rate as defined by the function
                fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams, FileSystem.nbWriteStreams,
                               FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC,
                -- finally use randomness to avoid preferring always the same FS
                DBMS_Random.value) LOOP
       BEGIN
         -- lock the complete diskServer as we will update all filesystems
         SELECT id INTO unused FROM DiskServer WHERE id = f.DiskServerId FOR UPDATE NOWAIT;
         SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
                f.diskServerId, f.name, f.mountPoint, f.fileSystemId, D.path, D.id, D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, C.lastUpdateTime
           INTO varDSId, diskServerName, mountPoint, varFSId, path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
           FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
          WHERE decode(D.status, 10, D.status, NULL) = dconst.DISKCOPY_CANBEMIGR
          -- 10 = dconst.DISKCOPY_CANBEMIGR. Has to be kept as a hardcoded number in order to use a function-based index.
            AND D.filesystem = f.fileSystemId
            AND StT.parent = streamId
            AND T.status = tconst.TAPECOPY_WAITINSTREAMS
            AND StT.child = T.id
            AND T.castorfile = D.castorfile
            AND C.id = D.castorfile
            AND ROWNUM < 2;
         -- found something on this filesystem, no need to go on
         varDSId := f.DiskServerId;
         varFSId := f.fileSystemId;
         EXIT;
       EXCEPTION WHEN NO_DATA_FOUND OR lockError THEN
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
  UPDATE TapeCopy SET status = tconst.TAPECOPY_SELECTED
   WHERE id = tapeCopyId;
  IF varFindNewFS = 1 THEN
    UPDATE Stream
       SET status = tconst.STREAM_RUNNING,
           lastFileSystemUsed = varFSId,
           lastButOneFileSystemUsed = varLastFSUsed,
           lastFileSystemChange = getTime()
     WHERE id = streamId AND status IN (tconst.STREAM_WAITMOUNT, tconst.STREAM_RUNNING);
  ELSE
    -- only update status
    UPDATE Stream
       SET status = tconst.STREAM_RUNNING
     WHERE id = streamId AND status IN (tconst.STREAM_WAITMOUNT, tconst.STREAM_RUNNING);
  END IF;
  -- detach the tapecopy from the stream now that it is SELECTED;
  DELETE FROM Stream2TapeCopy
   WHERE child = tapeCopyId;

  -- Update Filesystem state
  updateFSMigratorOpened(varFsDiskServer, varFSId, 0); /* XXX TODO FIXME This variable is used uninitialized at the end of the function. Was already the case in revision 21389 */
END;
/

/* repack migration candidate selection policy */
CREATE OR REPLACE PROCEDURE repackMigrSelPolicy(streamId IN INTEGER,
                                                diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                path OUT VARCHAR2, dci OUT INTEGER,
                                                castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                tapeCopyId OUT INTEGER, lastUpdateTime OUT INTEGER) AS
  varFSId INTEGER := 0;
  varDSId NUMBER;
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
        AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
        AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
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
        INTO varDSId, diskServerName, mountPoint, varFSId, path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
        FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
       WHERE decode(D.status, 10, D.status, NULL) = dconst.DISKCOPY_CANBEMIGR
       -- 10 = dconst.DISKCOPY_CANBEMIGR. Has to be kept as a hardcoded number in order to use a function-based index.
         AND D.filesystem = f.fileSystemId
         AND StT.parent = streamId
         AND T.status = tconst.TAPECOPY_WAITINSTREAMS
         AND StT.child = T.id
         AND T.castorfile = D.castorfile
         AND C.id = D.castorfile
         AND ROWNUM < 2;
      -- found something on this filesystem, no need to go on
      varDSId := f.DiskServerId;
      varFSId := f.fileSystemId;
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
  UPDATE TapeCopy SET status = tconst.TAPECOPY_SELECTED
   WHERE id = tapeCopyId;
  UPDATE Stream
     SET status = tconst.STREAM_RUNNING,
         lastFileSystemUsed = varFSId,
         lastButOneFileSystemUsed = lastFileSystemUsed,
         lastFileSystemChange = getTime()
   WHERE id = streamId AND status IN (tconst.STREAM_WAITMOUNT, tconst.STREAM_RUNNING);
  -- detach the tapecopy from the stream now that it is SELECTED;
  DELETE FROM Stream2TapeCopy
   WHERE child = tapeCopyId;

  -- Update Filesystem state
  updateFSMigratorOpened(varDSId, varFSId, 0);
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
      updateFsRecallerOpened(fsDiskServer, fileSystemId, 0); -- XXX Is this a status?
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
  UPDATE DiskCopy SET status = dconst.DISKCOPY_FAILED
   WHERE castorFile = cfId
     AND status = dconst.DISKCOPY_WAITTAPERECALL;
  -- Drop tape copies. Ideally, we should keep some track that
  -- the recall failed in order to prevent future recalls until some
  -- sort of manual intervention. For the time being, as we can't
  -- say whether the failure is fatal or not, we drop everything
  -- and we won't deny a future request for recall.
  deleteTapeCopies(cfId);
  UPDATE SubRequest 
     SET status = dconst.SUBREQUEST_FAILED,
         getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED, -- (not strictly correct but the request is over anyway)
         lastModificationTime = getTime(),
         errorCode = 1015,  -- SEINTERNAL
         errorMessage = 'File recall from tape has failed, please try again later',
         parent = 0
   WHERE castorFile = cfId
     AND status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_WAITSUBREQ);
END;
/

/* PL/SQL method implementing streamsToDo */
CREATE OR REPLACE PROCEDURE streamsToDo(res OUT castorTape.Stream_Cur) AS
  sId NUMBER;
  streams "numList";
BEGIN
   -- JUST rtcpclientd
  FOR s IN (SELECT id FROM Stream WHERE status = tconst.STREAM_PENDING) LOOP
    BEGIN
      SELECT /*+ LEADING(Stream2TapeCopy TapeCopy DiskCopy FileSystem DiskServer) */
             s.id INTO sId
        FROM Stream2TapeCopy, TapeCopy, DiskCopy, FileSystem, DiskServer
       WHERE Stream2TapeCopy.parent = s.id
         AND Stream2TapeCopy.child = TapeCopy.id
         AND TapeCopy.castorFile = DiskCopy.CastorFile
         AND DiskCopy.fileSystem = FileSystem.id
         AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
         AND DiskServer.id = FileSystem.DiskServer
         AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
         AND ROWNUM < 2;
      INSERT INTO StreamsToDoHelper VALUES (sId);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- just ignore as this stream has no available candidate
      NULL;
    END;
  END LOOP;
  SELECT id BULK COLLECT INTO Streams FROM StreamsToDoHelper;
  FORALL i in streams.FIRST..streams.LAST
    UPDATE Stream SET status = tconst.STREAM_WAITDRIVE
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
     AND DiskCopy.status = dconst.DISKCOPY_WAITTAPERECALL;
  -- delete any previous failed diskcopy for this castorfile (due to failed recall attempts for instance)
  DELETE FROM Id2Type WHERE id IN (SELECT id FROM DiskCopy WHERE castorFile = cfId AND status = dconst.DISKCOPY_FAILED);
  DELETE FROM DiskCopy WHERE castorFile = cfId AND status = dconst.DISKCOPY_FAILED;
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
     SET status = dconst.DISKCOPY_STAGED,
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
       SET status = dconst.SUBREQUEST_RESTART,
           getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
           lastModificationTime = getTime(), parent = 0
     WHERE id = subRequestId;
    -- And trigger new migrations if missing tape copies were detected
    IF missingTCs > 0 THEN
      DECLARE
        tcId INTEGER;
      BEGIN
        UPDATE DiskCopy
           SET status = dconst.DISKCOPY_CANBEMIGR
         WHERE id = dci;
        FOR i IN 1..missingTCs LOOP
          INSERT INTO TapeCopy (id, copyNb, castorFile, status)
          VALUES (ids_seq.nextval, 0, cfId, tconst.TAPECOPY_CREATED)
          RETURNING id INTO tcId;
          INSERT INTO Id2Type (id, type) VALUES (tcId, 30); -- OBJ_TapeCopy
        END LOOP;
      END;
    END IF;
  END IF;
  -- restart other requests waiting on this recall
  UPDATE /*+ INDEX(ST I_SUBREQUEST_PARENT) */ SubRequest ST
       SET status = dconst.SUBREQUEST_RESTART,
           getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
           lastModificationTime = getTime(), parent = 0
   WHERE parent = subRequestId;
  -- Trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(cfId, ouid, ogid);
END;
/

CREATE OR REPLACE PROCEDURE deleteOrStopStream(streamId IN INTEGER) AS
-- If the specified stream has no tape copies then this procedure deletes it,
-- else if the stream has tape copies then this procedure sets its status to
-- STREAM_STOPPED.  In both cases the associated tape is detached from the
-- stream and its status is set to TAPE_UNUSED.

  CHILD_RECORD_FOUND EXCEPTION;
  PRAGMA EXCEPTION_INIT(CHILD_RECORD_FOUND, -02292);
  DEADLOCK_DETECTED EXCEPTION;
  PRAGMA EXCEPTION_INIT(DEADLOCK_DETECTED, -00060);
  unused NUMBER;

BEGIN
  -- Try to take a lock on the stream, taking note that the stream may already
  -- have been delete because the migrator, rtcpclientd and mighunterd race to
  -- delete streams
  BEGIN
    SELECT id INTO unused FROM Stream WHERE id = streamId FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Return because the stream has already been deleted
    RETURN;
  END;

  -- Try to delete the stream.  If the mighunter daemon is running in
  -- rtcpclientd mode, then this delete may fail for two expected reasons.  The
  -- mighunterd daemon may have added more tape copies in the meantime and
  -- will therefore cause a CHILD_RECORD_FOUND exception due to the
  -- corresponding entries in the Stream2TapeCopy table.  The mighunter daemon
  -- may be adding new tape copies right this moment and will therefore cause a
  -- DEADLOCK_DETECTED exception.
  BEGIN
    DELETE FROM Stream  WHERE id = streamId;
    DELETE FROM Id2Type WHERE id = streamId;
  EXCEPTION
    -- When the stream cannot be deleted
    WHEN CHILD_RECORD_FOUND OR DEADLOCK_DETECTED THEN
      -- Stop the stream and reset its tape link and last file system change
      UPDATE Stream
        SET
          status = TCONST.STREAM_STOPPED,
          tape = NULL,
          lastFileSystemChange = NULL
        WHERE
          id = streamId;
  END;

  -- Complete the detachment of the tape
  UPDATE Tape
    SET status = TCONST.TAPE_UNUSED, stream = NULL
    WHERE stream = streamId;
END deleteOrStopStream;
/

/* PL/SQL method implementing resetStream */
CREATE OR REPLACE PROCEDURE resetStream (streamId IN INTEGER) AS
BEGIN
  deleteOrStopStream(streamId);
END;
/

/* PL/SQL method implementing segmentsForTape */
CREATE OR REPLACE PROCEDURE segmentsForTape (tapeId IN INTEGER, segments
OUT castor.Segment_Cur) AS
  segs "numList";
  rows PLS_INTEGER := 500;
  CURSOR c1 IS
    SELECT Segment.id FROM Segment
     WHERE Segment.tape = tapeId AND Segment.status = tconst.SEGMENT_UNPROCESSED ORDER BY Segment.fseq
    FOR UPDATE;
BEGIN
  -- JUST rtcpclientd
  OPEN c1;
  FETCH c1 BULK COLLECT INTO segs LIMIT rows;
  CLOSE c1;

  IF segs.COUNT > 0 THEN
    UPDATE Tape SET status = tconst.TAPE_MOUNTED
     WHERE id = tapeId;
    FORALL j IN segs.FIRST..segs.LAST -- bulk update with the forall..
      UPDATE Segment SET status = tconst.SEGMENT_SELECTED
       WHERE id = segs(j);
  END IF;

  OPEN segments FOR
    SELECT fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm,
           segmCksum, errMsgTxt, errorCode, severity, blockId0, blockId1,
           blockId2, blockId3, creationTime, id, tape, copy, status, priority
      FROM Segment
     WHERE id IN (SELECT /*+ CARDINALITY(segsTable 5) */ *
                    FROM TABLE(segs) segsTable);
END;
/

/* PL/SQL method implementing anySegmentsForTape */
CREATE OR REPLACE PROCEDURE anySegmentsForTape
(tapeId IN INTEGER, nb OUT INTEGER) AS
BEGIN
  -- JUST rtcpclientd
  SELECT count(*) INTO nb FROM Segment
   WHERE Segment.tape = tapeId
     AND Segment.status = tconst.SEGMENT_UNPROCESSED;
  IF nb > 0 THEN
    UPDATE Tape SET status = tconst.TAPE_WAITMOUNT
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
    SELECT fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm,
           segmCksum, errMsgTxt, errorCode, severity, blockId0, blockId1,
           blockId2, blockId3, creationTime, id, tape, copy, status, priority
      FROM Segment
     WHERE Segment.status = tconst.SEGMENT_FAILED;
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
     AND diskcopy.status = dconst.DISKCOPY_CANBEMIGR
     AND subrequest.status = dconst.SUBREQUEST_REPACK
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
  unused VARCHAR2(2048);
BEGIN
  SELECT value INTO unused
    FROM CastorConfig
   WHERE class = 'tape'
     AND KEY = 'interfaceDaemon'
     AND value = 'rtcpclientd';
  -- JUST rtcpclientd
  -- Deal with Migrations
  -- 1) Ressurect tapecopies for migration
  UPDATE TapeCopy SET status = tconst.TAPECOPY_TOBEMIGRATED WHERE status = tconst.TAPECOPY_SELECTED;
  -- 2) Clean up the streams
  UPDATE Stream SET status = tconst.STREAM_PENDING 
   WHERE status NOT IN (tconst.STREAM_PENDING, tconst.STREAM_CREATED, tconst.STREAM_STOPPED, tconst.STREAM_WAITPOLICY)
  RETURNING tape BULK COLLECT INTO tpIds;
  UPDATE Stream SET tape = NULL WHERE tape != 0;
  -- 3) Reset the tape for migration
  FORALL i IN tpIds.FIRST .. tpIds.LAST  
    UPDATE tape SET stream = 0, status = tconst.TAPE_UNUSED -- XXX Should not be NULL?
     WHERE status IN (tconst.TAPE_WAITDRIVE, tconst.TAPE_WAITMOUNT, tconst.TAPE_MOUNTED) AND id = tpIds(i);

  -- Deal with Recalls
  UPDATE Segment SET status = tconst.SEGMENT_UNPROCESSED
   WHERE status = tconst.SEGMENT_SELECTED; -- Resurrect SELECTED segment
  UPDATE Tape SET status = tconst.TAPE_PENDING
   WHERE tpmode = tconst.TPMODE_READ AND status IN (tconst.TAPE_WAITDRIVE, tconst.TAPE_WAITMOUNT, tconst.TAPE_MOUNTED); -- Resurrect the tapes running for recall
  UPDATE Tape A SET status = tconst.TAPE_WAITPOLICY
   WHERE status IN (tconst.TAPE_UNUSED, tconst.TAPE_FAILED, tconst.TAPE_UNKNOWN) AND EXISTS
    (SELECT id FROM Segment WHERE status = tconst.SEGMENT_UNPROCESSED AND tape = A.id);
  COMMIT;
END;
/

/** Functions for the MigHunterDaemon **/

CREATE OR REPLACE PROCEDURE migHunterCleanUp(svcName IN VARCHAR2)
AS
-- Cleans up the migration-hunter data in the database.
--
-- This procedure is called during the start-up logic of a new migration-hunter
-- daemon.
--
-- This procedure raises application error -20001 if the service-class
-- specified by svcName is unknown.
  svcId NUMBER;
BEGIN
  -- Get the database-ID of the service-class with the specified name
  BEGIN
    SELECT id INTO svcId FROM SvcClass WHERE name = svcName;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR(-20001,
      'Failed to clean-up the migration-hunter data in the database' ||
      ': No such service-class' ||
      ': svcName=' || svcName);
  END;

  -- clean up tapecopies, WAITPOLICY reset into TOBEMIGRATED
  UPDATE
     /*+ LEADING(TC CF)
         INDEX_RS_ASC(CF PK_CASTORFILE_ID)
         INDEX_RS_ASC(TC I_TAPECOPY_STATUS) */ 
         TapeCopy TC
     SET status = tconst.TAPECOPY_TOBEMIGRATED
   WHERE status = tconst.TAPECOPY_WAITPOLICY
     AND EXISTS (
       SELECT 'x' 
         FROM CastorFile CF
        WHERE TC.castorFile = CF.id
          AND CF.svcclass = svcId);
  -- clean up streams, WAITPOLICY reset into CREATED
  UPDATE Stream SET status = tconst.STREAM_CREATED WHERE status = tconst.STREAM_WAITPOLICY AND tapepool IN
   (SELECT svcclass2tapepool.child
      FROM svcclass2tapepool
     WHERE svcId = svcclass2tapepool.parent);
  COMMIT;
END;
/


/* Gets the tape copies to be attached to the streams of the specified service class. */
CREATE OR REPLACE PROCEDURE inputForMigrationPolicy(
  svcclassName IN  VARCHAR2,
  policyName   OUT NOCOPY VARCHAR2,
  svcId        OUT NUMBER,
  dbInfo       OUT castorTape.DbMigrationInfo_Cur) AS
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
     SET status = tconst.TAPECOPY_WAITPOLICY
   WHERE status IN (tconst.TAPECOPY_CREATED, tconst.TAPECOPY_TOBEMIGRATED)
     AND (EXISTS
       (SELECT 'x' FROM SubRequest, StageRepackRequest
         WHERE StageRepackRequest.svcclass = svcId
           AND SubRequest.request = StageRepackRequest.id
           AND SubRequest.status = dconst.SUBREQUEST_REPACK
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
     AND Stream.status = tconst.STREAM_RUNNING;
  UPDATE stream SET status = tconst.STREAM_WAITPOLICY
   WHERE Stream.status IN (tconst.STREAM_WAITSPACE, tconst.STREAM_CREATED, tconst.STREAM_STOPPED)
     AND Stream.id
      IN (SELECT Stream.id FROM Stream,SvcClass2TapePool
           WHERE Stream.Tapepool = SvcClass2TapePool.child
             AND SvcClass2TapePool.parent = svcId)
  RETURNING Stream.id BULK COLLECT INTO strIds;
  COMMIT;
  
  -- check for overloaded streams
  SELECT count(*) INTO tcNum FROM stream2tapecopy 
   WHERE parent IN 
    (SELECT /*+ CARDINALITY(stridTable 5) */ *
       FROM TABLE(strIds) stridTable);
  IF (tcnum > 10000 * maxstream) AND (maxstream > 0) THEN
    -- emergency mode
    OPEN dbInfo FOR
      SELECT Stream.id, 10000, 10000, gettime
        FROM Stream
       WHERE Stream.id IN
         (SELECT /*+ CARDINALITY(stridTable 5) */ *
            FROM TABLE(strIds) stridTable)
         AND Stream.status = tconst.STREAM_WAITPOLICY
       GROUP BY Stream.id;
  ELSE
  -- return for policy
  OPEN dbInfo FOR
    SELECT /*+ INDEX(CastorFile PK_CastorFile_Id) */ Stream.id,
           count(distinct Stream2TapeCopy.child),
           sum(CastorFile.filesize), gettime() - min(CastorFile.creationtime)
      FROM Stream2TapeCopy, TapeCopy, CastorFile, Stream
     WHERE Stream.id IN
        (SELECT /*+ CARDINALITY(stridTable 5) */ *
           FROM TABLE(strIds) stridTable)
       AND Stream2TapeCopy.child = TapeCopy.id
       AND TapeCopy.castorfile = CastorFile.id
       AND Stream.id = Stream2TapeCopy.parent
       AND Stream.status = tconst.STREAM_WAITPOLICY
     GROUP BY Stream.id
   UNION ALL
    SELECT Stream.id, 0, 0, 0
      FROM Stream WHERE Stream.id IN
        (SELECT /*+ CARDINALITY(stridTable 5) */ *
           FROM TABLE(strIds) stridTable)
       AND Stream.status = tconst.STREAM_WAITPOLICY
       AND NOT EXISTS 
        (SELECT 'x' FROM Stream2TapeCopy ST WHERE ST.parent = Stream.ID);
  END IF;         
END;
/


CREATE OR REPLACE PROCEDURE streamsForStreamPolicy (
  inSvcClassName                 IN  VARCHAR2,
  outSvcClassId                  OUT NUMBER,
  outStreamPolicyName            OUT NOCOPY VARCHAR2,
  outNbDrives                    OUT INTEGER,
  outStreamsForPolicy            OUT castorTape.StreamForPolicy_Cur)
/**
 * For the service-class specified by inSvcClassName, this procedure gets the
 * service-class database ID, the stream-policy name and the list of candidate
 * streams to be passed to the stream-policy.
 *
 * Please note the list of candidate streams includes streams with no
 * tape-copies attached.
 *
 * On success this function sets and commits the status of the streams for the
 * stream-policy to STREAM_WAITPOLICY (tragic number 7).
 *
 * This procedure raises application error -20001 if the service-class specified
 * by inSvcClassName is unknown.  In this case no modification is made to the
 * database and no commit is executed.
 *
 * @param inSvcClassName      The name of the service-class 
 * @param outSvcClassId       The database ID of the service-class.
 * @param outStreamPolicyName The name of the stream-policy of the
 *                            service-class.
 * @param outNbDrives         The maximum number of drives the service-class
 *                            can use at any single moment in time.
 * @param outStreamsForPolicy Cursor to the set of candidate streams to be
 *                            processed by the stream-policy.
 */
AS
  varStreamIds "numList"; -- Stream ids to be passed to the stream-policy
  varNumTapeCopies NUMBER := 0;  -- Number of tape-copies on the policy-streams
  varTooManyTapeCopiesToQuery BOOLEAN := FALSE; -- True if there are too many
                                                -- tape-copies to query
BEGIN
  -- Get the id, stream-policy name and number of drives of the service-class
  -- specified by inSvcClassName
  BEGIN
    SELECT id, streamPolicy, nbDrives
      INTO outSvcClassId, outStreamPolicyName, outNbDrives
      FROM SvcClass
     WHERE SvcClass.name = inSvcClassName;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR(-20001,
        'Unknown service-class name' ||
        ': inSvcClassName=' || inSvcClassName);
  END;

  -- Mark the streams to be processed by the stream-policy
  --
  -- Note that there is a COMMIT statement which means the database cannot help
  -- if the mighunter daemon crashes and forgets which streams it has marked
  -- for itself
  --
  -- Note that there is a race-condition between the MigHunterThread attaching
  -- tape-copies to newly created and empty streams and the the StreamThread
  -- deleting newly created threads with no tape-copies attached to them
  UPDATE Stream
     SET Stream.status = tconst.STREAM_WAITPOLICY
   WHERE Stream.status IN (tconst.STREAM_WAITSPACE, tconst.STREAM_CREATED, tconst.STREAM_STOPPED)
     AND Stream.id IN (
           SELECT Stream.id
             FROM Stream
            INNER JOIN SvcClass2TapePool
               ON (Stream.Tapepool = SvcClass2TapePool.child)
            WHERE SvcClass2TapePool.parent = outSvcClassId)
  RETURNING Stream.id BULK COLLECT INTO varStreamIds;
  COMMIT;
  
  -- Get the total number of tape-copies on the policy-streams
  SELECT count(*)
    INTO varNumTapeCopies
    FROM Stream2tapecopy 
   WHERE parent IN (
           SELECT /*+ CARDINALITY(streamIdTable 5) */ *
             FROM TABLE(varStreamIds) streamIdTable);

  -- Determine whether or not there are too many tape-copies to query, taking
  -- into account that nbDrives may have been modified and may be invalid
  -- (nbDrives < 1)
  IF outNbDrives >= 1 THEN
    varTooManyTapeCopiesToQuery := varNumTapeCopies > 10000 * outNbDrives;
  ELSE
    varTooManyTapeCopiesToQuery := varNumTapeCopies > 10000;
  END IF;

  IF varTooManyTapeCopiesToQuery THEN
    -- Enter emergency mode
    OPEN outStreamsForPolicy FOR
      SELECT Stream.id,
             10000, -- numTapeCopies
             10000*1073741824, -- totalBytes (Force file size to be 1 GiB)
             48*3600, -- ageOfOldestTapeCopy (Force age to be 48 hours)
             Stream.tapepool
        FROM Stream
       WHERE Stream.id IN (
                SELECT /*+ CARDINALITY(streamIdTable 5) */ *
                  FROM TABLE(varStreamIds) streamIdTable)
         AND Stream.status = tconst.STREAM_WAITPOLICY;
  ELSE
    OPEN outStreamsForPolicy FOR
      SELECT /*+ INDEX(CastorFile PK_CastorFile_Id) */ Stream.id,
             count(Stream2TapeCopy.child), -- numTapeCopies
             sum(CastorFile.filesize), -- totalBytes
             gettime() - min(CastorFile.creationtime), -- ageOfOldestTapeCopy
             Stream.tapepool
        FROM Stream2TapeCopy
       INNER JOIN Stream     ON (Stream2TapeCopy.parent = Stream.id    )
       INNER JOIN TapeCopy   ON (Stream2TapeCopy.child  = TapeCopy.id  )
       INNER JOIN CastorFile ON (TapeCopy.castorFile    = CastorFile.id)
       WHERE Stream.id IN (
               SELECT /*+ CARDINALITY(stridTable 5) */ *
                 FROM TABLE(varStreamIds) streamIdTable)
                  AND Stream.status = tconst.STREAM_WAITPOLICY
       GROUP BY Stream.tapepool, Stream.id
      UNION ALL /* Append streams with no tape-copies attached */
      SELECT Stream.id,
             0, -- numTapeCopies
             0, -- totalBytes
             0, -- ageOfOldestTapeCopy
             0  -- tapepool
        FROM Stream
       WHERE Stream.id IN (
               SELECT /*+ CARDINALITY(streamIdTable 5) */ *
                 FROM TABLE(varStreamIds) streamIdTable)
         AND Stream.status = tconst.STREAM_WAITPOLICY
         AND NOT EXISTS (
               SELECT 'x'
                 FROM Stream2TapeCopy ST
                WHERE ST.parent = Stream.ID);
  END IF;         
END streamsForStreamPolicy;
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

        INSERT INTO Stream
          (id, initialsizetotransfer, lastFileSystemChange, tape, lastFileSystemUsed,
           lastButOneFileSystemUsed, tapepool, status)
        VALUES (ids_seq.nextval, initSize, NULL, NULL, NULL, NULL, tpId, tconst.STREAM_CREATED)
        RETURN id INTO strId;
        INSERT INTO Id2Type (id, type) VALUES (strId,26); -- Stream type
    	IF doClone = 1 THEN
	  BEGIN
	    -- clone the new stream with one from the same tapepool
	    SELECT id, initialsizetotransfer INTO streamToClone, oldSize
              FROM Stream WHERE tapepool = tpId AND id != strId AND ROWNUM < 2;
            FOR tcId IN (SELECT child FROM Stream2TapeCopy
                          WHERE Stream2TapeCopy.parent = streamToClone)
            LOOP
              -- a take the first one, they are supposed to be all the same
              INSERT INTO stream2tapecopy (parent, child)
              VALUES (strId, tcId.child);
            END LOOP;
            UPDATE Stream SET initialSizeToTransfer = oldSize
             WHERE id = strId;
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

/* attach tapecopies to streams for rtcpclientd */
CREATE OR REPLACE PROCEDURE attachTCRtcp
(tapeCopyIds IN castor."cnumList",
 tapePoolId IN NUMBER)
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
       WHERE Stream.tapepool = tapePoolId;
      IF nbStream <> 0 THEN
        -- we have at least a stream for that tapepool
        SELECT id INTO unused
          FROM TapeCopy
         WHERE status IN (tconst.TAPECOPY_WAITINSTREAMS, tconst.TAPECOPY_WAITPOLICY) AND id = tapeCopyIds(i) FOR UPDATE;
        -- let's attach it to the different streams
        FOR streamId IN (SELECT id FROM Stream
                          WHERE Stream.tapepool = tapePoolId ) LOOP
          UPDATE TapeCopy SET status = tconst.TAPECOPY_WAITINSTREAMS
           WHERE status = tconst.TAPECOPY_WAITPOLICY AND id = tapeCopyIds(i);
          DECLARE CONSTRAINT_VIOLATED EXCEPTION;
          PRAGMA EXCEPTION_INIT (CONSTRAINT_VIOLATED, -1);
          BEGIN
            INSERT INTO stream2tapecopy (parent ,child)
            VALUES (streamId.id, tapeCopyIds(i));
          EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
            -- if the stream does not exist anymore
            UPDATE tapecopy SET status = tconst.TAPECOPY_WAITPOLICY WHERE id = tapeCopyIds(i);
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
    UPDATE TapeCopy SET status = tconst.TAPECOPY_TOBEMIGRATED WHERE id = tapeCopyIds(i) AND status = tconst.TAPECOPY_WAITPOLICY;
  COMMIT;
END;
/

/* attach tapecopies to streams for tapegateway */
CREATE OR REPLACE PROCEDURE attachTCGateway
(tapeCopyIds IN castor."cnumList",
 tapePoolId IN NUMBER)
AS
  unused NUMBER;
  streamId NUMBER; -- stream attached to the tapepool
  nbTapeCopies NUMBER;
BEGIN
  -- WARNING: tapegateway ONLY version
  FOR str IN (SELECT id FROM Stream WHERE tapepool = tapePoolId) LOOP
    BEGIN
      -- add choosen tapecopies to all Streams associated to the tapepool used by the policy
      SELECT id INTO streamId FROM stream WHERE id = str.id FOR UPDATE;
      -- add choosen tapecopies to all Streams associated to the tapepool used by the policy
      FOR i IN tapeCopyIds.FIRST .. tapeCopyIds.LAST LOOP
         BEGIN     
           SELECT /*+ index(tapecopy, PK_TAPECOPY_ID)*/ id INTO unused
             FROM TapeCopy
            WHERE Status in (tconst.TAPECOPY_WAITINSTREAMS, tconst.TAPECOPY_WAITPOLICY) AND id = tapeCopyIds(i) FOR UPDATE;
           DECLARE CONSTRAINT_VIOLATED EXCEPTION;
           PRAGMA EXCEPTION_INIT (CONSTRAINT_VIOLATED, -1);
           BEGIN
             INSERT INTO stream2tapecopy (parent ,child)
             VALUES (streamId, tapeCopyIds(i));
             UPDATE /*+ index(tapecopy, PK_TAPECOPY_ID)*/ TapeCopy
                SET Status = tconst.TAPECOPY_WAITINSTREAMS WHERE status = tconst.TAPECOPY_WAITPOLICY AND id = tapeCopyIds(i); 
           EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
             -- if the stream does not exist anymore
             -- it might also be that the tapecopy does not exist anymore
             -- already exist the tuple parent-child
             NULL;
           END;
         EXCEPTION WHEN NO_DATA_FOUND THEN
           -- Go on the tapecopy has been resurrected or migrated
           NULL;
         END;
      END LOOP; -- loop tapecopies
      COMMIT;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- no stream anymore
      NULL;
    END;
  END LOOP; -- loop streams

  -- resurrect the ones never attached
  FORALL i IN tapeCopyIds.FIRST .. tapeCopyIds.LAST
    UPDATE TapeCopy SET status = tconst.TAPECOPY_TOBEMIGRATED WHERE id = tapeCopyIds(i) AND status = tconst.TAPECOPY_WAITPOLICY;
  COMMIT;
END;
/

/* generic attach tapecopies to stream */
CREATE OR REPLACE PROCEDURE attachTapeCopiesToStreams 
(tapeCopyIds IN castor."cnumList",
 tapePoolId IN NUMBER) AS
  unused VARCHAR2(2048);
BEGIN
  BEGIN
    SELECT value INTO unused
      FROM CastorConfig
     WHERE class = 'tape'
       AND key   = 'interfaceDaemon'
       AND value = 'tapegatewayd';
  EXCEPTION WHEN NO_DATA_FOUND THEN  -- rtcpclientd
    attachTCRtcp(tapeCopyIds, tapePoolId);
    RETURN;
  END;
  -- tapegateway
  attachTCGateway(tapeCopyIds, tapePoolId);
END;
/

/* start choosen stream */
CREATE OR REPLACE PROCEDURE startChosenStreams
  (streamIds IN castor."cnumList") AS
BEGIN
  IF (TapegatewaydIsRunning) THEN
    FORALL i IN streamIds.FIRST .. streamIds.LAST
      UPDATE Stream S
         SET S.status = tconst.STREAM_PENDING,
             S.TapeGatewayRequestId = ids_seq.nextval
       WHERE S.status = tconst.STREAM_WAITPOLICY
         AND S.id = streamIds(i);
  ELSE
    FORALL i IN streamIds.FIRST .. streamIds.LAST
      UPDATE Stream S
         SET S.status = tconst.STREAM_PENDING
       WHERE S.status = tconst.STREAM_WAITPOLICY
         AND S.id = streamIds(i);
  END IF;
  COMMIT;
END;
/

/* stop chosen stream */
CREATE OR REPLACE PROCEDURE stopChosenStreams
        (streamIds IN castor."cnumList") AS
  nbTc NUMBER;
BEGIN
  FOR i IN streamIds.FIRST .. streamIds.LAST LOOP
    deleteOrStopStream(streamIds(i));
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
    UPDATE TapeCopy SET status = tconst.TAPECOPY_TOBEMIGRATED WHERE status = tconst.TAPECOPY_WAITPOLICY
       AND id = migrationCandidates(i);
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
    UPDATE TapeCopy SET status = tconst.TAPECOPY_FAILED WHERE id = tapecopyIds(i) AND status = tconst.TAPECOPY_WAITPOLICY;

  -- repack subrequests to be archived
  FOR i IN tapecopyIds.FIRST .. tapecopyIds.LAST LOOP
    BEGIN
      SELECT subrequest.id INTO srId FROM subrequest, tapecopy 
       WHERE subrequest.castorfile = tapecopy.castorfile
         AND tapecopy.id = tapecopyIds(i)
         AND subrequest.status = dconst.SUBREQUEST_REPACK;
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
       Tape.id,
       Tape.vid,
       count(distinct segment.id),
       sum(CastorFile.fileSize),
       getTime() - min(Segment.creationTime) age,
       max(Segment.priority)
      FROM TapeCopy, CastorFile, Segment, Tape
     WHERE Tape.id = Segment.tape
       AND TapeCopy.id = Segment.copy
       AND CastorFile.id = TapeCopy.castorfile
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
     FOR SELECT /*+ NO_USE_MERGE(TAPE SEGMENT TAPECOPY CASTORFILE) NO_USE_HASH(TAPE SEGMENT TAPECOPY CASTORFILE) INDEX_RS_ASC(SEGMENT I_SEGMENT_TAPE) INDEX_RS_ASC(TAPE I_TAPE_STATUS) INDEX_RS_ASC(TAPE
COPY PK_TAPECOPY_ID) INDEX_RS_ASC(CASTORFILE PK_CASTORFILE_ID) */ Tape.id,
                Tape.vid,
                count ( distinct segment.id ),
                sum ( CastorFile.fileSize ),
                getTime ( ) - min ( Segment.creationTime ) age,
                max ( Segment.priority ),
                Tape.status
           FROM TapeCopy,
                CastorFile,
                Segment,
                Tape
          WHERE Tape.id = Segment.tape
            AND TapeCopy.id = Segment.copy
            AND CastorFile.id = TapeCopy.castorfile
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
  IF (TapegatewaydIsRunning) THEN
    FOR i IN tapeIds.FIRST .. tapeIds.LAST LOOP
      UPDATE Tape T
         SET T.TapegatewayRequestId = ids_seq.nextval,
             T.status = tconst.TAPE_PENDING
       WHERE T.status = tconst.TAPE_WAITPOLICY AND T.id = tapeIds(i);
      -- XXX FIXME TODO this is a hack needed to add the TapegatewayRequestId which was missing.
      UPDATE Tape T
         SET T.TapegatewayRequestId = ids_seq.nextval
       WHERE T.status = tconst.TAPE_PENDING AND T.TapegatewayRequestId IS NULL 
         AND T.id = tapeIds(i);
    END LOOP; 
  ELSE
    FORALL i IN tapeIds.FIRST .. tapeIds.LAST
      UPDATE Tape SET status = tconst.TAPE_PENDING WHERE status = tconst.TAPE_WAITPOLICY AND id = tapeIds(i);
  END IF;
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
  -- note that if the request is over (all in 9,11) or not started (0), nothing is done
  SELECT id INTO reqId 
    FROM StageRepackRequest R 
   WHERE repackVid = inputVid
     AND EXISTS 
       (SELECT 1 FROM SubRequest 
         WHERE request = R.id AND status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_REPACK));
  -- fail subrequests
  UPDATE Subrequest SET status = dconst.SUBREQUEST_FAILED_FINISHED
   WHERE request = reqId AND status NOT IN (dconst.SUBREQUEST_FAILED_FINISHED, dconst.SUBREQUEST_ARCHIVED)
  RETURNING castorFile, diskcopy BULK COLLECT INTO cfIds, dcIds;
  SELECT id INTO srId 
    FROM SubRequest 
   WHERE request = reqId AND ROWNUM = 1;
  archiveSubReq(srId, 9);

  -- fail related diskcopies
  FORALL i IN dcIds.FIRST .. dcids.LAST
    UPDATE DiskCopy
       SET status = decode(status, dconst.DISKCOPY_WAITTAPERECALL, dconst.DISKCOPY_FAILED, dconst.DISKCOPY_INVALID) -- WAITTAPERECALL->FAILED, otherwise INVALID
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
  UPDATE tape SET status = tconst.TAPE_UNUSED WHERE vid = inputVid AND tpmode = tconst.TPMODE_READ;
  -- update other tapes which could have been involved
  FORALL i IN tapeIds.FIRST .. tapeIds.LAST
    UPDATE tape SET status = tconst.TAPE_UNUSED WHERE id = tapeIds(i);
  -- commit the transation
  COMMIT;
EXCEPTION WHEN NO_DATA_FOUND THEN 
  COMMIT;
END;
/

/*
restartStuckRecalls is a wrokaround procedure required by the rtcpclientd
daemon.

Restart the (recall) segments that are recognized as stuck.
This workaround (sr #112306: locking issue in CMS stager)
will be dropped as soon as the TapeGateway will be used in production.

Notes for query readability:
TAPE status:    (0)TAPE_UNUSED, (1)TAPE_PENDING, (2)TAPE_WAITDRIVE, 
                (3)TAPE_WAITMOUNT, (6)TAPE_FAILED
SEGMENT status: (0)SEGMENT_UNPROCESSED, (7)SEGMENT_SELECTED
*/
CREATE OR REPLACE PROCEDURE restartStuckRecalls AS
  unused VARCHAR2(2048);
BEGIN
  -- Do nothing and return if the tape gateway is running
  BEGIN
    SELECT value INTO unused
      FROM CastorConfig
     WHERE class = 'tape'
       AND key   = 'interfaceDaemon'
       AND value = 'tapegatewayd';
     RETURN;
  EXCEPTION WHEN NO_DATA_FOUND THEN  -- rtcpclientd
    -- Do nothing and continue
    NULL;
  END;

  -- Notes for query readability:
  -- TAPE status:    (0)TAPE_UNUSED, (1)TAPE_PENDING, (2)TAPE_WAITDRIVE, 
  --                 (3)TAPE_WAITMOUNT, (6)TAPE_FAILED
  -- SEGMENT status: (0)SEGMENT_UNPROCESSED, (7)SEGMENT_SELECTED

  -- Mark as unused all of the recall tapes whose state maybe stuck due to an
  -- rtcpclientd crash. Such tapes will be pending, waiting for a drive, or
  -- waiting for a mount, and will be associated with segments that are neither
  -- un-processed nor selected.
  UPDATE tape SET status=tconst.TAPE_UNUSED
   WHERE tpmode = tconst.TPMODE_READ
     AND status IN (tconst.TAPE_PENDING, tconst.TAPE_WAITDRIVE, tconst.TAPE_WAITMOUNT)
     AND id NOT IN (SELECT tape FROM segment 
                     WHERE status IN (tconst.SEGMENT_UNPROCESSED, tconst.SEGMENT_SELECTED));

  -- Mark as unprocessed all recall segments that are marked as being selected
  -- and are associated with unused or failed recall tapes that have 1 or more
  -- unprocessed or selected segments.
  UPDATE segment SET status = tconst.SEGMENT_UNPROCESSED 
   WHERE status = tconst.SEGMENT_SELECTED 
     AND tape IN (SELECT id FROM tape WHERE tpmode = tconst.TPMODE_READ 
                     AND status IN (tconst.TAPE_UNUSED, tconst.TAPE_FAILED) 
                     AND id IN (SELECT tape FROM segment 
                                 WHERE status IN (tconst.SEGMENT_UNPROCESSED, tconst.SEGMENT_SELECTED))
                  );

  -- Mark as pending all recall tapes that are unused or failed, and have
  -- unprocessed and selected segments.
  UPDATE tape SET status = tconst.TAPE_PENDING
   WHERE tpmode = tconst.TPMODE_READ 
     AND status IN (tconst.TAPE_UNUSED, tconst.TAPE_FAILED) 
     AND id IN (SELECT tape FROM segment 
                 WHERE status IN (tconst.SEGMENT_UNPROCESSED, tconst.SEGMENT_SELECTED));

  COMMIT;
END restartStuckRecalls;
/


/*
The default state of the stager database is to be compatible with the
rtcpclientd daemon as opposed to the tape gateway daemon.  Therefore create the
restartStuckRecallsJob which will call the restartStuckRecalls workaround
procedure every hour.
*/
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('RESTARTSTUCKRECALLSJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'RESTARTSTUCKRECALLSJOB',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN restartStuckRecalls(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=60',
      ENABLED         => TRUE,
      COMMENTS        => 'Workaround to restart stuck recalls');
END;
/


/**
 * Returns true if the sppecified tape inteface daemon is running.
 *
 * @param daemonName The name of the interface daemon.
 */
CREATE OR REPLACE FUNCTION tapeInterfaceDaemonIsRunning(
  daemonName IN VARCHAR2)
RETURN BOOLEAN IS
  nbRows NUMBER := 0;
BEGIN
  SELECT COUNT(*) INTO nbRows
    FROM CastorConfig
    WHERE class = 'tape'
      AND key   = 'interfaceDaemon'
      AND VALUE = daemonName;

  RETURN nbRows > 0;
END tapeInterfaceDaemonIsRunning;
/


/**
 * Returns true if the rtcpclientd daemon is running.
 */ 
CREATE OR REPLACE FUNCTION rtcpclientdIsRunning
RETURN BOOLEAN IS 
BEGIN
  RETURN tapeInterfaceDaemonIsRunning('rtcpclientd');
END rtcpclientdIsRunning;
/


/**
 * Returns true if the tape gateway daemon is running.
 */
CREATE OR REPLACE FUNCTION tapegatewaydIsRunning
RETURN BOOLEAN IS
BEGIN
  RETURN tapeInterfaceDaemonIsRunning('tapegatewayd');
END tapegatewaydIsRunning;
/


CREATE OR REPLACE PROCEDURE lockCastorFileById(
/**
 * Locks the row in the castor-file table with the specified database ID.
 *
 * This procedure raises application error -20001 when no row exists in the
 * castor-file table with the specified database ID.
 *
 * @param inCastorFileId The database ID of the row to be locked.
 */
  inCastorFileId INTEGER
) AS
  varDummyCastorFileId INTEGER := 0;
BEGIN
  SELECT CastorFile.id
    INTO varDummyCastorFileId
    FROM CastorFile
   WHERE CastorFile.id = inCastorFileId
     FOR UPDATE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RAISE_APPLICATION_ERROR(-20001,
    'Castor-file does not exist' ||
    ': inCastorFileId=' || inCastorFileId);
END lockCastorFileById;
/
