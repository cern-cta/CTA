/*******************************************************************
 *
 * @(#)$RCSfile: oracleTape.sql,v $ $Revision: 1.661 $ $Date: 2008/05/30 07:31:05 $ $Author: waldron $
 *
 * PL/SQL code for the interface to the tape system
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/


/* Used to create a row INTO NbTapeCopiesInFS whenever a new
   Stream is created */
CREATE OR REPLACE TRIGGER tr_Stream_Insert
BEFORE INSERT ON Stream
FOR EACH ROW
BEGIN
  FOR item in (SELECT id FROM FileSystem) LOOP
    INSERT INTO NbTapeCopiesInFS (FS, Stream, NbTapeCopies) VALUES (item.id, :new.id, 0);
  END LOOP;
END;


/* Used to delete rows IN NbTapeCopiesInFS whenever a
   Stream is deleted */
CREATE OR REPLACE TRIGGER tr_Stream_Delete
BEFORE DELETE ON Stream
FOR EACH ROW
BEGIN
  DELETE FROM NbTapeCopiesInFS WHERE Stream = :old.id;
END;


/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a TapeCopy is linked to a Stream */
CREATE OR REPLACE TRIGGER tr_Stream2TapeCopy_Insert
AFTER INSERT ON STREAM2TAPECOPY
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
  cfSize NUMBER;
BEGIN
  -- added this lock because of several copies of different file systems
  -- from different streams which can cause deadlock
  LOCK TABLE NbTapeCopiesInFS IN ROW SHARE MODE;
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies + 1
   WHERE FS IN (SELECT DiskCopy.FileSystem
                  FROM DiskCopy, TapeCopy
                 WHERE DiskCopy.CastorFile = TapeCopy.castorFile
                   AND TapeCopy.id = :new.child
                   AND DiskCopy.status = 10) -- CANBEMIGR
     AND Stream = :new.parent;
END;


/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a TapeCopy is unlinked from a Stream */
CREATE OR REPLACE TRIGGER tr_Stream2TapeCopy_Delete
BEFORE DELETE ON STREAM2TAPECOPY
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE cfSize NUMBER;
BEGIN  
  -- added this lock because of several copies of different file systems 
  -- from different streams which can cause deadlock
  LOCK TABLE NbTapeCopiesInFS IN ROW SHARE MODE;
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies - 1
   WHERE FS IN (SELECT DiskCopy.FileSystem
                  FROM DiskCopy, TapeCopy
                 WHERE DiskCopy.CastorFile = TapeCopy.castorFile
                   AND TapeCopy.id = :old.child
                   AND DiskCopy.status = 10) -- CANBEMIGR
     AND Stream = :old.parent;
END;

/************************************************/
/* Triggers to keep NbTapeCopiesInFS consistent */
/************************************************/

/* Used to create a row in NbTapeCopiesInFS and FileSystemsToCheck 
   whenever a new FileSystem is created */
CREATE OR REPLACE TRIGGER tr_FileSystem_Insert
BEFORE INSERT ON FileSystem
FOR EACH ROW
BEGIN
  FOR item in (SELECT id FROM Stream) LOOP
    INSERT INTO NbTapeCopiesInFS (FS, Stream, NbTapeCopies) VALUES (:new.id, item.id, 0);
  END LOOP;
  INSERT INTO FileSystemsToCheck (FileSystem, ToBeChecked) VALUES (:new.id, 0);
END;

/* Used to delete rows in NbTapeCopiesInFS and FileSystemsToCheck 
   whenever a FileSystem is deleted */
CREATE OR REPLACE TRIGGER tr_FileSystem_Delete
BEFORE DELETE ON FileSystem
FOR EACH ROW
BEGIN
  DELETE FROM NbTapeCopiesInFS WHERE FS = :old.id;
  DELETE FROM FileSystemsToCheck WHERE FileSystem = :old.id;
END;

/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a DiskCopy has been replicated and the new one
   is put into CANBEMIGR status from the
   WAITDISK2DISKCOPY status */
CREATE OR REPLACE TRIGGER tr_DiskCopy_Update
AFTER UPDATE of status ON DiskCopy
FOR EACH ROW
WHEN (old.status = 1 AND -- WAITDISK2DISKCOPY
      new.status = 10) -- CANBEMIGR
BEGIN
-- added this lock because of severaval copies of different file systems 
--  from different streams which can cause deadlock
  LOCK TABLE  NbTapeCopiesInFS IN ROW SHARE MODE;
  UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies + 1
   WHERE FS = :new.fileSystem
     AND Stream IN (SELECT Stream2TapeCopy.parent
                      FROM Stream2TapeCopy, TapeCopy
                     WHERE TapeCopy.castorFile = :new.castorFile
                       AND Stream2TapeCopy.child = TapeCopy.id
                       AND TapeCopy.status = 2); -- WAITINSTREAMS
END;


/* Used to create a row INTO LockTable whenever a new
   DiskServer is created */
CREATE OR REPLACE TRIGGER tr_DiskServer_Insert
BEFORE INSERT ON DiskServer
FOR EACH ROW
BEGIN
  INSERT INTO LockTable (DiskServerId, TheLock) VALUES (:new.id, 0);
END;


/* Used to delete rows IN LockTable whenever a
   DiskServer is deleted */
CREATE OR REPLACE TRIGGER tr_DiskServer_Delete
BEFORE DELETE ON DiskServer
FOR EACH ROW
BEGIN
  DELETE FROM LockTable WHERE DiskServerId = :old.id;
END;



/* PL/SQL methods to update FileSystem weight for new migrator streams */
CREATE OR REPLACE PROCEDURE updateFsMigratorOpened
(ds IN INTEGER, fs IN INTEGER, fileSize IN INTEGER) AS
BEGIN
  /* We lock first the diskserver in order to lock all the
     filesystems of this DiskServer in an atomical way */
  UPDATE DiskServer SET nbMigratorStreams = nbMigratorStreams + 1 WHERE id = ds;
  UPDATE FileSystem SET nbMigratorStreams = nbMigratorStreams + 1 WHERE id = fs;
END;

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


/* PL/SQL method implementing anyTapeCopyForStream.
 * This implementation is not the original one. It uses NbTapeCopiesInFS
 * because a join on the tables between DiskServer and Stream2TapeCopy
 * costs too much. It should actually not be the case but ORACLE is unable
 * to optimize correctly queries having a ROWNUM clause. It processes the
 * the query without it (yes !!!) and applies the clause afterwards.
 * Here is the previous select in case ORACLE improves some day :
 * SELECT \/*+ FIRST_ROWS *\/ TapeCopy.id INTO unused
 * FROM DiskServer, FileSystem, DiskCopy, CastorFile, TapeCopy, Stream2TapeCopy
 *  WHERE DiskServer.id = FileSystem.diskserver
 *   AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
 *   AND FileSystem.id = DiskCopy.filesystem
 *   AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
 *   AND DiskCopy.castorfile = CastorFile.id
 *   AND TapeCopy.castorfile = Castorfile.id
 *   AND Stream2TapeCopy.child = TapeCopy.id
 *   AND Stream2TapeCopy.parent = streamId
 *   AND TapeCopy.status = 2 -- WAITINSTREAMS
 *   AND ROWNUM < 2;  */
CREATE OR REPLACE PROCEDURE anyTapeCopyForStream(streamId IN INTEGER, res OUT INTEGER) AS
  unused INTEGER;
BEGIN
  SELECT NbTapeCopiesInFS.NbTapeCopies INTO unused
    FROM NbTapeCopiesInFS, FileSystem, DiskServer
   WHERE NbTapeCopiesInFS.stream = streamId
     AND NbTapeCopiesInFS.NbTapeCopies > 0
     AND FileSystem.id = NbTapeCopiesInFS.FS
     AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
     AND DiskServer.id = FileSystem.DiskServer
     AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
     AND ROWNUM < 2;
  res := 1;
EXCEPTION
 WHEN NO_DATA_FOUND THEN
  res := 0;
END;


/* PL/SQL method implementing bestTapeCopyForStream */
CREATE OR REPLACE PROCEDURE bestTapeCopyForStream(streamId IN INTEGER,
                                                  diskServerName OUT VARCHAR2, mountPoint OUT VARCHAR2,
                                                  path OUT VARCHAR2, dci OUT INTEGER,
                                                  castorFileId OUT INTEGER, fileId OUT INTEGER,
                                                  nsHost OUT VARCHAR2, fileSize OUT INTEGER,
                                                  tapeCopyId OUT INTEGER, optimized IN INTEGER) AS
  fileSystemId INTEGER := 0;
  dsid NUMBER;
  fsDiskServer NUMBER;
  lastFSChange NUMBER;
  lastFSUsed NUMBER;
  lastButOneFSUsed NUMBER;
  findNewFS NUMBER := 1;
  nbMigrators NUMBER := 0;
  unused "numList";
BEGIN
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
        SELECT P.path, P.diskcopy_id, P.castorfile,
               C.fileId, C.nsHost, C.fileSize, P.id
          INTO path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId
          FROM (SELECT /*+ USE_NL(D) USE_NL(T) USE_NL(ST) INDEX(T I_TAPECOPY_CF_STATUS_2) INDEX(ST I_PK_STREAM2TAPECOPY) */
                D.path, D.diskcopy_id, D.castorfile, T.id
                  FROM (SELECT /*+ INDEX(DK I_DISKCOPY_FS_STATUS_10) */
                               DiskCopy.path path, DiskCopy.id diskcopy_id, DiskCopy.castorfile
                          FROM DiskCopy
                         WHERE decode(DiskCopy.status,10,DiskCopy.status,null) = 10 -- CANBEMIGR
                           AND DiskCopy.filesystem = lastButOneFSUsed) D,
                        TapeCopy T, Stream2TapeCopy ST
                 WHERE T.castorfile = D.castorfile
                   AND ST.child = T.id
                   AND ST.parent = streamId
                   AND decode(T.status,2,T.status,null) = 2 -- WAITINSTREAMS
                   AND ROWNUM < 2
             ) P, castorfile C
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
    -- We lock here a given DiskServer. See the comment for the creation of the LockTable
    -- table for a full explanation of why we need such a stupid UPDATE statement
    UPDATE LockTable SET theLock = 1
     WHERE diskServerId = (
       SELECT DS.diskserver_id
         FROM (
           -- The double level of selects is due to the fact that ORACLE is unable
           -- to use ROWNUM and ORDER BY at the same time. Thus, we have to first compute
           -- the maxRate and then select on it.
           SELECT diskserver.id diskserver_id
             FROM FileSystem FS, NbTapeCopiesInFS, DiskServer
            WHERE FS.id = NbTapeCopiesInFS.FS
              AND NbTapeCopiesInFS.NbTapeCopies > 0
              AND NbTapeCopiesInFS.Stream = StreamId
              AND FS.status IN (0, 1)         -- PRODUCTION, DRAINING
              AND DiskServer.id = FS.diskserver
              AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
              -- Ignore diskservers where a migrator already exists
              AND DiskServer.id NOT IN (
                SELECT DISTINCT(FileSystem.diskServer)
                  FROM FileSystem, Stream
                 WHERE FileSystem.id = Stream.lastfilesystemused
                   AND Stream.status IN (3)   -- SELECTED
                   AND optimized = 1
              )
            ORDER BY FileSystemRate(FS.readRate, FS.writeRate, FS.nbReadStreams, FS.nbWriteStreams, 
                     FS.nbReadWriteStreams, FS.nbMigratorStreams, FS.nbRecallerStreams) DESC, dbms_random.value
            ) DS
        WHERE ROWNUM < 2)
    RETURNING diskServerId INTO dsid;
    -- Now we got our Diskserver but we lost all other data (due to the fact we had
    -- to do an update for the lock and we could not do a join in the update).
    -- So here we select all we need
    SELECT FN.name, FN.mountPoint, FN.diskserver, FN.id
      INTO diskServerName, mountPoint, fsDiskServer, fileSystemId
      FROM (
        SELECT DiskServer.name, FS.mountPoint, FS.diskserver, FS.id
          FROM FileSystem FS, NbTapeCopiesInFS, Diskserver
         WHERE FS.id = NbTapeCopiesInFS.FS
           AND DiskServer.id = FS.diskserver
           AND NbTapeCopiesInFS.NbTapeCopies > 0
           AND NbTapeCopiesInFS.Stream = StreamId
           AND FS.status IN (0, 1)    -- PRODUCTION, DRAINING
           AND FS.diskserver = dsId
         ORDER BY FileSystemRate(FS.readRate, FS.writeRate, FS.nbReadStreams, FS.nbWriteStreams,
                  FS.nbReadWriteStreams, FS.nbMigratorStreams, FS.nbRecallerStreams) DESC, dbms_random.value
         ) FN
     WHERE ROWNUM < 2;
    SELECT P.path, P.diskcopy_id, P.castorfile,
           C.fileId, C.nsHost, C.fileSize, P.id
      INTO path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId
      FROM (SELECT /*+ USE_NL(D) USE_NL(T) USE_NL(ST) INDEX(T I_TAPECOPY_CF_STATUS_2) INDEX(ST I_PK_STREAM2TAPECOPY) */
            D.path, D.diskcopy_id, D.castorfile, T.id
              FROM (SELECT /*+ INDEX(DK I_DISKCOPY_FS_STATUS_10) */
                           DiskCopy.path path, DiskCopy.id diskcopy_id, DiskCopy.castorfile
                      FROM DiskCopy
                     WHERE decode(DiskCopy.status,10,DiskCopy.status,null) = 10 -- CANBEMIGR
                       AND DiskCopy.filesystem = fileSystemId) D,
                    TapeCopy T, Stream2TapeCopy ST
             WHERE T.castorfile = D.castorfile
               AND ST.child = T.id
               AND ST.parent = streamId
               AND decode(T.status,2,T.status,null) = 2 -- WAITINSTREAMS
               AND ROWNUM < 2) P, castorfile C
     WHERE P.castorfile = C.id;
  END IF;
  -- update status of selected tapecopy and stream
  UPDATE TapeCopy SET status = 3 -- SELECTED
   WHERE id = tapeCopyId;
  -- get locks on all the stream we will handle in order to avoid a
  -- deadlock with the attachTapeCopiesToStreams procedure
  --
  -- the deadlock would occur with updates to the NbTapeCopiesInFS
  -- table performed by the tr_stream2tapecopy_delete trigger triggered
  -- by the "DELETE FROM Stream2TapeCopy" statement below
  SELECT 1 BULK COLLECT
    INTO unused
    FROM Stream
    WHERE id IN (SELECT parent FROM Stream2TapeCopy WHERE child = tapeCopyId)
    ORDER BY id
    FOR UPDATE; 
  UPDATE Stream
     SET status = 3, -- RUNNING
         lastFileSystemUsed = fileSystemId, lastButOneFileSystemUsed = lastFileSystemUsed
   WHERE id = streamId;
  IF findNewFS = 1 THEN
    -- time only if we changed FS
    UPDATE Stream SET lastFileSystemChange = getTime() WHERE id = streamId;
  END IF;
  -- detach the tapecopy from the stream now that it is SELECTED
  -- the triggers will update NbTapeCopiesInFS accordingly
  DELETE FROM Stream2TapeCopy
   WHERE child = tapeCopyId;

  -- Update Filesystem state
  updateFSMigratorOpened(fsDiskServer, fileSystemId, 0);

  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- If the procedure was called with optimization enabled,
    -- rerun it again with optimization disabled to make sure
    -- there is really nothing to migrate!! Why? optimization
    -- excludes filesystems as being migration candidates if
    -- a migration stream is already running there. This allows
    -- us to maximise bandwidth to tape and not to saturate a
    -- diskserver with too many streams. However, in small disk
    -- pools this behaviour results in mounting, writing one
    -- file and dismounting of tapes as the tpdaemon reads ahead
    -- many files at a time! (#28097)
    IF optimized = 1 THEN
      bestTapeCopyForStream(streamId,
                            diskServerName,
                            mountPoint,
                            path,
                            dci,
                            castorFileId,
                            fileId,
                            nsHost,
                            fileSize,
                            tapeCopyId,
			    0);
      -- Since we recursively called ourselves, we should not do
      -- any update in the outer call
      RETURN;
    END IF;
    -- Reset last filesystems used
    UPDATE Stream
       SET lastFileSystemUsed = 0, lastButOneFileSystemUsed = 0
     WHERE id = streamId;
    -- No data found means the selected filesystem has no
    -- tapecopies to be migrated. Thus we go to next one
    -- However, we reset the NbTapeCopiesInFS row that failed
    -- This is not 100% safe but is far better than retrying
    -- in the same conditions
    IF 0 != fileSystemId THEN
      UPDATE NbTapeCopiesInFS
         SET NbTapeCopies = 0
       WHERE Stream = StreamId
         AND NbTapeCopiesInFS.FS = fileSystemId;
      bestTapeCopyForStream(streamId,
                            diskServerName,
                            mountPoint,
                            path,
                            dci,
                            castorFileId,
                            fileId,
                            nsHost,
                            fileSize,
                            tapeCopyId,
			    optimized);
    END IF;
END;


/* PL/SQL method implementing bestFileSystemForSegment */
CREATE OR REPLACE PROCEDURE bestFileSystemForSegment(segmentId IN INTEGER, diskServerName OUT VARCHAR2,
                                                     rmountPoint OUT VARCHAR2, rpath OUT VARCHAR2,
                                                     dcid OUT INTEGER, optimized IN INTEGER) AS
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
                      SELECT id, svcClass from StageGetNextRequest UNION ALL
                      SELECT id, svcClass from StageUpdateRequest UNION ALL
                      SELECT id, svcClass from StagePrepareToUpdateRequest UNION ALL
                      SELECT id, svcClass from StageUpdateNextRequest) Request,
                      SubRequest, CastorFile
               WHERE CastorFile.id = cfid
                 AND SubRequest.castorfile = cfid
                 AND SubRequest.status = 4 
                 AND Request.id = SubRequest.request
                 AND Request.svcclass = DiskPool2SvcClass.child
                 AND FileSystem.diskpool = DiskPool2SvcClass.parent
                 AND FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > CastorFile.fileSize
                 AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
                 AND DiskServer.id = FileSystem.diskServer
                 AND DiskServer.status = 0 -- DISKSERVER_PRODUCTION
                 -- Ignore diskservers where a recaller already exists
                 AND DiskServer.id NOT IN (
                   SELECT DISTINCT(FileSystem.diskServer)
                     FROM FileSystem
                    WHERE nbRecallerStreams != 0
                      AND optimized = 1
                 )
            ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams, FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC, dbms_random.value)
    LOOP
      diskServerName := a.name;
      rmountPoint    := a.mountPoint;
      fileSystemId   := a.id;
      -- Check that we don't already have a copy of this file on this filesystem.
      -- This will never happen in normal operations but may be the case if a filesystem
      -- was disabled and did come back while the tape recall was waiting.
      -- Even if we could optimize by cancelling remaining unneeded tape recalls when a
      -- fileSystem comes backs, the ones running at the time of the come back will have     
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

    -- If we didn't find a filesystem rerun with optimization disabled
    IF fileSystemId = 0 THEN
      IF optimized = 1 THEN
        bestFileSystemForSegment(segmentId, diskServerName, rmountPoint, rpath, dcid, 0);
	RETURN;
      ELSE
        RAISE NO_DATA_FOUND; -- we did not find any suitable FS, even without optimization
      END IF;
    END IF;
  END IF;

  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Just like in bestTapeCopyForStream if we were called with optimization enabled
    -- and nothing was found, rerun the procedure again with optimization disabled to
    -- truly make sure nothing is found!
    IF optimized = 1 THEN
      bestFileSystemForSegment(segmentId, diskServerName, rmountPoint, rpath, dcid, 0);
      -- Since we recursively called ourselves, we should not do
      -- any update in the outer call
      RETURN;
    ELSE
      RAISE;
    END IF;
END;


/* PL/SQL method implementing fileRecallFailed */
CREATE OR REPLACE PROCEDURE fileRecallFailed(tapecopyId IN INTEGER) AS
 SubRequestId NUMBER;
 dci NUMBER;
 fsId NUMBER;
 fileSize NUMBER;
BEGIN
  SELECT SubRequest.id, DiskCopy.id, CastorFile.filesize
    INTO SubRequestId, dci, fileSize
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2; -- DISKCOPY_WAITTAPERECALL
  UPDATE DiskCopy SET status = 4 WHERE id = dci RETURNING fileSystem INTO fsid; -- DISKCOPY_FAILED
  IF SubRequestId IS NOT NULL THEN
    UPDATE SubRequest SET status = 7, -- SUBREQUEST_FAILED
                          getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED (not strictly correct but the request is over anyway)
                          lastModificationTime = getTime()
     WHERE id = SubRequestId;
    UPDATE SubRequest SET status = 7, -- SUBREQUEST_FAILED
                          getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
                          lastModificationTime = getTime(),
                          parent = 0
     WHERE parent = SubRequestId;
  END IF;
END;


/* PL/SQL method implementing streamsToDo */
CREATE OR REPLACE PROCEDURE streamsToDo(res OUT castor.Stream_Cur) AS
  streams "numList";
BEGIN
  SELECT UNIQUE Stream.id BULK COLLECT INTO streams
    FROM Stream, NbTapeCopiesInFS, FileSystem, DiskServer
   WHERE Stream.status = 0 -- PENDING
     AND NbTapeCopiesInFS.stream = Stream.id
     AND NbTapeCopiesInFS.NbTapeCopies > 0
     AND FileSystem.id = NbTapeCopiesInFS.FS
     AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
     AND DiskServer.id = FileSystem.DiskServer
     AND DiskServer.status IN (0, 1); -- PRODUCTION, DRAINING
  UPDATE Stream SET status = 1 -- WAITDRIVE
   WHERE id MEMBER OF streams;
  OPEN res FOR
    SELECT Stream.id, Stream.InitialSizeToTransfer, Stream.status,
           TapePool.id, TapePool.name
      FROM Stream, TapePool
     WHERE Stream.id MEMBER OF streams
       AND Stream.TapePool = TapePool.id;
END;


/* PL/SQL method implementing fileRecalled */
CREATE OR REPLACE PROCEDURE fileRecalled(tapecopyId IN INTEGER) AS
  subRequestId NUMBER;
  dci NUMBER;
  reqType NUMBER;
  cfId NUMBER;
  fsId NUMBER;
  fs NUMBER;
BEGIN
  SELECT SubRequest.id, DiskCopy.id, CastorFile.id, DiskCopy.fileSystem, Castorfile.FileSize
    INTO subRequestId, dci, cfid, fsId, fs
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2;  -- DISKCOPY_WAITTAPERECALL
   
  -- cleanup:
  -- delete any previous failed diskcopy for this castorfile (due to failed recall attempts for instance)
  DELETE FROM Id2Type WHERE id IN (SELECT id FROM DiskCopy WHERE castorFile = cfId AND status = 4);
  DELETE FROM DiskCopy WHERE castorFile = cfId AND status = 4;   -- FAILED
  -- mark as invalid any previous diskcopy in disabled filesystems, which may have triggered this recall
  UPDATE DiskCopy SET status = 7  -- INVALID
   WHERE fileSystem IN (SELECT id FROM FileSystem WHERE status = 2)  -- DISABLED
     AND castorFile = cfId
     AND status = 0;  -- STAGED
  
  SELECT type INTO reqType FROM Id2Type WHERE id =
    (SELECT request FROM SubRequest WHERE id = subRequestId);
  -- Repack handling:
  -- create the tapecopies for waiting subrequests and update diskcopies
  IF reqType = 119 THEN      -- OBJ_StageRepackRequest
    startRepackMigration(subRequestId, cfId, dci);
  ELSE
    UPDATE DiskCopy
       SET status = 0,  -- DISKCOPY_STAGED
           lastAccessTime = getTime(),    -- for the GC, effective lifetime of this diskcopy starts now
           gcWeight = size2gcweight(fs)
     WHERE id = dci;
    -- restart this subrequest if it's not a repack one
    UPDATE SubRequest
       SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
     WHERE id = subRequestId;
  END IF;
  -- restart other requests waiting on this recall
  UPDATE SubRequest
     SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
   WHERE parent = subRequestId;
  updateFsFileClosed(fsId);
END;


/* PL/SQL method implementing resetStream */
CREATE OR REPLACE PROCEDURE resetStream (sid IN INTEGER) AS
  nbRes NUMBER;
  unused NUMBER;
BEGIN
  BEGIN
    -- First lock the stream
    SELECT id INTO unused FROM Stream WHERE id = sid FOR UPDATE;
    -- Selecting any column with hint FIRST_ROW and relying
    -- on the exception mechanism in case nothing is found is
    -- far better than issuing a SELECT count(*) because ORACLE
    -- will then ignore the FIRST_ROWS and take ages...
    SELECT /*+ FIRST_ROWS */ Tapecopy.id INTO nbRes
      FROM Stream2TapeCopy, TapeCopy
     WHERE Stream2TapeCopy.Parent = sid
       AND Stream2TapeCopy.Child = TapeCopy.id
       AND status = 2 -- TAPECOPY_WAITINSTREAMS
       AND ROWNUM < 2;
    -- We'we found one, update stream status
    UPDATE Stream SET status = 0, tape = 0, lastFileSystemChange = 0
     WHERE id = sid; -- STREAM_PENDING
  EXCEPTION  WHEN NO_DATA_FOUND THEN
    -- We've found nothing, delete stream
    DELETE FROM Stream2TapeCopy WHERE Parent = sid;
    DELETE FROM Id2Type WHERE id = sid;
    DELETE FROM Stream WHERE id = sid;
  END;
  -- in any case, unlink tape and stream
  UPDATE Tape SET Stream = 0 WHERE Stream = sid;
END;


/* PL/SQL method implementing segmentsForTape */
CREATE OR REPLACE PROCEDURE segmentsForTape (tapeId IN INTEGER, segments
OUT castor.Segment_Cur) AS
  segs "numList";
  rows PLS_INTEGER := 500;
  CURSOR c1 IS
    SELECT Segment.id FROM Segment
    WHERE Segment.tape = tapeId AND Segment.status = 0 
    FOR UPDATE;
BEGIN
  OPEN c1;
  FETCH c1 BULK COLLECT INTO segs LIMIT rows;
  CLOSE c1;

  IF segs.COUNT > 0 THEN
     UPDATE Tape SET status = 4 -- MOUNTED
       WHERE id = tapeId;
     FORALL j IN segs.FIRST..segs.LAST -- bulk update with the forall..
       UPDATE Segment set status = 7 -- SELECTED
       WHERE id = segs(j);
  END IF;

  OPEN segments FOR
    SELECT fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm, segmCksum,
           errMsgTxt, errorCode, severity, blockId0, blockId1, blockId2, blockId3,
           creationTime, id, tape, copy, status, priority
      FROM Segment
     WHERE id IN (SELECT /*+ CARDINALITY(segsTable 5) */ * FROM TABLE(segs) segsTable);
END;


/* PL/SQL method implementing anySegmentsForTape */
CREATE OR REPLACE PROCEDURE anySegmentsForTape
(tapeId IN INTEGER, nb OUT INTEGER) AS
BEGIN
  SELECT count(*) INTO nb FROM Segment
  WHERE Segment.tape = tapeId
    AND Segment.status = 0;
  IF nb > 0 THEN
    UPDATE Tape SET status = 3 -- WAITMOUNT
    WHERE id = tapeId;
  END IF;
END;


/* PL/SQL method implementing failedSegments */
CREATE OR REPLACE PROCEDURE failedSegments
(segments OUT castor.Segment_Cur) AS
BEGIN
  OPEN segments FOR
    SELECT fseq, offset, bytes_in, bytes_out, host_bytes, segmCksumAlgorithm, segmCksum, 
           errMsgTxt, errorCode, severity, blockId0, blockId1, blockId2, blockId3, 
           creationTime, id, tape, copy, status, priority 
      FROM Segment
     WHERE Segment.status = 6; -- SEGMENT_FAILED
END;


/** Functions for the MigHunterDaemon **/

/** Cleanup before starting a new MigHunterDaemon **/
CREATE OR REPLACE PROCEDURE migHunterCleanUp(svcName IN VARCHAR2)
AS
  svcId NUMBER;
BEGIN
  SELECT id INTO svcId FROM SvcClass WHERE name = svcName;
  -- clean up tapecopies , WAITPOLICY reset into TOBEMIGRATED 
  UPDATE TapeCopy A SET status = 1
   WHERE status = 7 AND EXISTS (
     SELECT 'x' FROM CastorFile 
      WHERE CastorFile.id = A.castorfile
        AND CastorFile.svcclass = svcId);
  -- clean up streams, WAITPOLICY reset into CREATED			
  UPDATE Stream SET status = 5 WHERE status = 7 AND tapepool IN
   (SELECT svcclass2tapepool.child 
      FROM svcclass2tapepool 
     WHERE svcId = svcclass2tapepool.parent);
  COMMIT;
END;

/* Get input for python migration policy */
CREATE OR REPLACE PROCEDURE inputForMigrationPolicy
(svcclassName IN VARCHAR2,
 policyName OUT VARCHAR2,
 svcId OUT NUMBER,
 dbInfo OUT castor.DbMigrationInfo_Cur) AS
 tcIds "numList";
BEGIN
  -- do the same operation of getMigrCandidate and return the dbInfoMigrationPolicy
  -- we look first for repack condidates for this svcclass
  -- we update atomically WAITPOLICY
  SELECT SvcClass.migratorpolicy, SvcClass.id INTO policyName, svcId 
    FROM SvcClass 
   WHERE SvcClass.name = svcClassName;
  
  UPDATE TapeCopy A SET status = 7
   WHERE status IN (0, 1) AND
    EXISTS (SELECT 'x' FROM  SubRequest, StageRepackRequest
             WHERE StageRepackRequest.svcclass = svcId 
               AND SubRequest.request = StageRepackRequest.id
               AND SubRequest.status = 12  -- SUBREQUEST_REPACK
               AND A.castorfile = SubRequest.castorfile
    ) RETURNING A.id -- CREATED / TOBEMIGRATED
    BULK COLLECT INTO tcIds;
  COMMIT;
  -- if we didn't find anything, we look 
  -- the usual svcclass from castorfile table.
  -- we update atomically WAITPOLICY
  IF tcIds.count = 0 THEN
    UPDATE TapeCopy A SET status = 7  
     WHERE status IN (0, 1) AND
      EXISTS ( SELECT 'x' FROM  CastorFile 
     	WHERE A.castorFile = CastorFile.id 
          AND CastorFile.svcClass = svcId 
      ) RETURNING A.id -- CREATED / TOBEMIGRATED
      BULK COLLECT INTO tcIds;
      COMMIT;
  END IF;
  -- return the full resultset
  OPEN dbInfo FOR
    SELECT TapeCopy.id, TapeCopy.copyNb, CastorFile.lastknownfilename, 
           CastorFile.nsHost, CastorFile.fileid, CastorFile.filesize 
      FROM Tapecopy,CastorFile
     WHERE CastorFile.id = TapeCopy.castorfile 
       AND TapeCopy.id IN (SELECT * FROM table(tcIds)); 
END;


/* Get input for python Stream Policy */
CREATE OR REPLACE PROCEDURE inputForStreamPolicy
(svcClassName IN VARCHAR2,
 policyName OUT VARCHAR2,
 runningStreams OUT INTEGER,
 maxStream OUT INTEGER,
 dbInfo OUT castor.DbStreamInfo_Cur)
AS
  tpId NUMBER; -- used in the loop
  tcId NUMBER; -- used in the loop
  streamId NUMBER; -- stream attached to the tapepool
  svcId NUMBER; -- id for the svcclass
  strIds "numList"; 
BEGIN	
  -- info for policy
  SELECT streamPolicy, nbDrives, id INTO policyName, maxStream, svcId 
    FROM SvcClass WHERE SvcClass.name = svcClassName;
  SELECT count(*) INTO runningStreams FROM Stream,SvcClass2TapePool WHERE Stream.TapePool = SvcClass2TapePool.child AND SvcClass2TapePool.parent=svcId AND Stream.status = 3;   
  UPDATE stream SET status=7 WHERE Stream.status IN (4,5,6) AND Stream.id 
       IN (select Stream.id FROM Stream,SvcClass2TapePool WHERE Stream.Tapepool=SvcClass2TapePool.child AND SvcClass2TapePool.parent= svcId ) returning Stream.id BULK COLLECT INTO strIds; 
  COMMIT;
  --- return for policy
  OPEN dbInfo FOR
    SELECT Stream.id, count(distinct Stream2TapeCopy.child), sum(CastorFile.filesize)
      FROM Stream2TapeCopy, TapeCopy, CastorFile, Stream
     WHERE 
       Stream.id in (SELECT * FROM table(strIds))
       AND Stream2TapeCopy.child = TapeCopy.id(+)  
       AND TapeCopy.castorfile = CastorFile.id(+) 
       AND Stream.id = Stream2TapeCopy.parent(+)
       AND Stream.status=7
     GROUP BY Stream.id;
END;

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
     WHERE name = svcClassName AND ROWNUM <2;
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
    retCode :=-2 ; -- RTCPCLD_MSG_DATALIMIT
    RETURN;
  END IF;
    
  IF nbOldStream >=0 AND (doClone = 1 OR nbMigrationCandidate > 0) THEN
    -- stream creator
    SELECT SvcClass.nbDrives INTO nbDrives FROM SvcClass WHERE id = svcId;
    IF nbDrives = 0 THEN
    	retCode :=-3 ; -- RESTORE NEEDED
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
        VALUES (ids_seq.nextval, initSize, 0, 0, 0, 0, tpId, 5) RETURN id INTO strId;
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
          	UPDATE Stream set initialSizeToTransfer=oldSize WHERE id=strId;        
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
      SELECT count(id) into nbStream FROM Stream 
       WHERE Stream.tapepool = tapePoolIds(i);
      IF nbStream <> 0 THEN 
        -- we have at least a stream for that tapepool
        SELECT id INTO unused 
          FROM TapeCopy 
         WHERE Status = 7 AND id = tapeCopyIds(i) FOR UPDATE;
        -- let's attach it to the different streams
        FOR streamId IN (SELECT id FROM Stream WHERE Stream.tapepool = tapePoolIds(i)) LOOP
          UPDATE TapeCopy SET Status = 2 WHERE Status = 7 AND id = tapeCopyIds(i);
          DECLARE CONSTRAINT_VIOLATED EXCEPTION;
          PRAGMA EXCEPTION_INIT (CONSTRAINT_VIOLATED, -1);
          BEGIN 
            INSERT INTO stream2tapecopy (parent ,child) VALUES (streamId.id, tapeCopyIds(i));
          EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
            -- if the stream does not exist anymore
            UPDATE tapecopy SET status = 7 where id = tapeCopyIds(i);
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
  FOR i IN tapeCopyIds.FIRST .. tapeCopyIds.LAST LOOP
    UPDATE TapeCopy SET Status = 1 WHERE id = tapeCopyIds(i) AND Status = 7;
  END LOOP;  
  COMMIT;
END;

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

/* stop chosen stream */
CREATE OR REPLACE PROCEDURE startChosenStreams
        (streamIds IN castor."cnumList", initSize IN NUMBER) AS
  nbTc NUMBER;
BEGIN	
  FOR i IN streamIds.FIRST .. streamIds.LAST LOOP
    BEGIN  
      SELECT count(*) INTO nbTc FROM stream2tapecopy WHERE parent = streamIds(i); 
      IF nbTc = 0 THEN
        DELETE FROM Stream where id = streamIds(i);
      ELSE
        UPDATE Stream 
           SET status = 0, -- PENDING
                -- initialSize overwritten to initSize only if it is 0
                initialSizeToTransfer = decode(initialSizeToTransfer, 0, initSize, initialSizeToTransfer)
         WHERE Stream.status = 7 -- WAITPOLICY
           AND id = streamIds(i);
      END IF;
      COMMIT;
   END;  
  END LOOP;
END;

/* resurrect Candidates */
CREATE OR REPLACE PROCEDURE resurrectCandidates
(migrationCandidates IN castor."cnumList") -- all candidate before applying the policy
AS
  unused "numList";
BEGIN
  FORALL i IN migrationCandidates.FIRST .. migrationCandidates.LAST
    UPDATE TapeCopy SET Status=1 WHERE Status=7 AND id=migrationCandidates(i);
  COMMIT;
END;

/* invalidate tape copies */
CREATE OR REPLACE PROCEDURE invalidateTapeCopies
(tapecopyIds IN castor."cnumList") -- tapecopies not in the nameserver
AS
BEGIN
  FORALL i IN tapecopyIds.FIRST .. tapecopyIds.LAST
    UPDATE TapeCopy SET status = 6 WHERE id = tapecopyIds(i) AND status=7;
  COMMIT;
END;

/** Functions for the RecHandlerDaemon **/

/* Get input for python recall policy */
CREATE OR REPLACE PROCEDURE inputForRecallPolicy(dbInfo OUT castor.DbRecallInfo_Cur) AS
  svcId NUMBER;
BEGIN  
  OPEN dbInfo FOR 
    SELECT tape.id,tape.vid, count(distinct segment.id), sum(castorfile.filesize), 
           gettime - min(segment.creationtime), max(Segment.priority)
      FROM TapeCopy, CastorFile, Segment, Tape
     WHERE Tape.id = Segment.tape(+) 
       AND TapeCopy.id(+) = Segment.copy
       AND CastorFile.id(+) = TapeCopy.castorfile 
       AND Tape.status IN (1, 2, 3, 8)  -- PENDING, WAITDRIVE, WAITMOUNT, WAITPOLICY 
       AND Segment.status = 0  -- SEGMENT_UNPROCESSED 
     GROUP BY Tape.id, Tape.vid
     HAVING count(distinct segment.id) > 0;
END;

/* resurrect tapes */
CREATE OR REPLACE PROCEDURE resurrectTapes 
(tapeIds IN castor."cnumList") 
AS
BEGIN
  FOR i IN tapeIds.FIRST .. tapeIds.LAST LOOP
    UPDATE Tape SET status = 1 WHERE status = 8 AND id = tapeIds(i);
  END LOOP;
  COMMIT;
END;



