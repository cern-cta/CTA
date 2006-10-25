/* Updates the count of tapecopies in NbTapeCopiesInFS
   whenever a TapeCopy is linked to a Stream */
CREATE OR REPLACE TRIGGER tr_Stream2TapeCopy_Insert
AFTER INSERT ON Stream2TapeCopy
FOR EACH ROW
BEGIN
-- added this lock because of severaval copies of different file systems 
--  from different streams which can cause deadlock  
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
   BEFORE DELETE ON Stream2TapeCopy
   FOR EACH ROW
   BEGIN  
   -- added this lock because of severaval copies of different file systems 
   --  from different streams which can cause deadlock
     LOCK TABLE NbTapeCopiesInFS IN ROW SHARE MODE;
     UPDATE NbTapeCopiesInFS SET NbTapeCopies = NbTapeCopies - 1
      WHERE FS IN (SELECT DiskCopy.FileSystem
       FROM DiskCopy, TapeCopy
      WHERE DiskCopy.CastorFile = TapeCopy.castorFile
        AND TapeCopy.id = :old.child
        AND DiskCopy.status = 10) -- CANBEMIGR
        AND Stream = :old.parent;
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


/*  PL/SQL method to archive a SubRequest */
CREATE OR REPLACE PROCEDURE archiveSubReq(srId IN INTEGER) AS
  rid INTEGER;
  rtype INTEGER;
  rclient INTEGER;
  nb INTEGER;
  reqType INTEGER;
BEGIN
  UPDATE SubRequest SET status = 8 -- FINISHED
   WHERE id = srId RETURNING request INTO rid;

   -- Try to see whether another subrequest in the same
  -- request is still processing
  SELECT count(*) INTO nb FROM SubRequest
   WHERE request = rid AND status <> 8;  -- all but FINISHED

  -- Archive request if all subrequests have finished
  IF nb = 0 THEN
    UPDATE SubRequest SET status=11 WHERE request=rid and status=8;  -- ARCHIVED 
  END IF;
END;




/* PL/SQL method implementing bestFileSystemForSegment */
CREATE OR REPLACE PROCEDURE bestFileSystemForSegment(segmentId IN INTEGER, diskServerName OUT VARCHAR2,
                                                     rmountPoint OUT VARCHAR2, rpath OUT VARCHAR2,
                                                     dci OUT INTEGER) AS
 fileSystemId NUMBER;
 castorFileId NUMBER;
 deviation NUMBER;
 fsDiskServer NUMBER;
 fileSize NUMBER;
BEGIN
 -- First get the DiskCopy and see whether it already has a fileSystem
 -- associated (case of a multi segment file)
 -- We also select on the DiskCopy status since we know it is
 -- in WAITTAPERECALL status and there may be other ones
 -- INVALID, GCCANDIDATE, DELETED, etc...
 SELECT DiskCopy.fileSystem, DiskCopy.path, DiskCopy.id, DiskCopy.CastorFile
   INTO fileSystemId, rpath, dci, castorFileId
   FROM TapeCopy, Segment, DiskCopy
    WHERE Segment.id = segmentId
     AND Segment.copy = TapeCopy.id
     AND DiskCopy.castorfile = TapeCopy.castorfile
     AND DiskCopy.status = 2; -- WAITTAPERECALL
 -- Check if the DiskCopy had a FileSystem associated
 IF fileSystemId > 0 THEN
   BEGIN
     -- it had one, force filesystem selection, unless it was disabled.
     SELECT DiskServer.name, DiskServer.id, FileSystem.mountPoint, FileSystem.fsDeviation
     INTO diskServerName, fsDiskServer, rmountPoint, deviation
     FROM DiskServer, FileSystem
      WHERE FileSystem.id = fileSystemId
       AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
       AND DiskServer.id = FileSystem.diskServer
       AND DiskServer.status = 0; -- DISKSERVER_PRODUCTION
     updateFsFileOpened(fsDiskServer, fileSystemId, deviation, 0);
   EXCEPTION WHEN NO_DATA_FOUND THEN
     -- Error, the filesystem or the machine was probably disabled in between
     raise_application_error(-20101, 'In a multi-segment file, FileSystem or Machine was disabled before all segments were recalled');
   END;
 ELSE
   DECLARE
     CURSOR c1 IS SELECT DiskServer.name, FileSystem.mountPoint, FileSystem.id,
                       FileSystem.fsDeviation, FileSystem.diskserver, CastorFile.fileSize
                    FROM DiskServer, FileSystem, DiskPool2SvcClass,
                         (SELECT id, svcClass from StageGetRequest UNION ALL
                          SELECT id, svcClass from StagePrepareToGetRequest UNION ALL
			  SELECT id, svcClass from StageRepackRequest UNION ALL
                          SELECT id, svcClass from StageGetNextRequest UNION ALL
                          SELECT id, svcClass from StageUpdateRequest UNION ALL
                          SELECT id, svcClass from StagePrepareToUpdateRequest UNION ALL
                          SELECT id, svcClass from StageUpdateNextRequest) Request,
                         SubRequest, CastorFile
                   WHERE CastorFile.id = castorfileId
                     AND SubRequest.castorfile = castorfileId
                     AND Request.id = SubRequest.request
                     AND Request.svcclass = DiskPool2SvcClass.child
                     AND FileSystem.diskpool = DiskPool2SvcClass.parent
                     AND FileSystem.free + FileSystem.deltaFree - FileSystem.reservedSpace > CastorFile.fileSize
                     AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
                     AND DiskServer.id = FileSystem.diskServer
                     AND DiskServer.status = 0 -- DISKSERVER_PRODUCTION
                   ORDER BY FileSystem.weight + FileSystem.deltaWeight DESC, FileSystem.fsDeviation ASC;
      NotOk NUMBER := 1;
      nb NUMBER;
    BEGIN
      OPEN c1;
      LOOP
        FETCH c1 INTO diskServerName, rmountPoint, fileSystemId, deviation, fsDiskServer, fileSize;
        -- Check that we don't alredy have a copy of this file on this filesystem.
        -- This will never happend in normal operations but may be the case if a filesystem
        -- was disabled and did come back while the tape recall was waiting.
        -- Even if we could optimize by canceling remaining unneeded tape recalls when a
        -- fileSystem comes backs, the ones running at the time of the come back will have
        -- the problem.
        EXIT WHEN c1%NOTFOUND;
        SELECT count(*) INTO nb
          FROM DiskCopy
         WHERE fileSystem = fileSystemId
           AND CastorFile = castorfileId
           AND status = 0; -- STAGED
        IF nb = 0 THEN
          NotOk := 0;
          EXIT;
        END IF;
      END LOOP;
      CLOSE c1;
      IF NotOk != 0 THEN
        raise_application_error(-20101, 'Recall could not find a FileSystem with no copy of this file !');
      END IF;      
      UPDATE DiskCopy SET fileSystem = fileSystemId WHERE id = dci;
      updateFsFileOpened(fsDiskServer, fileSystemId, deviation, fileSize);
    END;
  END IF;
END;


/* PL/SQL method implementing castor package */
CREATE OR REPLACE PACKAGE castor AS
  TYPE DiskCopyCore IS RECORD (
        id INTEGER,
        path VARCHAR2(2048),
        status NUMBER,
        fsWeight NUMBER,
        mountPoint VARCHAR2(2048),
        diskServer VARCHAR2(2048));
  TYPE DiskCopy_Cur IS REF CURSOR RETURN DiskCopyCore;
  TYPE Segment_Cur IS REF CURSOR RETURN Segment%ROWTYPE;
  TYPE GCLocalFileCore IS RECORD (
        fileName VARCHAR2(2048),
        diskCopyId INTEGER);
  TYPE GCLocalFiles_Cur IS REF CURSOR RETURN GCLocalFileCore;
  TYPE "strList" IS TABLE OF VARCHAR2(2048) index by binary_integer;
  TYPE "cnumList" IS TABLE OF NUMBER index by binary_integer;
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
        lastKnownFileName VARCHAR2(2048));
  TYPE QueryLine_Cur IS REF CURSOR RETURN QueryLine;
  TYPE FileList_Cur IS REF CURSOR RETURN FilesDeletedProcOutput%ROWTYPE;
  TYPE DiskPoolQueryLine IS RECORD (
        isDP INTEGER,
        isDS INTEGER,
        diskServerName VARCHAR(2048),
	diskServerStatus INTEGER,
        fileSystemmountPoint VARCHAR(2048),
	fileSystemfreeSpace INTEGER,
	fileSystemtotalSpace INTEGER,
	fileSystemreservedSpace INTEGER,
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
	fileSystemreservedSpace INTEGER,
	fileSystemminfreeSpace INTEGER,
        fileSystemmaxFreeSpace INTEGER,
        fileSystemStatus INTEGER);
  TYPE DiskPoolsQueryLine_Cur IS REF CURSOR RETURN DiskPoolsQueryLine;
  TYPE IDRecord IS RECORD (id INTEGER);
  TYPE IDRecord_Cur IS REF CURSOR RETURN IDRecord;
END castor;



/* PL/SQL method implementing fileRecalled */
CREATE OR REPLACE PROCEDURE fileRecalled(tapecopyId IN INTEGER) AS
  SubRequestId NUMBER;
  dci NUMBER;
  fsId NUMBER;
  fileSize NUMBER;
  reqType NUMBER;
  nbTC NUMBER(2);
  cfid NUMBER;
  fcnbcopies NUMBER; --number of tapecopies in fileclass
BEGIN
  SELECT SubRequest.id, DiskCopy.id, CastorFile.filesize, CastorFile.id, FileClass.nbcopies
    INTO SubRequestId, dci, fileSize, cfid, fcnbcopies
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile, FileClass
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND FileClass.id = Castorfile.fileclass
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2;
   
  SELECT type INTO reqType FROM Id2Type WHERE id =
    (SELECT request FROM SubRequest WHERE id = subRequestId);

  UPDATE DiskCopy SET status = decode(reqType, 119,6, 0)  -- DISKCOPY_STAGEOUT if OBJ_StageRepackRequest, else DISKCOPY_STAGED 
   WHERE id = dci RETURNING fileSystem INTO fsid;
  -- delete any previous failed diskcopy for this castorfile (due to failed recall attempts for instance)
  DELETE FROM Id2Type WHERE id IN (SELECT id FROM DiskCopy WHERE castorFile = cfId AND status = 4);
  DELETE FROM DiskCopy WHERE castorFile = cfId AND status = 4;
  
  -- Repack handling:
  -- create the number of tapecopies for waiting subrequests and update their diskcopy.
  IF reqType = 119 THEN      -- OBJ_StageRepackRequest
    -- how many do we have to create ?
     SELECT count(StageRepackRequest.repackvid) INTO nbTC FROM subrequest, StageRepackRequest 
     WHERE subrequest.castorfile = cfid
     AND SubRequest.request = StageRepackRequest.id
     AND subrequest.status in (4,5,6);
	
     -- we are not allowed to create more Tapecopies than in the fileclass specified
     IF fcnbcopies < nbTC THEN 
       nbTC := fcnbcopies;
     END IF;
	
     -- we need to update other subrequests with the diskcopy
     UPDATE SubRequest SET diskcopy = dci 
     WHERE subrequest.castorfile = cfid
     AND subrequest.status in (4,5,6)
     AND SubRequest.request in 
     		(SELECT id FROM StageRepackRequest); 
	   
     -- create the number of tapecopies for the files
    internalPutDoneFunc(cfid, fsId, 0, nbTC);
    /** to avoid additional scheduling of the subrequest(s) 
        (because it is now in 1), we do it
     */
    UPDATE subrequest SET status = 12 -- SUBREQUEST_REPACK
     WHERE subrequest.castorfile = cfid
       AND subrequest.status = 1; -- SUBREQUEST_RESTART
  END IF;
  
  IF subRequestId IS NOT NULL THEN
    UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
     WHERE id = SubRequestId; 
    UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
     WHERE parent = SubRequestId;
  END IF;
  updateFsFileClosed(fsId, fileSize, fileSize);
END;


/* PL/SQL method implementing stageRm */
CREATE OR REPLACE PROCEDURE stageRm (fid IN INTEGER,
                                     nh IN VARCHAR2,
                                     ret OUT INTEGER) AS
  cfId INTEGER;
  nbRes INTEGER;
BEGIN
 -- Lock the access to the CastorFile
 -- This, together with triggers will avoid new TapeCopies
 -- or DiskCopies to be added
 SELECT id INTO cfId FROM CastorFile
  WHERE fileId = fid AND nsHost = nh FOR UPDATE;
 -- check if removal is possible for Migration
 SELECT count(*) INTO nbRes FROM TapeCopy
  WHERE status IN (0, 1, 2, 3) -- CREATED, TOBEMIGRATED, WAITINSTREAMS, SELECTED
    AND castorFile = cfId;
 IF nbRes > 0 THEN
   -- We found something, thus we cannot recreate
   ret := 1;
   RETURN;
 END IF;
 -- check if removal is possible for Disk2DiskCopy
 SELECT count(*) INTO nbRes FROM DiskCopy
  WHERE status = 1 -- DISKCOPY_WAITDISK2DISKCOPY
    AND castorFile = cfId;
 IF nbRes > 0 THEN
   -- We found something, thus we cannot remove
   ret := 2;
   RETURN;
 END IF;
 -- mark all get/put requests for the file as failed
 -- so the clients eventually get an answer
 FOR sr IN (SELECT id, status
              FROM SubRequest
             WHERE castorFile = cfId) LOOP
   IF sr.status IN (0, 1, 2, 3, 4, 5, 6, 7, 10) THEN  -- All but FINISHED, FAILED_FINISHED, ARCHIVED
     UPDATE SubRequest SET status = 7 WHERE id = sr.id;  -- FAILED
   END IF;
 END LOOP;
 -- set DiskCopies to GCCANDIDATE/INVALID. Note that we keep
 -- WAITTAPERECALL diskcopies so that recalls can continue
 UPDATE DiskCopy SET status = 8 -- GCCANDIDATE
  WHERE castorFile = cfId
    AND status IN (0, 6, 10); -- STAGED, STAGEOUT, CANBEMIGR
 UPDATE DiskCopy SET status = 7 -- INVALID
  WHERE castorFile = cfId
    AND status IN (5, 11); -- WAITFS, WAITFS_SCHEDULING
 DECLARE
  segId INTEGER;
  unusedIds "numList";
 BEGIN
   -- First lock all segments for the file
   SELECT segment.id BULK COLLECT INTO unusedIds
     FROM Segment, TapeCopy
    WHERE TapeCopy.castorfile = cfId
      AND TapeCopy.id = Segment.copy
      FOR UPDATE;
   -- Check whether we have any segment in SELECTED
   SELECT segment.id INTO segId
     FROM Segment, TapeCopy
    WHERE TapeCopy.castorfile = cfId
      AND TapeCopy.id = Segment.copy
      AND Segment.status = 7 -- SELECTED
      AND ROWNUM < 2;
   -- Something is running, so give up
 EXCEPTION WHEN NO_DATA_FOUND THEN
   -- Nothing running
   FOR t IN (SELECT id FROM TapeCopy WHERE castorfile = cfId) LOOP
     FOR s IN (SELECT id FROM Segment WHERE copy = t.id) LOOP
       -- Delete the segment(s)
       DELETE FROM Id2Type WHERE id = s.id;
       DELETE FROM Segment WHERE id = s.id;
     END LOOP;
     -- Delete the TapeCopy
     DELETE FROM Id2Type WHERE id = t.id;
     DELETE FROM TapeCopy WHERE id = t.id;
   END LOOP;
   -- Delete the DiskCopies
   UPDATE DiskCopy
      SET status = 8  -- GCCANDIDATE
    WHERE status = 2  -- WAITTAPERECALL
      AND castorFile = cfId;
 END;
 ret := 0;
END;


/*
 * Runs Garbage collection anywhere needed.
 * This is fired as a DBMS JOB from the FS trigger.
 */
CREATE OR REPLACE PROCEDURE garbageCollect AS
  fs NUMBER;
BEGIN
  LOOP
    -- get the oldest FileSystem to be garbage collected
    SELECT fsid INTO fs FROM
     (SELECT fsid FROM FileSystemGC ORDER BY submissionTime ASC)
    WHERE ROWNUM < 2;

    DELETE FROM FileSystemGC WHERE fsid = fs;
    -- run the GC
    garbageCollectFS(fs);
    -- yield to other jobs/transactions
    DBMS_LOCK.sleep(seconds => 1.0);
  END LOOP;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL;            -- terminate the job
END;



/*
 * Trigger launching garbage collection whenever needed
 * Note that we only launch it when at least 5 gigs are to be deleted
 */
CREATE OR REPLACE TRIGGER tr_FileSystem_Update
AFTER UPDATE OF free, deltaFree, reservedSpace ON FileSystem
FOR EACH ROW
DECLARE
  freeSpace NUMBER;
  jobid NUMBER;
  gcstime NUMBER;
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  -- compute the actual free space taking into account reservations (reservedSpace)
  -- and already running GC processes (spaceToBeFreed)
  freeSpace := :new.free + :new.deltaFree - :new.reservedSpace + :new.spaceToBeFreed;
  -- shall we launch a new GC?
  IF :new.minFreeSpace * :new.totalSize  > freeSpace AND
     -- is it really worth launching it? (some other GCs maybe are already running
     -- so we accept it only if it will free more than 5 Gb)
     :new.maxFreeSpace * :new.totalSize > freeSpace + 5000000000 THEN

    -- ok, we queue this filesystem for being garbage collected
    -- we have to take a lock so that a previous job that would finish right
    -- now will wait for our insert and take the new filesystem
    LOCK TABLE FileSystemGC IN ROW SHARE MODE;
    SELECT min(submissionTime) INTO gcstime FROM FileSystemGC;
    INSERT INTO FileSystemGC VALUES (:new.id, getTime());
    -- is it the only FS waiting for GC, or are there other FSs waiting since >10 mins
    -- (it can happen if the job failed or it has been killed)?
    IF gcstime IS NULL OR gcstime < getTime() - 600 THEN
      -- we spawn a job to do the real work. This avoids mutating table error
      -- and ensures that the current update does not fail if GC fails
      DBMS_JOB.SUBMIT(jobid,'garbageCollect();');
    END IF;
    -- otherwise, a recent job is already running and will take over this FS too
  END IF;
  EXCEPTION
    -- the filesystem was already selected for GC, do nothing
    WHEN CONSTRAINT_VIOLATED THEN NULL;
END;




CREATE MATERIALIZED VIEW MView_FS_DS_DP2SC
ORGANIZATION HEAP PCTFREE 0 COMPRESS
CACHE NOPARALLEL BUILD IMMEDIATE
REFRESH ON COMMIT COMPLETE
WITH PRIMARY KEY USING ENFORCED CONSTRAINTS
DISABLE QUERY REWRITE
AS
  SELECT FS.id fsId, DP2SC.child scId,
         FS.mountpoint fsMountPoint, DS.name dsName
    FROM diskpool2svcclass DP2SC, diskserver DS, filesystem FS
   WHERE FS.status = 0   -- PRODUCTION
     AND DS.status = 0   -- PRODUCTION
     AND DS.id(+) = FS.diskServer
     AND DP2SC.parent(+) = FS.diskPool;




/*
 * PL/SQL method implementing the core part of stage queries
 * It takes a list of castorfile ids as input
 */
CREATE OR REPLACE PROCEDURE internalStageQuery
 (cfs IN "numList",
  svcClassId IN NUMBER,
  result OUT castor.QueryLine_Cur) AS
BEGIN
  OPEN result FOR
    -- Here we get the status for each CastorFile as follows: if a valid diskCopy is found,
    -- its status is returned, else if a (prepareTo)Get|Put request is found and no diskCopy is there,
    -- WAITTAPERECALL is returned, else -1 (INVALID) is returned
    SELECT UNIQUE CF_DC.fileid, CF_DC.nsHost, CF_DC.dcId, CF_DC.path, CF_DC.filesize,
           nvl(CF_DC.status, decode(SubRequest.status, 0,2, 3,2, -1)), 



               -- SubRequest in status 0,3 (START,WAITSCHED) => 2 = DISKCOPY_WAITTAPERECALL is returned
           FS_DS.dsName, FS_DS.fsMountPoint, CF_DC.nbaccesses, CF_DC.lastKnownFileName
      FROM SubRequest, MView_FS_DS_DP2SC FS_DS,


           (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
            SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
            SELECT id, svcClass FROM StagePrepareToUpdateRequest UNION ALL
            SELECT id, svcClass FROM StageRepackRequest UNION ALL
            SELECT id, svcClass FROM StageGetRequest
           ) Req,            
           (SELECT /*+ INDEX (CastorFile) INDEX (DiskCopy) */
                   UNIQUE CastorFile.id as cfId, CastorFile.fileid, CastorFile.nsHost,
                   CastorFile.fileSize, CastorFile.nbaccesses, CastorFile.lastKnownFileName,
                   DiskCopy.id as dcId, DiskCopy.path, DiskCopy.status, DiskCopy.fileSystem as fsId
              FROM CastorFile, DiskCopy
             WHERE CastorFile.id IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
               AND CastorFile.id = DiskCopy.castorFile(+)   -- search for a valid diskcopy
               AND DiskCopy.status IN (0, 1, 2, 4, 5, 6, 7, 10, 11)
                -- ignore diskCopies in status GCCANDIDATE, BEINGDELETED or any other 'unknown' one
           ) CF_DC
     WHERE FS_DS.fsId(+) = CF_DC.fsId
       AND CF_DC.cfId = SubRequest.castorFile(+)            -- OR search for a running request
       AND SubRequest.request = Req.id(+)
       AND (svcClassId = 0             -- no svcClass given, or...
         OR FS_DS.scId = svcClassId        -- found diskCopy on the given svcClass
         OR ((CF_DC.fsId = 0               -- diskcopy not yet associated with filesystem...
             OR CF_DC.dcId IS NULL)        -- or diskcopy not yet created at all (prepareToXxx req)...
           AND Req.svcClass = svcClassId))     -- ...but found stagein or prepareToPut request
  ORDER BY fileid, nshost;
END;



/* PL/SQL Procedure to find migration candidates (TapeCopies) for the passed ServiceClass.
It checks for the TapeCopies in CREATED and TOBEMIGRATED if there is a RepackRequest
going on. If so, it returns for the requested svcclass the Migration candidates.
*/
CREATE OR REPLACE PROCEDURE selectTapeCopiesForMigration
(svcclassId IN NUMBER, result OUT castor.IDRecord_Cur) AS
BEGIN 
  OPEN result FOR 
    -- we look first for repack condidates for this svcclass
    SELECT TapeCopy.id
      FROM TapeCopy, SubRequest, StageRepackRequest
     WHERE StageRepackRequest.svcclass = svcclassId
       AND SubRequest.request = StageRepackRequest.id
       AND SubRequest.status = 12  --SUBREQUEST_REPACK
       AND TapeCopy.castorfile = SubRequest.castorfile
       AND TapeCopy.status IN (0, 1);  -- CREATED / TOBEMIGRATED
    -- if we didn't find anything, we look 
    -- the usual svcclass from castorfile table.
  IF result%ROWCOUNT = 0 THEN
  OPEN result FOR 
    SELECT TapeCopy.id 
      FROM TapeCopy, CastorFile 
     WHERE TapeCopy.castorFile = CastorFile.id 
       AND CastorFile.svcClass = svcclassId
       AND TapeCopy.status IN (0, 1); --CREATED / TOBEMIGRATED
  END IF;
END;





