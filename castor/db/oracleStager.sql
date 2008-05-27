/*******************************************************************
 *
 * @(#)$RCSfile: oracleStager.sql,v $ $Revision: 1.666 $ $Date: 2008/05/27 12:47:02 $ $Author: waldron $
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
  TYPE TapeCopy_Cur IS REF CURSOR RETURN TapeCopy%ROWTYPE;
  TYPE Segment_Cur IS REF CURSOR RETURN Segment%ROWTYPE;
  TYPE StreamCore IS RECORD (
    id INTEGER,
    initialSizeToTransfer INTEGER,
    status NUMBER,
    tapePoolId NUMBER,
    tapePoolName VARCHAR2(2048));
  TYPE Stream_Cur IS REF CURSOR RETURN StreamCore;
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
  TYPE SchedulerResourceLine IS RECORD (
    diskServerName VARCHAR(2048),
    diskServerStatus INTEGER,
    diskServerAdminStatus INTEGER,
    fileSystemMountPoint VARCHAR(2048),
    fileSystemStatus INTEGER,
    fileSystemAdminStatus INTEGER,
    fileSystemDiskPoolName VARCHAR(2048),
    fileSystemSvcClassName VARCHAR(2048));
  TYPE SchedulerResources_Cur IS REF CURSOR RETURN SchedulerResourceLine;	
  TYPE FileEntry IS RECORD (
    fileid INTEGER,
    nshost VARCHAR2(2048));
  TYPE FileEntry_Cur IS REF CURSOR RETURN FileEntry;
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
    byteVolume NUMBER);
  TYPE DbStreamInfo_Cur IS REF CURSOR RETURN DbStreamInfo;
  TYPE DbRecallInfo IS RECORD ( 
    vid VARCHAR2(2048),
    tapeId NUMBER,
    dataVolume NUMBER,
    numbFiles NUMBER,
    expireTime NUMBER);
  TYPE DbRecallInfo_Cur IS REF CURSOR RETURN DbRecallInfo;
END castor;

CREATE OR REPLACE TYPE "numList" IS TABLE OF INTEGER;


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
    -- Delete the DiskCopy
    UPDATE DiskCopy
       SET status = 7  -- INVALID
     WHERE id = cf.id;
    -- Look for request associated to the diskCopy and restart
    -- it and all the waiting ones
    UPDATE SubRequest SET status = 7 -- FAILED
     WHERE diskCopy = cf.id RETURNING id BULK COLLECT INTO srIds;
    UPDATE SubRequest
       SET status = 7, parent = 0 -- FAILED
     WHERE status = 5 -- WAITSUBREQ
       AND parent MEMBER OF srIds
       AND castorfile = cf.castorfile;
  END LOOP;
END;


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
  -- on all filesystems scheduled to be checked.
  FOR cf IN (SELECT UNIQUE d.castorfile, e.id
               FROM DiskCopy d, DiskCopy e 
              WHERE d.castorfile = e.castorfile
                AND d.fileSystem IN 
                  (SELECT /*+ CARDINALITY(fsidTable 5) */ * 
                     FROM TABLE(fsIds) fsidTable)
                AND d.status IN (0, 10)
                AND e.status = 2) LOOP
    -- Cancel recall and restart subrequests
    cancelRecall(cf.castorfile, cf.id, 1); -- RESTART
  END LOOP;
END;


/* Used to check consistency of diskcopies when a filesytem
   comes back to production after having been disabled for
   some time */
CREATE OR REPLACE TRIGGER tr_FileSystem_Update
BEFORE UPDATE of status ON FileSystem
FOR EACH ROW
WHEN (old.status IN (1, 2) AND -- DRAINING, DISABLED
      new.status = 0) -- PRODUCTION
DECLARE
  s NUMBER;
BEGIN
  SELECT status INTO s FROM DiskServer WHERE id = :old.diskServer;
  IF (s = 0) THEN
    checkFSBackInProd(:old.id);
  END IF;
END;


/* Used to check consistency of diskcopies when filesytems
   comes back to production after having been disabled for
   some time */
CREATE OR REPLACE TRIGGER tr_DiskServer_Update
BEFORE UPDATE of status ON DiskServer
FOR EACH ROW
WHEN (old.status IN (1, 2) AND -- DRAINING, DISABLED
      new.status = 0) -- PRODUCTION
BEGIN
  FOR fs IN (SELECT id, status FROM FileSystem WHERE diskServer = :old.id) LOOP
    IF (fs.status = 0) THEN
      checkFSBackInProd(fs.id);
    END IF;
  END LOOP;
END;


/***************************************/
/* Some triggers to prevent dead locks */
/***************************************/

/* Used to avoid LOCK TABLE TapeCopy whenever someone wants
   to deal with the tapeCopies on a CastorFile.
   Due to this trigger, locking the CastorFile is enough
   to be safe */
CREATE OR REPLACE TRIGGER tr_TapeCopy_CastorFile
BEFORE INSERT OR UPDATE OF castorFile ON TapeCopy
FOR EACH ROW WHEN (new.castorFile > 0)
DECLARE
  unused NUMBER;
BEGIN
  SELECT id INTO unused FROM CastorFile
   WHERE id = :new.castorFile FOR UPDATE;
END;


/* Used to avoid LOCK TABLE TapeCopy whenever someone wants
   to deal with the tapeCopies on a CastorFile.
   Due to this trigger, locking the CastorFile is enough
   to be safe */
CREATE OR REPLACE TRIGGER tr_DiskCopy_CastorFile
BEFORE INSERT OR UPDATE OF castorFile ON DiskCopy
FOR EACH ROW WHEN (new.castorFile > 0)
DECLARE
  unused NUMBER;
BEGIN
  SELECT id INTO unused FROM CastorFile
   WHERE id = :new.castorFile FOR UPDATE;
END;


/* PL/SQL method to get the next SubRequest to do according to the given service */
CREATE OR REPLACE PROCEDURE subRequestToDo(service IN VARCHAR2,
                                           srId OUT INTEGER, srRetryCounter OUT INTEGER, srFileName OUT VARCHAR2,
                                           srProtocol OUT VARCHAR2, srXsize OUT INTEGER, srPriority OUT INTEGER,
                                           srStatus OUT INTEGER, srModeBits OUT INTEGER, srFlags OUT INTEGER,
                                           srSubReqId OUT VARCHAR2, srAnswered OUT INTEGER) AS
LockError EXCEPTION;
PRAGMA EXCEPTION_INIT (LockError, -54);
CURSOR c IS
   SELECT /*+ USE_NL */ id
     FROM SubRequest
    WHERE status in (0,1,2)  -- START, RESTART, RETRY
      AND EXISTS
         (SELECT /*+ index(a I_Id2Type_id) */ 'x'
            FROM Id2Type a, Type2Obj
           WHERE a.id = SubRequest.request
             AND a.type = Type2Obj.type
             AND Type2Obj.svcHandler = service)
    FOR UPDATE SKIP LOCKED;
BEGIN
  srId := 0;
  OPEN c;
  FETCH c INTO srId;
  UPDATE SubRequest SET status = 3, subReqId = nvl(subReqId, uuidGen())
   WHERE id = srId  -- WAITSCHED
    RETURNING retryCounter, fileName, protocol, xsize, priority, status, modeBits, flags, subReqId, answered
    INTO srRetryCounter, srFileName, srProtocol, srXsize, srPriority, srStatus, srModeBits, srFlags, srSubReqId, srAnswered;
  CLOSE c;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- just return srId = 0, nothing to do
  NULL;
WHEN LockError THEN
  -- We have observed ORA-00054 errors (resource busy and acquire with NOWAIT) even with
  -- the SKIP LOCKED clause. This is a workaround to ignore the error until we understand
  -- what to do, another thread will pick up the request so we don't do anything.
  NULL;
END;


/* PL/SQL method to get the next failed SubRequest to do according to the given service */
/* the service parameter is not used now, it will with the new stager */
CREATE OR REPLACE PROCEDURE subRequestFailedToDo(srId OUT INTEGER, srRetryCounter OUT INTEGER, srFileName OUT VARCHAR2,
                                                 srProtocol OUT VARCHAR2, srXsize OUT INTEGER, srPriority OUT INTEGER,
                                                 srStatus OUT INTEGER, srModeBits OUT INTEGER, srFlags OUT INTEGER,
                                                 srSubReqId OUT VARCHAR2, srErrorCode OUT NUMBER,
                                                 srErrorMessage OUT VARCHAR2) AS
CURSOR c IS
   SELECT /*+ USE_NL */ id, answered
     FROM SubRequest
    WHERE status = 7  -- FAILED
    FOR UPDATE SKIP LOCKED;
srAnswered INTEGER;
BEGIN
  srId := 0;
  OPEN c;
  FETCH c INTO srId, srAnswered;
  IF srAnswered = 1 THEN
    -- already answered, ignore it
    UPDATE SubRequest SET status = 9 WHERE id = srId;  -- FAILED_FINISHED
    srId := 0;
  ELSE
    UPDATE subrequest SET status = 10 WHERE id = srId   -- FAILED_ANSWERING
      RETURNING retryCounter, fileName, protocol, xsize, priority, status,
                modeBits, flags, subReqId, errorCode, errorMessage
      INTO srRetryCounter, srFileName, srProtocol, srXsize, srPriority, srStatus,
           srModeBits, srFlags, srSubReqId, srErrorCode, srErrorMessage;
  END IF;
  CLOSE c;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- just return srId = 0, nothing to do
  NULL;
END;


/* PL/SQL method to get the next request to do according to the given service */
CREATE OR REPLACE PROCEDURE requestToDo(service IN VARCHAR2, rId OUT INTEGER) AS
BEGIN
  DELETE FROM NewRequests 
   WHERE type IN (
     SELECT type FROM Type2Obj
      WHERE svcHandler = service
     )
   AND ROWNUM < 2 RETURNING id INTO rId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  rId := 0;   -- nothing to do
END;


/* PL/SQL method to make a SubRequest wait on another one, linked to the given DiskCopy */
CREATE OR REPLACE PROCEDURE makeSubRequestWait(srId IN INTEGER, dci IN INTEGER) AS
BEGIN
  -- all wait on the original one; also prevent to wait on a PrepareToPut, for the
  -- case when Updates and Puts come after a PrepareToPut and they need to wait on
  -- the first Update|Put to complete.
  -- Cf. recreateCastorFile and the DiskCopy statuses 5 and 11
  UPDATE SubRequest
     SET parent = (SELECT SubRequest.id
                     FROM SubRequest, DiskCopy, Id2Type
                    WHERE SubRequest.diskCopy = DiskCopy.id
                      AND DiskCopy.id = dci
                      AND SubRequest.request = Id2Type.id
                      AND Id2Type.type <> 37  -- OBJ_PrepareToPut
                      AND SubRequest.parent = 0
                      AND DiskCopy.status IN (1, 2, 5, 6, 11) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT, WAITFS_SCHEDULING
                      AND SubRequest.status IN (0, 1, 2, 4, 13, 14, 6)), -- START, RESTART, RETRY, WAITTAPERECALL, WAITFORSCHED, BEINGSCHED, READY
        status = 5, -- WAITSUBREQ
        lastModificationTime = getTime()
  WHERE SubRequest.id = srId;
END;


/* PL/SQL method to delete one single request */
CREATE OR REPLACE PROCEDURE deleteRequest(rId IN INTEGER) AS
  rtype INTEGER;
  rclient INTEGER;
BEGIN  -- delete Request, Client and SubRequests
  -- delete request from Id2Type
  DELETE FROM Id2Type WHERE id = rId RETURNING type INTO rtype;
 
  -- delete request and get client id
  IF rtype = 35 THEN -- StageGetRequest
    DELETE FROM StageGetRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 40 THEN -- StagePutRequest
    DELETE FROM StagePutRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 44 THEN -- StageUpdateRequest
    DELETE FROM StageUpdateRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 39 THEN -- StagePutDoneRequest
    DELETE FROM StagePutDoneRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 42 THEN -- StageRmRequest
    DELETE FROM StageRmRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 51 THEN -- StageReleaseFilesRequest
    DELETE FROM StageReleaseFilesRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 36 THEN -- StagePrepareToGetRequest
    DELETE FROM StagePrepareToGetRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 37 THEN -- StagePrepareToPutRequest
    DELETE FROM StagePrepareToPutRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 38 THEN -- StagePrepareToUpdateRequest
    DELETE FROM StagePrepareToUpdateRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 95 THEN -- SetFileGCWeightRequest
    DELETE FROM SetFileGCWeight WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 119 THEN -- StageRepackRequest
    DELETE FROM StageRepackRequest WHERE id = rId RETURNING client INTO rclient;
  ELSIF rtype = 133 THEN -- StageDiskCopyReplicaRequest
    DELETE FROM StageDiskCopyReplicaRequest WHERE id = rId RETURNING client INTO rclient;
  END IF;

  -- Delete Client
  DELETE FROM Id2Type WHERE id = rclient;
  DELETE FROM Client WHERE id = rclient;
  
  -- Delete SubRequests
  DELETE FROM Id2Type WHERE id IN
    (SELECT id FROM SubRequest WHERE request = rId);
  DELETE FROM SubRequest WHERE request = rId;
END;


/*  PL/SQL method to archive a SubRequest */
CREATE OR REPLACE PROCEDURE archiveSubReq(srId IN INTEGER) AS
  rId INTEGER;
  nb INTEGER;
  nbReqs INTEGER;
  cfId NUMBER;
  rtype INTEGER;
BEGIN
  UPDATE SubRequest
     SET status = 8, parent = 0 -- FINISHED
   WHERE id = srId
  RETURNING request, castorFile INTO rId, cfId;

  -- Try to see whether another subrequest in the same
  -- request is still processing
  SELECT count(*) INTO nb FROM SubRequest
   WHERE request = rid AND status <> 8;  -- all but FINISHED

  IF nb = 0 THEN
    -- all subrequests have finished: archive or delete depending on the request type
    SELECT type INTO rtype FROM Id2Type WHERE id = rId;
    IF rtype IN (51, 95) THEN -- Release, SetFileGCWeight
      deleteRequest(rId);
    ELSE
      UPDATE SubRequest SET status = 11 WHERE request = rId;  -- ARCHIVED 
    END IF;
  END IF;

  -- Check that we don't have too many requests for this file in the DB
  -- XXX dropped for the time being as it introduced deadlocks with itself and with selectFiles2Delete!
END;


/* PL/SQL method checking whether a given service class
   is declared disk only and had only full diskpools.
   Returns 1 in such a case, 0 else */
CREATE OR REPLACE FUNCTION checkFailJobsWhenNoSpace(svcClassId NUMBER)
RETURN NUMBER AS
  diskOnlyFlag NUMBER;
  defFileSize NUMBER;
  unused NUMBER;
BEGIN
  -- Determine if the service class is disk only and the default
  -- file size. If the default file size is 0 we assume 2G
  SELECT hasDiskOnlyBehavior, 
         decode(defaultFileSize, 0, 2000000000, defaultFileSize)
    INTO diskOnlyFlag, defFileSize
    FROM SvcClass 
   WHERE id = svcClassId;
  -- If diskonly check that the pool has space
  IF (diskOnlyFlag = 1) THEN
    SELECT count(*) INTO unused
      FROM diskpool2svcclass, FileSystem, DiskServer
     WHERE diskpool2svcclass.child = svcClassId
       AND diskpool2svcclass.parent = FileSystem.diskPool
       AND FileSystem.diskServer = DiskServer.id
       AND FileSystem.status = 0 -- PRODUCTION
       AND DiskServer.status = 0 -- PRODUCTION
       AND totalSize * minAllowedFreeSpace < free - defFileSize;
    IF (unused = 0) THEN
      RETURN 1;
    END IF;
  END IF;
  RETURN 0;
END;


/* PL/SQL method implementing checkForD2DCopyOrRecall */
/* dcId is the DiskCopy id of the best candidate for replica, 0 if none is found (tape recall), -1 in case of user error */
/* Internally used by getDiskCopiesForJob and processPrepareRequest */
CREATE OR REPLACE
PROCEDURE checkForD2DCopyOrRecall(cfId IN NUMBER, srId IN NUMBER, reuid IN NUMBER, regid IN NUMBER,
                                  svcClassId IN NUMBER, dcId OUT NUMBER, srcSvcClassId OUT NUMBER) AS
  destSvcClass VARCHAR2(2048);
  authDest NUMBER;
BEGIN
  -- First check whether we are a disk only pool that is already full.
  -- In such a case, we should fail the request with an ENOSPACE error
  IF (checkFailJobsWhenNoSpace(svcClassId) = 1) THEN
    dcId := -1;
    UPDATE SubRequest
       SET status = 7, -- FAILED
           errorCode = 28, -- ENOSPC
           errorMessage = 'File creation canceled since diskPool is full'
     WHERE id = srId;
    COMMIT;
    RETURN;
  END IF;
  -- Resolve the service class id to a name
  SELECT name INTO destSvcClass FROM SvcClass WHERE id = svcClassId;
  -- Check that the user has the necessary access rights to create a file in the
  -- destination service class. I.e Check for StagePutRequest access rights.
  checkPermission(destSvcClass, reuid, regid, 40, authDest);
  -- Check whether there are any diskcopies available for a disk2disk copy
  SELECT id, sourceSvcClassId INTO dcId, srcSvcClassId
    FROM (
      SELECT DiskCopy.id, SvcClass.name sourceSvcClass, SvcClass.id sourceSvcClassId
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
       WHERE DiskCopy.castorfile = cfId
         AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
         AND FileSystem.id = DiskCopy.fileSystem
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
         AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.id = FileSystem.diskserver
         AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
         -- Check that the user has the necessary access rights to replicate a
         -- file from the source service class. Note: instead of using a
         -- StageGetRequest type here we use a StagDiskCopyReplicaRequest type
         -- to be able to distinguish between and read and replication requst.
         AND checkPermissionOnSvcClass(SvcClass.name, reuid, regid, 133) = 0
         AND NOT EXISTS (
           -- Don't select source diskcopies which already failed more than 10 times
           SELECT 'x'
             FROM StageDiskCopyReplicaRequest R, SubRequest
            WHERE SubRequest.request = R.id
              AND R.sourceDiskCopy = DiskCopy.id
              AND SubRequest.status = 9 -- FAILED
           HAVING COUNT(*) >= 10)
       ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                               FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams,
                               FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC
    )
  WHERE authDest = 0
    AND ROWNUM < 2;
  -- We found at least one, therefore we schedule a disk2disk
  -- copy from the existing diskcopy not available to this svcclass
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- We found no diskcopies at all. We should not schedule
  -- and make a tape recall... except ... in 2 cases :
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
    UPDATE SubRequest
       SET status = 7, -- FAILED
           errorCode = CASE
             WHEN dcStatus IN (5,11) THEN 16 -- WAITFS, WAITFSSCHEDULING, EBUSY
             WHEN dcStatus = 6 AND fsStatus = 0 and dsStatus = 0 THEN 16 -- STAGEOUT, PRODUCTION, PRODUCTION, EBUSY
             ELSE 1718 -- ESTNOTAVAIL
           END,
           errorMessage = CASE
             WHEN dcStatus IN (5, 11) THEN -- WAITFS, WAITFSSCHEDULING
               'File is being (re)created right now by another user'
             WHEN dcStatus = 6 AND fsStatus = 0 and dsStatus = 0 THEN -- STAGEOUT, PRODUCTION, PRODUCTION
               'File is being written to in another SvcClass'
             ELSE
               'All copies of this file are unavailable for now. Please retry later'
           END
     WHERE id = srId;
    COMMIT;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- We did not find the very special case, so if the user has the necessary
    -- access rights to create file in the destination service class we 
    -- trigger a tape recall.
    IF authDest = 0 THEN
      dcId := 0;
    ELSE
      dcId := -1;
      UPDATE SubRequest
         SET status = 7, -- FAILED
             errorCode = 13, -- EACCES
             errorMessage = 'Insufficient user privileges to trigger a recall or file replication request to the '''||destSvcClass||''' service class '
       WHERE id = srId;
      COMMIT;
    END IF;
  END;
END;


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


/* PL/SQL method implementing createDiskCopyReplicaRequest */
CREATE OR REPLACE PROCEDURE createDiskCopyReplicaRequest
(sourceSrId IN INTEGER, sourceDcId IN INTEGER, sourceSvcId IN INTEGER, 
 destSvcId IN INTEGER) AS
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
BEGIN
  -- Extract the castorfile associated with the request, this is needed to 
  -- create the StageDiskCopyReplicaRequest's diskcopy and subrequest entries. 
  -- Also for disk2disk copying across service classes make the originating 
  -- subrequest wait on the completion of the transfer.
  IF sourceSrId > 0 THEN
    UPDATE SubRequest
       SET status = 5, parent = ids_seq.nextval -- WAITSUBREQ
     WHERE id = sourceSrId
    RETURNING castorFile, parent INTO cfId, srId;
  ELSE
    SELECT castorfile INTO cfId FROM DiskCopy WHERE id = sourceDcId;
    SELECT ids_seq.nextval INTO srId FROM Dual;
  END IF;

  SELECT fileid, nshost, filesize, lastknownfilename 
    INTO fileId, nsHost, fileSize, fileName
    FROM CastorFile WHERE id = cfId;

  -- Create the Client entry. 
  INSERT INTO Client (ipaddress, port, id, version, secure)
    VALUES (0, 0, ids_seq.nextval, 0, 0)
  RETURNING id INTO clientId;
  INSERT INTO Id2Type (id, type) VALUES (clientId, 129);  -- OBJ_Client
  
  -- Create the StageDiskCopyReplicaRequest. The euid and egid values default to
  -- 0 here, this indicates the request came from the user root. When the
  -- jobManager encounters the StageDiskCopyReplicaRequest it will automatically
  -- use the stage:st account for submission into LSF.
  SELECT ids_seq.nextval INTO destDcId FROM Dual;
  INSERT INTO StageDiskCopyReplicaRequest 
    (svcclassname, reqid, creationtime, lastmodificationtime, id, svcclass, 
     client, sourcediskcopy, destdiskcopy, sourceSvcClass)
  VALUES ((SELECT name FROM SvcClass WHERE id = destSvcId), uuidgen(), gettime(), 
     gettime(), ids_seq.nextval, destSvcId, clientId, sourceDcId, destDcId, sourceSvcId)
  RETURNING id INTO reqId;
  INSERT INTO Id2Type (id, type) VALUES (reqId, 133);  -- OBJ_StageDiskCopyReplicaRequest;
 
  -- Create the SubRequest setting the initial status to READYFORSCHED for
  -- immediate dispatching i.e no stager processing by the jobManager.
  INSERT INTO SubRequest
    (retrycounter, filename, protocol, xsize, priority, subreqid, flags, modebits,
     creationtime, lastmodificationtime, answered, id, diskcopy, castorfile, parent,
     status, request, getnextstatus, errorcode)
  VALUES (0, fileName, 'rfio', fileSize, 0, uuidgen(), 0, 0, gettime(), gettime(),
     0, srId, destDcId, cfId, 0, 13, reqId, 0, 0);
  INSERT INTO Id2Type (id, type) VALUES (srId, 27);  -- OBJ_SubRequest
  
  -- Create the DiskCopy without filesystem
  buildPathFromFileId(fileId, nsHost, destDcId, rpath);
  INSERT INTO DiskCopy (path, id, filesystem, castorfile, status, creationTime, lastAccessTime, gcWeight)
    VALUES (rpath, destDcId, 0, cfId, 1, getTime(), getTime(), size2gcweight(fileSize));  -- WAITDISK2DISKCOPY  
  INSERT INTO Id2Type (id, type) VALUES (destDcId, 5);  -- OBJ_DiskCopy
  COMMIT;
END;


/* PL/SQL method implementing getDiskCopiesForJob */
/* the result output is a DiskCopy status for STAGED, DISK2DISKCOPY, RECALL or WAITFS
   -1 for user failure, -2 or -3 for subrequest put in WAITSUBREQ */
CREATE OR REPLACE PROCEDURE getDiskCopiesForJob
        (srId IN INTEGER, result OUT INTEGER,
         sources OUT castor.DiskCopy_Cur) AS
  nbDCs INTEGER;
  nbDSs INTEGER;
  upd INTEGER;
  dcIds "numList";
  svcClassId NUMBER;
  srcSvcClassId NUMBER;
  cfId NUMBER;
  srcDcId NUMBER;
  d2dsrId NUMBER;
  reuid NUMBER;
  regid NUMBER;
BEGIN
  -- retrieve the castorFile and the svcClass for this subrequest
  SELECT SubRequest.castorFile, Request.euid, Request.egid, Request.svcClass, Request.upd
    INTO cfId, reuid, regid, svcClassId, upd
    FROM (SELECT id, euid, egid, svcClass, 0 upd from StageGetRequest UNION ALL
          SELECT id, euid, egid, svcClass, 1 upd from StageUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = srId;
  -- lock the castorFile to be safe in case of concurrent subrequests
  SELECT id into cfId FROM CastorFile where id = cfId FOR UPDATE;
  
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
    COMMIT;
    RETURN;
  END IF;
     
  -- Look for available diskcopies
  SELECT COUNT(DiskCopy.id) INTO nbDCs
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
      FOR SELECT id, path, status, fsRate, mountPoint, name FROM (
            SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status,
                   FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                                  FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams, 
                                  FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) fsRate,
                   FileSystem.mountPoint, DiskServer.name,
                   -- Use the power of analytics to create a cumulative value of the length
                   -- of the diskserver name and filesystem mountpoint. We do this because
                   -- when there are many many copies of a file the requested filesystems
                   -- string created by the stager from these results e.g. ds1:fs1|ds2:fs2|
                   -- can exceed the maximum length allowed by LSF causing the submission
                   -- process to fail.
                   SUM(LENGTH(DiskServer.name) + LENGTH(FileSystem.mountPoint) + 2)
                   OVER (ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, 
                                                 FileSystem.nbReadStreams,
                                                 FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams, 
                                                 FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams)
                          DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) bytes
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
             ORDER BY fsRate DESC)
         WHERE bytes <= 200;
    -- Give an hint to the stager whether internal replication can happen or not:
    -- count the number of diskservers which DON'T contain a diskCopy for this castorFile
    -- and are hence eligible for replication should it need to be done
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
            AND DiskCopy.status IN (0, 10));  -- STAGED, CANBEMIGR
    IF nbDSs = 0 THEN
      -- no room for replication
      result := 0;  -- DISKCOPY_STAGED
    ELSE
      -- we have some diskservers, the stager will ultimately decide whether to replicate
      result := 1;  -- DISKCOPY_WAITDISK2DISKCOPY
    END IF;
  ELSE
    -- No diskcopies available for this service class:
    -- first check whether there's already a disk to disk copy going on
    BEGIN
      SELECT SubRequest.id INTO d2dsrId
        FROM StageDiskCopyReplicaRequest Req, SubRequest
       WHERE SubRequest.request = Req.id
         AND Req.svcClass = svcClassId    -- this is the destination service class
         AND status IN (13, 14, 6)  -- WAITINGFORSCHED, BEINGSCHED, READY
         AND castorFile = cfId;
      -- found it, wait on it
      UPDATE SubRequest
         SET parent = d2dsrId, status = 5  -- WAITSUBREQ
       WHERE id = srId;
      result := -2;
      COMMIT;
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- not found, we may have to schedule a disk to disk copy or trigger a recall
      checkForD2DCopyOrRecall(cfId, srId, reuid, regid, svcClassId, srcDcId, srcSvcClassId);
      IF srcDcId > 0 THEN
        -- create DiskCopyReplica request and make this subRequest wait on it
        createDiskCopyReplicaRequest(srId, srcDcId, srcSvcClassId, svcClassId);
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


/* PL/SQL method internalPutDoneFunc, used by fileRecalled and putDoneFunc.
   checks for diskcopies in STAGEOUT and creates the tapecopies for migration
 */
CREATE OR REPLACE PROCEDURE internalPutDoneFunc (cfId IN INTEGER,
                                                 fs IN INTEGER,
                                                 context IN INTEGER,
                                                 nbTC IN INTEGER) AS
  tcId INTEGER;
  dcId INTEGER;
BEGIN
  -- if no tape copy or 0 length file, no migration
  -- so we go directly to status STAGED
  IF nbTC = 0 OR fs = 0 THEN
    UPDATE DiskCopy
       SET status = 0, -- STAGED
           lastAccessTime = getTime(),    -- for the GC, effective lifetime of this diskcopy starts now
           gcWeight = size2gcweight(fs)
     WHERE castorFile = cfId AND status = 6; -- STAGEOUT
  ELSE
    -- update the DiskCopy status to CANBEMIGR
    UPDATE DiskCopy 
       SET status = 10, -- CANBEMIGR
           lastAccessTime = getTime(),    -- for the GC, effective lifetime of this diskcopy starts now
           gcWeight = size2gcweight(fs)
     WHERE castorFile = cfId AND status = 6 -- STAGEOUT
     RETURNING id INTO dcId;
    -- Create TapeCopies
    FOR i IN 1..nbTC LOOP
      INSERT INTO TapeCopy (id, copyNb, castorFile, status)
           VALUES (ids_seq.nextval, i, cfId, 0) -- TAPECOPY_CREATED
      RETURNING id INTO tcId;
      INSERT INTO Id2Type (id, type) VALUES (tcId, 30); -- OBJ_TapeCopy
    END LOOP;
  END IF;
  -- If we are a real PutDone (and not a put outside of a prepareToPut/Update)
  -- then we have to archive the original prepareToPut/Update subRequest
  IF context = 2 THEN
    DECLARE
      srId NUMBER;
    BEGIN
      -- There can be only a single PrepareTo request: any subsequent PPut would be
      -- rejected and any subsequent PUpdate would be directly archived (cf. processPrepareRequest).
      SELECT SubRequest.id INTO srId
        FROM SubRequest, 
         (SELECT id FROM StagePrepareToPutRequest UNION ALL
          SELECT id FROM StagePrepareToUpdateRequest) Request
       WHERE SubRequest.castorFile = cfId
         AND SubRequest.request = Request.id
         AND SubRequest.status = 6;  -- READY
      archiveSubReq(srId);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL; -- Ignore the missing subrequest
    END;
  END IF;
END;


/* PL/SQL method implementing putDoneFunc */
CREATE OR REPLACE PROCEDURE putDoneFunc (cfId IN INTEGER,
                                         fs IN INTEGER,
                                         context IN INTEGER) AS
  nc INTEGER;
BEGIN
  -- get number of TapeCopies to create
  SELECT nbCopies INTO nc FROM FileClass, CastorFile
   WHERE CastorFile.id = cfId AND CastorFile.fileClass = FileClass.id;
  -- and execute the internal putDoneFunc with the number of TapeCopies to be created
  internalPutDoneFunc(cfId, fs, context, nc);
END;


/* PL/SQL procedure implementing startRepackMigration */
CREATE OR REPLACE PROCEDURE startRepackMigration(srId IN INTEGER, cfId IN INTEGER, dcId IN INTEGER) AS
  nbTC NUMBER(2);
  nbTCInFC NUMBER(2);
  fsId NUMBER;
BEGIN
  UPDATE DiskCopy SET status = 6  -- DISKCOPY_STAGEOUT 
   WHERE id = dcId RETURNING fileSystem INTO fsId;
  -- how many do we have to create ?
  -- select subrequest in status 3 in case of repack with already staged diskcopy
  SELECT count(StageRepackRequest.repackVid) INTO nbTC
    FROM SubRequest, StageRepackRequest 
   WHERE subrequest.castorfile = cfId
     AND SubRequest.request = StageRepackRequest.id
     AND SubRequest.status IN (3, 4, 5, 6);  -- SUBREQUEST_WAITSCHED, WAITTAPERECALL, WAITSUBREQ, READY
  SELECT nbCopies INTO nbTCInFC
    FROM FileClass, CastorFile
   WHERE CastorFile.id = cfId
     AND FileClass.id = Castorfile.fileclass;
  -- we are not allowed to create more TapeCopies than in the FileClass specified
  IF nbTCInFC < nbTC THEN
    nbTC := nbTCInFC;
  END IF;
  
  -- we need to update other subrequests with the diskcopy
  -- we also update status so that we don't reschedule them
  UPDATE SubRequest 
     SET diskCopy = dcId, status = 12  -- SUBREQUEST_REPACK
   WHERE SubRequest.castorFile = cfId
     AND SubRequest.status IN (4, 5, 6)  -- SUBREQUEST_WAITTAPERECALL, WAITSUBREQ, READY
     AND SubRequest.request IN
       (SELECT id FROM StageRepackRequest); 
  
  -- create the required number of tapecopies for the files
  internalPutDoneFunc(cfId, fsId, 0, nbTC);
  -- set svcClass in the CastorFile for the migration
  UPDATE CastorFile SET svcClass = 
    (SELECT R.svcClass 
       FROM StageRepackRequest R, SubRequest
      WHERE SubRequest.request = R.id
        AND SubRequest.id = srId)
   WHERE id = cfId;
   
  -- update remaining STAGED diskcopies to CANBEMIGR too
  -- we may have them as result of disk2disk copies, and so far
  -- we only dealt with dcId
  UPDATE DiskCopy SET status = 10  -- DISKCOPY_CANBEMIGR
   WHERE castorFile = cfId AND status = 0;  -- DISKCOPY_STAGED
END;


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
  reuid NUMBER;
  regid NUMBER;
BEGIN
  -- retrieve the castorfile, the svcclass and the reqId for this subrequest
  SELECT SubRequest.castorFile, Request.euid, Request.egid, Request.svcClass, Request.repack
    INTO cfId, reuid, regid, svcClassId, repack
    FROM (SELECT id, euid, egid, svcClass, 0 repack FROM StagePrepareToGetRequest UNION ALL
          SELECT id, euid, egid, svcClass, 1 repack FROM StageRepackRequest UNION ALL
          SELECT id, euid, egid, svcClass, 0 repack FROM StagePrepareToUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = srId;
  -- lock the castor file to be safe in case of two concurrent subrequest
  SELECT id into cfId FROM CastorFile where id = cfId FOR UPDATE;

  -- Look for available diskcopies. Note that we never wait on other requests
  -- and we include WAITDISK2DISKCOPY as they are going to be available.
  SELECT COUNT(DiskCopy.id) INTO nbDCs
    FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = svcClassId
     AND FileSystem.status = 0 -- PRODUCTION
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.status = 0 -- PRODUCTION
     AND DiskCopy.status IN (0, 1, 6, 10);  -- STAGED, WAITDISK2DISKCOPY, STAGEOUT, CANBEMIGR

  -- For DiskCopyReplicaRequests which are waiting to be scheduled, the filesystem
  -- link in the diskcopy table is set to 0. As a consequence of this it is not
  -- possible to determine the service class via the filesystem -> diskpool -> svcclass
  -- relationship, as assumed in the previous query. Instead the service class of 
  -- the replication request must be used!!!
  IF nbDCs = 0 THEN
    SELECT COUNT(DiskCopy.id) INTO nbDCs
      FROM DiskCopy, StageDiskCopyReplicaRequest
     WHERE DiskCopy.id = StageDiskCopyReplicaRequest.destDiskCopy
       AND StageDiskCopyReplicaRequest.svcclass = svcClassId
       AND DiskCopy.castorfile = cfId
       AND DiskCopy.status = 1; -- WAITDISK2DISKCOPY
  END IF;

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
        startRepackMigration(srId, cfId, srcDcId);
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
          -- However at the moment it's not easy to restart a waiting repack after	 
          -- a migration (relevant db callback should be put in rtcpcld_updcFileMigrated(),	 
          -- rtcpcldCatalogueInterface.c:3300), so we simply fail this repack	 
          -- request and rely for the time being on Repack to submit	 
          -- such double tape repacks one by one.
          UPDATE SubRequest
             SET status = 7,  -- FAILED
                 errorCode = 16,  -- EBUSY
                 errorMessage = 'File is currently being written or migrated'
           WHERE id = srId;
          COMMIT;
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
    IF repack = 1 THEN
      createDiskCopyReplicaRequest(srId, srcDcId, srvSvcClassId, svcClassId);
      result := -2;  -- Repack waits on the disk to disk copy
    ELSE
      createDiskCopyReplicaRequest(0, srcDcId, srvSvcClassId, svcClassId);
      result := 1;  -- DISKCOPY_WAITDISK2DISKCOPY, for logging purposes
    END IF;
  ELSIF srcDcId = 0 THEN  -- recall
    BEGIN
      -- check whether there's already a recall, and get its svcClass
      SELECT Request.svcClass, DiskCopy.id
        INTO recSvcClass, recDcId
        FROM (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
              SELECT id, svcClass FROM StageGetRequest UNION ALL
              SELECT id, svcClass FROM StageRepackRequest UNION ALL
              SELECT id, svcClass FROM StageUpdateRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToUpdateRequest) Request,
             SubRequest, DiskCopy
       WHERE SubRequest.request = Request.id
         AND SubRequest.castorFile = cfId
         AND DiskCopy.castorFile = cfId
         AND DiskCopy.status = 2  -- WAITTAPERECALL
         AND SubRequest.status = 4;  -- WAITTAPERECALL 
      -- we found one: we need to wait if either we are in a different svcClass
      -- so that afterwards a disk-to-disk copy is triggered, or in case of
      -- Repack so to trigger the repack migration. Note that Repack never
      -- sends a double repack request on the same file.
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
  SELECT Req.svcclass, SubRequest.castorfile
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
    SELECT subrequest.id INTO putSubReq
      FROM SubRequest, Id2Type
     WHERE SubRequest.castorfile = cfId
       AND SubRequest.request = Id2Type.id
       AND Id2Type.type IN (40, 44)  -- Put, Update
       AND SubRequest.status IN (0, 1, 2, 3, 6, 13, 14) -- START, RESTART, RETRY, WAITSCHED, READY, READYFORSCHED, BEINGSCHED
       AND ROWNUM < 2;
    -- we've found one, we wait
    UPDATE SubRequest
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
    IF nbDCs > 0 THEN
      -- we have it
      result := 1;
      putDoneFunc(cfId, fs, 2);   -- context = PutDone
    ELSE
      -- This is a PutDone without a put (otherwise we would have found
      -- a DiskCopy on a FileSystem), so we fail the subrequest.
      UPDATE SubRequest SET
        status = 7,
        errorCode = 1,  -- EPERM
        errorMessage = 'putDone without a put, or wrong service class'
      WHERE id = rsubReqId;
      result := 0;  -- no go
      COMMIT;
    END IF;
  END;
END;


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
  unused INTEGER;
  putSC INTEGER;
  pputSC INTEGER;
  contextPIPP INTEGER;
BEGIN
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO unused FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Determine the context (Put inside PrepareToPut ?)
  BEGIN
    -- check that we are a Put/Update
    SELECT Request.svcClass INTO putSC
      FROM (SELECT id, svcClass FROM StagePutRequest UNION ALL
            SELECT id, svcClass FROM StageUpdateRequest) Request, SubRequest
     WHERE SubRequest.id = srId
       AND Request.id = SubRequest.request;
    BEGIN
      -- check that there is a PrepareToPut/Update going on. There can be only a single one
      -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
      -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
      SELECT SubRequest.diskCopy, PrepareRequest.svcClass INTO dcId, pputSC
        FROM (SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
       WHERE SubRequest.CastorFile = cfId
         AND PrepareRequest.id = SubRequest.request
         AND SubRequest.status = 6;  -- READY
      -- if we got here, we are a Put/Update inside a PrepareToPut
      -- however, are we in the same service class ?
      IF putSC != pputSC THEN
        -- No, this means we are a Put/Update and another PrepareToPut
        -- is already running in a different service class. This is forbidden
        dcId := 0;
        UPDATE SubRequest
           SET status = 7, -- FAILED
               errorCode = 16, -- EBUSY
               errorMessage = 'A prepareToPut is running in another service class for this file'
         WHERE id = srId;
        COMMIT;
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
      -- In case of prepareToPut we need to check that we are we the only one
      SELECT count(SubRequest.diskCopy) INTO nbPReqs
        FROM (SELECT id FROM StagePrepareToPutRequest UNION ALL
              SELECT id FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
       WHERE SubRequest.castorFile = cfId
         AND PrepareRequest.id = SubRequest.request
         AND SubRequest.status = 6;  -- READY
      -- Note that we did not select ourselves (we are in status 3)
      IF nbPReqs > 0 THEN
        -- this means we are a PrepareToPut and another PrepareToPut/Update
        -- is already running. This is forbidden
        dcId := 0;
        UPDATE SubRequest
           SET status = 7, -- FAILED
               errorCode = 16, -- EBUSY
               errorMessage = 'Another prepareToPut/Update is ongoing for this file'
         WHERE id = srId;
        COMMIT;
        RETURN;
      END IF;
      -- Everything is ok then
      contextPIPP := 0;
    END;
  END;
  IF contextPIPP = 0 THEN
    -- check if there is space in the diskpool in case of
    -- disk only pool
    DECLARE
      svcClassId NUMBER;
    BEGIN
      -- get the svcclass
      SELECT svcClass INTO svcClassId
        FROM Subrequest,
             (SELECT id, svcClass FROM StagePutRequest UNION ALL
              SELECT id, svcClass FROM StageUpdateRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToUpdateRequest) Request
       WHERE SubRequest.id = srId
         AND Request.id = SubRequest.request;
      IF (checkFailJobsWhenNoSpace(svcClassId) = 1) THEN
        -- The svcClass is declared disk only and has no space
        -- thus we cannot recreate the file
        dcId := 0;
        UPDATE SubRequest
           SET status = 7, -- FAILED
               errorCode = 28, -- ENOSPC
               errorMessage = 'File creation canceled since diskPool is full'
         WHERE id = srId;
        COMMIT;
        RETURN;
      END IF;
    END;
    -- check if recreation is possible for TapeCopies
    SELECT count(*) INTO nbRes FROM TapeCopy
     WHERE status = 3 -- TAPECOPY_SELECTED
      AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot recreate
      dcId := 0;
      UPDATE SubRequest
         SET status = 7, -- FAILED
             errorCode = 16, -- EBUSY
             errorMessage = 'File recreation canceled since file is being migrated'
        WHERE id = srId;
      COMMIT;
      RETURN;
    END IF;
    -- check if recreation is possible for DiskCopies
    SELECT count(*) INTO nbRes FROM DiskCopy
     WHERE status IN (1, 2, 5, 6) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT
       AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot recreate
      dcId := 0;
      UPDATE SubRequest
         SET status = 7, -- FAILED
             errorCode = 16, -- EBUSY
             errorMessage = 'File recreation canceled since file is either being recalled, or replicated or created by another user'
       WHERE id = srId;
      COMMIT;
      RETURN;
    END IF;
    -- delete all tapeCopies
    deleteTapeCopies(cfId);
    -- set DiskCopies to INVALID
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE castorFile = cfId AND status IN (0, 10); -- STAGED, CANBEMIGR
    -- create new DiskCopy
    SELECT fileId, nsHost INTO fid, nh FROM CastorFile WHERE id = cfId;
    SELECT ids_seq.nextval INTO dcId FROM DUAL;
    buildPathFromFileId(fid, nh, dcId, rpath);
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime, lastAccessTime, gcWeight)
         VALUES (rpath, dcId, 0, cfId, 5, getTime(), getTime(), 0); -- status WAITFS
    INSERT INTO Id2Type (id, type) VALUES (dcId, 5); -- OBJ_DiskCopy
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
          SELECT SubRequest.id INTO srParent
            FROM SubRequest, Id2Type
           WHERE request = Id2Type.id
             AND type IN (40, 44)  -- Put, Update
             AND diskCopy = dcId 
             AND status IN (13, 14, 6)  -- READYFORSCHED, BEINGSCHED, READY
             AND ROWNUM < 2;   -- if we have more than one just take one of them
          UPDATE SubRequest
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
  -- reset filesize to 0: we are truncating the file
  UPDATE CastorFile SET fileSize = 0
   WHERE id = cfId;
  -- link SubRequest and DiskCopy
  UPDATE SubRequest SET diskCopy = dcId,
                        lastModificationTime = getTime()                        
   WHERE id = srId;
  -- we don't commit here, the stager will do that when
  -- the subRequest status will be updated to 6
END;


/* PL/SQL method implementing selectCastorFile */
CREATE OR REPLACE PROCEDURE selectCastorFile (fId IN INTEGER,
                                              nh IN VARCHAR2,
                                              sc IN INTEGER,
                                              fc IN INTEGER,
                                              fs IN INTEGER,
                                              fn IN VARCHAR2,
                                              rid OUT INTEGER,
                                              rfs OUT INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  BEGIN
    -- try to find an existing file
    SELECT id, fileSize INTO rid, rfs FROM CastorFile
      WHERE fileId = fid AND nsHost = nh;
    -- update lastAccess time
    UPDATE CastorFile SET LastAccessTime = getTime(),
                          nbAccesses = nbAccesses + 1,
                          lastKnownFileName = REGEXP_REPLACE(fn,'(/){2,}','/')
      WHERE id = rid;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- insert new row
    INSERT INTO CastorFile (id, fileId, nsHost, svcClass, fileClass, fileSize,
                            creationTime, lastAccessTime, nbAccesses, lastKnownFileName)
      VALUES (ids_seq.nextval, fId, nh, sc, fc, fs, getTime(), getTime(), 1, REGEXP_REPLACE(fn,'(/){2,}','/'))
      RETURNING id, fileSize INTO rid, rfs;
    INSERT INTO Id2Type (id, type) VALUES (rid, 2); -- OBJ_CastorFile
  END;
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  -- retry the select since a creation was done in between
  SELECT id, fileSize INTO rid, rfs FROM CastorFile
    WHERE fileId = fid AND nsHost = nh;
END;

/*PL/SQL method implementing selectPhysicalfilename for the preparetoGet request when the protocol is xrootd*/
CREATE OR REPLACE PROCEDURE selectPhysicalFileName(cfId IN NUMBER,
						      svcClassId IN NUMBER,
                             			      dcP OUT VARCHAR2,
						      fsmp OUT VARCHAR2) AS
BEGIN
 SELECT path, mountpoint INTO dcP,fsmp from(
        select DiskCopy.path, Filesystem.mountpoint
        FROM DiskCopy, SubRequest, FileSystem, DiskServer, DiskPool2SvcClass,castorfile
           WHERE castorfile.fileid=cfId
            AND FileSystem.diskpool = DiskPool2SvcClass.parent
            AND DiskPool2SvcClass.child = svcClassId
            AND DiskCopy.status IN (0, 6, 10) -- STAGED, STAGEOUT, CANBEMIGR
            AND FileSystem.id = DiskCopy.fileSystem
            AND FileSystem.status = 0  -- PRODUCTION
            AND DiskServer.id = FileSystem.diskServer
            AND DiskServer.status = 0  -- PRODUCTION
	   -- and rownum < 2
	   ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                                FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams,
                                FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC)
 where rownum < 2;
END;


/* PL/SQL method implementing stageRelease */
CREATE OR REPLACE PROCEDURE stageRelease (fid IN INTEGER,
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
  -- check if removal is possible for TapeCopies
  SELECT count(*) INTO nbRes FROM TapeCopy
   WHERE status = 3 -- TAPECOPY_SELECTED
     AND castorFile = cfId;
  IF nbRes > 0 THEN
    -- We found something, thus we cannot recreate
    ret := 1;
    RETURN;
  END IF;
  -- check if recreation is possible for SubRequests
  SELECT count(*) INTO nbRes FROM SubRequest
   WHERE status != 11 AND castorFile = cfId;   -- ARCHIVED
  IF nbRes > 0 THEN
    -- We found something, thus we cannot recreate
    ret := 2;
    RETURN;
  END IF;
  -- set DiskCopies to INVALID
  UPDATE DiskCopy SET status = 7 -- INVALID
   WHERE castorFile = cfId AND status = 0; -- STAGED
  ret := 0;
END;

/* PL/SQL method implementing stageForcedRm */
CREATE OR REPLACE PROCEDURE stageForcedRm (fid IN INTEGER,
                                           nh IN VARCHAR2,
                                           result OUT NUMBER) AS
  cfId INTEGER;
  nbRes INTEGER;
  dcsToRm "numList";
BEGIN
  -- by default, we are successful
  result := 1;
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO cfId FROM CastorFile
   WHERE fileId = fid AND nsHost = nh FOR UPDATE;
  -- list diskcopies
  SELECT id BULK COLLECT INTO dcsToRm
    FROM DiskCopy
   WHERE castorFile = cfId
     AND status IN (0, 5, 6, 10, 11);  -- STAGED, WAITFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
  -- If nothing found, report ENOENT
  IF dcsToRm.COUNT = 0 THEN
    result := 0;
    RETURN;
  END IF;
  -- Stop ongoing recalls
  deleteTapeCopies(cfId);
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  FOR sr IN (SELECT id, status FROM SubRequest
              WHERE diskcopy IN 
                (SELECT /*+ CARDINALITY(dcidTable 5) */ * 
                   FROM TABLE(dcsToRm) dcidTable)
                AND status IN (0, 1, 2, 5, 6, 13)) LOOP   -- START, WAITSUBREQ, READY, READYFORSCHED
    UPDATE SubRequest
       SET status = 7,  -- FAILED
           errorCode = 4,  -- EINTR
           errorMessage = 'Canceled by another user request',
           parent = 0
     WHERE (id = sr.id) OR (parent = sr.id);
  END LOOP;
  -- Set selected DiskCopies to INVALID
  FORALL i IN dcsToRm.FIRST .. dcsToRm.LAST
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE id = dcsToRm(i);
END;

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
BEGIN
  ret := 0;
  BEGIN
    -- Lock the access to the CastorFile
    -- This, together with triggers will avoid new TapeCopies
    -- or DiskCopies to be added
    SELECT id INTO cfId FROM CastorFile
     WHERE fileId = fid AND nsHost = nh FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- This file does not exist in the stager catalog
    -- so we just fail the request
    UPDATE SubRequest
       SET status = 7,  -- FAILED
           errorCode = 2,  -- ENOENT
           errorMessage = 'File not found on disk cache'
     WHERE id = srId;
    RETURN;
  END;
  -- First select involved diskCopies
  scId := svcClassId;
  IF scId > 0 THEN
    SELECT id BULK COLLECT INTO dcsToRm FROM (
      -- first physical diskcopies
      SELECT DC.id
        FROM DiskCopy DC, FileSystem, DiskPool2SvcClass DP2SC
       WHERE DC.castorFile = cfId
         AND DC.status IN (0, 1, 2, 6, 10)  -- STAGED, STAGEOUT, CANBEMIGR, WAITDISK2DISKCOPY, WAITTAPERECALL
         AND DC.fileSystem = FileSystem.id
         AND FileSystem.diskPool = DP2SC.parent
         AND DP2SC.child = scId)
    UNION ALL (
      -- and then diskcopies resulting from PrepareToPut|Update requests
      SELECT DC.id
        FROM (SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToUpdateRequest) PrepareRequest,
             SubRequest, DiskCopy DC
       WHERE SubRequest.diskCopy = DC.id
         AND PrepareRequest.id = SubRequest.request
         AND PrepareRequest.svcClass = scId
         AND DC.castorFile = cfId
         AND DC.status IN (5, 11)  -- WAITFS, WAITFS_SCHEDULING
      );
    IF dcsToRm.COUNT = 0 THEN
      -- We didn't find anything on this svcClass, fail and return
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 2,  -- ENOENT
             errorMessage = 'File not found on this service class'
       WHERE id = srId;
      RETURN;
    END IF;
    -- Check whether something else is left: if not, do as
    -- we are performing a stageRm everywhere
    SELECT count(*) INTO nbRes FROM DiskCopy
     WHERE castorFile = cfId
       AND status IN (0, 1, 2, 5, 6, 10, 11)  -- WAITDISK2DISKCOPY, WAITTAPERECALL, STAGED, STAGEOUT, CANBEMIGR, WAITFS, WAITFS_SCHEDULING
       AND id NOT IN (SELECT /*+ CARDINALITY(dcidTable 5) */ * 
                        FROM TABLE(dcsToRm) dcidTable);
    IF nbRes = 0 THEN
      scId := 0;
    END IF;
  END IF;
  IF scId = 0 THEN
    SELECT id BULK COLLECT INTO dcsToRm
      FROM DiskCopy
     WHERE castorFile = cfId
       AND status IN (0, 1, 2, 5, 6, 10, 11);  -- WAITDISK2DISKCOPY, WAITTAPERECALL, STAGED, WAITFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
  END IF;
  
  IF scId = 0 THEN
    -- full cleanup is to be performed, do all necessary checks beforehand
    DECLARE
      segId INTEGER;
      unusedIds "numList";
    BEGIN
      -- check if removal is possible for migration
      SELECT count(*) INTO nbRes FROM TapeCopy
       WHERE status IN (0, 1, 2, 3) -- CREATED, TOBEMIGRATED, WAITINSTREAMS, SELECTED
         AND castorFile = cfId;
      IF nbRes > 0 THEN
        -- We found something, thus we cannot remove
        UPDATE SubRequest
           SET status = 7,  -- FAILED
               errorCode = 16,  -- EBUSY
               errorMessage = 'The file is not yet migrated'
         WHERE id = srId;
        RETURN;
      END IF;
      -- check if removal is possible for Disk2DiskCopy
      SELECT count(*) INTO nbRes FROM DiskCopy
       WHERE status = 1 -- DISKCOPY_WAITDISK2DISKCOPY
         AND castorFile = cfId;
      IF nbRes > 0 THEN
        -- We found something, thus we cannot remove
        UPDATE SubRequest
           SET status = 7,  -- FAILED
               errorCode = 16,  -- EBUSY
               errorMessage = 'The file is being replicated'
         WHERE id = srId;
        RETURN;
      END IF;
      -- Stop ongoing recalls if stageRm either everywhere or the only available diskcopy.
      -- This is not entirely clean: a proper operation here should be to
      -- drop the SubRequest waiting for recall but keep the recall if somebody
      -- else is doing it, and taking care of other WAITSUBREQ requests as well...
      -- but it's fair enough, provided that the last stageRm will cleanup everything.
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
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 16,  -- EBUSY
             errorMessage = 'The file is being recalled from tape'
       WHERE id = srId;
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Nothing running
      deleteTapeCopies(cfId);
      -- Delete the DiskCopies
      UPDATE DiskCopy
         SET status = 7  -- INVALID
       WHERE status = 2  -- WAITTAPERECALL
         AND castorFile = cfId;
      -- Mark the 'recall' SubRequests as failed
      -- so that clients eventually get an answer
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 4,  -- EINTR
             errorMessage = 'Recall canceled by another user request'
       WHERE castorFile = cfId and status IN (4, 5);   -- WAITTAPERECALL, WAITSUBREQ
    END;
  END IF;

  -- Now perform the remove:
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  FOR sr IN (SELECT id FROM SubRequest
              WHERE diskcopy IN (SELECT /*+ CARDINALITY(dcidTable 5) */ * 
                                   FROM TABLE(dcsToRm) dcidTable)
                AND status IN (0, 1, 2, 5, 6, 13)) LOOP   -- START, WAITSUBREQ, READY, READYFORSCHED
    UPDATE SubRequest 
       SET status = 7, parent = 0,  -- FAILED
           errorCode = 4,  -- EINT
           errorMessage = 'Canceled by another user request'
     WHERE (id = sr.id) OR (parent = sr.id);
  END LOOP;
  -- Set selected DiskCopies to INVALID. In any case keep
  -- WAITTAPERECALL diskcopies so that recalls can continue.
  -- Note that WAITFS and WAITFS_SCHEDULING DiskCopies don't exist on disk
  -- so they will only be taken by the cleaning daemon for the failed DCs.
  FORALL i IN dcsToRm.FIRST .. dcsToRm.LAST
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE id = dcsToRm(i);
  ret := 1;  -- ok
END;


/* PL/SQL method implementing a setFileGCWeight request */
CREATE OR REPLACE PROCEDURE setFileGCWeightProc
(fid IN NUMBER, nh IN VARCHAR2, svcClassId IN NUMBER, weight IN FLOAT, ret OUT INTEGER) AS
BEGIN
  ret := 0;
  UPDATE DiskCopy
     SET gcWeight = gcWeight + weight
   WHERE castorFile = (SELECT id FROM CastorFile WHERE fileid = fid AND nshost = nh)
     AND fileSystem IN (
       SELECT FileSystem.id
         FROM FileSystem, DiskPool2SvcClass D2S
        WHERE FileSystem.diskPool = D2S.parent
          AND D2S.child = svcClassId);
  IF SQL%ROWCOUNT > 0 THEN
    ret := 1;   -- some diskcopies found, ok
  END IF;
END;


/* PL/SQL procedure which is executed whenever a files has been written to tape by the migrator to
 * check, whether the file information has to be added to the NameServer or to replace an entry 
 * (repack case)
 */
CREATE OR REPLACE PROCEDURE checkFileForRepack(fid IN INTEGER, ret OUT VARCHAR2) AS
  sreqid NUMBER;
BEGIN
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
  UPDATE SubRequest set status = 11  -- SUBREQUEST_ARCHIVED
   WHERE SubRequest.id = sreqid;
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
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
  UPDATE SubRequest SET status = newStatus,
                        answered = 1,
                        lastModificationTime = getTime() WHERE id = srId;
  IF newStatus IN (6, 11) THEN  -- READY, ARCHIVED
    UPDATE SubRequest SET getNextStatus = 1 -- GETNEXTSTATUS_FILESTAGED
     WHERE id = srId;
  END IF;
  -- Check whether it was the last subrequest in the request
  result := 1;
  SELECT id INTO result FROM SubRequest
   WHERE request = reqId
     AND status IN (0, 1, 2, 3, 4, 5, 7, 10, 12, 13, 14)   -- all but FINISHED, FAILED_FINISHED, ARCHIVED
     AND answered = 0
     AND ROWNUM < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN -- No data found means we were last
  result := 0;
END;


/* PL/SQL method implementing storeClusterStatus */
CREATE OR REPLACE PROCEDURE storeClusterStatus
(machines IN castor."strList",
 fileSystems IN castor."strList",
 machineValues IN castor."cnumList",
 fileSystemValues IN castor."cnumList") AS
 ind NUMBER;
 machine NUMBER := 0;
 fs NUMBER := 0;
BEGIN
  -- First Update Machines
  FOR i in machines.FIRST .. machines.LAST LOOP
    ind := machineValues.FIRST + 9 * (i - machines.FIRST);
    IF machineValues(ind + 1) = 3 THEN -- ADMIN DELETED
      BEGIN
        SELECT id INTO machine FROM DiskServer WHERE name = machines(i);
        DELETE FROM id2Type WHERE id = machine;
        DELETE FROM id2Type WHERE id IN (SELECT id FROM FileSystem WHERE diskServer = machine);
        DELETE FROM FileSystem WHERE diskServer = machine;
        DELETE FROM DiskServer WHERE name = machines(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- Fine, was already deleted
        NULL;
      END;
    ELSE
      BEGIN
        SELECT id INTO machine FROM DiskServer WHERE name = machines(i);
        UPDATE DiskServer
           SET status = machineValues(ind),
               adminStatus = machineValues(ind + 1),
               readRate = machineValues(ind + 2),
               writeRate = machineValues(ind + 3),
               nbReadStreams = machineValues(ind + 4),
               nbWriteStreams = machineValues(ind + 5),
               nbReadWriteStreams = machineValues(ind + 6),
               nbMigratorStreams = machineValues(ind + 7),
               nbRecallerStreams = machineValues(ind + 8)
         WHERE name = machines(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        DECLARE
          mid INTEGER;
        BEGIN
          -- we should insert a new machine here
          SELECT ids_seq.nextval INTO mId FROM DUAL;
          INSERT INTO DiskServer (name, id, status, adminStatus, readRate, writeRate, nbReadStreams,
                   nbWriteStreams, nbReadWriteStreams, nbMigratorStreams, nbRecallerStreams)
           VALUES (machines(i), mid, machineValues(ind),
                   machineValues(ind + 1), machineValues(ind + 2),
                   machineValues(ind + 3), machineValues(ind + 4),
                   machineValues(ind + 5), machineValues(ind + 6),
                   machineValues(ind + 7), machineValues(ind + 8));
          INSERT INTO Id2Type (id, type) VALUES (mid, 8); -- OBJ_DiskServer
        END;
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
      SELECT id INTO machine FROM DiskServer WHERE name = fileSystems(i);
    ELSE
      IF fileSystemValues(ind + 1) = 3 THEN -- ADMIN DELETED
        BEGIN
          SELECT id INTO fs FROM FileSystem WHERE mountPoint = fileSystems(i) AND diskServer = machine;
          DELETE FROM id2Type WHERE id = fs;
          DELETE FROM FileSystem WHERE id = fs;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- Fine, was already deleted
          NULL;
        END;
      ELSE
        BEGIN
          SELECT diskServer INTO machine FROM FileSystem
           WHERE mountPoint = fileSystems(i) AND diskServer = machine;
          UPDATE FileSystem
             SET status = fileSystemValues(ind),
                 adminStatus = fileSystemValues(ind + 1),
                 readRate = fileSystemValues(ind + 2),
                 writeRate = fileSystemValues(ind + 3),
                 nbReadStreams = fileSystemValues(ind + 4),
                 nbWriteStreams = fileSystemValues(ind + 5),
                 nbReadWriteStreams = fileSystemValues(ind + 6),
                 nbMigratorStreams = fileSystemValues(ind + 7),
                 nbRecallerStreams = fileSystemValues(ind + 8),
                 free = fileSystemValues(ind + 9),
                 totalSize = fileSystemValues(ind + 10),
                 minFreeSpace = fileSystemValues(ind + 11),
                 maxFreeSpace = fileSystemValues(ind + 12),
                 minAllowedFreeSpace = fileSystemValues(ind + 13)
           WHERE mountPoint = fileSystems(i)
             AND diskServer = machine;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          DECLARE
            fsid INTEGER;
          BEGIN
            -- we should insert a new filesystem here
            SELECT ids_seq.nextval INTO fsId FROM DUAL;
            INSERT INTO FileSystem (free, mountPoint,
                   minFreeSpace, minAllowedFreeSpace, maxFreeSpace,
                   totalSize, readRate, writeRate, nbReadStreams,
                   nbWriteStreams, nbReadWriteStreams, nbMigratorStreams, nbRecallerStreams,
                   id, diskPool, diskserver, status, adminStatus)
            VALUES (fileSystemValues(ind + 9), fileSystems(i), fileSystemValues(ind+11),
                    fileSystemValues(ind + 13), fileSystemValues(ind + 12),
                    fileSystemValues(ind + 10), fileSystemValues(ind + 2),
                    fileSystemValues(ind + 3), fileSystemValues(ind + 4),
                    fileSystemValues(ind + 5), fileSystemValues(ind + 6),
                    fileSystemValues(ind + 7), fileSystemValues(ind + 8),
                    fsid, 0, machine, 2, 1); -- FILESYSTEM_DISABLED, ADMIN_FORCE
            INSERT INTO Id2Type (id, type) VALUES (fsid, 12); -- OBJ_FileSystem
          END;
        END;
      END IF;
      ind := ind + 14;
    END IF;
    -- Release the lock on the FileSystem as soon as possible to prevent
    -- deadlocks with other activities e.g. recaller
    COMMIT;
  END LOOP;
END;
