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


/* this function tries to create a CastorFile and deals with the associated race conditions
   In case race conditions are really bad, the method can fail and return False */
CREATE OR REPLACE FUNCTION createCastorFile (fId IN INTEGER,
                                             nh IN VARCHAR2,
                                             fcId IN INTEGER,
                                             fs IN INTEGER,
                                             fn IN VARCHAR2,
                                             srId IN NUMBER,
                                             inNsOpenTime IN NUMBER,
                                             waitForLock IN BOOLEAN,
                                             rid OUT INTEGER,
                                             rfs OUT INTEGER) RETURN BOOLEAN AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  -- take care that the name of the new file is not already the lastKnownFileName
  -- of another file, that was renamed but for which the lastKnownFileName has
  -- not been updated.
  -- We actually reset the lastKnownFileName of such a file if needed
  -- Note that this procedure will run in an autonomous transaction so that
  -- no dead lock can result from taking a second lock within this transaction
  dropReusedLastKnownFileName(fn);
  -- insert new row (see selectCastorFile inline comments for the TRUNC() operation)
  INSERT INTO CastorFile (id, fileId, nsHost, fileClass, fileSize, creationTime,
                          lastAccessTime, lastUpdateTime, nsOpenTime, lastKnownFileName, tapeStatus)
    VALUES (ids_seq.nextval, fId, nh, fcId, fs, getTime(), getTime(),
            TRUNC(inNsOpenTime), inNsOpenTime, normalizePath(fn), NULL)
    RETURNING id, fileSize INTO rid, rfs;
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET castorFile = rid
   WHERE id = srId;
  RETURN True;
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  -- the violated constraint indicates that the file was created by another client
  -- while we were trying to create it ourselves. We can thus use the newly created file
  BEGIN
    IF waitForLock THEN
      SELECT id, fileSize INTO rid, rfs FROM CastorFile
        WHERE fileId = fid AND nsHost = nh FOR UPDATE;
    ELSE
      SELECT id, fileSize INTO rid, rfs FROM CastorFile
        WHERE fileId = fid AND nsHost = nh FOR UPDATE NOWAIT;
    END IF;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET castorFile = rid
     WHERE id = srId;
    RETURN True;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- damn, the file created by the other client already disappeared before we could use it !
    -- give up for this round
    RETURN False;
  END;
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
                          lastKnownFileName = normalizePath(fn)
     WHERE id = rid;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET castorFile = rid
     WHERE id = srId
     RETURNING reqType INTO varReqType;
    IF varReqType IN (37, 38, 40, 44) THEN  -- all write operations
      -- reset lastUpdateTime: this is used to prevent parallel Put operations from
      -- overtaking each other. As it is still relying on diskservers' timestamps
      -- with seconds precision, we truncate the usec-precision nameserver timestamp
      UPDATE CastorFile
         SET lastUpdateTime = TRUNC(inNsOpenTime),
             nsOpenTime = inNsOpenTime
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
    -- we did not find the file, let's try to create a new one.
    -- This may fail with a low probability (subtle race condition, see comments
    -- in the method), so we try in a loop
    DECLARE
      varSuccess BOOLEAN := False;
    BEGIN
      WHILE NOT varSuccess LOOP
        varSuccess := createCastorFile(fId, nh, fcId, fs, fn, srId, inNsOpenTime, waitForLock, rid, rfs);
      END LOOP;
    END;
  END;
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
                                              inNsOpenTimeInUsec IN INTEGER,
                                              rid OUT INTEGER,
                                              rfs OUT INTEGER) AS
  nsHostName VARCHAR2(2048);
BEGIN
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- call internal method
  selectCastorFileInternal(fId, nsHostName, fc, fs, fn, srId, inNsOpenTimeInUsec/1000000, TRUE, rid, rfs);
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
   WHERE castorfile = inCfId
     AND ROWNUM < 2;
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
           SELECT /*+ INDEX(DiskCopy I_DiskCopy_CastorFile) */ DISTINCT(DiskServer.id)
             FROM DiskCopy, FileSystem, DiskServer
            WHERE DiskCopy.castorfile = inCfId
              AND DiskCopy.fileSystem = FileSystem.id
              AND FileSystem.diskserver = DiskServer.id
              AND DiskCopy.status = dconst.DISKCOPY_VALID);
      IF varNbDSs > 0 THEN
        BEGIN
          -- yes, we can replicate, create a replication request without waiting on it.
          createDisk2DiskCopyJob(inCfId, inNsOpenTime, inSvcClassId, inEuid, inEgid,
                                 dconst.REPLICATIONTYPE_INTERNAL, NULL, NULL);
          -- log it
          logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_GET_REPLICATION, inFileId, inNsHost, 'stagerd',
                   'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId) ||
                   ' euid=' || TO_CHAR(inEuid) || ' egid=' || TO_CHAR(inEgid));
        EXCEPTION WHEN NO_DATA_FOUND THEN
          logToDLF(inReqUUID, dlf.LVL_WARNING, dlf.STAGER_GET_REPLICATION_FAIL, inFileId, inNsHost, 'stagerd',
                   'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId) ||
                   ' euid=' || TO_CHAR(inEuid) || ' egid=' || TO_CHAR(inEgid));
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
  -- check that no concurrent put is already running
  DECLARE
    varAnyStageoutDC INTEGER;
  BEGIN
    SELECT /*+ INDEX(DiskCopy I_DiskCopy_Castorfile) */
           COUNT(*) INTO varAnyStageoutDC FROM DiskCopy
     WHERE status IN (dconst.DISKCOPY_WAITFS, dconst.DISKCOPY_STAGEOUT)
       AND castorFile = inCfId
       AND ROWNUM < 2;
    IF varAnyStageoutDC > 0 THEN
      -- the file is already being recreated -> EBUSY
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.EBUSY,
             errorMessage = 'File recreation canceled since file is being recreated by another request'
       WHERE id = inSrId;
      logToDLF(inReqUUID, dlf.LVL_USER_ERROR, dlf.STAGER_RECREATION_IMPOSSIBLE, inFileId, inNsHost, 'stagerd',
               'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId) ||
               ' fileClassId=' || getFileClassName(inFileClassId) || ' reason="file being recreated"');
      RETURN 0;
    END IF;
  END;

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
