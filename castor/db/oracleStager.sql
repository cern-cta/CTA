/*******************************************************************
 *
 * @(#)$RCSfile: oracleStager.sql,v $ $Revision: 1.752 $ $Date: 2009/08/17 15:08:33 $ $Author: sponcec3 $
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
  TYPE TapeCopy IS RECORD (
    castorFile NUMBER,
    id NUMBER,
    copyNb NUMBER,
    status NUMBER,
    errorCode NUMBER,
    nbRetry NUMBER,
    fileTransActionId NUMBER,
    fseq NUMBER,
    missingCopies NUMBER,
    tapeGatewayRequest NUMBER,
    vid VARCHAR2(2048));
  TYPE TapeCopy_Cur IS REF CURSOR RETURN TapeCopy;
  TYPE Segment_Rec IS RECORD (
    fseq NUMBER,
    offset INTEGER,
    bytes_in INTEGER,
    bytes_out INTEGER,
    host_bytes INTEGER,
    segmCksumAlgorithm VARCHAR2(2048),
    segmCksum NUMBER,
    errMsgTxt VARCHAR2(2048),
    errorCode NUMBER,
    severity NUMBER,
    blockId0 INTEGER,
    blockId1 INTEGER,
    blockId2 INTEGER,
    blockId3 INTEGER,
    creationTime INTEGER,
    id INTEGER,
    tape INTEGER,
    copy INTEGER,
    status INTEGER,
    priority INTEGER);
  TYPE Segment_Cur IS REF CURSOR RETURN Segment_Rec;
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
  TYPE DiskServerName IS RECORD (diskServer VARCHAR(2048));
  TYPE DiskServerList_Cur IS REF CURSOR RETURN DiskServerName;
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
    -- Invalidate the DiskCopy 	 
    UPDATE DiskCopy 	 
       SET status = 7  -- INVALID 	 
     WHERE id = cf.id;
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
  -- Look for recalls concerning files that are STAGED or CANBEMIGR
  -- on all filesystems scheduled to be checked, and restart their
  -- subrequests (reconsidering the recall source).
  FOR cf IN (SELECT /*+ USE_NL(E D)
                     INDEX_RS_ASC(E I_DiskCopy_Status)
                     INDEX_RS_ASC(D I_DiskCopy_CastorFile) */
                    UNIQUE D.castorfile, E.id
               FROM DiskCopy D, DiskCopy E
              WHERE D.castorfile = E.castorfile
                AND D.fileSystem IN
                  (SELECT /*+ CARDINALITY(fsidTable 5) */ *
                     FROM TABLE(fsIds) fsidTable)
                AND D.status IN (0, 10)
                AND E.status = 2) LOOP
    -- Cancel recall and restart subrequests
    cancelRecall(cf.castorfile, cf.id, 1); -- RESTART
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
  IF :old.status IN (1, 2) AND  -- DRAINING, DISABLED
     :new.status = 0 THEN       -- PRODUCTION
    checkFsBackInProd(:old.id);
  END IF;
  -- Cancel any ongoing draining operations if the filesystem is not in a
  -- DRAINING state
  IF :new.status != 1 THEN  -- DRAINING
    UPDATE DrainingFileSystem
       SET status = 3  -- INTERRUPTED
     WHERE fileSystem = :new.id
       AND status IN (0, 1, 2, 7);  -- CREATED, INITIALIZING, RUNNING, RESTART
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
  -- a PRODUCTION.
  IF :old.status IN (1, 2) AND  -- DRAINING, DISABLED
     :new.status = 0 THEN       -- PRODUCTION
    FOR fs IN (SELECT id FROM FileSystem
                WHERE diskServer = :old.id
                  AND status = 0)  -- PRODUCTION
    LOOP
      checkFsBackInProd(fs.id);
    END LOOP;
  END IF;
  -- Cancel all draining operations if the diskserver is disabled.
  IF :new.status = 2 THEN  -- DISABLED
    UPDATE DrainingFileSystem
       SET status = 3  -- INTERRUPTED
     WHERE fileSystem IN
       (SELECT FileSystem.id FROM FileSystem
         WHERE FileSystem.diskServer = :new.id)
       AND status IN (0, 1, 2, 7);  -- CREATED, INITIALIZING, RUNNING, RESTART
  END IF;
  -- If the diskserver is in PRODUCTION cancel the draining operation of 
  -- filesystems not in DRAINING.
  IF :new.status = 0 THEN  -- PRODUCTION
    UPDATE DrainingFileSystem
       SET status = 3  -- INTERRUPTED
     WHERE fileSystem IN
       (SELECT FileSystem.id FROM FileSystem
         WHERE FileSystem.diskServer = :new.ID
           AND FileSystem.status != 1)  -- DRAINING
       AND status IN (0, 1, 2, 7);  -- CREATED, INITIALIZING, RUNNING, RESTART
  END IF; 
END;
/


/* Trigger used to check if the maxReplicaNb has been exceeded
 * after a diskcopy has changed its status to STAGED or CANBEMIGR
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
                  SELECT DiskCopy.id
                    FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass,
                         DiskServer
                   WHERE DiskCopy.filesystem = FileSystem.id
                     AND FileSystem.diskpool = DiskPool2SvcClass.parent
                     AND FileSystem.diskserver = DiskServer.id
                     AND DiskPool2SvcClass.child = SvcClass.id
                     AND DiskCopy.castorfile = a.castorfile
                     AND DiskCopy.status IN (0, 10)  -- STAGED, CANBEMIGR
                     AND SvcClass.id = a.svcclass
                   -- Select DISABLED or DRAINING hardware first
                   ORDER BY decode(FileSystem.status, 0,
                            decode(DiskServer.status, 0, 0, 1), 1) ASC,
                            DiskCopy.gcWeight DESC))
               WHERE ind > maxReplicaNb)
    LOOP
      -- Sanity check, make sure that the last copy is never dropped!
      SELECT count(*) INTO nbFiles
        FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass, DiskServer
       WHERE DiskCopy.filesystem = FileSystem.id
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND FileSystem.diskserver = DiskServer.id
         AND DiskPool2SvcClass.child = SvcClass.id
         AND DiskCopy.castorfile = a.castorfile
         AND DiskCopy.status IN (0, 10)  -- STAGED, CANBEMIGR
         AND SvcClass.id = a.svcclass;
      IF nbFiles = 1 THEN
        EXIT;  -- Last file, so exit the loop
      END IF;
      -- Invalidate the diskcopy
      UPDATE DiskCopy
         SET status = 7,  -- INVALID
             gcType = 2   -- Too many replicas
       WHERE id = b.id;
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
CREATE OR REPLACE TRIGGER tr_DiskCopy_Online
AFTER UPDATE OF status ON DiskCopy
FOR EACH ROW
WHEN ((old.status != 10) AND    -- !CANBEMIGR -> {STAGED, CANBEMIGR}
      (new.status = 0 OR new.status = 10))     
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
    INSERT INTO TooManyReplicasHelper
    VALUES (svcId, :new.castorfile);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    RETURN;  -- Entry already exists!
  END;
END;
/


/***************************************/
/* Some triggers to prevent dead locks */
/***************************************/

/* Used to avoid LOCK TABLE TapeCopy whenever someone wants
   to deal with the tapeCopies on a CastorFile.
   XXX To be dropped when rtcpclientd is removed. */
CREATE OR REPLACE TRIGGER tr_TapeCopy_CastorFile
BEFORE INSERT OR UPDATE OF castorFile ON TapeCopy
FOR EACH ROW WHEN (new.castorFile > 0)
DECLARE
  unused NUMBER;
BEGIN
  SELECT id INTO unused FROM CastorFile
   WHERE id = :new.castorFile FOR UPDATE;
END;
/


/* Used to avoid LOCK TABLE TapeCopy whenever someone wants
   to deal with the tapeCopies on a CastorFile.
   XXX To be dropped when rtcpclientd is removed. */
CREATE OR REPLACE TRIGGER tr_DiskCopy_CastorFile
BEFORE INSERT OR UPDATE OF castorFile ON DiskCopy
FOR EACH ROW WHEN (new.castorFile > 0)
DECLARE
  unused NUMBER;
BEGIN
  SELECT id INTO unused FROM CastorFile
   WHERE id = :new.castorFile FOR UPDATE;
END;
/

CREATE OR REPLACE TRIGGER tr_Tape_Insert
  BEFORE INSERT ON Tape
FOR EACH ROW
/**
 * Converts an inserted lastVdqmPingTime of NULL to the current time.
 */
BEGIN
  IF :NEW.lastVdqmPingTime IS NULL THEN
    :NEW.lastVdqmPingTime := getTime();
  END IF;
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
  CURSOR SRcur IS SELECT /*+ FIRST_ROWS(10) INDEX(SR I_SubRequest_RT_CT_ID) */ SR.id
                    FROM SubRequest PARTITION (P_STATUS_0_1_2) SR
                   WHERE SR.svcHandler = service
                   ORDER BY SR.creationTime ASC;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  varSrId NUMBER;
  varRName VARCHAR2(100);
  varClientId NUMBER;
BEGIN
  OPEN SRcur;
  -- Loop on candidates until we can lock one
  LOOP
    -- Fetch next candidate
    FETCH SRcur INTO varSrId;
    EXIT WHEN SRcur%NOTFOUND;
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
      EXIT;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        -- Got to next candidate, this subrequest was processed already and its status changed
        NULL;
      WHEN SrLocked THEN
        -- Go to next candidate, this subrequest is being processed by another thread
        NULL;
    END;
  END LOOP;
  CLOSE SRcur;
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
      OR abortedSRstatus = dconst.SUBREQUEST_READYFORSCHED
      OR abortedSRstatus = dconst.SUBREQUEST_BEINGSCHED THEN
      -- standard case, we only have to fail the subrequest
      UPDATE SubRequest SET status = 7 WHERE id = sr.srId;
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 0, '');
    WHEN abortedSRstatus = dconst.SUBREQUEST_WAITTAPERECALL THEN
      -- recall case, let's see whether we can cancel the recall
      DECLARE
        segId INTEGER;
        unusedIds "numList";
      BEGIN
        -- XXX First lock all segments for the file. Note that
        -- XXX this step should be dropped once the tapeGateway
        -- XXX is deployed. The current recaller does not take
        -- XXX the proper lock on the castorFiles, hence we
        -- XXX need this here
        SELECT Segment.id BULK COLLECT INTO unusedIds
          FROM Segment, TapeCopy
         WHERE TapeCopy.castorfile = sr.cfId
           AND TapeCopy.id = Segment.copy
         ORDER BY Segment.id
           FOR UPDATE OF Segment.id;
        -- Check whether we have any segment in SELECTED
        SELECT segment.id INTO segId
          FROM Segment, TapeCopy
         WHERE TapeCopy.castorfile = sr.cfId
           AND TapeCopy.id = Segment.copy
           AND Segment.status = 7 -- SELECTED
           AND ROWNUM < 2;
        -- Something is running, so give up
        INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 16, 'Cannot abort ongoing recall'); -- EBUSY
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- Nothing running, we can cancel the recall  
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET status = 7 WHERE id = sr.srId;
        deleteTapeCopies(sr.cfId);
        UPDATE DiskCopy SET status = dconst.DISKCOPY_FAILED
         WHERE castorfile = sr.cfid AND status = dconst.DISKCOPY_WAITTAPERECALL;
        INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 0, '');
      END;
    WHEN abortedSRstatus = dconst.SUBREQUEST_FAILED
      OR abortedSRstatus = dconst.SUBREQUEST_FAILED_FINISHED
      OR abortedSRstatus = dconst.SUBREQUEST_FAILED_ANSWERING THEN
      -- subrequest has failed, nothing to abort
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 22, 'Cannot abort failed subRequest'); -- EINVAL
    WHEN abortedSRstatus = dconst.SUBREQUEST_FINISHED
      OR abortedSRstatus = dconst.SUBREQUEST_ARCHIVED THEN
      -- subrequest is over, nothing to abort
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 22, 'Cannot abort completed subRequest'); -- EINVAL
    ELSE
      -- unknown status !
      INSERT INTO ProcessBulkRequestHelper
      VALUES (sr.fileId, sr.nsHost, 1015, 'Found unknown status for request : ' || TO_CHAR(abortedSRstatus)); -- SEINTERNAL
  END CASE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- subRequest was deleted in the mean time !
  INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 2, 'Targeted SubRequest has just been deleted'); -- ENOENT
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
      OR abortedSRstatus = dconst.SUBREQUEST_READYFORSCHED
      OR abortedSRstatus = dconst.SUBREQUEST_BEINGSCHED THEN
      -- standard case, we only have to fail the subrequest
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_FAILED
       WHERE id = sr.srId;
      UPDATE DiskCopy SET status = dconst.DISKCOPY_FAILED
       WHERE castorfile = sr.cfid AND status IN (dconst.DISKCOPY_STAGEOUT,
                                                 dconst.DISKCOPY_WAITFS,
                                                 dconst.DISKCOPY_WAITFS_SCHEDULING);
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 0, '');
    WHEN abortedSRstatus = dconst.SUBREQUEST_FAILED
      OR abortedSRstatus = dconst.SUBREQUEST_FAILED_FINISHED
      OR abortedSRstatus = dconst.SUBREQUEST_FAILED_ANSWERING THEN
      -- subrequest has failed, nothing to abort
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 22, 'Cannot abort failed subRequest'); -- EINVAL
    WHEN abortedSRstatus = dconst.SUBREQUEST_FINISHED
      OR abortedSRstatus = dconst.SUBREQUEST_ARCHIVED THEN
      -- subrequest is over, nothing to abort
      INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 22, 'Cannot abort completed subRequest'); -- EINVAL
    ELSE
      -- unknown status !
      INSERT INTO ProcessBulkRequestHelper
      VALUES (sr.fileId, sr.nsHost, 1015, 'Found unknown status for request : ' || TO_CHAR(abortedSRstatus)); -- SEINTERNAL
  END CASE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- subRequest was deleted in the mean time !
  INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 2, 'Targeted SubRequest has just been deleted'); -- ENOENT
END;
/

/* PL/SQL method to process bulk abort on a given Repack request */
CREATE OR REPLACE PROCEDURE processBulkAbortForRepack(abortReqId IN INTEGER, origReqId IN INTEGER) AS
  abortedSRstatus NUMBER;
  srsToUpdate "numList";
  dcrecsToUpdate "numList";
  dcmigrsToUpdate "numList";
  nbItems NUMBER;
  nbItemsDone NUMBER := 0;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  unused NUMBER;
  firstOne BOOLEAN := TRUE;
  commitWork BOOLEAN := FALSE;
BEGIN
  -- Gather the list of subrequests to abort
  INSERT INTO ProcessBulkAbortFileReqsHelper (
    SELECT /*+ INDEX(Subrequest I_Subrequest_CastorFile)*/
           SubRequest.id, CastorFile.id, CastorFile.fileId, CastorFile.nsHost, SubRequest.subreqId
      FROM SubRequest, CastorFile
     WHERE SubRequest.castorFile = CastorFile.id
       AND request = origReqId
       AND status IN (dconst.SUBREQUEST_RESTART, dconst.SUBREQUEST_RETRY,
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
        -- note the revalidation of the status and even of the existence of the subrequest
        -- as it may have changed before we got the lock on the Castorfile in processBulkAbortFileReqs
        SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ status
          INTO abortedSRstatus
          FROM SubRequest
         WHERE id = sr.srId;
        CASE
          WHEN abortedSRstatus = dconst.SUBREQUEST_RESTART
            OR abortedSRstatus = dconst.SUBREQUEST_RETRY
            OR abortedSRstatus = dconst.SUBREQUEST_WAITSUBREQ THEN
            -- easy case, we only have to fail the subrequest
            INSERT INTO ProcessRepackAbortHelperSR VALUES (sr.srId);
          WHEN abortedSRstatus = dconst.SUBREQUEST_WAITTAPERECALL THEN
            -- recall case, let's see whether we can cancel the recall
            DECLARE
              segId INTEGER;
              unusedIds "numList";
            BEGIN
              -- Check whether we have any segment in SELECTED
              SELECT segment.id INTO segId
                FROM Segment, TapeCopy
               WHERE TapeCopy.castorfile = sr.cfId
                 AND TapeCopy.id = Segment.copy
                 AND Segment.status = 7 -- SELECTED
                 AND ROWNUM < 2;
              -- Something is running, so give up
              INSERT INTO ProcessBulkRequestHelper VALUES (sr.fileId, sr.nsHost, 16, 'Cannot abort ongoing recall'); -- EBUSY
            EXCEPTION WHEN NO_DATA_FOUND THEN
              -- Nothing running, we can cancel the recall  
              INSERT INTO ProcessRepackAbortHelperSR VALUES (sr.srId);
              deleteTapeCopies(sr.cfId);
              INSERT INTO ProcessRepackAbortHelperDCrec VALUES (sr.cfId);
            END;
          WHEN abortedSRstatus = dconst.SUBREQUEST_REPACK THEN
            -- stop the migration and put back the file to STAGED
            INSERT INTO ProcessRepackAbortHelperSR VALUES (sr.srId);
            deleteTapeCopies(sr.cfId);
            INSERT INTO ProcessRepackAbortHelperDCmigr VALUES (sr.cfId);
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
    FORALL i IN srsToUpdate.FIRST .. srsToUpdate.LAST
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET parent = NULL, diskCopy = NULL, lastModificationTime = getTime(),
             status = 9, errorCode = 1701, errorMessage='Aborted explicitely'
       WHERE id = srsToUpdate(i);
    SELECT cfId BULK COLLECT INTO dcrecsToUpdate FROM ProcessRepackAbortHelperDCrec;
    FORALL i IN dcrecsToUpdate.FIRST .. dcrecsToUpdate.LAST
      UPDATE DiskCopy SET status = dconst.DISKCOPY_FAILED
       WHERE castorfile = dcrecsToUpdate(i) AND status = dconst.DISKCOPY_WAITTAPERECALL;
    SELECT cfId BULK COLLECT INTO dcmigrsToUpdate FROM ProcessRepackAbortHelperDCmigr;
    FORALL i IN dcmigrsToUpdate.FIRST .. dcmigrsToUpdate.LAST
      UPDATE DiskCopy SET status = dconst.DISKCOPY_STAGED
       WHERE castorfile = dcmigrsToUpdate(i) AND status = dconst.DISKCOPY_CANBEMIGR;
    -- commit
    COMMIT;
    -- reset all counters
    nbItems := nbItems - nbItemsDone;
    nbItemsDone := 0;
    firstOne := TRUE;
    commitWork := FALSE;
  END LOOP;
END;
/

/* PL/SQL method to process bulk abort on files related requests */
CREATE OR REPLACE PROCEDURE processBulkAbortFileReqs
(abortReqId IN INTEGER, origReqId IN INTEGER,
 fileIds IN "numList", nsHosts IN strListTable, reqType IN NUMBER) AS
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
    INSERT INTO processBulkAbortFileReqsHelper (
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
        INSERT INTO processBulkAbortFileReqsHelper VALUES (srId, cfId, fileIds(i), nsHosts(i), srUuid);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- this fileid/nshost did not exist in the request, send an error back
        INSERT INTO ProcessBulkRequestHelper
        VALUES (fileIds(i), nsHosts(i), 2, 'No subRequest found for this fileId/nsHost'); -- ENOENT
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
        INSERT INTO TransfersToAbort VALUES (sr.uuid);
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
  FORALL i IN ids.FIRST .. ids.LAST DELETE FROM NsFileId WHERE id = ids(i);
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
    INSERT INTO ProcessBulkRequestHelper VALUES (0, '', 2, 'Request not found, or abort not supported for this request type'); -- ENOENT
    RETURN;
  END;
  IF reqType IN (1,2) THEN
    processBulkAbortFileReqs(abortReqId, requestId, fileIds, nsHosts, reqType);
  ELSE
    processBulkAbortForRepack(abortReqId, requestId);
  END IF;
END;
/

/* PL/SQL method to process bulk abort requests */
CREATE OR REPLACE PROCEDURE abortRepackRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   clientIP IN INTEGER,
   parentUUID IN VARCHAR2,
   rSubResults OUT castor.FileResult_Cur) AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  rIpAddress INTEGER;
  rport INTEGER;
  rReqUuid VARCHAR2(2048);
BEGIN
  -- mark the repack request as being aborted
  UPDATE StageRepackRequest SET status = tconst.REPACK_ABORTING WHERE reqId = parentUUID;
  COMMIT;
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  INSERT INTO StageAbortRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
  VALUES (0,userName,euid,egid,0,pid,machine,'','',uuidgen(),creationTime,creationTime,parentUUID,reqId,0,clientId);
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,0,0,0,clientId);
  -- process the abort
  processBulkAbort(reqId, rIpAddress, rport, rReqUuid);
  -- mark the repack request as ABORTED
  UPDATE StageRepackRequest SET status = tconst.REPACK_ABORTED WHERE reqId = parentUUID;
  -- return all results
  OPEN rSubResults FOR
    SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
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
BEGIN
  -- in case we do not find anything, rtype should be 0
  rType := 0;
  OPEN Rcur;
  -- Loop on candidates until we can lock one
  LOOP
    -- Fetch next candidate
    FETCH Rcur INTO requestId;
    EXIT WHEN Rcur%NOTFOUND;
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
  END LOOP;
  CLOSE Rcur;
END;
/

/* PL/SQL method to handle a repack request */
CREATE OR REPLACE PROCEDURE handleRepackRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   clientIP IN INTEGER,
   reqVID IN VARCHAR2,
   rSubResults OUT castor.FileResult_Cur) AS
  varReqId INTEGER;
  clientId INTEGER;
  creationTime NUMBER;
  svcClassId INTEGER;
  varSubreqId INTEGER;
  cfId INTEGER;
  dcId INTEGER;
  tapeId INTEGER;
  tapeStatus INTEGER;
  repackProtocol VARCHAR2(2048);
  recallPolicy VARCHAR2(2048);
  result NUMBER;
  nsHostName VARCHAR2(2048);
  lastKnownFileName VARCHAR2(2048);
  priority INTEGER;
  unused INTEGER;
  firstCF boolean := True;
  nbFilesProcessed NUMBER := 0;
  nbFilesFailed NUMBER := 0;
  isOngoing boolean := False;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, 119);
  -- insertion time
  creationTime := getTime();
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,0,0,0,ids_seq.nextval)
  RETURNING id INTO clientId;
  -- insert the request itself
  INSERT INTO StageRepackRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, repackVid, id, svcClass, client,status)
  VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',uuidgen(),creationTime,creationTime,reqVID,ids_seq.nextval,svcClassId,clientId,tconst.REPACK_STARTING)
  RETURNING id INTO varReqId;
  -- Clear the temporary table for subresults
  DELETE FROM ProcessBulkRequestHelper;
  -- Check which protocol should be used for writing files to disk
  repackProtocol := getConfigOption('Repack', 'Protocol', 'rfio');
  -- name server host name
  nsHostName := getConfigOption('stager', 'nsHost', '');
  -- check and create the tape if needed. Its status depends on the presence of a recallerPolicy
  -- Note that selectTapeForRecall uses autonomous transaction so that we do not keep a lock
  SELECT recallerPolicy INTO recallPolicy FROM SvcClass WHERE id = svcClassId;
  IF recallPolicy IS NULL THEN
    tapeStatus := tconst.TAPE_PENDING;
  ELSE
    tapeStatus := tconst.TAPE_WAITPOLICY;
  END IF;
  tapeId := selectTapeForRecall(reqVID, tapeStatus);
  -- compute the priority of this request
  BEGIN
    SELECT priority INTO priority FROM PriorityMap
     WHERE euid = euid AND egid = egid AND ROWNUM < 2;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    priority := 0;
  END;
  -- Get the list of files to repack from the NS DB via DBLink and store them in memory
  -- in a temporary table. We do that so that we do not keep an open cursor for too long
  -- in the nameserver DB
  INSERT INTO RepackTapeSegments (SELECT s_fileid, blockid, fseq, segSize, copyno, fileclass
                                    FROM Cns_Seg_Metadata@remotens, Cns_File_Metadata@remotens
                                   WHERE vid = reqVID
                                     AND s_fileid = fileid
                                     AND s_status = '-'
                                     AND status != 'D');
  FOR segment IN (SELECT * FROM RepackTapeSegments) LOOP
    -- Commit from time to time
    IF nbFilesProcessed = 1000 THEN
      COMMIT;
      firstCF := TRUE;
      nbFilesProcessed := 0;
    END IF;
    -- lastKnownFileName we will have in the DB
    lastKnownFileName := CONCAT('Repack_', TO_CHAR(segment.s_fileid));
    -- find the Castorfile (and take a lock on it)
    DECLARE
      locked EXCEPTION;
      PRAGMA EXCEPTION_INIT (locked, -54);
    BEGIN
      -- This may raise a SrLocked exception as we do not want to wait for locks (except on first file).
      -- In such a case, we commit what we've done so far and retry this file, this time waiting for the lock.
      -- The reason for such a complex code is to avoid commiting each file separately, as it would be
      -- too heavy. On the other hand, we still need to avoid dead locks.
      -- Note that we pass 0 for the subrequest id, thus the subrequest will not be attached to the
      -- CastorFile. We actually attach it when we create it.
      selectCastorFileInternal(segment.s_fileid, nsHostName, svcClassId, segment.fileclass,
                               segment.segSize, lastKnownFileName, 0, creationTime, firstCF, cfid, unused);
      firstCF := FALSE;
    EXCEPTION WHEN locked THEN
      -- commit what we've done so far
      COMMIT;
      -- And lock the castorfile (waiting this time)
      selectCastorFileInternal(segment.s_fileid, nsHostName, svcClassId, segment.fileclass,
                               segment.segSize, lastKnownFileName, 0, creationTime, TRUE, cfid, unused);
    END;
    nbFilesProcessed := nbFilesProcessed + 1;
    -- create  subrequest for this file.
    -- Note that the svcHandler is set to PrepReqSvc. Most of the time, it will not be used,
    -- as the rest of this procedure will handle the subrequests itself. The only case where it is
    -- needed is when some subrequests are put to WAITSUBREQ. The the PrepReqSvc will be
    -- in charge of handling them one by one when they can wake up
    INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, parent, status, request, getNextStatus, reqType)
    VALUES (0, lastKnownFileName, repackProtocol, segment.segSize, 0, uuidGen(), 0, 0, creationTime, creationTime, 0, 0, '', NULL, 'PrepReqSvc', ids_seq.nextval, 0, cfId, NULL, dconst.SUBREQUEST_START, varReqId, 0, 119)
    RETURNING id INTO varSubreqId;
    -- find out whether this file is already staged or not
    processPrepareRequest(varSubreqId, result);
    CASE
      WHEN result = -2 OR result = 0 OR result = 1 THEN
        -- SubRequest has been put in WAITSUBREQ, WAITDISK2DISKCOPY or directly to REPACK. Nothing to do
        isOngoing := True;
      WHEN result = -1 THEN
        -- SubRequest has been failed, log the error
        INSERT INTO ProcessBulkRequestHelper
          (SELECT segment.s_fileid, nsHostName, errorCode, errorMessage
             FROM SubRequest WHERE id = varSubreqId);
        nbFilesFailed := nbFilesFailed + 1;
      WHEN result = 2 THEN
        -- standard case : we need to log and to trigger the recall
        triggerRepackRecall(varSubreqId, cfId, tapeId, segment.s_fileid, nsHostName, segment.blockid, segment.fseq, segment.copyno, priority, euid, egid);
        isOngoing := True;
    END CASE;
  END LOOP;
  -- cleanup RepackTapeSegments
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RepackTapeSegments';
  -- update status of the RepackRequest
  IF isOngoing THEN
    UPDATE StageRepackRequest SET status = tconst.REPACK_ONGOING WHERE StageRepackRequest.id = varReqId;
  ELSE
    IF nbFilesFailed > 0 THEN
      UPDATE StageRepackRequest SET status = tconst.REPACK_FAILED WHERE StageRepackRequest.id = varReqId;
    ELSE
      -- CASE of an 'empty' repack : the tape had no files at all
      UPDATE StageRepackRequest SET status = tconst.REPACK_FINISHED WHERE StageRepackRequest.id = varReqId;
    END IF;
  END IF;
  -- open cursor on results
  OPEN rSubResults FOR
    SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
END;
/

/* PL/SQL method to handle a repack subrequest.
 * Repack requests are normally handled as a whole via the handleRepackRequest procedure.
 * However, some subrequests may be put in WAITSUBREQ on that processing.
 * They are further processed here, when the wait is over.
 */
CREATE OR REPLACE PROCEDURE handleRepackSubRequest(srId IN INTEGER, rIpAddress OUT INTEGER,
                                                   rport OUT INTEGER, rReqUuid OUT VARCHAR2,
                                                   rSubResults OUT castor.FileResult_Cur) AS
  nsHostName VARCHAR2(2048);
  varFileid NUMBER;
  varReqid NUMBER;
  varCfid NUMBER;
  varReqvid VARCHAR2(2048);
  varEuid INTEGER;
  varEgid INTEGER;
  varPriority INTEGER;
  varBlockid RAW(4);
  varFseq INTEGER;
  varCopyno INTEGER;
  varTapeId INTEGER;
  varTapeStatus INTEGER;
  varSvcClassId INTEGER;
  varRecallPolicy VARCHAR2(2048);
  varResult NUMBER;
BEGIN
  -- name server host name
  nsHostName := getConfigOption('stager', 'nsHost', '');
  -- Get the request id and the fileid of the file
  SELECT fileid, request, CastorFile.id
    INTO varFileid, varReqid, varCfId
    FROM SubRequest, CastorFile
   WHERE SubRequest.id = srId
     AND SubRequest.castorFile = CastorFile.id;
  -- get the repack request's content
  SELECT repackVid, ipAddress, port, reqId, euid, egid, svcClass
    INTO varReqvid, rIpAddress, rport, rReqUuid, varEuid, varEgid, varSvcClassId
    FROM StageRepackRequest, Client
   WHERE StageRepackRequest.id = varReqid
     AND StageRepackRequest.client = Client.id;
  -- find out whether this file is already staged or not
  processPrepareRequest(srId, varResult);
  CASE
    WHEN varResult = -2 OR varResult = 0 THEN
      -- SubRequest has been put in WAITSUBREQ or directly to REPACK. Nothing to do, only log
      INSERT INTO ProcessBulkRequestHelper VALUES (varFileid, nsHostName, 0, '');
    WHEN varResult = -1 THEN
      -- SubRequest has been failed, log the error
      INSERT INTO ProcessBulkRequestHelper
        (SELECT varFileid, nsHostName, errorCode, errorMessage
           FROM SubRequest WHERE id = srId);
    ELSE
      -- standard case : we need to log and to trigger the recall
      INSERT INTO ProcessBulkRequestHelper VALUES (varFileid, nsHostName, 0, '');
      -- compute the priority of this request
      BEGIN
        SELECT priority INTO varPriority FROM PriorityMap
         WHERE euid = varEuid AND egid = varEgid AND ROWNUM < 2;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        varPriority := 0;
      END;
      -- check and create the tape if needed. Its status depends on the presence of a recallerPolicy
      -- Note that selectTapeForRecall uses autonomous transaction so that we do not keep a lock
      SELECT recallerPolicy INTO varRecallPolicy FROM SvcClass WHERE id = varSvcClassId;
      IF varRecallPolicy IS NULL THEN
        varTapeStatus := tconst.TAPE_PENDING;
      ELSE
        varTapeStatus := tconst.TAPE_WAITPOLICY;
      END IF;
      varTapeId := selectTapeForRecall(varReqvid, varTapeStatus);
      -- get all data we need for trigering a recall
      SELECT blockid, fseq, copyno
        INTO varBlockid, varFseq, varCopyno
        FROM Cns_Seg_Metadata@remotens, Cns_File_Metadata@remotens
       WHERE s_status = '-'
         AND status != 'D'
         AND vid = varReqvid
         AND s_fileid = varFileid;
      -- trigger the recall
      triggerRepackRecall(srId, varCfid, varTapeId, varFileid, nsHostName, varBlockid, varFseq, varCopyno, varPriority, varEuid, varEgid);
  END CASE;
  -- open cursor on results
  OPEN rSubResults FOR
    SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
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
BEGIN
  OPEN c;
  LOOP
    FETCH c INTO varSRId;
    EXIT WHEN c%NOTFOUND;
    BEGIN
      SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ answered INTO varSrAnswered
        FROM SubRequest PARTITION (P_STATUS_7) 
       WHERE id = varSRId FOR UPDATE NOWAIT;
      IF varSrAnswered = 1 THEN
        -- already answered, ignore it
        archiveSubReq(varSRId, dconst.SUBREQUEST_FAILED_FINISHED);
      ELSE
        -- we got our subrequest, select all relevant data
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ subrequest
           SET status = dconst.SUBREQUEST_FAILED_ANSWERING
         WHERE id = varSRId
        RETURNING fileName, subReqId, errorCode, errorMessage,
          (SELECT object FROM Type2Obj WHERE type = reqType), request, castorFile
        INTO srFileName, srSubReqId, srErrorCode, srErrorMessage, varRName, varRId, varCFId;
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
          END CASE;
          SELECT ipAddress, port, version
            INTO clIpAddress, clPort, clVersion
            FROM Client WHERE id = varClientId;
          IF varCFId > 0 THEN
            SELECT fileId INTO srFileId FROM CastorFile WHERE id = varCFId;
          END IF;
          EXIT;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- This should never happen, we have an orphaned subrequest.
          -- As we couldn't get the client, we just archive and move on.
          srId := 0;
          archiveSubReq(varSRId, dconst.SUBREQUEST_FAILED_FINISHED);
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
  END LOOP;
  CLOSE c;
END;
/


/* PL/SQL method to get the next request to do according to the given service */
CREATE OR REPLACE PROCEDURE requestToDo(service IN VARCHAR2, rId OUT INTEGER, rType OUT INTEGER) AS
BEGIN
  DELETE FROM NewRequests
   WHERE type IN (
     SELECT type FROM Type2Obj
      WHERE svcHandler = service
        AND svcHandler IS NOT NULL
     )
   AND ROWNUM < 2 RETURNING id, type INTO rId, rType;
EXCEPTION WHEN NO_DATA_FOUND THEN
  rId := 0;   -- nothing to do
  rType := 0;
END;
/


/* PL/SQL method to make a SubRequest wait on another one, linked to the given DiskCopy */
CREATE OR REPLACE PROCEDURE makeSubRequestWait(srId IN INTEGER, dci IN INTEGER) AS
BEGIN
  -- all wait on the original one; also prevent to wait on a PrepareToPut, for the
  -- case when Updates and Puts come after a PrepareToPut and they need to wait on
  -- the first Update|Put to complete.
  -- Cf. recreateCastorFile and the DiskCopy statuses 5 and 11
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET parent = (SELECT SubRequest.id
                     FROM SubRequest, DiskCopy
                    WHERE SubRequest.diskCopy = DiskCopy.id
                      AND DiskCopy.id = dci
                      AND SubRequest.reqType <> 37  -- OBJ_PrepareToPut
                      AND SubRequest.parent = 0
                      AND DiskCopy.status IN (1, 2, 5, 6, 11) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT, WAITFS_SCHEDULING
                      AND SubRequest.status IN (0, 1, 2, 4, 13, 14, 6)), -- START, RESTART, RETRY, WAITTAPERECALL, READYFORSCHED, BEINGSCHED, READY
        status = 5, -- WAITSUBREQ
        lastModificationTime = getTime()
  WHERE SubRequest.id = srId;
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
     SET parent = NULL, diskCopy = NULL,  -- unlink this subrequest as it's dead now
         lastModificationTime = getTime(),
         status = finalStatus
   WHERE id = srId
   RETURNING request, reqType, (SELECT object FROM Type2Obj WHERE type = reqType) INTO rId, rType, rName;
  BEGIN
    -- Try to see whether another subrequest in the same
    -- request is still being processed
    SELECT id INTO unused FROM SubRequest
     WHERE request = rId AND status NOT IN (8, 9) AND ROWNUM < 2;  -- all but {FAILED_,}FINISHED
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- All subrequests have finished, we can archive:
    -- take lock on the request (it is enough the following be
    -- executed serially, multiple executions are idempotent)
    -- and drop the associated Client entity
    EXECUTE IMMEDIATE
      'BEGIN SELECT client INTO :clientId FROM '|| rName ||' WHERE id = :rId FOR UPDATE; END;'
      USING OUT clientId, IN rId;
    DELETE FROM Client WHERE id = clientId;
    -- archive the successful subrequests
    UPDATE /*+ INDEX(SubRequest I_SubRequest_Request) */ SubRequest
       SET status = 11    -- ARCHIVED
     WHERE request = rId
       AND status = 8;  -- FINISHED
    -- in case of repack, change the status of the request
    IF rType = 119 THEN
      DECLARE
        nbfailures NUMBER;
      BEGIN
        SELECT count(*) INTO nbfailures FROM SubRequest
         WHERE request = rId
           AND status = dconst.SUBREQUEST_FAILED_FINISHED
           AND ROWNUM < 2;
        UPDATE StageRepackRequest
           SET status = CASE nbfailures WHEN 1 THEN tconst.REPACK_FAILED ELSE tconst.REPACK_FINISHED END
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
       AND FileSystem.status = 0 -- PRODUCTION
       AND DiskServer.status = 0 -- PRODUCTION
       AND totalSize * minAllowedFreeSpace < free - defFileSize;
    IF (c = 0) THEN
      RETURN 1;
    END IF;
  END IF;
  RETURN 0;
END;
/

/* PL/SQL method checking whether the given service class
 * doesn't provide tape backend and the given file class asks for tape copies.
 * Returns 1 in such a case, 0 else
 */
CREATE OR REPLACE FUNCTION checkFailPutWhenTape0(svcClassId NUMBER, fileClassId NUMBER)
RETURN NUMBER AS
  nbTCs INTEGER;
  nbForcedTCs INTEGER;
  nbTPs INTEGER;
BEGIN
  -- get #tapeCopies requested by this file
  SELECT nbCopies INTO nbTCs
    FROM FileClass WHERE id = fileClassId;
  -- get #tapeCopies from the forcedFileClass: if no forcing
  -- we assume we have tape backend and we let the job
  SELECT nvl(nbCopies, nbTCs) INTO nbForcedTCs
    FROM FileClass, SvcClass
   WHERE SvcClass.forcedFileClass = FileClass.id(+)
     AND SvcClass.id = svcClassId;
  IF nbTCs > nbForcedTCs THEN
    -- typically, when nbTCs = 1 and nbForcedTCs = 0: fail the job
    RETURN 1;
  ELSE
    -- get #tapePools configured in this svcClass
    SELECT COUNT(*) INTO nbTPs FROM SvcClass2TapePool
     WHERE parent = svcClassId;
    IF nbTCs > 0 AND nbTPs = 0 THEN
      -- This is a configuration mistake, and we stop the user in this case.
      -- However, many other conditions should be met to make sure the file
      -- being written goes to tape (see e.g. bug #68020).
      -- To be reviewed once the migration policy logic is refactored.
      RETURN 1;
    ELSE
      RETURN 0;
    END IF;
  END IF;
END;
/


/* PL/SQL method implementing getBestDiskCopyToReplicate. */
CREATE OR REPLACE PROCEDURE getBestDiskCopyToReplicate
  (cfId IN NUMBER, reuid IN NUMBER, regid IN NUMBER, internal IN NUMBER,
   destSvcClassId IN NUMBER, dcId OUT NUMBER, srcSvcClassId OUT NUMBER) AS
  destSvcClass VARCHAR2(2048);
BEGIN
  -- Select the best diskcopy available to replicate and for which the user has
  -- access too.
  SELECT id, srcSvcClassId INTO dcId, srcSvcClassId
    FROM (
      SELECT DiskCopy.id, SvcClass.id srcSvcClassId
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
       WHERE DiskCopy.castorfile = cfId
         AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
         AND FileSystem.id = DiskCopy.fileSystem
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
         AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.id = FileSystem.diskserver
         AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
         -- If this is an internal replication request make sure that the diskcopy
         -- is in the same service class as the source.
         AND (SvcClass.id = destSvcClassId OR internal = 0)
         -- Check that the user has the necessary access rights to replicate a
         -- file from the source service class. Note: instead of using a
         -- StageGetRequest type here we use a StagDiskCopyReplicaRequest type
         -- to be able to distinguish between and read and replication requst.
         AND checkPermission(SvcClass.name, reuid, regid, 133) = 0
         AND NOT EXISTS (
           -- Don't select source diskcopies which already failed more than 10 times
           SELECT /*+ INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */ 'x'
             FROM StageDiskCopyReplicaRequest R, SubRequest
            WHERE SubRequest.request = R.id
              AND R.sourceDiskCopy = DiskCopy.id
              AND SubRequest.status = 9 -- FAILED_FINISHED
           HAVING COUNT(*) >= 10)
       ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                               FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams,
                               FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC,
                DBMS_Random.value)
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
       AND FileSystem.status = 0 -- PRODUCTION
       AND FileSystem.diskserver = DiskServer.id
       AND DiskServer.status = 0 -- PRODUCTION
       AND DiskCopy.status IN (0, 6, 10)  -- STAGED, STAGEOUT, CANBEMIGR
     ORDER BY FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                             FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams,
                             FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC,
              DBMS_Random.value)
   WHERE rownum < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RAISE; -- No file found to be read
END;
/


/* PL/SQL method implementing checkForD2DCopyOrRecall
 * dcId is the DiskCopy id of the best candidate for replica, 0 if none is found (tape recall), -1 in case of user error
 * Internally used by getDiskCopiesForJob and processPrepareRequest
 */
CREATE OR REPLACE
PROCEDURE checkForD2DCopyOrRecall(cfId IN NUMBER, srId IN NUMBER, reuid IN NUMBER, regid IN NUMBER,
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
       SET status = 7, -- FAILED
           errorCode = 28, -- ENOSPC
           errorMessage = 'File creation canceled since diskPool is full'
     WHERE id = srId;
    RETURN;
  END IF;
  -- Resolve the destination service class id to a name
  SELECT name INTO destSvcClass FROM SvcClass WHERE id = svcClassId;
  -- Determine if there are any copies of the file in the same service class
  -- on DISABLED or DRAINING hardware. If we found something then set the user
  -- and group id to -1 this effectively disables the later privilege checks
  -- to see if the user can trigger a d2d or recall. (#55745)
  BEGIN
    SELECT -1, -1 INTO userid, groupid
      FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
     WHERE DiskCopy.fileSystem = FileSystem.id
       AND DiskCopy.castorFile = cfId
       AND DiskCopy.status IN (0, 10)  -- STAGED, CANBEMIGR
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = svcClassId
       AND FileSystem.diskServer = DiskServer.id
       AND (DiskServer.status != 0  -- !PRODUCTION
        OR  FileSystem.status != 0) -- !PRODUCTION
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
       SET status = 7, -- FAILED
           errorCode = 13, -- EACCES
           errorMessage = 'Insufficient user privileges to trigger a tape recall or file replication to the '''||destSvcClass||''' service class'
     WHERE id = srId;
    RETURN;
  END IF;
  -- Try to find a diskcopy to replicate
  getBestDiskCopyToReplicate(cfId, userid, groupid, 0, svcClassId, dcId, srcSvcClassId);
  -- We found at least one, therefore we schedule a disk2disk
  -- copy from the existing diskcopy not available to this svcclass
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- We found no diskcopies at all. We should not schedule
  -- and make a tape recall... except ... in 3 cases :
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
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 7, -- FAILED
           errorCode = CASE
             WHEN dcStatus IN (5, 11) THEN 16 -- WAITFS, WAITFSSCHEDULING, EBUSY
             WHEN dcStatus = 6 AND fsStatus = 0 AND dsStatus = 0 THEN 16 -- STAGEOUT, PRODUCTION, PRODUCTION, EBUSY
             ELSE 1718 -- ESTNOTAVAIL
           END,
           errorMessage = CASE
             WHEN dcStatus IN (5, 11) THEN -- WAITFS, WAITFSSCHEDULING
               'File is being (re)created right now by another user'
             WHEN dcStatus = 6 AND fsStatus = 0 and dsStatus = 0 THEN -- STAGEOUT, PRODUCTION, PRODUCTION
               'File is being written to in another service class'
             ELSE
               'All copies of this file are unavailable for now. Please retry later'
           END
     WHERE id = srId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Check whether the user has the rights to issue a tape recall to
    -- the destination service class.
    IF checkPermission(destSvcClass, userid, groupid, 161) != 0 THEN
      -- Fail the subrequest and notify the client
      dcId := -1;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7, -- FAILED
             errorCode = 13, -- EACCES
             errorMessage = 'Insufficient user privileges to trigger a tape recall to the '''||destSvcClass||''' service class'
       WHERE id = srId;
    ELSE
      -- We did not find the very special case so trigger a tape recall.
      dcId := 0;
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
  path := CONCAT(CONCAT(CONCAT(CONCAT(TO_CHAR(MOD(fid,100),'FM09'), '/'),
                 CONCAT(TO_CHAR(fid), '@')),
                 nsHost), CONCAT('.', TO_CHAR(dcid)));
END;
/


/* PL/SQL method implementing createDiskCopyReplicaRequest */
CREATE OR REPLACE PROCEDURE createDiskCopyReplicaRequest
(sourceSrId IN INTEGER, sourceDcId IN INTEGER, sourceSvcId IN INTEGER,
 destSvcId IN INTEGER, ouid IN INTEGER, ogid IN INTEGER) AS
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
  rfs VARCHAR(2048);
BEGIN
  -- Extract the castorfile associated with the request, this is needed to
  -- create the StageDiskCopyReplicaRequest's diskcopy and subrequest entries.
  -- Also for disk2disk copying across service classes make the originating
  -- subrequest wait on the completion of the transfer.
  IF sourceSrId > 0 THEN
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 5, parent = ids_seq.nextval -- WAITSUBREQ
     WHERE id = sourceSrId
    RETURNING castorFile, parent INTO cfId, srId;
  ELSE
    SELECT castorfile INTO cfId FROM DiskCopy WHERE id = sourceDcId;
    SELECT ids_seq.nextval INTO srId FROM Dual;
  END IF;

  -- Extract CastorFile information
  SELECT fileid, nshost, filesize, lastknownfilename
    INTO fileId, nsHost, fileSize, fileName
    FROM CastorFile WHERE id = cfId;

  -- Create the Client entry.
  INSERT INTO Client (ipaddress, port, id, version, secure)
    VALUES (0, 0, ids_seq.nextval, 0, 0)
  RETURNING id INTO clientId;

  -- Create the StageDiskCopyReplicaRequest. The euid and egid values default to
  -- 0 here, this indicates the request came from the user root.
  SELECT ids_seq.nextval INTO destDcId FROM Dual;
  INSERT INTO StageDiskCopyReplicaRequest
    (svcclassname, reqid, creationtime, lastmodificationtime, id, svcclass,
     client, sourcediskcopy, destdiskcopy, sourceSvcClass)
  VALUES ((SELECT name FROM SvcClass WHERE id = destSvcId), uuidgen(), gettime(),
     gettime(), ids_seq.nextval, destSvcId, clientId, sourceDcId, destDcId, sourceSvcId)
  RETURNING id INTO reqId;

  -- Determine the requested filesystem value
  SELECT DiskServer.name || ':' || FileSystem.mountpoint INTO rfs
    FROM DiskCopy, FileSystem, DiskServer
   WHERE DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskServer = DiskServer.id
     AND DiskCopy.id = sourceDcId;

  -- Create the SubRequest setting the initial status to READYFORSCHED for
  -- immediate dispatching i.e no stager processing
  INSERT INTO SubRequest
    (retrycounter, filename, protocol, xsize, priority, subreqid, flags, modebits,
     creationtime, lastmodificationtime, answered, id, diskcopy, castorfile, parent,
     status, request, getnextstatus, errorcode, requestedfilesystems, svcHandler, reqType)
  VALUES (0, fileName, 'rfio', fileSize, 0, uuidgen(), 0, 0, gettime(), gettime(),
     0, srId, destDcId, cfId, 0, 13, reqId, 0, 0, rfs, 'NotNullNeeded', 133); -- OBJ_StageDiskCopyReplicaRequest;

  -- Create the DiskCopy without filesystem
  buildPathFromFileId(fileId, nsHost, destDcId, rpath);
  INSERT INTO DiskCopy
    (path, id, filesystem, castorfile, status, creationTime, lastAccessTime,
     gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
  VALUES (rpath, destDcId, 0, cfId, 1, getTime(), getTime(), 0, fileSize, 0, ouid, ogid);  -- WAITDISK2DISKCOPY
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
  SELECT ids_seq.nextval INTO dcId FROM DUAL;
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
   WHERE ROWNUM < 2;
  -- compute it's gcWeight
  gcwProc := castorGC.getRecallWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(0); END;'
    USING OUT gcw;
  -- then create the DiskCopy
  INSERT INTO DiskCopy
    (path, id, filesystem, castorfile, status,
     creationTime, lastAccessTime, gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
  VALUES (dcPath, dcId, fsId, cfId, 0,   -- STAGED
          getTime(), getTime(), GCw, 0, 0, ouid, ogid);
  -- link to the SubRequest and schedule an access if requested
  IF schedule = 0 THEN
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET diskCopy = dcId WHERE id = srId;
  ELSE
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET diskCopy = dcId,
           requestedFileSystems = fsPath,
           xsize = 0, status = 13, -- READYFORSCHED
           getNextStatus = 1 -- FILESTAGED
     WHERE id = srId;    
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  raise_application_error(-20115, 'No suitable filesystem found for this empty file');
END;
/

/* PL/SQL method implementing replicateOnClose */
CREATE OR REPLACE PROCEDURE replicateOnClose(cfId IN NUMBER, ouid IN INTEGER, ogid IN INTEGER) AS
  unused NUMBER;
  srcDcId NUMBER;
  srcSvcClassId NUMBER;
  ignoreSvcClass NUMBER;
BEGIN
  -- Lock the castorfile
  SELECT id INTO unused FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Loop over all service classes where replication is required
  FOR a IN (SELECT SvcClass.id FROM (
              -- Determine the number of copies of the file in all service classes
              SELECT * FROM (
                SELECT SvcClass.id, count(*) available
                  FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
                 WHERE DiskCopy.filesystem = FileSystem.id
                   AND DiskCopy.castorfile = cfId
                   AND FileSystem.diskpool = DiskPool2SvcClass.parent
                   AND DiskPool2SvcClass.child = SvcClass.id
                   AND DiskCopy.status IN (0, 1, 10)  -- STAGED, WAITDISK2DISKCOPY, CANBEMIGR
                   AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
                   AND DiskServer.id = FileSystem.diskserver
                   AND DiskServer.status IN (0, 1)  -- PRODUCTION, DRAINING
                 GROUP BY SvcClass.id)
             ) results, SvcClass
             -- Join the results with the service class table and determine if
             -- additional copies need to be created
             WHERE results.id = SvcClass.id
               AND SvcClass.replicateOnClose = 1
               AND results.available < SvcClass.maxReplicaNb)
  LOOP
    -- Ignore this replication if there is already a pending replication request
    -- for the given castorfile in the target/destination service class. Once
    -- the replication has gone through, this procedure will be called again.
    -- This insures that only one replication request is active at any given time
    -- and that we don't create too many replication requests that may exceed
    -- the maxReplicaNb defined on the service class
    BEGIN
      SELECT /*+ INDEX(StageDiskCopyReplicaRequest I_StageDiskCopyReplic_DestDC) */ DiskCopy.id INTO unused
        FROM DiskCopy, StageDiskCopyReplicaRequest
       WHERE StageDiskCopyReplicaRequest.destdiskcopy = DiskCopy.id
         AND StageDiskCopyReplicaRequest.svcclass = a.id
         AND DiskCopy.castorfile = cfId
         AND DiskCopy.status = 1  -- WAITDISK2DISKCOPY
         AND rownum < 2;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- Select the best diskcopy to be replicated. Note: we force that the
        -- replication must happen internally within the service class. This
        -- prevents D2 style activities from impacting other more controlled
        -- service classes. E.g. D2 replication should not impact CDR
        getBestDiskCopyToReplicate(cfId, -1, -1, 1, a.id, srcDcId, srcSvcClassId);
        -- Trigger a replication request.
        createDiskCopyReplicaRequest(0, srcDcId, srcSvcClassId, a.id, ouid, ogid);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        NULL;  -- No copies to replicate from
      END;
    END;
  END LOOP;
END;
/


/* PL/SQL method implementing getDiskCopiesForJob */
/* the result output is a DiskCopy status for STAGED, DISK2DISKCOPY, RECALL or WAITFS
   -1 for user failure, -2 or -3 for subrequest put in WAITSUBREQ */
CREATE OR REPLACE PROCEDURE getDiskCopiesForJob
        (srId IN INTEGER, result OUT INTEGER,
         sources OUT castor.DiskCopy_Cur) AS
  nbDCs INTEGER;
  nbDSs INTEGER;
  maxDCs INTEGER;
  upd INTEGER;
  dcIds "numList";
  dcStatus INTEGER;
  svcClassId NUMBER;
  srcSvcClassId NUMBER;
  cfId NUMBER;
  srcDcId NUMBER;
  d2dsrId NUMBER;
  reuid NUMBER;
  regid NUMBER;
BEGIN
  -- retrieve the castorFile and the svcClass for this subrequest
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         SubRequest.castorFile, Request.euid, Request.egid, Request.svcClass, Request.upd
    INTO cfId, reuid, regid, svcClassId, upd
    FROM (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                 id, euid, egid, svcClass, 0 upd from StageGetRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                 id, euid, egid, svcClass, 1 upd from StageUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = srId;
  -- lock the castorFile to be safe in case of concurrent subrequests
  SELECT id INTO cfId FROM CastorFile WHERE id = cfId FOR UPDATE;

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
    RETURN;
  END IF;

  -- Look for available diskcopies. The status is needed for the
  -- internal replication processing, and only if count = 1, hence
  -- the min() function does not represent anything here.
  SELECT COUNT(DiskCopy.id), min(DiskCopy.status) INTO nbDCs, dcStatus
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
      FOR SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ DiskCopy.id, DiskCopy.path, DiskCopy.status,
                 FileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                                FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams,
                                FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) fsRate,
                 FileSystem.mountPoint, DiskServer.name
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
           ORDER BY fsRate DESC;
    -- Internal replication processing
    IF upd = 1 OR (nbDCs = 1 AND dcStatus = 6) THEN -- DISKCOPY_STAGEOUT
      -- replication forbidden
      result := 0;  -- DISKCOPY_STAGED
    ELSE
      -- check whether there's already an ongoing replication
      BEGIN
        SELECT /*+ INDEX(StageDiskCopyReplicaRequest I_StageDiskCopyReplic_DestDC) */ DiskCopy.id INTO srcDcId
          FROM DiskCopy, StageDiskCopyReplicaRequest
         WHERE DiskCopy.id = StageDiskCopyReplicaRequest.destDiskCopy
           AND StageDiskCopyReplicaRequest.svcclass = svcClassId
           AND DiskCopy.castorfile = cfId
           AND DiskCopy.status = dconst.DISKCOPY_WAITDISK2DISKCOPY;
        -- found an ongoing replication, we don't trigger another one
        result := dconst.DISKCOPY_STAGED;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- ok, we can replicate; do we need to? compare total current
        -- # of diskcopies, regardless hardware availability, against maxReplicaNb.
        SELECT COUNT(*), max(maxReplicaNb) INTO nbDCs, maxDCs FROM (
          SELECT DiskCopy.id, maxReplicaNb
            FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass
           WHERE DiskCopy.castorfile = cfId
             AND DiskCopy.fileSystem = FileSystem.id
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND DiskPool2SvcClass.child = SvcClass.id
             AND SvcClass.id = svcClassId
             AND DiskCopy.status IN (dconst.DISKCOPY_STAGED, dconst.DISKCOPY_CANBEMIGR));
        IF nbDCs < maxDCs OR maxDCs = 0 THEN
          -- we have to replicate. Do it only if we have enough
          -- available diskservers.
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
                  AND DiskCopy.status IN (dconst.DISKCOPY_STAGED, dconst.DISKCOPY_CANBEMIGR));
          IF nbDSs > 0 THEN
            BEGIN
              -- yes, we can replicate. Select the best candidate for replication
              srcDcId := 0;
              getBestDiskCopyToReplicate(cfId, -1, -1, 1, svcClassId, srcDcId, svcClassId);
              -- and create a replication request without waiting on it.
              createDiskCopyReplicaRequest(0, srcDcId, svcClassId, svcClassId, reuid, regid);
              -- result is different for logging purposes
              result := dconst.DISKCOPY_WAITDISK2DISKCOPY;
            EXCEPTION WHEN NO_DATA_FOUND THEN
              -- replication failed. We still go ahead with the access
              result := dconst.DISKCOPY_STAGED;  
            END;
          ELSE
            -- no replication to be done
            result := dconst.DISKCOPY_STAGED;
          END IF;
        ELSE
          -- no replication to be done
          result := dconst.DISKCOPY_STAGED;
        END IF;
      END;
    END IF;   -- end internal replication processing
  ELSE
    -- No diskcopies available for this service class:
    -- first check whether there's already a disk to disk copy going on
    BEGIN
      SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile) INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */ SubRequest.id INTO d2dsrId
        FROM StageDiskCopyReplicaRequest Req, SubRequest
       WHERE SubRequest.request = Req.id
         AND Req.svcClass = svcClassId    -- this is the destination service class
         AND status IN (13, 14, 6)  -- WAITINGFORSCHED, BEINGSCHED, READY
         AND castorFile = cfId;
      -- found it, wait on it
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET parent = d2dsrId, status = 5  -- WAITSUBREQ
       WHERE id = srId;
      result := -2;
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- not found, we may have to schedule a disk to disk copy or trigger a recall
      checkForD2DCopyOrRecall(cfId, srId, reuid, regid, svcClassId, srcDcId, srcSvcClassId);
      IF srcDcId > 0 THEN
        -- create DiskCopyReplica request and make this subRequest wait on it
        createDiskCopyReplicaRequest(srId, srcDcId, srcSvcClassId, svcClassId, reuid, regid);
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
/


/* PL/SQL method internalPutDoneFunc, used by fileRecalled and putDoneFunc.
   checks for diskcopies in STAGEOUT and creates the tapecopies for migration
 */
CREATE OR REPLACE PROCEDURE internalPutDoneFunc (cfId IN INTEGER,
                                                 fs IN INTEGER,
                                                 context IN INTEGER,
                                                 nbTC IN INTEGER,
                                                 svcClassId IN INTEGER) AS
  tcId INTEGER;
  dcId INTEGER;
  gcwProc VARCHAR2(2048);
  gcw NUMBER;
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  -- get function to use for computing the gc weight of the brand new diskCopy
  gcwProc := castorGC.getUserWeight(svcClassId);
  -- if no tape copy or 0 length file, no migration
  -- so we go directly to status STAGED
  IF nbTC = 0 OR fs = 0 THEN
    EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:fs, 0); END;'
      USING OUT gcw, IN fs;
    UPDATE DiskCopy
       SET status = 0, -- STAGED
           lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
           gcWeight = gcw,
	   diskCopySize = fs
     WHERE castorFile = cfId AND status = 6 -- STAGEOUT
     RETURNING owneruid, ownergid INTO ouid, ogid;
  ELSE
    EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:fs, 10); END;'
      USING OUT gcw, IN fs;
    -- update the DiskCopy status to CANBEMIGR
    dcId := 0;
    UPDATE DiskCopy
       SET status = 10, -- CANBEMIGR
           lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
           gcWeight = gcw,
	   diskCopySize = fs
     WHERE castorFile = cfId AND status = 6 -- STAGEOUT
     RETURNING id, owneruid, ownergid INTO dcId, ouid, ogid;
    IF dcId > 0 THEN
      -- Only if we really found the relevant diskcopy, create TapeCopies
      -- This is an extra sanity check, see also the deleteOutOfDateStageOutDCs procedure
      FOR i IN 1..nbTC LOOP
        INSERT INTO TapeCopy (id, copyNb, castorFile, status)
             VALUES (ids_seq.nextval, i, cfId, 0) -- TAPECOPY_CREATED
        RETURNING id INTO tcId;
      END LOOP;
    END IF;
  END IF;
  -- If we are a real PutDone (and not a put outside of a prepareToPut/Update)
  -- then we have to archive the original prepareToPut/Update subRequest
  IF context = 2 THEN
    -- There can be only a single PrepareTo request: any subsequent PPut would be
    -- rejected and any subsequent PUpdate would be directly archived (cf. processPrepareRequest).
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
  -- get number of TapeCopies to create
  SELECT nbCopies INTO nc FROM FileClass, CastorFile
   WHERE CastorFile.id = cfId AND CastorFile.fileClass = FileClass.id;
  -- and execute the internal putDoneFunc with the number of TapeCopies to be created
  internalPutDoneFunc(cfId, fs, context, nc, svcClassId);
END;
/


/* PL/SQL procedure implementing startRepackMigration */
CREATE OR REPLACE PROCEDURE startRepackMigration
(srId IN INTEGER, cfId IN INTEGER, dcId IN INTEGER,
 reuid OUT INTEGER, regid OUT INTEGER) AS
  nbTC NUMBER(2);
  nbTCInFC NUMBER(2);
  fs NUMBER;
  svcClassId NUMBER;
BEGIN
  UPDATE DiskCopy SET status = 6  -- DISKCOPY_STAGEOUT
   WHERE id = dcId RETURNING diskCopySize INTO fs;
  -- how many do we have to create ?
  SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */
         count(StageRepackRequest.repackVid) INTO nbTC
    FROM SubRequest, StageRepackRequest
   WHERE SubRequest.request = StageRepackRequest.id
     AND (SubRequest.id = srId
       OR (SubRequest.castorFile = cfId
         AND SubRequest.status IN (4, 5)));  -- WAITTAPERECALL, WAITSUBREQ
  SELECT nbCopies INTO nbTCInFC
    FROM FileClass, CastorFile
   WHERE CastorFile.id = cfId
     AND FileClass.id = Castorfile.fileclass;
  -- we are not allowed to create more TapeCopies than in the FileClass specified
  IF nbTCInFC < nbTC THEN
    nbTC := nbTCInFC;
  END IF;
  -- update the Repack subRequest for this file. The status REPACK
  -- stays until the migration to the new tape is over.
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET diskCopy = dcId, status = 12  -- REPACK
   WHERE id = srId;   
  -- get the service class, uid and gid
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id) INDEX(R PK_StageRepackRequest_Id) */ R.svcClass, euid, egid
    INTO svcClassId, reuid, regid
    FROM StageRepackRequest R, SubRequest
   WHERE SubRequest.request = R.id
     AND SubRequest.id = srId;
  -- create the required number of tapecopies for the files
  -- XXX For the time being, nbTC will be 1 for sure until we're able
  -- XXX to handle repacking of dual-copy files 
  internalPutDoneFunc(cfId, fs, 0, nbTC, svcClassId);
  -- set svcClass in the CastorFile for the migration
  UPDATE CastorFile SET svcClass = svcClassId WHERE id = cfId;
  -- update remaining STAGED diskcopies to CANBEMIGR too
  -- we may have them as result of disk2disk copies, and so far
  -- we only dealt with dcId
  UPDATE DiskCopy SET status = 10  -- DISKCOPY_CANBEMIGR
   WHERE castorFile = cfId AND status = 0;  -- DISKCOPY_STAGED
END;
/

/* PL/SQL procedure implementing selectTape
 * get the given tape or create it
 * Note that we run in an autonomous transaction.
 */
CREATE OR REPLACE FUNCTION selectTapeForRecall(vid IN VARCHAR2, newStatus IN INTEGER)
RETURN INTEGER AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
  tapeId INTEGER;
  status INTEGER;
BEGIN
  -- try to get the tape, assuming it exists
  -- Note the hardcoded side, as this column is deprecated
  SELECT id, status INTO tapeId, status FROM Tape WHERE vid = vid AND tpmode = tconst.TPMODE_READ AND side = 0;
  IF status = tconst.TAPE_UNUSED OR status = tconst.TAPE_FINISHED OR
     status = tconst.TAPE_FAILED OR status = tconst.TAPE_UNKNOWN THEN
    UPDATE Tape SET status = newStatus WHERE id = tapeId;
    COMMIT;
  END IF;
  RETURN tapeId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no tape found, create it
  BEGIN
    SELECT ids_seq.nextval INTO tapeId FROM DUAL;
    INSERT INTO TAPE (id, vid, side, tpmode, status)
    VALUES (tapeId, vid, 0, tconst.TPMODE_READ, newStatus);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    -- insertion failed with constraint violated.
    -- This means that somebody else was faster.
    -- So go back to select
    SELECT id INTO tapeId FROM Tape WHERE vid = vid AND tpmode = tconst.TPMODE_READ AND side = 0;
    IF status = tconst.TAPE_UNUSED OR status = tconst.TAPE_FINISHED OR
       status = tconst.TAPE_FAILED OR status = tconst.TAPE_UNKNOWN THEN
      UPDATE Tape SET status = newStatus WHERE id = tapeId;
    END IF;
  END;
  COMMIT;
  RETURN tapeId;
END;


/* PL/SQL procedure implementing triggerRepackRecall
 * this creates all rows needed to recall a given file in the repack context
 * that is Tape (if needed), Segment, TapeCopy, DiskCopy and updates
 * the subrequest.
 */
CREATE OR REPLACE PROCEDURE triggerRepackRecall
(srId IN INTEGER, cfId IN INTEGER, tapeId IN INTEGER, fileId IN INTEGER, nsHost IN VARCHAR2, block IN RAW,
 fseq IN INTEGER, copynb IN INTEGER, priority IN INTEGER, euid IN INTEGER, egid IN INTEGER) AS
  segId INTEGER;
  tcId INTEGER;
  dcId INTEGER;
  recallPolicy VARCHAR2(2048);
  dcpath VARCHAR2(2048);
  hexblock VARCHAR2(8);
  block0 INTEGER;
  block1 INTEGER;
  block2 INTEGER;
  block3 INTEGER;
BEGIN
  -- get needed ids
  SELECT ids_seq.nextval INTO segId FROM DUAL;
  SELECT ids_seq.nextval INTO tcId FROM DUAL;
  SELECT ids_seq.nextval INTO dcId FROM DUAL;
  -- insert a Segment
  hexblock := RAWTOHEX(block);
  block0 := TO_NUMBER(SUBSTR(hexblock,1,2),'XX');
  block1 := TO_NUMBER(SUBSTR(hexblock,3,2),'XX');
  block2 := TO_NUMBER(SUBSTR(hexblock,5,2),'XX');
  block3 := TO_NUMBER(SUBSTR(hexblock,7,2),'XX');
  INSERT INTO Segment (id, blockId0, blockId1, blockId2, blockId3, fseq, creationTime, status, copy, tape, priority)
  VALUES (segId, block0, block1, block2, block3, fseq, getTime(), tconst.SEGMENT_UNPROCESSED, tcId, tapeId, priority);
  -- insert a TapeCopy
  INSERT INTO TapeCopy (id, copyNb, status, castorFile, fseq)
  VALUES (tcId, copynb, tconst.TAPECOPY_TOBERECALLED, cfId, fseq);
  -- insert a DiskCopy
  buildPathFromFileId(fileId, nsHost, dcId, dcPath);
  INSERT INTO DiskCopy (id, status, creationTime, castorFile, ownerUid, ownerGid, path)
  VALUES (dcId, dconst.DISKCOPY_WAITTAPERECALL, getTime(), cfId, euid, egid, dcPath);
  -- udpate SubRequest
  UPDATE SubRequest SET diskcopy = dcId, status = dconst.SUBREQUEST_WAITTAPERECALL WHERE id = srId;
END;
/

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
  recRepack INTEGER;
  reuid NUMBER;
  regid NUMBER;
BEGIN
  -- retrieve the castorfile, the svcclass and the reqId for this subrequest
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         SubRequest.castorFile, Request.euid, Request.egid, Request.svcClass, Request.repack
    INTO cfId, reuid, regid, svcClassId, repack
    FROM (SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */
                 id, euid, egid, svcClass, 0 repack FROM StagePrepareToGetRequest UNION ALL
          SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */
                 id, euid, egid, svcClass, 1 repack FROM StageRepackRequest UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                 id, euid, egid, svcClass, 0 repack FROM StagePrepareToUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = srId;
  -- lock the castor file to be safe in case of two concurrent subrequest
  SELECT id INTO cfId FROM CastorFile WHERE id = cfId FOR UPDATE;

  -- Look for available diskcopies. Note that we never wait on other requests
  -- and we include WAITDISK2DISKCOPY as they are going to be available.
  -- For those ones, the filesystem link in the diskcopy table is set to 0,
  -- hence it is not possible to determine the service class via the
  -- filesystem -> diskpool -> svcclass relationship and the replication
  -- request is used.
  SELECT COUNT(*) INTO nbDCs FROM (
    SELECT DiskCopy.id
      FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
     WHERE DiskCopy.castorfile = cfId
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskpool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = svcClassId
       AND FileSystem.status = 0 -- PRODUCTION
       AND FileSystem.diskserver = DiskServer.id
       AND DiskServer.status = 0 -- PRODUCTION
       AND DiskCopy.status IN (dconst.DISKCOPY_STAGED, dconst.DISKCOPY_STAGEOUT, dconst.DISKCOPY_CANBEMIGR)
     UNION ALL
    SELECT /*+ INDEX(StageDiskCopyReplicaRequest I_StageDiskCopyReplic_DestDC) */ DiskCopy.id
      FROM DiskCopy, StageDiskCopyReplicaRequest
     WHERE DiskCopy.id = StageDiskCopyReplicaRequest.destDiskCopy
       AND StageDiskCopyReplicaRequest.svcclass = svcClassId
       AND DiskCopy.castorfile = cfId
       AND DiskCopy.status = 1); -- WAITDISK2DISKCOPY
  
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
        startRepackMigration(srId, cfId, srcDcId, reuid, regid);
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
          -- However at the moment the tapegateway does not handle double repacks,
          -- so we simply fail this repack and rely on Repack to submit
          -- such double tape repacks one by one.
          UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
             SET status = 9,  -- FAILED
                 errorCode = 16,  -- EBUSY
                 errorMessage = 'File is currently being written or migrated'
           WHERE id = srId;
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
    createDiskCopyReplicaRequest(srId, srcDcId, srvSvcClassId, svcClassId, reuid, regid);
    result := 1;  -- DISKCOPY_WAITDISK2DISKCOPY, for logging purposes
  ELSIF srcDcId = 0 THEN  -- recall
    BEGIN
      -- check whether there's already a recall, and get its svcClass
      SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ Request.svcClass, DiskCopy.id, repack
        INTO recSvcClass, recDcId, recRepack
        FROM (SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ 
                     id, svcClass, 0 repack FROM StagePrepareToGetRequest UNION ALL
              SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                     id, svcClass, 0 repack FROM StageGetRequest UNION ALL
              SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */
                     id, svcClass, 1 repack FROM StageRepackRequest UNION ALL
              SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                     id, svcClass, 0 repack FROM StageUpdateRequest UNION ALL
              SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                     id, svcClass, 0 repack FROM StagePrepareToUpdateRequest) Request,
             SubRequest, DiskCopy
       WHERE SubRequest.request = Request.id
         AND SubRequest.castorFile = cfId
         AND DiskCopy.castorFile = cfId
         AND DiskCopy.status = 2  -- WAITTAPERECALL
         AND SubRequest.status = 4;  -- WAITTAPERECALL
      -- we found one: we need to wait if either we are in a different svcClass
      -- so that afterwards a disk-to-disk copy is triggered, or in case of
      -- Repack so to trigger the repack migration. We also protect ourselves
      -- from a double repack request on the same file.
      IF repack = 1 AND recRepack = 1 THEN
        -- we are not able to handle a double repack, see the detailed comment above
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
           SET status = 7,  -- FAILED
               errorCode = 16,  -- EBUSY
               errorMessage = 'File is currently being repacked'
         WHERE id = srId;
        result := -1;  -- user error
        RETURN;
      END IF;
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
       AND status IN (0, 1, 2, 3, 6, 13, 14) -- START, RESTART, RETRY, WAITSCHED, READY, READYFORSCHED, BEINGSCHED
       AND ROWNUM < 2;
    -- we've found one, we wait
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
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
    IF nbDCs = 0 THEN
      -- This is a PutDone without a put (otherwise we would have found
      -- a DiskCopy on a FileSystem), so we fail the subrequest.
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET
        status = 7,  -- FAILED
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
  fclassId INTEGER;
  sclassId INTEGER;
  putSC INTEGER;
  pputSC INTEGER;
  contextPIPP INTEGER;
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  -- Get data and lock access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT fileclass INTO fclassId FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Determine the context (Put inside PrepareToPut ?)
  BEGIN
    -- check that we are a Put/Update
    SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ Request.svcClass INTO putSC
      FROM (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                   id, svcClass FROM StagePutRequest UNION ALL
            SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                   id, svcClass FROM StageUpdateRequest) Request, SubRequest
     WHERE SubRequest.id = srId
       AND Request.id = SubRequest.request;
    BEGIN
      -- check that there is a PrepareToPut/Update going on. There can be only a single one
      -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
      -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
      SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest.diskCopy,
             PrepareRequest.svcClass INTO dcId, pputSC
        FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */
                     id, svcClass FROM StagePrepareToPutRequest UNION ALL
              SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                     id, svcClass FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
       WHERE SubRequest.CastorFile = cfId
         AND PrepareRequest.id = SubRequest.request
         AND SubRequest.status = 6;  -- READY
      -- if we got here, we are a Put/Update inside a PrepareToPut
      -- however, are we in the same service class ?
      IF putSC != pputSC THEN
        -- No, this means we are a Put/Update and another PrepareToPut
        -- is already running in a different service class. This is forbidden
        dcId := 0;
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
           SET status = 7, -- FAILED
               errorCode = 16, -- EBUSY
               errorMessage = 'A prepareToPut is running in another service class for this file'
         WHERE id = srId;
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
      -- In case of prepareToPut we need to check that we are the only one
      SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ count(SubRequest.diskCopy) INTO nbPReqs
        FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id
                FROM StagePrepareToPutRequest UNION ALL
              SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id
                FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
       WHERE SubRequest.castorFile = cfId
         AND PrepareRequest.id = SubRequest.request
         AND SubRequest.status = 6;  -- READY
      -- Note that we did not select ourselves (we are in status 3)
      IF nbPReqs > 0 THEN
        -- this means we are a PrepareToPut and another PrepareToPut/Update
        -- is already running. This is forbidden
        dcId := 0;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 7, -- FAILED
               errorCode = 16, -- EBUSY
               errorMessage = 'Another prepareToPut/Update is ongoing for this file'
         WHERE id = srId;
        RETURN;
      END IF;
      -- Everything is ok then
      contextPIPP := 0;
    END;
  END;
  -- check if there is space in the diskpool in case of
  -- disk only pool
  -- First get the svcclass and the user
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ svcClass, euid, egid INTO sclassId, ouid, ogid
    FROM Subrequest,
         (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                 id, svcClass, euid, egid FROM StagePutRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                 id, svcClass, euid, egid FROM StageUpdateRequest UNION ALL
          SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */
                 id, svcClass, euid, egid FROM StagePrepareToPutRequest UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                 id, svcClass, euid, egid FROM StagePrepareToUpdateRequest) Request
   WHERE SubRequest.id = srId
     AND Request.id = SubRequest.request;
  IF checkFailJobsWhenNoSpace(sclassId) = 1 THEN
    -- The svcClass is declared disk only and has no space
    -- thus we cannot recreate the file
    dcId := 0;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 7, -- FAILED
           errorCode = 28, -- ENOSPC
           errorMessage = 'File creation canceled since disk pool is full'
     WHERE id = srId;
    RETURN;
  END IF;
  IF contextPIPP = 0 THEN
    -- Puts inside PrepareToPuts don't need the following checks
    -- check if the file existed in advance with a fileclass incompatible with this svcClass
    IF checkFailPutWhenTape0(sclassId, fclassId) = 1 THEN
      -- The svcClass is disk only and the file being overwritten asks for tape copy.
      -- This is impossible, so we deny the operation
      dcId := 0;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7, -- FAILED
             errorCode = 22, -- EINVAL
             errorMessage = 'File recreation canceled since this service class doesn''t provide tape backend'
       WHERE id = srId;
      RETURN;
    END IF;
    -- check if recreation is possible for TapeCopies
    SELECT count(*) INTO nbRes FROM TapeCopy
     WHERE status = tconst.TAPECOPY_SELECTED
      AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot recreate
      dcId := 0;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7, -- FAILED
             errorCode = 16, -- EBUSY
             errorMessage = 'File recreation canceled since file is being migrated'
        WHERE id = srId;
      RETURN;
    END IF;
    -- check if recreation is possible for DiskCopies
    SELECT count(*) INTO nbRes FROM DiskCopy
     WHERE status IN (1, 2, 5, 6, 11) -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, STAGEOUT, WAITFS_SCHEDULING
       AND castorFile = cfId;
    IF nbRes > 0 THEN
      -- We found something, thus we cannot recreate
      dcId := 0;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7, -- FAILED
             errorCode = 16, -- EBUSY
             errorMessage = 'File recreation canceled since file is either being recalled, or replicated or created by another user'
       WHERE id = srId;
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
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime, lastAccessTime, gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
         VALUES (rpath, dcId, 0, cfId, 5, getTime(), getTime(), 0, 0, 0, ouid, ogid); -- status WAITFS
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
          SELECT /*+ INDEX(Subrequest I_Subrequest_DiskCopy)*/ id INTO srParent
            FROM SubRequest
           WHERE reqType IN (40, 44)  -- Put, Update
             AND diskCopy = dcId
             AND status IN (13, 14, 6)  -- READYFORSCHED, BEINGSCHED, READY
             AND ROWNUM < 2;   -- if we have more than one just take one of them
          UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
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
  -- Reset svcClass to the request's one as we want to use the new one for migration.
  -- However, we don't reset the filesize, this will be done at prepareForMigration time
  -- so that in case the old file needs to be recovered from tape after e.g. a failed
  -- transfer, we correctly have the previous size.
  UPDATE CastorFile SET svcClass = sclassId
   WHERE id = cfId;
  -- link SubRequest and DiskCopy
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET diskCopy = dcId,
         lastModificationTime = getTime()
   WHERE id = srId;
  -- we don't commit here, the stager will do that when
  -- the subRequest status will be updated to 6
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

/* PL/SQL method implementing selectCastorFile
 * This is only a wrapper on selectCastorFileInternal
 */
CREATE OR REPLACE PROCEDURE selectCastorFile (fId IN INTEGER,
                                              nh IN VARCHAR2,
                                              sc IN INTEGER,
                                              fc IN INTEGER,
                                              fs IN INTEGER,
                                              fn IN VARCHAR2,
                                              srId IN NUMBER,
                                              lut IN NUMBER,
                                              rid OUT INTEGER,
                                              rfs OUT INTEGER) AS
  nsHostName VARCHAR2(2048);
BEGIN
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- call internal method
  selectCastorFileInternal(fId, nsHostName, sc, fc, fs, fn, srId, lut, TRUE, rid, rfs);
END;
/

/* PL/SQL method implementing selectCastorFile */
CREATE OR REPLACE PROCEDURE selectCastorFileInternal (fId IN INTEGER,
                                                      nh IN VARCHAR2,
                                                      sc IN INTEGER,
                                                      fc IN INTEGER,
                                                      fs IN INTEGER,
                                                      fn IN VARCHAR2,
                                                      srId IN NUMBER,
                                                      lut IN NUMBER,
                                                      waitForLock IN BOOLEAN,
                                                      rid OUT INTEGER,
                                                      rfs OUT INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  previousLastKnownFileName VARCHAR2(2048);
  fcId NUMBER;
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
    -- The file is still there, so update lastAccessTime and lastKnownFileName.
    UPDATE CastorFile SET lastAccessTime = getTime(),
                          lastKnownFileName = normalizePath(fn)
     WHERE id = rid;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET castorFile = rid
     WHERE id = srId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- we did not find the file, let's create a new one
    -- take care that the name of the new file is not already the lastKnownFileName
    -- of another file, that was renamed but for which the lastKnownFileName has
    -- not been updated.
    -- We actually reset the lastKnownFileName of such a file if needed
    -- Note that this procedure will run in an autonomous transaction so that
    -- no dead lock can result from taking a second lock within this transaction
    dropReusedLastKnownFileName(fn);
    -- insert new row
    INSERT INTO CastorFile (id, fileId, nsHost, svcClass, fileClass, fileSize,
                            creationTime, lastAccessTime, lastUpdateTime, lastKnownFileName)
      VALUES (ids_seq.nextval, fId, nh, sc, fcId, fs, getTime(), getTime(), lut, normalizePath(fn))
      RETURNING id, fileSize INTO rid, rfs;
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET castorFile = rid
     WHERE id = srId;
  END;
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  -- the violated constraint indicates that the file was created by another client
  -- while we were trying to create it ourselves. We can thus use the newly created file
  IF waitForLock THEN
    SELECT id, fileSize INTO rid, rfs FROM CastorFile
      WHERE fileId = fid AND nsHost = nh FOR UPDATE;
  ELSE
    SELECT id, fileSize INTO rid, rfs FROM CastorFile
      WHERE fileId = fid AND nsHost = nh FOR UPDATE NOWAIT;
  END IF;
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET castorFile = rid
   WHERE id = srId;
END;
/

/* PL/SQL method implementing stageRelease */
CREATE OR REPLACE PROCEDURE stageRelease (fid IN INTEGER,
                                          nh IN VARCHAR2,
                                          ret OUT INTEGER) AS
  cfId INTEGER;
  nbRes INTEGER;
  nsHostName VARCHAR2(2048);
BEGIN
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO cfId FROM CastorFile
   WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
  -- check if removal is possible for TapeCopies
  SELECT count(*) INTO nbRes FROM TapeCopy
   WHERE status = tconst.TAPECOPY_SELECTED
     AND castorFile = cfId;
  IF nbRes > 0 THEN
    -- We found something, thus we cannot recreate
    ret := 1;
    RETURN;
  END IF;
  -- check if recreation is possible for SubRequests
  SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nbRes FROM SubRequest
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
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO cfId FROM CastorFile
   WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
  -- list diskcopies
  SELECT id BULK COLLECT INTO dcsToRm
    FROM DiskCopy
   WHERE castorFile = cfId
     AND status IN (0, 5, 6, 10, 11);  -- STAGED, WAITFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
  -- Stop ongoing recalls
  deleteTapeCopies(cfId);
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  FOR sr IN (SELECT /*+ INDEX(Subrequest I_Subrequest_DiskCopy)*/ id, status FROM SubRequest
              WHERE diskcopy IN
                (SELECT /*+ CARDINALITY(dcidTable 5) */ *
                   FROM TABLE(dcsToRm) dcidTable)
                AND status IN (0, 1, 2, 5, 6, 12, 13)) LOOP   -- START, RESTART, RETRY, WAITSUBREQ, READY, READYFORSCHED
    UPDATE SubRequest
       SET status = 7,  -- FAILED
           errorCode = 4,  -- EINTR
           errorMessage = 'Canceled by another user request',
           parent = 0
     WHERE (id = sr.id) OR (parent = sr.id);
  END LOOP;
  -- Set selected DiskCopies to INVALID
  FORALL i IN dcsToRm.FIRST .. dcsToRm.LAST
    UPDATE DiskCopy
       SET status = 7, -- INVALID
           gcType = inGcType
     WHERE id = dcsToRm(i);
END;
/


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
  srIds "numList";
  dcStatus INTEGER;
  nsHostName VARCHAR2(2048);
  migSvcClass NUMBER;
BEGIN
  ret := 0;
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  BEGIN
    -- Lock the access to the CastorFile
    -- This, together with triggers will avoid new TapeCopies
    -- or DiskCopies to be added
    SELECT id, svcClass INTO cfId, migSvcClass FROM CastorFile
     WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- This file does not exist in the stager catalog
    -- so we just fail the request
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = 7,  -- FAILED
           errorCode = 2,  -- ENOENT
           errorMessage = 'File not found on disk cache'
     WHERE id = srId;
    RETURN;
  END;
  -- First select involved diskCopies
  scId := svcClassId;
  dcStatus := 0;
  IF scId > 0 THEN
    SELECT id BULK COLLECT INTO dcsToRm FROM (
      -- first physical diskcopies
      SELECT DC.id
        FROM DiskCopy DC, FileSystem, DiskPool2SvcClass DP2SC
       WHERE DC.castorFile = cfId
         AND DC.status IN (0, 6, 10)  -- STAGED, STAGEOUT, CANBEMIGR
         AND DC.fileSystem = FileSystem.id
         AND FileSystem.diskPool = DP2SC.parent
         AND DP2SC.child = scId)
    UNION ALL (
      -- and then diskcopies resulting from ongoing requests, for which the previous
      -- query wouldn't return any entry because of e.g. missing filesystem
      SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ DC.id
        FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id
                FROM StagePrepareToPutRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id
                FROM StagePrepareToGetRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id
                FROM StagePrepareToUpdateRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id
                FROM StageRepackRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ id
                FROM StagePutRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id
                FROM StageGetRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id
                FROM StageUpdateRequest WHERE svcClass = scId UNION ALL
              SELECT /*+ INDEX(StageDiskCopyReplicaRequest PK_StageDiskCopyReplicaRequ_Id) */ id
                FROM StageDiskCopyReplicaRequest WHERE svcClass = scId) Request,
             SubRequest, DiskCopy DC
       WHERE SubRequest.diskCopy = DC.id
         AND Request.id = SubRequest.request
         AND DC.castorFile = cfId
         AND DC.status IN (1, 2, 5, 11)  -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, WAITFS_SCHEDULING
      );
    IF dcsToRm.COUNT = 0 THEN
      -- We didn't find anything on this svcClass, fail and return
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7,  -- FAILED
             errorCode = 2,  -- ENOENT
             errorMessage = 'File not found on this service class'
       WHERE id = srId;
      RETURN;
    END IF;
    -- Select current status of the diskCopies
    SELECT status INTO dcStatus
      FROM DiskCopy
     WHERE id = dcsToRm(1);
    -- make sure we don't drop the last diskcopy of the original service class
    -- as it is needed to migrate safely. Indeed, nothing can insure that other copies
    -- are in service classes that can migrate to the requested tapepool(s)
    -- In case it is not the case, give up with the deletion.
    IF dcStatus = 10 THEN  -- CANBEMIGR
      SELECT count(*) INTO nbRes
        FROM DiskCopy, FileSystem, DiskPool2SvcClass 
       WHERE DiskCopy.fileSystem = FileSystem.id
         AND FileSystem.diskPool = DiskPool2SvcClass.parent
         AND DiskCopy.castorFile = cfId
         AND DiskCopy.status = 10  -- CANBEMIGR
         AND diskpool2svcclass.child = migSvcClass
         AND DiskCopy.id NOT IN
          (SELECT /*+ CARDINALITY(dcidTable 5) */ * FROM TABLE(dcsToRm) dcidTable);
      IF nbRes = 0 THEN
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
           SET status = 7,  -- FAILED
               errorCode = 16,  -- EBUSY
               errorMessage = 'As the file is not yet migrated, we cannot drop the last copy in this service class'
         WHERE id = srId;
         RETURN;
      END IF;
    ELSE
      -- Check whether something else is left: if not, do as
      -- if we are performing a stageRm everywhere.
      SELECT count(*) INTO nbRes FROM DiskCopy
         WHERE castorFile = cfId
           AND status IN (0, 2, 5, 6, 10, 11)  -- STAGED, WAITTAPERECALL, STAGEOUT, CANBEMIGR, WAITFS, WAITFS_SCHEDULING
           AND id NOT IN (SELECT /*+ CARDINALITY(dcidTable 5) */ *
                            FROM TABLE(dcsToRm) dcidTable);
      IF nbRes = 0 THEN
        -- nothing found, so we're dropping the last copy; then
        -- we need to perform all the checks to make sure we can
        -- allow the removal.
        scId := 0;
      END IF;
    END IF;
  END IF;

  IF scId = 0 THEN
    -- full cleanup is to be performed, do all necessary checks beforehand
    DECLARE
      segId INTEGER;
      unusedIds "numList";
    BEGIN
      -- check if removal is possible for migration
      SELECT count(*) INTO nbRes FROM DiskCopy
       WHERE status = 10 -- DISKCOPY_CANBEMIGR
         AND castorFile = cfId;
      IF nbRes > 0 THEN
        -- We found something, thus we cannot remove
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
           SET status = 7,  -- FAILED
               errorCode = 16,  -- EBUSY
               errorMessage = 'The file is not yet migrated'
         WHERE id = srId;
        RETURN;
      END IF;
      -- Stop ongoing recalls if stageRm either everywhere or the only available diskcopy.
      -- This is not entirely clean: a proper operation here should be to
      -- drop the SubRequest waiting for recall but keep the recall if somebody
      -- else is doing it, and taking care of other WAITSUBREQ requests as well...
      -- but it's fair enough, provided that the last stageRm will cleanup everything.
      -- XXX First lock all segments for the file. Note that
      -- XXX this step should be dropped once the tapeGateway
      -- XXX is deployed. The current recaller does not take
      -- XXX the proper lock on the castorFiles, hence we
      -- XXX need this here
      SELECT Segment.id BULK COLLECT INTO unusedIds
        FROM Segment, TapeCopy
       WHERE TapeCopy.castorfile = cfId
         AND TapeCopy.id = Segment.copy
       ORDER BY Segment.id
      FOR UPDATE OF Segment.id;
      -- Check whether we have any segment in SELECTED
      SELECT segment.id INTO segId
        FROM Segment, TapeCopy
       WHERE TapeCopy.castorfile = cfId
         AND TapeCopy.id = Segment.copy
         AND Segment.status = 7 -- SELECTED
         AND ROWNUM < 2;
      -- Something is running, so give up
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = 7,  -- FAILED
             errorCode = 16,  -- EBUSY
             errorMessage = 'The file is being recalled from tape'
       WHERE id = srId;
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Nothing running. We still may have found nothing at all...
      SELECT count(*) INTO nbRes FROM DiskCopy
       WHERE castorFile = cfId
         AND status NOT IN (4, 7, 9);  -- anything but FAILED, INVALID, BEINGDELETED
      IF nbRes = 0 THEN
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
           SET status = 7,  -- FAILED
               errorCode = 2,  -- ENOENT
               errorMessage = 'File not found on disk cache'
         WHERE id = srId;
        RETURN;
      END IF;
      
      deleteTapeCopies(cfId);
      -- Reselect what needs to be removed
      SELECT id BULK COLLECT INTO dcsToRm
        FROM DiskCopy
       WHERE castorFile = cfId
         AND status IN (0, 1, 2, 5, 6, 10, 11);  -- STAGED, WAIT*, STAGEOUT, CANBEMIGR
    END;
  END IF;

  -- Now perform the remove:
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  SELECT /*+ INDEX(SR I_SubRequest_DiskCopy) */ id
    BULK COLLECT INTO srIds
    FROM SubRequest SR
   WHERE diskcopy IN
     (SELECT /*+ CARDINALITY(dcidTable 5) */ * 	 
        FROM TABLE(dcsToRm) dcidTable) 	 
     AND status IN (0, 1, 2, 4, 5, 6, 13); -- START, RESTART, RETRY, WAITTAPERECALL, WAITSUBREQ, READY, READYFORSCHED
  IF srIds.COUNT > 0 THEN
    DECLARE
      srUuid VARCHAR(2048);
    BEGIN
      FOR i IN srIds.FIRST .. srIds.LAST LOOP
        UPDATE SubRequest
           SET status = CASE
                 WHEN status IN (6, 13) AND reqType = 133 THEN 9 ELSE 7
               END,  -- FAILED_FINISHED for DiskCopyReplicaRequests in status READYFORSCHED or READY, otherwise FAILED
               -- this so that user requests in status WAITSUBREQ are always marked FAILED even if they wait on a replication
               errorCode = 4,  -- EINTR
               errorMessage = 'Canceled by another user request'
         WHERE id = srIds(i) OR parent = srIds(i)
         RETURNING subreqId INTO srUuid;
        -- make the scheduler aware so that it can remove the transfer from the queues if needed
        INSERT INTO TransfersToAbort VALUES (srUuid);
      END LOOP;
    END;
    -- wake up the scheduler so that it can remove the transfer from the queues now
    DBMS_ALERT.SIGNAL('transfersToAbort', '');
  END IF;
  -- Set selected DiskCopies to either INVALID or FAILED. We deliberately
  -- ignore WAITDISK2DISKCOPY's (see bug #78826)
  FOR i IN dcsToRm.FIRST .. dcsToRm.LAST LOOP
    SELECT status INTO dcStatus
      FROM DiskCopy
     WHERE id = dcsToRm(i);
    UPDATE DiskCopy
       -- WAITTAPERECALL,WAITFS[_SCHED] -> FAILED, others -> INVALID
       SET status = decode(status, 2,4, 5,4, 11,4, 7)
     WHERE id = dcsToRm(i);
  END LOOP;
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
     AND status IN (0, 1, 2, 3, 4, 5, 7, 10, 12, 13, 14)   -- all but FINISHED, FAILED_FINISHED, ARCHIVED
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

/* PL/SQL function to elect a rmmaster master */
CREATE OR REPLACE FUNCTION isMonitoringMaster RETURN NUMBER IS
  locked EXCEPTION;
  PRAGMA EXCEPTION_INIT (locked, -54);
BEGIN
  LOCK TABLE RmMasterLock IN EXCLUSIVE MODE NOWAIT;
  RETURN 1;
EXCEPTION WHEN locked THEN
  RETURN 0;
END;
/

/* PL/SQL method implementing storeClusterStatus */
CREATE OR REPLACE PROCEDURE storeClusterStatus
(machines IN castor."strList",
 fileSystems IN castor."strList",
 machineValues IN castor."cnumList",
 fileSystemValues IN castor."cnumList") AS
 found   NUMBER;
 ind     NUMBER;
 dsId    NUMBER := 0;
 fs      NUMBER := 0;
 fsIds   "numList";
BEGIN
  -- Sanity check
  IF machines.COUNT = 0 OR fileSystems.COUNT = 0 THEN
    RETURN;
  END IF;
  -- First Update Machines
  FOR i IN machines.FIRST .. machines.LAST LOOP
    ind := machineValues.FIRST + 9 * (i - machines.FIRST);
    IF machineValues(ind + 1) = 3 THEN -- ADMIN DELETED
      BEGIN
        -- Resolve the machine name to its id
        SELECT id INTO dsId FROM DiskServer
         WHERE name = machines(i);
        -- If any of the filesystems belonging to the diskserver are currently
        -- in the process of being drained then do not delete the diskserver or
        -- its associated filesystems. Why? Because of unique constraint
        -- violations between the FileSystem and DrainingDiskCopy table.
        SELECT fileSystem BULK COLLECT INTO fsIds
          FROM DrainingFileSystem DFS, FileSystem FS
         WHERE DFS.fileSystem = FS.id
           AND FS.diskServer = dsId;
        IF fsIds.COUNT > 0 THEN
          -- Entries found so flag the draining process as DELETING
          UPDATE DrainingFileSystem
             SET status = 6  -- DELETING
           WHERE fileSystem IN
             (SELECT /*+ CARDINALITY(fsIdTable 5) */ *
                FROM TABLE (fsIds) fsIdTable);
        ELSE
          -- There is no outstanding process to drain the diskservers
          -- filesystems so we can now delete it.
          DELETE FROM FileSystem WHERE diskServer = dsId;
          DELETE FROM DiskServer WHERE name = machines(i);
        END IF;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        NULL;  -- Already deleted
      END;
    ELSE
      BEGIN
        SELECT id INTO dsId FROM DiskServer
         WHERE name = machines(i);
        UPDATE DiskServer
           SET status             = machineValues(ind),
               adminStatus        = machineValues(ind + 1),
               readRate           = machineValues(ind + 2),
               writeRate          = machineValues(ind + 3),
               nbReadStreams      = machineValues(ind + 4),
               nbWriteStreams     = machineValues(ind + 5),
               nbReadWriteStreams = machineValues(ind + 6),
               nbMigratorStreams  = machineValues(ind + 7),
               nbRecallerStreams  = machineValues(ind + 8)
         WHERE name = machines(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- We should insert a new machine here
        INSERT INTO DiskServer (name, id, status, adminStatus, readRate,
                                writeRate, nbReadStreams, nbWriteStreams,
                                nbReadWriteStreams, nbMigratorStreams,
                                nbRecallerStreams)
         VALUES (machines(i), ids_seq.nextval, machineValues(ind),
                 machineValues(ind + 1), machineValues(ind + 2),
                 machineValues(ind + 3), machineValues(ind + 4),
                 machineValues(ind + 5), machineValues(ind + 6),
                 machineValues(ind + 7), machineValues(ind + 8));
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
      SELECT id INTO dsId FROM DiskServer
       WHERE name = fileSystems(i);
    ELSE
      IF fileSystemValues(ind + 1) = 3 THEN -- ADMIN DELETED
        BEGIN
          -- Resolve the mountpoint name to its id
          SELECT id INTO fs
            FROM FileSystem
           WHERE mountPoint = fileSystems(i)
             AND diskServer = dsId;
          -- Check to see if the filesystem is currently in the process of
          -- being drained. If so, we flag it for deletion.
          found := 0;
          UPDATE DrainingFileSystem
             SET status = 6  -- DELETING
           WHERE fileSystem = fs
          RETURNING fs INTO found;
          -- No entry found so delete the filesystem.
          IF found = 0 THEN
            DELETE FROM FileSystem WHERE id = fs;
          END IF;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          NULL;  -- Already deleted
        END;
      ELSE
        BEGIN
          SELECT diskServer INTO dsId FROM FileSystem
           WHERE mountPoint = fileSystems(i) AND diskServer = dsId;
          UPDATE FileSystem
             SET status              = fileSystemValues(ind),
                 adminStatus         = fileSystemValues(ind + 1),
                 readRate            = fileSystemValues(ind + 2),
                 writeRate           = fileSystemValues(ind + 3),
                 nbReadStreams       = fileSystemValues(ind + 4),
                 nbWriteStreams      = fileSystemValues(ind + 5),
                 nbReadWriteStreams  = fileSystemValues(ind + 6),
                 nbMigratorStreams   = fileSystemValues(ind + 7),
                 nbRecallerStreams   = fileSystemValues(ind + 8),
                 free                = fileSystemValues(ind + 9),
                 totalSize           = fileSystemValues(ind + 10),
                 minFreeSpace        = fileSystemValues(ind + 11),
                 maxFreeSpace        = fileSystemValues(ind + 12),
                 minAllowedFreeSpace = fileSystemValues(ind + 13)
           WHERE mountPoint          = fileSystems(i)
             AND diskServer          = dsId;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- we should insert a new filesystem here
          INSERT INTO FileSystem (free, mountPoint, minFreeSpace,
                                  minAllowedFreeSpace, maxFreeSpace, totalSize,
                                  readRate, writeRate, nbReadStreams,
                                  nbWriteStreams, nbReadWriteStreams,
                                  nbMigratorStreams, nbRecallerStreams, id,
                                  diskPool, diskserver, status, adminStatus)
          VALUES (fileSystemValues(ind + 9), fileSystems(i), fileSystemValues(ind+11),
                  fileSystemValues(ind + 13), fileSystemValues(ind + 12),
                  fileSystemValues(ind + 10), fileSystemValues(ind + 2),
                  fileSystemValues(ind + 3), fileSystemValues(ind + 4),
                  fileSystemValues(ind + 5), fileSystemValues(ind + 6),
                  fileSystemValues(ind + 7), fileSystemValues(ind + 8),
                  ids_seq.nextval, 0, dsId, 2, 1); -- FILESYSTEM_DISABLED, ADMIN_FORCE
        END;
      END IF;
      ind := ind + 14;
    END IF;
    -- Release the lock on the FileSystem as soon as possible to prevent
    -- deadlocks with other activities e.g. recaller
    COMMIT;
  END LOOP;
END;
/


/* PL/SQL method implementing selectPriority */
CREATE OR REPLACE PROCEDURE selectPriority(
  inUid IN INTEGER,
  inGid IN INTEGER,
  inPriority IN INTEGER,
  dbInfo OUT castor.TapeAccessPriority_Cur) AS
BEGIN
  OPEN dbInfo FOR
    SELECT euid, egid, priority FROM PriorityMap
     WHERE (euid = inUid OR inUid = -1)
       AND (egid = inGid OR inGid = -1)
       AND (priority = inPriority OR inPriority = -1);
END;
/

/* PL/SQL method implementing enterPriority
   it can raise constraint violation exception */
CREATE OR REPLACE PROCEDURE enterPriority(
  inUid IN NUMBER,
  inGid IN NUMBER,
  inPriority IN INTEGER) AS
BEGIN
  INSERT INTO PriorityMap (euid, egid, priority)
  VALUES (inUid, inGid, inPriority);
END;
/


/* PL/SQL method implementing deletePriority */
CREATE OR REPLACE PROCEDURE deletePriority(
  inUid IN INTEGER,
  inGid IN INTEGER) AS
BEGIN
  DELETE FROM PriorityMap
   WHERE (euid = inUid OR inUid = -1)
     AND (egid = inGid OR inGid = -1);
END;
/
