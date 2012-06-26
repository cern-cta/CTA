/*******************************************************************
 *
 * PL/SQL code for the tape gateway daemon
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* PL/SQL declaration for the castorTape package */
CREATE OR REPLACE PACKAGE castorTape AS 
  TYPE TapeGatewayRequest IS RECORD (
    accessMode INTEGER,
    mountTransactionId NUMBER, 
    vid VARCHAR2(2048));
  TYPE TapeGatewayRequest_Cur IS REF CURSOR RETURN TapeGatewayRequest;
  TYPE VIDPriorityRec IS RECORD (vid VARCHAR2(2048), vdqmPriority INTEGER);
  TYPE VIDPriority_Cur IS REF CURSOR RETURN VIDPriorityRec;
  TYPE FileToRecallCore IS RECORD (
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   fileTransactionId NUMBER,
   filePath VARCHAR2(2048),
   blockId RAW(4),
   fseq INTEGER);
  TYPE FileToRecallCore_Cur IS REF CURSOR RETURN  FileToRecallCore;  
  TYPE FileToMigrateCore IS RECORD (
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   lastKnownFileName VARCHAR2(2048),
   filePath VARCHAR2(2048),
   fileTransactionId NUMBER,
   fseq INTEGER,
   fileSize NUMBER);
  TYPE FileToMigrateCore_Cur IS REF CURSOR RETURN  FileToMigrateCore;  
END castorTape;
/

/* attach drive request to a recallMount or a migrationMount */
CREATE OR REPLACE
PROCEDURE tg_attachDriveReq(inVID IN VARCHAR2,
                            inVdqmId IN INTEGER,
                            inMode IN INTEGER,
                            inLabel IN VARCHAR2,
                            inDensity IN VARCHAR2) AS
BEGIN
  IF inMode = tconst.WRITE_DISABLE THEN
    UPDATE RecallMount
       SET lastvdqmpingtime   = gettime(),
           mountTransactionId = inVdqmId,
           status             = tconst.RECALLMOUNT_WAITDRIVE,
           label              = inLabel,
           density            = inDensity
     WHERE VID = inVID;
  ELSE
    UPDATE MigrationMount
       SET lastvdqmpingtime   = gettime(),
           mountTransactionId = inVdqmId,
           status             = tconst.MIGRATIONMOUNT_WAITDRIVE,
           label              = inLabel,
           density            = inDensity
     WHERE VID = inVID;
  END IF;
END;
/

/* attach the tapes to the migration mounts  */
CREATE OR REPLACE
PROCEDURE tg_attachTapesToMigMounts (
  inStartFseqs IN castor."cnumList",
  inMountIds   IN castor."cnumList",
  inTapeVids   IN castor."strList") AS
BEGIN
  -- Sanity check
  IF (inStartFseqs.COUNT != inTapeVids.COUNT) THEN
    RAISE_APPLICATION_ERROR (-20119,
       'Size mismatch for arrays: inStartFseqs.COUNT='||inStartFseqs.COUNT
       ||' inTapeVids.COUNT='||inTapeVids.COUNT);
  END IF;
  FORALL i IN inMountIds.FIRST .. inMountIds.LAST
    UPDATE MigrationMount
       SET VID = inTapeVids(i),
           lastFseq = inStartFseqs(i),
           startTime = getTime(),
           status = tconst.MIGRATIONMOUNT_SEND_TO_VDQM
     WHERE id = inMountIds(i);
  COMMIT;
END;
/

/* update the db when a tape session is ended */
CREATE OR REPLACE PROCEDURE tg_endTapeSession(inMountTransactionId IN NUMBER,
                                              inErrorCode IN INTEGER) AS
  varMjIds "numList";    -- recall/migration job Ids
  varMountId INTEGER;
BEGIN
  -- Let's assume this is a migration mount
  SELECT id INTO varMountId
    FROM MigrationMount
   WHERE mountTransactionId = inMountTransactionId
   FOR UPDATE;
  -- yes, it's a migration mount: delete it and detach all selected jobs
  UPDATE MigrationJob
     SET status = tconst.MIGRATIONJOB_PENDING,
         VID = NULL,
         mountTransactionId = NULL
   WHERE mountTransactionId = inMountTransactionId
     AND status = tconst.MIGRATIONJOB_SELECTED;
  DELETE FROM MigrationMount
   WHERE id = varMountId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- was not a migration session, let's try a recall one
  DECLARE
    varVID VARCHAR2(2048);
    varRjIds "numList";
  BEGIN
    SELECT vid INTO varVID
      FROM RecallMount
     WHERE mountTransactionId = inMountTransactionId
     FOR UPDATE;
    -- it was a recall mount
    -- find and reset the all RecallJobs of files for this VID
    UPDATE RecallJob
       SET status = tconst.RECALLJOB_NEW
     WHERE castorFile IN (SELECT castorFile
                            FROM RecallJob
                           WHERE VID = varVID
                             AND status IN (tconst.RECALLJOB_PENDING,
                                            tconst.RECALLJOB_SELECTED,
                                            tconst.RECALLJOB_RETRYMOUNT));
    DELETE FROM RecallMount WHERE vid = varVID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Small infusion of paranoia ;-) We should never reach that point...
    ROLLBACK;
    RAISE_APPLICATION_ERROR (-20119,
      'No recall or migration mount found for mountTransactionId ' ||
       inMountTransactionId || ' in tg_endTapeSession');
  END;
END;
/


/* PL/SQL method implementing bestFileSystemForRecall */
CREATE OR REPLACE PROCEDURE bestFileSystemForRecall(inCfId IN INTEGER, outFilePath OUT VARCHAR2) AS
  varCfId INTEGER;
  varFileSystemId NUMBER := 0;
  nb NUMBER;
BEGIN
  -- try and select a good FileSystem for this recall
  FOR f IN (SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/
                   DiskServer.name ||':'|| FileSystem.mountPoint AS remotePath, FileSystem.id,
                   FileSystem.diskserver, CastorFile.fileSize, CastorFile.fileId, CastorFile.nsHost
              FROM DiskServer, FileSystem, DiskPool2SvcClass,
                   (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClass FROM StageGetRequest                            UNION ALL
                    SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, svcClass FROM StagePrepareToGetRequest UNION ALL
                    SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, svcClass from StageRepackRequest                   UNION ALL
                    SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClass from StageUpdateRequest                   UNION ALL
                    SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcClass FROM StagePrepareToUpdateRequest) Request,
                    SubRequest, CastorFile
             WHERE CastorFile.id = inCfId
               AND SubRequest.castorfile = inCfId
               AND SubRequest.status = dconst.SUBREQUEST_WAITTAPERECALL
               AND Request.id = SubRequest.request
               AND Request.svcclass = DiskPool2SvcClass.child
               AND FileSystem.diskpool = DiskPool2SvcClass.parent
               -- a priori, we want to have enough free space. However, if we don't, we accept to start writing
               -- if we have a minimum of 30GB free and count on gerbage collection to liberate space while writing
               -- We still check that the file fit on the disk, and actually keep a 30% margin so that very recent
               -- files can be kept
               AND (FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > CastorFile.fileSize
                 OR (FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > 30000000000
                 AND FileSystem.totalSize * 0.7 > CastorFile.fileSize))
               AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
               AND DiskServer.id = FileSystem.diskServer
               AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
          ORDER BY -- first minimize concurrent migrators/recallers in the same FS or DS
                   FileSystem.nbRecallerStreams + FileSystem.nbMigratorStreams ASC,
                   DiskServer.nbRecallerStreams + DiskServer.nbMigratorStreams ASC,
                   -- then order by rate as defined by the function
                   fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                                  FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams) DESC,
                   -- use randomness to avoid preferring always the same FS when everything is idle
                   DBMS_Random.value)
  LOOP
    varFileSystemId := f.id;
    buildPathFromFileId(f.fileId, f.nsHost, ids_seq.nextval, outFilePath);
    outFilePath := f.remotePath || outFilePath;
    -- Check that we don't already have a copy of this file on this filesystem.
    -- This will never happen in normal operations but may be the case if a filesystem
    -- was disabled and did come back while the tape recall was waiting.
    -- Even if we optimize by cancelling remaining unneeded tape recalls when a
    -- fileSystem comes back, the ones running at the time of the come back will have
    -- the problem.
    SELECT count(*) INTO nb
      FROM DiskCopy
     WHERE fileSystem = f.id
       AND castorfile = inCfid
       AND status IN (dconst.DISKCOPY_STAGED, dconst.DISKCOPY_CANBEMIGR);
    IF nb != 0 THEN
      raise_application_error(-20115, 'Recaller could not find a FileSystem in production in the requested SvcClass and without copies of this file');
    END IF;
    RETURN;
  END LOOP;
  IF varFileSystemId = 0 THEN
    raise_application_error(-20115, 'No suitable filesystem found for this recalled file');
  END IF;
END;
/


/* get the migration mounts without any tape associated */
CREATE OR REPLACE
PROCEDURE tg_getMigMountsWithoutTapes(outStrList OUT SYS_REFCURSOR) AS
BEGIN
  -- get migration mounts in state WAITTAPE
  OPEN outStrList FOR
    SELECT M.id, TP.name
      FROM MigrationMount M, Tapepool TP
     WHERE M.status = tconst.MIGRATIONMOUNT_WAITTAPE
       AND M.tapepool = TP.id 
       FOR UPDATE OF M.id SKIP LOCKED;   
END;
/

/* get tape with a pending request in VDQM */
CREATE OR REPLACE
PROCEDURE tg_getTapesWithDriveReqs(
  inTimeLimit     IN  NUMBER,
  outTapeRequest  OUT castorTape.tapegatewayrequest_Cur) AS
  varTgrId        "numList";
  varRecMountIds  "numList";
  varMigMountIds  "numList";
  varNow          NUMBER;
BEGIN 
  -- we only look for the Recall/MigrationMounts which have a VDQM ping
  -- time older than inTimeLimit
  -- No need to query the clock all the time
  varNow := gettime();
  
  -- Find all the recall mounts and lock
  SELECT id BULK COLLECT INTO varRecMountIds
    FROM RecallMount
   WHERE status IN (tconst.RECALLMOUNT_WAITDRIVE, tconst.RECALLMOUNT_RECALLING)
     AND varNow - lastVdqmPingTime > inTimeLimit
     FOR UPDATE SKIP LOCKED;
     
  -- Find all the migration mounts and lock
  SELECT id BULK COLLECT INTO varMigMountIds
    FROM MigrationMount
   WHERE status IN (tconst.MIGRATIONMOUNT_WAITDRIVE, tconst.MIGRATIONMOUNT_MIGRATING)
     AND varNow - lastVdqmPingTime > inTimeLimit
     FOR UPDATE SKIP LOCKED;
     
  -- Update the last VDQM ping time for all of them.
  UPDATE RecallMount
     SET lastVdqmPingTime = varNow
   WHERE id IN (SELECT /*+ CARDINALITY(trTable 5) */ * 
                  FROM TABLE (varRecMountIds));
  UPDATE MigrationMount
     SET lastVdqmPingTime = varNow
   WHERE id IN (SELECT /*+ CARDINALITY(trTable 5) */ *
                  FROM TABLE (varMigMountIds));
                   
  -- Return them
  OPEN outTapeRequest FOR
    -- Read case
    SELECT tconst.WRITE_DISABLE, mountTransactionId, VID
      FROM RecallMount
     WHERE id IN (SELECT /*+ CARDINALITY(trTable 5) */ *
                    FROM TABLE(varRecMountIds))
     UNION ALL
    -- Write case
    SELECT tconst.WRITE_ENABLE, mountTransactionId, VID
      FROM MigrationMount
     WHERE id IN (SELECT /*+ CARDINALITY(trTable 5) */ *
                    FROM TABLE(varMigMountIds));
END;
/

/* get a the list of tapes to be sent to VDQM */
CREATE OR REPLACE
PROCEDURE tg_getTapeWithoutDriveReq(outVID OUT VARCHAR2,
                                    outVdqmPriority OUT INTEGER,
                                    outMode OUT INTEGER) AS
BEGIN
  -- try to find a migration mount
  SELECT VID, 0, 1  -- harcoded priority to 0, mode 1 == WRITE_ENABLE
    INTO outVID, outVdqmPriority, outMode
    FROM MigrationMount
   WHERE status = tconst.MIGRATIONMOUNT_SEND_TO_VDQM
     AND ROWNUM < 2
     FOR UPDATE SKIP LOCKED;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no migration mount to process, try to find a recall mount
  BEGIN
    SELECT RecallMount.VID, RecallGroup.vdqmPriority, 0  -- mode 0 == WRITE_DISABLE
      INTO outVID, outVdqmPriority, outMode
      FROM RecallMount, RecallGroup
     WHERE RecallMount.status = tconst.RECALLMOUNT_NEW
       AND RecallMount.recallGroup = RecallGroup.id
       AND ROWNUM < 2
       FOR UPDATE OF RecallMount.id SKIP LOCKED;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- no recall mount to process either
    outVID := '';
    outVdqmPriority := 0;
    outMode := 0;
  END;
END;
/

/* get tape to release in VMGR */
CREATE OR REPLACE
PROCEDURE tg_getTapeToRelease(
  inMountTransactionId IN  INTEGER, 
  outVID      OUT NOCOPY VARCHAR2, 
  outMode     OUT INTEGER,
  outFull     OUT INTEGER) AS
BEGIN
  -- suppose it's a recall case
  SELECT vid INTO outVID 
    FROM RecallMount
   WHERE mountTransactionId = inMountTransactionId;
  outMode := tconst.WRITE_DISABLE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no a recall, then let's suppose it's a migration
  BEGIN
    SELECT vid, full
    INTO outVID, outFull
      FROM MigrationMount
     WHERE mountTransactionId = inMountTransactionId;
    outMode := tconst.WRITE_ENABLE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- must have been already cleaned by the checker
    NULL;
  END;
END;
/

/* restart taperequest which had problems */
CREATE OR REPLACE
PROCEDURE tg_restartLostReqs(inMountTransactionIds IN castor."cnumList") AS
BEGIN
 FOR i IN inMountTransactionIds.FIRST .. inMountTransactionIds.LAST LOOP   
   tg_endTapeSession(inMountTransactionIds(i), 0);
 END LOOP;
 COMMIT;
END;
/

/* Checks whether a recall that was reported successful is ok from the namespace
 * point of view. This includes :
 *   - checking that the file still exists
 *   - checking that the file was not overwritten
 *   - checking the checksum, and setting it if there was none
 * In case one of the check fails, appropriate cleaning actions are taken.
 * Returns whether the checks were all ok. If not, the caller should
 * return immediately as all corrective actions were already taken.
 */
CREATE OR REPLACE FUNCTION checkRecallInNS(inCfId IN INTEGER,
                                           inMountTransactionId IN INTEGER,
                                           inVID IN VARCHAR2,
                                           inCopyNb IN INTEGER,
                                           inFseq IN INTEGER,
                                           inFileId IN INTEGER,
                                           inNsHost IN VARCHAR2,
                                           inCksumName IN VARCHAR2,
                                           inCksumValue IN INTEGER,
                                           inLastUpdateTime IN NUMBER,
                                           inReqId IN VARCHAR2,
                                           inLogContext IN VARCHAR2) RETURN BOOLEAN AS
  varNSLastUpdateTime NUMBER;
  varNSCsumtype VARCHAR2(2048);
  varNSCsumvalue VARCHAR2(2048);
BEGIN
  -- check the namespace
  SELECT mtime, csumtype, csumvalue
    INTO varNSLastUpdateTime, varNSCsumtype, varNSCsumvalue
    FROM Cns_File_Metadata@remoteNs
   WHERE fileid = inFileId;

  -- was the file overwritten in the meantime ?
  IF varNSLastUpdateTime > inLastUpdateTime THEN
    -- It was, recall should be restarted from scratch
    deleteRecallJobs(inCfId);
    UPDATE SubRequest
       SET status = dconst.SUBREQUEST_RESTART
     WHERE castorFile = inCfId
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
    -- log "setFileRecalled : file was overwritten during recall, restarting from scratch"
    logToDLF(inReqId, dlf.LVL_NOTICE, dlf.RECALL_FILE_OVERWRITTEN, inFileId, inNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' NsLastUpdateTime=' || TO_CHAR(varNSLastUpdateTime) ||
             ' CFLastUpdateTime=' || TO_CHAR(inLastUpdateTime));
    RETURN FALSE;
  END IF;

  -- is the checksum set in the namespace ?
  IF varNSCsumtype IS NULL THEN
    -- no -> let's set it (note that the function called commits in the remote DB)
    setSegChecksumWhenNull@remoteNS(inFileId, inCopyNb, inCksumName, inCksumValue);
    -- log 'setFileRecalled : created missing checksum in the namespace'
    logToDLF(inReqId, dlf.LVL_SYSTEM, dlf.RECALL_CREATED_CHECKSUM, inFileId, inNsHost, 'nsd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' CopyNo=' || inCopyNb ||
             ' TPVID=' || inVID || ' Fseq=' || TO_CHAR(inFseq) || ' checksumType='  || inCksumName ||
             ' checksumValue=' || TO_CHAR(inCksumValue));
  ELSE
    -- is the checksum matching ?
    -- note that this is probably useless as it was already checked at transfer time
    IF inCksumName = varNSCsumtype AND TO_CHAR(inCksumValue, 'XXXXXXXX') != varNSCsumvalue THEN
      -- not matching ! Retry the recall
      retryOrFailRecall(inCfId, inVID);
      -- log "setFileRecalled : bad checksum detected, will retry if allowed"
      logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_BAD_CHECKSUM, inFileId, inNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
               ' fseq=' || TO_CHAR(inFseq) || ' checksumType='  || inCksumName ||
               ' expectedChecksumValue=' || varNSCsumvalue ||
               ' checksumValue=' || TO_CHAR(inCksumValue, 'XXXXXXXX'));
      RETURN FALSE;
    END IF;
  END IF;
  RETURN TRUE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- file got dropped from the namespace, recall should be cancelled and requests failed
  deleteRecallJobs(inCfId);
  UPDATE SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOENT,
           errorMessage = 'File disappeared from namespace during recall'
     WHERE castorFile = inCfId
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
  -- log "setFileRecalled : file was dropped from namespace during recall, giving up"
  logToDLF(inReqId, dlf.LVL_NOTICE, dlf.RECALL_FILE_DROPPED, inFileId, inNsHost, 'tapegatewayd',
           'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
           ' fseq=' || TO_CHAR(inFseq) || ' CFLastUpdateTime=' || TO_CHAR(inLastUpdateTime) || ' ' || inLogContext);
  RETURN FALSE;
END;
/

/* update the db after a successful recall */
CREATE OR REPLACE PROCEDURE tg_setFileRecalled(inMountTransactionId IN INTEGER,
                                               inFseq IN INTEGER,
                                               inFilePath IN VARCHAR2,
                                               inCksumName IN VARCHAR2,
                                               inCksumValue IN INTEGER,
                                               inReqId IN VARCHAR2,
                                               inLogContext IN VARCHAR2) AS
  varFileId         INTEGER;
  varNsHost         VARCHAR2(2048);
  varVID            VARCHAR2(2048);
  varCopyNb         INTEGER;
  varSvcClassId     INTEGER;
  varEuid           INTEGER;
  varEgid           INTEGER;
  varLastUpdateTime INTEGER;
  varCfId           INTEGER;
  varFSId           INTEGER;
  varDCPath         VARCHAR2(2048);
  varDcId           INTEGER;
  varFileSize       INTEGER;
  varNbMigrationsStarted INTEGER;
  varGcWeight       NUMBER;
  varGcWeightProc   VARCHAR2(2048);
BEGIN
  -- first lock Castorfile, check NS and parse path

  -- Get RecallJob and lock Castorfile
  BEGIN
    SELECT CastorFile.id, CastorFile.fileId, CastorFile.nsHost, CastorFile.lastUpdateTime,
           CastorFile.fileSize, RecallMount.VID, RecallJob.copyNb, RecallJob.svcClass,
           RecallJob.euid, RecallJob.egid
      INTO varCfId, varFileId, varNsHost, varLastUpdateTime, varFileSize, varVID,
           varCopyNb, varSvcClassId, varEuid, varEgid
      FROM RecallMount, RecallJob, CastorFile
     WHERE RecallMount.mountTransactionId = inMountTransactionId
       AND RecallJob.vid = RecallMount.vid
       AND RecallJob.fseq = inFseq
       AND RecallJob.status = tconst.RECALLJOB_SELECTED
       AND RecallJob.castorFile = CastorFile.id
       FOR UPDATE OF CastorFile.id;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "setFileRecalled : unable to identify Recall. giving up"
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_NOT_FOUND, 0, '', 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' ' || inLogContext);
    RETURN;
  END;
  -- Check that the file is still there in the namespace (and did not get overwritten)
  -- Note that error handling and logging is done inside the function
  IF NOT checkRecallInNS(varCfId, inMountTransactionId, varVID, varCopyNb, inFseq, varFileId, varNsHost, 
                         inCksumName, inCksumValue, varLastUpdateTime, inReqId, inLogContext) THEN RETURN; END IF;
  -- get diskserver, filesystem and path from full path in input
  BEGIN
    parsePath(inFilePath, varFSId, varDCPath, varDCId);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "setFileRecalled : unable to parse input path. giving up"
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_INVALID_PATH, varFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' ' || inLogContext);
    RETURN;
  END;

  -- Then deal with recalljobs and potential migrationJobs

  -- Delete recall jobs
  DELETE FROM RecallJob WHERE castorFile = varCfId;
  -- trigger waiting migrations if any
  -- Note that we reset the creation time as if the MigrationJob was created right now
  -- this is because "creationTime" is actually the time of entering the "PENDING" state
  -- in the cases where the migrationJob went through a WAITINGONRECALL state
  UPDATE /*+ INDEX (MigrationJob I_MigrationJob_CFVID) */ MigrationJob
     SET status = tconst.MIGRATIONJOB_PENDING,
         creationTime = getTime()
   WHERE status = tconst.MIGRATIONJOB_WAITINGONRECALL
     AND castorFile = varCfId;
  varNbMigrationsStarted := SQL%ROWCOUNT;

  -- Deal with DiskCopies

  -- compute GC weight of the recalled diskcopy
  varGcWeightProc := castorGC.getRecallWeight(varSvcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || varGcWeightProc || '(:size); END;'
    USING OUT varGcWeight, IN varFileSize;
  -- create the DiskCopy
  INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                        ownerUid, ownerGid, id, gcType, fileSystem, castorFile, status)
  VALUES (varDCPath, varGcWeight, getTime(), getTime(), varFileSize, 0,
          varEuid, varEgid, varDCId, NULL, varFSId, varCfId,
          decode(varNbMigrationsStarted, -- status
                 0, dconst.DISKCOPY_STAGED,   -- STAGED if no migrations
                 dconst.DISKCOPY_CANBEMIGR)); -- otherwise CANBEMIGR
  -- in case there are migrations, update remaining STAGED diskcopies to CANBEMIGR
  -- we may have them in case we've reacalled a staged file, and so far we only dealt with dcId
  IF varNbMigrationsStarted > 0 THEN
    UPDATE DiskCopy SET status = 10  -- DISKCOPY_CANBEMIGR
     WHERE castorFile = varCfId AND status = 0;  -- DISKCOPY_STAGED
  END IF;

  -- Finally deal with user requests

  UPDATE SubRequest
     SET status = decode(reqType,
                         119, dconst.SUBREQUEST_REPACK, -- repack case
                         dconst.SUBREQUEST_RESTART),    -- standard case
         getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
         lastModificationTime = getTime()
   WHERE castorFile = varCfId
     AND status = dconst.SUBREQUEST_WAITTAPERECALL;

  -- trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(varCfId, varEuid, varEgid);

  -- log success
  logToDLF(inReqId, dlf.LVL_SYSTEM, dlf.RECALL_COMPLETED_DB, varFileId, varNsHost, 'tapegatewayd',
           'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
           ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' ' || inLogContext);
END;
/

/* Attempt to retry a recall. Fail it in case it should not be retried anymore */
CREATE OR REPLACE PROCEDURE retryOrFailRecall(inCfId IN NUMBER, inVID IN VARCHAR2) AS
  varUnused INTEGER;
  varRecallStillAlive INTEGER;
BEGIN
  -- lock castorFile
  SELECT id INTO varUnused FROM CastorFile WHERE id = inCfId FOR UPDATE;
  -- increase retry counters within mount and set recallJob status to NEW
  UPDATE RecallJob
     SET nbRetriesWithinMount = nbRetriesWithinMount + 1,
         status = tconst.RECALLJOB_NEW
   WHERE castorFile = inCfId
     AND VID = inVID;
  -- detect the RecallJobs with too many retries within this mount
  -- mark them for a retry on next mount
  UPDATE RecallJob
     SET nbRetriesWithinMount = 0,
         nbMounts = nbMounts + 1,
         status = tconst.RECALLJOB_RETRYMOUNT
   WHERE castorFile = inCfId
     AND VID = inVID
     AND nbRetriesWithinMount >= TO_NUMBER(getConfigOption('Recall', 'MaxNbRetriesWithinMount', 2));
  -- stop here if no recallJob was concerned
  IF SQL%ROWCOUNT = 0 THEN RETURN; END IF;
  -- detect RecallJobs with too many mounts
  DELETE RecallJob
   WHERE castorFile = inCfId
     AND VID = inVID
     AND nbMounts >= TO_NUMBER(getConfigOption('Recall', 'MaxNbMounts', 3));
  -- check whether other RecallJobs are still around for this file (other copies on tape)
  SELECT count(*) INTO varRecallStillAlive
    FROM RecallJob
   WHERE castorFile = inCfId
     AND ROWNUM < 2;
  -- if no remaining recallJobs, the subrequests are failed
  IF varRecallStillAlive = 0 THEN
    UPDATE /*+ INDEX(Subrequest I_Subrequest_Castorfile) */ SubRequest 
       SET status = dconst.SUBREQUEST_FAILED,
           lastModificationTime = getTime(),
           errorCode = 1015,  -- SEINTERNAL
           errorMessage = 'File recall from tape has failed, please try again later'
     WHERE castorFile = inCfId 
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
  END IF;
END;
/

/* Attempt to retry a migration. Fail it in case it should not be retried anymore */
CREATE OR REPLACE PROCEDURE retryOrFailMigration(inMountTrId IN NUMBER, inFileId IN VARCHAR2, inNsHost IN VARCHAR2,
                                                 inErrorCode IN NUMBER, inReqId IN VARCHAR2) AS
  varFileTrId NUMBER;
BEGIN
  -- For the time being, we ignore the error code and apply the same policy to any
  -- tape-side error. Note that NS errors like ENOENT are caught at a second stage and never retried.
  -- Check if a new retry is allowed
  UPDATE (
    SELECT nbRetries, status, vid, mountTransactionId, fileTransactionId
      FROM MigrationJob MJ, CastorFile CF
     WHERE mountTransactionId = inMountTrId
       AND MJ.castorFile = CF.id
       AND CF.fileId = inFileId
       AND CF.nsHost = inNsHost
       AND nbRetries <= TO_NUMBER(getConfigOption('Migration', 'MaxNbMounts', 7)))
    SET nbRetries = nbRetries + 1,
        status = tconst.MIGRATIONJOB_PENDING,
        vid = NULL,
        mountTransactionId = NULL
    RETURNING fileTransactionId INTO varFileTrId;
  IF SQL%ROWCOUNT = 0 THEN
    -- Nb of retries exceeded or migration job not found, fail migration
    failFileMigration(inMountTrId, inFileId, inErrorCode, inReqId);
  -- ELSE we have one more retry, which has been logged upstream
  END IF;
END;
/


/* update the db when a tape session is started */
CREATE OR REPLACE
PROCEDURE tg_startTapeSession(inMountTransactionId IN NUMBER,
                              outVid        OUT NOCOPY VARCHAR2,
                              outAccessMode OUT INTEGER,
                              outRet        OUT INTEGER,
                              outDensity    OUT NOCOPY VARCHAR2,
                              outLabel      OUT NOCOPY VARCHAR2) AS
  varUnused   NUMBER;
  varTapePool INTEGER;
BEGIN
  outRet := 0;
  -- try to deal with a read case
  UPDATE RecallMount
     SET status = tconst.RECALLMOUNT_RECALLING
   WHERE mountTransactionId = inMountTransactionId
  RETURNING VID, tconst.WRITE_DISABLE, 0, density, label
    INTO outVid, outAccessMode, outRet, outDensity, outLabel;
  IF SQL%ROWCOUNT > 0 THEN
    -- it is a read case
    -- check whether there is something to do
    BEGIN
      SELECT id INTO varUnused FROM RecallJob WHERE VID=outVID AND ROWNUM < 2;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- no more file to recall. Force the cleanup and return -1
      UPDATE RecallMount
         SET lastvdqmpingtime = 0
       WHERE mountTransactionId = inMountTransactionId;
      outRet:=-1;
    END;
  ELSE
    -- not a read, so it should be a write
    UPDATE MigrationMount
       SET status = tconst.MIGRATIONMOUNT_MIGRATING
     WHERE mountTransactionId = inMountTransactionId
    RETURNING VID, tconst.WRITE_ENABLE, 0, density, label, tapePool
    INTO outVid, outAccessMode, outRet, outDensity, outLabel, varTapePool;
    IF SQL%ROWCOUNT > 0 THEN
      -- it is a write case
      -- check whether there is something to do
      BEGIN
        SELECT id INTO varUnused FROM MigrationJob WHERE tapePool=varTapePool AND ROWNUM < 2;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- no more file to migrate. Force the cleanup and return -1
        UPDATE MigrationMount
           SET lastvdqmpingtime = 0
         WHERE mountTransactionId = inMountTransactionId;
        outRet:=-1;
      END;
    ELSE
      -- it was neither a read nor a write -> not found error.
      outRet:=-2; -- UNKNOWN request
    END IF;
  END IF;
END;
/

/* delete MigrationMount */
CREATE OR REPLACE PROCEDURE tg_deleteMigrationMount(inMountId IN NUMBER) AS
BEGIN
  DELETE FROM MigrationMount WHERE id=inMountId;
END;
/


/* fail recall of a given CastorFile for a non existing tape */
CREATE OR REPLACE PROCEDURE cancelRecallForCFAndVID(inCfId IN INTEGER,
                                                    inVID IN VARCHAR2,
                                                    inErrorCode IN INTEGER,
                                                    inErrorMsg IN VARCHAR2) AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  varNbRecalls INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
BEGIN
  -- lock castorFile, skip if it's missing
  -- (it may have disappeared in the mean time as we held no lock)
  BEGIN
    SELECT fileid, nsHost INTO varFileId, varNsHost
      FROM CastorFile
     WHERE id = inCfId
       FOR UPDATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN;
  END;
  -- log "Canceling RecallJobs for given VID"
  logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_CANCEL_RECALLJOB_VID, varFileId, varNsHost, 'tapegatewayd',
           'errorCode=' || TO_CHAR(inErrorCode) ||
           ' errorMessage=' || inErrorMsg ||
           ' TPVID=' || inVID);
  -- remove recallJobs that need the non existing tape
  DELETE FROM RecallJob WHERE castorfile = inCfId AND VID=inVID;
  -- check if other recallJobs remain (typically dual copy tapes)
  SELECT COUNT(*) INTO varNbRecalls FROM RecallJob WHERE castorfile = inCfId;
  -- if no remaining recalls, fail requests and cleanup
  IF varNbRecalls = 0 THEN
    -- log "Failing Recall(s)"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_FAILING, varFileId, varNsHost, 'tapegatewayd',
             'errorCode=' || TO_CHAR(inErrorCode) ||
             ' errorMessage=' || inErrorMsg ||
             ' TPVID=' || inVID);
    -- delete potential migration jobs waiting on recalls
    deleteMigrationJobsForRecall(inCfId);
    -- Fail the associated subrequest(s)
    UPDATE /*+ INDEX(SR I_Subrequest_Castorfile)*/ SubRequest SR
       SET SR.status = dconst.SUBREQUEST_FAILED,
           SR.getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED, --  (not strictly correct but the request is over anyway)
           SR.lastModificationTime = getTime(),
           SR.errorCode = 1015,  -- SEINTERNAL
           SR.errorMessage = 'File recall from tape has failed (tape not available), please try again later',
           SR.parent = 0
     WHERE SR.castorFile = inCfId
       AND SR.status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_WAITSUBREQ);
  END IF;
  -- commit
  COMMIT;
END;
/

CREATE OR REPLACE PROCEDURE cancelMigrationOrRecall(inMode IN INTEGER,
                                                    inVID IN VARCHAR2,
                                                    inErrorCode IN INTEGER,
                                                    inErrorMsg IN VARCHAR2) AS
BEGIN
  IF inMode = tconst.WRITE_ENABLE THEN
    -- cancel the migration
    DELETE FROM MigrationMount WHERE VID = inVID;
  ELSE
    -- cancel the recall
    DELETE FROM RecallMount WHERE VID = inVID;
    -- fail the recalls of all files that waited for this tape
    FOR file IN (SELECT castorFile FROM RecallJob WHERE VID = inVID) LOOP
      -- note that this call commits
      cancelRecallForCFAndVID(file.castorFile, inVID, inErrorCode, inErrorMsg);
    END LOOP;
  END IF;
END;
/

/* flag tape as full for a given session */
CREATE OR REPLACE PROCEDURE tg_flagTapeFull (inMountTransactionId IN NUMBER) AS
BEGIN
  UPDATE MigrationMount SET full = 1 WHERE mountTransactionId = inMountTransactionId;
END;
/

/* Find the VID of the tape used in a tape session */
CREATE OR REPLACE PROCEDURE tg_getMigrationMountVid (
    inMountTransactionId IN NUMBER,
    outVid          OUT NOCOPY VARCHAR2,
    outTapePool     OUT NOCOPY VARCHAR2) AS
    varMMId         NUMBER;
    varUnused       NUMBER;
BEGIN
  SELECT MigrationMount.vid, TapePool.name
    INTO outVid, outTapePool
    FROM MigrationMount, TapePool
   WHERE TapePool.id = MigrationMount.tapePool
     AND MigrationMount.mountTransactionId = inMountTransactionId;
END;
/


/* insert new Migration Mount */
CREATE OR REPLACE PROCEDURE insertMigrationMount(inTapePoolId IN NUMBER,
                                                 outMountId OUT INTEGER) AS
  varMigJobId INTEGER;
BEGIN
  -- Check that the mount would be honoured by running a dry-run file selection.
  -- This is almost a duplicate of the query in tg_getFilesToMigrate.
  SELECT /*+ FIRST_ROWS_1
             LEADING(MigrationJob CastorFile DiskCopy FileSystem DiskServer)
             USE_NL(MMigrationJob CastorFile DiskCopy FileSystem DiskServer)
             INDEX(CastorFile PK_CastorFile_Id)
             INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile)
             INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusId) */
         MigrationJob.id mjId INTO varMigJobId
    FROM MigrationJob, DiskCopy, FileSystem, DiskServer, CastorFile
   WHERE MigrationJob.tapePool = inTapePoolId
     AND MigrationJob.status = tconst.MIGRATIONJOB_PENDING
     AND CastorFile.id = MigrationJob.castorFile
     AND CastorFile.id = DiskCopy.castorFile
     AND DiskCopy.status = dconst.DISKCOPY_CANBEMIGR
     AND FileSystem.id = DiskCopy.fileSystem
     AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
     AND DiskServer.id = FileSystem.diskServer
     AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
     AND ROWNUM < 2;
  -- The select worked out, create a mount for this tape pool
  INSERT INTO MigrationMount
              (mountTransactionId, id, startTime, VID, label, density,
               lastFseq, lastVDQMPingTime, tapePool, status)
    VALUES (NULL, ids_seq.nextval, gettime(), NULL, NULL, NULL,
            NULL, 0, inTapePoolId, tconst.MIGRATIONMOUNT_WAITTAPE)
    RETURNING id INTO outMountId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- No valid candidate found: this could happen e.g. when candidates exist
  -- but reside on non-available hardware. In this case we drop the mount and log
  outMountId := 0;
END;
/


/* DB job to start new migration mounts */
CREATE OR REPLACE PROCEDURE startMigrationMounts AS
  varNbPreExistingMounts INTEGER;
  varTotalNbMounts INTEGER := 0;
  varDataAmount INTEGER;
  varNbFiles INTEGER;
  varOldestCreationTime NUMBER;
  varMountId INTEGER;
BEGIN
  -- loop through tapepools
  FOR t IN (SELECT id, name, nbDrives, minAmountDataForMount,
                   minNbFilesForMount, maxFileAgeBeforeMount
              FROM TapePool) LOOP
    -- get number of mounts already running for this tapepool
    SELECT nvl(count(*), 0) INTO varNbPreExistingMounts
      FROM MigrationMount
     WHERE tapePool = t.id;
    varTotalNbMounts := varNbPreExistingMounts;
    -- get the amount of data and number of files to migrate, plus the age of the oldest file
    SELECT nvl(SUM(fileSize), 0), COUNT(*), nvl(MIN(creationTime), 0)
      INTO varDataAmount, varNbFiles, varOldestCreationTime
      FROM MigrationJob
     WHERE tapePool = t.id
       AND status = tconst.MIGRATIONJOB_PENDING;
    -- Create as many mounts as needed according to amount of data and number of files
    WHILE (varTotalNbMounts < t.nbDrives) AND
          ((varDataAmount/(varTotalNbMounts+1) >= t.minAmountDataForMount) OR
           (varNbFiles/(varTotalNbMounts+1) >= t.minNbFilesForMount)) AND
          (varTotalNbMounts+1 <= varNbFiles) LOOP   -- in case minAmountDataForMount << avgFileSize, stop creating more than one mount per file
      insertMigrationMount(t.id, varMountId);
      varTotalNbMounts := varTotalNbMounts + 1;
      IF varMountId = 0 THEN
        -- log "startMigrationMounts: failed migration mount creation due to lack of files"
        logToDLF(NULL, dlf.LVL_WARNING, dlf.MIGMOUNT_NO_FILE, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(varOldestCreationTime));
        -- no need to continue as we could not find a single file to migrate
        EXIT;
      ELSE
        -- log "startMigrationMounts: created new migration mount"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NEW_MOUNT, 0, '', 'tapegatewayd',
                 'mountId=' || TO_CHAR(varMountId) ||
                 ' tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(varOldestCreationTime));
      END IF;
    END LOOP;
    -- force creation of a unique mount in case no mount was created at all and some files are too old
    IF varNbFiles > 0 AND varTotalNbMounts = 0 AND t.nbDrives > 0 AND
       gettime() - varOldestCreationTime > t.maxFileAgeBeforeMount THEN
      insertMigrationMount(t.id, varMountId);
      IF varMountId = 0 THEN
        -- log "startMigrationMounts: failed migration mount creation due to lack of files"
        logToDLF(NULL, dlf.LVL_WARNING, dlf.MIGMOUNT_AGE_NO_FILE, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(varOldestCreationTime));
      ELSE
        -- log "startMigrationMounts: created new migration mount based on age"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NEW_MOUNT_AGE, 0, '', 'tapegatewayd',
                 'mountId=' || TO_CHAR(varMountId) ||
                 ' tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(varOldestCreationTime));
      END IF;
    ELSE
      IF varTotalNbMounts = varNbPreExistingMounts THEN 
        -- log "startMigrationMounts: no need for new migration mount"
        logToDLF(NULL, dlf.LVL_DEBUG, dlf.MIGMOUNT_NOACTION, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(nvl(varDataAmount,0)) ||
                 ' nbFilesInQueue=' || TO_CHAR(nvl(varNbFiles,0)) ||
                 ' oldestCreationTime=' || TO_CHAR(nvl(varOldestCreationTime,0)));
      END IF;
    END IF;
    COMMIT;
  END LOOP;
END;
/

/* DB job to start new recall mounts */
CREATE OR REPLACE PROCEDURE startRecallMounts AS
   varNbMounts INTEGER;
   varNbExtraMounts INTEGER := 0;
BEGIN
  -- loop through RecallGroups
  FOR rg IN (SELECT id, name, nbDrives, minAmountDataForMount,
                    minNbFilesForMount, maxFileAgeBeforeMount
               FROM RecallGroup
              ORDER BY vdqmPriority DESC) LOOP
    -- get number of mounts already running for this recallGroup
    SELECT COUNT(*) INTO varNbMounts
      FROM RecallMount
     WHERE recallGroup = rg.id;
    -- check whether some tapes should be mounted
    IF varNbMounts < rg.nbDrives THEN
      -- loop over the best candidates
      FOR tape IN (SELECT vid, SUM(fileSize) dataAmount, COUNT(*) nbFiles, gettime() - MIN(creationTime) maxAge
                     FROM RecallJob
                    WHERE recallGroup = rg.id
                      AND status = tconst.RECALLJOB_NEW
                    GROUP BY vid
                   HAVING (SUM(fileSize) >= rg.minAmountDataForMount OR
                           COUNT(*) >= rg.minNbFilesForMount OR
                           gettime() - MIN(creationTime) > rg.maxFileAgeBeforeMount)
                      AND VID NOT IN (SELECT vid FROM RecallMount)
                    ORDER BY MIN(creationTime)) LOOP
        -- if we created enough, stop
        IF varNbMounts + varNbExtraMounts = rg.nbDrives THEN EXIT; END IF;
        -- else trigger a new mount
        INSERT INTO RecallMount (id, VID, recallGroup, startTime, status)
        VALUES (ids_seq.nextval, tape.vid, rg.id, gettime(), tconst.RECALLMOUNT_NEW);
        varNbExtraMounts := varNbExtraMounts + 1;
        -- mark all recallJobs of concerned files PENDING
        UPDATE RecallJob
           SET status = tconst.RECALLJOB_PENDING
         WHERE status = tconst.RECALLJOB_NEW
           AND castorFile IN (SELECT UNIQUE castorFile
                                FROM RecallJob
                               WHERE VID = tape.vid);
        -- log "startRecallMounts: created new recall mount"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECMOUNT_NEW_MOUNT, 0, '', 'tapegatewayd',
                 'recallGroup=' || rg.name ||
                 ' tape=' || tape.vid ||
                 ' nbExistingMounts=' || TO_CHAR(varNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(tape.dataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(tape.nbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(tape.maxAge));
      END LOOP;
      IF varNbExtraMounts = 0 THEN
        -- log "startRecallMounts: no candidate found for a mount"
        logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECMOUNT_NOACTION_NOCAND, 0, '',
                 'tapegatewayd', 'recallGroup=' || rg.name);
      END IF;
    ELSE
      -- log "startRecallMounts: not allowed to start new recall mount. Maximum nb of drives has been reached"
      logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECMOUNT_NOACTION_NODRIVE, 0, '',
               'tapegatewayd', 'recallGroup=' || rg.name);
    END IF;
    COMMIT;
  END LOOP;
END;
/

/*** Disk-Tape interface, Migration ***/

/* Get next candidates for a given migration mount.
 * input:  VDQM transaction id, count and total size
 * output: outVid    the target VID,
           outFiles  a cursor for the set of migration candidates. 
 * Locks are taken on the selected migration jobs.
 *
 * We should only propose a migration job for a file that does not
 * already have another copy migrated to the same tape.
 * The already migrated copies are kept in MigratedSegment until the whole set
 * of siblings has been migrated.
 */
CREATE OR REPLACE PROCEDURE tg_getBulkFilesToMigrate(inLogContext IN VARCHAR2,
                                                     inMountTrId IN NUMBER,
                                                     inCount IN INTEGER,
                                                     inTotalSize IN INTEGER,
                                                     outFiles OUT castorTape.FileToMigrateCore_cur) AS
  varMountId NUMBER;
  varCount INTEGER;
  varTotalSize INTEGER;
  varVid VARCHAR2(10);
  varNewFseq INTEGER;
  varFileTrId NUMBER;
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
BEGIN
  BEGIN
    -- Get id, VID and last valid fseq for this migration mount, lock
    SELECT id, vid, lastFSeq INTO varMountId, varVid, varNewFseq
      FROM MigrationMount
     WHERE mountTransactionId = inMountTrId
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- migration mount is over or unknown request: return an empty cursor
    OPEN outFiles FOR
      SELECT fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fseq, fileSize
        FROM FilesToMigrateHelper;
    RETURN;
  END;
  varCount := 0;
  varTotalSize := 0;
  -- Get candidates up to inCount or inTotalSize
  FOR Cand IN (
    SELECT /*+ FIRST_ROWS(100)
               LEADING(MigrationMount MigrationJob CastorFile DiskCopy FileSystem DiskServer)
               USE_NL(MigrationMount MigrationJob CastorFile DiskCopy FileSystem DiskServer)
               INDEX(CastorFile PK_CastorFile_Id)
               INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile)
               INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusId) */
           MigrationJob.id mjId, DiskServer.name || ':' || FileSystem.mountPoint || DiskCopy.path filePath,
           CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, CastorFile.lastKnownFileName
      FROM MigrationMount, MigrationJob, CastorFile, DiskCopy, FileSystem, DiskServer
     WHERE MigrationMount.id = varMountId
       AND MigrationJob.tapePool = MigrationMount.tapePool
       AND MigrationJob.status = tconst.MIGRATIONJOB_PENDING
       AND CastorFile.id = MigrationJob.castorFile
       AND CastorFile.id = DiskCopy.castorFile
       AND DiskCopy.status = dconst.DISKCOPY_CANBEMIGR
       AND FileSystem.id = DiskCopy.fileSystem
       AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
       AND DiskServer.id = FileSystem.diskServer
       AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
       AND (MigrationMount.VID NOT IN (SELECT /*+ INDEX_RS_ASC(MigratedSegment I_MigratedSegment_CFCopyNBVID) */ vid
                                         FROM MigratedSegment
                                        WHERE castorFile = MigrationJob.castorfile
                                          AND copyNb != MigrationJob.destCopyNb))
       FOR UPDATE OF MigrationJob.id SKIP LOCKED)
  LOOP
    BEGIN
      -- Try to take this candidate on this mount
      INSERT INTO FilesToMigrateHelper (fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fileSize, fseq)
        VALUES (Cand.fileId, Cand.nsHost, Cand.lastKnownFileName, Cand.filePath, ids_seq.NEXTVAL, Cand.fileSize, varNewFseq)
        RETURNING fileTransactionId INTO varFileTrId;
    EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- If we fail here, it means that another copy of this file was already selected for this mount.
      -- Not a big deal, we skip this candidate and keep going.
      CONTINUE;
    END;
    varCount := varCount + 1;
    varTotalSize := varTotalSize + Cand.fileSize;
    UPDATE MigrationJob
       SET status = tconst.MIGRATIONJOB_SELECTED,
           vid = varVid,
           fSeq = varNewFseq,
           mountTransactionId = inMountTrId,
           fileTransactionId = varFileTrId
     WHERE id = Cand.mjId;
    varNewFseq := varNewFseq + 1;    -- we still decide the fseq for each migration candidate
    IF varCount >= inCount OR varTotalSize >= inTotalSize THEN
      -- we have enough candidates for this round, exit loop
      EXIT;
    END IF;
  END LOOP;
  -- Update last fseq
  UPDATE MigrationMount
     SET lastFseq = varNewFseq
   WHERE id = varMountId;
  -- Return all candidates (potentially an empty cursor). Don't commit now, this will be done
  -- in C++ after the results have been collected as the temporary table will be emptied. 
  OPEN outFiles FOR
    SELECT fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fseq, fileSize
      FROM FilesToMigrateHelper;
END;
/

/* Wrapper procedure for the setOrReplaceSegmentsForFiles call in the NS DB. Because we can't
 * pass arrays, and temporary tables are forbidden with distributed transactions, we use standard
 * tables on the Stager DB (while the NS table is still temporary) to pass the data
 * and we wrap everything in an autonomous transaction to isolate the caller.
 */
CREATE OR REPLACE PROCEDURE ns_setOrReplaceSegments(inReqId IN VARCHAR2,
                                                    outNSTimeInfos OUT floatList,
                                                    outNSErrorCodes OUT "numList",
                                                    outNSMsgs OUT strListTable,
                                                    outNSFileIds OUT "numList",
                                                    outNSParams OUT strListTable) AS
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  -- "Bulk" transfer data to the NS DB
  INSERT /*+ APPEND */ INTO SetSegmentsForFilesHelper@RemoteNS
    SELECT * FROM FileMigrationResultsHelper
     WHERE reqId = inReqId;
  DELETE FROM FileMigrationResultsHelper
   WHERE reqId = inReqId;
  -- This call autocommits all segments in the NameServer
  setOrReplaceSegmentsForFiles@RemoteNS(inReqId);
  -- Retrieve results from the NS DB in bulk and clean data
  SELECT timeinfo, ec, msg, fileId, params
    BULK COLLECT INTO outNSTimeInfos, outNSErrorCodes, outNSMsgs, outNSFileIds, outNSParams
    FROM ResultsLogHelper@RemoteNS
   WHERE reqId = inReqId;
  DELETE FROM ResultsLogHelper@RemoteNS
   WHERE reqId = inReqId;
  -- this commits the remote transaction
  COMMIT;
END;
/

/* Commit a set of succeeded/failed migration processes to the NS and stager db.
 * Locks are taken on the involved castorfiles one by one, then to the dependent entities.
 */
CREATE OR REPLACE PROCEDURE tg_setBulkFileMigrationResult(inLogContext IN VARCHAR2,
                                                          inMountTrId IN NUMBER,
                                                          inFileIds IN "numList",
                                                          inFileTrIds IN "numList",
                                                          inFseqs IN "numList",
                                                          inBlockIds IN strListTable,
                                                          inChecksumTypes IN strListTable,
                                                          inChecksums IN "numList",
                                                          inComprSizes IN "numList",
                                                          inTransferredSizes IN "numList",
                                                          inErrorCodes IN "numList",
                                                          inErrorMsgs IN strListTable
                                                          ) AS
  varStartTime TIMESTAMP;
  varNsHost VARCHAR2(2048);
  varReqId VARCHAR2(36);
  varLastUpdTime NUMBER;
  varCopyNo NUMBER;
  varOldCopyNo NUMBER;
  varVid VARCHAR2(10);
  varNSTimeInfos floatList;
  varNSErrorCodes "numList";
  varNSMsgs strListTable;
  varNSFileIds "numList";
  varNSParams strListTable;
  varParams VARCHAR2(4000);
BEGIN
  varStartTime := SYSTIMESTAMP;
  varReqId := uuidGen();
  -- Get the NS host name
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Prepare input for nameserver
  FOR i IN inFileTrIds.FIRST .. inFileTrIds.LAST LOOP
    IF inErrorCodes(i) = 0 THEN
      -- Successful migration, collect additional data.
      -- Note that this is NOT bulk to preserve the order in the input arrays.
      BEGIN
        SELECT CF.lastUpdateTime, nvl(MJ.originalCopyNb, 0), MJ.vid, MJ.destCopyNb
          INTO varLastUpdTime, varOldCopyNo, varVid, varCopyNo
          FROM CastorFile CF, MigrationJob MJ
         WHERE MJ.castorFile = CF.id
           AND CF.fileid = inFileIds(i)
           AND MJ.mountTransactionId = inMountTrId
           AND MJ.fileTransactionId = inFileTrIds(i)
           AND status = tconst.MIGRATIONJOB_SELECTED;
        -- Store in a temporary table, to be transfered to the NS DB.
        -- Note that this is an ON COMMIT DELETE table and we never take locks or commit until
        -- after the NS call: if anything goes bad (including the db link being broken) we bail out
        -- without needing to rollback.
        INSERT INTO FileMigrationResultsHelper
          (reqId, fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
           vid, fseq, blockId, checksumType, checksum)
        VALUES (varReqId, inFileIds(i), varLastUpdTime, varCopyNo, varOldCopyNo,
                inTransferredSizes(i), inComprSizes(i), varVid, inFseqs(i),
                strtoRaw4(inBlockIds(i)), inChecksumTypes(i), inChecksums(i));
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- Log 'unable to identify migration, giving up'
        varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' fileTransactionId='|| to_char(inFileTrIds(i))
          ||' '|| inLogContext;
        logToDLF(varReqid, dlf.LVL_ERROR, dlf.MIGRATION_NOT_FOUND, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
      END;
    ELSE
      -- Fail/retry this migration, log 'migration failed, will retry if allowed'
      varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' ErrorCode='|| to_char(inErrorCodes(i))
                   ||' ErrorMessage="'|| inErrorMsgs(i) ||'" VID='|| varVid
                   ||' copyNb='|| to_char(varCopyNo) ||' '|| inLogContext;
      logToDLF(varReqid, dlf.LVL_WARNING, dlf.MIGRATION_RETRY, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
      retryOrFailMigration(inMountTrId, inFileIds(i), varNsHost, inErrorCodes(i), varReqId);
      COMMIT;
    END IF;
  END LOOP;
  -- Commit all the entries in FileMigrationResultsHelper so that the next call can take them
  COMMIT;

  DECLARE
    varUnused INTEGER;
  BEGIN
    -- boundary case: if nothing to do, just skip the remote call and the
    -- subsequent FOR loop as it would fail.
    SELECT 1 INTO varUnused FROM FileMigrationResultsHelper
     WHERE reqId = varReqId AND ROWNUM < 2;
    -- The following procedure wraps the remote calls in an autonomous transaction
    ns_setOrReplaceSegments(varReqId, varNSTimeInfos, varNSErrorCodes, varNSMsgs, varNSFileIds, varNSParams);

    -- Process the results
    FOR i IN varNSFileIds.FIRST .. varNSFileIds.LAST LOOP
      -- First log on behalf of the NS
      
      INSERT INTO DLFLogs (timeinfo, uuid, priority, msg, fileId, nsHost, source, params)
        VALUES (varNSTimeinfos(i), varReqid,
                CASE WHEN varNSErrorCodes(i) IN (0,-1) THEN dlf.LVL_SYSTEM ELSE dlf.LVL_ERROR END,
                varNSMsgs(i), varNSFileIds(i), varNsHost, 'nsd', varNSParams(i));
      
      -- Now process file by file, depending on the result. Skip pure log entries with error code = -1.
      IF varNSErrorCodes(i) = -1 THEN CONTINUE; END IF;
      CASE
      WHEN varNSErrorCodes(i) = 0 THEN
        -- All right, commit the migration in the stager
        tg_setFileMigrated(inMountTrId, varNSFileIds(i), varReqId, inLogContext);

      WHEN varNSErrorCodes(i) = serrno.ENOENT
        OR varNSErrorCodes(i) = serrno.ENSFILECHG
        OR varNSErrorCodes(i) = serrno.ENSTOOMANYSEGS THEN
        -- The migration was useless because either the file is gone, or it has been modified elsewhere,
        -- or there were already enough copies on tape for it. Fail and update disk cache accordingly.
        failFileMigration(inMountTrId, varNSFileIds(i), varNSErrorCodes(i), varReqId);

      ELSE
        -- Attempt to retry for all other NS errors. To be reviewed whether some of the NS errors are to be considered fatal.
        varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' ErrorCode='|| to_char(varNSErrorCodes(i))
                     ||' ErrorMessage="'|| varNSMsgs(i) ||'" '|| inLogContext;
        logToDLF(varReqid, dlf.LVL_WARNING, dlf.MIGRATION_RETRY, varNSFileIds(i), varNsHost, 'tapegatewayd', varParams);
        retryOrFailMigration(inMountTrId, varNSFileIds(i), varNsHost, varNSErrorCodes(i), varReqId);
      END CASE;
      -- Commit file by file
      COMMIT;
    END LOOP;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Nothing to do after processing the error cases
    NULL;
  END;
  -- Final log
  varParams := 'mountTransactionId='|| to_char(inMountTrId)
               ||' NbFiles='|| inFileIds.COUNT ||' '|| inLogContext
               ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
               ||' AvgProcessingTime='|| getSecs(varStartTime, SYSTIMESTAMP)/inFileIds.COUNT;
  logToDLF(varReqid, dlf.LVL_SYSTEM, dlf.BULK_MIGRATION_COMPLETED, 0, '', 'tapegatewayd', varParams);
END;
/

/* Commit a successful file migration */
CREATE OR REPLACE PROCEDURE tg_setFileMigrated(inMountTrId IN NUMBER, inFileId IN NUMBER,
                                               inReqId IN VARCHAR2, inLogContext IN VARCHAR2) AS
  varNsHost VARCHAR2(2048);
  varCfId NUMBER;
  varCopyNb INTEGER;
  varVID VARCHAR2(10);
  varOrigVID VARCHAR2(10);
  varMigJobCount INTEGER;
  varParams VARCHAR2(4000);
BEGIN
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Lock the CastorFile
  SELECT CF.id INTO varCfId FROM CastorFile CF
   WHERE CF.fileid = inFileId 
     AND CF.nsHost = varNsHost
     FOR UPDATE;
  -- delete the corresponding migration job, create the new migrated segment
  DELETE FROM MigrationJob
   WHERE castorFile = varCfId
     AND mountTransactionId = inMountTrId
  RETURNING destCopyNb, VID, originalVID
    INTO varCopyNb, varVID, varOrigVID;
  -- check if another migration should be performed
  SELECT count(*) INTO varMigJobCount
    FROM MigrationJob
   WHERE castorFile = varCfId;
  IF varMigJobCount = 0 THEN
    -- no more migrations, delete all migrated segments 
    DELETE FROM MigratedSegment
     WHERE castorFile = varCfId;
  ELSE
    -- another migration ongoing, keep track of the one just completed
    INSERT INTO MigratedSegment VALUES (varCfId, varCopyNb, varVID);
  END IF;
  -- Tell the Disk Cache that migration is over
  dc_setFileMigrated(varCfId, varOrigVID, (varMigJobCount = 0));
  -- Log 'db updates after full migration completed'
  varParams := 'TPVID='|| varVID ||' mountTransactionId='|| to_char(inMountTrId) ||' '|| inLogContext;
  logToDLF(inReqid, dlf.LVL_SYSTEM, dlf.MIGRATION_COMPLETED, inFileId, varNsHost, 'tapegatewayd', varParams);
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Log 'file not found, giving up'
  varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' '|| inLogContext;
  logToDLF(inReqid, dlf.LVL_ERROR, dlf.MIGRATION_NOT_FOUND, inFileId, varNsHost, 'tapegatewayd', varParams);
END;
/

/* update the db after a successful migration - disk side */
CREATE OR REPLACE PROCEDURE dc_setFileMigrated(
  inCfId          IN NUMBER,
  inOriginVID     IN VARCHAR2,     -- NOT NULL only for repacked files
  inLastMigration IN BOOLEAN) AS   -- whether this is the last copy on tape
  varSrId NUMBER;
BEGIN
  IF inLastMigration THEN
     -- Mark all disk copies as staged
     UPDATE DiskCopy
        SET status= dconst.DISKCOPY_STAGED
      WHERE castorFile = inCfId
        AND status= dconst.DISKCOPY_CANBEMIGR;
  END IF;
  IF inOriginVID IS NOT NULL THEN
    -- archive the repack subrequest associated to this VID (there can only be one)
    SELECT /*+ INDEX(SR I_Subrequest_CastorFile) */ SR.id INTO varSrId
      FROM SubRequest SR, StageRepackRequest Req
      WHERE SR.castorfile = inCfId
        AND SR.status = dconst.SUBREQUEST_REPACK
        AND SR.request = Req.id
        AND Req.RepackVID = inOriginVID;
    archiveSubReq(varSrId, dconst.SUBREQUEST_FINISHED);
  END IF;
END;
/

/* Fail a file migration, potentially archiving outstanding repack requests */
CREATE OR REPLACE PROCEDURE failFileMigration(inMountTrId IN NUMBER, inFileId IN NUMBER,
                                              inErrorCode IN INTEGER, inReqId IN VARCHAR2) AS
  varNsHost VARCHAR2(2048);
  varCfId NUMBER;
  varCFLastUpdateTime NUMBER;
  varSrIds "numList";
  varOriginalCopyNb NUMBER;
  varMigJobCount NUMBER;
BEGIN
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Lock castor file
  SELECT id, lastUpdateTime INTO varCfId, varCFLastUpdateTime FROM CastorFile WHERE fileId = inFileId FOR UPDATE;
  -- delete migration job
  DELETE FROM MigrationJob 
   WHERE castorFile = varCFId AND mountTransactionId = inMountTrId
  RETURNING originalCopyNb INTO varOriginalCopyNb;
  -- check if another migration should be performed
  SELECT count(*) INTO varMigJobCount
    FROM MigrationJob
   WHERE castorfile = varCfId;
  IF varMigJobCount = 0 THEN
     -- no other migration, delete all migrated segments
     DELETE FROM MigratedSegment
      WHERE castorfile = varCfId;
  END IF;
  -- fail repack subrequests
  IF varOriginalCopyNb IS NOT NULL THEN
    -- find and archive the repack subrequest(s)
    SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile) */
           SubRequest.id BULK COLLECT INTO varSrIds
      FROM SubRequest
     WHERE SubRequest.castorfile = varCfId
       AND subrequest.status = dconst.SUBREQUEST_REPACK;
    FOR i IN varSrIds.FIRST .. varSrIds.LAST LOOP
      archiveSubReq(varSrIds(i), dconst.SUBREQUEST_FAILED_FINISHED);
    END LOOP;
    -- set back the diskcopies to STAGED otherwise repack will wait forever
    UPDATE DiskCopy
       SET status = dconst.DISKCOPY_STAGED
     WHERE castorfile = varCfId
       AND status = dconst.DISKCOPY_CANBEMIGR;
  END IF;
  
  -- Log depending on the error: some are not pathological and have dedicated handling
  IF inErrorCode = serrno.ENOENT OR inErrorCode = serrno.ENSFILECHG THEN
    -- in this case, disk cache is stale
    UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID
     WHERE status = dconst.DISKCOPY_CANBEMIGR
       AND castorFile = varCfId;
    -- Log 'file was dropped or modified during migration, giving up'
    logToDLF(inReqid, dlf.LVL_NOTICE, dlf.MIGRATION_FILE_DROPPED, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || to_char(inMountTrId) || ' ErrorCode=' || to_char(inErrorCode)
             ||' lastUpdateTime=' || to_char(varCFLastUpdateTime));
  ELSIF inErrorCode = serrno.ENSTOOMANYSEGS THEN
    -- do as if migration was successful
    UPDATE DiskCopy SET status = dconst.DISKCOPY_STAGED
     WHERE status = dconst.DISKCOPY_CANBEMIGR
       AND castorFile = varCfId;
    -- Log 'file already had enough copies on tape, ignoring new segment'
    logToDLF(inReqid, dlf.LVL_NOTICE, dlf.MIGRATION_SUPERFLUOUS_COPY, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || to_char(inMountTrId));
  ELSE
    -- Any other case, log 'migration to tape failed for this file, giving up'
    logToDLF(inReqid, dlf.LVL_ERROR, dlf.MIGRATION_FAILED, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || to_char(inMountTrId) || ' LastErrorCode=' || to_char(inErrorCode));
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- File was dropped, log 'file not found when failing migration'
  logToDLF(inReqid, dlf.LVL_ERROR, dlf.MIGRATION_FAILED_NOT_FOUND, inFileId, varNsHost, 'tapegatewayd',
           'mountTransactionId=' || to_char(inMountTrId) || ' LastErrorCode=' || to_char(inErrorCode));
END;
/


/*** Disk-Tape interface, Recall ***/

/* Get next candidates for a given recall mount.
 * input:  VDQM transaction id, count and total size
 * output: outFiles, a cursor for the set of recall candidates.
 * A lock is taken on the selected recall mount.
 */
CREATE OR REPLACE PROCEDURE tg_getBulkFilesToRecall(inLogContext IN VARCHAR2,
                                                    inMountTrId IN NUMBER,
                                                    inCount IN INTEGER,
                                                    inTotalSize IN INTEGER,
                                                    outFiles OUT castorTape.FileToRecallCore_cur) AS
  varVid VARCHAR2(10);
  varPreviousFseq INTEGER;
  varCount INTEGER;
  varTotalSize INTEGER;
  varPath VARCHAR2(2048);
  varFileTrId INTEGER;
  varNewFseq INTEGER;
  bestFSForRecall_error EXCEPTION;
  PRAGMA EXCEPTION_INIT(bestFSForRecall_error, -20115);
BEGIN
  BEGIN
    -- Get VID and last processed fseq for this recall mount, lock
    SELECT vid, lastProcessedFseq INTO varVid, varPreviousFseq
      FROM RecallMount
     WHERE mountTransactionId = inMountTrId
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- recall is over or unknown request: return an empty cursor
    OPEN outFiles FOR
      SELECT fileId, nsHost, fileTransactionId, filePath, blockId, fseq
        FROM FilesToRecallHelper;
    RETURN;
  END;
  varCount := 0;
  varTotalSize := 0;
  varNewFseq := varPreviousFseq;
  -- Get candidates up to inCount or inTotalSize
  -- Select only the ones further down the tape (fseq > current one)
  <<candidateLoop>>
  FOR Cand IN (
    -- Find the unprocessed recallJobs of this tape with lowest fSeq
    -- that is above the previous one. If none, take the recallJob with lowest fseq.
    SELECT RecallJob.id AS rjId, fSeq, blockId, RecallJob.fileSize, castorFile AS cfId, fileId, nsHost
      FROM RecallJob, CastorFile
     WHERE vid = varVid
       AND status = tconst.RECALLJOB_PENDING
       AND RecallJob.castorFile = CastorFile.id
       AND fseq > varNewFseq
     ORDER BY fseq ASC
       FOR UPDATE OF RecallJob.id SKIP LOCKED)
  LOOP
    BEGIN
      -- Find the best filesystem to recall the selected file
      bestFileSystemForRecall(Cand.cfId, varPath);
      varCount := varCount + 1;
      varTotalSize := varTotalSize + Cand.fileSize;
      varNewFseq := Cand.fseq;
      INSERT INTO FilesToRecallHelper (fileId, nsHost, fileTransactionId, filePath, blockId, fSeq)
        VALUES (Cand.fileId, Cand.nsHost, ids_seq.nextval, varPath, Cand.blockId, Cand.fSeq)
        RETURNING fileTransactionId INTO varFileTrId;
      -- update RecallJob
      UPDATE RecallJob
         SET status = tconst.RECALLJOB_SELECTED,
             fileTransactionID = varFileTrId
       WHERE id = Cand.rjId;
      IF varCount >= inCount OR varTotalSize >= inTotalSize THEN
        -- we have enough candidates for this round, exit loop
        EXIT;
      END IF;
    EXCEPTION WHEN bestFSForRecall_error OR NO_DATA_FOUND THEN
      -- log 'bestFileSystemForRecall could not find a suitable destination for this recall' and skip it
      -- XXX we should actually abort this recall + restart SRs
      logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_FS_NOT_FOUND, Cand.fileId, Cand.nsHost, 'tapegatewayd',
        'errorMessage="' || SQLERRM || '"');
    END;
  END LOOP;
  -- check whether we've done enough. If not, consider taking lower fseqs
  IF varNewFseq > -1 AND varCount < inCount AND varTotalSize < inTotalSize THEN
    varNewFseq := -1;
    GOTO candidateLoop;
  END IF;
  -- Record last fseq at the mount level
  UPDATE RecallMount
     SET lastProcessedFseq = varNewFseq
   WHERE vid = varVid;
  -- Return all candidates. Don't commit now, this will be done in C++
  -- after the results have been collected as the temporary table will be emptied.
  OPEN outFiles FOR
    SELECT fileId, nsHost, fileTransactionId, filePath, blockId, fseq
      FROM FilesToRecallHelper;
END;
/


/* Commit a set of succeeded/failed recall processes to the NS and stager db.
 * Locks are taken on the involved castorfiles one by one, then to the dependent entities.
 */
CREATE OR REPLACE PROCEDURE tg_setBulkFileRecallResult(inLogContext IN VARCHAR2,
                                                       inMountTrId IN NUMBER,
                                                       inFileIds IN "numList",
                                                       inFileTrIds IN "numList",
                                                       inFilePaths IN strListTable,
                                                       inFseqs IN "numList",
                                                       inChecksumNames IN strListTable,
                                                       inChecksums IN "numList",
                                                       inFileSizes IN "numList",
                                                       inErrorCodes IN "numList",
                                                       inErrorMsgs IN strListTable) AS
  varCfId NUMBER;
  varVID VARCHAR2(10);
  varReqId VARCHAR2(36);
  varStartTime TIMESTAMP;
  varNsHost VARCHAR2(2048);
  varParams VARCHAR2(4000);
BEGIN
  varStartTime := SYSTIMESTAMP;
  varReqId := uuidGen();
  -- Get the NS host name
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Get the current VID
  SELECT VID INTO varVID
    FROM RecallMount
   WHERE mountTransactionId = inMountTrId;
  -- Loop over the input
  FOR i IN inFileIds.FIRST .. inFileIds.LAST LOOP
    BEGIN
      -- Find and lock related castorFile
      SELECT id INTO varCfId
        FROM CastorFile
       WHERE fileid = inFileIds(i)
         AND nsHost = varNsHost
         FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- log "setFileRecalled : unable to identify Recall. giving up"
      logToDLF(varReqId, dlf.LVL_ERROR, dlf.RECALL_NOT_FOUND, inFileIds(i), varNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTrId) || ' TPVID=' || varVID ||
               ' fseq=' || TO_CHAR(inFseqs(i)) || ' filePath=' || inFilePaths(i) || ' ' || inLogContext);
      CONTINUE;
    END;
    -- Now deal with each recall one by one
    IF inErrorCodes(i) = 0 THEN
      -- Recall successful, check NS and update stager + log
      tg_setFileRecalled(inMountTrId, inFseqs(i), inFilePaths(i), inChecksumNames(i), inChecksums(i),
                         varReqId, inLogContext);
    ELSE
      -- Recall failed at tapeserver level, attempt to retry it
      -- log "setBulkFileRecallResult : recall process failed, will retry if allowed"
      logToDLF(varReqId, dlf.LVL_ERROR, dlf.RECALL_FAILED, inFileIds(i), varNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTrId) || ' TPVID=' || varVID ||
               ' fseq=' || TO_CHAR(inFseqs(i)) || ' errorMessage="' || inErrorMsgs(i) ||'" '|| inLogContext);
      retryOrFailRecall(varCfId, varVID);
    END IF;
    COMMIT;
  END LOOP;
  -- Final log
  varParams := 'mountTransactionId='|| to_char(inMountTrId)
               ||' NbFiles='|| inFileIds.COUNT ||' '|| inLogContext
               ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
               ||' AvgProcessingTime='|| getSecs(varStartTime, SYSTIMESTAMP)/inFileIds.COUNT;
  logToDLF(varReqid, dlf.LVL_SYSTEM, dlf.BULK_RECALL_COMPLETED, 0, '', 'tapegatewayd', varParams);
END;
/


/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('MIGRATIONMOUNTSJOB', 'RECALLMOUNTSJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every minute executing the startMigrationMounts procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'MigrationMountsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN startMigrationMounts(); END;'', ''tapegatewayd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Creates MigrationMount entries when new migrations should start');

  -- Create a db job to be run every minute executing the startRecallMounts procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'RecallMountsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN startRecallMounts(); END;'', ''tapegatewayd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Creates RecallMount entries when new recalls should start');
END;
/
