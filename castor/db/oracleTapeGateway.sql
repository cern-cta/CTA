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
       SET status = tconst.RECALLJOB_PENDING
     WHERE castorFile IN (SELECT castorFile
                            FROM RecallJob
                           WHERE VID = varVID
                             AND (status = tconst.RECALLJOB_SELECTED
                               OR status = tconst.RECALLJOB_RETRYMOUNT));
    DELETE FROM RecallMount WHERE vid = varVID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- reaching this point means that the tape session was already ended by somebody else
    -- This can typically be the VDQMChecker of the tapegateway. We log a warning and
    -- return ok, as there is nothing to do anyway
    logToDLF(NULL, dlf.LVL_NOTICE, 'endTapeSession: no recall or migration mount found',
             0, '', 'tapegatewayd', 'mountTransactionId=' || TO_CHAR(inMountTransactionId));
  END;
END;
/

/* update the db when a tape session is ended. This autonomous transaction wrapper
 * allow cleanup of leftover sessions when creating new sessions */
CREATE OR REPLACE PROCEDURE tg_endTapeSessionAT(inMountTransactionId IN NUMBER,
                                                inErrorCode IN INTEGER) AS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  tg_endTapeSession(inMountTransactionId, inErrorCode);
  COMMIT;
END;
/

/* find all migration mounts involving a set of tapes */
CREATE OR REPLACE PROCEDURE tg_getMigMountReqsForVids(inVids IN strListTable,
                                                      outBlockingSessions OUT SYS_REFCURSOR) AS
BEGIN
    OPEN  outBlockingSessions FOR
      SELECT vid TPVID, mountTransactionId VDQMREQID
        FROM MigrationMount
       WHERE vid IN (SELECT * FROM TABLE (inVids));
END;
/

/* PL/SQL method implementing bestFileSystemForRecall */
CREATE OR REPLACE PROCEDURE bestFileSystemForRecall(inCfId IN INTEGER, outFilePath OUT VARCHAR2) AS
  varCfId INTEGER;
  nb NUMBER;
BEGIN
  outFilePath := '';
  -- try and select a good FileSystem/DataPool for this recall
  FOR f IN (SELECT * FROM (
              SELECT /*+ INDEX_RS_ASC(RecallJob I_RecallJob_Castorfile_VID) */
                     DiskServer.name ||':'|| FileSystem.mountPoint AS remotePath, FileSystem.id,
                     FileSystem.diskserver, CastorFile.fileSize, CastorFile.fileId, CastorFile.nsHost,
                     1 isDiskPool
                FROM DiskServer, FileSystem, DiskPool2SvcClass, CastorFile, RecallJob
               WHERE CastorFile.id = inCfId
                 AND RecallJob.castorFile = inCfId
                 AND RecallJob.svcClass = DiskPool2SvcClass.child
                 AND FileSystem.diskPool = DiskPool2SvcClass.parent
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
                 AND DiskServer.hwOnline = 1
            ORDER BY -- order by filesystem load first: this works if the feedback loop is fast enough, that is
                     -- the transfer of the selected files in bulk does not take more than a couple of minutes
                     FileSystem.nbMigratorStreams + FileSystem.nbRecallerStreams ASC,
                     -- then use randomness for tie break
                     DBMS_Random.value)
             UNION ALL
            SELECT * FROM (
              -- take max 3 random diskservers in the data pools involved
              SELECT /*+ INDEX_RS_ASC(RecallJob I_RecallJob_Castorfile_VID) */
                     DiskServer.name ||':' AS remotePath, 0 as id,
                     DiskServer.id AS diskServer, CastorFile.fileSize,
                     CastorFile.fileId, CastorFile.nsHost, 0 isDiskPool
                FROM DiskServer, DataPool, DataPool2SvcClass, CastorFile, RecallJob
               WHERE CastorFile.id = inCfId
                 AND RecallJob.castorFile = inCfId
                 AND RecallJob.svcClass = DataPool2SvcClass.child
                 AND DiskServer.dataPool = DataPool2SvcClass.parent
                 -- a priori, we want to have enough free space. However, if we don't, we accept to start writing
                 -- if we have a minimum of 30GB free and count on gerbage collection to liberate space while writing
                 -- We still check that the file fit on the disk, and actually keep a 30% margin so that very recent
                 -- files can be kept
                 AND (DataPool.free - DataPool.minAllowedFreeSpace * DataPool.totalSize > CastorFile.fileSize
                   OR (DataPool.free - DataPool.minAllowedFreeSpace * DataPool.totalSize > 30000000000
                   AND DataPool.totalSize * 0.7 > CastorFile.fileSize))
                 AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
                 AND DiskServer.hwOnline = 1
               ORDER BY DBMS_Random.value)
             WHERE ROWNUM < 4)
  LOOP
    buildPathFromFileId(f.fileId, f.nsHost, ids_seq.nextval, outFilePath, f.isDiskPool = 1);
    outFilePath := f.remotePath || outFilePath;
    -- In case we selected a filesystem (and no datapool)
    IF f.id > 0 THEN
      -- Check that we don't already have a copy of this file on the selected filesystem.
      -- This will never happen in normal operations but may be the case if a filesystem
      -- was disabled and did come back while the tape recall was waiting.
      -- Even if we optimize by cancelling remaining unneeded tape recalls when a
      -- fileSystem comes back, the ones running at the time of the come back will have
      -- the problem.
      SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */ count(*) INTO nb
        FROM DiskCopy
       WHERE fileSystem = f.id
         AND castorfile = inCfid
         AND status = dconst.DISKCOPY_VALID;
      IF nb != 0 THEN
        raise_application_error(-20115, 'Recaller could not find a FileSystem in production in the requested SvcClass and without copies of this file');
      END IF;
      RETURN;
    END IF;
  END LOOP;
  IF outFilePath IS NULL THEN
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

/* resets a CastorFile, its diskcopies and recall/migrationJobs when it
 * was overwritten in the namespace. This includes :
 *    - updating the CastorFile with the new NS data
 *    - mark current DiskCopies for GC
 *    - restart any pending recalls
 *    - discard any pending migrations
 * XXXX This is a preliminary version of this function that is used only
 * XXXX in the context of overwritten files during recalls. It has to be
 * XXXX completed and tested before any other usage. In particular, is
 * XXXX does not handle the Disk2DiskCopy case
 */
CREATE OR REPLACE PROCEDURE resetOverwrittenCastorFile(inCfId INTEGER,
                                                       inNewOpenTime NUMBER,
                                                       inNewSize INTEGER) AS
BEGIN
  -- update the Castorfile
  UPDATE CastorFile
     SET nsOpenTime = inNewOpenTime,
         fileSize = inNewSize,
         lastAccessTime = getTime()
   WHERE id = inCfId;
  -- cancel ongoing recalls, if any
  deleteRecallJobs(inCfId);
  -- cancel ongoing migrations, if any
  deleteMigrationJobs(inCfId);
  -- invalidate existing DiskCopies, if any
  UPDATE DiskCopy
     SET status = dconst.DISKCOPY_INVALID,
         gcType = dconst.GCTYPE_OVERWRITTEN
   WHERE castorFile = inCfId
     AND status = dconst.DISKCOPY_VALID;
  -- restart ongoing requests
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_RESTART
   WHERE castorFile = inCfId
     AND status = dconst.SUBREQUEST_WAITTAPERECALL;
END;
/

/* Checks whether repack requests are ongoing for a file and archives them depending on the
 * provided error code.
 * Can be called because of Nameserver errors after recalls or after migrations
 * (cf. failFileMigration and checkRecallInNS).
 */
CREATE OR REPLACE PROCEDURE archiveOrFailRepackSubreq(inCfId INTEGER, inErrorCode INTEGER) AS
  varSrIds "numList";
BEGIN
  -- find and archive any repack subrequest(s)
  SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile) */
         SubRequest.id BULK COLLECT INTO varSrIds
    FROM SubRequest
   WHERE SubRequest.castorfile = inCfId
     AND subrequest.reqType = 119;  -- OBJ_StageRepackRequest
  FOR i IN 1 .. varSrIds.COUNT LOOP
    -- archive: ENOENT and ENSFILECHG are considered as non-errors in a Repack context (#97529)
    archiveSubReq(varSrIds(i), CASE WHEN inErrorCode IN (serrno.ENOENT, serrno.ENSFILECHG)
      THEN dconst.SUBREQUEST_FINISHED ELSE dconst.SUBREQUEST_FAILED_FINISHED END);
    -- for error reporting
    UPDATE SubRequest
       SET errorCode = inErrorCode,
           errorMessage = CASE
             WHEN inErrorCode IN (serrno.ENOENT, serrno.ENSFILECHG) THEN
               ''
             WHEN inErrorCode = serrno.ENSNOSEG THEN
               'Segment was dropped during repack, skipping'
             WHEN inErrorCode = serrno.ENSTOOMANYSEGS THEN
               'File has too many segments on tape, skipping'
             ELSE
               'Migration failed, reached maximum number of retries'
             END
     WHERE id = varSrIds(i);
  END LOOP;
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
                                           inLastOpenTime IN NUMBER,
                                           inReqId IN VARCHAR2,
                                           inLogContext IN VARCHAR2) RETURN BOOLEAN AS
  varNSOpenTime NUMBER;
  varNSSize INTEGER;
  varNSCsumtype VARCHAR2(2048);
  varNSCsumvalue VARCHAR2(2048);
BEGIN
  -- retrieve data from the namespace: note the truncation of stagerTime to 5 digits.
  -- This is needed for consistency with the stager code that uses the OCCI API and thus
  -- loses precision when recuperating 64 bits integers into doubles
  -- (lack of support for 64 bits numbers in OCCI).
  SELECT TRUNC(stagerTime,5), csumtype, csumvalue, filesize
    INTO varNSOpenTime, varNSCsumtype, varNSCsumvalue, varNSSize
    FROM Cns_File_Metadata@RemoteNS
   WHERE fileid = inFileId;
  -- was the file overwritten in the meantime ?
  IF varNSOpenTime > inLastOpenTime THEN
    -- yes ! reset it and thus restart the recall from scratch
    resetOverwrittenCastorFile(inCfId, varNSOpenTime, varNSSize);
    -- in case of repack, just stop and archive the corresponding request(s) as we're not interested
    -- any longer (the original segment disappeared). This potentially stops the entire recall process.
    archiveOrFailRepackSubreq(inCfId, serrno.ENSFILECHG);
    -- log "setFileRecalled : file was overwritten during recall, restarting from scratch or skipping repack"
    logToDLF(inReqId, dlf.LVL_NOTICE, dlf.RECALL_FILE_OVERWRITTEN, inFileId, inNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' NSOpenTime=' || TRUNC(varNSOpenTime, 6) ||
             ' NsOpenTimeAtStager=' || TRUNC(inLastOpenTime, 6) ||' '|| inLogContext);
    RETURN FALSE;
  END IF;

  -- is the checksum set in the namespace ?
  IF varNSCsumtype IS NULL THEN
    -- no -> let's set it (note that the function called commits in the remote DB)
    setSegChecksumWhenNull@remoteNS(inFileId, inCopyNb, inCksumName, inCksumValue);
    -- log 'checkRecallInNS : created missing checksum in the namespace'
    logToDLF(inReqId, dlf.LVL_SYSTEM, dlf.RECALL_CREATED_CHECKSUM, inFileId, inNsHost, 'nsd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' copyNb=' || TO_CHAR(inCopyNb) ||
             ' TPVID=' || inVID || ' fseq=' || TO_CHAR(inFseq) || ' checksumType='  || inCksumName ||
             ' checksumValue=' || TO_CHAR(inCksumValue));
  ELSE
    -- is the checksum matching ?
    -- note that this is probably useless as it was already checked at transfer time
    IF inCksumName = varNSCsumtype AND TO_CHAR(inCksumValue, 'XXXXXXXX') != varNSCsumvalue THEN
      -- not matching ! log "checkRecallInNS : bad checksum detected, will retry if allowed"
      logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_BAD_CHECKSUM, inFileId, inNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
               ' fseq=' || TO_CHAR(inFseq) || ' copyNb=' || TO_CHAR(inCopyNb) || ' checksumType=' || inCksumName ||
               ' expectedChecksumValue=' || varNSCsumvalue ||
               ' checksumValue=' || TO_CHAR(inCksumValue, 'XXXXXXXX') ||' '|| inLogContext);
      retryOrFailRecall(inCfId, inVID, inReqId, inLogContext);
      RETURN FALSE;
    END IF;
  END IF;
  RETURN TRUE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- file got dropped from the namespace, recall should be cancelled
  deleteRecallJobs(inCfId);
  -- potentially terminate repack requests
  archiveOrFailRepackSubreq(inCfId, serrno.ENOENT);
  -- and fail remaining requests
  UPDATE SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOENT,
           errorMessage = 'File was removed during recall'
     WHERE castorFile = inCfId
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
  -- log "checkRecallInNS : file was dropped from namespace during recall, giving up"
  logToDLF(inReqId, dlf.LVL_NOTICE, dlf.RECALL_FILE_DROPPED, inFileId, inNsHost, 'tapegatewayd',
           'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
           ' fseq=' || TO_CHAR(inFseq) || ' CFLastOpenTime=' || TO_CHAR(inLastOpenTime) || ' ' || inLogContext);
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
  varLastOpenTime   NUMBER;
  varCfId           INTEGER;
  varFSId           INTEGER;
  varDPId           INTEGER;
  varDCPath         VARCHAR2(2048);
  varDcId           INTEGER;
  varFileSize       INTEGER;
  varFileClassId    INTEGER;
  varNbMigrationsStarted INTEGER;
  varGcWeight       NUMBER;
  varGcWeightProc   VARCHAR2(2048);
  varRecallStartTime NUMBER;
BEGIN
  -- get diskserver, filesystem and path from full path in input
  BEGIN
    parsePath(inFilePath, varFSId, varDPId, varDCPath, varDCId, varFileId, varNsHost);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "setFileRecalled : unable to parse input path. giving up"
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_INVALID_PATH, 0, '', 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' ' || inLogContext);
    RETURN;
  END;

  -- first lock Castorfile, check NS and parse path
  -- Get RecallJob and lock Castorfile
  BEGIN
    SELECT CastorFile.id, CastorFile.fileId, CastorFile.nsHost, CastorFile.nsOpenTime,
           CastorFile.fileSize, CastorFile.fileClass, RecallMount.VID, RecallJob.copyNb,
           RecallJob.euid, RecallJob.egid
      INTO varCfId, varFileId, varNsHost, varLastOpenTime, varFileSize, varFileClassId, varVID,
           varCopyNb, varEuid, varEgid
      FROM RecallMount, RecallJob, CastorFile
     WHERE RecallMount.mountTransactionId = inMountTransactionId
       AND RecallJob.vid = RecallMount.vid
       AND RecallJob.fseq = inFseq
       AND (RecallJob.status = tconst.RECALLJOB_SELECTED
         OR RecallJob.status = tconst.RECALLJOB_SELECTED2NDCOPY)
       AND RecallJob.castorFile = CastorFile.id
       AND ROWNUM < 2
       FOR UPDATE OF CastorFile.id;
    -- the ROWNUM < 2 clause is worth a comment here :
    -- this select will select a single CastorFile and RecallMount, but may select
    -- several RecallJobs "linked" to them. All these recall jobs have the same copyNb
    -- but different uid/gid. They exist because these different uid/gid are attached
    -- to different recallGroups.
    -- In case of several recallJobs present, they are all equally responsible for the
    -- recall, thus we pick the first one as "the" responsible. The only consequence is
    -- that it's uid/gid will be used for the DiskCopy creation
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "setFileRecalled : unable to identify Recall. giving up"
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_NOT_FOUND, varFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' ' || inLogContext);
    RETURN;
  END;

  -- Deal with the DiskCopy: it is created now as the recall is effectively over. The subsequent
  -- check in the NS may make it INVALID, which is fine as opposed to forget about it and generating dark data.

  -- compute GC weight of the recalled diskcopy
  -- first get the svcClass
  IF varFSId > 0 THEN
    SELECT Diskpool2SvcClass.child INTO varSvcClassId
      FROM Diskpool2SvcClass, FileSystem
     WHERE FileSystem.id = varFSId
       AND Diskpool2SvcClass.parent = FileSystem.diskPool
       AND ROWNUM < 2;
  ELSE
    SELECT DataPool2SvcClass.child INTO varSvcClassId
      FROM DataPool2SvcClass
     WHERE DataPool2SvcClass.parent = varDPId
       AND ROWNUM < 2;
  END IF;
  -- Again, the ROWNUM < 2 is worth a comment : the pool may be attached
  -- to several svcClasses. However, we do not support that these different
  -- SvcClasses have different GC policies (actually the GC policy should be
  -- moved to the DiskPool/DataPool table in the future). Thus it is safe
  -- to take any SvcClass from the list
  varGcWeightProc := castorGC.getRecallWeight(varSvcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || varGcWeightProc || '(:size); END;'
    USING OUT varGcWeight, IN varFileSize;
  -- create the DiskCopy, after getting how many copies on tape we have, for the importance number
  DECLARE
    varNbCopiesOnTape INTEGER;
  BEGIN
    SELECT nbCopies INTO varNbCopiesOnTape FROM FileClass WHERE id = varFileClassId;
    INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                          ownerUid, ownerGid, id, gcType, fileSystem, dataPool,
                          castorFile, status, importance)
    VALUES (varDCPath, varGcWeight, getTime(), getTime(), varFileSize, 0,
            varEuid, varEgid, varDCId, NULL, varFSId, varDPId, varCfId, dconst.DISKCOPY_VALID,
            -1-varNbCopiesOnTape*100);
  END;

  -- Check that the file is still there in the namespace (and did not get overwritten)
  -- Note that error handling and logging is done inside the function
  IF NOT checkRecallInNS(varCfId, inMountTransactionId, varVID, varCopyNb, inFseq, varFileId, varNsHost,
                         inCksumName, inCksumValue, varLastOpenTime, inReqId, inLogContext) THEN
    RETURN;
  END IF;

  -- Then deal with recalljobs and potential migrationJobs
  -- Find out starting time of oldest recall for logging purposes
  SELECT MIN(creationTime) INTO varRecallStartTime FROM RecallJob WHERE castorFile = varCfId;
  -- Delete recall jobs
  DELETE FROM RecallJob WHERE castorFile = varCfId;
  -- trigger waiting migrations if any
  -- Note that we reset the creation time as if the MigrationJob was created right now
  -- this is because "creationTime" is actually the time of entering the "PENDING" state
  -- in the cases where the migrationJob went through a WAITINGONRECALL state
  UPDATE /*+ INDEX_RS_ASC (MigrationJob I_MigrationJob_CFVID) */ MigrationJob
     SET status = tconst.MIGRATIONJOB_PENDING,
         creationTime = getTime()
   WHERE status = tconst.MIGRATIONJOB_WAITINGONRECALL
     AND castorFile = varCfId;
  varNbMigrationsStarted := SQL%ROWCOUNT;
  -- in case there are migrations, update CastorFile's tapeStatus to NOTONTAPE, otherwise it is ONTAPE
  UPDATE CastorFile
     SET tapeStatus = CASE varNbMigrationsStarted
                        WHEN 0
                        THEN dconst.CASTORFILE_ONTAPE
                        ELSE dconst.CASTORFILE_NOTONTAPE
                      END
   WHERE id = varCfId;

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
           ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' recallTime=' ||
           to_char(trunc(getTime() - varRecallStartTime, 0)) || ' ' || inLogContext);
END;
/

/* Attempt to retry a recall. Fail it in case it should not be retried anymore */
CREATE OR REPLACE PROCEDURE retryOrFailRecall(inCfId IN NUMBER, inVID IN VARCHAR2,
                                              inReqId IN VARCHAR2, inLogContext IN VARCHAR2) AS
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
  varRecallStillAlive INTEGER;
BEGIN
  -- lock castorFile
  SELECT fileId, nsHost INTO varFileId, varNsHost
    FROM CastorFile WHERE id = inCfId FOR UPDATE;
  -- increase retry counters within mount and set recallJob status to NEW
  UPDATE RecallJob
     SET nbRetriesWithinMount = nbRetriesWithinMount + 1,
         status = tconst.RECALLJOB_PENDING
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
  SELECT /*+ INDEX_RS_ASC(RecallJob I_RecallJob_CastorFile_VID) */
         count(*) INTO varRecallStillAlive
    FROM RecallJob
   WHERE castorFile = inCfId
     AND ROWNUM < 2;
  -- if no remaining recallJobs, the subrequests are failed
  IF varRecallStillAlive = 0 THEN
    UPDATE /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile) */ SubRequest 
       SET status = dconst.SUBREQUEST_FAILED,
           lastModificationTime = getTime(),
           errorCode = serrno.SEINTERNAL,
           errorMessage = 'File recall from tape has failed, please try again later'
     WHERE castorFile = inCfId 
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
     -- log 'File recall has permanently failed'
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_PERMANENTLY_FAILED, varFileId, varNsHost,
      'tapegatewayd', ' TPVID=' || inVID ||' '|| inLogContext);
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
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_CANCEL_RECALLJOB_VID, varFileId, varNsHost, 'tapegatewayd',
           'errorCode=' || TO_CHAR(inErrorCode) ||
           ' errorMessage="' || inErrorMsg ||
           '" TPVID=' || inVID);
  -- remove recallJobs that need the non existing tape
  DELETE FROM RecallJob WHERE castorfile = inCfId AND VID=inVID;
  -- check if other recallJobs remain (typically dual copy tapes)
  SELECT /*+ INDEX_RS_ASC(RecallJob I_RecallJob_CastorFile_VID) */
         count(*) INTO varNbRecalls
    FROM RecallJob WHERE castorfile = inCfId;
  -- if no remaining recalls, fail requests and cleanup
  IF varNbRecalls = 0 THEN
    -- log "Failing Recall(s)"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_FAILING, varFileId, varNsHost, 'tapegatewayd',
             'errorCode=' || TO_CHAR(inErrorCode) ||
             ' errorMessage="' || inErrorMsg ||
             '" TPVID=' || inVID);
    -- delete potential migration jobs waiting on recalls
    deleteMigrationJobsForRecall(inCfId);
    -- Fail the associated subrequest(s)
    UPDATE /*+ INDEX_RS_ASC(SR I_Subrequest_Castorfile)*/ SubRequest SR
       SET SR.status = dconst.SUBREQUEST_FAILED,
           SR.getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED, --  (not strictly correct but the request is over anyway)
           SR.lastModificationTime = getTime(),
           SR.errorCode = serrno.SEINTERNAL,
           SR.errorMessage = 'File recall from tape has failed (tape not available), please try again later'
     WHERE SR.castorFile = inCfId
       AND SR.status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_WAITSUBREQ);
  END IF;
  -- commit
  COMMIT;
END;
/

/* Cancel a tape session before startup e.g. in case of a VMGR errors when checking the tape.
 * Not to be called on a running session as the procedure assumes no running migration/recall job
 * is attached to the session being deleted.
 */
CREATE OR REPLACE PROCEDURE cancelMigrationOrRecall(inMode IN INTEGER,
                                                    inVID IN VARCHAR2,
                                                    inErrorCode IN INTEGER,
                                                    inErrorMsg IN VARCHAR2) AS
BEGIN
  IF inMode = tconst.WRITE_ENABLE THEN
    -- cancel the migration. No job has been attached yet
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
                                                   minimumAge IN INTEGER,
                                                   outMountId OUT INTEGER) AS
  varMigJobId INTEGER;
BEGIN
  -- Check that the mount would be honoured by running a dry-run file selection:
  -- note that in case the mount was triggered because of age, we check that
  -- we have a valid candidate that is at least minimumAge seconds old.
  -- This is almost a duplicate of the query in tg_getFilesToMigrate.
  SELECT /*+ FIRST_ROWS_1
             LEADING(MigrationJob CastorFile DiskCopy FileSystem DiskServer)
             USE_NL(MigrationJob CastorFile DiskCopy FileSystem DiskServer)
             INDEX(CastorFile PK_CastorFile_Id)
             INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile)
             INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusCT) */
         MigrationJob.id mjId INTO varMigJobId
    FROM MigrationJob, DiskCopy, CastorFile
   WHERE MigrationJob.tapePool = inTapePoolId
     AND MigrationJob.status = tconst.MIGRATIONJOB_PENDING
     AND (minimumAge = 0 OR MigrationJob.creationTime < getTime() - minimumAge)
     AND CastorFile.id = MigrationJob.castorFile
     AND CastorFile.id = DiskCopy.castorFile
     AND CastorFile.tapeStatus = dconst.CASTORFILE_NOTONTAPE
     AND DiskCopy.status = dconst.DISKCOPY_VALID
     AND EXISTS (SELECT 1 FROM FileSystem, DiskServer
                  WHERE FileSystem.id = DiskCopy.fileSystem
                    AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION,
                                              dconst.FILESYSTEM_DRAINING,
                                              dconst.FILESYSTEM_READONLY)
                    AND DiskServer.id = FileSystem.diskServer
                    AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION,
                                              dconst.DISKSERVER_DRAINING,
                                              dconst.DISKSERVER_READONLY)
                    AND DiskServer.hwOnline = 1
                  UNION ALL
                 SELECT 1 FROM DiskServer
                  WHERE DiskServer.dataPool = diskCopy.dataPool
                    AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION,
                                              dconst.DISKSERVER_DRAINING,
                                              dconst.DISKSERVER_READONLY)
                    AND DiskServer.hwOnline = 1
                    AND ROWNUM < 2)
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
      insertMigrationMount(t.id, 0, varMountId);
      varTotalNbMounts := varTotalNbMounts + 1;
      IF varMountId = 0 THEN
        -- log "startMigrationMounts: failed migration mount creation due to lack of files"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NO_FILE, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
        -- no need to continue as we could not find a single file to migrate
        EXIT;
      ELSE
        -- log "startMigrationMounts: created new migration mount"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NEW_MOUNT, 0, '', 'tapegatewayd',
                 'MigrationMountId=' || TO_CHAR(varMountId) ||
                 ' tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
      END IF;
    END LOOP;
    -- force creation of a unique mount in case no mount was created at all and some files are too old
    IF varNbFiles > 0 AND varTotalNbMounts = 0 AND t.nbDrives > 0 AND
       gettime() - varOldestCreationTime > t.maxFileAgeBeforeMount THEN
      insertMigrationMount(t.id, t.maxFileAgeBeforeMount, varMountId);
      IF varMountId = 0 THEN
        -- log "startMigrationMounts: failed migration mount creation due to lack of files"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_AGE_NO_FILE, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
      ELSE
        -- log "startMigrationMounts: created new migration mount based on age"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NEW_MOUNT_AGE, 0, '', 'tapegatewayd',
                 'MigrationMountId=' || TO_CHAR(varMountId) ||
                 ' tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
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
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(nvl(varOldestCreationTime,0))));
      END IF;
    END IF;
    COMMIT;
  END LOOP;
END;
/

/* insert new Recall Mount */
CREATE OR REPLACE PROCEDURE insertRecallMount(inRecallGroupId IN NUMBER,
                                                        inVid IN VARCHAR2,
                                                outMountCount OUT INTEGER) AS
  varUnused INTEGER;
BEGIN
  -- We receive a candidate recall mount. Before actually posting the recall
  -- mount we will make sure at least one recall would be honored from the mount
  -- This protection mechanism will protect against unavailability of disk 
  -- servers (this did happen during a network incident, leading to looping
  -- mounts).
  -- The duty of this procedure is to actually insert the recall mount and log
  -- log it, if all is fine, and to log the problem if not. It will just report
  -- the number of created mounts (1 or 0) to the upstream caller.

  -- Last sanity check. Will give up automatically by means of exception, which
  -- will change the return value and log.
  SELECT RecallJob.id INTO varUnused
    FROM RecallJob, SvcClass
   WHERE SvcClass.id = RecallJob.svcClass
     AND RecallJob.vid = inVid
     AND RecallJob.status = tconst.RECALLJOB_PENDING
     AND EXISTS (SELECT 1 FROM DiskPool2SvcClass, FileSystem, DiskServer
                  WHERE DiskPool2SvcClass.child = SvcClass.id
                    AND FileSystem.diskPool = DiskPool2SvcClass.parent
                    AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
                    AND DiskServer.id = FileSystem.diskServer
                    AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
                    AND DiskServer.hwonline =  1
                  UNION ALL
                 SELECT 1 FROM DataPool2SvcClass, DiskServer
                  WHERE DataPool2SvcClass.child = SvcClass.id
                    AND DiskServer.dataPool = DataPool2SvcClass.parent
                    AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
                    AND DiskServer.hwonline =  1
                    AND ROWNUM < 2)
     AND ROWNUM < 2;
  -- We passed the test, insert the recall mount:
  INSERT INTO RecallMount (id, VID, recallGroup, startTime, status)
       VALUES (ids_seq.nextval, inVid, inRecallGroupId, gettime(), tconst.RECALLMOUNT_NEW);
  outMountCount := 1;
EXCEPTION WHEN NO_DATA_FOUND THEN
  outMountCount := 0;
END;
/

/* DB job to start new recall mounts */
CREATE OR REPLACE PROCEDURE startRecallMounts AS
   varNbMounts INTEGER;
   varNbExtraMounts INTEGER := 0;
   varNewMounts INTEGER;
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
      DECLARE
        varVID VARCHAR2(2048);
        varDataAmount INTEGER;
        varNbFiles INTEGER;
        varOldestCreationTime NUMBER;
      BEGIN
        -- loop over the best candidates until we have enough mounts
        WHILE varNbMounts + varNbExtraMounts < rg.nbDrives LOOP
          SELECT * INTO varVID, varDataAmount, varNbFiles, varOldestCreationTime FROM (
            SELECT vid, SUM(fileSize) dataAmount, COUNT(*) nbFiles, MIN(creationTime)
              FROM RecallJob
             WHERE recallGroup = rg.id
               AND status = tconst.RECALLJOB_PENDING
             GROUP BY vid
            HAVING (SUM(fileSize) >= rg.minAmountDataForMount OR
                    COUNT(*) >= rg.minNbFilesForMount OR
                    gettime() - MIN(creationTime) > rg.maxFileAgeBeforeMount)
               AND VID NOT IN (SELECT vid FROM RecallMount)
             ORDER BY MIN(creationTime))
           WHERE ROWNUM < 2;
          -- trigger a new mount, with checks
          insertRecallMount(rg.id, varVID, varNewMounts);
          IF varNewMounts > 0 THEN
            varNbExtraMounts := varNbExtraMounts + varNewMounts;
            -- log "startRecallMounts: created new recall mount"
            logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECMOUNT_NEW_MOUNT, 0, '', 'tapegatewayd',
                     'recallGroup=' || rg.name ||
                     ' TPVID=' || varVid ||
                     ' nbExistingMounts=' || TO_CHAR(varNbMounts) ||
                     ' nbNewMountsSoFar=' || TO_CHAR(varNbExtraMounts) ||
                     ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                     ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                     ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
          ELSE
            -- The sanity check failed: log and report no recall mount got created for
            -- tape.
            -- "startRecallMounts: not creating mount that would have been empty (possible issue with destination diskpools)"
            logToDLF(NULL, dlf.LVL_WARNING, dlf.RECMOUNT_FAILED_NEW_MOUNT, 0, '', 'tapegatewayd',
                     'recallGroup=' || rg.name ||
                     ' TPVID=' || varVid);
          END IF;
        END LOOP;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- nothing left to recall, just exit nicely
        NULL;
      END;
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
  varOldestAcceptableAge NUMBER;
  varVid VARCHAR2(10);
  varNewFseq INTEGER;
  varFileTrId NUMBER;
  varTpId INTEGER;
  varUnused INTEGER;
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
BEGIN
  BEGIN
    -- Get id, VID and last valid fseq for this migration mount, lock
    SELECT id, vid, tapePool, lastFSeq INTO varMountId, varVid, varTpId, varNewFseq
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
  SELECT TapePool.maxFileAgeBeforeMount INTO varOldestAcceptableAge
    FROM TapePool, MigrationMount
   WHERE MigrationMount.id = varMountId
     AND MigrationMount.tapePool = TapePool.id;
  -- Get candidates up to inCount or inTotalSize
  FOR Cand IN (
    SELECT /*+ FIRST_ROWS(100)
               LEADING(Job CastorFile Location)
               USE_NL(Job CastorFile Location)
               INDEX(CastorFile PK_CastorFile_Id) */
           Job.id mjId, Location.filePath,
           CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, CastorFile.lastKnownFileName,
           Castorfile.id as castorfile
      FROM (SELECT * FROM
             (SELECT /*+ FIRST_ROWS(100) INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusCT) */
                     id, castorfile, destCopyNb, creationTime
                FROM MigrationJob
               WHERE tapePool = varTpId
                 AND status = tconst.MIGRATIONJOB_PENDING
               ORDER BY creationTime)
             WHERE ROWNUM < TO_NUMBER(getConfigOption('Migration', 'NbMigCandConsidered', 10000)))
           Job, CastorFile,
           (SELECT /*+ LEADING(DiskCopy, FileSystem, DiskServer)
                       USE_NL(DiskCopy, FileSystem, DiskServer)
                       INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */
                   DiskCopy.castorFile,
                   DiskServer.name || ':' || FileSystem.mountPoint || DiskCopy.path AS filePath,
                   FileSystem.nbRecallerStreams + FileSystem.nbMigratorStreams AS rate
              FROM FileSystem, DiskServer, DiskCopy
             WHERE DiskCopy.status = dconst.DISKCOPY_VALID
               AND FileSystem.id = DiskCopy.fileSystem
               AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION,
                                         dconst.FILESYSTEM_DRAINING,
                                         dconst.FILESYSTEM_READONLY)
               AND DiskServer.id = FileSystem.diskServer
               AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION,
                                         dconst.DISKSERVER_DRAINING,
                                         dconst.DISKSERVER_READONLY)
               AND DiskServer.hwOnline = 1
             UNION ALL
            SELECT DiskCopy.id, DiskServer.name || ':' || DiskCopy.path AS filePath, 0 AS rate
              FROM DiskServer, DiskCopy
             WHERE DiskCopy.status = dconst.DISKCOPY_VALID
               AND DiskServer.dataPool = DiskCopy.dataPool
               AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION,
                                         dconst.DISKSERVER_DRAINING,
                                         dconst.DISKSERVER_READONLY)
               AND DiskServer.hwOnline = 1) Location
     WHERE CastorFile.id = Job.castorFile
       AND CastorFile.id = Location.castorFile
       AND CastorFile.tapeStatus = dconst.CASTORFILE_NOTONTAPE
       AND NOT EXISTS (SELECT /*+ USE_NL(MigratedSegment)
                                  INDEX_RS_ASC(MigratedSegment I_MigratedSegment_CFCopyNBVID) */ 1
                         FROM MigratedSegment
                        WHERE MigratedSegment.castorFile = Job.castorfile
                          AND MigratedSegment.copyNb != Job.destCopyNb
                          AND MigratedSegment.vid = varVid)
       ORDER BY -- we first order by a multi-step function, which gives old guys incrasingly more priority:
                -- migrations younger than varOldestAccep2tableAge will be taken last
                TRUNC(Job.creationTime/varOldestAcceptableAge) ASC,
                -- and then, for all migrations between (N-1)*varOldestAge and N*varOldestAge, by filesystem load
                Location.rate ASC)
  LOOP
    -- last part of the above statement. Could not be part of it as ORACLE insisted on not
    -- optimizing properly the execution plan
    BEGIN
      SELECT /*+ INDEX_RS_ASC(MJ I_MigrationJob_CFVID) */ 1 INTO varUnused
        FROM MigrationJob MJ
       WHERE MJ.castorFile = Cand.castorFile
         AND MJ.vid = varVid
         AND MJ.vid IS NOT NULL;
      -- found one, so skip this candidate
      CONTINUE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- nothing, it's a valid candidate. Let's lock it and revalidate the status
      DECLARE
        MjLocked EXCEPTION;
        PRAGMA EXCEPTION_INIT (MjLocked, -54);
      BEGIN
        SELECT id INTO varUnused
          FROM MigrationJob
         WHERE id = Cand.mjId
           AND status = tconst.MIGRATIONJOB_PENDING
           FOR UPDATE NOWAIT;
      EXCEPTION WHEN MjLocked THEN
        -- this migration job is being handled else where, let's go to next one
        CONTINUE;
                WHEN NO_DATA_FOUND THEN
        -- this migration job has already been handled else where, let's go to next one
        CONTINUE;
      END;
    END;
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
                                                    outNSIsOnlyLogs OUT "numList",
                                                    outNSTimeInfos OUT floatList,
                                                    outNSErrorCodes OUT "numList",
                                                    outNSMsgs OUT strListTable,
                                                    outNSFileIds OUT "numList",
                                                    outNSParams OUT strListTable) AS
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  -- "Bulk" transfer data to the NS DB
  INSERT /*+ APPEND */ INTO SetSegsForFilesInputHelper@RemoteNS
    (reqId, fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
     vid, fseq, blockId, checksumType, checksum) (
    SELECT reqId, fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
           vid, fseq, blockId, checksumType, checksum
      FROM FileMigrationResultsHelper
     WHERE reqId = inReqId);
  DELETE FROM FileMigrationResultsHelper
   WHERE reqId = inReqId;
  COMMIT;  -- commit the remote insertion, otherwise a ORA-01002 (fetch out of sequence) may happen
  -- This call autocommits all successful segments in the NameServer, and reports
  -- successes and errors as entries in the SetSegsForFilesResultsHelper table
  setOrReplaceSegmentsForFiles@RemoteNS(inReqId);
  -- Retrieve results from the NS DB in bulk and clean data
  SELECT isOnlyLog, timeinfo, errorCode, msg, fileId, params
    BULK COLLECT INTO outNSIsOnlyLogs, outNSTimeInfos, outNSErrorCodes, outNSMsgs, outNSFileIds, outNSParams
    FROM SetSegsForFilesResultsHelper@RemoteNS
   WHERE reqId = inReqId;
  DELETE FROM SetSegsForFilesResultsHelper@RemoteNS
   WHERE reqId = inReqId;
  -- this commits the remote deletion
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
  varNsOpenTime NUMBER;
  varCopyNo NUMBER;
  varOldCopyNo NUMBER;
  varVid VARCHAR2(10);
  varNSIsOnlyLogs "numList";
  varNSTimeInfos floatList;
  varNSErrorCodes "numList";
  varNSMsgs strListTable;
  varNSFileIds "numList" := "numList"();
  varNSParams strListTable;
  varParams VARCHAR2(4000);
  varNbSentToNS INTEGER := 0;
  varLastUpdateTime INTEGER;
BEGIN
  varStartTime := SYSTIMESTAMP;
  varReqId := uuidGen();
  -- Get the NS host name
  varNsHost := getConfigOption('stager', 'nsHost', '');
  FOR i IN inFileTrIds.FIRST .. inFileTrIds.LAST LOOP
    BEGIN
      -- Collect additional data. Note that this is NOT bulk
      -- to preserve the order in the input arrays.
      SELECT CF.nsOpenTime, CF.lastUpdateTime, nvl(MJ.originalCopyNb, 0), MJ.vid, MJ.destCopyNb
        INTO varNsOpenTime, varLastUpdateTime, varOldCopyNo, varVid, varCopyNo
        FROM CastorFile CF, MigrationJob MJ
       WHERE MJ.castorFile = CF.id
         AND CF.fileid = inFileIds(i)
         AND MJ.mountTransactionId = inMountTrId
         AND MJ.fileTransactionId = inFileTrIds(i)
         AND status = tconst.MIGRATIONJOB_SELECTED;
        -- Store in a temporary table, to be transfered to the NS DB
      IF inErrorCodes(i) = 0 THEN
        -- Successful migration
        INSERT INTO FileMigrationResultsHelper
          (reqId, fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
           vid, fseq, blockId, checksumType, checksum)
        VALUES (varReqId, inFileIds(i), varNsOpenTime, varCopyNo, varOldCopyNo,
                inTransferredSizes(i), CASE inComprSizes(i) WHEN 0 THEN 1 ELSE inComprSizes(i) END, varVid, inFseqs(i),
                strtoRaw4(inBlockIds(i)), inChecksumTypes(i), inChecksums(i));
        varNbSentToNS := varNbSentToNS + 1;
      ELSE
        -- Fail/retry this migration, log 'migration failed, will retry if allowed'
        varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' ErrorCode='|| to_char(inErrorCodes(i))
                     ||' ErrorMessage="'|| inErrorMsgs(i) ||'" VID='|| varVid
                     ||' copyNb='|| to_char(varCopyNo) ||' '|| inLogContext;
        logToDLF(varReqid, dlf.LVL_WARNING, dlf.MIGRATION_RETRY, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
        retryOrFailMigration(inMountTrId, inFileIds(i), varNsHost, inErrorCodes(i), varReqId);
        -- here we commit immediately because retryOrFailMigration took a lock on the CastorFile
        COMMIT;
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Log 'unable to identify migration, giving up'
      varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' fileTransactionId='|| to_char(inFileTrIds(i))
        ||' '|| inLogContext;
      logToDLF(varReqid, dlf.LVL_ERROR, dlf.MIGRATION_NOT_FOUND, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
    END;
  END LOOP;
  -- Commit all the entries in FileMigrationResultsHelper so that the next call can take them
  COMMIT;

  DECLARE
    varUnused INTEGER;
  BEGIN
    -- boundary case: if nothing to do, just skip the remote call and the
    -- subsequent FOR loop as it would be useless (and would fail).
    SELECT 1 INTO varUnused FROM FileMigrationResultsHelper
     WHERE reqId = varReqId AND ROWNUM < 2;
    -- The following procedure wraps the remote calls in an autonomous transaction
    ns_setOrReplaceSegments(varReqId, varNSIsOnlyLogs, varNSTimeInfos, varNSErrorCodes, varNSMsgs, varNSFileIds, varNSParams);

    -- Process the results
    FOR i IN 1 .. varNSFileIds.COUNT LOOP
      -- First log on behalf of the NS
      -- We classify the log level based on the error code here.
      -- Current error codes are:
      --   ENOENT, EACCES, EBUSY, EEXIST, EISDIR, EINVAL, SEINTERNAL, SECHECKSUM, ENSFILECHG, ENSNOSEG
      --   ENSTOOMANYSEGS, ENSOVERWHENREP, ERTWRONGSIZE, ESTNOSEGFOUND
      -- default level is ERROR. Some cases can be demoted to warning when it's a normal case
      -- (like file deleted by user in the mean time).
      logToDLFWithTime(varNSTimeinfos(i), varReqid,
                       CASE varNSErrorCodes(i) 
                         WHEN 0                 THEN dlf.LVL_SYSTEM
                         WHEN serrno.ENOENT     THEN dlf.LVL_WARNING
                         WHEN serrno.ENSFILECHG THEN dlf.LVL_WARNING
                         ELSE                        dlf.LVL_ERROR
                       END,
                       varNSMsgs(i), varNSFileIds(i), varNsHost, 'nsd', varNSParams(i));
      -- Now skip pure log entries and process file by file, depending on the result
      IF varNSIsOnlyLogs(i) = 1 THEN CONTINUE; END IF;
      CASE
      WHEN varNSErrorCodes(i) = 0 THEN
        -- All right, commit the migration in the stager
        tg_setFileMigrated(inMountTrId, varNSFileIds(i), varReqId, inLogContext);

      WHEN varNSErrorCodes(i) = serrno.ENOENT
        OR varNSErrorCodes(i) = serrno.ENSNOSEG
        OR varNSErrorCodes(i) = serrno.ENSFILECHG
        OR varNSErrorCodes(i) = serrno.ENSTOOMANYSEGS THEN
        -- The migration was useless because either the file is gone, or it has been modified elsewhere,
        -- or there were already enough copies on tape for it. Fail and update disk cache accordingly.
        failFileMigration(inMountTrId, varNSFileIds(i), varNSErrorCodes(i), varReqId);

      ELSE
        -- Attempt to retry for all other NS errors. To be reviewed whether some of the NS errors are to be considered fatal.
        varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' '|| varNSParams(i) ||' '|| inLogContext;
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
  -- Final log, "setBulkFileMigrationResult: bulk migration completed"
  varParams := 'mountTransactionId='|| to_char(inMountTrId)
               ||' NbInputFiles='|| inFileIds.COUNT
               ||' NbSentToNS='|| varNbSentToNS
               ||' NbFilesBackFromNS='|| varNSFileIds.COUNT
               ||' '|| inLogContext
               ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
               ||' AvgProcessingTime='|| trunc(getSecs(varStartTime, SYSTIMESTAMP)/inFileIds.COUNT, 6);
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
  varMigStartTime NUMBER;
  varSrId INTEGER;
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
  RETURNING destCopyNb, VID, originalVID, creationTime
    INTO varCopyNb, varVID, varOrigVID, varMigStartTime;
  -- check if another migration should be performed
  SELECT /*+ INDEX_RS_ASC(MigrationJob I_MigrationJob_CFVID) */
         count(*) INTO varMigJobCount
    FROM MigrationJob
   WHERE castorFile = varCfId;
  IF varMigJobCount = 0 THEN
    -- no more migrations, delete all migrated segments 
    DELETE FROM MigratedSegment
     WHERE castorFile = varCfId;
    -- And mark CastorFile as ONTAPE
    UPDATE CastorFile
       SET tapeStatus= dconst.CASTORFILE_ONTAPE
     WHERE id = varCfId;
  ELSE
    -- another migration ongoing, keep track of the one just completed
    INSERT INTO MigratedSegment (castorFile, copyNb, vid)
    VALUES (varCfId, varCopyNb, varVID);
  END IF;
  -- Do we have to deal with a repack ?
  IF varOrigVID IS NOT NULL THEN
    -- Yes we do, then archive the repack subrequest associated
    -- Note that there may be several if we are dealing with old bad tapes
    -- that have 2 copies of the same file on them. Thus we take one at random
    SELECT /*+ INDEX_RS_ASC(SR I_Subrequest_CastorFile) */ SR.id INTO varSrId
      FROM SubRequest SR, StageRepackRequest Req
      WHERE SR.castorfile = varCfId
        AND SR.status = dconst.SUBREQUEST_REPACK
        AND SR.request = Req.id
        AND Req.RepackVID = varOrigVID
        AND ROWNUM < 2;
    archiveSubReq(varSrId, dconst.SUBREQUEST_FINISHED);
  END IF;
  -- Log 'db updates after full migration completed'
  varParams := 'TPVID='|| varVID ||' mountTransactionId='|| to_char(inMountTrId) ||
    ' migrationTime=' || to_char(trunc(getTime() - varMigStartTime, 0)) || ' '|| inLogContext;
  logToDLF(inReqid, dlf.LVL_SYSTEM, dlf.MIGRATION_COMPLETED, inFileId, varNsHost, 'tapegatewayd', varParams);
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Log 'file not found, giving up'
  varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' '|| inLogContext;
  logToDLF(inReqid, dlf.LVL_ERROR, dlf.MIGRATION_NOT_FOUND, inFileId, varNsHost, 'tapegatewayd', varParams);
END;
/


/* Fail a file migration, potentially archiving outstanding repack requests */
CREATE OR REPLACE PROCEDURE failFileMigration(inMountTrId IN NUMBER, inFileId IN NUMBER,
                                              inErrorCode IN INTEGER, inReqId IN VARCHAR2) AS
  varNsHost VARCHAR2(2048);
  varCfId NUMBER;
  varNsOpenTime NUMBER;
  varSrIds "numList";
  varOriginalCopyNb NUMBER;
  varMigJobCount NUMBER;
  varErrorCode INTEGER := inErrorCode;
BEGIN
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Lock castor file
  SELECT id, nsOpenTime INTO varCfId, varNsOpenTime
    FROM CastorFile WHERE fileId = inFileId FOR UPDATE;
  -- delete migration job
  DELETE FROM MigrationJob
   WHERE castorFile = varCFId AND mountTransactionId = inMountTrId
  RETURNING originalCopyNb INTO varOriginalCopyNb;
  -- check if another migration should be performed
  SELECT /*+ INDEX_RS_ASC(MigrationJob I_MigrationJob_CFVID) */
         count(*) INTO varMigJobCount
    FROM MigrationJob
   WHERE castorfile = varCfId;
  IF varMigJobCount = 0 THEN
     -- no other migration, delete all migrated segments
     DELETE FROM MigratedSegment
      WHERE castorfile = varCfId;
  END IF;
  -- terminate repack subrequests
  IF varOriginalCopyNb IS NOT NULL THEN
    archiveOrFailRepackSubreq(varCfId, inErrorCode);
    -- set back the CastorFile to ONTAPE otherwise repack will wait forever
    UPDATE CastorFile
       SET tapeStatus = dconst.CASTORFILE_ONTAPE
     WHERE id = varCfId;
  END IF;
  
  IF varErrorCode = serrno.ENOENT THEN
    -- unfortunately, tape servers can throw this error too (see SR #136759), so we have to double check
    -- prior to taking destructive actions on the file: if the file does exist in the Nameserver, then
    -- replace the error code to a generic ETSYS (taped system error), otherwise keep ENOENT
    BEGIN
      SELECT 1902 INTO varErrorCode FROM Dual
       WHERE EXISTS (SELECT 1 FROM Cns_file_metadata@RemoteNS WHERE fileid = inFileId);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;
    END;
  END IF;
  -- Log depending on the error: some are not pathological and have dedicated handling
  IF varErrorCode = serrno.ENOENT OR varErrorCode = serrno.ENSFILECHG OR varErrorCode = serrno.ENSNOSEG THEN
    -- in this case, disk cache is stale
    UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID, gcType=GCTYPE_OVERWRITTEN
     WHERE status = dconst.DISKCOPY_VALID
       AND castorFile = varCfId;
    -- cleanup other migration jobs for that file if any
    DELETE FROM MigrationJob WHERE castorfile = varCfId;
    -- Log 'file was dropped or modified during migration, giving up'
    logToDLF(inReqid, dlf.LVL_NOTICE, dlf.MIGRATION_FILE_DROPPED, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || inMountTrId || ' ErrorCode=' || varErrorCode ||
             ' NsOpenTimeAtStager=' || trunc(varNsOpenTime, 6));
  ELSIF varErrorCode = serrno.ENSTOOMANYSEGS THEN
    -- do as if migration was successful
    UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_ONTAPE WHERE id = varCfId;
    -- Log 'file already had enough copies on tape, ignoring new segment'
    logToDLF(inReqid, dlf.LVL_NOTICE, dlf.MIGRATION_SUPERFLUOUS_COPY, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || inMountTrId);
  ELSE
    -- Any other case, log 'migration to tape failed for this file, giving up'
    logToDLF(inReqid, dlf.LVL_ERROR, dlf.MIGRATION_FAILED, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || inMountTrId || ' LastErrorCode=' || varErrorCode);
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- File was dropped, log 'file not found when failing migration'
  logToDLF(inReqid, dlf.LVL_ERROR, dlf.MIGRATION_FAILED_NOT_FOUND, inFileId, varNsHost, 'tapegatewayd',
           'mountTransactionId=' || inMountTrId || ' LastErrorCode=' || varErrorCode);
END;
/


/*** Disk-Tape interface, Recall ***/

/* Get next candidates for a given recall mount.
 * input:  VDQM transaction id, count and total size
 * output: outFiles, a cursor for the set of recall candidates.
 */
CREATE OR REPLACE PROCEDURE tg_getBulkFilesToRecall(inLogContext IN VARCHAR2,
                                                    inMountTrId IN NUMBER,
                                                    inCount IN INTEGER,
                                                    inTotalSize IN INTEGER,
                                                    outFiles OUT SYS_REFCURSOR) AS
  varVid VARCHAR2(10);
  varPreviousFseq INTEGER;
  varCount INTEGER;
  varTotalSize INTEGER;
  varPath VARCHAR2(2048);
  varFileTrId INTEGER;
  varNewFseq INTEGER;
  bestFSForRecall_error EXCEPTION;
  PRAGMA EXCEPTION_INIT(bestFSForRecall_error, -20115);
  varNbLockedFiles INTEGER := 0;
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
      SELECT fileId, nsHost, fileTransactionId, filePath, blockId, fseq, copyNb,
             euid, egid, VID, fileSize, creationTime, nbRetriesInMount, nbMounts
        FROM FilesToRecallHelper;
    RETURN;
  END;
  varCount := 0;
  varTotalSize := 0;
  varNewFseq := varPreviousFseq;
  -- Get candidates up to inCount or inTotalSize
  -- Select only the ones further down the tape (fseq > current one) as long as possible
  LOOP
    DECLARE
      varRjId INTEGER;
      varFSeq INTEGER;
      varBlockId RAW(4);
      varFileSize INTEGER;
      varCfId INTEGER;
      varFileId INTEGER;
      varNsHost VARCHAR2(2048);
      varCopyNb NUMBER;
      varEuid NUMBER;
      varEgid NUMBER;
      varCreationTime NUMBER;
      varNbRetriesInMount NUMBER;
      varNbMounts NUMBER;
      CfLocked EXCEPTION;
      PRAGMA EXCEPTION_INIT (CfLocked, -54);
    BEGIN
      -- Find the unprocessed recallJobs of this tape with lowest fSeq
      -- that is above the previous one
      SELECT * INTO varRjId, varFSeq, varBlockId, varFileSize, varCfId, varCopyNb,
                    varEuid, varEgid, varCreationTime, varNbRetriesInMount,
                    varNbMounts
        FROM (SELECT id, fSeq, blockId, fileSize, castorFile, copyNb, eUid, eGid,
                     creationTime, nbRetriesWithinMount,  nbMounts
                FROM RecallJob
               WHERE vid = varVid
                 AND status = tconst.RECALLJOB_PENDING
                 AND fseq > varNewFseq
               ORDER BY fseq ASC)
       WHERE ROWNUM < 2;
      -- lock the corresponding CastorFile, give up if we do not manage as it means that
      -- this file is already being handled by someone else
      -- Note that the giving up is handled by the handling of the CfLocked exception
      SELECT fileId, nsHost INTO varFileId, varNsHost
        FROM CastorFile
       WHERE id = varCfId
         FOR UPDATE NOWAIT;
      -- Now that we have the lock, double check that the RecallJob is still there and
      -- valid (due to race conditions, it may have been processed in between our first select
      -- and the taking of the lock)
      BEGIN
        SELECT id INTO varRjId FROM RecallJob WHERE id = varRJId AND status = tconst.RECALLJOB_PENDING;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- we got the race condition ! So this has already been handled, let's move to next file
        CONTINUE;
      END;
      -- move up last fseq used. Note that it moves up even if bestFileSystemForRecall
      -- (or any other statement) fails and the file is actually not recalled.
      -- The goal is that a potential retry within the same mount only occurs after
      -- we went around the other files on this tape.
      varNewFseq := varFseq;
      -- Find the best filesystem to recall the selected file
      bestFileSystemForRecall(varCfId, varPath);
      varCount := varCount + 1;
      varTotalSize := varTotalSize + varFileSize;
      INSERT INTO FilesToRecallHelper (fileId, nsHost, fileTransactionId, filePath, blockId, fSeq,
                 copyNb, euid, egid, VID, fileSize, creationTime, nbRetriesInMount, nbMounts)
        VALUES (varFileId, varNsHost, ids_seq.nextval, varPath, varBlockId, varFSeq,
                varCopyNb, varEuid, varEgid, varVID, varFileSize, varCreationTime, varNbRetriesInMount,
                varNbMounts)
        RETURNING fileTransactionId INTO varFileTrId;
      -- update RecallJobs of this file. Only the recalled one gets a fileTransactionId
      UPDATE RecallJob
         SET status = CASE WHEN id = varRjId THEN tconst.RECALLJOB_SELECTED ELSE tconst.RECALLJOB_SELECTED2NDCOPY END,
             fileTransactionID = CASE WHEN id = varRjId THEN varFileTrId ELSE NULL END
       WHERE castorFile = varCfId;
      IF varCount >= inCount OR varTotalSize >= inTotalSize THEN
        -- we have enough candidates for this round, exit loop
        EXIT;
      END IF;
    EXCEPTION
      WHEN CfLocked THEN
        -- Go to next candidate, this CastorFile is being processed by another thread
        -- still check that this does not happen too often
        -- the reason is that a long standing lock (due to another bug) would make us spin
        -- like mad (learnt the hard way in production...)
        varNbLockedFiles := varNbLockedFiles + 1;
        IF varNbLockedFiles >= 100 THEN
          DECLARE
            lastSQL VARCHAR2(2048);
            prevSQL VARCHAR2(2048);
          BEGIN
            -- find the blocking SQL
            SELECT lastSql.sql_text, prevSql.sql_text INTO lastSQL, prevSQL
              FROM v$session currentSession, v$session blockerSession, v$sql lastSql, v$sql prevSql
             WHERE currentSession.sid = (SELECT sys_context('USERENV','SID') FROM DUAL)
               AND blockerSession.sid(+) = currentSession.blocking_session
               AND lastSql.sql_id(+) = blockerSession.sql_id
               AND prevSql.sql_id(+) = blockerSession.prev_sql_id;
            -- log the issue and exit, as if we were out of candidates
            logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_LOOPING_ON_LOCK, varFileId, varNsHost, 'tapegatewayd',
                   'SQLOfLockingSession="' || lastSQL || '" PreviousSQLOfLockingSession="' || prevSQL ||
                   '" mountTransactionId=' || to_char(inMountTrId) ||' '|| inLogContext);
            EXIT;
          END;
        END IF;
      WHEN bestFSForRecall_error THEN
        -- log 'bestFileSystemForRecall could not find a suitable destination for this recall' and skip it
        logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_FS_NOT_FOUND, varFileId, varNsHost, 'tapegatewayd',
                 'errorMessage="' || SQLERRM || '" mountTransactionId=' || to_char(inMountTrId) ||' '|| inLogContext);
        -- mark the recall job as failed, and maybe retry
        retryOrFailRecall(varCfId, varVID, NULL, inLogContext);
      WHEN NO_DATA_FOUND THEN
        -- nothing found. In case we did not try so far, try to restart with low fseqs
        IF varNewFseq > -1 THEN
          varNewFseq := -1;
        ELSE
          -- low fseqs were tried, we are really out of candidates, so exit the loop
          EXIT;
        END IF;
    END;
  END LOOP;
  -- Record last fseq at the mount level
  UPDATE RecallMount
     SET lastProcessedFseq = varNewFseq
   WHERE vid = varVid;
  -- Return all candidates. Don't commit now, this will be done in C++
  -- after the results have been collected as the temporary table will be emptied.
  OPEN outFiles FOR
    SELECT fileId, nsHost, fileTransactionId, filePath, blockId, fseq,
           copyNb, euid, egid, VID, fileSize, creationTime,
           nbRetriesInMount, nbMounts
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
      -- log "setBulkFileRecallResult : unable to identify Recall. giving up"
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
      logToDLF(varReqId, dlf.LVL_WARNING, dlf.RECALL_FAILED, inFileIds(i), varNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTrId) || ' TPVID=' || varVID ||
               ' fseq=' || TO_CHAR(inFseqs(i)) || ' errorMessage="' || inErrorMsgs(i) ||'" '|| inLogContext);
      retryOrFailRecall(varCfId, varVID, varReqId, inLogContext);
    END IF;
    COMMIT;
  END LOOP;
  -- log "setBulkFileRecallResult: bulk recall completed"
  varParams := 'mountTransactionId='|| to_char(inMountTrId)
               ||' NbFiles='|| inFileIds.COUNT ||' '|| inLogContext
               ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
               ||' AvgProcessingTime='|| trunc(getSecs(varStartTime, SYSTIMESTAMP)/inFileIds.COUNT, 6);
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
