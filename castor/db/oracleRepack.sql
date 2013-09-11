/*******************************************************************
 *
 * PL/SQL code for Repack
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/


/* PL/SQL method to process bulk Repack abort requests */
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
  varCreationTime NUMBER;
  rIpAddress INTEGER;
  rport INTEGER;
  rReqUuid VARCHAR2(2048);
  varVID VARCHAR2(10);
  varStartTime NUMBER;
  varOldStatus INTEGER;
BEGIN
  -- lock and mark the repack request as being aborted
  SELECT status, repackVID INTO varOldStatus, varVID
    FROM StageRepackRequest
   WHERE reqId = parentUUID
   FOR UPDATE;
  IF varOldStatus = tconst.REPACK_SUBMITTED THEN
    -- the request was just submitted, simply drop it from the queue
    DELETE FROM StageRepackRequest WHERE reqId = parentUUID;
    logToDLF(parentUUID, dlf.LVL_SYSTEM, dlf.REPACK_ABORTED, 0, '', 'repackd', 'TPVID=' || varVID);
    COMMIT;
    -- and return an empty cursor - not that nice...
    OPEN rSubResults FOR
      SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
    RETURN;
  END IF;
  UPDATE StageRepackRequest SET status = tconst.REPACK_ABORTING
   WHERE reqId = parentUUID;
  COMMIT;  -- so to make the status change visible
  BEGIN
    varStartTime := getTime();
    logToDLF(parentUUID, dlf.LVL_SYSTEM, dlf.REPACK_ABORTING, 0, '', 'repackd', 'TPVID=' || varVID);
    -- get unique ids for the request and the client and get current time
    SELECT ids_seq.nextval INTO reqId FROM DUAL;
    SELECT ids_seq.nextval INTO clientId FROM DUAL;
    varCreationTime := getTime();
    -- insert the request itself
    INSERT INTO StageAbortRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName,
      userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
    VALUES (0, userName, euid, egid, 0, pid, machine, '', '', uuidgen(),
      varCreationTime, varCreationTime, parentUUID, reqId, 0, clientId);
    -- insert the client information
    INSERT INTO Client (ipAddress, port, version, secure, id)
    VALUES (clientIP, 0, 0, 0, clientId);
    -- process the abort
    processBulkAbort(reqId, rIpAddress, rport, rReqUuid);
    -- mark the repack request as ABORTED
    UPDATE StageRepackRequest SET status = tconst.REPACK_ABORTED WHERE reqId = parentUUID;
    logToDLF(parentUUID, dlf.LVL_SYSTEM, dlf.REPACK_ABORTED, 0, '', 'repackd',
      'TPVID=' || varVID || ' elapsedTime=' || to_char(getTime() - varStartTime));
    -- return all results
    OPEN rSubResults FOR
      SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
  EXCEPTION WHEN OTHERS THEN
    -- Something went wrong when aborting: log and fail
    UPDATE StageRepackRequest SET status = tconst.REPACK_FAILED WHERE reqId = parentUUID;
    logToDLF(parentUUID, dlf.LVL_ERROR, dlf.REPACK_ABORTED_FAILED, 0, '', 'repackd', 'TPVID=' || varVID
      || ' errorMessage="' || SQLERRM || '" stackTrace="' || dbms_utility.format_error_backtrace ||'"');
    COMMIT;
    RAISE;
  END;
END;
/


/* PL/SQL method to submit a Repack request. This method returns immediately
   and the processing is done asynchronously by a DB job. */
CREATE OR REPLACE PROCEDURE submitRepackRequest
  (inMachine IN VARCHAR2,
   inEuid IN INTEGER,
   inEgid IN INTEGER,
   inPid IN INTEGER,
   inUserName IN VARCHAR2,
   inSvcClassName IN VARCHAR2,
   clientIP IN INTEGER,
   reqVID IN VARCHAR2) AS
  varClientId INTEGER;
  varSvcClassId INTEGER;
  varReqUUID VARCHAR2(36);
BEGIN
  -- do prechecks and get the service class
  varSvcClassId := insertPreChecks(inEuid, inEgid, inSvcClassName, 119);
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP, 0, 0, 0, ids_seq.nextval)
  RETURNING id INTO varClientId;
  varReqUUID := uuidGen();
  -- insert the request in status SUBMITTED, the SubRequests will be created afterwards
  INSERT INTO StageRepackRequest (reqId, machine, euid, egid, pid, userName, svcClassName, svcClass, client, repackVID,
    userTag, flags, mask, creationTime, lastModificationTime, status, fileCount, totalSize, id)
  VALUES (varReqUUID, inMachine, inEuid, inEgid, inPid, inUserName, inSvcClassName, varSvcClassId, varClientId, reqVID,
    varReqUUID, 0, 0, getTime(), getTime(), tconst.REPACK_SUBMITTED, 0, 0, ids_seq.nextval);
  COMMIT;
  logToDLF(varReqUUID, dlf.LVL_SYSTEM, dlf.REPACK_SUBMITTED, 0, '', 'repackd', 'TPVID=' || reqVID);
END;
/

/* PL/SQL method to handle a Repack request. This is performed as part of a DB job. */
CREATE OR REPLACE PROCEDURE handleRepackRequest(inReqUUID IN VARCHAR2,
                                                outNbFilesProcessed OUT INTEGER, outNbFailures OUT INTEGER) AS
  varEuid INTEGER;
  varEgid INTEGER;
  svcClassId INTEGER;
  varRepackVID VARCHAR2(10);
  varReqId INTEGER;
  cfId INTEGER;
  dcId INTEGER;
  repackProtocol VARCHAR2(2048);
  nsHostName VARCHAR2(2048);
  lastKnownFileName VARCHAR2(2048);
  varRecallGroupId INTEGER;
  varRecallGroupName VARCHAR2(2048);
  varCreationTime NUMBER;
  unused INTEGER;
  firstCF boolean := True;
  isOngoing boolean := False;
  varTotalSize INTEGER := 0;
BEGIN
  UPDATE StageRepackRequest SET status = tconst.REPACK_STARTING
   WHERE reqId = inReqUUID
  RETURNING id, euid, egid, svcClass, repackVID
    INTO varReqId, varEuid, varEgid, svcClassId, varRepackVID;
  -- commit so that the status change is visible, even if the request is empty for the moment
  COMMIT;
  outNbFilesProcessed := 0;
  outNbFailures := 0;
  -- Check which protocol should be used for writing files to disk
  repackProtocol := getConfigOption('Repack', 'Protocol', 'rfio');
  -- creation time for the subrequests
  varCreationTime := getTime();
  -- name server host name
  nsHostName := getConfigOption('stager', 'nsHost', '');
  -- find out the recallGroup to be used for this request
  getRecallGroup(varEuid, varEgid, varRecallGroupId, varRecallGroupName);
  -- update potentially missing Cns_file_metadata.stagertime. This will be dropped in 2.1.15.
  updateStagerTime@RemoteNS(varRepackVID);
  COMMIT;
  -- Get the list of files to repack from the NS DB via DBLink and store them in memory
  -- in a temporary table. We do that so that we do not keep an open cursor for too long
  -- in the nameserver DB
  -- Note the truncation of stagerTime to 5 digits. This is needed for consistency with
  -- the stager code that uses the OCCI api and thus loses precision when recuperating
  -- 64 bits integers into doubles (lack of support for 64 bits numbers in OCCI)
  INSERT INTO RepackTapeSegments (fileId, lastOpenTime, blockId, fseq, segSize, copyNb, fileClass, allSegments)
    (SELECT s_fileid, TRUNC(stagertime,5), blockid, fseq, segSize,
            copyno, fileclass,
            (SELECT LISTAGG(TO_CHAR(oseg.copyno)||','||oseg.vid, ',')
             WITHIN GROUP (ORDER BY copyno)
               FROM Cns_Seg_Metadata@remotens oseg
              WHERE oseg.s_fileid = seg.s_fileid
                AND oseg.s_status = '-'
              GROUP BY oseg.s_fileid)
       FROM Cns_Seg_Metadata@remotens seg, Cns_File_Metadata@remotens fileEntry
      WHERE seg.vid = varRepackVID
        AND seg.s_fileid = fileEntry.fileid
        AND seg.s_status = '-'
        AND fileEntry.status != 'D');
  FOR segment IN (SELECT * FROM RepackTapeSegments) LOOP
    DECLARE
      varSubreqId INTEGER;
      varSubreqUUID VARCHAR2(2048);
      varSrStatus INTEGER := dconst.SUBREQUEST_REPACK;
      varMjStatus INTEGER := tconst.MIGRATIONJOB_PENDING;
      varNbCopies INTEGER;
      varSrErrorCode INTEGER := 0;
      varSrErrorMsg VARCHAR2(2048) := NULL;
      varWasRecalled NUMBER;
      varMigrationTriggered BOOLEAN := False;
    BEGIN
      outNbFilesProcessed := outNbFilesProcessed + 1;
      varTotalSize := varTotalSize + segment.segSize;
      -- Commit from time to time
      IF MOD(outNbFilesProcessed, 1000) = 0 THEN
        COMMIT;
        firstCF := TRUE;
      END IF;
      -- lastKnownFileName we will have in the DB
      lastKnownFileName := CONCAT('Repack_', TO_CHAR(segment.fileid));
      -- find the Castorfile (and take a lock on it)
      DECLARE
        locked EXCEPTION;
        PRAGMA EXCEPTION_INIT (locked, -54);
      BEGIN
        -- This may raise a Locked exception as we do not want to wait for locks (except on first file).
        -- In such a case, we commit what we've done so far and retry this file, this time waiting for the lock.
        -- The reason for such a complex code is to avoid commiting each file separately, as it would be
        -- too heavy. On the other hand, we still need to avoid dead locks.
        -- Note that we pass 0 for the subrequest id, thus the subrequest will not be attached to the
        -- CastorFile. We actually attach it when we create it.
        selectCastorFileInternal(segment.fileid, nsHostName, segment.fileclass,
                                 segment.segSize, lastKnownFileName, 0, segment.lastOpenTime, firstCF, cfid, unused);
        firstCF := FALSE;
      EXCEPTION WHEN locked THEN
        -- commit what we've done so far
        COMMIT;
        -- And lock the castorfile (waiting this time)
        selectCastorFileInternal(segment.fileid, nsHostName, segment.fileclass,
                                 segment.segSize, lastKnownFileName, 0, segment.lastOpenTime, TRUE, cfid, unused);
      END;
      -- create  subrequest for this file.
      -- Note that the svcHandler is not set. It will actually never be used as repacks are handled purely in PL/SQL
      INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, status, request, getNextStatus, reqType)
      VALUES (0, lastKnownFileName, repackProtocol, segment.segSize, 0, uuidGen(), 0, 0, varCreationTime, varCreationTime, 1, 0, '', NULL, 'NotNullNeeded', ids_seq.nextval, 0, cfId, dconst.SUBREQUEST_START, varReqId, 0, 119)
      RETURNING id, subReqId INTO varSubreqId, varSubreqUUID;
      -- if the file is being overwritten, fail
      SELECT count(DiskCopy.id) INTO varNbCopies
        FROM DiskCopy
       WHERE DiskCopy.castorfile = cfId
         AND DiskCopy.status = dconst.DISKCOPY_STAGEOUT;
      IF varNbCopies > 0 THEN
        varSrStatus := dconst.SUBREQUEST_FAILED;
        varSrErrorCode := serrno.EBUSY;
        varSrErrorMsg := 'File is currently being overwritten';
        outNbFailures := outNbFailures + 1;
      ELSE
        -- find out whether this file is already on disk
        SELECT /*+ INDEX(DC I_DiskCopy_CastorFile) */ count(DC.id)
          INTO varNbCopies
          FROM DiskCopy DC, FileSystem, DiskServer
         WHERE DC.castorfile = cfId
           AND DC.fileSystem = FileSystem.id
           AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
           AND FileSystem.diskserver = DiskServer.id
           AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
           AND DiskServer.hwOnline = 1
           AND DC.status = dconst.DISKCOPY_VALID;
        IF varNbCopies = 0 THEN
          -- find out whether this file is already being recalled
          SELECT count(*) INTO varWasRecalled FROM RecallJob WHERE castorfile = cfId AND ROWNUM < 2;
          IF varWasRecalled = 0 THEN
            -- trigger recall
            triggerRepackRecall(cfId, segment.fileid, nsHostName, segment.blockid,
                                segment.fseq, segment.copyNb, varEuid, varEgid,
                                varRecallGroupId, svcClassId, varRepackVID, segment.segSize,
                                segment.fileclass, segment.allSegments, inReqUUID, varSubreqUUID, varRecallGroupName);
          END IF;
          -- file is being recalled
          varSrStatus := dconst.SUBREQUEST_WAITTAPERECALL;
          isOngoing := TRUE;
        END IF;
        -- deal with migrations
        IF varSrStatus = dconst.SUBREQUEST_WAITTAPERECALL THEN
          varMJStatus := tconst.MIGRATIONJOB_WAITINGONRECALL;
        END IF;
        DECLARE
          noValidCopyNbFound EXCEPTION;
          PRAGMA EXCEPTION_INIT (noValidCopyNbFound, -20123);
          noMigrationRoute EXCEPTION;
          PRAGMA EXCEPTION_INIT (noMigrationRoute, -20100);
        BEGIN
          triggerRepackMigration(cfId, varRepackVID, segment.fileid, segment.copyNb, segment.fileclass,
                                 segment.segSize, segment.allSegments, varMJStatus, varMigrationTriggered);
          IF varMigrationTriggered THEN
            -- update CastorFile tapeStatus
            UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_NOTONTAPE WHERE id = cfId;
          END IF;
          isOngoing := True;
        EXCEPTION WHEN noValidCopyNbFound OR noMigrationRoute THEN
          -- cleanup recall part if needed
          IF varWasRecalled = 0 THEN
            DELETE FROM RecallJob WHERE castorFile = cfId;
          END IF;
          -- fail SubRequest
          varSrStatus := dconst.SUBREQUEST_FAILED;
          varSrErrorCode := serrno.EINVAL;
          varSrErrorMsg := SQLERRM;
          outNbFailures := outNbFailures + 1;
        END;
      END IF;
      -- update SubRequest
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
         SET status = varSrStatus,
             errorCode = varSrErrorCode,
             errorMessage = varSrErrorMsg
       WHERE id = varSubreqId;
    EXCEPTION WHEN OTHERS THEN
      -- something went wrong: log "handleRepackRequest: unexpected exception caught"
      outNbFailures := outNbFailures + 1;
      varSrErrorMsg := 'Oracle error caught : ' || SQLERRM;
      logToDLF(NULL, dlf.LVL_ERROR, dlf.REPACK_UNEXPECTED_EXCEPTION, segment.fileId, nsHostName, 'repackd',
        'errorCode=' || to_char(SQLCODE) ||' errorMessage="' || varSrErrorMsg
        ||'" stackTrace="' || dbms_utility.format_error_backtrace ||'"');
      -- cleanup and fail SubRequest
      IF varWasRecalled = 0 THEN
        DELETE FROM RecallJob WHERE castorFile = cfId;
      END IF;
      IF varMigrationTriggered THEN
        DELETE FROM MigrationJob WHERE castorFile = cfId;
        DELETE FROM MigratedSegment WHERE castorFile = cfId;
      END IF;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.SEINTERNAL,
             errorMessage = varSrErrorMsg
       WHERE id = varSubreqId;
    END;
  END LOOP;
  -- cleanup RepackTapeSegments
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RepackTapeSegments';
  -- update status of the RepackRequest
  IF isOngoing THEN
    UPDATE StageRepackRequest
       SET status = tconst.REPACK_ONGOING,
           fileCount = outNbFilesProcessed,
           totalSize = varTotalSize
     WHERE StageRepackRequest.id = varReqId;
  ELSE
    IF outNbFailures > 0 THEN
      UPDATE StageRepackRequest
         SET status = tconst.REPACK_FAILED,
             fileCount = outNbFilesProcessed,
             totalSize = varTotalSize
       WHERE StageRepackRequest.id = varReqId;
    ELSE
      -- CASE of an 'empty' repack : the tape had no files at all
      UPDATE StageRepackRequest
         SET status = tconst.REPACK_FINISHED,
             fileCount = 0,
             totalSize = 0
       WHERE StageRepackRequest.id = varReqId;
    END IF;
  END IF;
  COMMIT;
END;
/

/* PL/SQL procedure implementing triggerRepackMigration */
CREATE OR REPLACE PROCEDURE triggerRepackMigration
(cfId IN INTEGER, vid IN VARCHAR2, fileid IN INTEGER, copyNb IN INTEGER,
 fileclass IN INTEGER, fileSize IN INTEGER, allSegments IN VARCHAR2,
 inMJStatus IN INTEGER, migrationTriggered OUT boolean) AS
  varMjId INTEGER;
  varNb INTEGER;
  varDestCopyNb INTEGER;
  varNbCopies INTEGER;
  varAllSegments strListTable;
BEGIN
  -- check whether we already have a migrationJob for this copy of the file
  SELECT id INTO varMjId
    FROM MigrationJob
   WHERE castorFile = cfId
     AND destCopyNb = copyNb;
  -- we have a migrationJob for this copy ! This means that the file has been overwritten
  -- and the new version is about to go to tape. We thus don't have anything to do
  -- for this file
  migrationTriggered := False;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no migration job for this copyNb, we can proceed
  -- first let's parse the list of all segments
  SELECT * BULK COLLECT INTO varAllSegments
    FROM TABLE(strTokenizer(allSegments));
  DECLARE
    varAllCopyNbs "numList" := "numList"();
    varAllVIDs strListTable := strListTable();
  BEGIN
    FOR i IN varAllSegments.FIRST .. varAllSegments.LAST/2 LOOP
      varAllCopyNbs.EXTEND;
      varAllCopyNbs(i) := TO_NUMBER(varAllSegments(2*i-1));
      varAllVIDs.EXTEND;
      varAllVIDs(i) := varAllSegments(2*i);
    END LOOP;
    -- find the new copy number to be used. This is the minimal one
    -- that is lower than the allowed number of copies and is not
    -- already used by an ongoing migration or by a valid migrated copy
    SELECT nbCopies INTO varNbCopies FROM FileClass WHERE classId = fileclass;
    FOR i IN 1 .. varNbCopies LOOP
      -- if we are reusing the original copy number, it's ok
      IF i = copyNb THEN
        varDestCopyNb := i;
      ELSE
        BEGIN
          -- check whether this copy number is already in use by a valid copy
          SELECT * INTO varNb FROM TABLE(varAllCopyNbs)
           WHERE COLUMN_VALUE = i;
          -- this copy number is in use, go to next one
        EXCEPTION WHEN NO_DATA_FOUND THEN
          BEGIN
            -- check whether this copy number is in use by an ongoing migration
            SELECT destCopyNb INTO varNb FROM MigrationJob WHERE castorFile = cfId AND destCopyNb = i;
            -- this copy number is in use, go to next one
          EXCEPTION WHEN NO_DATA_FOUND THEN
            -- copy number is not in use, we take it
            varDestCopyNb := i;
            EXIT;
          END;
        END;
      END IF;
    END LOOP;
    IF varDestCopyNb IS NULL THEN
      RAISE_APPLICATION_ERROR (-20123,
        'Unable to find a valid copy number for the migration of file ' ||
        TO_CHAR (fileid) || ' in triggerRepackMigration. ' ||
        'The file probably has too many valid copies.');
    END IF;
    -- create new migration
    initMigration(cfId, fileSize, vid, copyNb, varDestCopyNb, inMJStatus);
    -- create migrated segments for the existing segments if there are none
    SELECT /*+ INDEX_RS_ASC (MigratedSegment I_MigratedSegment_CFCopyNbVID) */ count(*) INTO varNb
      FROM MigratedSegment
     WHERE castorFile = cfId;
    IF varNb = 0 THEN
      FOR i IN varAllCopyNbs.FIRST .. varAllCopyNbs.LAST LOOP
        INSERT INTO MigratedSegment (castorFile, copyNb, VID)
        VALUES (cfId, varAllCopyNbs(i), varAllVIDs(i));
      END LOOP;
    END IF;
    -- all is fine, migration was triggered
    migrationTriggered := True;
  END;
END;
/

/* PL/SQL procedure implementing triggerRepackRecall
 * this triggers a recall in the repack context
 */
CREATE OR REPLACE PROCEDURE triggerRepackRecall
(inCfId IN INTEGER, inFileId IN INTEGER, inNsHost IN VARCHAR2, inBlock IN RAW,
 inFseq IN INTEGER, inCopynb IN INTEGER, inEuid IN INTEGER, inEgid IN INTEGER,
 inRecallGroupId IN INTEGER, inSvcClassId IN INTEGER, inVid IN VARCHAR2, inFileSize IN INTEGER,
 inFileClass IN INTEGER, inAllSegments IN VARCHAR2, inReqUUID IN VARCHAR2,
 inSubReqUUID IN VARCHAR2, inRecallGroupName IN VARCHAR2) AS
  varLogParam VARCHAR2(2048);
  varAllCopyNbs "numList" := "numList"();
  varAllVIDs strListTable := strListTable();
  varAllSegments strListTable;
  varFileClassId INTEGER;
BEGIN
  -- create recallJob for the given VID, copyNb, etc.
  INSERT INTO RecallJob (id, castorFile, copyNb, recallGroup, svcClass, euid, egid,
                         vid, fseq, status, fileSize, creationTime, blockId, fileTransactionId)
  VALUES (ids_seq.nextval, inCfId, inCopynb, inRecallGroupId, inSvcClassId,
          inEuid, inEgid, inVid, inFseq, tconst.RECALLJOB_PENDING, inFileSize, getTime(),
          inBlock, NULL);
  -- log "created new RecallJob"
  varLogParam := 'SUBREQID=' || inSubReqUUID || ' RecallGroup=' || inRecallGroupName;
  logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.RECALL_CREATING_RECALLJOB, inFileId, inNsHost, 'stagerd',
           varLogParam || ' fileClass=' || TO_CHAR(inFileClass) || ' copyNb=' || TO_CHAR(inCopynb)
           || ' TPVID=' || inVid || ' fseq=' || TO_CHAR(inFseq) || ' FileSize=' || TO_CHAR(inFileSize));
  -- create missing segments if needed
  SELECT * BULK COLLECT INTO varAllSegments
    FROM TABLE(strTokenizer(inAllSegments));
  FOR i IN 1 .. varAllSegments.COUNT/2 LOOP
    varAllCopyNbs.EXTEND;
    varAllCopyNbs(i) := TO_NUMBER(varAllSegments(2*i-1));
    varAllVIDs.EXTEND;
    varAllVIDs(i) := varAllSegments(2*i);
  END LOOP;
  SELECT id INTO varFileClassId
    FROM FileClass WHERE classId = inFileClass;
  createMJForMissingSegments(inCfId, inFileSize, varFileClassId, varAllCopyNbs,
                             varAllVIDs, inFileId, inNsHost, varLogParam);
END;
/


/* PL/SQL procedure implementing repackManager, the repack job
   that really handles all repack requests */
CREATE OR REPLACE PROCEDURE repackManager AS
  varReqUUID VARCHAR2(36);
  varRepackVID VARCHAR2(10);
  varStartTime NUMBER;
  varTapeStartTime NUMBER;
  varNbRepacks INTEGER;
  varNbFiles INTEGER;
  varNbFailures INTEGER;
BEGIN
  SELECT count(*) INTO varNbRepacks
    FROM StageRepackRequest
   WHERE status = tconst.REPACK_STARTING;
  IF varNbRepacks > 0 THEN
    -- this shouldn't happen, possibly this procedure is running in parallel: give up and log
    -- "repackManager: Repack processes still starting, no new ones will be started for this round"
    logToDLF(NULL, dlf.LVL_NOTICE, dlf.REPACK_JOB_ONGOING, 0, '', 'repackd', '');
    RETURN;
  END IF;
  varStartTime := getTime();
  WHILE TRUE LOOP
    varTapeStartTime := getTime();
    BEGIN
      -- get an outstanding repack to start, and lock to prevent
      -- a concurrent abort (there cannot be anyone else)
      SELECT reqId, repackVID INTO varReqUUID, varRepackVID
        FROM StageRepackRequest
       WHERE id = (SELECT id
                     FROM (SELECT id
                             FROM StageRepackRequest
                            WHERE status = tconst.REPACK_SUBMITTED
                            ORDER BY creationTime ASC)
                    WHERE ROWNUM < 2)
         FOR UPDATE;
      -- start the repack for this request: this can take some considerable time
      handleRepackRequest(varReqUUID, varNbFiles, varNbFailures);
      -- log 'Repack process started'
      logToDLF(varReqUUID, dlf.LVL_SYSTEM, dlf.REPACK_STARTED, 0, '', 'repackd',
        'TPVID=' || varRepackVID || ' nbFiles=' || TO_CHAR(varNbFiles)
        || ' nbFailures=' || TO_CHAR(varNbFailures)
        || ' elapsedTime=' || TO_CHAR(getTime() - varTapeStartTime));
      varNbRepacks := varNbRepacks + 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- if no new repack is found to start, terminate
      EXIT;
    END;
  END LOOP;
  IF varNbRepacks > 0 THEN
    -- log some statistics
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REPACK_JOB_STATS, 0, '', 'repackd',
      'nbStarted=' || TO_CHAR(varNbRepacks) || ' elapsedTime=' || TO_CHAR(TRUNC(getTime() - varStartTime)));
  END IF;
END;
/


/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name = 'REPACKMANAGERJOB')
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every 2 minutes executing the repackManager procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'RepackManagerJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN repackManager(); END;'', ''repackd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=2',
      ENABLED         => TRUE,
      COMMENTS        => 'Database job to manage the repack process');
END;
/
