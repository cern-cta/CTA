/*******************************************************************
 *
 * PL/SQL code for the disk-tape interface
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* Get next candidates for a given migration mount.
 * input:  VDQM transaction id, count and total size
 * output: outVid    the target VID,
           outRet    return code (-2: migration is over; -1: no more candidates; 0: candidates found)
           outFiles  a cursor for the set of migration candidates 
 * Locks are taken on the selected migration jobs.
 *
 * We should only propose a migration job for a file that does not
 * already have another copy migrated to the same tape.
 * The already migrated copies are kept in MigratedSegment until the whole set
 * of siblings has been migrated.
 */
CREATE OR REPLACE PROCEDURE tg_getBulkFilesToMigrate(inMountTrId IN NUMBER,
                                                     inCount IN INTEGER, inTotalSize IN INTEGER,
                                                     outFiles OUT castorTape.FileToMigrateCore_cur) AS
  varMountId NUMBER;
  varCount INTEGER;
  varTotalSize INTEGER;
  varVid VARCHAR2(10);
  varNewFseq INTEGER;
  varFileTrId NUMBER;
BEGIN
  BEGIN
    -- Extract id and tape gateway request and VID from the migration mount
    SELECT id, vid INTO varMountId, varVid
      FROM MigrationMount
     WHERE mountTransactionId = inMountTrId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- migration mount is over
    RETURN;
  END;
  -- Get last valid fseq and lock this mount
  SELECT lastFseq INTO varNewFseq
    FROM MigrationMount
   WHERE id = varMountId FOR UPDATE;
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
    varNewFseq := varNewFseq + 1;    -- we still decide the fseq for each migration candidate
    varCount := varCount + 1;
    varTotalSize := varTotalSize + Cand.fileSize;
    INSERT INTO FilesToMigrateHelper (fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fileSize, fseq)
      VALUES (Cand.fileId, Cand.nsHost, Cand.lastKnownFileName, Cand.filePath, ids_seq.NEXTVAL, Cand.fileSize, varNewFseq)
    RETURNING fileTransactionId INTO varFileTrId;
    -- Take this candidate on this mount
    UPDATE MigrationJob
       SET status = tconst.MIGRATIONJOB_SELECTED,
           vid = varVid,
           fSeq = varNewFseq,
           mountTransactionId = inMountTrId,
           fileTransactionId = varFileTrId
     WHERE id = Cand.mjId;
    IF varCount > inCount OR varTotalSize > inTotalSize THEN
      -- we have enough candidates for this round, exit loop
      EXIT;
    END IF;
  END LOOP;
  IF varCount > 0 THEN
    -- Update last fseq
    UPDATE MigrationMount
       SET lastFseq = varNewFseq
     WHERE id = varMountId;
    -- Return all candidates. Don't commit now, this will be done in C++
    -- after the results have been collected as the temporary table will be emptied. 
    OPEN outFiles FOR
      SELECT fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fseq, fileSize
        FROM FilesToMigrateHelper;
  END IF;
  -- ELSE no candidates found, return an empty cursor
END;
/


CREATE OR REPLACE PROCEDURE tg_setBulkFileMigrationResult(inMountTrId IN NUMBER,
                                                          inFileIds IN castor."cnumList",
                                                          inFileTrIds IN castor."cnumList",
                                                          inFseqs IN castor."cnumList",
                                                          inBlockIds IN castor."strList",
                                                          inChecksumTypes IN castor."strList",
                                                          inChecksums IN castor."cnumList",
                                                          inComprSizes IN castor."cnumList",
                                                          inTransferredSizes IN castor."cnumList",
                                                          inErrorCodes IN castor."cnumList",
                                                          inErrorMsgs IN castor."strList",
                                                          inLogContext IN VARCHAR2) AS
  varStartTime TIMESTAMP;
  varNsHost VARCHAR2(2048);
  varReqId VARCHAR2(36);
  varLastUpdTime NUMBER;
  varCopyNo NUMBER;
  varOldCopyNo NUMBER;
  varVid VARCHAR2(10);
  varNSTimeInfos "numList";
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
        -- Log 'file not found, giving up'
        varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' '|| inLogContext;
        logToDLF(varReqid, dlf.LVL_ERROR, dlf.MIGRATION_NOT_FOUND, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
      END;
    ELSE
      -- Fail/retry this migration, log 'migration failed, will retry if allowed'
      varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' ErrorCode='|| to_char(inErrorCodes(i))
                   ||' ErrorMessage="'|| inErrorMsgs(i) ||'" VID='|| varVid
                   ||' copyNb='|| to_char(varCopyNo) ||' '|| inLogContext;
      logToDLF(varReqid, dlf.LVL_WARNING, dlf.MIGRATION_RETRY, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
      -- The following call commits in an autonomous transaction!
      retryOrFailMigration(inMountTrId, inFileIds(i), varNsHost, inErrorCodes(i), varReqId);
    END IF;
  END LOOP;

  -- "Bulk" transfer data to the NS DB (FORALL is not supported here)
  INSERT INTO SetSegmentsForFilesHelper@RemoteNS
    SELECT * FROM FileMigrationResultsHelper;
  
  -- This call autocommits all segments in the NameServer
  setOrReplaceSegmentsForFiles@RemoteNS(varReqId);
  
  -- Retrieve results from the NS DB in bulk and clean result data
  SELECT timeinfo, ec, msg, fileId, params
    BULK COLLECT INTO varNSTimeInfos, varNSErrorCodes, varNSMsgs, varNSFileIds, varNSParams
    FROM ResultsLogHelper@RemoteNS
   WHERE reqId = varReqId;
  DELETE FROM ResultsLogHelper@RemoteNS
   WHERE reqId = varReqId;

  -- Process the results
  FOR i IN varNSFileIds.FIRST .. varNSFileIds.LAST LOOP
    -- First log on behalf of the NS
    INSERT INTO DLFLogs (timeinfo, uuid, priority, msg, fileId, nsHost, source, params)
      VALUES (varNSTimeinfos(i), varReqid,
              CASE varNSErrorCodes(i) WHEN 0 THEN dlf.LVL_SYSTEM ELSE dlf.LVL_ERROR END,
              varNSMsgs(i), varNSFileIds(i), varNsHost, 'nsd', varNSParams(i));
    
    -- Now process file by file, depending on the result
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
  -- Final log
  varParams := 'Function="tg_setBulkFileMigrationResult" mountTransactionId='|| to_char(inMountTrId)
               ||' NbFiles='|| inFileIds.COUNT ||' '|| inLogContext
               ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
               ||' AvgProcessingTime='|| getSecs(varStartTime, SYSTIMESTAMP)/inFileIds.COUNT;
  logToDLF(varReqid, dlf.LVL_SYSTEM, dlf.BULK_MIGRATION_COMPLETED, 0, '', 'tapegatewayd', varParams);
END;
/


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
    -- another migration ongoing, keep track of this one 
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
    SELECT /*+ INDEX(SR I_Subrequest_Castorfile) */ SubRequest.id INTO varSrId
      FROM SubRequest, StageRepackRequest
      WHERE SubRequest.castorfile = inCfId
        AND SubRequest.status = dconst.SUBREQUEST_REPACK
        AND SubRequest.request = StageRepackRequest.id
        AND StageRepackRequest.RepackVID = inOriginVID;
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


/*** Recall ***/
/* XXX move from oracleTapeGateway */
