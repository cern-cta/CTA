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
  TYPE VIDRec IS RECORD (vid VARCHAR2(2048));
  TYPE VID_Cur IS REF CURSOR RETURN VIDRec;
  TYPE VIDPriorityRec IS RECORD (vid VARCHAR2(2048), vdqmPriority INTEGER);
  TYPE VIDPriority_Cur IS REF CURSOR RETURN VIDPriorityRec;
  TYPE DbMigrationInfo IS RECORD (
    id NUMBER,
    copyNb NUMBER,
    fileName VARCHAR2(2048),
    nsHost VARCHAR2(2048),
    fileId NUMBER,
    fileSize NUMBER);
  TYPE DbMigrationInfo_Cur IS REF CURSOR RETURN DbMigrationInfo;
  TYPE FileToRecallCore IS RECORD (
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   diskserver VARCHAR2(2048),
   mountPoint VARCHAR(2048),
   path VARCHAR2(2048),
   fseq INTEGER,
   fileTransactionId NUMBER,
   blockId RAW(4));
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
       SET lastvdqmpingtime = gettime(),
           mountTransactionId     = inVdqmId,
           status           = tconst.MIGRATIONMOUNT_WAITDRIVE,
           label            = inLabel,
           density          = inDensity
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
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
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
CREATE OR REPLACE
PROCEDURE tg_endTapeSession(inMountTransactionId IN NUMBER,
                            inErrorCode IN INTEGER) AS
  varMjIds "numList";    -- recall/migration job Ids
  varTgrId INTEGER;
BEGIN
  -- try to delete the migration mount (may not be one)
  DELETE FROM MigrationMount
   WHERE mountTransactionId = inMountTransactionId
  RETURNING tapeGatewayRequestId INTO varTgrId;
  IF SQL%ROWCOUNT > 0 THEN
    -- it was a migration mounts
    -- find and lock the MigrationJobs.
    SELECT id BULK COLLECT INTO varMjIds
      FROM MigrationJob
     WHERE tapeGatewayRequestId = varTgrId
       FOR UPDATE;
    -- Process the write case
    -- just resurrect the remaining migrations
    UPDATE MigrationJob
       SET status = tconst.MIGRATIONJOB_PENDING,
           VID = NULL,
           TapeGatewayRequestId = NULL
     WHERE id IN (SELECT * FROM TABLE(varMjIds))
       AND status = tconst.MIGRATIONJOB_SELECTED;
  ELSE
    -- was not a migration session, let's try a recall one
    DECLARE
      varVID VARCHAR2(2048);
      varRjIds "numList";
    BEGIN
      DELETE FROM RecallMount
       WHERE mountTransactionId = inMountTransactionId
      RETURNING VID INTO varVID;
      IF SQL%ROWCOUNT > 0 THEN
        -- it was a recall mount
        -- find and reset the all RecallJobs of files for this VID
        UPDATE RecallJob
           SET status    = tconst.RECALLJOB_NEW
         WHERE castorFile IN (SELECT castorFile
                                FROM RecallJob
                               WHERE VID = varVID
                                 AND status IN (tconst.RECALLJOB_PENDING,
                                                tconst.RECALLJOB_SELECTED,
                                                tconst.RECALLJOB_RETRYMOUNT));
      ELSE
        -- Small infusion of paranoia ;-) We should never reach that point...
        ROLLBACK;
        RAISE_APPLICATION_ERROR (-20119,
         'No tape or migration mount found on second pass for mountTransactionId ' ||
         inMountTransactionId || ' in tg_endTapeSession');
      END IF;
    END;
  END IF;
END;
/

/* mark a migration or recall as failed saving in the db the error code associated with the failure */
CREATE OR REPLACE
PROCEDURE tg_failFileTransfer(inMountTransactionId IN NUMBER,
                              inFileId IN NUMBER,
                              inNsHost IN VARCHAR2,
                              inErrorCode IN NUMBER) AS
  varVID VARCHAR2(2048);
  varCfId NUMBER;
BEGIN
  -- Lock related castorfile
  SELECT CF.id INTO varCfId 
    FROM CastorFile CF
   WHERE CF.fileid = inFileId
     AND CF.nsHost = inNsHost 
    FOR UPDATE;
  -- suppose it's a recall case
  SELECT VID INTO varVID
    FROM RecallMount
   WHERE mountTransactionId = inMountTransactionId;
  -- mark RecallJob for retry
  retryRecall(varCfId, varVID);
EXCEPTION WHEN  NO_DATA_FOUND THEN
  -- so it must be a migration case
  BEGIN
    SELECT VID INTO varVID
      FROM MigrationMount
     WHERE mountTransactionId = inMountTransactionId;
    UPDATE MigrationJob
       SET status    = tconst.MIGRATIONJOB_RETRY,
           errorcode = inErrorCode,
           vid       = NULL
     WHERE castorFile = varCfId
       AND VID = varVID; 
  EXCEPTION WHEN  NO_DATA_FOUND THEN
    -- not a migration either -> complain in case of failure
    ROLLBACK;
    RAISE_APPLICATION_ERROR(-20119, 'No tape or migration mount found on second pass for mountTransactionId ' ||
                            inMountTransactionId || ' in tg_failFileTransfer');
  END;
END;
/

/* retrieve from the db all the migration jobs that faced a failure */
CREATE OR REPLACE
PROCEDURE tg_getFailedMigrations(outFailedMigrationJob_c OUT SYS_REFCURSOR) AS
  varFailedIds "numList";
BEGIN
  /* Find and lock the migration jobs we will work on */
  SELECT mj.id
    BULK COLLECT INTO varFailedIds
    FROM MigrationJob mj
   WHERE mj.status = tconst.MIGRATIONJOB_RETRY
     AND ROWNUM < 1000
     FOR UPDATE SKIP LOCKED;
  /* Report theirs Id, joined with other information */
  OPEN outFailedMigrationJob_c FOR
    SELECT mj.id, mj.errorCode, mj.nbRetry, cf.fileId, cf.nsHost
      FROM MigrationJob mj
      LEFT OUTER JOIN castorfile cf ON cf.id = mj.castorfile
     WHERE mj.id IN (SELECT * FROM TABLE (varFailedIds));
END;
/

/* default migration candidate selection policy */
CREATE OR REPLACE
PROCEDURE tg_defaultMigrSelPolicy(inMountId IN INTEGER,
                                  outDiskServerName OUT NOCOPY VARCHAR2,
                                  outMountPoint OUT NOCOPY VARCHAR2,
                                  outPath OUT NOCOPY VARCHAR2,
                                  outDiskCopyId OUT INTEGER,
                                  outLastKnownFileName OUT NOCOPY VARCHAR2,
                                  outFileId OUT INTEGER,
                                  outNsHost OUT NOCOPY VARCHAR2,
                                  outFileSize OUT INTEGER,
                                  outMigJobId OUT INTEGER,
                                  outLastUpdateTime OUT INTEGER) AS
  /* Find the next file to migrate for a given migration mount.
   *
   * Procedure's input: migration mount id
   * Procedure's output: non-zero MigrationJob ID
   *
   * Lock taken on the migration job if it selects one.
   *
   * Per policy we should only propose a migration job for a file that does not
   * already have another copy migrated to the same tape.
   * The already migrated copies are kept in MigratedSegment until the whole set
   * of siblings has been migrated.
   */
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
  CURSOR c IS
    SELECT /*+ FIRST_ROWS_1
               LEADING(MigrationMount MigrationJob CastorFile DiskCopy FileSystem DiskServer)
               USE_NL(MigrationMount MigrationJob CastorFile DiskCopy FileSystem DiskServer)
               INDEX(CastorFile PK_CastorFile_Id)
               INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile)
               INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusId) */
           DiskServer.name, FileSystem.mountPoint, DiskCopy.path, DiskCopy.id, CastorFile.lastKnownFilename,
           CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, MigrationJob.id, CastorFile.lastUpdateTime
      FROM MigrationMount, MigrationJob, DiskCopy, FileSystem, DiskServer, CastorFile
     WHERE MigrationMount.id = inMountId
       AND MigrationJob.tapePool = MigrationMount.tapePool
       AND MigrationJob.status = tconst.MIGRATIONJOB_PENDING
       AND CastorFile.id = MigrationJob.castorFile
       AND CastorFile.id = DiskCopy.castorFile
       AND DiskCopy.status = dconst.DISKCOPY_CANBEMIGR
       AND FileSystem.id = DiskCopy.fileSystem
       AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
       AND DiskServer.id = FileSystem.diskServer
       AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
       AND (MigrationMount.VID IS NULL OR 
            MigrationMount.VID NOT IN (SELECT /*+ INDEX_RS_ASC(MigratedSegment I_MigratedSegment_CFCopyNBVID) */ vid
                                        FROM MigratedSegment
                                       WHERE castorFile = MigrationJob.castorfile
                                         AND copyNb != MigrationJob.destCopyNb))
       FOR UPDATE OF MigrationJob.id SKIP LOCKED;
BEGIN
  OPEN c;
  FETCH c INTO outDiskServerName, outMountPoint, outPath, outDiskCopyId, outLastKnownFileName,
               outFileId, outNsHost, outFileSize, outMigJobId, outLastUpdateTime;
  CLOSE c;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Nothing to migrate. Simply return
  CLOSE c;
END;
/

/* get a candidate for migration */
CREATE OR REPLACE
PROCEDURE tg_getFileToMigrate(
  inVDQMtransacId IN  NUMBER,
  outRet           OUT INTEGER,
  outVid        OUT NOCOPY VARCHAR2,
  outputFile    OUT castorTape.FileToMigrateCore_cur) AS
  -- This procedure finds the next file to migrate
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  varMountId NUMBER;
  varDiskServer VARCHAR2(2048);
  varMountPoint VARCHAR2(2048);
  varPath  VARCHAR2(2048);
  varDiskCopyId NUMBER;
  varFileId NUMBER;
  varNsHost VARCHAR2(2048);
  varFileSize  INTEGER;
  varMigJobId  INTEGER;
  varLastUpdateTime NUMBER;
  varLastKnownName VARCHAR2(2048);
  varTgRequestId NUMBER;
  varUnused INTEGER;
BEGIN
  outRet:=0;
  BEGIN
    -- Extract id and tape gateway request and VID from the migration mount
    SELECT id, TapeGatewayRequestId, vid INTO varMountId, varTgRequestId, outVid
      FROM MigrationMount
     WHERE mountTransactionId = inVDQMtransacId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    outRet:=-2;   -- migration mount is over
    RETURN;
  END;
  -- default policy is used in all cases in version 2.1.12
  tg_defaultMigrSelPolicy(varMountId,varDiskServer,varMountPoint,varPath,
    varDiskCopyId ,varLastKnownName, varFileId, varNsHost,varFileSize,
    varMigJobId,varLastUpdateTime);
  IF varMigJobId IS NULL THEN
    outRet := -1; -- the migration selection policy didn't find any candidate
    RETURN;
  END IF;

  DECLARE
    varNewFseq NUMBER;
  BEGIN
   -- Atomically increment and read the next FSEQ to be written to. fSeq is held
   -- in the tape structure.
   UPDATE MigrationMount
      SET lastFseq = lastFseq + 1
     WHERE id = varMountId
   RETURNING lastFseq-1 into varNewFseq; -- The previous value is where we'll write

   -- Update the migration job and attach it to a newly created file transaction ID
   UPDATE MigrationJob
      SET status = tconst.MIGRATIONJOB_SELECTED,
          VID = outVID,
          fSeq = varNewFseq,
          tapeGatewayRequestId = varTgRequestId,
          fileTransactionId = TG_FileTrId_Seq.NEXTVAL
    WHERE id = varMigJobId;

   OPEN outputFile FOR
     SELECT varFileId,varNshost,varLastUpdateTime,varDiskServer,varMountPoint,
            varPath,varLastKnownName,fseq,varFileSize,fileTransactionId
       FROM MigrationJob
      WHERE id = varMigJobId;

  END;
  COMMIT;
END;
/

/* PL/SQL method implementing bestFileSystemForRecall */
CREATE OR REPLACE PROCEDURE bestFileSystemForRecall(inCfId IN INTEGER, diskServerName OUT VARCHAR2,
                                                    rmountPoint OUT VARCHAR2, rpath OUT VARCHAR2) AS
  varCfId INTEGER;
  fileSystemId NUMBER := 0;
  fsDiskServer NUMBER;
  fileSize NUMBER;
  nb NUMBER;
BEGIN
  -- try and select a good FileSystem for this recall
  FOR a IN (SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/
                   DiskServer.name, FileSystem.mountPoint, FileSystem.id,
                   FileSystem.diskserver, CastorFile.fileSize, CastorFile.fileId, CastorFile.nsHost
              FROM DiskServer, FileSystem, DiskPool2SvcClass,
                   (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClass from StageGetRequest                            UNION ALL
                    SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, svcClass from StagePrepareToGetRequest UNION ALL
                    SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, svcClass from StageRepackRequest                   UNION ALL
                    SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClass from StageUpdateRequest                   UNION ALL
                    SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcClass from StagePrepareToUpdateRequest) Request,
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
          ORDER BY -- first prefer DSs without concurrent migrators/recallers
                   DiskServer.nbRecallerStreams ASC, FileSystem.nbMigratorStreams ASC,
                   -- order by rate as defined by the function
                   fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                                  FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams) DESC,
                   -- use randomness to avoid preferring always the same FS when everything is idle
                   DBMS_Random.value)
  LOOP
    diskServerName := a.name;
    rmountPoint    := a.mountPoint;
    fileSystemId   := a.id;
    buildPathFromFileId(a.fileId, a.nsHost, ids_seq.nextval, rpath);
    -- Check that we don't already have a copy of this file on this filesystem.
    -- This will never happen in normal operations but may be the case if a filesystem
    -- was disabled and did come back while the tape recall was waiting.
    -- Even if we optimize by cancelling remaining unneeded tape recalls when a
    -- fileSystem comes back, the ones running at the time of the come back will have
    -- the problem.
    SELECT count(*) INTO nb
      FROM DiskCopy
     WHERE fileSystem = a.id
       AND castorfile = inCfid
       AND status = dconst.DISKCOPY_STAGED;
    IF nb != 0 THEN
      raise_application_error(-20103, 'Recaller could not find a FileSystem in production in the requested SvcClass and without copies of this file');
    END IF;
    RETURN;
  END LOOP;
  IF fileSystemId = 0 THEN
    raise_application_error(-20115, 'No suitable filesystem found for this recalled file');
  END IF;
END;
/

/* get a candidate for recall */
CREATE OR REPLACE
PROCEDURE tg_getFileToRecall (inMountTransactionId IN  NUMBER,
                              outRet OUT INTEGER,
                              outVid OUT NOCOPY VARCHAR2,
                              outFile OUT castorTape.FileToRecallCore_Cur) AS
  varDSName VARCHAR2(2048); -- Disk Server name
  varMPoint VARCHAR2(2048); -- Mount Point
  varPath   VARCHAR2(2048); -- Diskcopy path

  varPreviousFseq INTEGER;
  varRjId         INTEGER;
  varCfId         INTEGER;
  varNewFSeq      INTEGER;
BEGIN 
  BEGIN
    SELECT vid, lastProcessedFseq INTO outVid, varPreviousFseq
      FROM RecallMount
     WHERE mountTransactionId = inMountTransactionId
       FOR UPDATE;
  EXCEPTION WHEN  NO_DATA_FOUND THEN
     -- unknown request
     outRet := -2;
     RETURN;
  END; 

  BEGIN
    -- Find the unprocessed recallJob of this tape with lowest fSeq
    -- that is above the previous one. If none, take the recallJob with lowest fseq.
    SELECT id, fSeq, castorFile INTO varRjId, varNewFSeq, varCfId
      FROM (SELECT id, fSeq, castorFile
              FROM RecallJob
             WHERE vid = outvid  
               AND status = tconst.RECALLJOB_PENDING
             ORDER BY CASE WHEN fseq > varPreviousFseq THEN 1 ELSE 0 END DESC,
                      fseq ASC)
     WHERE ROWNUM < 2;
    -- Lock the corresponding castorfile
    SELECT id INTO varCfId
      FROM Castorfile
     WHERE id = varCfId 
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
     outRet := -1; -- NO MORE FILES
     COMMIT;
     RETURN;
  END;

  -- Find the best filesystem to recall the selected file
  DECLARE
    application_error EXCEPTION;
    PRAGMA EXCEPTION_INIT(application_error,-20115);
  BEGIN
    bestFileSystemForRecall(varCfId,varDSName,varMPoint,varPath);
  EXCEPTION WHEN application_error OR NO_DATA_FOUND THEN 
    outRet := -3;
    COMMIT;
    RETURN;
  END;

  -- update recallMount and RecallJob
  UPDATE RecallJob
     SET status = tconst.RECALLJOB_SELECTED,
         fileTransactionID = TG_FileTrId_Seq.NEXTVAL
   WHERE id = varRjId;
  -- Record this recall at the MigrationMount level
  UPDATE RecallMount
     SET lastProcessedFseq = varNewFSeq
   WHERE vid = outVid;

  -- return all needed info
  OPEN outFile FOR 
    SELECT Castorfile.fileid, Castorfile.nshost, varDSName, varMPoint,
           varPath, varNewFSeq, RecallJob.fileTransactionID, RecallJob.blockId
      FROM RecallJob, Castorfile
     WHERE RecallJob.id = varRjId
       AND Castorfile.id = RecallJob.castorfile;
END;
/

/* get the information from the db for a successful migration */
CREATE OR REPLACE
PROCEDURE tg_getRepackVidAndFileInfo(
  inFileId          IN  NUMBER, 
  inNsHost          IN VARCHAR2,
  inFseq            IN  INTEGER, 
  inMountTransactionId IN  NUMBER, 
  inBytesTransfered IN  NUMBER,
  outOriginalVID    OUT NOCOPY VARCHAR2,
  outOriginalCopyNb OUT NUMBER,
  outDestVID        OUT NOCOPY VARCHAR2,
  outDestCopyNb     OUT INTEGER, 
  outLastTime       OUT NUMBER,
  outFileClass      OUT NOCOPY VARCHAR2,
  outRet            OUT INTEGER) AS 
  varCfId              NUMBER;  -- CastorFile Id
  varFileSize          NUMBER;  -- Expected file size
  varTgrId             NUMBER;  -- Tape gateway request Id
BEGIN
  outOriginalVID := NULL;
  outOriginalCopyNb := NULL;
   -- ignore the repack state
  BEGIN
    SELECT CF.lastupdatetime, CF.id, CF.fileSize,      FC.name 
      INTO outLastTime,     varCfId, varFileSize, outFileClass
      FROM CastorFile CF 
      LEFT OUTER JOIN FileClass FC ON FC.Id = CF.FileClass
     WHERE CF.fileid = inFileId 
       AND CF.nshost = inNsHost;
     IF (outFileClass IS NULL) THEN
       outFileClass := 'UNKNOWN';
     END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR (-20119,
         'Castorfile not found for File ID='||inFileId||' and nshost = '||
           inNsHost||' in tg_getRepackVidAndFileInfo.');
  END;
  IF varFileSize <> inBytesTransfered THEN
  -- fail the file
    tg_failFileTransfer(inMountTransactionId, inFileId, inNsHost,  1613); -- wrongfilesize
    COMMIT;
    outRet := -1;
    RETURN;
  ELSE
    outRet:=0;
  END IF;
  
  BEGIN
    SELECT tapeGatewayRequestId INTO varTgrId
      FROM MigrationMount
     WHERE mountTransactionId = inMountTransactionId;
    BEGIN
      SELECT originalCopyNb, originalVID, destcopyNb, VID
        INTO outOriginalCopyNb, outOriginalVID, outDestCopyNb, outDestVID
        FROM MigrationJob
       WHERE TapeGatewayRequestId = varTgrId
         AND castorfile = varCfId
         AND fseq= inFseq;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR (-20119,
         'MigrationJob not found for castorfile='||varCFId||'(File ID='||inFileId||' and nshost = '||
           inNsHost||') and fSeq='||inFseq||' in tg_getRepackVidAndFileInfo.');
    END;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- nothing to do, we did not find any MigrationMount
    NULL;
  END;  
END;
/

/* get the migration mounts without any tape associated */
CREATE OR REPLACE
PROCEDURE tg_getMigMountsWithoutTapes(outStrList OUT SYS_REFCURSOR) AS
BEGIN
  -- get migration mounts  in state WAITTAPE with a non-NULL TapeGatewayRequestId
  OPEN outStrList FOR
    SELECT M.id, M.TapeGatewayRequestId, TP.name
      FROM MigrationMount M,Tapepool TP
     WHERE M.status = tconst.MIGRATIONMOUNT_WAITTAPE
       AND M.TapeGatewayRequestId IS NOT NULL
       AND M.tapepool=TP.id 
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
PROCEDURE tg_getTapeWithoutDriveReq(outMigrations OUT castorTape.VID_Cur,
                                    outRecalls OUT castorTape.VIDPriority_Cur) AS
BEGIN
  OPEN outMigrations FOR
    SELECT VID
      FROM MigrationMount
     WHERE status = tconst.MIGRATIONMOUNT_SEND_TO_VDQM
       FOR UPDATE SKIP LOCKED;
  OPEN outRecalls FOR
    SELECT RecallMount.VID, RecallGroup.vdqmPriority
      FROM RecallMount, RecallGroup
     WHERE RecallMount.status = tconst.RECALLMOUNT_NEW
       AND RecallMount.recallGroup = RecallGroup.id
       FOR UPDATE OF RecallMount.id SKIP LOCKED;
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
 FOR mountTransactionId IN inMountTransactionIds.FIRST .. inMountTransactionIds.LAST LOOP   
   tg_endTapeSession(mountTransactionId, 0);
 END LOOP;
 COMMIT;
END;
/

/* update the db after a successful migration - tape side */
CREATE OR REPLACE
PROCEDURE TG_SetFileMigrated(
  inFileId          IN  NUMBER,
  inNsHost          IN  VARCHAR2, 
  inFseq            IN  INTEGER, 
  inFileTransaction IN  NUMBER) AS
  varMigJobCount  INTEGER;
  varCfId         NUMBER;
  varCopyNb       NUMBER;
  varVID          VARCHAR2(10);
  varOrigVID      VARCHAR2(10);
BEGIN
  -- Lock the CastorFile
  SELECT CF.id INTO varCfId FROM CastorFile CF
   WHERE CF.fileid = inFileId 
     AND CF.nsHost = inNsHost 
     FOR UPDATE;
  -- delete the corresponding migration job, create the new migrated segment
  DELETE FROM MigrationJob
   WHERE fileTransactionId = inFileTransaction
     AND fSeq = inFseq
  RETURNING destCopyNb, VID, originalVID
    INTO varCopyNb, varVID, varOrigVID;
  -- check if another migration should be performed
  SELECT count(*) INTO varMigJobCount
    FROM MigrationJob
    WHERE castorfile = varCfId;
  IF varMigJobCount = 0 THEN
    -- no more migrations, delete all migrated segments 
    DELETE FROM MigratedSegment
     WHERE castorfile = varCfId;
  ELSE
    -- another migration ongoing, keep track of this one 
    INSERT INTO MigratedSegment VALUES (varCfId, varCopyNb, varVID);
  END IF;
  -- Tell the Disk Cache that migration is over
  dc_setFileMigrated(varCfId, varOrigVID, (varMigJobCount = 0));
  COMMIT;
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
    -- archive the repack subrequest associated to this migrationJob
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


/* update the db after a successful migration */
CREATE OR REPLACE
PROCEDURE TG_DropSuperfluousSegment(
  inFileId          IN  NUMBER,
  inNsHost          IN  VARCHAR2, 
  inFseq            IN  INTEGER, 
  inFileTransaction IN  NUMBER) AS
  varMigJobCount      INTEGER;
  varCfId               NUMBER;
  varCopyNb             NUMBER;
  varVID                VARCHAR2(2048);
BEGIN
  -- Lock the CastorFile
  SELECT CF.id INTO varCfId FROM CastorFile CF
   WHERE CF.fileid = inFileId 
     AND CF.nsHost = inNsHost 
     FOR UPDATE;
  -- delete the corresponding migration job, but skip creating the new migrated segment
  DELETE FROM MigrationJob
   WHERE FileTransactionId = inFileTransaction
     AND fSeq = inFseq
  RETURNING destCopyNb, VID INTO varCopyNb, varVID;
  -- check if another migration should be performed
  SELECT count(*) INTO varMigJobCount
    FROM MigrationJob
    WHERE castorfile = varCfId;
  IF varMigJobCount = 0 THEN
     -- Mark all disk copies as staged and delete all migrated segments together.
     UPDATE DiskCopy
        SET status= dconst.DISKCOPY_STAGED
      WHERE castorFile = varCfId
        AND status= dconst.DISKCOPY_CANBEMIGR;
     DELETE FROM MigratedSegment
      WHERE castorfile = varCfId;
  END IF;
  -- archive Repack requests should any be in the db
  FOR i IN (
    SELECT /*+ INDEX(SR I_Subrequest_Castorfile)*/ SR.id FROM SubRequest SR
    WHERE SR.castorfile = varCfId AND
          SR.status = dconst.SUBREQUEST_REPACK
    ) LOOP
    archiveSubReq(i.id, 8); -- SUBREQUEST_FINISHED
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
                                           inLastUpdateTime IN NUMBER) RETURN BOOLEAN AS
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
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_FILE_OVERWRITTEN, inFileId, inNsHost, 'tapegatewayd',
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
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_CREATED_CHECKSUM, inFileId, inNsHost, 'nsd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' CopyNo=' || inCopyNb ||
             ' TPVID=' || inVID || ' Fseq=' || TO_CHAR(inFseq) || ' checksumType='  || inCksumName ||
             ' checksumValue=' || TO_CHAR(inCksumValue));
  ELSE
    -- is the checksum matching ?
    -- note that this is probably useless as it was already checked at transfer time
    IF inCksumName = varNSCsumtype AND TO_CHAR(inCksumValue, 'XXXXXXXX') != varNSCsumvalue THEN
      -- not matching ! Retry the recall
      retryRecall(inCfId, inVID);
      -- log "setFileRecalled : bad checksum detected, will retry if allowed"
      logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_BAD_CHECKSUM, inFileId, inNsHost, 'tapegatewayd',
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
           errorCode = 2, -- ENOENT
           errorMessage = 'File disappeared from namespace during recall'
     WHERE castorFile = inCfId
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
  -- log "setFileRecalled : file was dropped from namespace during recall, giving up"
  logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_FILE_DROPPED, inFileId, inNsHost, 'tapegatewayd',
           'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
           ' fseq=' || TO_CHAR(inFseq) || ' CFLastUpdateTime=' || TO_CHAR(inLastUpdateTime));
  RETURN FALSE;
END;
/

/* update the db after a successful recall */
CREATE OR REPLACE PROCEDURE tg_setFileRecalled(inMountTransactionId IN INTEGER,
                                               inFseq IN INTEGER,
                                               inFilePath IN VARCHAR2,
                                               inCksumName IN VARCHAR2,
                                               inCksumValue IN INTEGER) AS
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

  -- Get RecallJob and lock Castorfile (+lock Castorfile)
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
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_NOT_FOUND, 0, '', 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath);
    RETURN;
  END;
  -- Check that the file is still there in the namespace (and did not get overwritten)
  -- Note that error handling and logging is done inside the function
  IF NOT checkRecallInNS(varCfId, inMountTransactionId, varVID, varCopyNb, inFseq, varFileId, varNsHost, 
                         inCksumName, inCksumValue, varLastUpdateTime) THEN RETURN; END IF;
  -- get diskserver, filesystem and path from full path in input
  BEGIN
    parsePath(inFilePath, varFSId, varDCPath, varDCId);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "setFileRecalled : unable to parse input path. giving up"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_INVALID_PATH, varFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath);
    RETURN;
  END;

  -- Then deal with recalljobs and potential migrationJobs

  -- Delete recall jobs
  DELETE FROM RecallJob WHERE castorFile = varCfId;
  -- trigger waiting migrations if any
  -- Note that we reset the creation time as if the MigrationJob was created right now
  -- this is because "creationTime" is actually the time of entering the "PENDING" state
  -- in the cases where the migrationJob went through a WAITINGONRECALL state
  UPDATE MigrationJob
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
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_COMPLETED_DB, varFileId, varNsHost, 'tapegatewayd',
           'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
           ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath);
END;
/

/* save in the db the results returned by the retry policy for migration */
CREATE OR REPLACE
PROCEDURE tg_setMigRetryResult(
  mjToRetry IN castor."cnumList",
  mjToFail  IN castor."cnumList" ) AS
  varSrIds "numList";
  varCfId NUMBER;
  varOriginalCopyNb NUMBER;
  varMigJobCount NUMBER;
BEGIN
  -- check because oracle cannot handle empty buffer
  IF mjToRetry( mjToRetry.FIRST) != -1 THEN
    -- restarted the one to be retried
    FOR i IN mjToRetry.FIRST .. mjToRetry.LAST LOOP
      UPDATE MigrationJob SET
        status = tconst.MIGRATIONJOB_PENDING,
        nbretry = nbretry+1,
        vid = NULL  -- this job will not go to this volume after all, at least not now...
        WHERE id = mjToRetry(i);
    END LOOP;
  END IF;

  -- check because oracle cannot handle empty buffer
  IF mjToFail(mjToFail.FIRST) != -1 THEN
    -- go through failed migration jobs
    FOR i IN mjToFail.FIRST .. mjToFail.LAST LOOP
      -- delete migration job
      DELETE FROM MigrationJob WHERE id = mjToFail(i)
      RETURNING originalCopyNb, castorfile INTO varOriginalCopyNb, varCfId;
      -- check if another migration should be performed
      SELECT count(*) INTO varMigJobCount
        FROM MigrationJob
       WHERE castorfile = varCfId;
      IF varMigJobCount = 0 THEN
         -- no other migration, delete all migrated segments together.
         DELETE FROM MigratedSegment
          WHERE castorfile = varCfId;
      END IF;
      -- fail repack subrequests
      IF varOriginalCopyNb IS NOT NULL THEN
        -- find and archive the repack subrequest(s)
        SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/
               SubRequest.id BULK COLLECT INTO varSrIds
          FROM SubRequest
          WHERE SubRequest.castorfile = varCfId
          AND subrequest.status = dconst.SUBREQUEST_REPACK;
        FOR i IN varSrIds.FIRST .. varSrIds.LAST LOOP
          archivesubreq(varSrIds(i), dconst.SUBREQUEST_FAILED_FINISHED);
        END LOOP;
        -- set back the diskcopies to STAGED otherwise repack will wait forever
        UPDATE DiskCopy
          SET status = dconst.DISKCOPY_STAGED
          WHERE castorfile = varCfId
          AND status = dconst.DISKCOPY_CANBEMIGR;
      END IF;
    END LOOP;
  END IF;

  COMMIT;
END;
/

/* attempt to retry a recall. Fail it in case it should not be retried anymore */
CREATE OR REPLACE PROCEDURE retryRecall(inCfId NUMBER, inVID VARCHAR2) AS
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
  -- stop here is no recallJob was concerned
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
    UPDATE /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest 
       SET status = dconst.SUBREQUEST_FAILED,
           lastModificationTime = getTime(),
           errorCode = 1015,  -- SEINTERNAL
           errorMessage = 'File recall from tape has failed, please try again later'
     WHERE castorFile = inCfId 
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
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
  varUnused          NUMBER;
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
CREATE OR REPLACE
PROCEDURE tg_deleteMigrationMount(inMountId IN NUMBER) AS
BEGIN
  DELETE FROM MigrationMount WHERE id=inMountId;
END;
/

CREATE OR REPLACE
PROCEDURE TG_SetFileStaleInMigration(
  /* When discovering in name server that a file has been changed during its migration,
  we have to finish it and indicate to the stager that the discopy is outdated */
  inMountTransactionId IN NUMBER,
  inFileId             IN NUMBER,
  inNsHost             IN VARCHAR2,
  inFseq               IN INTEGER,
  inFileTransaction    IN NUMBER) AS
  varCfId                 NUMBER;
  varUnused               NUMBER;
BEGIN
  -- Find MigrationMount from vdqm vol req ID and lock it
  SELECT mountTransactionId INTO varUnused FROM MigrationMount
   WHERE mountTransactionId = inMountTransactionId FOR UPDATE;
  -- Lock the CastorFile
  SELECT CF.id INTO varCfId FROM CastorFile CF
   WHERE CF.fileid = inFileId
     AND CF.nsHost = inNsHost
     FOR UPDATE;
  -- XXX This migration job is done. The other ones (if any) will have to live
  -- an independant life without the diskcopy (and fail)
  -- After the schema remake in 09/2011 for 2.1.12, we expect to
  -- fully cover those cases.
  -- As of today,we are supposed to repack a file's tape copies
  -- one by one. So should be exempt of this problem.
  DELETE FROM MigrationJob
   WHERE FileTransactionId = inFileTransaction
     AND fSeq = inFseq;
  -- The disk copy is known stale. Let's invalidate it:
  UPDATE DiskCopy DC
     SET DC.status= dconst.DISKCOPY_INVALID
   WHERE DC.castorFile = varCfId
     AND DC.status= dconst.DISKCOPY_CANBEMIGR;
  -- archive Repack requests should any be in the db
  FOR i IN (
    SELECT /*+ INDEX(SR I_Subrequest_Castorfile)*/ SR.id FROM SubRequest SR
    WHERE SR.castorfile = varCfId AND
          SR.status= dconst.SUBREQUEST_REPACK
    ) LOOP
      archivesubreq(i.id, 8); -- SUBREQUEST_FINISHED
  END LOOP;
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
CREATE OR REPLACE
PROCEDURE tg_flagTapeFull (inMountTransactionId IN NUMBER) AS
BEGIN
  UPDATE MigrationMount SET full = 1 WHERE mountTransactionId = inMountTransactionId;
END;
/

/* Find the VID of the tape used in a tape session */
CREATE OR REPLACE
PROCEDURE tg_getMigrationMountVid (
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
                                                 outTgRequestId OUT INTEGER) AS
  varMountId NUMBER;
  varDiskServer VARCHAR2(2048);
  varMountPoint VARCHAR2(2048);
  varPath VARCHAR2(2048);
  varDiskCopyId NUMBER;
  varLastKnownName VARCHAR2(2048);
  varFileId NUMBER;
  varNsHost VARCHAR2(2048);
  varFileSize INTEGER;
  varMigJobId INTEGER;
  varLastUpdateTime NUMBER;
BEGIN
  -- try to create a mount
  INSERT INTO MigrationMount
              (mountTransactionId, tapeGatewayRequestId, id, startTime, VID, label, density,
               lastFseq, lastVDQMPingTime, tapePool, status)
   VALUES (NULL, ids_seq.nextval, ids_seq.nextval, gettime(), NULL, NULL, NULL,
           NULL, 0, inTapePoolId, tconst.MIGRATIONMOUNT_WAITTAPE)
   RETURNING id, tapeGatewayRequestId INTO varMountId, outTgRequestId;
  -- check that the mount will be honoured by running a dry-run file selection
  -- Caveat: this will select for update a file for migartion.
  tg_defaultMigrSelPolicy(varMountId, varDiskServer, varMountPoint, varPath,
    varDiskCopyId, varLastKnownName, varFileId, varNsHost,varFileSize,
    varMigJobId, varLastUpdateTime);
  IF varMigJobId IS NULL THEN
    -- No valid candidate found: this could happen e.g. when candidates exist
    -- but reside on non-available hardware. In this case we drop the mount and log
    DELETE FROM MigrationMount WHERE id = varMountId;
    outTgRequestId := 0;
  END IF;
END;
/

/* resurrect tapes */
CREATE OR REPLACE PROCEDURE startMigrationMounts AS
  varNbPreExistingMounts INTEGER;
  varTotalNbMounts INTEGER := 0;
  varDataAmount INTEGER;
  varNbFiles INTEGER;
  varOldestCreationTime NUMBER;
  varTGRequestId INTEGER;
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
    SELECT SUM(fileSize), COUNT(*), MIN(creationTime) INTO varDataAmount, varNbFiles, varOldestCreationTime
      FROM MigrationJob
     WHERE tapePool = t.id
       AND status = tconst.MIGRATIONJOB_PENDING
     GROUP BY tapePool;
    -- Create as many mounts as needed according to amount of data and number of files
    WHILE (varTotalNbMounts < t.nbDrives) AND
          ((varDataAmount/(varTotalNbMounts+1) >= t.minAmountDataForMount) OR
           (varNbFiles/(varTotalNbMounts+1) >= t.minNbFilesForMount)) AND
          (varTotalNbMounts+1 < varNbFiles) LOOP   -- in case minAmountDataForMount << avgFileSize, stop creating more than one mount per file
      insertMigrationMount(t.id, varTGRequestId);
      varTotalNbMounts := varTotalNbMounts + 1;
      IF varTGRequestId = 0 THEN
        -- log "startMigrationMounts: failed migration mount creation due to lack of files"
        logToDLF(NULL, dlf.LVL_WARNING, dlf.MIGMOUNT_NO_FILE, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(varOldestCreationTime));
        -- no need to continue as we could not find a single file to migrate
        -- note that we've logge
        EXIT;
      ELSE
        -- log "startMigrationMounts: created new migration mount"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NEW_MOUNT, 0, '', 'tapegatewayd',
                 'mountTransactionId=' || TO_CHAR(varTGRequestId) ||
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
      insertMigrationMount(t.id, varTGRequestId);
      IF varTGRequestId = 0 THEN
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
                 'mountTransactionId=' || TO_CHAR(varTGRequestId) ||
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

/* startRecallMounts */
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
      JOB_ACTION      => 'BEGIN startMigrationMounts(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Creates MigrationMount entries when new migrations should start');

  -- Create a db job to be run every minute executing the startRecallMounts procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'RecallMountsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startRecallMounts(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Creates RecallMount entries when new recalls should start');
END;
/
