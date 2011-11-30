/*******************************************************************
 *
 * @(#)$RCSfile: oracleTapeGateway.sql,v $ $Revision: 1.12 $ $Date: 2009/08/13 15:14:25 $ $Author: gtaur $
 *
 * PL/SQL code for the tape gateway daemon
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* PROCEDURE */

/* tg_findFromTGRequestId */
CREATE OR REPLACE
PROCEDURE tg_findFromTGRequestId (
  inTapeGatewayRequestId IN  INTEGER,
  outTapeId              OUT INTEGER,
  outMountId            OUT INTEGER) AS
BEGIN
  -- Will return a valid ID in either outTapeId or outMountId,
  -- and NULL in the other when finding the object corresponding to
  -- this TGR request ID.
  --
  -- Will throw an exception in case of non-unicity.

  -- Look for read tapes:
  BEGIN
    SELECT id INTO outTapeId
      FROM Tape
     WHERE TapeGatewayRequestId = inTapeGatewayRequestId
       AND tpMode = tconst.TPMODE_READ;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outTapeId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple read tapes for same TapeGatewayRequestId: '|| 
         inTapeGatewayRequestId || ' in tg_findFromTGRequestId');
     -- Let other exceptions fly through.
   END;
   
   -- Look for migration mounts
   BEGIN
     SELECT id INTO outMountId
       FROM MigrationMount
      WHERE TapeGatewayRequestId = inTapeGatewayRequestId;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outMountId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple migration mounts for same TapeGatewayRequestId: '|| 
         inTapeGatewayRequestId || ' in tg_findFromTGRequestId');
     -- Let other exceptions fly through.     
   END;
   
   -- Check for migration mount/tape collision
   IF (outMountId IS NOT NULL AND outTapeId IS NOT NULL) THEN
     RAISE_APPLICATION_ERROR (-20119, 
       'Found both read tape (id='||outTapeId||') and migration mount (id='||
       outMountId||') for TapeGatewayRequestId: '||
       inTapeGatewayRequestId || ' in tg_findFromTGRequestId');
   END IF;
END;
/

/* tg_findFromVDQMReqId */
CREATE OR REPLACE
PROCEDURE tg_findFromVDQMReqId (
  inVDQMReqId IN  INTEGER,
  outTapeId              OUT INTEGER,
  outMountId            OUT INTEGER) AS
BEGIN
  -- Will return a valid ID in either outTapeId or outMountId,
  -- and NULL in the other when finding the object corresponding to
  -- this TGR request ID.
  --
  -- Will throw an exception in case of non-unicity.

  -- Look for read tapes:
  BEGIN
    SELECT id INTO outTapeId
      FROM Tape
     WHERE VDQMVolReqId = inVDQMReqId
       AND tpMode = tconst.TPMODE_READ;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outTapeId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple read tapes for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.
   END;
   
   -- Look for migration mounts
   BEGIN
     SELECT id INTO outMountId
       FROM MigrationMount
      WHERE VDQMVolReqId = inVDQMReqId;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outMountId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple migration mounts for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.     
   END;
   
   -- Check for migration mount/tape collision
   IF (outMountId IS NOT NULL AND outTapeId IS NOT NULL) THEN
     RAISE_APPLICATION_ERROR (-20119, 
       'Found both read tape (id='||outTapeId||') and migration mount (id='||
       outMountId||') for VDQMVolReqId: '||
       inVDQMReqId || ' in tg_findFromVDQMReqId');
   END IF;
END;
/

/* tg_RequestIdFromVDQMReqId */
CREATE OR REPLACE
PROCEDURE tg_RequestIdFromVDQMReqId (
  inVDQMReqId IN  INTEGER,
  outTgrId    OUT INTEGER) AS
  varTapeId       INTEGER;
  varMountId     INTEGER;
BEGIN
  -- Will return a valid tape gateway request Id if one and only one read
  -- tape or migration mount is found with this VDQM request ID.
  --
  -- Will throw an exception in case of non-unicity.
  --
  -- Will return NULL in case of not found.
  outTgrId := NULL;
  -- Look for read tapes:
  BEGIN
    SELECT id, TapeGatewayRequestId INTO varTapeId, outTgrId
      FROM Tape
     WHERE VDQMVolReqId = inVDQMReqId
       AND tpMode = tconst.TPMODE_READ;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL; -- It's OK, could be a migration mount.
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple read tapes for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.
   END;
   
   -- Look for migration mounts
   BEGIN
     SELECT id, TapeGatewayRequestId INTO varMountId, outTgrId
       FROM MigrationMount
      WHERE VDQMVolReqId = inVDQMReqId;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL; -- It's OK, might have been a tape.
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple migration mounts for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.     
   END;
   
   -- Check for migration mount/tape collision
   IF (varMountId IS NOT NULL AND varTapeId IS NOT NULL) THEN
     outTgrId := NULL;
     RAISE_APPLICATION_ERROR (-20119, 
       'Found both read tape (id='||varTapeId||') and migration mount (id='||
       varMountId||') for VDQMVolReqId: '||
       inVDQMReqId || ' in tg_findFromVDQMReqId');
   END IF;
END;
/

/* tg_findVDQMReqFromTGReqId */
CREATE OR REPLACE
PROCEDURE tg_findVDQMReqFromTGReqId (
  inTGReqId     IN  INTEGER,
  outVDQMReqId  OUT INTEGER) AS
  varTapeId         NUMBER;
  varMountId       NUMBER;
BEGIN
  -- Helper function. Wrapper to another helper.
  tg_findFromTGRequestId (inTGReqId, varTapeId, varMountId);
  IF (varTapeId IS NOT NULL) THEN
    SELECT T.vdqmVolReqId INTO outVDQMReqId
      FROM Tape T WHERE T.id = varTapeId;
  ELSIF (varMountId IS NOT NULL) THEN
    SELECT vdqmVolReqId INTO outVDQMReqId
      FROM MigrationMount WHERE id = varMountId;  
  ELSE
    RAISE_APPLICATION_ERROR (-20119, 
         'Could not find migration mount or tape read for TG request Id='|| 
         inTGReqId || ' in tg_findVDQMReqFromTGReqId');
  END IF;
END;
/

/* attach drive request to tape */
CREATE OR REPLACE
PROCEDURE tg_attachDriveReqToTape(
  inTapeRequestId IN NUMBER,
  inVdqmId    IN NUMBER,
  inDgn       IN VARCHAR2,
  inLabel     IN VARCHAR2,
  inDensity   IN VARCHAR2) AS
  varTapeId   INTEGER;
  varMountId INTEGER;
/* Update the status and propoerties of the Tape structure related to
 * a tape request, and the migration mount state in case of a write.
 * All other properties are attached to the tape itself.
 */
BEGIN
  -- Update tape or migration mount, whichever is relevant. First find:
  tg_findFromTGRequestId (inTapeRequestId, varTapeId, varMountId);
  
  -- Process one or the other (we trust the called function to not provide both)
  IF (varTapeId IS NOT NULL) THEN
    -- In the case of a read, only the tape itself is involved
    -- update reading tape which have been submitted to vdqm => WAIT_DRIVE.
    UPDATE Tape T
       SET T.lastvdqmpingtime = gettime(),
           T.starttime        = gettime(),
           T.vdqmvolreqid     = inVdqmId,
           T.Status           = tconst.TAPE_WAITDRIVE,
           T.dgn              = inDgn,
           T.label            = inLabel,
           T.density          = inDensity
     WHERE T.Id = varTapeId;
    COMMIT;
    RETURN;
  ELSIF (varMountId IS NOT NULL) THEN
    -- We have to update the tape as well (potentially, we keep the can-fail
    -- query based update of the previous system.
    SAVEPOINT Tape_Mismatch;
    DECLARE
      varTapeFromMount NUMBER;
      varTp             Tape%ROWTYPE;
    BEGIN
      UPDATE MigrationMount
         SET status = tconst.MIGRATIONMOUNT_WAITDRIVE,
             label = inLabel,
             density = inDensity,
             vdqmvolreqid = inVdqmId
       WHERE id = varMountId;
      COMMIT;
      RETURN;
    END; -- END of local block for varTapeFromMount and varTp
  ELSE RAISE_APPLICATION_ERROR (-20119,
       'Found no migration mount or read tape for TapeRequestId: '||inTapeRequestId);
  END IF;
END;
/
        
/* attach the tapes to the migration mounts  */
CREATE OR REPLACE
PROCEDURE tg_attachTapesToMigrationMounts (
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
END;
/

/* update the db when a tape session is ended */
CREATE OR REPLACE
PROCEDURE tg_endTapeSession(inTransId IN NUMBER, inErrorCode IN INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varUnused NUMBER;
  varTpId NUMBER;        -- TapeGateway Taperecall
  varTgrId NUMBER;       -- TapeGatewayRequest ID
  varMountId NUMBER;     -- Migration mount ID
  varSegNum INTEGER;     -- Segment count
  varTcIds "numList";    -- recall/migration job Ids

BEGIN
  -- Prepare to revert changes
  SAVEPOINT MainEndTapeSession;
  -- Find the Tape read or migration mount for this VDQM request
  tg_findFromVDQMReqId (inTransId, varTpId, varMountId);
  -- Pre-process the read and write: find corresponding TapeGatewayRequest Id.
  -- Lock corresponding Tape or MigrationMount. This will bomb if we
  -- don't find exactly ones (which is good).
  varTgrId := NULL;
  IF (varTpId IS NOT NULL) THEN
    -- Find and lock tape
    SELECT TapeGatewayRequestId INTO varTgrId
      FROM Tape
     WHERE id = varTpId
       FOR UPDATE;
  ELSIF (varMountId IS NOT NULL) THEN
    -- Find and lock migration mount
    SELECT TapeGatewayRequestId INTO varTgrId
      FROM MigrationMount
     WHERE id = varMountId
       FOR UPDATE;
  ELSE
    -- Nothing found for the VDQMRequestId: whine and leave.
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'No tape or migration mount found for VDQM ID='|| inTransId);
  END IF;
  -- If we failed to get the TG req Id, no point in going further.
  IF (varTgrId IS NULL) THEN
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'Got NULL TapeGatewayRequestId for tape ID='|| varTpId||
     ' or MigrationMount Id='|| varMountId||' processing VDQM Id='||inTransId||
     ' in tg_endTapeSession.');
  END IF;

  -- Process the read case
  IF (varTpId IS NOT NULL) THEN
    -- find and lock the RecallJobs
    SELECT id BULK COLLECT INTO varTcIds
      FROM RecallJob
     WHERE TapeGatewayRequestId = varTgrId
       FOR UPDATE;
    IF (inErrorCode != 0) THEN
        -- if a failure is reported
        -- fail all the segments
        UPDATE Segment SEG
           SET SEG.status=tconst.SEGMENT_FAILED
         WHERE SEG.copy IN (SELECT * FROM TABLE(varTcIds));
        -- mark RecallJob as RECALLJOB_RETRY
        UPDATE RecallJob
           SET status    = tconst.RECALLJOB_RETRY,
               errorcode = inErrorCode
         WHERE id IN (SELECT * FROM TABLE(varTcIds));
    END IF;
    -- resurrect lost segments
    UPDATE Segment SEG
       SET SEG.status = tconst.SEGMENT_UNPROCESSED
     WHERE SEG.status = tconst.SEGMENT_SELECTED
       AND SEG.tape = varTpId;
    -- check if there is work for this tape
    SELECT count(*) INTO varSegNum
      FROM segment SEG
     WHERE SEG.Tape = varTpId
       AND status = tconst.SEGMENT_UNPROCESSED;
    -- Restart the unprocessed segments' tape if there are any.
    IF varSegNum > 0 THEN
      UPDATE Tape T
         SET T.status = tconst.TAPE_WAITPOLICY -- for rechandler
       WHERE T.id=varTpId;
    ELSE
      UPDATE Tape
         SET status = tconst.TAPE_UNUSED
       WHERE id=varTpId;
     END IF;
  ELSIF (varMountId IS NOT NULL) THEN
    -- find and lock the MigrationJobs.
    SELECT id BULK COLLECT INTO varTcIds
      FROM MigrationJob
     WHERE TapeGatewayRequestId = varTgrId
       FOR UPDATE;
    -- Process the write case
    IF inErrorCode != 0 THEN
      -- if a failure is reported, retry
      UPDATE MigrationJob
         SET status=tconst.MIGRATIONJOB_RETRY,
             VID=NULL,
             TapeGatewayRequestId = NULL,
             errorcode=inErrorCode,
             nbretry=0
       WHERE id IN (SELECT * FROM TABLE(varTcIds));
    ELSE
      -- just resurrect them if they were lost
      UPDATE MigrationJob
         SET status = tconst.MIGRATIONJOB_PENDING,
             VID = NULL,
             TapeGatewayRequestId = NULL
       WHERE id IN (SELECT * FROM TABLE(varTcIds))
         AND status = tconst.MIGRATIONJOB_SELECTED;
    END IF;
    -- delete the migration mount
    DELETE FROM MigrationMount  WHERE tapegatewayrequestid = varMountId;
  ELSE

    -- Small infusion of paranoia ;-) We should never reach that point...
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'No tape or migration mount found on second pass for VDQM ID='|| inTransId ||
     ' in tg_endTapeSession');
  END IF;
  COMMIT;
END;
/

/* mark a migration or recall as failed saving in the db the error code associated with the failure */
CREATE OR REPLACE
PROCEDURE tg_failFileTransfer(
  inTransId      IN NUMBER,    -- The VDQM transaction ID
  inFileId    IN NUMBER,       -- File ID
  inNsHost    IN VARCHAR2,     -- NS Host
  inFseq      IN INTEGER,      -- fSeq of the tape copy
  inErrorCode IN INTEGER)  AS  -- Error Code
  varUnused NUMBER;            -- dummy
  varTgrId NUMBER;             -- Tape Gateway Request Id
  varMountId NUMBER;           -- MigrationMount Id
  varTpId NUMBER;              -- Tape Id
  varTcId NUMBER;              -- recall/migration job Id
BEGIN
  -- Prepare to return everything to its original state in case of problem.
  SAVEPOINT MainFailFileSession;
  
  -- Find related Read tape or migration mount from VDQM Id
  tg_findFromVDQMReqId(inTransId, varTpId, varMountId);
  
  -- Lock related castorfile -- TODO: This should be a procedure-based access to
  -- the disk system.
  SELECT CF.id INTO varUnused 
    FROM CastorFile CF
   WHERE CF.fileid = inFileId
     AND CF.nsHost = inNsHost 
    FOR UPDATE;
  
  -- Case dependent part
  IF (varTpId IS NOT NULL) THEN
    -- We handle a read case
    -- fail the segment on that tape
    UPDATE Segment SEG
       SET SEG.status    = tconst.SEGMENT_FAILED,
           SEG.severity  = inErrorCode,
           SEG.errorCode = -1 
     WHERE SEG.fseq = inFseq 
       AND SEG.tape = varTpId 
    RETURNING SEG.copy INTO varTcId;
    -- mark RecallJob for retry
    UPDATE RecallJob
       SET status    = tconst.RECALLJOB_RETRY,
           errorcode = inErrorCode 
     WHERE id = varTcId;
  ELSIF (varMountId IS NOT NULL) THEN
    -- Write case
    SELECT TapeGatewayRequestId INTO varTgrId
      FROM MigrationMount
     WHERE MigrationMount.id = varMountId;
    -- mark MigrationJob for retry. It should be the migration job with the proper 
    -- TapegatewayRequest + having a matching Fseq.
    UPDATE MigrationJob
       SET status    = tconst.MIGRATIONJOB_RETRY,
           errorcode = inErrorCode,
           vid       = NULL
     WHERE TapegatewayRequestId = varTgrId
       AND fSeq = inFseq; 
  ELSE
    -- Complain in case of failure
    ROLLBACK TO SAVEPOINT MainFailFileSession;
    RAISE_APPLICATION_ERROR (-20119, 
     'No tape or migration mount found on second pass for VDQM ID='|| inTransId||
     ' in tg_failFileTransfer');
  END IF;
EXCEPTION WHEN  NO_DATA_FOUND THEN
  NULL;
END;
/

/* retrieve from the db all the migration jobs that faced a failure */
CREATE OR REPLACE
PROCEDURE tg_getFailedMigrations(outFailedMigrationJob_c OUT castor.FailedMigrationJob_Cur) AS
BEGIN
  OPEN outFailedMigrationJob_c FOR
    SELECT id, errorCode, nbRetry
      FROM MigrationJob
     WHERE MigrationJob.status = tconst.MIGRATIONJOB_RETRY
       AND ROWNUM < 1000 
       FOR UPDATE SKIP LOCKED; 
END;
/


/* retrieve from the db all the recall jobs that faced a failure */
CREATE OR REPLACE
PROCEDURE tg_getFailedRecalls(outFailedRecallJob_c OUT castor.FailedRecallJob_Cur) AS
BEGIN
  OPEN outFailedRecallJob_c FOR
    SELECT id, errorCode, nbRetry
      FROM RecallJob
     WHERE RecallJob.status = tconst.RECALLJOB_RETRY
       AND ROWNUM < 1000 
       FOR UPDATE SKIP LOCKED;
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
               INDEX(CastorFile PK_CastorFile_Id)
               INDEX(DiskCopy I_DiskCopy_CastorFile)
               INDEX(MigrationJob I_MigrationJob_TapePoolStatus) */
           DiskServer.name, FileSystem.mountPoint, DiskCopy.path, DiskCopy.id, CastorFile.lastKnownFilename,
           CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, MigrationJob.id, CastorFile.lastUpdateTime
      FROM MigrationMount, MigrationJob, DiskCopy, FileSystem, DiskServer, CastorFile
     WHERE MigrationMount.id = inMountId
       AND MigrationJob.tapePool = MigrationMount.tapePool
       AND MigrationJob.status = tconst.MIGRATIONJOB_PENDING
       AND CastorFile.id = MigrationJob.castorFile
       AND DiskCopy.castorFile = MigrationJob.castorFile
       AND FileSystem.id = DiskCopy.fileSystem
       AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
       AND DiskServer.id = FileSystem.diskServer
       AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
       AND MigrationMount.VID NOT IN (SELECT /*+ INDEX_RS_ASC(MigratedSegment I_MigratedSegment_CFCopyNBVID) */ vid 
                                        FROM MigratedSegment
                                       WHERE castorFile = MigrationJob.castorfile
                                         AND copyNb != MigrationJob.destCopyNb)
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
     WHERE VDQMVolReqId = inVDQMtransacId;
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

/* get a candidate for recall */
CREATE OR REPLACE
PROCEDURE tg_getFileToRecall (inTransId IN  NUMBER, outRet OUT INTEGER,
  outVid OUT NOCOPY VARCHAR2, outFile OUT castorTape.FileToRecallCore_Cur) AS
  varTgrId         INTEGER; -- TapeGateway Request Id
  varDSName VARCHAR2(2048); -- Disk Server name
  varMPoint VARCHAR2(2048); -- Mount Point
  varPath   VARCHAR2(2048); -- Diskcopy path
  varSegId          NUMBER; -- Segment Id
  varDcId           NUMBER; -- Disk Copy Id
  varRjId           NUMBER; -- Recalljob Id
  varTapeId         NUMBER; -- Tape Id
  varNewFSeq       INTEGER; -- new FSeq
  varUnused         NUMBER;
BEGIN 
  outRet:=0;
  BEGIN
    -- master lock on the tape read
    -- Find tape
    tg_FindFromVDQMReqId (inTransId, varTapeId, varUnused);
    IF (varTapeId IS NULL) THEN 
      outRet := -2;
      RETURN;
    END IF;
    -- Take lock on tape
    SELECT T.TapeGatewayRequestId, T.vid INTO varTgrId, outVid
      FROM Tape T
     WHERE T.id = varTapeId
       FOR UPDATE;
  EXCEPTION WHEN  NO_DATA_FOUND THEN
     outRet:=-2; -- ERROR
     RETURN;
  END; 
  BEGIN
    -- Find the unprocessed segment of this tape with lowest fSeq
    SELECT       id,       fSeq,    Copy 
      INTO varSegId, varNewFSeq, varRjId 
      FROM (SELECT SEG.id id, SEG.fSeq fSeq, SEG.Copy Copy 
              FROM Segment SEG
             WHERE SEG.tape = varTapeId  
               AND SEG.status = tconst.SEGMENT_UNPROCESSED
             ORDER BY SEG.fseq ASC)
     WHERE ROWNUM < 2;
    -- Lock the corresponding castorfile
    SELECT CF.id INTO varUnused 
      FROM Castorfile CF, RecallJob RJ
     WHERE CF.id = RJ.castorfile 
       AND RJ.id = varRjId 
       FOR UPDATE OF CF.id;
  EXCEPTION WHEN NO_DATA_FOUND THEN
     outRet := -1; -- NO MORE FILES
     COMMIT;
     RETURN;
  END;
  DECLARE
    application_error EXCEPTION;
    PRAGMA EXCEPTION_INIT(application_error,-20115);
  BEGIN
    bestFileSystemForSegment(varSegId,varDSName,varMPoint,varPath,varDcId);
  EXCEPTION WHEN application_error  OR NO_DATA_FOUND THEN 
    outRet := -3;
    COMMIT;
    RETURN;
  END;
  -- Update the RecallJob's parameters
  UPDATE RecallJob
     SET fseq = varNewFSeq,
         TapeGatewayRequestId = varTgrId,
         FileTransactionID = TG_FileTrId_Seq.NEXTVAL
   WHERE id = varRjId;
   -- Update the segment's status
  UPDATE Segment SEG 
     SET SEG.status = tconst.SEGMENT_SELECTED
   WHERE SEG.id=varSegId 
     AND SEG.status = tconst.SEGMENT_UNPROCESSED;
  OPEN outFile FOR 
    SELECT CF.fileid, CF.nshost, varDSName, varMPoint, varPath, varNewFSeq , 
           RJ.FileTransactionID
      FROM RecallJob RJ, Castorfile CF
     WHERE RJ.id = varRjId
       AND CF.id = RJ.castorfile;
END;
/

/* get the information from the db for a successful migration */
CREATE OR REPLACE
PROCEDURE tg_getRepackVidAndFileInfo(
  inFileId          IN  NUMBER, 
  inNsHost          IN VARCHAR2,
  inFseq            IN  INTEGER, 
  inTransId         IN  NUMBER, 
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
    tg_failFileTransfer(inTransId,inFileId, inNsHost, inFseq,  1613); -- wrongfilesize
    COMMIT;
    outRet := -1;
    RETURN;
  ELSE
    outRet:=0;
  END IF;
  
  tg_RequestIdFromVDQMReqId(inTransId, varTgrId);
  IF (varTgrId IS NOT NULL) THEN
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
  END IF;  
END;
/

/* get the information from the db for a successful recall */
CREATE OR REPLACE
PROCEDURE tg_getSegmentInfo(
  inTransId     IN NUMBER,
  inFseq        IN INTEGER,
  outVid       OUT NOCOPY VARCHAR2,
  outCopyNb    OUT INTEGER ) AS
  varTrId          NUMBER;
  varTapeId        NUMBER;
  varUnused        NUMBER;
BEGIN
  -- We are looking for a recalled tape with this TGR request ID.
  tg_FindFromVDQMReqId (inTransId, varTapeId, varUnused);

  -- Throw an exception in case of not found.
  IF (varTapeId IS NULL) THEN
    outVid := NULL;
    outCopyNb := NULL;
    RAISE_APPLICATION_ERROR (-20119,
         'No tape read found for VDQMReqId'||
         inTransId || ' in tg_getSegmentInfo');
  END IF;

  -- Get the data
  BEGIN
  SELECT T.vid, T.TapeGatewayRequestId  INTO outVid, varTrId
    FROM Tape T
   WHERE T.id=varTapeId;

  SELECT copynb INTO outCopyNb
    FROM RecallJob
   WHERE fseq = inFseq
     AND TapeGateWayRequestId = varTrId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    outVid := NULL;
    outCopyNb := NULL;
    RAISE_APPLICATION_ERROR (-20119,
         'Tailed to find tape or tape copy for VDQM Id='||
         inTransId || ',TapeId='||varTapeId||' TapeGatewayReqId='||varTrId||
         ' in tg_getSegmentInfo');
  END;
END;
/

/* get the migration mounts without any tape associated */
CREATE OR REPLACE
PROCEDURE tg_getMigMountsWithoutTapes(outStrList OUT castorTape.MigrationMount_Cur) AS
BEGIN
  -- get migration mounts  in state WAITTAPE with a non-NULL TapeGatewayRequestId
  OPEN outStrList FOR
    SELECT M.id, TP.name
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
  outTapeRequest OUT castorTape.tapegatewayrequest_Cur) AS
  varTgrId        "numList";
  varTapeReadIds  "numList";
  varMigMountIds    "numList";
  varNow          NUMBER;
BEGIN 
  -- get requests in WAITING_TAPESERVER and ONGOING
  -- With the tape gateway request table dropping, this boils down to
  -- streams in state STREAM_WAITDRIVE, STREAM_WAITMOUNT, STREAM_RUNNING
  -- or Tape reads in state TAPE_WAITTAPEDRIVE or MOUNTED.
  
  -- In addition, we only look for the tape reads/streams which have a VDQM ping
  -- time older than inTimeLimit.  
  
  -- TODO: The function name should reflect the fact that it's actually dealing
  -- with a timeout mechanism.
  
  -- TODO: I do not have the TAPE_WAITMOUNT state in the lifecycle diagram, but
  -- include it nevertheless. This is a safe option as the select will be limite
  -- to tapes in tape read mode. If the diagram is right, this will have no
  -- effect and if the diagram is wrong, this will lead to cover the
  -- non-decribed case.

  -- No need to query the clock all the time
  varNow := gettime();
  
  -- Find all the tapes and lock
  SELECT T.id BULK COLLECT INTO varTapeReadIds
    FROM Tape T
   WHERE T.status IN ( tconst.TAPE_WAITDRIVE, tconst.TAPE_WAITMOUNT, tconst.TAPE_MOUNTED )
     AND T.tpMode = tconst.TPMODE_READ
     AND T.TapeGatewayRequestId IS NOT NULL
     AND varNow - T.lastVdqmPingTime > inTimeLimit
     FOR UPDATE SKIP LOCKED;
     
  -- Find all the migration mounts and lock
  SELECT id BULK COLLECT INTO varMigMountIds
    FROM MigrationMount
   WHERE status IN ( tconst.MIGRATIONMOUNT_WAITDRIVE, tconst.MIGRATIONMOUNT_MIGRATING )
     AND varNow - lastVdqmPingTime > inTimeLimit
     FOR UPDATE SKIP LOCKED;
     
  -- Update the last VDQM ping time for all of them.
  UPDATE Tape T
     SET T.lastVdqmPingTime = varNow
   WHERE T.id IN ( SELECT /*+ CARDINALITY(trTable 5) */ * 
                     FROM TABLE (varTapeReadIds) );
  UPDATE MigrationMount
     SET lastVdqmPingTime = varNow
   WHERE id IN ( SELECT /*+ CARDINALITY(trTable 5) */ *
                   FROM TABLE (varMigMountIds));
                   
  -- Return them. For VDQM request IT, we have to split the select in 2 and
  -- union in the end, unlike in the previous statement.
  OPEN outTapeRequest FOR
    -- Read case
    SELECT T.TpMode, T.TapeGatewayRequestId, T.startTime,
           T.lastvdqmpingtime, T.vdqmVolReqid, 
           T.vid
      FROM Tape T
     WHERE T.Id IN (SELECT /*+ CARDINALITY(trTable 5) */ *
                    FROM TABLE(varTapeReadIds))
     UNION ALL
    -- Write case
    SELECT tconst.TPMODE_WRITE, tapeGatewayRequestId, startTime,
           lastvdqmpingtime, vdqmVolReqid, VID
      FROM MigrationMount
     WHERE id IN (SELECT /*+ CARDINALITY(trTable 5) */ *
                    FROM TABLE(varMigMountIds));
END;
/

/* get a tape without any drive requests sent to VDQM */
CREATE OR REPLACE
PROCEDURE tg_getTapeWithoutDriveReq(
  outReqId    OUT NUMBER,
  outTapeMode OUT NUMBER,
  outTapeSide OUT INTEGER,
  outTapeVid  OUT NOCOPY VARCHAR2) AS
  varMigMountId     NUMBER;
  varTapeId       NUMBER;
BEGIN
  -- Initially looked for tapegateway request in state SEND_TO_VDQM
  -- Find a tapegateway request id for which there is a tape read in
  -- state TAPE_PENDING or a migration mount in state SEND_TO_VDQM.
  -- This method is called until there are no more pending tapes
  -- We serve writes first and then reads
  BEGIN
    SELECT tapeGatewayRequestId, 1, 0, VID  -- note that the tape side is deprecated and thus hardcoded to 0
      INTO outReqId, outTapeMode, outTapeSide, outTapeVid
      FROM MigrationMount
     WHERE status = tconst.MIGRATIONMOUNT_SEND_TO_VDQM
       AND ROWNUM < 2
     ORDER BY dbms_random.value()
       FOR UPDATE SKIP LOCKED;
    RETURN;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Nothing to be found, we'll just carry on to the reads.
    NULL;
  END;
  BEGIN -- The read casse
    SELECT T.TapeGatewayRequestId,     0,      T.side,      T.vid,      T.id
      INTO outReqId,         outTapeMode, outTapeSide, outTapeVid, varTapeId
      FROM Tape T
     WHERE T.tpMode = tconst.TPMODE_READ
       AND T.status = tconst.TAPE_PENDING
       AND ROWNUM < 2
       FOR UPDATE SKIP LOCKED;
     -- Potential lazy/late definition of the request id
     -- We might be confronted to a not defined request id if the tape was created
     -- by the stager straight into pending state in the absence of a recall policy
     -- otherwise, the tape will go through resurrect tape (rec handler) and all
     -- will be fine.
     -- If we get here, we found a tape so the request id must be defined when leaving.
     IF (outReqId IS NULL OR outReqId = 0) THEN
       SELECT ids_seq.nextval INTO outReqId FROM DUAL;
       UPDATE Tape T SET T.TapeGatewayRequestId = outReqId WHERE T.id = varTapeId;
     END IF; 
  EXCEPTION WHEN NO_DATA_FOUND THEN
    outReqId := 0;
  END;
END;
/


/* get tape to release in VMGR */
CREATE OR REPLACE
PROCEDURE tg_getTapeToRelease(
  inVdqmReqId IN  INTEGER, 
  outVID      OUT NOCOPY VARCHAR2, 
  outMode     OUT INTEGER,
  outFull     OUT INTEGER ) AS
  varMountId      NUMBER;
  varTpId         NUMBER;
BEGIN
  -- Find Tape read or migration mount for this vdqm request
  tg_findFromVDQMReqId(inVdqmReqId, varTpId, varMountId);
  
   IF (varTpId IS NOT NULL) THEN -- read case
     outMode := 0;
     SELECT T.vid INTO outVID 
       FROM Tape T
       WHERE T.id = varTpId; 
   ELSIF (varMountId IS NOT NULL) THEN -- write case
     outMode := 1;
     SELECT vid, full
     INTO outVID, outFull
       FROM MigrationMount
      WHERE id = varMountId;
   END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- already cleaned by the checker
  NULL;
END;
/

/* invalidate a file that it is not possible to recall */
CREATE OR REPLACE
PROCEDURE tg_invalidateFile(
  inTransId   IN NUMBER,
  inFileId    IN NUMBER, 
  inNsHost    IN VARCHAR2,
  inFseq      IN INTEGER,
  inErrorCode IN INTEGER) AS
  varTapeId      NUMBER;
BEGIN
  UPDATE Tape
     SET lastfseq = lastfseq-1
   WHERE VDQMVolReqId = inTransId
     AND tpMode = tconst.TPMODE_READ;
   tg_failfiletransfer(inTransId, inFileId, inNsHost, inFseq, inErrorCode);
END;
/

/* restart taperequest which had problems */
CREATE OR REPLACE
PROCEDURE tg_restartLostReqs(
  trIds IN castor."cnumList") AS
  vdqmId INTEGER;
BEGIN
 FOR  i IN trIds.FIRST .. trIds.LAST LOOP   
   BEGIN
     tg_findVDQMReqFromTGReqId(trIds(i), vdqmId);
     tg_endTapeSession(vdqmId,0);
   EXCEPTION WHEN NO_DATA_FOUND THEN
     NULL;
   END;
 END LOOP;
 COMMIT;
END;
/


/* update the db after a successful migration */
CREATE OR REPLACE
PROCEDURE TG_SetFileMigrated(
  inTransId         IN  NUMBER, 
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
  -- delete the corresponding migration job, create the new migrated segment
  DELETE FROM MigrationJob
   WHERE FileTransactionId = inFileTransaction
     AND fSeq = inFseq
  RETURNING destCopyNb, VID INTO varCopyNb, varVID;
  INSERT INTO MigratedSegment VALUES (varCfId, varCopyNb, varVID);
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


/* update the db after a successful recall */
CREATE OR REPLACE
PROCEDURE tg_setFileRecalled(
  inTransId          IN  NUMBER,
  inFileId           IN  NUMBER,
  inNsHost           IN  VARCHAR2,
  inFseq             IN  NUMBER,
  inFileTransaction  IN  NUMBER) AS
  varDcId                NUMBER;         -- DiskCopy Id
  varCfId                NUMBER;         -- CastorFile Id
  varSubrequestId        NUMBER;
  varRequestId           NUMBER;
  varFileSize            NUMBER;
  varGcWeight            NUMBER;         -- Garbage collection weight
  varGcWeightProc        VARCHAR(2048);  -- Garbage collection weighting procedure name
  varEuid                INTEGER;        -- Effective user Id
  varEgid                INTEGER;        -- Effective Group Id
  varSvcClassId          NUMBER;         -- Service Class Id
  varNbMigrationsStarted INTEGER;
BEGIN
  -- find and lock castor file for the nsHost/fileID
  SELECT CF.id, CF.fileSize INTO varCfId, varFileSize
    FROM CastorFile CF
   WHERE CF.fileid = inFileId
     AND CF.nsHost = inNsHost
     FOR UPDATE;
  -- find the disk copy. There should be only one.
  SELECT DC.id INTO varDcId
    FROM DiskCopy DC
   WHERE DC.castorFile = varCfId
     AND DC.status = dconst.DISKCOPY_WAITTAPERECALL;
  -- delete recall jobs
  FOR t IN (SELECT id FROM RecallJob WHERE castorfile = varCfId) LOOP
    DELETE FROM Segment WHERE copy = t.id;
    DELETE FROM RecallJob WHERE id = t.id;
  END LOOP;
  -- update diskcopy status, size and gweight
  SELECT /*+ INDEX(SR I_Subrequest_DiskCopy)*/ SR.id, SR.request
    INTO varSubrequestId, varRequestId
    FROM SubRequest SR
   WHERE SR.diskcopy = varDcId;
  -- get information about the request
  SELECT REQ.svcClass, REQ.euid, REQ.egid INTO varSvcClassId, varEuid, varEgid
    FROM (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClass, euid, egid FROM StageGetRequest                                  UNION ALL
          SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, svcClass, euid, egid FROM StagePrepareToGetRequest       UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClass, euid, egid FROM StageUpdateRequest                         UNION ALL
          SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcClass, euid, egid FROM StagePrepareToUpdateRequest UNION ALL
          SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, svcClass, euid, egid FROM StageRepackRequest) REQ
    WHERE REQ.id = varRequestId;
  -- compute GC weight of the recalled diskcopy
  varGcWeightProc := castorGC.getRecallWeight(varSvcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || varGcWeightProc || '(:size); END;'
    USING OUT varGcWeight, IN varFileSize;
  -- trigger the migration of additionnal/repack copies
  UPDATE MigrationJob SET status = tconst.MIGRATIONJOB_PENDING
   WHERE status = tconst.MIGRATIONJOB_WAITINGONRECALL
     AND castorFile = varCfId;
  varNbMigrationsStarted := SQL%ROWCOUNT;
  -- update diskcopy
  UPDATE DiskCopy DC
    SET DC.status = decode(varNbMigrationsStarted,
                           0, dconst.DISKCOPY_STAGED,      -- STAGED if no migrations
                              dconst.DISKCOPY_CANBEMIGR),  -- otherwise CANBEMIGR
        DC.lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
        DC.gcWeight = varGcWeight,
        DC.diskCopySize = varFileSize
    WHERE Dc.id = varDcId;
  -- determine the type of the request
  IF varNbMigrationsStarted > 0 THEN
    -- update remaining STAGED diskcopies to CANBEMIGR too
    -- we may have them as result of disk2disk copies, and so far
    -- we only dealt with dcId
    UPDATE DiskCopy SET status = 10  -- DISKCOPY_CANBEMIGR
     WHERE castorFile = varCfId AND status = 0;  -- DISKCOPY_STAGED
  END IF;
  -- restart the subrequest so that the stager can follow it up or repack can proceed
  UPDATE /*+ INDEX(SR PK_Subrequest_Id)*/ SubRequest
     SET status = decode(reqType,
                         119, dconst.SUBREQUEST_REPACK, -- repack case
                         dconst.SUBREQUEST_RESTART),    -- standard case
         getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
         lastModificationTime = getTime(),
         parent = 0,
         diskCopy = varDcId
   WHERE id = varSubrequestId;
  -- restart other requests waiting on this recall
  UPDATE /*+ INDEX(SR I_Subrequest_Parent)*/ SubRequest
     SET status = decode(reqType,
                         119, dconst.SUBREQUEST_REPACK, -- repack case
                         dconst.SUBREQUEST_RESTART),    -- standard case
         getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
         lastModificationTime = getTime(),
         parent = 0,
         diskCopy = varDcId
   WHERE parent = varSubrequestId;
  -- trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(varCfId, varEuid, varEgid);
  -- commit
  COMMIT;
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
          archivesubreq(varSrIds(i), 9);
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

/* save in the db the results returned by the retry policy for recall */
CREATE OR REPLACE PROCEDURE tg_setRecRetryResult(
  rjToRetry IN castor."cnumList", 
  rjToFail  IN castor."cnumList"  ) AS
  tapeId NUMBER;
  cfId NUMBER;

BEGIN
  -- I restart the recall that I want to retry
  -- check because oracle cannot handle empty buffer
  IF rjToRetry(rjToRetry.FIRST) != -1 THEN 

    -- recall job => TOBERECALLED
    FORALL i IN rjToRetry.FIRST .. rjToRetry.LAST
      UPDATE RecallJob
        SET status    = tconst.RECALLJOB_TOBERECALLED,
            errorcode = 0,
            nbretry   = nbretry+1 
        WHERE id=rjToRetry(i);
    
    -- segment => UNPROCESSED
    -- tape => PENDING if UNUSED OR FAILED with still segments unprocessed
    FOR i IN rjToRetry.FIRST .. rjToRetry.LAST LOOP
      UPDATE Segment
        SET status = tconst.SEGMENT_UNPROCESSED
        WHERE copy = rjToRetry(i)
        RETURNING tape INTO tapeId;
      UPDATE Tape
        SET status = tconst.TAPE_WAITPOLICY
        WHERE id = tapeId AND
          status IN (tconst.TAPE_UNUSED, tconst.TAPE_FAILED);
    END LOOP;
  END IF;
  
  -- I mark as failed the hopeless recall
  -- check because oracle cannot handle empty buffer
  IF rjToFail(rjToFail.FIRST) != -1 THEN
    FOR i IN rjToFail.FIRST .. rjToFail.LAST  LOOP

      -- lock castorFile
      SELECT castorFile INTO cfId 
        FROM RecallJob, CastorFile
        WHERE RecallJob.id = rjToFail(i) 
        AND CastorFile.id = RecallJob.castorfile 
        FOR UPDATE OF castorfile.id;

      -- fail diskcopy
      UPDATE DiskCopy SET status = dconst.DISKCOPY_FAILED
        WHERE castorFile = cfId 
        AND status = dconst.DISKCOPY_WAITTAPERECALL;
      
      -- Drop recall jobs. Ideally, we should keep some track that
      -- the recall failed in order to prevent future recalls until some
      -- sort of manual intervention. For the time being, as we can't
      -- say whether the failure is fatal or not, we drop everything
      -- and we won't deny a future request for recall.
      deleteRecallJobs(cfId);
      
      -- fail subrequests
      UPDATE /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ SubRequest 
        SET status = dconst.SUBREQUEST_FAILED,
            getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED, --  (not strictly correct but the request is over anyway)
            lastModificationTime = getTime(),
            errorCode = 1015,  -- SEINTERNAL
            errorMessage = 'File recall from tape has failed, please try again later',
            parent = 0
        WHERE castorFile = cfId 
        AND status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_WAITSUBREQ);
    
    END LOOP;
  END IF;
  COMMIT;
END;
/


/* update the db when a tape session is started */
CREATE OR REPLACE
PROCEDURE  tg_startTapeSession(
  inVdqmReqId    IN  NUMBER,
  outVid         OUT NOCOPY VARCHAR2,
  outAccessMode  OUT INTEGER,
  outRet         OUT INTEGER,
  outDensity     OUT NOCOPY VARCHAR2,
  outLabel       OUT NOCOPY VARCHAR2 ) AS
  varTGReqId         NUMBER;
  varTpId            NUMBER;
  varMountId        NUMBER;
  varUnused          NUMBER;
BEGIN
  outRet:=-2;
  -- set the request to ONGOING
  -- Transition from REQUEST WAITING TAPE SERVER to ONGOING
  -- is equivalent to WAITTAPERIVE to MOUNTED for the tape read
  -- and WAITDRIVE or WAITMOUNT to RUNNING for a migration job.

  -- Step 1, pick the migration mount or tape.
  tg_findFromVDQMReqId(inVdqmReqId, varTpId, varMountId);
  IF (varTpId IS NOT NULL) THEN
    -- Read case
    outAccessMode := 0;
    BEGIN
      SELECT 1 INTO varUnused FROM dual
       WHERE EXISTS (SELECT 'x' FROM Segment S
                      WHERE S.tape = varTpId);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      UPDATE Tape T
         SET T.lastvdqmpingtime=0
       WHERE T.id = varTpId; -- to force the cleanup
      outRet:=-1; --NO MORE FILES
      COMMIT;
      RETURN;
    END;
    UPDATE Tape T
       SET T.status = tconst.TAPE_MOUNTED
     WHERE T.id = varTpId
       AND T.tpmode = tconst.TPMODE_READ
    RETURNING T.vid,  T.label,  T.density
      INTO   outVid, outLabel, outDensity; -- tape is MOUNTED
    outRet:=0;
    COMMIT;
    RETURN;
  ELSIF (varMountId IS NOT NULL) THEN
    -- Write case
    outAccessMode := 1;
    BEGIN
      SELECT 1 INTO varUnused FROM dual
       WHERE EXISTS (SELECT 'x' FROM MigrationJob, MigrationMount
                      WHERE MigrationJob.tapepool = MigrationMount.tapepool
                        AND MigrationMount.id = varMountId);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- no more files
      UPDATE MigrationMount
         SET lastVDQMPingTime = 0
       WHERE id = varMountId;
      outRet:=-1; --NO MORE FILES
      outVid:=NULL;
      COMMIT;
      RETURN;
    END;
    UPDATE MigrationMount
       SET status = tconst.MIGRATIONMOUNT_MIGRATING
     WHERE id = varMountId
     RETURNING VID, label, density INTO outVid, outLabel, outDensity;
    outRet:=0;
    COMMIT;
  ELSE
    -- Not found
    outRet:=-2; -- UNKNOWN request
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
  inTransId         IN  NUMBER,
  inFileId          IN  NUMBER,
  inNsHost          IN  VARCHAR2,
  inFseq            IN  INTEGER,
  inFileTransaction IN  NUMBER) AS
  varCfId               NUMBER;
  varUnused             NUMBER;
BEGIN
  -- Find MigrationMount from vdqm vol req ID and lock it
  SELECT VDQMVolReqId INTO varUnused FROM MigrationMount
   WHERE VDQMVolReqId = inTransId FOR UPDATE;
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

/* delete taperequest  for not existing tape */
CREATE OR REPLACE
PROCEDURE tg_deleteTapeRequest( inTGReqId IN NUMBER ) AS
  /* The tape gateway request does not exist per se, but 
   * references to its ID should be removed (with needed consequences
   * from the structures pointing to it) */
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varTpReqId NUMBER;     -- Tape Recall (TapeGatewayReequest.TapeRecall)
  varMountId NUMBER;       -- migration mount Id.
  varSegNum INTEGER;
  varCfId NUMBER;        -- CastorFile Id
  varRjIds "numList";    -- recall job IDs
BEGIN
  -- Find the relevant migration mount or reading tape id.
  tg_findFromTGRequestId (inTGReqId, varTpReqId, varMountId);
  -- Find out whether this is a read or a write
  IF (varTpReqId IS NOT NULL) THEN
    -- Lock and reset the tape in case of a read
    UPDATE Tape T
      SET T.status = tconst.TAPE_UNUSED
      WHERE T.id = varTpReqId;
    SELECT SEG.copy BULK COLLECT INTO varRjIds 
      FROM Segment SEG 
     WHERE SEG.tape = varTpReqId;
    FOR i IN varRjIds.FIRST .. varRjIds.LAST  LOOP
      -- lock castorFile
      SELECT RJ.castorFile INTO varCfId 
        FROM RecallJob RJ, CastorFile CF
        WHERE RJ.id = varRjIds(i) 
        AND CF.id = RJ.castorfile 
        FOR UPDATE OF CF.id;
      -- fail diskcopy, drop recall and migration jobs
      UPDATE DiskCopy DC SET DC.status = dconst.DISKCOPY_FAILED
       WHERE DC.castorFile = varCfId 
         AND DC.status = dconst.DISKCOPY_WAITTAPERECALL;
      deleteRecallJobs(varCfId);
      -- Fail the subrequest
      UPDATE /*+ INDEX(SR I_Subrequest_Castorfile)*/ SubRequest SR
         SET SR.status = dconst.SUBREQUEST_FAILED,
             SR.getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED, --  (not strictly correct but the request is over anyway)
             SR.lastModificationTime = getTime(),
             SR.errorCode = 1015,  -- SEINTERNAL
             SR.errorMessage = 'File recall from tape has failed, please try again later',
             SR.parent = 0
       WHERE SR.castorFile = varCfId 
         AND SR.status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_WAITSUBREQ);
       -- Release lock on castorFile
       COMMIT;
    END LOOP;
  ELSIF (varMountId IS NOT NULL) THEN
    -- In case of a write, delete the migration mount
    DELETE FROM MigrationMount  WHERE vdqmVolReqId = varMountId;
  ELSE
    -- Wrong Access Mode encountered. Notify.
    RAISE_APPLICATION_ERROR(-20292, 'tg_deleteTapeRequest: no read tape or '||
      'migration mount found for TapeGatewayRequestId: '|| inTGReqId);
  END IF;
END;
/

/* flag tape as full for a given session */
CREATE OR REPLACE
PROCEDURE tg_flagTapeFull ( inVDQMReqId IN NUMBER ) AS
  /* The tape gateway request does not exist per se, but 
   * references to its ID should be removed (with needed consequences
   * from the structures pointing to it) */
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varUnused NUMBER;
  varMJId NUMBER;
BEGIN
  -- Find the relevant migration or recall mount id.
  tg_findFromVDQMReqId (inVDQMReqId, varUnused, varMJId);
  -- Find out whether this is a read or a write
  IF (varMJId IS NOT NULL) THEN
    UPDATE MigrationMount
       SET full = 1
     WHERE id = varMJId;
  ELSE
    -- Wrong Access Mode encountered. Notify.
    RAISE_APPLICATION_ERROR(-20292, 'tg_flagTapeFullForMigrationSession: '||
      'no migration mount found for VDQMRequestId: '|| inVDQMReqId);
  END IF;
END;
/

/* Find the VID of the tape used in a tape session */
CREATE OR REPLACE
PROCEDURE tg_getMigrationMountVid (
    inVDQMReqId     IN NUMBER,
    outVid          OUT NOCOPY VARCHAR2,
    outTapePool     OUT NOCOPY VARCHAR2) AS
    varMMId         NUMBER;
    varUnused       NUMBER;
BEGIN
  -- Find the relevant stream.
  tg_findFromVDQMReqId (inVDQMReqId, varUnused, varMMId);
  -- Return migration mount and tapepool information
  IF (varMMId IS NOT NULL) THEN
    SELECT MM.vid,     TP.name
      INTO outVid, outTapePool
      FROM MigrationMount MM
     INNER JOIN TapePool TP ON TP.id = MM.tapePool
     WHERE MM.id = varMMId;
  ELSE
    -- Wrong Access Mode encountered. Notify.
    RAISE_APPLICATION_ERROR(-20292, 'tg_getMigrationMountVid: '||
      'no migration mount found for VDQMRequestId: '|| inVDQMReqId);
  END IF;
END;
/
