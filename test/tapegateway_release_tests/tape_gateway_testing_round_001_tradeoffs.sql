/*******************************************************************
 *
 * @(#)$RCSfile$ $Revision$ $Date$ $Author$
 *
 * This file contains is an SQL script used to test polices of the tape gateway
 * on a disposable stager schema, which is supposed to be initially empty.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

 /* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE;


create or replace
TRIGGER TR_TapeCopy_VID
BEFORE INSERT OR UPDATE OF Status ON TapeCopy
FOR EACH ROW
BEGIN
  /* Enforce the state integrity of VID in state transitions */

  CASE
    WHEN (:new.Status IN (3) AND :new.VID IS NULL) -- 3 TAPECOPY_SELECTED
      THEN
      /* No enforcement for rtcpclientd to let it run unmodified */
      DECLARE
        varRtcpcdRunning  NUMBER;
      BEGIN
        SELECT COUNT (*) INTO varRtcpcdRunning
          FROM CastorConfig ccfg
         WHERE ccfg.class = 'tape'
           AND ccfg.key   = 'interfaceDaemon'
           AND ccfg.value = 'rtcpclientd';
        IF (varRtcpcdRunning = 0) THEN
          RAISE_APPLICATION_ERROR(-20119,
            'Moving/creating (in)to TAPECOPY_SELECTED State without a VID');
        END IF;
      END;
    WHEN (UPDATING('Status') AND :new.Status IN (5, 9) AND -- 5 TAPECOPY_STAGED
                                                      -- 9 TAPECOPY_MIG_RETRY
      :new.VID != :old.VID)
      THEN
      RAISE_APPLICATION_ERROR(-20119,
        'Moving to STAGED/MIG_RETRY State without carrying the VID over');
    WHEN (:new.status IN (8)) -- 8 TAPECOPY_REC_RETRY
      THEN
      NULL; -- Free for all case
    WHEN (:new.Status NOT IN (3,5,8,9) AND :new.VID IS NOT NULL)
                                                     -- 3 TAPECOPY_SELECTED
                                                     -- 5 TAPECOPY_STAGED
                                                     -- 8 TAPECOPY_REC_RETRY
                                                     -- 9 TAPECOPY_MIG_RETRY
      THEN
      RAISE_APPLICATION_ERROR(-20119,
        'Moving/creating (in)to TapeCopy state where VID makes no sense, yet VID!=NULL');
    ELSE
      NULL;
  END CASE;
END;
/

/* Adding proper exception raising in case of error finding the service class */

create or replace
PROCEDURE migHunterCleanUp(svcName IN VARCHAR2)
AS
  svcId NUMBER;
BEGIN
  SELECT id INTO svcId FROM SvcClass WHERE name = svcName;
  -- clean up tapecopies, WAITPOLICY reset into TOBEMIGRATED
  UPDATE
     /*+ LEADING(TC CF)
         INDEX_RS_ASC(CF PK_CASTORFILE_ID)
         INDEX_RS_ASC(TC I_TAPECOPY_STATUS) */
         TapeCopy TC
     SET status = 1
   WHERE status = 7
     AND EXISTS (
       SELECT 'x'
         FROM CastorFile CF
        WHERE TC.castorFile = CF.id
          AND CF.svcclass = svcId);
  -- clean up streams, WAITPOLICY reset into CREATED
  UPDATE Stream SET status = 5 WHERE status = 7 AND tapepool IN
   (SELECT svcclass2tapepool.child
      FROM svcclass2tapepool
     WHERE svcId = svcclass2tapepool.parent);
  COMMIT;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    ROLLBACK;
    RAISE_APPLICATION_ERROR (-20119, 'Service class not found: '||svcname||
      ' in migHunterCleanUp');
END;
/

/* tg_getTapeWithoutDriveReq returned vdqm request ID instead of tape gateway request ID. Fixed */

create or replace
PROCEDURE tg_getTapeWithoutDriveReq(
  outReqId    OUT NUMBER,
  outTapeMode OUT NUMBER,
  outTapeSide OUT INTEGER,
  outTapeVid  OUT NOCOPY VARCHAR2) AS
  varStreamId     NUMBER;
BEGIN
  -- Initially looked for tapegateway request in state TO_BE_SENT_TO_VDQM
  -- Find a tapegateway request id for which there is a tape read in
  -- state TAPE_PENDING or a Stream in state STREAM_WAIT_TAPE.
  -- This method is called until there are no more pending tapes
  -- We serve writes first and then reads
  BEGIN
    SELECT S.id INTO varStreamId
      FROM Stream S
     WHERE S.status = 8 -- STREAM_TOBESENTTOVDQM
       AND ROWNUM < 2
       FOR UPDATE SKIP LOCKED;
    SELECT S.TapeGatewayRequestId,     1,      T.side,      T.vid
      INTO outReqId, outTapeMode, outTapeSide, outTapeVid
      FROM Stream S, Tape T
     WHERE T.id = S.tape
       AND S.id = varStreamId;
    RETURN;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL; -- No write found, carry on to the read.
  END;
  BEGIN -- The read casse
    SELECT T.TapeGatewayRequestId,     0,      T.side,      T.vid
      INTO outReqId, outTapeMode, outTapeSide, outTapeVid
      FROM Tape T
     WHERE T.tpMode = 0 -- TAPE_READ
       AND T.status = 1 -- TAPE_PENDING
       AND ROWNUM < 2
       FOR UPDATE SKIP LOCKED;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    outReqId := 0;
  END;
END;
/

/* Fixed vdqm request ID being put in tape instead of stream, plus wrong location for the piece of code updating the stream. */

create or replace
PROCEDURE tg_attachTapesToStreams (
  inStartFseqs IN castor."cnumList",
  inStrIds     IN castor."cnumList",
  inTapeVids   IN castor."strList") AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  varTapeId NUMBER;
  varUnused NUMBER;
BEGIN
  -- Sanity check
  IF (inStartFseqs.COUNT != inStrIds.COUNT
    OR inStrIds.COUNT != inTapeVids.COUNT) THEN
    RAISE_APPLICATION_ERROR (-20119,
       'Size mismatch for arrays: inStartFseqs.COUNT='||inStartFseqs.COUNT
       ||' inStrIds.COUNT='||inStrIds.COUNT
       ||' inTapeVids.COUNT='||inTapeVids.COUNT);
  END IF;
  FOR i IN inStrIds.FIRST .. inStrIds.LAST LOOP
    varTapeId:=NULL;
    -- Lock the stream (will be updated later).
    SELECT S.Id INTO varUnused
      FROM Stream S
     WHERE S.Id = inStrIds(i)
       FOR UPDATE;
    -- Try and update the tape. In case of failure (not found) we'll create it.
    UPDATE Tape T
       SET T.Stream = inStrIds(i),
           T.Status = 2, -- TAPE_WAITDRIVE
           T.lastFseq = inStartfseqs(i)
     WHERE T.tpmode= 1 -- TAPE_MODE_WRITE
       AND T.vid=inTapeVids(i)
    RETURNING T.Id INTO varTapeId;
    -- If there was indeed no tape, just create it.
    IF varTapeId IS NULL THEN
      DECLARE
        varTape Tape%ROWTYPE;
      BEGIN
        -- Try to insert the tape
        SELECT ids_seq.nextval INTO varTape.id FROM DUAL;
        varTape.vid       := inTapeVids(i);
        varTape.side      := 0;
        varTape.tpMode    := 1; -- TAPE_MODE_WRITE
        varTape.errMsgTxt := NULL;
        varTape.errorCode := 0;
        varTape.severity  := 0;
        varTape.vwaddress := NULL;
        varTape.stream    := inStrIds(i);
        varTape.status    := 2;  -- TAPE_WAITDRIVE
        varTape.lastFseq  := inStartfseqs(i);
        INSERT INTO Tape T
        VALUES varTape;
        INSERT INTO id2type (id,type) values (varTape.Id,29);
      EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- TODO: proper locking could prevent this.
      -- It could happen that the tape go created in the mean time. So now we
      -- can update it
        UPDATE Tape T
           SET T.Stream = inStrIds(i),
               T.Status = 2 -- TAPE_WAITDRIVE
         WHERE T.tpmode = 1 -- TAPE_MODE_WRITE
           AND T.vid = inTapeVids(i)
        RETURNING T.id INTO varTapeId;
        -- If this failed, then blow up!
        IF (varTapeId IS NULL) THEN
          ROLLBACK;
          RAISE_APPLICATION_ERROR (-20119,
            'in tg_attachTapesToStreams, failed to recreate or update tape in '||
            'write mode for tape VID ='||inTapeVids(i)||' for tape '||i||
            ' out of '||inTapeVids.COUNT||'. Rolled back the whole operation.');
        END IF;
      END;
    END IF;
    -- Finally update the stream we locked earlier
    UPDATE Stream S
       SET S.tape = varTapeId,
           S.status = 8 -- STREAM_TOBESENTTOVDQM
     WHERE S.id = inStrIds(i);
    -- And save this loop's result
    COMMIT;
  END LOOP;
END;
/

/* Added missing commits in some execution pathes */

create or replace
PROCEDURE  tg_startTapeSession(
  inVdqmReqId    IN  NUMBER,
  outVid         OUT NOCOPY VARCHAR2,
  outAccessMode  OUT INTEGER,
  outRet         OUT INTEGER,
  outDensity     OUT NOCOPY VARCHAR2,
  outLabel       OUT NOCOPY VARCHAR2 ) AS
  varTGReqId         NUMBER;
  varTpId            NUMBER;
  varStreamId        NUMBER;
  varUnused          NUMBER;
BEGIN
  outRet:=-2;
  -- set the request to ONGOING
  -- Transition from REQUEST WAITING TAPE SERVER to ONGOING
  -- is equivalent to WAITTAPERIVE to MOUNTED for the tape read
  -- and WAITDRIVE ot WAITMOUNT to RUNNING for a stream.

  -- Step 1, pick the stream or tape.
  tg_findFromVDQMReqId(inVdqmReqId, varTpId, varStreamId);
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
       SET T.status = 4 -- TAPE_MOUNTED
     WHERE T.id = varTpId
       AND T.tpmode = 0   -- Read
    RETURNING T.vid,  T.label,  T.density
      INTO   outVid, outLabel, outDensity; -- tape is MOUNTED
    outRet:=0;
    COMMIT;
    RETURN;
  ELSIF (varStreamId IS NOT NULL) THEN
    -- Write case
    outAccessMode := 1;
    BEGIN
      SELECT 1 INTO varUnused FROM dual
       WHERE EXISTS (SELECT 'x' FROM Stream2TapeCopy STTC
                      WHERE STTC.parent = varStreamId);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- no more files
      SELECT S.tape INTO varTpId
        FROM Stream S
       WHERE S.id = varStreamId;
      UPDATE Tape T
         SET T.lastvdqmpingtime=0
       WHERE T.id=varTpId; -- to force the cleanup
      outRet:=-1; --NO MORE FILES
      outVid:=NULL;
      COMMIT;
      RETURN;
    END;
    UPDATE Stream S
       SET S.status = 3 -- STREAM_RUNNING
     WHERE S.id = varStreamId
     RETURNING S.tape INTO varTpId; -- RUNNING
    UPDATE Tape T
       SET T.status = 4 -- TAPE_MOUNTED
     WHERE T.id = varTpId
    RETURNING T.vid,  T.label,  T.density
        INTO outVid, outLabel, outDensity;
    outRet:=0;
    COMMIT;
  ELSE
    -- Not found
    outRet:=-2; -- UNKNOWN request
  END IF;
END;
/

/* tg_endtapesession did not commit  (And it should). */ 

create or replace
PROCEDURE tg_endTapeSession(inTransId IN NUMBER, inErrorCode IN INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varUnused NUMBER;
  varTpId NUMBER;        -- TapeGateway Taperecall
  varTgrId NUMBER;       -- TapeGatewayRequest ID
  varStrId NUMBER;       -- Stream ID
  varSegNum INTEGER;     -- Segment count
  varTcIds "numList";    -- TapeCopy Ids

BEGIN
  -- Prepare to revert changes
  SAVEPOINT MainEndTapeSession;
  -- Find the Tape read or Stream for this VDQM request
  tg_findFromVDQMReqId (inTransId, varTpId, varStrId);
  -- Pre-process the read and write: find corresponding TapeGatewayRequest Id.
  -- Lock corresponding Tape or Stream. This will bomb if we
  -- don't find exactly ones (which is good).
  varTgrId := NULL;
  IF (varTpId IS NOT NULL) THEN
    -- Find and lock tape
    SELECT T.TapeGatewayRequestId INTO varTgrId
      FROM Tape T
     WHERE T.Id = varTpId
       FOR UPDATE;
  ELSIF (varStrId IS NOT NULL) THEN
    -- Find and lock stream
    SELECT S.TapeGatewayRequestId INTO varTgrId
      FROM Stream S
     WHERE S.Id = varStrId
       FOR UPDATE;
  ELSE
    -- Nothing found for the VDQMRequestId: whine and leave.
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'No tape or stream found for VDQM ID='|| inTransId);
  END IF;
  -- If we failed to get the TG req Id, no point in going further.
  IF (varTgrId IS NULL) THEN
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'Got NULL TapeGatewayRequestId for tape ID='|| varTpId||
     ' or Stream Id='|| varStrId||' processing VDQM Id='||inTransId||
     ' in tg_endTapeSession.');
  END IF;

  -- Common processing for reads and write: find and lock the tape copies.
  SELECT TC.id BULK COLLECT INTO varTcIds
    FROM TapeCopy TC
   WHERE TC.TapeGatewayRequestId = varTgrId
     FOR UPDATE OF TC.id;

  -- Process the read case
  IF (varTpId IS NOT NULL) THEN
    IF (inErrorCode != 0) THEN
        -- if a failure is reported
        -- fail all the segments
        UPDATE Segment SEG
           SET SEG.status=6  -- SEGMENT_FAILED
         WHERE SEG.copy IN (SELECT * FROM TABLE(varTcIds));
        -- mark tapecopies as  REC_RETRY
        UPDATE TapeCopy TC
           SET TC.status    = 8, -- TAPECOPY_REC_RETRY
               TC.errorcode = inErrorCode
         WHERE TC.id IN (SELECT * FROM TABLE(varTcIds));
    END IF;
    -- resurrect lost segments
    UPDATE Segment SEG
       SET SEG.status = 0  -- SEGMENT_UNPROCESSED
     WHERE SEG.status = 7  -- SEGMENT_SELECTED
       AND SEG.tape = varTpId;
    -- check if there is work for this tape
    SELECT count(*) INTO varSegNum
      FROM segment SEG
     WHERE SEG.Tape = varTpId
       AND status = 0;  -- SEGMENT_UNPROCESSED
    -- Restart the unprocessed segments' tape if there are any.
    IF varSegNum > 0 THEN
      UPDATE Tape T
         SET T.status = 8 -- TAPE_WAITPOLICY for rechandler
       WHERE T.id=varTpId;
    ELSE
      UPDATE Tape
         SET status = 0 -- TAPE_UNUSED
       WHERE id=varTpId;
     END IF;
  ELSIF (varStrId IS NOT NULL) THEN

    -- Process the write case
    deleteOrStopStream(varStrId);
    IF inErrorCode != 0 THEN
      -- if a failure is reported
      -- retry MIG_RETRY
      UPDATE TapeCopy TC
         SET TC.status=9,  -- TAPECOPY_MIG_RETRY
             TC.errorcode=inErrorCode,
             TC.nbretry=0
       WHERE TC.id IN (SELECT * FROM TABLE(varTcIds));
    ELSE
      -- just resurrect them if they were lost
      UPDATE TapeCopy TC
         SET TC.status = 1 -- TAPECOPY_TOBEMIGRATED
       WHERE TC.id IN (SELECT * FROM TABLE(varTcIds))
         AND TC.status = 3; -- TAPECOPY_SELECT
    END IF;
  ELSE

    -- Small infusion of paranoia ;-) We should never reach that point...
    ROLLBACK TO SAVEPOINT MainEndTapeSession;
    RAISE_APPLICATION_ERROR (-20119,
     'No tape or stream found on second pass for VDQM ID='|| inTransId ||
     ' in tg_endTapeSession');
  END IF;
  COMMIT;
END;
/

/* tg_setMigRetryResult failed to remove the vid of the tapecopy when puting it in error. */

create or replace
PROCEDURE tg_setMigRetryResult(
  tcToRetry IN castor."cnumList",
  tcToFail  IN castor."cnumList" ) AS
  srId NUMBER;
  cfId NUMBER;

BEGIN
   -- check because oracle cannot handle empty buffer
  IF tcToRetry( tcToRetry.FIRST) != -1 THEN
    
    -- restarted the one to be retried
    FOR i IN tctoretry.FIRST .. tctoretry.LAST LOOP
      UPDATE TapeCopy SET
        status = 1, -- TAPECOPY_TOBEMIGRATED
        nbretry = nbretry+1,
        vid = NULL  -- this tapecopy will not go to this volume after all, at least not now...
        WHERE id = tcToRetry(i);
    END LOOP;
  END IF;

  -- check because oracle cannot handle empty buffer
  IF tcToFail(tcToFail.FIRST) != -1 THEN
    -- fail the tapecopies
    FORALL i IN tctofail.FIRST .. tctofail.LAST
      UPDATE TapeCopy SET
        status = 6 -- TAPECOPY_FAILED
        WHERE id = tcToFail(i);

    -- fail repack subrequests
    FOR i IN tcToFail.FIRST .. tcToFail.LAST LOOP
        BEGIN
        -- we don't need a lock on castorfile because we cannot have a parallel migration of the same file using repack
          SELECT SubRequest.id, SubRequest.castorfile into srId, cfId
            FROM SubRequest,TapeCopy
            WHERE TapeCopy.id = tcToFail(i)
            AND SubRequest.castorfile = TapeCopy.castorfile
            AND subrequest.status = 12;

          -- STAGED because the copy on disk most probably is valid and the failure of repack happened during the migration

          UPDATE DiskCopy
            SET status = 0  -- DISKCOPY_STAGED
            WHERE castorfile = cfId
            AND status=10; -- otherwise repack will wait forever

          archivesubreq(srId,9);

        EXCEPTION WHEN NO_DATA_FOUND THEN
          NULL;
        END;
     END LOOP;
  END IF;

  COMMIT;
END;
/

/* tg_getFileToMigrate failed to return a unique file transaction ID instead of the heuristic tapecopy id
  as a transaction number and failed to store any value in the field filetransactionID for the tape copy.
  now fixed. */

create or replace
PROCEDURE tg_getFileToMigrate(
  inVDQMtransacId IN  NUMBER,
  outRet           OUT INTEGER,
  outVid        OUT NOCOPY VARCHAR2,
  outputFile    OUT castorTape.FileToMigrateCore_cur) AS
  /*
   * This procedure finds the next file to migrate according to a policy, chosen
   * from the context.
   */
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  varStrId NUMBER;
  varPolicy VARCHAR2(100);
  varDiskServer VARCHAR2(2048);
  varMountPoint VARCHAR2(2048);
  varPath  VARCHAR2(2048);
  varDiskCopyId NUMBER:=0;
  varCastorFileId NUMBER;
  varFileId NUMBER;
  varNsHost VARCHAR2(2048);
  varFileSize  INTEGER;
  varTapeCopyId  INTEGER:=0;
  varLastUpdateTime NUMBER;
  varLastKnownName VARCHAR2(2048);
  varTgRequestId NUMBER;
  varUnused INTEGER;
BEGIN
  outRet:=0;
  -- Get ready to rollback
  SAVEPOINT MainGetFileMigr;
  BEGIN
    -- Find stream
    tg_FindFromVDQMReqId (inVDQMtransacId, varUnused, varStrId);
    IF (varStrId IS NULL) THEN
      ROLLBACK TO SAVEPOINT MainFailFileSession;
      RAISE_APPLICATION_ERROR (-20119,
        'No stream found on second pass for VDQM ID='|| inVDQMtransacId||
        ' in tg_getFileToMigrate');
    END IF;
    -- Extracte tape gateway request Id.
    SELECT S.TapeGatewayRequestId INTO varTgRequestId
      FROM Stream S
     WHERE S.Id = varStrId;
    -- Get Tape's VID.
    SELECT T.VID INTO outVid
      FROM Tape T
     WHERE T.Id IN
         (SELECT S.Tape
            FROM Stream S
           WHERE S.Id = varStrId);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    ROLLBACK TO SAVEPOINT MainFailFileSession;
    outRet:=-2;   -- stream is over
    RETURN;
  END;
  -- Check for existence of tape copies for this stream
  BEGIN
    SELECT 1 INTO varUnused FROM dual
      WHERE EXISTS (SELECT 'x' FROM Stream2TapeCopy STTC
                      WHERE STTC.parent=varStrId);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    outRet:=-1;   -- no more files
    RETURN;
  END;
 -- lock to avoid deadlock with mighunter
  SELECT S.Id INTO varUnused FROM Stream S WHERE S.Id=varStrId
     FOR UPDATE OF S.Id;
  -- get the policy name and execute the policy
  /* BEGIN */
  SELECT TP.migrSelectPolicy INTO varPolicy
    FROM Stream S, TapePool TP
   WHERE S.Id = varStrId
     AND S.tapePool = TP.Id;
  -- check for NULL value
  IF varPolicy IS NULL THEN
    varPolicy := 'defaultMigrSelPolicy';
  END IF;
  /* Commenting out this catch as stream with no tape pool is an error condition
  TODO: Check and drop entirely.
  EXCEPTION WHEN NO_DATA_FOUND THEN
    varPolicy := 'defaultMigrSelPolicy';
  END;*/

  IF  varPolicy = 'repackMigrSelPolicy' THEN
    -- repack policy
    tg_repackMigrSelPolicy(varStrId,varDiskServer,varMountPoint,varPath,
      varDiskCopyId ,varCastorFileId,varFileId,varNsHost,varFileSize,
      varTapeCopyId,varLastUpdateTime);
  ELSIF  varPolicy = 'drainDiskMigrSelPolicy' THEN
    -- drain disk policy
    tg_drainDiskMigrSelPolicy(varStrId,varDiskServer,varMountPoint,varPath,
      varDiskCopyId ,varCastorFileId,varFileId,varNsHost,varFileSize,
      varTapeCopyId,varLastUpdateTime);
  ELSE
    -- default
    tg_defaultMigrSelPolicy(varStrId,varDiskServer,varMountPoint,varPath,
      varDiskCopyId ,varCastorFileId,varFileId,varNsHost,varFileSize,
      varTapeCopyId,varLastUpdateTime);
  END IF;

  IF varTapeCopyId = 0 OR varDiskCopyId=0 THEN
    outRet := -1; -- the migration selection policy didn't find any candidate
    COMMIT; -- TODO: Check if ROLLBACK is not better...
    RETURN;
  END IF;

  -- Here we found a tapeCopy and we process it
  -- update status of selected tapecopy and stream
  -- Sanity check: There should be no tapecopies for the same castor file where
  -- the volume ID is the same.
  DECLARE
    varConflicts NUMBER;
  BEGIN
    SELECT COUNT(*) INTO varConflicts
      FROM TapeCopy TC
     WHERE TC.CastorFile = varCastorFileId
       AND TC.VID = outVID;
    IF (varConflicts != 0) THEN
      RAISE_APPLICATION_ERROR (-20119, 'About to move a second copy to the same tape!');
    END IF;
  END;
  UPDATE TapeCopy TC
     SET TC.Status = 3, -- SELECTED
         TC.VID = outVID
   WHERE TC.Id = varTapeCopyId;
  -- detach the tapecopy from the stream now that it is SELECTED;
  DELETE FROM Stream2TapeCopy STTC
   WHERE STTC.child = varTapeCopyId;

  SELECT CF.lastKnownFileName INTO varLastKnownName
    FROM CastorFile CF
   WHERE CF.Id = varCastorFileId; -- we rely on the check done before TODO: which check?

  DECLARE
    varNewFseq NUMBER;
  BEGIN
   -- Atomically increment and read the next FSEQ to be written to. fSeq is held
   -- in the tape structure.
   UPDATE Tape T
      SET T.lastfseq=T.lastfseq+1
     WHERE T.Id IN
         (SELECT S.Tape
            FROM Stream S
           WHERE S.Id = varStrId)
     RETURNING T.lastfseq-1 into varNewFseq; -- The previous value is where we'll write

   -- Update the tapecopy and attach it to a newly created file transaction ID
   UPDATE TapeCopy TC
      SET TC.fSeq = varNewFseq,
          TC.tapeGatewayRequestId = varTgRequestId,
          TC.diskCopy = varDiskCopyId,
          TC.fileTransactionId = TG_FileTrId_Seq.NEXTVAL
    WHERE TC.Id = varTapeCopyId;

   OPEN outputFile FOR
     SELECT varFileId,varNshost,varLastUpdateTime,varDiskServer,varMountPoint,
            varPath,varLastKnownName,TC.fseq,varFileSize,TC.fileTransactionId
       FROM TapeCopy TC
      WHERE TC.Id = varTapeCopyId;

  END;
  COMMIT;
END;
/

/*  tg_attachDriveReqToTape did set the vdqm request ID to the tape instead of the stream in write mode */

create or replace
PROCEDURE tg_attachDriveReqToTape(
  inTapeRequestId IN NUMBER,
  inVdqmId    IN NUMBER,
  inDgn       IN VARCHAR2,
  inLabel     IN VARCHAR2,
  inDensity   IN VARCHAR2) AS
  varTapeId   INTEGER;
  varStreamId INTEGER;
/* Update the status and propoerties of the Tape structure related to
 * a tape request, and the Stream state in case of a write.
 * All other properties are attached to the tape itself.
 */
BEGIN
  -- Update tape of stream, whichever is relevant. First find:
  tg_findFromTGRequestId (inTapeRequestId, varTapeId, varStreamId);
  
  -- Process one or the other (we trust the called function to not provide both)
  IF (varTapeId IS NOT NULL) THEN
    -- In the case of a read, only the tape itself is involved
    -- update reading tape which have been submitted to vdqm => WAIT_DRIVE.
    UPDATE Tape T
       SET T.lastvdqmpingtime = gettime(),
           T.starttime        = gettime(),
           T.vdqmvolreqid     = inVdqmId,
           T.Status           = 2, -- TAPE_WAITDRIVE
           T.dgn              = inDgn,
           T.label            = inLabel,
           T.density          = inDensity
     WHERE T.Id = varTapeId;
    COMMIT;
    RETURN;
  ELSIF (varStreamId IS NOT NULL) THEN
    -- We have to update the tape as well (potentially, we keep the can-fail
    -- query based update of the previous system.
    SAVEPOINT Tape_Mismatch;
    DECLARE
      varTapeFromStream NUMBER;
      varTp             Tape%ROWTYPE;
    BEGIN
      UPDATE STREAM S
         SET S.Status = 1  -- STREAM_WAITDRIVE
       WHERE S.Id = varStreamId
      RETURNING S.Tape
        INTO varTapeFromStream;
      BEGIN
        SELECT T.* INTO varTp
          FROM Tape T
         WHERE T.Id = varTapeFromStream
           FOR UPDATE;
      EXCEPTION
        WHEN NO_DATA_FOUND OR TOO_MANY_ROWS THEN
          ROLLBACK TO SAVEPOINT Tape_Mismatch;
          RAISE_APPLICATION_ERROR (-20119,
            'Wrong number of tapes found for stream '||varStreamId);
      END;
      IF (varTp.TpMode != 1) THEN -- TAPE_WRITE
        ROLLBACK TO SAVEPOINT Tape_Mismatch;
        RAISE_APPLICATION_ERROR (-20119,
          'Wrong type of tape found for stream:'||varStreamId||' tape:'||
          varTp.Id||' TpMode:'||varTp.TpMode);
      END IF;
      varTp.Status          := 9; -- TAPE_ATTACHED_TO_STREAM
      varTp.dgn             := inDgn;
      varTp.label           := inLabel;
      varTp.density         := inDensity;
      varTp.vdqmvolreqid    := NULL; -- The VDQM request ID "belong" to the stream in write mode"
      UPDATE Tape T
         SET ROW = varTp
       WHERE T.Id = varTp.Id;
      UPDATE Stream S
         SET S.vdqmvolreqid = inVdqmId
       WHERE S.id = varStreamId;
      COMMIT;
      RETURN;
    END; -- END of local block for varTapeFromStream and varTp
  ELSE RAISE_APPLICATION_ERROR (-20119,
       'Found no stream or read tape for TapeRequestId: '||inTapeRequestId);
  END IF;
END;
/

/* resurrectTapes needed to do extra massaging on some half-set tapes (correct state, but not request ID) */
create or replace
PROCEDURE resurrectTapes
(tapeIds IN castor."cnumList")
AS
BEGIN
  IF (TapegatewaydIsRunning) THEN
    FOR i IN tapeIds.FIRST .. tapeIds.LAST LOOP
      UPDATE Tape T
         SET T.TapegatewayRequestId = ids_seq.nextval,
             T.status = 1
       WHERE T.status = 8 AND T.id = tapeIds(i);
      -- FIXME TODO this is a hack needed to add the TapegatewayRequestId which was missing.
      UPDATE Tape T
         SET T.TapegatewayRequestId = ids_seq.nextval
       WHERE T.status = 1 AND T.TapegatewayRequestId IS NULL 
         AND T.id = tapeIds(i);
    END LOOP; 
  ELSE
    FOR i IN tapeIds.FIRST .. tapeIds.LAST LOOP
      UPDATE Tape SET status = 1 WHERE status = 8 AND id = tapeIds(i);
    END LOOP;
  END IF;
  COMMIT;
END;
/

/* tg_getSegmentInfo: Droped useless arguments passed to the procedure (signature change) */

create or replace
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

  SELECT TC.copynb INTO outCopyNb
    FROM TapeCopy TC
   WHERE TC.fseq = inFseq
     AND TC.TapeGateWayRequestId = varTrId;
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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

/* Massage the procedures once for the not-tape gateway related */
BEGIN
  FOR obj in (
    SELECT *
      FROM user_objects
      WHERE
            object_type IN ('FUNCTION', 'PROCEDURE')
        AND status = 'INVALID' AND object_name NOT LIKE 'TG_%') LOOP
     EXECUTE IMMEDIATE 'ALTER ' || obj.object_type || ' ' || obj.object_name || ' COMPILE';
   END LOOP;
END;
/

/* Massage the TG family */
BEGIN
  FOR obj in (
    SELECT *
      FROM user_objects
      WHERE
            object_type IN ('FUNCTION', 'PROCEDURE')
        AND status = 'INVALID' AND object_name LIKE 'TG_%') LOOP
     EXECUTE IMMEDIATE 'ALTER ' || obj.object_type || ' ' || obj.object_name || ' COMPILE';
   END LOOP;
END;
/

/* Find the remaining buggers */
 SELECT *
      FROM user_objects
      WHERE
            object_type IN ('FUNCTION', 'PROCEDURE')
        AND status = 'INVALID';

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/
