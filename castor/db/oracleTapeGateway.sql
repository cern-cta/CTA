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
create or replace
PROCEDURE tg_findFromTGRequestId (
  inTapeGatewayRequestId IN  INTEGER,
  outTapeId              OUT INTEGER,
  outStreamId            OUT INTEGER) AS
BEGIN
  -- Will return a valid ID in either outTapeId or outStreamId,
  -- and NULL in the other when finding the object corresponding to
  -- this TGR request ID.
  --
  -- Will throw an exception in case of non-unicity.

  -- Look for read tapes:
  BEGIN
    SELECT T.id INTO outTapeId
      FROM TAPE T
     WHERE T.TapeGatewayRequestId = inTapeGatewayRequestId
       AND T.tpMode = 0; -- TAPE_READ
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outTapeId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple read tapes for same TapeGatewayRequestId: '|| 
         inTapeGatewayRequestId || ' in tg_findFromTGRequestId');
     -- Let other exceptions fly through.
   END;
   
   -- Look for streams
   BEGIN
     SELECT S.id INTO outStreamId
       FROM Stream S
      WHERE S.TapeGatewayRequestId = inTapeGatewayRequestId;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outStreamId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple streams for same TapeGatewayRequestId: '|| 
         inTapeGatewayRequestId || ' in tg_findFromTGRequestId');
     -- Let other exceptions fly through.     
   END;
   
   -- Check for stream/tape collision
   IF (outStreamId IS NOT NULL AND outTapeId IS NOT NULL) THEN
     RAISE_APPLICATION_ERROR (-20119, 
       'Found both read tape (id='||outTapeId||') and Stream (id='||
       outStreamId||') for TapeGatewayRequestId: '||
       inTapeGatewayRequestId || ' in tg_findFromTGRequestId');
   END IF;
END;
/

/* tg_findFromVDQMReqId */
create or replace
PROCEDURE tg_findFromVDQMReqId (
  inVDQMReqId IN  INTEGER,
  outTapeId              OUT INTEGER,
  outStreamId            OUT INTEGER) AS
BEGIN
  -- Will return a valid ID in either outTapeId or outStreamId,
  -- and NULL in the other when finding the object corresponding to
  -- this TGR request ID.
  --
  -- Will throw an exception in case of non-unicity.

  -- Look for read tapes:
  BEGIN
    SELECT T.id INTO outTapeId
      FROM TAPE T
     WHERE T.VDQMVolReqId = inVDQMReqId
       AND T.tpMode = 0; -- TAPE_READ
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outTapeId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple read tapes for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.
   END;
   
   -- Look for streams
   BEGIN
     SELECT S.id INTO outStreamId
       FROM Stream S
      WHERE S.VDQMVolReqId = inVDQMReqId;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       outStreamId := NULL;
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple streams for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.     
   END;
   
   -- Check for stream/tape collision
   IF (outStreamId IS NOT NULL AND outTapeId IS NOT NULL) THEN
     RAISE_APPLICATION_ERROR (-20119, 
       'Found both read tape (id='||outTapeId||') and Stream (id='||
       outStreamId||') for VDQMVolReqId: '||
       inVDQMReqId || ' in tg_findFromVDQMReqId');
   END IF;
END;
/

/* tg_RequestIdFromVDQMReqId */
create or replace
PROCEDURE tg_RequestIdFromVDQMReqId (
  inVDQMReqId IN  INTEGER,
  outTgrId    OUT INTEGER) AS
  varTapeId       INTEGER;
  varStreamId     INTEGER;
BEGIN
  -- Will return a valid tape gateway request Id if one and only one read
  -- tape or stream is found with this VDQM request ID.
  --
  -- Will throw an exception in case of non-unicity.
  --
  -- Will return NULL in case of not found.
  outTgrId := NULL;
  -- Look for read tapes:
  BEGIN
    SELECT T.id, T.TapeGatewayRequestId INTO varTapeId, outTgrId
      FROM TAPE T
     WHERE T.VDQMVolReqId = inVDQMReqId
       AND T.tpMode = 0; -- TAPE_READ
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL; -- It's OK, could be a stream.
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple read tapes for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.
   END;
   
   -- Look for streams
   BEGIN
     SELECT S.id, S.TapeGatewayRequestId INTO varStreamId, outTgrId
       FROM Stream S
      WHERE S.VDQMVolReqId = inVDQMReqId;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL; -- It's OK, might have been a tape.
     WHEN TOO_MANY_ROWS THEN
       RAISE_APPLICATION_ERROR (-20119, 
         'Found multiple streams for same VDQMVolReqId: '|| 
         inVDQMReqId || ' in tg_findFromVDQMReqId');
     -- Let other exceptions fly through.     
   END;
   
   -- Check for stream/tape collision
   IF (varStreamId IS NOT NULL AND varTapeId IS NOT NULL) THEN
     outTgrId := NULL;
     RAISE_APPLICATION_ERROR (-20119, 
       'Found both read tape (id='||varTapeId||') and Stream (id='||
       varStreamId||') for VDQMVolReqId: '||
       inVDQMReqId || ' in tg_findFromVDQMReqId');
   END IF;
END;
/

/* tg_findVDQMReqFromTGReqId */
create or replace
PROCEDURE tg_findVDQMReqFromTGReqId (
  inTGReqId     IN  INTEGER,
  outVDQMReqId  OUT INTEGER) AS
  varTapeId         NUMBER;
  varStreamId       NUMBER;
BEGIN
  -- Helper function. Wrapper to another helper.
  tg_findFromTGRequestId (inTGReqId, varTapeId, varStreamId);
  IF (varTapeId IS NOT NULL) THEN
    SELECT T.vdqmVolReqId INTO outVDQMReqId
      FROM Tape T WHERE T.id = varTapeId;
  ELSIF (varStreamId IS NOT NULL) THEN
    SELECT S.vdqmVolReqId INTO outVDQMReqId
      FROM Stream S WHERE S.id = varTapeId;  
  ELSE
    RAISE_APPLICATION_ERROR (-20119, 
         'Could not find stream or tape read for TG request Id='|| 
         inTGReqId || ' in tg_findVDQMReqFromTGReqId');
  END IF;
END;
/

/* attach drive request to tape */
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
        
/* attach the tapes to the streams  */
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
        VALUES varTape RETURNING T.id into varTapeId;
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
      END;
      -- If this failed, then blow up!
      IF (varTapeId IS NULL) THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR (-20119,
          'in tg_attachTapesToStreams, failed to recreate or update tape in '||
          'write mode for tape VID ='||inTapeVids(i)||' for tape '||i||
          ' out of '||inTapeVids.COUNT||'. Rolled back the whole operation.');
      END IF;
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

/* update the db when a tape session is ended */
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

/* mark a migration or recall as failed saving in the db the error code associated with the failure */
create or replace
PROCEDURE tg_failFileTransfer(
  inTransId      IN NUMBER,    -- The VDQM transaction ID
  inFileId    IN NUMBER,       -- File ID
  inNsHost    IN VARCHAR2,     -- NS Host
  inFseq      IN INTEGER,      -- Tapecopy's fSeq
  inErrorCode IN INTEGER)  AS  -- Error Code
  varUnused NUMBER;            -- dummy
  varTgrId NUMBER;             -- Tape Gateway Request Id
  varStrId NUMBER;             -- Stream Id
  varTpId NUMBER;              -- Tape Id
  varTcId NUMBER;              -- TapeCopy Id
  varAccessMode INTEGER;       -- Access mode
BEGIN
  -- Prepare to return everything to its oroginal state in case of problem.
  SAVEPOINT MainFailFileSession;
  
  -- Find related Read tape or stream from VDQM Id
  tg_findFromVDQMReqId(inTransId, varTpId, varStrId);
  
  -- Lock related castorfile -- TODO: This should be a procedrelized access to
  -- the disk system.
  SELECT CF.id INTO varUnused 
    FROM CastorFile CF
   WHERE CF.fileid = inFileId
     AND CF.nsHost = inNsHost 
    FOR UPDATE;
  
  -- Case dependant part
  IF (varTpId IS NOT NULL) THEN
    -- We handle a read case
    -- fail the segment on that tape
    UPDATE Segment SEG
       SET SEG.status    = 6, -- SEGMENT_FAILED
           SEG.severity  = inErrorCode,
           SEG.errorCode = -1 
     WHERE SEG.fseq = inFseq 
       AND SEG.tape = varTpId 
    RETURNING SEG.copy INTO varTcId;
    -- mark tapecopy as REC_RETRY
    UPDATE TapeCopy TC
       SET TC.status    = 8, -- TAPECOPY_REC_RETRY
           TC.errorcode = inErrorCode 
     WHERE TC.id = varTcId;  
  ELSIF (varStrId IS NOT NULL) THEN
    -- Write case
    SELECT T.id, S.TapeGatewayRequestId INTO varTpId, varTgrId
      FROM Tape T, Stream  S
     WHERE T.id = S.tape
       AND S.id = varStrId;
    -- mark tapecopy as MIG_RETRY. It should be the tapecopy with the proper 
    -- TapegatewayRequest + having a matching Fseq.
    UPDATE TapeCopy TC
       SET TC.status    = 9, -- TAPECOPY_MIG_RETRY
           TC.errorcode = inErrorCode 
     WHERE TC.TapegatewayRequestId = varTgrId
       AND TC.fSeq = inFseq; 
  ELSE
  
    -- Complain in case of failure
    ROLLBACK TO SAVEPOINT MainFailFileSession;
    RAISE_APPLICATION_ERROR (-20119, 
     'No tape or stream found on second pass for VDQM ID='|| inTransId||
     ' in tg_failFileTransfer');
  END IF;
EXCEPTION WHEN  NO_DATA_FOUND THEN
  NULL;
END;
/

/* retrieve from the db all the tapecopies that faced a failure for migration */
create or replace
PROCEDURE  tg_getFailedMigrations(outTapeCopies_c OUT castor.TapeCopy_Cur) AS
BEGIN
  -- get TAPECOPY_MIG_RETRY
  OPEN outTapeCopies_c FOR
    SELECT *
      FROM TapeCopy TC
     WHERE TC.status = 9 -- TAPECOPY_MIG_RETRY
       AND ROWNUM < 1000 
       FOR UPDATE SKIP LOCKED; 
END;
/


/* retrieve from the db all the tapecopies that faced a failure for recall */
create or replace
PROCEDURE  tg_getFailedRecalls(outTapeCopies_c OUT castor.TapeCopy_Cur) AS
BEGIN
  -- get TAPECOPY_REC_RETRY
  OPEN outTapeCopies_c FOR
    SELECT *
      FROM TapeCopy TC
     WHERE TC.status = 8 -- TAPECOPY_REC_RETRY
      AND ROWNUM < 1000 
      FOR UPDATE SKIP LOCKED;
END;
/

/* default migration candidate selection policy */
create or replace
PROCEDURE tg_defaultMigrSelPolicy(inStreamId IN INTEGER,
                                  outDiskServerName OUT NOCOPY VARCHAR2,
                                  outMountPoint OUT NOCOPY VARCHAR2,
                                  outPath OUT NOCOPY VARCHAR2,
                                  outDiskCopyId OUT INTEGER,
                                  outCastorFileId OUT INTEGER,
                                  outFileId OUT INTEGER,
                                  outNsHost OUT NOCOPY VARCHAR2, 
                                  outFileSize OUT INTEGER,
                                  outTapeCopyId OUT INTEGER, 
                                  outLastUpdateTime OUT INTEGER) AS
  /* Find the next tape copy to migrate from a given stream ID.
   * 
   * Procedure's input: Stream Id for a stream that is locked by caller.
   *
   * Procedure's output: Returns a non-zero TapeCopy ID on full success
   * Can return a non-zero DiskServer Id when a DiskServer got selected without 
   * selecting any tape copy.
   * Data modification: The function updates the stream's filesystem information
   * in case a new one got seleted.
   *
   * Lock taken on the diskserver in some cases.
   * Lock taken on the tapecopy if it selects one.
   * Lock taken on  the Stream when a new disk server is selected.
   * 
   * Commits: The function does not commit data.
   *
   * Per policy we should only propose a tape copy for a file that does not 
   * already have a tapecopy attached for or mirgated to the same
   * tape.
   * The tape's VID can be found from the streamId by:
   * Stream->Tape->VID.
   * The tapecopies carry VID themselves, when in stated STAGED, SELECTED and 
   * in error states. In other states the VID must be null, per constraint.
   * The already migrated tape copies are kept until the whole set of siblings 
   * have been migrated. Nothing else is guaranteed to be.
   * 
   * From this we can find a list of our potential siblings (by castorfile) from
   * this TapeGatewayRequest, and prevent the selection of tapecopies whose 
   * siblings already live on the same tape.
   */
  varFileSystemId INTEGER := 0;
  varDiskServerId NUMBER;
  varLastFSChange NUMBER;
  varLastFSUsed NUMBER;
  varLastButOneFSUsed NUMBER;
  varFindNewFS NUMBER := 1;
  varNbMigrators NUMBER := 0;
  varUnused NUMBER;
  LockError EXCEPTION;
  varVID VARCHAR2(2048 BYTE);
  PRAGMA EXCEPTION_INIT (LockError, -54);
BEGIN
  outTapeCopyId := 0;
  -- Find out which tape we're talking about
  SELECT T.VID INTO varVID 
    FROM Tape T, Stream S 
   WHERE S.Id = inStreamId 
     AND T.Id = S.Tape;
  -- First try to see whether we should resuse the same filesystem as last time
  SELECT S.lastFileSystemChange, S.lastFileSystemUsed, 
         S.lastButOneFileSystemUsed
    INTO varLastFSChange, varLastFSUsed, varLastButOneFSUsed
    FROM Stream S 
   WHERE S.id = inStreamId;
  -- If the filesystem has changed in the last 5 minutes, consider its reuse
  IF getTime() < varLastFSChange + 900 THEN
    -- Count the number of streams referencing the filesystem
    SELECT (SELECT count(*) FROM stream S
             WHERE S.lastFileSystemUsed = varLastButOneFSUsed) +
           (SELECT count(*) FROM stream S 
             WHERE S.lastButOneFileSystemUsed = varLastButOneFSUsed)
      INTO varNbMigrators FROM DUAL;
    -- only go if we are the only migrator on the file system.
    IF varNbMigrators = 1 THEN
      BEGIN
        -- check states of the diskserver and filesystem and get mountpoint 
        -- and diskserver name
        SELECT DS.name, FS.mountPoint, FS.id 
          INTO outDiskServerName, outMountPoint, varFileSystemId
          FROM FileSystem FS, DiskServer DS
         WHERE FS.diskServer = DS.id
           AND FS.id = varLastButOneFSUsed
           AND FS.status IN (0, 1)  -- PRODUCTION, DRAINING
           AND DS.status IN (0, 1); -- PRODUCTION, DRAINING
        -- we are within the time range, so we try to reuse the filesystem
        SELECT /*+ FIRST_ROWS(1)  LEADING(D T ST) */
               D.path, D.id, D.castorfile, T.id
          INTO outPath, outDiskCopyId, outCastorFileId, outTapeCopyId
          FROM DiskCopy D, TapeCopy T, Stream2TapeCopy STTC
         WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
           AND D.filesystem = varLastButOneFSUsed
           AND STTC.parent = inStreamId
           AND T.status = 2 -- WAITINSTREAMS
           AND STTC.child = T.id
           AND T.castorfile = D.castorfile
           -- Do not select a tapecopy for which a sibling TC is or will be on 
           -- on this tape.
           AND varVID NOT IN (
                 SELECT DISTINCT T2.VID FROM TapeCopy T2
                  WHERE T2.CastorFile=T.Castorfile
                    AND T2.Status IN (3, 5)) -- TAPECOPY_SELECTED, TAPECOPY_STAGED
           AND ROWNUM < 2 FOR UPDATE OF T.id NOWAIT;
        -- Get addition info
        SELECT CF.FileId, CF.NsHost, CF.FileSize, CF.lastUpdateTime
          INTO outFileId, outNsHost, outFileSize, outLastUpdateTime
          FROM CastorFile CF
         WHERE CF.Id = outCastorFileId;
        -- we found one, no need to go for new filesystem
        varFindNewFS := 0;
      EXCEPTION WHEN NO_DATA_FOUND OR LockError THEN
        -- found no tapecopy or diskserver, filesystem are down. We'll go 
        -- through the normal selection
        NULL;
      END;
    END IF;
  END IF;
  IF varFindNewFS = 1 THEN
    FOR f IN (
    SELECT FileSystem.id AS FileSystemId, DiskServer.id AS DiskServerId, 
           DiskServer.name, FileSystem.mountPoint
      FROM Stream, SvcClass2TapePool, DiskPool2SvcClass, FileSystem, DiskServer
     WHERE Stream.id = inStreamId
       AND Stream.TapePool = SvcClass2TapePool.child
       AND SvcClass2TapePool.parent = DiskPool2SvcClass.child
       AND DiskPool2SvcClass.parent = FileSystem.diskPool
       AND FileSystem.diskServer = DiskServer.id
       AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
       AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
     ORDER BY -- first prefer diskservers where no migrator runs and filesystems
              -- with no recalls
              DiskServer.nbMigratorStreams ASC, 
              FileSystem.nbRecallerStreams ASC,
              -- then order by rate as defined by the function
              fileSystemRate(FileSystem.readRate,
                             FileSystem.writeRate,
                             FileSystem.nbReadStreams,
                             FileSystem.nbWriteStreams,
                             FileSystem.nbReadWriteStreams,
                             FileSystem.nbMigratorStreams,
                             FileSystem.nbRecallerStreams) DESC,
              -- finally use randomness to avoid preferring always the same FS
              DBMS_Random.value) LOOP
    BEGIN
      -- Get ready to release lock if the diskserver or tapecopy is not 
      -- to our liking
      SAVEPOINT DServ_TCopy_Lock;
      -- lock the complete diskServer as we will update all filesystems
      SELECT D.id INTO varUnused FROM DiskServer D WHERE D.id = f.DiskServerId 
         FOR UPDATE NOWAIT;
      SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
             F.diskServerId, f.name, f.mountPoint, 
             f.fileSystemId, D.path, D.id,
             D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, 
             C.lastUpdateTime
          INTO varDiskServerId, outDiskServerName, outMountPoint, 
             varFileSystemId, outPath, outDiskCopyId,
             outCastorFileId, outFileId, outNsHost, outFileSize, outTapeCopyId, 
             outLastUpdateTime
          FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
         WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
           AND D.filesystem = f.fileSystemId
           AND StT.parent = inStreamId
           AND T.status = 2 -- WAITINSTREAMS
           AND StT.child = T.id
           AND T.castorfile = D.castorfile
           AND C.id = D.castorfile
           AND varVID NOT IN (
                 SELECT DISTINCT T2.VID FROM TapeCopy T2
                  WHERE T2.CastorFile=T.Castorfile
                    AND T2.Status IN (3, 5)) -- TAPECOPY_SELECTED, TAPECOPY_STAGED
           AND ROWNUM < 2 FOR UPDATE OF t.id NOWAIT;
        -- found something on this filesystem, no need to go on
        varDiskServerId := f.DiskServerId;
        varFileSystemId := f.fileSystemId;
        EXIT;
      EXCEPTION WHEN NO_DATA_FOUND OR LockError THEN
         -- either the filesystem is already locked or we found nothing,
         -- let's rollback in case there was NO_DATA_FOUND to release the lock
         ROLLBACK TO SAVEPOINT DServ_TCopy_Lock;
       END;
    END LOOP;
  END IF;

  IF outTapeCopyId = 0 THEN
    -- Nothing found, return; locks will be released by the caller
    RETURN;
  END IF;
  
  IF varFindNewFS = 1 THEN
    UPDATE Stream S
       SET S.lastFileSystemUsed = varFileSystemId,
           -- We store the old value (implicitely available
           -- when reading (reading = :old) to the new row value
           -- (write = :new). So it works.
           S.lastButOneFileSystemUsed = S.lastFileSystemUsed,
           S.lastFileSystemChange = getTime()
     WHERE S.id = inStreamId;
  END IF;

  -- Update Filesystem state
  updateFSMigratorOpened(varDiskServerId, varFileSystemId, 0);
END;
/

/* drain disk migration candidate selection policy */

create or replace PROCEDURE tg_drainDiskMigrSelPolicy(streamId IN INTEGER,
                                    diskServerName OUT NOCOPY VARCHAR2, 
                                    mountPoint OUT NOCOPY VARCHAR2,
                                    path OUT NOCOPY VARCHAR2,
                                    dci OUT INTEGER,
                                    castorFileId OUT INTEGER,
                                    fileId OUT INTEGER,
                                    nsHost OUT NOCOPY VARCHAR2,
                                    fileSize OUT INTEGER,
                                    tapeCopyId OUT INTEGER,
                                    lastUpdateTime OUT INTEGER) AS
  fileSystemId INTEGER := 0;
  diskServerId NUMBER;
  fsDiskServer NUMBER;
  lastFSChange NUMBER;
  lastFSUsed NUMBER;
  lastButOneFSUsed NUMBER;
  findNewFS NUMBER := 1;
  nbMigrators NUMBER := 0;
  unused NUMBER;
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
BEGIN
  tapeCopyId := 0;
  -- First try to see whether we should resuse the same filesystem as last time
  SELECT lastFileSystemChange, lastFileSystemUsed, lastButOneFileSystemUsed
    INTO lastFSChange, lastFSUsed, lastButOneFSUsed
    FROM Stream WHERE id = streamId;
  IF getTime() < lastFSChange + 1800 THEN
    SELECT (SELECT count(*) FROM stream WHERE lastFileSystemUsed = lastFSUsed)
      INTO nbMigrators FROM DUAL;
    -- only go if we are the only migrator on the box
    IF nbMigrators = 1 THEN
      BEGIN
        -- check states of the diskserver and filesystem and get mountpoint and diskserver name
        SELECT diskserver.id, name, mountPoint, FileSystem.id INTO diskServerId, diskServerName, mountPoint, fileSystemId
          FROM FileSystem, DiskServer
         WHERE FileSystem.diskServer = DiskServer.id
           AND FileSystem.id = lastFSUsed
           AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
           AND DiskServer.status IN (0, 1); -- PRODUCTION, DRAINING
        -- we are within the time range, so we try to reuse the filesystem
        
        SELECT /*+ ORDERED USE_NL(D T) INDEX(T I_TapeCopy_CF_Status_2) INDEX(ST I_Stream2TapeCopy_PC) */
              D.path, D.diskcopy_id, D.castorfile, T.id INTO path, dci, castorFileId, tapeCopyId
          FROM (SELECT /*+ INDEX(DK I_DiskCopy_FS_Status_10) */
                             DK.path path, DK.id diskcopy_id, DK.castorfile
                  FROM DiskCopy DK
                  WHERE decode(DK.status, 10, DK.status, NULL) = 10 -- CANBEMIGR
                  AND DK.filesystem = lastFSUsed)  D, TapeCopy T, Stream2TapeCopy ST
          WHERE T.castorfile = D.castorfile
          AND ST.child = T.id
          AND ST.parent = streamId
          AND decode(T.status, 2, T.status, NULL) = 2 -- WAITINSTREAMS
          AND ROWNUM < 2 FOR UPDATE OF T.id NOWAIT;   
        
        SELECT  C.fileId, C.nsHost, C.fileSize,  C.lastUpdateTime
          INTO  fileId, nsHost, fileSize, lastUpdateTime
          FROM  castorfile C
          WHERE castorfileId = C.id;
         
        -- we found one, no need to go for new filesystem
        findNewFS := 0;
      EXCEPTION WHEN NO_DATA_FOUND OR LockError THEN
        -- found no tapecopy or diskserver, filesystem are down. We'll go through the normal selection
        NULL;
      END;
    END IF;
  END IF;
  IF findNewFS = 1 THEN
    -- We try first to reuse the diskserver of the lastFSUsed, even if we change filesystem
    FOR f IN (
      SELECT FileSystem.id AS FileSystemId, DiskServer.id AS DiskServerId, DiskServer.name, FileSystem.mountPoint
        FROM FileSystem, DiskServer
       WHERE FileSystem.diskServer = DiskServer.id
         AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.id = lastButOneFSUsed) LOOP
       BEGIN
         -- lock the complete diskServer as we will update all filesystems
         SELECT id INTO unused FROM DiskServer WHERE id = f.DiskServerId FOR UPDATE NOWAIT;
         SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
                f.diskServerId, f.name, f.mountPoint, f.fileSystemId, D.path, D.id, D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, C.lastUpdateTime
           INTO diskServerId, diskServerName, mountPoint, fileSystemId, path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
           FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
          WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
            AND D.filesystem = f.fileSystemId
            AND StT.parent = streamId
            AND T.status = 2 -- WAITINSTREAMS
            AND StT.child = T.id
            AND T.castorfile = D.castorfile
            AND C.id = D.castorfile
            AND ROWNUM < 2 FOR UPDATE OF T.id NOWAIT;
         -- found something on this filesystem, no need to go on
         diskServerId := f.DiskServerId;
         fileSystemId := f.fileSystemId;
         EXIT;
       EXCEPTION WHEN NO_DATA_FOUND OR LockError THEN
         -- either the filesystem is already locked or we found nothing,
         -- let's go to the next one
         NULL;
       END;
    END LOOP;
  END IF;
  IF tapeCopyId = 0 THEN
    -- Then we go for all potential filesystems. Note the duplication of code, due to the fact that ORACLE cannot order unions
    FOR f IN (
      SELECT FileSystem.id AS FileSystemId, DiskServer.id AS DiskServerId, DiskServer.name, FileSystem.mountPoint
        FROM Stream, SvcClass2TapePool, DiskPool2SvcClass, FileSystem, DiskServer
       WHERE Stream.id = streamId
         AND Stream.TapePool = SvcClass2TapePool.child
         AND SvcClass2TapePool.parent = DiskPool2SvcClass.child
         AND DiskPool2SvcClass.parent = FileSystem.diskPool
         AND FileSystem.diskServer = DiskServer.id
         AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
         AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
       ORDER BY -- first prefer diskservers where no migrator runs and filesystems with no recalls
                DiskServer.nbMigratorStreams ASC, FileSystem.nbRecallerStreams ASC,
                -- then order by rate as defined by the function
                fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams, FileSystem.nbWriteStreams,
                               FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC,
                -- finally use randomness to avoid preferring always the same FS
                DBMS_Random.value) LOOP
       BEGIN
         -- lock the complete diskServer as we will update all filesystems
         SELECT id INTO unused FROM DiskServer WHERE id = f.DiskServerId FOR UPDATE NOWAIT;
         SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
                f.diskServerId, f.name, f.mountPoint, f.fileSystemId, D.path, D.id, D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, C.lastUpdateTime
           INTO diskServerId, diskServerName, mountPoint, fileSystemId, path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
           FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
          WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
            AND D.filesystem = f.fileSystemId
            AND StT.parent = streamId
            AND T.status = 2 -- WAITINSTREAMS
            AND StT.child = T.id
            AND T.castorfile = D.castorfile
            AND C.id = D.castorfile
            AND ROWNUM < 2 FOR UPDATE OF T.id NOWAIT;
         -- found something on this filesystem, no need to go on
         diskServerId := f.DiskServerId;
         fileSystemId := f.fileSystemId;
         EXIT;
       EXCEPTION WHEN NO_DATA_FOUND OR LockError THEN
         -- either the filesystem is already locked or we found nothing,
         -- let's go to the next one
         NULL;
       END;
    END LOOP;
  END IF;

  IF tapeCopyId = 0 THEN
    -- Nothing found, return; locks will be released by the caller
    RETURN;
  END IF;
  
  IF findNewFS = 1 THEN
    UPDATE Stream
       SET lastFileSystemUsed = fileSystemId,
           lastButOneFileSystemUsed = lastFileSystemUsed,
           lastFileSystemChange = getTime()
     WHERE id = streamId;
  END IF;

  -- Update Filesystem state
  updateFSMigratorOpened(diskServerId, fileSystemId, 0);
END;
/


/* repack migration candidate selection policy */

CREATE OR REPLACE PROCEDURE tg_repackMigrSelPolicy(
  streamId       IN  INTEGER,
  diskServerName OUT NOCOPY VARCHAR2,
  mountPoint     OUT NOCOPY VARCHAR2,
  path           OUT NOCOPY VARCHAR2, dci OUT INTEGER,
  castorFileId   OUT INTEGER, fileId OUT INTEGER,
  nsHost         OUT NOCOPY VARCHAR2,
  fileSize       OUT INTEGER,
  tapeCopyId     OUT INTEGER,
  lastUpdateTime OUT INTEGER) AS
  fileSystemId INTEGER := 0;
  diskServerId NUMBER;
  lastFSChange NUMBER;
  lastFSUsed NUMBER;
  lastButOneFSUsed NUMBER;
  nbMigrators NUMBER := 0;
  unused NUMBER;
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
BEGIN
  tapeCopyId := 0;
  FOR f IN (
    SELECT FileSystem.id AS FileSystemId, DiskServer.id AS DiskServerId, DiskServer.name, FileSystem.mountPoint
       FROM Stream, SvcClass2TapePool, DiskPool2SvcClass, FileSystem, DiskServer
      WHERE Stream.id = streamId
        AND Stream.TapePool = SvcClass2TapePool.child
        AND SvcClass2TapePool.parent = DiskPool2SvcClass.child
        AND DiskPool2SvcClass.parent = FileSystem.diskPool
        AND FileSystem.diskServer = DiskServer.id
        AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
        AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
      ORDER BY -- first prefer diskservers where no migrator runs and filesystems with no recalls
               DiskServer.nbMigratorStreams ASC, FileSystem.nbRecallerStreams ASC,
               -- then order by rate as defined by the function
               fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams, FileSystem.nbWriteStreams,
                              FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams, FileSystem.nbRecallerStreams) DESC,
               -- finally use randomness to avoid preferring always the same FS
               DBMS_Random.value) LOOP
    BEGIN
      -- lock the complete diskServer as we will update all filesystems
      SELECT id INTO unused FROM DiskServer WHERE id = f.DiskServerId FOR UPDATE NOWAIT;
      SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
             f.diskServerId, f.name, f.mountPoint, f.fileSystemId, D.path, D.id, D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, C.lastUpdateTime
        INTO diskServerId, diskServerName, mountPoint, fileSystemId, path, dci, castorFileId, fileId, nsHost, fileSize, tapeCopyId, lastUpdateTime
        FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
       WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
         AND D.filesystem = f.fileSystemId
         AND StT.parent = streamId
         AND T.status = 2 -- WAITINSTREAMS
         AND StT.child = T.id
         AND T.castorfile = D.castorfile
         AND C.id = D.castorfile
         AND ROWNUM < 2 FOR UPDATE OF T.id NOWAIT;
      -- found something on this filesystem, no need to go on
      diskServerId := f.DiskServerId;
      fileSystemId := f.fileSystemId;
      EXIT;
    EXCEPTION WHEN NO_DATA_FOUND OR LockError THEN
      -- either the filesystem is already locked or we found nothing,
      -- let's go to the next one
      NULL;
    END;
  END LOOP;

  IF tapeCopyId = 0 THEN
    -- Nothing found, return; locks will be released by the caller
    RETURN;
  END IF;
  
  UPDATE Stream
     SET lastFileSystemUsed = fileSystemId,
         lastButOneFileSystemUsed = lastFileSystemUsed,
         lastFileSystemChange = getTime()
   WHERE id = streamId;

  -- Update Filesystem state
  updateFSMigratorOpened(diskServerId, fileSystemId, 0);
END;
/

/* get a candidate for migration */
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

/* get a candidate for recall */
create or replace
PROCEDURE tg_getFileToRecall (inTransId IN  NUMBER, outRet OUT INTEGER,
  outVid OUT NOCOPY VARCHAR2, outFile OUT castorTape.FileToRecallCore_Cur) AS
  varTgrId         INTEGER; -- TapeGateway Request Id
  varDSName VARCHAR2(2048); -- Disk Server name
  varMPoint VARCHAR2(2048); -- Mount Point
  varPath   VARCHAR2(2048); -- Diskcopy path
  varSegId          NUMBER; -- Segment Id
  varDcId           NUMBER; -- Disk Copy Id
  varTcId           NUMBER; -- Tape Copy Id
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
    SELECT   SEG.id,   SEG.fSeq, SEG.Copy 
      INTO varSegId, varNewFSeq, varTcId 
      FROM Segment SEG
     WHERE SEG.tape = varTapeId  
       AND SEG.status = 0 -- SEGMENT_UNPROCESSED
       AND ROWNUM < 2
     ORDER BY SEG.fseq ASC;
    -- Lock the corresponding castorfile
    SELECT CF.id INTO varUnused 
      FROM Castorfile CF, TapeCopy TC
     WHERE CF.id = TC.castorfile 
       AND TC.id = varTcId 
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
  -- Update the TapeCopy's parameters
  UPDATE TapeCopy TC
     SET TC.fseq = varNewFSeq,
         TC.TapeGatewayRequestId = varTgrId,
         TC.DiskCopy = varDcId,
         TC.FileTransactionID = TG_FileTrId_Seq.NEXTVAL
   WHERE TC.id = varTcId;
   -- Update the segment's status
  UPDATE Segment SEG 
     SET SEG.status = 7 -- SEGMENT_SELECTED
   WHERE SEG.id=varSegId 
     AND SEG.status = 0; -- SEGMENT_UNPROCESSED
  OPEN outFile FOR 
    SELECT CF.fileid, CF.nshost, varDSName, varMPoint, varPath, varNewFSeq , 
           TC.FileTransactionID
      FROM TapeCopy TC, Castorfile CF
     WHERE TC.id = varTcId
       AND CF.id=TC.castorfile;
END;
/

/* get the information from the db for a successful migration */
create or replace
PROCEDURE tg_getRepackVidAndFileInfo(
  inFileId          IN  NUMBER, 
  inNsHost          IN VARCHAR2,
  inFseq            IN  INTEGER, 
  inTransId         IN  NUMBER, 
  inBytesTransfered IN  NUMBER,
  outRepackVid     OUT NOCOPY VARCHAR2, 
  outVID           OUT NOCOPY VARCHAR,
  outCopyNb        OUT INTEGER, 
  outLastTime      OUT NUMBER, 
  outRet           OUT INTEGER) AS 
  varCfId              NUMBER;  -- CastorFile Id
  varFileSize          NUMBER;  -- Expected file size
  varTgrId             NUMBER;  -- Tape gateway request Id
BEGIN
  outRepackVid:=NULL;
   -- ignore the repack state
  SELECT CF.lastupdatetime, CF.id, CF.fileSize 
    INTO outLastTime, varCfId, varFileSize
    FROM CastorFile CF
   WHERE CF.fileid = inFileId 
     AND CF.nshost = inNsHost;
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
    SELECT TC.copyNb INTO outcopynb 
      FROM TapeCopy TC
     WHERE TC.TapeGatewayRequestId = varTgrId
       AND TC.castorfile = varCfId
       AND TC.fseq= inFseq;
    
    SELECT T.vid INTO outVID 
      FROM Tape T, Stream S
     WHERE T.id = S.tape
       AND S.TapeGatewayRequestId = varTgrId;
  END IF;
  
  BEGIN 
   --REPACK case
    -- Get the repackvid field from the existing request (if none, then we are not in a repack process
     SELECT SRR.repackvid INTO outRepackVid
       FROM SubRequest sR, StageRepackRequest SRR
      WHERE SRR.id = SR.request
        AND sR.status = 12 -- SUBREQUEST_REPACK
        AND sR.castorFile = varCfId
        AND ROWNUM < 2;
  EXCEPTION WHEN NO_DATA_FOUND THEN
   -- standard migration
    NULL;
  END;
END;
/

/* get the information from the db for a successful recall */
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

/* get the stream without any tape associated */
create or replace
PROCEDURE tg_getStreamsWithoutTapes(outStrList OUT castorTape.Stream_Cur) AS
BEGIN
  -- get streams in state PENDING with a non-NULL TapeGatewayRequestId
  OPEN outStrList FOR
    SELECT S.id, S.initialsizetotransfer, S.status, S.tapepool, TP.name
      FROM Stream S,Tapepool TP
     WHERE S.status = 0 -- STREAM_PENDING
       AND S.TapeGatewayRequestId IS NOT NULL
       AND S.tapepool=TP.id 
       FOR UPDATE OF S.id SKIP LOCKED;   
END;
/

/* get tape with a pending request in VDQM */
create or replace
PROCEDURE tg_getTapesWithDriveReqs(
  inTimeLimit     IN  NUMBER,
  outTapeRequest OUT castorTape.tapegatewayrequest_Cur) AS
  varTgrId        "numList";
  varTapeReadIds  "numList";
  varTapeWriteIds "numList";
  varStreamIds    "numList";
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
   WHERE T.status IN ( 2, 3, 4 ) -- TAPE_WAITDRIVE, TAPE_WAITMOUNT, TAPE_MOUNTED
     AND T.tpMode = 0 -- TAPE_READ
     AND T.TapeGatewayRequestId IS NOT NULL
     AND varNow - T.lastVdqmPingTime > inTimeLimit
     FOR UPDATE SKIP LOCKED;
     
  -- Find all the streams and lock
  SELECT S.id, T.id BULK COLLECT INTO varStreamIds, varTapeWriteIds
    FROM Stream S, Tape T
   WHERE S.Status IN ( 1, 2, 3 ) -- STREAM_WAITDRIVE, STREAM_WAITMOUNT, 
                                 -- STREAM_RUNNING
     AND S.TapeGatewayRequestId IS NOT NULL
     AND S.Tape = T.Id
     AND varNow - T.lastVdqmPingTime > inTimeLimit
     FOR UPDATE SKIP LOCKED;
     
  -- Update the last VDQM ping time for all of them.
  varNow := gettime();
  UPDATE Tape T
     SET T.lastVdqmPingTime = varNow
   WHERE T.id IN ( SELECT /*+ CARDINALITY(trTable 5) */ * 
                     FROM TABLE (varTapeReadIds)
                    UNION ALL SELECT /*+ CARDINALITY(trTable 5) */ *
                     FROM TABLE (varTapeWriteIds));
                   
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
    SELECT T.tpMode, S.TapeGatewayRequestId, T.startTime,
           T.lastvdqmpingtime, S.vdqmVolReqid,
           T.vid
      FROM Tape T, Stream S
     WHERE S.Id IN (SELECT /*+ CARDINALITY(trTable 5) */ *
                    FROM TABLE(varStreamIds))
       AND S.Tape = T.id;
END;
/

/* get a tape without any drive requests sent to VDQM */
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
    BEGIN
      SELECT S.id INTO varStreamId
        FROM Stream S
       WHERE S.status = 8 -- STREAM_TOBESENTTOVDQM
         AND ROWNUM < 2
       ORDER BY dbms_random.value()
         FOR UPDATE SKIP LOCKED;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      varStreamId := NULL; -- Nothing to be found, we'll just carry on to the reads.
    END;
    IF (varStreamId IS NOT NULL) THEN
      SELECT S.TapeGatewayRequestId,     1,      T.side,      T.vid
        INTO outReqId, outTapeMode, outTapeSide, outTapeVid
        FROM Stream S, Tape T
       WHERE T.id = S.tape
         AND S.id = varStreamId;
      RETURN;
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE_APPLICATION_ERROR (-20119, 'Stream in stage STREAM_TOBESENTTOVDQM has no tape attached!' ||
      'Stream='||varStreamId);
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


/* get tape to release in VMGR */
create or replace
PROCEDURE tg_getTapeToRelease(
  inVdqmReqId IN  INTEGER, 
  outVID      OUT NOCOPY VARCHAR2, 
  outMode     OUT INTEGER ) AS
  varStrId        NUMBER;
  varTpId         NUMBER;
BEGIN
  -- Find Tape read or stream for this vdqm request
  tg_findFromVDQMReqId(inVdqmReqId, varTpId, varStrId);
  
   IF (varTpId IS NOT NULL) THEN -- read case
     outMode := 0;
     SELECT T.vid INTO outVID 
       FROM Tape T
       WHERE T.id = varTpId; 
   ELSIF (varStrId IS NOT NULL) THEN -- write case
     outMode := 1;
     SELECT T.vid INTO outVID 
       FROM Tape T, Stream S
      WHERE S.id=varStrId
        AND S.tape=T.id;
   END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- already cleaned by the checker
  NULL;
END;
/

/* invalidate a file that it is not possible to tape as candidate to migrate or recall */
create or replace
PROCEDURE tg_invalidateFile(
  inTransId   IN NUMBER,
  inFileId    IN NUMBER, 
  inNsHost    IN VARCHAR2,
  inFseq      IN INTEGER,
  inErrorCode IN INTEGER) AS
  varTapeId      NUMBER;
  varStreamId    NUMBER;
BEGIN
  tg_findFromVDQMReqId (inTransId, varTapeId, varStreamId);
  IF (varStreamId IS NOT NULL) THEN -- We want the tape so in case we are 
  --migrating, "convert" the stream Id into a tape id.
    SELECT S.tape INTO varTapeId 
      FROM Stream S
     WHERE S.Id=varStreamId;
  END IF;
  -- Now we should have a tape id in all cases
  IF (varTapeId IS NOT NULL) THEN
    UPDATE Tape T
       SET T.lastfseq = T.lastfseq-1,
           T.vdqmvolreqid = inTransId
     WHERE T.id = varTapeId;
     tg_failfiletransfer(inTransId, inFileId, inNsHost, inFseq, inErrorCode);
  ELSE
    RAISE_APPLICATION_ERROR (-20119, 
         'Tailed to find tape for VDQM Id='|| 
         inTransId || ' in tg_invalidateFile');
  END IF;
END;
/


/* restart taperequest which had problems */
create or replace
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
create or replace
PROCEDURE TG_SetFileMigrated(
  inTransId         IN  NUMBER, 
  inFileId          IN  NUMBER,
  inNsHost          IN  VARCHAR2, 
  inFseq            IN  INTEGER, 
  inFileTransaction IN  NUMBER,
  outStreamReport   OUT castor.StreamReport_Cur) AS
  varUnused             NUMBER;
  varTapeCopyCount      INTEGER;
  varCfId               NUMBER;
  varTcId               NUMBER;
  varDCId               NUMBER;
  varTcIds              "numList";
  varTapeId             NUMBER;
  varStreamId           NUMBER;
BEGIN
  -- Find Stream or tape from vdqm vol req ID Lock
  tg_findFromVDQMReqId (inTransId, varTapeId, varStreamId);
  IF (varTapeId IS NOT NULL) THEN
    SELECT T.Id INTO varUnused
      FROM Tape T WHERE T.Id = varTapeId
       FOR UPDATE;
  ELSIF (varStreamId IS NOT NULL) THEN
    SELECT S.Id INTO varUnused
      FROM Stream S WHERE S.Id = varStreamId
       FOR UPDATE;  
  ELSE
    RAISE_APPLICATION_ERROR (-20119, 
         'Could not find stream or tape read for VDQM request Id='|| 
         inTransId || ' in TG_SetFileMigrated');
  END IF;
  -- Lock the CastorFile
  SELECT CF.id INTO varCfId FROM CastorFile CF
   WHERE CF.fileid = inFileId 
     AND CF.nsHost = inNsHost 
     FOR UPDATE;
  -- Locate the corresponding tape copy and Disk Copy, Lock
  SELECT   TC.id, TC.DiskCopy
    INTO varTcId,     varDcId
    FROM TapeCopy TC
   WHERE TC.FileTransactionId = inFileTransaction
     AND TC.fSeq = inFseq
     FOR UPDATE;
  UPDATE tapecopy TC
     SET TC.status = 5 -- TAPECOPY_STAGED
   WHERE TC.id = varTcId;
  SELECT count(*) INTO varTapeCopyCount
    FROM tapecopy TC
    WHERE TC.castorfile = varCfId  
     AND STATUS != 5; -- TAPECOPY_STAGED
  -- let's check if another copy should be done, if not, we're done for this file.
  IF varTapeCopyCount = 0 THEN
     -- Mark all disk copies as staged and delete all tape copies together.
     UPDATE DiskCopy DC
        SET DC.status=0   -- DISKCOPY_STAGED
      WHERE DC.castorFile = varCfId
        AND DC.status=10; -- DISKCOPY_CANBEMIGR
     DELETE FROM tapecopy TC
      WHERE castorfile = varCfId 
  RETURNING id BULK COLLECT INTO varTcIds;
     FORALL i IN varTcIds.FIRST .. varTcIds.LAST
       DELETE FROM id2type 
         WHERE id=varTcIds(i);
  END IF;
  -- archive Repack requests should any be in the db
  FOR i IN (
    SELECT SR.id FROM SubRequest SR
    WHERE SR.castorfile = varCfId AND
          SR.status=12 -- SUBREQUEST_REPACK
    ) LOOP
      archivesubreq(i.id, 8); -- SUBREQUEST_FINISHED
  END LOOP;
  -- return data for informing the rmMaster
  OPEN outStreamReport FOR
   SELECT DS.name,FS.mountpoint 
     FROM DiskServer DS,FileSystem FS, DiskCopy DC 
    WHERE DC.id = varDcId 
      AND DC.filesystem = FS.id 
      AND FS.diskserver = DS.id;
  COMMIT;
END;
/


/* update the db after a successful recall */
create or replace
PROCEDURE tg_setFileRecalled(
  inTransId          IN  NUMBER, 
  inFileId           IN  NUMBER,
  inNsHost           IN  VARCHAR2, 
  inFseq             IN  NUMBER, 
  inFileTransaction  IN  NUMBER,
  outStreamReport   OUT castor.StreamReport_Cur) AS
  varTcId               NUMBER;         -- TapeCopy Id
  varDcId               NUMBER;         -- DiskCopy Id
  varCfId               NUMBER;         -- CastorFile Id
  srId NUMBER;
  varSubrequestId       NUMBER; 
  varRequestId          NUMBER;
  varFileSize           NUMBER;
  varGcWeight           NUMBER;         -- Garbage collection weight
  varGcWeightProc       VARCHAR(2048);  -- Garbage collection weighting procedure name
  varEuid               INTEGER;        -- Effective user Id
  varEgid               INTEGER;        -- Effective Group Id
  varSvcClassId         NUMBER;         -- Service Class Id
  varMissingCopies      INTEGER;
  varUnused             NUMBER;
  varTapeId             NUMBER;
  varStreamId           NUMBER;
BEGIN
  SAVEPOINT TGReq_CFile_TCopy_Locking;
  -- Find Stream or tape from vdqm vol req ID Lock
  tg_findFromVDQMReqId (inTransId, varTapeId, varStreamId);
  IF (varTapeId IS NOT NULL) THEN
    SELECT T.Id INTO varUnused
      FROM Tape T WHERE T.Id = varTapeId
       FOR UPDATE;
  ELSIF (varStreamId IS NOT NULL) THEN
    SELECT S.Id INTO varUnused
      FROM Stream S WHERE S.Id = varStreamId
       FOR UPDATE;  
  ELSE
    ROLLBACK TO SAVEPOINT TGReq_CFile_TCopy_Locking;
    RAISE_APPLICATION_ERROR (-20119, 
         'Could not find stream or tape read for VDQM request Id='|| 
         inTransId || ' in TG_SetFileMigrated');
  END IF;
  -- find and lock castor file for the nsHost/fileID
  SELECT CF.id, CF.fileSize INTO varCfId, varFileSize
    FROM CastorFile CF 
   WHERE CF.fileid = inFileId 
     AND CF.nsHost = inNsHost 
     FOR UPDATE;
  -- Find and lock the tape copy
  varTcId := NULL;
  SELECT TC.id, TC.diskcopy INTO varTcId, varDcId
    FROM TapeCopy TC
   WHERE TC.FileTransactionId = inFileTransaction
     AND TC.fSeq = inFseq
     FOR UPDATE;
  -- If nothing found, die releasing the locks
  IF varTCId = NULL THEN
    ROLLBACK TO SAVEPOINT TGReq_CFile_TCopy_Locking;
    RAISE NO_DATA_FOUND;
  END IF;
  -- missing tapecopies handling
  SELECT TC.missingCopies INTO varMissingCopies
    FROM TapeCopy TC WHERE TC.id = varTcId;
  -- delete tapecopies
  deleteTapeCopies(varCfId);
  -- update diskcopy status, size and gweight
  SELECT SR.id, SR.request
    INTO varSubrequestId, varRequestId
    FROM SubRequest SR
   WHERE SR.diskcopy = varDcId;
  SELECT REQ.svcClass, REQ.euid, REQ.egid INTO varSvcClassId, varEuid, varEgid
    FROM (SELECT id, svcClass, euid, egid FROM StageGetRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StagePrepareToGetRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StageUpdateRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StagePrepareToUpdateRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StageRepackRequest) REQ
    WHERE REQ.id = varRequestId;
  varGcWeightProc := castorGC.getRecallWeight(varSvcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || varGcWeightProc || '(:size); END;'
    USING OUT varGcWeight, IN varFileSize;
  UPDATE DiskCopy DC
    SET DC.status = decode(varMissingCopies, 0, 0, 10), -- DISKCOPY_STAGED if varMissingCopies = 0, otherwise CANBEMIGR
        DC.lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
        DC.gcWeight = varGcWeight,
        DC.diskCopySize = varFileSize
    WHERE Dc.id = varDcId;
  -- restart this subrequest so that the stager can follow it up
  UPDATE SubRequest SR
     SET SR.status = 1, SR.getNextStatus = 1,  -- SUBREQUEST_RESTART, GETNEXTSTATUS_FILESTAGED
         SR.lastModificationTime = getTime(), SR.parent = 0
   WHERE SR.id = varSubrequestId;
  -- and trigger new migrations if missing tape copies were detected
  IF varMissingCopies > 0 THEN
    DECLARE
      newTcId INTEGER;
    BEGIN
      FOR i IN 1..varMissingCopies LOOP
        INSERT INTO TapeCopy (id, copyNb, castorFile, status, nbRetry, missingCopies)
        VALUES (ids_seq.nextval, 0, varCfId, 0, 0, 0)  -- TAPECOPY_CREATED
        RETURNING id INTO newTcId;
        INSERT INTO Id2Type (id, type) VALUES (newTcId, 30); -- OBJ_TapeCopy
      END LOOP;
    END;
  END IF;
  -- restart other requests waiting on this recall
  UPDATE SubRequest SR
     SET SR.status = 1, SR.getNextStatus = 1,  -- SUBREQUEST_RESTART, GETNEXTSTATUS_FILESTAGED
         SR.lastModificationTime = getTime(), SR.parent = 0
   WHERE SR.parent = varSubrequestId;
  -- trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(varCfId, varEuid, varEgid);
  -- return data for informing the rmMaster
  OPEN outStreamReport FOR
    SELECT DS.name, FS.mountpoint 
      FROM DiskServer DS, FileSystem FS, DiskCopy DC
      WHERE DC.id = varDcId
      AND DC.filesystem = FS.id 
      AND FS.diskserver = DS.id;
  COMMIT;
END;
/




/* save in the db the results returned by the retry policy for migration */
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

/* save in the db the results returned by the retry policy for recall */
CREATE OR REPLACE PROCEDURE tg_setRecRetryResult(
  tcToRetry IN castor."cnumList", 
  tcToFail  IN castor."cnumList"  ) AS
  tapeId NUMBER;
  cfId NUMBER;

BEGIN
  -- I restart the recall that I want to retry
  -- check because oracle cannot handle empty buffer
  IF tcToRetry(tcToRetry.FIRST) != -1 THEN 

    -- tapecopy => TOBERECALLED
    FORALL i IN tcToRetry.FIRST .. tcToRetry.LAST
      UPDATE TapeCopy
        SET status    = 4, -- TAPECOPY_TOBERECALLED
            errorcode = 0,
            nbretry   = nbretry+1 
        WHERE id=tcToRetry(i);
    
    -- segment => UNPROCESSED
    -- tape => PENDING if UNUSED OR FAILED with still segments unprocessed
    FOR i IN tcToRetry.FIRST .. tcToRetry.LAST LOOP
      UPDATE Segment
        SET status = 0 -- SEGMENT_UNPROCESSED
        WHERE copy = tcToRetry(i)
        RETURNING tape INTO tapeId;
      UPDATE Tape
        SET status = 8 -- TAPE_WAITPOLICY
        WHERE id = tapeId AND
          status IN (0, 6); -- TAPE_UNUSED, TAPE_FAILED
    END LOOP;
  END IF;
  
  -- I mark as failed the hopeless recall
  -- check because oracle cannot handle empty buffer
  IF tcToFail(tcToFail.FIRST) != -1 THEN
    FOR i IN tcToFail.FIRST .. tcToFail.LAST  LOOP

      -- lock castorFile	
      SELECT castorFile INTO cfId 
        FROM TapeCopy,CastorFile
        WHERE TapeCopy.id = tcToFail(i) 
        AND CastorFile.id = TapeCopy.castorfile 
        FOR UPDATE OF castorfile.id;

      -- fail diskcopy
      UPDATE DiskCopy SET status = 4 -- DISKCOPY_FAILED
        WHERE castorFile = cfId 
        AND status = 2; -- DISKCOPY_WAITTAPERECALL   
      
      -- Drop tape copies. Ideally, we should keep some track that
      -- the recall failed in order to prevent future recalls until some
      -- sort of manual intervention. For the time being, as we can't
      -- say whether the failure is fatal or not, we drop everything
      -- and we won't deny a future request for recall.
      deleteTapeCopies(cfId);
      
      -- fail subrequests
      UPDATE SubRequest 
        SET status = 7, -- SUBREQUEST_FAILED
            getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED (not strictly correct but the request is over anyway)
            lastModificationTime = getTime(),
            errorCode = 1015,  -- SEINTERNAL
            errorMessage = 'File recall from tape has failed, please try again later',
            parent = 0
        WHERE castorFile = cfId 
        AND status IN (4, 5); -- WAITTAPERECALL, WAITSUBREQ
    
    END LOOP;
  END IF;
  COMMIT;
END;
/


/* update the db when a tape session is started */
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

/* Check configuration */
CREATE OR REPLACE PROCEDURE tg_checkConfiguration AS
  unused VARCHAR2(2048);
BEGIN
  -- This fires an exception if the db is configured not to run the tapegateway
  SELECT value INTO unused FROM castorconfig WHERE class='tape' AND key='interfaceDaemon' AND value='tapegatewayd';
END;
/


/* delete streams for not existing tapepools */
create or replace
PROCEDURE tg_deleteStream(inStrId IN NUMBER) AS
  varUnused NUMBER;
  varTcIds  "numList"; -- TapeCopy Ids
  varTgrId   NUMBER;   -- TapeGatewayRequest Id
BEGIN
  -- First lock the stream
  SELECT S.id INTO varUnused FROM Stream S 
   WHERE S.id = inStrId FOR UPDATE;
  -- Disconnect the tapecopies
  DELETE FROM stream2tapecopy STTC
   WHERE STTC.parent = inStrId 
  RETURNING STTC.child BULK COLLECT INTO varTcIds;
  -- Hand back the orphaned tape copies to the MigHunter (by setting back their
  -- statues, mighunter will pick them up on it).
  FORALL i IN varTcIds.FIRST .. VarTcIds.LAST
    UPDATE tapecopy TC
       SET TC.status = 1 -- TAPECOPY_TOBEMIGRATED
     WHERE TC.Id = varTcIds(i)
       AND NOT EXISTS (SELECT 'x' FROM stream2tapecopy STTC 
                        WHERE STTC.child = varTcIds(i));
  -- Finally drop the stream itself
  DELETE FROM id2type ITT where ITT.id = inStrId;
  DELETE FROM Stream S where S.id= inStrId;
END;
/


/* delete taperequest  for not existing tape */
create or replace
PROCEDURE tg_deleteTapeRequest( inTGReqId IN NUMBER ) AS
  /* The tape gateway request does not exist per se, but 
   * references to its ID should be removed (with needed consequences
   * from the structures pointing to it) */
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varTpReqId NUMBER;     -- Tape Recall (TapeGatewayReequest.TapeRecall)
  varStrId NUMBER;       -- Stream Id.
  varSegNum INTEGER;
  varCfId NUMBER;        -- CastorFile Id
  varTcIds "numList";    -- Tapecopies IDs
  varSrIds "numList";
BEGIN
  -- Find the relevant stream or reading tape id.
  tg_findFromTGRequestId (inTGReqId, varTpReqId, varStrId);
  -- Find out whether this is a read or a write
  IF (varTpReqId IS NOT NULL) THEN
    -- Lock and reset the tape in case of a read
    UPDATE Tape T
      SET T.status = 0 -- TAPE_UNUSED
      WHERE T.id = varTpReqId;
    SELECT SEG.copy BULK COLLECT INTO varTcIds 
      FROM Segment SEG 
     WHERE SEG.tape = varTpReqId;
    FOR i IN varTcIds.FIRST .. varTcIds.LAST  LOOP
      -- lock castorFile	
      SELECT TC.castorFile INTO varCfId 
        FROM TapeCopy TC, CastorFile CF
        WHERE TC.id = varTcIds(i) 
        AND CF.id = TC.castorfile 
        FOR UPDATE OF CF.id;
      -- fail diskcopy, drop tapecopies
      UPDATE DiskCopy DC SET DC.status = 4 -- DISKCOPY_FAILED
       WHERE DC.castorFile = varCfId 
         AND DC.status = 2; -- DISKCOPY_WAITTAPERECALL   
      deleteTapeCopies(varCfId);
      -- Fail the subrequest
      UPDATE SubRequest SR
         SET SR.status = 7, -- SUBREQUEST_FAILED
             SR.getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED (not strictly correct but the request is over anyway)
             SR.lastModificationTime = getTime(),
             SR.errorCode = 1015,  -- SEINTERNAL
             SR.errorMessage = 'File recall from tape has failed, please try again later',
             SR.parent = 0
       WHERE SR.castorFile = varCfId 
         AND SR.status IN (4, 5); -- WAITTAPERECALL, WAITSUBREQ
    END LOOP;
  ELSIF (varStrId IS NOT NULL) THEN
    -- In case of a write, reset the stream
    DeleteOrStopStream (varStrId);
  ELSE
    -- Wrong Access Mode encountered. Notify.
    RAISE_APPLICATION_ERROR(-20292, 'tg_deleteTapeRequest: no read tape or '||
      'stream found for TapeGatewayRequestId: '|| inTGReqId);
  END IF;
END;
/


