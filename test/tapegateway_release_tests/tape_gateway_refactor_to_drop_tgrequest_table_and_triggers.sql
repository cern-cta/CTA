/*******************************************************************
 *
 * @(#)$RCSfile$ $Revision$ $Date$ $Author$
 *
 * This file contains is an SQL script used to test polices of the tape gateway
 * on a disposable stager schema, which is supposed to be initially empty.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/
 
 /*  W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    */
 /*
  * D O   N O    U S E    O N    A   P R O D   O R   D E V    S T A G E R
  *
  * Only intended for throwaway schema on devdb10
  */
   /*  W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    W A R N I N G    */
   
/* 
 * The tapegateway request is an extension of the existing structure in the DB
 * that add persistency for some inforamtion rtcpclientd failed to keep in the 
 * DB, making itself stateful.\
 */

/*
 CREATE TABLE TapeGatewayRequest (
              accessMode NUMBER,      
              startTime INTEGER, 
              lastVdqmPingTime INTEGER, 
              vdqmVolReqId NUMBER, 
              nbRetry NUMBER, 
              lastFseq NUMBER, 
              id INTEGER CONSTRAINT PK_TapeGatewayRequest_Id PRIMARY KEY, 
              streamMigration INTEGER,
              tapeRecall INTEGER, 
              status INTEGER);
*/ 
 
/* Plan and strategy:
 * - Accessmode is useless => in stream, accessmode=1 (write)/ in tape, 
 * accessmode is covered my tpmode.
 * - startTime: used for tracking tg_attachDriveReqToTape (update) only read in
 * tg_getTapesWithDriveReqs, but applies to read tape and stream
 * - lastVdqmPingTime: same as previous plus reset in tg_starttapesession 
 * (double use?)
 * - vdqmVolReqId: Many references. The major one! referenced in 
 * tg_restartlostreqs, which was not worked onto in the removal of 
 * tapegatewaysubrequests.
 * - nbRetry: Never unused!
 * - lastFseq: migration use only. Used in tg_getfiletomigrate, 
 * tg_attachetapestostreams, tg_invalidatefile. Added to Tape nevertheless
 * as all the properties go to tape. Stream is the driver and the connector
 * between the tape and the 
 * - id: will go in both tape and stream as TapeGatewayRequestId.
 * - StreamMigration (now Stream) => rendered irrelevant by the merge 
 * TGR<=> Stream
 * - TapeRecall (now Tape) => rendered irrelevant by the merge TGR<=>Tape
 * - Status: Tape and TGR's status lifecycles were are in exact match 
 * (TAPE_WAITTAPEDRIVE=TGR_WAITING_TAPESERVER, TAPE_MOUNTED=TGR_ONGOING,
 * TAPE_UNUSED=TDR destroyed)
 * 
 *         For Streams it's not so straightforward: TGR_TOBESENTTOVDQM is and
 *         extra state, and TGR.WAITINGTAPESERVER is covered by both 
 *         STREAM_WAITDRIVE and STREAM_WAITMOUNT. This one can be covered by a 
 *         simple OR if needed.
 *
 * Global strategy: as much information as possible is kept attached to the tape.
 * The stream still drives the writing as we bunch the write (not the reads)
 * so the status is held in the stream and the write tape's status is set to
 * 9; -- TAPE_ATTACHED_TO_STREAM
 */

 /*
 CREATE TABLE TapeGatewayRequest (
              accessMode NUMBER,      
              startTime INTEGER, 
              lastVdqmPingTime INTEGER, 
              vdqmVolReqId NUMBER, 
              nbRetry NUMBER, 
              lastFseq NUMBER, 
              id INTEGER CONSTRAINT PK_TapeGatewayRequest_Id PRIMARY KEY, 
              streamMigration INTEGER,
              tapeRecall INTEGER, 
              status INTEGER);
*/

/* ******** Data structure transision ******** */
-- ALTER TABLE TapeGatewayRequest DROP COLUMN accessMode;

-- ALTER TABLE Tape ADD (startTime NUMBER);
-- ALTER TABLE TapeGatewayRequest DROP COLUMN startTime;

-- ALTER TABLE Tape ADD (lastVdqmPingTime NUMBER);
-- ALTER TABLE TapeGatewayRequest DROP COLUMN lastVdqmPingTime;

-- For Tape reads only */
-- ALTER TABLE Tape ADD (vdqmVolReqId NUMBER);
-- For Tape writes only */
-- ALTER TABLE Stream ADD (vdqmVolReqId NUMBER);
-- ALTER TABLE TapeGatewayRequest DROP COLUMN vdqmVolReqId;

-- ALTER TABLE TapeGatewayRequest DROP COLUMN nbRetry;

-- ALTER TABLE Tape ADD (lastFseq NUMBER);
-- ALTER TABLE TapeGatewayRequest DROP COLUMN lastFseq;

-- ALTER TABLE Stream ADD (TapeGatewayRequestId NUMBER);
-- ALTER TABLE Tape ADD (TapeGatewayRequestId NUMBER);
-- ALTER TABLE TapeCopy DROP CONSTRAINT "FK_TAPECOPY_TGR";
-- ALTER TABLE TapeCopy RENAME COLUMN TapeGatewayRequest TO TapeGatewayRequestId;
-- ALTER TABLE TapeGatewayRequest DROP CONSTRAINT PK_TAPEGATEWAYREQUEST_ID;
-- ALTER TABLE TapeGatewayRequest DROP COLUMN id;

-- ALTER TABLE TapeGatewayRequest DROP COLUMN Stream;
-- ALTER TABLE TapeGatewayRequest DROP COLUMN TapeRecall;
--ALTER TABLE TapeGatewayRequest DROP COLUMN Status;*/
--SQL Error: ORA-12983: cannot drop all columns in a table*/
-- DROP TABLE TapeGatewayRequest;

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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
      varTp.vdqmvolreqid    := inVdqmId;
      UPDATE Tape T
         SET ROW = varTp
       WHERE T.Id = varTp.Id;
      COMMIT;
      RETURN;
    END; -- END of local block for varTapeFromStream and varTp
  ELSE RAISE_APPLICATION_ERROR (-20119, 
       'Found no stream or read tape for TapeRequestId: '||inTapeRequestId);
  END IF;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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
      -- Finally update the stream we locked earlier
      UPDATE Stream S 
         SET S.tape = varTapeId
       WHERE S.id = inStrIds(i);
      -- And save this loop's result
      COMMIT;
    END IF;
  END LOOP;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/


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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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
  -- TODO: should we not commit here?
END;  
/    


/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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
   
   UPDATE TapeCopy TC
      SET TC.fSeq = varNewFseq,
          TC.tapeGatewayRequestId = varTgRequestId,
          TC.diskCopy = varDiskCopyId
    WHERE TC.Id = varTapeCopyId;

   OPEN outputFile FOR
     SELECT varFileId,varNshost,varLastUpdateTime,varDiskServer,varMountPoint,
            varPath,varLastKnownName,TC.fseq,varFileSize,varTapeCopyId
       FROM TapeCopy TC
      WHERE TC.Id = varTapeCopyId;

  END;
  COMMIT;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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


/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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


/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_getSegmentInfo(
  inTransId     IN NUMBER,
  inFileId      IN NUMBER, 
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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

/* PL/SQL declaration for the castorTape package */
CREATE OR REPLACE PACKAGE castorTape AS 
   TYPE TapeGatewayRequestExtended IS RECORD (
    accessMode NUMBER,
    id NUMBER,
    starttime NUMBER,
    lastvdqmpingtime NUMBER, 
    vdqmvolreqid NUMBER, 
    vid VARCHAR2(2048));
  TYPE TapeGatewayRequest_Cur IS REF CURSOR RETURN TapeGatewayRequestExtended;
  TYPE TapeGatewayRequestCore IS RECORD (
    tpmode INTEGER,
    side INTEGER,
    vid VARCHAR2(2048),
    tapeRequestId NUMBER);
  TYPE TapeGatewayRequestCore_Cur IS REF CURSOR RETURN TapeGatewayRequestCore;
  TYPE StreamCore IS RECORD (
    id INTEGER,
    initialSizeToTransfer INTEGER,
    status NUMBER,
    tapePoolId NUMBER,
    tapePoolName VARCHAR2(2048));
  TYPE Stream_Cur IS REF CURSOR RETURN StreamCore; 
  TYPE DbMigrationInfo IS RECORD (
    id NUMBER,
    copyNb NUMBER,
    fileName VARCHAR2(2048),
    nsHost VARCHAR2(2048),
    fileId NUMBER,
    fileSize NUMBER);
  TYPE DbMigrationInfo_Cur IS REF CURSOR RETURN DbMigrationInfo;
  TYPE DbStreamInfo IS RECORD (
    id NUMBER,
    numFile NUMBER,
    byteVolume NUMBER,
    age NUMBER);
  TYPE DbStreamInfo_Cur IS REF CURSOR RETURN DbStreamInfo;
  TYPE DbRecallInfo IS RECORD (
    vid VARCHAR2(2048),
    tapeId NUMBER,
    dataVolume NUMBER,
    numbFiles NUMBER,
    expireTime NUMBER,
    priority NUMBER);
  TYPE DbRecallInfo_Cur IS REF CURSOR RETURN DbRecallInfo;
  TYPE FileToRecallCore IS RECORD (
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   diskserver VARCHAR2(2048),
   mountPoint VARCHAR(2048),
   path VARCHAR2(2048),
   fseq INTEGER,
   fileTransactionId NUMBER);
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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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


/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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
     WHERE S.status = 8 -- STREAM_WAITTAPE
       AND ROWNUM < 2
       FOR UPDATE SKIP LOCKED;
    SELECT S.VDQMVolReqId,     1,      T.side,      T.vid
      INTO outReqId, outTapeMode, outTapeSide, outTapeVid
      FROM Stream S, Tape T
     WHERE T.id = S.tape
       AND S.id = varStreamId;
    RETURN;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL; -- No write found, carry on to the read.
  END;
  BEGIN -- The read casse
    SELECT T.VDQMVolReqId,     0,      T.side,      T.vid
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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/


create or replace
PROCEDURE tg_findVDQMReqFromTGReqId (
  inTGReqId     IN  INTEGER,
  outVDQMReqId  OUT INTEGER) AS
  varTapeId         NUMBER;
  varStreamId       NUMBER;
BEGIN
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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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


/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

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
  ELSE
    -- Not found
    outRet:=-2; -- UNKNOWN request
  END IF;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE resurrectTapes
(tapeIds IN castor."cnumList")
AS
BEGIN
  IF (TapegatewaydIsRunning) THEN
    FORALL i IN tapeIds.FIRST .. tapeIds.LAST
      UPDATE Tape T
         SET T.TapegatewayRequestId = ids_seq.nextval,
             T.status = 1
       WHERE T.status = 8 AND T.id = tapeIds(i);
  ELSE
    FORALL i IN tapeIds.FIRST .. tapeIds.LAST
      UPDATE Tape SET status = 1 WHERE status = 8 AND id = tapeIds(i);    
  END IF;
  COMMIT;
END;
/

--DROP TRIGGER tr_Tape_pending; -- Not necessary: trigger is not present
--/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE startChosenStreams
  (streamIds IN castor."cnumList") AS
BEGIN
  IF (TapegatewaydIsRunning) THEN
    FORALL i IN streamIds.FIRST .. streamIds.LAST
      UPDATE Stream S
         SET S.status = 0, -- PENDING
             S.TapeGatewayRequestId = ids_seq.nextval
       WHERE S.status = 7 -- WAITPOLICY
         AND S.id = streamIds(i);
  ELSE
    FORALL i IN streamIds.FIRST .. streamIds.LAST
      UPDATE Stream S
         SET S.status = 0 -- PENDING
       WHERE S.status = 7 -- WAITPOLICY
         AND S.id = streamIds(i);
  ENd IF;
  COMMIT;
END;
/

--DROP TRIGGER tr_Stream_pending; -- Not necessary: trigger is not present
--/

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

