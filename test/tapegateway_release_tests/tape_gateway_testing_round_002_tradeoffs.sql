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
 
-- Added pickiness to the procedure to go in an exception when there is a 
-- stream missing a tape yet asking to go to vdqm.
--
-- This has the side effect of preventing the recall too.

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


-- Fixed a bug where the id of the newly created tape was not properly sored in a variable, leading to
-- a corrupted stream structure. (counstraint ?)
--
-- Moved a check to a broader context where it would have caught the problem.
--

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

