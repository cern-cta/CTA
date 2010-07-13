CREATE OR REPLACE PROCEDURE ops_deleteAllStreams AS

-- This procedure deletes all the streams.
--
-- Please note that all mighunter and tape-gateway daemons must be shutdown
-- before this procedure is called.
--
-- Please note that this procedure doe NOT commit or rollback the transaction,
-- this is the responsibility of the caller.
--
-- This procedure raises an application error with code -20001 if a lock could
-- not be taken on all of the tape-streams.
--
-- This procedure raises an application error with code -20002 if not all of
-- the tape-streams could be deleted.

streamIdsVar           "numList";
attachedTapeCopyIdsVar "numList";
tapeGatewayRequestIdVar NUMBER(38);

BEGIN
  -- Try to get a lock on and the database id of each of the tape-streams.
  --
  -- A lock is being taken on each tape-stream in-case a mighunter or
  -- tape-gateway daemon is running.  These daemons should have been shutdown
  -- before this procedure was called, however it is always best to be safe.
  BEGIN
    SELECT id BULK COLLECT INTO streamIdsVar FROM Stream FOR UPDATE;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20001,
        'Failed to take a lock on all tape-streams: SQLCODE=' || SQLCODE);
  END;

  -- Get the database ids of all the tape-copies attached to the now locked
  -- tape-streams
  SELECT DISTINCT /*+ CARDINALITY(StreamIdsVarTable 5) */ child
    BULK COLLECT INTO attachedTapeCopyIdsVar
    FROM Stream2TapeCopy
    WHERE parent IN (
      SELECT /*+ CARDINALITY(StreamIdsVarTable 5) */ *
        FROM TABLE(streamIdsVar) StreamIdsVarTable);

  -- Detach all of the attached tape-copies
  FOR i IN 1 .. attachedTapeCopyIdsVar.COUNT LOOP
    ops_resetTapeCopy(attachedTapeCopyIdsVar(i));
  END LOOP;

  BEGIN
    -- For each tape-stream
    FOR i IN 1 .. streamIdsVar.COUNT LOOP

      -- Delete the associated tape-gateway request if there is one
      DELETE FROM TapeGatewayRequest WHERE streamMigration=streamIdsVar(i)
        RETURNING id INTO tapeGatewayRequestIdVar;
      IF SQL%ROWCOUNT != 0 THEN
        DELETE FROM id2Type WHERE id = tapeGatewayRequestIdVar;
      END IF;

      -- Delete the tape-stream
      DELETE FROM Stream  WHERE id=streamIdsVar(i);
      DELETE FROM id2Type WHERE id=streamIdsVar(i);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20002,
        'Failed to delete all tape-streams: SQLCODE=' || SQLCODE);
  END;
END;
/
