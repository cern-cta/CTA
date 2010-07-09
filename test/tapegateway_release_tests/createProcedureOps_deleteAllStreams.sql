CREATE OR REPLACE PROCEDURE ops_deleteAllStreams AS

-- This procedure deletes all the streams.
--
-- Pleaee nothe that all mighunter and tape-gateway daemons must be shutdown
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

BEGIN
  -- Try to get a lock on and the database ids of all of the tape-streams
  BEGIN
    SELECT id BULK COLLECT INTO streamIdsVar FROM Stream FOR UPDATE;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20001,
        'Failed to take a lock on all tape-streams: SQLCODE=' || SQLCODE);
  END;

  -- Get the database ids of all the tape-copies attached to 1 or more
  -- tape-streams
  SELECT DISTINCT child BULK COLLECT INTO attachedTapeCopyIdsVar
    FROM Stream2TapeCopy;

  -- Detach all of the attached tape-copies
  FOR i IN 1 .. attachedTapeCopyIdsVar.COUNT LOOP
    ops_resetTapeCopy(attachedTapeCopyIdsVar(i));
  END LOOP;

  -- Delete all of the tape-streams and any associated tape-gateway requests
  BEGIN
    FOR i IN 1 .. streamIdsVar.COUNT LOOP
      DELETE FROM TapeGatewayRequest WHERE streamMigration=streamIdsVar(i);
      DELETE FROM Stream WHERE id=streamIdsVar(i);
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20002,
        'Failed to delete all tape-streams: SQLCODE=' || SQLCODE);
  END;
END;
/
