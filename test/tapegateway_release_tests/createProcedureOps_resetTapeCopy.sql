CREATE OR REPLACE PROCEDURE ops_resetTapeCopy(
  tapeCopyIdVar IN NUMBER
) AS

-- This procedure resets the tape-copy with the specified id to
-- TAPECOPY_CREATED.  This includes detaching the tape-copy from the streams
-- and deleting the corresponding tape-gateway sub-request if there is one.
--
-- Please note that this procedure does NOT commit the transaction, this is the
-- responsibility of the caller.
--
-- This procedure raises an application error with code -20001 if the specified
-- tape-copy does not exist.

  tapeGatewaySubRequestIdVar NUMBER(38);
BEGIN
  -- Try to set the status of the tape-copy to TAPECOPY_CREATED
  UPDATE TapeCopy
    SET STATUS = 0 -- TAPECOPY_CREATED
    WHERE id = tapeCopyIdVar;

  -- Raise an exception if the tape-copy does not exist
  IF SQL%ROWCOUNT = 0 THEN
    RAISE_APPLICATION_ERROR(-20001,
      'Tape-copy does not exist: tapeCopyIdVar=' || tapeCopyIdVar);
  END IF;

  -- Detach the tape-copy from the streams
  DELETE FROM Stream2TapeCopy WHERE child = tapeCopyIdVar;

  -- If there is a the corresponding tape-gateway sub-request the delete it
  DELETE FROM TapeGatewaySubRequest WHERE tapeCopy = tapeCopyIdVar
    RETURNING id INTO tapeGatewaySubRequestIdVar;
  IF SQL%ROWCOUNT != 0 THEN
    DELETE FROM id2Type WHERE id = tapeGatewaySubRequestIdVar;
  END IF;
END;
/
