/* Switch on tapegatewayd */


BEGIN 
 DELETE FROM castorconfig 
   WHERE class='tape' AND  key='daemonName';
 INSERT INTO castorconfig (class, key, value, description) 
	VALUES ('tape', 'daemonName', 'tapegatewayd','name of the daemon use to interface the tape system');
 COMMIT;
END;
/


/* Recalls */

ALTER TRIGGER tr_Tape_Pending enable;


/* Migrations */

ALTER TRIGGER tr_Stream_Pending enable;


/*resurrect old candidates to have the trigger firing for them*/

DECLARE
 tpIds "numList";
BEGIN
  -- Deal with Migrations
  -- 1) Ressurect tapecopies for migration
  UPDATE TapeCopy SET status = 1 WHERE status = 3;
  -- 2) Clean up the streams
  UPDATE Stream SET status = 0 
   WHERE status NOT IN (0, 5, 6, 7) --PENDING, CREATED, STOPPED, WAITPOLICY
  RETURNING tape BULK COLLECT INTO tpIds;
  UPDATE Stream SET tape = NULL WHERE tape != 0;
  -- 3) Reset the tape for migration
  FORALL i IN tpIds.FIRST .. tpIds.LAST  
    UPDATE tape SET stream = 0, status = 0 WHERE status IN (2, 3, 4) AND id = tpIds(i);
  -- Deal with Recalls
  UPDATE Segment SET status = 0 WHERE status = 7; -- Resurrect SELECTED segment
  UPDATE Tape SET status = 1 WHERE tpmode = 0 AND status IN (2, 3, 4); -- Resurrect the tapes running for recall
  UPDATE Tape A SET status = 8 
   WHERE status IN (0, 6, 7) AND EXISTS
    (SELECT id FROM Segment WHERE status = 0 AND tape = A.id);
  COMMIT;
END;
/



