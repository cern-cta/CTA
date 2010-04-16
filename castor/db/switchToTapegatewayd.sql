/******************************************************************************
 *                 castor/db/switchToTapegatewayd.sql
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Giulia Taurelli, Nicola Bessone and Steven Murray
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

BEGIN
  -- Do nothing and rise an exception if the database is already compatible 
  -- with the tapegatewayd daemon
  IF tapegatewaydIsRunning THEN
    raise_application_error(-20000,
      'PL/SQL switchToTapegatewayd: Tapegateway already running.');
  END IF;
END;
/


-- Whilst this script is modifiying the database, the database is not
-- compatible with either the rtcpclientd or tape gateway daemons
UPDATE CastorConfig
  SET value = 'SwitchingToTapeGeteway'
  WHERE
    class = 'tape' AND
    key   = 'interfaceDaemon';
COMMIT;


BEGIN
  -- Remove the restartStuckRecallsJob as this job will not exist in the
  -- future tape gateway only schema
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('RESTARTSTUCKRECALLSJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;
END;
/


-- Create the tr_Tape_Pending trigger which automatically inserts a
-- row into the TapeGatewayRequest table when a recall-tape is pending
CREATE OR REPLACE TRIGGER tr_Tape_Pending
  AFTER UPDATE OF status ON Tape
  FOR EACH ROW WHEN (
    -- The status of the tape has been changed to "recall pending"
    NOT (old.status = 1) AND -- TAPE_PENDING
    new.status = 1       AND -- TAPE_PENDING
    new.tpmode = 0)          -- WRITE_DISABLED
BEGIN
  INSERT INTO TapeGatewayRequest(accessmode, id, taperecall, status)
    VALUES (TCONST.WRITE_DISABLE, ids_seq.nextval, :new.id, 
      TCONST.TAPEGATEWAY_TO_BE_SENT_TO_VDQM);
EXCEPTION
  -- Do nothing if the row already exists
  WHEN DUP_VAL_ON_INDEX THEN
    NULL;
END tr_Tape_Pending;
/


-- Create the tr_Stream_Pending trigger which automatically inserts a
-- row into the TapeGatewayRequest table when a migrate-stream is pending
CREATE OR REPLACE TRIGGER tr_Stream_Pending
  AFTER UPDATE of status ON Stream
  FOR EACH ROW WHEN (
    -- The status of the stream has been changed to "pending"
    NOT (old.status = 0) AND -- STREAM_PENDING
    new.status = 0)          -- STREAM_PENDING
BEGIN
  INSERT INTO TapeGatewayRequest (accessMode, id, streamMigration, Status)
    VALUES (TCONST.WRITE_ENABLE, ids_seq.nextval, :new.id, 
      TCONST.TAPEGATEWAY_TO_BE_RESOLVED);
EXCEPTION 
 -- Do nothing if the row already exists
  WHEN DUP_VAL_ON_INDEX THEN
    NULL;
END tr_Stream_Pending;
/


DECLARE
  tpIds "numList";
BEGIN
  -- Deal with Migrations
  -- 1) Ressurect tapecopies for migration
  UPDATE TapeCopy SET status = TCONST.TAPECOPY_TOBEMIGRATED 
    WHERE status = TCONST.TAPECOPY_SELECTED;
  -- 2) Clean up the streams
  UPDATE Stream SET status = TCONST.STREAM_PENDING
    WHERE status NOT IN (TCONST.STREAM_PENDING, TCONST.STREAM_CREATED, 
    TCONST.STREAM_STOPPED, TCONST.STREAM_WAITPOLICY)
    RETURNING tape BULK COLLECT INTO tpIds;
  UPDATE Stream SET tape = NULL WHERE tape != 0;
  -- 3) Reset the tape for migration
  FORALL i IN tpIds.FIRST .. tpIds.LAST
    UPDATE tape SET stream = 0, status = TCONST.TAPE_UNUSED
      WHERE status IN (TCONST.TAPE_WAITDRIVE, TCONST.TAPE_WAITMOUNT, 
      TCONST.TAPE_MOUNTED) AND id = tpIds(i); 

  -- Deal with Recalls
  UPDATE Segment SET status = TCONST.SEGMENT_UNPROCESSED
    WHERE status = TCONST.SEGMENT_SELECTED; -- Resurrect selected segments
  UPDATE Tape SET status = TCONST.TAPE_PENDING
    WHERE tpmode = TCONST.WRITE_DISABLE AND 
          status IN (TCONST.TAPE_WAITDRIVE, TCONST.TAPE_WAITMOUNT, 
          TCONST.TAPE_MOUNTED); -- Resurrect the tapes running for recall
  UPDATE Tape A SET status = TCONST.TAPE_WAITPOLICY
    WHERE status IN (TCONST.TAPE_UNUSED, TCONST.TAPE_FAILED, 
    TCONST.TAPE_UNKNOWN) AND EXISTS
    ( SELECT id FROM Segment 
      WHERE status = TCONST.SEGMENT_UNPROCESSED AND tape = A.id);

  -- Start the restartStuckRecallsJob
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'RESTARTSTUCKRECALLSJOB',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN restartStuckRecalls(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=60',
      ENABLED         => TRUE,
      COMMENTS        => 'Workaround to restart stuck recalls');
END;
/

-- The database is now compatible with the tapegatewayd daemon
UPDATE CastorConfig
  SET value = 'tapegatewayd'
  WHERE
    class = 'tape' AND
    key   = 'interfaceDaemon';
COMMIT;
