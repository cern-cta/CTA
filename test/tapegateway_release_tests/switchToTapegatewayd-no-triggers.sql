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

DECLARE
  tpIds "numList";
BEGIN
  -- Deal with Migrations
  -- 1) Ressurect tapecopies for migration
  UPDATE TapeCopy SET status = 1 -- TAPECOPY_TOBEMIGRATED 
    WHERE status = 3; -- TAPECOPY_SELECTED
  -- 2) Clean up the streams
  UPDATE Stream
    SET
      status = 0 -- STREAM_PENDING
    WHERE status NOT IN (
      0, -- STREAM_PENDING
      5, -- STREAM_CREATED
      6, -- STREAM_STOPPED
      7  -- STREAM_WAITPOLICY
      ) RETURNING tape BULK COLLECT INTO tpIds;
  UPDATE Stream SET tape = NULL WHERE tape != 0;
  -- 3) Reset the tape for migration
  FORALL i IN tpIds.FIRST .. tpIds.LAST
    UPDATE tape
      SET
        stream = 0,
        status = 0 -- TAPE_UNUSED
      WHERE status IN (
        2, -- TAPE_WAITDRIVE,
        3, -- TAPE_WAITMOUNT, 
        4  -- TAPE_MOUNTED
        ) AND id = tpIds(i); 

  -- Deal with Recalls
  UPDATE Segment
    SET
      status = 0 -- SEGMENT_UNPROCESSED
    WHERE status = 7; -- SEGMENT_SELECTED
  UPDATE Tape
    SET
      status = 1 -- TAPE_PENDING
    WHERE
          tpmode = 0 -- WRITE_DISABLE
      AND status IN (
        2, -- TAPE_WAITDRIVE
        3, -- TAPE_WAITMOUNT
        4  -- TAPE_MOUNTED
        ); -- Resurrect the tapes running for recall
  UPDATE Tape A
    SET
      status = 8 -- TAPE_WAITPOLICY
    WHERE status IN (
      0, -- TAPE_UNUSED
      6, -- TAPE_FAILED
      7  -- TAPE_UNKNOWN
      ) AND EXISTS (
        SELECT id FROM Segment 
          WHERE
                status = 0 -- SEGMENT_UNPROCESSED
            AND tape = A.id);

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
/
