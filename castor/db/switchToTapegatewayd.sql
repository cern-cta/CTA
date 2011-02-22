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

-- Failures fron this point on will require some manual intervention.

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
  UPDATE TapeCopy tc SET tc.status = TCONST.TAPECOPY_TOBEMIGRATED 
    WHERE tc.status IN (TCONST.TAPECOPY_WAITPOLICY, TCONST.TAPECOPY_WAITINSTREAMS,
                        TCONST.TAPECOPY_SELECTED, TCONST.TAPECOPY_MIGRETRY)
                        -- STAGED and FAILED can stay the same, other states are for recalls.
  -- 2) Clean up the streams and tapes for writing
  DELETE FROM Stream;
  DELETE FROM Stream2Tapecopy;
  DELETE FROM Tape T WHERE T.TPMode = TCONST.TPMODE_WRITE;
  COMMIT;
  
  -- Deal with Recalls
  -- Segments
  UPDATE Segment SET status = TCONST.SEGMENT_UNPROCESSED
    WHERE status IN (TCONST.SEGMENT_SELECTED); -- Resurrect selected segments
  UPDATE Segment SET status = TCONST.SEGMENT_FAILED
    WHERE status IN (TCONST.SEGMENT_RETRIED); -- RETRIED is not used in tape gateway.
    
  -- Tapes
  -- Resurrect the tapes running for recall
  UPDATE Tape SET status = TCONST.TAPE_PENDING
    WHERE tpmode = TCONST.TPMODE_READ AND 
          status IN (TCONST.TAPE_WAITDRIVE, TCONST.TAPE_WAITMOUNT, 
          TCONST.TAPE_MOUNTED); 
  UPDATE Tape A SET status = TCONST.TAPE_WAITPOLICY
    WHERE status IN (TCONST.TAPE_UNUSED, TCONST.TAPE_FAILED, 
    TCONST.TAPE_UNKNOWN) AND EXISTS
    ( SELECT id FROM Segment 
      WHERE status = TCONST.SEGMENT_UNPROCESSED AND tape = A.id);
   -- Other tapes are not relevant
   DELETE FROM Tape T
    WHERE T.tpMode = TCONST.TPMODE_READ AND (
       T. Status NOT IN (TCONST.TAPE_WAITPOLICY) OR
       NOT EXISTS ( SELECT id FROM Segment 
      WHERE status = TCONST.SEGMENT_UNPROCESSED AND tape = A.id));

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
