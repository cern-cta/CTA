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

-- Failures from this point on will require some manual intervention.

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

BEGIN
  -- Deal with Migrations
  -- 1) Ressurect tapecopies for migration
  UPDATE TapeCopy tc SET tc.status = TCONST.TAPECOPY_TOBEMIGRATED,
                         tc.VID = NULL
    WHERE tc.status IN (TCONST.TAPECOPY_WAITPOLICY, TCONST.TAPECOPY_WAITINSTREAMS,
                        TCONST.TAPECOPY_SELECTED, TCONST.TAPECOPY_MIG_RETRY);
                        -- STAGED and FAILED can stay the same, other states are for recalls.
END;
/

TODOTODO Convert failed and retied segments into a summary of retry count, error code for the latest one.

-- Streams do not need to be kept. The mighunter will recreate them all.
DELETE FROM Stream2TapeCopy;
DELETE FROM STREAM;

  -- Deal with Recalls
  -- Segments
BEGIN
  UPDATE Segment SET status = TCONST.SEGMENT_UNPROCESSED
    WHERE status IN (TCONST.SEGMENT_SELECTED); -- Resurrect selected segments
END;
/

BEGIN
  -- Write tape mounts can be removed.
  DELETE FROM Tape T WHERE T.TPMode = TCONST.TPMODE_WRITE;
  
   -- Read tape mounts that are not referenced by any segment are dropped
   DELETE FROM Tape T
    WHERE T.tpMode = TCONST.TPMODE_READ AND (
       NOT EXISTS ( SELECT Seg.id FROM Segment Seg
      WHERE Seg.tape = T.id));  -- Resurrect the tapes running for recall
  -- Reset active read tape mounts to pending (re-do)
  UPDATE Tape SET status = TCONST.TAPE_PENDING
    WHERE tpmode = TCONST.TPMODE_READ AND 
          status IN (TCONST.TAPE_WAITDRIVE, TCONST.TAPE_WAITMOUNT, 
          TCONST.TAPE_MOUNTED);
  -- Resurrect tapes with UNPROCESSED segments (preserving WAITPOLICY state)
  UPDATE Tape T SET T.status = TCONST.TAPE_PENDING
    WHERE T.status NOT IN (TCONST.TAPE_WAITPOLICY, TCONST.TAPE_PENDING) AND EXISTS
    ( SELECT Seg.id FROM Segment Seg
      WHERE Seg.status = TCONST.SEGMENT_UNPROCESSED AND Seg.tape = T.id);
   -- We keep the tapes referenced by failed segments and move them to UNUSED. Those are the ones WITHOUT
   -- unprocessed segments
   UPDATE Tape T SET T.status = TCONST.TAPE_UNUSED
    WHERE NOT EXISTS
    ( SELECT Seg.id FROM Segment Seg
      WHERE Seg.status = TCONST.SEGMENT_UNPROCESSED AND Seg.tape = T.id);
   -- Pending tapes need a tapegatewayrequest id
   UPDATE Tape T SET T.TapeGatewayRequestId = ids_seq.nextval;
END;
/

---------------------------------------------------------------------------------------------------------
-- The database is now compatible with the tapegatewayd daemon
UPDATE CastorConfig
  SET value = 'tapegatewayd'
  WHERE
    class = 'tape' AND
    key   = 'interfaceDaemon';
COMMIT;
