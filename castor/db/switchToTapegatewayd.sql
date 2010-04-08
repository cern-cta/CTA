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

DECLARE
  tpIds "numList";
  unused VARCHAR2(2048);
BEGIN
  -- Do nothing and return if the database is already compatible with the
  -- tapegatewayd daemon
  BEGIN
    SELECT value INTO unused
     FROM CastorConfig
     WHERE class = 'tape'
       AND key   = 'interfaceDaemon'
       AND value = 'tapegatewayd';
     RETURN;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Do nothing and continue
    NULL;
  END;

  -- The database is about to be modified and is therefore not compatible with
  -- either the rtcpclientd daemon or the tape gateway daemon
  UPDATE CastorConfig
    SET value = 'NONE'
    WHERE
      class = 'tape' AND
      key   = 'interfaceDaemon';
  COMMIT;

  -- Remove the restartStuckRecallsJob as this job will not exist in the
  -- future tape gateway only schema
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('RESTARTSTUCKRECALLSJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create the tr_Tape_Pending trigger which automatically inserts a
  -- row into the TapeGatewayRequest table when a recall-tape is pending
  EXECUTE IMMEDIATE
    'CREATE OR REPLACE TRIGGER tr_Tape_Pending' ||
    '  AFTER UPDATE OF status ON Tape' ||
    '  FOR EACH ROW WHEN (new.status = castor.TAPE_PENDING and new.tpmode=castor.WRITE_DISABLE) ' ||
    'DECLARE' ||
    '  unused NUMBER; ' ||
    'BEGIN' ||
    '  SELECT id INTO unused' ||
    '    FROM TapeGatewayRequest' ||
    '    WHERE taperecall=:new.id; ' ||
    'EXCEPTION WHEN NO_DATA_FOUND THEN ' ||
    '  INSERT INTO TapeGatewayRequest (accessmode, starttime,' ||
    '    lastvdqmpingtime, vdqmvolreqid, id, streammigration, taperecall,' ||
    '    status)' ||
    '    VALUES (0,null,null,null,ids_seq.nextval,null,:new.id,1); ' ||
    'END tr_Tape_Pending;';

  -- Create the tr_Stream_Pending trigger which automatically inserts a
  -- row into the TapeGatewayRequest table when a recall-tape is pending
  EXECUTE IMMEDIATE
    'CREATE OR REPLACE TRIGGER tr_Stream_Pending' ||
    '  AFTER UPDATE of status ON Stream' ||
    '  FOR EACH ROW WHEN (new.status = castor.STRAM_PENDING ) ' ||
    'DECLARE' ||
    '  unused NUMBER; ' ||
    'BEGIN' ||
    '  SELECT id INTO unused' ||
    '    FROM TapeGatewayRequest' ||
    '    WHERE streammigration=:new.id; ' ||
    'EXCEPTION WHEN NO_DATA_FOUND THEN' ||
    '  INSERT INTO TapeGatewayRequest (accessMode, startTime, ' ||
    '    lastVdqmPingTime, vdqmVolReqId, id, streamMigration, TapeRecall, ' ||
    '    Status) ' ||
    '    VALUES (1,null,null,null,ids_seq.nextval,:new.id,null,0); ' ||
    'END tr_Stream_Pending;';

  -- Deal with Migrations
  -- 1) Ressurect tapecopies for migration
  UPDATE TapeCopy SET status = castor.TAPECOPY_TOBEMIGRATED 
    WHERE status = castor.TAPECOPY_SELECTED;
  -- 2) Clean up the streams
  UPDATE Stream SET status = castor.STREAM_PENDING
    WHERE status NOT IN (castor.STREAM_PENDING, castor.STREAM_CREATED, 
    castor.STREAM_STOPPED, castor.STREAM_WAITPOLICY)
    RETURNING tape BULK COLLECT INTO tpIds;
  UPDATE Stream SET tape = NULL WHERE tape != 0;
  -- 3) Reset the tape for migration
  FORALL i IN tpIds.FIRST .. tpIds.LAST
    UPDATE tape SET stream = 0, status = castor.TAPE_UNUSED
      WHERE status IN (castor.TAPE_WAITDRIVE, castor.TAPE_WAITMOUNT, 
      castor.TAPE_MOUNTED) AND id = tpIds(i); 

  -- Deal with Recalls
  UPDATE Segment SET status = castor.SEGMENT_UNPROCESSED
    WHERE status = castor.SEGMENT_SELECTED; -- Resurrect SELECTED segment
  UPDATE Tape SET status = castor.TAPE_PENDING
    WHERE tpmode = castor.WRITE_DISABLE AND 
          status IN (castor.TAPE_WAITDRIVE, castor.TAPE_WAITMOUNT, 
          castor.TAPE_MOUNTED); -- Resurrect the tapes running for recall
  UPDATE Tape A SET status = castor.TAPE_WAITPOLICY
    WHERE status IN (castor.TAPE_UNUSED, castor.TAPE_FAILED, 
    castor.TAPE_UNKNOWN) AND EXISTS
    (SELECT id FROM Segment 
     WHERE status = castor.SEGMENT_UNPROCESSED AND tape = A.id);

  -- Start the restartSuckRecallsJob
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'RESTARTSTUCKRECALLSJOB',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN restartStuckRecalls(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=60',
      ENABLED         => TRUE,
      COMMENTS        => 'Workaround to restart stuck recalls');

  -- The database is now compatible with the tapegatewayd daemon
  UPDATE CastorConfig
    SET value = 'tapegatewayd'
    WHERE
      class = 'tape' AND
      key   = 'interfaceDaemon';
  COMMIT;

END;
/
