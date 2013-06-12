/*******************************************************************
 * PL/SQL code for Draining FileSystems Logic
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* deletes a DrainingJob and related objects */
CREATE OR REPLACE PROCEDURE deleteDrainingJob(inDjId IN INTEGER) AS
  varUnused INTEGER;
BEGIN
  -- take a lock on the drainingJob
  SELECT id INTO varUnused FROM DrainingJob WHERE id = inDjId;
  -- drop ongoing Disk2DiskCopyJobs
  DELETE FROM Disk2DiskCopyJob WHERE drainingJob = inDjId;
  -- delete associated errors
  DELETE FROM DrainingErrors WHERE drainingJob = inDjId;
  -- finally delete the DrainingJob
  DELETE FROM DrainingJob WHERE id = inDjId;
END;
/

/* handle the creation of the Disk2DiskCopyJobs related to a given
 * DrainingJob */
CREATE OR REPLACE PROCEDURE handleDrainingJob(inDjId IN INTEGER) AS
  varFsId INTEGER;
  varSvcClassId INTEGER;
  varAutoDelete INTEGER;
  varFileMask INTEGER;
  varEuid INTEGER;
  varEgid INTEGER;
  varNbFiles INTEGER := 0;
  varNbBytes INTEGER := 0;
BEGIN
  -- update Draining Job to STARTING, and get its caracteristics
  UPDATE DrainingJob SET status = dconst.DRAININGJOB_STARTING WHERE id = inDjId
  RETURNING fileSystem, svcClass, autoDelete, fileMask, euid, egid
    INTO varFsId, varSvcClassId, varAutoDelete, varFileMask, varEuid, varEgid;
  COMMIT;
  -- Loop over the creation of Disk2DiskCopyJobs
  FOR F IN (SELECT CastorFile.id cfId, Castorfile.nsOpenTime, DiskCopy.id dcId, CastorFile.fileSize
              FROM DiskCopy, CastorFile
             WHERE fileSystem = varFsId
               AND CastorFile.id = DiskCopy.castorFile
               AND ((varFileMask = dconst.DRAIN_FILEMASK_NOTONTAPE AND
                     CastorFile.tapeStatus IN (dconst.CASTORFILE_NOTONTAPE, dconst.CASTORFILE_DISKONLY)) OR
                    (varFileMask = dconst.DRAIN_FILEMASK_ALL))
               AND DiskCopy.status = dconst.DISKCOPY_VALID) LOOP
    createDisk2DiskCopyJob(F.cfId, F.nsOpenTime, varSvcClassId, varEuid, varEgid, dconst.REPLICATIONTYPE_DRAINING,
                           CASE varFileMask WHEN 1 THEN F.dcId ELSE NULL END, inDjId);
    varNbFiles := varNbFiles + 1;
    varNbBytes := varNbBytes + F.fileSize;
    -- commit and update counters from time to time
    IF MOD(varNbFiles, 1000) = 0 THEN
      UPDATE DrainingJob
         SET totalFiles = varNbFiles,
             totalBytes = varNbBytes,
             lastModificationTime = getTime()
       WHERE id = inDjId;
      COMMIT;
    END IF;
  END LOOP;
  -- final update of DrainingJob
  UPDATE DrainingJob
     SET status = dconst.DRAININGJOB_RUNNING,
         totalFiles = varNbFiles,
         totalBytes = varNbBytes,
         lastModificationTime = getTime()
   WHERE id = inDjId;
  COMMIT;
END;
/

/* Procedure responsible for managing the draining process */
CREATE OR REPLACE PROCEDURE drainManager AS
  varStartTime NUMBER;
  varNbDrainingJobs INTEGER;
BEGIN
  -- Delete the COMPLETED jobs older than 7 days
  DELETE FROM DrainingJob
   WHERE status = dconst.DRAININGJOB_FINISHED
     AND lastModificationTime < getTime() - (7 * 86400);
  COMMIT;
  -- check we do not run twice in parallel the starting of draining jobs
  SELECT count(*) INTO varNbDrainingJobs
    FROM DrainingJob
   WHERE status = dconst.DRAININGJOB_STARTING;
  IF varNbDrainingJobs > 0 THEN
    -- this shouldn't happen, possibly this procedure is running in parallel: give up and log
    -- "drainingManager: Draining jobs still starting, no new ones will be started for this round"
    logToDLF(NULL, dlf.LVL_NOTICE, dlf.DRAINING_JOB_ONGOING, 0, '', 'draind', '');
    RETURN;
  END IF;
  varStartTime := getTime();
  WHILE TRUE LOOP
    DECLARE
      varDrainStartTime NUMBER;
      varDjId INTEGER;
    BEGIN
      varDrainStartTime := getTime();
      -- get an outstanding DrainingJob to start
      SELECT id INTO varDJId
        FROM (SELECT id
                FROM DrainingJob
               WHERE status = dconst.DRAININGJOB_SUBMITTED
               ORDER BY creationTime ASC)
       WHERE ROWNUM < 2;
      -- start the draining: this can take some considerable time and will
      -- regularly commit after updating the DrainingJob
      handleDrainingJob(varDjId);
      varNbDrainingJobs := varNbDrainingJobs + 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- if no new repack is found to start, terminate
      EXIT;
    END;
  END LOOP;
  IF varNbDrainingJobs > 0 THEN
    -- log some statistics
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DRAINING_JOB_STATS, 0, '', 'draind',
      'nbStarted=' || TO_CHAR(varNbDrainingJobs) || ' elapsedTime=' || TO_CHAR(TRUNC(getTime() - varStartTime)));
  END IF;
END;
/


/* SQL statement for DBMS_SCHEDULER job creation */
BEGIN
  -- Remove jobs related to the draining logic before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('DRAINMANAGERJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create the drain manager job to be executed every minute
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'drainManagerJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN drainManager(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 5/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Database job to manage the draining process');
END;
/
