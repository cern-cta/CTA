/*******************************************************************
 * PL/SQL code for Draining FileSystems Logic
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* deletes a DrainingJob and related objects */
CREATE OR REPLACE PROCEDURE deleteDrainingJob(inDjId IN INTEGER) AS
  varUnused INTEGER;
BEGIN
  -- take a lock on the drainingJob
  SELECT id INTO varUnused FROM DrainingJob WHERE id = inDjId FOR UPDATE;
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
  varStartedNewJobs BOOLEAN := False;
  varNbRunningJobs INTEGER;
BEGIN
  -- update Draining Job to STARTING, and get its caracteristics. Note that most of the time,
  -- it was already in STARTING, but that does not harm and we select what we need
  UPDATE DrainingJob SET status = dconst.DRAININGJOB_STARTING WHERE id = inDjId
  RETURNING fileSystem, svcClass, autoDelete, fileMask, euid, egid
    INTO varFsId, varSvcClassId, varAutoDelete, varFileMask, varEuid, varEgid;
  -- check how many disk2DiskCopyJobs are already running for this draining job
  SELECT count(*) INTO varNbRunningJobs FROM Disk2DiskCopyJob WHERE drainingJob = inDjId;
  -- Loop over the creation of Disk2DiskCopyJobs. Select max 1000 files, taking running
  -- ones into account. Also Take the most important jobs first
  FOR F IN (SELECT * FROM
             (SELECT CastorFile.id cfId, Castorfile.nsOpenTime, DiskCopy.id dcId, CastorFile.fileSize
                FROM DiskCopy, CastorFile
               WHERE DiskCopy.fileSystem = varFsId
                 AND CastorFile.id = DiskCopy.castorFile
                 AND ((varFileMask = dconst.DRAIN_FILEMASK_NOTONTAPE AND
                       CastorFile.tapeStatus IN (dconst.CASTORFILE_NOTONTAPE, dconst.CASTORFILE_DISKONLY)) OR
                      (varFileMask = dconst.DRAIN_FILEMASK_ALL))
                 AND DiskCopy.status = dconst.DISKCOPY_VALID
               ORDER BY DiskCopy.importance DESC)
             WHERE ROWNUM <= 1000-varNbRunningJobs) LOOP
    varStartedNewJobs := True;
    createDisk2DiskCopyJob(F.cfId, F.nsOpenTime, varSvcClassId, varEuid, varEgid, dconst.REPLICATIONTYPE_DRAINING,
                           CASE varFileMask WHEN 1 THEN F.dcId ELSE NULL END, inDjId);
    varNbFiles := varNbFiles + 1;
    varNbBytes := varNbBytes + F.fileSize;
  END LOOP;
  -- commit and update counters
  IF varStartedNewJobs THEN
    UPDATE DrainingJob
       SET totalFiles = totalFiles + varNbFiles,
           totalBytes = totalBytes + varNbBytes,
           lastModificationTime = getTime()
     WHERE id = inDjId;
  ELSE
    -- final update of DrainingJob TO RUNNING if there was nothing to do
    UPDATE DrainingJob SET status = dconst.DRAININGJOB_RUNNING WHERE id = inDjId;
  END IF;
  COMMIT;
END;
/

/* Procedure responsible for managing the draining process
 * note the locking that makes sure we are not running twice in parallel
 * as this will be restarted regularly by an Oracle job, but may take long to conclude
 */
CREATE OR REPLACE PROCEDURE drainManager AS
BEGIN
  -- Delete the COMPLETED jobs older than 7 days
  DELETE FROM DrainingJob
   WHERE status = dconst.DRAININGJOB_FINISHED
     AND lastModificationTime < getTime() - (7 * 86400);
  COMMIT;

  -- Loop over the Draining Jobs
  FOR dj IN (SELECT id FROM DrainingJob
              WHERE status IN (dconst.DRAININGJOB_SUBMITTED, dconst.DRAININGJOB_STARTING)) LOOP
    -- handle the draining job, that is check whether to start new Disk2DiskCopy for it
    handleDrainingJob(dj.id);
  END LOOP;
END;
/

/* Procedure responsible for rebalancing one given filesystem by moving away
 * the given amount of data */
CREATE OR REPLACE PROCEDURE rebalance(inFsId IN INTEGER, inDataAmount IN INTEGER,
                                      inDestSvcClassId IN INTEGER) AS
  CURSOR DCcur IS
    SELECT /*+ FIRST_ROWS_10 */
           DiskCopy.id, DiskCopy.diskCopySize, DiskCopy.owneruid, DiskCopy.ownergid,
           CastorFile.id, CastorFile.nsOpenTime
      FROM DiskCopy, CastorFile
     WHERE DiskCopy.fileSystem = inFsId
       AND DiskCopy.status = dconst.DISKCOPY_VALID
       AND CastorFile.id = DiskCopy.castorFile;
  varDcId INTEGER;
  varDcSize INTEGER;
  varEuid INTEGER;
  varEgid INTEGER;
  varCfId INTEGER;
  varNsOpenTime INTEGER;
  varTotalRebalanced INTEGER := 0;
BEGIN
  -- disk to disk copy files out of this node until we reach inDataAmount
    -- Loop on candidates until we can lock one
  LOOP
    -- Fetch next candidate
    FETCH DCcur INTO varDcId, varDcSize, varEuid, varEgid, varCfId, varNsOpenTime;
    varTotalRebalanced := varTotalRebalanced + varDcSize;
    -- stop if it would be too much
    IF varTotalRebalanced > inDataAmount THEN RETURN; END IF;
    -- create disk2DiskCopyJob for this diskCopy
    createDisk2DiskCopyJob(varCfId, varNsOpenTime, inDestSvcClassId,
                           varEuid, varEgid, dconst.REPLICATIONTYPE_REBALANCE,
                           varDcId, NULL);
  END LOOP;
END;
/

/* Procedure responsible for rebalancing of data on nodes within diskpools */
CREATE OR REPLACE PROCEDURE rebalancingManager AS
  varFreeRef NUMBER;
  varSensibility NUMBER;
  varAlreadyRebalancing INTEGER;
BEGIN
  -- go through all service classes
  FOR SC IN (SELECT id FROM SvcClass) LOOP
    -- check if we are already rebalancing
    SELECT count(*) INTO varAlreadyRebalancing
      FROM Disk2DiskCopyJob
     WHERE destSvcClass = SC.id
       AND replicationType = dconst.REPLICATIONTYPE_REBALANCE
       AND ROWNUM < 2;
    -- if yes, do nothing for this round
    IF varAlreadyRebalancing > 0 THEN
      CONTINUE;
    END IF;
    -- compute average filling of filesystems
    SELECT AVG(free/totalSize) INTO varFreeRef
      FROM FileSystem, DiskPool2SvcClass
     WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
       AND DiskPool2SvcClass.child = SC.id GROUP BY SC.id;
    -- get sensibility of the rebalancing
    varSensibility := TO_NUMBER(getConfigOption('Rebalancing', 'Sensibility', '5'))/100;
    -- for each filesystem too full compared to average, rebalance
    FOR FS IN (SELECT id, varFreeRef*totalSize-free dataToMove
                 FROM FileSystem, DiskPool2SvcClass
                WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
                  AND DiskPool2SvcClass.child = SC.id
                  AND varFreeRef - free/totalSize > varSensibility) LOOP
      rebalance(FS.id, FS.dataToMove, SC.id);
    END LOOP;
  END LOOP;
END;
/

/* SQL statement for DBMS_SCHEDULER job creation */
BEGIN
  -- Remove jobs related to the draining logic before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('DRAINMANAGERJOB', 'REBALANCINGJOB'))
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

  -- Create the drain manager job to be executed every minute
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'rebalancingJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN rebalancingManager(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 5/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Database job to manage rebalancing of data on nodes within diskpools');
END;
/
