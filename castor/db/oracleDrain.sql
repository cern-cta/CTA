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

/* handle the creation of the Disk2DiskCopyJobs for the running drainingJobs */
CREATE OR REPLACE PROCEDURE drainRunner AS
  varNbFiles INTEGER := 0;
  varNbBytes INTEGER := 0;
  varNbRunningJobs INTEGER;
  varMaxNbOfSchedD2dPerDrain INTEGER;
BEGIN
  -- get maxNbOfSchedD2dPerDrain
  varMaxNbOfSchedD2dPerDrain := TO_NUMBER(getConfigOption('Draining', 'MaxNbSchedD2dPerDrain', '1000'));
  -- loop over draining jobs
  FOR dj IN (SELECT id, fileSystem, svcClass, fileMask, euid, egid
               FROM DrainingJob WHERE status = dconst.DRAININGJOB_RUNNING) LOOP
    -- check how many disk2DiskCopyJobs are already running for this draining job
    SELECT count(*) INTO varNbRunningJobs FROM Disk2DiskCopyJob WHERE drainingJob = dj.id;
    -- Loop over the creation of Disk2DiskCopyJobs. Select max 1000 files, taking running
    -- ones into account. Also Take the most important jobs first
    FOR F IN (SELECT * FROM
               (SELECT CastorFile.id cfId, Castorfile.nsOpenTime, DiskCopy.id dcId, CastorFile.fileSize
                  FROM DiskCopy, CastorFile
                 WHERE DiskCopy.fileSystem = dj.fileSystem
                   AND CastorFile.id = DiskCopy.castorFile
                   AND ((dj.fileMask = dconst.DRAIN_FILEMASK_NOTONTAPE AND
                         CastorFile.tapeStatus IN (dconst.CASTORFILE_NOTONTAPE, dconst.CASTORFILE_DISKONLY)) OR
                        (dj.fileMask = dconst.DRAIN_FILEMASK_ALL))
                   AND DiskCopy.status = dconst.DISKCOPY_VALID
                   AND NOT EXISTS (SELECT 1 FROM Disk2DiskCopyJob WHERE castorFile=CastorFile.id)
                 ORDER BY DiskCopy.importance DESC)
               WHERE ROWNUM <= varMaxNbOfSchedD2dPerDrain-varNbRunningJobs) LOOP
      createDisk2DiskCopyJob(F.cfId, F.nsOpenTime, dj.svcClass, dj.euid, dj.egid,
                             dconst.REPLICATIONTYPE_DRAINING, F.dcId, dj.id);
      varNbFiles := varNbFiles + 1;
      varNbBytes := varNbBytes + F.fileSize;
    END LOOP;
    -- commit and update counters
    UPDATE DrainingJob
       SET totalFiles = totalFiles + varNbFiles,
           totalBytes = totalBytes + varNbBytes,
           lastModificationTime = getTime()
     WHERE id = dj.id;
    COMMIT;
  END LOOP;
END;
/

/* Procedure responsible for managing the draining process
 * note the locking that makes sure we are not running twice in parallel
 * as this will be restarted regularly by an Oracle job, but may take long to conclude
 */
CREATE OR REPLACE PROCEDURE drainManager AS
  varTFiles INTEGER;
  varTBytes INTEGER;
BEGIN
  -- Delete the COMPLETED jobs older than 7 days
  DELETE FROM DrainingJob
   WHERE status = dconst.DRAININGJOB_FINISHED
     AND lastModificationTime < getTime() - (7 * 86400);
  COMMIT;

  -- Start new DrainingJobs if needed
  FOR dj IN (SELECT id, fileSystem FROM DrainingJob WHERE status = dconst.DRAININGJOB_SUBMITTED) LOOP
    UPDATE DrainingJob SET status = dconst.DRAININGJOB_STARTING WHERE id = dj.id;
    COMMIT;
    SELECT count(*), SUM(diskCopySize) INTO varTFiles, varTBytes
      FROM DiskCopy
     WHERE fileSystem = dj.fileSystem
       AND status = dconst.DISKCOPY_VALID;
    UPDATE DrainingJob
       SET totalFiles=varTFiles, totalBytes=varTBytes, status = dconst.DRAININGJOB_RUNNING
     WHERE id = dj.id;
    COMMIT;
  END LOOP;
END;
/

/* Procedure responsible for rebalancing one given filesystem by moving away
 * the given amount of data */
CREATE OR REPLACE PROCEDURE rebalance(inFsId IN INTEGER, inDataAmount IN INTEGER,
                                      inDestSvcClassId IN INTEGER,
                                      inDiskServerName IN VARCHAR2, inMountPoint IN VARCHAR2) AS
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
  varNbFilesRebalanced INTEGER := 0;
BEGIN
  -- disk to disk copy files out of this node until we reach inDataAmount
  -- "rebalancing : starting" message
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REBALANCING_START, 0, '', 'stagerd',
           'DiskServer=' || inDiskServerName || ' mountPoint=' || inMountPoint ||
           ' dataTomove=' || TO_CHAR(inDataAmount));
  -- Loop on candidates until we can lock one
  OPEN DCcur;
  LOOP
    -- Fetch next candidate
    FETCH DCcur INTO varDcId, varDcSize, varEuid, varEgid, varCfId, varNsOpenTime;
    varTotalRebalanced := varTotalRebalanced + varDcSize;
    varNbFilesRebalanced := varNbFilesRebalanced + 1;
    -- stop if it would be too much
    IF varTotalRebalanced > inDataAmount THEN RETURN; END IF;
    -- create disk2DiskCopyJob for this diskCopy
    createDisk2DiskCopyJob(varCfId, varNsOpenTime, inDestSvcClassId,
                           varEuid, varEgid, dconst.REPLICATIONTYPE_REBALANCE,
                           varDcId, NULL);
  END LOOP;
  -- "rebalancing : stopping" message
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REBALANCING_STOP, 0, '', 'stagerd',
           'DiskServer=' || inDiskServerName || ' mountPoint=' || inMountPoint ||
           ' dataMoved=' || TO_CHAR(varTotalRebalanced) ||
           ' nbFilesMoved=' || TO_CHAR(varNbFilesRebalanced));
END;
/

/* Procedure responsible for rebalancing of data on nodes within diskpools */
CREATE OR REPLACE PROCEDURE rebalancingManager AS
  varFreeRef NUMBER;
  varSensibility NUMBER;
  varNbDS INTEGER;
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
    -- check that we have more than one diskserver online
    SELECT count(unique DiskServer.name) INTO varNbDS
      FROM FileSystem, DiskPool2SvcClass, DiskServer
     WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
       AND DiskPool2SvcClass.child = SC.id
       AND DiskServer.id = FileSystem.diskServer
       AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
       AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
       AND DiskServer.hwOnline = 1;
    -- if only 1 diskserver available, do nothing for this round
    IF varNbDS < 2 THEN
      CONTINUE;
    END IF;
    -- compute average filling of filesystems on production machines
    -- note that read only ones are not taken into account as they cannot
    -- be filled anymore
    SELECT AVG(free/totalSize) INTO varFreeRef
      FROM FileSystem, DiskPool2SvcClass, DiskServer
     WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
       AND DiskPool2SvcClass.child = SC.id
       AND DiskServer.id = FileSystem.diskServer
       AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
       AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
       AND DiskServer.hwOnline = 1
     GROUP BY SC.id;
    -- get sensibility of the rebalancing
    varSensibility := TO_NUMBER(getConfigOption('Rebalancing', 'Sensibility', '5'))/100;
    -- for each filesystem too full compared to average, rebalance
    -- note that we take the read only ones into account here
    FOR FS IN (SELECT FileSystem.id, varFreeRef*totalSize-free dataToMove,
                      DiskServer.name ds, FileSystem.mountPoint
                 FROM FileSystem, DiskPool2SvcClass, DiskServer
                WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
                  AND DiskPool2SvcClass.child = SC.id
                  AND varFreeRef - free/totalSize > varSensibility
                  AND DiskServer.id = FileSystem.diskServer
                  AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
                  AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
                  AND DiskServer.hwOnline = 1) LOOP
      rebalance(FS.id, FS.dataToMove, SC.id, FS.ds, FS.mountPoint);
    END LOOP;
  END LOOP;
END;
/

/* SQL statement for DBMS_SCHEDULER job creation */
BEGIN
  -- Remove jobs related to the draining logic before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('DRAINMANAGERJOB', 'DRAINRUNNERJOB', 'REBALANCINGJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;


  -- Create the drain manager job to be executed every minute. This one starts and clean up draining jobs
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'drainManagerJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN drainManager(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Database job to manage the draining process');

  -- Create the drain runner job to be executed every minute. This one checks whether new
  -- disk2diskCopies need to be created for a given draining job
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'drainRunnerJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN drainRunner(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
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
