/*******************************************************************
 * @(#)$RCSfile: oracleDrain.sql,v $ $Revision: 1.3 $ $Date: 2009/02/12 14:25:36 $ $Author: itglp $
 * PL/SQL code for Draining FileSystems Logic
 *
 * Additional procedures modified to support the DrainingFileSystems
 * logic include: disk2DiskCopyDone, disk2DiskCopyFailed,
 * selectFiles2Delete, storeClusterStatus tr_DiskServer_Update and
 * tr_FileSystem_Update
 *
 * Note: the terms filesystem and mountpoint are used interchangeably
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement for the delete trigger on the DrainingFileSystem table */
CREATE OR REPLACE TRIGGER tr_DrainingFileSystem_Delete
BEFORE DELETE ON DrainingFileSystem
FOR EACH ROW
DECLARE
  CURSOR c IS
    SELECT STDCRR.id, SR.id subrequest, STDCRR.client
      FROM StageDiskCopyReplicaRequest STDCRR, DrainingDiskCopy DDC,
           SubRequest SR
     WHERE STDCRR.sourceDiskCopy = DDC.diskCopy
       AND SR.request = STDCRR.id
       AND decode(DDC.status, 4, DDC.status, NULL) = 4  -- FAILED
       AND SR.status = 9  -- FAILED_FINISHED
       AND DDC.fileSystem = :old.fileSystem;
  reqIds    "numList";
  clientIds "numList";
  srIds     "numList";
BEGIN
  -- Remove failed transfers requests from the StageDiskCopyReplicaRequest
  -- table. If we do not do this files which failed due to "Maximum number of
  -- attempts exceeded" cannot be resubmitted to the system.
  -- (see getBestDiskCopyToReplicate)
  LOOP
    OPEN c;
    FETCH c BULK COLLECT INTO reqIds, srIds, clientIds LIMIT 10000;
    -- Break out of the loop when the cursor returns no results
    EXIT WHEN c%NOTFOUND;
    -- Delete data
    FORALL i IN reqIds.FIRST .. reqIds.LAST
      DELETE FROM Id2Type WHERE id IN (reqIds(i), clientIds(i), srIds(i));
    FORALL i IN clientIds.FIRST .. clientIds.LAST
      DELETE FROM Client WHERE id = clientIds(i);
    FORALL i IN srIds.FIRST .. srIds.LAST
      DELETE FROM SubRequest WHERE id = srIds(i);
    CLOSE c;
    -- Release locks
    COMMIT;
  END LOOP;
  -- Delete all data related to the filesystem from the draining diskcopy table
  DELETE FROM DrainingDiskCopy
   WHERE fileSystem = :old.fileSystem;
END;
/


/* Function to convert seconds into a time string using the format:
 * DD-MON-YYYY HH24:MI:SS. If seconds is not defined then the current time
 * will be returned.
 */
CREATE OR REPLACE FUNCTION getTimeString
(seconds IN NUMBER DEFAULT NULL,
 format  IN VARCHAR2 DEFAULT 'DD-MON-YYYY HH24:MI:SS')
RETURN VARCHAR2 AS
BEGIN
  RETURN (to_char(to_date('01-JAN-1970', 'DD-MON-YYYY') +
          nvl(seconds, getTime()) / (60 * 60 * 24), format));
END;
/


/* Function used to convert bytes into a human readable string using powers
 * of 1000 (SI units). E.g 2000 = 2k.
 */
CREATE OR REPLACE FUNCTION sizeOfFmtSI(bytes IN NUMBER) RETURN VARCHAR2 AS
  unit VARCHAR2(2);
  num  NUMBER := bytes;
BEGIN
  IF bytes > 1000000000000000 THEN  -- PETA
    num := trunc(bytes / 1000000000000000, 2);
    unit := 'P';
  ELSIF bytes > 1000000000000 THEN  -- TERA
    num := trunc(bytes / 1000000000000, 2);
    unit := 'T';
  ELSIF bytes > 1000000000 THEN     -- GIGA
    num := trunc(bytes / 1000000000, 2);
    unit := 'G';
  ELSIF bytes > 1000000 THEN        -- MEGA
    num := trunc(bytes / 1000000, 2);
    unit := 'M';
  ELSIF bytes > 1000 THEN           -- KILO
    num := trunc(bytes / 1000, 2);
    unit := 'k';
  END IF;

  RETURN (to_char(num, 'FM99900.009') || unit);
END;
/


/* Function used to covert elapsed time in seconds into a human readable
 * string. For example, 3600 seconds = '00 01:00:00'
 */
CREATE OR REPLACE FUNCTION getInterval(startTime IN NUMBER, endTime IN NUMBER)
RETURN VARCHAR2 AS
  elapsed NUMBER;
  ret     VARCHAR2(2048);
BEGIN
  -- If the elapsed time is negative or greater than 99 days return an error
  elapsed := ceil(endTime) - floor(startTime);
  IF elapsed > 86400 * 99 OR elapsed < 0 THEN
    RETURN '## ##:##:##';
  END IF;
  -- Convert the elapsed time in seconds to an interval string
  -- e.g. +000000011 13:46:40.
  ret := rtrim(numtodsinterval(elapsed, 'SECOND'), 0);
  -- Remove the trailing '.' and leading '+'
  ret := substr(ret, 2, length(ret) - 2);
  -- Remove the leading '0'
  RETURN substr(ret, + 8);
END;
/


/* SQL statement for the creation of the DrainingOverview view */
CREATE OR REPLACE VIEW DrainingOverview
AS
  SELECT DS.name DiskServer,
         FS.mountPoint MountPoint,
         -- Determine the status of the filesystem being drained. If it's in a
         -- PRODUCTION status then the status of the diskserver is taken.
         decode(
           decode(FS.status, 0, DS.status, FS.status),
                  0, 'PRODUCTION',
                  1, 'DRAINING',
                  2, 'DISABLED', 'UNKNOWN') FileSystemStatus,
         DFS.username Username,
         DFS.machine Machine,
         getTimeString(ceil(DFS.creationTime)) Created,
         DFS.maxTransfers MaxTransfers,
         DFS.totalFiles TotalFiles,
         sizeOfFmtSI(DFS.totalBytes) TotalSize,
         -- Translate the fileMask value to string
         decode(DFS.fileMask,
                0, 'STAGED',
                1, 'CANBEMIGR',
                2, 'ALL', 'UNKNOWN') FileMask,
         -- Translate the autoDelete value to a string
         decode(DFS.autoDelete, 0, 'NO', 1, 'YES', 'UNKNOWN') AutoDelete,
         -- The target service class
         SVC.name SvcClass,
         -- Determine the status of the draining process. If the last update
         -- time is more than 60 minutes ago the process is considered to be
         -- STALLED.
         decode(DFS.status, 0, 'CREATED', 1, 'INITIALIZING', 2,
           decode(sign((getTime() - 3600) - DFS.lastUpdateTime),
                  -1, 'RUNNING', 'STALLED'),
                   3, 'INTERRUPTED',
                   4, 'FAILED',
                   5, 'COMPLETED',
                   6, 'DELETING', 'UNKNOWN') Status,
         nvl(DDCS.filesRemaining, 0) FilesRemaining,
         sizeOfFmtSI(nvl(DDCS.bytesRemaining, 0)) SizeRemaining,
         nvl(DDCS.running, 0) Running,
         nvl(DDCS.failed, 0) Failed,
         -- Calculate how long the process for draining the filesystem has been
         -- running. If the process is in a CREATED or INITIALIZING status
         -- 00 00:00:00 will be returned.
         decode(DFS.status, 0, '00 00:00:00', 1, '00 00:00:00',
           decode(sign(DFS.status - 3), -1,
             getInterval(DFS.startTime, gettime()),
               getInterval(DFS.startTime, DFS.lastUpdateTime))) RunTime,
         -- Calculate how far the process has gotten as a percentage of the data
         -- already transferred. If the process is in a CREATED, INITIALIZING or
         -- DELETING status N/A will be returned.
         decode(DFS.status, 0, 'N/A', 1, 'N/A', 6, 'N/A',
           decode(DFS.totalBytes, 0, '100%',
             concat(to_char(
               floor(((DFS.totalBytes - nvl(DDCS.bytesRemaining, 0)) /
                       DFS.totalBytes) * 100)), '%'))) Progress,
         -- Calculate the estimated time to completion in seconds if the process
         -- is in a RUNNING status and more than 10% of the data has already by
         -- transferred.
         decode(DFS.status, 2,
           decode(sign((getTime() - 3600) - DFS.lastUpdateTime), -1,
             decode(sign((((DFS.totalBytes - nvl(DDCS.bytesRemaining, 0)) /
                            DFS.totalBytes) * 100) - 10), -1, 'N/A',
               getInterval(0, trunc(DDCS.bytesRemaining / ((DFS.totalBytes -
                           nvl(DDCS.bytesRemaining, 0)) /
                           (getTime() - DFS.startTime))))), 'N/A'), 'N/A') ETC
    FROM (
      SELECT fileSystem,
             max(decode(status, 3, nbFiles, 0)) Running,
             max(decode(status, 4, nbFiles, 0)) Failed,
             sum(nbFiles) FilesRemaining,
             sum(totalFileSize) BytesRemaining
        FROM DrainingDiskCopy_MV
       GROUP BY fileSystem
    ) DDCS
   RIGHT JOIN DrainingFileSystem DFS
      ON DFS.fileSystem = DDCS.fileSystem
   INNER JOIN FileSystem FS
      ON DFS.fileSystem = FS.id
   INNER JOIN DiskServer DS
      ON FS.diskServer = DS.id
   INNER JOIN SvcClass SVC
      ON DFS.svcClass = SVC.id
   ORDER BY DS.name, FS.mountPoint;


/* SQL statement for the creation of the DrainingFailures view */
CREATE OR REPLACE VIEW DrainingFailures
AS
  SELECT DS.name DiskServer,
  	 FS.mountPoint MountPoint,
         regexp_replace
           ((FS.MountPoint || '/' || DC.path),'(/){2,}','/') Path,
         nvl(DDC.comments, 'Unknown') Comments
    FROM DrainingDiskCopy DDC, DiskCopy DC, FileSystem FS, DiskServer DS
   WHERE DDC.diskCopy = DC.id
     AND decode(DDC.status, 4, DDC.status, NULL) = 4  -- FAILED
     AND DC.fileSystem = FS.id
     AND FS.diskServer = DS.id
   ORDER BY DS.name, FS.mountPoint, DC.path;


/* Procedure responsible for stopping the draining process for a diskserver
 * or filesystem. In no filesystem is specified then all filesystems
 * associated to the diskserver will be stopped.
 */
CREATE OR REPLACE PROCEDURE stopDraining(inNodeName   IN VARCHAR,
                                         inMountPoint IN VARCHAR2 DEFAULT NULL)
AS
  fsIds "numList";
  reqIds "numList";
BEGIN
  -- Check that the nodename and mountpoint input options are valid
  SELECT FileSystem.id BULK COLLECT INTO fsIds
    FROM FileSystem, DiskServer
   WHERE FileSystem.diskServer = DiskServer.id
     AND (FileSystem.mountPoint = inMountPoint
      OR inMountPoint IS NULL)
     AND DiskServer.name = inNodeName;
  IF fsIds.COUNT = 0 THEN
    IF inMountPoint IS NULL THEN
      raise_application_error
        (-20019, 'Diskserver does not exist or has no mountpoints');
    ELSE
      raise_application_error
        (-20015, 'Diskserver and mountpoint does not exist');
    END IF;
  END IF;
  -- Update the filesystem entries to DELETING. The drainManager job will
  -- finalize the deletion of the entry once all outstanding transfers have
  -- terminated.
  UPDATE DrainingFileSystem
     SET status = 6  -- DELETING
   WHERE fileSystem IN
     (SELECT /*+ CARDINALITY(fsIdTable 5) */ *
        FROM TABLE (fsIds) fsIdTable);
END;
/


/* Procedure responsible for starting the draining process for a diskserver
 * or filesystem. If no filesystem is specified then all filesystems
 * associated to the diskserver will be add to the list of filesystems to be
 * drained.
 */
CREATE OR REPLACE
PROCEDURE startDraining(inNodeName     IN VARCHAR,
                        inMountPoint   IN VARCHAR DEFAULT NULL,
                        inSvcClass     IN VARCHAR DEFAULT NULL,
                        inFileMask     IN NUMBER DEFAULT 1,
                        inAutoDelete   IN NUMBER DEFAULT 0,
                        inMaxTransfers IN NUMBER DEFAULT 50)
AS
  ret    NUMBER;
  fsIds  "numList";
  svcId  NUMBER;
  unused NUMBER;
  mntPnt VARCHAR2(2048);
BEGIN
  -- Check that the nodename and mountpoint input options are valid
  SELECT FileSystem.id BULK COLLECT INTO fsIds
    FROM FileSystem, DiskServer
   WHERE FileSystem.diskServer = Diskserver.id
     AND (FileSystem.mountPoint = inMountPoint
      OR inMountPoint IS NULL)
     AND DiskServer.name = inNodeName;
  IF fsIds.COUNT = 0 THEN
    IF inMountPoint IS NULL THEN
      raise_application_error
        (-20019, 'Diskserver does not exist or has no mountpoints');
    ELSE
      raise_application_error
        (-20015, 'Diskserver and mountpoint does not exist');
    END IF;
  END IF;
  -- Loop over the mountpoints extracted above and validate that the service
  -- class and status of the mountpoint are correct.
  FOR i IN fsIds.FIRST .. fsIds.LAST
  LOOP
    SELECT mountPoint INTO mntPnt
      FROM FileSystem WHERE id = fsIds(i);
    -- If no service class option is defined attempt to automatically find one.
    -- Note: this only works if the filesystem belongs to one and only one
    -- service class. If this is not the case the user must explicit provide
    -- one on the command line using the --svcclass option to the
    -- draindiskserver tool.
    IF inSvcClass IS NULL THEN
      BEGIN
        SELECT SvcClass.id INTO svcId
          FROM FileSystem, DiskPool2SvcClass, SvcClass
         WHERE FileSystem.diskPool = DiskPool2SvcClass.parent
           AND DiskPool2SvcClass.child = SvcClass.id
           AND FileSystem.id = fsIds(i);
      EXCEPTION
        WHEN TOO_MANY_ROWS THEN
          raise_application_error
            (-20101, 'Mountpoint: '||mntPnt||' belongs to multiple service
                      classes, please specify which service class to use, using
                      the --svcclass option');
        WHEN NO_DATA_FOUND THEN
          raise_application_error
            (-20120, 'Mountpoint: '||mntPnt||' does not belong to any
                      service class');
      END;
    ELSE
      -- Check if the user supplied service class name exists
      ret := checkForValidSvcClass(inSvcClass, 0, 1);
      -- Check that the mountpoint belongs to the service class provided by the
      -- user. This check is necessary as we do not support the draining of a
      -- mountpoint to a service class which it is not already a member of.
      BEGIN
        SELECT SvcClass.id INTO SvcId
          FROM FileSystem, DiskPool2SvcClass, SvcClass
         WHERE FileSystem.diskPool = DiskPool2SvcClass.parent
           AND DiskPool2SvcClass.child = SvcClass.id
           AND FileSystem.id = fsIds(i)
           AND SvcClass.name = inSvcClass;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        raise_application_error
          (-20117, 'Mountpoint: '||mntPnt||' does not belong to the '''||
                    inSvcClass||''' service class');
      END;
    END IF;
    -- If the mountpoint is not in a DRAINING status then the draining process
    -- cannot proceed.
    BEGIN
      SELECT FS.diskPool INTO unused
        FROM FileSystem FS, DiskServer DS
       WHERE FS.diskServer = DS.id
         AND FS.id = fsIds(i)
         AND decode(FS.status, 0, DS.status, FS.status) = 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      raise_application_error
        (-20116, 'Mountpoint: '||mntPnt||' is not in a DRAINING state');
    END;
    -- Check to see if the mountpoint is already being drained. Note: we do not
    -- allow the resubmission of a mountpoint without a prior DELETION unless
    -- the previous process was not in a FAILED or COMPLETED state
    BEGIN
      SELECT fileSystem INTO unused
        FROM DrainingFileSystem
       WHERE fileSystem = fsIds(i)
         AND status NOT IN (4, 5);  -- FAILED, COMPLETED
      raise_application_error
        (-20118, 'Mountpoint: '||mntPnt||' is already being drained');
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Cleanup
      DELETE FROM DrainingFileSystem
       WHERE fileSystem = fsIds(i);
      -- Insert the new mountpoint into the list of those to be drained. The
      -- drainManager job will later pick it up and start the draining process.
      INSERT INTO DrainingFileSystem
        (userName, machine, creationTime, fileSystem, svcClass, autoDelete,
         fileMask, maxTransfers)
      VALUES
        (-- For the time being the draindiskserver command is distributed with
         -- the castor-dbtools package and uses the /etc/castor/ORASTAGERCONFIG
         -- password to connect to the database. As the file is only readable
         -- by the root user and the st group, the OS_USER will either be root
         -- or stage. This is not very interesting!!
         sys_context('USERENV', 'OS_USER'),
         sys_context('USERENV', 'HOST'),
         getTime(), fsIds(i), svcId, inAutoDelete, inFileMask, inMaxTransfers);
    END;
  END LOOP;
END;
/


/* Procedure responsible for processing the snapshot of files that need to be
 * replicated in order to drain a filesystem.
 */
CREATE OR REPLACE
PROCEDURE drainFileSystem(fsId IN NUMBER)
AS
  unused     NUMBER;
  res        NUMBER;
  dcId       NUMBER;
  cfId       NUMBER;
  svcId      NUMBER;
  ouid       NUMBER;
  ogid       NUMBER;
  autoDelete NUMBER;
BEGIN
  -- As this procedure is called recursively we release the previous calls
  -- locks. This prevents the procedure from locking too many castorfile
  -- entries which could result in service degradation.
  COMMIT;
  -- Update the filesystems entry in the DrainingFileSystem table to indicate
  -- that activity is ongoing. If no entry exist then the filesystem is not
  -- under the control of the draining logic.
  svcId := 0;
  UPDATE DrainingFileSystem
     SET lastUpdateTime = getTime()
   WHERE fileSystem = fsId
     AND status = 2  -- RUNNING
  RETURNING svcclass, autoDelete INTO svcId, autoDelete;
  IF svcId = 0 THEN
    RETURN;  -- Do nothing
  END IF;
  -- Extract the next diskcopy to be processed. Note: this is identical to the
  -- way that subrequests are picked up in the SubRequestToDo procedure. The N
  -- levels of SELECTS and hints allow us to process the entries in the
  -- DrainingDiskCopy snapshot in an ordered way.
  dcId := 0;
  UPDATE DrainingDiskCopy
     SET status = 2  -- PROCESSING
   WHERE diskCopy = (
     SELECT diskCopy FROM (
       SELECT /*+ INDEX(DC I_DrainingDCs_PC) */ DDC.diskCopy
         FROM DrainingDiskCopy DDC
        WHERE DDC.fileSystem = fsId
          AND DDC.status IN (0, 1)  -- CREATED, RESTARTED
        ORDER BY DDC.priority DESC, DDC.creationTime ASC)
     WHERE rownum < 2)
  RETURNING diskCopy INTO dcId;
  IF dcId = 0 THEN
    -- If there are no transfers outstanding related to the draining process we
    -- can update the filesystem entry in the DrainingFileSystem table for the
    -- last time.
    UPDATE DrainingFileSystem
       SET status = decode((SELECT count(*)
                              FROM DrainingDiskCopy
                             WHERE status = 4  -- FAILED
                               AND fileSystem = fsId), 0, 5, 4),
           lastUpdateTime = getTime()
     WHERE fileSystem = fsId
       -- Check to make sure there are no transfers outstanding.
       AND (SELECT count(*) FROM DrainingDiskCopy
             WHERE status = 3  -- RUNNING
               AND fileSystem = fsId) = 0;
    RETURN;  -- Out of work!
  END IF;
  -- The DrainingDiskCopy table essentially contains a snapshot of the work
  -- that needs to be done at the time the draining process was initiated for a
  -- filesystem. As a result, the diskcopy id previously extracted may no longer
  -- be valid as the snapshot information is outdated. Here we simply verify
  -- that the diskcopy is still what we expected.
  BEGIN
    -- Determine the castorfile id
    SELECT castorFile INTO cfId FROM DiskCopy WHERE id = dcId;
    -- Lock the castorfile
    SELECT id INTO cfId
      FROM CastorFile WHERE id = cfId FOR UPDATE;
    -- Check that the status of the diskcopy is STAGED or CANBEMIGR. If the
    -- diskcopy is not in one of these states then it is no longer a candidate
    -- to be replicated. For example, it may have been deleted, resulting in the
    -- diskcopy being invalidated and dropped by the garbage collector.
    SELECT ownerUid, ownerGid INTO ouid, ogid
      FROM DiskCopy
     WHERE id = dcId
       AND status IN (0, 10);  -- STAGED, CANBEMIGR
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- The diskcopy no longer exists so we delete it from the snapshot of files
    -- to be processed.
    DELETE FROM DrainingDiskCopy
      WHERE diskCopy = dcId AND fileSystem = fsId;
    drainFileSystem(fsId);
    RETURN;  -- No replication required
  END;
  -- Check to see if there is already an outstanding request to replicate the
  -- castorfile to the target service class. If so, we wait on that replication
  -- to complete. This avoids having multiple requests to replicate the same
  -- file to the same target service class multiple times.
  BEGIN
    SELECT DiskCopy.id INTO res
      FROM DiskCopy, StageDiskCopyReplicaRequest
     WHERE StageDiskCopyReplicaRequest.destDiskCopy = DiskCopy.id
       AND StageDiskCopyReplicaRequest.svcclass = svcId
       AND DiskCopy.castorFile = cfId
       AND DiskCopy.status = 1  -- WAITDISK2DISKCOPY
       AND rownum < 2;
    IF res > 0 THEN
      -- Wait on another replication to complete.
      UPDATE DrainingDiskCopy
         SET status = 3,  -- WAITD2D
             parent = res
       WHERE diskCopy = dcId AND fileSystem = fsId;
       RETURN;
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL;  -- No pending replications running
  END;
  -- Just because the file was listed in the snapshot doesn't mean that it must
  -- be replicated! Here we check to see if the file is available on another
  -- diskserver in the target service class and if enough copies of the file
  -- are available.
  --
  -- The decode logic used here essentially states:
  --   If replication on close is enabled and there are enough copies available
  --   to satisfy the maxReplicaNb on the service class, then do not replicate.
  -- Otherwise,
  --   If replication on close is disabled and a copy exists elsewhere, then do
  --   do not replicate. All other cases result in triggering replication.
  --
  -- Notes:
  -- - The check to see if we have enough copies online when replication on
  --   close is enabled will effectively rebalance the pool!
  SELECT decode(replicateOnClose, 1,
         decode((available - maxReplicaNb) -
                 abs(available - maxReplicaNb), 0, 0, 1),
         decode(sign(available), 1, 0, 1)) INTO res
    FROM (
      SELECT count(*) available
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
       WHERE DiskCopy.fileSystem = FileSystem.id
         AND DiskCopy.castorFile = cfId
         AND DiskCopy.id != dcId
         AND DiskCopy.status IN (0, 10)  -- STAGED, CANBEMIGR
         AND FileSystem.diskPool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = svcId
         AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
         AND FileSystem.diskServer = DiskServer.id
         AND DiskServer.status IN (0, 1)  -- PRODUCTION, DRAINING
     ) results, SvcClass
   WHERE SvcClass.id = svcId;
  IF res = 0 THEN
    -- No replication is required, there are enough copies of the file to
    -- satisfy the requirements of the target service class.
    IF autoDelete = 1 THEN
      -- Invalidate the diskcopy so that it can be garbage collected.
      UPDATE DiskCopy
         SET status = 7,  -- INVALID
             gctype = 3   -- Draining filesystem
       WHERE id = dcId AND fileSystem = fsId;
    END IF;
    -- Delete the diskcopy from the snapshot as no action is required.
    DELETE FROM DrainingDiskCopy
     WHERE diskCopy = dcId AND fileSystem = fsId;
    drainFileSystem(fsId);
    RETURN;
  END IF;
  -- If we have attempted to replicate the file more than 10 times already then
  -- give up! The error will be exposed later to an administrator for manual
  -- corrective action.
  SELECT count(*) INTO res
    FROM StageDiskCopyReplicaRequest R, SubRequest
   WHERE SubRequest.request = R.id
     AND R.sourceDiskCopy = dcId
     AND SubRequest.status = 9; -- FAILED_FINISHED
  IF res >= 10 THEN
    UPDATE DrainingDiskCopy
       SET status = 4,
           comments = 'Maximum number of attempts exceeded'
     WHERE diskCopy = dcId AND fileSystem = fsId;
    drainFileSystem(fsId);
    RETURN;  -- Try again
  END IF;
  -- Trigger a replication request for the file
  createDiskCopyReplicaRequest(0, dcId, svcId, svcId, ouid, ogid);
  -- Update the status of the file
  UPDATE DrainingDiskCopy SET status = 3  -- WAITD2D
   WHERE diskCopy = dcId AND fileSystem = fsId;
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
      JOB_ACTION      => 'BEGIN DrainManager(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 5/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Database job to manage the draining process');
END;
/


/* Procedure responsible for managing the draining process */
CREATE OR REPLACE PROCEDURE drainManager AS
  fsIds  "numList";
  reqIds "numList";
  unused NUMBER;
BEGIN
  -- Delete the filesystems which are:
  --  A) in a DELETING state and have no transfers pending in the scheduler.
  --  B) are COMPLETED and older the 7 days.
  DELETE FROM DrainingFileSystem
   WHERE (NOT EXISTS (SELECT 'x' FROM DrainingDiskCopy_MV
                       WHERE fileSystem = DrainingFileSystem.fileSystem
                         AND status = 3)  -- WAITD2D
          AND status = 6)  -- DELETING
      OR (status = 5 AND lastUpdateTime < getTime() - (7 * 86400));
  -- Release locks
  COMMIT;
  -- Process filesystems which in a CREATED state
  UPDATE DrainingFileSystem
     SET status = 1   -- INITIALIZING
   WHERE status = 0;  -- CREATED
  -- Release locks, this isn't really necessary but its done so that the user
  -- gets feedback when listing the filesystems which are being drained. If we
  -- dont do this, filesystems could remain in a CREATED state for a very very
  -- long time.
  COMMIT;
  -- Loop over filesystems in t
  FOR fs IN (SELECT fileSystem
               FROM DrainingFileSystem
              WHERE status = 1) -- INITIALIZAING
  LOOP
    BEGIN
      -- Acquire a lock on the DrainingFileSystem entry. We need to this as
      -- there are commits inside the loop!
      SELECT fileSystem INTO unused
        FROM DrainingFileSystem
       WHERE status = 1  -- INITIALIZING
         AND fileSystem = fs.fileSystem
         FOR UPDATE;
      -- Create the DrainingDiskCopy snapshot
      INSERT /*+ APPEND */ INTO DrainingDiskCopy
        (fileSystem, diskCopy, creationTime, priority, fileSize)
          (SELECT /*+ index(DC I_DiskCopy_FileSystem) */
                  DC.fileSystem, DC.id, DC.creationTime, DC.status,
                  DC.diskCopySize
             FROM DiskCopy DC, DrainingFileSystem DFS
            WHERE DFS.fileSystem = DC.fileSystem
              AND DFS.fileSystem = fs.fileSystem
              AND ((DC.status = 0         AND DFS.fileMask = 0)    -- STAGED
               OR  (DC.status = decode(DC.status, 10, DC.status, NULL)
                    AND DFS.fileMask = 1)                          -- CANBEMIGR
               OR  (DC.status IN (0, 10)  AND DFS.fileMask = 2))); -- ALL
      -- Update the DrainingFileSystem counters
      FOR a IN (SELECT fileSystem, count(*) files, sum(DDC.fileSize) bytes
                  FROM DrainingDiskCopy DDC
                 WHERE DDC.fileSystem = fs.fileSystem
                   AND DDC.status = 0  -- CREATED
                 GROUP BY DDC.fileSystem)
      LOOP
        UPDATE DrainingFileSystem
           SET totalFiles = a.files,
               totalBytes = a.bytes
         WHERE fileSystem = a.fileSystem;
      END LOOP;
      -- Update the filesystem entry to RUNNING
      UPDATE DrainingFileSystem
         SET status = 2,  -- RUNNING
             startTime = getTime(),
             lastUpdateTime = getTime()
       WHERE fileSystem = fs.fileSystem;
      -- Start the process of draining the filesystems. For an explanation of
      -- the query refer to: "Creating N Copies of a Row":
      -- http://forums.oracle.com/forums/message.jspa?messageID=1953433#1953433
      FOR a IN ( SELECT fileSystem
                   FROM DrainingFileSystem
                  WHERE fileSystem = fs.fileSystem
                CONNECT BY CONNECT_BY_ROOT fileSystem = fileSystem
                    AND LEVEL <= maxTransfers
                    AND PRIOR sys_guid() IS NOT NULL
                  ORDER BY fileSystem)
      LOOP
        drainFileSystem(a.fileSystem);
      END LOOP;
      -- Release locks
      COMMIT;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Do nothing, by the time we came to process the filesystem it was no
      -- longer in an INITIALIZING state.
      NULL;
    END;
  END LOOP;
END;
/
