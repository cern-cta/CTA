/******************************************************************************
 *                 stager_2.1.14-14_to_2.1.14-14-1.sql
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
 * This script upgrades a CASTOR v2.1.14-14 STAGER database to v2.1.14-14-1
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  -- If we have encountered an error rollback any previously non committed
  -- operations. This prevents the UPDATE of the UpgradeLog from committing
  -- inconsistent data to the database.
  ROLLBACK;
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_14_2'
     AND release = '2_1_14_14_1'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'STAGER'
     AND release LIKE '2_1_14_14%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_14_1', 'TRANSPARENT');
COMMIT;

/* Schema changes go here */
/**************************/
DECLARE
  unused VARCHAR2(2048);
BEGIN
  SELECT trigger_name INTO UNUSED FROM user_triggers
   WHERE trigger_name = 'TR_SUBREQUEST_INFORMERROR';
  EXECUTE IMMEDIATE 'DROP TRIGGER tr_SubRequest_informError';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- already dropped, ignore
  NULL;
END;
/

DECLARE
  unused VARCHAR2(2048);
BEGIN
  SELECT value INTO unused FROM CastorConfig
   WHERE key = 'MaxDataSchedD2dPerDrain';
EXCEPTION WHEN NO_DATA_FOUND THEN
  INSERT INTO CastorConfig
   VALUES ('Draining', 'MaxDataSchedD2dPerDrain', '10000000000', 'The maximum amount of data that each draining job should send to the scheduler in one go.');
END;
/

/* Update and revalidation of PL-SQL code */
/******************************************/
/* Search and delete old diskCopies in bad states */
CREATE OR REPLACE PROCEDURE deleteFailedDiskCopies(timeOut IN NUMBER) AS
  dcIds "numList";
  cfIds "numList";
BEGIN
  LOOP
    -- select INVALID diskcopies without filesystem (they can exist after a
    -- stageRm that came before the diskcopy had been created on disk) and ALL FAILED
    -- ones (coming from failed recalls or failed removals from the GC daemon).
    -- Note that we don't select INVALID diskcopies from recreation of files
    -- because they are taken by the standard GC as they physically exist on disk.
    -- go only for 1000 at a time and retry if the limit was reached
    SELECT id
      BULK COLLECT INTO dcIds
      FROM DiskCopy
     WHERE (status = 4 OR (status = 7 AND fileSystem = 0))
       AND creationTime < getTime() - timeOut
       AND ROWNUM <= 1000;
    SELECT /*+ INDEX(DC PK_DiskCopy_ID) */ UNIQUE castorFile
      BULK COLLECT INTO cfIds
      FROM DiskCopy DC
     WHERE id IN (SELECT /*+ CARDINALITY(ids 5) */ * FROM TABLE(dcIds) ids);
    -- drop the DiskCopies - not in bulk because of the constraint violation check
    FOR i IN 1 .. dcIds.COUNT LOOP
      DECLARE
        CONSTRAINT_VIOLATED EXCEPTION;
        PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -2292);
      BEGIN
        DELETE FROM DiskCopy WHERE id = dcIds(i);
      EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
        IF sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_DRAININGERRORS_CASTORFILE) violated%' OR
           sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_DISK2DISKCOPYJOB_SRCDCID) violated%' THEN
          -- Ignore the deletion, this diskcopy was either implied in a draining action and
          -- the draining error is still around or it is the source of another d2d copy that
          -- is not over
          NULL;
        ELSE
          -- Any other constraint violation is an error
          RAISE;
        END IF;
      END;
    END LOOP;
    COMMIT;
    -- maybe delete the CastorFiles if nothing is left for them
    FOR i IN 1 .. cfIds.COUNT LOOP
      deleteCastorFile(cfIds(i));
    END LOOP;
    COMMIT;
    -- exit if we did less than 1000
    IF dcIds.COUNT < 1000 THEN EXIT; END IF;
  END LOOP;
END;
/

/*
 * PL/SQL method implementing filesDeleted
 * Note that we don't increase the freespace of the fileSystem.
 * This is done by the monitoring daemon, that knows the
 * exact amount of free space.
 * dcIds gives the list of diskcopies to delete.
 * fileIds returns the list of castor files to be removed
 * from the name server
 */
CREATE OR REPLACE PROCEDURE filesDeletedProc
(dcIds IN castor."cnumList",
 fileIds OUT castor.FileList_Cur) AS
  fid NUMBER;
  fc NUMBER;
  nsh VARCHAR2(2048);
  nb INTEGER;
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- List the castorfiles to be cleaned up afterwards
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      INSERT INTO FilesDeletedProcHelper (cfId, dcId) (
        SELECT castorFile, id FROM DiskCopy
         WHERE id = dcIds(i));
    -- Use a normal loop to clean castorFiles. Note: We order the list to
    -- prevent a deadlock
    FOR cf IN (SELECT cfId, dcId
                 FROM filesDeletedProcHelper
                ORDER BY cfId ASC) LOOP
      DECLARE
        CONSTRAINT_VIOLATED EXCEPTION;
        PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -2292);
      BEGIN
        -- Get data and lock the castorFile
        SELECT fileId, nsHost, fileClass
          INTO fid, nsh, fc
          FROM CastorFile
         WHERE id = cf.cfId FOR UPDATE;
        -- delete the original diskcopy to be dropped
        DELETE FROM DiskCopy WHERE id = cf.dcId;
        -- Cleanup:
        -- See whether it has any other DiskCopy or any new Recall request:
        -- if so, skip the rest
        SELECT count(*) INTO nb FROM DiskCopy
         WHERE castorFile = cf.cfId;
        IF nb > 0 THEN
          CONTINUE;
        END IF;
        SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
          FROM SubRequest
         WHERE castorFile = cf.cfId
           AND status = dconst.SUBREQUEST_WAITTAPERECALL;
        IF nb > 0 THEN
          CONTINUE;
        END IF;
        -- Now check for Disk2DiskCopy jobs
        SELECT /*+ INDEX(I_Disk2DiskCopyJob_cfId) */ count(*) INTO nb FROM Disk2DiskCopyJob
         WHERE castorFile = cf.cfId;
        IF nb > 0 THEN
          CONTINUE;
        END IF;
        -- Nothing found, check for any other subrequests
        SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
          FROM SubRequest
         WHERE castorFile = cf.cfId
           AND status IN (1, 2, 3, 4, 5, 6, 7, 10, 12, 13);  -- all but START, FINISHED, FAILED_FINISHED, ARCHIVED
        IF nb = 0 THEN
          -- Nothing left, delete the CastorFile
          DELETE FROM CastorFile WHERE id = cf.cfId;
        ELSE
          -- Fail existing subrequests for this file
          UPDATE /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile)*/ SubRequest
             SET status = dconst.SUBREQUEST_FAILED
           WHERE castorFile = cf.cfId
             AND status IN (1, 2, 3, 4, 5, 6, 12, 13);  -- same as above
        END IF;
        -- Check whether this file potentially had copies on tape
        SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
        IF nb = 0 THEN
          -- This castorfile was created with no copy on tape
          -- So removing it from the stager means erasing
          -- it completely. We should thus also remove it
          -- from the name server
          INSERT INTO FilesDeletedProcOutput (fileId, nsHost) VALUES (fid, nsh);
        END IF;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- Ignore, this means that the castorFile did not exist.
        -- There is thus no way to find out whether to remove the
        -- file from the nameserver. For safety, we thus keep it
        NULL;
      WHEN CONSTRAINT_VIOLATED THEN
        IF sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_%_CASTORFILE) violated%' THEN
          -- Ignore the deletion, probably some draining/rebalancing/recall activity created
          -- a new Disk2DiskCopyJob/RecallJob entity while we were attempting to drop the CastorFile
          NULL;
        ELSE
          -- Any other constraint violation is an error
          RAISE;
        END IF;
      END;
    END LOOP;
  END IF;
  OPEN fileIds FOR
    SELECT fileId, nsHost FROM FilesDeletedProcOutput;
END;
/

/* handle the creation of the Disk2DiskCopyJobs for the running drainingJobs */
CREATE OR REPLACE PROCEDURE drainRunner AS
  varNbRunningJobs INTEGER;
  varDataRunningJobs INTEGER;
  varMaxNbOfSchedD2dPerDrain INTEGER;
  varMaxDataOfSchedD2dPerDrain INTEGER;
  varUnused INTEGER;
BEGIN
  -- get maxNbOfSchedD2dPerDrain and MaxDataOfSchedD2dPerDrain
  varMaxNbOfSchedD2dPerDrain := TO_NUMBER(getConfigOption('Draining', 'MaxNbSchedD2dPerDrain', '1000'));
  varMaxDataOfSchedD2dPerDrain := TO_NUMBER(getConfigOption('Draining', 'MaxDataSchedD2dPerDrain', '10000000000')); -- 10 GB
  -- loop over draining jobs
  FOR dj IN (SELECT id, fileSystem, svcClass, fileMask, euid, egid
               FROM DrainingJob WHERE status = dconst.DRAININGJOB_RUNNING) LOOP
    DECLARE
      CONSTRAINT_VIOLATED EXCEPTION;
      PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -2292);
    BEGIN
      BEGIN
        -- lock the draining job first
        SELECT id INTO varUnused FROM DrainingJob WHERE id = dj.id FOR UPDATE;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- it was (already!) canceled, go to the next one
        CONTINUE;
      END;
      -- check how many disk2DiskCopyJobs are already running for this draining job
      SELECT count(*), nvl(sum(CastorFile.fileSize), 0) INTO varNbRunningJobs, varDataRunningJobs
        FROM Disk2DiskCopyJob, CastorFile
       WHERE Disk2DiskCopyJob.drainingJob = dj.id
         AND CastorFile.id = Disk2DiskCopyJob.castorFile;
      -- Loop over the creation of Disk2DiskCopyJobs. Select max 1000 files, taking running
      -- ones into account. Also take the most important jobs first
      logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DRAINING_REFILL, 0, '', 'stagerd',
               'svcClass=' || getSvcClassName(dj.svcClass) || ' DrainReq=' ||
               TO_CHAR(dj.id) || ' MaxNewJobsCount=' || TO_CHAR(varMaxNbOfSchedD2dPerDrain-varNbRunningJobs));
      FOR F IN (SELECT * FROM
                 (SELECT CastorFile.id cfId, Castorfile.nsOpenTime, DiskCopy.id dcId, CastorFile.fileSize
                    FROM DiskCopy, CastorFile
                   WHERE DiskCopy.fileSystem = dj.fileSystem
                     AND CastorFile.id = DiskCopy.castorFile
                     AND ((dj.fileMask = dconst.DRAIN_FILEMASK_NOTONTAPE AND
                           CastorFile.tapeStatus IN (dconst.CASTORFILE_NOTONTAPE, dconst.CASTORFILE_DISKONLY)) OR
                          (dj.fileMask = dconst.DRAIN_FILEMASK_ALL))
                     AND DiskCopy.status = dconst.DISKCOPY_VALID
                     AND NOT EXISTS (SELECT 1 FROM DrainingErrors WHERE castorFile = CastorFile.id AND drainingJob = dj.id)
                     -- don't recreate disk-to-disk copy jobs for the ones already done in previous rounds
                     AND NOT EXISTS (SELECT 1 FROM Disk2DiskCopyJob WHERE castorFile = CastorFile.id AND drainingJob = dj.id)
                   ORDER BY DiskCopy.importance DESC)
                 WHERE ROWNUM <= varMaxNbOfSchedD2dPerDrain-varNbRunningJobs) LOOP
        -- Do not schedule more that varMaxAmountOfSchedD2dPerDrain
        IF varDataRunningJobs <= varMaxDataOfSchedD2dPerDrain THEN
          createDisk2DiskCopyJob(F.cfId, F.nsOpenTime, dj.svcClass, dj.euid, dj.egid,
                                 dconst.REPLICATIONTYPE_DRAINING, F.dcId, TRUE, dj.id, FALSE);
          varDataRunningJobs := varDataRunningJobs + F.fileSize;
        ELSE
          -- enough data amount, we stop scheduling
          EXIT;
        END IF;
      END LOOP;
      UPDATE DrainingJob
         SET lastModificationTime = getTime()
       WHERE id = dj.id;
      COMMIT;
    EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- check that the constraint violated is due to deletion of the drainingJob
      IF sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_DISK2DISKCOPYJOB_DRAINJOB) violated%' THEN
        -- give up with this DrainingJob as it was canceled
        ROLLBACK;
      ELSE
        RAISE;
      END IF;
    END;
  END LOOP;
END;
/

CREATE OR REPLACE PROCEDURE storeReports
(inStrParams IN castor."strList",
 inNumParams IN castor."cnumList") AS
 varDsId NUMBER;
 varFsId NUMBER;
 varHeartbeatTimeout NUMBER;
 emptyReport BOOLEAN := False;
BEGIN
  -- quick check of the vector lengths
  IF MOD(inStrParams.COUNT,2) != 0 THEN
    IF inStrParams.COUNT = 1 AND inStrParams(1) = 'Empty' THEN
      -- work around the "PLS-00418: array bind type must match PL/SQL table row type"
      -- error launched by Oracle when empty arrays are passed as parameters
      emptyReport := True;
    ELSE
      RAISE_APPLICATION_ERROR (-20125, 'Invalid call to storeReports : ' ||
                                       '1st vector has odd number of elements (' ||
                                       TO_CHAR(inStrParams.COUNT) || ')');
    END IF;
  END IF;
  IF MOD(inNumParams.COUNT,8) != 0 AND NOT emptyReport THEN
    RAISE_APPLICATION_ERROR (-20125, 'Invalid call to storeReports : ' ||
                             '2nd vector has wrong number of elements (' ||
                             TO_CHAR(inNumParams.COUNT) || ' instead of ' ||
                             TO_CHAR(inStrParams.COUNT*4) || ')');
  END IF;
  IF NOT emptyReport THEN
    -- Go through the concerned filesystems
    FOR i IN 0 .. inStrParams.COUNT/2-1 LOOP
      -- update DiskServer
      varDsId := NULL;
      UPDATE DiskServer
         SET hwOnline = 1,
             lastHeartbeatTime = getTime()
       WHERE name = inStrParams(2*i+1)
      RETURNING id INTO varDsId;
      -- if it did not exist, create it
      IF varDsId IS NULL THEN
        INSERT INTO DiskServer (name, id, status, hwOnline, lastHeartbeatTime)
         VALUES (inStrParams(2*i+1), ids_seq.nextval, dconst.DISKSERVER_DISABLED, 1, getTime())
        RETURNING id INTO varDsId;
      END IF;
      -- update FileSystem
      varFsId := NULL;
      UPDATE FileSystem
         SET maxFreeSpace = inNumParams(8*i+1),
             minAllowedFreeSpace = inNumParams(8*i+2),
             totalSize = inNumParams(8*i+3),
             free = inNumParams(8*i+4),
             nbReadStreams = inNumParams(8*i+5),
             nbWriteStreams = inNumParams(8*i+6),
             nbRecallerStreams = inNumParams(8*i+7),
             nbMigratorStreams = inNumParams(8*i+8)
       WHERE diskServer=varDsId AND mountPoint=inStrParams(2*i+2)
      RETURNING id INTO varFsId;
      -- if it did not exist, create it
      IF varFsId IS NULL THEN
        INSERT INTO FileSystem (mountPoint, maxFreeSpace, minAllowedFreeSpace, totalSize, free,
                                nbReadStreams, nbWriteStreams, nbRecallerStreams, nbMigratorStreams,
                                id, diskPool, diskserver, status)
        VALUES (inStrParams(2*i+2), inNumParams(8*i+1), inNumParams(8*i+2), inNumParams(8*i+3),
                inNumParams(8*i+4), inNumParams(8*i+5), inNumParams(8*i+6), inNumParams(8*i+7),
                inNumParams(8*i+8), ids_seq.nextval, 0, varDsId, dconst.FILESYSTEM_DISABLED);
      END IF;
      -- commit diskServer by diskServer, otherwise multiple reports may deadlock each other
      COMMIT;
    END LOOP;
  END IF;

  -- now disable nodes that have too old reports
  varHeartbeatTimeout := TO_NUMBER(getConfigOption('DiskServer', 'HeartbeatTimeout', '180'));
  UPDATE DiskServer
     SET hwOnline = 0
   WHERE lastHeartbeatTime < getTime() - varHeartbeatTimeout
     AND hwOnline = 1;
END;
/

/* PL/SQL method to either force GC of the given diskCopies or delete them when the physical files behind have been lost */
CREATE OR REPLACE PROCEDURE deleteDiskCopies(inDcIds IN castor."cnumList", inFileIds IN castor."cnumList",
                                             inForce IN BOOLEAN, inDryRun IN BOOLEAN,
                                             outRes OUT castor.DiskCopyResult_Cur, outDiskPool OUT VARCHAR2) AS
  varNsHost VARCHAR2(100);
  varFileName VARCHAR2(2048);
  varCfId INTEGER;
  varNbRemaining INTEGER;
  varStatus INTEGER;
  varLogParams VARCHAR2(2048);
  varFileSize INTEGER;
BEGIN
  DELETE FROM DeleteDiskCopyHelper;
  -- Insert all data in bulk for efficiency reasons
  FORALL i IN inFileIds.FIRST .. inFileIds.LAST
    INSERT INTO DeleteDiskCopyHelper (dcId, fileId, fStatus, rc)
    VALUES (inDcIds(i), inFileIds(i), 'd', -1);
  -- And now gather all remote Nameserver statuses. This could not be
  -- incorporated in the previous query, because Oracle would give:
  -- PLS-00739: FORALL INSERT/UPDATE/DELETE not supported on remote tables.
  -- Note that files that are not found in the Nameserver remain with fStatus = 'd',
  -- which means they can be safely deleted: we're anticipating the NS synch.
  UPDATE DeleteDiskCopyHelper
     SET fStatus = '-'
   WHERE EXISTS (SELECT 1 FROM Cns_file_metadata@RemoteNS F
                  WHERE status = '-' AND F.fileId IN
                    (SELECT fileId FROM DeleteDiskCopyHelper));
  UPDATE DeleteDiskCopyHelper
     SET fStatus = 'm'
   WHERE EXISTS (SELECT 1 FROM Cns_file_metadata@RemoteNS F
                  WHERE status = 'm' AND F.fileId IN
                    (SELECT fileId FROM DeleteDiskCopyHelper));
  -- A better and more generic implementation would have been:
  -- UPDATE DeleteDiskCopyHelper H
  --    SET fStatus = nvl((SELECT F.status
  --                         FROM Cns_file_metadata@RemoteNS F
  --                        WHERE F.fileId = H.fileId), 'd');
  -- Unfortunately, that one is much less efficient as Oracle does not use
  -- the DB link in bulk, therefore making the query extremely slow (several mins)
  -- when handling large numbers of files (e.g. an entire mount point).
  COMMIT;
  FOR dc IN (SELECT dcId, fileId, fStatus FROM DeleteDiskCopyHelper) LOOP
    BEGIN
      -- get data and lock
      SELECT castorFile, DiskCopy.status, DiskCopy.diskCopySize, DiskPool.name
        INTO varCfId, varStatus, varFileSize, outDiskPool
        FROM DiskPool, FileSystem, DiskCopy
       WHERE DiskCopy.id = dc.dcId
         AND DiskCopy.fileSystem = FileSystem.id
         AND FileSystem.diskPool = DiskPool.id;
      SELECT nsHost, lastKnownFileName
        INTO varNsHost, varFileName
        FROM CastorFile
       WHERE id = varCfId
         AND fileId = dc.fileId
         FOR UPDATE;
      varLogParams := 'FileName="' || varFileName ||'" DiskPool="'|| outDiskPool
        ||'" fileSize='|| varFileSize ||' dcId='|| dc.dcId ||' status='
        || getObjStatusName('DiskCopy', 'status', varStatus);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- diskcopy not found in stager
      UPDATE DeleteDiskCopyHelper
         SET rc = dconst.DELDC_ENOENT
       WHERE dcId = dc.dcId;
      COMMIT;
      CONTINUE;
    END;
    -- count remaining ones
    SELECT count(*) INTO varNbRemaining FROM DiskCopy
     WHERE castorFile = varCfId
       AND status = dconst.DISKCOPY_VALID
       AND id != dc.dcId;
    -- and update their importance if needed (other copy exists and dropped one was valid)
    IF varNbRemaining > 0 AND varStatus = dconst.DISKCOPY_VALID AND (NOT inDryRun) THEN
      UPDATE DiskCopy SET importance = importance + 1
       WHERE castorFile = varCfId
         AND status = dconst.DISKCOPY_VALID;
    END IF;
    IF inForce THEN
      -- the physical diskcopy is deemed lost: delete the diskcopy entry
      -- and potentially drop dangling entities
      IF NOT inDryRun THEN
        DELETE FROM DiskCopy WHERE id = dc.dcId;
      END IF;
      IF varStatus = dconst.DISKCOPY_STAGEOUT THEN
        -- fail outstanding requests
        UPDATE SubRequest
           SET status = dconst.SUBREQUEST_FAILED,
               errorCode = serrno.SEINTERNAL,
               errorMessage = 'File got lost while being written to'
         WHERE diskCopy = dc.dcId
           AND status = dconst.SUBREQUEST_READY;
      END IF;
      -- was it the last active one?
      IF varNbRemaining = 0 THEN
        IF NOT inDryRun THEN
          -- yes, drop the (now bound to fail) migration job(s)
          deleteMigrationJobs(varCfId);
        END IF;
        -- check if the entire castorFile chain can be dropped
        IF NOT inDryRun THEN
          deleteCastorFile(varCfId);
        END IF;
        IF dc.fStatus = 'm' THEN
          -- file is on tape: let's recall it. This may potentially trigger a new migration
          UPDATE DeleteDiskCopyHelper
             SET rc = dconst.DELDC_RECALL
           WHERE dcId = dc.dcId;
          IF NOT inDryRun THEN
            logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_RECALL, dc.fileId, varNsHost, 'stagerd', varLogParams);
          END IF;
        ELSIF dc.fStatus = 'd' THEN
          -- file was dropped, report as if we have run a standard GC
          UPDATE DeleteDiskCopyHelper
             SET rc = dconst.DELDC_GC
           WHERE dcId = dc.dcId;
          IF NOT inDryRun THEN
            logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_GC, dc.fileId, varNsHost, 'stagerd', varLogParams);
          END IF;
        ELSE
          -- file is really lost, we'll remove the namespace entry afterwards
          UPDATE DeleteDiskCopyHelper
             SET rc = dconst.DELDC_LOST
           WHERE dcId = dc.dcId;
          IF NOT inDryRun THEN
            logToDLF(NULL, dlf.LVL_WARNING, dlf.DELETEDISKCOPY_LOST, dc.fileId, varNsHost, 'stagerd', varLogParams);
          END IF;
        END IF;
      ELSE
        -- it was not the last valid copy, replicate from another one
          UPDATE DeleteDiskCopyHelper
             SET rc = dconst.DELDC_REPLICATION
           WHERE dcId = dc.dcId;
        IF NOT inDryRun THEN
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_REPLICATION, dc.fileId, varNsHost, 'stagerd', varLogParams);
        END IF;
      END IF;
    ELSE
      -- similarly to stageRm, check that the deletion is allowed:
      -- basically only files on tape may be dropped in case no data loss is provoked,
      -- or files already dropped from the namespace. The rest is forbidden.
      IF (varStatus = dconst.DISKCOPY_VALID AND (varNbRemaining > 0 OR dc.fStatus = 'm' OR varFileSize = 0))
         OR dc.fStatus = 'd' THEN
        UPDATE DeleteDiskCopyHelper
           SET rc = dconst.DELDC_GC
         WHERE dcId = dc.dcId;
        IF NOT inDryRun THEN
          UPDATE DiskCopy
             SET status = dconst.DISKCOPY_INVALID, gcType = dconst.GCTYPE_ADMIN
           WHERE id = dc.dcId;
           logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_GC, dc.fileId, varNsHost, 'stagerd', varLogParams);
        END IF;
      ELSE
        -- nothing is done, just record no-action
        UPDATE DeleteDiskCopyHelper
           SET rc = dconst.DELDC_NOOP
         WHERE dcId = dc.dcId;
        IF NOT inDryRun THEN
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_NOOP, dc.fileId, varNsHost, 'stagerd', varLogParams);
        END IF;
        COMMIT;
        CONTINUE;
      END IF;
    END IF;
    COMMIT;   -- release locks file by file
  END LOOP;
  -- return back all results for the python script to post-process them,
  -- including performing all required actions
  OPEN outRes FOR
    SELECT dcId, fileId, rc FROM DeleteDiskCopyHelper;
END;
/


/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_14_14_1';
COMMIT;
