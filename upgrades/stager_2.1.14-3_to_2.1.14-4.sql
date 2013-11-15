/******************************************************************************
 *                 stager_2.1.14-3_to_2.1.14-4.sql
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
 * This script upgrades a CASTOR v2.1.14-2 STAGER database to v2.1.14-3
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
     AND release = '2_1_14_4'
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
     AND release LIKE '2_1_14_3%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_4', 'TRANSPARENT');
COMMIT;

/* Job management */
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs)
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name, force=>TRUE);
    END IF;
    -- Schedule the start date of the job to 15 minutes from now. This
    -- basically pauses the job for 15 minutes so that the upgrade can
    -- go through as quickly as possible.
    dbms_scheduler.set_attribute(a.job_name, 'START_DATE', SYSDATE + 15/1440);
  END LOOP;
END;
/

/* Fix bad contraint on Recalljob table */
ALTER TABLE RecallJob drop constraint CK_RecallJob_Status;
ALTER TABLE RecallJob
  ADD CONSTRAINT CK_RecallJob_Status
  CHECK (status IN (1, 2, 3));

/* drop trigger replaced by direct calls */
DROP TRIGGER tr_SubRequest_informRestart;


/* Revalidate changed PL/SQL code */

/**
 * Package containing the definition of all DLF levels and messages logged from the SQL-to-DLF API
 */
CREATE OR REPLACE PACKAGE dlf
AS
  /* message levels */
  LVL_EMERGENCY  CONSTANT PLS_INTEGER := 0; /* LOG_EMERG   System is unusable */
  LVL_ALERT      CONSTANT PLS_INTEGER := 1; /* LOG_ALERT   Action must be taken immediately */
  LVL_CRIT       CONSTANT PLS_INTEGER := 2; /* LOG_CRIT    Critical conditions */
  LVL_ERROR      CONSTANT PLS_INTEGER := 3; /* LOG_ERR     Error conditions */
  LVL_WARNING    CONSTANT PLS_INTEGER := 4; /* LOG_WARNING Warning conditions */
  LVL_NOTICE     CONSTANT PLS_INTEGER := 5; /* LOG_NOTICE  Normal but significant condition */
  LVL_USER_ERROR CONSTANT PLS_INTEGER := 5; /* LOG_NOTICE  Normal but significant condition */
  LVL_AUTH       CONSTANT PLS_INTEGER := 5; /* LOG_NOTICE  Normal but significant condition */
  LVL_SECURITY   CONSTANT PLS_INTEGER := 5; /* LOG_NOTICE  Normal but significant condition */
  LVL_SYSTEM     CONSTANT PLS_INTEGER := 6; /* LOG_INFO    Informational */
  LVL_DEBUG      CONSTANT PLS_INTEGER := 7; /* LOG_DEBUG   Debug-level messages */

  /* messages */
  FILE_DROPPED_BY_CLEANING     CONSTANT VARCHAR2(2048) := 'File was dropped by internal cleaning';
  PUTDONE_ENFORCED_BY_CLEANING CONSTANT VARCHAR2(2048) := 'PutDone enforced by internal cleaning';
  
  DBJOB_UNEXPECTED_EXCEPTION   CONSTANT VARCHAR2(2048) := 'Unexpected exception caught in DB job';

  MIGMOUNT_NO_FILE             CONSTANT VARCHAR2(2048) := 'startMigrationMounts: failed migration mount creation due to lack of files';
  MIGMOUNT_AGE_NO_FILE         CONSTANT VARCHAR2(2048) := 'startMigrationMounts: failed migration mount creation base on age due to lack of files';
  MIGMOUNT_NEW_MOUNT           CONSTANT VARCHAR2(2048) := 'startMigrationMounts: created new migration mount';
  MIGMOUNT_NEW_MOUNT_AGE       CONSTANT VARCHAR2(2048) := 'startMigrationMounts: created new migration mount based on age';
  MIGMOUNT_NOACTION            CONSTANT VARCHAR2(2048) := 'startMigrationMounts: no need for new migration mount';

  RECMOUNT_NEW_MOUNT           CONSTANT VARCHAR2(2048) := 'startRecallMounts: created new recall mount';
  RECMOUNT_NOACTION_NODRIVE    CONSTANT VARCHAR2(2048) := 'startRecallMounts: not allowed to start new recall mount. Maximum nb of drives has been reached';
  RECMOUNT_NOACTION_NOCAND     CONSTANT VARCHAR2(2048) := 'startRecallMounts: no candidate found for a mount';

  RECALL_FOUND_ONGOING_RECALL  CONSTANT VARCHAR2(2048) := 'createRecallCandidate: found already running recall';
  RECALL_UNKNOWN_NS_ERROR      CONSTANT VARCHAR2(2048) := 'createRecallCandidate: error when retrieving segments from namespace';
  RECALL_NO_SEG_FOUND          CONSTANT VARCHAR2(2048) := 'createRecallCandidate: no valid segment to recall found';
  RECALL_NO_SEG_FOUND_AT_ALL   CONSTANT VARCHAR2(2048) := 'createRecallCandidate: no segment found for this file. File is probably lost';
  RECALL_INVALID_SEGMENT       CONSTANT VARCHAR2(2048) := 'createRecallCandidate: found unusable segment';
  RECALL_UNUSABLE_TAPE         CONSTANT VARCHAR2(2048) := 'createRecallCandidate: found segment on unusable tape';
  RECALL_CREATING_RECALLJOB    CONSTANT VARCHAR2(2048) := 'createRecallCandidate: created new RecallJob';
  RECALL_MISSING_COPIES        CONSTANT VARCHAR2(2048) := 'createRecallCandidate: detected missing copies on tape';
  RECALL_MISSING_COPIES_NOOP   CONSTANT VARCHAR2(2048) := 'createRecallCandidate: detected missing copies on tape, but migrations ongoing';
  RECALL_MJ_FOR_MISSING_COPY   CONSTANT VARCHAR2(2048) := 'createRecallCandidate: create new MigrationJob to migrate missing copy';
  RECALL_COPY_STILL_MISSING    CONSTANT VARCHAR2(2048) := 'createRecallCandidate: could not find enough valid copy numbers to create missing copy';
  RECALL_MISSING_COPY_NO_ROUTE CONSTANT VARCHAR2(2048) := 'createRecallCandidate: no route to tape defined for missing copy';
  RECALL_MISSING_COPY_ERROR    CONSTANT VARCHAR2(2048) := 'createRecallCandidate: unexpected error when creating missing copy';
  RECALL_CANCEL_BY_VID         CONSTANT VARCHAR2(2048) := 'Canceling tape recall for given VID';
  RECALL_CANCEL_RECALLJOB_VID  CONSTANT VARCHAR2(2048) := 'Canceling RecallJobs for given VID';
  RECALL_FAILING               CONSTANT VARCHAR2(2048) := 'Failing Recall(s)';
  RECALL_FS_NOT_FOUND          CONSTANT VARCHAR2(2048) := 'bestFileSystemForRecall could not find a suitable destination for this recall';
  RECALL_LOOPING_ON_LOCK       CONSTANT VARCHAR2(2048) := 'Giving up with recall as we are looping on locked file(s)';
  RECALL_NOT_FOUND             CONSTANT VARCHAR2(2048) := 'setBulkFileRecallResult: unable to identify recall, giving up';
  RECALL_INVALID_PATH          CONSTANT VARCHAR2(2048) := 'setFileRecalled: unable to parse input path, giving up';
  RECALL_COMPLETED_DB          CONSTANT VARCHAR2(2048) := 'setFileRecalled: db updates after full recall completed';
  RECALL_FILE_OVERWRITTEN      CONSTANT VARCHAR2(2048) := 'setFileRecalled: file was overwritten during recall, restarting from scratch or skipping repack';
  RECALL_FILE_DROPPED          CONSTANT VARCHAR2(2048) := 'checkRecallInNS: file was dropped from namespace during recall, giving up';
  RECALL_BAD_CHECKSUM          CONSTANT VARCHAR2(2048) := 'checkRecallInNS: bad checksum detected, will retry if allowed';
  RECALL_CREATED_CHECKSUM      CONSTANT VARCHAR2(2048) := 'checkRecallInNS: created missing checksum in the namespace';
  RECALL_FAILED                CONSTANT VARCHAR2(2048) := 'setBulkFileRecallResult: recall process failed, will retry if allowed';
  RECALL_PERMANENTLY_FAILED    CONSTANT VARCHAR2(2048) := 'setFileRecalled: recall process failed permanently';
  BULK_RECALL_COMPLETED        CONSTANT VARCHAR2(2048) := 'setBulkFileRecallResult: bulk recall completed';
  
  MIGRATION_CANCEL_BY_VID      CONSTANT VARCHAR2(2048) := 'Canceling tape migration for given VID';
  MIGRATION_COMPLETED          CONSTANT VARCHAR2(2048) := 'setFileMigrated: db updates after full migration completed';
  MIGRATION_NOT_FOUND          CONSTANT VARCHAR2(2048) := 'setFileMigrated: unable to identify migration, giving up';
  MIGRATION_RETRY              CONSTANT VARCHAR2(2048) := 'setBulkFilesMigrationResult: migration failed, will retry if allowed';
  MIGRATION_FILE_DROPPED       CONSTANT VARCHAR2(2048) := 'failFileMigration: file was dropped or modified during migration, giving up';
  MIGRATION_SUPERFLUOUS_COPY   CONSTANT VARCHAR2(2048) := 'failFileMigration: file already had enough copies on tape, ignoring new segment';
  MIGRATION_FAILED             CONSTANT VARCHAR2(2048) := 'failFileMigration: migration to tape failed for this file, giving up';
  MIGRATION_FAILED_NOT_FOUND   CONSTANT VARCHAR2(2048) := 'failFileMigration: file not found when failing migration';
  BULK_MIGRATION_COMPLETED     CONSTANT VARCHAR2(2048) := 'setBulkFileMigrationResult: bulk migration completed';

  REPACK_SUBMITTED             CONSTANT VARCHAR2(2048) := 'New Repack request submitted';
  REPACK_ABORTING              CONSTANT VARCHAR2(2048) := 'Aborting Repack request';
  REPACK_ABORTED               CONSTANT VARCHAR2(2048) := 'Repack request aborted';
  REPACK_ABORTED_FAILED        CONSTANT VARCHAR2(2048) := 'Aborting Repack request failed, dropping it';
  REPACK_JOB_ONGOING           CONSTANT VARCHAR2(2048) := 'repackManager: Repack processes still starting, no new ones will be started for this round';
  REPACK_STARTED               CONSTANT VARCHAR2(2048) := 'repackManager: Repack process started';
  REPACK_JOB_STATS             CONSTANT VARCHAR2(2048) := 'repackManager: Repack processes statistics';
  REPACK_UNEXPECTED_EXCEPTION  CONSTANT VARCHAR2(2048) := 'handleRepackRequest: unexpected exception caught';

  DRAINING_REFILL              CONSTANT VARCHAR2(2048) := 'drainRunner: Creating new replication jobs';

  DELETEDISKCOPY_RECALL        CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy was lost, about to recall from tape';
  DELETEDISKCOPY_REPLICATION   CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy was lost, about to replicate from another pool';
  DELETEDISKCOPY_LOST          CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: file was LOST and is being dropped from the system';
  DELETEDISKCOPY_GC            CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy is being garbage collected';
  DELETEDISKCOPY_NOOP          CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy could not be garbage collected';

  STAGER_GET                   CONSTANT VARCHAR2(2048) := 'Get Request';
  STAGER_PUT                   CONSTANT VARCHAR2(2048) := 'Put Request';
  STAGER_UPDATE                CONSTANT VARCHAR2(2048) := 'Update Request';
  STAGER_PREPARETOGET          CONSTANT VARCHAR2(2048) := 'PrepareToGet Request';
  STAGER_PREPARETOPUT          CONSTANT VARCHAR2(2048) := 'PrepareToPut Request';
  STAGER_PREPARETOUPDATE       CONSTANT VARCHAR2(2048) := 'PrepareToUpdate Request';

  STAGER_D2D_TRIGGERED         CONSTANT VARCHAR2(2048) := 'Triggering DiskCopy replication';
  STAGER_WAITSUBREQ            CONSTANT VARCHAR2(2048) := 'Request moved to Wait';
  STAGER_UNABLETOPERFORM       CONSTANT VARCHAR2(2048) := 'Unable to perform request, notifying user';
  STAGER_RECREATION_IMPOSSIBLE CONSTANT VARCHAR2(2048) := 'Impossible to recreate CastorFile';
  STAGER_CASTORFILE_RECREATION CONSTANT VARCHAR2(2048) := 'Recreating CastorFile';
  STAGER_GET_REPLICATION       CONSTANT VARCHAR2(2048) := 'Triggering internal DiskCopy replication';
  STAGER_GET_REPLICATION_FAIL  CONSTANT VARCHAR2(2048) := 'Triggering internal DiskCopy replication failed';
  STAGER_DISKCOPY_FOUND        CONSTANT VARCHAR2(2048) := 'Available DiskCopy found';

  REPORT_HEART_BEAT_RESUMED    CONSTANT VARCHAR2(2048) := 'Heartbeat resumed for diskserver, status changed to PRODUCTION';
  
  D2D_CREATING_JOB             CONSTANT VARCHAR2(2048) := 'Created new Disk2DiskCopyJob';
  D2D_CANCELED_AT_START        CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Replication request canceled while queuing in scheduler or transfer already started';
  D2D_MULTIPLE_COPIES_ON_DS    CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Multiple copies of this file already found on this diskserver';
  D2D_SOURCE_GONE              CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Source has disappeared while queuing in scheduler, retrying';
  D2D_SRC_DISABLED             CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Source diskserver/filesystem was DISABLED meanwhile';
  D2D_DEST_NOT_PRODUCTION      CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Destination diskserver/filesystem not in PRODUCTION any longer';
  D2D_START_OK                 CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart called and returned successfully';
  D2D_D2DDONE_CANCEL           CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : Invalidating new copy as job was canceled';
  D2D_D2DDONE_BADSIZE          CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : File replication size mismatch';
  D2D_D2DDONE_OK               CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : Replication successful';
  D2D_D2DDONE_RETRIED          CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : Retrying disk to disk copy';
  D2D_D2DDONE_NORETRY          CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : no retry, giving up';
  D2D_D2DFAILED                CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : replication failed';
  REBALANCING_START            CONSTANT VARCHAR2(2048) := 'rebalancing : starting';
  REBALANCING_STOP             CONSTANT VARCHAR2(2048) := 'rebalancing : stopping';
END dlf;
/


/* This procedure resets the lastKnownFileName the CastorFile that has a given name
   inside an autonomous transaction. This should be called before creating/renaming any
   CastorFile so that lastKnownFileName stays unique */
CREATE OR REPLACE PROCEDURE dropReusedLastKnownFileName(fileName IN VARCHAR2) AS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  UPDATE /*+ INDEX_RS_ASC (CastorFile I_CastorFile_lastKnownFileName) */ CastorFile
     SET lastKnownFileName = TO_CHAR(id)
   WHERE lastKnownFileName = normalizePath(fileName);
  COMMIT;
END;
/

/* PL/SQL method implementing fixLastKnownFileName */
CREATE OR REPLACE PROCEDURE fixLastKnownFileName(inFileName IN VARCHAR2, inCfId IN INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  UPDATE CastorFile SET lastKnownFileName = normalizePath(inFileName)
   WHERE id = inCfId;
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  -- we have another file that already uses the new name of this one...
  -- It has probably changed name in the namespace before its na
  -- let's fix this, but we won't put the right name there
  -- Note that this procedure will run in an autonomous transaction so that
  -- no dead lock can result from taking a second lock within this transaction
  dropReusedLastKnownFileName(inFileName);
  UPDATE CastorFile SET lastKnownFileName = normalizePath(inFileName)
   WHERE id = inCfId;
END;
/

/* PL/SQL method implementing createEmptyFile */
CREATE OR REPLACE PROCEDURE createEmptyFile
(cfId IN NUMBER, fileId IN NUMBER, nsHost IN VARCHAR2,
 srId IN INTEGER, schedule IN INTEGER) AS
  dcPath VARCHAR2(2048);
  gcw NUMBER;
  gcwProc VARCHAR(2048);
  fsId NUMBER;
  dcId NUMBER;
  svcClassId NUMBER;
  ouid INTEGER;
  ogid INTEGER;
  fsPath VARCHAR2(2048);
BEGIN
  -- update filesize overriding any previous value
  UPDATE CastorFile SET fileSize = 0 WHERE id = cfId;
  -- get an id for our new DiskCopy
  dcId := ids_seq.nextval();
  -- compute the DiskCopy Path
  buildPathFromFileId(fileId, nsHost, dcId, dcPath);
  -- find a fileSystem for this empty file
  SELECT id, svcClass, euid, egid, name || ':' || mountpoint
    INTO fsId, svcClassId, ouid, ogid, fsPath
    FROM (SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
                 FileSystem.id, Request.svcClass, Request.euid, Request.egid, DiskServer.name, FileSystem.mountpoint
            FROM DiskServer, FileSystem, DiskPool2SvcClass,
                 (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                         id, svcClass, euid, egid from StageGetRequest UNION ALL
                  SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */
                         id, svcClass, euid, egid from StagePrepareToGetRequest UNION ALL
                  SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */
                         id, svcClass, euid, egid from StageUpdateRequest UNION ALL
                  SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */
                         id, svcClass, euid, egid from StagePrepareToUpdateRequest) Request,
                  SubRequest
           WHERE SubRequest.id = srId
             AND Request.id = SubRequest.request
             AND Request.svcclass = DiskPool2SvcClass.child
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
             AND DiskServer.id = FileSystem.diskServer
             AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
             AND DiskServer.hwOnline = 1
        ORDER BY -- order by rate as defined by the function
                 fileSystemRate(FileSystem.nbReadStreams, FileSystem.nbWriteStreams) DESC,
                 -- finally use randomness to avoid preferring always the same FS
                 DBMS_Random.value)
   WHERE ROWNUM < 2;
  -- compute it's gcWeight
  gcwProc := castorGC.getRecallWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(0); END;'
    USING OUT gcw;
  -- then create the DiskCopy
  INSERT INTO DiskCopy
    (path, id, filesystem, castorfile, status, importance,
     creationTime, lastAccessTime, gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
  VALUES (dcPath, dcId, fsId, cfId, dconst.DISKCOPY_VALID, -1,
          getTime(), getTime(), GCw, 0, 0, ouid, ogid);
  -- link to the SubRequest and schedule an access if requested
  IF schedule = 0 THEN
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET diskCopy = dcId WHERE id = srId;
  ELSE
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET diskCopy = dcId,
           requestedFileSystems = fsPath,
           xsize = 0, status = dconst.SUBREQUEST_READYFORSCHED,
           getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED
     WHERE id = srId;
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  raise_application_error(-20115, 'No suitable filesystem found for this empty file');
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
    DECLARE
      CONSTRAINT_VIOLATED EXCEPTION;
      PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);      
    BEGIN
      -- check how many disk2DiskCopyJobs are already running for this draining job
      SELECT count(*) INTO varNbRunningJobs FROM Disk2DiskCopyJob WHERE drainingJob = dj.id;
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
    EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- check that the constraint violated is due to deletion of the drainingJob
      IF sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_DISK2DISKCOPYJOB_DRAINJOB) violated%' THEN
        -- give up with this DrainingJob as it was canceled
        ROLLBACK;
      ELSE
        raise;
      END IF;
    END;
  END LOOP;
END;
/

/* update the db when a tape session is ended */
CREATE OR REPLACE PROCEDURE tg_endTapeSession(inMountTransactionId IN NUMBER,
                                              inErrorCode IN INTEGER) AS
  varMjIds "numList";    -- recall/migration job Ids
  varMountId INTEGER;
BEGIN
  -- Let's assume this is a migration mount
  SELECT id INTO varMountId
    FROM MigrationMount
   WHERE mountTransactionId = inMountTransactionId
   FOR UPDATE;
  -- yes, it's a migration mount: delete it and detach all selected jobs
  UPDATE MigrationJob
     SET status = tconst.MIGRATIONJOB_PENDING,
         VID = NULL,
         mountTransactionId = NULL
   WHERE mountTransactionId = inMountTransactionId
     AND status = tconst.MIGRATIONJOB_SELECTED;
  DELETE FROM MigrationMount
   WHERE id = varMountId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- was not a migration session, let's try a recall one
  DECLARE
    varVID VARCHAR2(2048);
    varRjIds "numList";
  BEGIN
    SELECT vid INTO varVID
      FROM RecallMount
     WHERE mountTransactionId = inMountTransactionId
     FOR UPDATE;
    -- it was a recall mount
    -- find and reset the all RecallJobs of files for this VID
    UPDATE RecallJob
       SET status = tconst.RECALLJOB_PENDING
     WHERE castorFile IN (SELECT castorFile
                            FROM RecallJob
                           WHERE VID = varVID
                             AND fileTransactionId IS NOT NULL);
    DELETE FROM RecallMount WHERE vid = varVID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Small infusion of paranoia ;-) We should never reach that point...
    ROLLBACK;
    RAISE_APPLICATION_ERROR (-20119, 'endTapeSession: no recall or migration mount found');
  END;
END;
/

/* Procedure responsible for managing the draining process
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
       SET totalFiles = varTFiles,
           totalBytes = nvl(varTBytes, 0),
           status = decode(varTBytes, NULL, dconst.DRAININGJOB_FINISHED, dconst.DRAININGJOB_RUNNING)
     WHERE id = dj.id;
    COMMIT;
  END LOOP;
END;
/

CREATE OR REPLACE PROCEDURE drainRunner AS
  varNbFiles INTEGER := 0;
  varNbBytes INTEGER := 0;
  varNbRunningJobs INTEGER;
  varMaxNbOfSchedD2dPerDrain INTEGER;
  varUnused INTEGER;
BEGIN
  -- get maxNbOfSchedD2dPerDrain
  varMaxNbOfSchedD2dPerDrain := TO_NUMBER(getConfigOption('Draining', 'MaxNbSchedD2dPerDrain', '1000'));
  -- loop over draining jobs
  FOR dj IN (SELECT id, fileSystem, svcClass, fileMask, euid, egid
               FROM DrainingJob WHERE status = dconst.DRAININGJOB_RUNNING) LOOP
    DECLARE
      CONSTRAINT_VIOLATED EXCEPTION;
      PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);      
    BEGIN
      -- lock the draining Job first
      SELECT id INTO varUnused FROM DrainingJob WHERE id = dj.id FOR UPDATE;
      -- check how many disk2DiskCopyJobs are already running for this draining job
      SELECT count(*) INTO varNbRunningJobs FROM Disk2DiskCopyJob WHERE drainingJob = dj.id;
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
    EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- check that the constraint violated is due to deletion of the drainingJob
      IF sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_DISK2DISKCOPYJOB_DRAINJOB) violated%' THEN
        -- give up with this DrainingJob as it was canceled
        ROLLBACK;
      ELSE
        raise;
      END IF;
    END;
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
        PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
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
        IF sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_DISK2DISKCOPYJOB_CASTORFILE) violated%' THEN
          -- Ignore the deletion, probably some draining/rebalancing activity created a Disk2DiskCopyJob entity
          -- while we were attempting to drop the CastorFile
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

/* PL/SQL method implementing getFileIdsForSrs.
   This method returns the list of fileids associated to the given list of
   subrequests */
CREATE OR REPLACE PROCEDURE getFileIdsForSrs
  (subReqIds IN castor."strList", fileids OUT castor.FileEntry_Cur) AS
  fid NUMBER;
  nh VARCHAR(2048);
BEGIN
  FOR i IN subReqIds.FIRST .. subReqIds.LAST LOOP
    BEGIN
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_SubreqId)*/ fileid, nsHost INTO fid, nh
        FROM Castorfile, SubRequest
       WHERE SubRequest.subreqId = subReqIds(i)
         AND SubRequest.castorFile = CastorFile.id;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- must be disk to disk copies
      BEGIN
        SELECT fileid, nsHost INTO fid, nh
          FROM Castorfile, Disk2DiskCopyJob
         WHERE Disk2DiskCopyJob.transferid = subReqIds(i)
           AND Disk2DiskCopyJob.castorFile = CastorFile.id;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- not even a disk to disk copy: it must have been dropped meanwhile by a
        -- transfermanagerd in another head node. Just insert an empty row, it will
        -- be handled by the synchronizer thread of transfermanagerd.
        fid := 0;
        nh := '';
      END;
    END;
    INSERT INTO GetFileIdsForSrsHelper (rowno, fileId, nsHost) VALUES (i, fid, nh);
  END LOOP;
  OPEN fileids FOR SELECT nh, fileid FROM GetFileIdsForSrsHelper ORDER BY rowno;
END;
/

/* inserts StageFileQueryRequest requests in the stager DB */    
CREATE OR REPLACE PROCEDURE insertStageFileQueryRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   fileName IN VARCHAR2,
   qpValues IN castor."strList",
   qpTypes IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  queryParamId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 33 THEN -- StageFileQueryRequest
      INSERT INTO StageFileQueryRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, fileName, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,fileName,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertFileQueryRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on query parameters. Note that the array is never empty (see the C++ calling method).
  FOR i IN qpValues.FIRST .. qpValues.LAST LOOP
    -- get unique ids for the query parameter
    SELECT ids_seq.nextval INTO queryParamId FROM DUAL;
    -- insert the query parameter
    INSERT INTO QueryParameter (value, id, query, querytype)
    VALUES (qpValues(i), queryParamId, reqId, qpTypes(i));
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
  -- send an alert to accelerate the processing of the request
  DBMS_ALERT.SIGNAL('wakeUpQueryReqSvc', '');
END;
/


/* PL/SQL procedure to handle disk-to-disk copy replication */
CREATE OR REPLACE PROCEDURE handleReplication(inSRId IN INTEGER, inFileId IN INTEGER,
                                              inNsHost IN VARCHAR2, inCfId IN INTEGER,
                                              inNsOpenTime IN INTEGER, inSvcClassId IN INTEGER,
                                              inEuid IN INTEGER, inEGID IN INTEGER,
                                              inReqUUID IN VARCHAR2, inSrUUID IN VARCHAR2) AS
  varUnused INTEGER;
BEGIN
  -- check whether there's already an ongoing replication
  SELECT id INTO varUnused
    FROM Disk2DiskCopyJob
   WHERE castorfile = inCfId
     AND ROWNUM < 2;
  -- there is an ongoing replication, so just let it go and do nothing more
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no ongoing replication, let's see whether we need to trigger one
  -- that is compare total current # of diskcopies, regardless hardware availability, against maxReplicaNb
  DECLARE
    varNbDCs INTEGER;
    varMaxDCs INTEGER;
    varNbDss INTEGER;
  BEGIN
    SELECT COUNT(*), max(maxReplicaNb) INTO varNbDCs, varMaxDCs FROM (
      SELECT DiskCopy.id, maxReplicaNb
        FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass
       WHERE DiskCopy.castorfile = inCfId
         AND DiskCopy.fileSystem = FileSystem.id
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
         AND SvcClass.id = inSvcClassId
         AND DiskCopy.status = dconst.DISKCOPY_VALID);
    IF varNbDCs < varMaxDCs OR varMaxDCs = 0 THEN
      -- we have to replicate. But we should ony do it if we have enough available diskservers.
      SELECT COUNT(DISTINCT(DiskServer.name)) INTO varNbDSs
        FROM DiskServer, FileSystem, DiskPool2SvcClass
       WHERE FileSystem.diskServer = DiskServer.id
         AND FileSystem.diskPool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = inSvcClassId
         AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
         AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
         AND DiskServer.hwOnline = 1
         AND DiskServer.id NOT IN (
           SELECT /*+ INDEX(DiskCopy I_DiskCopy_CastorFile) */ DISTINCT(DiskServer.id)
             FROM DiskCopy, FileSystem, DiskServer
            WHERE DiskCopy.castorfile = inCfId
              AND DiskCopy.fileSystem = FileSystem.id
              AND FileSystem.diskserver = DiskServer.id
              AND DiskCopy.status = dconst.DISKCOPY_VALID);
      IF varNbDSs > 0 THEN
        BEGIN
          -- yes, we can replicate, create a replication request without waiting on it.
          createDisk2DiskCopyJob(inCfId, inNsOpenTime, inSvcClassId, inEuid, inEgid,
                                 dconst.REPLICATIONTYPE_INTERNAL, NULL, NULL, TRUE);
          -- log it
          logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_GET_REPLICATION, inFileId, inNsHost, 'stagerd',
                   'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId) ||
                   ' euid=' || TO_CHAR(inEuid) || ' egid=' || TO_CHAR(inEgid));
        EXCEPTION WHEN NO_DATA_FOUND THEN
          logToDLF(inReqUUID, dlf.LVL_WARNING, dlf.STAGER_GET_REPLICATION_FAIL, inFileId, inNsHost, 'stagerd',
                   'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId) ||
                   ' euid=' || TO_CHAR(inEuid) || ' egid=' || TO_CHAR(inEgid));
        END;
      END IF;
    END IF;
  END;
END;
/

/* PL/SQL method implementing triggerD2dOrRecall
 * returns 1 if a recall was successfully triggered
 */
CREATE OR REPLACE FUNCTION triggerD2dOrRecall(inCfId IN INTEGER, inNsOpenTime IN INTEGER,  inSrId IN INTEGER,
                                              inFileId IN INTEGER, inNsHost IN VARCHAR2,
                                              inEuid IN INTEGER, inEgid IN INTEGER,
                                              inSvcClassId IN INTEGER, inFileSize IN INTEGER,
                                              inReqUUID IN VARCHAR2, inSrUUID IN VARCHAR2) RETURN INTEGER AS
  varSrcDcId NUMBER;
  varSrcSvcClassId NUMBER;
BEGIN
  -- Check whether we can use disk to disk copy or whether we need to trigger a recall
  checkForD2DCopyOrRecall(inCfId, inSrId, inEuid, inEgid, inSvcClassId, varSrcDcId, varSrcSvcClassId);
  IF varSrcDcId > 0 THEN
    -- create DiskCopyCopyJob and make this subRequest wait on it
    createDisk2DiskCopyJob(inCfId, inNsOpenTime, inSvcClassId, inEuid, inEgid,
                           dconst.REPLICATIONTYPE_USER, NULL, NULL, TRUE);
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_WAITSUBREQ
     WHERE id = inSrId;
    logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_D2D_TRIGGERED, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID || ' svcClass=' || getSvcClassName(inSvcClassId) ||
             ' srcDcId=' || TO_CHAR(varSrcDcId) || ' euid=' || TO_CHAR(inEuid) ||
             ' egid=' || TO_CHAR(inEgid));
  ELSIF varSrcDcId = 0 THEN
    -- no diskcopy found, no disk to disk copy possibility
    IF inFileSize = 0 THEN
      -- case of a 0 size file, we create it on the fly and schedule it
      createEmptyFile(inCfId, inFileId, inNsHost, inSrId, 1);
    ELSE
      -- regular file, go for a recall
      IF (createRecallCandidate(inSrId) = dconst.SUBREQUEST_FAILED) THEN 
        RETURN 0;
      END IF;
    END IF;
  ELSE
    -- user error
    logToDLF(inReqUUID, dlf.LVL_USER_ERROR, dlf.STAGER_UNABLETOPERFORM, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId));
    RETURN 0;
  END IF;
  RETURN 1;
END;
/

/* PL/SQL method implementing createDisk2DiskCopyJob */
CREATE OR REPLACE PROCEDURE createDisk2DiskCopyJob
(inCfId IN INTEGER, inNsOpenTime IN INTEGER, inDestSvcClassId IN INTEGER,
 inOuid IN INTEGER, inOgid IN INTEGER, inReplicationType IN INTEGER,
 inReplacedDcId IN INTEGER, inDrainingJob IN INTEGER, inDoSignal IN BOOLEAN) AS
  varD2dCopyJobId INTEGER;
  varDestDcId INTEGER;
BEGIN
  varD2dCopyJobId := ids_seq.nextval();
  varDestDcId := ids_seq.nextval();
  -- Create the Disk2DiskCopyJob
  INSERT INTO Disk2DiskCopyJob (id, transferId, creationTime, status, retryCounter, ouid, ogid,
                                destSvcClass, castorFile, nsOpenTime, replicationType,
                                replacedDcId, destDcId, drainingJob)
  VALUES (varD2dCopyJobId, uuidgen(), gettime(), dconst.DISK2DISKCOPYJOB_PENDING, 0, inOuid, inOgid,
          inDestSvcClassId, inCfId, inNsOpenTime, inReplicationType,
          inReplacedDcId, varDestDcId, inDrainingJob);

  -- log "Created new Disk2DiskCopyJob"
  DECLARE
    varFileId INTEGER;
    varNsHost VARCHAR2(2048);
  BEGIN
    SELECT fileid, nsHost INTO varFileId, varNsHost FROM CastorFile WHERE id = inCfId;
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_CREATING_JOB, varFileId, varNsHost, 'stagerd',
             'destSvcClass=' || getSvcClassName(inDestSvcClassId) || ' nsOpenTime=' || TO_CHAR(inNsOpenTime) ||
             ' uid=' || TO_CHAR(inOuid) || ' gid=' || TO_CHAR(inOgid) || ' replicationType=' ||
             getObjStatusName('Disk2DiskCopyJob', 'replicationType', inReplicationType) ||
             ' TransferId=' || TO_CHAR(varD2dCopyJobId) || ' replacedDcId=' || TO_CHAR(inReplacedDcId ||
             ' DrainReq=' || TO_CHAR(inDrainingJob)));
  END;
  
  IF inDoSignal THEN
    -- wake up transfermanager
    DBMS_ALERT.SIGNAL('d2dReadyToSchedule', '');
  END IF;
END;
/


/* PL/SQL method implementing replicateOnClose */
CREATE OR REPLACE PROCEDURE replicateOnClose(cfId IN NUMBER, ouid IN INTEGER, ogid IN INTEGER) AS
  varNsOpenTime NUMBER;
  srcDcId NUMBER;
  srcSvcClassId NUMBER;
  ignoreSvcClass NUMBER;
BEGIN
  -- Lock the castorfile and take the nsOpenTime
  SELECT nsOpenTime INTO varNsOpenTime FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Loop over all service classes where replication is required
  FOR a IN (SELECT SvcClass.id FROM (
              -- Determine the number of copies of the file in all service classes
              SELECT * FROM (
                SELECT  /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */
                       SvcClass.id, count(*) available
                  FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
                 WHERE DiskCopy.filesystem = FileSystem.id
                   AND DiskCopy.castorfile = cfId
                   AND FileSystem.diskpool = DiskPool2SvcClass.parent
                   AND DiskPool2SvcClass.child = SvcClass.id
                   AND DiskCopy.status = dconst.DISKCOPY_VALID
                   AND FileSystem.status IN
                       (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
                   AND DiskServer.id = FileSystem.diskserver
                   AND DiskServer.status IN
                       (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
                 GROUP BY SvcClass.id)
             ) results, SvcClass
             -- Join the results with the service class table and determine if
             -- additional copies need to be created
             WHERE results.id = SvcClass.id
               AND SvcClass.replicateOnClose = 1
               AND results.available < SvcClass.maxReplicaNb)
  LOOP
    BEGIN
      -- Trigger a replication request.
      createDisk2DiskCopyJob(cfId, varNsOpenTime, a.id, ouid, ogid, dconst.REPLICATIONTYPE_USER, NULL, NULL, TRUE);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;  -- No copies to replicate from
    END;
  END LOOP;
END;
/

/* inserts file Requests in the stager DB.
 * This handles StageGetRequest, StagePrepareToGetRequest, StagePutRequest,
 * StagePrepareToPutRequest, StageUpdateRequest, StagePrepareToUpdateRequest,
 * StagePutDoneRequest, StagePrepareToUpdateRequest, and StageRmRequest requests.
 */    
CREATE OR REPLACE PROCEDURE insertFileRequest
  (userTag IN VARCHAR2,
   machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   mask IN INTEGER,
   userName IN VARCHAR2,
   flags IN INTEGER,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   inReqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   freeStrParam IN VARCHAR2,
   freeNumParam IN NUMBER,
   srFileNames IN castor."strList",
   srProtocols IN castor."strList",
   srXsizes IN castor."cnumList",
   srFlags IN castor."cnumList",
   srModeBits IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  subreqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  svcHandler VARCHAR2(100);
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, inReqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN inReqType = 35 THEN -- StageGetRequest
      INSERT INTO StageGetRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 36 THEN -- StagePrepareToGetRequest
      INSERT INTO StagePrepareToGetRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 40 THEN -- StagePutRequest
      INSERT INTO StagePutRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 37 THEN -- StagePrepareToPutRequest
      INSERT INTO StagePrepareToPutRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 44 THEN -- StageUpdateRequest
      INSERT INTO StageUpdateRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 38 THEN -- StagePrepareToUpdateRequest
      INSERT INTO StagePrepareToUpdateRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 42 THEN -- StageRmRequest
      INSERT INTO StageRmRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 39 THEN -- StagePutDoneRequest
      INSERT INTO StagePutDoneRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,freeStrParam,reqId,svcClassId,clientId);
    WHEN inReqType = 95 THEN -- SetFileGCWeight
      INSERT INTO SetFileGCWeight (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, weight, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,freeNumParam,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertFileRequest : ' || TO_CHAR(inReqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- get the request's service handler
  SELECT svcHandler INTO svcHandler FROM Type2Obj WHERE type=inReqType;
  -- Loop on subrequests
  FOR i IN srFileNames.FIRST .. srFileNames.LAST LOOP
    -- get unique ids for the subrequest
    SELECT ids_seq.nextval INTO subreqId FROM DUAL;
    -- insert the subrequest
    INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, status, request, getNextStatus, reqType)
    VALUES (0, srFileNames(i), srProtocols(i), srXsizes(i), 0, NULL, srFlags(i), srModeBits(i), creationTime, creationTime, 0, 0, '', NULL, svcHandler, subreqId, NULL, NULL, dconst.SUBREQUEST_START, reqId, 0, inReqType);
  END LOOP;
  -- send one single alert to accelerate the processing of the request
  CASE
  WHEN inReqType = 35 OR   -- StageGetRequest
       inReqType = 40 OR   -- StagePutRequest
       inReqType = 44 THEN -- StageUpdateRequest
    DBMS_ALERT.SIGNAL('wakeUpJobReqSvc', '');
  WHEN inReqType = 36 OR   -- StagePrepareToGetRequest
       inReqType = 37 OR   -- StagePrepareToPutRequest
       inReqType = 38 THEN -- StagePrepareToUpdateRequest
    DBMS_ALERT.SIGNAL('wakeUpPrepReqSvc', '');
  WHEN inReqType = 42 OR   -- StageRmRequest
       inReqType = 39 OR   -- StagePutDoneRequest
       inReqType = 95 THEN -- SetFileGCWeight
    DBMS_ALERT.SIGNAL('wakeUpStageReqSvc', '');
  END CASE;
END;
/

/* Function to return the name of a given file class */
CREATE OR REPLACE FUNCTION getFileClassName(fileClassId NUMBER) RETURN VARCHAR2 IS
  varFileClassName VARCHAR2(2048);
BEGIN
  SELECT name INTO varFileClassName FROM FileClass WHERE id = fileClassId;
  RETURN varFileClassName;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RETURN 'Unknown(' || fileClassId || ')';
END;
/

/* Function to return the name of a given service class */
CREATE OR REPLACE FUNCTION getSvcClassName(svcClassId NUMBER) RETURN VARCHAR2 IS
  varSvcClassName VARCHAR2(2048);
BEGIN
  SELECT name INTO varSvcClassName FROM SvcClass WHERE id = svcClassId;
  RETURN varSvcClassName;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RETURN 'Unknown(' || svcClassId || ')';
END;
/

/* PL/SQL method implementing putStart */
CREATE OR REPLACE PROCEDURE putStart
        (srId IN INTEGER, selectedDiskServer IN VARCHAR2, selectedMountPoint IN VARCHAR2,
         rdcId OUT INTEGER, rdcStatus OUT INTEGER, rdcPath OUT VARCHAR2) AS
  varCfId INTEGER;
  srStatus INTEGER;
  srSvcClass INTEGER;
  fsId INTEGER;
  fsStatus INTEGER;
  dsStatus INTEGER;
  varHwOnline INTEGER;
  prevFsId INTEGER;
BEGIN
  -- Get diskCopy and subrequest related information
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         SubRequest.castorFile, SubRequest.diskCopy, SubRequest.status, DiskCopy.fileSystem,
         Request.svcClass
    INTO varCfId, rdcId, srStatus, prevFsId, srSvcClass
    FROM SubRequest, DiskCopy,
         (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ id, svcClass FROM StagePutRequest UNION ALL
          SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClass FROM StageGetRequest UNION ALL
          SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClass FROM StageUpdateRequest) Request
   WHERE SubRequest.diskcopy = Diskcopy.id
     AND SubRequest.id = srId
     AND SubRequest.request = Request.id;
  -- Check that we did not cancel the SubRequest in the mean time
  IF srStatus IN (dconst.SUBREQUEST_FAILED, dconst.SUBREQUEST_FAILED_FINISHED) THEN
    raise_application_error(-20104, 'SubRequest canceled while queuing in scheduler. Giving up.');
  END IF;
  -- Get selected filesystem and its status
  SELECT FileSystem.id, FileSystem.status, DiskServer.status, DiskServer.hwOnline
    INTO fsId, fsStatus, dsStatus, varHwOnline
    FROM DiskServer, FileSystem
   WHERE FileSystem.diskserver = DiskServer.id
     AND DiskServer.name = selectedDiskServer
     AND FileSystem.mountPoint = selectedMountPoint;
  -- Check that a job has not already started for this diskcopy. Refer to
  -- bug #14358
  IF prevFsId > 0 AND prevFsId <> fsId THEN
    raise_application_error(-20104, 'This job has already started for this DiskCopy. Giving up.');
  END IF;
  IF fsStatus != dconst.FILESYSTEM_PRODUCTION OR dsStatus != dconst.DISKSERVER_PRODUCTION OR varHwOnline = 0 THEN
    raise_application_error(-20104, 'The selected diskserver/filesystem is not in PRODUCTION any longer. Giving up.');
  END IF;
  -- In case the DiskCopy was in WAITFS_SCHEDULING,
  -- restart the waiting SubRequests
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_RESTART, lastModificationTime = getTime()
   WHERE status = dconst.SUBREQUEST_WAITSUBREQ
     AND castorFile = varCfId;
  DBMS_ALERT.SIGNAL('wakeUpJobReqSvc', '');
  -- link DiskCopy and FileSystem and update DiskCopyStatus
  UPDATE DiskCopy
     SET status = 6, -- DISKCOPY_STAGEOUT
         fileSystem = fsId,
         nbCopyAccesses = nbCopyAccesses + 1
   WHERE id = rdcId
   RETURNING status, path
   INTO rdcStatus, rdcPath;
EXCEPTION WHEN NO_DATA_FOUND THEN
   raise_application_error(-20104, 'SubRequest canceled while queuing in scheduler. Giving up.');
END;
/

/* PL/SQL method implementing disk2DiskCopyEnded
 * Note that inDestDsName, inDestPath and inReplicaFileSize are not used when inErrorMessage is not NULL
 * inErrorCode is used in case of error to decide whether to retry and also to invalidate
 * the source diskCopy if the error is an ENOENT
 */
CREATE OR REPLACE PROCEDURE disk2DiskCopyEnded
(inTransferId IN VARCHAR2, inDestDsName IN VARCHAR2, inDestPath IN VARCHAR2,
 inReplicaFileSize IN INTEGER, inErrorCode IN INTEGER, inErrorMessage IN VARCHAR2) AS
  varCfId INTEGER;
  varUid INTEGER := -1;
  varGid INTEGER := -1;
  varDestDcId INTEGER;
  varDestSvcClass INTEGER;
  varRepType INTEGER;
  varReplacedDcId INTEGER;
  varRetryCounter INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
  varFileSize INTEGER;
  varDestPath VARCHAR2(2048);
  varDestFsId INTEGER;
  varDcGcWeight NUMBER := 0;
  varDcImportance NUMBER := 0;
  varNewDcStatus INTEGER := dconst.DISKCOPY_VALID;
  varLogMsg VARCHAR2(2048);
  varComment VARCHAR2(2048);
  varDrainingJob VARCHAR2(2048);
BEGIN
  varLogMsg := CASE WHEN inErrorMessage IS NULL THEN dlf.D2D_D2DDONE_OK ELSE dlf.D2D_D2DFAILED END;
  BEGIN
    -- Get data from the disk2DiskCopy Job
    SELECT castorFile, ouid, ogid, destDcId, destSvcClass, replicationType,
           replacedDcId, retryCounter, drainingJob
      INTO varCfId, varUid, varGid, varDestDcId, varDestSvcClass, varRepType,
           varReplacedDcId, varRetryCounter, varDrainingJob
      FROM Disk2DiskCopyjob
     WHERE transferId = inTransferId;
    -- lock the castor file (and get logging info)
    SELECT fileid, nsHost, fileSize INTO varFileId, varNsHost, varFileSize
      FROM CastorFile
     WHERE id = varCfId
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- two possibilities here :
    --   - disk2diskCopyJob not found. It was probably canceled.
    --   - the castorFile has disappeared before we locked it, ant the
    --     disk2diskCopyJob too as we have a foreign key constraint.
    -- So our brand new copy has to be created as invalid to trigger GC.
    varNewDcStatus := dconst.DISKCOPY_INVALID;
    varLogMsg := dlf.D2D_D2DDONE_CANCEL;
  END;
  -- check the filesize
  IF inReplicaFileSize != varFileSize THEN
    -- replication went wrong !
    IF varLogMsg = dlf.D2D_D2DDONE_OK THEN
      varLogMsg := dlf.D2D_D2DDONE_BADSIZE;
      varNewDcStatus := dconst.DISKCOPY_INVALID;
    END IF;
  END IF;
  -- Log success or failure of the replication
  varComment := 'transferId=' || inTransferId ||
         ' destSvcClass=' || getSvcClassName(varDestSvcClass) ||
         ' dstDcId=' || TO_CHAR(varDestDcId) || ' destPath=' || inDestDsName || ':' || inDestPath ||
         ' euid=' || TO_CHAR(varUid) || ' egid=' || TO_CHAR(varGid) || 
         ' fileSize=' || TO_CHAR(varFileSize);
  IF inErrorMessage IS NOT NULL THEN
    varComment := varComment || ' replicaFileSize=' || TO_CHAR(inReplicaFileSize) ||
                  ' errorMessage=' || inErrorMessage;
  END IF;
  logToDLF(NULL, dlf.LVL_SYSTEM, varLogMsg, varFileId, varNsHost, 'stagerd', varComment);
  -- if success, create new DiskCopy, restart waiting requests, cleanup and handle replicate on close
  IF inErrorMessage IS NULL THEN
    -- get filesystem of the diskcopy and parse diskcopy path
    SELECT FileSystem.id, SUBSTR(inDestPath, LENGTH(FileSystem.mountPoint)+1)
      INTO varDestFsId, varDestPath
      FROM DiskServer, FileSystem
     WHERE DiskServer.name = inDestDsName
       AND FileSystem.diskServer = DiskServer.id
       AND INSTR(inDestPath, FileSystem.mountPoint) = 1;
    -- compute GcWeight and importance of the new copy
    IF varNewDcStatus = dconst.DISKCOPY_VALID THEN
      DECLARE
        varGcwProc VARCHAR2(2048);
      BEGIN
        varGcwProc := castorGC.getCopyWeight(varFileSize);
        EXECUTE IMMEDIATE
          'BEGIN :newGcw := ' || varGcwProc || '(:size); END;'
          USING OUT varDcGcWeight, IN varFileSize;
        SELECT /*+ INDEX_RS_ASC (DiskCopy I_DiskCopy_CastorFile) */
               COUNT(*)+1 INTO varDCImportance FROM DiskCopy
         WHERE castorFile=varCfId AND status = dconst.DISKCOPY_VALID;
      END;
    END IF;
    -- create the new DiskCopy
    INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                          owneruid, ownergid, id, gcType, fileSystem, castorFile, status, importance)
    VALUES (varDestPath, varDcGcWeight, getTime(), getTime(), varFileSize, 0, varUid, varGid, varDestDcId,
            CASE varNewDcStatus WHEN dconst.DISKCOPY_INVALID THEN dconst.GCTYPE_OVERWRITTEN ELSE NULL END,
            varDestFsId, varCfId, varNewDcStatus, varDCImportance);
    -- Wake up waiting subrequests
    UPDATE SubRequest
       SET status = dconst.SUBREQUEST_RESTART,
           getNextStatus = CASE WHEN inErrorMessage IS NULL THEN dconst.GETNEXTSTATUS_FILESTAGED ELSE getNextStatus END,
           lastModificationTime = getTime()
     WHERE status = dconst.SUBREQUEST_WAITSUBREQ
       AND castorfile = varCfId;
    DBMS_ALERT.SIGNAL('wakeUpJobReqSvc', '');
    -- delete the disk2diskCopyJob
    DELETE FROM Disk2DiskCopyjob WHERE transferId = inTransferId;
    -- In case of valid new copy
    IF varNewDcStatus = dconst.DISKCOPY_VALID THEN
      -- update importance of other DiskCopies if it's an additional one
      IF varReplacedDcId IS NOT NULL THEN
        UPDATE DiskCopy SET importance = varDCImportance WHERE castorFile=varCfId;
      END IF;
      -- drop source if requested
      UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID WHERE id = varReplacedDcId;
      -- Trigger the creation of additional copies of the file, if any
      replicateOnClose(varCfId, varUid, varGid);
    END IF;
    -- In case of draining, update DrainingJob
    IF varDrainingJob IS NOT NULL THEN
      updateDrainingJobOnD2dEnd(varDrainingJob, varFileSize, False);
    END IF;
  ELSE
    DECLARE
      varMaxNbD2dRetries INTEGER := TO_NUMBER(getConfigOption('D2dCopy', 'MaxNbRetries', 2));
    BEGIN
      -- shall we try again ?
      -- we should not when the job was deliberately killed, neither when we reach the maximum
      -- number of attempts
      IF varRetryCounter < varMaxNbD2dRetries AND inErrorCode != serrno.ESTKILLED THEN
        -- yes, so let's restart the Disk2DiskCopyJob
        UPDATE Disk2DiskCopyJob
           SET status = dconst.DISK2DISKCOPYJOB_PENDING,
               retryCounter = varRetryCounter + 1
         WHERE transferId = inTransferId;
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_D2DDONE_RETRIED, varFileId, varNsHost, 'stagerd', varComment ||
                 ' RetryNb=' || TO_CHAR(varRetryCounter+1) || ' maxNbRetries=' || TO_CHAR(varMaxNbD2dRetries));
      ELSE
        -- no retry, let's delete the disk to disk job copy
        BEGIN
          DELETE FROM Disk2DiskCopyjob WHERE transferId = inTransferId;
          -- and remember the error in case of draining
          IF varDrainingJob IS NOT NULL THEN
            INSERT INTO DrainingErrors (drainingJob, errorMsg, fileId, nsHost)
            VALUES (varDrainingJob, inErrorMessage, varFileId, varNsHost);
          END IF;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- the Disk2DiskCopyjob was already dropped (e.g. because of an interrupted draining)
          -- in such a case, forget about the error
          NULL;
        END;
        logToDLF(NULL, dlf.LVL_NOTICE, dlf.D2D_D2DDONE_NORETRY, varFileId, varNsHost, 'stagerd', varComment ||
                 ' maxNbRetries=' || TO_CHAR(varMaxNbD2dRetries));
        -- Fail waiting subrequests
        UPDATE SubRequest
           SET status = dconst.SUBREQUEST_FAILED,
               lastModificationTime = getTime(),
               errorCode = serrno.SEINTERNAL,
               errorMessage = 'Disk to disk copy failed after ' || TO_CHAR(varMaxNbD2dRetries) ||
                              'retries. Last error was : ' || inErrorMessage
         WHERE status = dconst.SUBREQUEST_WAITSUBREQ
           AND castorfile = varCfId;
        -- In case of draining, update DrainingJob
        IF varDrainingJob IS NOT NULL THEN
          updateDrainingJobOnD2dEnd(varDrainingJob, varFileSize, True);
        END IF;
      END IF;
    END;
  END IF;
END;
/

/* PL/SQL method implementing putFailedProc */
CREATE OR REPLACE PROCEDURE putFailedProcExt(srId IN NUMBER, errno IN NUMBER, errmsg IN VARCHAR2) AS
  dcId INTEGER;
  cfId INTEGER;
  unused INTEGER;
BEGIN
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ diskCopy, castorFile
    INTO dcId, cfId
    FROM SubRequest
   WHERE id = srId;
  -- Fail the subRequest
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET status = dconst.SUBREQUEST_FAILED,
         errorCode = errno,
         errorMessage = errmsg
   WHERE id = srId;
  -- Determine the context (Put inside PrepareToPut/Update ?)
  BEGIN
    -- Check that there is a PrepareToPut/Update going on. There can be only a single one
    -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
    -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
    SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile)*/ SubRequest.id INTO unused
      FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id FROM StagePrepareToPutRequest UNION ALL
            SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
     WHERE SubRequest.castorFile = cfId
       AND PrepareRequest.id = SubRequest.request
       AND SubRequest.status = 6; -- READY
    -- The select worked out, so we have a prepareToPut/Update
    -- In such a case, we do not cleanup DiskCopy and CastorFile
    -- but we have to wake up a potential waiting putDone
    UPDATE SubRequest
       SET status = dconst.SUBREQUEST_RESTART
     WHERE castorFile = cfId
       AND reqType = 39  -- PutDone
       AND SubRequest.status = dconst.SUBREQUEST_WAITSUBREQ;
    DBMS_ALERT.SIGNAL('wakeUpStageReqSvc', '');
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- This means we are a standalone put
    -- thus cleanup DiskCopy and maybe the CastorFile
    -- (the physical file is dropped by the job)
    DELETE FROM DiskCopy WHERE id = dcId;
    deleteCastorFile(cfId);
  END;
END;
/


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM user_objects
             WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY')
               AND status = 'INVALID')
  LOOP
    IF a.object_type = 'PACKAGE BODY' THEN a.object_type := 'PACKAGE'; END IF;
    BEGIN
      EXECUTE IMMEDIATE 'ALTER ' ||a.object_type||' '||a.object_name||' COMPILE';
    EXCEPTION WHEN OTHERS THEN
      -- ignore, so that we continue compiling the other invalid items
      NULL;
    END;
  END LOOP;
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_14_3';
COMMIT;
