/******************************************************************************
 *                 stager_2.1.14-2_to_2.1.14-3.sql
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
     AND release = '2_1_14_3'
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
     AND release LIKE '2_1_14_2%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_3', 'TRANSPARENT');
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

/* small schema change : added indexes */
CREATE INDEX I_DLFLogs_Msg ON DLFLogs(msg);
CREATE INDEX I_CastorFile_TapeStatus ON CastorFile(tapeStatus);

/* A view to spot late or stuck migrations */
/*******************************************/
/* It returns all files that are not yet on tape, that are existing in the namespace
 * and for which migration is pending for more than 24h.
 */
CREATE VIEW LateMigrationsView AS
  SELECT /*+ LEADING(CF DC MJ CnsFile) INDEX_RS_ASC(CF I_CastorFile_TapeStatus) INDEX_RS_ASC(DC I_DiskCopy_CastorFile) INDEX_RS_ASC(MJ I_MigrationJob_CastorFile) */
         CF.fileId, CF.lastKnownFileName as filePath, DC.creationTime as dcCreationTime,
         decode(MJ.creationTime, NULL, -1, getTime() - MJ.creationTime) as mjElapsedTime, nvl(MJ.status, -1) as mjStatus
    FROM CastorFile CF, DiskCopy DC, MigrationJob MJ, cns_file_metadata@remotens CnsFile
   WHERE CF.fileId = CnsFile.fileId
     AND DC.castorFile = CF.id
     AND MJ.castorFile(+) = CF.id
     AND CF.tapeStatus = 0  -- CASTORFILE_NOTONTAPE
     AND DC.creationTime < getTime() - 86400
  ORDER BY DC.creationTime DESC;

/* Update and revalidation of PL-SQL code */
/******************************************/

/* PL/SQL method used by the stager to collect the logging made in the DB */
CREATE OR REPLACE PROCEDURE dumpDBLogs(logEntries OUT castor.LogEntry_Cur) AS
  rowIds strListTable;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
BEGIN
  BEGIN
    -- lock whatever we can from the table. This is to prevent deadlocks.
    SELECT /*+ INDEX_RS_ASC(DLFLogs I_DLFLogs_Msg) */ ROWID BULK COLLECT INTO rowIds
      FROM DLFLogs
      WHERE ROWNUM < 10000
      FOR UPDATE NOWAIT;
    -- insert data on tmp table and drop selected entries
    INSERT INTO DLFLogsHelper (timeinfo, uuid, priority, msg, fileId, nsHost, SOURCE, params)
     (SELECT timeinfo, uuid, priority, msg, fileId, nsHost, SOURCE, params
      FROM DLFLogs WHERE ROWID IN (SELECT * FROM TABLE(rowIds)));
    DELETE FROM DLFLogs WHERE ROWID IN (SELECT * FROM TABLE(rowIds));
  EXCEPTION WHEN SrLocked THEN
    -- nothing we can lock, as someone else already has the lock.
    -- The logs will be taken by this other guy, so just give up
    NULL;
  END;
  -- return list of entries by opening a cursor on temp table
  OPEN logEntries FOR
    SELECT timeinfo, uuid, priority, msg, fileId, nsHost, source, params FROM DLFLogsHelper;
END;
/

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

  DRAINING_JOB_ONGOING         CONSTANT VARCHAR2(2048) := 'drainingManager: Draining jobs still starting, no new ones will be started for this round';
  DRAINING_STARTED             CONSTANT VARCHAR2(2048) := 'drainingManager: Draining process started';

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

/* Get next candidates for a given recall mount.
 * input:  VDQM transaction id, count and total size
 * output: outFiles, a cursor for the set of recall candidates.
 */
create or replace 
PROCEDURE tg_getBulkFilesToRecall(inLogContext IN VARCHAR2,
                                                    inMountTrId IN NUMBER,
                                                    inCount IN INTEGER,
                                                    inTotalSize IN INTEGER,
                                                    outFiles OUT SYS_REFCURSOR) AS
  varVid VARCHAR2(10);
  varPreviousFseq INTEGER;
  varCount INTEGER;
  varTotalSize INTEGER;
  varPath VARCHAR2(2048);
  varFileTrId INTEGER;
  varNewFseq INTEGER;
  bestFSForRecall_error EXCEPTION;
  PRAGMA EXCEPTION_INIT(bestFSForRecall_error, -20115);
  varNbLockedFiles INTEGER := 0;
BEGIN
  BEGIN
    -- Get VID and last processed fseq for this recall mount, lock
    SELECT vid, lastProcessedFseq INTO varVid, varPreviousFseq
      FROM RecallMount
     WHERE mountTransactionId = inMountTrId
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- recall is over or unknown request: return an empty cursor
    OPEN outFiles FOR
      SELECT fileId, nsHost, fileTransactionId, filePath, blockId, fseq, copyNb,
             euid, egid, VID, fileSize, creationTime, nbRetriesInMount, nbMounts
        FROM FilesToRecallHelper;
    RETURN;
  END;
  varCount := 0;
  varTotalSize := 0;
  varNewFseq := varPreviousFseq;
  -- Get candidates up to inCount or inTotalSize
  -- Select only the ones further down the tape (fseq > current one) as long as possible
  LOOP
    DECLARE
      varRjId INTEGER;
      varFSeq INTEGER;
      varBlockId RAW(4);
      varFileSize INTEGER;
      varCfId INTEGER;
      varFileId INTEGER;
      varNsHost VARCHAR2(2048);
      varCopyNb NUMBER;
      varEuid NUMBER;
      varEgid NUMBER;
      varCreationTime NUMBER;
      varNbRetriesInMount NUMBER;
      varNbMounts NUMBER;
      CfLocked EXCEPTION;
      PRAGMA EXCEPTION_INIT (CfLocked, -54);
    BEGIN
      -- Find the unprocessed recallJobs of this tape with lowest fSeq
      -- that is above the previous one
      SELECT * INTO varRjId, varFSeq, varBlockId, varFileSize, varCfId, varCopyNb,
                    varEuid, varEgid, varCreationTime, varNbRetriesInMount,
                    varNbMounts
        FROM (SELECT id, fSeq, blockId, fileSize, castorFile, copyNb, eUid, eGid,
                     creationTime, nbRetriesWithinMount,  nbMounts
                FROM RecallJob
               WHERE vid = varVid
                 AND status = tconst.RECALLJOB_PENDING
                 AND fseq > varNewFseq
               ORDER BY fseq ASC)
       WHERE ROWNUM < 2;
      -- lock the corresponding CastorFile, give up if we do not manage as it means that
      -- this file is already being handled by someone else
      -- Note that the giving up is handled by the handling of the CfLocked exception
      SELECT fileId, nsHost INTO varFileId, varNsHost
        FROM CastorFile
       WHERE id = varCfId
         FOR UPDATE NOWAIT;
      -- Now that we have the lock, double check that the RecallJob is still there and
      -- valid (due to race condition, it may have been processed in between our first select
      -- and the takin gof the lock)
      BEGIN
        SELECT id INTO varRjId FROM RecallJob WHERE id = varRJId AND status = tconst.RECALLJOB_PENDING;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- we got the race condition ! So this has already been handled, let's move to next file
        CONTINUE;
      END;
      -- move up last fseq used. Note that it moves up even if bestFileSystemForRecall
      -- (or any other statement) fails and the file is actually not recalled.
      -- The goal is that a potential retry within the same mount only occurs after
      -- we went around the other files on this tape.
      varNewFseq := varFseq;
      -- Find the best filesystem to recall the selected file
      bestFileSystemForRecall(varCfId, varPath);
      varCount := varCount + 1;
      varTotalSize := varTotalSize + varFileSize;
      INSERT INTO FilesToRecallHelper (fileId, nsHost, fileTransactionId, filePath, blockId, fSeq,
                 copyNb, euid, egid, VID, fileSize, creationTime, nbRetriesInMount, nbMounts)
        VALUES (varFileId, varNsHost, ids_seq.nextval, varPath, varBlockId, varFSeq,
                varCopyNb, varEuid, varEgid, varVID, varFileSize, varCreationTime, varNbRetriesInMount,
                varNbMounts)
        RETURNING fileTransactionId INTO varFileTrId;
      -- update RecallJobs of this file. Only the recalled one gets a fileTransactionId
      UPDATE RecallJob
         SET status = tconst.RECALLJOB_SELECTED,
             fileTransactionID = CASE WHEN id = varRjId THEN varFileTrId ELSE NULL END
       WHERE castorFile = varCfId;
      IF varCount >= inCount OR varTotalSize >= inTotalSize THEN
        -- we have enough candidates for this round, exit loop
        EXIT;
      END IF;
    EXCEPTION
      WHEN CfLocked THEN
        -- Go to next candidate, this CastorFile is being processed by another thread
        -- still check that this does not happen too often
        -- the reason is that a long standing lock (due to another bug) would make us spin
        -- like mad (learnt the hard way in production...)
        varNbLockedFiles := varNbLockedFiles + 1;
        IF varNbLockedFiles >= 100 THEN
          DECLARE
            lastSQL VARCHAR2(2048);
            prevSQL VARCHAR2(2048);
          BEGIN
            -- find the blocking SQL
            SELECT lastSql.sql_text, prevSql.sql_text INTO lastSQL, prevSQL
              FROM v$session currentSession, v$session blockerSession, v$sql lastSql, v$sql prevSql
             WHERE currentSession.sid = (SELECT sys_context('USERENV','SID') FROM DUAL)
               AND blockerSession.sid(+) = currentSession.blocking_session
               AND lastSql.sql_id(+) = blockerSession.sql_id
               AND prevSql.sql_id(+) = blockerSession.prev_sql_id;
            -- log the issue and exit, as if we were out of candidates
            logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_LOOPING_ON_LOCK, varFileId, varNsHost, 'tapegatewayd',
                   'SQLOfLockingSession="' || lastSQL || '" PreviousSQLOfLockingSession="' || prevSQL ||
                   '" mountTransactionId=' || to_char(inMountTrId) ||' '|| inLogContext);
            EXIT;
          END;
        END IF;
      WHEN bestFSForRecall_error THEN
        -- log 'bestFileSystemForRecall could not find a suitable destination for this recall' and skip it
        logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_FS_NOT_FOUND, varFileId, varNsHost, 'tapegatewayd',
                 'errorMessage="' || SQLERRM || '" mountTransactionId=' || to_char(inMountTrId) ||' '|| inLogContext);
        -- mark the recall job as failed, and maybe retry
        retryOrFailRecall(varCfId, varVID, NULL, inLogContext);
      WHEN NO_DATA_FOUND THEN
        -- nothing found. In case we did not try so far, try to restart with low fseqs
        IF varNewFseq > -1 THEN
          varNewFseq := -1;
        ELSE
          -- low fseqs were tried, we are really out of candidates, so exit the loop
          EXIT;
        END IF;
    END;
  END LOOP;
  -- Record last fseq at the mount level
  UPDATE RecallMount
     SET lastProcessedFseq = varNewFseq
   WHERE vid = varVid;
  -- Return all candidates. Don't commit now, this will be done in C++
  -- after the results have been collected as the temporary table will be emptied.
  OPEN outFiles FOR
    SELECT fileId, nsHost, fileTransactionId, filePath, blockId, fseq,
           copyNb, euid, egid, VID, fileSize, creationTime,
           nbRetriesInMount, nbMounts
      FROM FilesToRecallHelper;
END;
/

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
    -- also note the use of decode and the extra totalSize > 0 to protect
    -- us against division by 0. The decode is needed in case this filter
    -- is applied first, before the totalSize > 0
    SELECT AVG(free/decode(totalSize, 0, 1, totalSize)) INTO varFreeRef
      FROM FileSystem, DiskPool2SvcClass, DiskServer
     WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
       AND DiskPool2SvcClass.child = SC.id
       AND DiskServer.id = FileSystem.diskServer
       AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
       AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
       AND FileSystem.totalSize > 0
       AND DiskServer.hwOnline = 1
     GROUP BY SC.id;
    -- get sensibility of the rebalancing
    varSensibility := TO_NUMBER(getConfigOption('Rebalancing', 'Sensibility', '5'))/100;
    -- for each filesystem too full compared to average, rebalance
    -- note that we take the read only ones into account here
    -- also note the use of decode and the extra totalSize > 0 to protect
    -- us against division by 0. The decode is needed in case this filter
    -- is applied first, before the totalSize > 0
    FOR FS IN (SELECT FileSystem.id, varFreeRef*decode(totalSize, 0, 1, totalSize)-free dataToMove,
                      DiskServer.name ds, FileSystem.mountPoint
                 FROM FileSystem, DiskPool2SvcClass, DiskServer
                WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
                  AND DiskPool2SvcClass.child = SC.id
                  AND varFreeRef - free/totalSize > varSensibility
                  AND DiskServer.id = FileSystem.diskServer
                  AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
                  AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
                  AND FileSystem.totalSize > 0
                  AND DiskServer.hwOnline = 1) LOOP
      rebalance(FS.id, FS.dataToMove, SC.id, FS.ds, FS.mountPoint);
    END LOOP;
  END LOOP;
END;
/

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
      END;
    END LOOP;
  END IF;
  OPEN fileIds FOR
    SELECT fileId, nsHost FROM FilesDeletedProcOutput;
END;
/

CREATE OR REPLACE PROCEDURE deleteCastorFile(cfId IN NUMBER) AS
  nb NUMBER;
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -2292);
BEGIN
  -- First try to lock the castorFile
  SELECT id INTO nb FROM CastorFile
   WHERE id = cfId FOR UPDATE NOWAIT;

  -- See whether pending SubRequests exist
  SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
    FROM SubRequest
   WHERE castorFile = cfId
     AND status IN (1, 2, 3, 4, 5, 6, 7, 10, 12, 13);   -- All but START, FINISHED, FAILED_FINISHED, ARCHIVED
  -- If any SubRequest, give up
  IF nb = 0 THEN
    DECLARE
      fid NUMBER;
      fc NUMBER;
      nsh VARCHAR2(2048);
    BEGIN
      -- Delete the CastorFile
      -- this will raise a constraint violation if any DiskCopy, MigrationJob, RecallJob or Disk2DiskCopyJob
      -- still exists for this CastorFile. It is caught and we give up with the deletion in such a case.
      DELETE FROM CastorFile WHERE id = cfId
        RETURNING fileId, nsHost, fileClass
        INTO fid, nsh, fc;
      -- check whether this file potentially had copies on tape
      SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
      IF nb = 0 THEN
        -- This castorfile was created with no copy on tape
        -- So removing it from the stager means erasing
        -- it completely. We should thus also remove it
        -- from the name server
        INSERT INTO FilesDeletedProcOutput (fileId, nsHost) VALUES (fid, nsh);
      END IF;
    END;
  END IF;
EXCEPTION
  WHEN CONSTRAINT_VIOLATED THEN
    -- ignore, this means we still have some DiskCopy or Job around
    NULL;
  WHEN NO_DATA_FOUND THEN
    -- ignore, this means that the castorFile did not exist.
    -- There is thus no way to find out whether to remove the
    -- file from the nameserver. For safety, we thus keep it
    NULL;
  WHEN LockError THEN
    -- ignore, somebody else is dealing with this castorFile
    NULL;
END;
/

CREATE OR REPLACE PROCEDURE syncRunningTransfers(machine IN VARCHAR2,
                                                 transfers IN castor."strList",
                                                 killedTransfersCur OUT castor.TransferRecord_Cur) AS
  unused VARCHAR2(2048);
  varFileid NUMBER;
  varNsHost VARCHAR2(2048);
  varReqId VARCHAR2(2048);
  killedTransfers castor."strList";
  errnos castor."cnumList";
  errmsg castor."strList";
BEGIN
  -- cleanup from previous round
  DELETE FROM SyncRunningTransfersHelper2;
  -- insert the list of running transfers into a temporary table for easy access
  FORALL i IN transfers.FIRST .. transfers.LAST
    INSERT INTO SyncRunningTransfersHelper (subreqId) VALUES (transfers(i));
  -- Go through all running transfers from the DB point of view for the given diskserver
  FOR SR IN (SELECT SubRequest.id, SubRequest.subreqId, SubRequest.castorfile, SubRequest.request
               FROM SubRequest, DiskCopy, FileSystem, DiskServer
              WHERE SubRequest.status = dconst.SUBREQUEST_READY
                AND Subrequest.reqType IN (35, 37, 44)  -- StageGet/Put/UpdateRequest
                AND Subrequest.diskCopy = DiskCopy.id
                AND DiskCopy.fileSystem = FileSystem.id
                AND FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = machine) LOOP
    BEGIN
      -- check if they are running on the diskserver
      SELECT subReqId INTO unused FROM SyncRunningTransfersHelper
       WHERE subreqId = SR.subreqId;
      -- this one was still running, nothing to do then
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- this transfer is not running anymore although the stager DB believes it is
      -- we first get its reqid and fileid
      BEGIN
        SELECT Request.reqId INTO varReqId FROM
          (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ reqId, id from StageGetRequest UNION ALL
           SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ reqId, id from StagePutRequest UNION ALL
           SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ reqId, id from StageUpdateRequest) Request
         WHERE Request.id = SR.request;
        SELECT fileid, nsHost INTO varFileid, varNsHost FROM CastorFile WHERE id = SR.castorFile;
        -- and we put it in the list of transfers to be failed with code 1015 (SEINTERNAL)
        INSERT INTO SyncRunningTransfersHelper2 (subreqId, reqId, fileid, nsHost, errorCode, errorMsg)
        VALUES (SR.subreqId, varReqId, varFileid, varNsHost, 1015, 'Transfer has been killed while running');
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- not even the request exists any longer in the DB. It may have disappeared meanwhile
        -- as we didn't take any lock yet. Fine, ignore and move on.
        NULL;
      END;
    END;
  END LOOP;
  -- fail the transfers that are no more running
  SELECT subreqId, errorCode, errorMsg BULK COLLECT
    INTO killedTransfers, errnos, errmsg
    FROM SyncRunningTransfersHelper2;
  -- Note that the next call will commit (even once per transfer to kill)
  -- This is ok as SyncRunningTransfersHelper2 was declared "ON COMMIT PRESERVE ROWS" and
  -- is a temporary table so it's content is only visible to our connection.
  transferFailedSafe(killedTransfers, errnos, errmsg);
  -- and return list of transfers that have been failed, for logging purposes
  OPEN killedTransfersCur FOR
    SELECT subreqId, reqId, fileid, nsHost FROM SyncRunningTransfersHelper2;
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
