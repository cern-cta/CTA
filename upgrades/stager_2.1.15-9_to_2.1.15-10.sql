/******************************************************************************
 *                 stager_2.1.15-8_to_2.1.15-9.sql
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
 * This script upgrades a CASTOR v2.1.15-9 STAGER database to v2.1.15-10
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
   WHERE schemaVersion = '2_1_15_0'
     AND release = '2_1_15_10'
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
     AND release LIKE '2_1_15_9%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_15_0', '2_1_15_10', 'TRANSPARENT');
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
  FILE_DROPPED_BY_CLEANING     CONSTANT VARCHAR2(2048) := 'deleteOutOfDateStageOutDCs: File was dropped by internal cleaning';
  PUTDONE_ENFORCED_BY_CLEANING CONSTANT VARCHAR2(2048) := 'deleteOutOfDateStageOutDCs: PutDone enforced by internal cleaning';
  DELETING_REQUESTS            CONSTANT VARCHAR2(2048) := 'deleteTerminatedRequests: Cleaning up completed requests';
  D2D_DROPPED_BY_CLEANING      CONSTANT VARCHAR2(2048) := 'deleteStaleDisk2DiskCopyJobs: D2D job removed by internal cleaning';
  
  DBJOB_UNEXPECTED_EXCEPTION   CONSTANT VARCHAR2(2048) := 'Unexpected exception caught in DB job';

  MIGMOUNT_NO_FILE             CONSTANT VARCHAR2(2048) := 'startMigrationMounts: failed migration mount creation due to lack of files';
  MIGMOUNT_AGE_NO_FILE         CONSTANT VARCHAR2(2048) := 'startMigrationMounts: failed migration mount creation base on age due to lack of files';
  MIGMOUNT_NEW_MOUNT           CONSTANT VARCHAR2(2048) := 'startMigrationMounts: created new migration mount';
  MIGMOUNT_NEW_MOUNT_AGE       CONSTANT VARCHAR2(2048) := 'startMigrationMounts: created new migration mount based on age';
  MIGMOUNT_NOACTION            CONSTANT VARCHAR2(2048) := 'startMigrationMounts: no need for new migration mount';

  RECMOUNT_NEW_MOUNT           CONSTANT VARCHAR2(2048) := 'startRecallMounts: created new recall mount';
  RECMOUNT_FAILED_NEW_MOUNT    CONSTANT VARCHAR2(2048) := 'startRecallMounts: not creating mount that would have been empty (possible issue with destination diskpools)';
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
  RECALL_NOT_FOUND             CONSTANT VARCHAR2(2048) := 'Unable to identify recall, giving up';
  RECALL_INVALID_PATH          CONSTANT VARCHAR2(2048) := 'setFileRecalled: unable to parse input path, giving up';
  RECALL_COMPLETED_DB          CONSTANT VARCHAR2(2048) := 'setFileRecalled: db updates after full recall completed';
  RECALL_FILE_OVERWRITTEN      CONSTANT VARCHAR2(2048) := 'setFileRecalled: file was overwritten during recall, restarting from scratch or skipping repack';
  RECALL_FILE_DROPPED          CONSTANT VARCHAR2(2048) := 'checkRecallInNS: file was dropped from namespace during recall, giving up';
  RECALL_BAD_CHECKSUM          CONSTANT VARCHAR2(2048) := 'checkRecallInNS: bad checksum detected, will retry if allowed';
  RECALL_SEG_INCONSISTENT      CONSTANT VARCHAR2(2048) := 'checkRecallInNS: inconsistency detected at segment level, will retry if allowed';
  RECALL_CREATED_CHECKSUM      CONSTANT VARCHAR2(2048) := 'checkRecallInNS: created missing checksum in the namespace';
  RECALL_FAILED                CONSTANT VARCHAR2(2048) := 'setBulkFileRecallResult: recall process failed, will retry if allowed';
  RECALL_PERMANENTLY_FAILED    CONSTANT VARCHAR2(2048) := 'setFileRecalled: recall process failed permanently';
  BULK_RECALL_COMPLETED        CONSTANT VARCHAR2(2048) := 'setBulkFileRecallResult: bulk recall completed';
  
  MIGRATION_CANCEL_BY_VID      CONSTANT VARCHAR2(2048) := 'Canceling tape migration for given VID';
  MIGRATION_COMPLETED          CONSTANT VARCHAR2(2048) := 'setFileMigrated: db updates after full migration completed';
  MIGRATION_NOT_FOUND          CONSTANT VARCHAR2(2048) := 'Unable to identify migration, giving up';
  MIGRATION_RETRY              CONSTANT VARCHAR2(2048) := 'setBulkFilesMigrationResult: migration failed, will retry if allowed';
  MIGRATION_FILE_DROPPED       CONSTANT VARCHAR2(2048) := 'failFileMigration: file was dropped or modified during migration, giving up';
  MIGRATION_SUPERFLUOUS_COPY   CONSTANT VARCHAR2(2048) := 'failFileMigration: file already had enough copies on tape, ignoring new segment';
  MIGRATION_FAILED             CONSTANT VARCHAR2(2048) := 'failFileMigration: migration to tape failed for this file, giving up';
  MIGRATION_FAILED_NOT_FOUND   CONSTANT VARCHAR2(2048) := 'failFileMigration: file not found when failing migration';
  BULK_MIGRATION_COMPLETED     CONSTANT VARCHAR2(2048) := 'setBulkFilesMigrationResult: bulk migration completed';

  REPACK_SUBMITTED             CONSTANT VARCHAR2(2048) := 'New Repack request submitted';
  REPACK_ABORTING              CONSTANT VARCHAR2(2048) := 'Aborting Repack request';
  REPACK_ABORTED               CONSTANT VARCHAR2(2048) := 'Repack request aborted';
  REPACK_ABORTED_FAILED        CONSTANT VARCHAR2(2048) := 'Aborting Repack request failed, dropping it';
  REPACK_STARTED               CONSTANT VARCHAR2(2048) := 'repackManager: Repack process started';
  REPACK_JOB_STATS             CONSTANT VARCHAR2(2048) := 'repackManager: Repack processes statistics';
  REPACK_UNEXPECTED_EXCEPTION  CONSTANT VARCHAR2(2048) := 'handleRepackRequest: unexpected exception caught';
  REPACK_COMPLETED             CONSTANT VARCHAR2(2048) := 'Repack completed successfully';
  REPACK_FAILED                CONSTANT VARCHAR2(2048) := 'Repack ended with failures';

  DRAINING_REFILL              CONSTANT VARCHAR2(2048) := 'drainRunner: Creating new replication jobs';

  DELETEDISKCOPY_RECALL        CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy was lost, about to recall from tape';
  DELETEDISKCOPY_REPLICATION   CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy was lost, about to replicate from another pool';
  DELETEDISKCOPY_LOST          CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: file was LOST and is being dropped from the system';
  DELETEDISKCOPY_GC            CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy is being garbage collected';
  DELETEDISKCOPY_NOOP          CONSTANT VARCHAR2(2048) := 'deleteDiskCopy: diskCopy could not be garbage collected';

  STAGER_GET                   CONSTANT VARCHAR2(2048) := 'Get Request';
  STAGER_PUT                   CONSTANT VARCHAR2(2048) := 'Put Request';
  STAGER_PREPARETOGET          CONSTANT VARCHAR2(2048) := 'PrepareToGet Request';
  STAGER_PREPARETOPUT          CONSTANT VARCHAR2(2048) := 'PrepareToPut Request';

  STAGER_D2D_TRIGGERED         CONSTANT VARCHAR2(2048) := 'Triggering DiskCopy replication';
  STAGER_WAITSUBREQ            CONSTANT VARCHAR2(2048) := 'Request moved to Wait';
  STAGER_UNABLETOPERFORM       CONSTANT VARCHAR2(2048) := 'Unable to perform request, notifying user';
  STAGER_RECREATION_IMPOSSIBLE CONSTANT VARCHAR2(2048) := 'Impossible to recreate CastorFile';
  STAGER_CASTORFILE_RECREATION CONSTANT VARCHAR2(2048) := 'Recreating CastorFile';
  STAGER_GET_REPLICATION       CONSTANT VARCHAR2(2048) := 'Triggering internal DiskCopy replication';
  STAGER_GET_REPLICATION_FAIL  CONSTANT VARCHAR2(2048) := 'Triggering internal DiskCopy replication failed';
  STAGER_DISKCOPY_FOUND        CONSTANT VARCHAR2(2048) := 'Available DiskCopy found';
  STAGER_DELETED_WRITTENTO     CONSTANT VARCHAR2(2048) := 'File was deleted while it was written to. Giving up with migration';
  STAGER_PUTSTART              CONSTANT VARCHAR2(2048) := 'putStart completed successfully';
  STAGER_PUTENDED              CONSTANT VARCHAR2(2048) := 'putEnded completed successfully';
  STAGER_GETSTART              CONSTANT VARCHAR2(2048) := 'getStart completed successfully';
  STAGER_GETENDED              CONSTANT VARCHAR2(2048) := 'getEnded completed successfully';

  NS_PROCESSING_COMPLETE       CONSTANT VARCHAR2(2048) := 'Processing complete';
  NS_CLOSEX_ERROR              CONSTANT VARCHAR2(2048) := 'Error closing file';

  REPORT_HEART_BEAT_RESUMED    CONSTANT VARCHAR2(2048) := 'Heartbeat resumed for diskserver, status changed to PRODUCTION';
  
  D2D_CREATING_JOB             CONSTANT VARCHAR2(2048) := 'Created new Disk2DiskCopyJob';
  D2D_CANCELED_AT_START        CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Replication request canceled while queuing in scheduler or transfer already started';
  D2D_MULTIPLE_COPIES_ON_DS    CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Multiple copies of this file already found on this diskserver';
  D2D_SOURCE_GONE              CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Source has disappeared while queuing in scheduler, retrying';
  D2D_SRC_DISABLED             CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Source diskserver/filesystem was DISABLED meanwhile';
  D2D_DEST_NOT_PRODUCTION      CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart : Destination diskserver/filesystem not in PRODUCTION any longer';
  D2D_START_OK                 CONSTANT VARCHAR2(2048) := 'disk2DiskCopyStart completed successfully';
  D2D_D2DDONE_CANCEL           CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : Invalidating new copy as job was canceled';
  D2D_D2DDONE_OK               CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : Replication successful';
  D2D_D2DDONE_RETRIED          CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : Retrying disk to disk copy';
  D2D_D2DDONE_NORETRY          CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : no retry, giving up';
  D2D_D2DFAILED                CONSTANT VARCHAR2(2048) := 'disk2DiskCopyEnded : replication failed';
  REBALANCING_START            CONSTANT VARCHAR2(2048) := 'rebalancing : starting';
  REBALANCING_STOP             CONSTANT VARCHAR2(2048) := 'rebalancing : stopping';
END dlf;
/
CREATE OR REPLACE PACKAGE dconst
AS

  CASTORFILE_NOTONTAPE        CONSTANT PLS_INTEGER :=  0;
  CASTORFILE_ONTAPE           CONSTANT PLS_INTEGER :=  1;
  CASTORFILE_DISKONLY         CONSTANT PLS_INTEGER :=  2;

  DISKCOPY_VALID              CONSTANT PLS_INTEGER :=  0;
  DISKCOPY_FAILED             CONSTANT PLS_INTEGER :=  4;
  DISKCOPY_WAITFS             CONSTANT PLS_INTEGER :=  5;
  DISKCOPY_STAGEOUT           CONSTANT PLS_INTEGER :=  6;
  DISKCOPY_INVALID            CONSTANT PLS_INTEGER :=  7;
  DISKCOPY_BEINGDELETED       CONSTANT PLS_INTEGER :=  9;
  DISKCOPY_WAITFS_SCHEDULING  CONSTANT PLS_INTEGER := 11;

  DISKSERVER_PRODUCTION       CONSTANT PLS_INTEGER := 0;
  DISKSERVER_DRAINING         CONSTANT PLS_INTEGER := 1;
  DISKSERVER_DISABLED         CONSTANT PLS_INTEGER := 2;
  DISKSERVER_READONLY         CONSTANT PLS_INTEGER := 3;

  FILESYSTEM_PRODUCTION       CONSTANT PLS_INTEGER := 0;
  FILESYSTEM_DRAINING         CONSTANT PLS_INTEGER := 1;
  FILESYSTEM_DISABLED         CONSTANT PLS_INTEGER := 2;
  FILESYSTEM_READONLY         CONSTANT PLS_INTEGER := 3;
  
  DRAININGJOB_SUBMITTED       CONSTANT PLS_INTEGER := 0;
  DRAININGJOB_STARTING        CONSTANT PLS_INTEGER := 1;
  DRAININGJOB_RUNNING         CONSTANT PLS_INTEGER := 2;
  DRAININGJOB_FAILED          CONSTANT PLS_INTEGER := 4;
  DRAININGJOB_FINISHED        CONSTANT PLS_INTEGER := 5;

  DRAIN_FILEMASK_NOTONTAPE    CONSTANT PLS_INTEGER := 0;
  DRAIN_FILEMASK_ALL          CONSTANT PLS_INTEGER := 1;
  
  SUBREQUEST_START            CONSTANT PLS_INTEGER :=  0;
  SUBREQUEST_RESTART          CONSTANT PLS_INTEGER :=  1;
  SUBREQUEST_RETRY            CONSTANT PLS_INTEGER :=  2;
  SUBREQUEST_WAITSCHED        CONSTANT PLS_INTEGER :=  3;
  SUBREQUEST_WAITTAPERECALL   CONSTANT PLS_INTEGER :=  4;
  SUBREQUEST_WAITSUBREQ       CONSTANT PLS_INTEGER :=  5;
  SUBREQUEST_READY            CONSTANT PLS_INTEGER :=  6;
  SUBREQUEST_FAILED           CONSTANT PLS_INTEGER :=  7;
  SUBREQUEST_FINISHED         CONSTANT PLS_INTEGER :=  8;
  SUBREQUEST_FAILED_FINISHED  CONSTANT PLS_INTEGER :=  9;
  SUBREQUEST_ARCHIVED         CONSTANT PLS_INTEGER := 11;
  SUBREQUEST_REPACK           CONSTANT PLS_INTEGER := 12;
  SUBREQUEST_READYFORSCHED    CONSTANT PLS_INTEGER := 13;

  GETNEXTSTATUS_NOTAPPLICABLE CONSTANT PLS_INTEGER :=  0;
  GETNEXTSTATUS_FILESTAGED    CONSTANT PLS_INTEGER :=  1;
  GETNEXTSTATUS_NOTIFIED      CONSTANT PLS_INTEGER :=  2;

  DISKPOOLQUERYTYPE_DEFAULT   CONSTANT PLS_INTEGER :=  0;
  DISKPOOLQUERYTYPE_AVAILABLE CONSTANT PLS_INTEGER :=  1;
  DISKPOOLQUERYTYPE_TOTAL     CONSTANT PLS_INTEGER :=  2;

  DISKPOOLSPACETYPE_FREE      CONSTANT PLS_INTEGER :=  0;
  DISKPOOLSPACETYPE_CAPACITY  CONSTANT PLS_INTEGER :=  1;

  GCTYPE_AUTO                 CONSTANT PLS_INTEGER :=  0;
  GCTYPE_USER                 CONSTANT PLS_INTEGER :=  1;
  GCTYPE_TOOMANYREPLICAS      CONSTANT PLS_INTEGER :=  2;
  GCTYPE_DRAINING             CONSTANT PLS_INTEGER :=  3;
  GCTYPE_NSSYNCH              CONSTANT PLS_INTEGER :=  4;
  GCTYPE_OVERWRITTEN          CONSTANT PLS_INTEGER :=  5;
  GCTYPE_ADMIN                CONSTANT PLS_INTEGER :=  6;
  GCTYPE_FAILEDD2D            CONSTANT PLS_INTEGER :=  7;
  GCTYPE_FAILEDRECALL         CONSTANT PLS_INTEGER :=  8;
  
  DELDC_LOST                  CONSTANT PLS_INTEGER :=  4;
  DELDC_NOOP                  CONSTANT PLS_INTEGER :=  6;

  DISK2DISKCOPYJOB_PENDING    CONSTANT PLS_INTEGER :=  0;
  DISK2DISKCOPYJOB_SCHEDULED  CONSTANT PLS_INTEGER :=  1;
  DISK2DISKCOPYJOB_RUNNING    CONSTANT PLS_INTEGER :=  2;

  REPLICATIONTYPE_USER        CONSTANT PLS_INTEGER :=  0;
  REPLICATIONTYPE_INTERNAL    CONSTANT PLS_INTEGER :=  1;
  REPLICATIONTYPE_DRAINING    CONSTANT PLS_INTEGER :=  2;
  REPLICATIONTYPE_REBALANCE   CONSTANT PLS_INTEGER :=  3;

END dconst;
/

CREATE OR REPLACE FUNCTION checkRecallInNS(inCfId IN INTEGER,
                                           inMountTransactionId IN INTEGER,
                                           inVID IN VARCHAR2,
                                           inCopyNb IN INTEGER,
                                           inFseq IN INTEGER,
                                           inFileId IN INTEGER,
                                           inNsHost IN VARCHAR2,
                                           inCksumName IN VARCHAR2,
                                           inCksumValue IN INTEGER,
                                           inLastOpenTime IN NUMBER,
                                           inReqId IN VARCHAR2,
                                           inLogContext IN VARCHAR2) RETURN BOOLEAN AS
  varNSOpenTime NUMBER;
  varNSSize INTEGER;
  varNSCsumtype VARCHAR2(2048);
  varNSCsumvalue VARCHAR2(2048);
  varNSSegSize INTEGER;
  varNSSegCsumtype VARCHAR2(2048);
  varNSSegCsumvalue NUMBER;
BEGIN
  -- retrieve data from the namespace: note the truncation of stagerTime to 5 digits.
  -- This is needed for consistency with the stager code that uses the OCCI API and thus
  -- loses precision when recuperating 64 bits integers into doubles
  -- (lack of support for 64 bits numbers in OCCI).
  SELECT TRUNC(stagerTime,5), csumtype, csumvalue, filesize
    INTO varNSOpenTime, varNSCsumtype, varNSCsumvalue, varNSSize
    FROM Cns_File_Metadata@RemoteNS
   WHERE fileid = inFileId;
  -- was the file overwritten in the meantime ?
  IF varNSOpenTime > inLastOpenTime THEN
    -- yes ! reset it and thus restart the recall from scratch
    resetOverwrittenCastorFile(inCfId, varNSOpenTime, varNSSize);
    -- in case of repack, just stop and archive the corresponding request(s) as we're not interested
    -- any longer (the original segment disappeared). This potentially stops the entire recall process.
    archiveOrFailRepackSubreq(inCfId, serrno.ENSFILECHG);
    -- log "setFileRecalled : file was overwritten during recall, restarting from scratch or skipping repack"
    logToDLF(inReqId, dlf.LVL_NOTICE, dlf.RECALL_FILE_OVERWRITTEN, inFileId, inNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' NSOpenTime=' || TRUNC(varNSOpenTime, 6) ||
             ' NsOpenTimeAtStager=' || TRUNC(inLastOpenTime, 6) ||' '|| inLogContext);
    RETURN FALSE;
  END IF;

  -- is the checksum set in the namespace at the file level ?
  IF varNSCsumtype IS NOT NULL THEN
    -- is the checksum matching at the file level ?
    IF inCksumName = 'adler32' AND varNSCsumtype = 'AD' AND
       TRIM(TO_CHAR(inCksumValue, 'xxxxxxxx')) != TRIM(varNSCsumvalue) THEN
      -- not matching ! log "checkRecallInNS : bad checksum detected, will retry if allowed"
      logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_BAD_CHECKSUM, inFileId, inNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
               ' fseq=' || TO_CHAR(inFseq) || ' copyNb=' || TO_CHAR(inCopyNb) || ' checksumType=' || inCksumName ||
               ' expectedChecksumValue=' || TRIM(varNSCsumvalue) ||
               ' checksumValue=' || TRIM(TO_CHAR(inCksumValue, 'xxxxxxxx')) ||' '|| inLogContext);
      retryOrFailRecall(inCfId, inVID, inReqId, inLogContext);
      UPDATE DiskCopy
         SET status = dconst.DISKCOPY_INVALID, gctype = dconst.GCTYPE_FAILEDRECALL
       WHERE castorFile = inCfId
         AND status = dconst.DISKCOPY_VALID;
      RETURN FALSE;
    END IF;
  END IF;

  -- retrieve segment checksum from the namespace and check consistency
  SELECT checksum_name, checksum, segsize
    INTO varNSSegCsumtype, varNSSegCsumvalue, varNSSegSize
    FROM Cns_Seg_Metadata@RemoteNS
   WHERE s_fileid = inFileId AND copyno = inCopyNb;
  -- is the checksum and size matching at the segment level ?
  IF inCksumName != 'adler32' OR varNSSegCsumtype != 'adler32' OR
     inCksumValue != varNSSegCsumvalue OR varNSSegSize != varNSSize THEN
    -- not consistent ! log "checkRecallInNS : inconsistency detected at segment level, will retry if allowed"
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_SEG_INCONSISTENT, inFileId, inNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' copyNb=' || TO_CHAR(inCopyNb) || ' checksumType=' || inCksumName ||
             ' expectedChecksumValue=' || varNSSegCsumvalue ||
             ' checksumValue=' || TRIM(TO_CHAR(inCksumValue, 'xxxxxxxx')) ||
             ' fileSize=' || varNSSize || ' segSize=' || varNSSegSize ||' '|| inLogContext);
    retryOrFailRecall(inCfId, inVID, inReqId, inLogContext);
    UPDATE DiskCopy
       SET status = dconst.DISKCOPY_INVALID, gctype = dconst.GCTYPE_FAILEDRECALL
     WHERE castorFile = inCfId
       AND status = dconst.DISKCOPY_VALID;
    RETURN FALSE;
  END IF;

  RETURN TRUE;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- file got dropped from the namespace, recall should be cancelled
  deleteRecallJobs(inCfId);
  -- potentially terminate repack requests
  archiveOrFailRepackSubreq(inCfId, serrno.ENOENT);
  -- and fail remaining requests
  UPDATE SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOENT,
           errorMessage = 'File was removed during recall'
     WHERE castorFile = inCfId
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
  -- log "checkRecallInNS : file was dropped from namespace during recall, giving up"
  logToDLF(inReqId, dlf.LVL_NOTICE, dlf.RECALL_FILE_DROPPED, inFileId, inNsHost, 'tapegatewayd',
           'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
           ' fseq=' || TO_CHAR(inFseq) || ' CFLastOpenTime=' || TO_CHAR(inLastOpenTime) || ' ' || inLogContext);
  RETURN FALSE;
END;
/

CREATE OR REPLACE PROCEDURE disk2DiskCopyEnded
(inTransferId IN VARCHAR2, inDestDsName IN VARCHAR2, inDestPath IN VARCHAR2,
 inReplicaFileSize IN INTEGER, inCksumValue IN VARCHAR2, inErrorCode IN INTEGER, inErrorMessage IN VARCHAR2) AS
  varCfId INTEGER;
  varUid INTEGER := -1;
  varGid INTEGER := -1;
  varDestDcId INTEGER;
  varSrcDcId INTEGER;
  varDropSource INTEGER;
  varDestSvcClass INTEGER;
  varRepType INTEGER;
  varRetryCounter INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
  varFCksum VARCHAR2(10);
  varFileSize INTEGER;
  varDestPath VARCHAR2(2048);
  varDestFsId INTEGER;
  varDestDpId INTEGER;
  varDcGcWeight NUMBER := 0;
  varDcImportance NUMBER := 0;
  varNewDcStatus INTEGER := dconst.DISKCOPY_VALID;
  varLogMsg VARCHAR2(2048) := dlf.D2D_D2DDONE_OK;
  varComment VARCHAR2(2048);
  varDrainingJob VARCHAR2(2048);
  varErrorMessage VARCHAR2(2048) := inErrorMessage;
BEGIN
  BEGIN
    IF inDestPath IS NOT NULL THEN
      -- Parse destination path
      parsePath(inDestDsName ||':'|| inDestPath, varDestFsId, varDestDpId, varDestPath, varDestDcId, varFileId, varNsHost);
    -- ELSE we are called because of an error at start: try to gather information
    -- from the Disk2DiskCopyJob entry and fail accordingly.
    END IF;
    -- Get data from the Disk2DiskCopyJob
    SELECT castorFile, ouid, ogid, destDcId, srcDcId, destSvcClass, replicationType,
           dropSource, retryCounter, drainingJob
      INTO varCfId, varUid, varGid, varDestDcId, varSrcDcId, varDestSvcClass, varRepType,
           varDropSource, varRetryCounter, varDrainingJob
      FROM Disk2DiskCopyJob
     WHERE transferId = inTransferId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- The job was probably canceled: so our brand new copy
    -- has to be created as invalid to trigger GC, and linked
    -- to the (hopefully existing) correct CastorFile.
    varNewDcStatus := dconst.DISKCOPY_INVALID;
    varLogMsg := dlf.D2D_D2DDONE_CANCEL;
    BEGIN
      SELECT id INTO varCfId
        FROM CastorFile
       WHERE fileId = varFileId;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Here we also lost the CastorFile: this could happen
      -- if the GC ran meanwhile. Fail and leave dark data behind,
      -- the GC will eventually catch up. A full solution would be
      -- to gather here all missing information to correctly
      -- recreate the CastorFile entry, but this is too complex
      -- for what we would gain.
      logToDLF(NULL, dlf.LVL_NOTICE, dlf.D2D_D2DDONE_CANCEL, varFileId, varNsHost, 'transfermanagerd',
               'transferId=' || inTransferId || ' errorMessage="CastorFile disappeared, giving up"');
      RETURN;
    END;
  END;
  -- on success, check the filesize and the checksum
  IF varErrorMessage IS NULL THEN
    DECLARE
      BadChecksum EXCEPTION;
      PRAGMA EXCEPTION_INIT (BadChecksum, -6502);
    BEGIN
      SELECT csumValue INTO varFCksum
        FROM Cns_file_metadata@remoteNS
       WHERE fileId = varFileId;
      IF inReplicaFileSize != varFileSize OR to_number(inCksumValue, 'XXXXXXXX') != to_number(varFCksum, 'XXXXXXXX') THEN
        -- replication went wrong !
        varNewDcStatus := dconst.DISKCOPY_INVALID;
        varErrorMessage := 'File size/checksum mismatch during replication, the source file is probably corrupted';
      END IF;
    EXCEPTION WHEN BadChecksum THEN
      -- the checksum is not a number?!
      varNewDcStatus := dconst.DISKCOPY_INVALID;
      varErrorMessage := 'Invalid checksum value "' || inCksumValue || '", giving up';
    END;
  END IF;
  -- lock the castor file (and get logging info)
  SELECT fileid, nsHost, fileSize INTO varFileId, varNsHost, varFileSize
    FROM CastorFile
   WHERE id = varCfId
     FOR UPDATE;
  -- Log success or failure of the replication
  IF varLogMsg = dlf.D2D_D2DDONE_OK AND varErrorMessage IS NOT NULL THEN
    varLogMsg := dlf.D2D_D2DFAILED;
  END IF;
  varComment := 'SUBREQID=' || inTransferId ||
         ' destSvcClass=' || getSvcClassName(varDestSvcClass) ||
         ' destDcId=' || TO_CHAR(varDestDcId) || ' destPath="' || inDestPath ||
         '" euid=' || TO_CHAR(varUid) || ' egid=' || TO_CHAR(varGid) ||
         ' fileSize=' || TO_CHAR(varFileSize) || ' checksum=' || inCksumValue;
  IF varErrorMessage IS NOT NULL THEN
    varComment := varComment || ' replicaFileSize=' || TO_CHAR(inReplicaFileSize) ||
                  ' errorCode=' || inErrorCode || ' errorMessage="' || varErrorMessage || '"';
    varNewDcStatus := dconst.DISKCOPY_INVALID;
  END IF;
  logToDLF(NULL, dlf.LVL_SYSTEM, varLogMsg, varFileId, varNsHost, 'transfermanagerd', varComment);
  IF varErrorMessage IS NULL THEN
    -- compute GcWeight and importance of the new copy
    DECLARE
      varGcwProc VARCHAR2(2048);
    BEGIN
      varGcwProc := castorGC.getCopyWeight(varDestSvcClass);
      EXECUTE IMMEDIATE
        'BEGIN :newGcw := ' || varGcwProc || '(:size); END;'
        USING OUT varDcGcWeight, IN varFileSize;
      SELECT /*+ INDEX_RS_ASC (DiskCopy I_DiskCopy_CastorFile) */
             COUNT(*)+1 INTO varDCImportance FROM DiskCopy
       WHERE castorFile=varCfId AND status = dconst.DISKCOPY_VALID;
    END;
  END IF;
  -- create the new DiskCopy in all cases
  -- we may try twice in case we get a constraint violated and the violation disappears (see details below)
  FOR attempts IN 1..2 LOOP
    DECLARE
      CONSTRAINT_VIOLATED EXCEPTION;
      PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
    BEGIN
      INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                            owneruid, ownergid, id, gcType, fileSystem, datapool, castorFile,
                            status, importance)
      VALUES (varDestPath, varDcGcWeight, getTime(), getTime(), varFileSize, 0,
              varUid, varGid, varDestDcId,
              CASE varNewDcStatus WHEN dconst.DISKCOPY_INVALID
                                  THEN dconst.GCTYPE_FAILEDD2D
                                  ELSE NULL END,
              varDestFsId, varDestDpId, varCfId, varNewDcStatus, varDCImportance);
      EXIT;
    EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- we do not manage to create the DiskCopy as another exists with the same id
      -- this can be due to a kill transfer that came during the transfer and we are
      -- now processing the end of transfer (a failure) while we have already done
      -- the job during the kill transfer. We will however double check by looking
      -- at the status and path of the existing DiskCopy
      DECLARE
        varStatus NUMBER;
        varPath VARCHAR2(2048);
      BEGIN
        SELECT path, status INTO varPath, varStatus FROM DiskCopy WHERE id = varDestDcId;
        IF varPath != varDestPath OR
           varStatus NOT IN (dconst.DISKCOPY_FAILED, dconst.DISKCOPY_INVALID, dconst.DISKCOPY_BEINGDELETED) THEN
          -- not the expected case, reraise the exception
          logToDLF(NULL, dlf.LVL_SYSTEM, 'Constraint violation debugging', varFileId, varNsHost,
                   'transfermanagerd', varComment || ' varPath=' || varPath || ' varDestPath=' ||
                   varDestPath || ' varStatus=' || varStatus || ' varNewDcStatus=' || varNewDcStatus);
          RAISE;
        END IF;
        -- Expected case, we are happy, exit the loop
        EXIT;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- the colliding DiskCopy has disappeared ! Let's go back to our insert, in case
        -- it was not the case we have described. We do not have anything to do, just let
        -- the second attempt go through
        -- Note that there will be no third attempt as nothing can recreate the DiskCopy
        -- with that id anymore
        NULL;
      END;
    END;
  END LOOP;
  -- if success, restart waiting requests, cleanup and handle replicate on close
  IF varErrorMessage IS NULL THEN
    -- In case of draining, update DrainingJob: this is done before the rest to respect the locking order
    IF varDrainingJob IS NOT NULL THEN
      updateDrainingJobOnD2dEnd(varDrainingJob, varFileSize, False);
    END IF;
    -- Wake up waiting subrequests
    UPDATE SubRequest
       SET status = dconst.SUBREQUEST_RESTART,
           getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
           lastModificationTime = getTime()
     WHERE status = dconst.SUBREQUEST_WAITSUBREQ
       AND castorfile = varCfId;
    alertSignalNoLock('wakeUpJobReqSvc');
    -- delete the disk2diskCopyJob
    DELETE FROM Disk2DiskCopyJob WHERE transferId = inTransferId;
    -- In case of valid new copy
    IF varDropSource = 1 THEN
      -- drop source if requested
      UPDATE DiskCopy
         SET status = dconst.DISKCOPY_INVALID, gcType=dconst.GCTYPE_DRAINING
       WHERE id = varSrcDcId;
    ELSE
      -- update importance of other DiskCopies if it's an additional one
      UPDATE DiskCopy SET importance = varDCImportance WHERE castorFile = varCfId;
    END IF;
    -- trigger the creation of additional copies of the file, if any
    replicateOnClose(varCfId, varUid, varGid, varDestSvcClass);
  ELSE
    -- failure
    DECLARE
      varMaxNbD2dRetries INTEGER := to_number(getConfigOption('D2dCopy', 'MaxNbRetries', 2));
      varNewDestDcId INTEGER := ids_seq.nextval();
    BEGIN
      -- shall we try again ?
      -- we should not when the job was deliberately killed, neither when we reach the maximum
      -- number of attempts
      IF varRetryCounter + 1 < varMaxNbD2dRetries AND inErrorCode != serrno.ESTKILLED THEN
        -- yes, so let's restart the Disk2DiskCopyJob
        UPDATE Disk2DiskCopyJob
           SET status = dconst.DISK2DISKCOPYJOB_PENDING,
               destDcId = varNewDestDcId,
               retryCounter = varRetryCounter + 1
         WHERE transferId = inTransferId;
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_D2DDONE_RETRIED, varFileId, varNsHost, 'transfermanagerd', varComment ||
                 ' RetryNb=' || TO_CHAR(varRetryCounter+1) || ' maxNbRetries=' || TO_CHAR(varMaxNbD2dRetries));
      ELSE
        -- No retry. In case of draining, update DrainingJob
        IF varDrainingJob IS NOT NULL THEN
          updateDrainingJobOnD2dEnd(varDrainingJob, varFileSize, True);
        END IF;
        -- and delete the disk to disk copy job
        BEGIN
          DELETE FROM Disk2DiskCopyJob WHERE transferId = inTransferId;
          -- and remember the error in case of draining
          IF varDrainingJob IS NOT NULL THEN
            INSERT INTO DrainingErrors (drainingJob, errorMsg, fileId, nsHost, castorFile, timeStamp)
            VALUES (varDrainingJob, varErrorMessage, varFileId, varNsHost, varCfId, getTime());
          END IF;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- the Disk2DiskCopyJob was already dropped (e.g. because of an interrupted draining)
          -- in such a case, forget about the error
          NULL;
        END;
        logToDLF(NULL, dlf.LVL_NOTICE, dlf.D2D_D2DDONE_NORETRY, varFileId, varNsHost, 'transfermanagerd', varComment ||
                 ' maxNbRetries=' || TO_CHAR(varMaxNbD2dRetries));
        -- Fail waiting subrequests
        UPDATE SubRequest
           SET status = dconst.SUBREQUEST_FAILED,
               lastModificationTime = getTime(),
               errorCode = serrno.SEINTERNAL,
               errorMessage = 'Disk to disk copy failed after ' || TO_CHAR(varMaxNbD2dRetries) ||
                              ' retries. Last error was : ' || varErrorMessage
         WHERE status = dconst.SUBREQUEST_WAITSUBREQ
           AND castorfile = varCfId;
      END IF;
    END;
  END IF;
END;
/

DELETE FROM ObjStatus WHERE Object = 'DiskCopy' AND Field = 'gcType';
BEGIN
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_AUTO, 'GCTYPE_AUTO');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_USER, 'GCTYPE_USER');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_TOOMANYREPLICAS, 'GCTYPE_TOOMANYREPLICAS');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_DRAINING, 'GCTYPE_DRAINING');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_NSSYNCH, 'GCTYPE_NSSYNCH');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_OVERWRITTEN, 'GCTYPE_OVERWRITTEN');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_ADMIN, 'GCTYPE_ADMIN');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_FAILEDD2D, 'GCTYPE_FAILEDD2D');
  setObjStatusName('DiskCopy', 'gcType', dconst.GCTYPE_FAILEDRECALL, 'GCTYPE_FAILEDRECALL');
END;
/
ALTER TABLE DiskCopy DROP CONSTRAINT CK_DiskCopy_GcType;
ALTER TABLE DiskCopy   ADD CONSTRAINT CK_DiskCopy_GcType
  CHECK (gcType IN (0, 1, 2, 3, 4, 5, 6, 7, 8));


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_15_10';
COMMIT;
