/******************************************************************************
 *                 stager_2.1.15-0_to_2.1.15-3.sql
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
 * This script upgrades a CASTOR v2.1.15-0 STAGER database to v2.1.15-3
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
     AND release = '2_1_15_3'
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
     AND (release LIKE '2_1_15_0%' OR release LIKE '2_1_15_1%' OR release LIKE '2_1_15_2%');
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_15_0', '2_1_15_3', 'TRANSPARENT');
COMMIT;

/* Schema changes go here */
/**************************/

/* Fix RecallJob constraint */
alter table RECALLJOB DROP constraint CK_RECALLJOB_STATUS;
alter table RECALLJOB ADD constraint CK_RECALLJOB_STATUS CHECK (status IN (1,2,3,4)) enable novalidate;

DROP VIEW LateMigrationsView;
CREATE VIEW LateMigrationsView AS
  SELECT /*+ LEADING(CF DC FileSystem DiskServer MJ CnsFile)
             USE_NL(CF DC FileSystem DiskServer MJ CnsFile)
             INDEX_RS_ASC(CF I_CastorFile_TapeStatus) INDEX_RS_ASC(DC I_DiskCopy_CastorFile) INDEX_RS_ASC(MJ I_MigrationJob_CastorFile) */
         CF.fileId, CF.lastKnownFileName AS filePath,
         decode(MJ.creationTime, NULL, -1, getTime() - MJ.creationTime) AS mjElapsedTime, nvl(MJ.status, -1) AS mjStatus,
         DC.creationTime AS dcCreationTime, DiskServer.name || ':' || FileSystem.mountPoint || DC.path AS location,
         decode(DiskServer.hwOnline, 0, 'N',
           decode(DiskServer.status, 2, 'N',
             decode(FileSystem.status, 2, 'N', 'Y'))) AS available
    FROM CastorFile CF, DiskCopy DC, MigrationJob MJ, cns_file_metadata@remotens CnsFile,
         FileSystem, DiskServer
   WHERE CF.fileId = CnsFile.fileId
     AND DC.castorFile = CF.id
     AND MJ.castorFile(+) = CF.id
     AND DC.fileSystem = FileSystem.id
     AND FileSystem.diskServer = DiskServer.id
     AND CF.tapeStatus = 0  -- CASTORFILE_NOTONTAPE
     AND DC.status = 0  -- DISKCOPY_VALID
     AND CF.fileSize > 0
     AND DC.creationTime < getTime() - 86400
  UNION
  SELECT /*+ LEADING(CF DC DataPool MJ CnsFile
             USE_NL(CF DC FileSystem DataPool MJ CnsFile)
             INDEX_RS_ASC(CF I_CastorFile_TapeStatus) INDEX_RS_ASC(DC I_DiskCopy_CastorFile) INDEX_RS_ASC(MJ I_MigrationJob_CastorFile) */
         CF.fileId, CF.lastKnownFileName AS filePath,
         decode(MJ.creationTime, NULL, -1, getTime() - MJ.creationTime) AS mjElapsedTime, nvl(MJ.status, -1) AS mjStatus,
         DC.creationTime AS dcCreationTime, 'ceph://' || DataPool.name || ':' || DC.path AS location, 'Y' as available
    FROM CastorFile CF, DiskCopy DC, MigrationJob MJ, DataPool, cns_file_metadata@remotens CnsFile
   WHERE CF.fileId = CnsFile.fileId
     AND DC.castorFile = CF.id
     AND MJ.castorFile(+) = CF.id
     AND DC.dataPool = DataPool.id
     AND CF.tapeStatus = 0  -- CASTORFILE_NOTONTAPE
     AND DC.status = 0  -- DISKCOPY_VALID
     AND CF.fileSize > 0
     AND DC.creationTime < getTime() - 86400
  ORDER BY dcCreationTime DESC;


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

INSERT INTO CastorConfig
  VALUES ('cleaning', 'staleDisk2DiskCopyJobsTimeout', '6', 'Timeout for stuck disk2diskCopyJobs in hours');

/* Changes in the PL/SQL code */
/******************************/

/* PL/SQL method to process bulk Repack abort requests */
CREATE OR REPLACE PROCEDURE abortRepackRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   clientIP IN INTEGER,
   parentUUID IN VARCHAR2,
   rSubResults OUT castor.FileResult_Cur) AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  varCreationTime NUMBER;
  rIpAddress INTEGER;
  rport INTEGER;
  rReqUuid VARCHAR2(2048);
  varVID VARCHAR2(10);
  varStartTime NUMBER;
  varOldStatus INTEGER;
BEGIN
  -- lock and mark the repack request as being aborted
  SELECT status, repackVID INTO varOldStatus, varVID
    FROM StageRepackRequest
   WHERE reqId = parentUUID
   FOR UPDATE;
  IF varOldStatus = tconst.REPACK_SUBMITTED THEN
    -- the request was just submitted, simply drop it from the queue
    DELETE FROM StageRepackRequest WHERE reqId = parentUUID;
    logToDLF(parentUUID, dlf.LVL_SYSTEM, dlf.REPACK_ABORTED, 0, '', 'repackd', 'TPVID=' || varVID);
    COMMIT;
    -- and return an empty cursor - not that nice...
    OPEN rSubResults FOR
      SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
    RETURN;
  END IF;
  UPDATE StageRepackRequest SET status = tconst.REPACK_ABORTING
   WHERE reqId = parentUUID;
  COMMIT;  -- so to make the status change visible
  BEGIN
    varStartTime := getTime();
    logToDLF(parentUUID, dlf.LVL_SYSTEM, dlf.REPACK_ABORTING, 0, '', 'repackd', 'TPVID=' || varVID);
    -- get unique ids for the request and the client and get current time
    SELECT ids_seq.nextval INTO reqId FROM DUAL;
    SELECT ids_seq.nextval INTO clientId FROM DUAL;
    varCreationTime := getTime();
    -- insert the request itself
    INSERT INTO StageAbortRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName,
      userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
    VALUES (0, userName, euid, egid, 0, pid, machine, '', '', uuidgen(),
      varCreationTime, varCreationTime, parentUUID, reqId, 0, clientId);
    -- insert the client information
    INSERT INTO Client (ipAddress, port, version, secure, id)
    VALUES (clientIP, 0, 0, 0, clientId);
    -- process the abort
    processBulkAbort(reqId, rIpAddress, rport, rReqUuid);
    -- mark the repack request as ABORTED
    UPDATE StageRepackRequest SET status = tconst.REPACK_ABORTED WHERE reqId = parentUUID;
    logToDLF(parentUUID, dlf.LVL_SYSTEM, dlf.REPACK_ABORTED, 0, '', 'repackd',
      'TPVID=' || varVID || ' elapsedTime=' || to_char(getTime() - varStartTime));
    -- return all results
    OPEN rSubResults FOR
      SELECT fileId, nsHost, errorCode, errorMessage FROM ProcessBulkRequestHelper;
  EXCEPTION WHEN OTHERS THEN
    -- Something went wrong when aborting: log and fail
    UPDATE StageRepackRequest SET status = tconst.REPACK_FAILED WHERE reqId = parentUUID;
    logToDLF(parentUUID, dlf.LVL_ERROR, dlf.REPACK_ABORTED_FAILED, 0, '', 'repackd', 'TPVID=' || varVID
      || ' errorMessage="' || SQLERRM || '" stackTrace="' || dbms_utility.format_error_backtrace() ||'"');
    COMMIT;
    RAISE;
  END;
END;
/

/* PL/SQL procedure implementing repackManager, the repack job
   that really handles all repack requests */
CREATE OR REPLACE PROCEDURE repackManager AS
  varReqUUID VARCHAR2(36);
  varRepackVID VARCHAR2(10);
  varStartTime NUMBER;
  varTapeStartTime NUMBER;
  varNbRepacks INTEGER := 0;
  varNbFiles INTEGER;
  varNbFailures INTEGER;
BEGIN
  varStartTime := getTime();
  WHILE TRUE LOOP
    varTapeStartTime := getTime();
    BEGIN
      -- get an outstanding repack to start, and lock to prevent
      -- a concurrent abort (there cannot be anyone else).
      -- Note that we include repack requests in status STARTING,
      -- which means requests that got somehow interrupted in their
      -- processing.
      SELECT reqId, repackVID INTO varReqUUID, varRepackVID
        FROM StageRepackRequest
       WHERE id = (SELECT id
                     FROM (SELECT id
                             FROM StageRepackRequest
                            WHERE status IN (tconst.REPACK_SUBMITTED, tconst.REPACK_STARTING)
                            ORDER BY creationTime ASC)
                    WHERE ROWNUM < 2)
         FOR UPDATE;
      -- start the repack for this request: this can take some considerable time
      handleRepackRequest(varReqUUID, varNbFiles, varNbFailures);
      -- log 'Repack process started'
      logToDLF(varReqUUID, dlf.LVL_SYSTEM, dlf.REPACK_STARTED, 0, '', 'repackd',
        'TPVID=' || varRepackVID || ' nbFiles=' || TO_CHAR(varNbFiles)
        || ' nbFailures=' || TO_CHAR(varNbFailures)
        || ' elapsedTime=' || TO_CHAR(getTime() - varTapeStartTime));
      varNbRepacks := varNbRepacks + 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- if no new repack is found to start, terminate
      EXIT;
    END;
  END LOOP;
  IF varNbRepacks > 0 THEN
    -- log some statistics
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REPACK_JOB_STATS, 0, '', 'repackd',
      'nbStarted=' || TO_CHAR(varNbRepacks) || ' elapsedTime=' || TO_CHAR(TRUNC(getTime() - varStartTime)));
  END IF;
END;
/

/* PL/SQL method creating MigrationJobs for missing segments of a file if needed */
/* Can throw a -20100 exception when no route to tape is found for the missing segments */
CREATE OR REPLACE PROCEDURE createMJForMissingSegments(inCfId IN INTEGER,
                                                       inFileSize IN INTEGER,
                                                       inFileClassId IN INTEGER,
                                                       inAllValidCopyNbs IN "numList",
                                                       inAllValidVIDs IN strListTable,
                                                       inNbExistingSegments IN INTEGER,
                                                       inFileId IN INTEGER,
                                                       inNsHost IN VARCHAR2,
                                                       inLogParams IN VARCHAR2) AS
  varExpectedNbCopies INTEGER;
  varCreatedMJs INTEGER := 0;
  varNextCopyNb INTEGER := 1;
  varNb INTEGER;
BEGIN
  -- check whether there are missing segments and whether we should create new ones
  SELECT nbCopies INTO varExpectedNbCopies FROM FileClass WHERE id = inFileClassId;
  IF varExpectedNbCopies > inNbExistingSegments THEN
    -- some copies are missing
    DECLARE
      unused INTEGER;
    BEGIN
      -- check presence of migration jobs for this file
      SELECT id INTO unused FROM MigrationJob WHERE castorFile=inCfId AND ROWNUM < 2;
      -- there are MigrationJobs already, so remigrations were already handled. Nothing to be done
      -- we typically are in a situation where the file was already waiting for recall for
      -- another recall group.
      -- log "detected missing copies on tape, but migrations ongoing"
      logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECALL_MISSING_COPIES_NOOP, inFileId, inNsHost, 'stagerd',
               inLogParams || ' nbMissingCopies=' || TO_CHAR(varExpectedNbCopies-inNbExistingSegments));
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- we need to remigrate this file
      NULL;
    END;
    -- log "detected missing copies on tape"
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_MISSING_COPIES, inFileId, inNsHost, 'stagerd',
             inLogParams || ' nbMissingCopies=' || TO_CHAR(varExpectedNbCopies-inNbExistingSegments));
    -- copies are missing, try to recreate them
    WHILE varExpectedNbCopies > inNbExistingSegments + varCreatedMJs AND varNextCopyNb <= varExpectedNbCopies LOOP
      BEGIN
        -- check whether varNextCopyNb is already in use by a valid copy
        SELECT * INTO varNb FROM TABLE(inAllValidCopyNbs) WHERE COLUMN_VALUE=varNextCopyNb;
        -- this copy number is in use, go to next one
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- copy number is not in use, create a migrationJob using it (may throw exceptions)
        initMigration(inCfId, inFileSize, NULL, NULL, varNextCopyNb, tconst.MIGRATIONJOB_WAITINGONRECALL);
        varCreatedMJs := varCreatedMJs + 1;
        -- log "create new MigrationJob to migrate missing copy"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_MJ_FOR_MISSING_COPY, inFileId, inNsHost, 'stagerd',
                 inLogParams || ' copyNb=' || TO_CHAR(varNextCopyNb));
      END;
      varNextCopyNb := varNextCopyNb + 1;
    END LOOP;
    -- Did we create new copies ?
    IF varExpectedNbCopies > inNbExistingSegments + varCreatedMJs THEN
      -- We did not create enough new copies, this means that we did not find enough
      -- valid copy numbers. Odd... Log something !
      logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_COPY_STILL_MISSING, inFileId, inNsHost, 'stagerd',
               inLogParams || ' nbCopiesStillMissing=' ||
               TO_CHAR(varExpectedNbCopies - inAllValidCopyNbs.COUNT - varCreatedMJs));
    ELSE
      -- Yes, then create migrated segments for the existing segments if there are none
      SELECT count(*) INTO varNb FROM MigratedSegment WHERE castorFile = inCfId;
      IF varNb = 0 THEN
        FOR i IN 1..inAllValidCopyNbs.COUNT LOOP
          INSERT INTO MigratedSegment (castorFile, copyNb, VID)
          VALUES (inCfId, inAllValidCopyNbs(i), inAllValidVIDs(i));
        END LOOP;
      END IF;
    END IF;
  END IF;
END;
/

/* PL/SQL method creating RecallJobs
 * It also creates MigrationJobs for eventually missing segments
 * It returns 0 if successful, else an error code
 */
CREATE OR REPLACE FUNCTION createRecallJobs(inCfId IN INTEGER,
                                            inFileId IN INTEGER,
                                            inNsHost IN VARCHAR2,
                                            inFileSize IN INTEGER,
                                            inFileClassId IN INTEGER,
                                            inRecallGroupId IN INTEGER,
                                            inSvcClassId IN INTEGER,
                                            inEuid IN INTEGER,
                                            inEgid IN INTEGER,
                                            inRequestTime IN NUMBER,
                                            inLogParams IN VARCHAR2) RETURN INTEGER AS
  -- list of all valid segments, whatever the tape status. Used to trigger remigrations
  varAllValidCopyNbs "numList" := "numList"();
  varAllValidVIDs strListTable := strListTable();
  varNbExistingSegments INTEGER := 0;
  -- whether we found a segment at all (valid or not). Used to detect potentially lost files
  varFoundSeg boolean := FALSE;
  varI INTEGER := 1;
  NO_TAPE_ROUTE EXCEPTION;
  PRAGMA EXCEPTION_INIT(NO_TAPE_ROUTE, -20100);
  varErrorMsg VARCHAR2(2048);
BEGIN
  BEGIN
    -- loop over the existing segments
    FOR varSeg IN (SELECT s_fileId as fileId, 0 as lastModTime, copyNo, segSize, 0 as comprSize,
                          Cns_seg_metadata.vid, fseq, blockId, checksum_name, nvl(checksum, 0) as checksum,
                          Cns_seg_metadata.s_status as segStatus, Vmgr_tape_status_view.status as tapeStatus
                     FROM Cns_seg_metadata@remotens, Vmgr_tape_status_view@remotens
                    WHERE Cns_seg_metadata.s_fileid = inFileId
                      AND Vmgr_tape_status_view.VID = Cns_seg_metadata.VID
                    ORDER BY copyno, fsec) LOOP
      varFoundSeg := TRUE;
      -- Is the segment valid
      IF varSeg.segStatus = '-' THEN
        -- Is the segment on a valid tape from recall point of view ?
        IF BITAND(varSeg.tapeStatus, tconst.TAPE_DISABLED) = 0 AND
           BITAND(varSeg.tapeStatus, tconst.TAPE_EXPORTED) = 0 AND
           BITAND(varSeg.tapeStatus, tconst.TAPE_ARCHIVED) = 0 THEN
          -- remember the copy number and tape
          varAllValidCopyNbs.EXTEND;
          varAllValidCopyNbs(varI) := varSeg.copyno;
          varAllValidVIDs.EXTEND;
          varAllValidVIDs(varI) := varSeg.vid;
          varI := varI + 1;
          -- create recallJob
          INSERT INTO RecallJob (id, castorFile, copyNb, recallGroup, svcClass, euid, egid,
                                 vid, fseq, status, fileSize, creationTime, blockId, fileTransactionId)
          VALUES (ids_seq.nextval, inCfId, varSeg.copyno, inRecallGroupId, inSvcClassId,
                  inEuid, inEgid, varSeg.vid, varSeg.fseq, tconst.RECALLJOB_PENDING, inFileSize, inRequestTime,
                  varSeg.blockId, NULL);
          varNbExistingSegments := varNbExistingSegments + 1;
          -- log "created new RecallJob"
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_CREATING_RECALLJOB, inFileId, inNsHost, 'stagerd',
                   inLogParams || ' copyNb=' || TO_CHAR(varSeg.copyno) || ' TPVID=' || varSeg.vid ||
                   ' fseq=' || TO_CHAR(varSeg.fseq || ' FileSize=' || TO_CHAR(inFileSize)));
        ELSE
          -- Should the segment be counted in the count of existing segments ?
          -- In other terms, should we recreate a segment for replacing this one ?
          -- Yes if the segment in on an EXPORTED tape.
          IF BITAND(varSeg.tapeStatus, tconst.TAPE_EXPORTED) = 0 THEN
            -- invalid tape found with segments that are counting for the total count.
            -- "createRecallCandidate: found segment on unusable tape"
            logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_UNUSABLE_TAPE, inFileId, inNsHost, 'stagerd',
                     inLogParams || ' segStatus=OK tapeStatus=' || tapeStatusToString(varSeg.tapeStatus) ||
                     ' recreatingSegment=No');
            varNbExistingSegments := varNbExistingSegments + 1;
          ELSE
            -- invalid tape found with segments that will be completely ignored.
            -- "createRecallCandidate: found segment on unusable tape"
            logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECALL_UNUSABLE_TAPE, inFileId, inNsHost, 'stagerd',
                     inLogParams || ' segStatus=OK tapeStatus=' || tapeStatusToString(varSeg.tapeStatus) ||
                     ' recreatingSegment=Yes');
          END IF;
        END IF;
      ELSE
        -- invalid segment tape found. Log it.
        -- "createRecallCandidate: found unusable segment"
        logToDLF(NULL, dlf.LVL_NOTICE, dlf.RECALL_INVALID_SEGMENT, inFileId, inNsHost, 'stagerd',
                 inLogParams || ' segStatus=' ||
                 CASE varSeg.segStatus WHEN '-' THEN 'OK'
                                       WHEN 'D' THEN 'DISABLED'
                                       ELSE 'UNKNOWN:' || varSeg.segStatus END);
      END IF;
    END LOOP;
  EXCEPTION WHEN OTHERS THEN
    -- log "error when retrieving segments from namespace"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_UNKNOWN_NS_ERROR, inFileId, inNsHost, 'stagerd',
             inLogParams || ' ErrorMessage=' || SQLERRM);
    RETURN serrno.SEINTERNAL;
  END;
  -- If we did not find any valid segment to recall, log a critical error as the file is probably lost
  IF NOT varFoundSeg THEN
    -- log "createRecallCandidate: no segment found for this file. File is probably lost"
    logToDLF(NULL, dlf.LVL_CRIT, dlf.RECALL_NO_SEG_FOUND_AT_ALL, inFileId, inNsHost, 'stagerd', inLogParams);
    RETURN serrno.ESTNOSEGFOUND;
  END IF;
  -- If we found no valid segment (but some disabled ones), log a warning
  IF varAllValidCopyNbs.COUNT = 0 THEN
    -- log "createRecallCandidate: no valid segment to recall found"
    logToDLF(NULL, dlf.LVL_WARNING, dlf.RECALL_NO_SEG_FOUND, inFileId, inNsHost, 'stagerd', inLogParams);
    RETURN serrno.ESTNOSEGFOUND;
  END IF;
  BEGIN
    -- create missing segments if needed
    createMJForMissingSegments(inCfId, inFileSize, inFileClassId, varAllValidCopyNbs,
                               varAllValidVIDs, varNbExistingSegments, inFileId, inNsHost, inLogParams);
  EXCEPTION WHEN NO_TAPE_ROUTE THEN
    -- there's at least a missing segment and we cannot recreate it!
    -- log a "no route to tape defined for missing copy" error, but don't fail the recall
    logToDLF(NULL, dlf.LVL_ALERT, dlf.RECALL_MISSING_COPY_NO_ROUTE, inFileId, inNsHost, 'stagerd', inLogParams);
  WHEN OTHERS THEN
    -- some other error happened, log "unexpected error when creating missing copy", but don't fail the recall
    varErrorMsg := 'Oracle error caught : ' || SQLERRM;
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_MISSING_COPY_ERROR, inFileId, inNsHost, 'stagerd',
      'errorCode=' || to_char(SQLCODE) ||' errorMessage="' || varErrorMsg
      ||'" stackTrace="' || dbms_utility.format_error_backtrace() ||'"');
  END;
  RETURN 0;
END;
/


CREATE OR REPLACE PROCEDURE deleteDiskCopies(inArgs IN castor."strList",
                                             inForce IN BOOLEAN, inDryRun IN BOOLEAN,
                                             outRes OUT castor.DiskCopyResult_Cur) AS
BEGIN
  DELETE FROM DeleteDiskCopyHelper;
  -- Go through arguments one by one, parse them and fill temporary table with the files to process
  -- 2 formats are accepted :
  --    host:/mountpoint : drop all files on a given FileSystem
  --    host:<fullFilePath> : drop the given file
  FOR i IN 1..inArgs.COUNT LOOP
    DECLARE
      varMountPoint VARCHAR2(2048);
      varDataPool VARCHAR2(2048);
      varPath VARCHAR2(2048);
      varDcId INTEGER;
      varFileId INTEGER;
      varNsHost VARCHAR2(2048);
    BEGIN
      parsePath(inArgs(i), varMountPoint, varDataPool, varPath, varDcId, varFileId, varNsHost);
      -- we've got a file path
      INSERT INTO DeleteDiskCopyHelper (dcId, fileId, fStatus, rc)
      VALUES (varDcId, varFileId, 'd', -1);
    EXCEPTION WHEN OTHERS THEN
      DECLARE
        varColonPos INTEGER;
        varDiskServerName VARCHAR2(2048);
        varMountPoint VARCHAR2(2048);
        varFsId INTEGER;
      BEGIN
        -- not a file path, probably a mountPoint
        varColonPos := INSTR(inArgs(i), ':', 1, 1);
        IF varColonPos = 0 THEN
          raise_application_error(-20100, 'incorrect/incomplete value found as argument, giving up');
        END IF;
        varDiskServerName := SUBSTR(inArgs(i), 1, varColonPos-1);
        varMountPoint := SUBSTR(inArgs(i), varColonPos+1);
        -- check existence of this DiskServer/mountPoint
        BEGIN
          SELECT FileSystem.id INTO varFsId
            FROM FileSystem, DiskServer
           WHERE FileSystem.mountPoint = varMountPoint
             AND FileSystem.diskServer = DiskServer.id
             AND DiskServer.name = varDiskServerName;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          raise_application_error(-20100, 'no filesystem found for ' || inArgs(i) || ', giving up. Note that machines name must be fully qualified.');
        END;
        -- select the disk copies to be deleted
        INSERT INTO DeleteDiskCopyHelper (dcId, fileId, fStatus, msg, rc)
          SELECT DiskCopy.id, CastorFile.fileid, 'd', '', -1
            FROM DiskCopy, CastorFile
           WHERE DiskCopy.castorFile = CastorFile.id
             AND DiskCopy.fileSystem = varFsId;
      END;
    END;
  END LOOP;
  -- now call the internal method doing the real job
  internalDeleteDiskCopies(inForce, inDryRun, outRes);
END;
/

/* PL/SQL method deleting recall jobs of a castorfile */
CREATE OR REPLACE PROCEDURE deleteRecallJobsKeepSelected(cfId NUMBER) AS
  varUnused INTEGER;
BEGIN
  -- Loop over the recall jobs
  DELETE FROM RecallJob WHERE castorfile = cfId AND status != tconst.RECALLJOB_SELECTED;
  -- check whether we still have a RecallJob
  SELECT id INTO varUnused FROM RecallJob WHERE castorfile = cfId AND ROWNUM < 2;
  -- a recall job is still around, we are done
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no more recallJob, deal with potential waiting migrationJobs
  deleteMigrationJobsForRecall(cfId);
END;
/

/* PL/SQL method to fail a subrequest in WAITTAPERECALL
 * and eventually the recall itself if it's the only subrequest waiting for it
 */
CREATE OR REPLACE PROCEDURE failRecallSubReq(inSrId IN INTEGER, inCfId IN INTEGER) AS
  varNbSRs INTEGER;
BEGIN
  -- recall case. First fail the subrequest
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET status = dconst.SUBREQUEST_FAILED
   WHERE id = inSrId;
  -- check whether there are other subRequests waiting for a recall
  SELECT COUNT(*) INTO varNbSrs
    FROM SubRequest
   WHERE castorFile = inCfId
     AND status = dconst.SUBREQUEST_WAITTAPERECALL;
  IF varNbSrs = 0 THEN
    -- no other subrequests, so drop recalls
    deleteRecallJobsKeepSelected(inCfId);
  END IF;
END;
/


/*** initMigration ***/
CREATE OR REPLACE PROCEDURE initMigration
(cfId IN INTEGER, datasize IN INTEGER, originalVID IN VARCHAR2,
 originalCopyNb IN INTEGER, destCopyNb IN INTEGER, inMJStatus IN INTEGER) AS
  varTpId INTEGER;
  varSizeThreshold INTEGER;
BEGIN
  varSizeThreshold := TO_NUMBER(getConfigOption('Migration', 'SizeThreshold', '300000000'));
  -- Find routing
  BEGIN
    SELECT tapePool INTO varTpId FROM MigrationRouting MR, CastorFile
     WHERE MR.fileClass = CastorFile.fileClass
       AND CastorFile.id = cfId
       AND MR.copyNb = destCopyNb
       AND (MR.isSmallFile = (CASE WHEN datasize < varSizeThreshold THEN 1 ELSE 0 END) OR MR.isSmallFile IS NULL);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    DECLARE
      varFileClassName VARCHAR2(2048);
      varID INTEGER;
      varClassId INTEGER;
    BEGIN
      SELECT id, classId, name INTO varID, varClassId, varFileClassName
        FROM FileClass
       WHERE id = (SELECT FileClass FROM CastorFile WHERE id = cfId);
      -- No routing rule found means a user-visible error on the putDone or on the file close operation
      raise_application_error(-20100, 'Cannot find an appropriate tape routing for this file, aborting - '
                              || 'fileclass was ' || varFileClassName || ' (id ' || varId
                              || ', classId ' || varClassId || ')');
    END;
  END;
  -- Create tape copy and attach to the appropriate tape pool
  INSERT INTO MigrationJob (fileSize, creationTime, castorFile, originalVID, originalCopyNb, destCopyNb,
                            tapePool, nbRetries, status, mountTransactionId, id)
    VALUES (datasize, getTime(), cfId, originalVID, originalCopyNb, destCopyNb, varTpId, 0,
            inMJStatus, NULL, ids_seq.nextval);
END;
/

/* inserts file Requests in the stager DB.
 * This handles StageGetRequest, StagePrepareToGetRequest,
 * StagePutRequest, StagePrepareToPutRequest,
 * StagePutDoneRequest, and StageRmRequest requests.
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
  protocols VARCHAR2(100);
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, inReqType);
  -- get the list of valid protocols
  protocols := ' ' || getConfigOption('Stager', 'Protocols', 'rfio rfio3 gsiftp xroot') || ' ';
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
  FOR i IN 1..srFileNames.COUNT LOOP
    -- check protocol validity for Get/Put requests only, for other requests the protocol is irrelevant
    IF inReqType IN (35, 40) AND INSTR(protocols, ' ' || srProtocols(i) || ' ') = 0 THEN
      raise_application_error(-20122, 'Unsupported protocol in insertFileRequest : ' || srProtocols(i));
    END IF;
    -- get unique ids for the subrequest
    SELECT ids_seq.nextval INTO subreqId FROM DUAL;
    -- insert the subrequest
    INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, status, request, getNextStatus, reqType)
    VALUES (0, srFileNames(i), srProtocols(i), srXsizes(i), 0, NULL, srFlags(i), srModeBits(i), creationTime, creationTime, 0, '', NULL, svcHandler, subreqId, NULL, NULL, dconst.SUBREQUEST_START, reqId, 0, inReqType);
  END LOOP;
  -- send one single alert to accelerate the processing of the request
  CASE
  WHEN inReqType = 35 OR   -- StageGetRequest
       inReqType = 40 OR   -- StagePutRequest
       inReqType = 36 OR   -- StagePrepareToGetRequest
       inReqType = 37 THEN -- StagePrepareToPutRequest
    alertSignalNoLock('wakeUpJobReqSvc');
  WHEN inReqType = 42 OR   -- StageRmRequest
       inReqType = 39 OR   -- StagePutDoneRequest
       inReqType = 95 THEN -- SetFileGCWeight
    alertSignalNoLock('wakeUpStageReqSvc');
  END CASE;
END;
/


/* inserts StageAbort Request in the stager DB */ 	 
CREATE OR REPLACE PROCEDURE insertStageAbortRequest
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
   parentUUID IN VARCHAR2,
   fileids IN castor."cnumList",
   nsHosts IN castor."strList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  fileidId INTEGER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 50 THEN -- StageAbortRequest
      INSERT INTO StageAbortRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,parentUUID,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertStageAbortRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on fileids
  FOR i IN 1..fileids.COUNT LOOP
    -- ignore fake items passed only because ORACLE does not like empty arrays
    IF nsHosts(i) IS NULL THEN EXIT; END IF;
    -- get unique ids for the fileid
    SELECT ids_seq.nextval INTO fileidId FROM DUAL;
    -- insert the fileid
    INSERT INTO NsFileId (fileId, nsHost, id, request)
    VALUES (fileids(i), nsHosts(i), fileidId, reqId);
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
  -- send an alert to accelerate the processing of the request
  alertSignalNoLock('wakeUpBulkStageReqSvc');
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
  FOR i IN 1..qpValues.COUNT LOOP
    -- get unique ids for the query parameter
    SELECT ids_seq.nextval INTO queryParamId FROM DUAL;
    -- insert the query parameter
    INSERT INTO QueryParameter (value, id, query, querytype)
    VALUES (qpValues(i), queryParamId, reqId, qpTypes(i));
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
  -- send an alert to accelerate the processing of the request
  alertSignalNoLock('wakeUpQueryReqSvc');
END;
/

/* inserts GC Requests in the stager DB
 * This handles NsFilesDeleted, FilesDeleted, FilesDeletionFailed and StgFilesDeleted 
 * requests.
 */
CREATE OR REPLACE PROCEDURE insertGCRequest
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
   nsHost IN VARCHAR2,
   diskCopyIds IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  gcFileId INTEGER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 142 THEN -- NsFilesDeleted
      INSERT INTO NsFilesDeleted (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, nsHost, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,nsHost,reqId,svcClassId,clientId);
    WHEN reqType = 74 THEN -- FilesDeleted
      INSERT INTO FilesDeleted (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN reqType = 83 THEN -- FilesDeletionFailed
      INSERT INTO FilesDeletionFailed (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN reqType = 149 THEN -- StgFilesDeleted
      INSERT INTO StgFilesDeleted (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, nsHost, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,nsHost,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertGCRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on diskCopies
  FOR i IN 1..diskCopyIds.COUNT LOOP
    -- get unique ids for the diskCopy
    SELECT ids_seq.nextval INTO gcFileId FROM DUAL;
    -- insert the fileid
    INSERT INTO GCFile (diskCopyId, id, request)
    VALUES (diskCopyIds(i), gcFileId, reqId);
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
  -- send an alert to accelerate the processing of the request
  alertSignalNoLock('wakeUpGCSvc');
END;
/


/* PL/SQL method implementing stageRm */
CREATE OR REPLACE PROCEDURE stageRm (srId IN INTEGER,
                                     fid IN INTEGER,
                                     nh IN VARCHAR2,
                                     svcClassId IN INTEGER,
                                     ret OUT INTEGER) AS
  nsHostName VARCHAR2(2048);
  cfId INTEGER;
  dcsToRm "numList";
  dcsToRmStatus "numList";
  dcsToRmCfStatus "numList";
  nbRJsDeleted INTEGER;
  varNbValidRmed INTEGER;
BEGIN
  ret := 0;
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  BEGIN
    -- Lock the access to the CastorFile
    -- This, together with triggers will avoid new migration/recall jobs
    -- or DiskCopies to be added
    SELECT id INTO cfId FROM CastorFile
     WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- This file does not exist in the stager catalog
    -- so we just fail the request
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOENT,
           errorMessage = 'File not found on disk cache'
     WHERE id = srId;
    RETURN;
  END;

  -- select the list of DiskCopies to be deleted
  SELECT id, status, tapeStatus BULK COLLECT INTO dcsToRm, dcsToRmStatus, dcsToRmCfStatus FROM (
    -- first physical diskcopies
    SELECT /*+ INDEX_RS_ASC(DC I_DiskCopy_CastorFile) */
           DiskCopy.id, DiskCopy.status, CastorFile.tapeStatus
      FROM DiskCopy, FileSystem, DiskPool2SvcClass, CastorFile
     WHERE DiskCopy.castorFile = cfId
       AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND (DiskPool2SvcClass.child = svcClassId OR svcClassId = 0)
       AND CastorFile.id = cfId)
     UNION ALL
    SELECT /*+ INDEX_RS_ASC(DC I_DiskCopy_CastorFile) */
           DiskCopy.id, DiskCopy.status, CastorFile.tapeStatus
      FROM DiskCopy, DataPool2SvcClass, CastorFile
     WHERE DiskCopy.castorFile = cfId
       AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
       AND DiskCopy.dataPool = DataPool2SvcClass.parent
       AND (DataPool2SvcClass.child = svcClassId OR svcClassId = 0)
       AND CastorFile.id = cfId
     UNION ALL
    -- and then diskcopies resulting from ongoing requests, for which the previous
    -- query wouldn't return any entry because of e.g. missing filesystem
    SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile)*/ DC.id, DC.status, CastorFile.tapeStatus
      FROM (SELECT /*+ INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id) */ id
              FROM StagePrepareToPutRequest WHERE svcClass = svcClassId UNION ALL
            SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ id
              FROM StagePutRequest WHERE svcClass = svcClassId) Request,
           SubRequest, DiskCopy DC, CastorFile
     WHERE SubRequest.diskCopy = DC.id
       AND Request.id = SubRequest.request
       AND DC.castorFile = cfId
       AND DC.status IN (dconst.DISKCOPY_WAITFS, dconst.DISKCOPY_WAITFS_SCHEDULING)
       AND CastorFile.id = cfId;

  -- in case we are dropping diskcopies not yet on tape, ensure that we have at least one copy left on disk
  IF dcsToRmStatus.COUNT > 0 THEN
    IF dcsToRmStatus(1) = dconst.DISKCOPY_VALID AND dcsToRmCfStatus(1) = dconst.CASTORFILE_NOTONTAPE THEN
      BEGIN
        SELECT castorFile INTO cfId
          FROM DiskCopy
         WHERE castorFile = cfId
           AND status = dconst.DISKCOPY_VALID
           AND id NOT IN (SELECT /*+ CARDINALITY(dcidTable 5) */ * FROM TABLE(dcsToRm) dcidTable)
           AND ROWNUM < 2;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- nothing left, so we would lose the file. Better to forbid stagerm
        UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
           SET status = dconst.SUBREQUEST_FAILED,
               errorCode = serrno.EBUSY,
               errorMessage = 'The file is not yet migrated'
         WHERE id = srId;
        RETURN;
      END;
    END IF;

    -- fail diskcopies : WAITFS[_SCHED] -> FAILED, others -> INVALID
    UPDATE DiskCopy
       SET status = decode(status, dconst.DISKCOPY_WAITFS, dconst.DISKCOPY_FAILED,
                                   dconst.DISKCOPY_WAITFS_SCHEDULING, dconst.DISKCOPY_FAILED,
                                   dconst.DISKCOPY_INVALID)
     WHERE id IN (SELECT /*+ CARDINALITY(dcidTable 5) */ * FROM TABLE(dcsToRm) dcidTable)
    RETURNING SUM(decode(status, dconst.DISKCOPY_VALID, 1, 0)) INTO varNbValidRmed;

    -- update importance of remaining DiskCopies, if any
    UPDATE DiskCopy SET importance = importance + varNbValidRmed
     WHERE castorFile = cfId AND status = dconst.DISKCOPY_VALID;

    -- fail the subrequests linked to the deleted diskcopies
    FOR sr IN (SELECT /*+ INDEX_RS_ASC(SR I_SubRequest_DiskCopy) */ id, subreqId
                 FROM SubRequest SR
                WHERE diskcopy IN (SELECT /*+ CARDINALITY(dcidTable 5) */ * FROM TABLE(dcsToRm) dcidTable)
                  AND status IN (dconst.SUBREQUEST_START, dconst.SUBREQUEST_RESTART,
                                 dconst.SUBREQUEST_RETRY, dconst.SUBREQUEST_WAITTAPERECALL,
                                 dconst.SUBREQUEST_WAITSUBREQ, dconst.SUBREQUEST_READY,
                                 dconst.SUBREQUEST_READYFORSCHED)) LOOP
      UPDATE SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.EINTR,
             errorMessage = 'Canceled by another user request'
       WHERE id = sr.id
          OR (castorFile = cfId AND status = dconst.SUBREQUEST_WAITSUBREQ);
      -- make the scheduler aware so that it can remove the transfer from the queues if needed
      INSERT INTO TransfersToAbort (uuid) VALUES (sr.subreqId);
    END LOOP;
  END IF;

  -- delete RecallJobs that should be canceled
  DELETE FROM RecallJob
   WHERE castorfile = cfId AND (svcClass = svcClassId OR svcClassId = 0);
  nbRJsDeleted := SQL%ROWCOUNT;
  -- in case we've dropped something, check whether we still have recalls ongoing
  IF nbRJsDeleted > 0 THEN
    BEGIN
      SELECT castorFile INTO cfId
        FROM RecallJob
       WHERE castorFile = cfId;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- all recalls are canceled for this file
      -- deal with potential waiting migrationJobs
      deleteMigrationJobsForRecall(cfId);
      -- fail corresponding requests
      UPDATE SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.EINTR,
             errorMessage = 'Canceled by another user request'
       WHERE castorFile = cfId
         AND status = dconst.SUBREQUEST_WAITTAPERECALL;
    END;
  END IF;

  -- In case nothing was dropped at all, complain
  IF dcsToRm.COUNT = 0 AND nbRJsDeleted = 0 THEN
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ENOENT,
           errorMessage = CASE WHEN svcClassId = 0 THEN 'File not found on disk cache'
                               ELSE 'File not found on this service class' END
     WHERE id = srId;
    RETURN;
  END IF;

  -- In case of something to abort, first commit and then signal the service.
  -- This procedure is called by the stager with autocommit=TRUE, hence
  -- committing here is safe.
  IF dcsToRmStatus.COUNT > 0 THEN
    COMMIT;
    -- wake up the scheduler so that it can remove the transfer from the queues now
    alertSignalNoLock('transfersToAbort');
  END IF;

  ret := 1;  -- ok
END;
/

/* A wrapper to run DB jobs and catch+log any exception they may throw */
CREATE OR REPLACE PROCEDURE startDbJob(jobCode VARCHAR2, source VARCHAR2) AS
BEGIN
  EXECUTE IMMEDIATE jobCode;
EXCEPTION WHEN OTHERS THEN
  logToDLF(NULL, dlf.LVL_ALERT, dlf.DBJOB_UNEXPECTED_EXCEPTION, 0, '', source,
    'jobCode="'|| jobCode ||'" errorCode=' || to_char(SQLCODE) ||' errorMessage="' || SQLERRM
    ||'" stackTrace="' || dbms_utility.format_error_backtrace() ||'"');
END;
/

/* PL/SQL method implementing storeReports. This procedure stores new reports
 * and disables nodes for which last report (aka heartbeat) is too old. 
 * For efficiency reasons the input parameters to this method
 * are 2 vectors. The first ones is a list of strings with format :
 *  (diskServerName1, mountPoint1, diskServerName2, mountPoint2, ...)
 * representing a set of mountpoints with the diskservername repeated
 * for each mountPoint.
 * The second vector is a list of numbers with format :
 *  (maxFreeSpace1, minAllowedFreeSpace1, totalSpace1, freeSpace1,
 *   nbReadStreams1, nbWriteStreams1, nbRecalls1, nbMigrations1,
 *   maxFreeSpace2, ...)
 * where 8 values are given for each of the mountPoints in the first vector
 */
CREATE OR REPLACE PROCEDURE storeReports
(inStrParams IN castor."strList",
 inNumParams IN castor."cnumList") AS
 varDsId NUMBER;
 varFsId NUMBER;
 varDpId NUMBER;
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
      -- update FileSystem or data pool
      IF SUBSTR(inStrParams(2*i+2),0,1) = '/' THEN
        varFsId := NULL;
        UPDATE FileSystem
           SET maxFreeSpace = inNumParams(8*i+1),
               minAllowedFreeSpace = inNumParams(8*i+2),
               totalSize = inNumParams(8*i+3),
               free = inNumParams(8*i+4),
               nbReadStreams = inNumParams(8*i+5),
               nbWriteStreams = inNumParams(8*i+6),
               nbRecallerStreams = inNumParams(8*i+7),
               nbMigratorStreams = inNumParams(8*i+8),
               status = CASE totalSize WHEN 0 THEN dconst.FILESYSTEM_DISABLED ELSE status END
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
      ELSE
        UPDATE DataPool
           SET maxFreeSpace = inNumParams(8*i+1),
               minAllowedFreeSpace = inNumParams(8*i+2),
               totalSize = inNumParams(8*i+3),
               free = inNumParams(8*i+4)
         WHERE name = inStrParams(2*i+2)
        RETURNING id INTO varDpId;
        -- if it did not exist, create it
        IF varDpId IS NULL THEN
          INSERT INTO DataPool (maxFreeSpace, minAllowedFreeSpace, totalSize, free, id, name)
          VALUES (inNumParams(8*i+1), inNumParams(8*i+2), inNumParams(8*i+3),
                  inNumParams(8*i+4), ids_seq.nextval, inStrParams(2*i+2));
        END IF;
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


/* PL/SQL method implementing handlePrepareToGet
 * returns whether the client should be answered
 */
CREATE OR REPLACE FUNCTION handlePrepareToGet(inCfId IN INTEGER, inSrId IN INTEGER,
                                              inFileId IN INTEGER, inNsHost IN VARCHAR2,
                                              inFileSize IN INTEGER, inNsOpenTimeInUsec IN INTEGER)
RETURN INTEGER AS
  varNsOpenTime INTEGER;
  varEuid NUMBER;
  varEgid NUMBER;
  varSvcClassId NUMBER;
  varReqUUID VARCHAR(2048);
  varReqId INTEGER;
  varSrUUID VARCHAR(2048);
  varIsAnswered INTEGER;
BEGIN
  -- lock the castorFile to be safe in case of concurrent subrequests
  SELECT nsOpenTime INTO varNsOpenTime FROM CastorFile WHERE id = inCfId FOR UPDATE;
  -- retrieve the svcClass, user and log data for this subrequest
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
         Request.euid, Request.egid, Request.svcClass,
         Request.reqId, Request.id, SubRequest.subreqId, SubRequest.answered
    INTO varEuid, varEgid, varSvcClassId, varReqUUID, varReqId, varSrUUID, varIsAnswered
    FROM (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                 id, euid, egid, svcClass, reqId from StagePrepareToGetRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = inSrId;
  -- log
  logToDLF(varReqUUID, dlf.LVL_DEBUG, dlf.STAGER_PREPARETOGET, inFileId, inNsHost, 'stagerd',
           'SUBREQID=' || varSrUUID);

  -- We should actually check whether our disk cache is stale,
  -- that is IF CF.nsOpenTime < inNsOpenTime THEN invalidate our diskcopies.
  -- This is pending the full deployment of the 'new open mode' as implemented
  -- in the fix of bug #95189: Time discrepencies between
  -- disk servers and name servers can lead to silent data loss on input.
  -- The problem being that in 'Compatibility' mode inNsOpenTime is the
  -- namespace's mtime, which can be modified by nstouch,
  -- hence nstouch followed by a Get would destroy the data on disk!

  -- First look for available diskcopies. Note that we never wait on other requests.
  -- and we include Disk2DiskCopyJobs as they are going to produce available DiskCopies.
  DECLARE
    varNbDCs INTEGER;
  BEGIN
    SELECT COUNT(*) INTO varNbDCs FROM (
      SELECT /*+ INDEX_RS_ASC (DiskCopy I_DiskCopy_CastorFile) */ DiskCopy.id
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
       WHERE DiskCopy.castorfile = inCfId
         AND DiskCopy.fileSystem = FileSystem.id
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = varSvcClassId
         AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
         AND FileSystem.diskserver = DiskServer.id
         AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
         AND DiskServer.hwOnline = 1
         AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
       UNION ALL
      SELECT DiskCopy.id
        FROM DiskCopy, DataPool2SvcClass
       WHERE DiskCopy.castorfile = inCfId
         AND DiskCopy.dataPool = DataPool2SvcClass.parent
         AND DataPool2SvcClass.child = varSvcClassId
         AND EXISTS (SELECT 1 FROM DiskServer
                     WHERE DiskServer.dataPool = DiskCopy.dataPool
                       AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION,
                                                 dconst.DISKSERVER_READONLY))
         AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
       UNION ALL
      SELECT id
        FROM Disk2DiskCopyJob
       WHERE destSvcclass = varSvcClassId
         AND castorfile = inCfId);
    IF varNbDCs > 0 THEN
      -- some available diskcopy was found.
      logToDLF(varReqUUID, dlf.LVL_DEBUG, dlf.STAGER_DISKCOPY_FOUND, inFileId, inNsHost, 'stagerd',
              'SUBREQID=' || varSrUUID);
      -- update and archive SubRequest
      UPDATE SubRequest
         SET getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED
       WHERE id = inSrId;
      archiveSubReq(inSrId, dconst.SUBREQUEST_FINISHED);
      -- all went fine, answer to client if needed
      IF varIsAnswered > 0 THEN
         RETURN 0;
      END IF;
    ELSE
      IF triggerD2dOrRecall(inCfId, varNsOpenTime, inSrId, inFileId, inNsHost, varEuid, varEgid,
                            varSvcClassId, inFileSize, varReqUUID, varSrUUID) != 0 THEN
        -- recall started, we are done, update subrequest and answer to client
        UPDATE SubRequest SET status = dconst.SUBREQUEST_WAITTAPERECALL, answered=1 WHERE id = inSrId;
      ELSE
        -- could not start recall, SubRequest has been marked as FAILED, no need to answer
        RETURN 0;
      END IF;
    END IF;
    -- first lock the request. Note that we are always in a case of PrepareToPut as
    -- we will answer the client.
    SELECT id INTO varReqId FROM StagePrepareToGetRequest WHERE id = varReqId FOR UPDATE;
    -- now do the check on whether we were the last subrequest
    RETURN wasLastSubRequest(varReqId);
  END;
END;
/


/*
 * handle a raw put/upd (i.e. outside a prepareToPut) or a prepareToPut/Upd
 * return 1 if the client needs to be replied to, else 0
 */
CREATE OR REPLACE FUNCTION handleRawPutOrPPut(inCfId IN INTEGER, inSrId IN INTEGER,
                                              inFileId IN INTEGER, inNsHost IN VARCHAR2,
                                              inFileClassId IN INTEGER, inSvcClassId IN INTEGER,
                                              inEuid IN INTEGER, inEgid IN INTEGER,
                                              inReqUUID IN VARCHAR2, inSrUUID IN VARCHAR2,
                                              inDoSchedule IN BOOLEAN, inNsOpenTime IN INTEGER)
RETURN INTEGER AS
  varReqId INTEGER;
BEGIN
  -- check that no concurrent put is already running
  DECLARE
    varAnyStageoutDC INTEGER;
  BEGIN
    SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_Castorfile) */
           COUNT(*) INTO varAnyStageoutDC FROM DiskCopy
     WHERE status IN (dconst.DISKCOPY_WAITFS, dconst.DISKCOPY_STAGEOUT)
       AND castorFile = inCfId
       AND ROWNUM < 2;
    IF varAnyStageoutDC > 0 THEN
      -- the file is already being recreated -> EBUSY
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.EBUSY,
             errorMessage = 'File recreation canceled since file is being recreated by another request'
       WHERE id = inSrId;
      logToDLF(inReqUUID, dlf.LVL_USER_ERROR, dlf.STAGER_RECREATION_IMPOSSIBLE, inFileId, inNsHost, 'stagerd',
               'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId) ||
               ' fileClassId=' || getFileClassName(inFileClassId) || ' reason="file being recreated"');
      RETURN 0;
    END IF;
  END;

  -- check if the file can be routed to tape
  IF checkNoTapeRouting(inFileClassId) = 1 THEN
    -- We could not route the file to tape, so we fail the opening
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ESTNOTAPEROUTE,
           errorMessage = 'File recreation canceled since the file cannot be routed to tape'
     WHERE id = inSrId;
    logToDLF(inReqUUID, dlf.LVL_USER_ERROR, dlf.STAGER_RECREATION_IMPOSSIBLE, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId) ||
             ' fileClassId=' || getFileClassName(inFileClassId) || ' reason="no route to tape"');
    RETURN 0;
  END IF;

  -- delete ongoing disk2DiskCopyJobs
  deleteDisk2DiskCopyJobs(inCfId);

  -- delete ongoing recalls
  deleteRecallJobs(inCfId);

  -- fail recall requests pending on the previous file
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_FAILED,
         errorCode = serrno.EINTR,
         errorMessage = 'Canceled by another user request'
   WHERE castorFile = inCfId
     AND status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_REPACK);

  -- delete ongoing migrations
  deleteMigrationJobs(inCfId);

  -- set DiskCopies to INVALID
  UPDATE DiskCopy
     SET status = dconst.DISKCOPY_INVALID,
         gcType = dconst.GCTYPE_OVERWRITTEN
   WHERE castorFile = inCfId
     AND status = dconst.DISKCOPY_VALID;

  -- create new DiskCopy, associate it to SubRequest and schedule
  DECLARE
    varDcId INTEGER;
  BEGIN
    logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_CASTORFILE_RECREATION, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID || ' svcClassId=' || getSvcClassName(inSvcClassId));
    -- DiskCopy creation
    varDcId := ids_seq.nextval();
    INSERT INTO DiskCopy (path, id, FileSystem, castorFile, status, creationTime, lastAccessTime,
                          gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid, importance)
    VALUES ('', varDcId, NULL, inCfId, dconst.DISKCOPY_WAITFS, getTime(), getTime(), 0, 0, 0, inEuid, inEgid, 0);
    -- update and schedule the subRequest if needed
    IF inDoSchedule THEN
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET diskCopy = varDcId, lastModificationTime = getTime(),
             xsize = CASE WHEN xsize = 0
                          THEN (SELECT defaultFileSize FROM SvcClass WHERE id = inSvcClassId)
                          ELSE xsize
                     END,
             status = dconst.SUBREQUEST_READYFORSCHED
       WHERE id = inSrId;
    ELSE
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
         SET diskCopy = varDcId, lastModificationTime = getTime(),
             status = dconst.SUBREQUEST_READY,
             answered = 1
       WHERE id = inSrId
      RETURNING request INTO varReqId;
    END IF;
    -- reset the castorfile size, lastUpdateTime and nsOpenTime as the file was truncated
    UPDATE CastorFile
       SET fileSize = 0,
           lastUpdateTime = TRUNC(inNsOpenTime),
           nsOpenTime = inNsOpenTime
     WHERE id = inCfId;
  END;
  IF inDoSchedule THEN
    RETURN 0; -- do not answer client, the diskserver will
  ELSE
    -- first lock the request. Note that we are always in a case of PrepareToPut as
    -- we will answer the client.
    SELECT id INTO varReqId FROM StagePrepareToPutRequest WHERE id = varReqId FOR UPDATE;
    -- now do the check on whether we were the last subrequest
    RETURN wasLastSubRequest(varReqId);
  END IF;
END;
/


/* PL/SQL method implementing getStart */
CREATE OR REPLACE PROCEDURE getStart(inTransferId IN VARCHAR2, selectedDiskServer IN VARCHAR2,
                                     selectedMountPoint IN VARCHAR2, outPath OUT VARCHAR2) AS
  srId INTEGER;
  cfId INTEGER;
  dcId INTEGER;
  fsId INTEGER := NULL;
  varDsId INTEGER := NULL;
  dpId INTEGER := NULL;
  nh VARCHAR2(2048);
  fileSize INTEGER;
  srSvcClass INTEGER;
  proto VARCHAR2(2048);
  nbAc NUMBER;
  gcw NUMBER;
  gcwProc VARCHAR2(2048);
  cTime NUMBER;
  fsStatus INTEGER := dconst.FILESYSTEM_PRODUCTION;
  dsStatus INTEGER;
  varHwOnline INTEGER;
  varDiskCopySize INTEGER;
  varDcStatus INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(100);
  varDpName VARCHAR2(2048);
BEGIN
  -- Get data and take a lock on the CastorFile. Associated with triggers,
  -- this guarantees we are the only ones dealing with its copies
  SELECT /*+ INDEX_RS_ASC(SubRequest I_SubRequest_SubReqId)
             INDEX_RS_ASC(StageGetRequest PK_StageGetRequest_Id) */
         CastorFile.fileSize, CastorFile.id,
         Req.svcClass, CastorFile.fileId, CastorFile.nsHost, SubRequest.id
    INTO fileSize, cfId, srSvcClass, varFileId, varNsHost, srId
    FROM CastorFile, SubRequest, StageGetRequest Req
   WHERE CastorFile.id = SubRequest.castorFile
     AND SubRequest.request = Req.id
     AND SubRequest.subreqId = inTransferid
     FOR UPDATE OF CastorFile.id;
  -- Get selected filesystem/datapool
  IF selectedMountPoint IS NULL THEN
    SELECT DiskServer.dataPool, DiskServer.status,
           DiskServer.hwOnline, DiskServer.id, DataPool.name
      INTO dpId, dsStatus, varHwOnline, varDsId, varDpName
      FROM DiskServer, DataPool
     WHERE DiskServer.name = selectedDiskServer
       AND DataPool.id = DiskServer.dataPool;
  ELSE 
    SELECT FileSystem.id, FileSystem.status, DiskServer.status, DiskServer.hwOnline
      INTO fsId, fsStatus, dsStatus, varHwOnline
      FROM DiskServer, FileSystem
     WHERE FileSystem.diskserver = DiskServer.id
       AND DiskServer.name = selectedDiskServer
       AND FileSystem.mountPoint = selectedMountPoint;
  END IF;
  -- Now check that the hardware status is still valid for a Get request
  IF NOT (fsStatus IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY) AND
          dsStatus IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY) AND
          varHwOnline = 1) THEN
    raise_application_error(-20114, 'The selected diskserver/filesystem is not in PRODUCTION or READONLY any longer. Giving up.');
  END IF;
  -- Check that the request is still valid
  SELECT id INTO srId
    FROM SubRequest
   WHERE id = srId AND status NOT IN (dconst.SUBREQUEST_FAILED, dconst.SUBREQUEST_FAILED_FINISHED);
  -- Try to find local DiskCopy
  SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_Castorfile) */
         id, nbCopyAccesses, gcWeight, creationTime
    INTO dcId, nbac, gcw, cTime
    FROM DiskCopy
   WHERE DiskCopy.castorfile = cfId
     AND (DiskCopy.filesystem = fsId OR DiskCopy.dataPool = dpId)
     AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
     AND ROWNUM < 2;
  -- We found it, so we are settled and we'll use the local copy.
  -- For the ROWNUM < 2 condition: it might happen that we have more than one, because
  -- the scheduling may have scheduled a replication on a fileSystem which already had a previous diskcopy.
  -- We don't care and we randomly took the first one.
  -- First we will compute the new gcWeight of the diskcopy
  IF nbac = 0 THEN
    gcwProc := castorGC.getFirstAccessHook(srSvcClass);
    IF gcwProc IS NOT NULL THEN
      EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:oldGcw, :cTime); END;'
        USING OUT gcw, IN gcw, IN cTime;
    END IF;
  ELSE
    gcwProc := castorGC.getAccessHook(srSvcClass);
    IF gcwProc IS NOT NULL THEN
      EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:oldGcw, :cTime, :nbAc); END;'
        USING OUT gcw, IN gcw, IN cTime, IN nbac;
    END IF;
  END IF;
  -- Here we also update the gcWeight taking into account the new lastAccessTime
  -- and the weightForAccess from our svcClass: this is added as a bonus to
  -- the selected diskCopy.
  UPDATE DiskCopy
     SET gcWeight = gcw,
         lastAccessTime = getTime(),
         nbCopyAccesses = nbCopyAccesses + 1
   WHERE id = dcId
  RETURNING path, status, diskCopySize
    INTO outPath, varDcStatus, varDiskCopySize;
  IF selectedMountPoint IS NULL THEN
    outPath := getConfigOption('DiskManager', 'PoolUserId', 'castor') || '@' || varDpName || '/' || outPath;
  ELSE
    outPath := selectedMountPoint || outPath;
  END IF;
  -- Update the SubRequest and set the link with the DiskCopy
  UPDATE /*+ INDEX_RS_ASC(SubRequest PK_Subrequest_Id)*/ SubRequest
     SET diskCopy = dcId
   WHERE id = srId;
  -- Deal with recalls of empty files: redirect to /dev/null
  IF varDcStatus = dconst.DISKCOPY_VALID AND varDiskCopySize = 0 THEN
    outPath := '/dev/null';
  END IF;
  -- Log successful completion
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.STAGER_GETSTART, varFileId, varNsHost, 'stagerd', 'SUBREQID='|| inTransferId
    || 'destinationPath=' || selectedDiskServer ||':'|| selectedMountPoint);
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- No disk copy found on selected FileSystem, or subRequest not valid any longer.
  -- This can happen if a diskcopy was available and got disabled, or if an abort came,
  -- before this job was scheduled. Bad luck, we fail the request, the user will have to retry
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_FAILED, errorCode = 1725, errorMessage = 'Request canceled while queuing'
   WHERE id = srId;
  COMMIT;
  raise_application_error(-20114, 'File invalidated while queuing in the scheduler, please try again');
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
  FOR i IN 1..subReqIds.COUNT LOOP
    BEGIN
      SELECT /*+ INDEX_RS_ASC(SubRequest I_Subrequest_SubreqId)*/ fileid, nsHost INTO fid, nh
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

/* handle a put/upd outside a prepareToPut/Upd */
CREATE OR REPLACE PROCEDURE handlePutInsidePrepareToPut(inCfId IN INTEGER, inSrId IN INTEGER,
                                                        inFileId IN INTEGER, inNsHost IN VARCHAR2,
                                                        inDcId IN INTEGER, inSvcClassId IN INTEGER,
                                                        inReqUUID IN VARCHAR2, inSrUUID IN VARCHAR2,
                                                        inNsOpenTime IN INTEGER) AS
  varFsId INTEGER;
  varDpId INTEGER;
  varStatus INTEGER;
BEGIN
  BEGIN
    -- Retrieve the infos about the DiskCopy to be used
    SELECT fileSystem, dataPool, status INTO varFsId, varDpId, varStatus
      FROM DiskCopy
     WHERE id = inDcId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- The DiskCopy has disappeared in the mean time, removed by another request
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.ESTKILLED,
           errorMessage = 'SubRequest canceled while being handled.'
     WHERE id = inSrId;
    logToDLF(inReqUUID, dlf.LVL_USER_ERROR, dlf.STAGER_RECREATION_IMPOSSIBLE, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID || ' reason="canceled"');
    RETURN;
  END;

  -- handle the case where another concurrent Put request overtook us
  IF varStatus = dconst.DISKCOPY_WAITFS_SCHEDULING THEN
    -- another Put request was faster, subRequest needs to wait on it
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_WAITSUBREQ
     WHERE id = inSrId;
    logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_WAITSUBREQ, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID);
    RETURN;
  ELSE
    IF varStatus = dconst.DISKCOPY_WAITFS THEN
      -- we are the first put/update in the prepare session
      -- we change the diskCopy status to ensure that nobody else schedules it
      UPDATE DiskCopy SET status = dconst.DISKCOPY_WAITFS_SCHEDULING
       WHERE castorFile = inCfId
         AND status = dconst.DISKCOPY_WAITFS;
    END IF;
  END IF;

  -- schedule the request
  DECLARE
    varReqFileSystem VARCHAR(2048) := '';
  BEGIN
    logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.STAGER_CASTORFILE_RECREATION, inFileId, inNsHost, 'stagerd',
             'SUBREQID=' || inSrUUID);
    -- retrieve requested filesystem if any
    IF varFsId != 0 THEN
      SELECT diskServer.name || ':' || fileSystem.mountPoint INTO varReqFileSystem
        FROM FileSystem, DiskServer
       WHERE FileSystem.id = varFsId
         AND DiskServer.id = FileSystem.diskServer;
    END IF;
    -- in case of datapool, take a random diskServer
    IF varDpId != 0 THEN
      SELECT name || ':' INTO varReqFileSystem FROM (
        SELECT diskServer.name
          FROM DiskServer
         WHERE DiskServer.dataPool = varDpId
         ORDER BY DBMS_Random.value)
       WHERE ROWNUM < 2;
    END IF;
    -- update and schedule the subRequest
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET diskCopy = inDcId,
           lastModificationTime = getTime(),
           requestedFileSystems = varReqFileSystem,
           xsize = CASE WHEN xsize = 0
                        THEN (SELECT defaultFileSize FROM SvcClass WHERE id = inSvcClassId)
                        ELSE xsize
                   END,
           status = dconst.SUBREQUEST_READYFORSCHED
     WHERE id = inSrId;
    -- reset the castorfile size, lastUpdateTime and nsOpenTime as the file was truncated
    UPDATE CastorFile
       SET fileSize = 0,
           lastUpdateTime = TRUNC(inNsOpenTime),
           nsOpenTime = inNsOpenTime
     WHERE id = inCfId;
  END;
END;
/

/* PL/SQL method to handle a Repack request. This is performed as part of a DB job. */
CREATE OR REPLACE PROCEDURE handleRepackRequest(inReqUUID IN VARCHAR2,
                                                outNbFilesProcessed OUT INTEGER, outNbFailures OUT INTEGER) AS
  varEuid INTEGER;
  varEgid INTEGER;
  svcClassId INTEGER;
  varRepackVID VARCHAR2(10);
  varReqId INTEGER;
  cfId INTEGER;
  dcId INTEGER;
  nsHostName VARCHAR2(2048);
  lastKnownFileName VARCHAR2(2048);
  varRecallGroupId INTEGER;
  varRecallGroupName VARCHAR2(2048);
  varCreationTime NUMBER;
  unused INTEGER;
  firstCF boolean := True;
  isOngoing boolean := False;
  varTotalSize INTEGER := 0;
BEGIN
  UPDATE StageRepackRequest SET status = tconst.REPACK_STARTING
   WHERE reqId = inReqUUID
  RETURNING id, euid, egid, svcClass, repackVID
    INTO varReqId, varEuid, varEgid, svcClassId, varRepackVID;
  -- commit so that the status change is visible, even if the request is empty for the moment
  COMMIT;
  outNbFilesProcessed := 0;
  outNbFailures := 0;
  -- creation time for the subrequests
  varCreationTime := getTime();
  -- name server host name
  nsHostName := getConfigOption('stager', 'nsHost', '');
  -- find out the recallGroup to be used for this request
  getRecallGroup(varEuid, varEgid, varRecallGroupId, varRecallGroupName);
  -- Get the list of files to repack from the NS DB via DBLink and store them in memory
  -- in a temporary table. We do that so that we do not keep an open cursor for too long
  -- in the nameserver DB
  -- Note the truncation of stagerTime to 5 digits. This is needed for consistency with
  -- the stager code that uses the OCCI api and thus loses precision when recuperating
  -- 64 bits integers into doubles (lack of support for 64 bits numbers in OCCI)
  INSERT INTO RepackTapeSegments (fileId, lastOpenTime, blockId, fseq, segSize, copyNb, fileClass, allSegments)
    (SELECT s_fileid, TRUNC(stagertime,5), blockid, fseq, segSize,
            copyno, fileclass,
            (SELECT LISTAGG(TO_CHAR(oseg.copyno)||','||oseg.vid, ',')
             WITHIN GROUP (ORDER BY copyno)
               FROM Cns_Seg_Metadata@remotens oseg
              WHERE oseg.s_fileid = seg.s_fileid
                AND oseg.s_status = '-'
              GROUP BY oseg.s_fileid)
       FROM Cns_Seg_Metadata@remotens seg, Cns_File_Metadata@remotens fileEntry
      WHERE seg.vid = varRepackVID
        AND seg.s_fileid = fileEntry.fileid
        AND seg.s_status = '-'
        AND fileEntry.status != 'D');
  FOR segment IN (SELECT * FROM RepackTapeSegments) LOOP
    DECLARE
      varSubreqId INTEGER;
      varSubreqUUID VARCHAR2(2048);
      varSrStatus INTEGER := dconst.SUBREQUEST_REPACK;
      varMjStatus INTEGER := tconst.MIGRATIONJOB_PENDING;
      varNbCopies INTEGER;
      varSrErrorCode INTEGER := 0;
      varSrErrorMsg VARCHAR2(2048) := NULL;
      varWasRecalled NUMBER;
      varMigrationTriggered BOOLEAN := False;
    BEGIN
      IF MOD(outNbFilesProcessed, 1000) = 0 THEN
        -- Commit from time to time. Update total counter so that the display is correct.
        UPDATE StageRepackRequest
           SET fileCount = outNbFilesProcessed
         WHERE reqId = inReqUUID;
        COMMIT;
        firstCF := TRUE;
      END IF;
      outNbFilesProcessed := outNbFilesProcessed + 1;
      varTotalSize := varTotalSize + segment.segSize;
      -- lastKnownFileName we will have in the DB
      lastKnownFileName := CONCAT('Repack_', TO_CHAR(segment.fileid));
      -- find the Castorfile (and take a lock on it)
      DECLARE
        locked EXCEPTION;
        PRAGMA EXCEPTION_INIT (locked, -54);
      BEGIN
        -- This may raise a Locked exception as we do not want to wait for locks (except on first file).
        -- In such a case, we commit what we've done so far and retry this file, this time waiting for the lock.
        -- The reason for such a complex code is to avoid commiting each file separately, as it would be
        -- too heavy. On the other hand, we still need to avoid dead locks.
        -- Note that we pass 0 for the subrequest id, thus the subrequest will not be attached to the
        -- CastorFile. We actually attach it when we create it.
        selectCastorFileInternal(segment.fileid, nsHostName, segment.fileclass,
                                 segment.segSize, lastKnownFileName, 0, segment.lastOpenTime, firstCF, cfid, unused);
        firstCF := FALSE;
      EXCEPTION WHEN locked THEN
        -- commit what we've done so far
        COMMIT;
        -- And lock the castorfile (waiting this time)
        selectCastorFileInternal(segment.fileid, nsHostName, segment.fileclass,
                                 segment.segSize, lastKnownFileName, 0, segment.lastOpenTime, TRUE, cfid, unused);
      END;
      -- create  subrequest for this file.
      -- Note that the svcHandler is not set. It will actually never be used as repacks are handled purely in PL/SQL
      INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, status, request, getNextStatus, reqType)
      VALUES (0, lastKnownFileName, 'notNullNeeded', segment.segSize, 0, uuidGen(), 0, 0, varCreationTime, varCreationTime, 0, '', NULL, 'NotNullNeeded', ids_seq.nextval, 0, cfId, dconst.SUBREQUEST_START, varReqId, 0, 119)
      RETURNING id, subReqId INTO varSubreqId, varSubreqUUID;
      -- if the file is being overwritten, fail
      SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */
             count(DiskCopy.id) INTO varNbCopies
        FROM DiskCopy
       WHERE DiskCopy.castorfile = cfId
         AND DiskCopy.status = dconst.DISKCOPY_STAGEOUT;
      IF varNbCopies > 0 THEN
        varSrStatus := dconst.SUBREQUEST_FAILED;
        varSrErrorCode := serrno.EBUSY;
        varSrErrorMsg := 'File is currently being overwritten';
        outNbFailures := outNbFailures + 1;
      ELSE
        -- find out whether this file is already on disk
        SELECT count(id) INTO varNbCopies FROM (
          SELECT DiskCopy.id
            FROM DiskCopy, FileSystem, DiskServer
           WHERE DiskCopy.castorfile = cfId
             AND DiskCopy.fileSystem = FileSystem.id
             AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
             AND FileSystem.diskserver = DiskServer.id
             AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
             AND DiskServer.hwOnline = 1
             AND DiskCopy.status = dconst.DISKCOPY_VALID
          UNION
          SELECT DiskCopy.id
            FROM DiskCopy
           WHERE DiskCopy.castorfile = cfId
             AND DiskCopy.status = dconst.DISKCOPY_VALID
             AND EXISTS (SELECT 1 FROM DiskServer
                          WHERE DiskCopy.dataPool = DiskServer.DataPool
                            AND DiskServer.hwOnline = 1));
        IF varNbCopies = 0 THEN
          -- find out whether this file is already being recalled from this tape
          SELECT /*+ INDEX_RS_ASC(RecallJob I_RecallJob_CastorFile) */ count(*)
            INTO varWasRecalled FROM RecallJob WHERE castorfile = cfId AND vid != varRepackVID;
          IF varWasRecalled = 0 THEN
            -- trigger recall: if we have dual copy files, this may trigger a second recall,
            -- which will race with the first as it happens for user-driven recalls
            triggerRepackRecall(cfId, segment.fileid, nsHostName, segment.blockid,
                                segment.fseq, segment.copyNb, varEuid, varEgid,
                                varRecallGroupId, svcClassId, varRepackVID, segment.segSize,
                                segment.fileclass, segment.allSegments, inReqUUID, varSubreqUUID, varRecallGroupName);
          END IF;
          -- file is being recalled
          varSrStatus := dconst.SUBREQUEST_WAITTAPERECALL;
          isOngoing := TRUE;
        END IF;
        -- deal with migrations
        IF varSrStatus = dconst.SUBREQUEST_WAITTAPERECALL THEN
          varMJStatus := tconst.MIGRATIONJOB_WAITINGONRECALL;
        END IF;
        DECLARE
          noValidCopyNbFound EXCEPTION;
          PRAGMA EXCEPTION_INIT (noValidCopyNbFound, -20123);
          noMigrationRoute EXCEPTION;
          PRAGMA EXCEPTION_INIT (noMigrationRoute, -20100);
        BEGIN
          triggerRepackMigration(cfId, varRepackVID, segment.fileid, segment.copyNb, segment.fileclass,
                                 segment.segSize, segment.allSegments, varMJStatus, varMigrationTriggered);
          IF varMigrationTriggered THEN
            -- update CastorFile tapeStatus
            UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_NOTONTAPE WHERE id = cfId;
          END IF;
          isOngoing := True;
        EXCEPTION WHEN noValidCopyNbFound OR noMigrationRoute THEN
          -- log
          logToDLF(NULL, dlf.LVL_ERROR, dlf.REPACK_UNEXPECTED_EXCEPTION, segment.fileId, nsHostName, 'repackd',
                   'errorCode=' || to_char(SQLCODE) ||' errorMessage="' || SQLERRM ||'"');
          -- cleanup recall part if needed
          IF varWasRecalled = 0 THEN
            DELETE FROM RecallJob WHERE castorFile = cfId;
          END IF;
          -- fail SubRequest
          varSrStatus := dconst.SUBREQUEST_FAILED;
          varSrErrorCode := serrno.EINVAL;
          varSrErrorMsg := SQLERRM;
          outNbFailures := outNbFailures + 1;
        END;
      END IF;
      -- update SubRequest
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
         SET status = varSrStatus,
             errorCode = varSrErrorCode,
             errorMessage = varSrErrorMsg
       WHERE id = varSubreqId;
    EXCEPTION WHEN OTHERS THEN
      -- something went wrong: log "handleRepackRequest: unexpected exception caught"
      outNbFailures := outNbFailures + 1;
      varSrErrorMsg := 'Oracle error caught : ' || SQLERRM;
      logToDLF(NULL, dlf.LVL_ERROR, dlf.REPACK_UNEXPECTED_EXCEPTION, segment.fileId, nsHostName, 'repackd',
        'errorCode=' || to_char(SQLCODE) ||' errorMessage="' || varSrErrorMsg
        ||'" stackTrace="' || dbms_utility.format_error_backtrace() ||'"');
      -- cleanup and fail SubRequest
      IF varWasRecalled = 0 THEN
        DELETE FROM RecallJob WHERE castorFile = cfId;
      END IF;
      IF varMigrationTriggered THEN
        DELETE FROM MigrationJob WHERE castorFile = cfId;
        DELETE FROM MigratedSegment WHERE castorFile = cfId;
      END IF;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.SEINTERNAL,
             errorMessage = varSrErrorMsg
       WHERE id = varSubreqId;
    END;
  END LOOP;
  -- cleanup RepackTapeSegments
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RepackTapeSegments';
  -- update status of the RepackRequest
  IF isOngoing THEN
    UPDATE StageRepackRequest
       SET status = tconst.REPACK_ONGOING,
           fileCount = outNbFilesProcessed,
           totalSize = varTotalSize
     WHERE StageRepackRequest.id = varReqId;
  ELSE
    IF outNbFailures > 0 THEN
      UPDATE StageRepackRequest
         SET status = tconst.REPACK_FAILED,
             fileCount = outNbFilesProcessed,
             totalSize = varTotalSize
       WHERE StageRepackRequest.id = varReqId;
    ELSE
      -- CASE of an 'empty' repack : the tape had no files at all
      UPDATE StageRepackRequest
         SET status = tconst.REPACK_FINISHED,
             fileCount = 0,
             totalSize = 0
       WHERE StageRepackRequest.id = varReqId;
    END IF;
  END IF;
  COMMIT;
END;
/

/* PL/SQL method to process bulk abort on files related requests */
CREATE OR REPLACE PROCEDURE processBulkAbortFileReqs
(origReqId IN INTEGER, fileIds IN "numList", nsHosts IN strListTable, reqType IN NUMBER) AS
  nbItems NUMBER;
  nbItemsDone NUMBER := 0;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  unused NUMBER;
  firstOne BOOLEAN := TRUE;
  commitWork BOOLEAN := FALSE;
BEGIN
  -- Gather the list of subrequests to abort
  IF fileIds.count() = 0 THEN
    -- handle the case of an empty request, meaning that all files should be aborted
    INSERT INTO ProcessBulkAbortFileReqsHelper (srId, cfId, fileId, nsHost, uuid) (
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Request)*/
             SubRequest.id, CastorFile.id, CastorFile.fileId, CastorFile.nsHost, SubRequest.subreqId
        FROM SubRequest, CastorFile
       WHERE SubRequest.castorFile = CastorFile.id
         AND request = origReqId);
  ELSE
    -- handle the case of selective abort
    FOR i IN 1..fileIds.COUNT LOOP
      DECLARE
        CONSTRAINT_VIOLATED EXCEPTION;
        PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
      BEGIN
        -- note that we may insert several rows in one go in case the abort request contains
        -- several times the same file
        INSERT INTO processBulkAbortFileReqsHelper
          (SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_CastorFile)*/
                  DISTINCT SubRequest.id, CastorFile.id, fileIds(i), nsHosts(i), SubRequest.subreqId
             FROM SubRequest, CastorFile
            WHERE request = origReqId
              AND SubRequest.castorFile = CastorFile.id
              AND CastorFile.fileid = fileIds(i)
              AND CastorFile.nsHost = nsHosts(i));
        -- check that we found something
        IF SQL%ROWCOUNT = 0 THEN
          -- this fileid/nshost did not exist in the request, send an error back
          INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
          VALUES (fileIds(i), nsHosts(i), serrno.ENOENT, 'No subRequest found for this fileId/nsHost');
        END IF;
      EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
        -- the insertion in ProcessBulkRequestHelper triggered a violation of the
        -- primary key. This primary key being the subrequest id, this means that
        -- this subrequest is already in the list of the ones to be aborted. So
        -- nothing left to be done
        NULL;
      END;
    END LOOP;
  END IF;
  SELECT COUNT(*) INTO nbItems FROM processBulkAbortFileReqsHelper;
  -- handle aborts in bulk while avoiding deadlocks
  WHILE nbItems > 0 LOOP
    FOR sr IN (SELECT srId, cfId, fileId, nsHost, uuid FROM processBulkAbortFileReqsHelper) LOOP
      BEGIN
        IF firstOne THEN
          -- on the first item, we take a blocking lock as we are sure that we will not
          -- deadlock and we would like to process at least one item to not loop endlessly
          SELECT id INTO unused FROM CastorFile WHERE id = sr.cfId FOR UPDATE;
          firstOne := FALSE;
        ELSE
          -- on the other items, we go for a non blocking lock. If we get it, that's
          -- good and we process this extra subrequest within the same session. If
          -- we do not get the lock, then we close the session here and go for a new
          -- one. This will prevent dead locks while ensuring that a minimal number of
          -- commits is performed.
          SELECT id INTO unused FROM CastorFile WHERE id = sr.cfId FOR UPDATE NOWAIT;
        END IF;
        -- we got the lock on the Castorfile, we can handle the abort for this subrequest
        CASE reqType
          WHEN 1 THEN processAbortForGet(sr);
          WHEN 2 THEN processAbortForPut(sr);
        END CASE;
        DELETE FROM processBulkAbortFileReqsHelper WHERE srId = sr.srId;
        -- make the scheduler aware so that it can remove the transfer from the queues if needed
        INSERT INTO TransfersToAbort (uuid) VALUES (sr.uuid);
        nbItemsDone := nbItemsDone + 1;
      EXCEPTION WHEN SrLocked THEN
        commitWork := TRUE;
      END;
      -- commit anyway from time to time, to avoid too long redo logs
      IF commitWork OR nbItemsDone >= 1000 THEN
        -- exit the current loop and restart a new one, in order to commit without getting invalid ROWID errors
        EXIT;
      END IF;
    END LOOP;
    -- commit
    COMMIT;
    -- wake up the scheduler so that it can remove the transfer from the queues
    alertSignalNoLock('transfersToAbort');
    -- reset all counters
    nbItems := nbItems - nbItemsDone;
    nbItemsDone := 0;
    firstOne := TRUE;
    commitWork := FALSE;
  END LOOP;
END;
/

/* PL/SQL method implementing putStart */
CREATE OR REPLACE PROCEDURE putStart(inTransferId IN VARCHAR2, selectedDiskServer IN VARCHAR2,
                                     selectedMountPoint IN VARCHAR2, outPath OUT VARCHAR2) AS
  varCfId INTEGER;
  varDcId INTEGER;
  srStatus INTEGER;
  srSvcClass INTEGER;
  fsId INTEGER := NULL;
  varDsId INTEGER := NULL;
  dpId INTEGER := NULL;
  fsStatus INTEGER := dconst.FILESYSTEM_PRODUCTION;
  dsStatus INTEGER;
  varHwOnline INTEGER;
  prevFsId INTEGER;
  prevDPId INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(100);
  varDpName VARCHAR2(2048);
BEGIN
  -- Get data and lock castorfile
  SELECT /*+ INDEX_RS_ASC(SubRequest I_SubRequest_SubReqId) */
         SubRequest.castorFile, DiskCopy.id, SubRequest.status, DiskCopy.fileSystem,
         DiskCopy.dataPool, Request.svcClass, CastorFile.fileId, CastorFile.nsHost
    INTO varCfId, varDcId, srStatus, prevFsId, prevDPId, srSvcClass, varFileid, varNsHost
    FROM SubRequest, DiskCopy, StagePutRequest Request, CastorFile
   WHERE SubRequest.diskcopy = Diskcopy.id
     AND SubRequest.subreqId = inTransferId
     AND SubRequest.request = Request.id
     AND CastorFile.id = SubRequest.castorFile
     FOR UPDATE OF CastorFile.id;
  -- Check that we did not cancel the SubRequest in the mean time
  IF srStatus IN (dconst.SUBREQUEST_FAILED, dconst.SUBREQUEST_FAILED_FINISHED) THEN
    raise_application_error(-20104, 'SubRequest canceled while queuing in scheduler. Giving up.');
  END IF;
  -- Get selected filesystem/datapool
  IF selectedMountPoint IS NULL THEN
    SELECT DiskServer.dataPool, DiskServer.status,
           DiskServer.hwOnline, DiskServer.id, DataPool.name
      INTO dpId, dsStatus, varHwOnline, varDsId, varDpName
      FROM DiskServer, DataPool
     WHERE DiskServer.name = selectedDiskServer
       AND DataPool.id = DiskServer.dataPool;
  ELSE 
    SELECT FileSystem.id, FileSystem.status, DiskServer.status, DiskServer.hwOnline
      INTO fsId, fsStatus, dsStatus, varHwOnline
      FROM DiskServer, FileSystem
     WHERE FileSystem.diskserver = DiskServer.id
       AND DiskServer.name = selectedDiskServer
       AND FileSystem.mountPoint = selectedMountPoint;
  END IF;
  -- Check that a job has not already started for this diskcopy. Refer to
  -- bug #14358
  IF (prevFsId > 0 AND prevFsId <> fsId) OR (prevDPId > 0 AND prevDPId <> dpId) THEN
    raise_application_error(-20104, 'This job has already started for this DiskCopy. Giving up.');
  END IF;
  IF fsStatus != dconst.FILESYSTEM_PRODUCTION OR dsStatus != dconst.DISKSERVER_PRODUCTION OR varHwOnline = 0 THEN
    raise_application_error(-20104, 'The selected diskserver/filesystem is not in PRODUCTION any longer. Giving up.');
  END IF;
  -- In case the DiskCopy was in WAITFS_SCHEDULING,
  -- restart the waiting SubRequests
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_RESTART, lastModificationTime = getTime(),
         diskServer = varDsId
   WHERE status = dconst.SUBREQUEST_WAITSUBREQ
     AND castorFile = varCfId;
  alertSignalNoLock('wakeUpJobReqSvc');
  -- compute path of this diskcopy
  buildPathFromFileId(varFileId, varNsHost, varDcId, outPath, selectedMountPoint IS NOT NULL);
  -- link DiskCopy and FileSystem/DataPool and update DiskCopyStatus
  UPDATE DiskCopy
     SET status = 6, -- DISKCOPY_STAGEOUT
         path = outPath,
         fileSystem = fsId,
         dataPool = dpId,
         nbCopyAccesses = nbCopyAccesses + 1
   WHERE id = varDcId;
  IF selectedMountPoint IS NULL THEN
    outPath := getConfigOption('DiskManager', 'PoolUserId', 'castor') || '@' || varDpName || '/' || outPath;
  ELSE
    outPath := selectedMountPoint || outPath;
  END IF;
  -- Log successful completion
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.STAGER_PUTSTART, varFileId, varNsHost, 'stagerd', 'SUBREQID='|| inTransferId
    || 'destinationPath=' || selectedDiskServer ||':'|| selectedMountPoint);
EXCEPTION WHEN NO_DATA_FOUND THEN
  raise_application_error(-20104, 'SubRequest canceled while queuing in scheduler. Giving up.');
END;
/

CREATE OR REPLACE PROCEDURE deleteStaleDisk2DiskCopyJobs(timeOut IN NUMBER) AS
-- Select stale Disk2DiskCopyJob entries, that is either already scheduled/running
-- or still pending but originated by a user request (other types of replication may take very long)
  CURSOR s IS
    SELECT id FROM Disk2DiskCopyJob
     WHERE (replicationType = dconst.REPLICATIONTYPE_USER
         OR status IN (dconst.DISK2DISKCOPYJOB_SCHEDULED, dconst.DISK2DISKCOPYJOB_RUNNING))
       AND creationTime < getTime() - timeOut;
  ids "numList";
  varCfId INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(100);
BEGIN
  OPEN s;
  LOOP
    FETCH s BULK COLLECT INTO ids LIMIT 10000;
    EXIT WHEN ids.count = 0;
    FOR i IN 1..ids.COUNT LOOP
      SELECT castorFile INTO varCfId FROM Disk2DiskCopyJob WHERE id = ids(i);
      SELECT fileid, nsHost INTO varFileId, varNsHost FROM CastorFile
       WHERE id = varCfId
       FOR UPDATE;
      DELETE FROM Disk2DiskCopyJob WHERE id = ids(i);
      UPDATE SubRequest
         SET status = dconst.SUBREQUEST_RESTART
       WHERE status = dconst.SUBREQUEST_WAITSUBREQ
         AND castorFile = varCfId;
      COMMIT;
      logToDLF(NULL, dlf.LVL_WARNING, dlf.D2D_DROPPED_BY_CLEANING, varFileId, varNsHost, 'stagerd', '');
    END LOOP;
  END LOOP;
END;
/

/* Runs cleanup operations */
CREATE OR REPLACE PROCEDURE cleanup AS
  t INTEGER;
BEGIN
  -- First perform some cleanup of old stuff:
  -- for each, read relevant timeout from configuration table
  t := TO_NUMBER(getConfigOption('cleaning', 'outOfDateStageOutDCsTimeout', '72'));
  deleteOutOfDateStageOutDCs(t*3600);
  t := TO_NUMBER(getConfigOption('cleaning', 'failedDCsTimeout', '72'));
  deleteFailedDiskCopies(t*3600);
  t := TO_NUMBER(getConfigOption('cleaning', 'staleDisk2DiskCopyJobsTimeout', '6'));
  deleteStaleDisk2DiskCopyJobs(t*3600);
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
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -2292);
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- List the castorfiles to be cleaned up afterwards
    FORALL i IN 1..dcIds.COUNT
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
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- This means that the castorFile did not exist.
        -- There is thus no way to find out whether to remove the
        -- file from the nameserver. For safety, we thus keep it
        CONTINUE;
      END;
      -- delete the original diskcopy to be dropped
      DELETE FROM DiskCopy WHERE id = cf.dcId;
      -- Cleanup: attempt to delete the CastorFile. Thanks to FKs,
      -- this will fail if in the meantime some other activity took
      -- ownership of the CastorFile entry.
      BEGIN
        DELETE FROM CastorFile WHERE id = cf.cfId;
        -- And check whether this file potentially had copies on tape
        SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
        IF nb = 0 THEN
          -- This castorfile was created with no copy on tape
          -- So removing it from the stager means erasing
          -- it completely. We should thus also remove it
          -- from the name server
          INSERT INTO FilesDeletedProcOutput (fileId, nsHost) VALUES (fid, nsh);
        END IF;
      EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
        -- Ignore the deletion, some draining/rebalancing/recall activity
        -- started to reuse the CastorFile
        NULL;
      END;
    END LOOP;
  END IF;
  OPEN fileIds FOR
    SELECT fileId, nsHost FROM FilesDeletedProcOutput;
END;
/

/* PL/SQL method implementing filesDeletionFailedProc */
CREATE OR REPLACE PROCEDURE filesDeletionFailedProc
(dcIds IN castor."cnumList") AS
  cfId NUMBER;
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- Loop over the files
    FORALL i IN 1..dcIds.COUNT
      UPDATE DiskCopy SET status = 4 -- FAILED
       WHERE id = dcIds(i);
  END IF;
END;
/

/* PL/SQL method implementing nsFilesDeletedProc */
CREATE OR REPLACE PROCEDURE nsFilesDeletedProc
(nh IN VARCHAR2,
 fileIds IN castor."cnumList",
 orphans OUT castor.IdRecord_Cur) AS
  unused NUMBER;
  nsHostName VARCHAR2(2048);
BEGIN
  IF fileIds.COUNT <= 0 THEN
    RETURN;
  END IF;
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- Loop over the deleted files and split the orphan ones
  -- from the normal ones
  FOR fid in 1..fileIds.COUNT LOOP
    BEGIN
      SELECT id INTO unused FROM CastorFile
       WHERE fileid = fileIds(fid) AND nsHost = nsHostName;
      stageForcedRm(fileIds(fid), nsHostName, dconst.GCTYPE_NSSYNCH);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- this file was dropped from nameServer AND stager
      -- and still exists on disk. We put it into the list
      -- of orphan fileids to return
      INSERT INTO NsFilesDeletedOrphans (fileId) VALUES (fileIds(fid));
    END;
  END LOOP;
  -- return orphan ones
  OPEN orphans FOR SELECT * FROM NsFilesDeletedOrphans;
END;
/

/* PL/SQL method implementing stgFilesDeletedProc */
CREATE OR REPLACE PROCEDURE stgFilesDeletedProc
(dcIds IN castor."cnumList",
 stgOrphans OUT castor.IdRecord_Cur) AS
  unused NUMBER;
BEGIN
  -- Nothing to do
  IF dcIds.COUNT <= 0 THEN
    RETURN;
  END IF;
  -- Insert diskcopy ids into a temporary table
  FORALL i IN 1..dcIds.COUNT
   INSERT INTO StgFilesDeletedOrphans (diskCopyId) VALUES (dcIds(i));
  -- Return a list of diskcopy ids which no longer exist
  OPEN stgOrphans FOR
    SELECT diskCopyId FROM StgFilesDeletedOrphans
     WHERE NOT EXISTS (
        SELECT /*+ INDEX(DiskCopy PK_DiskCopy_Id) */ 'x' FROM DiskCopy
         WHERE id = diskCopyId);
END;
/

/* attach the tapes to the migration mounts  */
CREATE OR REPLACE
PROCEDURE tg_attachTapesToMigMounts (
  inStartFseqs IN castor."cnumList",
  inMountIds   IN castor."cnumList",
  inTapeVids   IN castor."strList") AS
BEGIN
  -- Sanity check
  IF (inStartFseqs.COUNT != inTapeVids.COUNT) THEN
    RAISE_APPLICATION_ERROR (-20119,
       'Size mismatch for arrays: inStartFseqs.COUNT='||inStartFseqs.COUNT
       ||' inTapeVids.COUNT='||inTapeVids.COUNT);
  END IF;
  FORALL i IN 1..inMountIds.COUNT
    UPDATE MigrationMount
       SET VID = inTapeVids(i),
           lastFseq = inStartFseqs(i),
           startTime = getTime(),
           status = tconst.MIGRATIONMOUNT_SEND_TO_VDQM
     WHERE id = inMountIds(i);
  COMMIT;
END;
/

/* restart taperequest which had problems */
CREATE OR REPLACE
PROCEDURE tg_restartLostReqs(inMountTransactionIds IN castor."cnumList") AS
BEGIN
 FOR i IN 1..inMountTransactionIds.COUNT LOOP
   tg_endTapeSession(inMountTransactionIds(i), 0);
 END LOOP;
 COMMIT;
END;
/

/* Commit a set of succeeded/failed migration processes to the NS and stager db.
 * Locks are taken on the involved castorfiles one by one, then to the dependent entities.
 */
CREATE OR REPLACE PROCEDURE tg_setBulkFileMigrationResult(inLogContext IN VARCHAR2,
                                                          inMountTrId IN NUMBER,
                                                          inFileIds IN "numList",
                                                          inFileTrIds IN "numList",
                                                          inFseqs IN "numList",
                                                          inBlockIds IN strListTable,
                                                          inChecksumTypes IN strListTable,
                                                          inChecksums IN "numList",
                                                          inComprSizes IN "numList",
                                                          inTransferredSizes IN "numList",
                                                          inErrorCodes IN "numList",
                                                          inErrorMsgs IN strListTable
                                                          ) AS
  varStartTime TIMESTAMP;
  varNsHost VARCHAR2(2048);
  varReqId VARCHAR2(36);
  varNsOpenTime NUMBER;
  varCopyNo NUMBER;
  varOldCopyNo NUMBER;
  varVid VARCHAR2(10);
  varNSIsOnlyLogs "numList";
  varNSTimeInfos floatList;
  varNSErrorCodes "numList";
  varNSMsgs strListTable;
  varNSFileIds "numList" := "numList"();
  varNSParams strListTable;
  varParams VARCHAR2(4000);
  varNbSentToNS INTEGER := 0;
  varLastUpdateTime INTEGER;
BEGIN
  varStartTime := SYSTIMESTAMP;
  varReqId := uuidGen();
  -- Get the NS host name
  varNsHost := getConfigOption('stager', 'nsHost', '');
  FOR i IN 1..inFileTrIds.COUNT LOOP
    BEGIN
      -- Collect additional data. Note that this is NOT bulk
      -- to preserve the order in the input arrays.
      SELECT CF.nsOpenTime, CF.lastUpdateTime, nvl(MJ.originalCopyNb, 0), MJ.vid, MJ.destCopyNb
        INTO varNsOpenTime, varLastUpdateTime, varOldCopyNo, varVid, varCopyNo
        FROM CastorFile CF, MigrationJob MJ
       WHERE MJ.castorFile = CF.id
         AND CF.fileid = inFileIds(i)
         AND MJ.mountTransactionId = inMountTrId
         AND MJ.fileTransactionId = inFileTrIds(i)
         AND status = tconst.MIGRATIONJOB_SELECTED;
        -- Store in a temporary table, to be transfered to the NS DB
      IF inErrorCodes(i) = 0 THEN
        -- Successful migration
        INSERT INTO FileMigrationResultsHelper
          (reqId, fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
           vid, fseq, blockId, checksumType, checksum)
        VALUES (varReqId, inFileIds(i), varNsOpenTime, varCopyNo, varOldCopyNo,
                inTransferredSizes(i), CASE inComprSizes(i) WHEN 0 THEN 1 ELSE inComprSizes(i) END, varVid, inFseqs(i),
                strtoRaw4(inBlockIds(i)), inChecksumTypes(i), inChecksums(i));
        varNbSentToNS := varNbSentToNS + 1;
      ELSE
        -- Fail/retry this migration, log 'migration failed, will retry if allowed'
        varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' ErrorCode='|| to_char(inErrorCodes(i))
                     ||' ErrorMessage="'|| inErrorMsgs(i) ||'" TPVID='|| varVid
                     ||' copyNb='|| to_char(varCopyNo) ||' '|| inLogContext;
        logToDLF(varReqid, dlf.LVL_WARNING, dlf.MIGRATION_RETRY, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
        retryOrFailMigration(inMountTrId, inFileIds(i), varNsHost, inErrorCodes(i), varReqId);
        -- here we commit immediately because retryOrFailMigration took a lock on the CastorFile
        COMMIT;
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Log 'unable to identify migration, giving up'
      varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' fileTransactionId='|| to_char(inFileTrIds(i))
        ||' '|| inLogContext;
      logToDLF(varReqid, dlf.LVL_ERROR, dlf.MIGRATION_NOT_FOUND, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
    END;
  END LOOP;
  -- Commit all the entries in FileMigrationResultsHelper so that the next call can take them
  COMMIT;

  DECLARE
    varUnused INTEGER;
  BEGIN
    -- boundary case: if nothing to do, just skip the remote call and the
    -- subsequent FOR loop as it would be useless (and would fail).
    SELECT 1 INTO varUnused FROM FileMigrationResultsHelper
     WHERE reqId = varReqId AND ROWNUM < 2;
    -- The following procedure wraps the remote calls in an autonomous transaction
    ns_setOrReplaceSegments(varReqId, varNSIsOnlyLogs, varNSTimeInfos, varNSErrorCodes, varNSMsgs, varNSFileIds, varNSParams);

    -- Process the results
    FOR i IN 1 .. varNSFileIds.COUNT LOOP
      -- First log on behalf of the NS
      -- We classify the log level based on the error code here.
      -- Current error codes are:
      --   ENOENT, EACCES, EBUSY, EEXIST, EISDIR, EINVAL, SEINTERNAL, SECHECKSUM, ENSFILECHG, ENSNOSEG
      --   ENSTOOMANYSEGS, ENSOVERWHENREP, ERTWRONGSIZE, ESTNOSEGFOUND
      -- default level is ERROR. Some cases can be demoted to warning when it's a normal case
      -- (like file deleted by user in the mean time).
      logToDLFWithTime(varNSTimeinfos(i), varReqid,
                       CASE varNSErrorCodes(i) 
                         WHEN 0                 THEN dlf.LVL_SYSTEM
                         WHEN serrno.ENOENT     THEN dlf.LVL_WARNING
                         WHEN serrno.ENSFILECHG THEN dlf.LVL_WARNING
                         ELSE                        dlf.LVL_ERROR
                       END,
                       varNSMsgs(i), varNSFileIds(i), varNsHost, 'nsd', varNSParams(i));
      -- Now skip pure log entries and process file by file, depending on the result
      IF varNSIsOnlyLogs(i) = 1 THEN CONTINUE; END IF;
      CASE
      WHEN varNSErrorCodes(i) = 0 THEN
        -- All right, commit the migration in the stager
        tg_setFileMigrated(inMountTrId, varNSFileIds(i), varReqId, inLogContext);

      WHEN varNSErrorCodes(i) = serrno.ENOENT
        OR varNSErrorCodes(i) = serrno.ENSNOSEG
        OR varNSErrorCodes(i) = serrno.ENSFILECHG
        OR varNSErrorCodes(i) = serrno.ENSTOOMANYSEGS THEN
        -- The migration was useless because either the file is gone, or it has been modified elsewhere,
        -- or there were already enough copies on tape for it. Fail and update disk cache accordingly.
        failFileMigration(inMountTrId, varNSFileIds(i), varNSErrorCodes(i), varReqId);

      ELSE
        -- Attempt to retry for all other NS errors. To be reviewed whether some of the NS errors are to be considered fatal.
        varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' '|| varNSParams(i) ||' '|| inLogContext;
        logToDLF(varReqid, dlf.LVL_WARNING, dlf.MIGRATION_RETRY, varNSFileIds(i), varNsHost, 'tapegatewayd', varParams);
        retryOrFailMigration(inMountTrId, varNSFileIds(i), varNsHost, varNSErrorCodes(i), varReqId);
      END CASE;
      -- Commit file by file
      COMMIT;
    END LOOP;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Nothing to do after processing the error cases
    NULL;
  END;
  -- Final log, "setBulkFileMigrationResult: bulk migration completed"
  varParams := 'mountTransactionId='|| to_char(inMountTrId)
               ||' NbInputFiles='|| inFileIds.COUNT
               ||' NbSentToNS='|| varNbSentToNS
               ||' NbFilesBackFromNS='|| varNSFileIds.COUNT
               ||' '|| inLogContext
               ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
               ||' AvgProcessingTime='|| trunc(getSecs(varStartTime, SYSTIMESTAMP)/inFileIds.COUNT, 6);
  logToDLF(varReqid, dlf.LVL_SYSTEM, dlf.BULK_MIGRATION_COMPLETED, 0, '', 'tapegatewayd', varParams);
END;
/

/* Commit a set of succeeded/failed recall processes to the NS and stager db.
 * Locks are taken on the involved castorfiles one by one, then to the dependent entities.
 */
CREATE OR REPLACE PROCEDURE tg_setBulkFileRecallResult(inLogContext IN VARCHAR2,
                                                       inMountTrId IN NUMBER,
                                                       inFileIds IN "numList",
                                                       inFileTrIds IN "numList",
                                                       inFilePaths IN strListTable,
                                                       inFseqs IN "numList",
                                                       inChecksumNames IN strListTable,
                                                       inChecksums IN "numList",
                                                       inFileSizes IN "numList",
                                                       inErrorCodes IN "numList",
                                                       inErrorMsgs IN strListTable) AS
  varCfId NUMBER;
  varVID VARCHAR2(10);
  varReqId VARCHAR2(36);
  varStartTime TIMESTAMP;
  varNsHost VARCHAR2(2048);
  varParams VARCHAR2(4000);
BEGIN
  varStartTime := SYSTIMESTAMP;
  varReqId := uuidGen();
  -- Get the NS host name
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Get the current VID
  SELECT VID INTO varVID
    FROM RecallMount
   WHERE mountTransactionId = inMountTrId;
  -- Loop over the input
  FOR i IN 1..inFileIds.COUNT LOOP
    BEGIN
      -- Find and lock related castorFile
      SELECT id INTO varCfId
        FROM CastorFile
       WHERE fileid = inFileIds(i)
         AND nsHost = varNsHost
         FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- log "Unable to identify Recall. giving up"
      logToDLF(varReqId, dlf.LVL_ERROR, dlf.RECALL_NOT_FOUND, inFileIds(i), varNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTrId) || ' TPVID=' || varVID ||
               ' fseq=' || TO_CHAR(inFseqs(i)) || ' filePath=' || inFilePaths(i) || ' ' || inLogContext);
      CONTINUE;
    END;
    -- Now deal with each recall one by one
    IF inErrorCodes(i) = 0 THEN
      -- Recall successful, check NS and update stager + log
      tg_setFileRecalled(inMountTrId, inFseqs(i), inFilePaths(i), inChecksumNames(i), inChecksums(i),
                         varReqId, inLogContext);
    ELSE
      -- Recall failed at tapeserver level, attempt to retry it
      -- log "setBulkFileRecallResult : recall process failed, will retry if allowed"
      logToDLF(varReqId, dlf.LVL_WARNING, dlf.RECALL_FAILED, inFileIds(i), varNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTrId) || ' TPVID=' || varVID ||
               ' fseq=' || TO_CHAR(inFseqs(i)) || ' errorMessage="' || inErrorMsgs(i) ||'" '|| inLogContext);
      retryOrFailRecall(varCfId, varVID, varReqId, inLogContext);
    END IF;
    COMMIT;
  END LOOP;
  -- log "setBulkFileRecallResult: bulk recall completed"
  varParams := 'mountTransactionId='|| to_char(inMountTrId)
               ||' NbFiles='|| inFileIds.COUNT ||' '|| inLogContext
               ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
               ||' AvgProcessingTime='|| trunc(getSecs(varStartTime, SYSTIMESTAMP)/inFileIds.COUNT, 6);
  logToDLF(varReqid, dlf.LVL_SYSTEM, dlf.BULK_RECALL_COMPLETED, 0, '', 'tapegatewayd', varParams);
END;
/
/* PL/SQL method implementing transferFailedSafe, providing bulk termination of file
 * transfers.
 */
CREATE OR REPLACE
PROCEDURE transferFailedSafe(subReqIds IN castor."strList",
                             errnos IN castor."cnumList",
                             errmsgs IN castor."strList") AS
  srId  NUMBER;
  dcId  NUMBER;
  cfId  NUMBER;
  rType NUMBER;
  errNo INTEGER;
  errMsg VARCHAR2(2048);
BEGIN
  -- give up if nothing to be done
  IF subReqIds.COUNT = 0 THEN RETURN; END IF;
  -- Loop over all transfers to fail
  FOR i IN 1..subReqIds.COUNT LOOP
    BEGIN
      -- Get the necessary information needed about the request.
      SELECT /*+ INDEX_RS_ASC(SubRequest I_Subrequest_SubreqId)*/ id, diskCopy, reqType, castorFile
        INTO srId, dcId, rType, cfId
        FROM SubRequest
       WHERE subReqId = subReqIds(i)
         AND status = dconst.SUBREQUEST_READY;
      -- Lock the CastorFile.
      SELECT id INTO cfId FROM CastorFile
       WHERE id = cfId FOR UPDATE;
      -- Confirm SubRequest status hasn't changed after acquisition of lock
      SELECT /*+ INDEX(SubRequest PK_Subrequest_Id)*/ id INTO srId FROM SubRequest
       WHERE id = srId AND status = dconst.SUBREQUEST_READY;
      -- Call the relevant cleanup procedure for the transfer, procedures that
      -- would have been called if the transfer failed on the remote execution host.
      errNo := errnos(i);
      errMsg := errmsgs(i);
      IF rType = 40 THEN      -- StagePutRequest
        putEnded(subReqIds(i), 0, 0, '', '', errNo, errMsg);
      ELSE                    -- StageGetRequest
        getEnded(subReqIds(i), errNo, errMsg);
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- try disk2diskCopyJob
        SELECT id into srId FROM Disk2diskCopyJob WHERE transferId = subReqIds(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        CONTINUE;  -- The SubRequest/disk2DiskCopyJob may have been removed, nothing to be done.
      END;
      disk2DiskCopyEnded(subReqIds(i), '', '', 0, '', errnos(i), errmsgs(i));
    END;
    -- Release locks
    COMMIT;
  END LOOP;
END;
/

/* PL/SQL method implementing transferFailedLockedFile, providing bulk termination of file
 * transfers. in case the castorfile is already locked
 */
CREATE OR REPLACE
PROCEDURE transferFailedLockedFile(subReqIds IN castor."strList",
                                   errnos IN castor."cnumList",
                                   errmsgs IN castor."strList")
AS
  srId  NUMBER;
  dcId  NUMBER;
  rType NUMBER;
  errNo INTEGER;
  errMsg VARCHAR2(2048);
BEGIN
  FOR i IN 1..subReqIds.COUNT LOOP
    BEGIN
      -- Get the necessary information needed about the request.
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_SubreqId)*/ id, diskCopy, reqType
        INTO srId, dcId, rType
        FROM SubRequest
       WHERE subReqId = subReqIds(i)
         AND status = dconst.SUBREQUEST_READY;
      -- Call the relevant cleanup procedure for the transfer, procedures that
      -- would have been called if the transfer failed on the remote execution host.
      errNo := errnos(i);
      errMsg := errmsgs(i);
      IF rType = 40 THEN      -- StagePutRequest
        putEnded(subReqIds(i), 0, 0, '', '', errNo, errMsg);
      ELSE                    -- StageGetRequest
        getEnded(subReqIds(i), errNo, errMsg);
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- try disk2diskCopyJob
        SELECT id into srId FROM Disk2diskCopyJob WHERE transferId = subReqIds(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        CONTINUE;  -- The SubRequest/disk2DiskCopyJob may have be removed, nothing to be done.
      END;
      -- found it, call disk2DiskCopyEnded
      disk2DiskCopyEnded(subReqIds(i), '', '', 0, '', errnos(i), errmsgs(i));
    END;
  END LOOP;
END;
/

/* PL/SQL procedure implementing triggerRepackMigration */
CREATE OR REPLACE PROCEDURE triggerRepackMigration
(cfId IN INTEGER, vid IN VARCHAR2, fileid IN INTEGER, copyNb IN INTEGER,
 fileclass IN INTEGER, fileSize IN INTEGER, allSegments IN VARCHAR2,
 inMJStatus IN INTEGER, migrationTriggered OUT boolean) AS
  varMjId INTEGER;
  varNb INTEGER;
  varDestCopyNb INTEGER;
  varNbCopies INTEGER;
  varAllSegments strListTable;
BEGIN
  -- check whether we already have a migrationJob for this copy of the file
  SELECT id INTO varMjId
    FROM MigrationJob
   WHERE castorFile = cfId
     AND destCopyNb = copyNb;
  -- we have a migrationJob for this copy ! This means that the file has been overwritten
  -- and the new version is about to go to tape. We thus don't have anything to do
  -- for this file
  migrationTriggered := False;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no migration job for this copyNb, we can proceed
  -- first let's parse the list of all segments
  SELECT * BULK COLLECT INTO varAllSegments
    FROM TABLE(strTokenizer(allSegments));
  DECLARE
    varAllCopyNbs "numList" := "numList"();
    varAllVIDs strListTable := strListTable();
  BEGIN
    FOR i IN 1..varAllSegments.COUNT/2 LOOP
      varAllCopyNbs.EXTEND;
      varAllCopyNbs(i) := TO_NUMBER(varAllSegments(2*i-1));
      varAllVIDs.EXTEND;
      varAllVIDs(i) := varAllSegments(2*i);
    END LOOP;
    -- find the new copy number to be used. This is the minimal one
    -- that is lower than the allowed number of copies and is not
    -- already used by an ongoing migration or by a valid migrated copy
    SELECT nbCopies INTO varNbCopies FROM FileClass WHERE classId = fileclass;
    FOR i IN 1 .. varNbCopies LOOP
      -- if we are reusing the original copy number, it's ok
      IF i = copyNb THEN
        varDestCopyNb := i;
      ELSE
        BEGIN
          -- check whether this copy number is already in use by a valid copy
          SELECT * INTO varNb FROM TABLE(varAllCopyNbs)
           WHERE COLUMN_VALUE = i;
          -- this copy number is in use, go to next one
        EXCEPTION WHEN NO_DATA_FOUND THEN
          BEGIN
            -- check whether this copy number is in use by an ongoing migration
            SELECT destCopyNb INTO varNb FROM MigrationJob WHERE castorFile = cfId AND destCopyNb = i;
            -- this copy number is in use, go to next one
          EXCEPTION WHEN NO_DATA_FOUND THEN
            -- copy number is not in use, we take it
            varDestCopyNb := i;
            EXIT;
          END;
        END;
      END IF;
    END LOOP;
    IF varDestCopyNb IS NULL THEN
      RAISE_APPLICATION_ERROR (-20123,
        'Unable to find a valid copy number for the migration of file ' ||
        TO_CHAR (fileid) || ' in triggerRepackMigration. ' ||
        'The file probably has too many valid copies.');
    END IF;
    -- create new migration
    initMigration(cfId, fileSize, vid, copyNb, varDestCopyNb, inMJStatus);
    -- create migrated segments for the existing segments if there are none
    SELECT /*+ INDEX_RS_ASC (MigratedSegment I_MigratedSegment_CFCopyNbVID) */ count(*) INTO varNb
      FROM MigratedSegment
     WHERE castorFile = cfId;
    IF varNb = 0 THEN
      FOR i IN 1..varAllCopyNbs.COUNT LOOP
        INSERT INTO MigratedSegment (castorFile, copyNb, VID)
        VALUES (cfId, varAllCopyNbs(i), varAllVIDs(i));
      END LOOP;
    END IF;
    -- Reset the CastorFile as not on tape: it may have already been there before this Repack
    -- request and with tapeStatus = ONTAPE, but now we want to have a migration.
    UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_NOTONTAPE
     WHERE id = cfId;
    -- all is fine, migration was triggered
    migrationTriggered := True;
  END;
END;
/

/* PL/SQL method implementing syncRunningTransfers
 * This is called by the transfer manager daemon on the restart of a disk server manager
 * in order to sync running transfers in the database with the reality of the machine.
 * This is particularly useful to terminate cleanly transfers interupted by a power cut
 */
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
  FORALL i IN 1..transfers.COUNT
    INSERT INTO SyncRunningTransfersHelper (subreqId) VALUES (transfers(i));
  -- Go through all running transfers from the DB point of view for the given diskserver
  FOR SR IN (SELECT /*+ LEADING(SubRequest DiskCopy FileSystem DiskServer)
                        USE_NL(SubRequest DiskCopy FileSystem DiskServer) */
                    SubRequest.id, SubRequest.subreqId, SubRequest.castorfile, SubRequest.request
               FROM SubRequest PARTITION (P_STATUS_ACTIVE), DiskCopy, FileSystem, DiskServer
              WHERE SubRequest.status = dconst.SUBREQUEST_READY
                AND Subrequest.reqType IN (35, 37)  -- StageGet/PutRequest
                AND Subrequest.diskCopy = DiskCopy.id
                AND DiskCopy.fileSystem = FileSystem.id
                AND FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = machine
              UNION
             SELECT /*+ LEADING(DiskServer SubRequest) USE_NL(DiskServer SubRequest) */
                    SubRequest.id, SubRequest.subreqId, SubRequest.castorfile, SubRequest.request
               FROM SubRequest PARTITION (P_STATUS_ACTIVE), DiskServer
              WHERE SubRequest.status = dconst.SUBREQUEST_READY
                AND Subrequest.reqType IN (35, 37)  -- StageGet/PutRequest
                AND Subrequest.diskServer = DiskServer.id
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
           SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ reqId, id from StagePutRequest) Request
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


/* inserts ChangePrivilege Request in the stager DB */ 	 
CREATE OR REPLACE PROCEDURE insertChangePrivilegeRequest
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
   isGranted IN INTEGER,
   euids IN castor."cnumList",
   egids IN castor."cnumList",
   reqTypes IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  subobjId INTEGER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 152 THEN -- ChangePrivilege
      INSERT INTO ChangePrivilege (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, isGranted, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,isGranted,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertChangePrivilege : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on request types
  FOR i IN 1..reqTypes.COUNT LOOP
    -- get unique ids for the request type
    SELECT ids_seq.nextval INTO subobjId FROM DUAL;
    -- insert the request type
    INSERT INTO RequestType (reqType, id, request)
    VALUES (reqTypes(i), subobjId, reqId);
  END LOOP;
  -- Loop on BWUsers
  FOR i IN 1..euids.COUNT LOOP
    -- get unique ids for the request type
    SELECT ids_seq.nextval INTO subobjId FROM DUAL;
    -- insert the BWUser
    INSERT INTO BWUser (euid, egid, id, request)
    VALUES (euids(i), egids(i), subobjId, reqId);
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
  -- send an alert to accelerate the processing of the request
  alertSignalNoLock('wakeUpQueryReqSvc');
END;
/

/* PL/SQL procedure implementing triggerRepackRecall
 * this triggers a recall in the repack context
 */
CREATE OR REPLACE PROCEDURE triggerRepackRecall
(inCfId IN INTEGER, inFileId IN INTEGER, inNsHost IN VARCHAR2, inBlock IN RAW,
 inFseq IN INTEGER, inCopynb IN INTEGER, inEuid IN INTEGER, inEgid IN INTEGER,
 inRecallGroupId IN INTEGER, inSvcClassId IN INTEGER, inVid IN VARCHAR2, inFileSize IN INTEGER,
 inFileClass IN INTEGER, inAllValidSegments IN VARCHAR2, inReqUUID IN VARCHAR2,
 inSubReqUUID IN VARCHAR2, inRecallGroupName IN VARCHAR2) AS
  varLogParam VARCHAR2(2048);
  varAllValidCopyNbs "numList" := "numList"();
  varAllValidVIDs strListTable := strListTable();
  varAllValidSegments strListTable;
  varFileClassId INTEGER;
BEGIN
  -- create recallJob for the given VID, copyNb, etc.
  INSERT INTO RecallJob (id, castorFile, copyNb, recallGroup, svcClass, euid, egid,
                         vid, fseq, status, fileSize, creationTime, blockId, fileTransactionId)
  VALUES (ids_seq.nextval, inCfId, inCopynb, inRecallGroupId, inSvcClassId,
          inEuid, inEgid, inVid, inFseq, tconst.RECALLJOB_PENDING, inFileSize, getTime(),
          inBlock, NULL);
  -- log "created new RecallJob"
  varLogParam := 'SUBREQID=' || inSubReqUUID || ' RecallGroup=' || inRecallGroupName;
  logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.RECALL_CREATING_RECALLJOB, inFileId, inNsHost, 'stagerd',
           varLogParam || ' fileClass=' || TO_CHAR(inFileClass) || ' copyNb=' || TO_CHAR(inCopynb)
           || ' TPVID=' || inVid || ' fseq=' || TO_CHAR(inFseq) || ' FileSize=' || TO_CHAR(inFileSize));
  -- create missing segments if needed
  SELECT * BULK COLLECT INTO varAllValidSegments
    FROM TABLE(strTokenizer(inAllValidSegments));
  FOR i IN 1 .. varAllValidSegments.COUNT/2 LOOP
    varAllValidCopyNbs.EXTEND;
    varAllValidCopyNbs(i) := TO_NUMBER(varAllValidSegments(2*i-1));
    varAllValidVIDs.EXTEND;
    varAllValidVIDs(i) := varAllValidSegments(2*i);
  END LOOP;
  SELECT id INTO varFileClassId
    FROM FileClass WHERE classId = inFileClass;
  -- Note that the number given here for the number of existing segments is the total number of
  -- segment, without removing the ones on  EXPORTED tapes. This means that repack will not
  -- recreate new segments for files that have some copies on EXPORTED tapes.
  -- This is different from standard recalls that would recreate segments on EXPORTED tapes.
  createMJForMissingSegments(inCfId, inFileSize, varFileClassId, varAllValidCopyNbs,
                             varAllValidVIDs, varAllValidCopyNbs.COUNT, inFileId, inNsHost, varLogParam);
END;
/

/* revalidate all code */
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_15_3';
COMMIT;
