/******************************************************************************
 *                 castor/db/oracleConstants.sql
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
 * @author castor-dev@cern.ch
 *****************************************************************************/

/**
 * Package containing the definition of all tape-archive related PL/SQL
 * constants.
 */
CREATE OR REPLACE PACKAGE tconst
AS
  -- TPMODE
  WRITE_DISABLE CONSTANT PLS_INTEGER :=  0;
  WRITE_ENABLE  CONSTANT PLS_INTEGER :=  1;

  RECALLMOUNT_NEW        CONSTANT PLS_INTEGER := 0;
  RECALLMOUNT_WAITDRIVE  CONSTANT PLS_INTEGER := 1;
  RECALLMOUNT_RECALLING  CONSTANT PLS_INTEGER := 2;

  RECALLJOB_PENDING      CONSTANT PLS_INTEGER := 1;
  RECALLJOB_SELECTED     CONSTANT PLS_INTEGER := 2;
  RECALLJOB_RETRYMOUNT   CONSTANT PLS_INTEGER := 3;

  MIGRATIONMOUNT_WAITTAPE  CONSTANT PLS_INTEGER := 0;
  MIGRATIONMOUNT_SEND_TO_VDQM CONSTANT PLS_INTEGER := 1;
  MIGRATIONMOUNT_WAITDRIVE CONSTANT PLS_INTEGER := 2;
  MIGRATIONMOUNT_MIGRATING CONSTANT PLS_INTEGER := 3;

  MIGRATIONJOB_PENDING   CONSTANT PLS_INTEGER := 0;
  MIGRATIONJOB_SELECTED  CONSTANT PLS_INTEGER := 1;
  MIGRATIONJOB_WAITINGONRECALL CONSTANT PLS_INTEGER := 3;

  REPACK_SUBMITTED       CONSTANT PLS_INTEGER := 6;
  REPACK_STARTING        CONSTANT PLS_INTEGER := 0;
  REPACK_ONGOING         CONSTANT PLS_INTEGER := 1;
  REPACK_FINISHED        CONSTANT PLS_INTEGER := 2;
  REPACK_FAILED          CONSTANT PLS_INTEGER := 3;
  REPACK_ABORTING        CONSTANT PLS_INTEGER := 4;
  REPACK_ABORTED         CONSTANT PLS_INTEGER := 5;

  TAPE_DISABLED          CONSTANT PLS_INTEGER := 1;
  TAPE_EXPORTED          CONSTANT PLS_INTEGER := 2;
  TAPE_BUSY              CONSTANT PLS_INTEGER := 4;
  TAPE_FULL              CONSTANT PLS_INTEGER := 8;
  TAPE_RDONLY            CONSTANT PLS_INTEGER := 16;
  TAPE_ARCHIVED          CONSTANT PLS_INTEGER := 32;
END tconst;
/

CREATE OR REPLACE FUNCTION tapeStatusToString(status IN NUMBER) RETURN VARCHAR2 AS
  res VARCHAR2(2048);
  rebuildValue NUMBER := 0;
BEGIN
  IF status = 0 THEN RETURN 'OK'; END IF;
  IF BITAND(status, tconst.TAPE_DISABLED) != 0 THEN
    res := res || '|DISABLED';
    rebuildValue := rebuildValue + tconst.TAPE_DISABLED;
  END IF;
  IF BITAND(status, tconst.TAPE_EXPORTED) != 0  THEN
    res := res || '|EXPORTED';
    rebuildValue := rebuildValue + tconst.TAPE_EXPORTED;
  END IF;
  IF BITAND(status, tconst.TAPE_BUSY) != 0  THEN
    res := res || '|BUSY';
    rebuildValue := rebuildValue + tconst.TAPE_BUSY;
  END IF;
  IF BITAND(status, tconst.TAPE_FULL) != 0  THEN
    res := res || '|FULL';
    rebuildValue := rebuildValue + tconst.TAPE_FULL;
  END IF;
  IF BITAND(status, tconst.TAPE_RDONLY) != 0  THEN
    res := res || '|RDONLY';
    rebuildValue := rebuildValue + tconst.TAPE_RDONLY;
  END IF;
  IF BITAND(status, tconst.TAPE_ARCHIVED) != 0  THEN
    res := res || '|ARCHIVED';
    rebuildValue := rebuildValue + tconst.TAPE_ARCHIVED;
  END IF;
  IF res IS NULL THEN
    res := 'UNKNOWN:' || TO_CHAR(status);
  ELSE
    res := SUBSTR(res, 2);
    IF rebuildValue != status THEN
      res := res || '|UNKNOWN:' || TO_CHAR(status-rebuildValue);
    END IF;
  END IF;
  RETURN res;
END;
/

/**
 * Package containing the definition of all disk related PL/SQL constants.
 */
CREATE OR REPLACE PACKAGE dconst
AS

  CASTORFILE_NOTONTAPE       CONSTANT PLS_INTEGER :=  0;
  CASTORFILE_ONTAPE          CONSTANT PLS_INTEGER :=  1;
  CASTORFILE_DISKONLY        CONSTANT PLS_INTEGER :=  2;

  DISKCOPY_VALID             CONSTANT PLS_INTEGER :=  0;
  DISKCOPY_FAILED            CONSTANT PLS_INTEGER :=  4;
  DISKCOPY_WAITFS            CONSTANT PLS_INTEGER :=  5;
  DISKCOPY_STAGEOUT          CONSTANT PLS_INTEGER :=  6;
  DISKCOPY_INVALID           CONSTANT PLS_INTEGER :=  7;
  DISKCOPY_BEINGDELETED      CONSTANT PLS_INTEGER :=  9;
  DISKCOPY_WAITFS_SCHEDULING CONSTANT PLS_INTEGER := 11;

  DISKSERVER_PRODUCTION CONSTANT PLS_INTEGER := 0;
  DISKSERVER_DRAINING   CONSTANT PLS_INTEGER := 1;
  DISKSERVER_DISABLED   CONSTANT PLS_INTEGER := 2;
  DISKSERVER_READONLY   CONSTANT PLS_INTEGER := 3;

  FILESYSTEM_PRODUCTION CONSTANT PLS_INTEGER := 0;
  FILESYSTEM_DRAINING   CONSTANT PLS_INTEGER := 1;
  FILESYSTEM_DISABLED   CONSTANT PLS_INTEGER := 2;
  FILESYSTEM_READONLY   CONSTANT PLS_INTEGER := 3;
  
  DRAININGJOB_SUBMITTED    CONSTANT PLS_INTEGER := 0;
  DRAININGJOB_STARTING     CONSTANT PLS_INTEGER := 1;
  DRAININGJOB_RUNNING      CONSTANT PLS_INTEGER := 2;
  DRAININGJOB_FAILED       CONSTANT PLS_INTEGER := 4;
  DRAININGJOB_FINISHED    CONSTANT PLS_INTEGER := 5;

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

  DISKPOOLSPACETYPE_FREE     CONSTANT PLS_INTEGER :=  0;
  DISKPOOLSPACETYPE_CAPACITY CONSTANT PLS_INTEGER :=  1;

  GCTYPE_AUTO                CONSTANT PLS_INTEGER :=  0;
  GCTYPE_USER                CONSTANT PLS_INTEGER :=  1;
  GCTYPE_TOOMANYREPLICAS     CONSTANT PLS_INTEGER :=  2;
  GCTYPE_DRAINING            CONSTANT PLS_INTEGER :=  3;
  GCTYPE_NSSYNCH             CONSTANT PLS_INTEGER :=  4;
  GCTYPE_OVERWRITTEN         CONSTANT PLS_INTEGER :=  5;
  GCTYPE_ADMIN               CONSTANT PLS_INTEGER :=  6;
  GCTYPE_FAILEDD2D           CONSTANT PLS_INTEGER :=  7;
  
  DELDC_ENOENT               CONSTANT PLS_INTEGER :=  1;
  DELDC_RECALL               CONSTANT PLS_INTEGER :=  2;
  DELDC_REPLICATION          CONSTANT PLS_INTEGER :=  3;
  DELDC_LOST                 CONSTANT PLS_INTEGER :=  4;
  DELDC_GC                   CONSTANT PLS_INTEGER :=  5;
  DELDC_NOOP                 CONSTANT PLS_INTEGER :=  6;

  DISK2DISKCOPYJOB_PENDING   CONSTANT PLS_INTEGER :=  0;
  DISK2DISKCOPYJOB_SCHEDULED CONSTANT PLS_INTEGER :=  1;
  DISK2DISKCOPYJOB_RUNNING   CONSTANT PLS_INTEGER :=  2;

  REPLICATIONTYPE_USER       CONSTANT PLS_INTEGER :=  0;
  REPLICATIONTYPE_INTERNAL   CONSTANT PLS_INTEGER :=  1;
  REPLICATIONTYPE_DRAINING   CONSTANT PLS_INTEGER :=  2;
  REPLICATIONTYPE_REBALANCE  CONSTANT PLS_INTEGER :=  3;

END dconst;
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

/**
 * Package containing the definition of some relevant (s)errno values and messages.
 */
CREATE OR REPLACE PACKAGE serrno AS
  /* (s)errno values */
  ENOENT          CONSTANT PLS_INTEGER := 2;    /* No such file or directory */
  EINTR           CONSTANT PLS_INTEGER := 4;    /* Interrupted system call */
  EACCES          CONSTANT PLS_INTEGER := 13;   /* Permission denied */
  EBUSY           CONSTANT PLS_INTEGER := 16;   /* Device or resource busy */
  EEXIST          CONSTANT PLS_INTEGER := 17;   /* File exists */
  EISDIR          CONSTANT PLS_INTEGER := 21;   /* Is a directory */
  EINVAL          CONSTANT PLS_INTEGER := 22;   /* Invalid argument */
  ENOSPC          CONSTANT PLS_INTEGER := 28;   /* No space left on device */

  SEINTERNAL      CONSTANT PLS_INTEGER := 1015; /* Internal error */
  SECHECKSUM      CONSTANT PLS_INTEGER := 1037; /* Bad checksum */
  ENSFILECHG      CONSTANT PLS_INTEGER := 1402; /* File has been overwritten, request ignored */
  ENSNOSEG        CONSTANT PLS_INTEGER := 1403; /* Segment had been deleted */
  ENSTOOMANYSEGS  CONSTANT PLS_INTEGER := 1406; /* Too many copies on tape */
  ENSOVERWHENREP  CONSTANT PLS_INTEGER := 1407; /* Cannot overwrite valid segment when replacing */
  ERTWRONGSIZE    CONSTANT PLS_INTEGER := 1613; /* (Recalled) file size incorrect */
  ESTKILLED       CONSTANT PLS_INTEGER := 1713; /* aborted by kill */
  ESTNOTAVAIL     CONSTANT PLS_INTEGER := 1718; /* File is currently not available */
  ESTNOSEGFOUND   CONSTANT PLS_INTEGER := 1723; /* File has no copy on tape or no diskcopies are accessible */
  ESTNOTAPEROUTE  CONSTANT PLS_INTEGER := 1727; /* File recreation canceled since the file cannot be routed to tape */
  
  /* messages */
  ENOENT_MSG          CONSTANT VARCHAR2(2048) := 'No such file or directory';
  EINTR_MSG           CONSTANT VARCHAR2(2048) := 'Interrupted system call';
  EACCES_MSG          CONSTANT VARCHAR2(2048) := 'Permission denied';
  EBUSY_MSG           CONSTANT VARCHAR2(2048) := 'Device or resource busy';
  EEXIST_MSG          CONSTANT VARCHAR2(2048) := 'File exists';
  EISDIR_MSG          CONSTANT VARCHAR2(2048) := 'Is a directory';
  EINVAL_MSG          CONSTANT VARCHAR2(2048) := 'Invalid argument';
  
  SEINTERNAL_MSG      CONSTANT VARCHAR2(2048) := 'Internal error';
  SECHECKSUM_MSG      CONSTANT VARCHAR2(2048) := 'Checksum mismatch between segment and file';
  ENSFILECHG_MSG      CONSTANT VARCHAR2(2048) := 'File has been overwritten, request ignored';
  ENSNOSEG_MSG        CONSTANT VARCHAR2(2048) := 'Segment had been deleted';
  ENSTOOMANYSEGS_MSG  CONSTANT VARCHAR2(2048) := 'Too many copies on tape';
  ENSOVERWHENREP_MSG  CONSTANT VARCHAR2(2048) := 'Cannot overwrite valid segment when replacing';
  ERTWRONGSIZE_MSG    CONSTANT VARCHAR2(2048) := 'Incorrect file size';
  ESTNOSEGFOUND_MSG   CONSTANT VARCHAR2(2048) := 'File has no copy on tape or no diskcopies are accessible';
  ESTNOTAPEROUTE_MSG  CONSTANT VARCHAR2(2048) := 'File recreation canceled since the file cannot be routed to tape';
END serrno;
/
