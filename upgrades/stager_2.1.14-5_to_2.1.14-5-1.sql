/******************************************************************************
 *                 stager_2.1.14-5_to_2.1.14-5-1.sql
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
 * This script upgrades a CASTOR v2.1.14-5 STAGER database to v2.1.14-5-1
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
     AND release = '2_1_14_5_1'
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
     AND release = '2_1_14_5';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_5_1', 'TRANSPARENT');
COMMIT;


-- #103363: Creation of tape mounts for migration is not resilient to hardware unavailabilities

/* insert new Migration Mount */
CREATE OR REPLACE PROCEDURE insertMigrationMount(inTapePoolId IN NUMBER,
                                                 minimumAge IN INTEGER,
                                                 outMountId OUT INTEGER) AS
  varMigJobId INTEGER;
BEGIN
  -- Check that the mount would be honoured by running a dry-run file selection:
  -- note that in case the mount was triggered because of age, we check that
  -- we have a valid candidate that is at least minimumAge seconds old.
  -- This is almost a duplicate of the query in tg_getFilesToMigrate.
  SELECT /*+ FIRST_ROWS_1
             LEADING(MigrationJob CastorFile DiskCopy FileSystem DiskServer)
             USE_NL(MMigrationJob CastorFile DiskCopy FileSystem DiskServer)
             INDEX(CastorFile PK_CastorFile_Id)
             INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile)
             INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusId) */
         MigrationJob.id mjId INTO varMigJobId
    FROM MigrationJob, DiskCopy, FileSystem, DiskServer, CastorFile
   WHERE MigrationJob.tapePool = inTapePoolId
     AND MigrationJob.status = tconst.MIGRATIONJOB_PENDING
     AND (minimumAge = 0 OR MigrationJob.creationTime < getTime() - minimumAge)
     AND CastorFile.id = MigrationJob.castorFile
     AND CastorFile.id = DiskCopy.castorFile
     AND CastorFile.tapeStatus = dconst.CASTORFILE_NOTONTAPE
     AND DiskCopy.status = dconst.DISKCOPY_VALID
     AND FileSystem.id = DiskCopy.fileSystem
     AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
     AND DiskServer.id = FileSystem.diskServer
     AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
     AND DiskServer.hwOnline = 1
     AND ROWNUM < 2;
  -- The select worked out, create a mount for this tape pool
  INSERT INTO MigrationMount
              (mountTransactionId, id, startTime, VID, label, density,
               lastFseq, lastVDQMPingTime, tapePool, status)
    VALUES (NULL, ids_seq.nextval, gettime(), NULL, NULL, NULL,
            NULL, 0, inTapePoolId, tconst.MIGRATIONMOUNT_WAITTAPE)
    RETURNING id INTO outMountId;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- No valid candidate found: this could happen e.g. when candidates exist
  -- but reside on non-available hardware. In this case we drop the mount and log
  outMountId := 0;
END;
/


/* DB job to start new migration mounts */
CREATE OR REPLACE PROCEDURE startMigrationMounts AS
  varNbPreExistingMounts INTEGER;
  varTotalNbMounts INTEGER := 0;
  varDataAmount INTEGER;
  varNbFiles INTEGER;
  varOldestCreationTime NUMBER;
  varMountId INTEGER;
BEGIN
  -- loop through tapepools
  FOR t IN (SELECT id, name, nbDrives, minAmountDataForMount,
                   minNbFilesForMount, maxFileAgeBeforeMount
              FROM TapePool) LOOP
    -- get number of mounts already running for this tapepool
    SELECT nvl(count(*), 0) INTO varNbPreExistingMounts
      FROM MigrationMount
     WHERE tapePool = t.id;
    varTotalNbMounts := varNbPreExistingMounts;
    -- get the amount of data and number of files to migrate, plus the age of the oldest file
    SELECT nvl(SUM(fileSize), 0), COUNT(*), nvl(MIN(creationTime), 0)
      INTO varDataAmount, varNbFiles, varOldestCreationTime
      FROM MigrationJob
     WHERE tapePool = t.id
       AND status = tconst.MIGRATIONJOB_PENDING;
    -- Create as many mounts as needed according to amount of data and number of files
    WHILE (varTotalNbMounts < t.nbDrives) AND
          ((varDataAmount/(varTotalNbMounts+1) >= t.minAmountDataForMount) OR
           (varNbFiles/(varTotalNbMounts+1) >= t.minNbFilesForMount)) AND
          (varTotalNbMounts+1 <= varNbFiles) LOOP   -- in case minAmountDataForMount << avgFileSize, stop creating more than one mount per file
      insertMigrationMount(t.id, 0, varMountId);
      varTotalNbMounts := varTotalNbMounts + 1;
      IF varMountId = 0 THEN
        -- log "startMigrationMounts: failed migration mount creation due to lack of files"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NO_FILE, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
        -- no need to continue as we could not find a single file to migrate
        EXIT;
      ELSE
        -- log "startMigrationMounts: created new migration mount"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NEW_MOUNT, 0, '', 'tapegatewayd',
                 'mountId=' || TO_CHAR(varMountId) ||
                 ' tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
      END IF;
    END LOOP;
    -- force creation of a unique mount in case no mount was created at all and some files are too old
    IF varNbFiles > 0 AND varTotalNbMounts = 0 AND t.nbDrives > 0 AND
       gettime() - varOldestCreationTime > t.maxFileAgeBeforeMount THEN
      insertMigrationMount(t.id, t.maxFileAgeBeforeMount, varMountId);
      IF varMountId = 0 THEN
        -- log "startMigrationMounts: failed migration mount creation due to lack of files"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_AGE_NO_FILE, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
      ELSE
        -- log "startMigrationMounts: created new migration mount based on age"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.MIGMOUNT_NEW_MOUNT_AGE, 0, '', 'tapegatewayd',
                 'mountId=' || TO_CHAR(varMountId) ||
                 ' tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                 ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
      END IF;
    ELSE
      IF varTotalNbMounts = varNbPreExistingMounts THEN 
        -- log "startMigrationMounts: no need for new migration mount"
        logToDLF(NULL, dlf.LVL_DEBUG, dlf.MIGMOUNT_NOACTION, 0, '', 'tapegatewayd',
                 'tapePool=' || t.name ||
                 ' nbPreExistingMounts=' || TO_CHAR(varNbPreExistingMounts) ||
                 ' nbMounts=' || TO_CHAR(varTotalNbMounts) ||
                 ' dataAmountInQueue=' || TO_CHAR(nvl(varDataAmount,0)) ||
                 ' nbFilesInQueue=' || TO_CHAR(nvl(varNbFiles,0)) ||
                 ' oldestCreationTime=' || TO_CHAR(TRUNC(nvl(varOldestCreationTime,0))));
      END IF;
    END IF;
    COMMIT;
  END LOOP;
END;
/


-- #103370: The logic to resume recall jobs after an unmount is broken when dealing with double copy recalls
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
       SET status = tconst.RECALLJOB_PENDING,
           fileTransactionId = NULL
     WHERE castorFile IN (SELECT castorFile
                            FROM RecallJob
                           WHERE VID = varVID
                             AND (fileTransactionId IS NOT NULL OR status = tconst.RECALLJOB_RETRYMOUNT));
    DELETE FROM RecallMount WHERE vid = varVID;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Small infusion of paranoia ;-) We should never reach that point...
    ROLLBACK;
    RAISE_APPLICATION_ERROR (-20119, 'endTapeSession: no recall or migration mount found');
  END;
END;
/

/* Attempt to retry a recall. Fail it in case it should not be retried anymore */
CREATE OR REPLACE PROCEDURE retryOrFailRecall(inCfId IN NUMBER, inVID IN VARCHAR2,
                                              inReqId IN VARCHAR2, inLogContext IN VARCHAR2) AS
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
  varRecallStillAlive INTEGER;
BEGIN
  -- lock castorFile
  SELECT fileId, nsHost INTO varFileId, varNsHost
    FROM CastorFile WHERE id = inCfId FOR UPDATE;
  -- increase retry counters within mount and set recallJob status to NEW
  UPDATE RecallJob
     SET nbRetriesWithinMount = nbRetriesWithinMount + 1,
         status = tconst.RECALLJOB_PENDING,
         fileTransactionId = NULL
   WHERE castorFile = inCfId
     AND VID = inVID;
  -- detect the RecallJobs with too many retries within this mount
  -- mark them for a retry on next mount
  UPDATE RecallJob
     SET nbRetriesWithinMount = 0,
         nbMounts = nbMounts + 1,
         status = tconst.RECALLJOB_RETRYMOUNT
   WHERE castorFile = inCfId
     AND VID = inVID
     AND nbRetriesWithinMount >= TO_NUMBER(getConfigOption('Recall', 'MaxNbRetriesWithinMount', 2));
  -- stop here if no recallJob was concerned
  IF SQL%ROWCOUNT = 0 THEN RETURN; END IF;
  -- detect RecallJobs with too many mounts
  DELETE RecallJob
   WHERE castorFile = inCfId
     AND VID = inVID
     AND nbMounts >= TO_NUMBER(getConfigOption('Recall', 'MaxNbMounts', 3));
  -- check whether other RecallJobs are still around for this file (other copies on tape)
  SELECT /*+ INDEX_RS_ASC(RecallJob I_RecallJob_CastorFile_VID) */
         count(*) INTO varRecallStillAlive
    FROM RecallJob
   WHERE castorFile = inCfId
     AND ROWNUM < 2;
  -- if no remaining recallJobs, the subrequests are failed
  IF varRecallStillAlive = 0 THEN
    UPDATE /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile) */ SubRequest 
       SET status = dconst.SUBREQUEST_FAILED,
           lastModificationTime = getTime(),
           errorCode = serrno.SEINTERNAL,
           errorMessage = 'File recall from tape has failed, please try again later'
     WHERE castorFile = inCfId 
       AND status = dconst.SUBREQUEST_WAITTAPERECALL;
     -- log 'File recall has permanently failed'
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_PERMANENTLY_FAILED, varFileId, varNsHost,
      'tapegatewayd', ' TPVID=' || inVID ||' '|| inLogContext);
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE resetOverwrittenCastorFile(inCfId INTEGER,
                                                       inNewOpenTime NUMBER,
                                                       inNewSize INTEGER) AS
BEGIN
  -- update the Castorfile
  UPDATE CastorFile
     SET nsOpenTime = inNewOpenTime,
         fileSize = inNewSize,
         lastAccessTime = getTime()
   WHERE id = inCfId;
  -- cancel ongoing recalls, if any
  deleteRecallJobs(inCfId);
  -- cancel ongoing migrations, if any
  deleteMigrationJobs(inCfId);
  -- invalidate existing DiskCopies, if any
  UPDATE DiskCopy
     SET status = dconst.DISKCOPY_INVALID,
         gcType = dconst.GCTYPE_OVERWRITTEN
   WHERE castorFile = inCfId
     AND status = dconst.DISKCOPY_VALID;
  -- restart ongoing requests
  -- Note that we reset the "answered" flag of the subrequest. This will potentially lead to
  -- a wrong attempt to answer again the client (but won't harm as the client is gone in that case)
  -- but is needed as the current implementation of the stager also uses this flag to know
  -- whether to archive the subrequest. If we leave it to 1, the subrequests are wrongly
  -- archived when retried, leading e.g. to failing recalls
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_RESTART, answered = 0
   WHERE castorFile = inCfId
     AND status = dconst.SUBREQUEST_WAITTAPERECALL;
END;
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
  varOpenMode CHAR(1);
BEGIN
  -- retrieve data from the namespace: note that if stagerTime is (still) NULL,
  -- we're still in compatibility mode and we resolve to using mtime.
  -- To be dropped in 2.1.15 where stagerTime is NOT NULL by design.
  -- Note the truncation of stagerTime to 5 digits. This is needed for consistency with
  -- the stager code that uses the OCCI api and thus loses precision when recuperating
  -- 64 bits integers into doubles (lack of support for 64 bits numbers in OCCI)
  SELECT NVL(TRUNC(stagertime,5), mtime), csumtype, csumvalue, filesize
    INTO varNSOpenTime, varNSCsumtype, varNSCsumvalue, varNSSize
    FROM Cns_File_Metadata@RemoteNS
   WHERE fileid = inFileId;
  -- check open mode: in compatibility mode we still have only seconds precision,
  -- hence the NS open time has to be truncated prior to comparing it with our time.
  varOpenMode := getConfigOption@RemoteNS('stager', 'openmode', NULL);
  IF varOpenMode = 'C' THEN
    varNSOpenTime := TRUNC(varNSOpenTime);
  END IF;
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

  -- is the checksum set in the namespace ?
  IF varNSCsumtype IS NULL THEN
    -- no -> let's set it (note that the function called commits in the remote DB)
    setSegChecksumWhenNull@remoteNS(inFileId, inCopyNb, inCksumName, inCksumValue);
    -- log 'checkRecallInNS : created missing checksum in the namespace'
    logToDLF(inReqId, dlf.LVL_SYSTEM, dlf.RECALL_CREATED_CHECKSUM, inFileId, inNsHost, 'nsd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' copyNb=' || TO_CHAR(inCopyNb) ||
             ' TPVID=' || inVID || ' fseq=' || TO_CHAR(inFseq) || ' checksumType='  || inCksumName ||
             ' checksumValue=' || TO_CHAR(inCksumValue));
  ELSE
    -- is the checksum matching ?
    -- note that this is probably useless as it was already checked at transfer time
    IF inCksumName = varNSCsumtype AND TO_CHAR(inCksumValue, 'XXXXXXXX') != varNSCsumvalue THEN
      -- not matching ! log "checkRecallInNS : bad checksum detected, will retry if allowed"
      logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_BAD_CHECKSUM, inFileId, inNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || inVID ||
               ' fseq=' || TO_CHAR(inFseq) || ' copyNb=' || TO_CHAR(inCopyNb) || ' checksumType=' || inCksumName ||
               ' expectedChecksumValue=' || varNSCsumvalue ||
               ' checksumValue=' || TO_CHAR(inCksumValue, 'XXXXXXXX') ||' '|| inLogContext);
      retryOrFailRecall(inCfId, inVID, inReqId, inLogContext);
      RETURN FALSE;
    END IF;
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

/* PL/SQL method implementing bestFileSystemForRecall */
CREATE OR REPLACE PROCEDURE bestFileSystemForRecall(inCfId IN INTEGER, outFilePath OUT VARCHAR2) AS
  varCfId INTEGER;
  varFileSystemId NUMBER := 0;
  nb NUMBER;
BEGIN
  -- try and select a good FileSystem for this recall
  FOR f IN (SELECT /*+ INDEX_RS_ASC(RecallJob I_RecallJob_Castorfile_VID) */
                   DiskServer.name ||':'|| FileSystem.mountPoint AS remotePath, FileSystem.id,
                   FileSystem.diskserver, CastorFile.fileSize, CastorFile.fileId, CastorFile.nsHost
              FROM DiskServer, FileSystem, DiskPool2SvcClass, CastorFile, RecallJob
             WHERE CastorFile.id = inCfId
               AND RecallJob.castorFile = inCfId
               AND RecallJob.svcclass = DiskPool2SvcClass.child
               AND FileSystem.diskpool = DiskPool2SvcClass.parent
               -- a priori, we want to have enough free space. However, if we don't, we accept to start writing
               -- if we have a minimum of 30GB free and count on gerbage collection to liberate space while writing
               -- We still check that the file fit on the disk, and actually keep a 30% margin so that very recent
               -- files can be kept
               AND (FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > CastorFile.fileSize
                 OR (FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > 30000000000
                 AND FileSystem.totalSize * 0.7 > CastorFile.fileSize))
               AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
               AND DiskServer.id = FileSystem.diskServer
               AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
               AND DiskServer.hwOnline = 1
          ORDER BY -- use randomness to scatter recalls everywhere in the pool. This works unless the pool starts to be overloaded:
                   -- once a hot spot develops, recalls start to take longer and longer and thus tend to accumulate. However,
                   -- until we have a faster feedback system to rank filesystems, the fileSystemRate order has not proven to be better.
                   DBMS_Random.value)
  LOOP
    varFileSystemId := f.id;
    buildPathFromFileId(f.fileId, f.nsHost, ids_seq.nextval, outFilePath);
    outFilePath := f.remotePath || outFilePath;
    -- Check that we don't already have a copy of this file on this filesystem.
    -- This will never happen in normal operations but may be the case if a filesystem
    -- was disabled and did come back while the tape recall was waiting.
    -- Even if we optimize by cancelling remaining unneeded tape recalls when a
    -- fileSystem comes back, the ones running at the time of the come back will have
    -- the problem.
    SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */ count(*) INTO nb
      FROM DiskCopy
     WHERE fileSystem = f.id
       AND castorfile = inCfid
       AND status = dconst.DISKCOPY_VALID;
    IF nb != 0 THEN
      raise_application_error(-20115, 'Recaller could not find a FileSystem in production in the requested SvcClass and without copies of this file');
    END IF;
    RETURN;
  END LOOP;
  IF varFileSystemId = 0 THEN
    raise_application_error(-20115, 'No suitable filesystem found for this recalled file');
  END IF;
END;
/

-- #103387: Incorrect clean up of Disk2diskCopyJobs when they are cancelled

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
      FROM Disk2DiskCopyJob
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
    varDestDcId := ids_seq.nextval;
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
  logToDLF(NULL, dlf.LVL_SYSTEM, varLogMsg, varFileId, varNsHost, 'transfermanagerd', varComment);
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


/* Recompile all procedures, triggers and functions */
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_14_5_1';
COMMIT;
