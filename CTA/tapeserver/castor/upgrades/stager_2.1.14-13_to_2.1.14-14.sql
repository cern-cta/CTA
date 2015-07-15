/******************************************************************************
 *                 stager_2.1.14-13_to_2.1.14-14.sql
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
 * This script upgrades a CASTOR v2.1.14-13 STAGER database to v2.1.14-14
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
     AND release = '2_1_14_14'
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
     AND release LIKE '2_1_14_13%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_14', 'TRANSPARENT');
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

/* Schema change */
/*****************/

-- make TooManyReplicasHelper a regular table
DROP TABLE TooManyReplicasHelper;
CREATE TABLE TooManyReplicasHelper (svcClass NUMBER, castorFile NUMBER);
ALTER TABLE TooManyReplicasHelper
  ADD CONSTRAINT UN_TooManyReplicasHelp_SVC_CF
  UNIQUE (svcClass, castorFile);

DROP TRIGGER tr_DiskCopy_Stmt_Online;

-- changes in DrainingErrors table
DROP INDEX I_DrainingErrors_DJ;
DROP INDEX I_DrainingErrors_DC;
ALTER TABLE DrainingErrors DROP (diskCopy);
ALTER TABLE DrainingErrors ADD (castorFile INTEGER CONSTRAINT NN_DrainingErrors_CF NOT NULL);
CREATE INDEX I_DrainingErrors_DJ_CF ON DrainingErrors (drainingJob, CastorFile);

ALTER TABLE DrainingErrors
  ADD CONSTRAINT FK_DrainingErrors_CastorFile
  FOREIGN KEY (castorFile)
  REFERENCES CastorFile (id);

DELETE FROM CastorConfig WHERE class='cleaning' and key='terminatedRequestsTimeout';
INSERT INTO CastorConfig
  VALUES ('cleaning', 'failedRequestsTimeout', '168', 'Maximum timeout before removing failed requests from the database in hours');

/* This procedure is used to check if the maxReplicaNb has been exceeded
 * for some CastorFiles. It checks all the files listed in TooManyReplicasHelper
 * This is called from a DB job and is fed by the tr_DiskCopy_Created trigger
 * on creation of new diskcopies
 */
CREATE OR REPLACE PROCEDURE checkNbReplicas AS
  varSvcClassId INTEGER;
  varCfId INTEGER;
  varMaxReplicaNb NUMBER;
  varNbFiles NUMBER;
  varDidSth BOOLEAN;
BEGIN
  -- Loop over the CastorFiles to be processed
  LOOP
    varCfId := NULL;
    DELETE FROM TooManyReplicasHelper
     WHERE ROWNUM < 2
    RETURNING svcClass, castorFile INTO varSvcClassId, varCfId;
    IF varCfId IS NULL THEN
      -- we can exit, we went though all files to be processed
      EXIT;
    END IF;
    -- Lock the castorfile
    SELECT id INTO varCfId FROM CastorFile
     WHERE id = varCfId FOR UPDATE;
    -- Get the max replica number of the service class
    SELECT maxReplicaNb INTO varMaxReplicaNb
      FROM SvcClass WHERE id = varSvcClassId;
    -- Produce a list of diskcopies to invalidate should too many replicas be online.
    varDidSth := False;
    FOR b IN (SELECT id FROM (
                SELECT rownum ind, id FROM (
                  SELECT /*+ INDEX_RS_ASC (DiskCopy I_DiskCopy_Castorfile) */ DiskCopy.id
                    FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass,
                         DiskServer
                   WHERE DiskCopy.filesystem = FileSystem.id
                     AND FileSystem.diskpool = DiskPool2SvcClass.parent
                     AND FileSystem.diskserver = DiskServer.id
                     AND DiskPool2SvcClass.child = SvcClass.id
                     AND DiskCopy.castorfile = varCfId
                     AND DiskCopy.status = dconst.DISKCOPY_VALID
                     AND SvcClass.id = varSvcClassId
                   -- Select non-PRODUCTION hardware first
                   ORDER BY decode(FileSystem.status, 0,
                            decode(DiskServer.status, 0, 0, 1), 1) ASC,
                            DiskCopy.gcWeight DESC))
               WHERE ind > varMaxReplicaNb)
    LOOP
      -- Sanity check, make sure that the last copy is never dropped!
      SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */ count(*) INTO varNbFiles
        FROM DiskCopy, FileSystem, DiskPool2SvcClass, SvcClass, DiskServer
       WHERE DiskCopy.filesystem = FileSystem.id
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND FileSystem.diskserver = DiskServer.id
         AND DiskPool2SvcClass.child = SvcClass.id
         AND DiskCopy.castorfile = varCfId
         AND DiskCopy.status = dconst.DISKCOPY_VALID
         AND SvcClass.id = varSvcClassId;
      IF varNbFiles = 1 THEN
        EXIT;  -- Last file, so exit the loop
      END IF;
      -- Invalidate the diskcopy
      UPDATE DiskCopy
         SET status = dconst.DISKCOPY_INVALID,
             gcType = dconst.GCTYPE_TOOMANYREPLICAS
       WHERE id = b.id;
      varDidSth := True;
      -- update importance of remaining diskcopies
      UPDATE DiskCopy SET importance = importance + 1
       WHERE castorFile = varCfId
         AND status = dconst.DISKCOPY_VALID;
    END LOOP;
    IF varDidSth THEN COMMIT; END IF;
  END LOOP;
  -- commit the deletions in case no modification was done that commited them before
  COMMIT;
END;
/

/* Trigger used to provide input to the statement level trigger
 * defined above
 */
CREATE OR REPLACE TRIGGER tr_DiskCopy_Created
AFTER INSERT ON DiskCopy
FOR EACH ROW
WHEN (new.status = 0) -- dconst.DISKCOPY_VALID
DECLARE
  svcId  NUMBER;
  unused NUMBER;
  -- Trap `ORA-00001: unique constraint violated` errors
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
BEGIN
  -- Insert the information about the diskcopy being processed into
  -- the TooManyReplicasHelper. This information will be used later
  -- by the checkNbReplicasJob job. We cannot do the work of that
  -- job here as it would result in `ORA-04091: table is mutating,
  -- trigger/function` errors
  BEGIN
    SELECT SvcClass.id INTO svcId
      FROM FileSystem, DiskPool2SvcClass, SvcClass
     WHERE FileSystem.diskpool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = SvcClass.id
       AND FileSystem.id = :new.filesystem;
  EXCEPTION WHEN TOO_MANY_ROWS THEN
    -- The filesystem belongs to multiple service classes which is not
    -- supported by the replica management trigger.
    RETURN;
  END;
  -- Insert an entry into the TooManyReplicasHelper table.
  BEGIN
    INSERT INTO TooManyReplicasHelper (svcClass, castorFile)
    VALUES (svcId, :new.castorfile);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    RETURN;  -- Entry already exists!
  END;
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
  varSrcDcId INTEGER;
  varDropSource INTEGER;
  varDestSvcClass INTEGER;
  varRepType INTEGER;
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
  BEGIN
    IF inDestPath IS NOT NULL THEN
      -- Parse destination path
      parsePath(inDestDsName ||':'|| inDestPath, varDestFsId, varDestPath, varDestDcId, varFileId, varNsHost);
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
  varLogMsg := CASE WHEN inErrorMessage IS NULL THEN dlf.D2D_D2DDONE_OK ELSE dlf.D2D_D2DFAILED END;
  -- lock the castor file (and get logging info)
  SELECT fileid, nsHost, fileSize INTO varFileId, varNsHost, varFileSize
    FROM CastorFile
   WHERE id = varCfId
     FOR UPDATE;
  -- check the filesize
  IF inReplicaFileSize != varFileSize THEN
    -- replication went wrong !
    IF varLogMsg = dlf.D2D_D2DDONE_OK THEN
      varLogMsg := dlf.D2D_D2DDONE_BADSIZE;
      varNewDcStatus := dconst.DISKCOPY_INVALID;
    END IF;
  END IF;
  -- Log success or failure of the replication
  varComment := 'transferId="' || inTransferId ||
         '" destSvcClass=' || getSvcClassName(varDestSvcClass) ||
         ' dstDcId=' || TO_CHAR(varDestDcId) || ' destPath="' || inDestPath ||
         '" euid=' || TO_CHAR(varUid) || ' egid=' || TO_CHAR(varGid) ||
         ' fileSize=' || TO_CHAR(varFileSize);
  IF inErrorMessage IS NOT NULL THEN
    varComment := varComment || ' replicaFileSize=' || TO_CHAR(inReplicaFileSize) ||
                  ' errorMessage=' || inErrorMessage;
  END IF;
  logToDLF(NULL, dlf.LVL_SYSTEM, varLogMsg, varFileId, varNsHost, 'transfermanagerd', varComment);
  -- if success, create new DiskCopy, restart waiting requests, cleanup and handle replicate on close
  IF inErrorMessage IS NULL THEN
    -- In case of draining, update DrainingJob: this is done before the rest to respect the locking order
    IF varDrainingJob IS NOT NULL THEN
      updateDrainingJobOnD2dEnd(varDrainingJob, varFileSize, False);
    END IF;
    -- compute GcWeight and importance of the new copy
    IF varNewDcStatus = dconst.DISKCOPY_VALID THEN
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
    -- create the new DiskCopy
    INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                          owneruid, ownergid, id, gcType, fileSystem, castorFile, status, importance)
    VALUES (varDestPath, varDcGcWeight, getTime(), getTime(), varFileSize, 0, varUid, varGid, varDestDcId,
            CASE varNewDcStatus WHEN dconst.DISKCOPY_INVALID THEN dconst.GCTYPE_FAILEDD2D ELSE NULL END,
            varDestFsId, varCfId, varNewDcStatus, varDCImportance);
    -- Wake up waiting subrequests
    UPDATE SubRequest
       SET status = dconst.SUBREQUEST_RESTART,
           getNextStatus = CASE WHEN inErrorMessage IS NULL THEN dconst.GETNEXTSTATUS_FILESTAGED ELSE getNextStatus END,
           lastModificationTime = getTime()
           -- XXX due to bug #103715 requests for disk-to-disk copies actually stay in WAITTAPERECALL,
           -- XXX so we have to restart also those. This will go with the refactoring of the stager.
     WHERE status IN (dconst.SUBREQUEST_WAITSUBREQ, dconst.SUBREQUEST_WAITTAPERECALL)
       AND castorfile = varCfId;
    alertSignalNoLock('wakeUpJobReqSvc');
    -- delete the disk2diskCopyJob
    DELETE FROM Disk2DiskCopyJob WHERE transferId = inTransferId;
    -- In case of valid new copy
    IF varNewDcStatus = dconst.DISKCOPY_VALID THEN
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
      replicateOnClose(varCfId, varUid, varGid);
    END IF;
  ELSE
    -- failure
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
            VALUES (varDrainingJob, inErrorMessage, varFileId, varNsHost, varCfId, getTime());
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
                              'retries. Last error was : ' || inErrorMessage
         WHERE status = dconst.SUBREQUEST_WAITSUBREQ
           AND castorfile = varCfId;
      END IF;
    END;
  END IF;
END;
/


/* PL/SQL method implementing disk2DiskCopyStart
 * Note that cfId is only needed for proper logging in case the replication has been canceled.
 */
CREATE OR REPLACE PROCEDURE disk2DiskCopyStart
 (inTransferId IN VARCHAR2, inFileId IN INTEGER, inNsHost IN VARCHAR2,
  inDestDiskServerName IN VARCHAR2, inDestMountPoint IN VARCHAR2,
  inSrcDiskServerName IN VARCHAR2, inSrcMountPoint IN VARCHAR2,
  outDestDcPath OUT VARCHAR2, outSrcDcPath OUT VARCHAR2) AS
  varCfId INTEGER;
  varDestDcId INTEGER;
  varDestDsId INTEGER;
  varSrcDcId INTEGER;
  varSrcFsId INTEGER;
  varNbCopies INTEGER;
  varSrcFsStatus INTEGER;
  varSrcDsStatus INTEGER;
  varSrcHwOnline INTEGER;
  varDestFsStatus INTEGER;
  varDestDsStatus INTEGER;
  varDestHwOnline INTEGER;
BEGIN
  -- check the Disk2DiskCopyJob status and check that it was not canceled in the mean time
  BEGIN
    SELECT castorFile, destDcId INTO varCfId, varDestDcId
      FROM Disk2DiskCopyJob
     WHERE transferId = inTransferId
       AND status = dconst.DISK2DISKCOPYJOB_SCHEDULED
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "disk2DiskCopyStart : Replication request canceled while queuing in scheduler or transfer already started"
    logToDLF(NULL, dlf.LVL_USER_ERROR, dlf.D2D_CANCELED_AT_START, inFileId, inNsHost, 'transfermanagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' destDiskServer=' || inDestDiskServerName ||
             ' destMountPoint=' || inDestMountPoint || ' srcDiskServer=' || inSrcDiskServerName ||
             ' srcMountPoint=' || inSrcMountPoint);
    -- raise exception
    raise_application_error(-20110, dlf.D2D_CANCELED_AT_START || '');
  END;

  -- identify the source DiskCopy and diskserver/filesystem and check that it is still valid
  BEGIN
    SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */
           FileSystem.id, DiskCopy.id, FileSystem.status, DiskServer.status, DiskServer.hwOnline
      INTO varSrcFsId, varSrcDcId, varSrcFsStatus, varSrcDsStatus, varSrcHwOnline
      FROM DiskServer, FileSystem, DiskCopy
     WHERE DiskServer.name = inSrcDiskServerName
       AND DiskServer.id = FileSystem.diskServer
       AND FileSystem.mountPoint = inSrcMountPoint
       AND DiskCopy.FileSystem = FileSystem.id
       AND DiskCopy.status = dconst.DISKCOPY_VALID
       AND DiskCopy.castorFile = varCfId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "disk2DiskCopyStart : Source has disappeared while queuing in scheduler, retrying"
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_SOURCE_GONE, inFileId, inNsHost, 'transfermanagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' destDiskServer=' || inDestDiskServerName ||
             ' destMountPoint=' || inDestMountPoint || ' srcDiskServer=' || inSrcDiskServerName ||
             ' srcMountPoint=' || inSrcMountPoint);
    -- end the disktodisk copy (may be retried)
    disk2DiskCopyEnded(inTransferId, '', '', 0, 0, dlf.D2D_SOURCE_GONE);
    COMMIT; -- commit or raise_application_error will roll back for us :-(
    -- raise exception for the scheduling part
    raise_application_error(-20110, dlf.D2D_SOURCE_GONE);
  END;

  -- update the Disk2DiskCopyJob status and filesystem
  UPDATE Disk2DiskCopyJob
     SET status = dconst.DISK2DISKCOPYJOB_RUNNING
   WHERE transferId = inTransferId;

  IF (varSrcDsStatus = dconst.DISKSERVER_DISABLED OR varSrcFsStatus = dconst.FILESYSTEM_DISABLED
      OR varSrcHwOnline = 0) THEN
    -- log "disk2DiskCopyStart : Source diskserver/filesystem was DISABLED meanwhile"
    logToDLF(NULL, dlf.LVL_WARNING, dlf.D2D_SRC_DISABLED, inFileId, inNsHost, 'transfermanagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' diskServer=' || inSrcDiskServerName ||
             ' fileSystem=' || inSrcMountPoint);
    -- fail d2d transfer
    disk2DiskCopyEnded(inTransferId, '', '', 0, 0, 'Source was disabled');
    COMMIT; -- commit or raise_application_error will roll back for us :-(
    -- raise exception
    raise_application_error(-20110, dlf.D2D_SRC_DISABLED);
  END IF;

  -- get destination diskServer/filesystem and check its status
  SELECT DiskServer.id, DiskServer.status, FileSystem.status, DiskServer.hwOnline
    INTO varDestDsId, varDestDsStatus, varDestFsStatus, varDestHwOnline
    FROM DiskServer, FileSystem
   WHERE DiskServer.name = inDestDiskServerName
     AND FileSystem.mountPoint = inDestMountPoint
     AND FileSystem.diskServer = DiskServer.id;
  IF (varDestDsStatus != dconst.DISKSERVER_PRODUCTION OR varDestFsStatus != dconst.FILESYSTEM_PRODUCTION
      OR varDestHwOnline = 0) THEN
    -- log "disk2DiskCopyStart : Destination diskserver/filesystem not in PRODUCTION any longer"
    logToDLF(NULL, dlf.LVL_WARNING, dlf.D2D_DEST_NOT_PRODUCTION, inFileId, inNsHost, 'transfermanagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' diskServer=' || inDestDiskServerName);
    -- fail d2d transfer
    disk2DiskCopyEnded(inTransferId, '', '', 0, 0, 'Destination not in production');
    COMMIT; -- commit or raise_application_error will roll back for us :-(
    -- raise exception
    raise_application_error(-20110, dlf.D2D_DEST_NOT_PRODUCTION);
  END IF;

  -- Prevent multiple copies of the file to be created on the same diskserver
  SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_Castorfile) */ count(*) INTO varNbCopies
    FROM DiskCopy, FileSystem
   WHERE DiskCopy.filesystem = FileSystem.id
     AND FileSystem.diskserver = varDestDsId
     AND DiskCopy.castorfile = varCfId
     AND DiskCopy.status = dconst.DISKCOPY_VALID;
  IF varNbCopies > 0 THEN
    -- log "disk2DiskCopyStart : Multiple copies of this file already found on this diskserver"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.D2D_MULTIPLE_COPIES_ON_DS, inFileId, inNsHost, 'transfermanagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' diskServer=' || inDestDiskServerName);
    -- fail d2d transfer
    disk2DiskCopyEnded(inTransferId, '', '', 0, 0, 'Copy found on diskserver');
    COMMIT; -- commit or raise_application_error will roll back for us :-(
    -- raise exception
    raise_application_error(-20110, dlf.D2D_MULTIPLE_COPIES_ON_DS);
  END IF;

  -- build full path of destination copy
  buildPathFromFileId(inFileId, inNsHost, varDestDcId, outDestDcPath);
  outDestDcPath := inDestMountPoint || outDestDcPath;

  -- build full path of source copy
  buildPathFromFileId(inFileId, inNsHost, varSrcDcId, outSrcDcPath);
  outSrcDcPath := inSrcDiskServerName || ':' || inSrcMountPoint || outSrcDcPath;

  -- log "disk2DiskCopyStart called and returned successfully"
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_START_OK, inFileId, inNsHost, 'transfermanagerd',
           'TransferId=' || TO_CHAR(inTransferId) || ' srcPath=' || outSrcDcPath ||
           ' destPath=' || outDestDcPath);
END;
/

/* handle the creation of the Disk2DiskCopyJobs for the running drainingJobs */
CREATE OR REPLACE PROCEDURE drainRunner AS
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
      BEGIN
        -- lock the draining job first
        SELECT id INTO varUnused FROM DrainingJob WHERE id = dj.id FOR UPDATE;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- it was (already!) canceled, go to the next one
        CONTINUE;
      END;
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
                     AND NOT EXISTS (SELECT 1 FROM DrainingErrors WHERE castorFile = CastorFile.id AND drainingJob = dj.id)
                     -- don't recreate disk-to-disk copy jobs for the ones already done in previous rounds
                     AND NOT EXISTS (SELECT 1 FROM Disk2DiskCopyJob WHERE castorFile = CastorFile.id AND drainingJob = dj.id)
                   ORDER BY DiskCopy.importance DESC)
                 WHERE ROWNUM <= varMaxNbOfSchedD2dPerDrain-varNbRunningJobs) LOOP
        createDisk2DiskCopyJob(F.cfId, F.nsOpenTime, dj.svcClass, dj.euid, dj.egid,
                               dconst.REPLICATIONTYPE_DRAINING, F.dcId, TRUE, dj.id, FALSE);
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
  REPACK_JOB_ONGOING           CONSTANT VARCHAR2(2048) := 'repackManager: Repack processes still starting, no new ones will be started for this round';
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

/* fail recall of a given CastorFile for a non existing tape */
CREATE OR REPLACE PROCEDURE cancelRecallForCFAndVID(inCfId IN INTEGER,
                                                    inVID IN VARCHAR2,
                                                    inErrorCode IN INTEGER,
                                                    inErrorMsg IN VARCHAR2) AS
  PRAGMA AUTONOMOUS_TRANSACTION;
  varNbRecalls INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
BEGIN
  -- lock castorFile, skip if it's missing
  -- (it may have disappeared in the mean time as we held no lock)
  BEGIN
    SELECT fileid, nsHost INTO varFileId, varNsHost
      FROM CastorFile
     WHERE id = inCfId
       FOR UPDATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN;
  END;
  -- log "Canceling RecallJobs for given VID"
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_CANCEL_RECALLJOB_VID, varFileId, varNsHost, 'tapegatewayd',
           'errorCode=' || TO_CHAR(inErrorCode) ||
           ' errorMessage="' || inErrorMsg ||
           '" TPVID=' || inVID);
  -- remove recallJobs that need the non existing tape
  DELETE FROM RecallJob WHERE castorfile = inCfId AND VID=inVID;
  -- check if other recallJobs remain (typically dual copy tapes)
  SELECT /*+ INDEX_RS_ASC(RecallJob I_RecallJob_CastorFile_VID) */
         count(*) INTO varNbRecalls
    FROM RecallJob WHERE castorfile = inCfId;
  -- if no remaining recalls, fail requests and cleanup
  IF varNbRecalls = 0 THEN
    -- log "Failing Recall(s)"
    logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_FAILING, varFileId, varNsHost, 'tapegatewayd',
             'errorCode=' || TO_CHAR(inErrorCode) ||
             ' errorMessage="' || inErrorMsg ||
             '" TPVID=' || inVID);
    -- delete potential migration jobs waiting on recalls
    deleteMigrationJobsForRecall(inCfId);
    -- Fail the associated subrequest(s)
    UPDATE /*+ INDEX_RS_ASC(SR I_Subrequest_Castorfile)*/ SubRequest SR
       SET SR.status = dconst.SUBREQUEST_FAILED,
           SR.getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED, --  (not strictly correct but the request is over anyway)
           SR.lastModificationTime = getTime(),
           SR.errorCode = serrno.SEINTERNAL,
           SR.errorMessage = 'File recall from tape has failed (tape not available), please try again later'
     WHERE SR.castorFile = inCfId
       AND SR.status IN (dconst.SUBREQUEST_WAITTAPERECALL, dconst.SUBREQUEST_WAITSUBREQ);
  END IF;
  COMMIT;
END;
/

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
        PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
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

/* Search and delete old archived/failed subrequests and their requests */
CREATE OR REPLACE PROCEDURE deleteTerminatedRequests AS
  failuresTimeOut INTEGER;
  successesTimeOut INTEGER;
  rate INTEGER;
  srIds "numList";
  ct NUMBER;
BEGIN
  -- select requested timeout for failed requests from configuration table
  failuresTimeOut := 3600*TO_NUMBER(getConfigOption('cleaning', 'failedRequestsTimeout', '168'));  -- 1 week
  -- compute a rate-dependent timeout for the successful requests by looking at the
  -- last half-hour of activity: keep max 1M of them.
  SELECT 1800 * 1000000 / (count(*)+1) INTO successesTimeOut
    FROM SubRequest
   WHERE status = dconst.SUBREQUEST_ARCHIVED
     AND lastModificationTime > getTime() - 1800;
  IF successesTimeOut > failuresTimeOut THEN
    -- in case of light load, don't keep successful request for longer than failed ones
    successesTimeOut := failuresTimeOut;
  END IF;
  
  -- Delete castorFiles if nothing is left for them. Here we use
  -- a temporary table as we need to commit every ~1000 operations
  -- and keeping a cursor opened on the original select may take
  -- too long, leading to ORA-01555 'snapshot too old' errors.
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DeleteTermReqHelper';
  INSERT /*+ APPEND */ INTO DeleteTermReqHelper (srId, cfId)
    (SELECT SR.id, castorFile FROM SubRequest SR
      WHERE (SR.status = dconst.SUBREQUEST_ARCHIVED
             AND SR.lastModificationTime < getTime() - successesTimeOut)
         -- failed subrequests are kept according to the configured timeout
         OR (SR.status = dconst.SUBREQUEST_FAILED_FINISHED
             AND reqType != 119 AND SR.lastModificationTime < getTime() - failuresTimeOut));  -- StageRepackRequest
  COMMIT;  -- needed otherwise the next statement raises
           -- ORA-12838: cannot read/modify an object after modifying it in parallel
  -- 2nd part, separated from above for efficiency reasons
  INSERT /*+ APPEND */ INTO DeleteTermReqHelper (srId, cfId)
    (SELECT SR.id, castorFile FROM SubRequest SR, StageRepackRequest R
      WHERE SR.status = dconst.SUBREQUEST_FAILED_FINISHED
         -- only for the Repack case, we keep all failed subrequests around until
         -- the whole Repack request is over for more than <timeOut> seconds
        AND reqType = 119 AND R.lastModificationTime < getTime() - failuresTimeOut  -- StageRepackRequest
        AND R.id = SR.request);
  COMMIT;
  SELECT count(*) INTO ct FROM DeleteTermReqHelper;
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETING_REQUESTS, 0, '', 'stagerd',
    'SubRequestsCount=' || ct);
  ct := 0;
  FOR cf IN (SELECT UNIQUE cfId FROM DeleteTermReqHelper) LOOP
    deleteCastorFile(cf.cfId);
    ct := ct + 1;
    IF ct = 1000 THEN
      COMMIT;
      ct := 0;
    END IF;
  END LOOP;

  -- Now delete all old subRequests. We reuse here the
  -- temporary table, which serves as a snapshot of the
  -- entries to be deleted, and we use the FORALL logic
  -- (cf. bulkDelete) instead of a simple DELETE ...
  -- WHERE id IN (SELECT srId FROM DeleteTermReqHelper)
  -- for efficiency reasons. Moreover, we don't risk
  -- here the ORA-01555 error keeping the cursor open
  -- between commits as we are selecting on our
  -- temporary table.
  DECLARE
    CURSOR s IS
      SELECT srId FROM DeleteTermReqHelper;
    ids "numList";
  BEGIN
    OPEN s;
    LOOP
      FETCH s BULK COLLECT INTO ids LIMIT 10000;
      EXIT WHEN ids.count = 0;
      FORALL i IN 1 .. ids.COUNT
        DELETE FROM SubRequest WHERE id = ids(i);
      COMMIT;
    END LOOP;
    CLOSE s;
  END;
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DeleteTermReqHelper';

  -- And then related Requests, now orphaned.
    ---- Get ----
  bulkDeleteRequests('StageGetRequest');
    ---- Put ----
  bulkDeleteRequests('StagePutRequest');
    ---- Update ----
  bulkDeleteRequests('StageUpdateRequest');
    ---- PrepareToGet -----
  bulkDeleteRequests('StagePrepareToGetRequest');
    ---- PrepareToPut ----
  bulkDeleteRequests('StagePrepareToPutRequest');
    ---- PrepareToUpdate ----
  bulkDeleteRequests('StagePrepareToUpdateRequest');
    ---- PutDone ----
  bulkDeleteRequests('StagePutDoneRequest');
    ---- Rm ----
  bulkDeleteRequests('StageRmRequest');
    ---- SetGCWeight ----
  bulkDeleteRequests('SetFileGCWeight');

  -- Finally deal with Repack: this case is different because StageRepackRequests may be empty
  -- at the beginning. Therefore we only drop repacks that are in a completed state
  -- for more than the requested time.
  -- First failed ones (status FAILED, ABORTED)
  bulkDelete('SELECT id FROM StageRepackRequest R WHERE status IN (3, 5)
    AND NOT EXISTS (SELECT 1 FROM SubRequest WHERE request = R.id)
    AND lastModificationTime < getTime() - ' || failuresTimeOut || ';',
    'StageRepackRequest');
  -- Then successful ones (status FINISHED)
  bulkDelete('SELECT id FROM StageRepackRequest R WHERE status = 2
    AND NOT EXISTS (SELECT 1 FROM SubRequest WHERE request = R.id)
    AND lastModificationTime < getTime() - ' || successesTimeOut || ';',
    'StageRepackRequest');
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
  FOR dj IN (SELECT id, fileSystem, fileMask
               FROM DrainingJob WHERE status = dconst.DRAININGJOB_SUBMITTED) LOOP
    UPDATE DrainingJob SET status = dconst.DRAININGJOB_STARTING WHERE id = dj.id;
    COMMIT;
    -- Compute totals now. Jobs will be later added in bunches by drainRunner
    SELECT count(*), SUM(diskCopySize) INTO varTFiles, varTBytes
      FROM DiskCopy, CastorFile
     WHERE fileSystem = dj.fileSystem
       AND status = dconst.DISKCOPY_VALID
       AND CastorFile.id = DiskCopy.castorFile
       AND ((dj.fileMask = dconst.DRAIN_FILEMASK_NOTONTAPE AND
             CastorFile.tapeStatus IN (dconst.CASTORFILE_NOTONTAPE, dconst.CASTORFILE_DISKONLY)) OR
            (dj.fileMask = dconst.DRAIN_FILEMASK_ALL));
    UPDATE DrainingJob
       SET totalFiles = varTFiles,
           totalBytes = nvl(varTBytes, 0),
           status = decode(varTBytes, NULL, dconst.DRAININGJOB_FINISHED, dconst.DRAININGJOB_RUNNING)
     WHERE id = dj.id;
    COMMIT;
  END LOOP;
END;
/

/* Fail a file migration, potentially archiving outstanding repack requests */
CREATE OR REPLACE PROCEDURE failFileMigration(inMountTrId IN NUMBER, inFileId IN NUMBER,
                                              inErrorCode IN INTEGER, inReqId IN VARCHAR2) AS
  varNsHost VARCHAR2(2048);
  varCfId NUMBER;
  varNsOpenTime NUMBER;
  varSrIds "numList";
  varOriginalCopyNb NUMBER;
  varMigJobCount NUMBER;
  varErrorCode INTEGER := inErrorCode;
BEGIN
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Lock castor file
  SELECT id, nsOpenTime INTO varCfId, varNsOpenTime
    FROM CastorFile WHERE fileId = inFileId FOR UPDATE;
  -- delete migration job
  DELETE FROM MigrationJob
   WHERE castorFile = varCFId AND mountTransactionId = inMountTrId
  RETURNING originalCopyNb INTO varOriginalCopyNb;
  -- check if another migration should be performed
  SELECT /*+ INDEX_RS_ASC(MigrationJob I_MigrationJob_CFVID) */
         count(*) INTO varMigJobCount
    FROM MigrationJob
   WHERE castorfile = varCfId;
  IF varMigJobCount = 0 THEN
     -- no other migration, delete all migrated segments
     DELETE FROM MigratedSegment
      WHERE castorfile = varCfId;
  END IF;
  -- terminate repack subrequests
  IF varOriginalCopyNb IS NOT NULL THEN
    archiveOrFailRepackSubreq(varCfId, inErrorCode);
  END IF;
  
  IF varErrorCode = serrno.ENOENT THEN
    -- unfortunately, tape servers can throw this error too (see SR #136759), so we have to double check
    -- prior to taking destructive actions on the file: if the file does exist in the Nameserver, then
    -- replace the error code to a generic ETSYS (taped system error), otherwise keep ENOENT
    BEGIN
      SELECT 1902 INTO varErrorCode FROM Dual
       WHERE EXISTS (SELECT 1 FROM Cns_file_metadata@RemoteNS WHERE fileid = inFileId);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;
    END;
  END IF;
  -- Log depending on the error: some are not pathological and have dedicated handling
  IF varErrorCode = serrno.ENOENT OR varErrorCode = serrno.ENSFILECHG OR varErrorCode = serrno.ENSNOSEG THEN
    -- in this case, disk cache is stale
    UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID
     WHERE status = dconst.DISKCOPY_VALID
       AND castorFile = varCfId;
    -- cleanup other migration jobs for that file if any
    DELETE FROM MigrationJob WHERE castorfile = varCfId;
    -- Log 'file was dropped or modified during migration, giving up'
    logToDLF(inReqid, dlf.LVL_NOTICE, dlf.MIGRATION_FILE_DROPPED, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || inMountTrId || ' ErrorCode=' || varErrorCode ||
             ' NsOpenTimeAtStager=' || trunc(varNsOpenTime, 6));
  ELSIF varErrorCode = serrno.ENSTOOMANYSEGS THEN
    -- do as if migration was successful
    UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_ONTAPE WHERE id = varCfId;
    -- Log 'file already had enough copies on tape, ignoring new segment'
    logToDLF(inReqid, dlf.LVL_NOTICE, dlf.MIGRATION_SUPERFLUOUS_COPY, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || inMountTrId);
  ELSE
    -- Any other case, log 'migration to tape failed for this file, giving up'
    logToDLF(inReqid, dlf.LVL_ERROR, dlf.MIGRATION_FAILED, inFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || inMountTrId || ' LastErrorCode=' || varErrorCode);
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- File was dropped, log 'file not found when failing migration'
  logToDLF(inReqid, dlf.LVL_ERROR, dlf.MIGRATION_FAILED_NOT_FOUND, inFileId, varNsHost, 'tapegatewayd',
           'mountTransactionId=' || inMountTrId || ' LastErrorCode=' || varErrorCode);
END;
/

/* PL/SQL method implementing replicateOnClose */
CREATE OR REPLACE PROCEDURE replicateOnClose(cfId IN NUMBER, ouid IN INTEGER, ogid IN INTEGER) AS
  varNsOpenTime NUMBER;
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
      createDisk2DiskCopyJob(cfId, varNsOpenTime, a.id, ouid, ogid, dconst.REPLICATIONTYPE_USER, NULL, FALSE, NULL, TRUE);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;  -- No copies to replicate from
    END;
  END LOOP;
END;
/

/* DB job to start new recall mounts */
CREATE OR REPLACE PROCEDURE startRecallMounts AS
   varNbMounts INTEGER;
   varNbExtraMounts INTEGER := 0;
BEGIN
  -- loop through RecallGroups
  FOR rg IN (SELECT id, name, nbDrives, minAmountDataForMount,
                    minNbFilesForMount, maxFileAgeBeforeMount
               FROM RecallGroup
              ORDER BY vdqmPriority DESC) LOOP
    -- get number of mounts already running for this recallGroup
    SELECT COUNT(*) INTO varNbMounts
      FROM RecallMount
     WHERE recallGroup = rg.id;
    -- check whether some tapes should be mounted
    IF varNbMounts < rg.nbDrives THEN
      DECLARE
        varVID VARCHAR2(2048);
        varDataAmount INTEGER;
        varNbFiles INTEGER;
        varOldestCreationTime NUMBER;
      BEGIN
        -- loop over the best candidates until we have enough mounts
        WHILE varNbMounts + varNbExtraMounts < rg.nbDrives LOOP
          SELECT * INTO varVID, varDataAmount, varNbFiles, varOldestCreationTime FROM (
            SELECT vid, SUM(fileSize) dataAmount, COUNT(*) nbFiles, MIN(creationTime)
              FROM RecallJob
             WHERE recallGroup = rg.id
               AND status = tconst.RECALLJOB_PENDING
             GROUP BY vid
            HAVING (SUM(fileSize) >= rg.minAmountDataForMount OR
                    COUNT(*) >= rg.minNbFilesForMount OR
                    gettime() - MIN(creationTime) > rg.maxFileAgeBeforeMount)
               AND VID NOT IN (SELECT vid FROM RecallMount)
             ORDER BY MIN(creationTime))
           WHERE ROWNUM < 2;
          -- trigger a new mount
          INSERT INTO RecallMount (id, VID, recallGroup, startTime, status)
          VALUES (ids_seq.nextval, varVid, rg.id, gettime(), tconst.RECALLMOUNT_NEW);
          varNbExtraMounts := varNbExtraMounts + 1;
          -- log "startRecallMounts: created new recall mount"
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECMOUNT_NEW_MOUNT, 0, '', 'tapegatewayd',
                   'recallGroup=' || rg.name ||
                   ' TPVID=' || varVid ||
                   ' nbExistingMounts=' || TO_CHAR(varNbMounts) ||
                   ' nbNewMountsSoFar=' || TO_CHAR(varNbExtraMounts) ||
                   ' dataAmountInQueue=' || TO_CHAR(varDataAmount) ||
                   ' nbFilesInQueue=' || TO_CHAR(varNbFiles) ||
                   ' oldestCreationTime=' || TO_CHAR(TRUNC(varOldestCreationTime)));
        END LOOP;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- nothing left to recall, just exit nicely
        NULL;
      END;
      IF varNbExtraMounts = 0 THEN
        -- log "startRecallMounts: no candidate found for a mount"
        logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECMOUNT_NOACTION_NOCAND, 0, '',
                 'tapegatewayd', 'recallGroup=' || rg.name);
      END IF;
    ELSE
      -- log "startRecallMounts: not allowed to start new recall mount. Maximum nb of drives has been reached"
      logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECMOUNT_NOACTION_NODRIVE, 0, '',
               'tapegatewayd', 'recallGroup=' || rg.name);
    END IF;
    COMMIT;
  END LOOP;
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
  -- for the compatibility mode, to be dropped in 2.1.15
  varOpenMode CHAR(1);
  varLastUpdateTime INTEGER;
BEGIN
  varStartTime := SYSTIMESTAMP;
  varReqId := uuidGen();
  -- Get the NS host name
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Get the NS open mode: if compatibility, then CF.lastUpdTime is to be used for the
  -- concurrent modifications check like in 2.1.13, else the new CF.nsOpenTime column is used.
  varOpenMode := getConfigOption@RemoteNS('stager', 'openmode', NULL);
  FOR i IN inFileTrIds.FIRST .. inFileTrIds.LAST LOOP
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
      -- Use the correct timestamp in case of compatibility mode
      IF varOpenMode = 'C' THEN
        varNsOpenTime := varLastUpdateTime;
      END IF;
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
  FOR i IN inFileIds.FIRST .. inFileIds.LAST LOOP
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

/* PL/SQL procedure called when a repack request is over: see archiveSubReq */
CREATE OR REPLACE PROCEDURE handleEndOfRepack(inReqId INTEGER) AS
  varNbLeftSegs INTEGER;
  varStartTime NUMBER;
  varReqUuid VARCHAR2(40);
  varVID VARCHAR2(10);
BEGIN
  -- check how many segments are left in the original tape
  SELECT count(*) INTO varNbLeftSegs FROM Cns_seg_metadata@RemoteNS
   WHERE vid = (SELECT repackVID FROM StageRepackRequest WHERE id = inReqId);
  -- update request
  UPDATE StageRepackRequest
     SET status = CASE varNbLeftSegs WHEN 0 THEN tconst.REPACK_FINISHED ELSE tconst.REPACK_FAILED END,
         lastModificationTime = getTime()
   WHERE id = inReqId
  RETURNING creationTime, reqId, repackVID INTO varStartTime, varReqUuid, varVID;
  -- log successful or unsuccessful completion
  logToDLF(varReqUuid, dlf.LVL_SYSTEM, CASE varNbLeftSegs WHEN 0 THEN dlf.REPACK_COMPLETED ELSE dlf.REPACK_FAILED END, 0, '',
    'repackd', 'TPVID=' || varVID || ' nbLeftSegments=' || TO_CHAR(varNbLeftSegs) || ' elapsedTime=' || TO_CHAR(TRUNC(getTime() - varStartTime)));
END;
/

/* update the db after a successful recall */
CREATE OR REPLACE PROCEDURE tg_setFileRecalled(inMountTransactionId IN INTEGER,
                                               inFseq IN INTEGER,
                                               inFilePath IN VARCHAR2,
                                               inCksumName IN VARCHAR2,
                                               inCksumValue IN INTEGER,
                                               inReqId IN VARCHAR2,
                                               inLogContext IN VARCHAR2) AS
  varFileId         INTEGER;
  varNsHost         VARCHAR2(2048);
  varVID            VARCHAR2(2048);
  varCopyNb         INTEGER;
  varSvcClassId     INTEGER;
  varEuid           INTEGER;
  varEgid           INTEGER;
  varLastOpenTime   NUMBER;
  varCfId           INTEGER;
  varFSId           INTEGER;
  varDCPath         VARCHAR2(2048);
  varDcId           INTEGER;
  varFileSize       INTEGER;
  varFileClassId    INTEGER;
  varNbMigrationsStarted INTEGER;
  varGcWeight       NUMBER;
  varGcWeightProc   VARCHAR2(2048);
  varRecallStartTime NUMBER;
BEGIN
  -- get diskserver, filesystem and path from full path in input
  BEGIN
    parsePath(inFilePath, varFSId, varDCPath, varDCId, varFileId, varNsHost);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "setFileRecalled : unable to parse input path. giving up"
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_INVALID_PATH, 0, '', 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' ' || inLogContext);
    RETURN;
  END;

  -- first lock Castorfile, check NS and parse path
  -- Get RecallJob and lock Castorfile
  BEGIN
    SELECT CastorFile.id, CastorFile.fileId, CastorFile.nsHost, CastorFile.nsOpenTime,
           CastorFile.fileSize, CastorFile.fileClass, RecallMount.VID, RecallJob.copyNb,
           RecallJob.euid, RecallJob.egid
      INTO varCfId, varFileId, varNsHost, varLastOpenTime, varFileSize, varFileClassId, varVID,
           varCopyNb, varEuid, varEgid
      FROM RecallMount, RecallJob, CastorFile
     WHERE RecallMount.mountTransactionId = inMountTransactionId
       AND RecallJob.vid = RecallMount.vid
       AND RecallJob.fseq = inFseq
       AND RecallJob.status = tconst.RECALLJOB_SELECTED
       AND RecallJob.castorFile = CastorFile.id
       AND ROWNUM < 2
       FOR UPDATE OF CastorFile.id;
    -- the ROWNUM < 2 clause is worth a comment here :
    -- this select will select a single CastorFile and RecallMount, but may select
    -- several RecallJobs "linked" to them. All these recall jobs have the same copyNb
    -- but different uid/gid. They exist because these different uid/gid are attached
    -- to different recallGroups.
    -- In case of several recallJobs present, they are all equally responsible for the
    -- recall, thus we pick the first one as "the" responsible. The only consequence is
    -- that it's uid/gid will be used for the DiskCopy creation
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "Unable to identify Recall. giving up"
    logToDLF(inReqId, dlf.LVL_ERROR, dlf.RECALL_NOT_FOUND, varFileId, varNsHost, 'tapegatewayd',
             'mountTransactionId=' || TO_CHAR(inMountTransactionId) ||
             ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' ' || inLogContext);
    RETURN;
  END;

  -- Deal with the DiskCopy: it is created now as the recall is effectively over. The subsequent
  -- check in the NS may make it INVALID, which is fine as opposed to forget about it and generating dark data.

  -- compute GC weight of the recalled diskcopy
  -- first get the svcClass
  SELECT Diskpool2SvcClass.child INTO varSvcClassId
    FROM Diskpool2SvcClass, FileSystem
   WHERE FileSystem.id = varFSId
     AND Diskpool2SvcClass.parent = FileSystem.diskPool
     AND ROWNUM < 2;
  -- Again, the ROWNUM < 2 is worth a comment : the diskpool may be attached
  -- to several svcClasses. However, we do not support that these different
  -- SvcClasses have different GC policies (actually the GC policy should be
  -- moved to the DiskPool table in the future). Thus it is safe to take any
  -- SvcClass from the list
  varGcWeightProc := castorGC.getRecallWeight(varSvcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || varGcWeightProc || '(:size); END;'
    USING OUT varGcWeight, IN varFileSize;
  -- create the DiskCopy, after getting how many copies on tape we have, for the importance number
  DECLARE
    varNbCopiesOnTape INTEGER;
  BEGIN
    SELECT nbCopies INTO varNbCopiesOnTape FROM FileClass WHERE id = varFileClassId;
    INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                          ownerUid, ownerGid, id, gcType, fileSystem, castorFile, status, importance)
    VALUES (varDCPath, varGcWeight, getTime(), getTime(), varFileSize, 0,
            varEuid, varEgid, varDCId, NULL, varFSId, varCfId, dconst.DISKCOPY_VALID,
            -1-varNbCopiesOnTape*100);
  END;

  -- Check that the file is still there in the namespace (and did not get overwritten)
  -- Note that error handling and logging is done inside the function
  IF NOT checkRecallInNS(varCfId, inMountTransactionId, varVID, varCopyNb, inFseq, varFileId, varNsHost,
                         inCksumName, inCksumValue, varLastOpenTime, inReqId, inLogContext) THEN
    RETURN;
  END IF;

  -- Then deal with recalljobs and potential migrationJobs
  -- Find out starting time of oldest recall for logging purposes
  SELECT MIN(creationTime) INTO varRecallStartTime FROM RecallJob WHERE castorFile = varCfId;
  -- Delete recall jobs
  DELETE FROM RecallJob WHERE castorFile = varCfId;
  -- trigger waiting migrations if any
  -- Note that we reset the creation time as if the MigrationJob was created right now
  -- this is because "creationTime" is actually the time of entering the "PENDING" state
  -- in the cases where the migrationJob went through a WAITINGONRECALL state
  UPDATE /*+ INDEX_RS_ASC (MigrationJob I_MigrationJob_CFVID) */ MigrationJob
     SET status = tconst.MIGRATIONJOB_PENDING,
         creationTime = getTime()
   WHERE status = tconst.MIGRATIONJOB_WAITINGONRECALL
     AND castorFile = varCfId;
  varNbMigrationsStarted := SQL%ROWCOUNT;
  -- in case there are migrations, update CastorFile's tapeStatus to NOTONTAPE, otherwise it is ONTAPE
  UPDATE CastorFile
     SET tapeStatus = CASE varNbMigrationsStarted
                        WHEN 0
                        THEN dconst.CASTORFILE_ONTAPE
                        ELSE dconst.CASTORFILE_NOTONTAPE
                      END
   WHERE id = varCfId;

  -- Finally deal with user requests
  UPDATE SubRequest
     SET status = decode(reqType,
                         119, dconst.SUBREQUEST_REPACK, -- repack case
                         dconst.SUBREQUEST_RESTART),    -- standard case
         getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
         lastModificationTime = getTime()
   WHERE castorFile = varCfId
     AND status = dconst.SUBREQUEST_WAITTAPERECALL;

  -- trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(varCfId, varEuid, varEgid);

  -- log success
  logToDLF(inReqId, dlf.LVL_SYSTEM, dlf.RECALL_COMPLETED_DB, varFileId, varNsHost, 'tapegatewayd',
           'mountTransactionId=' || TO_CHAR(inMountTransactionId) || ' TPVID=' || varVID ||
           ' fseq=' || TO_CHAR(inFseq) || ' filePath=' || inFilePath || ' recallTime=' ||
           to_char(trunc(getTime() - varRecallStartTime, 0)) || ' ' || inLogContext);
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
  repackProtocol VARCHAR2(2048);
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
  -- Check which protocol should be used for writing files to disk
  repackProtocol := getConfigOption('Repack', 'Protocol', 'rfio');
  -- creation time for the subrequests
  varCreationTime := getTime();
  -- name server host name
  nsHostName := getConfigOption('stager', 'nsHost', '');
  -- find out the recallGroup to be used for this request
  getRecallGroup(varEuid, varEgid, varRecallGroupId, varRecallGroupName);
  -- update potentially missing metadata introduced with v2.1.14. This will be dropped in 2.1.15.
  update2114Data@RemoteNS(varRepackVID);
  COMMIT;
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
      INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, status, request, getNextStatus, reqType)
      VALUES (0, lastKnownFileName, repackProtocol, segment.segSize, 0, uuidGen(), 0, 0, varCreationTime, varCreationTime, 1, 0, '', NULL, 'NotNullNeeded', ids_seq.nextval, 0, cfId, dconst.SUBREQUEST_START, varReqId, 0, 119)
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
        SELECT /*+ INDEX_RS_ASC(DC I_DiskCopy_CastorFile) */
               count(DC.id) INTO varNbCopies
          FROM DiskCopy DC, FileSystem, DiskServer
         WHERE DC.castorfile = cfId
           AND DC.fileSystem = FileSystem.id
           AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
           AND FileSystem.diskserver = DiskServer.id
           AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
           AND DiskServer.hwOnline = 1
           AND DC.status = dconst.DISKCOPY_VALID;
        IF varNbCopies = 0 THEN
          -- find out whether this file is already being recalled from this tape
          SELECT count(*) INTO varWasRecalled FROM RecallJob WHERE castorfile = cfId AND vid != varRepackVID;
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
        ||'" stackTrace="' || dbms_utility.format_error_backtrace ||'"');
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

/*
 * Database jobs
 */
BEGIN
  -- Create a db job to be run every minute executing the checkNbReplicas procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'checkNbReplicasJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN checkNbReplicas(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Checking for extra replicas to be deleted');
END;
/

/* Code revalidation */
/*********************/

/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_14_14';
COMMIT;
