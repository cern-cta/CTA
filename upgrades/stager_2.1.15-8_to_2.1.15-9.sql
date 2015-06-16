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
 * This script upgrades a CASTOR v2.1.15-8 STAGER database to v2.1.15-9
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
     AND release = '2_1_15_9'
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
     AND release LIKE '2_1_15_8%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_15_0', '2_1_15_9', 'NON TRANSPARENT');
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

/* PL/SQL method implementing disk2DiskCopyEnded
 * Note that inDestDsName, inDestPath, inReplicaFileSize and inCksumValue are not used when inErrorMessage is not NULL
 * inErrorCode is used in case of error to decide whether to retry and also to invalidate
 * the source diskCopy if the error is an ENOENT
 */
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
    BEGIN
      SELECT csumValue INTO varFCksum
        FROM Cns_file_metadata@remoteNS
       WHERE fileId = varFileId;
      IF inReplicaFileSize != varFileSize OR to_number(inCksumValue, 'XXXXXXXX') != to_number(varFCksum, 'XXXXXXXX') THEN
        -- replication went wrong !
        varNewDcStatus := dconst.DISKCOPY_INVALID;
        varErrorMessage := 'File size/checksum mismatch during replication, the source file is probably corrupted';
      END IF;
    EXCEPTION WHEN INVALID_NUMBER THEN
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
  varComment := 'transferId="' || inTransferId ||
         '" destSvcClass=' || getSvcClassName(varDestSvcClass) ||
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
        IF varPath != varDestPath OR varStatus != varNewDcStatus THEN
          -- not the expected case, reraise the exception
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

/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_15_9';
COMMIT;
