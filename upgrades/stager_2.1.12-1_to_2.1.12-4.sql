/******************************************************************************
 *                 stager_2.1.12-1_to_2.1.12-4.sql
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
 * This script upgrades a CASTOR v2.1.12-1 STAGER database to v2.1.12-4
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
   WHERE schemaVersion = '2_1_12_0'
     AND release = '2_1_12_4'
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
     AND release LIKE '2_1_12_1%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_12_0', '2_1_12_4', 'TRANSPARENT');
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


/* Update and revalidation of PL-SQL code */
/******************************************/
CREATE OR REPLACE PROCEDURE startMigrationMounts AS
  varNbMounts INTEGER;
  varDataAmount INTEGER;
  varNbFiles INTEGER;
  varOldestCreationTime NUMBER;
BEGIN
  -- loop through tapepools
  FOR t IN (SELECT id, nbDrives, minAmountDataForMount,
                   minNbFilesForMount, maxFileAgeBeforeMount
              FROM TapePool) LOOP
    -- get number of mounts already running for this tapepool
    SELECT count(*) INTO varNbMounts
      FROM MigrationMount
     WHERE tapePool = t.id;
    -- get the amount of data and number of files to migrate, plus the age of the oldest file
    BEGIN
      SELECT SUM(fileSize), COUNT(*), MIN(creationTime) INTO varDataAmount, varNbFiles, varOldestCreationTime
        FROM MigrationJob
       WHERE tapePool = t.id
         AND status IN (tconst.MIGRATIONJOB_PENDING, tconst.MIGRATIONJOB_SELECTED)
       GROUP BY tapePool;
      -- Create as many mounts as needed according to amount of data and number of files
      WHILE (varNbMounts < t.nbDrives) AND
            ((varDataAmount/(varNbMounts+1) >= t.minAmountDataForMount) OR
             (varNbFiles/(varNbMounts+1) >= t.minNbFilesForMount)) AND
            (varNbMounts < varNbFiles) LOOP   -- in case minAmountDataForMount << avgFileSize, stop creating more than one mount per file
        insertMigrationMount(t.id);
        varNbMounts := varNbMounts + 1;
      END LOOP;
      -- force creation of a unique mount in case no mount was created at all and some files are too old
      IF varNbFiles > 0 AND varNbMounts = 0 AND t.nbDrives > 0 AND
         gettime() - varOldestCreationTime > t.maxFileAgeBeforeMount THEN
        insertMigrationMount(t.id);
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- there is no file to migrate, so nothing to do
      NULL;
    END;
    COMMIT;
  END LOOP;
END;
/

CREATE OR REPLACE PROCEDURE getUpdateStart
        (srId IN INTEGER, selectedDiskServer IN VARCHAR2, selectedMountPoint IN VARCHAR2,
         dci OUT INTEGER, rpath OUT VARCHAR2, rstatus OUT NUMBER, reuid OUT INTEGER,
         regid OUT INTEGER, diskCopySize OUT NUMBER) AS
  cfid INTEGER;
  fid INTEGER;
  dcId INTEGER;
  fsId INTEGER;
  dcIdInReq INTEGER;
  nh VARCHAR2(2048);
  fileSize INTEGER;
  srSvcClass INTEGER;
  proto VARCHAR2(2048);
  isUpd NUMBER;
  nbAc NUMBER;
  gcw NUMBER;
  gcwProc VARCHAR2(2048);
  cTime NUMBER;
BEGIN
  -- Get data
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ euid, egid, svcClass, upd, diskCopy
    INTO reuid, regid, srSvcClass, isUpd, dcIdInReq
    FROM SubRequest,
        (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, euid, egid, svcClass, 0 AS upd FROM StageGetRequest UNION ALL
         SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, euid, egid, svcClass, 1 AS upd FROM StageUpdateRequest) Request
   WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
  -- Take a lock on the CastorFile. Associated with triggers,
  -- this guarantees we are the only ones dealing with its copies
  SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ CastorFile.fileSize, CastorFile.id
    INTO fileSize, cfId
    FROM CastorFile, SubRequest
   WHERE CastorFile.id = SubRequest.castorFile
     AND SubRequest.id = srId FOR UPDATE OF CastorFile;
  -- Get selected filesystem
  SELECT FileSystem.id INTO fsId
    FROM DiskServer, FileSystem
   WHERE FileSystem.diskserver = DiskServer.id
     AND DiskServer.name = selectedDiskServer
     AND FileSystem.mountPoint = selectedMountPoint;
  -- Try to find local DiskCopy
  SELECT /*+ INDEX(DiskCopy I_DiskCopy_Castorfile) */ id, nbCopyAccesses, gcWeight, creationTime
    INTO dcId, nbac, gcw, cTime
    FROM DiskCopy
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.filesystem = fsId
     AND DiskCopy.status IN (0, 6, 10) -- STAGED, STAGEOUT, CANBEMIGR
     AND ROWNUM < 2;
  -- We found it, so we are settled and we'll use the local copy.
  -- It might happen that we have more than one, because the scheduling may have
  -- scheduled a replication on a fileSystem which already had a previous diskcopy.
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
  RETURNING id, path, status, diskCopySize INTO dci, rpath, rstatus, diskCopySize;
  -- Let's update the SubRequest and set the link with the DiskCopy
  UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
     SET DiskCopy = dci
   WHERE id = srId RETURNING protocol INTO proto;
  -- In case of an update, if the protocol used does not support
  -- updates properly (via firstByteWritten call), we should
  -- call firstByteWritten now and consider that the file is being
  -- modified.
  IF isUpd = 1 THEN
    handleProtoNoUpd(srId, proto);
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- No disk copy found on selected FileSystem. This can happen in 2 cases :
  --  + either a diskcopy was available and got disabled before this job
  --    was scheduled. Bad luck, we fail the request, the user will have to retry
  --  + or we are an update creating a file and there is a diskcopy in WAITFS
  --    or WAITFS_SCHEDULING associated to us. Then we have to call putStart
  -- So we first check the update hypothesis
  IF isUpd = 1 AND dcIdInReq IS NOT NULL THEN
    DECLARE
      stat NUMBER;
    BEGIN
      SELECT status INTO stat FROM DiskCopy WHERE id = dcIdInReq;
      IF stat IN (5, 11) THEN -- WAITFS, WAITFS_SCHEDULING
        -- it is an update creating a file, let's call putStart
        putStart(srId, selectedDiskServer, selectedMountPoint, dci, rstatus, rpath);
        RETURN;
      END IF;
    END;
  END IF;
  -- It was not an update creating a file, so we fail
  UPDATE SubRequest
     SET status = 7, errorCode = 1725, errorMessage='Request canceled while queuing'
   WHERE id = srId;
  COMMIT;
  raise_application_error(-20114, 'File invalidated while queuing in the scheduler, please try again');
END;
/

/* default migration candidate selection policy */
CREATE OR REPLACE
PROCEDURE tg_defaultMigrSelPolicy(inMountId IN INTEGER,
                                  outDiskServerName OUT NOCOPY VARCHAR2,
                                  outMountPoint OUT NOCOPY VARCHAR2,
                                  outPath OUT NOCOPY VARCHAR2,
                                  outDiskCopyId OUT INTEGER,
                                  outLastKnownFileName OUT NOCOPY VARCHAR2, 
                                  outFileId OUT INTEGER,
                                  outNsHost OUT NOCOPY VARCHAR2, 
                                  outFileSize OUT INTEGER,
                                  outMigJobId OUT INTEGER, 
                                  outLastUpdateTime OUT INTEGER) AS
  /* Find the next file to migrate for a given migration mount.
   *
   * Procedure's input: migration mount id
   * Procedure's output: non-zero MigrationJob ID
   *
   * Lock taken on the migration job if it selects one.
   * 
   * Per policy we should only propose a migration job for a file that does not 
   * already have another copy migrated to the same tape.
   * The already migrated copies are kept in MigratedSegment until the whole set
   * of siblings has been migrated.
   */
  LockError EXCEPTION;
  PRAGMA EXCEPTION_INIT (LockError, -54);
  CURSOR c IS
    SELECT /*+ FIRST_ROWS_1 
               LEADING(MigrationMount MigrationJob CastorFile DiskCopy FileSystem DiskServer)
               INDEX(CastorFile PK_CastorFile_Id)
               INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile)
               INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusId) */
           DiskServer.name, FileSystem.mountPoint, DiskCopy.path, DiskCopy.id, CastorFile.lastKnownFilename,
           CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, MigrationJob.id, CastorFile.lastUpdateTime
      FROM MigrationMount, MigrationJob, DiskCopy, FileSystem, DiskServer, CastorFile
     WHERE MigrationMount.id = inMountId
       AND MigrationJob.tapePool = MigrationMount.tapePool
       AND MigrationJob.status = tconst.MIGRATIONJOB_PENDING
       AND CastorFile.id = MigrationJob.castorFile
       AND DiskCopy.castorFile = MigrationJob.castorFile
       AND DiskCopy.status = dconst.DISKCOPY_CANBEMIGR
       AND FileSystem.id = DiskCopy.fileSystem
       AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING)
       AND DiskServer.id = FileSystem.diskServer
       AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING)
       AND MigrationMount.VID NOT IN (SELECT /*+ INDEX_RS_ASC(MigratedSegment I_MigratedSegment_CFCopyNBVID) */ vid 
                                        FROM MigratedSegment
                                       WHERE castorFile = MigrationJob.castorfile
                                         AND copyNb != MigrationJob.destCopyNb)
       FOR UPDATE OF MigrationJob.id SKIP LOCKED;
BEGIN
  OPEN c;
  FETCH c INTO outDiskServerName, outMountPoint, outPath, outDiskCopyId, outLastKnownFileName,
               outFileId, outNsHost, outFileSize, outMigJobId, outLastUpdateTime;
  CLOSE c;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Nothing to migrate. Simply return
  CLOSE c;
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
 WHERE release = '2_1_12_4';
COMMIT;
