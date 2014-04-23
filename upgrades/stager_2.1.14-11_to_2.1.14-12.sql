/******************************************************************************
 *                 stager_2.1.14-5_to_2.1.14-11.sql
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
 * This script upgrades a CASTOR v2.1.14-5 STAGER database to v2.1.14-11
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
     AND release = '2_1_14_12'
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
     AND release LIKE '2_1_14_11-1%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_12', 'TRANSPARENT');
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

CREATE INDEX I_MigrationJob_TPStatusCT ON MigrationJob(tapePool, status, creationTime);

XXX First put the code that uses the following

DROP INDEX I_FileSystem_Rate;
DROP FUNCTION fileSystemRate;


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
             INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusCT) */
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

CREATE OR REPLACE PROCEDURE tg_getBulkFilesToMigrate(inLogContext IN VARCHAR2,
                                                     inMountTrId IN NUMBER,
                                                     inCount IN INTEGER,
                                                     inTotalSize IN INTEGER,
                                                     outFiles OUT castorTape.FileToMigrateCore_cur) AS
  varMountId NUMBER;
  varCount INTEGER;
  varTotalSize INTEGER;
  varOldestAcceptableAge NUMBER;
  varVid VARCHAR2(10);
  varNewFseq INTEGER;
  varFileTrId NUMBER;
  varTpId INTEGER;
  varUnused INTEGER;
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
BEGIN
  BEGIN
    -- Get id, VID and last valid fseq for this migration mount, lock
    SELECT id, vid, tapePool, lastFSeq INTO varMountId, varVid, varTpId, varNewFseq
      FROM MigrationMount
     WHERE mountTransactionId = inMountTrId
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- migration mount is over or unknown request: return an empty cursor
    OPEN outFiles FOR
      SELECT fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fseq, fileSize
        FROM FilesToMigrateHelper;
    RETURN;
  END;
  varCount := 0;
  varTotalSize := 0;
  SELECT TapePool.maxFileAgeBeforeMount INTO varOldestAcceptableAge
    FROM TapePool, MigrationMount
   WHERE MigrationMount.id = varMountId
     AND MigrationMount.tapePool = TapePool.id;
  -- Get candidates up to inCount or inTotalSize
  FOR Cand IN (
    SELECT /*+ FIRST_ROWS(100)
               LEADING(Job CastorFile DiskCopy FileSystem DiskServer)
               USE_NL(Job CastorFile DiskCopy FileSystem DiskServer)
               INDEX(CastorFile PK_CastorFile_Id)
               INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */
           Job.id mjId, DiskServer.name || ':' || FileSystem.mountPoint || DiskCopy.path filePath,
           CastorFile.fileId, CastorFile.nsHost, CastorFile.fileSize, CastorFile.lastKnownFileName,
           Castorfile.id as castorfile
      FROM (SELECT * FROM
             (SELECT /*+ FIRST_ROWS(100) INDEX_RS_ASC(MigrationJob I_MigrationJob_TPStatusCT) */
                     id, castorfile, destCopyNb, creationTime
                FROM MigrationJob
               WHERE tapePool = varTpId
                 AND status = tconst.MIGRATIONJOB_PENDING
               ORDER BY creationTime)
             WHERE ROWNUM < TO_NUMBER(getConfigOption('Migration', 'NbMigCandConsidered', 10000)))
           Job, CastorFile, DiskCopy, FileSystem, DiskServer
     WHERE CastorFile.id = Job.castorFile
       AND CastorFile.id = DiskCopy.castorFile
       AND CastorFile.tapeStatus = dconst.CASTORFILE_NOTONTAPE
       AND DiskCopy.status = dconst.DISKCOPY_VALID
       AND FileSystem.id = DiskCopy.fileSystem
       AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
       AND DiskServer.id = FileSystem.diskServer
       AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
       AND DiskServer.hwOnline = 1
       AND NOT EXISTS (SELECT /*+ USE_NL(MigratedSegment)
                                  INDEX_RS_ASC(MigratedSegment I_MigratedSegment_CFCopyNBVID) */ 1
                         FROM MigratedSegment
                        WHERE MigratedSegment.castorFile = Job.castorfile
                          AND MigratedSegment.copyNb != Job.destCopyNb
                          AND MigratedSegment.vid = varVid)
       ORDER BY -- we first order by a multi-step function, which gives old guys incrasingly more priority:
                -- migrations younger than varOldestAccep2tableAge will be taken last
                TRUNC(Job.creationTime/varOldestAcceptableAge) ASC,
                -- and then, for all migrations between (N-1)*varOldestAge and N*varOldestAge, by filesystem load
                FileSystem.nbRecallerStreams + FileSystem.nbMigratorStreams ASC)
  LOOP
    -- last part of the above statement. Could not be part of it as ORACLE insisted on not
    -- optimizing properly the execution plan
    BEGIN
      SELECT /*+ INDEX_RS_ASC(MJ I_MigrationJob_CFVID) */ 1 INTO varUnused
        FROM MigrationJob MJ
       WHERE MJ.castorFile = Cand.castorFile
         AND MJ.vid = varVid
         AND MJ.vid IS NOT NULL;
      -- found one, so skip this candidate
      CONTINUE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- nothing, it's a valid candidate. Let's lock it
      DECLARE
        MjLocked EXCEPTION;
        PRAGMA EXCEPTION_INIT (MjLocked, -54);
      BEGIN
        SELECT id INTO varUnused FROM MigrationJob WHERE id = Cand.mjId FOR UPDATE NOWAIT;
      EXCEPTION WHEN MjLocked THEN
        -- this migration job is already handled else where, let's go to next one
        CONTINUE;
      END;
    END;
    BEGIN
      -- Try to take this candidate on this mount
      INSERT INTO FilesToMigrateHelper (fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fileSize, fseq)
        VALUES (Cand.fileId, Cand.nsHost, Cand.lastKnownFileName, Cand.filePath, ids_seq.NEXTVAL, Cand.fileSize, varNewFseq)
        RETURNING fileTransactionId INTO varFileTrId;
    EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- If we fail here, it means that another copy of this file was already selected for this mount.
      -- Not a big deal, we skip this candidate and keep going.
      CONTINUE;
    END;
    varCount := varCount + 1;
    varTotalSize := varTotalSize + Cand.fileSize;
    UPDATE MigrationJob
       SET status = tconst.MIGRATIONJOB_SELECTED,
           vid = varVid,
           fSeq = varNewFseq,
           mountTransactionId = inMountTrId,
           fileTransactionId = varFileTrId
     WHERE id = Cand.mjId;
    varNewFseq := varNewFseq + 1;    -- we still decide the fseq for each migration candidate
    IF varCount >= inCount OR varTotalSize >= inTotalSize THEN
      -- we have enough candidates for this round, exit loop
      EXIT;
    END IF;
  END LOOP;
  -- Update last fseq
  UPDATE MigrationMount
     SET lastFseq = varNewFseq
   WHERE id = varMountId;
  -- Return all candidates (potentially an empty cursor). Don't commit now, this will be done
  -- in C++ after the results have been collected as the temporary table will be emptied. 
  OPEN outFiles FOR
    SELECT fileId, nsHost, lastKnownFileName, filePath, fileTransactionId, fseq, fileSize
      FROM FilesToMigrateHelper;
END;
/

DROP INDEX I_MigrationJob_TPStatusId;

/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('MIGRATIONMOUNTSJOB', 'RECALLMOUNTSJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every minute executing the startMigrationMounts procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'MigrationMountsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN startMigrationMounts(); END;'', ''tapegatewayd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Creates MigrationMount entries when new migrations should start');

  -- Create a db job to be run every minute executing the startRecallMounts procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'RecallMountsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN startRecallMounts(); END;'', ''tapegatewayd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Creates RecallMount entries when new recalls should start');
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
 WHERE release = '2_1_14_12';
COMMIT;

/* Post-upgrade cleanup phase: archive all requests older than 24h and stuck in status 4 */
BEGIN
  FOR s in (SELECT id FROM SubRequest WHERE status = 4 AND creationTime < getTime() - 86400) LOOP
    archiveSubReq(s.id, dconst.SUBREQUEST_FINISHED);
    COMMIT;
  END LOOP;
END;
/
