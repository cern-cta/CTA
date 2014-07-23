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
 * This script upgrades a CASTOR v2.1.14-11 STAGER database to v2.1.14-13
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

/* Post-upgrade cleanup phase: archive all requests older than 24h and stuck in status 4 */
BEGIN
  FOR s in (SELECT id FROM SubRequest WHERE status = 4 AND creationTime < getTime() - 86400) LOOP
    archiveSubReq(s.id, dconst.SUBREQUEST_FINISHED);
    COMMIT;
  END LOOP;
END;
/

