/******************************************************************************
 *                 stager_2.1.12-0_to_2.1.12-1.sql
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
 * This script upgrades a CASTOR v2.1.12-0 STAGER database to v2.1.12-1
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
   WHERE schemaVersion = '2_1_12_PRE2'
     AND release = '2_1_12_PRE2'
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

/* Starting the upgrade */
INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_12_PRE2', '2_1_12_PRE2', 'NON TRANSPARENT');
COMMIT;

/* Job management */
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs)
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name, force=>TRUE);
    END IF;
    -- Schedule the start date of the job to 60 minutes from now. This
    -- basically pauses the job for 60 minutes so that the upgrade can
    -- go through as quickly as possible.
    dbms_scheduler.set_attribute(a.job_name, 'START_DATE', SYSDATE + 60/1440);
  END LOOP;
END;
/

/* Drop removed job from scheduler */
/***********************************/
  DBMS_SCHEDULER.DROP_JOB(
      JOB_NAME        => 'MigrationMountsJob',
      FORCE           => TRUE);

/* Schema changes go here */
/**************************/
/* Global temporary table to store reports from tg_startMigrationMounts */
CREATE GLOBAL TEMPORARY TABLE startMigMountReportHelper (
    tapePool       VARCHAR2(2048),
    requestID      NUMBER,
    sizeQueued     NUMBER,
    filesQueued    NUMBER,
    mountsBefore   NUMBER,
    mountsCreated  NUMBER,
    mountsAfter    NUMBER) ON COMMIT DELETE ROWS;

    
/* Update and revalidation of PL-SQL code */
/******************************************/
DROP PROCEDURE startMigrationMounts;

/* get the migration mounts without any tape associated */
CREATE OR REPLACE
PROCEDURE tg_getMigMountsWithoutTapes(outStrList OUT castorTape.MigrationMount_Cur) AS
BEGIN
  -- get migration mounts  in state WAITTAPE with a non-NULL TapeGatewayRequestId
  OPEN outStrList FOR
    SELECT M.id, M.TapeGatewayRequestId, TP.name
      FROM MigrationMount M,Tapepool TP
     WHERE M.status = tconst.MIGRATIONMOUNT_WAITTAPE
       AND M.TapeGatewayRequestId IS NOT NULL
       AND M.tapepool=TP.id 
       FOR UPDATE OF M.id SKIP LOCKED;   
END;
/


/* startMigationMounts: review the pending migrations for all tape pools
  and determine if, per time/count/size criterias, we should mount additional 
  tapes for migration. This is translated into the creation of that migrationMount */
CREATE OR REPLACE PROCEDURE tg_startMigrationMounts (
  out outResCur sys_refcursor ) AS
  TYPE startMigrationMountReport_t IS RECORD (
    tapePool       VARCHAR2(2048),
    requestID      NUMBER,
    sizeQueued     NUMBER,
    filesQueued    NUMBER,
    mountsBefore   NUMBER,
    mountsCreated  NUMBER,
    mountsAfter    NUMBER);
  TYPE startMigMountRepTab_t IS TABLE OF startMigrationMountReport_t;
  varReportEntry   NUMBER :=1;
  varMigMountRepTable  startMigMountRepTab_t := startMigMountRepTab_t();
BEGIN
  -- loop through tapepools
  FOR t IN (SELECT id, name, nbDrives, minAmountDataForMount,
                   minNbFilesForMount, maxFileAgeBeforeMount
              FROM TapePool) LOOP
    DECLARE
      varNbMounts INTEGER;
      varDataAmount INTEGER;
      varNbFiles INTEGER;
      varOldestCreationTime NUMBER;
      varMountCreated  NUMBER := 0;
    BEGIN
      -- get number of mounts already running for this tapepool
      SELECT count(*) INTO varNbMounts
        FROM MigrationMount
       WHERE tapePool = t.id;
      -- get the amount of data and number of files to migrate, plus the age of the oldest file
      DECLARE
        varTGRequestId   NUMBER := -1;
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
          insertMigrationMount(t.id, varTGRequestId);
          varNbMounts := varNbMounts + 1;
          -- Locally record the action
          varMountCreated := 1;
          varMigMountRepTable.EXTEND(1);
          varMigMountRepTable(varReportEntry).tapePool      := t.name;
          varMigMountRepTable(varReportEntry).requestID     := varTGReqId;
          varMigMountRepTable(varReportEntry).sizeQueued    := varDataAmount;
          varMigMountRepTable(varReportEntry).filesQueued   := varNbFiles;
          varMigMountRepTable(varReportEntry).mountsBefore  := varNbMounts - 1;
          varMigMountRepTable(varReportEntry).mountsCreated := 1;
          varMigMountRepTable(varReportEntry).mountsAfter   := varNbMounts;
          varReportEntry:= varReportEntry+1;
        END LOOP;
        -- force creation of a unique mount in case no mount was created at all and some files are too old
        IF varNbFiles > 0 AND varNbMounts = 0 AND t.nbDrives > 0 AND
           gettime() - varOldestCreationTime > t.maxFileAgeBeforeMount THEN
          insertMigrationMount(t.id, varTGRequestId);
          varMountCreated := 1;
          varMigMountRepTable.EXTEND(1);
          varMigMountRepTable(varReportEntry).tapePool      := t.name;
          varMigMountRepTable(varReportEntry).requestID     := varTGReqId;
          varMigMountRepTable(varReportEntry).sizeQueued    := varDataAmount;
          varMigMountRepTable(varReportEntry).filesQueued   := varNbFiles;
          varMigMountRepTable(varReportEntry).mountsBefore  := varNbMounts - 1;
          varMigMountRepTable(varReportEntry).mountsCreated := 1;
          varMigMountRepTable(varReportEntry).mountsAfter   := varNbMounts;
          varReportEntry:= varReportEntry+1;
        END IF;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- there is no file to migrate, so nothing to do
        NULL;
      END;
      COMMIT;
      -- Record the non-creation of migration mount
      IF (varMountCreated = 0) THEN
        varMigMountRepTable.EXTEND(1);
        varMigMountRepTable(varReportEntry).tapePool      := t.name;
        varMigMountRepTable(varReportEntry).requestID     := varTGReqId;
        varMigMountRepTable(varReportEntry).sizeQueued    := varDataAmount;
        varMigMountRepTable(varReportEntry).filesQueued   := varNbFiles;
        varMigMountRepTable(varReportEntry).mountsBefore  := varNbMounts;
        varMigMountRepTable(varReportEntry).mountsCreated := 0;
        varMigMountRepTable(varReportEntry).mountsAfter   := varNbMounts;
        varReportEntry:= varReportEntry+1;
      END IF;
    END;
  END LOOP;
  -- Each loop comited. We can not store the result in a temporary table without
  -- loosing the content on commit, and return a cursor to it.
  FOR i IN varMigMountRepTable.FIRST .. varMigMountRepTable.LAST LOOP
    insert into startMigMountReportHelper (tapePool, requestID, sizeQueued, 
                      filesQueued, mountsBefore, mountsCreated, mountsAfter)
  VALUES (varMigMountRepTable(i).tapePool , varMigMountRepTable(i).requestID , 
          varMigMountRepTable(i).sizeQueued , varMigMountRepTable(i).filesQueued ,
          varMigMountRepTable(i).mountsBefore , varMigMountRepTable(i).mountsCreated ,
          varMigMountRepTable(i).mountsCreated);
  END LOOP;
  open outResCur for select * from startMigMountReportHelper;
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
 WHERE release = '2_1_12_PRE2';
COMMIT;
