/******************************************************************************
 *                 stager_2.1.14_to_2.1.15.sql
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
 * This script upgrades a CASTOR v2.1.13-6 STAGER database to v2.1.14-0
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
     AND release = '2_1_15_0'
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
     AND release LIKE '2_1_14_1%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_15_0', '2_1_15_0', 'NON TRANSPARENT');
COMMIT;

/* Stop and drop all jobs. They will be recreated at the end */
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs)
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name, force=>TRUE);
    END IF;
    dbms_scheduler.drop_job(a.job_name);
  END LOOP;
END;
/

/* Schema changes */
/******************/

/* Drop updates from Type2Obj and redirect PrepareTo requests */
DELETE FROM Type2Obj WHERE type in (38, 44, 147);
DELETE FROM WhiteList WHERE reqType IN (38, 44, 147);
DELETE FROM BlackList WHERE reqType IN (38, 44, 147);
UPDATE Type2Obj SET svcHandler = 'JobReqSvc' WHERE type IN (36, 37);

/* drop updates */
DROP TABLE StagePrepareToUpdateRequest;
DROP TABLE StageUpdateRequest;
DROP TABLE FirstByteWritten;
DROP PROCEDURE FirstByteWrittenProc;
DROP PROCEDURE handleProtoNoUpd;
DROP PROCEDURE getBestDiskCopyToRead;
DROP PROCEDURE getUpdateDoneProc;
DROP PROCEDURE getUpdateStart;
DROP PROCEDURE getUpdateFailedProc;
DROP PROCEDURE getUpdateFailedProcExt;
DROP PROCEDURE prepareForMigration;
DROP PROCEDURE putFailedProc;
DROP PROCEDURE putFailedProcExt;

/* add DataPools */
CREATE TABLE DataPool
 (name VARCHAR2(2048),
  id INTEGER CONSTRAINT PK_DataPool_Id PRIMARY KEY,
  minAllowedFreeSpace NUMBER,
  maxFreeSpace NUMBER,
  totalSize INTEGER,
  free INTEGER)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE DataPool2SvcClass (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_DataPool2SvcClass_C on DataPool2SvcClass (child);
CREATE INDEX I_DataPool2SvcClass_P on DataPool2SvcClass (parent);
ALTER TABLE DataPool2SvcClass
  ADD CONSTRAINT FK_DataPool2SvcClass_P FOREIGN KEY (Parent) REFERENCES DataPool (id)
  ADD CONSTRAINT FK_DataPool2SvcClass_C FOREIGN KEY (Child) REFERENCES SvcClass (id);

ALTER TABLE DiskServer ADD (dataPool INTEGER);
ALTER TABLE DiskServer ADD CONSTRAINT FK_DiskServer_DataPool 
  FOREIGN KEY (dataPool) REFERENCES DataPool(id);

ALTER TABLE DiskCopy ADD (dataPool INTEGER);
CREATE INDEX I_DiskCopy_DataPool ON DiskCopy (dataPool);
CREATE INDEX I_DiskCopy_DP_GCW ON DiskCopy (dataPool, gcWeight);
CREATE INDEX I_DiskCopy_Status_7_DP ON DiskCopy (decode(status,7,status,NULL), dataPool);
ALTER TABLE DiskCopy ADD CONSTRAINT FK_DiskCopy_FileSystem
  FOREIGN KEY (FileSystem) REFERENCES FileSystem (id);
ALTER TABLE DiskCopy ADD CONSTRAINT FK_DiskCopy_DataPool
  FOREIGN KEY (DataPool) REFERENCES DataPool (id);

ALTER TABLE SubRequest ADD (diskServer INTEGER);
CREATE INDEX I_SubRequest_DiskServer ON SubRequest (diskServer);

ALTER TABLE DeleteDiskCopyHelper ADD (msg VARCHAR2(2048));
DROP PROCEDURE deleteDiskCopiesInFSs;

/* PL/SQL code revalidation */
/****************************/



/* Database jobs */
BEGIN
  -- Drop all monitoring jobs
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name LIKE ('MONITORINGJOB_%'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Recreate monitoring jobs
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'monitoringJob_1min',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startDbJob(''BEGIN CastorMon.waitTapeMigrationStats(); CastorMon.waitTapeRecallStats(); END;'', ''stagerd''); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/3600,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Generation of monitoring information about migrations and recalls backlog');
END;
/


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN recompileAll(); END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = SYSTIMESTAMP, state = 'COMPLETE'
 WHERE release = '2_1_15_0';
COMMIT;
