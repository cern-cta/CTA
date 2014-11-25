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
DELETE FROM Type2Obj WHERE type IN (38, 44, 147);
DELETE FROM WhiteList WHERE reqType IN (38, 44, 147);
DELETE FROM BlackList WHERE reqType IN (38, 44, 147);
UPDATE Type2Obj SET svcHandler = 'JobReqSvc' WHERE type IN (36, 37);
/* Drop other obsoleted entries from Type2Obj */
DELETE FROM Type2Obj WHERE type IN (18, 29, 84, 85, 86, 87, 88, 89, 90, 01, 92, 148);

/* Restructure the SubRequest table */
DROP TABLE SubRequest;
CREATE TABLE SubRequest (retryCounter NUMBER,
                         fileName VARCHAR2(2048),
                         protocol VARCHAR2(2048),
                         xsize INTEGER,
                         priority NUMBER,
                         subreqId VARCHAR2(2048),
                         flags NUMBER,
                         modeBits NUMBER,
                         creationTime INTEGER CONSTRAINT NN_SubRequest_CreationTime NOT NULL,
                         lastModificationTime INTEGER,
                         answered NUMBER,
                         errorCode NUMBER, 
                         errorMessage VARCHAR2(2048),
                         id NUMBER CONSTRAINT NN_SubRequest_Id NOT NULL,
                         diskcopy INTEGER,
                         diskServer INTEGER,
                         castorFile INTEGER,
                         status INTEGER,
                         request INTEGER,
                         getNextStatus INTEGER,
                         requestedFileSystems VARCHAR2(2048),
                         svcHandler VARCHAR2(2048) CONSTRAINT NN_SubRequest_SvcHandler NOT NULL,
                         reqType INTEGER CONSTRAINT NN_SubRequest_reqType NOT NULL)
  PCTFREE 50 PCTUSED 40 INITRANS 50
  ENABLE ROW MOVEMENT
  PARTITION BY LIST (STATUS)
  SUBPARTITION BY HASH(ID) SUBPARTITIONS 5
   (
    PARTITION P_STATUS_START    VALUES (0, 1, 2),      -- *START
    PARTITION P_STATUS_ACTIVE   VALUES (3, 4, 5, 6, 12),
    PARTITION P_STATUS_FAILED   VALUES (7),
    PARTITION P_STATUS_FINISHED VALUES (8, 9, 10, 11),        -- FAILED_*
    PARTITION P_STATUS_SCHED    VALUES (13, 14)      -- *SCHED
   );

ALTER TABLE SubRequest
  ADD CONSTRAINT PK_SubRequest_Id PRIMARY KEY (ID);
CREATE INDEX I_SubRequest_Svc_CT_ID ON SubRequest(svcHandler, creationTime, id) LOCAL;
CREATE INDEX I_SubRequest_Req_Stat_no89 ON SubRequest (request, decode(status,8,NULL,9,NULL,status));
CREATE INDEX I_SubRequest_Castorfile ON SubRequest (castorFile);
CREATE INDEX I_SubRequest_DiskCopy ON SubRequest (diskCopy);
CREATE INDEX I_SubRequest_DiskServer ON SubRequest (diskServer);
CREATE INDEX I_SubRequest_Request ON SubRequest (request);
CREATE INDEX I_SubRequest_SubReqId ON SubRequest (subReqId);
CREATE INDEX I_SubRequest_LastModTime ON SubRequest (lastModificationTime);

/* Modify SvcClass table for the drop of replicateOnClose */
ALTER TABLE SvcClass DROP COLUMN replicateOnClose;
ALTER TABLE SvcClass RENAME COLUMN maxReplicaNb to replicaNb;
DROP PROCEDURE handleReplication;

/* drop obsoleted entities */
-- updates support
DROP TABLE StagePrepareToUpdateRequest;
DROP TABLE StageUpdateRequest;
DROP TABLE FirstByteWritten;
-- internal requests for jobs
DROP TABLE GetUpdateStartRequest;
DROP TABLE GetUpdateDone;
DROP TABLE GetUpdateFailed;
DROP TABLE PutStartRequest;
DROP TABLE MoverCloseRequest;
DROP TABLE PutFailed;
-- obsolete for long
DROP TABLE GCLocalFile;

DROP PROCEDURE firstByteWrittenProc;
DROP PROCEDURE handleProtoNoUpd;
DROP PROCEDURE getBestDiskCopyToRead;
DROP PROCEDURE getUpdateDoneProc;
DROP PROCEDURE getUpdateStart;
DROP PROCEDURE getUpdateFailedProc;
DROP PROCEDURE getUpdateFailedProcExt;
DROP PROCEDURE prepareForMigration;
DROP PROCEDURE putFailedProc;
DROP PROCEDURE putFailedProcExt;
DROP PROCEDURE insertStartRequest;
DROP PROCEDURE insertMoverCloseRequest;
DROP PROCEDURE insertSimpleRequest;

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
DROP INDEX I_DiskCopy_FS_GCW;
DROP INDEX I_DiskCopy_Status_7_FS;
CREATE INDEX I_DiskCopy_FS_DP_GCW ON DiskCopy (nvl(fileSystem,0)+nvl(dataPool,0), gcWeight);
CREATE INDEX I_DiskCopy_Status_7_FS_DP ON DiskCopy (decode(status,7,status,NULL), nvl(fileSystem,0)+nvl(dataPool,0));
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


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN recompileAll(); END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = SYSTIMESTAMP, state = 'COMPLETE'
 WHERE release = '2_1_15_0';
COMMIT;
