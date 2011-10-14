/******************************************************************************
 *                 stager_2.1.11-2_to_2.1.12-0.sql
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
 * This script upgrades a CASTOR v2.1.11-2 STAGER database to v2.1.12-0
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
     AND release = '2_1_12_0'
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
     AND release LIKE '2_1_11_6%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_12_0', '2_1_12_0', 'NON TRANSPARENT');
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

/* Schema changes go here */
/**************************/

-- refactoring of the tapepool table
ALTER TABLE TapePool RENAME CONSTRAINT PK_TapePool_Id TO PK_OldTapePool_Id;
ALTER INDEX PK_TapePool_Id RENAME TO PK_OldTapePool_Id;
CREATE TABLE newTapePool (name VARCHAR2(2048) CONSTRAINT NN_TapePool_Name NOT NULL,
                          nbDrives INTEGER CONSTRAINT NN_TapePool_NbDrives NOT NULL,
                          minAmountDataForMount INTEGER CONSTRAINT NN_TapePool_MinAmountData NOT NULL,
                          minNbFilesForMount INTEGER CONSTRAINT NN_TapePool_MinNbFiles NOT NULL,
                          maxFileAgeBeforeMount INTEGER CONSTRAINT NN_TapePool_MaxFileAge NOT NULL,
                          lastEditor VARCHAR2(2048) CONSTRAINT NN_TapePool_LastEditor NOT NULL,
                          lastEditionTime NUMBER CONSTRAINT NN_TapePool_LastEditionTime NOT NULL,
                          id INTEGER CONSTRAINT PK_TapePool_Id PRIMARY KEY CONSTRAINT NN_TapePool_Id NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
INSERT INTO newTapePool (SELECT TapePool.name, SUM(SvcClassToNbDrives.nbDrives), 300000000000, 1000, 14400, '2.1.12 upgrade script', getTime(), TapePool.id
                           FROM TapePool, SvcClass2TapePool,
                                (SELECT SvcClass.id, SvcClass.nbDrives/SUM(1) nbDrives
                                   FROM SvcClass, SvcClass2TapePool
                                  WHERE SvcClass.id = SvcClass2TapePool.parent
                                  GROUP BY SvcClass.id, SvcClass.nbDrives) SvcClassToNbDrives
                          WHERE SvcClassToNbDrives.id = SvcClass2TapePool.parent
                            AND SvcClass2TapePool.child = TapePool.id
                          GROUP BY TapePool.id, TapePool.name);
DROP TABLE TapePool CASCADE CONSTRAINTS PURGE;
ALTER TABLE newTapePool RENAME TO TapePool;

-- modification of the SubRequest Table
ALTER TABLE SubRequest ADD (reqType INTEGER DEFAULT 0 CONSTRAINT NN_SubRequest_reqType NOT NULL);
-- cleanup subrequests for which the type cannot be retrieved from Id2Type
BEGIN
  FOR sr IN (SELECT SubRequest.id
               FROM SubRequest, Id2Type
              WHERE subrequest.request = Id2Type.id(+)
                AND Id2Type.id IS NULL) LOOP
    DELETE FROM SubRequest WHERE id = sr.id;
  END LOOP;
  -- and the related Requests
  bulkDeleteRequests('StageGetRequest');
  bulkDeleteRequests('StagePutRequest');
  bulkDeleteRequests('StageUpdateRequest');
  bulkDeleteRequests('StagePrepareToGetRequest');
  bulkDeleteRequests('StagePrepareToPutRequest');
  bulkDeleteRequests('StagePrepareToUpdateRequest');
  bulkDeleteRequests('StagePutDoneRequest');
  bulkDeleteRequests('StageRmRequest');
  bulkDeleteRequests('StageRepackRequest');
  bulkDeleteRequests('StageDiskCopyReplicaRequest');
  bulkDeleteRequests('SetFileGCWeight');
END;
/
-- set the reqType column for all subrequests
UPDATE SubRequest SET reqType = (SELECT type FROM Id2Type WHERE id = request);

-- table and column drops
ALTER TABLE Tape DROP COLUMN Stream;
ALTER TABLE CastorFile DROP COLUMN SvcClass;
ALTER TABLE SvcClass DROP (nbDrives, migratorPolicy, streamPolicy);
DROP TABLE Stream2TapeCopy CASCADE CONSTRAINTS PURGE;
DROP TABLE TapeCopy CASCADE CONSTRAINTS PURGE;
DROP TABLE TapeGatewaySubRequest CASCADE CONSTRAINTS PURGE;
DROP TABLE Stream CASCADE CONSTRAINTS PURGE;
DROP TABLE FilesClearedProcHelper;
DROP TABLE JobFailedProcHelper;
DROP TABLE Id2Type;
DROP TABLE SvcClass2TapePool;
DELETE FROM ObjStatus WHERE object = 'TapeCopy';
DELETE FROM ObjStatus WHERE object = 'Stream';
DELETE FROM Type2Obj WHERE object = 'TapeCopy'; 
DELETE FROM Type2Obj WHERE object = 'Stream'; 
DELETE FROM Type2Obj WHERE object = 'TapeCopyForMigration'; 
DELETE FROM Type2Obj WHERE object = 'TapeGatewaySubRequest';
DELETE FROM CastorConfig WHERE class = 'tape' and key = 'interfaceDaemon';
DELETE FROM CastorConfig WHERE class = 'RmMaster' and key = 'NoLSFMode';
DELETE FROM Tape WHERE tpmode = 1;
DROP INDEX I_DiskCopy_FS_Status_10;

-- some temporary table changes
DROP TABLE ProcessBulkAbortFileReqsHelper;
CREATE GLOBAL TEMPORARY TABLE ProcessBulkAbortFileReqsHelper
   (srId NUMBER, cfId NUMBER, fileId NUMBER, nsHost VARCHAR2(2048), uuid VARCHAR(2048))
  ON COMMIT PRESERVE ROWS;
ALTER TABLE ProcessBulkAbortFileReqsHelper
  ADD CONSTRAINT PK_ProcessBulkAbortFileRe_SrId PRIMARY KEY (srId);

-- table and columns adds
ALTER TABLE SvcClass ADD (lastEditor VARCHAR2(2048) DEFAULT '2.1.12 upgrade script'
                                     CONSTRAINT NN_SvcClass_LastEditor NOT NULL,
                          lastEditionTime INTEGER DEFAULT 0
                                     CONSTRAINT NN_SvcClass_LastEditionTime NOT NULL);
UPDATE SvcClass SET lastEditionTime = getTime();

CREATE TABLE RecallJob(copyNb NUMBER,
                       errorCode NUMBER,
                       nbRetry NUMBER,
                       missingCopies NUMBER, 
                       fseq NUMBER,
                       tapeGatewayRequestId NUMBER,
                       vid VARCHAR2(2048), 
                       fileTransactionId NUMBER,
                       id INTEGER CONSTRAINT PK_RecallJob_Id PRIMARY KEY CONSTRAINT NN_RecallJob_Id NOT NULL, 
                       castorFile INTEGER,
                       status INTEGER) 
INITRANS 50 PCTUSED 40 PCTFREE 50 ENABLE ROW MOVEMENT
PARTITION BY LIST (STATUS) (
  PARTITION P_STATUS_0_1   VALUES (0, 1),
  PARTITION P_STATUS_OTHER VALUES (DEFAULT)
);
CREATE INDEX I_RecallJob_VID ON RecallJob(VID);
CREATE INDEX I_RecallJob_Castorfile ON RecallJob (castorFile) LOCAL;
CREATE INDEX I_RecallJob_Status ON RecallJob (status) LOCAL;
ALTER TABLE RecallJob ADD CONSTRAINT UN_RECALLJOB_FILETRID 
   UNIQUE (FileTransactionId) USING INDEX;
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 3, 'RECALLJOB_SELECTED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 4, 'RECALLJOB_TOBERECALLED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 5, 'RECALLJOB_STAGED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 6, 'RECALLJOB_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 8, 'RECALLJOB_RETRY');
INSERT INTO Type2Obj (type, object) VALUES (30, 'RecallJob');

/* Definition of the MigrationMount table
 *   lastFileSystemChange : time of the last change of filesystem used for this migration
 *   vdqmVolReqId : 
 *   tapeGatewayRequestId : 
 *   VID : tape currently mounted (when applicable)
 *   label : label (i.e. format) of the currently mounted tape (when applicable)
 *   density : density of the currently mounted tape (when applicable)
 *   lastFseq : position of the last file written on the tape
 *   lastVDQMPingTime : last time we've pinged VDQM
 *   lastFileSystemUsed : last filesystem from which data was migrated
 *   lastButOneFileSystemUsed : last but one filesystem from which data was migrated
 *   tapePool : tapepool used by this migration
 *   status : current status of the migration
 */
CREATE TABLE MigrationMount (lastFileSystemChange INTEGER CONSTRAINT NN_MigrationMount_LastFSChange NOT NULL,
                             vdqmVolReqId INTEGER,
                             tapeGatewayRequestId INTEGER,
                             id INTEGER CONSTRAINT PK_MigrationMount_Id PRIMARY KEY CONSTRAINT NN_MigrationMount_Id NOT NULL,
                             startTime NUMBER CONSTRAINT NN_MigrationMount_startTime NOT NULL,
                             VID VARCHAR2(2048),
                             label VARCHAR2(2048),
                             density VARCHAR2(2048),
                             lastFseq INTEGER,
                             lastVDQMPingTime NUMBER CONSTRAINT NN_MigrationMount_lastVDQMPing NOT NULL,
                             lastFileSystemUsed INTEGER,
                             lastButOneFileSystemUsed INTEGER,
                             tapePool INTEGER CONSTRAINT NN_MigrationMount_TapePool NOT NULL,
                             status INTEGER CONSTRAINT NN_MigrationMount_Status NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE INDEX I_MigrationMount_TapePool ON MigrationMount(tapePool); 
ALTER TABLE MigrationMount ADD CONSTRAINT UN_MigrationMount_VDQM UNIQUE (vdqmVolReqId) USING INDEX;
ALTER TABLE MigrationMount ADD CONSTRAINT UN_MigrationMount_VID UNIQUE (VID) USING INDEX;
ALTER TABLE MigrationMount ADD CONSTRAINT FK_MigrationMount_TapePool
   FOREIGN KEY (tapePool) REFERENCES TapePool(id);
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationMount', 'status', 0, 'MIGRATIONMOUNT_WAITTAPE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationMount', 'status', 1, 'MIGRATIONMOUNT_WAITDRIVE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationMount', 'status', 2, 'MIGRATIONMOUNT_MIGRATING');


/* Definition of the MigrationJob table
 *   fileSize : size of the file to be migrated, in bytes
 *   VID : tape on which the file is being migrated (when applicable)
 *   creationTime : time of creation of this MigrationJob, in seconds since the epoch
 *   castorFile : the file to migrate
 *   copyNb : the number of the copy of the file to migrate
 *   tapePool : the tape pool where to migrate
 *   nbRetry : the number of retries we already went through
 *   errorcode : the error we got on last try (if any)
 *   tapeGatewayRequestId : an identifier for the migration session that is handling this job (when applicable)
 *   fileTransactionId : an identifier for this migration job
 *   fSeq : the file sequence of the copy created on tape for this job (when applicable)
 *   status : the status of the migration job
 */
CREATE TABLE MigrationJob (fileSize INTEGER CONSTRAINT NN_MigrationJob_FileSize NOT NULL,
                           VID VARCHAR2(2048),
                           creationTime NUMBER CONSTRAINT NN_MigrationJob_CreationTime NOT NULL,
                           castorFile INTEGER CONSTRAINT NN_MigrationJob_CastorFile NOT NULL,
                           copyNb INTEGER CONSTRAINT NN_MigrationJob_copyNb NOT NULL,
                           tapePool INTEGER CONSTRAINT NN_MigrationJob_TapePool NOT NULL,
                           nbRetry INTEGER CONSTRAINT NN_MigrationJob_nbRetry NOT NULL,
                           errorcode INTEGER,
                           tapeGatewayRequestId INTEGER CONSTRAINT NN_MigrationJob_tgRequestId NOT NULL,
                           FileTransactionId INTEGER,
                           fSeq INTEGER,
                           status INTEGER CONSTRAINT NN_MigrationJob_Status NOT NULL,
                           id INTEGER CONSTRAINT PK_MigrationJob_Id PRIMARY KEY CONSTRAINT NN_MigrationJob_Id NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE INDEX I_MigrationJob_CFVID ON MigrationJob(CastorFile, VID);
CREATE INDEX I_MigrationJob_CFCopyNb ON MigrationJob(CastorFile, copyNb);
CREATE INDEX I_MigrationJob_TapePoolSize ON MigrationJob(tapePool, fileSize);
CREATE INDEX I_MigrationJob_TPStatusCFId ON MigrationJob(tapePool, status, castorFile, id);
ALTER TABLE MigrationJob ADD CONSTRAINT UN_MigrationMount_CopyNb UNIQUE (castorFile, copyNb) USING INDEX I_MigrationJob_CFCopyNb;
ALTER TABLE MigrationJob ADD CONSTRAINT FK_MigrationJob_CastorFile
   FOREIGN KEY (castorFile) REFERENCES CastorFile(id);
ALTER TABLE MigrationJob ADD CONSTRAINT FK_MigrationJob_TapePool
   FOREIGN KEY (tapePool) REFERENCES TapePool(id);
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationJob', 'status', 0, 'MIGRATIONJOB_PENDING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationJob', 'status', 1, 'MIGRATIONJOB_SELECTED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationJob', 'status', 2, 'MIGRATIONJOB_MIGRATED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationJob', 'status', 6, 'MIGRATIONJOB_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('MigrationJob', 'status', 8, 'MIGRATIONJOB_RETRY');


/* Definition of the MigrationRouting table. Each line is a routing rule for migration jobs
 *   isSmallFile : whether this routing rule applies to small files
 *   copyNb : the copy number the routing rule applies to
 *   svcClass : the service class the routing rule applies to
 *   fileClass : the file class the routing rule applies to
 *   lastEditor : name of the last one that modified this routing rule.
 *   lastEditionTime : last time this routing rule was edited, in seconds since the epoch
 *   tapePool : the tape pool where to migrate files matching the above criteria
 */
CREATE TABLE MigrationRouting (isSmallFile INTEGER CONSTRAINT NN_MigrationRouting_IsFile NOT NULL,
                               copyNb INTEGER CONSTRAINT NN_MigrationRouting_CopyNb NOT NULL,
                               svcClass INTEGER CONSTRAINT NN_MigrationRouting_SvcClass NOT NULL,
                               fileClass INTEGER CONSTRAINT NN_MigrationRouting_FileClass NOT NULL,
                               lastEditor VARCHAR2(2048) CONSTRAINT NN_MigrationRouting_LastEditor NOT NULL,
                               lastEditionTime NUMBER CONSTRAINT NN_MigrationRouting_LastEdit NOT NULL,
                               tapePool INTEGER CONSTRAINT NN_MigrationRouting_TapePool NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE INDEX I_MigrationRouting_Rules ON MigrationRouting(svcClass, fileClass, copyNb, isSmallFile);
ALTER TABLE MigrationRouting ADD CONSTRAINT UN_MigrationRouting_Rules UNIQUE (svcClass, fileClass, copyNb, isSmallFile) USING INDEX I_MigrationRouting_Rules;
ALTER TABLE MigrationRouting ADD CONSTRAINT FK_MigrationRouting_SvcClass
   FOREIGN KEY (svcClass) REFERENCES SvcClass(id);
ALTER TABLE MigrationRouting ADD CONSTRAINT FK_MigrationRouting_FileClass
   FOREIGN KEY (fileClass) REFERENCES FileClass(id);
ALTER TABLE MigrationRouting ADD CONSTRAINT FK_MigrationRouting_TapePool
   FOREIGN KEY (tapePool) REFERENCES TapePool(id);

-- New repack implementation
ALTER TABLE StageRepackRequest ADD (status INTEGER);
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 0, 'REPACK_STARTING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 1, 'REPACK_ONGOING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 2, 'REPACK_FINISHED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 3, 'REPACK_FAILED');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 4, 'REPACK_ABORTING');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('StageRepackRequest', 'status', 5, 'REPACK_ABORTED');
/* Global temporary table to handle bulk update of subrequests in processBulkAbortForRepack */
CREATE GLOBAL TEMPORARY TABLE ProcessRepackAbortHelperSR (srId NUMBER) ON COMMIT DELETE ROWS;
/* Global temporary table to handle bulk update of diskCopies in processBulkAbortForRepack */
CREATE GLOBAL TEMPORARY TABLE ProcessRepackAbortHelperDCrec (cfId NUMBER) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMPORARY TABLE ProcessRepackAbortHelperDCmigr (cfId NUMBER) ON COMMIT DELETE ROWS;

CREATE GLOBAL TEMPORARY TABLE RepackTapeSegments (s_fileId NUMBER, blockid RAW(4), fseq NUMBER, segSize NUMBER, copyno NUMBER, fileClass NUMBER) ON COMMIT PRESERVE ROWS;
 
UPDATE Type2Obj SET svcHandler = 'BulkStageReqSvc' WHERE type = 119;
INSERT INTO CastorConfig
VALUES ('Repack', 'Protocol', 'rfio', 'The protocol that repack should use for writing files to disk');
INSERT INTO CastorConfig
VALUES ('Repack', 'MaxNbConcurrentClients', '5', 'The maximum number of repacks clients that are able to run concurrently. This are either clients atrting repacks or aborting running repacks. Providing that each of them will take a DB core, this number should roughtly be ~60% of the number of cores of the stager DB server');
COMMIT;

-- cleanup old PL/SQL code
CREATE OR REPLACE PROCEDURE dropSafe(procname IN VARCHAR2, objtype IN VARCHAR2) AS
  unused VARCHAR2(2048);
BEGIN
  SELECT object_name INTO unused FROM USER_OBJECTS WHERE object_name = procName;
  EXECUTE IMMEDIATE 'DROP ' || objtype || ' ' || procName;
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
/
CREATE OR REPLACE PROCEDURE dropFunctionSafe(funcname IN VARCHAR2) AS
BEGIN
  dropSafe(funcname, 'FUNCTION');
END;
/
CREATE OR REPLACE PROCEDURE dropProcedureSafe(funcname IN VARCHAR2) AS
BEGIN
  dropSafe(funcname, 'PROCEDURE');
END;
/
BEGIN
  dropProcedureSafe('DELETETAPECOPIES');
  dropProcedureSafe('ANYTAPECOPYFORSTREAM');
  dropProcedureSafe('ATTACHTAPECOPIESTOSTREAMS');
  dropProcedureSafe('ATTACHTCGATEWAY');
  dropProcedureSafe('ATTACHTCRTCP');
  dropProcedureSafe('BESTTAPECOPYFORSTREAM');
  dropProcedureSafe('CANCELRECALLFORTAPE');
  dropProcedureSafe('CREATEORUPDATESTREAM');
  dropProcedureSafe('DELETEORSTOPSTREAM');
  dropProcedureSafe('DRAINDISKMIGRSELPOLICY');
  dropProcedureSafe('REPACKMIGRSELPOLICY');
  dropProcedureSafe('GETSCHEDULERJOBS');
  dropProcedureSafe('FILESCLEAREDPROC');
  dropProcedureSafe('FILERECALLED');
  dropProcedureSafe('FILERECALLFAILED');
  dropProcedureSafe('REMOVEALLFORREPACK');
  dropProcedureSafe('INVALIDATETAPECOPIES');
  dropProcedureSafe('RESURRECTCANDIDATES');
  dropProcedureSafe('STOPCHOSENSTREAMS');
  dropProcedureSafe('STARTCHOSENSTREAMS');
  dropProcedureSafe('STREAMSFORSTREAMPOLICY');
  dropProcedureSafe('INPUTFORSTREAMPOLICY');
  dropProcedureSafe('MIGHUNTERCLEANUP');
  dropProcedureSafe('RTCPCLIENTDCLEANUP');
  dropProcedureSafe('RESETSTREAM');
  dropProcedureSafe('STREAMSTODO');
  dropProcedureSafe('JOBTOSCHEDULE');
  dropProcedureSafe('JOBFAILED');
  dropProcedureSafe('TG_DELETESTREAM');
  dropProcedureSafe('DEFAULTMIGRSELPOLICY');
  dropProcedureSafe('INPUTFORMIGRATIONPOLICY');
  dropProcedureSafe('DELETEMIGRATIONTAPECOPIES');
  dropProcedureSafe('DELETERECALLTAPECOPIES');
  dropProcedureSafe('DELETESINGLETAPECOPY');
  dropProcedureSafe('STAGERELEASE');
  dropProcedureSafe('TG_DRAINDISKMIGRSELPOLICY');
  dropProcedureSafe('TG_REPACKMIGRSELPOLICY');
  dropProcedureSafe('TG_GETSTREAMSWITHOUTTAPES');
  dropProcedureSafe('UPDATEFSMIGRATOROPENED');
  dropProcedureSafe('UPDATEFSRECALLEROPENED');
  dropProcedureSafe('INSERTCHANGEPRIVILEGE');
  dropProcedureSafe('INSERTLISTPRIVILEGE');
  dropFunctionSafe('GETTCS');
  dropFunctionSafe('CHECKAVAILOFSCHEDULERRFS');
  dropFunctionSafe('CHECKFAILPUTWHENTAPE0');
END;
/
DROP PROCEDURE dropProcedureSafe;
DROP PROCEDURE dropFunctionSafe;
DROP PROCEDURE dropSafe;

/* Update and revalidation of PL-SQL code */
/******************************************/

@oracleConstants.sql
@oracleCommon.sql
@oraclePerm.sql
@oracleStager.sql
@oracleJob.sql
@oracleQuery.sql
@oracleTape.sql
@oracleTapeGateway.sql
@oracleGC.sql
@oracleDrain.sql
@oracleDebug.sql
@oracleMonitoring.sql
@oracleRH.sql


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM user_objects
             WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY')
               AND status = 'INVALID')
  LOOP
    IF a.object_type = 'PACKAGE BODY' THEN a.object_type := 'PACKAGE'; END IF;
    EXECUTE IMMEDIATE 'ALTER ' ||a.object_type||' '||a.object_name||' COMPILE';
  END LOOP;
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_12_0';
COMMIT;
