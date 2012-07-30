/******************************************************************************
 *                 stager_2.1.12-4_to_2.1.13-0.sql
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
 * This script upgrades a CASTOR v2.1.12-4 STAGER database to v2.1.13-0
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
   WHERE schemaVersion = '2_1_13_0'
     AND release = '2_1_13_0'
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
     AND (release LIKE '2_1_12_4%');
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

/* check the nameserver version here. We need at least 2.1.13-0 */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM UpgradeLog@remoteNs
   WHERE startDate = (SELECT MAX(startDate) FROM UpgradeLog@remoteNs)
     AND (release LIKE '2_1_13_%');
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'Nameserver release mismatch. Please upgrade the nameserver database before this one.');
END;
/

/* Starting the upgrade */
INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_13_0', '2_1_13_0', 'NON TRANSPARENT');
COMMIT;

/* Job management */
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs)
  LOOP
    -- Stop and drop all jobs
    dbms_scheduler.drop_job(a.job_name, force=>TRUE);
  END LOOP;
END;
/

/*****************/
/* logon trigger */
/*****************/

/* allows the call of new versions of remote procedures when the signature is matching */
CREATE OR REPLACE TRIGGER tr_RemoteDepSignature AFTER LOGON ON SCHEMA
BEGIN
  EXECUTE IMMEDIATE 'ALTER SESSION SET REMOTE_DEPENDENCIES_MODE=SIGNATURE';
END;
/

/* Schema changes go here */
/**************************/
DROP TABLE CleanupJobLog;
CREATE TABLE DLFLogs
  (timeinfo NUMBER,
   uuid VARCHAR2(2048),
   priority INTEGER CONSTRAINT NN_DLFLogs_Priority NOT NULL,
   msg VARCHAR2(2048) CONSTRAINT NN_DLFLogs_Msg NOT NULL,
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   source VARCHAR2(2048),
   params VARCHAR2(2048));
CREATE GLOBAL TEMPORARY TABLE DLFLogsHelper
  (timeinfo NUMBER,
   uuid VARCHAR2(2048),
   priority INTEGER,
   msg VARCHAR2(2048),
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   source VARCHAR2(2048),
   params VARCHAR2(2048))
ON COMMIT DELETE ROWS;

DROP PROCEDURE dumpCleanupLogs;
DROP TABLE Tape;
DROP TABLE Segment;
DROP TABLE RecallJob;
DROP TABLE PriorityMap;
DROP TABLE ProcessRepackAbortHelperDCrec;
DROP SEQUENCE TG_FILETRID_SEQ;


/* Definition of the RecallGroup table
 *   id : unique id of the RecallGroup
 *   name : the name of the RecallGroup
 *   nbDrives : maximum number of drives that may be concurrently used across all users of this RecallGroup
 *   minAmountDataForMount : the minimum amount of data needed to trigger a new mount, in bytes
 *   minNbFilesForMount : the minimum number of files needed to trigger a new mount
 *   maxFileAgeBeforeMount : the maximum file age before a tape in mounted, in seconds
 *   vdqmPriority : the priority that should be used for VDQM requests
 *   lastEditor : the login from which the tapepool was last modified
 *   lastEditionTime : the time at which the tapepool was last modified
 * Note that a mount is attempted as soon as one of the three criterias is reached.
 */
CREATE TABLE RecallGroup(id INTEGER CONSTRAINT PK_RecallGroup_Id PRIMARY KEY CONSTRAINT NN_RecallGroup_Id NOT NULL, 
                         name VARCHAR2(2048) CONSTRAINT NN_RecallGroup_Name NOT NULL
                                             CONSTRAINT UN_RecallGroup_Name UNIQUE USING INDEX,
                         nbDrives INTEGER CONSTRAINT NN_RecallGroup_NbDrives NOT NULL,
                         minAmountDataForMount INTEGER CONSTRAINT NN_RecallGroup_MinAmountData NOT NULL,
                         minNbFilesForMount INTEGER CONSTRAINT NN_RecallGroup_MinNbFiles NOT NULL,
                         maxFileAgeBeforeMount INTEGER CONSTRAINT NN_RecallGroup_MaxFileAge NOT NULL,
                         vdqmPriority INTEGER DEFAULT 0 CONSTRAINT NN_RecallGroup_VdqmPriority NOT NULL,
                         lastEditor VARCHAR2(2048) CONSTRAINT NN_RecallGroup_LastEditor NOT NULL,
                         lastEditionTime NUMBER CONSTRAINT NN_RecallGroup_LastEdTime NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

INSERT INTO RecallGroup (id, name, nbDrives, minAmountDataForMount, minNbFilesForMount, maxFileAgeBeforeMount, vdqmPriority, lastEditor, lastEditionTime)
VALUES (ids_seq.nextval, 'default', 20, 10*1024*1024*1024, 10, 30*3600, 0, '2.1.13 upgrade script', getTime());

/* Definition of the RecallUser table
 *   euid : uid of the recall user
 *   egid : gid of the recall user
 *   recallGroup : the recall group to which this user belongs
 *   lastEditor : the login from which the tapepool was last modified
 *   lastEditionTime : the time at which the tapepool was last modified
 * Note that a mount is attempted as soon as one of the three criterias is reached.
 */
CREATE TABLE RecallUser(euid INTEGER,
                        egid INTEGER CONSTRAINT NN_RecallUser_Egid NOT NULL,
                        recallGroup INTEGER CONSTRAINT NN_RecallUser_RecallGroup NOT NULL,
                        lastEditor VARCHAR2(2048) CONSTRAINT NN_RecallUser_LastEditor NOT NULL,
                        lastEditionTime NUMBER CONSTRAINT NN_RecallUser_LastEdTime NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
-- see comment in the RecallMount table about why we need this index
CREATE INDEX I_RecallUser_RecallGroup ON RecallUser(recallGroup); 
ALTER TABLE RecallUser ADD CONSTRAINT FK_RecallUser_RecallGroup FOREIGN KEY (recallGroup) REFERENCES RecallGroup(id);

/* Definition of the RecallMount table
 *   id : unique id of the RecallGroup
 *   mountTransactionId : the VDQM transaction that this mount is dealing with
 *   vid : the tape mounted or to be mounted
 *   label : the label of the mounted tape
 *   density : the density of the mounted tape
 *   recallGroup : the recall group to which this mount belongs
 *   startTime : the time at which this mount started
 *   status : current status of the RecallMount (NEW, WAITDRIVE or RECALLING)
 *   lastVDQMPingTime : last time we have checked VDQM for this mount
 *   lastProcessedFseq : last fseq that was processed by this mount (-1 if none)
 */
CREATE TABLE RecallMount(id INTEGER CONSTRAINT PK_RecallMount_Id PRIMARY KEY CONSTRAINT NN_RecallMount_Id NOT NULL, 
                         mountTransactionId INTEGER CONSTRAINT UN_RecallMount_TransId UNIQUE USING INDEX,
                         VID VARCHAR2(2048) CONSTRAINT NN_RecallMount_VID NOT NULL
                                            CONSTRAINT UN_RecallMount_VID UNIQUE USING INDEX,
                         label VARCHAR2(2048),
                         density VARCHAR2(2048),
                         recallGroup INTEGER CONSTRAINT NN_RecallMount_RecallGroup NOT NULL,
                         startTime NUMBER CONSTRAINT NN_RecallMount_startTime NOT NULL,
                         status INTEGER CONSTRAINT NN_RecallMount_Status NOT NULL,
                         lastVDQMPingTime NUMBER DEFAULT 0 CONSTRAINT NN_RecallMount_lastVDQMPing NOT NULL,
                         lastProcessedFseq INTEGER DEFAULT -1 CONSTRAINT NN_RecallMount_Fseq NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
-- this index may sound counter prodcutive as we have very few rows and a full table scan will always be faster
-- However, it is needed to avoid a table lock on RecallGroup when taking a row lock on RecallMount,
-- via the existing foreign key. On top, this table lock is also taken in case of an update that does not
-- touch any row while with the index, no row lock is taken at all, as one may expect
CREATE INDEX I_RecallMount_RecallGroup ON RecallMount(recallGroup); 
ALTER TABLE RecallMount ADD CONSTRAINT FK_RecallMount_RecallGroup FOREIGN KEY (recallGroup) REFERENCES RecallGroup(id);
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallMount', 'status', 0, 'RECALLMOUNT_NEW');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallMount', 'status', 1, 'RECALLMOUNT_WAITDRIVE');
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallMount', 'status', 2, 'RECALLMOUNT_RECALLING');

/* Definition of the RecallJob table
 * id unique identifer of this RecallJob
 * castorFile the file to be recalled
 * copyNb the copy number of the segment that this recalljob is targetting
 * recallGroup the recallGroup that triggered the recall
 * svcClass the service class used when triggering the recall. Will be used to place the file on disk
 * euid the user that triggered the recall
 * egid the group that triggered the recall
 * vid the tape on which the targetted segment resides
 * fseq the file sequence number of the targetted segment on its tape
 * status status of the recallJob
 * filesize size of the segment to be recalled
 * creationTime time when this job was created
 * nbRetriesWithinMount number of times we have tried to read the file within the current tape mount
 * nbMounts number of times we have mounted a tape for this RecallJob
 * blockId blockId of the file
 * fileTransactionId
 */
CREATE TABLE RecallJob(id INTEGER CONSTRAINT PK_RecallJob_Id PRIMARY KEY CONSTRAINT NN_RecallJob_Id NOT NULL, 
                       castorFile INTEGER CONSTRAINT NN_RecallJob_CastorFile NOT NULL,
                       copyNb INTEGER CONSTRAINT NN_RecallJob_CopyNb NOT NULL,
                       recallGroup INTEGER CONSTRAINT NN_RecallJob_RecallGroup NOT NULL,
                       svcClass INTEGER CONSTRAINT NN_RecallJob_SvcClass NOT NULL,
                       euid INTEGER CONSTRAINT NN_RecallJob_Euid NOT NULL,
                       egid INTEGER CONSTRAINT NN_RecallJob_Egid NOT NULL,
                       vid VARCHAR2(2048) CONSTRAINT NN_RecallJob_VID NOT NULL,
                       fseq INTEGER CONSTRAINT NN_RecallJob_Fseq NOT NULL,
                       status INTEGER CONSTRAINT NN_RecallJob_Status NOT NULL,
                       fileSize INTEGER CONSTRAINT NN_RecallJob_FileSize NOT NULL,
                       creationTime INTEGER CONSTRAINT NN_RecallJob_CreationTime NOT NULL,
                       nbRetriesWithinMount NUMBER DEFAULT 0 CONSTRAINT NN_RecallJob_nbRetriesWM NOT NULL,
                       nbMounts NUMBER DEFAULT 0 CONSTRAINT NN_RecallJob_nbMounts NOT NULL,
                       blockId RAW(4) CONSTRAINT NN_RecallJob_blockId NOT NULL,
                       fileTransactionId INTEGER CONSTRAINT UN_RecallJob_FileTrId UNIQUE USING INDEX)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

-- see comment in the RecallMount table about why we need the next 3 indices (although here,
-- the size of the table by itself is asking for one)
CREATE INDEX I_RecallJob_SvcClass ON RecallJob (svcClass);
CREATE INDEX I_RecallJob_RecallGroup ON RecallJob (recallGroup);
CREATE INDEX I_RecallJob_Castorfile_VID ON RecallJob (castorFile, VID);
CREATE INDEX I_RecallJob_VIDFseq ON RecallJob (VID, fseq);

ALTER TABLE RecallJob ADD CONSTRAINT FK_RecallJob_SvcClass FOREIGN KEY (svcClass) REFERENCES SvcClass(id);
ALTER TABLE RecallJob ADD CONSTRAINT FK_RecallJob_RecallGroup FOREIGN KEY (recallGroup) REFERENCES RecallGroup(id);
ALTER TABLE RecallJob ADD CONSTRAINT FK_RecallJob_CastorFile FOREIGN KEY (castorFile) REFERENCES CastorFile(id);

DELETE FROM ObjStatus WHERE object = 'RecallJob';

-- NEW status is when the RecallJob is created and no mount is foreseen yet
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 0, 'RECALLJOB_NEW');
-- PENDING status is when a RecallMount has been created that will handle the file for this RecallJob
-- Note that it may not be the tape of the RecallJob itself if another copy is being recalled
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 1, 'RECALLJOB_PENDING');
-- SELECTED status is when the file is currently being recalled. This can only happen for RecallJobs
-- for which the tape has been mounted. Other RecallJobs for the same file stay in PENDING
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 2, 'RECALLJOB_SELECTED');
-- RETRYMOUNT status is when the file recall has failed and should be retried after remounting the tape
-- These will be reset to NEW on RecallMount deletion
INSERT INTO ObjStatus (object, field, statusCode, statusName) VALUES ('RecallJob', 'status', 3, 'RECALLJOB_RETRYMOUNT');

ALTER TABLE MigrationMount RENAME COLUMN vdqmVolReqId TO mountTransactionId;
ALTER TABLE MigrationMount DROP COLUMN tapeGatewayRequestId;
ALTER TABLE MigrationJob RENAME COLUMN nbRetry TO nbRetries;
ALTER TABLE MigrationJob DROP COLUMN errorCode;
ALTER TABLE MigrationJob ADD (mountTransactionId INTEGER);
ALTER TABLE MigrationJob DROP COLUMN tapeGatewayRequestId;
ALTER TABLE MigrationJob ADD CONSTRAINT FK_MigrationJob_MigrationMount
  FOREIGN KEY (mountTransactionId) REFERENCES MigrationMount(mountTransactionId);
CREATE INDEX I_MigrationJob_MountTransId ON MigrationJob(mountTransactionId);
CREATE INDEX I_MigrationRouting_TapePool ON MigrationRouting(tapePool);

DELETE FROM ObjStatus WHERE object = 'MigrationJob' AND statusCode = 8;

INSERT INTO CastorConfig
  VALUES ('Migration', 'SizeThreshold', '300000000', 'The threshold to consider a file "small" or "large" when routing it to tape');
INSERT INTO CastorConfig
  VALUES ('Recall', 'MaxNbRetriesWithinMount', '2', 'The maximum number of retries for recalling a file within the same tape mount. When exceeded, the recall may still be retried in another mount. See Recall/MaxNbMount entry');
INSERT INTO CastorConfig
  VALUES ('Recall', 'MaxNbMounts', '2', 'The maximum number of mounts for recalling a given file. When exceeded, the recall will be failed if no other tapecopy can be used. See also Recall/MaxNbRetriesWithinMount entry');
INSERT INTO CastorConfig
  VALUES ('Migration', 'MaxNbMounts', '7', 'The maximum number of mounts for migrating a given file. When exceeded, the migration will be considered failed: the MigrationJob entry will be dropped and the corresponding diskcopy left in status CANBEMIGR. An operator intervention is required to resume the migration.');
DELETE FROM CastorConfig
 WHERE key = 'MaxNbConcurrentClients';

/* enforce explicit gcPolicies */
UPDATE SvcClass SET gcPolicy = 'default' WHERE gcPolicy IS NULL;
COMMIT;
ALTER TABLE SvcClass MODIFY (gcPolicy CONSTRAINT NN_SvcClass_GcPolicy NOT NULL);
ALTER TABLE SvcClass ADD CONSTRAINT FK_SvcClass_GCPolicy
  FOREIGN KEY (gcPolicy) REFERENCES GcPolicy (name);
CREATE INDEX I_SvcClass_GcPolicy ON SvcClass (gcPolicy);

/* new LRUpin GC policy */
INSERT INTO GcPolicy VALUES ('LRUpin',
                             'castorGC.creationTimeUserWeight',
                             'castorGC.creationTimeRecallWeight',
                             'castorGC.creationTimeCopyWeight',
                             'castorGC.LRUFirstAccessHook',
                             'castorGC.LRUAccessHook',
                             'castorGC.LRUpinUserSetGCWeight');

/* Temporary table used to bulk select next candidates for recall and migration */
CREATE GLOBAL TEMPORARY TABLE FilesToRecallHelper
 (fileId NUMBER, nsHost VARCHAR2(100), fileTransactionId NUMBER,
  filePath VARCHAR2(2048), blockId RAW(4), fSeq INTEGER)
 ON COMMIT DELETE ROWS;

/* Temporary table used to bulk select next candidates for migration */
CREATE GLOBAL TEMPORARY TABLE FilesToMigrateHelper
 (fileId NUMBER CONSTRAINT UN_FilesToMigrateHelper_fileId UNIQUE,
  nsHost VARCHAR2(100), lastKnownFileName VARCHAR2(2048), filePath VARCHAR2(2048),
  fileTransactionId NUMBER, fileSize NUMBER, fSeq INTEGER)
 ON COMMIT DELETE ROWS;
 
CREATE TABLE FileMigrationResultsHelper
 (reqId VARCHAR2(36), fileId NUMBER, lastModTime NUMBER, copyNo NUMBER, oldCopyNo NUMBER, transfSize NUMBER,
  comprSize NUMBER, vid VARCHAR2(6), fSeq NUMBER, blockId RAW(4), checksumType VARCHAR2(16), checksum NUMBER);


/* Drop obsoleted code */
DROP PROCEDURE bestFileSystemForSegment;
DROP PROCEDURE resurrectTapes;
DROP PROCEDURE tapesAndMountsForRecallPolicy;
DROP PROCEDURE tg_attachDriveReqToTape;
DROP PROCEDURE tg_deleteTapeRequest;
DROP PROCEDURE tg_findFromTGRequestId;
DROP PROCEDURE tg_findFromVDQMReqId;
DROP PROCEDURE tg_findVDQMReqFromTGReqId;
DROP PROCEDURE tg_getFailedRecalls;
DROP PROCEDURE tg_getSegmentInfo;
DROP PROCEDURE tg_invalidateFile;
DROP PROCEDURE tg_requestIdFromVDQMReqId;
DROP PROCEDURE tg_setRecRetryResult;
DROP PROCEDURE tg_dropSuperfluousSegment;
DROP PROCEDURE tg_setMigRetryResult;
DROP PROCEDURE tg_setFileStaleInMigration;
DROP PROCEDURE tg_defaultMigrSelPolicy;
DROP PROCEDURE tg_failFileTransfer;
DROP PROCEDURE tg_getRepackVIDAndFileInfo;
DROP PROCEDURE tg_getFailedMigrations;
DROP PROCEDURE tg_getFileToMigrate;
DROP PROCEDURE tg_getFileToRecall;
DROP PROCEDURE deletePriority;
DROP PROCEDURE enterPriority;
DROP PROCEDURE selectPriority;
DROP PROCEDURE cancelRecall;
DROP FUNCTION selectTapeForRecall;
DROP FUNCTION triggerRepackRecall;

-- inputForRecallPolicy should have been dropped in previous upgrades
-- but was forgotten in some cases
DECLARE
  unused VARCHAR2(2048);
BEGIN
  SELECT object_name INTO unused FROM USER_OBJECTS WHERE object_name = 'INPUTFORRECALLPOLICY';
  EXECUTE IMMEDIATE 'DROP PROCEDURE INPUTFORRECALLPOLICY';
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
/

/* Resurrect all subrequest that were waiting so that we trigger new recalls after we have */
/* dropped all recall related tables */
/* also cleanup all diskcopies in WAITTAPERECALL as they should no exist anymore */
DELETE FROM DiskCopy WHERE status = 2;
UPDATE SubRequest SET status = 1
 WHERE status IN (4, 5);
COMMIT;

/* Resurrect all selected migration jobs so that they get relinked to new mounts */
UPDATE MigrationJob
   SET status = 0, mountTransactionId = NULL, nbRetries = 0
 WHERE status = 1;
COMMIT;

/* Index to speed up the deleteOutOfDateStageOutDCs job */
CREATE INDEX I_DiskCopy_Status_Open ON DiskCopy (decode(status,6,status,decode(status,5,status,decode(status,11,status,NULL))));
CREATE INDEX I_DiskCopy_Status_6 ON DiskCopy (decode(status,6,status,NULL));

/* Missing index on a FK */
CREATE INDEX I_CastorFile_FileClass ON CastorFile(FileClass);

DROP INDEX I_DrainingDCs_PC;
/* For the in-order processing, see drainFileSystem */
CREATE INDEX I_DrainingDCs_FSStPrioTimeDC
  ON DrainingDiskCopy (fileSystem, status, priority DESC, creationTime ASC, diskCopy);


/* Custom type to handle float arrays */
CREATE OR REPLACE TYPE floatList IS TABLE OF NUMBER;
/

-- XXXXX Revalidation of all the PL/SQL code
-- XXXXX To be copy pasted rather than included

@oracleConstants.sql
@oracleCommon.sql
@oracleDebug.sql
@oracleDrain.sql
@oracleGC.sql
@oracleJob.sql
@oraclePerm.sql
@oracleQuery.sql
@oracleRH.sql
@oracleRepack.sql
@oracleStager.sql
@oracleTapeGateway.sql
@oracleMonitoring.sql

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
 WHERE release = '2_1_13_0';
COMMIT;
