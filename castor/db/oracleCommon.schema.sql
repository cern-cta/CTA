/*******************************************************************
 *
 * This file contains all schema definitions which are not generated automatically.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_13_0';

/* Get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
  epoch            TIMESTAMP WITH TIME ZONE;
  now              TIMESTAMP WITH TIME ZONE;
  interval         INTERVAL DAY(9) TO SECOND;
  interval_days    NUMBER;
  interval_hours   NUMBER;
  interval_minutes NUMBER;
  interval_seconds NUMBER;
BEGIN
  epoch := TO_TIMESTAMP_TZ('01-JAN-1970 00:00:00 00:00',
    'DD-MON-YYYY HH24:MI:SS TZH:TZM');
  now := SYSTIMESTAMP AT TIME ZONE '00:00';
  interval         := now - epoch;
  interval_days    := EXTRACT(DAY    FROM (interval));
  interval_hours   := EXTRACT(HOUR   FROM (interval));
  interval_minutes := EXTRACT(MINUTE FROM (interval));
  interval_seconds := EXTRACT(SECOND FROM (interval));

  RETURN interval_days * 24 * 60 * 60 + interval_hours * 60 * 60 +
    interval_minutes * 60 + interval_seconds;
END;
/

/* Sequence for indices */
CREATE SEQUENCE ids_seq CACHE 300;

/* SQL statements for requests status */
/* Partitioning enables faster response (more than indexing) for the most frequent queries - credits to Nilo Segura */
CREATE TABLE newRequests (type NUMBER(38) CONSTRAINT NN_NewRequests_Type NOT NULL, id NUMBER(38) CONSTRAINT NN_NewRequests_Id NOT NULL, creation DATE CONSTRAINT NN_NewRequests_Creation NOT NULL, CONSTRAINT PK_NewRequests_Type_Id PRIMARY KEY (type, id))
ORGANIZATION INDEX
COMPRESS
PARTITION BY LIST (type)
 (
  PARTITION type_16 VALUES (16)  TABLESPACE stager_data,
  PARTITION type_21 VALUES (21)  TABLESPACE stager_data,
  PARTITION type_33 VALUES (33)  TABLESPACE stager_data,
  PARTITION type_34 VALUES (34)  TABLESPACE stager_data,
  PARTITION type_35 VALUES (35)  TABLESPACE stager_data,
  PARTITION type_36 VALUES (36)  TABLESPACE stager_data,
  PARTITION type_37 VALUES (37)  TABLESPACE stager_data,
  PARTITION type_38 VALUES (38)  TABLESPACE stager_data,
  PARTITION type_39 VALUES (39)  TABLESPACE stager_data,
  PARTITION type_40 VALUES (40)  TABLESPACE stager_data,
  PARTITION type_41 VALUES (41)  TABLESPACE stager_data,
  PARTITION type_42 VALUES (42)  TABLESPACE stager_data,
  PARTITION type_43 VALUES (43)  TABLESPACE stager_data,
  PARTITION type_44 VALUES (44)  TABLESPACE stager_data,
  PARTITION type_45 VALUES (45)  TABLESPACE stager_data,
  PARTITION type_46 VALUES (46)  TABLESPACE stager_data,
  PARTITION type_48 VALUES (48)  TABLESPACE stager_data,
  PARTITION type_49 VALUES (49)  TABLESPACE stager_data,
  PARTITION type_50 VALUES (50)  TABLESPACE stager_data,
  PARTITION type_51 VALUES (51)  TABLESPACE stager_data,
  PARTITION type_60 VALUES (60)  TABLESPACE stager_data,
  PARTITION type_64 VALUES (64)  TABLESPACE stager_data,
  PARTITION type_65 VALUES (65)  TABLESPACE stager_data,
  PARTITION type_66 VALUES (66)  TABLESPACE stager_data,
  PARTITION type_67 VALUES (67)  TABLESPACE stager_data,
  PARTITION type_78 VALUES (78)  TABLESPACE stager_data,
  PARTITION type_79 VALUES (79)  TABLESPACE stager_data,
  PARTITION type_80 VALUES (80)  TABLESPACE stager_data,
  PARTITION type_84 VALUES (84)  TABLESPACE stager_data,
  PARTITION type_90 VALUES (90)  TABLESPACE stager_data,
  PARTITION type_142 VALUES (142)  TABLESPACE stager_data,
  PARTITION type_144 VALUES (144)  TABLESPACE stager_data,
  PARTITION type_147 VALUES (147)  TABLESPACE stager_data,
  PARTITION type_149 VALUES (149)  TABLESPACE stager_data,
  PARTITION notlisted VALUES (default) TABLESPACE stager_data
 );

/* SQL statements for type CastorFile */
CREATE TABLE CastorFile (fileId INTEGER, nsHost VARCHAR2(2048), fileSize INTEGER, creationTime INTEGER, lastAccessTime INTEGER, lastKnownFileName VARCHAR2(2048), lastUpdateTime INTEGER, id INTEGER CONSTRAINT PK_CastorFile_Id PRIMARY KEY, fileClass INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statement for table SubRequest */
CREATE TABLE SubRequest
  (retryCounter NUMBER, fileName VARCHAR2(2048), protocol VARCHAR2(2048),
   xsize INTEGER, priority NUMBER, subreqId VARCHAR2(2048), flags NUMBER,
   modeBits NUMBER, creationTime INTEGER CONSTRAINT NN_SubRequest_CreationTime 
   NOT NULL, lastModificationTime INTEGER, answered NUMBER, errorCode NUMBER, 
   errorMessage VARCHAR2(2048), id NUMBER CONSTRAINT NN_SubRequest_Id NOT NULL,
   diskcopy INTEGER, castorFile INTEGER, parent INTEGER, status INTEGER,
   request INTEGER, getNextStatus INTEGER, requestedFileSystems VARCHAR2(2048),
   svcHandler VARCHAR2(2048) CONSTRAINT NN_SubRequest_SvcHandler NOT NULL,
   reqType INTEGER CONSTRAINT NN_SubRequest_reqType NOT NULL
  )
  PCTFREE 50 PCTUSED 40 INITRANS 50
  ENABLE ROW MOVEMENT
  PARTITION BY LIST (STATUS)
   (
    PARTITION P_STATUS_0_1_2 VALUES (0, 1, 2),      -- *START
    PARTITION P_STATUS_3     VALUES (3),
    PARTITION P_STATUS_4     VALUES (4),
    PARTITION P_STATUS_5     VALUES (5),
    PARTITION P_STATUS_6     VALUES (6),
    PARTITION P_STATUS_7     VALUES (7),
    PARTITION P_STATUS_8     VALUES (8),
    PARTITION P_STATUS_9_10  VALUES (9, 10),        -- FAILED_*
    PARTITION P_STATUS_11    VALUES (11),
    PARTITION P_STATUS_12    VALUES (12),
    PARTITION P_STATUS_13_14 VALUES (13, 14),       -- *SCHED
    PARTITION P_STATUS_OTHER VALUES (DEFAULT)
   );

/* SQL statements for constraints on the SubRequest table */
ALTER TABLE SubRequest
  ADD CONSTRAINT PK_SubRequest_Id PRIMARY KEY (ID);
CREATE INDEX I_SubRequest_RT_CT_ID ON SubRequest(svcHandler, creationTime, id) LOCAL
 (PARTITION P_STATUS_0_1_2,
  PARTITION P_STATUS_3,
  PARTITION P_STATUS_4,
  PARTITION P_STATUS_5,
  PARTITION P_STATUS_6,
  PARTITION P_STATUS_7,
  PARTITION P_STATUS_8,
  PARTITION P_STATUS_9_10,
  PARTITION P_STATUS_11,
  PARTITION P_STATUS_12,
  PARTITION P_STATUS_13_14,
  PARTITION P_STATUS_OTHER);

/* this index is dedicated to archivesubreq */
CREATE INDEX I_SubRequest_Req_Stat_no89 ON SubRequest (request, decode(status,8,NULL,9,NULL,status));

CREATE INDEX I_SubRequest_Castorfile ON SubRequest (castorFile);
CREATE INDEX I_SubRequest_DiskCopy ON SubRequest (diskCopy);
CREATE INDEX I_SubRequest_Request ON SubRequest (request);
CREATE INDEX I_SubRequest_Parent ON SubRequest (parent);
CREATE INDEX I_SubRequest_SubReqId ON SubRequest (subReqId);
CREATE INDEX I_SubRequest_LastModTime ON SubRequest (lastModificationTime) LOCAL;

BEGIN
  setObjStatusName('SubRequest', 'status', 0, 'SUBREQUEST_START');
  setObjStatusName('SubRequest', 'status', 1, 'SUBREQUEST_RESTART');
  setObjStatusName('SubRequest', 'status', 2, 'SUBREQUEST_RETRY');
  setObjStatusName('SubRequest', 'status', 3, 'SUBREQUEST_WAITSCHED');
  setObjStatusName('SubRequest', 'status', 4, 'SUBREQUEST_WAITTAPERECALL');
  setObjStatusName('SubRequest', 'status', 5, 'SUBREQUEST_WAITSUBREQ');
  setObjStatusName('SubRequest', 'status', 6, 'SUBREQUEST_READY');
  setObjStatusName('SubRequest', 'status', 7, 'SUBREQUEST_FAILED');
  setObjStatusName('SubRequest', 'status', 8, 'SUBREQUEST_FINISHED');
  setObjStatusName('SubRequest', 'status', 9, 'SUBREQUEST_FAILED_FINISHED');
  setObjStatusName('SubRequest', 'status', 11, 'SUBREQUEST_ARCHIVED');
  setObjStatusName('SubRequest', 'status', 12, 'SUBREQUEST_REPACK');
  setObjStatusName('SubRequest', 'status', 13, 'SUBREQUEST_READYFORSCHED');
  setObjStatusName('SubRequest', 'getNextStatus', 0, 'GETNEXTSTATUS_NOTAPPLICABLE');
  setObjStatusName('SubRequest', 'getNextStatus', 1, 'GETNEXTSTATUS_FILESTAGED');
  setObjStatusName('SubRequest', 'getNextStatus', 2, 'GETNEXTSTATUS_NOTIFIED');
END;
/


/************************************/
/* Garbage collection related table */
/************************************/

/* A table storing the Gc policies and detailing there configuration
 * For each policy, identified by a name, parameters are :
 *   - userWeight : the name of the PL/SQL function to be called to
 *     precompute the GC weight when a file is written by the user.
 *   - recallWeight : the name of the PL/SQL function to be called to
 *     precompute the GC weight when a file is recalled
 *   - copyWeight : the name of the PL/SQL function to be called to
 *     precompute the GC weight when a file is disk to disk copied
 *   - firstAccessHook : the name of the PL/SQL function to be called
 *     when the file is accessed for the first time. Can be NULL.
 *   - accessHook : the name of the PL/SQL function to be called
 *     when the file is accessed (except for the first time). Can be NULL.
 *   - userSetGCWeight : the name of the PL/SQL function to be called
 *     when a setFileGcWeight user request is processed can be NULL.
 * All functions return a number that is the new gcWeight.
 * In general, here are the signatures :
 *   userWeight(fileSize NUMBER, DiskCopyStatus NUMBER)
 *   recallWeight(fileSize NUMBER)
 *   copyWeight(fileSize NUMBER, DiskCopyStatus NUMBER, sourceWeight NUMBER))
 *   firstAccessHook(oldGcWeight NUMBER, creationTime NUMBER)
 *   accessHook(oldGcWeight NUMBER, creationTime NUMBER, nbAccesses NUMBER)
 *   userSetGCWeight(oldGcWeight NUMBER, userDelta NUMBER)
 * Few notes :
 *   diskCopyStatus can be STAGED(0) or CANBEMIGR(10)
 */
CREATE TABLE GcPolicy (name VARCHAR2(2048) CONSTRAINT NN_GcPolicy_Name NOT NULL CONSTRAINT PK_GcPolicy_Name PRIMARY KEY,
                       userWeight VARCHAR2(2048) CONSTRAINT NN_GcPolicy_UserWeight NOT NULL,
                       recallWeight VARCHAR2(2048) CONSTRAINT NN_GcPolicy_RecallWeight NOT NULL,
                       copyWeight VARCHAR2(2048) CONSTRAINT NN_GcPolicy_CopyWeight NOT NULL,
                       firstAccessHook VARCHAR2(2048) DEFAULT NULL,
                       accessHook VARCHAR2(2048) DEFAULT NULL,
                       userSetGCWeight VARCHAR2(2048) DEFAULT NULL);

/* Default policy, mainly based on file sizes */
INSERT INTO GcPolicy VALUES ('default',
                             'castorGC.sizeRelatedUserWeight',
                             'castorGC.sizeRelatedRecallWeight',
                             'castorGC.sizeRelatedCopyWeight',
                             'castorGC.dayBonusFirstAccessHook',
                             'castorGC.halfHourBonusAccessHook',
                             'castorGC.cappedUserSetGCWeight');
INSERT INTO GcPolicy VALUES ('FIFO',
                             'castorGC.creationTimeUserWeight',
                             'castorGC.creationTimeRecallWeight',
                             'castorGC.creationTimeCopyWeight',
                             NULL,
                             NULL,
                             NULL);
INSERT INTO GcPolicy VALUES ('LRU',
                             'castorGC.creationTimeUserWeight',
                             'castorGC.creationTimeRecallWeight',
                             'castorGC.creationTimeCopyWeight',
                             'castorGC.LRUFirstAccessHook',
                             'castorGC.LRUAccessHook',
                             NULL);
INSERT INTO GcPolicy VALUES ('LRUpin',
                             'castorGC.creationTimeUserWeight',
                             'castorGC.creationTimeRecallWeight',
                             'castorGC.creationTimeCopyWeight',
                             'castorGC.LRUFirstAccessHook',
                             'castorGC.LRUAccessHook',
                             'castorGC.LRUpinUserSetGCWeight');

/**********************************/
/* Recall/Migration related table */
/**********************************/

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

/* Insert the bare minimum to get a working recall:
 * create the default recall group to have a default recall mount traffic shaping.
 */
INSERT INTO RecallGroup (id, name, nbDrives, minAmountDataForMount, minNbFilesForMount,
                         maxFileAgeBeforeMount, vdqmPriority, lastEditor, lastEditionTime)
  VALUES (ids_seq.nextval, 'default', 20, 10*1024*1024*1024, 10, 30*3600, 0, 'Castor 2.1.13 or above installation script', getTime());


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
-- this index may sound counter productive as we have very few rows and a full table scan will always be faster
-- However, it is needed to avoid a table lock on RecallGroup when taking a row lock on RecallMount,
-- via the existing foreign key. On top, this table lock is also taken in case of an update that does not
-- touch any row while with the index, no row lock is taken at all, as one may expect
CREATE INDEX I_RecallMount_RecallGroup ON RecallMount(recallGroup); 
ALTER TABLE RecallMount ADD CONSTRAINT FK_RecallMount_RecallGroup FOREIGN KEY (recallGroup) REFERENCES RecallGroup(id);
BEGIN
  setObjStatusName('RecallMount', 'status', 0, 'RECALLMOUNT_NEW');
  setObjStatusName('RecallMount', 'status', 1, 'RECALLMOUNT_WAITDRIVE');
  setObjStatusName('RecallMount', 'status', 2, 'RECALLMOUNT_RECALLING');
END;
/

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

BEGIN
  -- PENDING status is when a RecallJob is created
  -- It is immediately candidate for being recalled by an ongoing recallMount
  setObjStatusName('RecallJob', 'status', 1, 'RECALLJOB_PENDING');
  -- SELECTED status is when the file is currently being recalled.
  -- Note all recallJobs of a given file will have this state while the file is being recalled,
  -- even if another copy is being recalled. The recallJob that is effectively used can be identified
  -- by its non NULL fileTransactionId
  setObjStatusName('RecallJob', 'status', 2, 'RECALLJOB_SELECTED');
  -- RETRYMOUNT status is when the file recall has failed and should be retried after remounting the tape
  -- These will be reset to PENDING on RecallMount deletion
  setObjStatusName('RecallJob', 'status', 3, 'RECALLJOB_RETRYMOUNT');
END;
/

/* Definition of the TapePool table
 *   name : the name of the TapePool
 *   nbDrives : maximum number of drives that may be concurrently used across all users of this TapePool
 *   minAmountDataForMount : the minimum amount of data needed to trigger a new mount, in bytes
 *   minNbFilesForMount : the minimum number of files needed to trigger a new mount
 *   maxFileAgeBeforeMount : the maximum file age before a tape in mounted, in seconds
 *   lastEditor : the login from which the tapepool was last modified
 *   lastEditionTime : the time at which the tapepool was last modified
 * Note that a mount is attempted as soon as one of the three criterias is reached.
 */
CREATE TABLE TapePool (name VARCHAR2(2048) CONSTRAINT NN_TapePool_Name NOT NULL,
                       nbDrives INTEGER CONSTRAINT NN_TapePool_NbDrives NOT NULL,
                       minAmountDataForMount INTEGER CONSTRAINT NN_TapePool_MinAmountData NOT NULL,
                       minNbFilesForMount INTEGER CONSTRAINT NN_TapePool_MinNbFiles NOT NULL,
                       maxFileAgeBeforeMount INTEGER CONSTRAINT NN_TapePool_MaxFileAge NOT NULL,
                       lastEditor VARCHAR2(2048) CONSTRAINT NN_TapePool_LastEditor NOT NULL,
                       lastEditionTime NUMBER CONSTRAINT NN_TapePool_LastEdTime NOT NULL,
                       id INTEGER CONSTRAINT PK_TapePool_Id PRIMARY KEY CONSTRAINT NN_TapePool_Id NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* Definition of the MigrationMount table
 *   mountTransactionId : the unique identifier of the mount transaction
 *   tapeGatewayRequestId : 
 *   VID : tape currently mounted (when applicable)
 *   label : label (i.e. format) of the currently mounted tape (when applicable)
 *   density : density of the currently mounted tape (when applicable)
 *   lastFseq : position of the last file written on the tape
 *   lastVDQMPingTime : last time we've pinged VDQM
 *   tapePool : tapepool used by this migration
 *   status : current status of the migration
 */
CREATE TABLE MigrationMount (mountTransactionId INTEGER CONSTRAINT UN_MigrationMount_VDQM UNIQUE USING INDEX,
                             id INTEGER CONSTRAINT PK_MigrationMount_Id PRIMARY KEY
                                        CONSTRAINT NN_MigrationMount_Id NOT NULL,
                             startTime NUMBER CONSTRAINT NN_MigrationMount_startTime NOT NULL,
                             VID VARCHAR2(2048) CONSTRAINT UN_MigrationMount_VID UNIQUE USING INDEX,
                             label VARCHAR2(2048),
                             density VARCHAR2(2048),
                             lastFseq INTEGER,
                             full INTEGER,
                             lastVDQMPingTime NUMBER CONSTRAINT NN_MigrationMount_lastVDQMPing NOT NULL,
                             tapePool INTEGER CONSTRAINT NN_MigrationMount_TapePool NOT NULL,
                             status INTEGER CONSTRAINT NN_MigrationMount_Status NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE INDEX I_MigrationMount_TapePool ON MigrationMount(tapePool); 
ALTER TABLE MigrationMount ADD CONSTRAINT FK_MigrationMount_TapePool
  FOREIGN KEY (tapePool) REFERENCES TapePool(id);
BEGIN
  setObjStatusName('MigrationMount', 'status', 0, 'MIGRATIONMOUNT_WAITTAPE');
  setObjStatusName('MigrationMount', 'status', 1, 'MIGRATIONMOUNT_SEND_TO_VDQM');
  setObjStatusName('MigrationMount', 'status', 2, 'MIGRATIONMOUNT_WAITDRIVE');
  setObjStatusName('MigrationMount', 'status', 3, 'MIGRATIONMOUNT_MIGRATING');
END;
/

/* Definition of the MigratedSegment table
 * This table lists segments existing on tape for the files being
 * migrating. This allows to avoid putting two copies of a given
 * file on the same tape.
 *   castorFile : the file concerned
 *   copyNb : the copy number of this segment
 *   VID : the tape on which this segment resides
 */
CREATE TABLE MigratedSegment(castorFile INTEGER CONSTRAINT NN_MigratedSegment_CastorFile NOT NULL,
                             copyNb INTEGER CONSTRAINT NN_MigratedSegment_CopyNb NOT NULL,
                             VID VARCHAR2(2048) CONSTRAINT NN_MigratedSegment_VID NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE UNIQUE INDEX I_MigratedSegment_CFCopyNbVID ON MigratedSegment(CastorFile, copyNb, VID);
ALTER TABLE MigratedSegment ADD CONSTRAINT FK_MigratedSegment_CastorFile
  FOREIGN KEY (castorFile) REFERENCES CastorFile(id);

/* Definition of the MigrationJob table
 *   fileSize : size of the file to be migrated, in bytes
 *   VID : tape on which the file is being migrated (when applicable)
 *   creationTime : time of creation of this MigrationJob, in seconds since the epoch.
 *                  In case the MigrationJob went through a "WAITINGONRECALL" status,
 *                  time when it (re)entered the "PENDING" state
 *   castorFile : the file to migrate
 *   originalVID :  in case of repack, the VID of the tape where the original copy is leaving
 *   originalCopyNb : in case of repack, the number of the original copy being replaced
 *   destCopyNb : the number of the new copy of the file to migrate to tape
 *   tapePool : the tape pool where to migrate
 *   nbRetry : the number of retries we already went through
 *   errorcode : the error we got on last try (if any)
 *   mountTransactionId : an identifier for the migration session that is handling this job (when applicable)
 *   fileTransactionId : an identifier for this migration job
 *   fSeq : the file sequence of the copy created on tape for this job (when applicable)
 *   status : the status of the migration job
 */
CREATE TABLE MigrationJob (fileSize INTEGER CONSTRAINT NN_MigrationJob_FileSize NOT NULL,
                           VID VARCHAR2(2048),
                           creationTime NUMBER CONSTRAINT NN_MigrationJob_CreationTime NOT NULL,
                           castorFile INTEGER CONSTRAINT NN_MigrationJob_CastorFile NOT NULL,
                           originalVID VARCHAR2(20),
                           originalCopyNb INTEGER,
                           destCopyNb INTEGER CONSTRAINT NN_MigrationJob_destcopyNb NOT NULL,
                           tapePool INTEGER CONSTRAINT NN_MigrationJob_TapePool NOT NULL,
                           nbRetries INTEGER DEFAULT 0 CONSTRAINT NN_MigrationJob_nbRetries NOT NULL,
                           mountTransactionId INTEGER,   -- this is NULL at the beginning
                           fileTransactionId INTEGER CONSTRAINT UN_MigrationJob_FileTrId UNIQUE USING INDEX,
                           fSeq INTEGER,
                           status INTEGER CONSTRAINT NN_MigrationJob_Status NOT NULL,
                           id INTEGER CONSTRAINT PK_MigrationJob_Id PRIMARY KEY 
                                      CONSTRAINT NN_MigrationJob_Id NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
-- see comment in the RecallMount table about why we need this index
CREATE INDEX I_MigrationJob_MountTransId ON MigrationJob(mountTransactionId);
CREATE INDEX I_MigrationJob_CFVID ON MigrationJob(castorFile, VID);
CREATE INDEX I_MigrationJob_TapePoolSize ON MigrationJob(tapePool, fileSize);
CREATE UNIQUE INDEX I_MigrationJob_TPStatusId ON MigrationJob(tapePool, status, id);
CREATE UNIQUE INDEX I_MigrationJob_CFCopyNb ON MigrationJob(castorFile, destCopyNb);
ALTER TABLE MigrationJob ADD CONSTRAINT UN_MigrationJob_CopyNb
  UNIQUE (castorFile, destCopyNb) USING INDEX I_MigrationJob_CFCopyNb;
ALTER TABLE MigrationJob ADD CONSTRAINT FK_MigrationJob_CastorFile
  FOREIGN KEY (castorFile) REFERENCES CastorFile(id);
ALTER TABLE MigrationJob ADD CONSTRAINT FK_MigrationJob_TapePool
  FOREIGN KEY (tapePool) REFERENCES TapePool(id);
ALTER TABLE MigrationJob ADD CONSTRAINT FK_MigrationJob_MigrationMount
  FOREIGN KEY (mountTransactionId) REFERENCES MigrationMount(mountTransactionId);
BEGIN
  setObjStatusName('MigrationJob', 'status', 0, 'MIGRATIONJOB_PENDING');
  setObjStatusName('MigrationJob', 'status', 1, 'MIGRATIONJOB_SELECTED');
  setObjStatusName('MigrationJob', 'status', 3, 'MIGRATIONJOB_WAITINGONRECALL');
END;
/

/* Definition of the MigrationRouting table. Each line is a routing rule for migration jobs
 *   isSmallFile : whether this routing rule applies to small files. Null means it applies to all files
 *   copyNb : the copy number the routing rule applies to
 *   fileClass : the file class the routing rule applies to
 *   lastEditor : name of the last one that modified this routing rule.
 *   lastEditionTime : last time this routing rule was edited, in seconds since the epoch
 *   tapePool : the tape pool where to migrate files matching the above criteria
 */
CREATE TABLE MigrationRouting (isSmallFile INTEGER,
                               copyNb INTEGER CONSTRAINT NN_MigrationRouting_CopyNb NOT NULL,
                               fileClass INTEGER CONSTRAINT NN_MigrationRouting_FileClass NOT NULL,
                               lastEditor VARCHAR2(2048) CONSTRAINT NN_MigrationRouting_LastEditor NOT NULL,
                               lastEditionTime NUMBER CONSTRAINT NN_MigrationRouting_LastEdTime NOT NULL,
                               tapePool INTEGER CONSTRAINT NN_MigrationRouting_TapePool NOT NULL)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
-- see comment in the RecallMount table about why we need thess indexes
CREATE INDEX I_MigrationRouting_TapePool ON MigrationRouting(tapePool);
CREATE INDEX I_MigrationRouting_Rules ON MigrationRouting(fileClass, copyNb, isSmallFile);
ALTER TABLE MigrationRouting ADD CONSTRAINT UN_MigrationRouting_Rules
  UNIQUE (fileClass, copyNb, isSmallFile) USING INDEX I_MigrationRouting_Rules;
ALTER TABLE MigrationRouting ADD CONSTRAINT FK_MigrationRouting_FileClass
  FOREIGN KEY (fileClass) REFERENCES FileClass(id);
ALTER TABLE MigrationRouting ADD CONSTRAINT FK_MigrationRouting_TapePool
  FOREIGN KEY (tapePool) REFERENCES TapePool(id);

/* Temporary table used to bulk select next candidates for recall and migration */
CREATE GLOBAL TEMPORARY TABLE FilesToRecallHelper
 (fileId NUMBER, nsHost VARCHAR2(100), fileTransactionId NUMBER,
  filePath VARCHAR2(2048), blockId RAW(4), fSeq INTEGER)
 ON COMMIT DELETE ROWS;

CREATE GLOBAL TEMPORARY TABLE FilesToMigrateHelper
 (fileId NUMBER CONSTRAINT UN_FilesToMigrateHelper_fileId UNIQUE,
  nsHost VARCHAR2(100), lastKnownFileName VARCHAR2(2048), filePath VARCHAR2(2048),
  fileTransactionId NUMBER, fileSize NUMBER, fSeq INTEGER)
 ON COMMIT DELETE ROWS;

/* The following would be a temporary table, except that as it is used through a distributed
   transaction and Oracle does not support temporary tables in such context, it is defined as
   a normal table. See ns_setOrReplaceSegments for more details */
CREATE TABLE FileMigrationResultsHelper
 (reqId VARCHAR2(36), fileId NUMBER, lastModTime NUMBER, copyNo NUMBER, oldCopyNo NUMBER, transfSize NUMBER,
  comprSize NUMBER, vid VARCHAR2(6), fSeq NUMBER, blockId RAW(4), checksumType VARCHAR2(16), checksum NUMBER);

/* SQL statements for type DiskServer */
CREATE TABLE DiskServer (name VARCHAR2(2048), lastHeartbeatTime NUMBER, id INTEGER CONSTRAINT PK_DiskServer_Id PRIMARY KEY, status INTEGER, hwOnline INTEGER DEFAULT 0) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE UNIQUE INDEX I_DiskServer_name ON DiskServer (name);
ALTER TABLE DiskServer MODIFY
  (status CONSTRAINT NN_DiskServer_Status NOT NULL,
   name CONSTRAINT NN_DiskServer_Name NOT NULL,
   hwOnline CONSTRAINT NN_DiskServer_hwOnline NOT NULL);
ALTER TABLE DiskServer ADD CONSTRAINT UN_DiskServer_Name UNIQUE (name);

BEGIN
  setObjStatusName('DiskServer', 'status', 0, 'DISKSERVER_PRODUCTION');
  setObjStatusName('DiskServer', 'status', 1, 'DISKSERVER_DRAINING');
  setObjStatusName('DiskServer', 'status', 2, 'DISKSERVER_DISABLED');
  setObjStatusName('DiskServer', 'status', 3, 'DISKSERVER_READONLY');
END;
/

/* SQL statements for type FileSystem */
CREATE TABLE FileSystem (free INTEGER, mountPoint VARCHAR2(2048), minAllowedFreeSpace NUMBER, maxFreeSpace NUMBER, totalSize INTEGER, nbReadStreams NUMBER, nbWriteStreams NUMBER, nbMigratorStreams NUMBER, nbRecallerStreams NUMBER, id INTEGER CONSTRAINT PK_FileSystem_Id PRIMARY KEY, diskPool INTEGER, diskserver INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
ALTER TABLE FileSystem ADD CONSTRAINT FK_FileSystem_DiskServer 
  FOREIGN KEY (diskServer) REFERENCES DiskServer(id);
ALTER TABLE FileSystem MODIFY
  (status     CONSTRAINT NN_FileSystem_Status NOT NULL,
   diskServer CONSTRAINT NN_FileSystem_DiskServer NOT NULL,
   mountPoint CONSTRAINT NN_FileSystem_MountPoint NOT NULL);
ALTER TABLE FileSystem ADD CONSTRAINT UN_FileSystem_DSMountPoint
  UNIQUE (diskServer, mountPoint);
CREATE INDEX I_FileSystem_DiskPool ON FileSystem (diskPool);
CREATE INDEX I_FileSystem_DiskServer ON FileSystem (diskServer);

BEGIN
  setObjStatusName('FileSystem', 'status', 0, 'FILESYSTEM_PRODUCTION');
  setObjStatusName('FileSystem', 'status', 1, 'FILESYSTEM_DRAINING');
  setObjStatusName('FileSystem', 'status', 2, 'FILESYSTEM_DISABLED');
  setObjStatusName('FileSystem', 'status', 3, 'FILESYSTEM_READONLY');
END;
/

/* SQL statements for type DiskPool */
CREATE TABLE DiskPool (name VARCHAR2(2048), id INTEGER CONSTRAINT PK_DiskPool_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE TABLE DiskPool2SvcClass (Parent INTEGER, Child INTEGER) INITRANS 50 PCTFREE 50;
CREATE INDEX I_DiskPool2SvcClass_C on DiskPool2SvcClass (child);
CREATE INDEX I_DiskPool2SvcClass_P on DiskPool2SvcClass (parent);


CREATE UNIQUE INDEX I_CastorFile_FileIdNsHost ON CastorFile (fileId, nsHost);
CREATE UNIQUE INDEX I_CastorFile_LastKnownFileName ON CastorFile (lastKnownFileName);

/* SQL statements for type DiskCopy */
CREATE TABLE DiskCopy (path VARCHAR2(2048), gcWeight NUMBER, creationTime INTEGER, lastAccessTime INTEGER, diskCopySize INTEGER, nbCopyAccesses NUMBER, owneruid NUMBER, ownergid NUMBER, id INTEGER CONSTRAINT PK_DiskCopy_Id PRIMARY KEY, gcType INTEGER, fileSystem INTEGER, castorFile INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE INDEX I_DiskCopy_Castorfile ON DiskCopy (castorFile);
CREATE INDEX I_DiskCopy_FileSystem ON DiskCopy (fileSystem);
CREATE INDEX I_DiskCopy_FS_GCW ON DiskCopy (fileSystem, gcWeight);
-- for queries on active statuses
CREATE INDEX I_DiskCopy_Status_6 ON DiskCopy (decode(status,6,status,NULL));
CREATE INDEX I_DiskCopy_Status_7_FS ON DiskCopy (decode(status,7,status,NULL), fileSystem);
CREATE INDEX I_DiskCopy_Status_9 ON DiskCopy (decode(status,9,status,NULL));
-- to speed up deleteOutOfDateStageOutDCs
CREATE INDEX I_DiskCopy_Status_Open ON DiskCopy (decode(status,6,status,decode(status,5,status,decode(status,11,status,NULL))));

BEGIN
  setObjStatusName('DiskCopy', 'gcType', 0, 'GCTYPE_AUTO');
  setObjStatusName('DiskCopy', 'gcType', 1, 'GCTYPE_USER');
  setObjStatusName('DiskCopy', 'gcType', 2, 'GCTYPE_TOOMANYREPLICAS');
  setObjStatusName('DiskCopy', 'gcType', 3, 'GCTYPE_DRAINING');
  setObjStatusName('DiskCopy', 'gcType', 4, 'GCTYPE_NSSYNCH');
  setObjStatusName('DiskCopy', 'gcType', 5, 'GCTYPE_OVERWRITTEN');
  setObjStatusName('DiskCopy', 'status', 0, 'DISKCOPY_STAGED');
  setObjStatusName('DiskCopy', 'status', 1, 'DISKCOPY_WAITDISK2DISKCOPY');
  setObjStatusName('DiskCopy', 'status', 4, 'DISKCOPY_FAILED');
  setObjStatusName('DiskCopy', 'status', 5, 'DISKCOPY_WAITFS');
  setObjStatusName('DiskCopy', 'status', 6, 'DISKCOPY_STAGEOUT');
  setObjStatusName('DiskCopy', 'status', 7, 'DISKCOPY_INVALID');
  setObjStatusName('DiskCopy', 'status', 9, 'DISKCOPY_BEINGDELETED');
  setObjStatusName('DiskCopy', 'status', 10, 'DISKCOPY_CANBEMIGR');
  setObjStatusName('DiskCopy', 'status', 11, 'DISKCOPY_WAITFS_SCHEDULING');
END;
/

CREATE INDEX I_StagePTGRequest_ReqId ON StagePrepareToGetRequest (reqId);
CREATE INDEX I_StagePTPRequest_ReqId ON StagePrepareToPutRequest (reqId);
CREATE INDEX I_StagePTURequest_ReqId ON StagePrepareToUpdateRequest (reqId);
CREATE INDEX I_StageGetRequest_ReqId ON StageGetRequest (reqId);
CREATE INDEX I_StagePutRequest_ReqId ON StagePutRequest (reqId);

/* Improve query execution in the checkFailJobsWhenNoSpace function */
CREATE INDEX I_StagePutRequest_SvcClass ON StagePutRequest (svcClass);

/* Indexing GCFile by Request */
CREATE INDEX I_GCFile_Request ON GCFile (request);

/* An index to speed up queries in FileQueryRequest, FindRequestRequest, RequestQueryRequest */
CREATE INDEX I_QueryParameter_Query ON QueryParameter (query);

/* Constraint on FileClass name */
ALTER TABLE FileClass ADD CONSTRAINT UN_FileClass_Name UNIQUE (name);
ALTER TABLE FileClass MODIFY (classid CONSTRAINT NN_FileClass_Name NOT NULL);

/* Add unique constraint on svcClass name */
ALTER TABLE SvcClass ADD CONSTRAINT UN_SvcClass_Name UNIQUE (name);

/* Custom type to handle int arrays */
CREATE OR REPLACE TYPE "numList" IS TABLE OF INTEGER;
/

/* Custom type to handle float arrays */
CREATE OR REPLACE TYPE floatList IS TABLE OF NUMBER;
/

/* Custom type to handle strings returned by pipelined functions */
CREATE OR REPLACE TYPE strListTable AS TABLE OF VARCHAR2(2048);
/

/* SvcClass constraints */
ALTER TABLE SvcClass
  MODIFY (name CONSTRAINT NN_SvcClass_Name NOT NULL);

ALTER TABLE SvcClass 
  MODIFY (forcedFileClass CONSTRAINT NN_SvcClass_ForcedFileClass NOT NULL);

ALTER TABLE SvcClass MODIFY (gcPolicy CONSTRAINT NN_SvcClass_GcPolicy NOT NULL);
ALTER TABLE SvcClass MODIFY (gcPolicy DEFAULT 'default');
ALTER TABLE SvcClass ADD CONSTRAINT FK_SvcClass_GCPolicy
  FOREIGN KEY (gcPolicy) REFERENCES GcPolicy (name);
CREATE INDEX I_SvcClass_GcPolicy ON SvcClass (gcPolicy);

ALTER TABLE SvcClass MODIFY (lastEditor CONSTRAINT NN_SvcClass_LastEditor NOT NULL);

ALTER TABLE SvcClass MODIFY (lastEditionTime CONSTRAINT NN_SvcClass_LastEditionTime NOT NULL);

/* DiskCopy constraints */
ALTER TABLE DiskCopy MODIFY (nbCopyAccesses DEFAULT 0);

ALTER TABLE DiskCopy MODIFY (gcType DEFAULT NULL);

ALTER TABLE DiskCopy ADD CONSTRAINT FK_DiskCopy_CastorFile
  FOREIGN KEY (castorFile) REFERENCES CastorFile (id)
  INITIALLY DEFERRED DEFERRABLE;

ALTER TABLE DiskCopy
  MODIFY (status CONSTRAINT NN_DiskCopy_Status NOT NULL);

/* CastorFile constraints */
ALTER TABLE CastorFile ADD CONSTRAINT FK_CastorFile_FileClass
  FOREIGN KEY (fileClass) REFERENCES FileClass (id)
  INITIALLY DEFERRED DEFERRABLE;
CREATE INDEX I_CastorFile_FileClass ON CastorFile(FileClass);

ALTER TABLE CastorFile ADD CONSTRAINT UN_CastorFile_LKFileName UNIQUE (LastKnownFileName);

ALTER TABLE CastorFile MODIFY (LastKnownFileName CONSTRAINT NN_CastorFile_LKFileName NOT NULL);

/* DiskPool2SvcClass constraints */
ALTER TABLE DiskPool2SvcClass ADD CONSTRAINT PK_DiskPool2SvcClass_PC
  PRIMARY KEY (parent, child);

/* Global temporary table to handle output of the filesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE FilesDeletedProcOutput
  (fileId NUMBER, nsHost VARCHAR2(2048))
  ON COMMIT PRESERVE ROWS;

/* Global temporary table to store castor file ids temporarily in the filesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE FilesDeletedProcHelper
  (cfId NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to handle output of the nsFilesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE NsFilesDeletedOrphans
  (fileid NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to handle output of the stgFilesDeletedProc procedure */
CREATE GLOBAL TEMPORARY TABLE StgFilesDeletedOrphans
  (diskCopyId NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to handle output of the processBulkAbortForGet procedure */
CREATE GLOBAL TEMPORARY TABLE ProcessBulkAbortFileReqsHelper
  (srId NUMBER, cfId NUMBER, fileId NUMBER, nsHost VARCHAR2(2048), uuid VARCHAR(2048))
  ON COMMIT PRESERVE ROWS;
ALTER TABLE ProcessBulkAbortFileReqsHelper
  ADD CONSTRAINT PK_ProcessBulkAbortFileRe_SrId PRIMARY KEY (srId);

/* Global temporary table to handle output of the processBulkRequest procedure */
CREATE GLOBAL TEMPORARY TABLE ProcessBulkRequestHelper
  (fileId NUMBER, nsHost VARCHAR2(2048), errorCode NUMBER, errorMessage VARCHAR2(2048))
  ON COMMIT PRESERVE ROWS;

/* Global temporary table to handle bulk update of subrequests in processBulkAbortForRepack */
CREATE GLOBAL TEMPORARY TABLE ProcessRepackAbortHelperSR (srId NUMBER) ON COMMIT DELETE ROWS;
/* Global temporary table to handle bulk update of diskCopies in processBulkAbortForRepack */
CREATE GLOBAL TEMPORARY TABLE ProcessRepackAbortHelperDCmigr (cfId NUMBER) ON COMMIT DELETE ROWS;

/* Tables to log the DB activity */
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

/* Temporary table to handle removing of priviledges */
CREATE GLOBAL TEMPORARY TABLE RemovePrivilegeTmpTable
  (svcClass VARCHAR2(2048),
   euid NUMBER,
   egid NUMBER,
   reqType NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to store ids temporarily in the bulkCreateObj procedures */
CREATE GLOBAL TEMPORARY TABLE BulkSelectHelper
  (objId NUMBER)
  ON COMMIT DELETE ROWS;

/* Global temporary table to store the information on diskcopyies which need to
 * processed to see if too many replicas are online. This temporary table is
 * required to solve the error: `ORA-04091: table is mutating, trigger/function`
 */
CREATE GLOBAL TEMPORARY TABLE TooManyReplicasHelper
  (svcClass NUMBER, castorFile NUMBER)
  ON COMMIT DELETE ROWS;

ALTER TABLE TooManyReplicasHelper 
  ADD CONSTRAINT UN_TooManyReplicasHelp_SVC_CF UNIQUE (svcClass, castorFile);

/* Global temporary table to store subRequest and castorFile ids for cleanup operations.
   See the deleteTerminatedRequest procedure for more details.
 */
CREATE GLOBAL TEMPORARY TABLE DeleteTermReqHelper
  (srId NUMBER, cfId NUMBER)
  ON COMMIT PRESERVE ROWS;

/* Global temporary table to handle output of the processBulkRequest procedure */
CREATE GLOBAL TEMPORARY TABLE getFileIdsForSrsHelper (rowno NUMBER, fileId NUMBER, nsHost VARCHAR(2048))
  ON COMMIT DELETE ROWS;

/*
 * Black and while list mechanism
 * In order to be able to enter a request for a given service class, you need :
 *   - to be in the white list for this service class
 *   - to not be in the black list for this services class
 * Being in a list means :
 *   - either that your uid,gid is explicitely in the list
 *   - or that your gid is in the list with null uid (that is group wildcard)
 *   - or there is an entry with null uid and null gid (full wild card)
 * The permissions can also have a request type. Default is null, that is everything.
 * By default anybody can do anything
 */
CREATE TABLE WhiteList (svcClass VARCHAR2(2048), euid NUMBER, egid NUMBER, reqType NUMBER);
CREATE TABLE BlackList (svcClass VARCHAR2(2048), euid NUMBER, egid NUMBER, reqType NUMBER);

/* Create the AdminUsers table */
CREATE TABLE AdminUsers (euid NUMBER, egid NUMBER);
ALTER TABLE AdminUsers ADD CONSTRAINT UN_AdminUsers_euid_egid UNIQUE (euid, egid);
INSERT INTO AdminUsers VALUES (0, 0);   -- root/root, to be removed
INSERT INTO AdminUsers VALUES (-1, -1); -- internal requests

/* Prompt for stage:st account */
PROMPT Configuration of the admin part of the B/W list
UNDEF stageUid
ACCEPT stageUid NUMBER PROMPT 'Enter the stage user id: ';
UNDEF stageGid
ACCEPT stageGid NUMBER PROMPT 'Enter the st group id: ';
INSERT INTO AdminUsers VALUES (&stageUid, &stageGid);

/* Prompt for additional administrators */
PROMPT In order to define admins that will be exempt of B/W list checks,
PROMPT (e.g. c3 group at CERN), please give a space separated list of
PROMPT <userid>:<groupid> pairs. userid can be empty, meaning any user
PROMPT in the specified group.
UNDEF adminList
ACCEPT adminList CHAR PROMPT 'List of admins: ';
DECLARE
  adminUserId NUMBER;
  adminGroupId NUMBER;
  ind NUMBER;
  errmsg VARCHAR(2048);
BEGIN
  -- If the adminList is empty do nothing
  IF '&adminList' IS NULL THEN
    RETURN;
  END IF;
  -- Loop over the adminList
  FOR admin IN (SELECT column_value AS s
                  FROM TABLE(strTokenizer('&adminList',' '))) LOOP
    BEGIN
      ind := INSTR(admin.s, ':');
      IF ind = 0 THEN
        errMsg := 'Invalid <userid>:<groupid> ' || admin.s || ', ignoring';
        RAISE INVALID_NUMBER;
      END IF;
      errMsg := 'Invalid userid ' || SUBSTR(admin.s, 1, ind - 1) || ', ignoring';
      adminUserId := TO_NUMBER(SUBSTR(admin.s, 1, ind - 1));
      errMsg := 'Invalid groupid ' || SUBSTR(admin.s, ind) || ', ignoring';
      adminGroupId := TO_NUMBER(SUBSTR(admin.s, ind+1));
      INSERT INTO AdminUsers (euid, egid) VALUES (adminUserId, adminGroupId);
    EXCEPTION WHEN INVALID_NUMBER THEN
      dbms_output.put_line(errMsg);
    END;
  END LOOP;
END;
/

/* Define the service handlers for the appropriate sets of stage request objects */
UPDATE Type2Obj SET svcHandler = 'JobReqSvc' WHERE type IN (35, 40, 44);
UPDATE Type2Obj SET svcHandler = 'PrepReqSvc' WHERE type IN (36, 37, 38);
UPDATE Type2Obj SET svcHandler = 'StageReqSvc' WHERE type IN (39, 42, 95);
UPDATE Type2Obj SET svcHandler = 'QueryReqSvc' WHERE type IN (33, 34, 41, 103, 131, 152, 155, 195);
UPDATE Type2Obj SET svcHandler = 'JobSvc' WHERE type IN (60, 64, 65, 67, 78, 79, 80, 93, 144, 147);
UPDATE Type2Obj SET svcHandler = 'GCSvc' WHERE type IN (73, 74, 83, 142, 149);
UPDATE Type2Obj SET svcHandler = 'BulkStageReqSvc' WHERE type IN (50, 119);

/* SQL statements for type StageDiskCopyReplicaRequest */
CREATE TABLE StageDiskCopyReplicaRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, id INTEGER CONSTRAINT PK_StageDiskCopyReplicaRequ_Id PRIMARY KEY, svcClass INTEGER, client INTEGER, sourceSvcClass INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* Set default values for the StageDiskCopyReplicaRequest table */
ALTER TABLE StageDiskCopyReplicaRequest MODIFY flags DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY euid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY egid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY mask DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY pid DEFAULT 0;
ALTER TABLE StageDiskCopyReplicaRequest MODIFY machine DEFAULT 'stager';

/* Indexing StageDiskCopyReplicaRequest by source and destination diskcopy id */
CREATE INDEX I_StageDiskCopyReplic_SourceDC 
  ON StageDiskCopyReplicaRequest (sourceDiskCopy);
CREATE INDEX I_StageDiskCopyReplic_DestDC 
  ON StageDiskCopyReplicaRequest (destDiskCopy);

/* Define a table for some configuration key-value pairs and populate it */
CREATE TABLE CastorConfig
  (class VARCHAR2(2048) CONSTRAINT NN_CastorConfig_Class NOT NULL, 
   key VARCHAR2(2048) CONSTRAINT NN_CastorConfig_Key NOT NULL, 
   value VARCHAR2(2048) CONSTRAINT NN_CastorConfig_Value NOT NULL, 
   description VARCHAR2(2048));

ALTER TABLE CastorConfig ADD CONSTRAINT UN_CastorConfig_class_key UNIQUE (class, key);

/* Prompt for the value of the general/instance options */
UNDEF instanceName
ACCEPT instanceName DEFAULT castor_stager PROMPT 'Enter the name of the castor instance: (default: castor_stager, example: castoratlas) '
SET VER OFF

INSERT INTO CastorConfig
  VALUES ('general', 'instance', '&instanceName', 'Name of this Castor instance');

/* Prompt for the value of the stager/nsHost option */
UNDEF stagerNsHost
ACCEPT stagerNsHost DEFAULT undefined PROMPT 'Enter the name of the stager/nsHost: (default: undefined, example: castorns) '

INSERT INTO CastorConfig
  VALUES ('stager', 'nsHost', '&stagerNsHost', 'The name of the name server host to set in the CastorFile table overriding the CNS/HOST option defined in castor.conf');


INSERT INTO CastorConfig
  VALUES ('general', 'owner', sys_context('USERENV', 'CURRENT_USER'), 'The database owner of the schema');
INSERT INTO CastorConfig
  VALUES ('cleaning', 'terminatedRequestsTimeout', '120', 'Maximum timeout for successful and failed requests in hours');
INSERT INTO CastorConfig
  VALUES ('cleaning', 'outOfDateStageOutDCsTimeout', '72', 'Timeout for STAGEOUT diskCopies in hours');
INSERT INTO CastorConfig
  VALUES ('cleaning', 'failedDCsTimeout', '72', 'Timeout for failed diskCopies in hours');
INSERT INTO CastorConfig
  VALUES ('Repack', 'Protocol', 'rfio', 'The protocol that repack should use for writing files to disk');
INSERT INTO CastorConfig
  VALUES ('Recall', 'MaxNbRetriesWithinMount', '2', 'The maximum number of retries for recalling a file within the same tape mount. When exceeded, the recall may still be retried in another mount. See Recall/MaxNbMount entry');
INSERT INTO CastorConfig
  VALUES ('Recall', 'MaxNbMounts', '2', 'The maximum number of mounts for recalling a given file. When exceeded, the recall will be failed if no other tapecopy can be used. See also Recall/MaxNbRetriesWithinMount entry');
INSERT INTO CastorConfig
  VALUES ('Migration', 'SizeThreshold', '300000000', 'The threshold to consider a file "small" or "large" when routing it to tape');
INSERT INTO CastorConfig
  VALUES ('Migration', 'MaxNbMounts', '7', 'The maximum number of mounts for migrating a given file. When exceeded, the migration will be considered failed: the MigrationJob entry will be dropped and the corresponding diskcopy left in status CANBEMIGR. An operator intervention is required to resume the migration.');
INSERT INTO CastorConfig
  VALUES ('DiskServer', 'HeartbeatTimeout', '180', 'The maximum amount of time in seconds that a diskserver can spend without sending any hearbeat before it is automatically set to offline.');
COMMIT;


/*********************************************************************/
/* FileSystemsToCheck used to optimise the processing of filesystems */
/* when they change status                                           */
/*********************************************************************/
CREATE TABLE FileSystemsToCheck (FileSystem NUMBER CONSTRAINT PK_FSToCheck_FS PRIMARY KEY, ToBeChecked NUMBER);


/**************/
/* Accounting */
/**************/

/* WARNING!!!! Changing this to a materialized view which is refresh at a set
 * frequency causes problems with the disk server draining tools.
 */
CREATE TABLE Accounting (euid INTEGER CONSTRAINT NN_Accounting_Euid NOT NULL, 
                         fileSystem INTEGER CONSTRAINT NN_Accounting_Filesystem NOT NULL,
                         nbBytes INTEGER);
ALTER TABLE Accounting 
ADD CONSTRAINT PK_Accounting_EuidFs PRIMARY KEY (euid, fileSystem);

/* SQL statement for the creation of the AccountingSummary view */
CREATE OR REPLACE VIEW AccountingSummary
AS
  SELECT (SELECT cast(last_start_date AS DATE) 
            FROM dba_scheduler_jobs
           WHERE job_name = 'ACCOUNTINGJOB'
             AND owner = 
              (SELECT value FROM CastorConfig
                WHERE class = 'general' AND key = 'owner')) timestamp,
         3600 interval, SvcClass.name SvcClass, Accounting.euid, 
         sum(Accounting.nbbytes) totalBytes
    FROM Accounting, FileSystem, DiskPool2SvcClass, svcclass
   WHERE Accounting.filesystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = SvcClass.id
   GROUP BY SvcClass.name, Accounting.euid
   ORDER BY SvcClass.name, Accounting.euid;


/*********************/
/* FileSystem rating */
/*********************/

/* Computes a 'rate' for the filesystem which is an agglomeration
   of weight and fsDeviation. The goal is to be able to classify
   the fileSystems using a single value and to put an index on it */
CREATE OR REPLACE FUNCTION fileSystemRate
(nbReadStreams IN NUMBER,
 nbWriteStreams IN NUMBER)
RETURN NUMBER DETERMINISTIC IS
BEGIN
  RETURN - nbReadStreams - nbWriteStreams;
END;
/

/* FileSystem index based on the rate. */
CREATE INDEX I_FileSystem_Rate
    ON FileSystem(fileSystemRate(nbReadStreams, nbWriteStreams));


/************/
/* Aborting */
/************/

CREATE TABLE TransfersToAbort (uuid VARCHAR2(2048)
  CONSTRAINT NN_TransfersToAbort_Uuid NOT NULL);

/*******************************/
/* running job synchronization */
/*******************************/

CREATE GLOBAL TEMPORARY TABLE SyncRunningTransfersHelper(subreqId VARCHAR2(2048)) ON COMMIT DELETE ROWS;
CREATE GLOBAL TEMPORARY TABLE SyncRunningTransfersHelper2
(subreqId VARCHAR2(2048), reqId VARCHAR2(2048),
 fileid NUMBER, nsHost VARCHAR2(2048),
 errorCode NUMBER, errorMsg VARCHAR2(2048))
ON COMMIT PRESERVE ROWS;

/********************************/
/* DB link to the nameserver db */
/********************************/

PROMPT Configuration of the database link to the CASTOR name space
UNDEF cnsUser
ACCEPT cnsUser CHAR DEFAULT 'castor' PROMPT 'Enter the nameserver db username (default castor): ';
UNDEF cnsPasswd
ACCEPT cnsPasswd CHAR PROMPT 'Enter the nameserver db password: ';
UNDEF cnsDbName
ACCEPT cnsDbName CHAR PROMPT 'Enter the nameserver db TNS name: ';
CREATE DATABASE LINK remotens
  CONNECT TO &cnsUser IDENTIFIED BY &cnsPasswd USING '&cnsDbName';


/**********/
/* Repack */
/**********/

/* SQL statements for type StageRepackRequest (not autogenerated any more) */
CREATE TABLE StageRepackRequest (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, repackVid VARCHAR2(2048), id INTEGER CONSTRAINT PK_StageRepackRequest_Id PRIMARY KEY, svcClass INTEGER, client INTEGER, status INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

BEGIN
  setObjStatusName('StageRepackRequest', 'status', 0, 'REPACK_STARTING');
  setObjStatusName('StageRepackRequest', 'status', 1, 'REPACK_ONGOING');
  setObjStatusName('StageRepackRequest', 'status', 2, 'REPACK_FINISHED');
  setObjStatusName('StageRepackRequest', 'status', 3, 'REPACK_FAILED');
  setObjStatusName('StageRepackRequest', 'status', 4, 'REPACK_ABORTING');
  setObjStatusName('StageRepackRequest', 'status', 5, 'REPACK_ABORTED');
  setObjStatusName('StageRepackRequest', 'status', 6, 'REPACK_SUBMITTED');
END;
/
CREATE INDEX I_StageRepackRequest_ReqId ON StageRepackRequest (reqId);

/* Temporary table used for listing segments of a tape */
/* efficiently via DB link when repacking              */
CREATE GLOBAL TEMPORARY TABLE RepackTapeSegments
 (fileId NUMBER, blockid RAW(4), fseq NUMBER, segSize NUMBER,
  copyNb NUMBER, fileClass NUMBER, allSegments VARCHAR2(2048))
 ON COMMIT PRESERVE ROWS;


/*****************/
/* logon trigger */
/*****************/

/* allows the call of new versions of remote procedures when the signature is matching */
CREATE OR REPLACE TRIGGER tr_RemoteDepSignature AFTER LOGON ON SCHEMA
BEGIN
  EXECUTE IMMEDIATE 'ALTER SESSION SET REMOTE_DEPENDENCIES_MODE=SIGNATURE';
END;
/

