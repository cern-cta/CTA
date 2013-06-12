/***** TO BE MERGED INTO stager_2.1.13-6_to_2.1.14-0.sql *****/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_0', '2_1_14_X', 'NON TRANSPARENT');
COMMIT;

/* For deleteDiskCopy */
CREATE GLOBAL TEMPORARY TABLE DeleteDiskCopyHelper
  (dcId INTEGER CONSTRAINT PK_DDCHelper_dcId PRIMARY KEY, rc INTEGER)
  ON COMMIT PRESERVE ROWS;

--  - change the status of diskCopies to merge STAGED and CANBEMIGR
--  - add a tapeStatus and nsOpenTime to CastorFile

ALTER TABLE CastorFile ADD (tapeStatus INTEGER, nsOpenTime NUMBER);
DELETE FROM ObjStatus WHERE object='DiskCopy' AND field='status'
                        AND statusName IN ('DISKCOPY_STAGED', 'DISKCOPY_CANBEMIGR');
BEGIN setObjStatusName('DiskCopy', 'status', 0, 'DISKCOPY_VALID'); END;
/


ALTER TABLE MigrationJob ADD CONSTRAINT CK_MigrationJob_FS_Positive CHECK (fileSize > 0);


-- temporary function-based index to speed up the following update
CREATE INDEX I_CF_OpenTimeNull
    ON CastorFile(decode(nvl(nsOpenTime, -1), -1, 1, NULL));

DECLARE
  remCF INTEGER;
  CURSOR cfs IS
    SELECT id FROM CastorFile WHERE decode(nvl(nsOpenTime, -1), -1, 1, NULL) = 1;
  ids "numList";
BEGIN
  FOR cf IN (SELECT unique castorFile, status, nbCopies
               FROM DiskCopy, CastorFile, FileClass
              WHERE DiskCopy.castorFile = CastorFile.id
                AND CastorFile.fileClass = FileClass.id
                AND DiskCopy.status IN (0, 10)) LOOP
    UPDATE CastorFile
       SET tapeStatus = DECODE(cf.status,
                               10, dconst.CASTORFILE_NOTONTAPE,
                               DECODE(cf.nbCopies,
                                      0, dconst.CASTORFILE_DISKONLY,
                                      dconst.CASTORFILE_ONTAPE))
     WHERE id = cf.castorFile;
  END LOOP;
  COMMIT;
  -- Update all nsOpenTime values
  LOOP
    OPEN cfs;
    FETCH cfs BULK COLLECT INTO ids LIMIT 1000;
    EXIT WHEN ids.count = 0;
    FORALL i IN 1 .. ids.COUNT
      UPDATE CastorFile SET nsOpenTime = lastUpdateTime WHERE id = ids(i);
    CLOSE cfs;
    COMMIT;
  END LOOP;

  SELECT COUNT(*) INTO remCF FROM CastorFile WHERE tapestatus IS NULL;

XXX cleanup files with no diskcopy ???

  IF remCF > 0 THEN
    raise_application_error(-20001, 'Found ' || TO_CHAR(remCF) || ' CastorFile with no DiskCopy !');
  END IF;
END;
/

ALTER TABLE CastorFile MODIFY (nsOpenTime CONSTRAINT NN_CastorFile_NsOpenTime NOT NULL);


ALTER TABLE StageRepackRequest ADD (fileCount INTEGER, totalSize INTEGER);
UPDATE StageRepackRequest SET fileCount = 0, totalSize = 0;  -- XXX do we need to recompute those figures? probably not...
ALTER TABLE StageRepackRequest MODIFY (fileCount CONSTRAINT NN_StageRepackReq_fileCount NOT NULL, totalSize CONSTRAINT NN_StageRepackReq_totalSize NOT NULL, status CONSTRAINT NN_StageRepackReq_status NOT NULL, repackVid CONSTRAINT NN_StageRepackReq_repackVid NOT NULL);

DROP TRIGGER tr_DiskCopy_Online;
DROP PROCEDURE getDiskCopiesForJob;
DROP PROCEDURE processPrepareRequest;
DROP PROCEDURE recreateCastorFile;

-- second round :
--  - add missing constraints on statuses, 
--  - new draining schema
--  - create Disk2DiskCopyJob table
--  - drop of stageDiskcopyReplicaRequest table
--  - drop parent column from SubRequest

ALTER TABLE SubRequest
  ADD CONSTRAINT CK_SubRequest_Status
  CHECK (status IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13));

ALTER TABLE RecallMount
  ADD CONSTRAINT CK_RecallMount_Status
  CHECK (status IN (0, 1, 2));

ALTER TABLE RecallJob
  ADD CONSTRAINT CK_RecallJob_Status
  CHECK (status IN (0, 1, 2));

ALTER TABLE MigrationMount
  ADD CONSTRAINT CK_MigrationMount_Status
  CHECK (status IN (0, 1, 2, 3));

ALTER TABLE MigrationJob
  ADD CONSTRAINT CK_MigrationJob_Status
  CHECK (status IN (0, 1, 3));

ALTER TABLE DiskServer
  ADD CONSTRAINT CK_DiskServer_Status
  CHECK (status IN (0, 1, 2, 3));

ALTER TABLE FileSystem
  ADD CONSTRAINT CK_FileSystem_Status
  CHECK (status IN (0, 1, 2, 3));

ALTER TABLE DiskCopy
  ADD CONSTRAINT CK_DiskCopy_Status
  CHECK (status IN (0, 4, 5, 6, 7, 9, 10, 11));

ALTER TABLE DiskCopy
  ADD CONSTRAINT CK_DiskCopy_GcType
  CHECK (gcType IN (0, 1, 2, 3, 4, 5));

ALTER TABLE StageRepackRequest
  ADD CONSTRAINT CK_StageRepackRequest_Status
  CHECK (status IN (0, 1, 2, 3, 4, 5, 6));

ALTER TABLE CastorFile
  ADD CONSTRAINT CK_CastorFile_TapeStatus
  CHECK (tapeStatus IN (0, 1, 2));

/* Creation of the DrainingJob table
 *   - id : unique identifier of the DrainingJob
 *   - userName, euid, egid : identification of the originator of the job
 *   - pid : process id of the originator of the job
 *   - machine : machine where the originator of the job was running
 *   - creationTime : time when the job was created
 *   - lastModificationTime : lest time the job was updated
 *   - fileSystem : id of the concerned filesystem
 *   - status : current status of the job. One of SUBMITTED, STARTING,
 *              RUNNING, FAILED, COMPLETED
 *   - svcClass : the target service class for the draining
 *   - autoDelete : whether source files should be invalidated after
 *                  their replication. One of 0 (no) and 1 (yes)
 *   - fileMask : indicates which files are concerned by the draining.
 *                One of NOTONTAPE, ALL
 *   - totalFiles, totalBytes : indication of the work to be done. These
 *                numbers are partial and increasing while starting
 *                and then stable while running
 *   - nbFailedBytes/Files, nbSuccessBytes/Files : indication of the
 *                work already done. These counters are updated while running
 *   - userComment : a user comment
 */
CREATE TABLE DrainingJob
  (id             INTEGER CONSTRAINT PK_DrainingJob_Id PRIMARY KEY,
   userName       VARCHAR2(2048) CONSTRAINT NN_DrainingJob_UserName NOT NULL,
   euid           INTEGER CONSTRAINT NN_DrainingJob_Euid NOT NULL,
   egid           INTEGER CONSTRAINT NN_DrainingJob_Egid NOT NULL,
   pid            INTEGER CONSTRAINT NN_DrainingJob_Pid NOT NULL,
   machine        VARCHAR2(2048) CONSTRAINT NN_DrainingJob_Machine NOT NULL,
   creationTime   INTEGER CONSTRAINT NN_DrainingJob_CT NOT NULL,
   lastModificationTime INTEGER CONSTRAINT NN_DrainingJob_LMT NOT NULL,
   status         INTEGER CONSTRAINT NN_DrainingJob_Status NOT NULL,
   fileSystem     INTEGER CONSTRAINT NN_DrainingJob_FileSystem NOT NULL 
                          CONSTRAINT UN_DrainingJob_FileSystem UNIQUE USING INDEX,
   svcClass       INTEGER CONSTRAINT NN_DrainingJob_SvcClass NOT NULL,
   autoDelete     INTEGER CONSTRAINT NN_DrainingJob_AutoDelete NOT NULL,
   fileMask       INTEGER CONSTRAINT NN_DrainingJob_FileMask NOT NULL,
   totalFiles     INTEGER CONSTRAINT NN_DrainingJob_TotFiles NOT NULL,
   totalBytes     INTEGER CONSTRAINT NN_DrainingJob_TotBytes NOT NULL,
   nbFailedBytes  INTEGER CONSTRAINT NN_DrainingJob_FailedFiles NOT NULL,
   nbFailedFiles  INTEGER CONSTRAINT NN_DrainingJob_FailedBytes NOT NULL,
   nbSuccessBytes INTEGER CONSTRAINT NN_DrainingJob_SuccessBytes NOT NULL,
   nbSuccessFiles INTEGER CONSTRAINT NN_DrainingJob_SuccessFiles NOT NULL,
   userComment    VARCHAR2(2048))
ENABLE ROW MOVEMENT;

BEGIN
  setObjStatusName('DrainingJob', 'status', 0, 'SUBMITTED');
  setObjStatusName('DrainingJob', 'status', 1, 'STARTING');
  setObjStatusName('DrainingJob', 'status', 2, 'RUNNING');
  setObjStatusName('DrainingJob', 'status', 4, 'FAILED');
  setObjStatusName('DrainingJob', 'status', 5, 'COMPLETED');
END;
/

ALTER TABLE DrainingJob
  ADD CONSTRAINT FK_DrainingJob_FileSystem
  FOREIGN KEY (fileSystem)
  REFERENCES FileSystem (id);

ALTER TABLE DrainingJob
  ADD CONSTRAINT CK_DrainingJob_Status
  CHECK (status IN (0, 1, 2, 4, 5));

ALTER TABLE DrainingJob
  ADD CONSTRAINT FK_DrainingJob_SvcClass
  FOREIGN KEY (svcClass)
  REFERENCES SvcClass (id);

ALTER TABLE DrainingJob
  ADD CONSTRAINT CK_DrainingJob_AutoDelete
  CHECK (autoDelete IN (0, 1));

ALTER TABLE DrainingJob
  ADD CONSTRAINT CK_DrainingJob_FileMask
  CHECK (fileMask IN (0, 1));

/* Creation of the DrainingErrors table
 *   - drainingJob : identifier of the concerned DrainingJob
 *   - errorMsg : the error that occured
 *   - fileId, nsHost : concerned file
 */
CREATE TABLE DrainingErrors
  (drainingJob  INTEGER CONSTRAINT NN_DrainingErrors_DJ NOT NULL,
   errorMsg     VARCHAR2(2048) CONSTRAINT NN_DrainingErrors_ErrorMsg NOT NULL,
   fileId       INTEGER CONSTRAINT NN_DrainingErrors_FileId NOT NULL,
   nsHost       VARCHAR2(2048) CONSTRAINT NN_DrainingErrors_NsHost NOT NULL)
ENABLE ROW MOVEMENT;

CREATE INDEX I_DrainingErrors_DJ ON DrainingErrors (drainingJob);

ALTER TABLE DrainingErrors
  ADD CONSTRAINT FK_DrainingErrors_DJ
  FOREIGN KEY (drainingJob)
  REFERENCES DrainingJob (id);

/* Definition of the Disk2DiskCopyJob table. Each line is a disk2diskCopy job to process
 *   id : unique DB identifier for this job
 *   transferId : unique identifier for the transfer associated to this job
 *   creationTime : creation time of this item, allows to compute easily waiting times
 *   status : status of the job (PENDING, SCHEDULED, RUNNING) 
 *   retryCounter : number of times the copy was attempted
 *   ouid : the originator user id
 *   ogid : the originator group id
 *   castorFile : the concerned file
 *   nsOpenTime : the nsOpenTime of the castorFile when this job was created
 *                Allows to detect if the file has been overwritten during replication
 *   destSvcClass : the destination service class
 *   replicationType : the type of replication involved (user, internal or draining)
 *   replacedDcId : in case of draining, the replaced diskCopy to be dropped
 *   destDcId : the destination diskCopy
 *   drainingJob : the draining job behind this d2dJob. Not NULL only if replicationType is DRAINING'
 */
CREATE TABLE Disk2DiskCopyJob
  (id NUMBER CONSTRAINT PK_Disk2DiskCopyJob_Id PRIMARY KEY 
             CONSTRAINT NN_Disk2DiskCopyJob_Id NOT NULL,
   transferId VARCHAR2(2048) CONSTRAINT NN_Disk2DiskCopyJob_TId NOT NULL,
   creationTime INTEGER CONSTRAINT NN_Disk2DiskCopyJob_CTime NOT NULL,
   status INTEGER CONSTRAINT NN_Disk2DiskCopyJob_Status NOT NULL,
   retryCounter INTEGER DEFAULT 0 CONSTRAINT NN_Disk2DiskCopyJob_retryCnt NOT NULL,
   ouid INTEGER CONSTRAINT NN_Disk2DiskCopyJob_ouid NOT NULL,
   ogid INTEGER CONSTRAINT NN_Disk2DiskCopyJob_ogid NOT NULL,
   castorFile INTEGER CONSTRAINT NN_Disk2DiskCopyJob_CastorFile NOT NULL,
   nsOpenTime INTEGER CONSTRAINT NN_Disk2DiskCopyJob_NSOpenTime NOT NULL,
   destSvcClass INTEGER CONSTRAINT NN_Disk2DiskCopyJob_dstSC NOT NULL,
   replicationType INTEGER CONSTRAINT NN_Disk2DiskCopyJob_Type NOT NULL,
   replacedDcId INTEGER,
   destDcId INTEGER CONSTRAINT NN_Disk2DiskCopyJob_DCId NOT NULL,
   drainingJob INTEGER)
INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;
CREATE INDEX I_Disk2DiskCopyJob_Tid ON Disk2DiskCopyJob(transferId);
CREATE INDEX I_Disk2DiskCopyJob_CfId ON Disk2DiskCopyJob(CastorFile);
CREATE INDEX I_Disk2DiskCopyJob_CT_Id ON Disk2DiskCopyJob(creationTime, id);
CREATE INDEX I_Disk2DiskCopyJob_drainingJob ON Disk2DiskCopyJob(drainingJob);
BEGIN
  -- PENDING status is when a Disk2DiskCopyJob is created
  -- It is immediately candidate for being scheduled
  setObjStatusName('Disk2DiskCopyJob', 'status', dconst.DISK2DISKCOPYJOB_PENDING, 'DISK2DISKCOPYJOB_PENDING');
  -- SCHEDULED status is when the Disk2DiskCopyJob has been scheduled and is not yet started
  setObjStatusName('Disk2DiskCopyJob', 'status', dconst.DISK2DISKCOPYJOB_SCHEDULED, 'DISK2DISKCOPYJOB_SCHEDULED');
  -- RUNNING status is when the disk to disk copy is ongoing
  setObjStatusName('Disk2DiskCopyJob', 'status', dconst.DISK2DISKCOPYJOB_RUNNING, 'DISK2DISKCOPYJOB_RUNNING');
  -- USER replication type is when replication is triggered by the user
  setObjStatusName('Disk2DiskCopyJob', 'replicationType', dconst.REPLICATIONTYPE_USER, 'REPLICATIONTYPE_USER');
  -- INTERNAL replication type is when replication is triggered internally (e.g. dual copy disk pools)
  setObjStatusName('Disk2DiskCopyJob', 'replicationType', dconst.REPLICATIONTYPE_INTERNAL, 'REPLICATIONTYPE_INTERNAL');
  -- DRAINING replication type is when replication is triggered by a drain operation
  setObjStatusName('Disk2DiskCopyJob', 'replicationType', dconst.REPLICATIONTYPE_DRAINING, 'REPLICATIONTYPE_DRAINING');
END;
/
ALTER TABLE Disk2DiskCopyJob ADD CONSTRAINT FK_Disk2DiskCopyJob_CastorFile
  FOREIGN KEY (castorFile) REFERENCES CastorFile(id);
ALTER TABLE Disk2DiskCopyJob ADD CONSTRAINT FK_Disk2DiskCopyJob_SvcClass
  FOREIGN KEY (destSvcClass) REFERENCES SvcClass(id);
ALTER TABLE Disk2DiskCopyJob ADD CONSTRAINT FK_Disk2DiskCopyJob_DrainJob
  FOREIGN KEY (drainingJob) REFERENCES DrainingJob(id);
ALTER TABLE Disk2DiskCopyJob
  ADD CONSTRAINT CK_Disk2DiskCopyJob_Status
  CHECK (status IN (0, 1, 2));
ALTER TABLE Disk2DiskCopyJob
  ADD CONSTRAINT CK_Disk2DiskCopyJob_type
  CHECK (replicationType IN (0, 1, 2));

INSERT INTO CastorConfig
  VALUES ('D2dCopy', 'MaxNbRetries', '2', 'The maximum number of retries for disk to disk copies before it is considered failed. Here 2 means we will do in total 3 attempts.');

DROP TABLE StageDiskCopyReplicaRequest;
ALTER TABLE SubRequest DROP (parent);
DROP PROCEDURE makeSubRequestWait;
DROP PROCEDURE disk2DiskCopyFailed;
DROP PROCEDURE disk2DiskCopyDone;
DROP PROCEDURE createDiskCopyReplicaRequest;
DROP PROCEDURE getBestDiskCopyToReplicate;
DROP PROCEDURE transferToSchedule;
DROP TABLE DrainingDiskCopy;
DROP TABLE DrainingFileSystem;
DROP PROCEDURE removeFailedDrainingTransfers;
DROP PROCEDURE drainFileSystem;
DROP PROCEDURE startDraining;
DROP PROCEDURE stopDraining;

UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_14_X';
COMMIT;
