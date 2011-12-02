/*******************************************************************	
 * @(#)$RCSfile: oracleTape.sql,v $ $Revision: 1.761 $ $Date: 2009/08/10 15:30:13 $ $Author: itglp $
 *
 * PL/SQL code for the interface to the tape system
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

 /* PL/SQL declaration for the castorTape package */
CREATE OR REPLACE PACKAGE castorTape AS 
   TYPE TapeGatewayRequestExtended IS RECORD (
    accessMode NUMBER,
    id NUMBER,
    starttime NUMBER,
    lastvdqmpingtime NUMBER, 
    vdqmvolreqid NUMBER, 
    vid VARCHAR2(2048));
  TYPE TapeGatewayRequest_Cur IS REF CURSOR RETURN TapeGatewayRequestExtended;
  TYPE TapeGatewayRequestCore IS RECORD (
    tpmode INTEGER,
    side INTEGER,
    vid VARCHAR2(2048),
    tapeRequestId NUMBER);
  TYPE TapeGatewayRequestCore_Cur IS REF CURSOR RETURN TapeGatewayRequestCore;
  TYPE MigrationMountCore IS RECORD (
    id INTEGER,
    tapePoolName VARCHAR2(2048));
  TYPE MigrationMount_Cur IS REF CURSOR RETURN MigrationMountCore; 
  TYPE DbMigrationInfo IS RECORD (
    id NUMBER,
    copyNb NUMBER,
    fileName VARCHAR2(2048),
    nsHost VARCHAR2(2048),
    fileId NUMBER,
    fileSize NUMBER);
  TYPE DbMigrationInfo_Cur IS REF CURSOR RETURN DbMigrationInfo;
  TYPE DbRecallInfo IS RECORD (
    vid VARCHAR2(2048),
    tapeId NUMBER,
    dataVolume NUMBER,
    numbFiles NUMBER,
    expireTime NUMBER,
    priority NUMBER);
  TYPE DbRecallInfo_Cur IS REF CURSOR RETURN DbRecallInfo;
  TYPE RecallMountsForPolicy IS RECORD (
    tapeId             NUMBER,
    vid                VARCHAR2(2048),
    numSegments        NUMBER,
    totalBytes         NUMBER,
    ageOfOldestSegment NUMBER,
    priority           NUMBER,
    status             NUMBER);
  TYPE RecallMountsForPolicy_Cur IS REF CURSOR RETURN RecallMountsForPolicy;
  TYPE FileToRecallCore IS RECORD (
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   diskserver VARCHAR2(2048),
   mountPoint VARCHAR(2048),
   path VARCHAR2(2048),
   fseq INTEGER,
   fileTransactionId NUMBER);
  TYPE FileToRecallCore_Cur IS REF CURSOR RETURN  FileToRecallCore;  
  TYPE FileToMigrateCore IS RECORD (
   fileId NUMBER,
   nsHost VARCHAR2(2048),
   lastModificationTime NUMBER,
   diskserver VARCHAR2(2048),
   mountPoint VARCHAR(2048),
   path VARCHAR2(2048),
   lastKnownFilename VARCHAR2(2048), 
   fseq INTEGER,
   fileSize NUMBER,
   fileTransactionId NUMBER);
  TYPE FileToMigrateCore_Cur IS REF CURSOR RETURN  FileToMigrateCore;  
END castorTape;
/

/* Trigger ensuring validity of VID in state transitions */
CREATE OR REPLACE TRIGGER TR_RecallJob_VID
BEFORE INSERT OR UPDATE OF Status ON RecallJob
FOR EACH ROW
BEGIN
  /* Enforce the state integrity of VID in state transitions */
  
  CASE :new.status
    WHEN  tconst.RECALLJOB_SELECTED THEN
      /* The VID MUST be defined when the RecallJob gets selected */
      IF :new.VID IS NULL THEN
        RAISE_APPLICATION_ERROR(-20119,
          'Moving/creating (in)to RECALLJOB_SELECTED State without a VID (TC.ID: '||
          :new.ID||' VID:'|| :old.VID||'=>'||:new.VID||' Status:'||:old.status||'=>'||:new.status||')');
      END IF;
    WHEN tconst.RECALLJOB_STAGED THEN
       /* The VID MUST be defined when the RecallJob goes to staged */
       IF :new.VID IS NULL THEN
         RAISE_APPLICATION_ERROR(-20119,
          'Moving/creating (in)to RECALLJOB_STAGED State without a VID (TC.ID: '||
          :new.ID||' VID:'|| :old.VID||'=>'||:new.VID||' Status:'||:old.status||'=>'||:new.status||')');
       END IF;
       /* The VID MUST remain the same when going to staged */
       IF :new.VID != :old.VID THEN
         RAISE_APPLICATION_ERROR(-20119,
           'Moving to STAGED State without carrying the VID over');
       END IF;
    ELSE
      /* In all other cases, VID should be NULL */
      IF :new.VID IS NOT NULL THEN
        RAISE_APPLICATION_ERROR(-20119,
          'Moving/creating (in)to RecallJob state where VID makes no sense, yet VID!=NULL (TC.ID: '||
          :new.ID||' VID:'|| :old.VID||'=>'||:new.VID||' Status:'||:old.status||'=>'||:new.status||')');
      END IF;
  END CASE;
END;
/

/* PL/SQL method implementing bestFileSystemForSegment */
CREATE OR REPLACE PROCEDURE bestFileSystemForSegment(segmentId IN INTEGER, diskServerName OUT VARCHAR2,
                                                     rmountPoint OUT VARCHAR2, rpath OUT VARCHAR2,
                                                     dcid OUT INTEGER) AS
  fileSystemId NUMBER;
  cfid NUMBER;
  fsDiskServer NUMBER;
  fileSize NUMBER;
  nb NUMBER;
BEGIN
  -- First get the DiskCopy and see whether it already has a fileSystem
  -- associated (case of a multi segment file)
  -- We also select on the DiskCopy status since we know it is
  -- in WAITTAPERECALL status and there may be other ones
  -- INVALID, GCCANDIDATE, DELETED, etc...
  SELECT DiskCopy.fileSystem, DiskCopy.path, DiskCopy.id, DiskCopy.CastorFile
    INTO fileSystemId, rpath, dcid, cfid
    FROM RecallJob, Segment, DiskCopy
   WHERE Segment.id = segmentId
     AND Segment.copy = RecallJob.id
     AND DiskCopy.castorfile = RecallJob.castorfile
     AND DiskCopy.status = dconst.DISKCOPY_WAITTAPERECALL;
  -- Check if the DiskCopy had a FileSystem associated
  IF fileSystemId > 0 THEN
    BEGIN
      -- It had one, force filesystem selection, unless it was disabled.
      SELECT DiskServer.name, DiskServer.id, FileSystem.mountPoint
        INTO diskServerName, fsDiskServer, rmountPoint
        FROM DiskServer, FileSystem
       WHERE FileSystem.id = fileSystemId
         AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
         AND DiskServer.id = FileSystem.diskServer
         AND DiskServer.status = dconst.DISKSERVER_PRODUCTION;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Error, the filesystem or the machine was probably disabled in between
      raise_application_error(-20101, 'In a multi-segment file, FileSystem or Machine was disabled before all segments were recalled');
    END;
  ELSE
    fileSystemId := 0;
    -- The DiskCopy had no FileSystem associated with it which indicates that
    -- This is a new recall. We try and select a good FileSystem for it!
    FOR a IN (SELECT /*+ INDEX(Subrequest I_Subrequest_Castorfile)*/
                     DiskServer.name, FileSystem.mountPoint, FileSystem.id,
                     FileSystem.diskserver, CastorFile.fileSize
                FROM DiskServer, FileSystem, DiskPool2SvcClass,
                     (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ id, svcClass from StageGetRequest                            UNION ALL
                      SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */ id, svcClass from StagePrepareToGetRequest UNION ALL
                      SELECT /*+ INDEX(StageRepackRequest PK_StageRepackRequest_Id) */ id, svcClass from StageRepackRequest                   UNION ALL
                      SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ id, svcClass from StageUpdateRequest                   UNION ALL
                      SELECT /*+ INDEX(StagePrepareToUpdateRequest PK_StagePrepareToUpdateRequ_Id) */ id, svcClass from StagePrepareToUpdateRequest) Request,
                      SubRequest, CastorFile
               WHERE CastorFile.id = cfid
                 AND SubRequest.castorfile = cfid
                 AND SubRequest.status = dconst.SUBREQUEST_WAITTAPERECALL
                 AND Request.id = SubRequest.request
                 AND Request.svcclass = DiskPool2SvcClass.child
                 AND FileSystem.diskpool = DiskPool2SvcClass.parent
                 AND FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > CastorFile.fileSize
                 AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
                 AND DiskServer.id = FileSystem.diskServer
                 AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
            ORDER BY -- first prefer DSs without concurrent migrators/recallers
                     DiskServer.nbRecallerStreams ASC, FileSystem.nbMigratorStreams ASC,
                     -- then order by rate as defined by the function
                     fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                                    FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams,
                                    FileSystem.nbRecallerStreams) DESC,
                     -- finally use randomness to avoid preferring always the same FS
                     DBMS_Random.value)
    LOOP
      diskServerName := a.name;
      rmountPoint    := a.mountPoint;
      fileSystemId   := a.id;
      -- Check that we don't already have a copy of this file on this filesystem.
      -- This will never happen in normal operations but may be the case if a filesystem
      -- was disabled and did come back while the tape recall was waiting.
      -- Even if we optimize by cancelling remaining unneeded tape recalls when a
      -- fileSystem comes back, the ones running at the time of the come back will have
      -- the problem.
      SELECT count(*) INTO nb
        FROM DiskCopy
       WHERE fileSystem = a.id
         AND castorfile = cfid
         AND status = dconst.DISKCOPY_STAGED;
      IF nb != 0 THEN
        raise_application_error(-20103, 'Recaller could not find a FileSystem in production in the requested SvcClass and without copies of this file');
      END IF;
      -- Set the diskcopy's filesystem
      UPDATE DiskCopy
         SET fileSystem = a.id
       WHERE id = dcid;
      RETURN;
    END LOOP;

    IF fileSystemId = 0 THEN
      raise_application_error(-20115, 'No suitable filesystem found for this recalled segment');
    END IF;
  END IF;
END;
/

/** Functions for the RecHandlerDaemon **/

CREATE OR REPLACE PROCEDURE tapesAndMountsForRecallPolicy (
  outRecallMounts      OUT castorTape.RecallMountsForPolicy_Cur,
  outNbMountsForRecall OUT NUMBER)
AS
-- Retrieves the input for the rechandler daemon to pass to the
-- rechandler-policy Python-function
--
-- @param outRecallMounts      List of recall-mounts which are either pending,
--                             waiting for a drive or waiting for the
--                             rechandler-policy.
-- @param outNbMountsForRecall The number of tapes currently mounted for recall
--                             for this stager.
BEGIN
  SELECT count(distinct Tape.vid )
    INTO outNbMountsForRecall
    FROM Tape
   WHERE Tape.status = tconst.TAPE_MOUNTED
     AND TPMODE = tconst.TPMODE_READ;

    OPEN outRecallMounts
     FOR SELECT /*+ NO_USE_MERGE(TAPE SEGMENT RECALLJOB CASTORFILE) NO_USE_HASH(TAPE SEGMENT RECALLJOB CASTORFILE) INDEX_RS_ASC(SEGMENT I_SEGMENT_TAPESTATUSFSEQ) INDEX_RS_ASC(TAPE I_TAPE_STATUS) INDEX_RS_ASC(TAPE
COPY PK_RECALLJOB_ID) INDEX_RS_ASC(CASTORFILE PK_CASTORFILE_ID) */ Tape.id,
                Tape.vid,
                count ( distinct segment.id ),
                sum ( CastorFile.fileSize ),
                getTime ( ) - min ( Segment.creationTime ) age,
                max ( Segment.priority ),
                Tape.status
           FROM RecallJob,
                CastorFile,
                Segment,
                Tape
          WHERE Tape.id = Segment.tape
            AND RecallJob.id = Segment.copy
            AND CastorFile.id = RecallJob.castorfile
            AND Tape.status IN (tconst.TAPE_PENDING, tconst.TAPE_WAITDRIVE, tconst.TAPE_WAITPOLICY)
            AND Segment.status = tconst.SEGMENT_UNPROCESSED
          GROUP BY Tape.id, Tape.vid, Tape.status
          ORDER BY age DESC;
    
END tapesAndMountsForRecallPolicy;
/

/* resurrect tapes */
CREATE OR REPLACE PROCEDURE resurrectTapes
(tapeIds IN castor."cnumList")
AS
BEGIN
  FOR i IN tapeIds.FIRST .. tapeIds.LAST LOOP
    UPDATE Tape T
       SET T.TapegatewayRequestId = ids_seq.nextval,
           T.status = tconst.TAPE_PENDING
     WHERE T.status = tconst.TAPE_WAITPOLICY AND T.id = tapeIds(i);
  END LOOP; 
  COMMIT;
END;	
/

/* insert new Migration Mount */
CREATE OR REPLACE PROCEDURE insertMigrationMount(inTapePoolId IN NUMBER) AS
BEGIN 
  INSERT INTO MigrationMount
              (vdqmVolReqId, tapeGatewayRequestId, id, startTime, VID, label, density,
               lastFseq, lastVDQMPingTime, tapePool, status)
       VALUES (NULL, ids_seq.nextval, ids_seq.nextval, gettime(), NULL, NULL, NULL,
               NULL, 0, inTapePoolId, tconst.MIGRATIONMOUNT_WAITTAPE);
END;
/

/* resurrect tapes */
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
    -- get number of mount already running for this tapepool
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
             (varNbFiles/(varNbMounts+1) >= t.minNbFilesForMount)) LOOP
        insertMigrationMount(t.id);
        varNbMounts := varNbMounts + 1;
      END LOOP;
      -- force creation of a unique mount in case no mount was created at all and some files are too old
      IF varNbMounts = 0 AND t.nbDrives > 0 AND gettime() - varOldestCreationTime > t.maxFileAgeBeforeMount THEN
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

/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('MIGRATIONMOUNTSJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every minute executing the deleteTerminatedRequests procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'MigrationMountsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN startMigrationMounts(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Creates MigrationMount entries when new migrations should start');
END;
/

