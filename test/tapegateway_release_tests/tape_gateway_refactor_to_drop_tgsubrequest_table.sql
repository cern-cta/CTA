/*******************************************************************
 *
 * @(#)$RCSfile$ $Revision$ $Date$ $Author$
 *
 * This file contains is an SQL script used to test polices of the tape gateway
 * on a disposable stager schema, which is supposed to be initially empty.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/
 
 /*
  * Table holding the known on-or-will-be-on tape tape copies
  * in order for the migration policy to not migrate a tape copy to a tape where
  * tape copies with different copy numbers exist.
  *
  * Entries in the cache are created when the tape copy is queued for the tape, flaged when the 
  * copy is successfuly migrated, also addded when discovered from the NS, and finally bulk droped
  * (for a given tape).
  */
  
  /* 
   * We will get rid of the following collumns :
   * CREATE TABLE "SCRATCH_DEV03"."TAPEGATEWAYSUBREQUEST"
  (
    "FSEQ"     NUMBER,
    "ID"       NUMBER(*,0),
    "TAPECOPY" NUMBER(*,0),
    "REQUEST"  NUMBER(*,0),
    "DISKCOPY" NUMBER(*,0),
    CONSTRAINT "PK_TAPEGATEWAYSUBREQUEST_ID" PRIMARY KEY ("ID") USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT) TABLESPACE "STAGER_DATA" ENABLE,
    CONSTRAINT "UN_TGSUBREQUEST_TR_DC" UNIQUE ("REQUEST", "DISKCOPY") USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT) TABLESPACE "STAGER_DATA" ENABLE,
    CONSTRAINT "FK_TGSUBREQUEST_TC" FOREIGN KEY ("TAPECOPY") REFERENCES "SCRATCH_DEV03"."TAPECOPY" ("ID") ENABLE,
    CONSTRAINT "FK_TGSUBREQUEST_DC" FOREIGN KEY ("DISKCOPY") REFERENCES "SCRATCH_DEV03"."DISKCOPY" ("ID") ENABLE,
    CONSTRAINT "FK_TGSUBREQUEST_TGR" FOREIGN KEY ("REQUEST") REFERENCES "SCRATCH_DEV03"."TAPEGATEWAYREQUEST" ("ID") ENABLE
  )
   */
 
-- ALTER TABLE TapeCopy ADD (fSeq NUMBER);
-- ALTER TABLE TAPEGATEWAYSUBREQUEST DROP COLUMN fSeq;
-- ALTER TABLE TAPEGATEWAYSUBREQUEST DROP COLUMN Id;
-- ALTER TABLE TAPEGATEWAYSUBREQUEST DROP COLUMN TapeCopy;
-- ALTER TABLE TapeCopy ADD (TapeGatewayRequest NUMBER (*,0));
ALTER TABLE TapeCopy ADD CONSTRAINT FK_TAPECOPY_TGR FOREIGN KEY (TapeGatewayRequest) REFERENCES TapeGatewayRequest (ID) ENABLE;
- -ALTER TABLE TAPEGATEWAYSUBREQUEST DROP CONSTRAINT UN_TGSUBREQUEST_TR_DC;
-- ALTER TABLE TapeCopy ADD CONSTRAINT UN_TAPECOPY_CF_TGR UNIQUE (TAPEGATEWAYREQUEST, CASTORFILE) USING INDEX;
-- ALTER TABLE TapeCopy DROP CONSTRAINT UN_TAPECOPY_CF_TGR;
-- ALTER TABLE TapeCopy DROP INDEX UN_TAPECOPY_CF_TGR; -- Dripping the contraint implicitly drops the implicit index.
-- ALTER TABLE TAPEGATEWAYSUBREQUEST DROP COLUMN REQUEST;
-- ALTER TABLE TapeCopy ADD (DiskCopy NUMBER(*,0));
-- ALTER TABLE TAPEGATEWAYSUBREQUEST DROP COLUMN DISKCOPY; -- Oops: SQL Error: ORA-12983: cannot drop all columns in a table 12983. 00000 -  "cannot drop all columns in a table" */
-- There you go: */
-- DROP TABLE TAPEGATEWAYSUBREQUEST;

-- 	Fix wrongly named column in TapeGatewayRequest */
-- ALTER TABLE TapeGatewayRequest RENAME COLUMN STREAMMIGRATION TO STREAM;

-- Keep track of the Volume ID to which a tapecopy is being migrated */
-- This is necessary as the life cycle of the STAGED tape copy can be longer 
-- than the one of the tapegatewayrequest, stream and finally tape it points
-- to. */
-- ALTER TABLE TapeCopy ADD (VID VARCHAR2(2048 BYTE));
-- We want to query this table by VID */
CREATE INDEX I_TapeCopy_VID ON TapeCopy(VID);

-- File request number has to be unique or null the life cycle is not clear, so 
-- we'll throw exception when a null file request ID will be returned to C++ */
-- ALTER TABLE TapeCopy ADD (FileTransactionId NUMBER);
ALTER TABLE TapeCopy ADD CONSTRAINT UN_TAPECOPY_FILETRID 
  UNIQUE (FileTransactionId) USING INDEX;
-- Create sequence for the File request IDs. */
CREATE SEQUENCE TG_FILETRID_SEQ START WITH 1 INCREMENT BY 1;
-- Ensure consistency with a trigger */
-- We MUST have a VID set when entring states SELECTED, STAGED, 
-- and CAN have it still set when entering states REC_RETRY or MIG_RETRY to log
-- the information.
-- when entering any other state, it's forbidden.
-- This criteria does not need history (it's all :new based) */
CREATE OR REPLACE TRIGGER TR_TapeCopy_VID
BEFORE INSERT OR UPDATE OF Status ON TapeCopy
FOR EACH ROW
BEGIN
  /* Enforce the state integrity of VID in state transitions */
  CASE 
    WHEN (:new.Status IN (3) AND :new.VID IS NULL) -- 3 TAPECOPY_SELECTED
      THEN
      RAISE_APPLICATION_ERROR(-20119, 
        'Moving/creating (in)to TAPECOPY_SELECTED State without a VID');
    WHEN (UPDATING('Status') AND :new.Status IN (5, 9) AND -- 5 TAPECOPY_STAGED
                                                      -- 9 TAPECOPY_MIG_RETRY
      :new.VID != :old.VID)
      THEN
      RAISE_APPLICATION_ERROR(-20119, 
        'Moving to STAGED/MIG_RETRY State without carrying the VID over');
    WHEN (:new.status IN (8)) -- 8 TAPECOPY_REC_RETRY
      THEN
      NULL; -- Free for all case
    WHEN (:new.Status NOT IN (3,5,8,9) AND :new.VID IS NOT NULL)
                                                     -- 3 TAPECOPY_SELECTED
                                                     -- 5 TAPECOPY_STAGED
                                                     -- 8 TAPECOPY_REC_RETRY
                                                     -- 9 TAPECOPY_MIG_RETRY
      THEN
      RAISE_APPLICATION_ERROR(-20119, 
        'Moving/creating (in)to TapeCopy state where VID makes no sense, yet VID!=NULL');
    ELSE
      NULL;
  END CASE;
END;
/

-- Fixem, one by one */

-- Replacing all references to TapeGatewaySubrequest */
create or replace
PROCEDURE tg_defaultMigrSelPolicy(inStreamId IN INTEGER,
                                  outDiskServerName OUT NOCOPY VARCHAR2,
                                  outMountPoint OUT NOCOPY VARCHAR2,
                                  outPath OUT NOCOPY VARCHAR2,
                                  outDiskCopyId OUT INTEGER,
                                  outCastorFileId OUT INTEGER,
                                  outFileId OUT INTEGER,
                                  outNsHost OUT NOCOPY VARCHAR2, 
                                  outFileSize OUT INTEGER,
                                  outTapeCopyId OUT INTEGER, 
                                  outLastUpdateTime OUT INTEGER) AS
  /* Find the next tape copy to migrate from a given stream ID.
   * 
   * Procedure's input: Stream Id for a stream that is locked by caller.
   *
   * Procedure's output: Returns a non-zero TapeCopy ID on full success
   * Can return a non-zero DiskServer Id when a DiskServer got selected without 
   * selecting any tape copy.
   * Data modification: The function updates the stream's filesystem information
   * in case a new one got seleted.
   *
   * Lock taken on the diskserver in some cases.
   * Lock taken on the tapecopy if it selects one.
   * Lock taken on  the Stream when a new disk server is selected.
   * 
   * Commits: The function does not commit data.
   *
   * Per policy we should only propose a tape copy for a file that does not 
   * already have a tapecopy attached for or mirgated to the same
   * tape.
   * The tape's VID can be found from the streamId by:
   * Stream->Tape->VID.
   * The tapecopies carry VID themselves, when in stated STAGED, SELECTED and 
   * in error states. In other states the VID must be null, per constraint.
   * The already migrated tape copies are kept until the whole set of siblings 
   * have been migrated. Nothing else is guaranteed to be.
   * 
   * From this we can find a list of our potential siblings (by castorfile) from
   * this TapeGatewayRequest, and prevent the selection of tapecopies whose 
   * siblings already live on the same tape.
   */
  varFileSystemId INTEGER := 0;
  varDiskServerId NUMBER;
  varLastFSChange NUMBER;
  varLastFSUsed NUMBER;
  varLastButOneFSUsed NUMBER;
  varFindNewFS NUMBER := 1;
  varNbMigrators NUMBER := 0;
  varUnused NUMBER;
  LockError EXCEPTION;
  varVID VARCHAR2(2048 BYTE);
  PRAGMA EXCEPTION_INIT (LockError, -54);
BEGIN
  outTapeCopyId := 0;
  -- Find out which tape we're talking about
  SELECT T.VID INTO varVID 
    FROM Tape T, Stream S 
   WHERE S.Id = inStreamId 
     AND T.Id = S.Tape;
  -- First try to see whether we should resuse the same filesystem as last time
  SELECT S.lastFileSystemChange, S.lastFileSystemUsed, 
         S.lastButOneFileSystemUsed
    INTO varLastFSChange, varLastFSUsed, varLastButOneFSUsed
    FROM Stream S 
   WHERE S.id = inStreamId;
  -- If the filesystem has changed in the last 5 minutes, consider its reuse
  IF getTime() < varLastFSChange + 900 THEN
    -- Count the number of streams referencing the filesystem
    SELECT (SELECT count(*) FROM stream S
             WHERE S.lastFileSystemUsed = varLastButOneFSUsed) +
           (SELECT count(*) FROM stream S 
             WHERE S.lastButOneFileSystemUsed = varLastButOneFSUsed)
      INTO varNbMigrators FROM DUAL;
    -- only go if we are the only migrator on the file system.
    IF varNbMigrators = 1 THEN
      BEGIN
        -- check states of the diskserver and filesystem and get mountpoint 
        -- and diskserver name
        SELECT DS.name, FS.mountPoint, FS.id 
          INTO outDiskServerName, outMountPoint, varFileSystemId
          FROM FileSystem FS, DiskServer DS
         WHERE FS.diskServer = DS.id
           AND FS.id = varLastButOneFSUsed
           AND FS.status IN (0, 1)  -- PRODUCTION, DRAINING
           AND DS.status IN (0, 1); -- PRODUCTION, DRAINING
        -- we are within the time range, so we try to reuse the filesystem
        SELECT /*+ FIRST_ROWS(1)  LEADING(D T ST) */
               D.path, D.id, D.castorfile, T.id
          INTO outPath, outDiskCopyId, outCastorFileId, outTapeCopyId
          FROM DiskCopy D, TapeCopy T, Stream2TapeCopy STTC
         WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
           AND D.filesystem = varLastButOneFSUsed
           AND STTC.parent = inStreamId
           AND T.status = 2 -- WAITINSTREAMS
           AND STTC.child = T.id
           AND T.castorfile = D.castorfile
           -- Do not select a tapecopy for which a sibling TC is or will be on 
           -- on this tape.
           AND varVID NOT IN (
                 SELECT DISTINCT T2.VID FROM TapeCopy T2
                  WHERE T2.CastorFile=T.Castorfile
                    AND T2.Status IN (3, 5)) -- TAPECOPY_SELECTED, TAPECOPY_STAGED
           AND ROWNUM < 2 FOR UPDATE OF T.id NOWAIT;
        -- Get addition info
        SELECT CF.FileId, CF.NsHost, CF.FileSize, CF.lastUpdateTime
          INTO outFileId, outNsHost, outFileSize, outLastUpdateTime
          FROM CastorFile CF
         WHERE CF.Id = outCastorFileId;
        -- we found one, no need to go for new filesystem
        varFindNewFS := 0;
      EXCEPTION WHEN NO_DATA_FOUND OR LockError THEN
        -- found no tapecopy or diskserver, filesystem are down. We'll go 
        -- through the normal selection
        NULL;
      END;
    END IF;
  END IF;
  IF varFindNewFS = 1 THEN
    FOR f IN (
    SELECT FileSystem.id AS FileSystemId, DiskServer.id AS DiskServerId, 
           DiskServer.name, FileSystem.mountPoint
      FROM Stream, SvcClass2TapePool, DiskPool2SvcClass, FileSystem, DiskServer
     WHERE Stream.id = inStreamId
       AND Stream.TapePool = SvcClass2TapePool.child
       AND SvcClass2TapePool.parent = DiskPool2SvcClass.child
       AND DiskPool2SvcClass.parent = FileSystem.diskPool
       AND FileSystem.diskServer = DiskServer.id
       AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
       AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
     ORDER BY -- first prefer diskservers where no migrator runs and filesystems
              -- with no recalls
              DiskServer.nbMigratorStreams ASC, 
              FileSystem.nbRecallerStreams ASC,
              -- then order by rate as defined by the function
              fileSystemRate(FileSystem.readRate,
                             FileSystem.writeRate,
                             FileSystem.nbReadStreams,
                             FileSystem.nbWriteStreams,
                             FileSystem.nbReadWriteStreams,
                             FileSystem.nbMigratorStreams,
                             FileSystem.nbRecallerStreams) DESC,
              -- finally use randomness to avoid preferring always the same FS
              DBMS_Random.value) LOOP
    BEGIN
      -- Get ready to release lock if the diskserver or tapecopy is not 
      -- to our liking
      SAVEPOINT DServ_TCopy_Lock;
      -- lock the complete diskServer as we will update all filesystems
      SELECT D.id INTO varUnused FROM DiskServer D WHERE D.id = f.DiskServerId 
         FOR UPDATE NOWAIT;
      SELECT /*+ FIRST_ROWS(1) LEADING(D T StT C) */
             F.diskServerId, f.name, f.mountPoint, 
             f.fileSystemId, D.path, D.id,
             D.castorfile, C.fileId, C.nsHost, C.fileSize, T.id, 
             C.lastUpdateTime
          INTO varDiskServerId, outDiskServerName, outMountPoint, 
             varFileSystemId, outPath, outDiskCopyId,
             outCastorFileId, outFileId, outNsHost, outFileSize, outTapeCopyId, 
             outLastUpdateTime
          FROM DiskCopy D, TapeCopy T, Stream2TapeCopy StT, Castorfile C
         WHERE decode(D.status, 10, D.status, NULL) = 10 -- CANBEMIGR
           AND D.filesystem = f.fileSystemId
           AND StT.parent = inStreamId
           AND T.status = 2 -- WAITINSTREAMS
           AND StT.child = T.id
           AND T.castorfile = D.castorfile
           AND C.id = D.castorfile
           AND varVID NOT IN (
                 SELECT DISTINCT T2.VID FROM TapeCopy T2
                  WHERE T2.CastorFile=T.Castorfile
                    AND T2.Status IN (3, 5)) -- TAPECOPY_SELECTED, TAPECOPY_STAGED
           AND ROWNUM < 2 FOR UPDATE OF t.id NOWAIT;
        -- found something on this filesystem, no need to go on
        varDiskServerId := f.DiskServerId;
        varFileSystemId := f.fileSystemId;
        EXIT;
      EXCEPTION WHEN NO_DATA_FOUND OR LockError THEN
         -- either the filesystem is already locked or we found nothing,
         -- let's rollback in case there was NO_DATA_FOUND to release the lock
         ROLLBACK TO SAVEPOINT DServ_TCopy_Lock;
       END;
    END LOOP;
  END IF;

  IF outTapeCopyId = 0 THEN
    -- Nothing found, return; locks will be released by the caller
    RETURN;
  END IF;
  
  IF varFindNewFS = 1 THEN
    UPDATE Stream S
       SET S.lastFileSystemUsed = varFileSystemId,
           -- We store the old value (implicitely available
           -- when reading (reading = :old) to the new row value
           -- (write = :new). So it works.
           S.lastButOneFileSystemUsed = S.lastFileSystemUsed,
           S.lastFileSystemChange = getTime()
     WHERE S.id = inStreamId;
  END IF;

  -- Update Filesystem state
  updateFSMigratorOpened(varDiskServerId, varFileSystemId, 0);
END;
/

create or replace
PROCEDURE tg_getFileToMigrate(
  inVDQMtransacId IN  NUMBER,
  outRet           OUT INTEGER,
  outVid        OUT NOCOPY VARCHAR2,
  outputFile    OUT castorTape.FileToMigrateCore_cur) AS
  /* 
   * This procedure finds TODO
   */ 
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  varStrId NUMBER;
  varPolicy VARCHAR2(100);
  varDiskServer VARCHAR2(2048);
  varMountPoint VARCHAR2(2048);
  varPath  VARCHAR2(2048);
  varDiskCopyId NUMBER:=0;
  varCastorFileId NUMBER;
  varFileId NUMBER;
  varNsHost VARCHAR2(2048);
  varFileSize  INTEGER;
  varTapeCopyId  INTEGER:=0;
  varLastUpdateTime NUMBER;
  varLastKnownName VARCHAR2(2048);
  varTgRequestId NUMBER;
  varUnused INTEGER;
BEGIN
  outRet:=0;
  BEGIN
    SELECT TGR.Stream, TGR.id INTO varStrId, varTgRequestId
      FROM TapeGatewayRequest TGR
     WHERE TGR.VDQMVolReqId = inVDQMtransacId;

    SELECT T.VID INTO outVid
      FROM Tape T
     WHERE T.Id IN
         (SELECT S.Tape
            FROM Stream S
           WHERE S.Id = varStrId);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    outRet:=-2;   -- stream is over
    RETURN;
  END;

  BEGIN  
    SELECT 1 INTO varUnused FROM dual 
      WHERE EXISTS (SELECT 'x' FROM Stream2TapeCopy STTC 
                      WHERE STTC.parent=varStrId);
  EXCEPTION WHEN NO_DATA_FOUND THEN
    outRet:=-1;   -- no more files
    RETURN;
  END;
 -- lock to avoid deadlock with mighunter 
  SELECT S.Id INTO varUnused FROM Stream S WHERE S.Id=varStrId 
     FOR UPDATE OF S.Id;
  -- get the policy name and execute the policy
  /* BEGIN */
  SELECT TP.migrSelectPolicy INTO varPolicy
    FROM Stream S, TapePool TP
   WHERE S.Id = varStrId
     AND S.tapePool = TP.Id;
  -- check for NULL value
  IF varPolicy IS NULL THEN
    varPolicy := 'defaultMigrSelPolicy';
  END IF;
  /* Commenting out this catch as stream with no tape pool is an error condition
  TODO: Check and drop entirely.
  EXCEPTION WHEN NO_DATA_FOUND THEN
    varPolicy := 'defaultMigrSelPolicy';
  END;*/

  IF  varPolicy = 'repackMigrSelPolicy' THEN
    -- repack policy
    tg_repackMigrSelPolicy(varStrId,varDiskServer,varMountPoint,varPath,
      varDiskCopyId ,varCastorFileId,varFileId,varNsHost,varFileSize,
      varTapeCopyId,varLastUpdateTime);
  ELSIF  varPolicy = 'drainDiskMigrSelPolicy' THEN
    -- drain disk policy
    tg_drainDiskMigrSelPolicy(varStrId,varDiskServer,varMountPoint,varPath,
      varDiskCopyId ,varCastorFileId,varFileId,varNsHost,varFileSize,
      varTapeCopyId,varLastUpdateTime);
  ELSE
    -- default
    tg_defaultMigrSelPolicy(varStrId,varDiskServer,varMountPoint,varPath,
      varDiskCopyId ,varCastorFileId,varFileId,varNsHost,varFileSize,
      varTapeCopyId,varLastUpdateTime);
  END IF;

  IF varTapeCopyId = 0 OR varDiskCopyId=0 THEN
    outRet := -1; -- the migration selection policy didn't find any candidate
    COMMIT; -- TODO: Check if ROLLBACK is not better...
    RETURN;
  END IF;

  -- Here we found a tapeCopy and we process it
  -- update status of selected tapecopy and stream
  -- Sanity check: There should be no tapecopies for the same castor file where
  -- the volume ID is the same.
  DECLARE
    varConflicts NUMBER;
  BEGIN
    SELECT COUNT(*) INTO varConflicts
      FROM TapeCopy TC
     WHERE TC.CastorFile = varCastorFileId
       AND TC.VID = outVID;
    IF (varConflicts != 0) THEN
      RAISE_APPLICATION_ERROR (-20119, 'About to move a second copy to the same tape!');
    END IF;
  END;
  UPDATE TapeCopy TC 
     SET TC.Status = 3, -- SELECTED
         TC.VID = outVID
   WHERE TC.Id = varTapeCopyId;
  -- detach the tapecopy from the stream now that it is SELECTED;
  DELETE FROM Stream2TapeCopy STTC
   WHERE STTC.child = varTapeCopyId;
   
  SELECT CF.lastKnownFileName INTO varLastKnownName
    FROM CastorFile CF
   WHERE CF.Id = varCastorFileId; -- we rely on the check done before TODO: which check?

  DECLARE
    varNewFseq NUMBER;
  BEGIN
   -- Atomically increment and read the next FSEQ to be written to
   UPDATE TapeGatewayRequest TGR
     SET TGR.lastfseq=TGR.lastfseq+1
     WHERE TGR.Id=varTgRequestId
     RETURNING TGR.lastfseq-1 into varNewFseq; -- The previous value is where we'll write
   
   UPDATE TapeCopy TC
      SET TC.fSeq = varNewFseq,
          TC.tapeGatewayRequest=varTgRequestId,
          TC.diskCopy=varDiskCopyId
    WHERE TC.Id = varTapeCopyId;

   OPEN outputFile FOR
     SELECT varFileId,varNshost,varLastUpdateTime,varDiskServer,varMountPoint,
            varPath,varLastKnownName,TC.fseq,varFileSize,varTapeCopyId
       FROM TapeCopy TC
      WHERE TC.Id = varTapeCopyId;

  END;
  COMMIT;
END;
/

/* TODO: The outputFile to the C++ contains a reference to the tapegateway subrequest... Have to clean up there too */

/* Take care of the fallout of the schema change */
create or replace
PROCEDURE cancelRecallForTape (inVid IN VARCHAR2) AS
BEGIN
  FOR a IN (SELECT DISTINCT(DiskCopy.id), DiskCopy.castorfile
              FROM Segment, Tape, TapeCopy, DiskCopy
             WHERE Segment.tape = Tape.id
               AND Segment.copy = TapeCopy.id
               AND DiskCopy.castorfile = TapeCopy.castorfile
               AND DiskCopy.status = 2  -- WAITTAPERECALL
               AND Tape.vid = inVid
             ORDER BY DiskCopy.id ASC)
  LOOP
    cancelRecall(a.castorfile, a.id, 7);
  END LOOP;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_attachDriveReqToTape(
  inTapeRequestId IN NUMBER,
  inVdqmId   IN NUMBER, 
  inDgn      IN VARCHAR2, 
  inLabel    IN VARCHAR2,
  inDensity  IN VARCHAR2) AS
BEGIN

  -- update tapegatewayrequest which have been submitted to vdqm =>  WAITING_TAPESERVER
  UPDATE TapeGatewayRequest TGR
     SET TGR.status           = 2, -- TG_REQUEST_WAITING_TAPESERVER
         TGR.lastvdqmpingtime = gettime(),
         TGR.starttime        = gettime(), 
         TGR.vdqmvolreqid     = inVdqmId
   WHERE TGR.id=inTapeRequestId; -- these are the ones waiting for vdqm to be sent again

  -- update stream for migration    
  UPDATE Stream S
     SET S.Status = 1 -- STREAM_WAITDRIVE
   WHERE S.id = 
      (SELECT TGR.stream FROM TapeGatewayRequest TGR 
        WHERE TGR.accessMode = 1 --  TG_REQUEST_ACCESSMODE_WRITE
          AND TGR.id=inTapeRequestId);  

  -- update tape for migration and recall    
  UPDATE Tape T
     SET T.Status  = 2, -- TAPE_WAITDRIVE
         T.dgn     = inDgn,
         T.label   = inLabel,
         T.density = inDensity 
   WHERE id = 
      (SELECT S.Tape
         FROM Stream S, TapeGatewayRequest TGR 
        WHERE S.id = TGR.Stream
          AND TGR.accessmode=1  --  TG_REQUEST_ACCESSMODE_WRITE
          AND TGR.id=inTapeRequestId) 
      OR id =
      (SELECT TGR.taperecall 
         FROM TapeGatewayRequest TGR
        WHERE accessmode=0  -- TG_REQUEST_ACCESSMODE_READ
          AND TGR.Id=inTapeRequestId);  
  COMMIT;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_attachTapesToStreams (
  inStartFseqs IN castor."cnumList",
  inStrIds     IN castor."cnumList", 
  inTapeVids   IN castor."strList") AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
  varTapeId NUMBER;
BEGIN
  FOR i IN inStrIds.FIRST .. inStrIds.LAST LOOP
    varTapeId:=NULL;
    -- update tapegatewayrequest
    UPDATE TapeGatewayRequest TGR
       SET TGR.Status = 1, -- TG_REQUEST_TO_BE_SENT_TO_VDQM
           TGR.lastfseq = inStartfseqs(i) 
     WHERE TGR.Stream = inStrIds(i); -- TO_BE_SENT_TO_VDQM
    UPDATE Tape T
       SET T.Stream = inStrIds(i),
           T.Status = 2 -- TAPE_WAITDRIVE
     WHERE T.tpmode= 1 -- TAPE_MODE_WRITE
       AND T.vid=inTapeVids(i) 
    RETURNING T.Id INTO varTapeId;
    IF varTapeId IS NULL THEN 
      BEGIN
        -- insert a tape if it is not already in the database
        INSERT INTO Tape T
               (          vid, side, tpmode, errmsgtxt, errorcode, severity,
                vwaddress,              id, stream,    status) 
        VALUES (inTapeVids(i),    0,      1,      null,         0,        0, -- 1 TAPE_MODE_WRITE
                     null, ids_seq.nextval,     0,          2) -- TAPE_WAITDRIVE
        RETURNING T.id INTO varTapeId;
        INSERT INTO id2type (id,type) values (varTapeId,29);
      EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- retry the select since a creation was done in between
        UPDATE Tape T
           SET T.Stream = inStrIds(i),
               T.Status = 2 -- TAPE_WAITDRIVE
         WHERE T.tpmode = 1 -- TAPE_MODE_WRITE
           AND T.vid = inTapeVids(i) 
        RETURNING T.id INTO varTapeId;
      END;
    END IF;
    -- update the stream
    UPDATE Stream S SET S.tape = varTapeId 
     WHERE S.id = inStrIds(i); 
  END LOOP;
  COMMIT;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_deleteStream(inStrId IN NUMBER) AS
  varUnused NUMBER;
  varTcIds  "numList"; -- TapeCopy Ids
  varTgrId   NUMBER;   -- TapeGatewayRequest Id
BEGIN
  -- First drop dependant TapeGatewayRequests.
  DELETE FROM tapegatewayrequest TGR
   WHERE TGR.Stream = inStrId RETURNING TGR.id INTO varTgrId;
  DELETE FROM id2type ITT WHERE ITT.Id = varTgrId;
  SELECT S.id INTO varUnused FROM Stream S 
   WHERE S.id = inStrId FOR UPDATE;
  -- Disconnect the tapecopies
  DELETE FROM stream2tapecopy STTC
   WHERE STTC.parent = inStrId 
  RETURNING STTC.child BULK COLLECT INTO varTcIds;
  -- Hand back the orphaned tape copies to the MigHunter (by setting back their
  -- statues, mighunter will pick them up on it).
  FORALL i IN varTcIds.FIRST .. VarTcIds.LAST
    UPDATE tapecopy TC
       SET TC.status = 1 -- TAPECOPY_TOBEMIGRATED
     WHERE TC.Id = varTcIds(i)
       AND NOT EXISTS (SELECT 'x' FROM stream2tapecopy STTC 
                        WHERE STTC.child = varTcIds(i));
  -- Finally drop the stream itself
  DELETE FROM id2type ITT where ITT.id = inStrId;
  DELETE FROM Stream S where S.id= inStrId;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_deleteTapeRequest( inTGReqId IN NUMBER ) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varTpReqId NUMBER;     -- Tape Recall (TapeGatewayReequest.TapeRecall)
  varStrId NUMBER;       -- Stream Id.
  varAccessMode INTEGER; -- TGRequest.Accessmode
  varSegNum INTEGER;
  varCfId NUMBER;        -- CastorFile Id
  varTcIds "numList";    -- Tapecopies IDs
  varSrIds "numList";
BEGIN
 --delete the tape request. subrequests don't exist
  DELETE FROM TapeGatewayRequest TGR
   WHERE TGR.id = inTGReqId 
     AND TGR.status = 1 -- TO_BE_SENT_TO_VDQM  
  RETURNING TGR.taperecall, TGR.stream, TGR.accessmode
    INTO        varTpReqId,   varStrId,  varAccessMode;

  DELETE FROM id2type ITT WHERE ITT.id = inTGReqId;
  IF varAccessMode = 0 THEN
    -- Lock and reset the tape in case of a read
    UPDATE Tape T
      SET T.status = 0 -- TAPE_UNUSED
      WHERE T.id = varTpReqId;
    SELECT SEG.copy BULK COLLECT INTO varTcIds 
      FROM Segment SEG 
     WHERE SEG.tape = varTpReqId;
    FOR i IN varTcIds.FIRST .. varTcIds.LAST  LOOP
      -- lock castorFile	
      SELECT TC.castorFile INTO varCfId 
        FROM TapeCopy TC, CastorFile CF
        WHERE TC.id = varTcIds(i) 
        AND CF.id = TC.castorfile 
        FOR UPDATE OF CF.id;
      -- fail diskcopy, drop tapecopies
      UPDATE DiskCopy DC SET DC.status = 4 -- DISKCOPY_FAILED
       WHERE DC.castorFile = varCfId 
         AND DC.status = 2; -- DISKCOPY_WAITTAPERECALL   
      deleteTapeCopies(varCfId);
      -- Fail the subrequest
      UPDATE SubRequest SR
         SET SR.status = 7, -- SUBREQUEST_FAILED
             SR.getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED (not strictly correct but the request is over anyway)
             SR.lastModificationTime = getTime(),
             SR.errorCode = 1015,  -- SEINTERNAL
             SR.errorMessage = 'File recall from tape has failed, please try again later',
             SR.parent = 0
       WHERE SR.castorFile = varCfId 
         AND SR.status IN (4, 5); -- WAITTAPERECALL, WAITSUBREQ
    END LOOP;
  ELSIF varAccessMode = 1 THEN
    -- In case of a write, reset the stream
    DeleteOrStopStream (varStrId);
  ELSE
    -- Wrong Access Mode encountered. Notify.
    RAISE_APPLICATION_ERROR(-20292, 'tg_deleteTapeRequest:Unexpected access mode '
      ||varAccessMode||' for TapeGatewayRequest.Id '||inTGReqId||'.');
  END IF;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_endTapeSession(inTransId IN NUMBER, inErrorCode IN INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -02292);
  varUnused NUMBER;
  varTpId NUMBER;        -- TapeGateway Taperecall
  varTrgId NUMBER;       -- TapeGatewayRequest ID
  varStrId NUMBER;       -- Stream ID
  varAccessMode INTEGER; -- Access mode
  varSegNum INTEGER;     -- Segment count
  varTcIds "numList";    -- TapeCopy Ids
BEGIN
  BEGIN
  -- Find the Tape Gateway Request and lock
    SELECT   TGR.id, TGR.taperecall, TGR.stream, TGR.accessmode
      INTO varTrgId,        varTpId,   varStrId,  varAccessMode 
      FROM TapeGatewayRequest TGR
      WHERE vdqmvolreqid = inTransId FOR UPDATE;
    -- Find and lock the TapeCopies
    SELECT TC.id BULK COLLECT INTO varTcIds
      FROM TapeCopy TC
     WHERE TC.TapeGatewayRequest = varTrgId
       FOR UPDATE OF TC.id;
    -- Delete Tape Gateway request
    DELETE FROM TapeGatewayRequest TGR
     WHERE TGR.id=varTrgId;
    DELETE FROM id2type ITT WHERE ITT.id=varTrgId;
    IF varAccessMode = 0 THEN -- This was a read request
      SELECT id INTO varUnused
        FROM Tape
       WHERE id = varTpId
         FOR UPDATE;
      IF inErrorCode != 0 THEN
          -- if a failure is reported
          -- fail all the segments
          FORALL i in varTcIds.FIRST .. varTcIds.LAST
            UPDATE Segment
              SET status=6  -- SEGMENT_FAILED
              WHERE copy = varTcIds(i);
          -- mark tapecopies as  REC_RETRY
          FORALL i in varTcIds.FIRST .. varTcIds.LAST
            UPDATE TapeCopy
              SET status    = 8, -- TAPECOPY_REC_RETRY
                  errorcode = inErrorCode
              WHERE id=varTcIds(i);
      END IF;
      -- resurrect lost segments
      UPDATE Segment SEG
         SET SEG.status = 0  -- SEGMENT_UNPROCESSED
       WHERE SEG.status = 7  -- SEGMENT_SELECTED
         AND SEG.tape = varTpId;
      -- check if there is work for this tape
      SELECT count(*) INTO varSegNum
        FROM segment SEG
       WHERE SEG.Tape = varTpId
         AND status = 0;  -- SEGMENT_UNPROCESSED
      -- Restart the unprocessed segments' tape if there are any.
      IF varSegNum > 0 THEN
        UPDATE Tape T
           SET T.status = 8 -- TAPE_WAITPOLICY for rechandler
         WHERE T.id=varTpId;
      ELSE
        UPDATE Tape
           SET status = 0 -- TAPE_UNUSED
         WHERE id=varTpId;
       END IF;
    ELSE
      -- write
      deleteOrStopStream(varStrId);
      IF inErrorCode != 0 THEN
        -- if a failure is reported
        -- retry MIG_RETRY
        FORALL i in varTcIds.FIRST .. varTcIds.LAST
          UPDATE TapeCopy TC 
             SET TC.status=9,  -- TAPECOPY_MIG_RETRY
                 TC.errorcode=inErrorCode,
                 TC.nbretry=0
           WHERE TC.id=varTcIds(i);
      ELSE
        -- just resurrect them if they were lost
        FORALL i in varTcIds.FIRST .. varTcIds.LAST
          UPDATE TapeCopy TC
             SET TC.status = 1 -- TAPECOPY_TOBEMIGRATED
           WHERE TC.id=varTcIds(i) 
             AND TC.status = 3; -- TAPECOPY_SELECT
      END IF;
    END IF;
    EXCEPTION WHEN  NO_DATA_FOUND THEN
      null;
    END;
    COMMIT;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_failFileTransfer(
  inTransId      IN NUMBER,    -- The VDQM transaction ID
  inFileId    IN NUMBER,       -- File ID
  inNsHost    IN VARCHAR2,     -- NS Host
  inFseq      IN INTEGER,      -- Tapecopy's fSeq
  inErrorCode IN INTEGER)  AS  -- Error Code
  varUnused NUMBER;            -- dummy
  varTgrId NUMBER;             -- Tape Gateway Request Id
  varStrId NUMBER;             -- Stream Id
  varTpId NUMBER;              -- Tape Id
  varTcId NUMBER;              -- TapeCopy Id
  varAccessMode INTEGER;       -- Access mode
BEGIN
  SELECT TGR.accessmode,   TGR.id, TGR.taperecall, TGR.stream
    INTO  varAccessMode, varTgrId,        varTpId,   varStrId 
    FROM TapeGatewayRequest TGR
   WHERE TGR.vdqmvolreqid = inTransId
     FOR UPDATE;
  SELECT CF.id INTO varUnused 
    FROM CastorFile CF
   WHERE CF.fileid = inFileId
     AND CF.nsHost = inNsHost 
    FOR UPDATE;
  IF varAccessMode = 0 THEN -- This was a read request
     -- fail the segment on that tape
     UPDATE Segment SEG
        SET SEG.status    = 6, -- SEGMENT_FAILED
            SEG.severity  = inErrorCode,
            SEG.errorCode = -1 
      WHERE SEG.fseq = inFseq 
        AND SEG.tape = varTpId 
     RETURNING SEG.copy INTO varTcId;
     -- mark tapecopy as REC_RETRY
     UPDATE TapeCopy TC
        SET TC.status    = 8, -- TAPECOPY_REC_RETRY
            TC.errorcode = inErrorCode 
      WHERE TC.id = varTcId;  
  ELSE  -- This was a migration (write)
     SELECT T.id INTO varTpId 
       FROM Tape T, Stream  S
      WHERE T.id = S.tape
        AND S.id = varStrId;
     -- mark tapecopy as MIG_RETRY
     UPDATE TapeCopy TC
       SET TC.status    = 9, -- TAPECOPY_MIG_RETRY
           TC.errorcode = inErrorCode 
      WHERE TC.id=varTcId; 
  END IF;  
EXCEPTION WHEN  NO_DATA_FOUND THEN
  null;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE  tg_getFailedMigrations(outTapeCopies_c OUT castor.TapeCopy_Cur) AS
BEGIN
  -- get TAPECOPY_MIG_RETRY
  OPEN outTapeCopies_c FOR
    SELECT *
      FROM TapeCopy TC
     WHERE TC.status = 9 -- TAPECOPY_MIG_RETRY
       AND ROWNUM < 1000 
       FOR UPDATE SKIP LOCKED; 
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE  tg_getFailedRecalls(outTapeCopies_c OUT castor.TapeCopy_Cur) AS
BEGIN
  -- get TAPECOPY_REC_RETRY
  OPEN outTapeCopies_c FOR
    SELECT *
      FROM TapeCopy TC
     WHERE TC.status = 8 -- TAPECOPY_REC_RETRY
      AND ROWNUM < 1000 
      FOR UPDATE SKIP LOCKED;
END;
/


/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_getFileToRecall (inTransId IN  NUMBER, outRet OUT INTEGER,
  outVid OUT NOCOPY VARCHAR2, outFile OUT castorTape.FileToRecallCore_Cur) AS
  varTgrId         INTEGER; -- TapeGateway Request Id
  varDSName VARCHAR2(2048); -- Disk Server name
  varMPoint VARCHAR2(2048); -- Mount Point
  varPath   VARCHAR2(2048); -- Diskcopy path
  varSegId          NUMBER; -- Segment Id
  varDcId           NUMBER; -- Disk Copy Id
  varTcId           NUMBER; -- Tape Copy Id
  varTapeId         NUMBER; -- Tape Id
  varNewFSeq       INTEGER; -- new FSeq
  varUnused         NUMBER;
BEGIN 
  outRet:=0;
  BEGIN
    -- master lock on the taperequest
    SELECT TGR.id, TGR.taperecall INTO varTgrId, varTapeId 
      FROM TapeGatewayRequest TGR
     WHERE TGR.vdqmVolReqId = inTransId 
       FOR UPDATE;
    SELECT T.vid INTO outVid 
      FROM Tape T
      WHERE T.id = varTapeId;
  EXCEPTION WHEN  NO_DATA_FOUND THEN
     outRet:=-2; -- ERROR
     RETURN;
  END; 
  BEGIN
    -- Find the unprocessed segment of this tape with lowest fSeq
    SELECT   SEG.id,   SEG.fSeq, SEG.Copy 
      INTO varSegId, varNewFSeq, varTcId 
      FROM Segment SEG
     WHERE SEG.tape = varTapeId  
       AND SEG.status = 0 -- SEGMENT_UNPROCESSED
       AND ROWNUM < 2
     ORDER BY SEG.fseq ASC;
    -- Lock the corresponding castorfile
    SELECT CF.id INTO varUnused 
      FROM Castorfile CF, TapeCopy TC
     WHERE CF.id = TC.castorfile 
       AND TC.id = varTcId 
       FOR UPDATE OF CF.id;
  EXCEPTION WHEN NO_DATA_FOUND THEN
     outRet := -1; -- NO MORE FILES
     COMMIT;
     RETURN;
  END;
  DECLARE
    application_error EXCEPTION;
    PRAGMA EXCEPTION_INIT(application_error,-20115);
  BEGIN
    bestFileSystemForSegment(varSegId,varDSName,varMPoint,varPath,varDcId);
  EXCEPTION WHEN application_error  OR NO_DATA_FOUND THEN 
    outRet := -3;
    COMMIT;
    RETURN;
  END;
  -- Update the TapeCopy's parameters
  UPDATE TapeCopy TC
     SET TC.fseq = varNewFSeq,
         TC.TapeGatewayRequest = varTgrId,
         TC.DiskCopy = varDcId,
         TC.FileTransactionID = TG_FileTrId_Seq.NEXTVAL
   WHERE TC.id = varTcId;
   -- Update the segment's status
  UPDATE Segment SEG 
     SET SEG.status = 7 -- SEGMENT_SELECTED
   WHERE SEG.id=varSegId 
     AND SEG.status = 0; -- SEGMENT_UNPROCESSED
  OPEN outFile FOR 
    SELECT CF.fileid, CF.nshost, varDSName, varMPoint, varPath, varNewFSeq , 
           TC.FileTransactionID
      FROM TapeCopy TC, Castorfile CF
     WHERE TC.id = varTcId
       AND CF.id=TC.castorfile;
END;
/



/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/


create or replace
PROCEDURE tg_getRepackVidAndFileInfo(
  inFileId          IN  NUMBER, 
  inNsHost          IN VARCHAR2,
  inFseq            IN  INTEGER, 
  inTransId         IN  NUMBER, 
  inBytesTransfered IN  NUMBER,
  outRepackVid     OUT NOCOPY VARCHAR2, 
  outVID           OUT NOCOPY VARCHAR,
  outCopyNb        OUT INTEGER, 
  outLastTime      OUT NUMBER, 
  outRet           OUT INTEGER) AS 
  varCfId NUMBER;      -- CastorFile Id
  varFileSize NUMBER;  -- Expected file size
BEGIN
  outRepackVid:=NULL;
   -- ignore the repack state
  SELECT CF.lastupdatetime, CF.id, CF.fileSize 
    INTO outLastTime, varCfId, varFileSize
    FROM CastorFile CF
   WHERE CF.fileid = inFileId 
     AND CF.nshost = inNsHost;
  IF varFileSize <> inBytesTransfered THEN
  -- fail the file
    tg_failFileTransfer(inTransId,inFileId, inNsHost, inFseq,  1613); -- wrongfilesize
    COMMIT;
    outRet := -1;
    RETURN;
  ELSE
    outRet:=0;
  END IF;
      
  SELECT TC.copyNb INTO outcopynb 
    FROM TapeCopy TC, TapeGatewayRequest TGR  
   WHERE TGR.id = TC.TapeGatewayRequest
     AND TC.castorfile = varCfId
     AND TC.fseq= inFseq
     AND TGR.vdqmVolReqId = inTransId;
 
  SELECT T.vid INTO outVID 
    FROM Tape T, Stream S, TapeGatewayRequest TGR 
   WHERE T.id = S.tape
     AND TGR.stream = S.id
     AND TGR.vdqmVolReqId = inTransId;

  BEGIN 
   --REPACK case
    -- Get the repackvid field from the existing request (if none, then we are not in a repack process
     SELECT SRR.repackvid INTO outRepackVid
       FROM SubRequest sR, StageRepackRequest SRR
      WHERE SRR.id = SR.request
        AND sR.status = 12 -- SUBREQUEST_REPACK
        AND sR.castorFile = varCfId
        AND ROWNUM < 2;
  EXCEPTION WHEN NO_DATA_FOUND THEN
   -- standard migration
    NULL;
  END;
END;
/


/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/


create or replace
PROCEDURE tg_getSegmentInfo(
  inTransId     IN  NUMBER,
  inFileId      IN  NUMBER, 
  inHost        IN  VARCHAR2,
  inFseq        IN  INTEGER, 
  outVid       OUT NOCOPY VARCHAR2, 
  outCopyNb    OUT INTEGER ) AS
  varTrId NUMBER;
BEGIN
  SELECT T.vid, TGR.id  INTO outVid, varTrId 
    FROM Tape T, TapeGatewayRequest TGR
   WHERE T.id=TGR.taperecall 
     AND TGR.vdqmvolreqid= inTransId
     AND T.tpmode = 0;

  SELECT TC.copynb INTO outCopyNb 
    FROM TapeCopy TC
   WHERE TC.fseq = inFseq
     AND TC.TapeGateWayRequest = varTrId;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/


create or replace
PROCEDURE tg_getStreamsWithoutTapes(outStrList OUT castorTape.Stream_Cur) AS
BEGIN
  -- get request in status TO_BE_RESOLVED
  OPEN outStrList FOR
    SELECT S.id, S.initialsizetotransfer, S.status, S.tapepool, TP.name
      FROM Stream S,Tapepool TP
     WHERE S.id IN 
        ( SELECT TGR.stream
            FROM TapeGatewayRequest TGR
           WHERE TGR.status=0 ) 
      AND S.tapepool=TP.id 
      FOR UPDATE OF S.id SKIP LOCKED;   
END;
/


/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_getTapesWithDriveReqs(
  inTimeLimit     IN  NUMBER,
  outTapeRequest OUT castorTape.tapegatewayrequest_Cur) AS
  varTgrId        "numList";
BEGIN 
  -- get requests in WAITING_TAPESERVER and ONGOING
  SELECT TGR.id BULK COLLECT INTO varTgrId 
    FROM TapeGatewayRequest TGR
   WHERE TGR.status IN (2,3)   -- WAITING_TAPESERVER and ONGOING
     AND gettime() - TGR.lastVdqmPingTime > inTimeLimit 
     FOR UPDATE SKIP LOCKED; 
	
  FORALL i IN varTgrId.FIRST .. varTgrId.LAST
  UPDATE TapeGatewayRequest TGR
     SET TGR.lastVdqmPingTime = gettime() 
   WHERE TGR.id = varTgrId(i);
 
  OPEN outTapeRequest FOR
      SELECT TGR.accessMode, TGR.id, TGR.starttime,
             TGR.lastvdqmpingtime, TGR.vdqmVolReqid,
             TGR.status, T.vid  
        FROM Stream S, TapeGatewayRequest TGR,Tape T
       WHERE TGR.id IN 
            (SELECT /*+ CARDINALITY(trTable 5) */ * 
               FROM TABLE(varTgrId) trTable)  
         AND S.id = TGR.stream
         AND S.tape=T.id 
         AND TGR.accessMode=1 -- Write

       UNION ALL

      SELECT TGR.accessMode, TGR.id, TGR.starttime, 
             TGR.lastvdqmpingtime, TGR.vdqmvolreqid,
             TGR.status, T.vid  
        FROM tapegatewayrequest TGR, Tape T
        WHERE T.id=TGR.tapeRecall 
        AND TGR.accessMode=0 -- Read
        AND TGR.id IN 
            (SELECT /*+ CARDINALITY(trTable 5) */ * 
             FROM TABLE(varTgrId) trTable);
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_getTapeToRelease(
  inVdqmReqId IN  INTEGER, 
  outVID      OUT NOCOPY VARCHAR2, 
  outMode     OUT INTEGER ) AS
  varStrId        NUMBER;
  varTpId         NUMBER;
BEGIN
  SELECT TGR.accessmode, TGR.stream, TGR.tapeRecall
    INTO        outMode,   varStrId,        varTpId
    FROM TapeGatewayRequest TGR
   WHERE TGR.vdqmvolreqid = inVdqmReqId;
   IF outMode = 0 THEN -- read case
     SELECT T.vid INTO outVID 
       FROM Tape T
       WHERE T.id = varTpId; 
   ELSE -- write case
     SELECT T.vid INTO outVID 
       FROM Tape T,Stream S
      WHERE S.id=varStrId
        AND S.tape=T.id;
   END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- already cleaned by the checker
  NULL;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_getTapeWithoutDriveReq(
  outReqId    OUT NUMBER,
  outTapeMode OUT NUMBER,
  outTapeSide OUT INTEGER,
  outTapeVid  OUT NOCOPY VARCHAR2) AS
BEGIN
  -- get a request in TO_BE_SENT_TO_VDQM
  SELECT TGR.id, TGR.accessmode INTO outReqId, outTapeMode 
    FROM TapeGatewayRequest TGR
   WHERE TGR.status=1 AND ROWNUM < 2 FOR UPDATE SKIP LOCKED;
  IF outTapeMode = 0 THEN
    -- read
    SELECT    T.tpMode,      T.side,      T.vid 
      INTO outTapeMode, outTapeSide, outTapeVid
      FROM TapeGatewayRequest TGR, Tape T
     WHERE TGR.id = outReqId
       AND T.id = TGR.tapeRecall;
  ELSE
     -- write  
    SELECT    T.tpMode,      T.side,      T.vid
      INTO outTapeMode, outTapeSide, outTapeVid
      FROM TapeGatewayRequest TGR, Tape T, Stream S
     WHERE TGR.id = outReqId
       AND S.id = TGR.stream 
       AND S.tape=T.id;
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  outReqId:=0; 
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_invalidateFile(
  inTransId   IN NUMBER,
  inFileId    IN NUMBER, 
  inNsHost    IN VARCHAR2,
  inFseq      IN INTEGER,
  inErrorCode IN INTEGER) AS
BEGIN
  UPDATE TapeGatewayRequest TGR
     SET TGR.lastfseq = TGR.lastfseq-1
   WHERE TGR.vdqmvolreqid = inTransId;
  tg_failfiletransfer(inTransId, inFileId, inNsHost, inFseq, inErrorCode);
END;
/


/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE TG_SetFileMigrated(
  inTransId         IN  NUMBER, 
  inFileId          IN  NUMBER,
  inNsHost          IN  VARCHAR2, 
  inFseq            IN  INTEGER, 
  inFileTransaction IN  NUMBER,
  outStreamReport   OUT castor.StreamReport_Cur) AS
  varUnused             NUMBER;
  varTapeCopyCount      INTEGER;
  varCfId               NUMBER;
  varTcId               NUMBER;
  varDCId               NUMBER;
  varTcIds              "numList";
BEGIN
  -- Find TapeGateWay request ID and Lock
  SELECT TGR.id
    INTO varUnused
    FROM TapeGatewayRequest TGR
   WHERE TGR.vdqmvolreqid = inTransId 
     FOR UPDATE;
  -- Lock the CastorFile
  SELECT CF.id INTO varCfId FROM CastorFile CF
   WHERE CF.fileid = inFileId 
     AND CF.nsHost = inNsHost 
     FOR UPDATE;
  -- Locate the corresponding tape copy and Disk Copy, Lock
  SELECT   TC.id, TC.DiskCopy
    INTO varTcId,     varDcId
    FROM TapeCopy TC
   WHERE TC.FileTransactionId = inFileTransaction
     AND TC.fSeq = inFseq
     FOR UPDATE;
  UPDATE tapecopy TC
     SET TC.status = 5 -- TAPECOPY_STAGED
   WHERE TC.id = varTcId;
  SELECT count(*) INTO varTapeCopyCount
    FROM tapecopy TC
    WHERE TC.castorfile = varCfId  
     AND STATUS != 5; -- TAPECOPY_STAGED
  -- let's check if another copy should be done, if not, we're done for this file.
  IF varTapeCopyCount = 0 THEN
     -- Mark all disk copies as staged and delete all tape copies together.
     UPDATE DiskCopy DC
        SET DC.status=0   -- DISKCOPY_STAGED
      WHERE DC.castorFile = varCfId
        AND DC.status=10; -- DISKCOPY_CANBEMIGR
     DELETE FROM tapecopy TC
      WHERE castorfile = varCfId 
  RETURNING id BULK COLLECT INTO varTcIds;
     FORALL i IN varTcIds.FIRST .. varTcIds.LAST
       DELETE FROM id2type 
         WHERE id=varTcIds(i);
  END IF;
  -- archive Repack requests should any be in the db
  FOR i IN (
    SELECT SR.id FROM SubRequest SR
    WHERE SR.castorfile = varCfId AND
          SR.status=12 -- SUBREQUEST_REPACK
    ) LOOP
      archivesubreq(i.id, 8); -- SUBREQUEST_FINISHED
  END LOOP;
  -- return data for informing the rmMaster
  OPEN outStreamReport FOR
   SELECT DS.name,FS.mountpoint 
     FROM DiskServer DS,FileSystem FS, DiskCopy DC 
    WHERE DC.id = varDcId 
      AND DC.filesystem = FS.id 
      AND FS.diskserver = DS.id;
  COMMIT;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE tg_setFileRecalled(
  inTransId          IN  NUMBER, 
  inFileId           IN  NUMBER,
  inNsHost           IN  VARCHAR2, 
  inFseq             IN  NUMBER, 
  inFileTransaction  IN  NUMBER,
  outStreamReport   OUT castor.StreamReport_Cur) AS
  varTcId               NUMBER;         -- TapeCopy Id
  varDcId               NUMBER;         -- DiskCopy Id
  varCfId               NUMBER;         -- CastorFile Id
  srId NUMBER;
  varSubrequestId       NUMBER; 
  varRequestId          NUMBER;
  varFileSize           NUMBER;
  varGcWeight           NUMBER;         -- Garbage collection weight
  varGcWeightProc       VARCHAR(2048);  -- Garbage collection weighting procedure name
  varEuid               INTEGER;        -- Effective user Id
  varEgid               INTEGER;        -- Effective Group Id
  varSvcClassId         NUMBER;         -- Service Class Id
  varMissingCopies      INTEGER;
  varUnused             NUMBER;
BEGIN
  SAVEPOINT TGReq_CFile_TCopy_Locking;
  -- take lock on the tape gateway request
  SELECT TGR.id INTO varUnused
    FROM TapeGatewayRequest TGR
   WHERE TGR.vdqmvolreqid = inTransId
     FOR UPDATE;
  -- find and lock castor file for the nsHost/fileID
  SELECT CF.id, CF.fileSize INTO varCfId, varFileSize
    FROM CastorFile CF 
   WHERE CF.fileid = inFileId 
     AND CF.nsHost = inNsHost 
     FOR UPDATE;
  -- Find and lock the tape copy
  varTcId := NULL;
  SELECT TC.id, TC.diskcopy INTO varTcId, varDcId
    FROM TapeCopy TC
   WHERE TC.FileTransactionId = inFileTransaction
     AND TC.fSeq = inFseq
     FOR UPDATE;
  -- If nothing found, die releasing the locks
  IF varTCId = NULL THEN
    ROLLBACK TO SAVEPOINT TGReq_CFile_TCopy_Locking;
    RAISE NO_DATA_FOUND;
  END IF;
  -- missing tapecopies handling
  SELECT TC.missingCopies INTO varMissingCopies
    FROM TapeCopy TC WHERE TC.id = varTcId;
  -- delete tapecopies
  deleteTapeCopies(varCfId);
  -- update diskcopy status, size and gweight
  SELECT SR.id, SR.request
    INTO varSubrequestId, varRequestId
    FROM SubRequest SR
   WHERE SR.diskcopy = varDcId;
  SELECT REQ.svcClass, REQ.euid, REQ.egid INTO varSvcClassId, varEuid, varEgid
    FROM (SELECT id, svcClass, euid, egid FROM StageGetRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StagePrepareToGetRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StageUpdateRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StagePrepareToUpdateRequest UNION ALL
          SELECT id, svcClass, euid, egid FROM StageRepackRequest) REQ
    WHERE REQ.id = varRequestId;
  varGcWeightProc := castorGC.getRecallWeight(varSvcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || varGcWeightProc || '(:size); END;'
    USING OUT varGcWeight, IN varFileSize;
  UPDATE DiskCopy DC
    SET DC.status = decode(varMissingCopies, 0, 0, 10), -- DISKCOPY_STAGED if varMissingCopies = 0, otherwise CANBEMIGR
        DC.lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
        DC.gcWeight = varGcWeight,
        DC.diskCopySize = varFileSize
    WHERE Dc.id = varDcId;
  -- restart this subrequest so that the stager can follow it up
  UPDATE SubRequest SR
     SET SR.status = 1, SR.getNextStatus = 1,  -- SUBREQUEST_RESTART, GETNEXTSTATUS_FILESTAGED
         SR.lastModificationTime = getTime(), SR.parent = 0
   WHERE SR.id = varSubrequestId;
  -- and trigger new migrations if missing tape copies were detected
  IF varMissingCopies > 0 THEN
    DECLARE
      newTcId INTEGER;
    BEGIN
      FOR i IN 1..varMissingCopies LOOP
        INSERT INTO TapeCopy (id, copyNb, castorFile, status, nbRetry, missingCopies)
        VALUES (ids_seq.nextval, 0, varCfId, 0, 0, 0)  -- TAPECOPY_CREATED
        RETURNING id INTO newTcId;
        INSERT INTO Id2Type (id, type) VALUES (newTcId, 30); -- OBJ_TapeCopy
      END LOOP;
    END;
  END IF;
  -- restart other requests waiting on this recall
  UPDATE SubRequest SR
     SET SR.status = 1, SR.getNextStatus = 1,  -- SUBREQUEST_RESTART, GETNEXTSTATUS_FILESTAGED
         SR.lastModificationTime = getTime(), SR.parent = 0
   WHERE SR.parent = varSubrequestId;
  -- trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(varCfId, varEuid, varEgid);
  -- return data for informing the rmMaster
  OPEN outStreamReport FOR
    SELECT DS.name, FS.mountpoint 
      FROM DiskServer DS, FileSystem FS, DiskCopy DC
      WHERE DC.id = varDcId
      AND DC.filesystem = FS.id 
      AND FS.diskserver = DS.id;
  COMMIT;
END;
/

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

create or replace
PROCEDURE  tg_startTapeSession(
  inVdqmReqId    IN  NUMBER, 
  outVid         OUT NOCOPY VARCHAR2, 
  outAccessMode  OUT INTEGER, 
  outRet         OUT INTEGER, 
  outDensity     OUT NOCOPY VARCHAR2, 
  outLabel       OUT NOCOPY VARCHAR2 ) AS
  varTGReqId         NUMBER;
  varTpId            NUMBER;
  varStreamId        NUMBER;
  varUnused          NUMBER;
BEGIN
  outRet:=-2;
  -- set the request to ONGOING
  UPDATE TapeGatewayRequest TGR
     SET TGR.status       = 3 -- TG_REQUEST_ONGOING
   WHERE TGR.vdqmVolreqid = inVdqmReqId 
     AND TGR.status       = 2 -- TG_REQUEST_WAITING_TAPESERVER
  RETURNING    TGR.id, TGR.accessMode,  TGR.stream, TGR.taperecall 
    INTO varTGReqId,   outAccessMode, varStreamId,        varTpId;
  IF varTGReqId IS NULL THEN
    outRet:=-2; -- UNKNOWN request
  ELSE
    IF outAccessMode = 0 THEN
      -- read tape mounted
      BEGIN  
        SELECT 1 INTO varUnused FROM dual 
         WHERE EXISTS (SELECT 'x' FROM Segment S
                        WHERE S.tape = varTpId);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        UPDATE TapeGatewayRequest TGR 
           SET TGR.lastvdqmpingtime=0 
         WHERE TGR.id = varTGReqId; -- to force the cleanup
        outRet:=-1; --NO MORE FILES
        COMMIT;
        RETURN;
      END;
      UPDATE Tape T
         SET T.status = 4 -- TAPE_MOUNTED
       WHERE T.id = varTpId
         AND T.tpmode = 0   -- Read
      RETURNING T.vid,  T.label,  T.density 
        INTO   outVid, outLabel, outDensity; -- tape is MOUNTED 
      outRet:=0;
    ELSE 
      -- write
      BEGIN  
        SELECT 1 INTO varUnused FROM dual 
         WHERE EXISTS (SELECT 'x' FROM Stream2TapeCopy STTC
                        WHERE STTC.parent = varStreamId);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- no more files
        UPDATE TapeGatewayRequest TGR
           SET TGR.lastvdqmpingtime=0 
         WHERE TGR.id=varTGReqId; -- to force the cleanup
        outRet:=-1; --NO MORE FILES
        outVid:=NULL;
        COMMIT;
        RETURN;
      END;
      UPDATE Stream S
         SET S.status = 3 -- STREAM_RUNNING
       WHERE id = (SELECT TGR.stream
                     FROM TapeGatewayRequest TGR
                    WHERE TGR.id = varTGReqId ) 
            RETURNING tape INTO varTpId; -- RUNNING
      UPDATE Tape T
         SET T.status = 4 -- TAPE_MOUNTED
       WHERE T.id = varTpId 
      RETURNING T.vid,  T.label,  T.density 
          INTO outVid, outLabel, outDensity;
     outRet:=0;
    END IF; 
  END IF;
  COMMIT;
END;
/


/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

/* Massage the procedures once for the not-tape gateway related */
-- BEGIN
--   FOR obj in (
--     SELECT *
--       FROM user_objects
--       WHERE
--             object_type IN ('FUNCTION', 'PROCEDURE')
--         AND status = 'INVALID' AND object_name NOT LIKE 'TG_%') LOOP
--      EXECUTE IMMEDIATE 'ALTER ' || obj.object_type || ' ' || obj.object_name || ' COMPILE';
--    END LOOP;
-- END;
-- /
-- 
-- /* Massage the TG family */
-- BEGIN
--   FOR obj in (
--     SELECT *
--       FROM user_objects
--       WHERE
--             object_type IN ('FUNCTION', 'PROCEDURE')
--         AND status = 'INVALID' AND object_name LIKE 'TG_%') LOOP
--      EXECUTE IMMEDIATE 'ALTER ' || obj.object_type || ' ' || obj.object_name || ' COMPILE';
--    END LOOP;
-- END;
-- /
-- 
-- /* Find the remaining buggers */
--  SELECT *
--       FROM user_objects
--       WHERE
--             object_type IN ('FUNCTION', 'PROCEDURE')
--         AND status = 'INVALID';

-- The compilation would fail here but subsequent patching in tape_gateway_refactor_to_drop_tgrequest_and_triggers.sql will fix it.

/*******************************************************************************
********************************************************************************
********************************************************************************
*******************************************************************************/

/* TODO: Update the test environment/procedures */

