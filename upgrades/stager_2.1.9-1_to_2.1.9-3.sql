/******************************************************************************
 *              stager_2.1.9-1_to_2.1.9-3
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
 * This script upgrades a CASTOR v2.1.9-1 STAGER database into v2.1.9-3
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
DECLARE
  unused VARCHAR2(30);
BEGIN
  SELECT table_name INTO unused
    FROM user_tables
   WHERE table_name = 'UPGRADELOG';
  EXECUTE IMMEDIATE
   'UPDATE UpgradeLog
       SET failureCount = failureCount + 1
     WHERE schemaVersion = ''2_1_9_0''
       AND release = ''2_1_9_3''
       AND state != ''COMPLETE''';
  COMMIT;
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
/

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE release LIKE '2_1_9_1%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

UPDATE CastorVersion SET release = '2_1_9_3';
COMMIT;


/* Schema changes go here */
/**************************/

/* Disable all jobs */
/********************/
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs WHERE enabled = 'TRUE')
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name);
    END IF;
    dbms_scheduler.disable(a.job_name);
  END LOOP;
END;
/

/* SQL statements for table UpgradeLog */
CREATE TABLE UpgradeLog (Username VARCHAR2(64) DEFAULT sys_context('USERENV', 'OS_USER') CONSTRAINT NN_UpgradeLog_Username NOT NULL, Machine VARCHAR2(64) DEFAULT sys_context('USERENV', 'HOST') CONSTRAINT NN_UpgradeLog_Machine NOT NULL, Program VARCHAR2(48) DEFAULT sys_context('USERENV', 'MODULE') CONSTRAINT NN_UpgradeLog_Program NOT NULL, StartDate TIMESTAMP(6) WITH TIME ZONE DEFAULT sysdate, EndDate TIMESTAMP(6) WITH TIME ZONE, FailureCount NUMBER DEFAULT 0, Type VARCHAR2(20) DEFAULT 'NON TRANSPARENT', State VARCHAR2(20) DEFAULT 'INCOMPLETE', SchemaVersion VARCHAR2(20) CONSTRAINT NN_UpgradeLog_SchemaVersion NOT NULL, Release VARCHAR2(20) CONSTRAINT NN_UpgradeLog_Release NOT NULL);

/* SQL statements for check constraints on the UpgradeLog table */
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_State
  CHECK (state IN ('COMPLETE', 'INCOMPLETE'));
  
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_Type
  CHECK (type IN ('TRANSPARENT', 'NON TRANSPARENT'));

/* SQL statement to populate the intial release value */
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('2_1_9_0', '2_1_9_3');
COMMIT;

DROP TABLE CastorVersion;

/* SQL statement to create the CastorVersion view */
CREATE OR REPLACE VIEW CastorVersion
AS
  SELECT decode(type, 'TRANSPARENT', schemaVersion,
           decode(state, 'INCOMPLETE', state, schemaVersion)) schemaVersion,
         decode(type, 'TRANSPARENT', release,
           decode(state, 'INCOMPLETE', state, release)) release
    FROM UpgradeLog
   WHERE startDate =
     (SELECT max(startDate) FROM UpgradeLog);

/* Make sure the DrainingDiskCopy table has ROW MOVEMENT enabled */
DECLARE
  unused VARCHAR2(30);
BEGIN
  SELECT table_name INTO unused FROM user_tables
   WHERE table_name = 'DRAININGDISKCOPY'
     AND row_movement = 'DISABLED';
  EXECUTE IMMEDIATE 'ALTER TABLE DrainingDiskCopy ENABLE ROW MOVEMENT';
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;  -- Nothing
END;
/

/* A primary key index for better scan of Stream2TapeCopy */
DECLARE
  unused VARCHAR2(30);
BEGIN
  SELECT constraint_name INTO unused
    FROM user_constraints
   WHERE constraint_name = 'NN_STREAM2TAPECOPY_PARENT';
EXCEPTION WHEN NO_DATA_FOUND THEN
  EXECUTE IMMEDIATE 
    'ALTER TABLE Stream2TapeCopy MODIFY
       (parent CONSTRAINT NN_Stream2TapeCopy_Parent NOT NULL,
        child  CONSTRAINT NN_Stream2TapeCopy_Child NOT NULL)';
  EXECUTE IMMEDIATE
    'ALTER TABLE Stream2TapeCopy
       ADD CONSTRAINTS PK_Stream2TapeCopy_PC PRIMARY KEY (parent, child) USING INDEX';
END;
/


/* Update and revalidation of PL-SQL code */
/******************************************/

/* SQL statement for the delete trigger on the DrainingFileSystem table */
CREATE OR REPLACE TRIGGER tr_DrainingFileSystem_Delete
BEFORE DELETE ON DrainingFileSystem
FOR EACH ROW
DECLARE
  CURSOR c IS
    SELECT STDCRR.id, SR.id subrequest, STDCRR.client
      FROM StageDiskCopyReplicaRequest STDCRR, DrainingDiskCopy DDC,
           SubRequest SR
     WHERE STDCRR.sourceDiskCopy = DDC.diskCopy
       AND SR.request = STDCRR.id
       AND decode(DDC.status, 4, DDC.status, NULL) = 4  -- FAILED
       AND SR.status = 9  -- FAILED_FINISHED
       AND DDC.fileSystem = :old.fileSystem;
  reqIds    "numList";
  clientIds "numList";
  srIds     "numList";
BEGIN
  -- Remove failed transfers requests from the StageDiskCopyReplicaRequest
  -- table. If we do not do this files which failed due to "Maximum number of
  -- attempts exceeded" cannot be resubmitted to the system.
  -- (see getBestDiskCopyToReplicate)
  LOOP
    OPEN c;
    FETCH c BULK COLLECT INTO reqIds, srIds, clientIds LIMIT 10000;
    -- Break out of the loop when the cursor returns no results
    EXIT WHEN reqIds.count = 0;
    -- Delete data
    FORALL i IN reqIds.FIRST .. reqIds.LAST
      DELETE FROM Id2Type WHERE id IN (reqIds(i), clientIds(i), srIds(i));
    FORALL i IN clientIds.FIRST .. clientIds.LAST
      DELETE FROM Client WHERE id = clientIds(i);
    FORALL i IN srIds.FIRST .. srIds.LAST
      DELETE FROM SubRequest WHERE id = srIds(i);
    CLOSE c;
  END LOOP;
  -- Delete all data related to the filesystem from the draining diskcopy table
  DELETE FROM DrainingDiskCopy
   WHERE fileSystem = :old.fileSystem;
END;
/

/* PL/SQL method implementing stageForcedRm */
CREATE OR REPLACE PROCEDURE stageForcedRm (fid IN INTEGER,
                                           nh IN VARCHAR2) AS
  cfId INTEGER;
  nbRes INTEGER;
  dcsToRm "numList";
  nsHostName VARCHAR2(2048);
BEGIN
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- Lock the access to the CastorFile
  -- This, together with triggers will avoid new TapeCopies
  -- or DiskCopies to be added
  SELECT id INTO cfId FROM CastorFile
   WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
  -- list diskcopies
  SELECT id BULK COLLECT INTO dcsToRm
    FROM DiskCopy
   WHERE castorFile = cfId
     AND status IN (0, 5, 6, 10, 11);  -- STAGED, WAITFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
  -- Stop ongoing recalls
  deleteTapeCopies(cfId);
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  FOR sr IN (SELECT id, status FROM SubRequest
              WHERE diskcopy IN
                (SELECT /*+ CARDINALITY(dcidTable 5) */ *
                   FROM TABLE(dcsToRm) dcidTable)
                AND status IN (0, 1, 2, 5, 6, 12, 13)) LOOP   -- START, RESTART, RETRY, WAITSUBREQ, READY, READYFORSCHED
    UPDATE SubRequest
       SET status = 7,  -- FAILED
           errorCode = 4,  -- EINTR
           errorMessage = 'Canceled by another user request',
           parent = 0
     WHERE (id = sr.id) OR (parent = sr.id);
  END LOOP;
  -- Set selected DiskCopies to INVALID
  FORALL i IN dcsToRm.FIRST .. dcsToRm.LAST
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE id = dcsToRm(i);
END;
/


/* PL/SQL method implementing stageRm */
CREATE OR REPLACE PROCEDURE stageRm (srId IN INTEGER,
                                     fid IN INTEGER,
                                     nh IN VARCHAR2,
                                     svcClassId IN INTEGER,
                                     ret OUT INTEGER) AS
  cfId INTEGER;
  scId INTEGER;
  nbRes INTEGER;
  dcsToRm "numList";
  srIds "numList";
  srType INTEGER;
  dcStatus INTEGER;
  nsHostName VARCHAR2(2048);
BEGIN
  ret := 0;
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  BEGIN
    -- Lock the access to the CastorFile
    -- This, together with triggers will avoid new TapeCopies
    -- or DiskCopies to be added
    SELECT id INTO cfId FROM CastorFile
     WHERE fileId = fid AND nsHost = nsHostName FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- This file does not exist in the stager catalog
    -- so we just fail the request
    UPDATE SubRequest
       SET status = 7,  -- FAILED
           errorCode = 2,  -- ENOENT
           errorMessage = 'File not found on disk cache'
     WHERE id = srId;
    RETURN;
  END;
  -- First select involved diskCopies
  scId := svcClassId;
  dcStatus := 0;
  IF scId > 0 THEN
    SELECT id BULK COLLECT INTO dcsToRm FROM (
      -- first physical diskcopies
      SELECT DC.id
        FROM DiskCopy DC, FileSystem, DiskPool2SvcClass DP2SC
       WHERE DC.castorFile = cfId
         AND DC.status IN (0, 6, 10)  -- STAGED, STAGEOUT, CANBEMIGR
         AND DC.fileSystem = FileSystem.id
         AND FileSystem.diskPool = DP2SC.parent
         AND DP2SC.child = scId)
    UNION ALL (
      -- and then diskcopies resulting from previous PrepareToPut|recall|replica requests
      SELECT DC.id
        FROM (SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToUpdateRequest UNION ALL
              SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
              SELECT id, svcClass FROM StageRepackRequest UNION ALL
              SELECT id, svcClass FROM StageDiskCopyReplicaRequest) PrepareRequest,
             SubRequest, DiskCopy DC
       WHERE SubRequest.diskCopy = DC.id
         AND PrepareRequest.id = SubRequest.request
         AND PrepareRequest.svcClass = scId
         AND DC.castorFile = cfId
         AND DC.status IN (1, 2, 5, 11)  -- WAITDISK2DISKCOPY, WAITTAPERECALL, WAITFS, WAITFS_SCHEDULING
      );
    IF dcsToRm.COUNT = 0 THEN
      -- We didn't find anything on this svcClass, fail and return
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 2,  -- ENOENT
             errorMessage = 'File not found on this service class'
       WHERE id = srId;
      RETURN;
    END IF;
    -- Check whether something else is left: if not, do as
    -- we are performing a stageRm everywhere.
    -- First select current status of the diskCopies: if CANBEMIGR,
    -- make sure we don't drop the last remaining valid migratable copy,
    -- i.e. exclude the disk only copies from the count.
    SELECT status INTO dcStatus
      FROM DiskCopy
     WHERE id = dcsToRm(1);
    IF dcStatus = 10 THEN  -- CANBEMIGR
      SELECT count(*) INTO nbRes FROM DiskCopy
       WHERE castorFile = cfId
         AND status = 10  -- CANBEMIGR
         AND id NOT IN (
           (SELECT /*+ CARDINALITY(dcidTable 5) */ *
              FROM TABLE(dcsToRm) dcidTable)
           UNION
           (SELECT DC.id     -- all diskcopies in Tape0 pools
              FROM DiskCopy DC, FileSystem, DiskPool2SvcClass D2S, SvcClass, FileClass
             WHERE DC.castorFile = cfId                         
               AND DC.fileSystem = FileSystem.id
               AND FileSystem.diskPool = D2S.parent
               AND D2S.child = SvcClass.id
               AND SvcClass.forcedFileClass = FileClass.id
               AND FileClass.nbCopies = 0));
    ELSE
      SELECT count(*) INTO nbRes FROM DiskCopy
         WHERE castorFile = cfId
           AND status IN (0, 2, 5, 6, 10, 11)  -- STAGED, WAITTAPERECALL, STAGEOUT, CANBEMIGR, WAITFS, WAITFS_SCHEDULING
           AND id NOT IN (SELECT /*+ CARDINALITY(dcidTable 5) */ *
                            FROM TABLE(dcsToRm) dcidTable);
    END IF;
    IF nbRes = 0 THEN
      -- nothing found, so we're dropping the last copy; then
      -- we need to perform all the checks to make sure we can
      -- allow the removal.
      scId := 0;
    END IF;
  END IF;

  IF scId = 0 THEN
    -- full cleanup is to be performed, do all necessary checks beforehand
    DECLARE
      segId INTEGER;
      unusedIds "numList";
    BEGIN
      -- check if removal is possible for migration
      SELECT count(*) INTO nbRes FROM DiskCopy
       WHERE status = 10 -- DISKCOPY_CANBEMIGR
         AND castorFile = cfId;
      IF nbRes > 0 THEN
        -- We found something, thus we cannot remove
        UPDATE SubRequest
           SET status = 7,  -- FAILED
               errorCode = 16,  -- EBUSY
               errorMessage = 'The file is not yet migrated'
         WHERE id = srId;
        RETURN;
      END IF;
      -- Stop ongoing recalls if stageRm either everywhere or the only available diskcopy.
      -- This is not entirely clean: a proper operation here should be to
      -- drop the SubRequest waiting for recall but keep the recall if somebody
      -- else is doing it, and taking care of other WAITSUBREQ requests as well...
      -- but it's fair enough, provided that the last stageRm will cleanup everything.
      -- XXX First lock all segments for the file. Note that
      -- XXX this step should be dropped once the tapeGateway
      -- XXX is deployed. The current recaller does not take
      -- XXX the proper lock on the castorFiles, hence we
      -- XXX need this here
      SELECT Segment.id BULK COLLECT INTO unusedIds
        FROM Segment, TapeCopy
       WHERE TapeCopy.castorfile = cfId
         AND TapeCopy.id = Segment.copy
       ORDER BY Segment.id
      FOR UPDATE OF Segment.id;
      -- Check whether we have any segment in SELECTED
      SELECT segment.id INTO segId
        FROM Segment, TapeCopy
       WHERE TapeCopy.castorfile = cfId
         AND TapeCopy.id = Segment.copy
         AND Segment.status = 7 -- SELECTED
         AND ROWNUM < 2;
      -- Something is running, so give up
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 16,  -- EBUSY
             errorMessage = 'The file is being recalled from tape'
       WHERE id = srId;
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Nothing running. We still may have found nothing at all...
      SELECT count(*) INTO nbRes FROM DiskCopy
       WHERE castorFile = cfId
         AND status NOT IN (4, 7, 9);  -- anything but FAILED, INVALID, BEINGDELETED
      IF nbRes = 0 THEN
        UPDATE SubRequest
           SET status = 7,  -- FAILED
               errorCode = 2,  -- ENOENT
               errorMessage = 'File not found on disk cache'
         WHERE id = srId;
        RETURN;
      END IF;
      
      deleteTapeCopies(cfId);
      -- Invalidate the DiskCopies
      UPDATE DiskCopy
         SET status = 7  -- INVALID
       WHERE status = 2  -- WAITTAPERECALL
         AND castorFile = cfId;
      -- Mark the 'recall' SubRequests as failed
      -- so that clients eventually get an answer
      UPDATE SubRequest
         SET status = 7,  -- FAILED
             errorCode = 4,  -- EINTR
             errorMessage = 'Recall canceled by another user request'
       WHERE castorFile = cfId and status IN (4, 5);   -- WAITTAPERECALL, WAITSUBREQ
      -- Reselect what needs to be removed
      SELECT id BULK COLLECT INTO dcsToRm
        FROM DiskCopy
       WHERE castorFile = cfId
         AND status IN (0, 1, 5, 6, 10, 11);  -- STAGED, WAITDISK2DISKCOPY, WAITFS, STAGEOUT, CANBEMIGR, WAITFS_SCHEDULING
    END;
  END IF;

  -- Now perform the remove:
  -- mark all get/put requests for those diskcopies
  -- and the ones waiting on them as failed
  -- so that clients eventually get an answer
  SELECT id BULK COLLECT INTO srIds
    FROM SubRequest 	 
   WHERE diskcopy IN
     (SELECT /*+ CARDINALITY(dcidTable 5) */ * 	 
        FROM TABLE(dcsToRm) dcidTable) 	 
         AND status IN (0, 1, 2, 5, 6, 13); -- START, WAITSUBREQ, READY, READYFORSCHED
  IF srIds.COUNT > 0 THEN
    FOR i IN srIds.FIRST .. srIds.LAST LOOP
      SELECT type INTO srType
        FROM SubRequest, Id2Type
       WHERE SubRequest.request = Id2Type.id
         AND SubRequest.id = srIds(i);
      UPDATE SubRequest
         SET status = CASE
             WHEN status IN (6, 13) AND srType = 133 THEN 9 ELSE 7
             END,  -- FAILED_FINISHED for DiskCopyReplicaRequests in status READYFORSCHED or READY, otherwise FAILED
             -- this so that user requests in status WAITSUBREQ are always marked FAILED even if they wait on a replication
             errorCode = 4,  -- EINTR
             errorMessage = 'Canceled by another user request'
       WHERE id = srIds(i) OR parent = srIds(i);
    END LOOP;
  END IF;
  -- Set selected DiskCopies to either INVALID or FAILED
  FORALL i IN dcsToRm.FIRST .. dcsToRm.LAST
    UPDATE DiskCopy SET status = 
           decode(status, 2,4, 5,4, 11,4, 7) -- WAITTAPERECALL,WAITFS[_SCHED] -> FAILED, others -> INVALID
     WHERE id = dcsToRm(i)
       AND status != 1;  -- WAITDISK2DISKCOPY
  ret := 1;  -- ok
END;
/

/* PL/SQL method to archive a SubRequest */
CREATE OR REPLACE PROCEDURE archiveSubReq(srId IN INTEGER, finalStatus IN INTEGER) AS
  unused INTEGER;
  rId INTEGER;
  rname VARCHAR2(100);
  srIds "numList";
  clientId INTEGER;
BEGIN
  SELECT request INTO rId
    FROM SubRequest
   WHERE id = srId;
  -- Lock the access to the Request
  SELECT Id2Type.id INTO rId
    FROM Id2Type
   WHERE id = rId FOR UPDATE;
  UPDATE SubRequest
     SET parent = NULL, diskCopy = NULL,  -- unlink this subrequest as it's dead now
         lastModificationTime = getTime(),
         status = finalStatus
   WHERE id = srId;
  BEGIN
    -- Try to see whether another subrequest in the same
    -- request is still being processed
    SELECT id INTO unused FROM SubRequest
     WHERE request = rId AND status NOT IN (8, 9) AND ROWNUM < 2;  -- all but {FAILED_,}FINISHED
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- All subrequests have finished, we can archive
    SELECT object INTO rname
      FROM Id2Type, Type2Obj
     WHERE id = rId
       AND Type2Obj.type = Id2Type.type;
    -- drop the associated Client entity and all Id2Type entries
    EXECUTE IMMEDIATE
      'BEGIN SELECT client INTO :cId FROM '|| rname ||' WHERE id = :rId; END;'
      USING OUT clientId, IN rId;
    DELETE FROM Client WHERE id = clientId;
    DELETE FROM Id2Type WHERE id IN (rId, clientId);
    SELECT id BULK COLLECT INTO srIds
      FROM SubRequest
     WHERE request = rId;
    FORALL i IN srIds.FIRST .. srIds.LAST
      DELETE FROM Id2Type WHERE id = srIds(i);
    -- archive the successful subrequests      
    UPDATE /*+ INDEX(SubRequest I_SubRequest_Request) */ SubRequest
       SET status = 11    -- ARCHIVED
     WHERE request = rId
       AND status = 8;  -- FINISHED
  END;
END;
/

/* PL/SQL method implementing createEmptyFile */
CREATE OR REPLACE PROCEDURE createEmptyFile
(cfId IN NUMBER, fileId IN NUMBER, nsHost IN VARCHAR2,
 srId IN INTEGER, schedule IN INTEGER) AS
  dcPath VARCHAR2(2048);
  gcw NUMBER;
  gcwProc VARCHAR(2048);
  fsId NUMBER;
  dcId NUMBER;
  svcClassId NUMBER;
  ouid INTEGER;
  ogid INTEGER;
  fsPath VARCHAR2(2048);
BEGIN
  -- update filesize overriding any previous value
  UPDATE CastorFile SET fileSize = 0 WHERE id = cfId;
  -- get an id for our new DiskCopy
  SELECT ids_seq.nextval INTO dcId FROM DUAL;
  -- compute the DiskCopy Path
  buildPathFromFileId(fileId, nsHost, dcId, dcPath);
  -- find a fileSystem for this empty file
  SELECT id, svcClass, euid, egid, name || ':' || mountpoint
    INTO fsId, svcClassId, ouid, ogid, fsPath
    FROM (SELECT FileSystem.id, Request.svcClass, Request.euid, Request.egid, DiskServer.name, FileSystem.mountpoint
            FROM DiskServer, FileSystem, DiskPool2SvcClass,
                 (SELECT id, svcClass, euid, egid from StageGetRequest UNION ALL
                  SELECT id, svcClass, euid, egid from StagePrepareToGetRequest UNION ALL
                  SELECT id, svcClass, euid, egid from StageUpdateRequest UNION ALL
                  SELECT id, svcClass, euid, egid from StagePrepareToUpdateRequest) Request,
                  SubRequest
           WHERE SubRequest.id = srId
             AND Request.id = SubRequest.request
             AND Request.svcclass = DiskPool2SvcClass.child
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND FileSystem.status = 0 -- FILESYSTEM_PRODUCTION
             AND DiskServer.id = FileSystem.diskServer
             AND DiskServer.status = 0 -- DISKSERVER_PRODUCTION
        ORDER BY -- first prefer DSs without concurrent migrators/recallers
                 DiskServer.nbRecallerStreams ASC, FileSystem.nbMigratorStreams ASC,
                 -- then order by rate as defined by the function
                 fileSystemRate(FileSystem.readRate, FileSystem.writeRate, FileSystem.nbReadStreams,
                                FileSystem.nbWriteStreams, FileSystem.nbReadWriteStreams, FileSystem.nbMigratorStreams,
                                FileSystem.nbRecallerStreams) DESC,
                 -- finally use randomness to avoid preferring always the same FS
                 DBMS_Random.value)
   WHERE ROWNUM < 2;
  -- compute it's gcWeight
  gcwProc := castorGC.getRecallWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(0); END;'
    USING OUT gcw;
  -- then create the DiskCopy
  INSERT INTO DiskCopy
    (path, id, filesystem, castorfile, status,
     creationTime, lastAccessTime, gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
  VALUES (dcPath, dcId, fsId, cfId, 0,   -- STAGED
          getTime(), getTime(), GCw, 0, 0, ouid, ogid);
  INSERT INTO Id2Type (id, type) VALUES (dcId, 5);  -- OBJ_DiskCopy
  -- link to the SubRequest and schedule an access if requested
  IF schedule = 0 THEN
    UPDATE SubRequest SET diskCopy = dcId WHERE id = srId;
  ELSE
    UPDATE SubRequest
       SET diskCopy = dcId,
           requestedFileSystems = fsPath,
           xsize = 0, status = 13, -- READYFORSCHED
           getNextStatus = 1 -- FILESTAGED
     WHERE id = srId;    
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  raise_application_error(-20115, 'No suitable filesystem found for this empty file');
END;
/

/* PL/SQL method implementing disk2DiskCopyFailed */
CREATE OR REPLACE PROCEDURE disk2DiskCopyFailed
(dcId IN INTEGER, enoent IN INTEGER) AS
  fsId    NUMBER;
  cfId    NUMBER;
  ouid    INTEGER;
  ogid    INTEGER;
  srcDcId NUMBER;
  srcFsId NUMBER;
  srId    NUMBER;
BEGIN
  fsId := 0;
  srcFsId := -1;
  IF enoent = 1 THEN
    -- Set all diskcopies to FAILED. We're preemptying the NS synchronization here
    UPDATE DiskCopy SET status = 4 -- FAILED
     WHERE castorFile =
       (SELECT castorFile FROM DiskCopy WHERE id = dcId); 
  ELSE
    -- Set the diskcopy status to INVALID so that it will be garbage collected
    -- at a later date.
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE status = 1 -- WAITDISK2DISKCOPY
       AND id = dcId
     RETURNING fileSystem, castorFile, owneruid, ownergid
      INTO fsId, cfId, ouid, ogid;
  END IF;
  BEGIN
    -- Determine the source diskcopy and filesystem involved in the replication
    SELECT sourceDiskCopy, fileSystem
      INTO srcDcId, srcFsId
      FROM DiskCopy, StageDiskCopyReplicaRequest
     WHERE StageDiskCopyReplicaRequest.sourceDiskCopy = DiskCopy.id
       AND StageDiskCopyReplicaRequest.destDiskCopy = dcId;
    -- Restart all entries in the snapshot of files to drain that may be
    -- waiting on the replication of the source diskcopy.
    UPDATE DrainingDiskCopy
       SET status = 1,  -- RESTARTED
           parent = 0
     WHERE status = 3  -- RUNNING
       AND (diskCopy = srcDcId
        OR  parent = srcDcId);
    -- Get the corresponding subRequest, if it exists
    SELECT id INTO srId
      FROM SubRequest
     WHERE diskCopy = dcId
       AND status IN (6, 14); -- READY, BEINGSHCED
    -- Wake up other subrequests waiting on it
    UPDATE SubRequest SET status = 1, -- RESTART
                          parent = 0
     WHERE parent = srId;
    -- Fail it
    archiveSubReq(srId, 9); -- FAILED_FINISHED
    -- If no filesystem was set on the diskcopy then we can safely delete it
    -- without waiting for garbage collection as the transfer was never started
    IF fsId = 0 THEN
      DELETE FROM DiskCopy WHERE id = dcId;
      DELETE FROM Id2Type WHERE id = dcId;
    END IF;
    -- Trigger the creation of additional copies of the file, if necessary.
    -- Note: We do this also on failure to be able to recover from transient
    -- errors, e.g. timeouts while waiting to be scheduled, but we don't on ENOENT.
    IF enoent = 0 THEN
      replicateOnClose(cfId, ouid, ogid);
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- SubRequest not found, don't trigger replicateOnClose here
    -- as we may have been restarted
    NULL;
  END;
  -- Continue draining process
  drainFileSystem(srcFsId);
  -- WARNING: previous call to drainFileSystem has a COMMIT inside. So all
  -- locks have been released!!
END;
/

/* PL/SQL method implementing replicateOnClose */
CREATE OR REPLACE PROCEDURE replicateOnClose(cfId IN NUMBER, ouid IN INTEGER, ogid IN INTEGER) AS
  unused NUMBER;
  srcDcId NUMBER;
  srcSvcClassId NUMBER;
  ignoreSvcClass NUMBER;
BEGIN
  -- Lock the castorfile
  SELECT id INTO unused FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Loop over all service classes where replication is required
  FOR a IN (SELECT SvcClass.id FROM (
              -- Determine the number of copies of the file in all service classes
              SELECT * FROM (
                SELECT SvcClass.id, count(*) available
                  FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass, SvcClass
                 WHERE DiskCopy.filesystem = FileSystem.id
                   AND DiskCopy.castorfile = cfId
                   AND FileSystem.diskpool = DiskPool2SvcClass.parent
                   AND DiskPool2SvcClass.child = SvcClass.id
                   AND DiskCopy.status IN (0, 1, 10)  -- STAGED, WAITDISK2DISKCOPY, CANBEMIGR
                   AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
                   AND DiskServer.id = FileSystem.diskserver
                   AND DiskServer.status IN (0, 1)  -- PRODUCTION, DRAINING
                 GROUP BY SvcClass.id)
             ) results, SvcClass
             -- Join the results with the service class table and determine if
             -- additional copies need to be created
             WHERE results.id = SvcClass.id
               AND SvcClass.replicateOnClose = 1
               AND results.available < SvcClass.maxReplicaNb)
  LOOP
    -- Ignore this replication if there is already a pending replication request
    -- for the given castorfile in the target/destination service class. Once
    -- the replication has gone through, this procedure will be called again.
    -- This insures that only one replication request is active at any given time
    -- and that we don't create too many replication requests that may exceed
    -- the maxReplicaNb defined on the service class
    BEGIN
      SELECT DiskCopy.id INTO unused
        FROM DiskCopy, StageDiskCopyReplicaRequest
       WHERE StageDiskCopyReplicaRequest.destdiskcopy = DiskCopy.id
         AND StageDiskCopyReplicaRequest.svcclass = a.id
         AND DiskCopy.castorfile = cfId
         AND DiskCopy.status = 1  -- WAITDISK2DISKCOPY
         AND rownum < 2;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- Select the best diskcopy to be replicated. Note: we force that the
        -- replication must happen internally within the service class. This
        -- prevents D2 style activities from impacting other more controlled
        -- service classes. E.g. D2 replication should not impact CDR
        getBestDiskCopyToReplicate(cfId, -1, -1, 1, a.id, srcDcId, srcSvcClassId);
        -- Trigger a replication request.
        createDiskCopyReplicaRequest(0, srcDcId, srcSvcClassId, a.id, ouid, ogid);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        NULL;  -- No copies to replicate from
      END;
    END;
  END LOOP;
END;
/

/* Procedure responsible for managing the draining process */
CREATE OR REPLACE PROCEDURE drainManager AS
  fsIds "numList";
BEGIN
  -- Delete the filesystems which are:
  --  A) in a DELETING state and have no transfers pending in the scheduler.
  --  B) are COMPLETED and older than 7 days.
  DELETE FROM DrainingFileSystem
   WHERE (NOT EXISTS (SELECT 'x' FROM DrainingDiskCopy
                       WHERE fileSystem = DrainingFileSystem.fileSystem
                         AND status = 3)  -- WAITD2D
          AND status = 6)  -- DELETING
      OR (status = 5 AND lastUpdateTime < getTime() - (7 * 86400));
  -- Process filesystems which in a CREATED state
  UPDATE DrainingFileSystem
     SET status = 1  -- INITIALIZING
   WHERE status = 0  -- CREATED
  RETURNING fileSystem BULK COLLECT INTO fsIds;
  -- Commit, this isn't really necessary but its done so that the user gets
  -- feedback when listing the filesystems which are being drained.
  COMMIT;
  IF fsIds.COUNT = 0 THEN
    -- Shrink the DrainingDiskCopy to keep the number of blocks small
    EXECUTE IMMEDIATE 'ALTER TABLE DrainingDiskCopy SHRINK SPACE CASCADE';
    RETURN;  -- No results
  END IF;
  -- Create the DrainingDiskCopy snapshot
  INSERT /*+ APPEND */ INTO DrainingDiskCopy
    (fileSystem, diskCopy, creationTime, priority, fileSize)
      (SELECT /*+ index(DC I_DiskCopy_FileSystem) */
              DC.fileSystem, DC.id, DC.creationTime, DC.status,
              DC.diskCopySize
         FROM DiskCopy DC, DrainingFileSystem DFS
        WHERE DFS.fileSystem = DC.fileSystem
          AND DFS.fileSystem IN
            (SELECT /*+ CARDINALITY(fsIdTable 5) */ *
               FROM TABLE (fsIds) fsIdTable)
          AND ((DC.status = 0         AND DFS.fileMask = 0)    -- STAGED
           OR  (DC.status = decode(DC.status, 10, DC.status, NULL)
                AND DFS.fileMask = 1)                          -- CANBEMIGR
           OR  (DC.status IN (0, 10)  AND DFS.fileMask = 2))); -- ALL
  -- Update the DrainingFileSystem counters
  FOR a IN (SELECT fileSystem, count(*) files, sum(DDC.fileSize) bytes
              FROM DrainingDiskCopy DDC
             WHERE DDC.fileSystem IN
               (SELECT /*+ CARDINALITY(fsIdTable 5) */ *
                  FROM TABLE (fsIds) fsIdTable)
               AND DDC.status = 0  -- CREATED
             GROUP BY DDC.fileSystem)
  LOOP
    UPDATE DrainingFileSystem
       SET totalFiles = a.files,
           totalBytes = a.bytes
     WHERE fileSystem = a.fileSystem;
  END LOOP;
  -- Update the filesystem entries to RUNNING
  UPDATE DrainingFileSystem
     SET status = 2,  -- RUNNING
         startTime = getTime(),
         lastUpdateTime = getTime()
   WHERE fileSystem IN
     (SELECT /*+ CARDINALITY(fsIdTable 5) */ *
        FROM TABLE (fsIds) fsIdTable);
  -- Start the process of draining the filesystems. For an explanation of the
  -- query refer to: "Creating N Copies of a Row":
  -- http://forums.oracle.com/forums/message.jspa?messageID=1953433#1953433
  FOR a IN ( SELECT fileSystem
               FROM DrainingFileSystem
              WHERE fileSystem IN
                (SELECT /*+ CARDINALITY(fsIdTable 5) */ *
                   FROM TABLE (fsIds) fsIdTable)
            CONNECT BY CONNECT_BY_ROOT fileSystem = fileSystem
                AND LEVEL <= maxTransfers
                AND PRIOR sys_guid() IS NOT NULL
              ORDER BY totalBytes ASC, fileSystem)
  LOOP
    drainFileSystem(a.fileSystem);
  END LOOP;
END;
/

/* PL/SQL procedure implementing startRepackMigration */
CREATE OR REPLACE PROCEDURE startRepackMigration
(srId IN INTEGER, cfId IN INTEGER, dcId IN INTEGER,
 reuid OUT INTEGER, regid OUT INTEGER) AS
  nbTC NUMBER(2);
  nbTCInFC NUMBER(2);
  fs NUMBER;
  svcClassId NUMBER;
BEGIN
  UPDATE DiskCopy SET status = 6  -- DISKCOPY_STAGEOUT
   WHERE id = dcId RETURNING diskCopySize INTO fs;
  -- how many do we have to create ?
  SELECT count(StageRepackRequest.repackVid) INTO nbTC
    FROM SubRequest, StageRepackRequest
   WHERE SubRequest.request = StageRepackRequest.id
     AND (SubRequest.id = srId
       OR (SubRequest.castorFile = cfId
         AND SubRequest.status IN (4, 5)));  -- WAITTAPERECALL, WAITSUBREQ
  SELECT nbCopies INTO nbTCInFC
    FROM FileClass, CastorFile
   WHERE CastorFile.id = cfId
     AND FileClass.id = Castorfile.fileclass;
  -- we are not allowed to create more TapeCopies than in the FileClass specified
  IF nbTCInFC < nbTC THEN
    nbTC := nbTCInFC;
  END IF;
  -- update the Repack subRequest for this file. The status REPACK
  -- stays until the migration to the new tape is over.
  UPDATE SubRequest
     SET diskCopy = dcId, status = 12  -- REPACK
   WHERE id = srId;   
  -- get the service class, uid and gid
  SELECT R.svcClass, euid, egid INTO svcClassId, reuid, regid
    FROM StageRepackRequest R, SubRequest
   WHERE SubRequest.request = R.id
     AND SubRequest.id = srId;
  -- create the required number of tapecopies for the files
  internalPutDoneFunc(cfId, fs, 0, nbTC, svcClassId);
  -- set svcClass in the CastorFile for the migration
  UPDATE CastorFile SET svcClass = svcClassId WHERE id = cfId;
  -- update remaining STAGED diskcopies to CANBEMIGR too
  -- we may have them as result of disk2disk copies, and so far
  -- we only dealt with dcId
  UPDATE DiskCopy SET status = 10  -- DISKCOPY_CANBEMIGR
   WHERE castorFile = cfId AND status = 0;  -- DISKCOPY_STAGED
END;
/

/* PL/SQL method to get the next SubRequest to do according to the given service */
CREATE OR REPLACE PROCEDURE subRequestToDo(service IN VARCHAR2,
                                           srId OUT INTEGER, srRetryCounter OUT INTEGER, srFileName OUT VARCHAR2,
                                           srProtocol OUT VARCHAR2, srXsize OUT INTEGER, srPriority OUT INTEGER,
                                           srStatus OUT INTEGER, srModeBits OUT INTEGER, srFlags OUT INTEGER,
                                           srSubReqId OUT VARCHAR2, srAnswered OUT INTEGER, srSvcHandler OUT VARCHAR2) AS
  CURSOR SRcur IS SELECT /*+ FIRST_ROWS(10) INDEX(SR I_SubRequest_RT_CT_ID) */ SR.id
                    FROM SubRequest PARTITION (P_STATUS_0_1_2) SR
                   WHERE SR.svcHandler = service
                   ORDER BY SR.creationTime ASC;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  srIntId NUMBER;
BEGIN
  OPEN SRcur;
  -- Loop on candidates until we can lock one
  LOOP
    -- Fetch next candidate
    FETCH SRcur INTO srIntId;
    EXIT WHEN SRcur%NOTFOUND;
    BEGIN
      -- Try to take a lock on the current candidate, and revalidate its status
      SELECT /*+ INDEX(SR PK_SubRequest_ID) */ id INTO srIntId
        FROM SubRequest PARTITION (P_STATUS_0_1_2) SR
       WHERE id = srIntId FOR UPDATE NOWAIT;
      -- Since we are here, we got the lock. We have our winner, let's update it
      UPDATE SubRequest
         SET status = 3, subReqId = nvl(subReqId, uuidGen()) -- WAITSCHED
       WHERE id = srIntId
      RETURNING id, retryCounter, fileName, protocol, xsize, priority, status, modeBits, flags, subReqId, answered, svcHandler
        INTO srId, srRetryCounter, srFileName, srProtocol, srXsize, srPriority, srStatus, srModeBits, srFlags, srSubReqId, srAnswered, srSvcHandler;
      EXIT;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        -- Got to next candidate, this subrequest was processed already and its status changed
        NULL;
      WHEN SrLocked THEN
        -- Go to next candidate, this subrequest is being processed by another thread
        NULL;
    END;
  END LOOP;
  CLOSE SRcur;
END;
/


/* Enable all jobs */
/*******************/
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs WHERE enabled = 'FALSE')
  LOOP
    dbms_scheduler.enable(a.job_name);
  END LOOP;
END;
/


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM user_objects
             WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION')
               AND status = 'INVALID')
  LOOP
    IF a.object_type = 'PROCEDURE' THEN
      EXECUTE IMMEDIATE 'ALTER PROCEDURE '||a.object_name||' COMPILE';
    ELSIF a.object_type = 'TRIGGER' THEN
      EXECUTE IMMEDIATE 'ALTER TRIGGER '||a.object_name||' COMPILE';
    ELSE
      EXECUTE IMMEDIATE 'ALTER FUNCTION '||a.object_name||' COMPILE';
    END IF;
  END LOOP;
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE';
COMMIT;