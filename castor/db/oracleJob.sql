/*******************************************************************
 *
 * @(#)$RCSfile: oracleJob.sql,v $ $Revision: 1.660 $ $Date: 2008/09/02 09:46:23 $ $Author: waldron $
 *
 * PL/SQL code for scheduling and job handling
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/


/* PL/SQL method to update FileSystem nb of streams when files are opened */
CREATE OR REPLACE PROCEDURE updateFsFileOpened
(ds IN INTEGER, fs IN INTEGER, fileSize IN INTEGER) AS
BEGIN
  /* We lock first the diskserver in order to lock all the
     filesystems of this DiskServer in an atomical way */
  UPDATE DiskServer SET nbReadWriteStreams = nbReadWriteStreams + 1 WHERE id = ds;
  UPDATE FileSystem SET nbReadWriteStreams = nbReadWriteStreams + 1,
                        free = free - fileSize   -- just an evaluation, monitoring will update it
   WHERE id = fs;
END;

/* PL/SQL method to update FileSystem nb of streams when files are closed */
CREATE OR REPLACE PROCEDURE updateFsFileClosed(fs IN INTEGER) AS
  ds INTEGER;
BEGIN
  /* We lock first the diskserver in order to lock all the
     filesystems of this DiskServer in an atomical way */
  SELECT DiskServer INTO ds FROM FileSystem WHERE id = fs;
  UPDATE DiskServer SET nbReadWriteStreams = decode(sign(nbReadWriteStreams-1),-1,0,nbReadWriteStreams-1) WHERE id = ds;
  /* now we can safely go */
  UPDATE FileSystem SET nbReadWriteStreams = decode(sign(nbReadWriteStreams-1),-1,0,nbReadWriteStreams-1)
  WHERE id = fs;
END;


/* This method should be called when the first byte is written to a file
 * opened with an update. This will kind of convert the update from a
 * get to a put behavior.
 */
CREATE OR REPLACE PROCEDURE firstByteWrittenProc(srId IN INTEGER) AS
  dcId NUMBER;
  cfId NUMBER;
  nbres NUMBER;
  stat NUMBER;
  fclassId NUMBER;
  sclassId NUMBER;
BEGIN
  -- Get data and lock the Castorfile
  SELECT castorfile, diskCopy INTO cfId, dcId
    FROM SubRequest WHERE id = srId;
  SELECT fileclass INTO fclassId FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Check that the file is not busy, i.e. that we are not
  -- in the middle of migrating it. If we are, just stop and raise
  -- a user exception
  SELECT count(*) INTO nbRes FROM TapeCopy
    WHERE status = 3 -- TAPECOPY_SELECTED
    AND castorFile = cfId;
  IF nbRes > 0 THEN
    raise_application_error(-20106, 'Trying to update a busy file (ongoing migration)');
  END IF;
  -- Check that we can indeed open the file in write mode
  -- 3 criteria have to be met :
  --   - we are not using a INVALID copy (we should not update an old version)
  --   - we are not in a disk only svcClass and the file class asks for tape copy
  --   - there is no other update/put going on or there is a prepareTo and we are
  --     dealing with the same copy.
  SELECT status INTO stat FROM DiskCopy WHERE id = dcId;
  -- First the invalid case
  IF stat = 7 THEN -- INVALID
    raise_application_error(-20106, 'Trying to update an invalid copy of a file (file has been modified by somebody else concurrently)');
  END IF;
  -- Then the disk only check
  SELECT svcClass INTO sclassId
    FROM Subrequest, StageUpdateRequest Request
   WHERE SubRequest.id = srId
     AND Request.id = SubRequest.request;
  IF checkFailPutWhenDiskOnly(sclassId, fclassId) = 1 THEN
     raise_application_error(-20106, 'File update canceled since this service class doesn''t provide tape backend');
  END IF;
  -- Otherwise, either we are alone or we are on the right copy and we
  -- only have to check that there is a prepareTo statement. We do the check
  -- only when needed, that is STAGEOUT case
  IF stat = 6 THEN -- STAGEOUT
    BEGIN
      -- do we have other ongoing requests ?
      SELECT count(*) INTO nbRes FROM SubRequest WHERE diskCopy = dcId AND id != srId;
      IF (nbRes > 0) THEN
        -- do we have a prepareTo Request ? There can be only a single one
        -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
        -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
        SELECT SubRequest.id INTO nbRes
          FROM (SELECT id FROM StagePrepareToPutRequest UNION ALL
                SELECT id FROM StagePrepareToUpdateRequest) PrepareRequest,
               SubRequest
         WHERE SubRequest.CastorFile = cfId
           AND PrepareRequest.id = SubRequest.request
           AND SubRequest.status = 6;  -- READY
      END IF;
      -- we do have a prepareTo, so eveything is fine
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- No prepareTo, so prevent the writing
      raise_application_error(-20106, 'Trying to update a file already open for write (and no prepareToPut/Update context found)');
    END;
  ELSE
    -- If we are not having a STAGEOUT diskCopy, we are the only ones to write,
    -- so we have to setup everything
    -- invalidate all diskcopies
    UPDATE DiskCopy SET status = 7 -- INVALID
     WHERE castorFile = cfId
       AND status IN (0, 10);
    -- except the one we are dealing with that goes to STAGEOUT
    UPDATE DiskCopy
       SET status = 6 -- STAGEOUT
     WHERE id = dcid;
    -- Suppress all Tapecopies (avoid migration of previous version of the file)
    deleteTapeCopies(cfId);
  END IF;
END;

/* Checks whether the protocol used is supporting updates and when not
 * calls firstByteWrittenProc as if the file was already modified */
CREATE OR REPLACE PROCEDURE handleProtoNoUpd
(srId IN INTEGER, protocol VARCHAR2) AS
BEGIN
  IF protocol != 'rfio' AND protocol != 'rfio3' THEN
    firstByteWrittenProc(srId);
  END IF;
END;


/* PL/SQL method implementing putStart */
CREATE OR REPLACE PROCEDURE putStart
        (srId IN INTEGER, fileSystemId IN INTEGER,
         rdcId OUT INTEGER, rdcStatus OUT INTEGER,
         rdcPath OUT VARCHAR2) AS
  srStatus INTEGER;
  prevFsId INTEGER;
BEGIN
  -- Get diskCopy id
  SELECT diskCopy, SubRequest.status, fileSystem INTO rdcId, srStatus, prevFsId
    FROM SubRequest, DiskCopy
   WHERE SubRequest.diskcopy = Diskcopy.id
     AND SubRequest.id = srId;
  -- Check that we did not cancel the SubRequest in the mean time
  IF srStatus IN (7, 9, 10) THEN -- FAILED, FAILED_FINISHED, FAILED_ANSWERING
    raise_application_error(-20104, 'SubRequest canceled while queuing in scheduler. Giving up.');
  END IF;
  IF prevFsId > 0 AND prevFsId <> fileSystemId THEN
    -- this could happen if LSF schedules the same job twice!
    -- (see bug report #14358 for the whole story)
    raise_application_error(-20107, 'This job has already started for this DiskCopy. Giving up.');
  END IF;

  -- Get older castorFiles with the same name and drop their lastKnownFileName
  UPDATE /*+ INDEX (castorFile) */ CastorFile SET lastKnownFileName = TO_CHAR(id)
   WHERE id IN (
    SELECT /*+ INDEX (cfOld) */ cfOld.id FROM CastorFile cfOld, CastorFile cfNew, SubRequest
     WHERE cfOld.lastKnownFileName = cfNew.lastKnownFileName
       AND cfOld.fileid <> cfNew.fileid
       AND cfNew.id = SubRequest.castorFile
       AND SubRequest.id = srId);
  -- In case the DiskCopy was in WAITFS_SCHEDULING,
  -- restart the waiting SubRequests
  UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0 -- SUBREQUEST_RESTART
   WHERE parent = srId;
  -- link DiskCopy and FileSystem and update DiskCopyStatus
  UPDATE DiskCopy SET status = 6, -- DISKCOPY_STAGEOUT
                      fileSystem = fileSystemId,
                      nbCopyAccesses = nbCopyAccesses + 1
   WHERE id = rdcId
   RETURNING status, path
   INTO rdcStatus, rdcPath;
END;

/* PL/SQL method implementing getUpdateStart */
CREATE OR REPLACE PROCEDURE getUpdateStart
        (srId IN INTEGER, fileSystemId IN INTEGER,
         dci OUT INTEGER, rpath OUT VARCHAR2,
         rstatus OUT NUMBER,
         reuid OUT INTEGER, regid OUT INTEGER) AS
  cfid INTEGER;
  fid INTEGER;
  dcId INTEGER;
  dcIdInReq INTEGER;
  nh VARCHAR2(2048);
  fileSize INTEGER;
  srSvcClass INTEGER;
  proto VARCHAR2(2048);
  isUpd NUMBER;
  nbAc NUMBER;
  gcw NUMBER;
  gcwProc VARCHAR2(2048);
  cTime NUMBER;
BEGIN
  -- Get data
  SELECT euid, egid, svcClass, upd, diskCopy
    INTO reuid, regid, srSvcClass, isUpd, dcIdInReq
    FROM SubRequest,
        (SELECT id, euid, egid, svcClass, 0 AS upd FROM StageGetRequest UNION ALL
         SELECT id, euid, egid, svcClass, 1 AS upd FROM StageUpdateRequest) Request
   WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
  -- Take a lock on the CastorFile. Associated with triggers,
  -- this guarantees we are the only ones dealing with its copies
  SELECT CastorFile.fileSize, CastorFile.id
    INTO fileSize, cfId
    FROM CastorFile, SubRequest
   WHERE CastorFile.id = SubRequest.castorFile
     AND SubRequest.id = srId FOR UPDATE;
  -- Try to find local DiskCopy
  SELECT /*+ INDEX(DISKCOPY I_DISKCOPY_CASTORFILE) */ id, nbCopyAccesses, gcWeight, creationTime
    INTO dcId, nbac, gcw, cTime
    FROM DiskCopy
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.filesystem = fileSystemId
     AND DiskCopy.status IN (0, 6, 10) -- STAGED, STAGEOUT, CANBEMIGR
     AND ROWNUM < 2;
  -- We found it, so we are settled and we'll use the local copy.
  -- It might happen that we have more than one, because LSF may have
  -- scheduled a replication on a fileSystem which already had a previous diskcopy.
  -- We don't care and we randomly took the first one.
  -- First we will compute the new gcWeight of the diskcopy
  IF nbac = 0 THEN
    gcwProc := castorGC.getFirstAccessHook(srSvcClass);
    IF gcwProc IS NOT NULL THEN
      EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:oldGcw, :cTime); END;'
        USING OUT gcw, IN gcw, IN cTime;
    END IF;
  ELSE
    gcwProc := castorGC.getAccessHook(srSvcClass);
    IF gcwProc IS NOT NULL THEN
      EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:oldGcw, :cTime, :nbAc); END;'
        USING OUT gcw, IN gcw, IN cTime, IN nbac;
    END IF;
  END IF;
  -- Here we also update the gcWeight taking into account the new lastAccessTime
  -- and the weightForAccess from our svcClass: this is added as a bonus to
  -- the selected diskCopy.
  UPDATE DiskCopy
     SET gcWeight = gcw,
         lastAccessTime = getTime(),
         nbCopyAccesses = nbCopyAccesses + 1
   WHERE id = dcId
  RETURNING id, path, status INTO dci, rpath, rstatus;
  -- Let's update the SubRequest and set the link with the DiskCopy
  UPDATE SubRequest SET DiskCopy = dci WHERE id = srId RETURNING protocol INTO proto;
  -- In case of an update, if the protocol used does not support
  -- updates properly (via firstByteWritten call), we should
  -- call firstByteWritten now and consider that the file is being
  -- modified.
  IF isUpd = 1 THEN
    handleProtoNoUpd(srId, proto);
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- No disk copy found on selected FileSystem. This can happen in 3 cases :
  --  + either a diskcopy was available and got disabled before this job
  --    was scheduled. Bad luck, we restart from scratch
  --  + or we are an update creating a file and there is a diskcopy in WAITFS
  --    or WAITFS_SCHEDULING associated to us. Then we have to call putStart
  --  + or we are recalling a 0-size file
  -- So we first check the update hypothesis
  IF isUpd = 1 AND dcIdInReq IS NOT NULL THEN
    DECLARE
      stat NUMBER;
    BEGIN
      SELECT status INTO stat FROM DiskCopy WHERE id = dcIdInReq;
      IF stat IN (5, 11) THEN -- WAITFS, WAITFS_SCHEDULING
        -- it is an update creating a file, let's call putStart
        putStart(srId, fileSystemId, dci, rstatus, rpath);
        RETURN;
      END IF;
    END;
  END IF;
  -- Now we check the 0-size file hypothesis
  -- XXX this is currently broken, to be fixed later
  -- It was not an update creating a file, so we restart
  UPDATE SubRequest SET status = 1 WHERE id = srId;
  dci := 0;
  rpath := '';
END;


/* PL/SQL method implementing disk2DiskCopyStart */
CREATE OR REPLACE PROCEDURE disk2DiskCopyStart
(dcId IN INTEGER, srcDcId IN INTEGER, destSvcClass IN VARCHAR2,
 destdiskServer IN VARCHAR2, destMountPoint IN VARCHAR2, destPath OUT VARCHAR2,
 srcDiskServer OUT VARCHAR2, srcMountPoint OUT VARCHAR2, srcPath OUT VARCHAR2,
 srcSvcClass OUT VARCHAR2, reqEuid OUT NUMBER, reqEgid OUT NUMBER) AS
  fsId NUMBER;
  cfId NUMBER;
  dsId NUMBER;
  res NUMBER;
  unused NUMBER;
  nbCopies NUMBER;
  cfNsHost VARCHAR2(2048);
BEGIN
  -- Check that we did not cancel the replication request in the mean time
  BEGIN
    SELECT SubRequest.status, 
           StageDiskCopyReplicaRequest.euid, StageDiskCopyReplicaRequest.egid
      INTO unused, reqEuid, reqEgid
      FROM SubRequest, StageDiskCopyReplicaRequest
     WHERE SubRequest.diskcopy = dcId
       AND SubRequest.request = StageDiskCopyReplicaRequest.id
       AND SubRequest.status = 6; -- READY
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20111, 'Replication request canceled while queuing in scheduler. Giving up.');
  END;
  -- Check to see if the proposed diskserver and filesystem selected by the
  -- scheduler to run the destination end of disk2disk copy transfer is in the
  -- correct service class. I.e the service class of the original request. This
  -- is done to prevent files being written to an incorrect service class when
  -- diskservers/filesystems are moved.
  BEGIN
    SELECT FileSystem.id, DiskServer.id INTO fsId, dsId
      FROM DiskServer, FileSystem, DiskPool2SvcClass, DiskPool, SvcClass
     WHERE FileSystem.diskserver = DiskServer.id
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = SvcClass.id
       AND DiskPool2SvcClass.parent = DiskPool.id
       AND SvcClass.name = destSvcClass
       AND DiskServer.name = destDiskServer
       AND FileSystem.mountPoint = destMountPoint;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20108, 'The destination job has been scheduled to the wrong service class.');
  END;
  -- Check whether the source diskcopy is still available. It may no longer be
  -- the case if it got disabled before the job started.
  BEGIN
    SELECT DiskServer.name, FileSystem.mountPoint, DiskCopy.path,
           CastorFile.id, CastorFile.nsHost, SvcClass.name
      INTO srcDiskServer, srcMountPoint, srcPath, cfId, cfNsHost, srcSvcClass
      FROM DiskCopy, CastorFile, DiskServer, FileSystem, DiskPool2SvcClass,
           SvcClass, StageDiskCopyReplicaRequest
     WHERE DiskCopy.id = srcDcId
       AND DiskCopy.castorfile = CastorFile.id
       AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
       AND FileSystem.id = DiskCopy.filesystem
       AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = SvcClass.id
       AND DiskServer.id = FileSystem.diskserver
       AND DiskServer.status IN (0, 1)
       -- For diskpools which belong to multiple service classes, make sure
       -- we are checking for the file in the correct service class!
       AND StageDiskCopyReplicaRequest.sourceDiskCopy = DiskCopy.id
       AND StageDiskCopyReplicaRequest.sourceSvcClass = SvcClass.id
       AND StageDiskCopyReplicaRequest.destDiskCopy = dcId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20109, 'The source DiskCopy to be replicated is no longer available.');
  END;
  -- Prevent multiple copies of the file being created on the same diskserver
  SELECT count(*) INTO nbCopies
    FROM DiskCopy, FileSystem
   WHERE DiskCopy.filesystem = FileSystem.id
     AND FileSystem.diskserver = dsId
     AND DiskCopy.castorfile = cfId
     AND DiskCopy.id != dcId
     AND DiskCopy.status IN (0, 1, 2, 10); -- STAGED, DISK2DISKCOPY, WAITTAPERECALL, CANBEMIGR
  IF nbCopies > 0 THEN
    raise_application_error(-20112, 'Multiple copies of this file already found on this diskserver');
  END IF;
  -- Update the filesystem of the destination diskcopy. If the update fails
  -- either the diskcopy doesn't exist anymore indicating the cancellation of
  -- the subrequest or another transfer has already started for it.
  UPDATE DiskCopy SET filesystem = fsId
   WHERE id = dcId
     AND filesystem = 0
     AND status = 1 -- WAITDISK2DISKCOPY
  RETURNING path INTO destPath;
  IF destPath IS NULL THEN
    raise_application_error(-20110, 'A transfer has already started for this DiskCopy.');
  END IF;
END;


/* PL/SQL method implementing disk2DiskCopyDone */
CREATE OR REPLACE PROCEDURE disk2DiskCopyDone
(dcId IN INTEGER, srcDcId IN INTEGER) AS
  srId INTEGER;
  cfId INTEGER;
  fsId INTEGER;
  maxRepl INTEGER;
  repl INTEGER;
  srcStatus INTEGER;
  proto VARCHAR2(2048);
  reqId NUMBER;
  svcClassId NUMBER;
  gcwProc VARCHAR2(2048);
  gcw NUMBER;
  fileSize NUMBER;
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  -- try to get the source status
  SELECT status, gcWeight, diskCopySize INTO srcStatus, gcw, fileSize
    FROM DiskCopy WHERE id = srcDcId;
  -- In case the status is null, it means that the source has been GCed
  -- while the disk to disk copy was processed. We will invalidate the
  -- brand new copy as we don't know in which status is should be
  IF srcStatus IS NULL THEN
    UPDATE DiskCopy SET status = 7 WHERE id = dcId -- INVALID
    RETURNING CastorFile, FileSystem INTO cfId, fsId;
    -- restart the SubRequests waiting for the copy
    UPDATE SubRequest SET status = 1, -- SUBREQUEST_RESTART
                          lastModificationTime = getTime()
     WHERE diskCopy = dcId RETURNING id INTO srId;
    UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0
     WHERE parent = srId; -- SUBREQUEST_RESTART
    -- update filesystem status
    updateFsFileClosed(fsId);
    -- archive the diskCopy replica request
    archiveSubReq(srId);
    RETURN;
  END IF;
  -- otherwise, we can validate the new diskcopy
  -- update SubRequest and get data
  UPDATE SubRequest SET status = 6, -- SUBREQUEST_READY
                        getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
                        lastModificationTime = getTime()
   WHERE diskCopy = dcId RETURNING id, protocol, request
    INTO srId, proto, reqId;
  SELECT SvcClass.id, maxReplicaNb
    INTO svcClassId, maxRepl
    FROM SvcClass, StageDiskCopyReplicaRequest Req, SubRequest
   WHERE SubRequest.id = srId
     AND SubRequest.request = Req.id
     AND Req.svcClass = SvcClass.id;
  -- compute gcWeight
  gcwProc := castorGC.getCopyWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:size, :status, :gcw); END;'
    USING OUT gcw, IN fileSize, srcStatus, gcw;
  -- update status
  UPDATE DiskCopy
     SET status = srcStatus,
         gcWeight = gcw
   WHERE id = dcId
  RETURNING castorFile, fileSystem, owneruid, ownergid
    INTO cfId, fsId, ouid, ogid;
  -- Lock the castorfile
  SELECT id INTO cfId FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- If the maxReplicaNb is exceeded, update one of the diskcopies in a
  -- DRAINING filesystem to INVALID.
  SELECT count(*) INTO repl
    FROM DiskCopy, FileSystem, DiskPool2SvcClass DP2SC
   WHERE castorFile = cfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskPool = DP2SC.parent
     AND DP2SC.child = svcClassId
     AND DiskCopy.status IN (0, 10);  -- STAGED, CANBEMIGR
  IF repl > maxRepl THEN
    -- We did replicate only because of a DRAINING filesystem.
    -- Invalidate one of the original diskcopies, not all of them for
    -- fault resiliency purposes
    UPDATE DiskCopy SET status = 7
     WHERE status IN (0, 10)  -- STAGED, CANBEMIGR
       AND castorFile = cfId
       AND fileSystem IN (
         SELECT FileSystem.id
           FROM FileSystem, DiskPool2SvcClass DP2SC
          WHERE FileSystem.diskPool = DP2SC.parent
            AND DP2SC.child = svcClassId
            AND status = 1)  -- DRAINING
       AND ROWNUM < 2;
  END IF;
  -- archive the diskCopy replica request
  archiveSubReq(srId);
  -- update filesystem status
  updateFsFileClosed(fsId);
  -- Trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(cfId, ouid, ogid);
  -- Wake up waiting subrequests
  UPDATE SubRequest SET status = 1,  -- SUBREQUEST_RESTART
         lastModificationTime = getTime(), parent = 0
   WHERE parent = srId;
END;


/* PL/SQL method implementing disk2DiskCopyFailed */
CREATE OR REPLACE PROCEDURE disk2DiskCopyFailed
(dcId IN INTEGER, res OUT INTEGER) AS
  fsId NUMBER;
  cfId NUMBER;
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  -- Set the diskcopy status to INVALID so that it will be garbage collected
  -- at a later date.
  fsId := 0;
  res := 0;
  UPDATE DiskCopy SET status = 7 -- INVALID
   WHERE status = 1 -- WAITDISK2DISKCOPY
     AND id = dcId
   RETURNING fileSystem, castorFile, owneruid, ownergid
    INTO fsId, cfId, ouid, ogid;
  -- Fail the corresponding subrequest
  UPDATE SubRequest SET status = 9, -- FAILED_FINISHED
         lastModificationTime = getTime()
   WHERE diskCopy = dcId
     AND status IN (6, 14) -- READY, BEINGSHCED
   RETURNING id INTO res;
  IF res = 0 THEN
    -- Dont trigger replicateOnClose here as we may have been restarted
    RETURN;
  END IF;
  -- If no filesystem was set on the diskcopy then we can safely delete it
  -- without waiting for garbage collection as the transfer was never started
  IF fsId = 0 THEN
    DELETE FROM DiskCopy WHERE id = dcId;
    DELETE FROM Id2Type WHERE id = dcId;
  END IF;
  -- Wake up other subrequests waiting on it
  UPDATE SubRequest SET status = 1, -- RESTART
                        lastModificationTime = getTime(),
                        parent = 0
   WHERE parent = res;
  -- Update filesystem status
  IF fsId > 0 THEN
    updateFsFileClosed(fsId);
  END IF;
  -- Trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(cfId, ouid, ogid);
END;


/* PL/SQL method implementing prepareForMigration */
CREATE OR REPLACE PROCEDURE prepareForMigration (srId IN INTEGER,
                                                 fs IN INTEGER,
                                                 ts IN NUMBER,
                                                 fId OUT NUMBER,
                                                 nh OUT VARCHAR2,
                                                 userId OUT INTEGER,
                                                 groupId OUT INTEGER,
                                                 errorCode OUT INTEGER) AS
  cfId INTEGER;
  dcId INTEGER;
  fsId INTEGER;
  scId INTEGER;
  realFileSize INTEGER;
  unused INTEGER;
  contextPIPP INTEGER;
BEGIN
  errorCode := 0;
  -- get CastorFile
  SELECT castorFile, diskCopy INTO cfId, dcId FROM SubRequest WHERE id = srId;
  -- Determine the context (Put inside PrepareToPut or not)
  -- check that we are a Put or an Update
  SELECT Request.id INTO unused
    FROM SubRequest,
       (SELECT id FROM StagePutRequest UNION ALL
        SELECT id FROM StageUpdateRequest) Request
   WHERE SubRequest.id = srId
     AND Request.id = SubRequest.request;
  BEGIN
    -- check that there is a PrepareToPut/Update going on. There can be only a single one
    -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
    -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
    SELECT SubRequest.diskCopy INTO unused
      FROM SubRequest,
       (SELECT id FROM StagePrepareToPutRequest UNION ALL
        SELECT id FROM StagePrepareToUpdateRequest) Request
     WHERE SubRequest.CastorFile = cfId
       AND Request.id = SubRequest.request
       AND SubRequest.status = 6; -- READY
    -- if we got here, we are a Put inside a PrepareToPut
    contextPIPP := 0;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- here we are a standalone Put
    contextPIPP := 1;
  END;

  -- Check whether the diskCopy is still in STAGEOUT. If not, the file
  -- was deleted via stageRm while being written to. Thus, we should just give up
  BEGIN
    SELECT status INTO unused
      FROM DiskCopy WHERE id = dcId AND status = 6; -- STAGEOUT
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- so we are in the case, we give up
    errorCode := 1;
    -- but we still would like to have the fileId and nameserver
    -- host for logging reasons
    SELECT fileId, nsHost INTO fId, nh
      FROM CastorFile WHERE id = cfId;
    RETURN;
  END;
  -- now we can safely update CastorFile's file size
  UPDATE CastorFile SET fileSize = fs, lastUpdateTime = ts
   WHERE id = cfId AND (lastUpdateTime IS NULL OR ts > lastUpdateTime);
  -- If ts < lastUpdateTime, we were late and another job already updated the
  -- CastorFile. So we nevertheless retrieve the real file size, and
  -- we take a lock on the CastorFile. Together with triggers, this insures that
  -- we are the only ones to deal with its copies.
  SELECT fileId, nsHost, fileSize INTO fId, nh, realFileSize
    FROM CastorFile
    WHERE id = cfId
    FOR UPDATE;
  -- get uid, gid and svcclass from Request
  SELECT euid, egid, svcClass INTO userId, groupId, scId
    FROM SubRequest,
      (SELECT euid, egid, id, svcClass from StagePutRequest UNION ALL
       SELECT euid, egid, id, svcClass from StageUpdateRequest UNION ALL
       SELECT euid, egid, id, svcClass from StagePutDoneRequest) Request
   WHERE SubRequest.request = Request.id AND SubRequest.id = srId;

  SELECT fileSystem INTO fsId from DiskCopy
   WHERE castorFile = cfId AND status = 6;
  updateFsFileClosed(fsId);

  IF contextPIPP != 0 THEN
    -- If not a put inside a PrepareToPut/Update, create TapeCopies
    -- and update DiskCopy status
    putDoneFunc(cfId, realFileSize, contextPIPP, scId);
  ELSE
    -- If put inside PrepareToPut/Update, restart any PutDone currently
    -- waiting on this put/update
    UPDATE SubRequest SET status = 1, parent = 0 -- RESTART
     WHERE id IN
      (SELECT SubRequest.id FROM SubRequest, Id2Type
        WHERE SubRequest.request = Id2Type.id
          AND Id2Type.type = 39       -- PutDone
          AND SubRequest.castorFile = cfId
          AND SubRequest.status = 5); -- WAITSUBREQ
  END IF;
  -- archive Subrequest
  archiveSubReq(srId);
  COMMIT;
END;


/* PL/SQL method implementing getUpdateDone */
CREATE OR REPLACE PROCEDURE getUpdateDoneProc
(subReqId IN NUMBER) AS
BEGIN
  archiveSubReq(subReqId);
END;


/* PL/SQL method implementing getUpdateFailed */
CREATE OR REPLACE PROCEDURE getUpdateFailedProc
(srId IN NUMBER) AS
BEGIN
  UPDATE SubRequest SET status = 9 -- FAILED_FINISHED
   WHERE id = srId;
  UPDATE SubRequest SET status = 1 -- RESTART
   WHERE parent = srId;
END;


/* PL/SQL method implementing putFailedProc */
CREATE OR REPLACE PROCEDURE putFailedProc(srId IN NUMBER) AS
  dcId INTEGER;
  fsId INTEGER;
  cfId INTEGER;
  unused INTEGER;
BEGIN
  -- Fail SubRequest
  UPDATE SubRequest
     SET status = 9 -- FAILED_FINISHED
   WHERE id = srId
  RETURNING diskCopy, castorFile INTO dcId, cfId;
  IF dcId > 0 THEN
    SELECT fileSystem INTO fsId FROM DiskCopy WHERE id = dcId;
    -- Take file closing into account
    IF fsId > 0 THEN
      updateFsFileClosed(fsId);
    END IF;
  -- ELSE the subRequest is not linked to a diskCopy: should never happen,
  -- but we observed NO DATA FOUND errors and the above SELECT is the only
  -- query that could trigger them! Anyway it's fine to ignore the FS update,
  -- it will be properly updated by the monitoring.
  END IF;
  -- Determine the context (Put inside PrepareToPut/Update ?)
  BEGIN
    -- Check that there is a PrepareToPut/Update going on. There can be only a single one
    -- or none. If there was a PrepareTo, any subsequent PPut would be rejected and any
    -- subsequent PUpdate would be directly archived (cf. processPrepareRequest).
    SELECT SubRequest.id INTO unused
      FROM (SELECT id FROM StagePrepareToPutRequest UNION ALL
            SELECT id FROM StagePrepareToUpdateRequest) PrepareRequest, SubRequest
     WHERE SubRequest.castorFile = cfId
       AND PrepareRequest.id = SubRequest.request
       AND SubRequest.status = 6; -- READY
    -- The select worked out, so we have a prepareToPut/Update
    -- In such a case, we do not cleanup DiskCopy and CastorFile
    -- but we have to wake up a potential waiting putDone
    UPDATE SubRequest SET status = 1, parent = 0 -- RESTART
     WHERE id IN
      (SELECT SubRequest.id
        FROM StagePutDoneRequest, SubRequest
       WHERE SubRequest.CastorFile = cfId
         AND StagePutDoneRequest.id = SubRequest.request
         AND SubRequest.status = 5); -- WAITSUBREQ
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- This means we are a standalone put
    -- thus cleanup DiskCopy and maybe the CastorFile
    -- (the physical file is dropped by the job)
    DELETE FROM DiskCopy WHERE id = dcId;
    DELETE FROM Id2Type WHERE id = dcId;
    deleteCastorFile(cfId);
  END;
END;


/* PL/SQL method implementing getSchedulerResources */
CREATE OR REPLACE
PROCEDURE getSchedulerResources(resources OUT castor.SchedulerResources_Cur) AS
BEGIN
  -- Provide a list of all scheduler resources. For the moment this is just
  -- the status of all diskservers, their associated filesystems and the
  -- service class they are in.
  OPEN resources
  FOR SELECT DiskServer.name, DiskServer.status, DiskServer.adminstatus,
	     Filesystem.mountpoint, FileSystem.status, FileSystem.adminstatus,
	     DiskPool.name, SvcClass.name
        FROM DiskServer, FileSystem, DiskPool2SvcClass, DiskPool, SvcClass
       WHERE FileSystem.diskServer = DiskServer.id
         AND FileSystem.diskPool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
         AND DiskPool2SvcClass.parent = DiskPool.id;
END;


/* PL/SQL method implementing failSchedulerJob */
CREATE OR REPLACE
PROCEDURE failSchedulerJob(srSubReqId IN VARCHAR2, srErrorCode IN NUMBER, res OUT INTEGER) AS
  reqType NUMBER;
  srId NUMBER;
  dcId NUMBER;
BEGIN
  -- Get the necessary information needed about the request and subrequest
  SELECT SubRequest.id, SubRequest.diskcopy, Id2Type.type
    INTO srId, dcId, reqType
    FROM SubRequest, Id2Type
   WHERE SubRequest.subreqid = srSubReqId
     AND SubRequest.request = Id2Type.id
     AND SubRequest.status IN (6, 14); -- READY, BEINGSCHED
  -- Call the relevant cleanup procedures for the job, procedures that they
  -- would have called themselves if the job had failed!
  IF reqType = 40 THEN -- Put
    putFailedProc(srId);
  ELSIF reqType = 133 THEN -- DiskCopyReplica
    disk2DiskCopyFailed(dcId, res);
    RETURN;
  ELSE -- Get or Update
    getUpdateFailedProc(srId);
  END IF;
  -- Fail the subrequest
  UPDATE SubRequest
     SET status = 7, -- FAILED
         errorCode = srErrorCode,
         lastModificationTime = getTime()
   WHERE id = srId
     AND answered = 0
  RETURNING id INTO res;
  -- In all cases, restart requests waiting on the failed one
  IF res IS NOT NULL THEN
    -- Wake up other subrequests waiting on it
    UPDATE SubRequest SET status = 1, -- RESTART
                          lastModificationTime = getTime(),
                          parent = 0
     WHERE parent = res AND status = 5; -- WAITSUBREQ
  END IF;
  -- CAUTION: if you add additional processing here make sure the logic
  -- is also covered in the disk2DiskCopyFailed procedure as we don't
  -- get this far in the processing for StageDiskCopyReplicaRequests!!
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- The subrequest may have been removed, nothing to be done
  res := 0;
  RETURN;
END;


/* PL/SQL method implementing postJobChecks */
CREATE OR REPLACE
PROCEDURE postJobChecks(srSubReqId IN VARCHAR2, srErrorCode IN NUMBER, res OUT INTEGER) AS
  unused NUMBER;
BEGIN
  -- Check to see if the subrequest status is SUBREQUEST_READY
  SELECT status INTO unused FROM SubRequest
   WHERE subreqid = srSubReqId
     AND status = 6; -- READY
  -- The job status is incorrect so terminate the scheduler job
  failSchedulerJob(srSubReqId, srErrorCode, res);
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- The subrequest has the correct status because A) the job was successfully
  -- scheduled in LSF or B) a postJobChecks call has already been performed by
  -- another jobManager
  res := 0;
  RETURN;
END;


/* PL/SQL method implementing jobToSchedule */
CREATE OR REPLACE
PROCEDURE jobToSchedule(srId OUT INTEGER, srSubReqId OUT VARCHAR2, srProtocol OUT VARCHAR2,
                        srXsize OUT INTEGER, srRfs OUT VARCHAR2, reqId OUT VARCHAR2,
                        cfFileId OUT INTEGER, cfNsHost OUT VARCHAR2, reqSvcClass OUT VARCHAR2,
                        reqType OUT INTEGER, reqEuid OUT INTEGER, reqEgid OUT INTEGER,
                        reqUsername OUT VARCHAR2, srOpenFlags OUT VARCHAR2, clientIp OUT INTEGER,
                        clientPort OUT INTEGER, clientVersion OUT INTEGER, clientType OUT INTEGER,
                        reqSourceDiskCopy OUT INTEGER, reqDestDiskCopy OUT INTEGER,
                        clientSecure OUT INTEGER, reqSourceSvcClass OUT VARCHAR2,
                        reqCreationTime OUT INTEGER, reqDefaultFileSize OUT INTEGER,
                        excludedHosts OUT castor.DiskServerList_Cur) AS
  dsId NUMBER;
  cfId NUMBER;
  unused NUMBER;
BEGIN
  -- Get the next subrequest to be scheduled.
  UPDATE SubRequest
     SET status = 14, lastModificationTime = getTime() -- BEINGSCHED
   WHERE status = 13 -- READYFORSCHED
     AND rownum < 2
 RETURNING id, subReqId, protocol, xsize, requestedFileSystems
    INTO srId, srSubReqId, srProtocol, srXsize, srRfs;

  -- Extract the rest of the information required to submit a job into the
  -- scheduler through the job manager.
  SELECT CastorFile.id, CastorFile.fileId, CastorFile.nsHost, SvcClass.name,
         Id2type.type, Request.reqId, Request.euid, Request.egid, Request.username,
	 Request.direction, Request.sourceDiskCopy, Request.destDiskCopy,
         Request.sourceSvcClass, Client.ipAddress, Client.port, Client.version,
	 (SELECT type
            FROM Id2type
           WHERE id = Client.id) clientType, Client.secure, Request.creationTime,
         decode(SvcClass.defaultFileSize, 0, 2000000000, SvcClass.defaultFileSize)
    INTO cfId, cfFileId, cfNsHost, reqSvcClass, reqType, reqId, reqEuid, reqEgid,
         reqUsername, srOpenFlags, reqSourceDiskCopy, reqDestDiskCopy, reqSourceSvcClass,
         clientIp, clientPort, clientVersion, clientType, clientSecure, reqCreationTime,
         reqDefaultFileSize
    FROM SubRequest, CastorFile, SvcClass, Id2type, Client,
         (SELECT id, username, euid, egid, reqid, client, creationTime,
                 'w' direction, svcClass, NULL sourceDiskCopy, NULL destDiskCopy,
                 NULL sourceSvcClass
            FROM StagePutRequest
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, creationTime,
                 'r' direction, svcClass, NULL sourceDiskCopy, NULL destDiskCopy,
                 NULL sourceSvcClass
            FROM StageGetRequest
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, creationTime,
                 'o' direction, svcClass, NULL sourceDiskCopy, NULL destDiskCopy,
                 NULL sourceSvcClass
            FROM StageUpdateRequest
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, creationTime - 3600,
                 'w' direction, svcClass, sourceDiskCopy, destDiskCopy,
                 (SELECT name FROM SvcClass WHERE id = sourceSvcClass)
            FROM StageDiskCopyReplicaRequest) Request
   WHERE SubRequest.id = srId
     AND SubRequest.castorFile = CastorFile.id
     AND Request.svcClass = SvcClass.id
     AND Id2type.id = SubRequest.request
     AND Request.id = SubRequest.request
     AND Request.client = Client.id;

  -- Extract additional information required for StageDiskCopyReplicaRequest's
  IF reqType = 133 THEN
    -- Set the requested filesystem for the job to the mountpoint of the
    -- source disk copy. The scheduler plugin needs this information to correctly
    -- schedule access to the filesystem.
    BEGIN
      SELECT CONCAT(CONCAT(DiskServer.name, ':'), FileSystem.mountpoint), DiskServer.id
        INTO srRfs, dsId
        FROM DiskServer, FileSystem, DiskCopy, DiskPool2SvcClass, SvcClass
       WHERE DiskCopy.id = reqSourceDiskCopy
         AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
         AND DiskCopy.filesystem = FileSystem.id
         AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
         AND FileSystem.diskserver = DiskServer.id
         AND DiskServer.status IN (0, 1)  -- PRODUCTION, DRAINING
         AND FileSystem.diskPool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
         AND SvcClass.name = reqSourceSvcClass;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- The source diskcopy has been removed before the jobManager could enter
      -- the job into LSF. Under this circumstance fail the diskcopy transfer.
      -- This will restart the subrequest and trigger a tape recall if possible
      disk2DiskCopyFailed(reqDestDiskCopy, unused);
      COMMIT;
      RAISE;
    END;

    -- Provide the job manager with a list of hosts to exclude as destination
    -- diskservers.
    OPEN excludedHosts FOR
      SELECT distinct(DiskServer.name)
        FROM DiskCopy, DiskServer, FileSystem, DiskPool2SvcClass, SvcClass
       WHERE DiskCopy.filesystem = FileSystem.id
         AND FileSystem.diskserver = DiskServer.id
         AND FileSystem.diskpool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
         AND SvcClass.name = reqSvcClass
         AND DiskCopy.castorfile = cfId
         AND DiskCopy.id != reqSourceDiskCopy
         AND DiskCopy.status IN (0, 1, 2, 10); -- STAGED, DISK2DISKCOPY, WAITTAPERECALL, CANBEMIGR
  END IF;
END;
