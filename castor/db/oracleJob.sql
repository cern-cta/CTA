/*******************************************************************
 *
 * @(#)$RCSfile: oracleJob.sql,v $ $Revision: 1.687 $ $Date: 2009/07/31 15:17:04 $ $Author: waldron $
 *
 * PL/SQL code for scheduling and job handling
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/


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
  -- Get data and lock the CastorFile
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
  IF checkFailPutWhenTape0(sclassId, fclassId) = 1 THEN
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
  -- Invalidate any ongoing replications
  UPDATE DiskCopy SET status = 7 -- INVALID
   WHERE castorFile = cfId
     AND status = 1; -- WAITDISK2DISKCOPY
END;
/


/* Checks whether the protocol used is supporting updates and when not
 * calls firstByteWrittenProc as if the file was already modified */
CREATE OR REPLACE PROCEDURE handleProtoNoUpd
(srId IN INTEGER, protocol VARCHAR2) AS
BEGIN
  IF protocol != 'rfio'  AND
     protocol != 'rfio3' AND
     protocol != 'xroot' THEN
    firstByteWrittenProc(srId);
  END IF;
END;
/


/* PL/SQL method implementing putStart */
CREATE OR REPLACE PROCEDURE putStart
        (srId IN INTEGER, selectedDiskServer IN VARCHAR2, selectedMountPoint IN VARCHAR2,
         rdcId OUT INTEGER, rdcStatus OUT INTEGER, rdcPath OUT VARCHAR2) AS
  srStatus INTEGER;
  srSvcClass INTEGER;
  fsId INTEGER;
  prevFsId INTEGER;
  blah NUMBER;
BEGIN
  -- Get diskCopy and subrequest related information
  SELECT SubRequest.diskCopy, SubRequest.status, DiskCopy.fileSystem,
         Request.svcClass
    INTO rdcId, srStatus, prevFsId, srSvcClass
    FROM SubRequest, DiskCopy,
         (SELECT id, svcClass FROM StagePutRequest UNION ALL
          SELECT id, svcClass FROM StageGetRequest UNION ALL
          SELECT id, svcClass FROM StageUpdateRequest) Request
   WHERE SubRequest.diskcopy = Diskcopy.id
     AND SubRequest.id = srId
     AND SubRequest.request = Request.id;
  -- Check that we did not cancel the SubRequest in the mean time
  IF srStatus IN (7, 9, 10) THEN -- FAILED, FAILED_FINISHED, FAILED_ANSWERING
    raise_application_error(-20104, 'SubRequest canceled while queuing in scheduler. Giving up.');
  END IF;
  -- Check to see if the proposed diskserver and filesystem selected by the
  -- scheduler to run the job is in the correct service class.
  BEGIN
    SELECT FileSystem.id INTO fsId
      FROM DiskServer, FileSystem, DiskPool2SvcClass
     WHERE FileSystem.diskserver = DiskServer.id
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = srSvcClass
       AND DiskServer.name = selectedDiskServer
       AND FileSystem.mountPoint = selectedMountPoint;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20114, 'Job scheduled to the wrong service class. Giving up.');
  END;
  -- Check that a job has not already started for this diskcopy. Refer to
  -- bug #14358
  IF prevFsId > 0 AND prevFsId <> fsId THEN
    raise_application_error(-20107, 'This job has already started for this DiskCopy. Giving up.');
  END IF;
  -- Get older castorFiles with the same name and drop their lastKnownFileName
  UPDATE /*+ INDEX (CastorFile) */ CastorFile
     SET lastKnownFileName = TO_CHAR(id)
   WHERE id IN (
    SELECT /*+ INDEX (cfOld I_CastorFile_lastKnownFileName) */ cfOld.id 
      FROM CastorFile cfOld, CastorFile cfNew, SubRequest
     WHERE cfOld.lastKnownFileName = cfNew.lastKnownFileName
       AND cfOld.fileid <> cfNew.fileid
       AND cfNew.id = SubRequest.castorFile
       AND SubRequest.id = srId);
  -- In case the DiskCopy was in WAITFS_SCHEDULING,
  -- restart the waiting SubRequests
  UPDATE SubRequest 
     SET status = 1, lastModificationTime = getTime(), parent = 0 -- SUBREQUEST_RESTART
   WHERE parent = srId;
  -- link DiskCopy and FileSystem and update DiskCopyStatus
  UPDATE DiskCopy 
     SET status = 6, -- DISKCOPY_STAGEOUT
         fileSystem = fsId,
         nbCopyAccesses = nbCopyAccesses + 1
   WHERE id = rdcId
   RETURNING status, path
   INTO rdcStatus, rdcPath;
END;
/


/* PL/SQL method implementing getUpdateStart */
CREATE OR REPLACE PROCEDURE getUpdateStart
        (srId IN INTEGER, selectedDiskServer IN VARCHAR2, selectedMountPoint IN VARCHAR2,
         dci OUT INTEGER, rpath OUT VARCHAR2, rstatus OUT NUMBER, reuid OUT INTEGER,
         regid OUT INTEGER, diskCopySize OUT NUMBER) AS
  cfid INTEGER;
  fid INTEGER;
  dcId INTEGER;
  fsId INTEGER;
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
  -- Check to see if the proposed diskserver and filesystem selected by the
  -- scheduler to run the job is in the correct service class.
  BEGIN
    SELECT FileSystem.id INTO fsId
      FROM DiskServer, FileSystem, DiskPool2SvcClass
     WHERE FileSystem.diskserver = DiskServer.id
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = srSvcClass
       AND DiskServer.name = selectedDiskServer
       AND FileSystem.mountPoint = selectedMountPoint;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20114, 'Job scheduled to the wrong service class. Giving up.');
  END;
  -- Try to find local DiskCopy
  SELECT /*+ INDEX(DiskCopy I_DiskCopy_Castorfile) */ id, nbCopyAccesses, gcWeight, creationTime
    INTO dcId, nbac, gcw, cTime
    FROM DiskCopy
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.filesystem = fsId
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
  RETURNING id, path, status, diskCopySize INTO dci, rpath, rstatus, diskCopySize;
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
  -- So we first check the update hypothesis
  IF isUpd = 1 AND dcIdInReq IS NOT NULL THEN
    DECLARE
      stat NUMBER;
    BEGIN
      SELECT status INTO stat FROM DiskCopy WHERE id = dcIdInReq;
      IF stat IN (5, 11) THEN -- WAITFS, WAITFS_SCHEDULING
        -- it is an update creating a file, let's call putStart
        putStart(srId, selectedDiskServer, selectedMountPoint, dci, rstatus, rpath);
        RETURN;
      END IF;
    END;
  END IF;
  -- It was not an update creating a file, so we restart
  UPDATE SubRequest SET status = 1 WHERE id = srId;
  dci := 0;
  rpath := '';
END;
/


/* PL/SQL method implementing disk2DiskCopyStart */
CREATE OR REPLACE PROCEDURE disk2DiskCopyStart
(dcId IN INTEGER, srcDcId IN INTEGER, destdiskServer IN VARCHAR2,
 destMountPoint IN VARCHAR2, destPath OUT VARCHAR2, destSvcClass OUT VARCHAR2,
 srcDiskServer OUT VARCHAR2, srcMountPoint OUT VARCHAR2, srcPath OUT VARCHAR2,
 srcSvcClass OUT VARCHAR2) AS
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
           StageDiskCopyReplicaRequest.svcClassName
      INTO unused, destSvcClass
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
      FROM DiskServer, FileSystem, DiskPool2SvcClass, SvcClass
     WHERE FileSystem.diskserver = DiskServer.id
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND DiskPool2SvcClass.child = SvcClass.id
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
/


/* PL/SQL method implementing disk2DiskCopyDone */
CREATE OR REPLACE PROCEDURE disk2DiskCopyDone
(dcId IN INTEGER, srcDcId IN INTEGER) AS
  srId       INTEGER;
  cfId       INTEGER;
  srcStatus  INTEGER;
  srcFsId    NUMBER;
  proto      VARCHAR2(2048);
  reqId      NUMBER;
  svcClassId NUMBER;
  gcwProc    VARCHAR2(2048);
  gcw        NUMBER;
  fileSize   NUMBER;
  ouid       INTEGER;
  ogid       INTEGER;
BEGIN
  -- Lock the CastorFile
  SELECT castorFile INTO cfId
    FROM DiskCopy
   WHERE id = dcId;
  SELECT id INTO cfId FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Try to get the source diskcopy status
  BEGIN
    SELECT status, gcWeight, diskCopySize, fileSystem
      INTO srcStatus, gcw, fileSize, srcFsId
      FROM DiskCopy
     WHERE id = srcDcId
       AND status IN (0, 10);  -- STAGED, CANBEMIGR
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- If no diskcopy was returned it means that the source has either:
    --   A) Been GCed while the copying was taking place or
    --   B) The diskcopy is no longer in a STAGED or CANBEMIGR state.
    -- As we do not know which status to put the new copy in and/or cannot
    -- trust that the file was not modified mid transfer, we invalidate the
    -- new copy.
    UPDATE DiskCopy SET status = 7 WHERE id = dcId -- INVALID
    RETURNING CastorFile INTO cfId;
    -- Restart the SubRequests waiting for the copy
    UPDATE SubRequest SET status = 1, -- SUBREQUEST_RESTART
                          lastModificationTime = getTime()
     WHERE diskCopy = dcId RETURNING id INTO srId;
    UPDATE SubRequest SET status = 1,
                          getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
                          lastModificationTime = getTime(),
                          parent = 0
     WHERE parent = srId; -- SUBREQUEST_RESTART
    -- Archive the diskCopy replica request
    archiveSubReq(srId, 8);  -- FINISHED
    -- Restart all entries in the snapshot of files to drain that may be
    -- waiting on the replication of the source diskcopy.
    UPDATE DrainingDiskCopy
       SET status = 1,  -- RESTARTED
           parent = 0
     WHERE status = 3  -- RUNNING
       AND (diskCopy = srcDcId
        OR  parent = srcDcId);
    drainFileSystem(srcFsId);
    RETURN;
  END;
  -- Otherwise, we can validate the new diskcopy
  -- update SubRequest and get data
  UPDATE SubRequest SET status = 6, -- SUBREQUEST_READY
                        lastModificationTime = getTime()
   WHERE diskCopy = dcId RETURNING id, protocol, request
    INTO srId, proto, reqId;
  SELECT SvcClass.id INTO svcClassId
    FROM SvcClass, StageDiskCopyReplicaRequest Req, SubRequest
   WHERE SubRequest.id = srId
     AND SubRequest.request = Req.id
     AND Req.svcClass = SvcClass.id;
  -- Compute gcWeight
  gcwProc := castorGC.getCopyWeight(svcClassId);
  EXECUTE IMMEDIATE
    'BEGIN :newGcw := ' || gcwProc || '(:size, :status, :gcw); END;'
    USING OUT gcw, IN fileSize, srcStatus, gcw;
  -- Update status
  UPDATE DiskCopy
     SET status = srcStatus,
         gcWeight = gcw
   WHERE id = dcId
  RETURNING castorFile, owneruid, ownergid
    INTO cfId, ouid, ogid;
  -- Wake up waiting subrequests
  UPDATE SubRequest SET status = 1,  -- SUBREQUEST_RESTART
                        getNextStatus = 1, -- GETNEXTSTATUS_FILESTAGED
                        lastModificationTime = getTime(),
                        parent = 0
   WHERE parent = srId;
  -- Archive the diskCopy replica request
  archiveSubReq(srId, 8);  -- FINISHED
  -- Trigger the creation of additional copies of the file, if necessary.
  replicateOnClose(cfId, ouid, ogid);
  -- Restart all entries in the snapshot of files to drain that may be
  -- waiting on the replication of the source diskcopy.
  UPDATE DrainingDiskCopy
     SET status = 1,  -- RESTARTED
         parent = 0
   WHERE status = 3  -- RUNNING
     AND (diskCopy = srcDcId
      OR  parent = srcDcId);
  drainFileSystem(srcFsId);
  -- WARNING: previous call to drainFileSystem has a COMMIT inside. So all
  -- locks have been released!!
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
  -- Lock the CastorFile
  SELECT id INTO cfId FROM CastorFile
   WHERE id = 
    (SELECT castorFile 
       FROM DiskCopy
      WHERE id = dcId) FOR UPDATE;
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


/* PL/SQL method implementing prepareForMigration
   returnCode can take 3 values :
    - 0 : nothing special
    - 1 : the file got deleted while it was written to
    - 2 : the file is 0 bytes and was thus "migrated" by only
          changing the diskCopy status.
*/
CREATE OR REPLACE PROCEDURE prepareForMigration (srId IN INTEGER,
                                                 fs IN INTEGER,
                                                 ts IN NUMBER,
                                                 fId OUT NUMBER,
                                                 nh OUT VARCHAR2,
                                                 returnCode OUT INTEGER) AS
  cfId INTEGER;
  dcId INTEGER;
  svcId INTEGER;
  realFileSize INTEGER;
  unused INTEGER;
  contextPIPP INTEGER;
BEGIN
  returnCode := 0;
  -- Get CastorFile
  SELECT castorFile, diskCopy INTO cfId, dcId FROM SubRequest WHERE id = srId;
  -- Lock the CastorFile
  SELECT id INTO cfId FROM CastorFile WHERE id = cfId FOR UPDATE;
  -- Determine the context (Put inside PrepareToPut or not)
  -- check that we are a Put or an Update
  SELECT Request.id INTO unused
    FROM SubRequest,
       (SELECT id FROM StagePutRequest UNION ALL
        SELECT id FROM StageUpdateRequest) Request
   WHERE SubRequest.id = srId
     AND Request.id = SubRequest.request;
  BEGIN
    -- Check that there is a PrepareToPut/Update going on. There can be only a single one
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
    -- So we are in the case, we give up
    returnCode := 1;
    -- But we still would like to have the fileId and nameserver
    -- host for logging reasons
    SELECT fileId, nsHost INTO fId, nh
      FROM CastorFile WHERE id = cfId;
    RETURN;
  END;
  -- Now we can safely update CastorFile's file size
  UPDATE CastorFile SET fileSize = fs, lastUpdateTime = ts
   WHERE id = cfId AND (lastUpdateTime IS NULL OR ts >= lastUpdateTime);
  -- If ts < lastUpdateTime, we were late and another job already updated the
  -- CastorFile. So we nevertheless retrieve the real file size, and
  -- we take a lock on the CastorFile. Together with triggers, this insures that
  -- we are the only ones to deal with its copies.
  SELECT fileId, nsHost, fileSize INTO fId, nh, realFileSize
    FROM CastorFile
    WHERE id = cfId
    FOR UPDATE;
  -- Get uid, gid and svcclass from Request
  SELECT svcClass INTO svcId
    FROM SubRequest,
      (SELECT id, svcClass FROM StagePutRequest UNION ALL
       SELECT id, svcClass FROM StageUpdateRequest UNION ALL
       SELECT id, svcClass FROM StagePutDoneRequest) Request
   WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
  IF contextPIPP != 0 THEN
    -- If not a put inside a PrepareToPut/Update, create TapeCopies
    -- and update DiskCopy status
    putDoneFunc(cfId, realFileSize, contextPIPP, svcId);
    -- identify the cases where a file is 0 bytes and has thus been
    -- "migrated" by only updating the DiskCopy status
    -- this case will then be handled by the OraJobSvc in order
    -- to drop any potential existing segment in the nameserver
    IF realFileSize = 0 THEN
      returnCode := 2;
    END IF;
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
  -- Archive Subrequest
  archiveSubReq(srId, 8);  -- FINISHED
END;
/


/* PL/SQL method implementing getUpdateDone */
CREATE OR REPLACE PROCEDURE getUpdateDoneProc
(srId IN NUMBER) AS
BEGIN
  archiveSubReq(srId, 8);  -- FINISHED
END;
/


/* PL/SQL method implementing getUpdateFailed */
CREATE OR REPLACE PROCEDURE getUpdateFailedProc
(srId IN NUMBER) AS
BEGIN
  -- Fail the subrequest. The stager will try and answer the client
  UPDATE SubRequest
     SET status = 7 -- FAILED
   WHERE id = srId;
  -- Wake up other subrequests waiting on it
  UPDATE SubRequest
     SET parent = 0, status = 1, -- RESTART
         lastModificationTime = getTime()
   WHERE parent = srId;
END;
/


/* PL/SQL method implementing putFailedProc */
CREATE OR REPLACE PROCEDURE putFailedProc(srId IN NUMBER) AS
  dcId INTEGER;
  cfId INTEGER;
  unused INTEGER;
BEGIN
  SELECT diskCopy, castorFile INTO dcId, cfId
    FROM SubRequest
   WHERE id = srId;
  -- Fail the subRequest
  UPDATE SubRequest
     SET status = 7 -- FAILED
   WHERE id = srId;
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
/


/* PL/SQL function implementing checkAvailOfSchedulerRFS, this function can be
 * used to determine if any of the requested filesystems specified by a
 * transfer are available. Returns 0 if at least one requested filesystem is
 * available otherwise 1.
 */
CREATE OR REPLACE FUNCTION checkAvailOfSchedulerRFS
  (rfs IN VARCHAR2, reqType IN NUMBER)
RETURN NUMBER IS
  rtn NUMBER;
BEGIN
  -- Count the number of requested filesystems which are available
  SELECT count(*) INTO rtn
    FROM DiskServer, FileSystem 
   WHERE DiskServer.id = FileSystem.diskServer
     AND DiskServer.name || ':' || FileSystem.mountPoint IN
       (SELECT /*+ CARDINALITY(rfsTable 10) */ * 
          FROM TABLE (strTokenizer(rfs, '|')) rfsTable)
     -- For a requested filesystem to be available the following criteria
     -- must be meet:
     --  - The diskserver and filesystem must not be in a DISABLED state
     --  - For StageDiskCopyReplicaRequests all other states are accepted
     --  - For all other requests the diskserver and filesystem must be in
     --    PRODUCTION
     AND decode(DiskServer.status, 2, 1,    -- Exclude DISABLED Diskservers
           decode(FileSystem.status, 2, 1,  -- Exclude DISABLED Filesystems
             decode(reqType, 133, 0,
                 decode(FileSystem.status + DiskServer.status, 0, 0, 1)))) = 0;
  IF rtn > 0 THEN
    RETURN 0;  -- We found some available requested filesystems
  END IF;
  RETURN 1;
END;
/


/* PL/SQL method implementing getSchedulerJobs. This method lists all known
 * transfers and whether they should be terminated because no space exists
 * in the target service class or none of the requested filesystems are
 * available.
 */
CREATE OR REPLACE PROCEDURE getSchedulerJobs
  (transfers OUT castor.SchedulerJobs_Cur) AS
BEGIN
  OPEN transfers FOR
    -- Use the NO_MERGE hint to prevent Oracle from executing the
    -- checkFailJobsWhenNoSpace function for every row in the output. In
    -- situations where there are many PENDING transfers in the scheduler
    -- this can be extremely inefficient and expensive.
    SELECT /*+ NO_MERGE(NSSvc) */
           SR.subReqId, Request.reqId, NSSvc.NoSpace,
           -- If there are no requested filesystems, refer to the NFSSvc
           -- output otherwise call the checkAvailOfSchedulerRFS function
           decode(SR.requestedFileSystems, NULL, NFSSvc.NoFSAvail,
             checkAvailOfSchedulerRFS(SR.requestedFileSystems,
                                      Request.reqType)) NoFSAvail
      FROM SubRequest SR,
        -- Union of all requests that could result in scheduler transfers
        (SELECT id, svcClass, reqid, 40  AS reqType
           FROM StagePutRequest                     UNION ALL
         SELECT id, svcClass, reqid, 133 AS reqType
           FROM StageDiskCopyReplicaRequest         UNION ALL
         SELECT id, svcClass, reqid, 35  AS reqType
           FROM StageGetRequest                     UNION ALL
         SELECT id, svcClass, reqid, 44  AS reqType
           FROM StageUpdateRequest) Request,
        -- Table of all service classes with a boolean flag to indicate
        -- if space is available
        (SELECT id, checkFailJobsWhenNoSpace(id) NoSpace
           FROM SvcClass) NSSvc,
        -- Table of all service classes with a boolean flag to indicate
        -- if there are any filesystems in PRODUCTION
        (SELECT id, nvl(NoFSAvail, 1) NoFSAvail FROM SvcClass
           LEFT JOIN 
             (SELECT DP2Svc.CHILD, decode(count(*), 0, 1, 0) NoFSAvail
                FROM DiskServer DS, FileSystem FS, DiskPool2SvcClass DP2Svc
               WHERE DS.ID = FS.diskServer
                 AND DS.status = 0  -- DISKSERVER_PRODUCTION
                 AND FS.diskPool = DP2Svc.parent
                 AND FS.status = 0  -- FILESYSTEM_PRODUCTION
               GROUP BY DP2Svc.child) results
             ON SvcClass.id = results.child) NFSSvc
     WHERE SR.status = 6  -- READY
       AND SR.request = Request.id
       AND SR.creationTime < getTime() - 60
       AND NSSvc.id = Request.svcClass
       AND NFSSvc.id = Request.svcClass;
END;
/


/* PL/SQL method implementing jobFailed, providing bulk termination of file
 * transfers.
 */
CREATE OR REPLACE
PROCEDURE jobFailed(subReqIds IN castor."strList", errnos IN castor."cnumList",
                    failedSubReqs OUT castor.JobFailedSubReqList_Cur)
AS
  srId  NUMBER;
  dcId  NUMBER;
  cfId  NUMBER;
  rType NUMBER;
BEGIN
  -- Clear the temporary table
  DELETE FROM JobFailedProcHelper;
  -- Loop over all jobs to fail
  FOR i IN subReqIds.FIRST .. subReqIds.LAST LOOP
    BEGIN
      -- Get the necessary information needed about the request.
      SELECT SubRequest.id, SubRequest.diskCopy,
             Id2Type.type, SubRequest.castorFile
        INTO srId, dcId, rType, cfId
        FROM SubRequest, Id2Type
       WHERE SubRequest.subReqId = subReqIds(i)
         AND SubRequest.status IN (6, 14)  -- READY, BEINGSCHED
         AND SubRequest.request = Id2Type.id;
       -- Lock the CastorFile.
       SELECT id INTO cfId FROM CastorFile
        WHERE id = cfId FOR UPDATE;
       -- Confirm SubRequest status hasn't changed after acquisition of lock
       SELECT id INTO srId FROM SubRequest
        WHERE id = srId AND status IN (6, 14);  -- READY, BEINGSCHED
       -- Update the reason for termination.
       UPDATE SubRequest 
          SET errorCode = decode(errnos(i), 0, errorCode, errnos(i))
        WHERE id = srId;
       -- Call the relevant cleanup procedure for the job, procedures that
       -- would have been called if the job failed on the remote execution host.
       IF rType = 40 THEN      -- StagePutRequest
         putFailedProc(srId);
       ELSIF rType = 133 THEN  -- StageDiskCopyReplicaRequest
         disk2DiskCopyFailed(dcId, 0);
       ELSE                    -- StageGetRequest or StageUpdateRequest
         getUpdateFailedProc(srId);
       END IF;
       -- Record in the JobFailedProcHelper temporary table that an action was
       -- taken
       INSERT INTO JobFailedProcHelper VALUES (subReqIds(i));
       -- Release locks
       COMMIT;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;  -- The SubRequest may have be removed, nothing to be done.
    END;
  END LOOP;
  -- Return the list of terminated jobs
  OPEN failedSubReqs FOR
    SELECT subReqId FROM JobFailedProcHelper;
END;
/


/* PL/SQL method implementing jobToSchedule */
CREATE OR REPLACE
PROCEDURE jobToSchedule(srId OUT INTEGER,              srSubReqId OUT VARCHAR2,
                        srProtocol OUT VARCHAR2,       srXsize OUT INTEGER,
                        srRfs OUT VARCHAR2,            reqId OUT VARCHAR2,
                        cfFileId OUT INTEGER,          cfNsHost OUT VARCHAR2,
                        reqSvcClass OUT VARCHAR2,      reqType OUT INTEGER,
                        reqEuid OUT INTEGER,           reqEgid OUT INTEGER,
                        reqUsername OUT VARCHAR2,      srOpenFlags OUT VARCHAR2,
                        clientIp OUT INTEGER,          clientPort OUT INTEGER,
                        clientVersion OUT INTEGER,     clientType OUT INTEGER,
                        reqSourceDiskCopy OUT INTEGER, reqDestDiskCopy OUT INTEGER,
                        clientSecure OUT INTEGER,      reqSourceSvcClass OUT VARCHAR2,
                        reqCreationTime OUT INTEGER,   reqDefaultFileSize OUT INTEGER,
                        excludedHosts OUT castor.DiskServerList_Cur) AS
  cfId NUMBER;
  -- Cursor to select the next candidate for submission to the scheduler orderd
  -- by creation time.
  CURSOR c IS
    SELECT /*+ FIRST_ROWS(10) INDEX(SR I_SubRequest_RT_CT_ID) */ SR.id
      FROM SubRequest
 PARTITION (P_STATUS_13_14) SR  -- RESTART, READYFORSCHED, BEINGSCHED
     ORDER BY SR.creationTime ASC;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
BEGIN
  -- Loop on all candidates for submission into LSF
  OPEN c;
  LOOP
    -- Retrieve the next candidate
    FETCH c INTO srId;
    IF c%NOTFOUND THEN
      -- There are no candidates available, throw a NO_DATA_FOUND exception
      -- which indicates to the job manager to retry in 10 seconds time
      RAISE NO_DATA_FOUND;
    END IF; 
    BEGIN
      -- Try to lock the current candidate, verify that the status is valid. A
      -- valid subrequest is either in READYFORSCHED or has been stuck in
      -- BEINGSCHED for more than 1800 seconds (30 mins)
      SELECT /*+ INDEX(SR PK_SubRequest_ID) */ id INTO srId
        FROM SubRequest PARTITION (P_STATUS_13_14) SR
       WHERE id = srId
         AND ((status = 13)  -- READYFORSCHED
          OR  (status = 14   -- BEINGSCHED
         AND lastModificationTime < getTime() - 1800))
         FOR UPDATE;
      -- We have successfully acquired the lock, so we update the subrequest
      -- status and modification time
      UPDATE SubRequest
         SET status = 14,  -- BEINGSHCED
             lastModificationTime = getTime()
       WHERE id = srId
      RETURNING id, subReqId, protocol, xsize, requestedFileSystems
        INTO srId, srSubReqId, srProtocol, srXsize, srRfs;
      EXIT;
    EXCEPTION
      -- Try again, either we failed to accquire the lock on the subrequest or
      -- the subrequest being processed is not the correct state 
      WHEN NO_DATA_FOUND THEN
        NULL;
      WHEN SrLocked THEN
        NULL;
    END;
  END LOOP;
  CLOSE c;

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
                 'w' direction, svcClass, NULL sourceDiskCopy,
                 NULL destDiskCopy, NULL sourceSvcClass
            FROM StagePutRequest
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, creationTime,
                 'r' direction, svcClass, NULL sourceDiskCopy,
                 NULL destDiskCopy, NULL sourceSvcClass
            FROM StageGetRequest
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, creationTime,
                 'o' direction, svcClass, NULL sourceDiskCopy,
                 NULL destDiskCopy, NULL sourceSvcClass
            FROM StageUpdateRequest
           UNION ALL
          SELECT id, username, euid, egid, reqid, client, creationTime,
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
    -- source disk copy. The scheduler plugin needs this information to
    -- correctly schedule access to the filesystem.
    BEGIN
      SELECT DiskServer.name || ':' || FileSystem.mountpoint INTO srRfs
        FROM DiskServer, FileSystem, DiskCopy, DiskPool2SvcClass, SvcClass
       WHERE DiskCopy.id = reqSourceDiskCopy
         AND DiskCopy.status IN (0, 10)  -- STAGED, CANBEMIGR
         AND DiskCopy.filesystem = FileSystem.id
         AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
         AND FileSystem.diskserver = DiskServer.id
         AND DiskServer.status IN (0, 1)  -- PRODUCTION, DRAINING
         AND FileSystem.diskPool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = SvcClass.id
         AND SvcClass.name = reqSourceSvcClass;
      UPDATE SubRequest SET requestedFileSystems = srRfs
       WHERE id = srId;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- The source diskcopy has been removed before the job manager daemon 
      -- could enter the job into LSF. Under this circumstance fail the 
      -- diskcopy transfer. This will restart the subrequest and trigger a tape
      -- recall if possible
      disk2DiskCopyFailed(reqDestDiskCopy, 0);
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
/
