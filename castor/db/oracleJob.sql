/*******************************************************************
 *
 *
 * PL/SQL code for scheduling and job handling
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

/* PL/SQL method implementing putStart */
CREATE OR REPLACE PROCEDURE putStart(inTransferId IN VARCHAR2, selectedDiskServer IN VARCHAR2,
                                     selectedMountPoint IN VARCHAR2, outPath OUT VARCHAR2) AS
  varCfId INTEGER;
  varDcId INTEGER;
  srStatus INTEGER;
  srSvcClass INTEGER;
  fsId INTEGER := NULL;
  varDsId INTEGER := NULL;
  dpId INTEGER := NULL;
  fsStatus INTEGER := dconst.FILESYSTEM_PRODUCTION;
  dsStatus INTEGER;
  varHwOnline INTEGER;
  prevFsId INTEGER;
  prevDPId INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(100);
  varDpName VARCHAR2(2048);
BEGIN
  -- Get data and lock castorfile
  SELECT /*+ INDEX_RS_ASC(SubRequest I_SubRequest_SubReqId) */
         SubRequest.castorFile, DiskCopy.id, SubRequest.status, DiskCopy.fileSystem,
         DiskCopy.dataPool, Request.svcClass, CastorFile.fileId, CastorFile.nsHost
    INTO varCfId, varDcId, srStatus, prevFsId, prevDPId, srSvcClass, varFileid, varNsHost
    FROM SubRequest, DiskCopy, StagePutRequest Request, CastorFile
   WHERE SubRequest.diskcopy = Diskcopy.id
     AND SubRequest.subreqId = inTransferId
     AND SubRequest.request = Request.id
     AND CastorFile.id = SubRequest.castorFile
     FOR UPDATE OF CastorFile.id;
  -- Check that we did not cancel the SubRequest in the mean time
  IF srStatus IN (dconst.SUBREQUEST_FAILED, dconst.SUBREQUEST_FAILED_FINISHED) THEN
    raise_application_error(-20104, 'SubRequest canceled while queuing in scheduler. Giving up.');
  END IF;
  -- Get selected filesystem/datapool
  IF selectedMountPoint IS NULL THEN
    SELECT DiskServer.dataPool, DiskServer.status,
           DiskServer.hwOnline, DiskServer.id, DataPool.name
      INTO dpId, dsStatus, varHwOnline, varDsId, varDpName
      FROM DiskServer, DataPool
     WHERE DiskServer.name = selectedDiskServer
       AND DataPool.id = DiskServer.dataPool;
  ELSE 
    SELECT FileSystem.id, FileSystem.status, DiskServer.status, DiskServer.hwOnline
      INTO fsId, fsStatus, dsStatus, varHwOnline
      FROM DiskServer, FileSystem
     WHERE FileSystem.diskserver = DiskServer.id
       AND DiskServer.name = selectedDiskServer
       AND FileSystem.mountPoint = selectedMountPoint;
  END IF;
  -- Check that a job has not already started for this diskcopy. Refer to
  -- bug #14358
  IF (prevFsId > 0 AND prevFsId <> fsId) OR (prevDPId > 0 AND prevDPId <> dpId) THEN
    raise_application_error(-20104, 'This job has already started for this DiskCopy. Giving up.');
  END IF;
  IF fsStatus != dconst.FILESYSTEM_PRODUCTION OR dsStatus != dconst.DISKSERVER_PRODUCTION OR varHwOnline = 0 THEN
    raise_application_error(-20104, 'The selected diskserver/filesystem is not in PRODUCTION any longer. Giving up.');
  END IF;
  -- In case the DiskCopy was in WAITFS_SCHEDULING,
  -- restart the waiting SubRequests
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_RESTART, lastModificationTime = getTime(),
         diskServer = varDsId
   WHERE status = dconst.SUBREQUEST_WAITSUBREQ
     AND castorFile = varCfId;
  alertSignalNoLock('wakeUpJobReqSvc');
  -- compute path of this diskcopy
  buildPathFromFileId(varFileId, varNsHost, varDcId, outPath, selectedMountPoint IS NOT NULL);
  -- link DiskCopy and FileSystem/DataPool and update DiskCopyStatus
  UPDATE DiskCopy
     SET status = 6, -- DISKCOPY_STAGEOUT
         path = outPath,
         fileSystem = fsId,
         dataPool = dpId,
         nbCopyAccesses = nbCopyAccesses + 1
   WHERE id = varDcId;
  IF selectedMountPoint IS NULL THEN
    outPath := getConfigOption('DiskManager', 'PoolUserId', 'castor') || '@' || varDpName || '/' || outPath;
  ELSE
    outPath := selectedMountPoint || outPath;
  END IF;
  -- Log successful completion
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.STAGER_PUTSTART, varFileId, varNsHost, 'stagerd', 'SUBREQID='|| inTransferId
    || 'destinationPath=' || selectedDiskServer ||':'|| selectedMountPoint);
EXCEPTION WHEN NO_DATA_FOUND THEN
  raise_application_error(-20104, 'SubRequest canceled while queuing in scheduler. Giving up.');
END;
/

/* PL/SQL method implementing getStart */
CREATE OR REPLACE PROCEDURE getStart(inTransferId IN VARCHAR2, selectedDiskServer IN VARCHAR2,
                                     selectedMountPoint IN VARCHAR2, outPath OUT VARCHAR2) AS
  srId INTEGER;
  cfId INTEGER;
  dcId INTEGER;
  fsId INTEGER := NULL;
  varDsId INTEGER := NULL;
  dpId INTEGER := NULL;
  nh VARCHAR2(2048);
  fileSize INTEGER;
  srSvcClass INTEGER;
  proto VARCHAR2(2048);
  nbAc NUMBER;
  gcw NUMBER;
  gcwProc VARCHAR2(2048);
  cTime NUMBER;
  fsStatus INTEGER := dconst.FILESYSTEM_PRODUCTION;
  dsStatus INTEGER;
  varHwOnline INTEGER;
  varDiskCopySize INTEGER;
  varDcStatus INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(100);
  varDpName VARCHAR2(2048);
BEGIN
  -- Get data and take a lock on the CastorFile. Associated with triggers,
  -- this guarantees we are the only ones dealing with its copies
  SELECT /*+ INDEX_RS_ASC(SubRequest I_SubRequest_SubReqId)
             INDEX_RS_ASC(StageGetRequest PK_StageGetRequest_Id) */
         CastorFile.fileSize, CastorFile.id,
         Req.svcClass, CastorFile.fileId, CastorFile.nsHost, SubRequest.id
    INTO fileSize, cfId, srSvcClass, varFileId, varNsHost, srId
    FROM CastorFile, SubRequest, StageGetRequest Req
   WHERE CastorFile.id = SubRequest.castorFile
     AND SubRequest.request = Req.id
     AND SubRequest.subreqId = inTransferid
     FOR UPDATE OF CastorFile.id;
  -- Get selected filesystem/datapool
  IF selectedMountPoint IS NULL THEN
    SELECT DiskServer.dataPool, DiskServer.status,
           DiskServer.hwOnline, DiskServer.id, DataPool.name
      INTO dpId, dsStatus, varHwOnline, varDsId, varDpName
      FROM DiskServer, DataPool
     WHERE DiskServer.name = selectedDiskServer
       AND DataPool.id = DiskServer.dataPool;
  ELSE 
    SELECT FileSystem.id, FileSystem.status, DiskServer.status, DiskServer.hwOnline
      INTO fsId, fsStatus, dsStatus, varHwOnline
      FROM DiskServer, FileSystem
     WHERE FileSystem.diskserver = DiskServer.id
       AND DiskServer.name = selectedDiskServer
       AND FileSystem.mountPoint = selectedMountPoint;
  END IF;
  -- Now check that the hardware status is still valid for a Get request
  IF NOT (fsStatus IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY) AND
          dsStatus IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY) AND
          varHwOnline = 1) THEN
    raise_application_error(-20114, 'The selected diskserver/filesystem is not in PRODUCTION or READONLY any longer. Giving up.');
  END IF;
  -- Check that the request is still valid
  SELECT id INTO srId
    FROM SubRequest
   WHERE id = srId AND status NOT IN (dconst.SUBREQUEST_FAILED, dconst.SUBREQUEST_FAILED_FINISHED);
  -- Try to find local DiskCopy
  SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_Castorfile) */
         id, nbCopyAccesses, gcWeight, creationTime
    INTO dcId, nbac, gcw, cTime
    FROM DiskCopy
   WHERE DiskCopy.castorfile = cfId
     AND (DiskCopy.filesystem = fsId OR DiskCopy.dataPool = dpId)
     AND DiskCopy.status IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_STAGEOUT)
     AND ROWNUM < 2;
  -- We found it, so we are settled and we'll use the local copy.
  -- For the ROWNUM < 2 condition: it might happen that we have more than one, because
  -- the scheduling may have scheduled a replication on a fileSystem which already had a previous diskcopy.
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
  RETURNING path, status, diskCopySize
    INTO outPath, varDcStatus, varDiskCopySize;
  IF selectedMountPoint IS NULL THEN
    outPath := getConfigOption('DiskManager', 'PoolUserId', 'castor') || '@' || varDpName || '/' || outPath;
  ELSE
    outPath := selectedMountPoint || outPath;
  END IF;
  -- Update the SubRequest and set the link with the DiskCopy
  UPDATE /*+ INDEX_RS_ASC(SubRequest PK_Subrequest_Id)*/ SubRequest
     SET diskCopy = dcId
   WHERE id = srId;
  -- Deal with recalls of empty files: redirect to /dev/null
  IF varDcStatus = dconst.DISKCOPY_VALID AND varDiskCopySize = 0 THEN
    outPath := '/dev/null';
  END IF;
  -- Log successful completion
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.STAGER_GETSTART, varFileId, varNsHost, 'stagerd', 'SUBREQID='|| inTransferId
    || ' destinationPath=' || selectedDiskServer ||':'|| selectedMountPoint);
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- No disk copy found on selected FileSystem, or subRequest not valid any longer.
  -- This can happen if a diskcopy was available and got disabled, or if an abort came,
  -- before this job was scheduled. Bad luck, we fail the request, the user will have to retry
  UPDATE SubRequest
     SET status = dconst.SUBREQUEST_FAILED, errorCode = 1725, errorMessage = 'Request canceled while queuing'
   WHERE id = srId;
  COMMIT;
  raise_application_error(-20114, 'File invalidated while queuing in the scheduler, please try again');
END;
/

/* PL/SQL method implementing getEnded. The transfer is considered successful iff errno = 0 */
CREATE OR REPLACE PROCEDURE getEnded(inTransferId IN VARCHAR2, inoutErrorCode IN OUT INTEGER, errmsg IN VARCHAR2) AS
  varSrId INTEGER;
  varCfId INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(100);
BEGIN
  -- Update the subrequest. Note we don't lock the CastorFile entry
  -- as we only touch this subrequest.
  UPDATE /*+ INDEX_RS_ASC(SubRequest I_SubRequest_SubReqId)*/ SubRequest
     SET status = CASE WHEN inoutErrorCode > 0 THEN dconst.SUBREQUEST_FAILED ELSE dconst.SUBREQUEST_FINISHED END,
         errorCode = inoutErrorCode,
         errorMessage = errmsg
   WHERE subReqId = inTransferId
  RETURNING castorFile, id INTO varCfId, varSrId;
  -- for logging purposes
  SELECT fileId, nsHost INTO varFileId, varNsHost
    FROM CastorFile
   WHERE id = varCfId;
  IF inoutErrorCode = 0 THEN
    -- no error, archive and log
    archiveSubReq(varSrId, dconst.SUBREQUEST_FINISHED);
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.STAGER_GETENDED, varFileId, varNsHost, 'stagerd', 'SUBREQID='|| inTransferId);
  ELSE
    -- request failed, fail and log
    archiveSubReq(varSrId, dconst.SUBREQUEST_FAILED_FINISHED);
    logToDLF(NULL, dlf.LVL_NOTICE, dlf.STAGER_GETENDED, varFileId, varNsHost, 'stagerd',
      'SUBREQID='|| inTransferId ||' errorMessage="'|| errmsg ||'" errorCode='|| inoutErrorCode);
    -- and return 0, the error has been handled
    inoutErrorCode := 0;
  END IF;
END;
/

/* PL/SQL method implementing putEnded.
 * If inoutErrorCode is > 0, the write operation is failed and inFileSize, inNewTimeStamp,
 * inCksumType and inCksumValue are ignored.
 */
CREATE OR REPLACE PROCEDURE putEnded(inTransferId IN VARCHAR2,
                                     inFileSize IN INTEGER,
                                     inNewTimeStamp IN NUMBER,
                                     inCksumType IN VARCHAR2,
                                     inCksumValue IN VARCHAR2,
                                     inoutErrorCode IN OUT INTEGER,
                                     inoutErrorMsg IN OUT VARCHAR2) AS
  srId INTEGER;
  cfId INTEGER;
  dcId INTEGER;
  svcId INTEGER;
  varRealFileSize INTEGER;
  unused INTEGER;
  contextPIPP INTEGER;
  varLastUpdTime NUMBER;
  varLastOpenTime NUMBER;
  varMsg VARCHAR2(2048) := '';
  varFileId INTEGER;
  varNsHost VARCHAR(2048);
BEGIN
  -- Get data and lock CastorFile
  SELECT /*+ INDEX_RS_ASC(SubRequest I_SubRequest_SubReqId) */ castorFile, diskCopy, SubRequest.id,
         CastorFile.id, fileid, nsHost, nvl(lastUpdateTime, 0), nsOpenTime
    INTO cfId, dcId, srId, cfId, varFileId, varNsHost, varLastUpdTime, varLastOpenTime
    FROM SubRequest, CastorFile
   WHERE subreqId = inTransferId
     AND SubRequest.castorFile = CastorFile.id
     FOR UPDATE OF CastorFile.id;
  -- Determine the context (Put inside PrepareToPut or not)
  BEGIN
    -- Check that there is a PrepareToPut going on. There can be only a
    -- single one or none. If there was a PrepareTo, any subsequent PPut would be rejected
    SELECT /*+ INDEX_RS_ASC(SubRequest I_Subrequest_Castorfile)
               INDEX_RS_ASC(PReq PK_StagePrepareToPutRequest_Id) */
           SubRequest.id INTO unused
      FROM SubRequest, StagePrepareToPutRequest PReq
     WHERE SubRequest.CastorFile = cfId
       AND PReq.id = SubRequest.request
       AND SubRequest.status = dconst.SUBREQUEST_READY;
    -- If we got here, we are a Put inside a PrepareToPut
    contextPIPP := 0;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Here we are a standalone Put
    contextPIPP := 1;
  END;
  -- Failure upstream?
  IF inoutErrorCode > 0 THEN
    -- fail the subRequest
    UPDATE /*+ INDEX_RS_ASC(SubRequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = inoutErrorCode,
           errorMessage = inoutErrorMsg
     WHERE id = srId;
    -- invalidate the DiskCopy, so that the GC has a chance to clean up
    UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID
     WHERE id = dcId;
    logToDLF(NULL, dlf.LVL_NOTICE, dlf.STAGER_PUTENDED, varFileId, varNsHost, 'stagerd',
      'SUBREQID='|| inTransferId ||' errorMessage="'|| inoutErrorMsg ||'" errorCode='|| inoutErrorCode);
    -- The error was dealt with, return 0 now
    inoutErrorCode := 0;
    RETURN;
  END IF;
  -- Check whether the diskCopy is still in STAGEOUT. If not, the file
  -- was deleted via stageRm while being written to. Thus, we should just give up
  BEGIN
    SELECT status INTO unused
      FROM DiskCopy WHERE id = dcId AND status = dconst.DISKCOPY_STAGEOUT;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- So we are in the case, we give up
    logToDLF(NULL, dlf.LVL_USER_ERROR, dlf.STAGER_DELETED_WRITTENTO, varFileId, varNsHost, 'stagerd', 'SUBREQID='|| inTransferId);
    -- and fail the subRequest
    UPDATE /*+ INDEX_RS_ASC(SubRequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = serrno.EBUSY,
           errorMessage = dlf.STAGER_DELETED_WRITTENTO
     WHERE id = srId;
    IF contextPIPP != 0 THEN
      -- On a standalone put cleanup DiskCopy and maybe the CastorFile
      -- (the physical file is dropped by the mover)
      DELETE FROM DiskCopy WHERE id = dcId;
      deleteCastorFile(cfId);
    END IF;
    RETURN;
  END;
  -- Check if the timestamps allow us to update
  IF inNewTimeStamp >= varLastUpdTime THEN
    -- Now we can safely update CastorFile's file size and time stamps
    UPDATE CastorFile SET fileSize = inFileSize, lastUpdateTime = inNewTimeStamp
     WHERE id = cfId;
  END IF;
  -- If ts < lastUpdateTime, we were late and another job already updated the
  -- CastorFile. So we nevertheless retrieve the real file size.
  SELECT fileSize INTO varRealFileSize FROM CastorFile WHERE id = cfId;
  -- Now close the file on the Nameserver
  BEGIN
    closex@remoteNS(varFileId, varRealFileSize, inCksumType, inCksumValue, inNewTimeStamp, varLastOpenTime, inoutErrorCode, varMsg);
    IF inoutErrorCode = 0 THEN
      logToDLF(NULL, dlf.LVL_SYSTEM, dlf.NS_PROCESSING_COMPLETE, varFileId, varNsHost, 'nsd', varMsg || ' SUBREQID='|| inTransferId);
    ELSE
      -- Nameserver error: log and fail the entire operation
      logToDLF(NULL, dlf.LVL_USER_ERROR, dlf.NS_CLOSEX_ERROR, varFileId, varNsHost, 'nsd', 'errorMessage="' || varMsg ||'" SUBREQID='|| inTransferId);
      inoutErrorMsg := varMsg;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    inoutErrorCode := serrno.SEINTERNAL;
    inoutErrorMsg := 'Internal error closing file in the Nameserver';
    logToDLF(NULL, dlf.LVL_ERROR, dlf.NS_CLOSEX_ERROR, varFileId, varNsHost, 'nsd', 'errorMessage="' || SQLERRM ||'" SUBREQID='|| inTransferId);
  END;
  IF inoutErrorCode != 0 THEN
    -- fail the subRequest
    UPDATE /*+ INDEX_RS_ASC(SubRequest PK_Subrequest_Id)*/ SubRequest
       SET status = dconst.SUBREQUEST_FAILED,
           errorCode = inoutErrorCode,
           errorMessage = varMsg
     WHERE id = srId;
    -- invalidate the DiskCopy, so that the GC has a chance to clean up
    UPDATE DiskCopy SET status = dconst.DISKCOPY_INVALID
     WHERE id = dcId;
    -- No log for the stager, it would be a duplicate of the nsd one
    RETURN;
  END IF;
  -- Get svcclass from Request
  SELECT /*+ INDEX_RS_ASC(SubRequest PK_Subrequest_Id)*/ svcClass INTO svcId
    FROM SubRequest,
      (SELECT /*+ INDEX_RS_ASC(StagePutRequest PK_StagePutRequest_Id) */ id, svcClass FROM StagePutRequest UNION ALL
       SELECT /*+ INDEX_RS_ASC(StagePutDoneRequest PK_StagePutDoneRequest_Id) */ id, svcClass FROM StagePutDoneRequest) Request
   WHERE SubRequest.request = Request.id AND SubRequest.id = srId;
  IF contextPIPP != 0 THEN
    -- If not a put inside a PrepareToPut/Update, trigger migration
    -- and update DiskCopy status
    putDoneFunc(cfId, varRealFileSize, contextPIPP, svcId);
  ELSE
    -- If put inside PrepareToPut/Update, restart any PutDone currently
    -- waiting on this put/update
    UPDATE /*+ INDEX_RS_ASC(SubRequest I_Subrequest_Castorfile)*/ SubRequest
       SET status = dconst.SUBREQUEST_RESTART
     WHERE reqType = 39  -- PutDone
       AND castorFile = cfId
       AND status = dconst.SUBREQUEST_WAITSUBREQ;
    -- and wake up the stager for processing it
    alertSignalNoLock('wakeUpStageReqSvc');
  END IF;
  -- Archive Subrequest
  archiveSubReq(srId, 8);  -- FINISHED
  -- Log successful completion
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.STAGER_PUTENDED, varFileId, varNsHost, 'stagerd',
    'SUBREQID='|| inTransferId ||' ChkSumType='|| inCksumType ||' ChkSumValue='|| inCksumValue ||' fileSize='|| varRealFileSize);
END;
/


/* Update a drainingJob at the end of a disk2diskcopy */
CREATE OR REPLACE PROCEDURE updateDrainingJobOnD2dEnd(inDjId IN INTEGER, inFileSize IN INTEGER,
                                                      inHasFailed IN BOOLEAN) AS
  varTotalFiles INTEGER;
  varNbFailedBytes INTEGER;
  varNbSuccessBytes INTEGER;
  varNbFailedFiles INTEGER;
  varNbSuccessFiles INTEGER;
  varStatus INTEGER;
BEGIN
  -- note the locking that ensures consistency of the counters
  SELECT status, totalFiles, nbFailedBytes, nbSuccessBytes, nbFailedFiles, nbSuccessFiles
    INTO varStatus, varTotalFiles, varNbFailedBytes, varNbSuccessBytes, varNbFailedFiles, varNbSuccessFiles
    FROM DrainingJob
   WHERE id = inDjId
     FOR UPDATE;
  -- update counters
  IF inHasFailed THEN
    -- case of failures
    varNbFailedBytes := varNbFailedBytes + inFileSize;
    varNbFailedFiles := varNbFailedFiles + 1;
  ELSE
    -- case of success
    varNbSuccessBytes := varNbSuccessBytes + inFileSize;
    varNbSuccessFiles := varNbSuccessFiles + 1;
  END IF;
  -- detect end of draining. Do not touch INTERRUPTED status
  IF varStatus = dconst.DRAININGJOB_RUNNING AND
     varNbSuccessFiles + varNbFailedFiles = varTotalFiles THEN
    IF varNbFailedFiles = 0 THEN
      varStatus := dconst.DRAININGJOB_FINISHED;
    ELSE
      varStatus := dconst.DRAININGJOB_FAILED;
    END IF;
  END IF;
  -- update DrainingJob
  UPDATE DrainingJob
     SET status = varStatus,
         nbFailedBytes = varNbFailedBytes,
         nbSuccessBytes = varNbSuccessBytes,
         nbFailedFiles = varNbFailedFiles,
         nbSuccessFiles = varNbSuccessFiles
   WHERE id = inDjId;
END;
/

/* PL/SQL method implementing disk2DiskCopyEnded
 * Note that inDestDsName, inDestPath, inReplicaFileSize and inCksumValue are not used when inErrorMessage is not NULL
 * inErrorCode is used in case of error to decide whether to retry and also to invalidate
 * the source diskCopy if the error is an ENOENT
 */
CREATE OR REPLACE PROCEDURE disk2DiskCopyEnded
(inTransferId IN VARCHAR2, inDestDsName IN VARCHAR2, inDestPath IN VARCHAR2,
 inReplicaFileSize IN INTEGER, inCksumValue IN VARCHAR2, inErrorCode IN INTEGER, inErrorMessage IN VARCHAR2) AS
  varCfId INTEGER;
  varUid INTEGER := -1;
  varGid INTEGER := -1;
  varDestDcId INTEGER;
  varSrcDcId INTEGER;
  varDropSource INTEGER;
  varDestSvcClass INTEGER;
  varRepType INTEGER;
  varRetryCounter INTEGER;
  varFileId INTEGER;
  varNsHost VARCHAR2(2048);
  varFCksum VARCHAR2(10);
  varFileSize INTEGER;
  varDestPath VARCHAR2(2048);
  varDestFsId INTEGER;
  varDestDpId INTEGER;
  varDcGcWeight NUMBER := 0;
  varDcImportance NUMBER := 0;
  varNewDcStatus INTEGER := dconst.DISKCOPY_VALID;
  varLogMsg VARCHAR2(2048) := dlf.D2D_D2DDONE_OK;
  varComment VARCHAR2(2048);
  varDrainingJob VARCHAR2(2048);
  varErrorMessage VARCHAR2(2048) := inErrorMessage;
BEGIN
  BEGIN
    IF inDestPath IS NOT NULL THEN
      -- Parse destination path
      parsePath(inDestDsName ||':'|| inDestPath, varDestFsId, varDestDpId, varDestPath, varDestDcId, varFileId, varNsHost);
    -- ELSE we are called because of an error at start: try to gather information
    -- from the Disk2DiskCopyJob entry and fail accordingly.
    END IF;
    -- Get data from the Disk2DiskCopyJob
    SELECT castorFile, ouid, ogid, destDcId, srcDcId, destSvcClass, replicationType,
           dropSource, retryCounter, drainingJob
      INTO varCfId, varUid, varGid, varDestDcId, varSrcDcId, varDestSvcClass, varRepType,
           varDropSource, varRetryCounter, varDrainingJob
      FROM Disk2DiskCopyJob
     WHERE transferId = inTransferId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- The job was probably canceled: so our brand new copy
    -- has to be created as invalid to trigger GC, and linked
    -- to the (hopefully existing) correct CastorFile.
    varNewDcStatus := dconst.DISKCOPY_INVALID;
    varLogMsg := dlf.D2D_D2DDONE_CANCEL;
    BEGIN
      SELECT id INTO varCfId
        FROM CastorFile
       WHERE fileId = varFileId;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Here we also lost the CastorFile: this could happen
      -- if the GC ran meanwhile. Fail and leave dark data behind,
      -- the GC will eventually catch up. A full solution would be
      -- to gather here all missing information to correctly
      -- recreate the CastorFile entry, but this is too complex
      -- for what we would gain.
      logToDLF(NULL, dlf.LVL_NOTICE, dlf.D2D_D2DDONE_CANCEL, varFileId, varNsHost, 'transfermanagerd',
               'transferId=' || inTransferId || ' errorMessage="CastorFile disappeared, giving up"');
      RETURN;
    END;
  END;
  -- on success, check the filesize and the checksum
  IF varErrorMessage IS NULL THEN
    BEGIN
      SELECT csumValue INTO varFCksum
        FROM Cns_file_metadata@remoteNS
       WHERE fileId = varFileId;
      IF inReplicaFileSize != varFileSize OR to_number(inCksumValue, 'XXXXXXXX') != to_number(varFCksum, 'XXXXXXXX') THEN
        -- replication went wrong !
        varNewDcStatus := dconst.DISKCOPY_INVALID;
        varErrorMessage := 'File size/checksum mismatch during replication, the source file is probably corrupted';
      END IF;
    EXCEPTION WHEN INVALID_NUMBER THEN
      -- the checksum is not a number?!
      varNewDcStatus := dconst.DISKCOPY_INVALID;
      varErrorMessage := 'Invalid checksum value "' || inCksumValue || '", giving up';
    END;
  END IF;
  -- lock the castor file (and get logging info)
  SELECT fileid, nsHost, fileSize INTO varFileId, varNsHost, varFileSize
    FROM CastorFile
   WHERE id = varCfId
     FOR UPDATE;
  -- Log success or failure of the replication
  IF varLogMsg = dlf.D2D_D2DDONE_OK AND varErrorMessage IS NOT NULL THEN
    varLogMsg := dlf.D2D_D2DFAILED;
  END IF;
  varComment := 'transferId="' || inTransferId ||
         '" destSvcClass=' || getSvcClassName(varDestSvcClass) ||
         ' destDcId=' || TO_CHAR(varDestDcId) || ' destPath="' || inDestPath ||
         '" euid=' || TO_CHAR(varUid) || ' egid=' || TO_CHAR(varGid) ||
         ' fileSize=' || TO_CHAR(varFileSize) || ' checksum=' || inCksumValue;
  IF varErrorMessage IS NOT NULL THEN
    varComment := varComment || ' replicaFileSize=' || TO_CHAR(inReplicaFileSize) ||
                  ' errorCode=' || inErrorCode || ' errorMessage="' || varErrorMessage || '"';
    varNewDcStatus := dconst.DISKCOPY_INVALID;
  END IF;
  logToDLF(NULL, dlf.LVL_SYSTEM, varLogMsg, varFileId, varNsHost, 'transfermanagerd', varComment);
  IF varErrorMessage IS NULL THEN
    -- compute GcWeight and importance of the new copy
    DECLARE
      varGcwProc VARCHAR2(2048);
    BEGIN
      varGcwProc := castorGC.getCopyWeight(varDestSvcClass);
      EXECUTE IMMEDIATE
        'BEGIN :newGcw := ' || varGcwProc || '(:size); END;'
        USING OUT varDcGcWeight, IN varFileSize;
      SELECT /*+ INDEX_RS_ASC (DiskCopy I_DiskCopy_CastorFile) */
             COUNT(*)+1 INTO varDCImportance FROM DiskCopy
       WHERE castorFile=varCfId AND status = dconst.DISKCOPY_VALID;
    END;
  END IF;
  -- create the new DiskCopy in all cases
  -- we may try twice in case we get a constraint violated and the violation disappears (see details below)
  FOR attempts IN 1..2 LOOP
    DECLARE
      CONSTRAINT_VIOLATED EXCEPTION;
      PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
    BEGIN
      INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                            owneruid, ownergid, id, gcType, fileSystem, datapool, castorFile,
                            status, importance)
      VALUES (varDestPath, varDcGcWeight, getTime(), getTime(), varFileSize, 0,
              varUid, varGid, varDestDcId,
              CASE varNewDcStatus WHEN dconst.DISKCOPY_INVALID
                                  THEN dconst.GCTYPE_FAILEDD2D
                                  ELSE NULL END,
              varDestFsId, varDestDpId, varCfId, varNewDcStatus, varDCImportance);
      EXIT;
    EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
      -- we do not manage to create the DiskCopy as another exists with the same id
      -- this can be due to a kill transfer that came during the transfer and we are
      -- now processing the end of transfer (a failure) while we have already done
      -- the job during the kill transfer. We will however double check by looking
      -- at the status and path of the existing DiskCopy
      DECLARE
        varStatus NUMBER;
        varPath VARCHAR2(2048);
      BEGIN
        SELECT path, status INTO varPath, varStatus FROM DiskCopy WHERE id = varDestDcId;
        IF varPath != varDestPath OR varStatus != varNewDcStatus THEN
          -- not the expected case, reraise the exception
          RAISE;
        END IF;
        -- Expected case, we are happy, exit the loop
        EXIT;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- the colliding DiskCopy has disappeared ! Let's go back to our insert, in case
        -- it was not the case we have described. We do not have anything to do, just let
        -- the second attempt go through
        -- Note that there will be no third attempt as nothing can recreate the DiskCopy
        -- with that id anymore
        NULL:
      END;
    END;
  END LOOP;
  -- if success, restart waiting requests, cleanup and handle replicate on close
  IF varErrorMessage IS NULL THEN
    -- In case of draining, update DrainingJob: this is done before the rest to respect the locking order
    IF varDrainingJob IS NOT NULL THEN
      updateDrainingJobOnD2dEnd(varDrainingJob, varFileSize, False);
    END IF;
    -- Wake up waiting subrequests
    UPDATE SubRequest
       SET status = dconst.SUBREQUEST_RESTART,
           getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED,
           lastModificationTime = getTime()
     WHERE status = dconst.SUBREQUEST_WAITSUBREQ
       AND castorfile = varCfId;
    alertSignalNoLock('wakeUpJobReqSvc');
    -- delete the disk2diskCopyJob
    DELETE FROM Disk2DiskCopyJob WHERE transferId = inTransferId;
    -- In case of valid new copy
    IF varDropSource = 1 THEN
      -- drop source if requested
      UPDATE DiskCopy
         SET status = dconst.DISKCOPY_INVALID, gcType=dconst.GCTYPE_DRAINING
       WHERE id = varSrcDcId;
    ELSE
      -- update importance of other DiskCopies if it's an additional one
      UPDATE DiskCopy SET importance = varDCImportance WHERE castorFile = varCfId;
    END IF;
    -- trigger the creation of additional copies of the file, if any
    replicateOnClose(varCfId, varUid, varGid, varDestSvcClass);
  ELSE
    -- failure
    DECLARE
      varMaxNbD2dRetries INTEGER := to_number(getConfigOption('D2dCopy', 'MaxNbRetries', 2));
      varNewDestDcId INTEGER := ids_seq.nextval();
    BEGIN
      -- shall we try again ?
      -- we should not when the job was deliberately killed, neither when we reach the maximum
      -- number of attempts
      IF varRetryCounter + 1 < varMaxNbD2dRetries AND inErrorCode != serrno.ESTKILLED THEN
        -- yes, so let's restart the Disk2DiskCopyJob
        UPDATE Disk2DiskCopyJob
           SET status = dconst.DISK2DISKCOPYJOB_PENDING,
               destDcId = varNewDestDcId,
               retryCounter = varRetryCounter + 1
         WHERE transferId = inTransferId;
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_D2DDONE_RETRIED, varFileId, varNsHost, 'transfermanagerd', varComment ||
                 ' RetryNb=' || TO_CHAR(varRetryCounter+1) || ' maxNbRetries=' || TO_CHAR(varMaxNbD2dRetries));
      ELSE
        -- No retry. In case of draining, update DrainingJob
        IF varDrainingJob IS NOT NULL THEN
          updateDrainingJobOnD2dEnd(varDrainingJob, varFileSize, True);
        END IF;
        -- and delete the disk to disk copy job
        BEGIN
          DELETE FROM Disk2DiskCopyJob WHERE transferId = inTransferId;
          -- and remember the error in case of draining
          IF varDrainingJob IS NOT NULL THEN
            INSERT INTO DrainingErrors (drainingJob, errorMsg, fileId, nsHost, castorFile, timeStamp)
            VALUES (varDrainingJob, varErrorMessage, varFileId, varNsHost, varCfId, getTime());
          END IF;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          -- the Disk2DiskCopyJob was already dropped (e.g. because of an interrupted draining)
          -- in such a case, forget about the error
          NULL;
        END;
        logToDLF(NULL, dlf.LVL_NOTICE, dlf.D2D_D2DDONE_NORETRY, varFileId, varNsHost, 'transfermanagerd', varComment ||
                 ' maxNbRetries=' || TO_CHAR(varMaxNbD2dRetries));
        -- Fail waiting subrequests
        UPDATE SubRequest
           SET status = dconst.SUBREQUEST_FAILED,
               lastModificationTime = getTime(),
               errorCode = serrno.SEINTERNAL,
               errorMessage = 'Disk to disk copy failed after ' || TO_CHAR(varMaxNbD2dRetries) ||
                              ' retries. Last error was : ' || varErrorMessage
         WHERE status = dconst.SUBREQUEST_WAITSUBREQ
           AND castorfile = varCfId;
      END IF;
    END;
  END IF;
END;
/

/* PL/SQL method implementing disk2DiskCopyStart
 * Note that cfId is only needed for proper logging in case the replication has been canceled.
 */
CREATE OR REPLACE PROCEDURE disk2DiskCopyStart
 (inTransferId IN VARCHAR2, inFileId IN INTEGER, inNsHost IN VARCHAR2,
  inDestDiskServerName IN VARCHAR2, inDestMountPoint IN VARCHAR2,
  inSrcDiskServerName IN VARCHAR2, inSrcMountPoint IN VARCHAR2,
  outDestDcPath OUT VARCHAR2, outSrcDcPath OUT VARCHAR2) AS
  varCfId INTEGER;
  varDestDcId INTEGER;
  varDestDsId INTEGER;
  varSrcDcId INTEGER;
  varSrcFsId INTEGER := NULL;
  varSrcDpId INTEGER := NULL;
  varNbCopies INTEGER;
  varSrcFsStatus INTEGER := dconst.FILESYSTEM_PRODUCTION;
  varSrcDsStatus INTEGER;
  varSrcHwOnline INTEGER;
  varDestFsStatus INTEGER := dconst.FILESYSTEM_PRODUCTION;
  varDestDsStatus INTEGER;
  varDestHwOnline INTEGER;
BEGIN
  -- check the Disk2DiskCopyJob status and check that it was not canceled in the mean time
  BEGIN
    SELECT castorFile, destDcId INTO varCfId, varDestDcId
      FROM Disk2DiskCopyJob
     WHERE transferId = inTransferId
       AND status = dconst.DISK2DISKCOPYJOB_SCHEDULED
       FOR UPDATE;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "disk2DiskCopyStart : Replication request canceled while queuing in scheduler or transfer already started"
    logToDLF(NULL, dlf.LVL_USER_ERROR, dlf.D2D_CANCELED_AT_START, inFileId, inNsHost, 'transfermanagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' destDiskServer=' || inDestDiskServerName ||
             ' destMountPoint=' || inDestMountPoint || ' srcDiskServer=' || inSrcDiskServerName ||
             ' srcMountPoint=' || inSrcMountPoint);
    -- raise exception
    raise_application_error(-20110, dlf.D2D_CANCELED_AT_START || '');
  END;

  -- identify the source DiskCopy and diskserver/filesystem/datapool and check that it is still valid
  BEGIN
    IF inSrcMountPoint IS NULL THEN
      SELECT DiskServer.dataPool, DiskCopy.id, DiskServer.status, DiskServer.hwOnline
        INTO varSrcDpId, varSrcDcId, varSrcDsStatus, varSrcHwOnline
        FROM DiskServer, DiskCopy
       WHERE name = inSrcDiskServerName
         AND DiskServer.dataPool = DiskCopy.dataPool
         AND DiskCopy.castorFile = varCfId
         AND ROWNUM < 2;
    ELSE
      SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */
             FileSystem.id, DiskCopy.id, FileSystem.status, DiskServer.status, DiskServer.hwOnline
        INTO varSrcFsId, varSrcDcId, varSrcFsStatus, varSrcDsStatus, varSrcHwOnline
        FROM DiskServer, FileSystem, DiskCopy
       WHERE DiskServer.name = inSrcDiskServerName
         AND DiskServer.id = FileSystem.diskServer
         AND FileSystem.mountPoint = inSrcMountPoint
         AND DiskCopy.FileSystem = FileSystem.id
         AND DiskCopy.status = dconst.DISKCOPY_VALID
         AND DiskCopy.castorFile = varCfId;
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- log "disk2DiskCopyStart : Source has disappeared while queuing in scheduler, retrying"
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_SOURCE_GONE, inFileId, inNsHost, 'transfermanagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' destDiskServer=' || inDestDiskServerName ||
             ' destMountPoint=' || inDestMountPoint || ' srcDiskServer=' || inSrcDiskServerName ||
             ' srcMountPoint=' || inSrcMountPoint);
    -- end the disktodisk copy (may be retried)
    disk2DiskCopyEnded(inTransferId, '', '', 0, '', 0, dlf.D2D_SOURCE_GONE);
    COMMIT; -- commit or raise_application_error will roll back for us :-(
    -- raise exception for the scheduling part
    raise_application_error(-20110, dlf.D2D_SOURCE_GONE);
  END;

  -- update the Disk2DiskCopyJob status and filesystem
  UPDATE Disk2DiskCopyJob
     SET status = dconst.DISK2DISKCOPYJOB_RUNNING
   WHERE transferId = inTransferId;

  IF (varSrcDsStatus = dconst.DISKSERVER_DISABLED OR varSrcFsStatus = dconst.FILESYSTEM_DISABLED
      OR varSrcHwOnline = 0) THEN
    -- log "disk2DiskCopyStart : Source diskserver/filesystem was DISABLED meanwhile"
    logToDLF(NULL, dlf.LVL_WARNING, dlf.D2D_SRC_DISABLED, inFileId, inNsHost, 'transfermanagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' diskServer=' || inSrcDiskServerName ||
             ' fileSystem=' || inSrcMountPoint);
    -- fail d2d transfer
    disk2DiskCopyEnded(inTransferId, '', '', 0, '', 0, 'Source was disabled');
    COMMIT; -- commit or raise_application_error will roll back for us :-(
    -- raise exception
    raise_application_error(-20110, dlf.D2D_SRC_DISABLED);
  END IF;

  -- get destination diskServer/filesystem and check its status
  IF inDestMountPoint IS NULL THEN
    SELECT DiskServer.id, DiskServer.status, DiskServer.hwOnline
      INTO varDestDsId, varDestDsStatus, varDestHwOnline
      FROM DiskServer
     WHERE name = inDestDiskServerName;
  ELSE
    SELECT DiskServer.id, DiskServer.status, FileSystem.status, DiskServer.hwOnline
      INTO varDestDsId, varDestDsStatus, varDestFsStatus, varDestHwOnline
      FROM DiskServer, FileSystem
     WHERE DiskServer.name = inDestDiskServerName
       AND FileSystem.mountPoint = inDestMountPoint
       AND FileSystem.diskServer = DiskServer.id;
  END IF;
  IF (varDestDsStatus != dconst.DISKSERVER_PRODUCTION OR varDestFsStatus != dconst.FILESYSTEM_PRODUCTION
      OR varDestHwOnline = 0) THEN
    -- log "disk2DiskCopyStart : Destination diskserver/filesystem not in PRODUCTION any longer"
    logToDLF(NULL, dlf.LVL_WARNING, dlf.D2D_DEST_NOT_PRODUCTION, inFileId, inNsHost, 'transfermanagerd',
             'TransferId=' || TO_CHAR(inTransferId) || ' diskServer=' || inDestDiskServerName);
    -- fail d2d transfer
    disk2DiskCopyEnded(inTransferId, '', '', 0, '', 0, 'Destination not in production');
    COMMIT; -- commit or raise_application_error will roll back for us :-(
    -- raise exception
    raise_application_error(-20110, dlf.D2D_DEST_NOT_PRODUCTION);
  END IF;

  IF inDestMountPoint IS NULL THEN
    -- Prevent multiple copies of the file to be created on the same diskserver when
    -- running in standard mode (with filesystems, no datapools)
    SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_Castorfile) */ count(*) INTO varNbCopies
      FROM DiskCopy, FileSystem
     WHERE DiskCopy.filesystem = FileSystem.id
       AND FileSystem.diskserver = varDestDsId
       AND DiskCopy.castorfile = varCfId
       AND DiskCopy.status = dconst.DISKCOPY_VALID;
    IF varNbCopies > 0 THEN
      -- log "disk2DiskCopyStart : Multiple copies of this file already found on this diskserver"
      logToDLF(NULL, dlf.LVL_ERROR, dlf.D2D_MULTIPLE_COPIES_ON_DS, inFileId, inNsHost, 'transfermanagerd',
               'TransferId=' || TO_CHAR(inTransferId) || ' diskServer=' || inDestDiskServerName);
      -- fail d2d transfer
      disk2DiskCopyEnded(inTransferId, '', '', 0, '', 0, 'Copy found on diskserver');
      COMMIT; -- commit or raise_application_error will roll back for us :-(
      -- raise exception
      raise_application_error(-20110, dlf.D2D_MULTIPLE_COPIES_ON_DS);
    END IF;
  END IF;

  -- build full path of destination copy
  buildPathFromFileId(inFileId, inNsHost, varDestDcId, outDestDcPath, inDestMountPoint IS NOT NULL);
  outDestDcPath := inDestMountPoint || outDestDcPath;

  -- build full path of source copy
  buildPathFromFileId(inFileId, inNsHost, varSrcDcId, outSrcDcPath, inSrcMountPoint IS NOT NULL);
  outSrcDcPath := inSrcDiskServerName || ':' || inSrcMountPoint || outSrcDcPath;

  -- log "disk2DiskCopyStart completed successfully"
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.D2D_START_OK, inFileId, inNsHost, 'transfermanagerd',
           'TransferId=' || TO_CHAR(inTransferId) || ' srcPath=' || outSrcDcPath ||
           ' destPath=' || inDestDiskServerName || ':' || outDestDcPath);
END;
/

/* PL/SQL method implementing getSchedulerTransfers.
   This method lists all known transfers
   that are started/pending for more than 5mn */
CREATE OR REPLACE PROCEDURE getSchedulerTransfers
  (transfers OUT castor.UUIDPairRecord_Cur) AS
BEGIN
  OPEN transfers FOR
    SELECT SR.subReqId, Request.reqid
      FROM SubRequest SR,
        -- Union of all requests that could result in scheduler transfers
        (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                id, svcClass, reqid, 40  AS reqType FROM StagePutRequest             UNION ALL
         SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                id, svcClass, reqid, 35  AS reqType FROM StageGetRequest) Request
     WHERE SR.status = 6  -- READY
       AND SR.request = Request.id
       AND SR.lastModificationTime < getTime() - 300
     UNION ALL
       SELECT transferId, '' FROM Disk2DiskCopyJob
        WHERE status IN (dconst.DISK2DISKCOPYJOB_SCHEDULED, dconst.DISK2DISKCOPYJOB_RUNNING)
          AND creationTime < getTime() - 300;
END;
/

/* PL/SQL method implementing getSchedulerD2dTransfers.
   This method lists all running D2d transfers */
CREATE OR REPLACE PROCEDURE getSchedulerD2dTransfers
  (transfers OUT castor.UUIDRecord_Cur) AS
BEGIN
  OPEN transfers FOR
    SELECT transferId
      FROM Disk2DiskCopyJob
     WHERE status IN (dconst.DISK2DISKCOPYJOB_SCHEDULED, dconst.DISK2DISKCOPYJOB_RUNNING);
END;
/

/* PL/SQL method implementing getFileIdsForSrs.
   This method returns the list of fileids associated to the given list of
   subrequests */
CREATE OR REPLACE PROCEDURE getFileIdsForSrs
  (subReqIds IN castor."strList", fileids OUT castor.FileEntry_Cur) AS
  fid NUMBER;
  nh VARCHAR(2048);
BEGIN
  FOR i IN 1..subReqIds.COUNT LOOP
    BEGIN
      SELECT /*+ INDEX_RS_ASC(SubRequest I_Subrequest_SubreqId)*/ fileid, nsHost INTO fid, nh
        FROM Castorfile, SubRequest
       WHERE SubRequest.subreqId = subReqIds(i)
         AND SubRequest.castorFile = CastorFile.id;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- must be disk to disk copies
      BEGIN
        SELECT fileid, nsHost INTO fid, nh
          FROM Castorfile, Disk2DiskCopyJob
         WHERE Disk2DiskCopyJob.transferid = subReqIds(i)
           AND Disk2DiskCopyJob.castorFile = CastorFile.id;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- not even a disk to disk copy: it must have been dropped meanwhile by a
        -- transfermanagerd in another head node. Just insert an empty row, it will
        -- be handled by the synchronizer thread of transfermanagerd.
        fid := 0;
        nh := '';
      END;
    END;
    INSERT INTO GetFileIdsForSrsHelper (rowno, fileId, nsHost) VALUES (i, fid, nh);
  END LOOP;
  OPEN fileids FOR SELECT nh, fileid FROM GetFileIdsForSrsHelper ORDER BY rowno;
END;
/

/* PL/SQL method implementing transferFailedSafe, providing bulk termination of file
 * transfers.
 */
CREATE OR REPLACE
PROCEDURE transferFailedSafe(subReqIds IN castor."strList",
                             errnos IN castor."cnumList",
                             errmsgs IN castor."strList") AS
  srId  NUMBER;
  dcId  NUMBER;
  cfId  NUMBER;
  rType NUMBER;
  errNo INTEGER;
  errMsg VARCHAR2(2048);
BEGIN
  -- give up if nothing to be done
  IF subReqIds.COUNT = 0 THEN RETURN; END IF;
  -- Loop over all transfers to fail
  FOR i IN 1..subReqIds.COUNT LOOP
    BEGIN
      -- Get the necessary information needed about the request.
      SELECT /*+ INDEX_RS_ASC(SubRequest I_Subrequest_SubreqId)*/ id, diskCopy, reqType, castorFile
        INTO srId, dcId, rType, cfId
        FROM SubRequest
       WHERE subReqId = subReqIds(i)
         AND status = dconst.SUBREQUEST_READY;
      -- Lock the CastorFile.
      SELECT id INTO cfId FROM CastorFile
       WHERE id = cfId FOR UPDATE;
      -- Confirm SubRequest status hasn't changed after acquisition of lock
      SELECT /*+ INDEX(SubRequest PK_Subrequest_Id)*/ id INTO srId FROM SubRequest
       WHERE id = srId AND status = dconst.SUBREQUEST_READY;
      -- Call the relevant cleanup procedure for the transfer, procedures that
      -- would have been called if the transfer failed on the remote execution host.
      errNo := errnos(i);
      errMsg := errmsgs(i);
      IF rType = 40 THEN      -- StagePutRequest
        putEnded(subReqIds(i), 0, 0, '', '', errNo, errMsg);
      ELSE                    -- StageGetRequest
        getEnded(subReqIds(i), errNo, errMsg);
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- try disk2diskCopyJob
        SELECT id into srId FROM Disk2diskCopyJob WHERE transferId = subReqIds(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        CONTINUE;  -- The SubRequest/disk2DiskCopyJob may have been removed, nothing to be done.
      END;
      disk2DiskCopyEnded(subReqIds(i), '', '', 0, '', errnos(i), errmsgs(i));
    END;
    -- Release locks
    COMMIT;
  END LOOP;
END;
/

/* PL/SQL method implementing transferFailedLockedFile, providing bulk termination of file
 * transfers. in case the castorfile is already locked
 */
CREATE OR REPLACE
PROCEDURE transferFailedLockedFile(subReqIds IN castor."strList",
                                   errnos IN castor."cnumList",
                                   errmsgs IN castor."strList")
AS
  srId  NUMBER;
  dcId  NUMBER;
  rType NUMBER;
  errNo INTEGER;
  errMsg VARCHAR2(2048);
BEGIN
  FOR i IN 1..subReqIds.COUNT LOOP
    BEGIN
      -- Get the necessary information needed about the request.
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_SubreqId)*/ id, diskCopy, reqType
        INTO srId, dcId, rType
        FROM SubRequest
       WHERE subReqId = subReqIds(i)
         AND status = dconst.SUBREQUEST_READY;
      -- Call the relevant cleanup procedure for the transfer, procedures that
      -- would have been called if the transfer failed on the remote execution host.
      errNo := errnos(i);
      errMsg := errmsgs(i);
      IF rType = 40 THEN      -- StagePutRequest
        putEnded(subReqIds(i), 0, 0, '', '', errNo, errMsg);
      ELSE                    -- StageGetRequest
        getEnded(subReqIds(i), errNo, errMsg);
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- try disk2diskCopyJob
        SELECT id into srId FROM Disk2diskCopyJob WHERE transferId = subReqIds(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        CONTINUE;  -- The SubRequest/disk2DiskCopyJob may have be removed, nothing to be done.
      END;
      -- found it, call disk2DiskCopyEnded
      disk2DiskCopyEnded(subReqIds(i), '', '', 0, '', errnos(i), errmsgs(i));
    END;
  END LOOP;
END;
/

CREATE OR REPLACE TRIGGER tr_SubRequest_informSchedReady AFTER UPDATE OF status ON SubRequest
FOR EACH ROW WHEN (new.status = 13) -- SUBREQUEST_READYFORSCHED
BEGIN
  alertSignalNoLock('transferReadyToSchedule');
END;
/

CREATE OR REPLACE FUNCTION selectRandomDestinationFs(inSvcClassId IN INTEGER,
                                                     inMinFreeSpace IN INTEGER,
                                                     inCfId IN INTEGER)
RETURN VARCHAR2 AS
  varResult VARCHAR2(2048) := '';
BEGIN
  -- note that we discard READONLY hardware and filter only the PRODUCTION one.
  FOR line IN
    (SELECT candidate FROM
       (SELECT DBMS_Random.value, candidate FROM
         (SELECT UNIQUE FIRST_VALUE (DiskServer.name || ':' || FileSystem.mountPoint)
                   OVER (PARTITION BY DiskServer.id ORDER BY DBMS_Random.value) AS candidate
            FROM DiskServer, FileSystem, DiskPool2SvcClass
           WHERE FileSystem.diskServer = DiskServer.id
             AND FileSystem.diskPool = DiskPool2SvcClass.parent
             AND DiskPool2SvcClass.child = inSvcClassId
             AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
             AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
             AND DiskServer.hwOnline = 1
             AND FileSystem.free - FileSystem.minAllowedFreeSpace * FileSystem.totalSize > inMinFreeSpace
             AND DiskServer.id NOT IN
                 (SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_Castorfile) */ diskserver FROM DiskCopy, FileSystem
                   WHERE DiskCopy.castorFile = inCfId
                     AND DiskCopy.status = dconst.DISKCOPY_VALID
                     AND FileSystem.id = DiskCopy.fileSystem)
           UNION
          SELECT DiskServer.name || ':' AS candidate
            FROM DiskServer, DataPool2SvcClass, DataPool
           WHERE DiskServer.dataPool = DataPool2SvcClass.parent
             AND DataPool2SvcClass.child = inSvcClassId
             AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
             AND DiskServer.hwOnline = 1
             AND DataPool.id = DataPool2SvcClass.parent
             AND DataPool.free - DataPool.minAllowedFreeSpace * DataPool.totalSize > inMinFreeSpace)
         ORDER BY 1)
      WHERE ROWNUM <= 3) LOOP
    IF LENGTH(varResult) IS NOT NULL THEN varResult := varResult || '|'; END IF;
    varResult := varResult || line.candidate;
  END LOOP;
  RETURN varResult;
END;
/

CREATE OR REPLACE FUNCTION selectAllSourceFs(inCfId IN INTEGER)
RETURN VARCHAR2 AS
  varResult VARCHAR2(2048) := '';
BEGIN
  -- in this case we take any non DISABLED hardware
  FOR line IN
    (SELECT candidate FROM
       (SELECT DBMS_Random.value, candidate FROM
         (SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_Castorfile) */
                 UNIQUE FIRST_VALUE (DiskServer.name || ':' || FileSystem.mountPoint)
                   OVER (PARTITION BY DiskServer.id ORDER BY DBMS_Random.value) AS candidate
            FROM DiskServer, FileSystem, DiskCopy
           WHERE DiskCopy.castorFile = inCfId
             AND DiskCopy.status = dconst.DISKCOPY_VALID
             AND DiskCopy.fileSystem = FileSystem.id
             AND FileSystem.diskServer = DiskServer.id
             AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
             AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_DRAINING, dconst.FILESYSTEM_READONLY)
             AND DiskServer.hwOnline = 1
           UNION
          SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_Castorfile) */
                 DiskServer.name || ':' AS candidate
            FROM DiskServer, DiskCopy
           WHERE DiskCopy.castorFile = inCfId
             AND DiskCopy.status = dconst.DISKCOPY_VALID
             AND DiskCopy.dataPool = DiskServer.dataPool
             AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_DRAINING, dconst.DISKSERVER_READONLY)
             AND DiskServer.hwOnline = 1)
         ORDER BY 1)
      WHERE ROWNUM <= 3) LOOP
    IF LENGTH(varResult) IS NOT NULL THEN varResult := varResult || '|'; END IF;
    varResult := varResult || line.candidate;
  END LOOP;
  RETURN varResult;
END;
/

/* PL/SQL method implementing userTransferToSchedule */
CREATE OR REPLACE
PROCEDURE userTransferToSchedule(srId OUT INTEGER,              srSubReqId OUT VARCHAR2,
                                 srProtocol OUT VARCHAR2,       srRfs OUT VARCHAR2,
                                 reqId OUT VARCHAR2,            cfFileId OUT INTEGER,
                                 cfNsHost OUT VARCHAR2,         reqSvcClass OUT VARCHAR2,
                                 reqType OUT INTEGER,           reqEuid OUT INTEGER,
                                 reqEgid OUT INTEGER,           srOpenFlags OUT VARCHAR2,
                                 clientIp OUT INTEGER,          clientPort OUT INTEGER,
                                 reqCreationTime OUT INTEGER) AS
  cfId NUMBER;
  -- Cursor to select the next candidate for submission to the scheduler ordered
  -- by creation time.
  -- Note that the where clause is not strictly needed, but this way Oracle is forced
  -- to use an INDEX RANGE SCAN instead of its preferred (and unstable upon load) FULL SCAN!
  CURSOR c IS
    SELECT /*+ FIRST_ROWS_10 INDEX(SR I_SubRequest_Svc_CT_ID) */ SR.id
      FROM SubRequest PARTITION (P_STATUS_SCHED) SR  -- READYFORSCHED
     WHERE svcHandler = 'JobReqSvc'
     ORDER BY SR.creationTime ASC;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  varSrId NUMBER;
  svcClassId NUMBER;
  unusedMessage VARCHAR2(2048);
  unusedStatus INTEGER;
  varXsize INTEGER;
BEGIN
  -- Open a cursor on potential candidates
  OPEN c;
  -- Retrieve the first candidate
  FETCH c INTO varSrId;
  IF c%NOTFOUND THEN
    -- There is no candidate available. Wait for next alert concerning something
    -- to schedule for a maximum of 3 seconds.
    -- We do not wait forever in order to to give the control back to the
    -- caller daemon in case it should exit.
    CLOSE c;
    waitSignalNoLock('transferReadyToSchedule');
    -- try again to find something now that we waited
    OPEN c;
    FETCH c INTO varSrId;
    IF c%NOTFOUND THEN
      -- still nothing. We will give back the control to the application
      -- so that it can handle cases like signals and exit. We will probably
      -- be back soon :-)
      RETURN;
    END IF;
  END IF;
  LOOP
    -- we reached this point because we have found at least one candidate
    -- let's loop on the candidates until we find one we can process
    BEGIN
      -- Try to lock the current candidate, verify that the status is valid. A
      -- valid subrequest is in status READYFORSCHED
      SELECT /*+ INDEX(SR PK_SubRequest_ID) */ id INTO varSrId
        FROM SubRequest PARTITION (P_STATUS_SCHED) SR
       WHERE id = varSrId
         AND status = dconst.SUBREQUEST_READYFORSCHED
         FOR UPDATE NOWAIT;
      -- We have successfully acquired the lock, so we update the subrequest
      -- status and modification time
      UPDATE /*+ INDEX(SubRequest PK_Subrequest_Id)*/ SubRequest
         SET status = dconst.SUBREQUEST_READY,
             lastModificationTime = getTime()
       WHERE id = varSrId
      RETURNING id, subReqId, protocol, xsize, requestedFileSystems
        INTO srId, srSubReqId, srProtocol, varXsize, srRfs;
      -- and we exit the loop on candidates
      EXIT;
    EXCEPTION
      -- Try again, either we failed to accquire the lock on the subrequest or
      -- the subrequest being processed is not the correct state
      WHEN NO_DATA_FOUND THEN
        NULL;
      WHEN SrLocked THEN
        NULL;
    END;
    -- we are here because the current candidate could not be handled
    -- let's go to the next one
    FETCH c INTO varSrId;
    IF c%NOTFOUND THEN
      -- no next one ? then we can return
      RETURN;
    END IF;
  END LOOP;
  CLOSE c;

  BEGIN
    -- We finally got a valid candidate, let's process it
    -- Extract the rest of the information required by transfer manager
    SELECT /*+ INDEX(SubRequest PK_Subrequest_Id)*/
           CastorFile.id, CastorFile.fileId, CastorFile.nsHost, SvcClass.name, SvcClass.id,
           Request.type, Request.reqId, Request.euid, Request.egid,
           Request.direction, Client.ipAddress, Client.port,
           Request.creationTime
      INTO cfId, cfFileId, cfNsHost, reqSvcClass, svcClassId, reqType, reqId, reqEuid, reqEgid,
           srOpenFlags, clientIp, clientPort, reqCreationTime
      FROM SubRequest, CastorFile, SvcClass, Client,
           (SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */
                   id, euid, egid, reqid, client, creationTime,
                   'w' direction, svcClass, 40 type
              FROM StagePutRequest
             UNION ALL
            SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                   id, euid, egid, reqid, client, creationTime,
                   'r' direction, svcClass, 35 type
              FROM StageGetRequest) Request
     WHERE SubRequest.id = srId
       AND SubRequest.castorFile = CastorFile.id
       AND Request.svcClass = SvcClass.id
       AND Request.id = SubRequest.request
       AND Request.client = Client.id;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Something went really wrong, our subrequest does not have the corresponding request or client.
    -- Just drop it and re-raise exception. Some rare occurrences have happened in the past,
    -- this catch-all logic protects the stager-scheduling system from getting stuck with a single such case.
    archiveSubReq(varSrId, dconst.SUBREQUEST_FAILED_FINISHED);
    COMMIT;
    raise_application_error(-20100, 'Request got corrupted and could not be processed');
  END;
  
  -- Select random filesystems to use if none is already requested. This only happens
  -- on Put requests, so we discard READONLY hardware and filter only the PRODUCTION one.
  IF LENGTH(srRfs) IS NULL THEN
    srRFs := selectRandomDestinationFs(svcClassId, varXsize, cfId);
  END IF;
END;
/

/* PL/SQL method implementing D2dTransferToSchedule */
CREATE OR REPLACE
PROCEDURE D2dTransferToSchedule(outTransferId OUT VARCHAR2, outReqId OUT VARCHAR2,
                                outFileId OUT INTEGER, outNsHost OUT VARCHAR2,
                                outEuid OUT INTEGER, outEgid OUT INTEGER,
                                outSvcClassName OUT VARCHAR2, outCreationTime OUT INTEGER,
                                outreplicationType OUT INTEGER,
                                outDestFileSystems OUT VARCHAR2, outSourceFileSystems OUT VARCHAR2) AS
  cfId NUMBER;
  -- Cursor to select the next candidate for submission to the scheduler orderd
  -- by creation time.
  -- Note that the where clause is not strictly needed, but this way Oracle is forced
  -- to use an INDEX RANGE SCAN instead of its preferred (and unstable upon load) FULL SCAN!
  CURSOR c IS
    SELECT /*+ INDEX_RS_ASC(Disk2DiskCopyJob I_Disk2DiskCopyJob_status_CT) */ Disk2DiskCopyJob.id
      FROM Disk2DiskCopyJob
     WHERE status = dconst.DISK2DISKCOPYJOB_PENDING
     ORDER BY creationTime ASC;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  varD2dJId INTEGER;
  varFileSize INTEGER;
  varCfId INTEGER;
  varUnusedMessage VARCHAR2(2048);
  varUnusedStatus INTEGER;
  varSvcClassId INTEGER;
BEGIN
  -- Open a cursor on potential candidates
  OPEN c;
  -- Retrieve the first candidate
  FETCH c INTO varD2dJId;
  IF c%NOTFOUND THEN
    -- There is no candidate available. Wait for next alert concerning something
    -- to schedule for a maximum of 3 seconds.
    -- We do not wait forever in order to to give the control back to the
    -- caller daemon in case it should exit.
    CLOSE c;
    waitSignalNoLock('d2dReadyToSchedule');
    -- try again to find something now that we waited
    OPEN c;
    FETCH c INTO varD2dJId;
    IF c%NOTFOUND THEN
      -- still nothing. We will give back the control to the application
      -- so that it can handle cases like signals and exit. We will probably
      -- be back soon :-)
      RETURN;
    END IF;
  END IF;
  LOOP
    -- we reached this point because we have found at least one candidate
    -- let's loop on the candidates until we find one we can process
    BEGIN
      -- Try to lock the current candidate, verify that the status is valid. A
      -- valid subrequest is in status READYFORSCHED
      SELECT /*+ INDEX(Disk2DiskCopyJob PK_Disk2DiskCopyJob_ID) */ id INTO varD2dJId
        FROM Disk2DiskCopyJob
       WHERE id = varD2dJId
         AND status = dconst.DISK2DISKCOPYJOB_PENDING
         FOR UPDATE NOWAIT;
      -- We have successfully acquired the lock, so we update the Disk2DiskCopyJob
      UPDATE /*+ INDEX(Disk2DiskCopyJob PK_Disk2DiskCopyJob_Id)*/ Disk2DiskCopyJob
         SET status = dconst.DISK2DISKCOPYJOB_SCHEDULED
       WHERE id = varD2dJId
      RETURNING transferId, castorFile, ouid, ogid, creationTime, destSvcClass,
                getSvcClassName(destSvcClass), replicationType
        INTO outTransferId, varCfId, outEuid, outEgid, outCreationTime, varSvcClassId,
             outSvcClassName, outReplicationType;
      -- Extract the rest of the information required by transfer manager
      SELECT fileId, nsHost, fileSize INTO outFileId, outNsHost, varFileSize
        FROM CastorFile
       WHERE CastorFile.id = varCfId;
      -- and we exit the loop on candidates
      EXIT;
    EXCEPTION
      -- Try again, either we failed to accquire the lock on the subrequest or
      -- the subrequest being processed is not the correct state
      WHEN NO_DATA_FOUND THEN
        NULL;
      WHEN SrLocked THEN
        NULL;
    END;
    -- we are here because the current candidate could not be handled
    -- let's go to the next one
    FETCH c INTO varD2dJId;
    IF c%NOTFOUND THEN
      -- no next one ? then we can return
      RETURN;
    END IF;
  END LOOP;
  CLOSE c;
  -- We finally got a valid candidate, let's select potential sources
  outSourceFileSystems := selectAllSourceFs(varCfId);
  -- Select random filesystems to use as destination.
  outDestFileSystems := selectRandomDestinationFs(varSvcClassId, varFileSize, varCfId);
END;
/

/* PL/SQL method implementing transfersToAbort */
CREATE OR REPLACE
PROCEDURE transfersToAbortProc(srUuidCur OUT castor.UUIDRecord_Cur) AS
  srUuid VARCHAR2(2048);
  srUuids strListTable;
  unusedMessage VARCHAR2(2048);
  unusedStatus INTEGER;
BEGIN
  BEGIN
    -- find out whether there is something
    SELECT uuid INTO srUuid FROM TransfersToAbort WHERE ROWNUM < 2;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- There is nothing to abort. Wait for next alert concerning something
    -- to abort or at least 3 seconds.
    waitSignalNoLock('transfersToAbort');
  END;
  -- we want to delete what we will return but deleting multiple rows is a nice way
  -- to have deadlocks. So we first take locks in NOWAIT mode
  SELECT uuid BULK COLLECT INTO srUuids FROM transfersToAbort FOR UPDATE SKIP LOCKED;
  DELETE FROM transfersToAbort WHERE uuid IN (SELECT * FROM TABLE(srUuids));
  -- Either we found something or we timed out, in both cases
  -- we go back to python so that it can handle cases like signals and exit
  -- We will probably be back soon :-)
  OPEN srUuidCur FOR 
    SELECT * FROM TABLE(srUuids);
END;
/

/* PL/SQL method implementing syncRunningTransfers
 * This is called by the transfer manager daemon on the restart of a disk server manager
 * in order to sync running transfers in the database with the reality of the machine.
 * This is particularly useful to terminate cleanly transfers interupted by a power cut
 */
CREATE OR REPLACE PROCEDURE syncRunningTransfers(machine IN VARCHAR2,
                                                 transfers IN castor."strList",
                                                 killedTransfersCur OUT castor.TransferRecord_Cur) AS
  unused VARCHAR2(2048);
  varFileid NUMBER;
  varNsHost VARCHAR2(2048);
  varReqId VARCHAR2(2048);
  killedTransfers castor."strList";
  errnos castor."cnumList";
  errmsg castor."strList";
BEGIN
  -- cleanup from previous round
  DELETE FROM SyncRunningTransfersHelper2;
  -- insert the list of running transfers into a temporary table for easy access
  FORALL i IN 1..transfers.COUNT
    INSERT INTO SyncRunningTransfersHelper (subreqId) VALUES (transfers(i));
  -- Go through all running transfers from the DB point of view for the given diskserver
  FOR SR IN (SELECT /*+ LEADING(SubRequest DiskCopy FileSystem DiskServer)
                        USE_NL(SubRequest DiskCopy FileSystem DiskServer) */
                    SubRequest.id, SubRequest.subreqId, SubRequest.castorfile, SubRequest.request
               FROM SubRequest PARTITION (P_STATUS_ACTIVE), DiskCopy, FileSystem, DiskServer
              WHERE SubRequest.status = dconst.SUBREQUEST_READY
                AND Subrequest.reqType IN (35, 37)  -- StageGet/PutRequest
                AND Subrequest.diskCopy = DiskCopy.id
                AND DiskCopy.fileSystem = FileSystem.id
                AND FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = machine
              UNION
             SELECT /*+ LEADING(DiskServer SubRequest) USE_NL(DiskServer SubRequest) */
                    SubRequest.id, SubRequest.subreqId, SubRequest.castorfile, SubRequest.request
               FROM SubRequest PARTITION (P_STATUS_ACTIVE), DiskServer
              WHERE SubRequest.status = dconst.SUBREQUEST_READY
                AND Subrequest.reqType IN (35, 37)  -- StageGet/PutRequest
                AND Subrequest.diskServer = DiskServer.id
                AND DiskServer.name = machine) LOOP
    BEGIN
      -- check if they are running on the diskserver
      SELECT subReqId INTO unused FROM SyncRunningTransfersHelper
       WHERE subreqId = SR.subreqId;
      -- this one was still running, nothing to do then
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- this transfer is not running anymore although the stager DB believes it is
      -- we first get its reqid and fileid
      BEGIN
        SELECT Request.reqId INTO varReqId FROM
          (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */ reqId, id from StageGetRequest UNION ALL
           SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ reqId, id from StagePutRequest) Request
         WHERE Request.id = SR.request;
        SELECT fileid, nsHost INTO varFileid, varNsHost FROM CastorFile WHERE id = SR.castorFile;
        -- and we put it in the list of transfers to be failed with code 1015 (SEINTERNAL)
        INSERT INTO SyncRunningTransfersHelper2 (subreqId, reqId, fileid, nsHost, errorCode, errorMsg)
        VALUES (SR.subreqId, varReqId, varFileid, varNsHost, 1015, 'Transfer has been killed while running');
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- not even the request exists any longer in the DB. It may have disappeared meanwhile
        -- as we didn't take any lock yet. Fine, ignore and move on.
        NULL;
      END;
    END;
  END LOOP;
  -- fail the transfers that are no more running
  SELECT subreqId, errorCode, errorMsg BULK COLLECT
    INTO killedTransfers, errnos, errmsg
    FROM SyncRunningTransfersHelper2;
  -- Note that the next call will commit (even once per transfer to kill)
  -- This is ok as SyncRunningTransfersHelper2 was declared "ON COMMIT PRESERVE ROWS" and
  -- is a temporary table so it's content is only visible to our connection.
  transferFailedSafe(killedTransfers, errnos, errmsg);
  -- and return list of transfers that have been failed, for logging purposes
  OPEN killedTransfersCur FOR
    SELECT subreqId, reqId, fileid, nsHost FROM SyncRunningTransfersHelper2;
END;
/
