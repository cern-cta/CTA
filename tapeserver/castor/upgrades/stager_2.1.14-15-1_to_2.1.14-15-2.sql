/******************************************************************************
 *                 stager_2.1.14-15-1_to_2.1.14-15-2.sql
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
 * This script upgrades a CASTOR v2.1.14-15-1 STAGER database to v2.1.14-15-2
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
   WHERE schemaVersion = '2_1_14_2'
     AND release = '2_1_14_15_2'
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
     AND release LIKE '2_1_14_15%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_15_2', 'TRANSPARENT');
COMMIT;

/* Update and revalidation of PL-SQL code */
/******************************************/

/*
 * PL/SQL method implementing filesDeleted
 * Note that we don't increase the freespace of the fileSystem.
 * This is done by the monitoring daemon, that knows the
 * exact amount of free space.
 * dcIds gives the list of diskcopies to delete.
 * fileIds returns the list of castor files to be removed
 * from the name server
 */
CREATE OR REPLACE PROCEDURE filesDeletedProc
(dcIds IN castor."cnumList",
 fileIds OUT castor.FileList_Cur) AS
  fid NUMBER;
  fc NUMBER;
  nsh VARCHAR2(2048);
  nb INTEGER;
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- List the castorfiles to be cleaned up afterwards
    FORALL i IN 1..dcIds.COUNT
      INSERT INTO FilesDeletedProcHelper (cfId, dcId) (
        SELECT castorFile, id FROM DiskCopy
         WHERE id = dcIds(i));
    -- Use a normal loop to clean castorFiles. Note: We order the list to
    -- prevent a deadlock
    FOR cf IN (SELECT cfId, dcId
                 FROM filesDeletedProcHelper
                ORDER BY cfId ASC) LOOP
      DECLARE
        CONSTRAINT_VIOLATED EXCEPTION;
        PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -2292);
      BEGIN
        -- Get data and lock the castorFile
        SELECT fileId, nsHost, fileClass
          INTO fid, nsh, fc
          FROM CastorFile
         WHERE id = cf.cfId FOR UPDATE;
        -- delete the original diskcopy to be dropped
        DELETE FROM DiskCopy WHERE id = cf.dcId;
        -- Cleanup:
        -- See whether it has any other DiskCopy or any new Recall request:
        -- if so, skip the rest
        SELECT count(*) INTO nb FROM DiskCopy
         WHERE castorFile = cf.cfId;
        IF nb > 0 THEN
          CONTINUE;
        END IF;
        SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
          FROM SubRequest
         WHERE castorFile = cf.cfId
           AND status = dconst.SUBREQUEST_WAITTAPERECALL;
        IF nb > 0 THEN
          CONTINUE;
        END IF;
        -- Now check for Disk2DiskCopy jobs
        SELECT /*+ INDEX(I_Disk2DiskCopyJob_cfId) */ count(*) INTO nb FROM Disk2DiskCopyJob
         WHERE castorFile = cf.cfId;
        IF nb > 0 THEN
          CONTINUE;
        END IF;
        -- Nothing found, check for any other subrequests
        SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile)*/ count(*) INTO nb
          FROM SubRequest
         WHERE castorFile = cf.cfId
           AND status IN (1, 2, 3, 4, 5, 6, 7, 10, 12, 13);  -- all but START, FINISHED, FAILED_FINISHED, ARCHIVED
        IF nb = 0 THEN
          -- Nothing left, delete the CastorFile
          DELETE FROM CastorFile WHERE id = cf.cfId;
        ELSE
          -- Fail existing subrequests for this file
          UPDATE /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile)*/ SubRequest
             SET status = dconst.SUBREQUEST_FAILED
           WHERE castorFile = cf.cfId
             AND status IN (1, 2, 3, 4, 5, 6, 12, 13);  -- same as above
        END IF;
        -- Check whether this file potentially had copies on tape
        SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
        IF nb = 0 THEN
          -- This castorfile was created with no copy on tape
          -- So removing it from the stager means erasing
          -- it completely. We should thus also remove it
          -- from the name server
          INSERT INTO FilesDeletedProcOutput (fileId, nsHost) VALUES (fid, nsh);
        END IF;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- Ignore, this means that the castorFile did not exist.
        -- There is thus no way to find out whether to remove the
        -- file from the nameserver. For safety, we thus keep it
        NULL;
      WHEN CONSTRAINT_VIOLATED THEN
        IF sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_%_CASTORFILE) violated%' THEN
          -- Ignore the deletion, probably some draining/rebalancing/recall activity created
          -- a new Disk2DiskCopyJob/RecallJob entity while we were attempting to drop the CastorFile
          NULL;
        ELSE
          -- Any other constraint violation is an error
          RAISE;
        END IF;
      END;
    END LOOP;
  END IF;
  OPEN fileIds FOR
    SELECT fileId, nsHost FROM FilesDeletedProcOutput;
END;
/

/* PL/SQL method implementing filesDeletionFailedProc */
CREATE OR REPLACE PROCEDURE filesDeletionFailedProc
(dcIds IN castor."cnumList") AS
  cfId NUMBER;
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- Loop over the files
    FORALL i IN 1..dcIds.COUNT
      UPDATE DiskCopy SET status = 4 -- FAILED
       WHERE id = dcIds(i);
  END IF;
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
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_SubreqId)*/ fileid, nsHost INTO fid, nh
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
BEGIN
  -- give up if nothing to be done
  IF subReqIds.COUNT = 0 THEN RETURN; END IF;
  -- Loop over all transfers to fail
  FOR i IN 1..subReqIds.COUNT LOOP
    BEGIN
      -- Get the necessary information needed about the request.
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_SubreqId)*/ id, diskCopy, reqType, castorFile
        INTO srId, dcId, rType, cfId
        FROM SubRequest
       WHERE subReqId = subReqIds(i)
         AND status = dconst.SUBREQUEST_READY;
      -- Lock the CastorFile.
      SELECT id INTO cfId FROM CastorFile
       WHERE id = cfId FOR UPDATE;
      -- Confirm SubRequest status hasn't changed after acquisition of lock
      SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/ id INTO srId FROM SubRequest
       WHERE id = srId AND status = dconst.SUBREQUEST_READY;
      -- Call the relevant cleanup procedure for the transfer, procedures that
      -- would have been called if the transfer failed on the remote execution host.
      IF rType = 40 THEN      -- StagePutRequest
       putFailedProcExt(srId, errnos(i), errmsgs(i));
      ELSE                    -- StageGetRequest or StageUpdateRequest
       getUpdateFailedProcExt(srId, errnos(i), errmsgs(i));
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- try disk2diskCopyJob
        SELECT id into srId FROM Disk2diskCopyJob WHERE transferId = subReqIds(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        CONTINUE;  -- The SubRequest/disk2DiskCopyJob may have been removed, nothing to be done.
      END;
      disk2DiskCopyEnded(subReqIds(i), '', '', 0, errnos(i), errmsgs(i));
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
PROCEDURE transferFailedLockedFile(subReqId IN castor."strList",
                                   errnos IN castor."cnumList",
                                   errmsgs IN castor."strList")
AS
  srId  NUMBER;
  dcId  NUMBER;
  rType NUMBER;
BEGIN
  FOR i IN 1..subReqId.COUNT LOOP
    BEGIN
      -- Get the necessary information needed about the request.
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_SubreqId)*/ id, diskCopy, reqType
        INTO srId, dcId, rType
        FROM SubRequest
       WHERE subReqId = subReqId(i)
         AND status = dconst.SUBREQUEST_READY;
      -- Call the relevant cleanup procedure for the transfer, procedures that
      -- would have been called if the transfer failed on the remote execution host.
      IF rType = 40 THEN      -- StagePutRequest
        putFailedProcExt(srId, errnos(i), errmsgs(i));
      ELSE                    -- StageGetRequest or StageUpdateRequest
        getUpdateFailedProcExt(srId, errnos(i), errmsgs(i));
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        -- try disk2diskCopyJob
        SELECT id into srId FROM Disk2diskCopyJob WHERE transferId = subReqId(i);
      EXCEPTION WHEN NO_DATA_FOUND THEN
        CONTINUE;  -- The SubRequest/disk2DiskCopyJob may have be removed, nothing to be done.
      END;
      -- found it, call disk2DiskCopyEnded
      disk2DiskCopyEnded(subReqId(i), '', '', 0, errnos(i), errmsgs(i));
    END;
  END LOOP;
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
  FOR SR IN (SELECT SubRequest.id, SubRequest.subreqId, SubRequest.castorfile, SubRequest.request
               FROM SubRequest, DiskCopy, FileSystem, DiskServer
              WHERE SubRequest.status = dconst.SUBREQUEST_READY
                AND Subrequest.reqType IN (35, 37, 44)  -- StageGet/Put/UpdateRequest
                AND Subrequest.diskCopy = DiskCopy.id
                AND DiskCopy.fileSystem = FileSystem.id
                AND FileSystem.diskServer = DiskServer.id
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
           SELECT /*+ INDEX(StagePutRequest PK_StagePutRequest_Id) */ reqId, id from StagePutRequest UNION ALL
           SELECT /*+ INDEX(StageUpdateRequest PK_StageUpdateRequest_Id) */ reqId, id from StageUpdateRequest) Request
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

/* inserts file Requests in the stager DB.
 * This handles StageGetRequest, StagePrepareToGetRequest, StagePutRequest,
 * StagePrepareToPutRequest, StageUpdateRequest, StagePrepareToUpdateRequest,
 * StagePutDoneRequest, StagePrepareToUpdateRequest, and StageRmRequest requests.
 */    
CREATE OR REPLACE PROCEDURE insertFileRequest
  (userTag IN VARCHAR2,
   machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   mask IN INTEGER,
   userName IN VARCHAR2,
   flags IN INTEGER,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   inReqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   freeStrParam IN VARCHAR2,
   freeNumParam IN NUMBER,
   srFileNames IN castor."strList",
   srProtocols IN castor."strList",
   srXsizes IN castor."cnumList",
   srFlags IN castor."cnumList",
   srModeBits IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  subreqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  svcHandler VARCHAR2(100);
  protocols VARCHAR2(100);
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, inReqType);
  -- get the list of valid protocols
  protocols := ' ' || getConfigOption('Stager', 'Protocols', 'rfio rfio3 gsiftp xroot') || ' ';
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN inReqType = 35 THEN -- StageGetRequest
      INSERT INTO StageGetRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 36 THEN -- StagePrepareToGetRequest
      INSERT INTO StagePrepareToGetRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 40 THEN -- StagePutRequest
      INSERT INTO StagePutRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 37 THEN -- StagePrepareToPutRequest
      INSERT INTO StagePrepareToPutRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 44 THEN -- StageUpdateRequest
      INSERT INTO StageUpdateRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 38 THEN -- StagePrepareToUpdateRequest
      INSERT INTO StagePrepareToUpdateRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 42 THEN -- StageRmRequest
      INSERT INTO StageRmRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN inReqType = 39 THEN -- StagePutDoneRequest
      INSERT INTO StagePutDoneRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,freeStrParam,reqId,svcClassId,clientId);
    WHEN inReqType = 95 THEN -- SetFileGCWeight
      INSERT INTO SetFileGCWeight (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, weight, id, svcClass, client)
      VALUES (flags,userName,euid,egid,mask,pid,machine,svcClassName,userTag,reqUUID,creationTime,creationTime,freeNumParam,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertFileRequest : ' || TO_CHAR(inReqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- get the request's service handler
  SELECT svcHandler INTO svcHandler FROM Type2Obj WHERE type=inReqType;
  -- Loop on subrequests
  FOR i IN 1..srFileNames.COUNT LOOP
    -- check protocol validity for Get/Update/Put requests only, for other requests the protocol is irrelevant
    IF inReqType IN (35, 40, 44) AND INSTR(protocols, ' ' || srProtocols(i) || ' ') = 0 THEN
      raise_application_error(-20122, 'Unsupported protocol in insertFileRequest : ' || srProtocols(i));
    END IF;
    -- get unique ids for the subrequest
    SELECT ids_seq.nextval INTO subreqId FROM DUAL;
    -- insert the subrequest
    INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, status, request, getNextStatus, reqType)
    VALUES (0, srFileNames(i), srProtocols(i), srXsizes(i), 0, NULL, srFlags(i), srModeBits(i), creationTime, creationTime, 0, 0, '', NULL, svcHandler, subreqId, NULL, NULL, dconst.SUBREQUEST_START, reqId, 0, inReqType);
  END LOOP;
  -- send one single alert to accelerate the processing of the request
  CASE
  WHEN inReqType = 35 OR   -- StageGetRequest
       inReqType = 40 OR   -- StagePutRequest
       inReqType = 44 THEN -- StageUpdateRequest
    alertSignalNoLock('wakeUpJobReqSvc');
  WHEN inReqType = 36 OR   -- StagePrepareToGetRequest
       inReqType = 37 OR   -- StagePrepareToPutRequest
       inReqType = 38 THEN -- StagePrepareToUpdateRequest
    alertSignalNoLock('wakeUpPrepReqSvc');
  WHEN inReqType = 42 OR   -- StageRmRequest
       inReqType = 39 OR   -- StagePutDoneRequest
       inReqType = 95 THEN -- SetFileGCWeight
    alertSignalNoLock('wakeUpStageReqSvc');
  END CASE;
END;
/

/* inserts StageFileQueryRequest requests in the stager DB */    
CREATE OR REPLACE PROCEDURE insertStageFileQueryRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   fileName IN VARCHAR2,
   qpValues IN castor."strList",
   qpTypes IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  queryParamId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 33 THEN -- StageFileQueryRequest
      INSERT INTO StageFileQueryRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, fileName, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,fileName,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertFileQueryRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on query parameters. Note that the array is never empty (see the C++ calling method).
  FOR i IN 1..qpValues.COUNT LOOP
    -- get unique ids for the query parameter
    SELECT ids_seq.nextval INTO queryParamId FROM DUAL;
    -- insert the query parameter
    INSERT INTO QueryParameter (value, id, query, querytype)
    VALUES (qpValues(i), queryParamId, reqId, qpTypes(i));
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
  -- send an alert to accelerate the processing of the request
  alertSignalNoLock('wakeUpQueryReqSvc');
END;
/

/* inserts StageAbort Request in the stager DB */    
CREATE OR REPLACE PROCEDURE insertStageAbortRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   parentUUID IN VARCHAR2,
   fileids IN castor."cnumList",
   nsHosts IN castor."strList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  fileidId INTEGER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 50 THEN -- StageAbortRequest
      INSERT INTO StageAbortRequest (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, parentUuid, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,parentUUID,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertStageAbortRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on fileids
  FOR i IN 1..fileids.COUNT LOOP
    -- ignore fake items passed only because ORACLE does not like empty arrays
    IF nsHosts(i) IS NULL THEN EXIT; END IF;
    -- get unique ids for the fileid
    SELECT ids_seq.nextval INTO fileidId FROM DUAL;
    -- insert the fileid
    INSERT INTO NsFileId (fileId, nsHost, id, request)
    VALUES (fileids(i), nsHosts(i), fileidId, reqId);
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
  -- send an alert to accelerate the processing of the request
  alertSignalNoLock('wakeUpBulkStageReqSvc');
END;
/

/* inserts GC Requests in the stager DB
 * This handles NsFilesDeleted, FilesDeleted, FilesDeletionFailed and StgFilesDeleted 
 * requests.
 */
CREATE OR REPLACE PROCEDURE insertGCRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   nsHost IN VARCHAR2,
   diskCopyIds IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  gcFileId INTEGER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 142 THEN -- NsFilesDeleted
      INSERT INTO NsFilesDeleted (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, nsHost, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,nsHost,reqId,svcClassId,clientId);
    WHEN reqType = 74 THEN -- FilesDeleted
      INSERT INTO FilesDeleted (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN reqType = 83 THEN -- FilesDeletionFailed
      INSERT INTO FilesDeletionFailed (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,reqId,svcClassId,clientId);
    WHEN reqType = 149 THEN -- StgFilesDeleted
      INSERT INTO StgFilesDeleted (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, nsHost, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,nsHost,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertGCRequest : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on diskCopies
  FOR i IN 1..diskCopyIds.COUNT LOOP
    -- get unique ids for the diskCopy
    SELECT ids_seq.nextval INTO gcFileId FROM DUAL;
    -- insert the fileid
    INSERT INTO GCFile (diskCopyId, id, request)
    VALUES (diskCopyIds(i), gcFileId, reqId);
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
  -- send an alert to accelerate the processing of the request
  alertSignalNoLock('wakeUpGCSvc');
END;
/

/* inserts ChangePrivilege Request in the stager DB */   
CREATE OR REPLACE PROCEDURE insertChangePrivilegeRequest
  (machine IN VARCHAR2,
   euid IN INTEGER,
   egid IN INTEGER,
   pid IN INTEGER,
   userName IN VARCHAR2,
   svcClassName IN VARCHAR2,
   reqUUID IN VARCHAR2,
   reqType IN INTEGER,
   clientIP IN INTEGER,
   clientPort IN INTEGER,
   clientVersion IN INTEGER,
   clientSecure IN INTEGER,
   isGranted IN INTEGER,
   euids IN castor."cnumList",
   egids IN castor."cnumList",
   reqTypes IN castor."cnumList") AS
  svcClassId NUMBER;
  reqId NUMBER;
  clientId NUMBER;
  creationTime NUMBER;
  subobjId INTEGER;
BEGIN
  -- do prechecks and get the service class
  svcClassId := insertPreChecks(euid, egid, svcClassName, reqType);
  -- get unique ids for the request and the client and get current time
  SELECT ids_seq.nextval INTO reqId FROM DUAL;
  SELECT ids_seq.nextval INTO clientId FROM DUAL;
  creationTime := getTime();
  -- insert the request itself
  CASE
    WHEN reqType = 152 THEN -- ChangePrivilege
      INSERT INTO ChangePrivilege (flags, userName, euid, egid, mask, pid, machine, svcClassName, userTag, reqId, creationTime, lastModificationTime, isGranted, id, svcClass, client)
      VALUES (0,userName,euid,egid,0,pid,machine,svcClassName,'',reqUUID,creationTime,creationTime,isGranted,reqId,svcClassId,clientId);
    ELSE
      raise_application_error(-20122, 'Unsupported request type in insertChangePrivilege : ' || TO_CHAR(reqType));
  END CASE;
  -- insert the client information
  INSERT INTO Client (ipAddress, port, version, secure, id)
  VALUES (clientIP,clientPort,clientVersion,clientSecure,clientId);
  -- Loop on request types
  FOR i IN 1..reqTypes.COUNT LOOP
    -- get unique ids for the request type
    SELECT ids_seq.nextval INTO subobjId FROM DUAL;
    -- insert the request type
    INSERT INTO RequestType (reqType, id, request)
    VALUES (reqTypes(i), subobjId, reqId);
  END LOOP;
  -- Loop on BWUsers
  FOR i IN 1..euids.COUNT LOOP
    -- get unique ids for the request type
    SELECT ids_seq.nextval INTO subobjId FROM DUAL;
    -- insert the BWUser
    INSERT INTO BWUser (euid, egid, id, request)
    VALUES (euids(i), egids(i), subobjId, reqId);
  END LOOP;
  -- insert a row into newRequests table to trigger the processing of the request
  INSERT INTO newRequests (id, type, creation) VALUES (reqId, reqType, to_date('01011970','ddmmyyyy') + 1/24/60/60 * creationTime);
  -- send an alert to accelerate the processing of the request
  alertSignalNoLock('wakeUpQueryReqSvc');
END;
/

/* PL/SQL procedure implementing triggerRepackMigration */
CREATE OR REPLACE PROCEDURE triggerRepackMigration
(cfId IN INTEGER, vid IN VARCHAR2, fileid IN INTEGER, copyNb IN INTEGER,
 fileclass IN INTEGER, fileSize IN INTEGER, allSegments IN VARCHAR2,
 inMJStatus IN INTEGER, migrationTriggered OUT boolean) AS
  varMjId INTEGER;
  varNb INTEGER;
  varDestCopyNb INTEGER;
  varNbCopies INTEGER;
  varAllSegments strListTable;
BEGIN
  -- check whether we already have a migrationJob for this copy of the file
  SELECT id INTO varMjId
    FROM MigrationJob
   WHERE castorFile = cfId
     AND destCopyNb = copyNb;
  -- we have a migrationJob for this copy ! This means that the file has been overwritten
  -- and the new version is about to go to tape. We thus don't have anything to do
  -- for this file
  migrationTriggered := False;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- no migration job for this copyNb, we can proceed
  -- first let's parse the list of all segments
  SELECT * BULK COLLECT INTO varAllSegments
    FROM TABLE(strTokenizer(allSegments));
  DECLARE
    varAllCopyNbs "numList" := "numList"();
    varAllVIDs strListTable := strListTable();
  BEGIN
    FOR i IN 1..varAllSegments.COUNT/2 LOOP
      varAllCopyNbs.EXTEND;
      varAllCopyNbs(i) := TO_NUMBER(varAllSegments(2*i-1));
      varAllVIDs.EXTEND;
      varAllVIDs(i) := varAllSegments(2*i);
    END LOOP;
    -- find the new copy number to be used. This is the minimal one
    -- that is lower than the allowed number of copies and is not
    -- already used by an ongoing migration or by a valid migrated copy
    SELECT nbCopies INTO varNbCopies FROM FileClass WHERE classId = fileclass;
    FOR i IN 1 .. varNbCopies LOOP
      -- if we are reusing the original copy number, it's ok
      IF i = copyNb THEN
        varDestCopyNb := i;
      ELSE
        BEGIN
          -- check whether this copy number is already in use by a valid copy
          SELECT * INTO varNb FROM TABLE(varAllCopyNbs)
           WHERE COLUMN_VALUE = i;
          -- this copy number is in use, go to next one
        EXCEPTION WHEN NO_DATA_FOUND THEN
          BEGIN
            -- check whether this copy number is in use by an ongoing migration
            SELECT destCopyNb INTO varNb FROM MigrationJob WHERE castorFile = cfId AND destCopyNb = i;
            -- this copy number is in use, go to next one
          EXCEPTION WHEN NO_DATA_FOUND THEN
            -- copy number is not in use, we take it
            varDestCopyNb := i;
            EXIT;
          END;
        END;
      END IF;
    END LOOP;
    IF varDestCopyNb IS NULL THEN
      RAISE_APPLICATION_ERROR (-20123,
        'Unable to find a valid copy number for the migration of file ' ||
        TO_CHAR (fileid) || ' in triggerRepackMigration. ' ||
        'The file probably has too many valid copies.');
    END IF;
    -- create new migration
    initMigration(cfId, fileSize, vid, copyNb, varDestCopyNb, inMJStatus);
    -- create migrated segments for the existing segments if there are none
    SELECT /*+ INDEX_RS_ASC (MigratedSegment I_MigratedSegment_CFCopyNbVID) */ count(*) INTO varNb
      FROM MigratedSegment
     WHERE castorFile = cfId;
    IF varNb = 0 THEN
      FOR i IN 1..varAllCopyNbs.COUNT LOOP
        INSERT INTO MigratedSegment (castorFile, copyNb, VID)
        VALUES (cfId, varAllCopyNbs(i), varAllVIDs(i));
      END LOOP;
    END IF;
    -- Reset the CastorFile as not on tape: it may have already been there before this Repack
    -- request and with tapeStatus = ONTAPE, but now we want to have a migration.
    UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_NOTONTAPE
     WHERE id = cfId;
    -- all is fine, migration was triggered
    migrationTriggered := True;
  END;
END;
/

/* PL/SQL procedure implementing triggerRepackRecall
 * this triggers a recall in the repack context
 */
CREATE OR REPLACE PROCEDURE triggerRepackRecall
(inCfId IN INTEGER, inFileId IN INTEGER, inNsHost IN VARCHAR2, inBlock IN RAW,
 inFseq IN INTEGER, inCopynb IN INTEGER, inEuid IN INTEGER, inEgid IN INTEGER,
 inRecallGroupId IN INTEGER, inSvcClassId IN INTEGER, inVid IN VARCHAR2, inFileSize IN INTEGER,
 inFileClass IN INTEGER, inAllValidSegments IN VARCHAR2, inReqUUID IN VARCHAR2,
 inSubReqUUID IN VARCHAR2, inRecallGroupName IN VARCHAR2) AS
  varLogParam VARCHAR2(2048);
  varAllValidCopyNbs "numList" := "numList"();
  varAllValidVIDs strListTable := strListTable();
  varAllValidSegments strListTable;
  varFileClassId INTEGER;
BEGIN
  -- create recallJob for the given VID, copyNb, etc.
  INSERT INTO RecallJob (id, castorFile, copyNb, recallGroup, svcClass, euid, egid,
                         vid, fseq, status, fileSize, creationTime, blockId, fileTransactionId)
  VALUES (ids_seq.nextval, inCfId, inCopynb, inRecallGroupId, inSvcClassId,
          inEuid, inEgid, inVid, inFseq, tconst.RECALLJOB_PENDING, inFileSize, getTime(),
          inBlock, NULL);
  -- log "created new RecallJob"
  varLogParam := 'SUBREQID=' || inSubReqUUID || ' RecallGroup=' || inRecallGroupName;
  logToDLF(inReqUUID, dlf.LVL_SYSTEM, dlf.RECALL_CREATING_RECALLJOB, inFileId, inNsHost, 'stagerd',
           varLogParam || ' fileClass=' || TO_CHAR(inFileClass) || ' copyNb=' || TO_CHAR(inCopynb)
           || ' TPVID=' || inVid || ' fseq=' || TO_CHAR(inFseq) || ' FileSize=' || TO_CHAR(inFileSize));
  -- create missing segments if needed
  SELECT * BULK COLLECT INTO varAllValidSegments
    FROM TABLE(strTokenizer(inAllValidSegments));
  FOR i IN 1 .. varAllValidSegments.COUNT/2 LOOP
    varAllValidCopyNbs.EXTEND;
    varAllValidCopyNbs(i) := TO_NUMBER(varAllValidSegments(2*i-1));
    varAllValidVIDs.EXTEND;
    varAllValidVIDs(i) := varAllValidSegments(2*i);
  END LOOP;
  SELECT id INTO varFileClassId
    FROM FileClass WHERE classId = inFileClass;
  -- Note that the number given here for the number of existing segments is the total number of
  -- segment, without removing the ones on  EXPORTED tapes. This means that repack will not
  -- recreate new segments for files that have some copies on EXPORTED tapes.
  -- This is different from standard recalls that would recreate segments on EXPORTED tapes.
  createMJForMissingSegments(inCfId, inFileSize, varFileClassId, varAllValidCopyNbs,
                             varAllValidVIDs, varAllValidCopyNbs.COUNT, inFileId, inNsHost, varLogParam);
END;
/

/* PL/SQL method to handle a Repack request. This is performed as part of a DB job. */
CREATE OR REPLACE PROCEDURE handleRepackRequest(inReqUUID IN VARCHAR2,
                                                outNbFilesProcessed OUT INTEGER, outNbFailures OUT INTEGER) AS
  varEuid INTEGER;
  varEgid INTEGER;
  svcClassId INTEGER;
  varRepackVID VARCHAR2(10);
  varReqId INTEGER;
  cfId INTEGER;
  dcId INTEGER;
  repackProtocol VARCHAR2(2048);
  nsHostName VARCHAR2(2048);
  lastKnownFileName VARCHAR2(2048);
  varRecallGroupId INTEGER;
  varRecallGroupName VARCHAR2(2048);
  varCreationTime NUMBER;
  unused INTEGER;
  firstCF boolean := True;
  isOngoing boolean := False;
  varTotalSize INTEGER := 0;
BEGIN
  UPDATE StageRepackRequest SET status = tconst.REPACK_STARTING
   WHERE reqId = inReqUUID
  RETURNING id, euid, egid, svcClass, repackVID
    INTO varReqId, varEuid, varEgid, svcClassId, varRepackVID;
  -- commit so that the status change is visible, even if the request is empty for the moment
  COMMIT;
  outNbFilesProcessed := 0;
  outNbFailures := 0;
  -- Check which protocol should be used for writing files to disk
  repackProtocol := getConfigOption('Repack', 'Protocol', 'rfio');
  -- creation time for the subrequests
  varCreationTime := getTime();
  -- name server host name
  nsHostName := getConfigOption('stager', 'nsHost', '');
  -- find out the recallGroup to be used for this request
  getRecallGroup(varEuid, varEgid, varRecallGroupId, varRecallGroupName);
  COMMIT;
  -- Get the list of files to repack from the NS DB via DBLink and store them in memory
  -- in a temporary table. We do that so that we do not keep an open cursor for too long
  -- in the nameserver DB
  -- Note the truncation of stagerTime to 5 digits. This is needed for consistency with
  -- the stager code that uses the OCCI api and thus loses precision when recuperating
  -- 64 bits integers into doubles (lack of support for 64 bits numbers in OCCI)
  INSERT INTO RepackTapeSegments (fileId, lastOpenTime, blockId, fseq, segSize, copyNb, fileClass, allSegments)
    (SELECT s_fileid, TRUNC(stagertime,5), blockid, fseq, segSize,
            copyno, fileclass,
            (SELECT LISTAGG(TO_CHAR(oseg.copyno)||','||oseg.vid, ',')
             WITHIN GROUP (ORDER BY copyno)
               FROM Cns_Seg_Metadata@remotens oseg
              WHERE oseg.s_fileid = seg.s_fileid
                AND oseg.s_status = '-'
              GROUP BY oseg.s_fileid)
       FROM Cns_Seg_Metadata@remotens seg, Cns_File_Metadata@remotens fileEntry
      WHERE seg.vid = varRepackVID
        AND seg.s_fileid = fileEntry.fileid
        AND seg.s_status = '-'
        AND fileEntry.status != 'D');
  FOR segment IN (SELECT * FROM RepackTapeSegments) LOOP
    DECLARE
      varSubreqId INTEGER;
      varSubreqUUID VARCHAR2(2048);
      varSrStatus INTEGER := dconst.SUBREQUEST_REPACK;
      varMjStatus INTEGER := tconst.MIGRATIONJOB_PENDING;
      varNbCopies INTEGER;
      varSrErrorCode INTEGER := 0;
      varSrErrorMsg VARCHAR2(2048) := NULL;
      varWasRecalled NUMBER;
      varMigrationTriggered BOOLEAN := False;
    BEGIN
      IF MOD(outNbFilesProcessed, 1000) = 0 THEN
        -- Commit from time to time. Update total counter so that the display is correct.
        UPDATE StageRepackRequest
           SET fileCount = outNbFilesProcessed
         WHERE reqId = inReqUUID;
        COMMIT;
        firstCF := TRUE;
      END IF;
      outNbFilesProcessed := outNbFilesProcessed + 1;
      varTotalSize := varTotalSize + segment.segSize;
      -- lastKnownFileName we will have in the DB
      lastKnownFileName := CONCAT('Repack_', TO_CHAR(segment.fileid));
      -- find the Castorfile (and take a lock on it)
      DECLARE
        locked EXCEPTION;
        PRAGMA EXCEPTION_INIT (locked, -54);
      BEGIN
        -- This may raise a Locked exception as we do not want to wait for locks (except on first file).
        -- In such a case, we commit what we've done so far and retry this file, this time waiting for the lock.
        -- The reason for such a complex code is to avoid commiting each file separately, as it would be
        -- too heavy. On the other hand, we still need to avoid dead locks.
        -- Note that we pass 0 for the subrequest id, thus the subrequest will not be attached to the
        -- CastorFile. We actually attach it when we create it.
        selectCastorFileInternal(segment.fileid, nsHostName, segment.fileclass,
                                 segment.segSize, lastKnownFileName, 0, segment.lastOpenTime, firstCF, cfid, unused);
        firstCF := FALSE;
      EXCEPTION WHEN locked THEN
        -- commit what we've done so far
        COMMIT;
        -- And lock the castorfile (waiting this time)
        selectCastorFileInternal(segment.fileid, nsHostName, segment.fileclass,
                                 segment.segSize, lastKnownFileName, 0, segment.lastOpenTime, TRUE, cfid, unused);
      END;
      -- create  subrequest for this file.
      -- Note that the svcHandler is not set. It will actually never be used as repacks are handled purely in PL/SQL
      INSERT INTO SubRequest (retryCounter, fileName, protocol, xsize, priority, subreqId, flags, modeBits, creationTime, lastModificationTime, answered, errorCode, errorMessage, requestedFileSystems, svcHandler, id, diskcopy, castorFile, status, request, getNextStatus, reqType)
      VALUES (0, lastKnownFileName, repackProtocol, segment.segSize, 0, uuidGen(), 0, 0, varCreationTime, varCreationTime, 1, 0, '', NULL, 'NotNullNeeded', ids_seq.nextval, 0, cfId, dconst.SUBREQUEST_START, varReqId, 0, 119)
      RETURNING id, subReqId INTO varSubreqId, varSubreqUUID;
      -- if the file is being overwritten, fail
      SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_CastorFile) */
             count(DiskCopy.id) INTO varNbCopies
        FROM DiskCopy
       WHERE DiskCopy.castorfile = cfId
         AND DiskCopy.status = dconst.DISKCOPY_STAGEOUT;
      IF varNbCopies > 0 THEN
        varSrStatus := dconst.SUBREQUEST_FAILED;
        varSrErrorCode := serrno.EBUSY;
        varSrErrorMsg := 'File is currently being overwritten';
        outNbFailures := outNbFailures + 1;
      ELSE
        -- find out whether this file is already on disk
        SELECT /*+ INDEX_RS_ASC(DC I_DiskCopy_CastorFile) */
               count(DC.id) INTO varNbCopies
          FROM DiskCopy DC, FileSystem, DiskServer
         WHERE DC.castorfile = cfId
           AND DC.fileSystem = FileSystem.id
           AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
           AND FileSystem.diskserver = DiskServer.id
           AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
           AND DiskServer.hwOnline = 1
           AND DC.status = dconst.DISKCOPY_VALID;
        IF varNbCopies = 0 THEN
          -- find out whether this file is already being recalled from this tape
          SELECT /*+ INDEX_RS_ASC(RecallJob I_RecallJob_CastorFile) */ count(*)
            INTO varWasRecalled FROM RecallJob WHERE castorfile = cfId AND vid != varRepackVID;
          IF varWasRecalled = 0 THEN
            -- trigger recall: if we have dual copy files, this may trigger a second recall,
            -- which will race with the first as it happens for user-driven recalls
            triggerRepackRecall(cfId, segment.fileid, nsHostName, segment.blockid,
                                segment.fseq, segment.copyNb, varEuid, varEgid,
                                varRecallGroupId, svcClassId, varRepackVID, segment.segSize,
                                segment.fileclass, segment.allSegments, inReqUUID, varSubreqUUID, varRecallGroupName);
          END IF;
          -- file is being recalled
          varSrStatus := dconst.SUBREQUEST_WAITTAPERECALL;
          isOngoing := TRUE;
        END IF;
        -- deal with migrations
        IF varSrStatus = dconst.SUBREQUEST_WAITTAPERECALL THEN
          varMJStatus := tconst.MIGRATIONJOB_WAITINGONRECALL;
        END IF;
        DECLARE
          noValidCopyNbFound EXCEPTION;
          PRAGMA EXCEPTION_INIT (noValidCopyNbFound, -20123);
          noMigrationRoute EXCEPTION;
          PRAGMA EXCEPTION_INIT (noMigrationRoute, -20100);
        BEGIN
          triggerRepackMigration(cfId, varRepackVID, segment.fileid, segment.copyNb, segment.fileclass,
                                 segment.segSize, segment.allSegments, varMJStatus, varMigrationTriggered);
          IF varMigrationTriggered THEN
            -- update CastorFile tapeStatus
            UPDATE CastorFile SET tapeStatus = dconst.CASTORFILE_NOTONTAPE WHERE id = cfId;
          END IF;
          isOngoing := True;
        EXCEPTION WHEN noValidCopyNbFound OR noMigrationRoute THEN
          -- cleanup recall part if needed
          IF varWasRecalled = 0 THEN
            DELETE FROM RecallJob WHERE castorFile = cfId;
          END IF;
          -- fail SubRequest
          varSrStatus := dconst.SUBREQUEST_FAILED;
          varSrErrorCode := serrno.EINVAL;
          varSrErrorMsg := SQLERRM;
          outNbFailures := outNbFailures + 1;
        END;
      END IF;
      -- update SubRequest
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
         SET status = varSrStatus,
             errorCode = varSrErrorCode,
             errorMessage = varSrErrorMsg
       WHERE id = varSubreqId;
    EXCEPTION WHEN OTHERS THEN
      -- something went wrong: log "handleRepackRequest: unexpected exception caught"
      outNbFailures := outNbFailures + 1;
      varSrErrorMsg := 'Oracle error caught : ' || SQLERRM;
      logToDLF(NULL, dlf.LVL_ERROR, dlf.REPACK_UNEXPECTED_EXCEPTION, segment.fileId, nsHostName, 'repackd',
        'errorCode=' || to_char(SQLCODE) ||' errorMessage="' || varSrErrorMsg
        ||'" stackTrace="' || dbms_utility.format_error_backtrace ||'"');
      -- cleanup and fail SubRequest
      IF varWasRecalled = 0 THEN
        DELETE FROM RecallJob WHERE castorFile = cfId;
      END IF;
      IF varMigrationTriggered THEN
        DELETE FROM MigrationJob WHERE castorFile = cfId;
        DELETE FROM MigratedSegment WHERE castorFile = cfId;
      END IF;
      UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id) */ SubRequest
         SET status = dconst.SUBREQUEST_FAILED,
             errorCode = serrno.SEINTERNAL,
             errorMessage = varSrErrorMsg
       WHERE id = varSubreqId;
    END;
  END LOOP;
  -- cleanup RepackTapeSegments
  EXECUTE IMMEDIATE 'TRUNCATE TABLE RepackTapeSegments';
  -- update status of the RepackRequest
  IF isOngoing THEN
    UPDATE StageRepackRequest
       SET status = tconst.REPACK_ONGOING,
           fileCount = outNbFilesProcessed,
           totalSize = varTotalSize
     WHERE StageRepackRequest.id = varReqId;
  ELSE
    IF outNbFailures > 0 THEN
      UPDATE StageRepackRequest
         SET status = tconst.REPACK_FAILED,
             fileCount = outNbFilesProcessed,
             totalSize = varTotalSize
       WHERE StageRepackRequest.id = varReqId;
    ELSE
      -- CASE of an 'empty' repack : the tape had no files at all
      UPDATE StageRepackRequest
         SET status = tconst.REPACK_FINISHED,
             fileCount = 0,
             totalSize = 0
       WHERE StageRepackRequest.id = varReqId;
    END IF;
  END IF;
  COMMIT;
END;
/

/* PL/SQL method to process bulk abort on files related requests */
CREATE OR REPLACE PROCEDURE processBulkAbortFileReqs
(origReqId IN INTEGER, fileIds IN "numList", nsHosts IN strListTable, reqType IN NUMBER) AS
  nbItems NUMBER;
  nbItemsDone NUMBER := 0;
  SrLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (SrLocked, -54);
  unused NUMBER;
  firstOne BOOLEAN := TRUE;
  commitWork BOOLEAN := FALSE;
BEGIN
  -- Gather the list of subrequests to abort
  IF fileIds.count() = 0 THEN
    -- handle the case of an empty request, meaning that all files should be aborted
    INSERT INTO ProcessBulkAbortFileReqsHelper (srId, cfId, fileId, nsHost, uuid) (
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Request)*/
             SubRequest.id, CastorFile.id, CastorFile.fileId, CastorFile.nsHost, SubRequest.subreqId
        FROM SubRequest, CastorFile
       WHERE SubRequest.castorFile = CastorFile.id
         AND request = origReqId);
  ELSE
    -- handle the case of selective abort
    FOR i IN 1..fileIds.COUNT LOOP
      DECLARE
        CONSTRAINT_VIOLATED EXCEPTION;
        PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
      BEGIN
        -- note that we may insert several rows in one go in case the abort request contains
        -- several times the same file
        INSERT INTO processBulkAbortFileReqsHelper
          (SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_CastorFile)*/
                  DISTINCT SubRequest.id, CastorFile.id, fileIds(i), nsHosts(i), SubRequest.subreqId
             FROM SubRequest, CastorFile
            WHERE request = origReqId
              AND SubRequest.castorFile = CastorFile.id
              AND CastorFile.fileid = fileIds(i)
              AND CastorFile.nsHost = nsHosts(i));
        -- check that we found something
        IF SQL%ROWCOUNT = 0 THEN
          -- this fileid/nshost did not exist in the request, send an error back
          INSERT INTO ProcessBulkRequestHelper (fileId, nsHost, errorCode, errorMessage)
          VALUES (fileIds(i), nsHosts(i), serrno.ENOENT, 'No subRequest found for this fileId/nsHost');
        END IF;
      EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
        -- the insertion in ProcessBulkRequestHelper triggered a violation of the
        -- primary key. This primary key being the subrequest id, this means that
        -- this subrequest is already in the list of the ones to be aborted. So
        -- nothing left to be done
        NULL;
      END;
    END LOOP;
  END IF;
  SELECT COUNT(*) INTO nbItems FROM processBulkAbortFileReqsHelper;
  -- handle aborts in bulk while avoiding deadlocks
  WHILE nbItems > 0 LOOP
    FOR sr IN (SELECT srId, cfId, fileId, nsHost, uuid FROM processBulkAbortFileReqsHelper) LOOP
      BEGIN
        IF firstOne THEN
          -- on the first item, we take a blocking lock as we are sure that we will not
          -- deadlock and we would like to process at least one item to not loop endlessly
          SELECT id INTO unused FROM CastorFile WHERE id = sr.cfId FOR UPDATE;
          firstOne := FALSE;
        ELSE
          -- on the other items, we go for a non blocking lock. If we get it, that's
          -- good and we process this extra subrequest within the same session. If
          -- we do not get the lock, then we close the session here and go for a new
          -- one. This will prevent dead locks while ensuring that a minimal number of
          -- commits is performed.
          SELECT id INTO unused FROM CastorFile WHERE id = sr.cfId FOR UPDATE NOWAIT;
        END IF;
        -- we got the lock on the Castorfile, we can handle the abort for this subrequest
        CASE reqType
          WHEN 1 THEN processAbortForGet(sr);
          WHEN 2 THEN processAbortForPut(sr);
        END CASE;
        DELETE FROM processBulkAbortFileReqsHelper WHERE srId = sr.srId;
        -- make the scheduler aware so that it can remove the transfer from the queues if needed
        INSERT INTO TransfersToAbort (uuid) VALUES (sr.uuid);
        nbItemsDone := nbItemsDone + 1;
      EXCEPTION WHEN SrLocked THEN
        commitWork := TRUE;
      END;
      -- commit anyway from time to time, to avoid too long redo logs
      IF commitWork OR nbItemsDone >= 1000 THEN
        -- exit the current loop and restart a new one, in order to commit without getting invalid ROWID errors
        EXIT;
      END IF;
    END LOOP;
    -- commit
    COMMIT;
    -- wake up the scheduler so that it can remove the transfer from the queues
    alertSignalNoLock('transfersToAbort');
    -- reset all counters
    nbItems := nbItems - nbItemsDone;
    nbItemsDone := 0;
    firstOne := TRUE;
    commitWork := FALSE;
  END LOOP;
END;
/

/* PL/SQL method creating MigrationJobs for missing segments of a file if needed */
/* Can throw a -20100 exception when no route to tape is found for the missing segments */
CREATE OR REPLACE PROCEDURE createMJForMissingSegments(inCfId IN INTEGER,
                                                       inFileSize IN INTEGER,
                                                       inFileClassId IN INTEGER,
                                                       inAllValidCopyNbs IN "numList",
                                                       inAllValidVIDs IN strListTable,
                                                       inNbExistingSegments IN INTEGER,
                                                       inFileId IN INTEGER,
                                                       inNsHost IN VARCHAR2,
                                                       inLogParams IN VARCHAR2) AS
  varExpectedNbCopies INTEGER;
  varCreatedMJs INTEGER := 0;
  varNextCopyNb INTEGER := 1;
  varNb INTEGER;
BEGIN
  -- check whether there are missing segments and whether we should create new ones
  SELECT nbCopies INTO varExpectedNbCopies FROM FileClass WHERE id = inFileClassId;
  IF varExpectedNbCopies > inNbExistingSegments THEN
    -- some copies are missing
    DECLARE
      unused INTEGER;
    BEGIN
      -- check presence of migration jobs for this file
      SELECT id INTO unused FROM MigrationJob WHERE castorFile=inCfId AND ROWNUM < 2;
      -- there are MigrationJobs already, so remigrations were already handled. Nothing to be done
      -- we typically are in a situation where the file was already waiting for recall for
      -- another recall group.
      -- log "detected missing copies on tape, but migrations ongoing"
      logToDLF(NULL, dlf.LVL_DEBUG, dlf.RECALL_MISSING_COPIES_NOOP, inFileId, inNsHost, 'stagerd',
               inLogParams || ' nbMissingCopies=' || TO_CHAR(varExpectedNbCopies-inNbExistingSegments));
      RETURN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- we need to remigrate this file
      NULL;
    END;
    -- log "detected missing copies on tape"
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_MISSING_COPIES, inFileId, inNsHost, 'stagerd',
             inLogParams || ' nbMissingCopies=' || TO_CHAR(varExpectedNbCopies-inNbExistingSegments));
    -- copies are missing, try to recreate them
    WHILE varExpectedNbCopies > inNbExistingSegments + varCreatedMJs AND varNextCopyNb <= varExpectedNbCopies LOOP
      BEGIN
        -- check whether varNextCopyNb is already in use by a valid copy
        SELECT * INTO varNb FROM TABLE(inAllValidCopyNbs) WHERE COLUMN_VALUE=varNextCopyNb;
        -- this copy number is in use, go to next one
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- copy number is not in use, create a migrationJob using it (may throw exceptions)
        initMigration(inCfId, inFileSize, NULL, NULL, varNextCopyNb, tconst.MIGRATIONJOB_WAITINGONRECALL);
        varCreatedMJs := varCreatedMJs + 1;
        -- log "create new MigrationJob to migrate missing copy"
        logToDLF(NULL, dlf.LVL_SYSTEM, dlf.RECALL_MJ_FOR_MISSING_COPY, inFileId, inNsHost, 'stagerd',
                 inLogParams || ' copyNb=' || TO_CHAR(varNextCopyNb));
      END;
      varNextCopyNb := varNextCopyNb + 1;
    END LOOP;
    -- Did we create new copies ?
    IF varExpectedNbCopies > inNbExistingSegments + varCreatedMJs THEN
      -- We did not create enough new copies, this means that we did not find enough
      -- valid copy numbers. Odd... Log something !
      logToDLF(NULL, dlf.LVL_ERROR, dlf.RECALL_COPY_STILL_MISSING, inFileId, inNsHost, 'stagerd',
               inLogParams || ' nbCopiesStillMissing=' ||
               TO_CHAR(varExpectedNbCopies - inAllValidCopyNbs.COUNT - varCreatedMJs));
    ELSE
      -- Yes, then create migrated segments for the existing segments if there are none
      SELECT count(*) INTO varNb FROM MigratedSegment WHERE castorFile = inCfId;
      IF varNb = 0 THEN
        FOR i IN 1..inAllValidCopyNbs.COUNT LOOP
          INSERT INTO MigratedSegment (castorFile, copyNb, VID)
          VALUES (inCfId, inAllValidCopyNbs(i), inAllValidVIDs(i));
        END LOOP;
      END IF;
    END IF;
  END IF;
END;
/

/* PL/SQL method to either force GC of the given diskCopies or delete them when the physical files behind have been lost */
CREATE OR REPLACE PROCEDURE deleteDiskCopies(inDcIds IN castor."cnumList", inFileIds IN castor."cnumList",
                                             inForce IN BOOLEAN, inDryRun IN BOOLEAN,
                                             outRes OUT castor.DiskCopyResult_Cur, outDiskPool OUT VARCHAR2) AS
  varNsHost VARCHAR2(100);
  varFileName VARCHAR2(2048);
  varCfId INTEGER;
  varNbRemaining INTEGER;
  varStatus INTEGER;
  varLogParams VARCHAR2(2048);
  varFileSize INTEGER;
BEGIN
  DELETE FROM DeleteDiskCopyHelper;
  -- Insert all data in bulk for efficiency reasons
  FORALL i IN 1..inFileIds.COUNT
    INSERT INTO DeleteDiskCopyHelper (dcId, fileId, fStatus, rc)
    VALUES (inDcIds(i), inFileIds(i), 'd', -1);
  -- And now gather all remote Nameserver statuses. This could not be
  -- incorporated in the previous query, because Oracle would give:
  -- PLS-00739: FORALL INSERT/UPDATE/DELETE not supported on remote tables.
  -- Note that files that are not found in the Nameserver remain with fStatus = 'd',
  -- which means they can be safely deleted: we're anticipating the NS synch.
  UPDATE DeleteDiskCopyHelper
     SET fStatus = '-'
   WHERE EXISTS (SELECT 1 FROM Cns_file_metadata@RemoteNS F
                  WHERE status = '-' AND F.fileId IN
                    (SELECT fileId FROM DeleteDiskCopyHelper));
  UPDATE DeleteDiskCopyHelper
     SET fStatus = 'm'
   WHERE EXISTS (SELECT 1 FROM Cns_file_metadata@RemoteNS F
                  WHERE status = 'm' AND F.fileId IN
                    (SELECT fileId FROM DeleteDiskCopyHelper));
  -- A better and more generic implementation would have been:
  -- UPDATE DeleteDiskCopyHelper H
  --    SET fStatus = nvl((SELECT F.status
  --                         FROM Cns_file_metadata@RemoteNS F
  --                        WHERE F.fileId = H.fileId), 'd');
  -- Unfortunately, that one is much less efficient as Oracle does not use
  -- the DB link in bulk, therefore making the query extremely slow (several mins)
  -- when handling large numbers of files (e.g. an entire mount point).
  COMMIT;
  FOR dc IN (SELECT dcId, fileId, fStatus FROM DeleteDiskCopyHelper) LOOP
    BEGIN
      -- get data and lock
      SELECT castorFile, DiskCopy.status, DiskCopy.diskCopySize, DiskPool.name
        INTO varCfId, varStatus, varFileSize, outDiskPool
        FROM DiskPool, FileSystem, DiskCopy
       WHERE DiskCopy.id = dc.dcId
         AND DiskCopy.fileSystem = FileSystem.id
         AND FileSystem.diskPool = DiskPool.id;
      SELECT nsHost, lastKnownFileName
        INTO varNsHost, varFileName
        FROM CastorFile
       WHERE id = varCfId
         AND fileId = dc.fileId
         FOR UPDATE;
      varLogParams := 'FileName="' || varFileName ||'" DiskPool="'|| outDiskPool
        ||'" fileSize='|| varFileSize ||' dcId='|| dc.dcId ||' status='
        || getObjStatusName('DiskCopy', 'status', varStatus);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- diskcopy not found in stager
      UPDATE DeleteDiskCopyHelper
         SET rc = dconst.DELDC_ENOENT
       WHERE dcId = dc.dcId;
      COMMIT;
      CONTINUE;
    END;
    -- count remaining ones
    SELECT count(*) INTO varNbRemaining FROM DiskCopy
     WHERE castorFile = varCfId
       AND status = dconst.DISKCOPY_VALID
       AND id != dc.dcId;
    -- and update their importance if needed (other copy exists and dropped one was valid)
    IF varNbRemaining > 0 AND varStatus = dconst.DISKCOPY_VALID AND (NOT inDryRun) THEN
      UPDATE DiskCopy SET importance = importance + 1
       WHERE castorFile = varCfId
         AND status = dconst.DISKCOPY_VALID;
    END IF;
    IF inForce THEN
      -- the physical diskcopy is deemed lost: delete the diskcopy entry
      -- and potentially drop dangling entities
      IF NOT inDryRun THEN
        DELETE FROM DiskCopy WHERE id = dc.dcId;
      END IF;
      IF varStatus = dconst.DISKCOPY_STAGEOUT THEN
        -- fail outstanding requests
        UPDATE SubRequest
           SET status = dconst.SUBREQUEST_FAILED,
               errorCode = serrno.SEINTERNAL,
               errorMessage = 'File got lost while being written to'
         WHERE diskCopy = dc.dcId
           AND status = dconst.SUBREQUEST_READY;
      END IF;
      -- was it the last active one?
      IF varNbRemaining = 0 THEN
        IF NOT inDryRun THEN
          -- yes, drop the (now bound to fail) migration job(s)
          deleteMigrationJobs(varCfId);
        END IF;
        -- check if the entire castorFile chain can be dropped
        IF NOT inDryRun THEN
          deleteCastorFile(varCfId);
        END IF;
        IF dc.fStatus = 'm' THEN
          -- file is on tape: let's recall it. This may potentially trigger a new migration
          UPDATE DeleteDiskCopyHelper
             SET rc = dconst.DELDC_RECALL
           WHERE dcId = dc.dcId;
          IF NOT inDryRun THEN
            logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_RECALL, dc.fileId, varNsHost, 'stagerd', varLogParams);
          END IF;
        ELSIF dc.fStatus = 'd' THEN
          -- file was dropped, report as if we have run a standard GC
          UPDATE DeleteDiskCopyHelper
             SET rc = dconst.DELDC_GC
           WHERE dcId = dc.dcId;
          IF NOT inDryRun THEN
            logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_GC, dc.fileId, varNsHost, 'stagerd', varLogParams);
          END IF;
        ELSE
          -- file is really lost, we'll remove the namespace entry afterwards
          UPDATE DeleteDiskCopyHelper
             SET rc = dconst.DELDC_LOST
           WHERE dcId = dc.dcId;
          IF NOT inDryRun THEN
            logToDLF(NULL, dlf.LVL_WARNING, dlf.DELETEDISKCOPY_LOST, dc.fileId, varNsHost, 'stagerd', varLogParams);
          END IF;
        END IF;
      ELSE
        -- it was not the last valid copy, replicate from another one
          UPDATE DeleteDiskCopyHelper
             SET rc = dconst.DELDC_REPLICATION
           WHERE dcId = dc.dcId;
        IF NOT inDryRun THEN
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_REPLICATION, dc.fileId, varNsHost, 'stagerd', varLogParams);
        END IF;
      END IF;
    ELSE
      -- similarly to stageRm, check that the deletion is allowed:
      -- basically only files on tape may be dropped in case no data loss is provoked,
      -- or files already dropped from the namespace. The rest is forbidden.
      IF (varStatus IN (dconst.DISKCOPY_VALID, dconst.DISKCOPY_FAILED) AND (varNbRemaining > 0 OR dc.fStatus = 'm' OR varFileSize = 0))
         OR dc.fStatus = 'd' THEN
        UPDATE DeleteDiskCopyHelper
           SET rc = dconst.DELDC_GC
         WHERE dcId = dc.dcId;
        IF NOT inDryRun THEN
          IF varStatus = dconst.DISKCOPY_VALID THEN
            UPDATE DiskCopy
               SET status = dconst.DISKCOPY_INVALID, gcType = dconst.GCTYPE_ADMIN
             WHERE id = dc.dcId;
          ELSE
            DELETE FROM DiskCopy WHERE ID = dc.dcId;
          END IF;
          -- do not forget to cancel pending migrations in case we've lost that last DiskCopy
          IF varNbRemaining = 0 THEN
            deleteMigrationJobs(varCfId);
          END IF;
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_GC, dc.fileId, varNsHost, 'stagerd', varLogParams);
        END IF;
      ELSE
        -- nothing is done, just record no-action
        UPDATE DeleteDiskCopyHelper
           SET rc = dconst.DELDC_NOOP
         WHERE dcId = dc.dcId;
        IF NOT inDryRun THEN
          logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DELETEDISKCOPY_NOOP, dc.fileId, varNsHost, 'stagerd', varLogParams);
        END IF;
        COMMIT;
        CONTINUE;
      END IF;
    END IF;
    COMMIT;   -- release locks file by file
  END LOOP;
  -- return back all results for the python script to post-process them,
  -- including performing all required actions
  OPEN outRes FOR
    SELECT dcId, fileId, rc FROM DeleteDiskCopyHelper;
END;
/

/* attach the tapes to the migration mounts  */
CREATE OR REPLACE
PROCEDURE tg_attachTapesToMigMounts (
  inStartFseqs IN castor."cnumList",
  inMountIds   IN castor."cnumList",
  inTapeVids   IN castor."strList") AS
BEGIN
  -- Sanity check
  IF (inStartFseqs.COUNT != inTapeVids.COUNT) THEN
    RAISE_APPLICATION_ERROR (-20119,
       'Size mismatch for arrays: inStartFseqs.COUNT='||inStartFseqs.COUNT
       ||' inTapeVids.COUNT='||inTapeVids.COUNT);
  END IF;
  FORALL i IN 1..inMountIds.COUNT
    UPDATE MigrationMount
       SET VID = inTapeVids(i),
           lastFseq = inStartFseqs(i),
           startTime = getTime(),
           status = tconst.MIGRATIONMOUNT_SEND_TO_VDQM
     WHERE id = inMountIds(i);
  COMMIT;
END;
/

/* restart taperequest which had problems */
CREATE OR REPLACE
PROCEDURE tg_restartLostReqs(inMountTransactionIds IN castor."cnumList") AS
BEGIN
 FOR i IN 1..inMountTransactionIds.COUNT LOOP
   tg_endTapeSession(inMountTransactionIds(i), 0);
 END LOOP;
 COMMIT;
END;
/

/* Commit a set of succeeded/failed migration processes to the NS and stager db.
 * Locks are taken on the involved castorfiles one by one, then to the dependent entities.
 */
CREATE OR REPLACE PROCEDURE tg_setBulkFileMigrationResult(inLogContext IN VARCHAR2,
                                                          inMountTrId IN NUMBER,
                                                          inFileIds IN "numList",
                                                          inFileTrIds IN "numList",
                                                          inFseqs IN "numList",
                                                          inBlockIds IN strListTable,
                                                          inChecksumTypes IN strListTable,
                                                          inChecksums IN "numList",
                                                          inComprSizes IN "numList",
                                                          inTransferredSizes IN "numList",
                                                          inErrorCodes IN "numList",
                                                          inErrorMsgs IN strListTable
                                                          ) AS
  varStartTime TIMESTAMP;
  varNsHost VARCHAR2(2048);
  varReqId VARCHAR2(36);
  varNsOpenTime NUMBER;
  varCopyNo NUMBER;
  varOldCopyNo NUMBER;
  varVid VARCHAR2(10);
  varNSIsOnlyLogs "numList";
  varNSTimeInfos floatList;
  varNSErrorCodes "numList";
  varNSMsgs strListTable;
  varNSFileIds "numList" := "numList"();
  varNSParams strListTable;
  varParams VARCHAR2(4000);
  varNbSentToNS INTEGER := 0;
  -- for the compatibility mode, to be dropped in 2.1.15
  varOpenMode CHAR(1);
  varLastUpdateTime INTEGER;
BEGIN
  varStartTime := SYSTIMESTAMP;
  varReqId := uuidGen();
  -- Get the NS host name
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Get the NS open mode: if compatibility, then CF.lastUpdTime is to be used for the
  -- concurrent modifications check like in 2.1.13, else the new CF.nsOpenTime column is used.
  varOpenMode := getConfigOption@RemoteNS('stager', 'openmode', NULL);
  FOR i IN 1..inFileTrIds.COUNT LOOP
    BEGIN
      -- Collect additional data. Note that this is NOT bulk
      -- to preserve the order in the input arrays.
      SELECT CF.nsOpenTime, CF.lastUpdateTime, nvl(MJ.originalCopyNb, 0), MJ.vid, MJ.destCopyNb
        INTO varNsOpenTime, varLastUpdateTime, varOldCopyNo, varVid, varCopyNo
        FROM CastorFile CF, MigrationJob MJ
       WHERE MJ.castorFile = CF.id
         AND CF.fileid = inFileIds(i)
         AND MJ.mountTransactionId = inMountTrId
         AND MJ.fileTransactionId = inFileTrIds(i)
         AND status = tconst.MIGRATIONJOB_SELECTED;
      -- Use the correct timestamp in case of compatibility mode
      IF varOpenMode = 'C' THEN
        varNsOpenTime := varLastUpdateTime;
      END IF;
        -- Store in a temporary table, to be transfered to the NS DB
      IF inErrorCodes(i) = 0 THEN
        -- Successful migration
        INSERT INTO FileMigrationResultsHelper
          (reqId, fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
           vid, fseq, blockId, checksumType, checksum)
        VALUES (varReqId, inFileIds(i), varNsOpenTime, varCopyNo, varOldCopyNo,
                inTransferredSizes(i), CASE inComprSizes(i) WHEN 0 THEN 1 ELSE inComprSizes(i) END, varVid, inFseqs(i),
                strtoRaw4(inBlockIds(i)), inChecksumTypes(i), inChecksums(i));
        varNbSentToNS := varNbSentToNS + 1;
      ELSE
        -- Fail/retry this migration, log 'migration failed, will retry if allowed'
        varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' ErrorCode='|| to_char(inErrorCodes(i))
                     ||' ErrorMessage="'|| inErrorMsgs(i) ||'" TPVID='|| varVid
                     ||' copyNb='|| to_char(varCopyNo) ||' '|| inLogContext;
        logToDLF(varReqid, dlf.LVL_WARNING, dlf.MIGRATION_RETRY, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
        retryOrFailMigration(inMountTrId, inFileIds(i), varNsHost, inErrorCodes(i), varReqId);
        -- here we commit immediately because retryOrFailMigration took a lock on the CastorFile
        COMMIT;
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Log 'unable to identify migration, giving up'
      varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' fileTransactionId='|| to_char(inFileTrIds(i))
        ||' '|| inLogContext;
      logToDLF(varReqid, dlf.LVL_ERROR, dlf.MIGRATION_NOT_FOUND, inFileIds(i), varNsHost, 'tapegatewayd', varParams);
    END;
  END LOOP;
  -- Commit all the entries in FileMigrationResultsHelper so that the next call can take them
  COMMIT;

  DECLARE
    varUnused INTEGER;
  BEGIN
    -- boundary case: if nothing to do, just skip the remote call and the
    -- subsequent FOR loop as it would be useless (and would fail).
    SELECT 1 INTO varUnused FROM FileMigrationResultsHelper
     WHERE reqId = varReqId AND ROWNUM < 2;
    -- The following procedure wraps the remote calls in an autonomous transaction
    ns_setOrReplaceSegments(varReqId, varNSIsOnlyLogs, varNSTimeInfos, varNSErrorCodes, varNSMsgs, varNSFileIds, varNSParams);

    -- Process the results
    FOR i IN 1 .. varNSFileIds.COUNT LOOP
      -- First log on behalf of the NS
      -- We classify the log level based on the error code here.
      -- Current error codes are:
      --   ENOENT, EACCES, EBUSY, EEXIST, EISDIR, EINVAL, SEINTERNAL, SECHECKSUM, ENSFILECHG, ENSNOSEG
      --   ENSTOOMANYSEGS, ENSOVERWHENREP, ERTWRONGSIZE, ESTNOSEGFOUND
      -- default level is ERROR. Some cases can be demoted to warning when it's a normal case
      -- (like file deleted by user in the mean time).
      logToDLFWithTime(varNSTimeinfos(i), varReqid,
                       CASE varNSErrorCodes(i) 
                         WHEN 0                 THEN dlf.LVL_SYSTEM
                         WHEN serrno.ENOENT     THEN dlf.LVL_WARNING
                         WHEN serrno.ENSFILECHG THEN dlf.LVL_WARNING
                         ELSE                        dlf.LVL_ERROR
                       END,
                       varNSMsgs(i), varNSFileIds(i), varNsHost, 'nsd', varNSParams(i));
      -- Now skip pure log entries and process file by file, depending on the result
      IF varNSIsOnlyLogs(i) = 1 THEN CONTINUE; END IF;
      CASE
      WHEN varNSErrorCodes(i) = 0 THEN
        -- All right, commit the migration in the stager
        tg_setFileMigrated(inMountTrId, varNSFileIds(i), varReqId, inLogContext);

      WHEN varNSErrorCodes(i) = serrno.ENOENT
        OR varNSErrorCodes(i) = serrno.ENSNOSEG
        OR varNSErrorCodes(i) = serrno.ENSFILECHG
        OR varNSErrorCodes(i) = serrno.ENSTOOMANYSEGS THEN
        -- The migration was useless because either the file is gone, or it has been modified elsewhere,
        -- or there were already enough copies on tape for it. Fail and update disk cache accordingly.
        failFileMigration(inMountTrId, varNSFileIds(i), varNSErrorCodes(i), varReqId);

      ELSE
        -- Attempt to retry for all other NS errors. To be reviewed whether some of the NS errors are to be considered fatal.
        varParams := 'mountTransactionId='|| to_char(inMountTrId) ||' '|| varNSParams(i) ||' '|| inLogContext;
        logToDLF(varReqid, dlf.LVL_WARNING, dlf.MIGRATION_RETRY, varNSFileIds(i), varNsHost, 'tapegatewayd', varParams);
        retryOrFailMigration(inMountTrId, varNSFileIds(i), varNsHost, varNSErrorCodes(i), varReqId);
      END CASE;
      -- Commit file by file
      COMMIT;
    END LOOP;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Nothing to do after processing the error cases
    NULL;
  END;
  -- Final log, "setBulkFileMigrationResult: bulk migration completed"
  varParams := 'mountTransactionId='|| to_char(inMountTrId)
               ||' NbInputFiles='|| inFileIds.COUNT
               ||' NbSentToNS='|| varNbSentToNS
               ||' NbFilesBackFromNS='|| varNSFileIds.COUNT
               ||' '|| inLogContext
               ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
               ||' AvgProcessingTime='|| trunc(getSecs(varStartTime, SYSTIMESTAMP)/inFileIds.COUNT, 6);
  logToDLF(varReqid, dlf.LVL_SYSTEM, dlf.BULK_MIGRATION_COMPLETED, 0, '', 'tapegatewayd', varParams);
END;
/

/* Commit a set of succeeded/failed recall processes to the NS and stager db.
 * Locks are taken on the involved castorfiles one by one, then to the dependent entities.
 */
CREATE OR REPLACE PROCEDURE tg_setBulkFileRecallResult(inLogContext IN VARCHAR2,
                                                       inMountTrId IN NUMBER,
                                                       inFileIds IN "numList",
                                                       inFileTrIds IN "numList",
                                                       inFilePaths IN strListTable,
                                                       inFseqs IN "numList",
                                                       inChecksumNames IN strListTable,
                                                       inChecksums IN "numList",
                                                       inFileSizes IN "numList",
                                                       inErrorCodes IN "numList",
                                                       inErrorMsgs IN strListTable) AS
  varCfId NUMBER;
  varVID VARCHAR2(10);
  varReqId VARCHAR2(36);
  varStartTime TIMESTAMP;
  varNsHost VARCHAR2(2048);
  varParams VARCHAR2(4000);
BEGIN
  varStartTime := SYSTIMESTAMP;
  varReqId := uuidGen();
  -- Get the NS host name
  varNsHost := getConfigOption('stager', 'nsHost', '');
  -- Get the current VID
  SELECT VID INTO varVID
    FROM RecallMount
   WHERE mountTransactionId = inMountTrId;
  -- Loop over the input
  FOR i IN 1..inFileIds.COUNT LOOP
    BEGIN
      -- Find and lock related castorFile
      SELECT id INTO varCfId
        FROM CastorFile
       WHERE fileid = inFileIds(i)
         AND nsHost = varNsHost
         FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- log "Unable to identify Recall. giving up"
      logToDLF(varReqId, dlf.LVL_ERROR, dlf.RECALL_NOT_FOUND, inFileIds(i), varNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTrId) || ' TPVID=' || varVID ||
               ' fseq=' || TO_CHAR(inFseqs(i)) || ' filePath=' || inFilePaths(i) || ' ' || inLogContext);
      CONTINUE;
    END;
    -- Now deal with each recall one by one
    IF inErrorCodes(i) = 0 THEN
      -- Recall successful, check NS and update stager + log
      tg_setFileRecalled(inMountTrId, inFseqs(i), inFilePaths(i), inChecksumNames(i), inChecksums(i),
                         varReqId, inLogContext);
    ELSE
      -- Recall failed at tapeserver level, attempt to retry it
      -- log "setBulkFileRecallResult : recall process failed, will retry if allowed"
      logToDLF(varReqId, dlf.LVL_WARNING, dlf.RECALL_FAILED, inFileIds(i), varNsHost, 'tapegatewayd',
               'mountTransactionId=' || TO_CHAR(inMountTrId) || ' TPVID=' || varVID ||
               ' fseq=' || TO_CHAR(inFseqs(i)) || ' errorMessage="' || inErrorMsgs(i) ||'" '|| inLogContext);
      retryOrFailRecall(varCfId, varVID, varReqId, inLogContext);
    END IF;
    COMMIT;
  END LOOP;
  -- log "setBulkFileRecallResult: bulk recall completed"
  varParams := 'mountTransactionId='|| to_char(inMountTrId)
               ||' NbFiles='|| inFileIds.COUNT ||' '|| inLogContext
               ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
               ||' AvgProcessingTime='|| trunc(getSecs(varStartTime, SYSTIMESTAMP)/inFileIds.COUNT, 6);
  logToDLF(varReqid, dlf.LVL_SYSTEM, dlf.BULK_RECALL_COMPLETED, 0, '', 'tapegatewayd', varParams);
END;
/


/* revalidate all code */
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_14_15_2';
COMMIT;
