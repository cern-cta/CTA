/******************************************************************************
 *              2.1.4_to_2.1.5-0.sql
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
 * @(#)$RCSfile: 2.1.4_to_2.1.5-0.sql,v $ $Release: 1.2 $ $Release$ $Date: 2007/10/24 17:04:51 $ $Author: itglp $
 *
 * This script upgrades a CASTOR v2.1.4 database into v2.1.5-0
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_4%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;

UPDATE CastorVersion SET release = '2_1_5_0';   -- no change to the schemaVersion at the moment
COMMIT;

/* New code for the new stager only */

CREATE TABLE Type2Obj (type INTEGER PRIMARY KEY, object VARCHAR2(100), svcHandler VARCHAR2(100));
INSERT INTO Type2Obj (type, object) VALUES (1, 'Ptr');
INSERT INTO Type2Obj (type, object) VALUES (2, 'CastorFile');
INSERT INTO Type2Obj (type, object) VALUES (3, 'OldClient');
INSERT INTO Type2Obj (type, object) VALUES (4, 'Cuuid');
INSERT INTO Type2Obj (type, object) VALUES (5, 'DiskCopy');
INSERT INTO Type2Obj (type, object) VALUES (6, 'DiskFile');
INSERT INTO Type2Obj (type, object) VALUES (7, 'DiskPool');
INSERT INTO Type2Obj (type, object) VALUES (8, 'DiskServer');
INSERT INTO Type2Obj (type, object) VALUES (10, 'FileClass');
INSERT INTO Type2Obj (type, object) VALUES (12, 'FileSystem');
INSERT INTO Type2Obj (type, object) VALUES (13, 'IClient');
INSERT INTO Type2Obj (type, object) VALUES (14, 'MessageAck');
INSERT INTO Type2Obj (type, object) VALUES (16, 'ReqIdRequest');
INSERT INTO Type2Obj (type, object) VALUES (17, 'Request');
INSERT INTO Type2Obj (type, object) VALUES (18, 'Segment');
INSERT INTO Type2Obj (type, object) VALUES (21, 'StageGetNextRequest');
INSERT INTO Type2Obj (type, object) VALUES (26, 'Stream');
INSERT INTO Type2Obj (type, object) VALUES (27, 'SubRequest');
INSERT INTO Type2Obj (type, object) VALUES (28, 'SvcClass');
INSERT INTO Type2Obj (type, object) VALUES (29, 'Tape');
INSERT INTO Type2Obj (type, object) VALUES (30, 'TapeCopy');
INSERT INTO Type2Obj (type, object) VALUES (31, 'TapePool');
INSERT INTO Type2Obj (type, object) VALUES (33, 'StageFileQueryRequest');
INSERT INTO Type2Obj (type, object) VALUES (34, 'StageFindRequestRequest');
INSERT INTO Type2Obj (type, object) VALUES (35, 'StageGetRequest');
INSERT INTO Type2Obj (type, object) VALUES (36, 'StagePrepareToGetRequest');
INSERT INTO Type2Obj (type, object) VALUES (37, 'StagePrepareToPutRequest');
INSERT INTO Type2Obj (type, object) VALUES (38, 'StagePrepareToUpdateRequest');
INSERT INTO Type2Obj (type, object) VALUES (39, 'StagePutDoneRequest');
INSERT INTO Type2Obj (type, object) VALUES (40, 'StagePutRequest');
INSERT INTO Type2Obj (type, object) VALUES (41, 'StageRequestQueryRequest');
INSERT INTO Type2Obj (type, object) VALUES (42, 'StageRmRequest');
INSERT INTO Type2Obj (type, object) VALUES (43, 'StageUpdateFileStatusRequest');
INSERT INTO Type2Obj (type, object) VALUES (44, 'StageUpdateRequest');
INSERT INTO Type2Obj (type, object) VALUES (45, 'FileRequest');
INSERT INTO Type2Obj (type, object) VALUES (46, 'QryRequest');
INSERT INTO Type2Obj (type, object) VALUES (48, 'StagePutNextRequest');
INSERT INTO Type2Obj (type, object) VALUES (49, 'StageUpdateNextRequest');
INSERT INTO Type2Obj (type, object) VALUES (50, 'StageAbortRequest');
INSERT INTO Type2Obj (type, object) VALUES (51, 'StageReleaseFilesRequest');
INSERT INTO Type2Obj (type, object) VALUES (58, 'DiskCopyForRecall');
INSERT INTO Type2Obj (type, object) VALUES (59, 'TapeCopyForMigration');
INSERT INTO Type2Obj (type, object) VALUES (60, 'GetUpdateStartRequest');
INSERT INTO Type2Obj (type, object) VALUES (62, 'BaseAddress');
INSERT INTO Type2Obj (type, object) VALUES (64, 'Disk2DiskCopyDoneRequest');
INSERT INTO Type2Obj (type, object) VALUES (65, 'MoverCloseRequest');
INSERT INTO Type2Obj (type, object) VALUES (66, 'StartRequest');
INSERT INTO Type2Obj (type, object) VALUES (67, 'PutStartRequest');
INSERT INTO Type2Obj (type, object) VALUES (69, 'IObject');
INSERT INTO Type2Obj (type, object) VALUES (70, 'IAddress');
INSERT INTO Type2Obj (type, object) VALUES (71, 'QueryParameter');
INSERT INTO Type2Obj (type, object) VALUES (72, 'DiskCopyInfo');
INSERT INTO Type2Obj (type, object) VALUES (73, 'Files2Delete');
INSERT INTO Type2Obj (type, object) VALUES (74, 'FilesDeleted');
INSERT INTO Type2Obj (type, object) VALUES (76, 'GCLocalFile');
INSERT INTO Type2Obj (type, object) VALUES (78, 'GetUpdateDone');
INSERT INTO Type2Obj (type, object) VALUES (79, 'GetUpdateFailed');
INSERT INTO Type2Obj (type, object) VALUES (80, 'PutFailed');
INSERT INTO Type2Obj (type, object) VALUES (81, 'GCFile');
INSERT INTO Type2Obj (type, object) VALUES (82, 'GCFileList');
INSERT INTO Type2Obj (type, object) VALUES (83, 'FilesDeletionFailed');
INSERT INTO Type2Obj (type, object) VALUES (93, 'PutDoneStart');
INSERT INTO Type2Obj (type, object) VALUES (95, 'SetFileGCWeight');
INSERT INTO Type2Obj (type, object) VALUES (101, 'DiskServerDescription');
INSERT INTO Type2Obj (type, object) VALUES (102, 'FileSystemDescription');
INSERT INTO Type2Obj (type, object) VALUES (103, 'DiskPoolQuery');
INSERT INTO Type2Obj (type, object) VALUES (104, 'EndResponse');
INSERT INTO Type2Obj (type, object) VALUES (105, 'FileResponse');
INSERT INTO Type2Obj (type, object) VALUES (106, 'StringResponse');
INSERT INTO Type2Obj (type, object) VALUES (107, 'Response');
INSERT INTO Type2Obj (type, object) VALUES (108, 'IOResponse');
INSERT INTO Type2Obj (type, object) VALUES (109, 'AbortResponse');
INSERT INTO Type2Obj (type, object) VALUES (110, 'RequestQueryResponse');
INSERT INTO Type2Obj (type, object) VALUES (111, 'FileQueryResponse');
INSERT INTO Type2Obj (type, object) VALUES (112, 'FindReqResponse');
INSERT INTO Type2Obj (type, object) VALUES (113, 'GetUpdateStartResponse');
INSERT INTO Type2Obj (type, object) VALUES (114, 'BasicResponse');
INSERT INTO Type2Obj (type, object) VALUES (115, 'StartResponse');
INSERT INTO Type2Obj (type, object) VALUES (116, 'GCFilesResponse');
INSERT INTO Type2Obj (type, object) VALUES (117, 'FileQryResponse');
INSERT INTO Type2Obj (type, object) VALUES (118, 'DiskPoolQueryResponse');
INSERT INTO Type2Obj (type, object) VALUES (119, 'StageRepackRequest');
INSERT INTO Type2Obj (type, object) VALUES (120, 'DiskServerStateReport');
INSERT INTO Type2Obj (type, object) VALUES (121, 'DiskServerMetricsReport');
INSERT INTO Type2Obj (type, object) VALUES (122, 'FileSystemStateReport');
INSERT INTO Type2Obj (type, object) VALUES (123, 'FileSystemMetricsReport');
INSERT INTO Type2Obj (type, object) VALUES (124, 'DiskServerAdminReport');
INSERT INTO Type2Obj (type, object) VALUES (125, 'FileSystemAdminReport');
INSERT INTO Type2Obj (type, object) VALUES (126, 'StreamReport');
INSERT INTO Type2Obj (type, object) VALUES (127, 'FileSystemStateAck');
INSERT INTO Type2Obj (type, object) VALUES (128, 'MonitorMessageAck');
INSERT INTO Type2Obj (type, object) VALUES (129, 'Client');
INSERT INTO Type2Obj (type, object) VALUES (130, 'JobSubmissionRequest');
INSERT INTO Type2Obj (type, object) VALUES (131, 'VersionQuery');
INSERT INTO Type2Obj (type, object) VALUES (132, 'VersionResponse');
INSERT INTO Type2Obj (type, object) VALUES (135, 'StageDiskCopyReplicaRequest');


/* define the service handlers for the appropriate sets of stage request objects */
UPDATE Type2Obj SET svcHandler = 'JobReqSvc' WHERE type in (35, 40, 44);
UPDATE Type2Obj SET svcHandler = 'PrepReqSvc' WHERE type in (36, 37, 38, 119);
UPDATE Type2Obj SET svcHandler = 'StageReqSvc' WHERE type in (39, 42, 95);
UPDATE Type2Obj SET svcHandler = 'QueryReqSvc' WHERE type in (33, 34, 41, 103, 131);
UPDATE Type2Obj SET svcHandler = 'JobSvc' WHERE type in (60, 64, 65, 67, 78, 79, 80, 93);
UPDATE Type2Obj SET svcHandler = 'GCSvc' WHERE type in (73, 74, 83);
COMMIT;

/* PL/SQL method to get the next SubRequest to do according to the given service */
CREATE OR REPLACE PROCEDURE subRequestToDo(service IN VARCHAR2,
                                           srId OUT INTEGER, srRetryCounter OUT INTEGER, srFileName OUT VARCHAR2,
                                           srProtocol OUT VARCHAR2, srXsize OUT INTEGER, srPriority OUT INTEGER,
                                           srStatus OUT INTEGER, srModeBits OUT INTEGER, srFlags OUT INTEGER,
                                           srSubReqId OUT VARCHAR2) AS
  firstRow VARCHAR2(18);
  CURSOR c IS
    SELECT /*+ USE_NL */ rowidtochar(rowid) FROM SubRequest
     WHERE status in (0,1,2)    -- START, RESTART, RETRY
       AND EXISTS
         (SELECT /*+ index(a I_Id2Type_id) */ 'x'
            FROM Id2Type a
           WHERE a.id = SubRequest.request
             AND a.type = Type2Obj.type
             AND Type2Obj.svcHandler = service);
BEGIN
  OPEN c;
  FETCH c INTO firstRow;
  UPDATE subrequest SET status = 3 WHERE rowid = CHARTOROWID(firstRow)   -- SUBREQUEST_WAITSCHED
    RETURNING id, retryCounter, fileName, protocol, xsize, priority, status, modeBits, flags, subReqId
    INTO srId, srRetryCounter, srFileName, srProtocol, srXsize, srPriority, srStatus, srModeBits, srFlags, srSubReqId;
  CLOSE c;
END;

/* PL/SQL method implementing checkForD2DCopyOrRecall */
/* Internally used by getDiskCopiesForJob and getDiskCopiesForPrepReq */
CREATE OR REPLACE PROCEDURE checkForD2DCopyOrRecall
                            (cfId IN NUMBER, srId IN NUMBER,
                             svcClassId IN NUMBER, result OUT NUMBER) AS
  unused NUMBER;
BEGIN
  -- First check whether we are a disk only pool that is already full.
  -- In such a case, we should fail the request with an ENOSPACE error
  IF (checkFailJobsWhenNoSpace(svcClassId) = 1) THEN
    result := -1;
    UPDATE SubRequest
       SET status = 7, -- FAILED
           errorCode = 28, -- ENOSPC
           errorMessage = 'File creation canceled since diskPool is full'
     WHERE id = srId;
    RETURN;
  END IF;
  -- Check whether there are any diskcopies available for a disk2disk copy
  SELECT DiskCopy.id INTO unused 
    FROM DiskCopy, FileSystem, DiskServer
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.status IN (0, 10) -- STAGED, CANBEMIGR
     AND FileSystem.id = DiskCopy.fileSystem
     AND FileSystem.status IN (0, 1) -- PRODUCTION, DRAINING
     AND DiskServer.id = FileSystem.diskserver
     AND DiskServer.status IN (0, 1) -- PRODUCTION, DRAINING
     AND ROWNUM < 2;
  -- We found at least one, therefore we schedule a disk2disk
  -- copy from the existing diskcopy not available to this svcclass
  result := 1;   -- DISKCOPY_WAITDISK2DISKCOPY
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- We found no diskcopies at all. We should not schedule
  -- and make a tape recall... except ... in 2 cases :
  --   - if there is some temporarily unavailable diskcopy
  --     that is in CANBEMIGR or STAGEOUT
  -- in such a case, what we have is an existing file, that
  -- was migrated, then overwritten but never migrated again.
  -- So the unavailable diskCopy is the only copy that is valid.
  -- We will tell the client that the file is unavailable
  -- and he/she will retry later
  --   - if we have an available STAGEOUT copy. This can happen
  -- when the copy is in a given svcclass and we were looking
  -- in another one. Since disk to disk copy is impossible in this
  -- case, the file is declared BUSY.
  --   - if we have an available WAITFS, WAITFSSCHEDULING copy in such
  -- a case, we tell the client that the file is BUSY
  DECLARE
    dcStatus NUMBER;
    fsStatus NUMBER;
    dsStatus NUMBER;
  BEGIN
    SELECT DiskCopy.status, nvl(FileSystem.status, 0), nvl(DiskServer.status, 0)
      INTO dcStatus, fsStatus, dsStatus
      FROM DiskCopy, FileSystem, DiskServer
     WHERE DiskCopy.castorfile = cfId
       AND DiskCopy.status IN (5, 6, 10, 11) -- WAITFS, STAGEOUT, CANBEMIGR, WAITFSSCHEDULING
       AND FileSystem.id(+) = DiskCopy.fileSystem
       AND DiskServer.id(+) = FileSystem.diskserver
       AND ROWNUM < 2;
    -- We are in one of the special cases. Don't schedule, don't recall
    result := -1;
    UPDATE SubRequest
       SET status = 7, -- FAILED
           errorCode = CASE
             WHEN dcStatus IN (5,11) THEN 16 -- WAITFS, WAITFSSCHEDULING, EBUSY
             WHEN dcStatus = 6 AND fsStatus = 0 and dsStatus = 0 THEN 16 -- STAGEOUT, PRODUCTION, PRODUCTION, EBUSY
             ELSE 1718 -- ESTNOTAVAIL
           END,
           errorMessage = CASE
             WHEN dcStatus IN (5, 11) THEN -- WAITFS, WAITFSSCHEDULING
               'File is being (re)created right now by another user'
             WHEN dcStatus = 6 AND fsStatus = 0 and dsStatus = 0 THEN -- STAGEOUT, PRODUCTION, PRODUCTION
               'File is being written to in another SvcClass'
             ELSE
               'All copies of this file are unavailable for now. Please retry later'
           END
     WHERE id = srId;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- We did not find the very special case, go for recall
    result := 2;  -- DISKCOPY_WAITTAPERECALL
  END;
END;

/* PL/SQL method implementing getDiskCopiesForJob */
/* the result output is a DiskCopy status for STAGED, DISK2DISKCOPY or RECALL, or -1 for failure */
CREATE OR REPLACE PROCEDURE getDiskCopiesForJob
        (srId IN INTEGER, result OUT INTEGER,
         sources OUT castor.DiskCopy_Cur) AS
  nbDCs INTEGER;
  dci "numList";
  svcClassId NUMBER;
  reqId NUMBER;
  cfId NUMBER;
BEGIN
  -- retrieve the castorFile, the svcClass and the reqId for this subrequest
  SELECT SubRequest.castorFile, Request.svcClass, Request.id
    INTO cfId, svcClassId, reqId
    FROM (SELECT id, svcClass from StageGetRequest UNION ALL
          SELECT id, svcClass from StageUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = srId;
  -- lock the castorFile to be safe in case of concurrent subrequests
  SELECT id into cfId FROM CastorFile where id = cfId FOR UPDATE;
  
  -- First see whether we should wait on an ongoing request
  -- XXX to be improved: look for StageDiskCopyReplicaRequests instead of WAITDISK2DISKCOPY diskCopies
  SELECT DiskCopy.id BULK COLLECT INTO dci
    FROM DiskCopy, FileSystem, DiskServer
   WHERE cfId = DiskCopy.castorfile
     AND FileSystem.id(+) = DiskCopy.fileSystem
     AND nvl(FileSystem.status, 0) = 0 -- PRODUCTION
     AND DiskServer.id(+) = FileSystem.diskServer
     AND nvl(DiskServer.status, 0) = 0 -- PRODUCTION
     AND DiskCopy.status IN (1, 2); -- WAITDISK2DISKCOPY, WAITTAPERECALL
  IF dci.COUNT > 0 THEN
    -- DiskCopy is in WAIT*, make SubRequest wait on previous subrequest and do not schedule
    makeSubRequestWait(srId, dci(1));
    result := -1;
    RETURN;
  END IF;
     
  -- Look for available diskcopies
  SELECT COUNT(DiskCopy.id) INTO nbDCs
    FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = svcClassId
     AND FileSystem.status = 0 -- PRODUCTION
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.status = 0 -- PRODUCTION
     AND DiskCopy.status IN (0, 6, 10); -- STAGED, STAGEOUT, CANBEMIGR
  IF nbDCs > 0 THEN
    -- Yes, we have some
    result := 0;   -- DISKCOPY_STAGED
    -- List available diskcopies for job scheduling
    OPEN sources
      FOR SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, 0,   -- fs rate does not apply here
                 FileSystem.mountPoint, DiskServer.name
            FROM DiskCopy, SubRequest, FileSystem, DiskServer, DiskPool2SvcClass
           WHERE SubRequest.id = srId
             AND SubRequest.castorfile = DiskCopy.castorfile
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND DiskPool2SvcClass.child = svcClassId
             AND DiskCopy.status IN (0, 6, 10) -- STAGED, STAGEOUT, CANBEMIGR
             AND FileSystem.id = DiskCopy.fileSystem
             AND FileSystem.status = 0 -- PRODUCTION
             AND DiskServer.id = FileSystem.diskServer
             AND DiskServer.status = 0; -- PRODUCTION
  ELSE
    -- No diskcopies available for this service class;
    -- we may have to schedule a disk to disk copy or trigger a recall
    checkForD2DCopyOrRecall(cfId, srId, svcClassId, result);
    --IF result = 1 THEN
       -- create DiskCopyReplicaRequest for our svcclass
       -- make this subrequest wait on it
       -- result := -1;
    --END IF;
  END IF;
END;

/* PL/SQL method implementing getDiskCopiesForPrepReq */
/* the result output is a DiskCopy status for STAGED, DISK2DISKCOPY or RECALL, or -1 for failure */
CREATE OR REPLACE PROCEDURE getDiskCopiesForPrepReq
        (srId IN INTEGER, result OUT INTEGER) AS
  nbDCs INTEGER;
  svcClassId NUMBER;
  reqId NUMBER;
  cfId NUMBER;
BEGIN
  -- retrieve the castorfile, the svcclass and the reqId for this subrequest
  SELECT SubRequest.castorFile, Request.svcClass, Request.id
    INTO cfId, svcClassId, reqId
    FROM (SELECT id, svcClass from StagePrepareToGetRequest UNION ALL
          SELECT id, svcClass from StageRepackRequest UNION ALL
          SELECT id, svcClass from StagePrepareToUpdateRequest) Request,
         SubRequest
   WHERE Subrequest.request = Request.id
     AND Subrequest.id = srId;
  -- lock the castor file to be safe in case of two concurrent subrequest
  SELECT id into cfId FROM CastorFile where id = cfId FOR UPDATE;

  -- Look for available diskcopies
  SELECT COUNT(DiskCopy.id) INTO nbDCs
    FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = svcClassId
     AND FileSystem.status = 0 -- PRODUCTION
     AND FileSystem.diskserver = DiskServer.id
     AND DiskServer.status = 0 -- PRODUCTION
     AND DiskCopy.status IN (0, 6, 10); -- STAGED, STAGEOUT, CANBEMIGR
  IF nbDCs > 0 THEN
    -- Yes, we have some
    result := 0;   -- DISKCOPY_STAGED
  ELSE
    -- No diskcopies available for this service class;
    -- we may have to schedule a disk to disk copy or trigger a recall
    checkForD2DCopyOrRecall(cfId, srId, svcClassId, result);
  END IF;
  --IF result = 1 THEN  -- DISKCOPY_DISK2DISKCOPY
    -- check whether there's already a disk2disk copy being performed in our svc class
    -- if not
    --   create DiskCopyReplicaRequest for this svcclass
    -- result := 0;
  --END IF;
  --IF result = 2 THEN  -- DISKCOPY_WAITTAPERECALL
    -- check whether there's already a recall, and get svcclass for it
    -- IF so, check svcclass
    --    IF different
    --       create DiskCopyReplicaRequest for this svcclass
    --       make it wait on the existing WAITTAPERECALL subrequest
    --    result := 0;
    -- ELSE let the stager trigger the recall (leave result = 2)
  --END IF;
END;


/* PL/SQL method implementing processPutDone */
CREATE OR REPLACE PROCEDURE processPutDone
        (rsubreqId IN INTEGER, result OUT INTEGER) AS
  stat "numList";
  dci "numList";
  svcClassId NUMBER;
  reqId NUMBER;
  cfId NUMBER;
  fs NUMBER;
BEGIN
  -- Get the svcclass, the castorFile and the reqId for this subrequest
  SELECT Req.id, Req.svcclass, SubRequest.castorfile
    INTO reqId, svcClassId, cfId
    FROM SubRequest, StagePutDoneRequest Req
   WHERE Subrequest.request = Req.id
     AND Subrequest.id = rsubreqId;
  -- lock the castor file to be safe in case of two concurrent subrequest
  SELECT id, fileSize INTO cfId, fs 
    FROM CastorFile 
   WHERE CastorFile.id = cfId FOR UPDATE;
  
  -- Try to see whether we have available DiskCopies.
  -- Here we look on all FileSystems regardless the status
  -- so that a putDone on a disabled one goes through as there's
  -- no real IO activity involved.
  SELECT DiskCopy.status, DiskCopy.id
    BULK COLLECT INTO stat, dci
    FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
   WHERE DiskCopy.castorfile = cfId
     AND DiskCopy.fileSystem = FileSystem.id
     AND FileSystem.diskpool = DiskPool2SvcClass.parent
     AND DiskPool2SvcClass.child = svcClassId
     AND FileSystem.diskserver = DiskServer.id
     AND DiskCopy.status = 6; -- STAGEOUT
  IF stat.COUNT > 0 THEN
    -- Check that there is no put going on
    -- If any, we'll wait on one of them
    DECLARE
      putSubReq NUMBER;
    BEGIN
      -- Try to find a running put
      SELECT subrequest.id INTO putSubReq
        FROM SubRequest, Id2Type
       WHERE SubRequest.castorfile = cfId
         AND SubRequest.request = Id2Type.id
         AND Id2Type.type = 40 -- Put
         AND SubRequest.status IN (0, 1, 2, 3, 6, 13, 14) -- START, RESTART, RETRY, WAITSCHED, READY, READYFORSCHED, BEINGSCHED
         AND ROWNUM < 2;
      -- we've found one, putDone will have to wait
      UPDATE SubRequest
         SET parent = putSubReq,
             status = 5, -- WAITSUBREQ
             lastModificationTime = getTime()
       WHERE id = rsubreqId;
      result := 0;  -- no go
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- no put waiting, we can continue
      result := 1;
      putDoneFunc(cfId, fs, 2);   -- context = PutDone
    END;
  ELSE
    -- This is a PutDone without a put (otherwise we would have found
    -- a DiskCopy on a FileSystem), so we fail the subrequest.
    UPDATE SubRequest SET
      status = 7,
      errorCode = 1,  -- EPERM
      errorMessage = 'putDone without a put, or wrong service class'
    WHERE id = rsubReqId;
    result := 0; -- no go
  END IF;
END;


/* PL/SQL method implementing updateAndCheckSubRequest */
CREATE OR REPLACE PROCEDURE updateAndCheckSubRequest(srId IN INTEGER, newStatus IN INTEGER, result OUT INTEGER) AS
  reqId INTEGER;
BEGIN
 -- Lock the access to the Request
 SELECT Id2Type.id INTO reqId
  FROM SubRequest, Id2Type
  WHERE SubRequest.id = srId
  AND Id2Type.id = SubRequest.request
  FOR UPDATE;
 -- Update Status
 UPDATE SubRequest SET status = newStatus,
                       answered = 1,
                       lastModificationTime = getTime() WHERE id = srId;
 IF newStatus = 6 THEN  -- READY
   UPDATE SubRequest SET getNextStatus = 1 -- GETNEXTSTATUS_FILESTAGED
    WHERE id = srId;
 END IF;
 -- Check whether it was the last subrequest in the request
 SELECT id INTO result FROM SubRequest
  WHERE request = reqId
    AND status IN (0, 1, 2, 3, 4, 5, 7, 10)   -- START, RESTART, RETRY, WAITSCHED, WAITTAPERECALL, WAITSUBREQ, FAILED, FAILED_ANSWERING
    AND answered = 0
    AND ROWNUM < 2;
EXCEPTION WHEN NO_DATA_FOUND THEN -- No data found means we were last
  result := 0;
END;


/* PL/SQL procedure implementing startRepackMigration */
CREATE OR REPLACE PROCEDURE startRepackMigration(srId IN INTEGER, cfId IN INTEGER, dcId IN INTEGER,
                                                 fsId IN INTEGER, fcnbCopies IN NUMBER) AS
  nbTC NUMBER(2);
BEGIN
  -- how many do we have to create ?
  SELECT count(StageRepackRequest.repackvid) INTO nbTC 
    FROM SubRequest, StageRepackRequest 
   WHERE subrequest.castorfile = cfId
     AND SubRequest.request = StageRepackRequest.id
     AND subrequest.status in (4,5,6);
  -- we are not allowed to create more Tapecopies than in the fileclass specified
  IF fcnbCopies < nbTC THEN 
    nbTC := fcnbCopies;
  END IF;
  
  -- we need to update other subrequests with the diskcopy
  -- and to update status (so we don't reschedule it)
  UPDATE SubRequest 
     SET diskCopy = dcId, status = 12  -- SUBREQUEST_REPACK
   WHERE SubRequest.castorFile = cfId
     AND SubRequest.status in (4,5,6)  -- SUBREQUEST_WAITTAPERECALL, WAITSUBREQ, READY
     AND SubRequest.request in 
       (SELECT id FROM StageRepackRequest); 
  
  -- create the required number of tapecopies for the files
  internalPutDoneFunc(cfId, fsId, 0, nbTC);
END;

/* PL/SQL method implementing fileRecalled */
CREATE OR REPLACE PROCEDURE fileRecalled(tapecopyId IN INTEGER) AS
  subRequestId NUMBER;
  dci NUMBER;
  fsId NUMBER;
  reqType NUMBER;
  cfid NUMBER;
  fcnbcopies NUMBER; --number of tapecopies in fileclass
BEGIN
  SELECT SubRequest.id, DiskCopy.id, CastorFile.id, FileClass.nbcopies
    INTO subRequestId, dci, cfid, fcnbcopies
    FROM TapeCopy, SubRequest, DiskCopy, CastorFile, FileClass
   WHERE TapeCopy.id = tapecopyId
     AND CastorFile.id = TapeCopy.castorFile
     AND DiskCopy.castorFile = TapeCopy.castorFile
     AND FileClass.id = Castorfile.fileclass
     AND SubRequest.diskcopy(+) = DiskCopy.id
     AND DiskCopy.status = 2;
   
  SELECT type INTO reqType FROM Id2Type WHERE id =
    (SELECT request FROM SubRequest WHERE id = subRequestId);

  UPDATE DiskCopy SET status = decode(reqType, 119,6, 0)  -- DISKCOPY_STAGEOUT if OBJ_StageRepackRequest, else DISKCOPY_STAGED 
   WHERE id = dci RETURNING fileSystem INTO fsId;
  -- delete any previous failed diskcopy for this castorfile (due to failed recall attempts for instance)
  DELETE FROM Id2Type WHERE id IN (SELECT id FROM DiskCopy WHERE castorFile = cfId AND status = 4);
  DELETE FROM DiskCopy WHERE castorFile = cfId AND status = 4;   -- FAILED
  -- mark as invalid any previous diskcopy in disabled filesystems, which may have triggered this recall
  UPDATE DiskCopy SET status = 7  -- INVALID
   WHERE fileSystem IN (SELECT id FROM FileSystem WHERE status = 2)  -- DISABLED
     AND castorFile = cfId
     AND status = 0;  -- STAGED
  
  -- Repack handling:
  -- create the number of tapecopies for waiting subrequests and update their diskcopy.
  IF reqType = 119 THEN      -- OBJ_StageRepackRequest
    startRepackMigration(subRequestId, cfId, dci, fsId, fcnbCopies);
  ELSE 
    IF subRequestId IS NOT NULL THEN
      UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
       WHERE id = subRequestId; 
      UPDATE SubRequest SET status = 1, lastModificationTime = getTime(), parent = 0  -- SUBREQUEST_RESTART
       WHERE parent = subRequestId;
    END IF;
  END IF;
  updateFsFileClosed(fsId);
END;
