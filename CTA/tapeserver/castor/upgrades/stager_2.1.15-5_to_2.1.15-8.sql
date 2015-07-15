/******************************************************************************
 *                 stager_2.1.15-5_to_2.1.15-8.sql
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
 * This script upgrades a CASTOR v2.1.15-5 STAGER database to v2.1.15-8
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
   WHERE schemaVersion = '2_1_15_0'
     AND release = '2_1_15_8'
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
     AND release LIKE '2_1_15_5%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_15_0', '2_1_15_8', 'TRANSPARENT');
COMMIT;

/* Job management */
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs)
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name, force=>TRUE);
    END IF;
    -- Schedule the start date of the job to 15 minutes from now. This
    -- basically pauses the job for 15 minutes so that the upgrade can
    -- go through as quickly as possible.
    dbms_scheduler.set_attribute(a.job_name, 'START_DATE', SYSDATE + 15/1440);
  END LOOP;
END;
/


/* Update and revalidation of PL-SQL code */
/******************************************/

/* PL/SQL method implementing selectFiles2Delete
   This is the standard garbage collector: it sorts VALID diskcopies
   that do not need to go to tape by gcWeight and selects them for deletion up to
   the desired free space watermark */
CREATE OR REPLACE PROCEDURE selectFiles2Delete(diskServerName IN VARCHAR2,
                                               files OUT castorGC.SelectFiles2DeleteLine_Cur) AS
  dcIds "numList";
  freed INTEGER;
  deltaFree INTEGER;
  toBeFreed INTEGER;
  dontGC INTEGER;
  totalCount INTEGER;
  unused INTEGER;
  backoff INTEGER;
  CastorFileLocked EXCEPTION;
  PRAGMA EXCEPTION_INIT (CastorFileLocked, -54);
BEGIN
  -- First of all, check if we are in a Disk1 pool
  dontGC := 0;
  FOR sc IN (SELECT disk1Behavior
               FROM SvcClass, DiskPool2SvcClass D2S, DiskServer, FileSystem
              WHERE SvcClass.id = D2S.child
                AND D2S.parent = FileSystem.diskPool
                AND FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = diskServerName
              UNION ALL
             SELECT disk1Behavior
               FROM SvcClass, DataPool2SvcClass, DiskServer
              WHERE SvcClass.id = DataPool2SvcClass.child
                AND DataPool2SvcClass.parent = DiskServer.dataPool
                AND DiskServer.name = diskServerName) LOOP
    -- If any of the service classes to which we belong (normally a single one)
    -- say this is Disk1, we don't GC files.
    IF sc.disk1Behavior = 1 THEN
      dontGC := 1;
      EXIT;
    END IF;
  END LOOP;

  -- Loop on all concerned fileSystems/DataPools in a random order.
  totalCount := 0;
  FOR fs IN (SELECT * FROM (SELECT DBMS_Random.value, FileSystem.id AS fsId
                              FROM FileSystem, DiskServer
                             WHERE FileSystem.diskServer = DiskServer.id
                               AND DiskServer.name = diskServerName
                             UNION ALL
                            SELECT DBMS_Random.value, DiskServer.dataPool AS fsId
                              FROM DiskServer
                             WHERE DiskServer.name = diskServerName)
             ORDER BY 1) LOOP
    -- Count the number of diskcopies on this filesystem that are in a
    -- BEINGDELETED state. These need to be reselected in any case.
    freed := 0;
    SELECT totalCount + count(*), nvl(sum(DiskCopy.diskCopySize), 0)
      INTO totalCount, freed
      FROM DiskCopy
     WHERE (fileSystem = fs.fsId OR dataPool = fs.fsId)
       AND decode(status, 9, status, NULL) = 9;  -- BEINGDELETED (decode used to use function-based index)

    -- estimate the number of GC running the "long" query, that is the one dealing with the GCing of
    -- VALID files.
    SELECT COUNT(*) INTO backoff
      FROM v$session s, v$sqltext t
     WHERE s.sql_id = t.sql_id AND t.sql_text LIKE '%I_DiskCopy_FS_GCW%';

    -- Process diskcopies that are in an INVALID state.
    UPDATE /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_Status_7_FS_DP)) */ DiskCopy
       SET status = 9, -- BEINGDELETED
           gcType = decode(gcType, NULL, dconst.GCTYPE_USER, gcType)
     WHERE (nvl(fileSystem,0)+nvl(dataPool,0) = fs.fsId)
       AND decode(status, 7, status, NULL) = 7  -- INVALID (decode used to use function-based index)
       AND rownum <= 10000 - totalCount
    RETURNING id BULK COLLECT INTO dcIds;
    COMMIT;

    -- If we have more than 10,000 files to GC, exit the loop. There is no point
    -- processing more as the maximum sent back to the client in one call is
    -- 10,000. This protects the garbage collector from being overwhelmed with
    -- requests and reduces the stager DB load. Furthermore, if too much data is
    -- sent back to the client, the transfer time between the stager and client
    -- becomes very long and the message may timeout or may not even fit in the
    -- clients receive buffer!
    totalCount := totalCount + dcIds.COUNT();
    EXIT WHEN totalCount >= 10000;

    -- Continue processing but with VALID files, only in case we are not already loaded
    IF dontGC = 0 AND backoff < 4 THEN
      -- Do not delete VALID files from non production hardware
      BEGIN
        IF fs.fsId > 0 THEN
          SELECT FileSystem.id INTO unused
            FROM DiskServer, FileSystem
           WHERE FileSystem.id = fs.fsId
             AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
             AND FileSystem.diskserver = DiskServer.id
             AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
             AND DiskServer.hwOnline = 1;
        ELSE
          SELECT DiskServer.id INTO unused
            FROM DiskServer
           WHERE dataPool = fs.fsId
             AND status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
             AND hwOnline = 1
             AND ROWNUM < 2;
        END IF;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        EXIT;
      END;
      -- Calculate the amount of space that would be freed on the filesystem
      -- if the files selected above were to be deleted.
      IF dcIds.COUNT > 0 THEN
        SELECT /*+ INDEX(DiskCopy PK_DiskCopy_Id) */ freed + sum(diskCopySize) INTO freed
          FROM DiskCopy
         WHERE DiskCopy.id IN
             (SELECT /*+ CARDINALITY(fsidTable 5) */ *
                FROM TABLE(dcIds) dcidTable);
      END IF;
      -- Get the amount of space to be liberated
      IF fs.fsId > 0 THEN
        SELECT decode(sign(maxFreeSpace * totalSize - free), -1, 0, maxFreeSpace * totalSize - free)
          INTO toBeFreed
          FROM FileSystem
         WHERE id = fs.fsId;
      ELSE
        SELECT decode(sign(maxFreeSpace * totalSize - free), -1, 0, maxFreeSpace * totalSize - free)
          INTO toBeFreed
          FROM DataPool
         WHERE id = fs.fsId;
      END IF;
      -- If space is still required even after removal of INVALID files, consider
      -- removing VALID files until we are below the free space watermark
      IF freed < toBeFreed THEN
        -- Loop on file deletions
        FOR dc IN (SELECT /*+ INDEX_RS_ASC(DiskCopy I_DiskCopy_FS_DP_GCW) */ DiskCopy.id, castorFile
                     FROM DiskCopy, CastorFile
                    WHERE (nvl(fileSystem,0)+nvl(dataPool,0) = fs.fsId)
                      AND status = dconst.DISKCOPY_VALID
                      AND CastorFile.id = DiskCopy.castorFile
                      AND CastorFile.tapeStatus IN (dconst.CASTORFILE_DISKONLY, dconst.CASTORFILE_ONTAPE)
                      ORDER BY gcWeight ASC) LOOP
          BEGIN
            -- Lock the CastorFile
            SELECT id INTO unused FROM CastorFile
             WHERE id = dc.castorFile FOR UPDATE NOWAIT;
            -- Mark the DiskCopy as being deleted
            UPDATE DiskCopy
               SET status = dconst.DISKCOPY_BEINGDELETED,
                   gcType = dconst.GCTYPE_AUTO
             WHERE id = dc.id RETURNING diskCopySize INTO deltaFree;
            totalCount := totalCount + 1;
            -- Update freed space
            freed := freed + deltaFree;
            -- update importance of remianing copies of the file if any
            UPDATE DiskCopy
               SET importance = importance + 1
             WHERE castorFile = dc.castorFile
               AND status = dconst.DISKCOPY_VALID;
            -- Shall we continue ?
            IF toBeFreed <= freed THEN
              EXIT;
            END IF;
            IF totalCount >= 10000 THEN
              EXIT;
            END IF;           
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              -- The file no longer exists or has the wrong state
              NULL;
            WHEN CastorFileLocked THEN
              -- Go to the next candidate, processing is taking place on the
              -- file
              NULL;
          END;
          COMMIT;
        END LOOP;
      END IF;
    END IF;
    -- We have enough files to exit the loop ?
    EXIT WHEN totalCount >= 10000;
  END LOOP;

  -- Now select all the BEINGDELETED diskcopies in this diskserver for the GC daemon
  OPEN files FOR
    SELECT /*+ INDEX(CastorFile PK_CastorFile_ID) */
           DC.path, DC.id,
           Castorfile.fileid, Castorfile.nshost,
           DC.lastAccessTime, DC.nbCopyAccesses, DC.gcWeight,
           DC.gcType, DC.svcClassList
      FROM CastorFile,
           (SELECT DiskCopy.castorFile,
                   FileSystem.mountPoint || DiskCopy.path AS path, DiskCopy.id,
                   DiskCopy.lastAccessTime, DiskCopy.nbCopyAccesses, DiskCopy.gcWeight,
                   getObjStatusName('DiskCopy', 'gcType', DiskCopy.gcType) AS gcType,
                   getSvcClassList(FileSystem.id) AS svcClassList
              FROM FileSystem, DiskServer, DiskCopy
             WHERE decode(DiskCopy.status, 9, DiskCopy.status, NULL) = 9 -- BEINGDELETED
               AND DiskCopy.fileSystem = FileSystem.id
               AND FileSystem.diskServer = DiskServer.id
               AND DiskServer.name = diskServerName
             UNION ALL
            SELECT DiskCopy.castorFile,
                   getConfigOption('DiskManager', 'PoolUserId', 'castor') || '@' || DataPool.name || '/' || DiskCopy.path AS path, DiskCopy.id,
                   DiskCopy.lastAccessTime, DiskCopy.nbCopyAccesses, DiskCopy.gcWeight,
                   getObjStatusName('DiskCopy', 'gcType', DiskCopy.gcType) AS gcType,
                   getSvcClassListDP(DiskServer.dataPool) AS svcClassList
              FROM DiskServer, DiskCopy, DataPool
             WHERE decode(DiskCopy.status, 9, DiskCopy.status, NULL) = 9 -- BEINGDELETED
               AND DiskCopy.dataPool = DataPool.id
               AND DiskCopy.dataPool = DiskServer.dataPool
               AND DiskServer.name = diskServerName
               AND ROWNUM < 2) DC
     WHERE DC.castorfile = CastorFile.id
       AND rownum <= 10000;
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

/* PL/SQL method implementing createEmptyFile */
CREATE OR REPLACE PROCEDURE createEmptyFile
(cfId IN NUMBER, fileId IN NUMBER, nsHost IN VARCHAR2,
 srId IN INTEGER, schedule IN INTEGER) AS
  dcPath VARCHAR2(2048);
  gcw NUMBER;
  gcwProc VARCHAR(2048);
  fsId NUMBER;
  dpId NUMBER;
  dcId NUMBER;
  svcClassId NUMBER;
  ouid INTEGER;
  ogid INTEGER;
  fsPath VARCHAR2(2048);
BEGIN
  -- update filesize overriding any previous value
  UPDATE CastorFile SET fileSize = 0 WHERE id = cfId;
  -- get an id for our new DiskCopy
  dcId := ids_seq.nextval();
  -- find a fileSystem for this empty file
  SELECT fsId, dpId, svcClass, euid, egid, name || ':' || mountpoint
    INTO fsId, dpId, svcClassId, ouid, ogid, fsPath
    FROM (SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
                 DBMS_Random.value, FileSystem.id AS FsId, NULL AS dpId, Request.svcClass,
                 Request.euid, Request.egid, DiskServer.name, FileSystem.mountpoint
            FROM DiskServer, FileSystem, DiskPool2SvcClass,
                 (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                         id, svcClass, euid, egid from StageGetRequest UNION ALL
                  SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */
                         id, svcClass, euid, egid from StagePrepareToGetRequest) Request,
                  SubRequest
           WHERE SubRequest.id = srId
             AND Request.id = SubRequest.request
             AND Request.svcclass = DiskPool2SvcClass.child
             AND FileSystem.diskpool = DiskPool2SvcClass.parent
             AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
             AND DiskServer.id = FileSystem.diskServer
             AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
             AND DiskServer.hwOnline = 1
           UNION ALL
          SELECT /*+ INDEX(Subrequest PK_Subrequest_Id)*/
                 DBMS_Random.value, NULL AS FsId, DataPool2SvcClass.parent AS dpId,
                 Request.svcClass, Request.euid, Request.egid, DiskServer.name, ''
            FROM DiskServer, DataPool2SvcClass,
                 (SELECT /*+ INDEX(StageGetRequest PK_StageGetRequest_Id) */
                         id, svcClass, euid, egid from StageGetRequest UNION ALL
                  SELECT /*+ INDEX(StagePrepareToGetRequest PK_StagePrepareToGetRequest_Id) */
                         id, svcClass, euid, egid from StagePrepareToGetRequest) Request,
                  SubRequest
           WHERE SubRequest.id = srId
             AND Request.id = SubRequest.request
             AND Request.svcclass = DataPool2SvcClass.child
             AND DiskServer.datapool = DataPool2SvcClass.parent
             AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
             AND DiskServer.hwOnline = 1
        ORDER BY 1) -- use randomness to scatter filesystem/DiskServer usage
   WHERE ROWNUM < 2;
  -- compute it's gcWeight
  gcwProc := castorGC.getRecallWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(0); END;'
    USING OUT gcw;
  -- compute the DiskCopy Path
  buildPathFromFileId(fileId, nsHost, dcId, dcPath, fsId IS NOT NULL);
  -- then create the DiskCopy
  INSERT INTO DiskCopy
    (path, id, filesystem, dataPool, castorfile, status, importance,
     creationTime, lastAccessTime, gcWeight, diskCopySize, nbCopyAccesses, owneruid, ownergid)
  VALUES (dcPath, dcId, fsId, dpId, cfId, dconst.DISKCOPY_VALID, -1,
          getTime(), getTime(), GCw, 0, 0, ouid, ogid);
  -- link to the SubRequest and schedule an access if requested
  IF schedule = 0 THEN
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest SET diskCopy = dcId WHERE id = srId;
  ELSE
    UPDATE /*+ INDEX(Subrequest PK_Subrequest_Id)*/ SubRequest
       SET diskCopy = dcId,
           requestedFileSystems = fsPath,
           xsize = 0, status = dconst.SUBREQUEST_READYFORSCHED,
           getNextStatus = dconst.GETNEXTSTATUS_FILESTAGED
     WHERE id = srId;
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  raise_application_error(-20115, 'No suitable filesystem found for this empty file');
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
  INSERT INTO DiskCopy (path, gcWeight, creationTime, lastAccessTime, diskCopySize, nbCopyAccesses,
                        owneruid, ownergid, id, gcType, fileSystem, datapool, castorFile,
                        status, importance)
  VALUES (varDestPath, varDcGcWeight, getTime(), getTime(), varFileSize, 0,
          varUid, varGid, varDestDcId,
          CASE varNewDcStatus WHEN dconst.DISKCOPY_INVALID
                              THEN dconst.GCTYPE_FAILEDD2D
                              ELSE NULL END,
          varDestFsId, varDestDpId, varCfId, varNewDcStatus, varDCImportance);
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

/* PL/SQL method internalPutDoneFunc, used by putDoneFunc.
   checks for diskcopies in STAGEOUT and creates the migration jobs
 */
CREATE OR REPLACE PROCEDURE internalPutDoneFunc (cfId IN INTEGER,
                                                 fs IN INTEGER,
                                                 context IN INTEGER,
                                                 nbTC IN INTEGER,
                                                 svcClassId IN INTEGER) AS
  tcId INTEGER;
  gcwProc VARCHAR2(2048);
  gcw NUMBER;
  ouid INTEGER;
  ogid INTEGER;
BEGIN
  -- compute the gc weight of the brand new diskCopy
  gcwProc := castorGC.getUserWeight(svcClassId);
  EXECUTE IMMEDIATE 'BEGIN :newGcw := ' || gcwProc || '(:fs); END;'
    USING OUT gcw, IN fs;
  -- update the DiskCopy
  UPDATE DiskCopy
     SET status = dconst.DISKCOPY_VALID,
         lastAccessTime = getTime(),  -- for the GC, effective lifetime of this diskcopy starts now
         gcWeight = gcw,
         diskCopySize = fs,
         importance = -1              -- we have a single diskcopy for now
   WHERE castorFile = cfId AND status = dconst.DISKCOPY_STAGEOUT
   RETURNING owneruid, ownergid INTO ouid, ogid;
  -- update the CastorFile
  UPDATE Castorfile SET tapeStatus = (CASE WHEN nbTC = 0 OR fs = 0
                                           THEN dconst.CASTORFILE_DISKONLY
                                           ELSE dconst.CASTORFILE_NOTONTAPE
                                       END)
   WHERE id = cfId;
  -- trigger migration when needed
  IF nbTC > 0 AND fs > 0 THEN
    FOR i IN 1..nbTC LOOP
      initMigration(cfId, fs, NULL, NULL, i, tconst.MIGRATIONJOB_PENDING);
    END LOOP;
  END IF;
  -- If we are a real PutDone (and not a put outside of a prepareToPut)
  -- then we have to archive the original prepareToPut subRequest
  IF context = 2 THEN
    -- There can be only a single PrepareTo request: any subsequent PPut would be rejected
    DECLARE
      srId NUMBER;
    BEGIN
      SELECT /*+ INDEX_RS_ASC(Subrequest I_Subrequest_Castorfile)
                 INDEX(StagePrepareToPutRequest PK_StagePrepareToPutRequest_Id */
             SubRequest.id INTO srId
        FROM SubRequest, StagePrepareToPutRequest
       WHERE SubRequest.castorFile = cfId
         AND SubRequest.request = StagePrepareToPutRequest.id
         AND SubRequest.status = 6;  -- READY
      archiveSubReq(srId, 8);  -- FINISHED
    EXCEPTION WHEN NO_DATA_FOUND THEN
      NULL;   -- ignore the missing subrequest
    END;
  END IF;
  IF svcClassId > 0 THEN
    -- Trigger the creation of additional copies of the file, if necessary.
    -- For this, we must know the service class: as the automatic putDone
    -- cleaning does not provide it, we skip this step in that case.
    replicateOnClose(cfId, ouid, ogid, svcClassId);
  END IF;
END;
/

/* handle the creation of the Disk2DiskCopyJobs for the running drainingJobs */
CREATE OR REPLACE PROCEDURE drainRunner AS
  varNbRunningJobs INTEGER;
  varDataRunningJobs INTEGER;
  varMaxNbFilesScheduled INTEGER;
  varMaxDataScheduled INTEGER;
  varUnused INTEGER;
BEGIN
  -- get maxNbFilesScheduled and maxDataScheduled
  varMaxNbFilesScheduled := TO_NUMBER(getConfigOption('Draining', 'MaxNbFilesScheduled', '1000'));
  varMaxDataScheduled := TO_NUMBER(getConfigOption('Draining', 'MaxDataScheduled', '10000000000')); -- 10 GB
  -- loop over draining jobs
  FOR dj IN (SELECT id, fileSystem, svcClass, fileMask, euid, egid
               FROM DrainingJob WHERE status = dconst.DRAININGJOB_RUNNING) LOOP
    BEGIN
      -- lock the draining job first
      SELECT id INTO varUnused FROM DrainingJob WHERE id = dj.id FOR UPDATE;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- it was (already!) canceled, go to the next one
      CONTINUE;
    END;
    -- check how many disk2DiskCopyJobs are already running for this draining job
    SELECT count(*), nvl(sum(CastorFile.fileSize), 0) INTO varNbRunningJobs, varDataRunningJobs
      FROM Disk2DiskCopyJob, CastorFile
     WHERE Disk2DiskCopyJob.drainingJob = dj.id
       AND CastorFile.id = Disk2DiskCopyJob.castorFile;
    -- Loop over the creation of Disk2DiskCopyJobs. Select max 1000 files, taking running
    -- ones into account. Also take the most important jobs first
    logToDLF(NULL, dlf.LVL_SYSTEM, dlf.DRAINING_REFILL, 0, '', 'stagerd',
             'svcClass=' || getSvcClassName(dj.svcClass) || ' DrainReq=' ||
             TO_CHAR(dj.id) || ' MaxNewJobsCount=' || TO_CHAR(varMaxNbFilesScheduled-varNbRunningJobs));
    FOR F IN (SELECT * FROM
               (SELECT CastorFile.id cfId, Castorfile.nsOpenTime, DiskCopy.id dcId, CastorFile.fileSize
                  FROM DiskCopy, CastorFile
                 WHERE DiskCopy.fileSystem = dj.fileSystem
                   AND CastorFile.id = DiskCopy.castorFile
                   AND ((dj.fileMask = dconst.DRAIN_FILEMASK_NOTONTAPE AND
                         CastorFile.tapeStatus IN (dconst.CASTORFILE_NOTONTAPE, dconst.CASTORFILE_DISKONLY)) OR
                        (dj.fileMask = dconst.DRAIN_FILEMASK_ALL))
                   AND DiskCopy.status = dconst.DISKCOPY_VALID
                   AND NOT EXISTS (SELECT /*+ INDEX_RS_ASC(DrainingErrors I_DrainingErrors_DJ_CF) */ 1
                                     FROM DrainingErrors WHERE castorFile = CastorFile.id AND drainingJob = dj.id)
                   -- don't recreate disk-to-disk copy jobs for the ones already done in previous rounds
                   AND NOT EXISTS (SELECT /*+ INDEX_RS_ASC(Disk2DiskCopyJob I_Disk2DiskCopyJob_DrainJob) */ 1
                                     FROM Disk2DiskCopyJob WHERE castorFile = CastorFile.id AND drainingJob = dj.id)
                 ORDER BY DiskCopy.importance DESC)
               WHERE ROWNUM <= varMaxNbFilesScheduled-varNbRunningJobs) LOOP
      -- Do not schedule more that varMaxAmountOfSchedD2dPerDrain
      IF varDataRunningJobs <= varMaxDataScheduled THEN
        createDisk2DiskCopyJob(F.cfId, F.nsOpenTime, dj.svcClass, dj.euid, dj.egid,
                               dconst.REPLICATIONTYPE_DRAINING, F.dcId, TRUE, dj.id, FALSE);
        varDataRunningJobs := varDataRunningJobs + F.fileSize;
      ELSE
        -- enough data amount, we stop scheduling
        EXIT;
      END IF;
    END LOOP;
    UPDATE DrainingJob
       SET lastModificationTime = getTime()
     WHERE id = dj.id;
    COMMIT;
  END LOOP;
END;
/

/* Procedure responsible for managing the draining process
 */
CREATE OR REPLACE PROCEDURE drainManager AS
  varTFiles INTEGER;
  varTBytes INTEGER;
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -2292);
BEGIN
  -- Delete the COMPLETED jobs older than 7 days
  BEGIN
    DELETE FROM DrainingJob
     WHERE status = dconst.DRAININGJOB_FINISHED
       AND lastModificationTime < getTime() - (7 * 86400);
    COMMIT;
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    -- check that the constraint violated is due to deleting a drainingJob
    IF sqlerrm LIKE '%constraint (CASTOR_STAGER.FK_DISK2DISKCOPYJOB_DRAINJOB) violated%' THEN
      -- yes, ignore and move on
      NULL;
    ELSE
      -- no, raise error and stop here
      RAISE;
    END IF;
  END;
  -- Start new DrainingJobs if needed
  FOR dj IN (SELECT id, fileSystem, fileMask
               FROM DrainingJob WHERE status = dconst.DRAININGJOB_SUBMITTED) LOOP
    UPDATE DrainingJob SET status = dconst.DRAININGJOB_STARTING WHERE id = dj.id;
    COMMIT;
    -- Compute totals now. Jobs will be later added in bunches by drainRunner
    SELECT count(*), SUM(diskCopySize) INTO varTFiles, varTBytes
      FROM DiskCopy, CastorFile
     WHERE fileSystem = dj.fileSystem
       AND status = dconst.DISKCOPY_VALID
       AND CastorFile.id = DiskCopy.castorFile
       AND ((dj.fileMask = dconst.DRAIN_FILEMASK_NOTONTAPE AND
             CastorFile.tapeStatus IN (dconst.CASTORFILE_NOTONTAPE, dconst.CASTORFILE_DISKONLY)) OR
            (dj.fileMask = dconst.DRAIN_FILEMASK_ALL));
    UPDATE DrainingJob
       SET totalFiles = varTFiles,
           totalBytes = nvl(varTBytes, 0),
           status = decode(varTBytes, NULL, dconst.DRAININGJOB_FINISHED, dconst.DRAININGJOB_RUNNING)
     WHERE id = dj.id;
    COMMIT;
  END LOOP;
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


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_15_8';
COMMIT;
