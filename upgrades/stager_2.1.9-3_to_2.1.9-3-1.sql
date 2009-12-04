/******************************************************************************
 *              2.1.9-3_to_2.1.9-3-1.sql
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
 * @(#)$RCSfile: template.sql,v $ $Release: 1.2 $ $Release$ $Date: 2009/02/16 09:13:00 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.9-3 DBNAME database into v2.1.9-3-1
 *
 * Fixes:
 * - #58882: Race condition in filesDeletedProc can result in orphaned
 *           CastorFile entries
 * - #59326: Missing index on StageDiskCopyReplicaRequest results in FTS in
 *           processPrepareRequest
 * - #59367: Deadlock between stageRm and postJobChecks
 * - #59435: stager_qry -r -n may forget some files
 * - #60002: RFE: Draining diskserver logic should avoid replicating STAGED
 *           files when the file-mask is CANBEMIGR
 * - #60064: stager_rm by service class does not cancel Put/Get requests if
 *           they have not yet been scheduled
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_9_0'
     AND release = '2_1_9_3_1'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE release LIKE '2_1_9_3%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('2_1_9_0', '2_1_9_3_1');
UPDATE UpgradeLog SET type = 'TRANSPARENT';
COMMIT;


/* Schema changes go here */
/**************************/

/* #59326: Missing index on StageDiskCopyReplicaRequest results in FTS in
 * processPrepareRequest
 */
DECLARE
  unused VARCHAR2(30);
BEGIN
  SELECT index_name INTO unused 
    FROM user_indexes
   WHERE index_name = 'I_STAGEDISKCOPYREPLIC_DESTDC';
EXCEPTION WHEN NO_DATA_FOUND THEN
  EXECUTE IMMEDIATE 'CREATE INDEX I_StageDiskCopyReplic_DestDC
                       ON StageDiskCopyReplicaRequest (destDiskCopy)';
END;
/

/* Update and revalidation of PL-SQL code */
/******************************************/

/*
 * PL/SQL method implementing the diskPoolQuery when listing pools
 */
CREATE OR REPLACE
PROCEDURE describeDiskPools
(svcClassName IN VARCHAR2, reqEuid IN INTEGER, reqEgid IN INTEGER,
 res OUT NUMBER, result OUT castor.DiskPoolsQueryLine_Cur) AS
BEGIN
  -- We use here analytic functions and the grouping sets functionality to
  -- get both the list of filesystems and a summary per diskserver and per
  -- diskpool. The grouping analytic function also allows to mark the summary
  -- lines for easy detection in the C++ code
  IF svcClassName = '*' OR svcClassName IS NULL THEN
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
             grouping(fs.mountPoint) AS IsFSGrouped,
             dp.name, ds.name, ds.status, fs.mountPoint,
             sum(getAvailFreeSpace(fs.status + ds.status,
                 fs.free - fs.minAllowedFreeSpace * fs.totalSize)) AS freeSpace,
             sum(fs.totalSize),
             fs.minFreeSpace, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp
       WHERE dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         GROUP BY grouping sets(
             (dp.name, ds.name, ds.status, fs.mountPoint,
               getAvailFreeSpace(fs.status + ds.status,
                                 fs.free - fs.minAllowedFreeSpace * fs.totalSize),
               fs.totalSize,
               fs.minFreeSpace, fs.maxFreeSpace, fs.status),
             (dp.name, ds.name, ds.status),
             (dp.name)
            )
         ORDER BY dp.name, IsDSGrouped DESC, ds.name, IsFSGrouped DESC, fs.mountpoint;
  ELSE 
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
             grouping(fs.mountPoint) AS IsFSGrouped,
             dp.name, ds.name, ds.status, fs.mountPoint,
             sum(getAvailFreeSpace(fs.status + ds.status,
                 fs.free - fs.minAllowedFreeSpace * fs.totalSize)) AS freeSpace,
             sum(fs.totalSize),
             fs.minFreeSpace, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp,
             DiskPool2SvcClass d2s, SvcClass sc
       WHERE sc.name = svcClassName
         AND sc.id = d2s.child
         AND checkPermissionOnSvcClass(sc.name, reqEuid, reqEgid, 103) = 0
         AND d2s.parent = dp.id
         AND dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         GROUP BY grouping sets(
             (dp.name, ds.name, ds.status, fs.mountPoint,
               getAvailFreeSpace(fs.status + ds.status,
                 fs.free - fs.minAllowedFreeSpace * fs.totalSize),
               fs.totalSize,
               fs.minFreeSpace, fs.maxFreeSpace, fs.status),
             (dp.name, ds.name, ds.status),
             (dp.name)
            )
         ORDER BY dp.name, IsDSGrouped DESC, ds.name, IsFSGrouped DESC, fs.mountpoint;
  END IF;
  -- If no results are available, check to see if any diskpool exists and if
  -- access to view all the diskpools has been revoked. The information extracted
  -- here will be used to send an appropriate error message to the client.
  IF result%ROWCOUNT = 0 THEN
    SELECT CASE count(*)
           WHEN sum(checkPermissionOnSvcClass(sc.name, reqEuid, reqEgid, 103)) THEN -1
           ELSE count(*) END
      INTO res
      FROM DiskPool2SvcClass d2s, SvcClass sc
     WHERE d2s.child = sc.id
       AND (sc.name = svcClassName OR svcClassName = '*' OR svcClassName IS NULL);
  END IF;
END;
/

/*
 * PL/SQL method implementing the diskPoolQuery for a given pool
 */
CREATE OR REPLACE
PROCEDURE describeDiskPool
(diskPoolName IN VARCHAR2, svcClassName IN VARCHAR2,
 res OUT NUMBER, result OUT castor.DiskPoolQueryLine_Cur) AS
BEGIN
  -- We use here analytic functions and the grouping sets functionnality to
  -- get both the list of filesystems and a summary per diskserver and per
  -- diskpool. The grouping analytic function also allows to mark the summary
  -- lines for easy detection in the C++ code
  IF svcClassName = '*' OR svcClassName IS NULL THEN
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
             grouping(fs.mountPoint) AS IsGrouped,
             ds.name, ds.status, fs.mountPoint,
             sum(getAvailFreeSpace(fs.status + ds.status,
                 fs.free - fs.minAllowedFreeSpace * fs.totalSize)) AS freeSpace,
             sum(fs.totalSize),
             fs.minFreeSpace, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp
       WHERE dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         AND dp.name = diskPoolName
         GROUP BY grouping sets(
             (ds.name, ds.status, fs.mountPoint,
               getAvailFreeSpace(fs.status + ds.status,
                 fs.free - fs.minAllowedFreeSpace * fs.totalSize),
               fs.totalSize,
               fs.minFreeSpace, fs.maxFreeSpace, fs.status),
             (ds.name, ds.status),
             (dp.name)
            )
         ORDER BY IsDSGrouped DESC, ds.name, IsGrouped DESC, fs.mountpoint;
  ELSE
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
             grouping(fs.mountPoint) AS IsGrouped,
             ds.name, ds.status, fs.mountPoint,
             sum(getAvailFreeSpace(fs.status + ds.status,
                 fs.free - fs.minAllowedFreeSpace * fs.totalSize)) AS freeSpace,
             sum(fs.totalSize),
             fs.minFreeSpace, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp,
             DiskPool2SvcClass d2s, SvcClass sc
       WHERE sc.name = svcClassName
         AND sc.id = d2s.child
         AND d2s.parent = dp.id
         AND dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         AND dp.name = diskPoolName
         GROUP BY grouping sets(
             (ds.name, ds.status, fs.mountPoint,
               getAvailFreeSpace(fs.status + ds.status,
                 fs.free - fs.minAllowedFreeSpace * fs.totalSize),
               fs.totalSize,
               fs.minFreeSpace, fs.maxFreeSpace, fs.status),
             (ds.name, ds.status),
             (dp.name)
            )
         ORDER BY IsDSGrouped DESC, ds.name, IsGrouped DESC, fs.mountpoint;
  END IF;
  -- If no results are available, check to see if any diskpool exists and if
  -- access to view all the diskpools has been revoked. The information extracted
  -- here will be used to send an appropriate error message to the client.
  IF result%ROWCOUNT = 0 THEN
    SELECT count(*) INTO res
      FROM DiskPool dp, DiskPool2SvcClass d2s, SvcClass sc
     WHERE d2s.child = sc.id
       AND d2s.parent = dp.id
       AND dp.name = diskPoolName
       AND (sc.name = svcClassName OR svcClassName = '*' OR svcClassName IS NULL);
  END IF;
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
      -- and then diskcopies resulting from ongoing requests, for which the previous
      -- query wouldn't return any entry because of e.g. missing filesystem
      SELECT DC.id
        FROM (SELECT id FROM StagePrepareToPutRequest WHERE svcClass = scId UNION ALL
              SELECT id FROM StagePrepareToGetRequest WHERE svcClass = scId UNION ALL
              SELECT id FROM StagePrepareToUpdateRequest WHERE svcClass = scId UNION ALL
              SELECT id FROM StageRepackRequest WHERE svcClass = scId UNION ALL
              SELECT id FROM StagePutRequest WHERE svcClass = scId UNION ALL
              SELECT id FROM StageGetRequest WHERE svcClass = scId UNION ALL
              SELECT id FROM StageUpdateRequest WHERE svcClass = scId UNION ALL
              SELECT id FROM StageDiskCopyReplicaRequest WHERE svcClass = scId) Request,
             SubRequest, DiskCopy DC
       WHERE SubRequest.diskCopy = DC.id
         AND Request.id = SubRequest.request
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
  SELECT /*+ INDEX(SR I_SubRequest_DiskCopy) */ id
    BULK COLLECT INTO srIds
    FROM SubRequest SR
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

/* Procedure responsible for processing the snapshot of files that need to be
 * replicated in order to drain a filesystem.
 */
CREATE OR REPLACE
PROCEDURE drainFileSystem(fsId IN NUMBER)
AS
  unused     NUMBER;
  res        NUMBER;
  dcId       NUMBER;
  cfId       NUMBER;
  svcId      NUMBER;
  ouid       NUMBER;
  ogid       NUMBER;
  autoDelete NUMBER;
  fileMask   NUMBER;
BEGIN
  -- As this procedure is called recursively we release the previous calls
  -- locks. This prevents the procedure from locking too many castorfile
  -- entries which could result in service degradation.
  COMMIT;
  -- Update the filesystems entry in the DrainingFileSystem table to indicate
  -- that activity is ongoing. If no entry exist then the filesystem is not
  -- under the control of the draining logic.
  svcId := 0;
  UPDATE DrainingFileSystem
     SET lastUpdateTime = getTime()
   WHERE fileSystem = fsId
     AND status = 2  -- RUNNING
  RETURNING svcclass, autoDelete, fileMask INTO svcId, autoDelete, fileMask;
  IF svcId = 0 THEN
    RETURN;  -- Do nothing
  END IF;
  -- Extract the next diskcopy to be processed. Note: this is identical to the
  -- way that subrequests are picked up in the SubRequestToDo procedure. The N
  -- levels of SELECTS and hints allow us to process the entries in the
  -- DrainingDiskCopy snapshot in an ordered way.
  dcId := 0;
  UPDATE DrainingDiskCopy
     SET status = 2  -- PROCESSING
   WHERE diskCopy = (
     SELECT diskCopy FROM (
       SELECT /*+ INDEX(DC I_DrainingDCs_PC) */ DDC.diskCopy
         FROM DrainingDiskCopy DDC
        WHERE DDC.fileSystem = fsId
          AND DDC.status IN (0, 1)  -- CREATED, RESTARTED
        ORDER BY DDC.priority DESC, DDC.creationTime ASC)
     WHERE rownum < 2)
  RETURNING diskCopy INTO dcId;
  IF dcId = 0 THEN
    -- If there are no transfers outstanding related to the draining process we
    -- can update the filesystem entry in the DrainingFileSystem table for the
    -- last time.
    UPDATE DrainingFileSystem
       SET status = decode((SELECT count(*)
                              FROM DrainingDiskCopy
                             WHERE status = 4  -- FAILED
                               AND fileSystem = fsId), 0, 5, 4),
           lastUpdateTime = getTime()
     WHERE fileSystem = fsId
       -- Check to make sure there are no transfers outstanding.
       AND (SELECT count(*) FROM DrainingDiskCopy
             WHERE status = 3  -- RUNNING
               AND fileSystem = fsId) = 0;
    RETURN;  -- Out of work!
  END IF;
  -- The DrainingDiskCopy table essentially contains a snapshot of the work
  -- that needs to be done at the time the draining process was initiated for a
  -- filesystem. As a result, the diskcopy id previously extracted may no longer
  -- be valid as the snapshot information is outdated. Here we simply verify
  -- that the diskcopy is still what we expected.
  BEGIN
    -- Determine the castorfile id
    SELECT castorFile INTO cfId FROM DiskCopy WHERE id = dcId;
    -- Lock the castorfile
    SELECT id INTO cfId FROM CastorFile
     WHERE id = cfId FOR UPDATE;
    -- Check that the status of the diskcopy matches what was specified in the
    -- filemask for the filesystem. If the diskcopy is not in the expected
    -- status then it is no longer a candidate to be replicated.
    SELECT ownerUid, ownerGid INTO ouid, ogid
      FROM DiskCopy
     WHERE id = dcId
       AND ((status = 0        AND fileMask = 0)    -- STAGED
        OR  (status = 10       AND fileMask = 1)    -- CANBEMIGR
        OR  (status IN (0, 10) AND fileMask = 2));  -- ALL
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- The diskcopy no longer exists or is not of interest so we delete it from
    -- the snapshot of files to be processed.
    DELETE FROM DrainingDiskCopy
      WHERE diskCopy = dcId AND fileSystem = fsId;
    drainFileSystem(fsId);
    RETURN;  -- No replication required
  END;
  -- Just because the file was listed in the snapshot doesn't mean that it must
  -- be replicated! Here we check to see if the file is available on another
  -- diskserver in the target service class and if enough copies of the file
  -- are available.
  --
  -- The decode logic used here essentially states:
  --   If replication on close is enabled and there are enough copies available
  --   to satisfy the maxReplicaNb on the service class, then do not replicate.
  -- Otherwise,
  --   If replication on close is disabled and a copy exists elsewhere, then do
  --   do not replicate. All other cases result in triggering replication.
  --
  -- Notes:
  -- - The check to see if we have enough copies online when replication on
  --   close is enabled will effectively rebalance the pool!
  SELECT decode(replicateOnClose, 1,
         decode((available - maxReplicaNb) -
                 abs(available - maxReplicaNb), 0, 0, 1),
         decode(sign(available), 1, 0, 1)) INTO res
    FROM (
      SELECT count(*) available
        FROM DiskCopy, FileSystem, DiskServer, DiskPool2SvcClass
       WHERE DiskCopy.fileSystem = FileSystem.id
         AND DiskCopy.castorFile = cfId
         AND DiskCopy.id != dcId
         AND DiskCopy.status IN (0, 10)  -- STAGED, CANBEMIGR
         AND FileSystem.diskPool = DiskPool2SvcClass.parent
         AND DiskPool2SvcClass.child = svcId
         AND FileSystem.status IN (0, 1)  -- PRODUCTION, DRAINING
         AND FileSystem.diskServer = DiskServer.id
         AND DiskServer.status IN (0, 1)  -- PRODUCTION, DRAINING
     ) results, SvcClass
   WHERE SvcClass.id = svcId;
  IF res = 0 THEN
    -- No replication is required, there are enough copies of the file to
    -- satisfy the requirements of the target service class.
    IF autoDelete = 1 THEN
      -- Invalidate the diskcopy so that it can be garbage collected.
      UPDATE DiskCopy
         SET status = 7,  -- INVALID
             gctype = 3   -- Draining filesystem
       WHERE id = dcId AND fileSystem = fsId;
    END IF;
    -- Delete the diskcopy from the snapshot as no action is required.
    DELETE FROM DrainingDiskCopy
     WHERE diskCopy = dcId AND fileSystem = fsId;
    drainFileSystem(fsId);
    RETURN;
  END IF;
  -- Check to see if there is already an outstanding request to replicate the
  -- castorfile to the target service class. If so, we wait on that replication
  -- to complete. This avoids having multiple requests to replicate the same
  -- file to the same target service class multiple times.
  BEGIN
    SELECT DiskCopy.id INTO res
      FROM DiskCopy, StageDiskCopyReplicaRequest
     WHERE StageDiskCopyReplicaRequest.destDiskCopy = DiskCopy.id
       AND StageDiskCopyReplicaRequest.svcclass = svcId
       AND DiskCopy.castorFile = cfId
       AND DiskCopy.status = 1  -- WAITDISK2DISKCOPY
       AND rownum < 2;
    IF res > 0 THEN
      -- Wait on another replication to complete.
      UPDATE DrainingDiskCopy
         SET status = 3,  -- WAITD2D
             parent = res
       WHERE diskCopy = dcId AND fileSystem = fsId;
       RETURN;
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    NULL;  -- No pending replications running
  END;
  -- If we have attempted to replicate the file more than 10 times already then
  -- give up! The error will be exposed later to an administrator for manual
  -- corrective action.
  SELECT count(*) INTO res
    FROM StageDiskCopyReplicaRequest R, SubRequest
   WHERE SubRequest.request = R.id
     AND R.sourceDiskCopy = dcId
     AND SubRequest.status = 9; -- FAILED_FINISHED
  IF res >= 10 THEN
    UPDATE DrainingDiskCopy
       SET status = 4,
           comments = 'Maximum number of attempts exceeded'
     WHERE diskCopy = dcId AND fileSystem = fsId;
    drainFileSystem(fsId);
    RETURN;  -- Try again
  END IF;
  -- Trigger a replication request for the file
  createDiskCopyReplicaRequest(0, dcId, svcId, svcId, ouid, ogid);
  -- Update the status of the file
  UPDATE DrainingDiskCopy SET status = 3  -- WAITD2D
   WHERE diskCopy = dcId AND fileSystem = fsId;
END;
/

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
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- List the castorfiles to be cleaned up afterwards
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      INSERT INTO filesDeletedProcHelper VALUES
           ((SELECT castorFile FROM DiskCopy
              WHERE id = dcIds(i)));
    -- Loop over the deleted files; first use FORALL for bulk operation
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      DELETE FROM Id2Type WHERE id = dcIds(i);
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      DELETE FROM DiskCopy WHERE id = dcIds(i);
    -- Then use a normal loop to clean castorFiles. Note: We order the list to
    -- prevent a deadlock
    FOR cf IN (SELECT DISTINCT(cfId)
                 FROM filesDeletedProcHelper
                ORDER BY cfId ASC) LOOP
      DECLARE
        nb NUMBER;
      BEGIN
        -- First try to lock the castorFile
        SELECT id INTO nb FROM CastorFile
         WHERE id = cf.cfId FOR UPDATE;
        -- See whether it has any DiskCopy
        SELECT count(*) INTO nb FROM DiskCopy
         WHERE castorFile = cf.cfId;
        -- If any DiskCopy, give up
        IF nb = 0 THEN
          -- Delete the TapeCopies
          deleteTapeCopies(cf.cfId);
          -- See whether pending SubRequests exist
          SELECT count(*) INTO nb FROM SubRequest
           WHERE castorFile = cf.cfId
             AND status IN (0, 1, 2, 3, 4, 5, 6, 7, 10, 12, 13, 14);  -- All but FINISHED, FAILED_FINISHED, ARCHIVED
          IF nb = 0 THEN
            -- No Subrequest, delete the CastorFile
            DECLARE
              fid NUMBER;
              fc NUMBER;
              nsh VARCHAR2(2048);
            BEGIN
              -- Delete the CastorFile
              DELETE FROM id2Type WHERE id = cf.cfId;
              DELETE FROM CastorFile WHERE id = cf.cfId
              RETURNING fileId, nsHost, fileClass
                INTO fid, nsh, fc;
              -- Check whether this file potentially had TapeCopies
              SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
              IF nb = 0 THEN
                -- This castorfile was created with no TapeCopy
                -- So removing it from the stager means erasing
                -- it completely. We should thus also remove it
                -- from the name server
                INSERT INTO FilesDeletedProcOutput VALUES (fid, nsh);
              END IF;
            END;
          ELSE
            -- SubRequests exist, fail them
            UPDATE SubRequest SET status = 7 -- FAILED
             WHERE castorFile = cf.cfId
               AND status IN (0, 1, 2, 3, 4, 5, 6, 12, 13, 14);
          END IF;
        END IF;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- Ignore, this means that the castorFile did not exist.
        -- There is thus no way to find out whether to remove the
        -- file from the nameserver. For safety, we thus keep it
        NULL;
      END;
    END LOOP;
  END IF;
  OPEN fileIds FOR SELECT * FROM FilesDeletedProcOutput;
END;
/

/* PL/SQL method implementing failSchedulerJob */
CREATE OR REPLACE
PROCEDURE failSchedulerJob(srSubReqId IN VARCHAR2, srErrorCode IN NUMBER, res OUT INTEGER) AS
  reqType NUMBER;
  srId NUMBER;
  dcId NUMBER;
  cfId NUMBER;
BEGIN
  res := 1;
  -- Get the necessary information needed about the request and subrequest
  SELECT SubRequest.id, SubRequest.diskcopy,
         Id2Type.type, SubRequest.castorFile
    INTO srId, dcId, reqType, cfId
    FROM SubRequest, Id2Type
   WHERE SubRequest.subreqid = srSubReqId
     AND SubRequest.request = Id2Type.id
     AND SubRequest.status IN (6, 14); -- READY, BEINGSCHED
  -- Lock the CastorFile
  SELECT id INTO cfId FROM CastorFile
   WHERE id = cfId FOR UPDATE;
  -- Set the error code
  UPDATE SubRequest
     SET errorCode = srErrorCode
   WHERE id = srId;
  -- Call the relevant cleanup procedures for the job, procedures that they
  -- would have called themselves if the job had failed!
  IF reqType = 40 THEN -- Put
    putFailedProc(srId);
  ELSIF reqType = 133 THEN -- DiskCopyReplica
    disk2DiskCopyFailed(dcId, 0);
  ELSE -- Get or Update
    getUpdateFailedProc(srId);
  END IF;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- The subrequest may have been removed, nothing to be done
  res := 0;
  RETURN;
END;
/

/*
 * PL/SQL method implementing the stager_qry based on request id
 */
CREATE OR REPLACE PROCEDURE reqIdStageQuery
 (rid IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
  SELECT /*+ NO_USE_HASH(REQLIST SR) USE_NL(REQLIST SR) 
             INDEX(SR I_SUBREQUEST_REQUEST) */
         sr.castorfile BULK COLLECT INTO cfs
    FROM SubRequest sr,
         (SELECT id
            FROM StagePreparetogetRequest
           WHERE reqid = rid
          UNION ALL
          SELECT id
            FROM StagePreparetoputRequest
           WHERE reqid = rid
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE reqid = rid
          UNION ALL
          SELECT id
            FROM stageGetRequest
           WHERE reqid = rid
          UNION ALL
          SELECT id
            FROM stagePutRequest
           WHERE reqid = rid
          UNION ALL
          SELECT id
            FROM StageRepackRequest
           WHERE reqid LIKE rid) reqlist
   WHERE sr.request = reqlist.id;
  IF cfs.COUNT > 0 THEN
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;
/

/*
 * PL/SQL method implementing the stager_qry based on user tag
 */
CREATE OR REPLACE PROCEDURE userTagStageQuery
 (tag IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
BEGIN
  SELECT sr.castorfile BULK COLLECT INTO cfs
    FROM SubRequest sr,
         (SELECT id
            FROM StagePreparetogetRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM StagePreparetoputRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM stageGetRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM stagePutRequest
           WHERE userTag LIKE tag) reqlist
   WHERE sr.request = reqlist.id;
  IF cfs.COUNT > 0 THEN
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;
/

/*
 * PL/SQL method implementing the LastRecalls stager_qry based on request id
 */
CREATE OR REPLACE PROCEDURE reqIdLastRecallsStageQuery
 (rid IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
  reqs "numList";
BEGIN
  SELECT id BULK COLLECT INTO reqs
    FROM (SELECT id
            FROM StagePreparetogetRequest
           WHERE reqid = rid
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE reqid = rid
          UNION ALL
          SELECT id
            FROM StageRepackRequest
           WHERE reqid = rid
          );
  IF reqs.COUNT > 0 THEN
    UPDATE SubRequest 
       SET getNextStatus = 2  -- GETNEXTSTATUS_NOTIFIED
     WHERE getNextStatus = 1  -- GETNEXTSTATUS_FILESTAGED
       AND request IN (SELECT * FROM TABLE(reqs))
    RETURNING castorfile BULK COLLECT INTO cfs;
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;
/

/*
 * PL/SQL method implementing the LastRecalls stager_qry based on user tag
 */
CREATE OR REPLACE PROCEDURE userTagLastRecallsStageQuery
 (tag IN VARCHAR2,
  svcClassId IN INTEGER,
  notfound OUT INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
  reqs "numList";
BEGIN
  SELECT id BULK COLLECT INTO reqs
    FROM (SELECT id
            FROM StagePreparetogetRequest
           WHERE userTag LIKE tag
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE userTag LIKE tag
          );
  IF reqs.COUNT > 0 THEN
    UPDATE SubRequest 
       SET getNextStatus = 2  -- GETNEXTSTATUS_NOTIFIED
     WHERE getNextStatus = 1  -- GETNEXTSTATUS_FILESTAGED
       AND request IN (SELECT * FROM TABLE(reqs))
    RETURNING castorfile BULK COLLECT INTO cfs;
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
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