/*******************************************************************
 *
 * @(#)$RCSfile: oracleQuery.sql,v $ $Revision: 1.652 $ $Date: 2009/01/22 07:18:55 $ $Author: waldron $
 *
 * PL/SQL code for the stager and resource monitoring
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/


/*
 * PL/SQL method implementing the core part of stage queries
 * It takes a list of castorfile ids as input
 */
CREATE OR REPLACE PROCEDURE internalStageQuery
 (cfs IN "numList",
  svcClassId IN NUMBER,
  result OUT castor.QueryLine_Cur) AS
BEGIN
  IF svcClassId = 0 THEN
    OPEN result FOR
      -- Here we get the status for each cf as follows: if a valid diskCopy is found,
      -- its status is returned, else if a (prepareTo)Get request is found and no diskCopy is there,
      -- WAITTAPERECALL is returned, else -1 (INVALID) is returned
      SELECT fileId, nsHost, dcId, path, fileSize, status, machine, mountPoint, nbCopyAccesses, lastKnownFileName
         FROM (SELECT
             -- we need to give these hints to the optimizer otherwise it goes for a full table scan (!)
             UNIQUE CastorFile.id, CastorFile.fileId, CastorFile.nsHost, DC.id AS dcId,
             DC.path, CastorFile.fileSize,
             CASE WHEN DC.id IS NULL THEN
                 nvl((SELECT UNIQUE decode(SubRequest.status, 0, 2, 3, 2, -1)
                    FROM SubRequest,
                        (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
                         SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
                         SELECT id, svcClass FROM StagePrepareToUpdateRequest UNION ALL
                         SELECT id, svcClass FROM StageRepackRequest UNION ALL
                         SELECT id, svcClass FROM StageGetRequest) Req
                          WHERE SubRequest.CastorFile = CastorFile.id
                            AND Subrequest.status IN (0, 3)
                            AND request = Req.id), -1)
                  ELSE DC.status END AS status,
             DC.machine, DC.mountPoint,
             DC.nbCopyAccesses, CastorFile.lastKnownFileName
        FROM CastorFile,
             (SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, DiskServer.name AS machine, FileSystem.mountPoint,
                     DiskPool2SvcClass.child AS dcSvcClass, DiskCopy.filesystem, DiskCopy.CastorFile, DiskCopy.nbCopyAccesses
                FROM FileSystem, DiskServer, DiskPool2SvcClass,
                     (SELECT id, status, filesystem, castorFile, path, nbCopyAccesses FROM DiskCopy
                       WHERE CastorFile IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
                         AND status IN (0, 1, 2, 4, 5, 6, 7, 10, 11)
                             -- search for diskCopies not GCCANDIDATE or BEINGDELETED
                       GROUP BY (id, status, filesystem, castorfile, path, nbCopyAccesses)) DiskCopy
               WHERE FileSystem.id(+) = DiskCopy.fileSystem
                 AND nvl(FileSystem.status, 0) = 0 -- PRODUCTION
                 AND DiskServer.id(+) = FileSystem.diskServer
                 AND nvl(DiskServer.status, 0) = 0 -- PRODUCTION
                 AND DiskPool2SvcClass.parent(+) = FileSystem.diskPool) DC
       WHERE CastorFile.id IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
         AND CastorFile.id = DC.castorFile(+))  -- search for valid diskcopy
    WHERE status != -1 -- when no diskcopy and no subrequest available, ignore garbage
    ORDER BY fileid, nshost;
  ELSE
    OPEN result FOR
      -- Here we get the status for each cf as follows: if a valid diskCopy is found,
      -- its status is returned, else if a (prepareTo)Get request is found and no diskCopy is there,
      -- WAITTAPERECALL is returned, else -1 (INVALID) is returned
      SELECT fileId, nsHost, dcId, path, fileSize, srStatus, machine, mountPoint, nbCopyAccesses, lastKnownFileName
        FROM (SELECT
            -- we need to give these hints to the optimizer otherwise it goes for a full table scan (!)
            UNIQUE CastorFile.id, CastorFile.fileId, CastorFile.nsHost, DC.id AS dcId,
            DC.path, CastorFile.fileSize, DC.status,
            CASE WHEN DC.dcSvcClass = svcClassId THEN DC.status
                 WHEN DC.fileSystem = 0 THEN
                  (SELECT UNIQUE decode(nvl(SubRequest.status, -1), -1, -1, dc.status)
                   FROM SubRequest,
                       (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
                        SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
                        SELECT id, svcClass FROM StagePrepareToUpdateRequest UNION ALL
                        SELECT id, svcClass FROM StageRepackRequest UNION ALL
                        SELECT id, svcClass FROM StageGetRequest) Req
                         WHERE SubRequest.CastorFile = CastorFile.id
                           AND request = Req.id
                           AND svcClass = svcClassId)
                 WHEN DC.id IS NULL THEN
                (SELECT UNIQUE decode(SubRequest.status, 0, 2, 3, 2, -1)
                   FROM SubRequest,
                       (SELECT id, svcClass FROM StagePrepareToGetRequest UNION ALL
                        SELECT id, svcClass FROM StagePrepareToPutRequest UNION ALL
                        SELECT id, svcClass FROM StagePrepareToUpdateRequest UNION ALL
                        SELECT id, svcClass FROM StageRepackRequest UNION ALL
                        SELECT id, svcClass FROM StageGetRequest) Req
                         WHERE SubRequest.CastorFile = CastorFile.id
                           AND request = Req.id
                           AND svcClass = svcClassId) END AS srStatus,
            DC.machine, DC.mountPoint,
            DC.nbCopyAccesses, CastorFile.lastKnownFileName
       FROM CastorFile,
            (SELECT DiskCopy.id, DiskCopy.path, DiskCopy.status, DiskServer.name AS machine, FileSystem.mountPoint,
                    DiskPool2SvcClass.child AS dcSvcClass, DiskCopy.filesystem, DiskCopy.CastorFile, DiskCopy.nbCopyAccesses
               FROM FileSystem, DiskServer, DiskPool2SvcClass,
                    (SELECT id, status, filesystem, castorFile, path, nbCopyAccesses FROM DiskCopy
                      WHERE CastorFile IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
                        AND status IN (0, 1, 2, 4, 5, 6, 7, 10, 11)
                            -- search for diskCopies not GCCANDIDATE or BEINGDELETED
                      GROUP BY (id, status, filesystem, castorfile, path, nbCopyAccesses)) DiskCopy
              WHERE FileSystem.id(+) = DiskCopy.fileSystem
                AND nvl(FileSystem.status, 0) = 0 -- PRODUCTION
                AND DiskServer.id(+) = FileSystem.diskServer
                AND nvl(DiskServer.status, 0) = 0 -- PRODUCTION
                AND DiskPool2SvcClass.parent(+) = FileSystem.diskPool) DC
      WHERE CastorFile.id IN (SELECT /*+ CARDINALITY(cfidTable 5) */ * FROM TABLE(cfs) cfidTable)
        AND CastorFile.id = DC.castorFile(+))  -- search for valid diskcopy
      WHERE srStatus != -1
      ORDER BY fileid, nshost;
   END IF;
END;
/


/*
 * PL/SQL method implementing the stager_qry based on file name
 */
CREATE OR REPLACE PROCEDURE fileNameStageQuery
 (fn IN VARCHAR2,
  svcClassId IN INTEGER,
  maxNbResponses IN INTEGER,
  result OUT castor.QueryLine_Cur) AS
  cfIds "numList";
BEGIN
  IF substr(fn, -1, 1) = '/' THEN  -- files in a 'subdirectory'
    SELECT /*+ INDEX(DiskPool2SvcClass I_DiskPool2SvcClass_C) */ CastorFile.id
      BULK COLLECT INTO cfIds
      FROM DiskCopy, FileSystem, DiskPool2SvcClass, CastorFile
     WHERE CastorFile.lastKnownFileName LIKE fn||'%'
       AND DiskCopy.castorFile = CastorFile.id
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND (DiskPool2SvcClass.child = svcClassId OR svcClassId = 0)
       AND ROWNUM <= maxNbResponses + 1;
  ELSE  -- exact match
    SELECT /*+ INDEX(DiskPool2SvcClass I_DiskPool2SvcClass_C) */ CastorFile.id
      BULK COLLECT INTO cfIds
      FROM DiskCopy, FileSystem, DiskPool2SvcClass, CastorFile
     WHERE CastorFile.lastKnownFileName = canonicalizePath(fn)
       AND DiskCopy.castorFile = CastorFile.id
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskPool = DiskPool2SvcClass.parent
       AND (DiskPool2SvcClass.child = svcClassId OR svcClassId = 0)
       AND ROWNUM <= maxNbResponses + 1;
  END IF;
  IF cfIds.COUNT > maxNbResponses THEN
    -- We have too many rows, we just give up
    raise_application_error(-20102, 'Too many matching files');
  END IF;
  internalStageQuery(cfIds, svcClassId, result);
END;
/


/*
 * PL/SQL method implementing the stager_qry based on file id
 */
CREATE OR REPLACE PROCEDURE fileIdStageQuery
 (fid IN NUMBER,
  nh IN VARCHAR2,
  svcClassId IN INTEGER,
  fileName IN VARCHAR2,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
  currentFileName VARCHAR2(2048);
  nsHostName VARCHAR2(2048);
BEGIN
  -- Get the stager/nsHost configuration option
  nsHostName := getConfigOption('stager', 'nsHost', nh);
  -- Extract CastorFile ids from the fileid
  SELECT id BULK COLLECT INTO cfs FROM CastorFile 
   WHERE fileId = fid AND nshost = nsHostName;
  -- Check and fix when needed the LastKnownFileNames
  IF (cfs.COUNT > 0) THEN
    SELECT lastKnownFileName INTO currentFileName
      FROM CastorFile
     WHERE id = cfs(cfs.FIRST);
    IF currentFileName != fileName THEN
      UPDATE CastorFile SET lastKnownFileName = fileName
       WHERE id = cfs(cfs.FIRST);
      COMMIT;
    END IF;
  END IF;
  -- Finally issue the actual query
  internalStageQuery(cfs, svcClassId, result);
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
  SELECT sr.castorfile BULK COLLECT INTO cfs
    FROM SubRequest sr,
         (SELECT id
            FROM StagePreparetogetRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StagePreparetoputRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM stageGetRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM stagePutRequest
           WHERE reqid LIKE rid
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
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StagePreparetoupdateRequest
           WHERE reqid LIKE rid
          UNION ALL
          SELECT id
            FROM StageRepackRequest
           WHERE reqid LIKE rid
          );
  IF reqs.COUNT > 0 THEN
    FORALL i IN reqs.FIRST..reqs.LAST
      UPDATE SubRequest SET getNextStatus = 2  -- GETNEXTSTATUS_NOTIFIED
       WHERE getNextStatus = 1  -- GETNEXTSTATUS_FILESTAGED
         AND request = reqs(i)
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
    FORALL i IN reqs.FIRST..reqs.LAST
      UPDATE SubRequest SET getNextStatus = 2  -- GETNEXTSTATUS_NOTIFIED
       WHERE getNextStatus = 1  -- GETNEXTSTATUS_FILESTAGED
         AND request = reqs(i)
      RETURNING castorfile BULK COLLECT INTO cfs;
    internalStageQuery(cfs, svcClassId, result);
  ELSE
    notfound := 1;
  END IF;
END;
/


/*
 * PL/SQL method implementing the diskPoolQuery when listing pools
 */
CREATE OR REPLACE PROCEDURE describeDiskPools
(svcClassName IN VARCHAR2, reqEuid IN INTEGER, reqEgid IN INTEGER,
 res OUT NUMBER, result OUT castor.DiskPoolsQueryLine_Cur) AS
BEGIN
  -- We use here analytic functions and the grouping sets functionnality to
  -- get both the list of filesystems and a summary per diskserver and per
  -- diskpool. The grouping analytic function also allows to mark the summary
  -- lines for easy detection in the C++ code
  IF svcClassName IS NULL THEN
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
            grouping(fs.mountPoint) AS IsFSGrouped,
              dp.name,
             ds.name, ds.status, fs.mountPoint,
             sum(decode(sign(fs.free - fs.minAllowedFreeSpace * fs.totalSize), -1, 0,
	         fs.free - fs.minAllowedFreeSpace * fs.totalSize)) AS freeSpace,
             sum(fs.totalSize),
             fs.minFreeSpace, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp
       WHERE dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         GROUP BY grouping sets(
             (dp.name, ds.name, ds.status, fs.mountPoint,
               decode(sign(fs.free - fs.minAllowedFreeSpace * fs.totalSize), -1, 0,
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
              dp.name,
             ds.name, ds.status, fs.mountPoint,
             sum(decode(sign(fs.free - fs.minAllowedFreeSpace * fs.totalSize), -1, 0,
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
               decode(sign(fs.free - fs.minAllowedFreeSpace * fs.totalSize), -1, 0,
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
       AND (sc.name = svcClassName OR svcClassName IS NULL);
  END IF;
END;
/



/*
 * PL/SQL method implementing the diskPoolQuery for a given pool
 */
CREATE OR REPLACE PROCEDURE describeDiskPool
(diskPoolName IN VARCHAR2, svcClassName IN VARCHAR2,
 res OUT NUMBER, result OUT castor.DiskPoolQueryLine_Cur) AS
BEGIN
  -- We use here analytic functions and the grouping sets functionnality to
  -- get both the list of filesystems and a summary per diskserver and per
  -- diskpool. The grouping analytic function also allows to mark the summary
  -- lines for easy detection in the C++ code
  IF svcClassName IS NULL THEN
    OPEN result FOR
      SELECT grouping(ds.name) AS IsDSGrouped,
             grouping(fs.mountPoint) AS IsGrouped,
             ds.name, ds.status, fs.mountPoint,
             sum(fs.free - fs.minAllowedFreeSpace * fs.totalSize) AS freeSpace,
             sum(fs.totalSize),
             fs.minFreeSpace, fs.maxFreeSpace, fs.status
        FROM FileSystem fs, DiskServer ds, DiskPool dp
       WHERE dp.id = fs.diskPool
         AND ds.id = fs.diskServer
         AND dp.name = diskPoolName
         GROUP BY grouping sets(
             (ds.name, ds.status, fs.mountPoint,
               fs.free - fs.minAllowedFreeSpace * fs.totalSize,
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
             sum(fs.free - fs.minAllowedFreeSpace * fs.totalSize) AS freeSpace,
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
               fs.free - fs.minAllowedFreeSpace * fs.totalSize,
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
       AND (sc.name = svcClassName OR svcClassName IS NULL);
  END IF;
END;
/
