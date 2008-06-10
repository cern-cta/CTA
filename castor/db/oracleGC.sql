/*******************************************************************
 *
 * @(#)$RCSfile: oracleGC.sql,v $ $Revision: 1.655 $ $Date: 2008/06/10 14:43:21 $ $Author: itglp $
 *
 * PL/SQL code for stager cleanup and garbage collecting
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/


/* PL/SQL declaration for the castorGC package */
CREATE OR REPLACE PACKAGE castorGC AS
  TYPE SelectFiles2DeleteLine IS RECORD (
        path VARCHAR2(2048),
        id NUMBER,
        fileId NUMBER,
        nsHost VARCHAR2(2048));
  TYPE SelectFiles2DeleteLine_Cur IS REF CURSOR RETURN SelectFiles2DeleteLine;
  TYPE JobLogEntry IS RECORD (
    fileid NUMBER,
    nshost VARCHAR2(2048),
    operation INTEGER);
  TYPE JobLogEntry_Cur IS REF CURSOR RETURN JobLogEntry; 
END castorGC;


/* PL/SQL method implementing selectFiles2Delete
   This is the standard garbage collector: it sorts STAGED
   diskcopies by gcWeight and selects them for deletion up to
   the desired free space watermark */
CREATE OR REPLACE PROCEDURE selectFiles2Delete(diskServerName IN VARCHAR2,
                                               files OUT castorGC.SelectFiles2DeleteLine_Cur) AS
  dcIds "numList";
  freed INTEGER;
  deltaFree INTEGER;
  toBeFreed INTEGER;
  dontGC INTEGER;
BEGIN
  -- First of all, check if we have GC enabled
  dontGC := 0;
  FOR sc IN (SELECT gcEnabled 
               FROM SvcClass, DiskPool2SvcClass D2S, DiskServer, FileSystem
              WHERE SvcClass.id = D2S.child
                AND D2S.parent = FileSystem.diskPool
                AND FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = diskServerName) LOOP
    -- If any of the service classes to which we belong (normally a single one)
    -- says don't GC, we don't GC STAGED files.
    IF sc.gcEnabled = 0 THEN
      dontGC := 1;
      EXIT;
    END IF;
  END LOOP;
  -- Loop on all concerned fileSystems
  FOR fs IN (SELECT FileSystem.id
               FROM FileSystem, DiskServer
              WHERE FileSystem.diskServer = DiskServer.id
                AND DiskServer.name = diskServerName) LOOP
    -- First take the INVALID diskcopies, they have to go in any case
    UPDATE DiskCopy
       SET status = 9, -- BEING_DELETED
           lastAccessTime = getTime() -- See comment below on the status = 9 condition
     WHERE fileSystem = fs.id 
       AND (
             (status = 7 AND NOT EXISTS -- INVALID
               (SELECT 'x' FROM SubRequest
                 WHERE SubRequest.diskcopy = DiskCopy.id
                   AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 12, 13, 14))) -- All but FINISHED, FAILED*, ARCHIVED
        OR (status = 9 AND lastAccessTime < getTime() - 1800))
        -- For failures recovery we also take all DiskCopies which were already
        -- selected but got stuck somehow and didn't get removed after 30 mins. 
    RETURNING id BULK COLLECT INTO dcIds;
    COMMIT;

    -- Continue processing but with STAGED files.
    IF dontGC = 0 THEN
      -- Determine the space that would be freed if the INVALID files selected above
      -- were to be removed
      IF dcIds.COUNT > 0 THEN
        SELECT SUM(fileSize) INTO freed
          FROM CastorFile, DiskCopy
         WHERE DiskCopy.castorFile = CastorFile.id
           AND DiskCopy.id IN 
             (SELECT /*+ CARDINALITY(fsidTable 5) */ * 
                FROM TABLE(dcIds) dcidTable);
      ELSE
        freed := 0;
      END IF;
      -- Get the amount of space to be liberated
      SELECT decode(sign(maxFreeSpace * totalSize - free), -1, 0, maxFreeSpace * totalSize - free)
        INTO toBeFreed
        FROM FileSystem
       WHERE id = fs.id;
      -- If space is still required even after removal of INVALID files, consider
      -- removing STAGED files until we are below the free space watermark
      IF freed < toBeFreed THEN
        -- Loop on file deletions
        FOR dc IN (SELECT id, castorFile FROM DiskCopy
                    WHERE fileSystem = fs.id
                      AND status = 0 -- STAGED
                      AND NOT EXISTS (
                          SELECT 'x' FROM SubRequest 
                           WHERE DiskCopy.status = 0 AND diskcopy = DiskCopy.id 
                             AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 12, 13, 14)) -- All but FINISHED, FAILED*, ARCHIVED
                      ORDER BY gcWeight ASC) LOOP
          -- Mark the DiskCopy
          UPDATE DiskCopy SET status = 9 -- BEINGDELETED
           WHERE id = dc.id;
          -- Update toBeFreed
          SELECT fileSize INTO deltaFree FROM CastorFile WHERE id = dc.castorFile;
          freed := freed + deltaFree;
          -- Shall we continue ?
          IF toBeFreed <= freed THEN
            EXIT;
          END IF;
        END LOOP;
      END IF;
      COMMIT;
    END IF;
  END LOOP;
      
  -- Now select all the BEINGDELETED diskcopies in this diskserver for the gcDaemon
  OPEN files FOR
    SELECT /*+ INDEX(CastorFile I_CastorFile_ID) */ FileSystem.mountPoint || DiskCopy.path, DiskCopy.id,
	   Castorfile.fileid, Castorfile.nshost
      FROM CastorFile, DiskCopy, FileSystem, DiskServer
     WHERE DiskCopy.status = 9 -- BEINGDELETED
       AND DiskCopy.castorfile = CastorFile.id
       AND DiskCopy.fileSystem = FileSystem.id
       AND FileSystem.diskServer = DiskServer.id
       AND DiskServer.name = diskServerName;
END;

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
    -- list the castorfiles to be cleaned up afterwards
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      INSERT INTO filesDeletedProcHelper VALUES
           ((SELECT castorFile FROM DiskCopy
              WHERE id = dcIds(i)));
    -- Loop over the deleted files; first use FORALL for bulk operation
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      DELETE FROM Id2Type WHERE id = dcIds(i);
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      DELETE FROM DiskCopy WHERE id = dcIds(i);
    -- then use a normal loop to clean castorFiles
    FOR cf IN (SELECT DISTINCT(cfId) FROM filesDeletedProcHelper) LOOP
      deleteCastorFile(cf.cfId);
    END LOOP;
  END IF;
  OPEN fileIds FOR SELECT * FROM FilesDeletedProcOutput;
END;

/*
 * PL/SQL method removing completely a file from the stager
 * including all its related objects (diskcopy, tapecopy, segments...)
 * The given files are supposed to already have been removed from the
 * name server
 * Note that we don't increase the freespace of the fileSystem.
 * This is done by the monitoring daemon, that knows the
 * exact amount of free space.
 * cfIds gives the list of files to delete.
 */
CREATE OR REPLACE PROCEDURE filesClearedProc(cfIds IN castor."cnumList") AS
  dcIds "numList";
BEGIN
  IF cfIds.COUNT <= 0 THEN
    RETURN;
  END IF;
  -- first convert the input array into a temporary table
  FORALL i IN cfIds.FIRST..cfIds.LAST
    INSERT INTO FilesClearedProcHelper (cfId) VALUES (cfIds(i));
  -- delete the DiskCopies in bulk
  SELECT id BULK COLLECT INTO dcIds
    FROM Diskcopy WHERE castorfile IN (SELECT cfId FROM FilesClearedProcHelper);
  FORALL i IN dcIds.FIRST .. dcIds.LAST
    DELETE FROM Id2Type WHERE id = dcIds(i);
  FORALL i IN dcIds.FIRST .. dcIds.LAST
    DELETE FROM DiskCopy WHERE id = dcIds(i);
  -- put SubRequests into FAILED (for non FINISHED ones)
  UPDATE SubRequest
     SET status = 7,  -- FAILED
         errorCode = 16,  -- EBUSY
         errorMessage = 'Request canceled by another user request'
   WHERE castorfile IN (SELECT cfId FROM FilesClearedProcHelper)
     AND status IN (0, 1, 2, 3, 4, 5, 6, 12, 13, 14);
  -- Loop over the deleted files for cleaning the tape copies
  FOR i in cfIds.FIRST .. cfIds.LAST LOOP
    deleteTapeCopies(cfIds(i));
  END LOOP;
  -- Finally drop castorFiles in bulk
  FORALL i IN cfIds.FIRST .. cfIds.LAST
    DELETE FROM Id2Type WHERE id = cfIds(i);
  FORALL i IN cfIds.FIRST .. cfIds.LAST
    DELETE FROM CastorFile WHERE id = cfIds(i);
END;

/* PL/SQL method implementing filesDeletionFailedProc */
CREATE OR REPLACE PROCEDURE filesDeletionFailedProc
(dcIds IN castor."cnumList") AS
  cfId NUMBER;
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- Loop over the files
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      UPDATE DiskCopy SET status = 4 -- FAILED
       WHERE id = dcIds(i);
  END IF;
END;



/* PL/SQL method implementing nsFilesDeletedProc */
CREATE OR REPLACE PROCEDURE nsFilesDeletedProc
(nsh IN VARCHAR2,
 fileIds IN castor."cnumList",
 orphans OUT castor.IdRecord_Cur) AS
  unused NUMBER;
BEGIN
  IF fileIds.COUNT <= 0 THEN
    RETURN;
  END IF;
  -- Loop over the deleted files and split the orphan ones
  -- from the normal ones
  FOR fid in fileIds.FIRST .. fileIds.LAST LOOP
    BEGIN
      SELECT id INTO unused FROM CastorFile
       WHERE fileid = fileIds(fid) AND nsHost = nsh;
      stageForcedRm(fileIds(fid), nsh, unused);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- this file was dropped from nameServer AND stager
      -- and still exists on disk. We put it into the list
      -- of orphan fileids to return
      INSERT INTO NsFilesDeletedOrphans VALUES(fileIds(fid));
    END;
  END LOOP;
  -- return orphan ones
  OPEN orphans FOR SELECT * FROM NsFilesDeletedOrphans;
END;


/* PL/SQL method implementing stgFilesDeletedProc */
CREATE OR REPLACE PROCEDURE stgFilesDeletedProc
(dcIds IN castor."cnumList",
 stgOrphans OUT castor.IdRecord_Cur) AS
  unused NUMBER;
BEGIN
  -- Nothing to do
  IF dcIds.COUNT <= 0 THEN
    RETURN;
  END IF;
  -- Insert diskcopy ids into a temporary table
  FORALL i IN dcIds.FIRST..dcIds.LAST
   INSERT INTO StgFilesDeletedOrphans VALUES(dcIds(i));
  -- Return a list of diskcopy ids which no longer exist
  OPEN stgOrphans FOR
    SELECT diskCopyId FROM StgFilesDeletedOrphans
     WHERE NOT EXISTS (
        SELECT 'x' FROM DiskCopy
         WHERE id = diskCopyId);
END;


/** Cleanup job **/

/* A little generic method to delete efficiently */
CREATE OR REPLACE PROCEDURE bulkDelete(sel IN VARCHAR2, tab IN VARCHAR2) AS
BEGIN
  EXECUTE IMMEDIATE
  'DECLARE
    CURSOR s IS '||sel||'
    ids "numList";
  BEGIN
    OPEN s;
    LOOP
      FETCH s BULK COLLECT INTO ids LIMIT 10000;
      FORALL i IN ids.FIRST..ids.LAST
        DELETE FROM Id2Type WHERE id = ids(i);
      FORALL i IN ids.FIRST..ids.LAST
        DELETE FROM '||tab||' WHERE id = ids(i);
      COMMIT;
      EXIT WHEN s%NOTFOUND;
    END LOOP;
  END;';
END;

/* A generic method to delete requests of a given type */
CREATE OR REPLACE Procedure bulkDeleteRequests(reqType IN VARCHAR) AS
BEGIN
  -- first the clients
  bulkDelete('SELECT client FROM '|| reqType ||' R WHERE
    NOT EXISTS (SELECT ''x'' FROM SubRequest WHERE request = R.id);',
    'Client');
  -- then the requests: they could be merged to make it more efficient
  bulkDelete('SELECT id FROM '|| reqType ||' R WHERE
    NOT EXISTS (SELECT ''x'' FROM SubRequest WHERE request = R.id);',
    reqType);
  COMMIT;
END;

/* Search and delete old archived/failed subrequests and their requests */
CREATE OR REPLACE PROCEDURE deleteTerminatedRequests AS
  timeOut INTEGER;
  rate INTEGER;
BEGIN
  -- select requested timeout from configuration table
  SELECT TO_NUMBER(value) INTO timeOut FROM CastorConfig
   WHERE class = 'cleaning' AND key = 'terminatedRequestsTimeout' AND ROWNUM < 2;
  timeOut := timeOut*3600;
  -- get a rough estimate of the current request processing rate
  SELECT count(*) INTO rate
    FROM SubRequest
   WHERE status IN (9, 11)  -- FAILED_FINISHED, ARCHIVED
     AND lastModificationTime > getTime() - 1800;
  IF rate > 0 AND (500000 / rate * 1800) < timeOut THEN
    timeOut := 500000 / rate * 1800;  -- keep 500k requests max
  END IF;

  -- now delete the SubRequests
  bulkDelete('SELECT id FROM SubRequest WHERE status IN (9, 11)
                AND lastModificationTime < getTime() - '|| timeOut ||';',
             'SubRequest');
  COMMIT;
  
  -- and then related Requests + Clients
    ---- Get ----
  bulkDeleteRequests('StageGetRequest');
    ---- Put ----
  bulkDeleteRequests('StagePutRequest');
    ---- Update ----
  bulkDeleteRequests('StageUpdateRequest');
    ---- PrepareToGet -----
  bulkDeleteRequests('StagePrepareToGetRequest');
    ---- PrepareToPut ----
  bulkDeleteRequests('StagePrepareToPutRequest');
    ---- PrepareToUpdate ----
  bulkDeleteRequests('StagePrepareToUpdateRequest');
    ---- PutDone ----
  bulkDeleteRequests('StagePutDoneRequest');
    ---- Rm ----
  bulkDeleteRequests('StageRmRequest');
    ---- Repack ----
  bulkDeleteRequests('StageRepackRequest');
    ---- DiskCopyReplica ----
  bulkDeleteRequests('StageDiskCopyReplicaRequest');
END;


/* Search and delete old diskCopies in bad states */
CREATE OR REPLACE PROCEDURE deleteFailedDiskCopies(timeOut IN NUMBER) AS
  dcIds "numList";
  cfIds "numList";
  ct INTEGER;
BEGIN
  -- select INVALID diskcopies without filesystem (they can exist after a
  -- stageRm that came before the diskcopy had been created on disk) and ALL FAILED
  -- ones (coming from failed recalls or failed removals from the gcDaemon).
  -- Note that we don't select INVALID diskcopies from recreation of files
  -- because they are taken by the standard GC as they physically exist on disk.
  SELECT id
    BULK COLLECT INTO dcIds 
    FROM DiskCopy
   WHERE (status = 4 OR (status = 7 AND fileSystem = 0))
     AND creationTime < getTime() - timeOut;
  SELECT /*+ INDEX(DC I_DiskCopy_ID) */ UNIQUE castorFile
    BULK COLLECT INTO cfIds
    FROM DiskCopy DC
   WHERE id IN (SELECT /*+ CARDINALITY(ids 5) */ * FROM TABLE(dcIds) ids);
  -- drop the DiskCopies
  FORALL i IN dcIds.FIRST..dcIds.LAST
    DELETE FROM Id2Type WHERE id = dcIds(i);
  FORALL i IN dcIds.FIRST..dcIds.LAST
    DELETE FROM DiskCopy WHERE id = dcIds(i);
  COMMIT;
  -- maybe delete the CastorFiles if nothing is left for them
  IF cfIds.COUNT > 0 THEN
    ct := 0;
    FOR c IN cfIds.FIRST..cfIds.LAST LOOP
      deleteCastorFile(cfIds(c));
      ct := ct + 1;
      IF ct = 1000 THEN
        -- commit every 1000, don't pause
        ct := 0;
        COMMIT;
      END IF;
    END LOOP;
    COMMIT;
  END IF;
END;

/* Deal with old diskCopies in STAGEOUT */
CREATE OR REPLACE PROCEDURE deleteOutOfDateStageOutDCs(timeOut IN NUMBER) AS
BEGIN
  -- Deal with old DiskCopies in STAGEOUT/WAITFS. The rule is to drop
  -- the ones with 0 fileSize and issue a putDone for the others
  FOR cf IN (SELECT c.filesize, c.id, c.fileId, c.nsHost, d.fileSystem, d.id AS dcid
               FROM DiskCopy d, Castorfile c
              WHERE c.id = d.castorFile
                AND d.creationTime < getTime() - timeOut
                AND d.status IN (5, 6, 11) -- WAITFS, STAGEOUT, WAITFS_SCHEDULING
		AND NOT EXISTS (
		  SELECT 'x'
		    FROM SubRequest, Id2Type
		   WHERE castorFile = c.id
		     AND SubRequest.request = Id2Type.id
		     AND status IN (0, 1, 2, 3, 5, 6, 13, 14) -- all active
		     AND type NOT IN (37, 38))) LOOP -- ignore PrepareToPut, PrepareToUpdate
    IF 0 = cf.fileSize THEN
      -- here we invalidate the diskcopy and let the GC run
      UPDATE DiskCopy SET status = 7 WHERE id = cf.dcid;
      INSERT INTO CleanupJobLog VALUES (cf.fileId, cf.nsHost, 0);
    ELSE
      -- here we issue a putDone
      putDoneFunc(cf.id, cf.fileSize, 2); -- context 2 : real putDone. Missing PPut requests are ignored.
      INSERT INTO CleanupJobLog VALUES (cf.fileId, cf.nsHost, 1);
    END IF;
  END LOOP;
  COMMIT;
END;

/* Deal with stuck recalls */
CREATE OR REPLACE PROCEDURE restartStuckRecalls AS
BEGIN
  UPDATE Segment SET status = 0 WHERE status = 7 and tape IN
    (SELECT id from Tape WHERE tpmode = 0 AND status IN (0,6) AND id IN
      (SELECT tape FROM Segment WHERE status = 7));
  UPDATE Tape SET status = 1 WHERE tpmode = 0 AND status IN (0,6) AND id IN
    (SELECT tape FROM Segment WHERE status in (0,7));
END;


/* Runs cleanup operations and a table shrink for maintenance purposes */
CREATE OR REPLACE PROCEDURE cleanup AS
  t INTEGER;
BEGIN
  -- First perform some cleanup of old stuff:
  -- for each, read relevant timeout from configuration table
  SELECT TO_NUMBER(value) INTO t FROM CastorConfig
   WHERE class = 'cleaning' AND key = 'outOfDateStageOutDCsTimeout' AND ROWNUM < 2;
  deleteOutOfDateStageOutDCs(t*3600);
  SELECT TO_NUMBER(value) INTO t FROM CastorConfig
   WHERE class = 'cleaning' AND key = 'failedDCsTimeout' AND ROWNUM < 2;
  deleteFailedDiskCopies(t*3600);
  restartStuckRecalls();
  
  -- Loop over all tables which support row movement and recover space from 
  -- the object and all dependant objects. We deliberately ignore tables 
  -- with function based indexes here as the 'shrink space' option is not 
  -- supported.
  FOR t IN (SELECT table_name FROM user_tables
             WHERE row_movement = 'ENABLED'
               AND table_name NOT IN (
                 SELECT table_name FROM user_indexes
                  WHERE index_type LIKE 'FUNCTION-BASED%')
               AND temporary = 'N')
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '|| t.table_name ||' SHRINK SPACE CASCADE';
  END LOOP;
END;


/* PL/SQL method used by the stager to log what it has been done by the cleanup job */
CREATE OR REPLACE PROCEDURE dumpCleanupLogs(jobLog OUT castorGC.JobLogEntry_Cur) AS
  unused NUMBER;
BEGIN
  SELECT fileid INTO unused FROM CleanupJobLog WHERE ROWNUM < 2;
  -- if we got here, we have something in the log table, let's lock it and dump it
  LOCK TABLE CleanupJobLog IN EXCLUSIVE MODE;
  OPEN jobLog FOR
    SELECT * FROM CleanupJobLog;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- nothing to do
  NULL;
END;



/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('HOUSEKEEPINGJOB', 'CLEANUPJOB', 'BULKCHECKFSBACKINPRODJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;
  
  -- Creates a db job to be run every 20 minutes executing the deleteTerminatedRequests procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'houseKeepingJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN deleteTerminatedRequests(); END;',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=20',
      ENABLED         => TRUE,
      COMMENTS        => 'Cleaning of terminated requests');

  -- Creates a db job to be run twice a day executing the cleanup procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'cleanupJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN cleanup(); END;',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=HOURLY; INTERVAL=12',
      ENABLED         => TRUE,
      COMMENTS        => 'Database maintenance');

  -- Creates a db job to be run every 5 minutes executing the bulkCheckFSBackInProd procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'bulkCheckFSBackInProdJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN bulkCheckFSBackInProd(); END;',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'Bulk operation to processing filesystem state changes');
END;
