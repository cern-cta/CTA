/*******************************************************************
 *
 * @(#)$RCSfile: oracleGC.sql,v $ $Revision: 1.643 $ $Date: 2008/03/12 18:15:46 $ $Author: itglp $
 *
 * PL/SQL code for stager cleanup and garbage collecting
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/


/* PL/SQL declaration for the castorGC package */
CREATE OR REPLACE PACKAGE castorGC AS
  TYPE GCItem IS RECORD (dcId INTEGER, fileSize NUMBER, utility NUMBER);
  TYPE GCItem_Table is TABLE OF GCItem;
  TYPE GCItem_Cur IS REF CURSOR RETURN GCItem;
  TYPE SelectFiles2DeleteLine IS RECORD (
        path VARCHAR2(2048),
        id NUMBER,
	fileId NUMBER,
	nsHost VARCHAR2(2048));
  TYPE SelectFiles2DeleteLine_Cur IS REF CURSOR RETURN SelectFiles2DeleteLine;
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
    -- if any of the service classes to which we belong (normally a single one)
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
           lastAccessTime = getTime()   -- see comment below on the status = 9 condition
     WHERE fileSystem = fs.id 
       AND (
             (status = 7 AND NOT EXISTS  -- INVALID
               (SELECT 'x' FROM SubRequest
                 WHERE SubRequest.diskcopy = DiskCopy.id
                   AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 12, 13, 14)))  -- All but FINISHED, FAILED*, ARCHIVED
          OR (status = 9 AND lastAccessTime < getTime() - 1800))
             -- for failures recovery we also take all DiskCopies which were already
             -- selected but got stuck somehow and didn't get removed after 30 mins. 
    RETURNING id BULK COLLECT INTO dcIds;
    COMMIT;
    IF dontGC = 1 THEN
      -- nothing else to do, exit the loop and eventually return the list of INVALID
      EXIT;
    END IF;
    IF dcIds.COUNT > 0 THEN
      SELECT SUM(fileSize) INTO freed
        FROM CastorFile, DiskCopy
       WHERE DiskCopy.castorFile = CastorFile.id
         AND DiskCopy.id IN (SELECT * FROM TABLE(dcIds));
    ELSE
      freed := 0;
    END IF;
    -- Get the amount of space to liberate
    SELECT maxFreeSpace * totalSize - free
      INTO toBeFreed
      FROM FileSystem
     WHERE id = fs.id;
    -- Check whether we're done with the INVALID
    IF toBeFreed <= freed THEN
      EXIT;
    END IF;
    -- Loop on file deletions
    FOR dc IN (SELECT id, castorFile FROM DiskCopy
                WHERE fileSystem = fs.id
                  AND status = 0  -- STAGED
                  AND NOT EXISTS (
                      SELECT 'x' FROM SubRequest 
                       WHERE DiskCopy.status = 0 AND diskcopy = DiskCopy.id 
                         AND SubRequest.status IN (0, 1, 2, 3, 4, 5, 6, 12, 13, 14))   -- All but FINISHED, FAILED*, ARCHIVED
                  ORDER BY gcWeight ASC) LOOP
      -- Mark the DiskCopy
      UPDATE DiskCopy SET status = 9  -- BEINGDELETED
       WHERE id = dc.id;
      -- Update toBeFreed
      SELECT fileSize INTO deltaFree FROM CastorFile WHERE id = dc.castorFile;
      freed := freed + deltaFree;
      -- Shall we continue ?
      IF toBeFreed <= freed THEN
        EXIT;
      END IF;
    END LOOP;
    COMMIT;
  END LOOP;
  
  -- now select all the BEINGDELETED diskcopies in this diskserver for the gcDaemon
  OPEN files FOR
    SELECT FileSystem.mountPoint||DiskCopy.path, DiskCopy.id,
	         Castorfile.fileid, Castorfile.nshost
      FROM CastorFile, DiskCopy, FileSystem, DiskServer
     WHERE DiskCopy.status = 9  -- BEINGDELETED
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
  cfId NUMBER;
BEGIN
  IF dcIds.COUNT > 0 THEN
    -- Loop over the deleted files; first use FORALL for bulk operation
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      DELETE FROM Id2Type WHERE id = dcIds(i);
    FORALL i IN dcIds.FIRST .. dcIds.LAST
      DELETE FROM DiskCopy WHERE id = dcIds(i);
    -- then use a normal loop to clean castorFiles
    FOR i IN dcIds.FIRST .. dcIds.LAST LOOP
      SELECT castorFile INTO cfId
        FROM DiskCopy
       WHERE id = dcIds(i);
      deleteCastorFile(cfId);
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
    cursor s IS '||sel||'
    ids "numList";
  BEGIN
    OPEN s;
    LOOP
      FETCH s BULK COLLECT INTO ids LIMIT 10000;
      FORALL i IN ids.FIRST..ids.LAST
        DELETE FROM '||tab||' WHERE id = ids(i);
      COMMIT;
      EXIT WHEN s%NOTFOUND;
    END LOOP;
  END;';
END;


/* PL/SQL method to delete a group of requests */
CREATE OR REPLACE PROCEDURE deleteRequests
  (tab IN VARCHAR2, typ IN VARCHAR2, cleanTab IN VARCHAR2) AS
BEGIN
  -- delete client id2type
  bulkDelete(
    'SELECT client FROM '||tab||', '||cleanTab||'
       WHERE '||tab||'.id = '||cleanTab||'.id;',
    'Id2Type');
  -- delete client itself
  bulkDelete(
    'SELECT client FROM '||tab||', '||cleanTab||'
       WHERE '||tab||'.id = '||cleanTab||'.id;',
    'Client');
  -- delete request id2type
  bulkDelete(
    'SELECT id FROM '||cleanTab||' WHERE type = '||typ||';',
    'Id2Type');
  -- delete request itself
  bulkDelete(
    'SELECT id FROM '||cleanTab||' WHERE type = '||typ||';',
    tab);
END;


/* internal procedure to efficiently delete all requests in the cleanTab table */
CREATE OR REPLACE PROCEDURE internalCleaningProc(cleanTab IN VARCHAR) AS
BEGIN
  -- Delete SubRequests
    -- Starting with id
  bulkDelete(
     'SELECT SubRequest.id FROM SubRequest, '||cleanTab||'
       WHERE SubRequest.request = '||cleanTab||'.id;',
     'Id2Type');
    -- Then the subRequests
  bulkDelete(
     'SELECT SubRequest.id FROM SubRequest, '||cleanTab||'
       WHERE SubRequest.request = '||cleanTab||'.id;',
     'SubRequest');
  -- Delete Request + Clients
    ---- Get ----
  deleteRequests('StageGetRequest', '35', cleanTab);
    ---- Put ----
  deleteRequests('StagePutRequest', '40', cleanTab);
    ---- Update ----
  deleteRequests('StageUpdateRequest', '44', cleanTab);
    ---- Rm ----
  deleteRequests('StageRmRequest', '42', cleanTab);
    ---- PrepareToGet -----
  deleteRequests('StagePrepareToGetRequest', '36', cleanTab);
    ---- PrepareToPut ----
  deleteRequests('StagePrepareToPutRequest', '37', cleanTab);
    ---- PrepareToUpdate ----
  deleteRequests('StagePrepareToUpdateRequest', '38', cleanTab);
    ---- PutDone ----
  deleteRequests('StagePutDoneRequest', '39', cleanTab);
    ---- Rm ----
  deleteRequests('StageRmRequest', '42', cleanTab);
    ---- Repack ----
  deleteRequests('StageRepackRequest', '119', cleanTab);
    ---- DiskCopyReplica ----
  deleteRequests('StageDiskCopyReplicaRequest', '133', cleanTab);
  EXECUTE IMMEDIATE 'TRUNCATE TABLE '||cleanTab;
END;


/* Search and delete too old archived subrequests and their requests */
CREATE OR REPLACE PROCEDURE deleteArchivedRequests(timeOut IN NUMBER) AS
BEGIN
  INSERT /*+ APPEND */ INTO ArchivedRequestCleaning
    SELECT UNIQUE request, type 
      FROM SubRequest, id2type
     WHERE subrequest.request = id2type.id
     GROUP BY request, type
    HAVING min(status) = 11
       AND max(status) = 11  -- only ARCHIVED requests are taken into account here
       AND max(lastModificationTime) < getTime() - timeOut;
  COMMIT;
  internalCleaningProc('ArchivedRequestCleaning');
END;

/* Search and delete "out of date" subrequests and their requests */
CREATE OR REPLACE PROCEDURE deleteOutOfDateRequests(timeOut IN NUMBER) AS
BEGIN
  INSERT /*+ APPEND */ INTO OutOfDateRequestCleaning
    SELECT UNIQUE request, type 
      FROM SubRequest, id2type
     WHERE subrequest.request = id2type.id
     GROUP BY request, type
    HAVING min(status) >= 8
       AND max(status) <= 12 -- repack 
       AND max(lastModificationTime) < getTime() - timeOut;
  COMMIT;
  internalCleaningProc('OutOfDateRequestCleaning');
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
   WHERE id IN (SELECT /*+ cardinality(ids 5) */ * FROM TABLE(dcIds) ids);
  -- drop the DiskCopies
  DELETE FROM Id2Type WHERE id IN (SELECT * FROM TABLE(dcIds));
  DELETE FROM DiskCopy WHERE id IN (SELECT * FROM TABLE(dcIds));
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
CREATE OR REPLACE PROCEDURE deleteOutOfDateStageOutDcs
(timeOut IN NUMBER,
 dropped OUT castor.FileEntry_Cur,
 putDones OUT castor.FileEntry_Cur) AS
BEGIN
  -- Deal with old DiskCopies in STAGEOUT/WAITFS that have a
  -- prepareToPut. The rule is to drop the ones with 0 fileSize
  -- and issue a putDone for the others
  FOR cf IN (SELECT c.filesize, c.id, c.fileId, c.nsHost, d.fileSystem, d.id AS dcid
               FROM DiskCopy d, SubRequest s, StagePrepareToPutRequest r, Castorfile c
              WHERE d.castorfile = s.castorfile
                AND s.request = r.id
                AND c.id = d.castorfile
                AND d.creationtime < getTime() - timeOut
                AND d.status IN (5, 6, 11)) LOOP -- WAITFS, STAGEOUT, WAITFS_SCHEDULING
    IF 0 = cf.fileSize THEN
      -- here we invalidate the diskcopy and let the GC run
      UPDATE DiskCopy SET status = 7 WHERE id = cf.dcid;
      INSERT INTO OutOfDateStageOutDropped VALUES (cf.fileId, cf.nsHost);
    ELSE
      -- here we issue a putDone
      putDoneFunc(cf.id, cf.fileSize, 2); -- context 2 : real putDone
      INSERT INTO OutOfDateStageOutPutDone VALUES (cf.fileId, cf.nsHost);
    END IF;
  END LOOP;
  OPEN dropped FOR SELECT * FROM OutOfDateStageOutDropped;
  OPEN putDones FOR SELECT * FROM OutOfDateStageOutPutDone;
END;

/* Deal with old diskCopies in WAITTAPERECALL */
CREATE OR REPLACE PROCEDURE deleteOutOfDateRecallDcs
(timeOut IN NUMBER,
 dropped OUT castor.FileEntry_Cur) AS
BEGIN
  FOR cf IN (SELECT c.filesize, c.id, c.fileId, c.nsHost, d.fileSystem, d.id AS dcid
               FROM DiskCopy d, SubRequest s, StagePrepareToPutRequest r, Castorfile c
              WHERE d.castorfile = s.castorfile
                AND s.request = r.id
                AND c.id = d.castorfile
                AND d.creationtime < getTime() - timeOut
                AND d.status = 2) LOOP -- WAITTAPERECALL
    -- cancel recall and fail subrequests
    cancelRecall(cf.id, cf.dcId, 7); -- FAILED
    INSERT INTO OutOfDateRecallDropped VALUES (cf.fileId, cf.nsHost);
  END LOOP;
  OPEN dropped FOR SELECT * FROM OutOfDateRecallDropped;
END;



/*
 * Runs a table shrink operation for maintenance purposes 
 */
CREATE OR REPLACE PROCEDURE tableShrink AS
BEGIN
  -- Loop over all tables which support row movement and recover space from 
  -- the object and all dependant objects. We deliberately ignore tables 
  -- with function based indexes here as the 'shrink space' option is not 
  -- supported.
  FOR a IN (SELECT table_name FROM user_tables
             WHERE row_movement = 'ENABLED'
               AND table_name NOT IN (
                 SELECT table_name FROM user_indexes
                  WHERE index_type LIKE 'FUNCTION-BASED%')
               AND temporary = 'N')
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||' SHRINK SPACE CASCADE';
  END LOOP;
END;
  


/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR a IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name IN ('TABLESHRINKJOB', 
	                        'BULKCHECKFSBACKINPRODJOB'))
  LOOP
    DBMS_SCHEDULER.DROP_JOB(a.job_name, TRUE);
  END LOOP;

  -- Creates a db job to be run every day executing the tableShrink procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'tableShrinkJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN tableShrink(); END;',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=DAILY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Database maintenance');

  -- Creates a db job to be run every 5 minutes executing the bulkCheckFSBackInProd
  -- procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'bulkCheckFSBackInProdJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN bulkCheckFSBackInProd(); END;',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'Bulk operation to processing filesystem state changes');
END;
