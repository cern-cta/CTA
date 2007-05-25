-- For logging
--DROP TABLE CleanupLogTable;
CREATE TABLE CleanupLogTable (fac NUMBER, message VARCHAR2(256), logDate NUMBER);

-- Cleanup old stage rm that were never deleted
DECLARE
  nothingLeft NUMBER;
  totalCount NUMBER;
  done NUMBER := 0;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (0, 'Cleanup old stage rm that were never deleted', getTime());
  COMMIT;
  SELECT COUNT(*) INTO totalCount FROM id2type, subrequest
   WHERE status = 6 AND request = id2type.id AND type = 42 AND subrequest.creationtime < getTime()-600000;
  LOOP
    nothingLeft := 1;
    -- do it 2000 by 2000 in order to not slow down the normal requests
    FOR sr IN (SELECT subrequest.id FROM id2type, subrequest
               WHERE status = 6 AND request = id2type.id
                 AND type = 42 AND rownum <= 2000
                 AND subrequest.creationtime < getTime()-600000) LOOP
      archiveSubReq(sr.id);
    nothingLeft := 0;
    END LOOP;
    -- commit between each bunch of 2000
    done := done + 2000;
    UPDATE CleanupLogTable
       SET message = 'Cleanup old stage rm that were never deleted' ||
           TO_CHAR(100*done/(totalCount+1), '999.99') || '% done', logDate = getTime()
     WHERE fac = 0;
    COMMIT;
    DBMS_LOCK.sleep(seconds => 1.0);
    IF nothingLeft = 1 THEN
      UPDATE CleanupLogTable
         SET message = 'Old stage rm that were never deleted were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
       WHERE fac = 0;
      COMMIT;
      EXIT;
    END IF;
  END LOOP;
END;


-- Remove DiskCopies stuck in status 9 (BEINGDELETED) because of deadlocks
-- Actually, we put them in GCCANDIDATE again and most of them will go to FAILED
-- But in case any needs file deletion, we are safe
DECLARE
  nbRowsUpdated NUMBER;
  totalCount NUMBER;
  done NUMBER := 0;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (1, 'Cleaning DiskCopies stuck in BEINGDELETED status', getTime());
  COMMIT;
  SELECT COUNT(*) INTO totalCount FROM DiskCopy WHERE status = 9 AND creationTime < getTime() - 600000;
  LOOP
    -- do it 5000 by 5000 in order to not slow down the normal requests
    UPDATE DiskCopy SET status = 8
     WHERE status = 9 AND creationTime < getTime() - 600000 AND ROWNUM <= 5000;
    nbRowsUpdated := SQL%ROWCOUNT;
    done := done + nbRowsUpdated;
    UPDATE CleanupLogTable
       SET message = 'Cleaning DiskCopies stuck in BEINGDELETED status' ||
           TO_CHAR(100*done/(totalCount+1), '999.99') || '% done', logDate = getTime()
     WHERE fac = 1;
    COMMIT;
    IF nbRowsUpdated = 0 THEN
      UPDATE CleanupLogTable
         SET message = 'DiskCopies stuck in BEINGDELETED status cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
       WHERE fac = 1;
      COMMIT;
      EXIT;
    END IF;
    DBMS_LOCK.sleep(seconds => 1.0);
  END LOOP;
END;

-- Cleaning the GC candidates and failed diskcopies that have no filesystem and will thus never be taken
CREATE OR REPLACE PROCEDURE filesDeletedProcNoFS
(dcIds IN castor."cnumList",
 fileIds OUT castor.FileList_Cur) AS
  cfId NUMBER;
  fsId NUMBER;
  fsize NUMBER;
  nb NUMBER;
  isFirst NUMBER;
BEGIN
 isFirst := 1;
 IF dcIds.COUNT > 0 THEN
  -- Loop over the deleted files
  FOR i in dcIds.FIRST .. dcIds.LAST LOOP
    SELECT castorFile, fileSystem INTO cfId, fsId
      FROM DiskCopy WHERE id = dcIds(i);
    LOOP
      DECLARE
        LockError EXCEPTION;
        PRAGMA EXCEPTION_INIT (LockError, -54);
      BEGIN
        -- Try to lock the Castor File and retrieve size
        SELECT fileSize INTO fsize FROM CastorFile where id = cfID FOR UPDATE NOWAIT;
        -- we got the lock, exit the loop
        EXIT;
      EXCEPTION WHEN LockError THEN
        -- then commit what we did to remove the dead lock
        COMMIT;
        -- and try again after some time
        dbms_lock.sleep(0.2);
      END;
    END LOOP;
    -- delete the DiskCopy
    DELETE FROM Id2Type WHERE id = dcIds(i);
    DELETE FROM DiskCopy WHERE id = dcIds(i);
    -- See whether the castorfile has no other DiskCopy
    SELECT count(*) INTO nb FROM DiskCopy
     WHERE castorFile = cfId;
    -- If any DiskCopy, give up
    IF nb = 0 THEN
      -- See whether the castorfile has any TapeCopy
      SELECT count(*) INTO nb FROM TapeCopy
       WHERE castorFile = cfId;
      -- If any TapeCopy, give up
      IF nb = 0 THEN
        -- See whether the castorfile has any pending SubRequest
        SELECT count(*) INTO nb FROM SubRequest
         WHERE castorFile = cfId
           AND status IN (0, 1, 2, 3, 4, 5, 6, 7, 10);   -- All but FINISHED, FAILED_FINISHED, ARCHIVED
        -- If any SubRequest, give up
        IF nb = 0 THEN
          DECLARE
            fid NUMBER;
            fc NUMBER;
            nsh VARCHAR2(2048);
          BEGIN
            -- Delete the CastorFile
            DELETE FROM id2Type WHERE id = cfId;
            DELETE FROM CastorFile WHERE id = cfId
              RETURNING fileId, nsHost, fileClass
              INTO fid, nsh, fc;
            -- check whether this file potentially had TapeCopies
            SELECT nbCopies INTO nb FROM FileClass WHERE id = fc;
            IF nb = 0 THEN
              -- This castorfile was created with no TapeCopy
              -- So removing it from the stager means erasing
              -- it completely. We should thus also remove it
              -- from the name server
              INSERT INTO FilesDeletedProcOutput VALUES (fid, nsh);
            END IF;
          END;
        END IF;
      END IF;
    END IF;
  END LOOP;
 END IF;
 OPEN fileIds FOR SELECT * FROM FilesDeletedProcOutput;
END;
DECLARE
  files castor."cnumList";
  emptyFileList castor."cnumList";
  totalCount NUMBER;
  i number;
  fileIds castor.FileList_Cur;
  done NUMBER := 0;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (2, 'Cleaning the GC candidates that have no filesystem', getTime());
  COMMIT;
  SELECT COUNT(UNIQUE id) INTO totalCount FROM DiskCopy where status IN (4,7,8) and (filesystem = 0 or filesystem is null);
  LOOP
    i := 1;
    files := emptyFileList; -- emptying collection
    -- do it 5000 by 5000 in order to not slow down the normal requests
    FOR dc IN (SELECT UNIQUE id FROM DiskCopy where status IN (4,7,8) and (filesystem = 0 or filesystem is null) AND ROWNUM <= 5000)
    LOOP
      files(i) := dc.id;
      i := i + 1;
    END LOOP;
    filesDeletedProcNoFS(files, fileIds);
    -- commit between each bunch of 5000
    done := done + 5000;
    UPDATE CleanupLogTable
       SET message = 'Cleaning the GC candidates that have no filesystem' ||
           TO_CHAR(100*done/(totalCount+1), '999.99') || '% done', logDate = getTime()
     WHERE fac = 2;
    COMMIT;
    DBMS_LOCK.sleep(seconds => 1.0);
    IF i = 1 THEN
      UPDATE CleanupLogTable
         SET message = 'GC candidates that have no filesystem were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
       WHERE fac = 2;
      COMMIT;
      EXIT;
    END IF;
  END LOOP;
END;
DROP PROCEDURE filesDeletedProcNoFS;

-- Cleaning the failed diskCopies with filesystem
DECLARE
  files castor."cnumList";
  emptyFileList castor."cnumList";
  totalCount NUMBER;
  dsCount NUMBER := 0;
  totalDsCount NUMBER;
  done NUMBER := 0;
  i number;
  fileIds castor.FileList_Cur;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (3, 'Cleaning failed diskCopies', getTime());
  COMMIT;
  SELECT COUNT(*) INTO totalCount FROM DiskCopy, FileSystem WHERE DiskCopy.status = 4 AND DiskCopy.fileSystem = fileSystem.id;
  SELECT count(*) INTO totalDsCount FROM DiskServer;
  -- Go diskserver by diskserver, otherwise we may have deadlocks in filesDeletedProc
  FOR ds in (SELECT id FROM DiskServer) LOOP
    BEGIN
      dsCount := dsCount + 1;
      LOOP
        i := 1;
        files := emptyFileList; -- emptying collection
        FOR dc IN (SELECT DiskCopy.id FROM DiskCopy, FileSystem
                    WHERE DiskCopy.status = 4 AND DiskCopy.fileSystem = fileSystem.id
                      AND fileSystem.diskServer = ds.id AND ROWNUM <= 5000)
        LOOP
          files(i) := dc.id;
          i := i + 1;
        END LOOP;
        IF i > 1 THEN
          filesDeletedProc(files, fileIds);
        END IF;
        done := done + i - 1;
        UPDATE CleanupLogTable
           SET message = 'Cleaning failed diskCopies' ||
               TO_CHAR(100*done/(totalCount+1), '999.99') || '% diskCopies cleaned, ' ||
               TO_CHAR(dsCount) || ' of ' || TO_CHAR(totalDsCount) || ' diskServers scanned', logDate = getTime()
         WHERE fac = 3;
        COMMIT;
        IF i = 1 THEN
          EXIT; -- exit inner loop
        END IF;
      END LOOP;
    END;
  END LOOP;
  UPDATE CleanupLogTable
     SET message = 'Failed diskCopies were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
   WHERE fac = 3;
  COMMIT;
END;

-- Cleaning the TapeCopies with no DiskCopies, due to bad cleaning after migration/recall error in rtcpcld
DECLARE
  nothingLeft NUMBER;
  totalCount NUMBER;
  done NUMBER := 0;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (4, 'Cleaning tapeCopies with no DiskCopies', getTime());
  COMMIT;
  SELECT COUNT(*) INTO totalCount FROM
               (SELECT t.castorfile AS c, t.id AS t, d.id AS d
                  FROM tapecopy t, diskcopy d WHERE t.castorfile = d.castorfile(+)) WHERE d IS NULL;
  LOOP
    nothingLeft := 1;
    -- do 10 castorfiles per commit in order to not slow down the normal requests
    -- The point is that we may delete many subrequests for one castorFile (1000s)
    FOR tc IN (SELECT c, t FROM
               (SELECT t.castorfile AS c, t.id AS t, d.id AS d
                  FROM tapecopy t, diskcopy d WHERE t.castorfile = d.castorfile(+))
               WHERE d IS NULL AND ROWNUM <= 10)
    LOOP
      nothingLeft := 0;
      DELETE FROM Stream2TapeCopy WHERE child = tc.t;
      DELETE FROM Id2Type WHERE id = tc.c;
      DELETE FROM Id2Type WHERE id = tc.t;
      DELETE FROM Castorfile WHERE id = tc.c;
      DELETE FROM TapeCopy WHERE id = tc.t;
      -- delete corresponding segments
      FOR s in (SELECT id FROM segment WHERE copy = tc.t) LOOP
        DELETE FROM Id2Type WHERE id = s.id;
        DELETE FROM Segment WHERE id = s.id;
      END LOOP;
      -- delete corresponding subrequests if castorfile is not 0
      IF (tc.c != 0) THEN
        FOR s in (SELECT id FROM subrequest WHERE castorfile = tc.c) LOOP
          archiveSubReq(s.id);
        END LOOP;
      END IF;
    END LOOP;
    -- commit between each bunch of 10
    done := done + 10;
    UPDATE CleanupLogTable
       SET message = 'Cleaning tapeCopies with no DiskCopies' ||
           TO_CHAR(100*done/(totalCount+1), '999.99') || '% done', logDate = getTime()
     WHERE fac = 4;
    COMMIT;
    DBMS_LOCK.sleep(seconds => .5);
    IF nothingLeft = 1 THEN
      UPDATE CleanupLogTable
         SET message = 'TapeCopies with no DiskCopies were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
       WHERE fac = 4;
      COMMIT;
      EXIT;
    END IF;
  END LOOP;
END;

-- Now the opposite: cleaning DiskCopies in WAITTAPERECALL with no TapeCopies (credits to Olof)
DECLARE
  totalCount NUMBER := 0;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (5, 'Failing diskCopies waiting for recall without TapeCopies', getTime());
  COMMIT;
  
  -- do everything in one go: this case is relatively rare and the cleanup doesn't involve many requests          
  FOR dc IN (select dc_id diskcopy_id , dc_c castorfile FROM (
                select dc.id dc_id, dc.castorfile dc_c, tc.id tc_id
                from tapecopy tc, diskcopy dc
                where dc.status = 2
                  and dc.castorfile = tc.castorfile(+))
             where tc_id is null) LOOP
    totalCount := totalCount + 1;
    UPDATE DISKCOPY SET STATUS=4 WHERE STATUS=2 AND ID=dc.diskcopy_id;
    UPDATE SUBREQUEST SET STATUS=1 WHERE STATUS IN (4,5) AND CASTORFILE=dc.castorfile;
    COMMIT;
  END LOOP;

  UPDATE CleanupLogTable
     SET message = 'DiskCopies waiting for recall with no TapeCopies were failed - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
   WHERE fac = 5;
  COMMIT;
END;

-- Cleanup FilesDeleted requests that stayed due to dead locks
DECLARE
  totalCount NUMBER;
  done NUMBER := 0;
  req NUMBER;
  cl NUMBER;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (6, 'Cleanup FilesDeleted requests', getTime());
  COMMIT;
  SELECT COUNT(*) INTO totalCount FROM FilesDeleted WHERE creationTime < getTime() - 600000;
  LOOP
    -- do only 1 request per commit in order to not slow down the normal requests
    -- The point is that we may delete many GcFiles for one request (1000s)
    SELECT id, client INTO req, cl FROM FilesDeleted WHERE creationTime < getTime() - 600000 AND ROWNUM < 2;
    -- Delete the client part
    DELETE FROM Id2Type WHERE id = cl;
    DELETE FROM Client WHERE id = cl;
    -- Delete the corresponding GCFiles
    FOR gcf IN (SELECT id FROM GCFile WHERE request = req) LOOP
      DELETE FROM Id2Type WHERE id = gcf.id;
      DELETE FROM GCFile WHERE id = gcf.id;      
    END LOOP;
    -- Delete the request itself
    DELETE FROM Id2Type WHERE id = req;
    DELETE FROM FilesDeleted WHERE id = req;
    -- commit at each request due to the big number of gcFiles per request
    done := done + 1;
    UPDATE CleanupLogTable
       SET message = 'Cleanup FilesDeleted requests' ||
           TO_CHAR(100*done/(totalCount+1), '999.99') || '% done', logDate = getTime()
     WHERE fac = 6;
    COMMIT;
  END LOOP;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    UPDATE CleanupLogTable
       SET message = 'FilesDeleted requests were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
     WHERE fac = 6;
    COMMIT;
END;

-- Cleanup FilesDeletionFailed request that stayed due to dead locks
DECLARE
  totalCount NUMBER;
  done NUMBER := 0;
  req NUMBER;
  cl NUMBER;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (7, 'Cleanup FilesDeletionFailed requests', getTime());
  COMMIT;
  SELECT COUNT(*) INTO totalCount FROM FilesDeletionFailed WHERE creationTime < getTime() - 600000;
  LOOP
    -- do only 1 request per commit in order to not slow down the normal requests
    -- The point is that we may delete many GcFiles for one request (1000s)
    SELECT id, client INTO req, cl FROM FilesDeletionFailed WHERE creationTime < getTime() - 600000 AND ROWNUM < 2;
    -- Delete the client part
    DELETE FROM Id2Type WHERE id = cl;
    DELETE FROM Client WHERE id = cl;
    -- Delete the corresponding GCFiles
    FOR gcf IN (SELECT id FROM GCFile WHERE request = req) LOOP
      DELETE FROM Id2Type WHERE id = gcf.id;
      DELETE FROM GCFile WHERE id = gcf.id;      
    END LOOP;
    -- Delete the request itself
    DELETE FROM Id2Type WHERE id = req;
    DELETE FROM FilesDeletionFailed WHERE id = req;
    -- commit at each request due to the big number of gcFiles per request
    done := done + 1;
    UPDATE CleanupLogTable
       SET message = 'Cleanup FilesDeletionFailed requests' ||
           TO_CHAR(100*done/(totalCount+1), '999.99') || '% done', logDate = getTime()
     WHERE fac = 7;
    COMMIT;
  END LOOP;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    UPDATE CleanupLogTable
       SET message = 'FilesDeletionFailed requests were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
     WHERE fac = 7;
    COMMIT;
END;

-- Cleanup of old subrequests stuck in status WAITSUBREQ and
-- which are waiting on nothing
DECLARE
  nothingLeft NUMBER;
  totalCount NUMBER;
  done NUMBER := 0;
BEGIN
 INSERT INTO CleanupLogTable VALUES (8, 'Cleaning SubRequests stuck in WAITSUBREQ 0% done', getTime());
 COMMIT;
 SELECT count(*) INTO totalCount FROM SubRequest
  WHERE creationtime < getTime() - 600000
    AND status = 5
    AND (parent NOT IN (SELECT id FROM SubRequest) OR parent IS NULL)
 LOOP
   nothingLeft := 1;
   -- do it in one go here because the heaviest part is the SELECT itself with the NOT IN clause
   FOR sr IN (SELECT id FROM SubRequest
               WHERE creationtime < getTime() - 600000
                 AND status = 5
                 AND (parent NOT IN (SELECT id FROM SubRequest) OR parent IS NULL) LOOP
     archiveSubReq(sr.id);
   END LOOP;
   -- commit and wait 5 seconds between each bunch of 1000
   -- done := done + 1000;
   -- UPDATE CleanupLogTable
   --   SET message = 'Cleaning SubRequests stuck in WAITSUBREQ' ||
   --       TO_CHAR(100*done/(totalCount+1), '999.99') || '% done', logDate = getTime()
   -- WHERE fac = 8;
   COMMIT;
   IF nothingLeft = 1 THEN
     UPDATE CleanupLogTable
        SET message = 'SubRequests stuck in WAITSUBREQ were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
      WHERE fac = 8;
     COMMIT;
     EXIT;
   END IF;
 END LOOP;
END;

-- Cleanup stageGetRequest having no subrequest anymore
DECLARE
  nothingLeft NUMBER;
  rclient NUMBER;
  totalCount NUMBER;
  done NUMBER := 0;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (9, 'Cleaning stageGetRequest having no subrequest', getTime());
  COMMIT;
  SELECT COUNT(req) INTO totalCount FROM (SELECT UNIQUE r.id AS req, s.id AS sub
                                            FROM stageGetRequest r, subrequest s
                                           WHERE s.request(+) = r.id)
   WHERE sub is null;
  LOOP
    nothingLeft := 1;
    -- do it 1000 by 1000 in order to not slow down the normal requests
    FOR r IN (SELECT req FROM (SELECT UNIQUE r.id AS req, s.id AS sub
                                 FROM stageGetRequest r, subrequest s
                                WHERE s.request(+) = r.id)
               WHERE sub IS NULL AND ROWNUM <= 1000) LOOP
     -- Delete the request
     DELETE FROM Id2Type WHERE id = r.req;
     DELETE FROM StageGetRequest WHERE id = r.req RETURNING client into rclient;
     -- and the client object
     DELETE FROM Id2Type WHERE id = rclient;
     DELETE FROM Client WHERE id = rclient;
     nothingLeft := 0;
    END LOOP;
    -- commit between each bunch of 1000
    done := done + 1000;
    UPDATE CleanupLogTable
       SET message = 'Cleaning stageGetRequest having no subrequest' ||
           TO_CHAR(100*done/(totalCount+1), '999.99') || '% done', logDate = getTime()
     WHERE fac = 9;
    COMMIT;
    IF nothingLeft = 1 THEN
      UPDATE CleanupLogTable
         SET message = 'stageGetRequest having no subrequest were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
       WHERE fac = 9;
      COMMIT;
      EXIT;
    END IF;
  END LOOP;
END;

-- Cleanup stagePrepareToGetRequest having no subrequest anymore
DECLARE
  nothingLeft NUMBER;
  rclient NUMBER;
  totalCount NUMBER;
  done NUMBER := 0;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (10, 'Cleaning stagePrepareToGetRequest having no subrequest', getTime());
  COMMIT;
  SELECT COUNT(req) INTO totalCount FROM (SELECT UNIQUE r.id AS req, s.id AS sub
                                            FROM stagePrepareToGetRequest r, subrequest s
                                           WHERE s.request(+) = r.id)
   WHERE sub is null;
  LOOP
    nothingLeft := 1;
    -- do it 1000 by 1000 in order to not slow down the normal requests
    FOR r IN (SELECT req FROM (SELECT UNIQUE r.id AS req, s.id AS sub
                                 FROM stagePrepareToGetRequest r, subrequest s
                                WHERE s.request(+) = r.id)
               WHERE sub IS NULL AND ROWNUM <= 1000) LOOP
     -- Delete the request
     DELETE FROM Id2Type WHERE id = r.req;
     DELETE FROM stagePrepareToGetRequest WHERE id = r.req RETURNING client into rclient;
     -- and the client object
     DELETE FROM Id2Type WHERE id = rclient;
     DELETE FROM Client WHERE id = rclient;
     nothingLeft := 0;
    END LOOP;
    -- commit between each bunch of 1000
    done := done + 1000;
    UPDATE CleanupLogTable
       SET message = 'Cleaning stagePrepareToGetRequest having no subrequest' ||
           TO_CHAR(100*done/(totalCount+1), '999.99') || '% done', logDate = getTime()
     WHERE fac = 10;
    COMMIT;
    IF nothingLeft = 1 THEN
      UPDATE CleanupLogTable
         SET message = 'stagePrepareToGetRequest having no subrequest were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
       WHERE fac = 10;
      COMMIT;
      EXIT;
    END IF;
  END LOOP;
END;

-- Cleanup stagePutRequest having no subrequest anymore
DECLARE
  nothingLeft NUMBER;
  rclient NUMBER;
  totalCount NUMBER;
  done NUMBER := 0;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (11, 'Cleaning stagePutRequest having no subrequest', getTime());
  COMMIT;
  SELECT COUNT(req) INTO totalCount FROM (SELECT UNIQUE r.id AS req, s.id AS sub
                                            FROM stagePutRequest r, subrequest s
                                           WHERE s.request(+) = r.id)
   WHERE sub is null;
  LOOP
    nothingLeft := 1;
    -- do it 1000 by 1000 in order to not slow down the normal requests
    FOR r IN (SELECT req FROM (SELECT UNIQUE r.id AS req, s.id AS sub
                                 FROM stagePutRequest r, subrequest s
                                WHERE s.request(+) = r.id)
               WHERE sub IS NULL AND ROWNUM <= 1000) LOOP
     -- Delete the request
     DELETE FROM Id2Type WHERE id = r.req;
     DELETE FROM stagePutRequest WHERE id = r.req RETURNING client into rclient;
     -- and the client object
     DELETE FROM Id2Type WHERE id = rclient;
     DELETE FROM Client WHERE id = rclient;
     nothingLeft := 0;
    END LOOP;
    -- commit between each bunch of 1000
    done := done + 1000;
    UPDATE CleanupLogTable
       SET message = 'Cleaning stagePutRequest having no subrequest' ||
           TO_CHAR(100*done/(totalCount+1), '999.99') || '% done', logDate = getTime()
     WHERE fac = 11;
    COMMIT;
    IF nothingLeft = 1 THEN
      UPDATE CleanupLogTable
         SET message = 'stagePutRequest having no subrequest were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
       WHERE fac = 11;
      COMMIT;
      EXIT;
    END IF;
  END LOOP;
END;

-- Cleanup stagePutDoneRequest having no subrequest anymore
DECLARE
  nothingLeft NUMBER;
  rclient NUMBER;
  totalCount NUMBER;
  done NUMBER := 0;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (12, 'Cleaning stagePutDoneRequest having no subrequest', getTime());
  COMMIT;
  SELECT COUNT(req) INTO totalCount FROM (SELECT UNIQUE r.id AS req, s.id AS sub
                                            FROM stagePutDoneRequest r, subrequest s
                                           WHERE s.request(+) = r.id)
   WHERE sub is null;
  LOOP
    nothingLeft := 1;
    -- do it 1000 by 1000 in order to not slow down the normal requests
    FOR r IN (SELECT req FROM (SELECT UNIQUE r.id AS req, s.id AS sub
                                 FROM stagePutDoneRequest r, subrequest s
                                WHERE s.request(+) = r.id)
               WHERE sub IS NULL AND ROWNUM <= 1000) LOOP
     -- Delete the request
     DELETE FROM Id2Type WHERE id = r.req;
     DELETE FROM stagePutDoneRequest WHERE id = r.req RETURNING client into rclient;
     -- and the client object
     DELETE FROM Id2Type WHERE id = rclient;
     DELETE FROM Client WHERE id = rclient;
     nothingLeft := 0;
    END LOOP;
    -- commit between each bunch of 1000
    done := done + 1000;
    UPDATE CleanupLogTable
       SET message = 'Cleaning stagePutDoneRequest having no subrequest' ||
           TO_CHAR(100*done/(totalCount+1), '999.99') || '% done', logDate = getTime()
     WHERE fac = 12;
    COMMIT;
    IF nothingLeft = 1 THEN
      UPDATE CleanupLogTable
         SET message = 'stagePutDoneRequest having no subrequest were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
       WHERE fac = 12;
      COMMIT;
      EXIT;
    END IF;
  END LOOP;
END;

-- Cleanup of old subrequests (code from deleteOutOfDateRequests)

CREATE TABLE REQCLEANING
    ( ID NUMBER NOT NULL ENABLE,
      TYPE NUMBER(*,0) NOT NULL ENABLE
    )
 PCTFREE 0 TABLESPACE STAGER_DATA
 PARTITION BY LIST (TYPE)
  (PARTITION P_35  VALUES (35),
   PARTITION P_40  VALUES (40),
   PARTITION P_42  VALUES (42),
   PARTITION P_39  VALUES (39),
   PARTITION P_36  VALUES (36),
   PARTITION P_37  VALUES (37),
   PARTITION P_NOTDEFINED  VALUES (default)
  );

-- Using the APPEND hint to use Direct Option load mechanism
DECLARE
  totalCount NUMBER;
BEGIN
  -- For logging
  INSERT INTO CleanupLogTable VALUES (13, 'Cleaning all old subrequests', getTime());
  COMMIT;
  INSERT /*+ APPEND */ INTO ReqCleaning
    SELECT UNIQUE request, type 
    FROM SubRequest, id2type
    WHERE subrequest.request = id2type.id
    GROUP BY request, type
    HAVING min(status) >= 6    -- here we're deleting subrequests stuck in READY as well 
       AND max(status) <= 11 
       AND max(lastModificationTime) < getTime() - 600000;
  COMMIT;
  SELECT count(*) INTO totalCount FROM ReqCleaning;

  -- Delete SubRequests

  DELETE FROM Id2Type WHERE id in (
    SELECT SubRequest.id FROM SubRequest, ReqCleaning
     WHERE SubRequest.request = ReqCleaning.id);
  
  DELETE FROM SubRequest WHERE request in (
    SELECT id FROM ReqCleaning);
  COMMIT;
  
  -- Delete Request + Clients 
              ---- Get ----
  
  DELETE FROM Id2Type WHERE id in (
    SELECT client FROM StageGetRequest, ReqCleaning
     WHERE StageGetRequest.id = ReqCleaning.id);
  
  DELETE FROM Client WHERE id in (
    SELECT client FROM StageGetRequest, ReqCleaning
     WHERE StageGetRequest.id = ReqCleaning.id);
  
  DELETE FROM StageGetRequest WHERE id in (
    SELECT id FROM ReqCleaning WHERE type = 35);
  
              ---- Put ----
  
  DELETE FROM Id2Type WHERE id in (
    SELECT client FROM StagePutRequest, ReqCleaning
     WHERE StagePutRequest.id = ReqCleaning.id);
  
  DELETE FROM Client WHERE id in (
    SELECT client FROM StagePutRequest, ReqCleaning
     WHERE StagePutRequest.id = ReqCleaning.id);
  
  DELETE FROM StagePutRequest WHERE id in (
    SELECT id FROM ReqCleaning WHERE type = 40);
  
              ---- Rm ----
  
  DELETE FROM Id2Type WHERE id in (
    SELECT client FROM StageRmRequest, ReqCleaning
     WHERE StageRmRequest.id = ReqCleaning.id);
  
  DELETE FROM Client WHERE id in (
    SELECT client FROM StageRmRequest, ReqCleaning
     WHERE StageRmRequest.id = ReqCleaning.id);
  
  DELETE FROM StageRmRequest WHERE id in (
    SELECT id FROM ReqCleaning WHERE type = 42);
  COMMIT;
  
             ---- PutDone ----
  
  DELETE FROM Id2Type WHERE id in (
    SELECT client FROM StagePutDoneRequest, ReqCleaning
     WHERE StagePutDoneRequest.id = ReqCleaning.id);
  
  DELETE FROM Client WHERE id in (
    SELECT client FROM StagePutDoneRequest, ReqCleaning
     WHERE StagePutDoneRequest.id = ReqCleaning.id);
  
  DELETE FROM StagePutDoneRequest WHERE id in (
    SELECT id FROM ReqCleaning WHERE type = 39);
  
            ---- PrepareToGet -----
  
  DELETE FROM Id2Type WHERE id in (
    SELECT client FROM StagePrepareToGetRequest, ReqCleaning
     WHERE StagePrepareToGetRequest.id = ReqCleaning.id);
  
  DELETE FROM Client WHERE id in (
    SELECT client FROM StagePrepareToGetRequest, ReqCleaning
     WHERE StagePrepareToGetRequest.id = ReqCleaning.id);
  
  DELETE FROM StagePrepareToGetRequest WHERE id in (
    SELECT id FROM ReqCleaning WHERE type = 36);
  
            ---- PrepareToPut ----
  
  DELETE FROM Id2Type WHERE id in (
    SELECT client FROM StagePrepareToPutRequest, ReqCleaning
     WHERE StagePrepareToPutRequest.id = ReqCleaning.id);
  
  DELETE FROM Client WHERE id in (
    SELECT client FROM StagePrepareToPutRequest, ReqCleaning
     WHERE StagePrepareToPutRequest.id = ReqCleaning.id);
  
  DELETE FROM StagePrepareToPutRequest WHERE id in (
    SELECT id FROM ReqCleaning WHERE type = 37);
  
  -- Finally delete all IDs
  
  DELETE FROM Id2Type WHERE id in (
    SELECT id FROM ReqCleaning);
  
  -- Last step is to commit the operations
  UPDATE CleanupLogTable
     SET message = 'Old subrequests were cleaned - ' || TO_CHAR(totalCount) || ' entries', logDate = getTime()
   WHERE fac = 13;
  COMMIT;
  DROP TABLE ReqCleaning;
END;


-- Optional steps follow

INSERT INTO CleanupLogTable VALUES (14, 'Shrinking tables', getTime());
COMMIT;

-- Shrinking space in the tables that have just been cleaned up.
-- This only works if tables have ROW MOVEMENT enabled, and it's
-- not a big problem if we don't do that. Newer Castor versions
-- do enable this feature in all involved tables.
ALTER TABLE Id2Type SHRINK SPACE CASCADE;
ALTER TABLE Client SHRINK SPACE CASCADE;
ALTER TABLE StagePrepareToPutRequest SHRINK SPACE CASCADE;
ALTER TABLE StagePrepareToGetRequest SHRINK SPACE CASCADE;
ALTER TABLE StagePutDoneRequest SHRINK SPACE CASCADE;
ALTER TABLE StageRmRequest SHRINK SPACE CASCADE;
ALTER TABLE StageGetRequest SHRINK SPACE CASCADE;
ALTER TABLE StagePutRequest SHRINK SPACE CASCADE;
ALTER TABLE DiskCopy SHRINK SPACE CASCADE;
ALTER TABLE SubRequest SHRINK SPACE CASCADE;

UPDATE CleanupLogTable
   SET message = 'All tables were shrunk', logDate = getTime()
 WHERE fac = 14;
COMMIT;

-- To provide a summary of the performed cleanup
SELECT * FROM CleanupLogTable;
