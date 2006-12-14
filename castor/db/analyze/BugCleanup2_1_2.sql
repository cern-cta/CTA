-- Cleanup old stage rm that were never deleted
-- Change only the value for creationtime
BEGIN
 LOOP
   FOR sr IN (select subrequest.id from id2type, subrequest
              where status = 6 and request = id2type.id
                and type = 42 and rownum <= 500
                and subrequest.creationtime < 1161750000)
   LOOP
     archiveSubReq(sr.id);
   END LOOP;
   COMMIT;
   DBMS_LOCK.sleep(seconds => 1.0);
 END LOOP;
 EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
END;


-- Remove DiskCopies stuck in status 9 (BEINGDELETED) because of deadlocks
-- Actually, we put them in GCCANDIDATE again and most of them will go to FAILED
-- But in case any needs file deletion, we are safe
-- You only need to change the id limit
declare
  i number := 0;
begin
  LOOP
    update DiskCopy set status = 8 where status = 9 and id <= 381300000 and rownum <= 10000;
    commit;
    i := i + 1;
    if i > 34 then exit; end if;
    DBMS_LOCK.sleep(seconds => 1200.0);
  end LOOP;
end;

-- Cleaning the GC candidates that have no filesystem and will thus never be taken
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
  i number;
  fileIds castor.FileList_Cur;
BEGIN
  LOOP
    i := 1;
    FOR dc IN (SELECT UNIQUE id FROM DiskCopy where status = 8 and (filesystem = 0 or filesystem is null) AND ROWNUM < 1000)
    LOOP
      files(i) := dc.id;
      i := i + 1;
    END LOOP;
    filesDeletedProc(files, fileIds);
    COMMIT;
    DBMS_LOCK.sleep(seconds => 2.0);
  END LOOP;
  EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
END;
DROP PROCEDURE filesDeletedProcNoFS;

-- Cleaning the failed diskCopies
DECLARE
  files castor."cnumList";
  i number;
  fileIds castor.FileList_Cur;
BEGIN
  -- Go diskserver by diskserver, otherwise we may have deadlocks in filesDeletedProc
  FOR ds in (SELECT id from DiskServer) LOOP
    BEGIN
      LOOP
        i := 1;
        FOR dc IN (SELECT DiskCopy.id FROM DiskCopy, FileSystem
                    WHERE DiskCopy.status = 4 AND DiskCopy.fileSystem = fileSystem.id
                      AND fileSystem.diskServer = ds.id AND ROWNUM < 1000)
        LOOP
          files(i) := dc.id;
          i := i + 1;
        END LOOP;
        filesDeletedProc(files, fileIds);
        COMMIT;
      END LOOP;
      EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
    END;
  END LOOP;
END;

-- Cleaning the TapeCopies with no DiskCopies, due to bad cleaning after migration/recall error in rtcpcld
DECLARE
BEGIN
  LOOP
    FOR tc IN (select c, t from
               (select t.castorfile as c, t.id as t, d.id as d
                  from tapecopy t, diskcopy d where t.castorfile = d.castorfile(+))
               where d is null and ROWNUM < 500)
    LOOP
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
      -- delete corresponding subrequests
      FOR s in (SELECT id FROM subrequest WHERE castorfile = tc.c) LOOP
        archiveSubReq(s.id);
      END LOOP;
    END LOOP;
    COMMIT;
    DBMS_LOCK.sleep(seconds => 2.0);
  END LOOP;
  EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
END;
