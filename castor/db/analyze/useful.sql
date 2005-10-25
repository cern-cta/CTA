-- Cleaning of old stagefilequeries
DECLARE
  cli INTEGER;
  que INTEGER;
BEGIN
 LOOP
   FOR item IN 
     (SELECT id
        FROM stagefilequeryrequest
       WHERE lastmodificationtime < 1120486851
         AND rownum < 51) LOOP 
     DELETE FROM stagefilequeryrequest WHERE id = item.id RETURNING Client INTO cli;
     DELETE FROM Id2Type WHERE id = item.id;
     DELETE FROM Client WHERE id = cli;
     DELETE FROM Id2Type WHERE id = cli;
     DELETE FROM QueryParameter WHERE query = item.id RETURNING id INTO que;
     DELETE FROM Id2Type WHERE id = que;
     COMMIT;
   END LOOP;
   DBMS_LOCK.sleep(seconds => 5.0);
 END LOOP;
 EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
END;




-- Cleaning of a filesystem that was erased
BEGIN
  -- For each fileSystem Available
  FOR fs in (SELECT id FROM FileSystem WHERE diskServer = 3350 AND status = 0) LOOP
    BEGIN
      LOOP
        -- Take DiskCopies 10 by 10
        FOR dc in (select id, castorfile from DiskCopy
                    WHERE fileSystem = fs.id AND status = 10 AND ROWNUM < 10) LOOP
          -- Loop on TapeCopies
          FOR tc IN (SELECT id From TapeCopy WHERE castorfile = dc.castorFile) LOOP
            DELETE FROM Stream2TapeCopy WHERE child = tc.id;
            DELETE from TapeCopy WHERE id = tc.id;
          END LOOP;
          -- set DiskCopies to INVALID
          UPDATE DiskCopy SET status = 7 -- INVALID
           WHERE id = dc.id;
        END LOOP;
        -- Pause after 10 deletions
        COMMIT;
        DBMS_LOCK.sleep(seconds => 2.0);
      END LOOP;
    EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
    END;
  END LOOP;
END;

-- Cleaning of failed subrequests
BEGIN
 LOOP
   FOR sr IN (SELECT id FROM SubRequest WHERE status = 9 and ROWNUM <= 100) LOOP
     archiveSubReq(sr.id);
   END LOOP;
   COMMIT;
   DBMS_LOCK.sleep(seconds => 5.0);
 END LOOP;
 EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
END;

-- Cleaning of old DiskCopies
drop index I_DiskCopy_Status;
CREATE INDEX I_DiskCopy_Status on DiskCopy
  (CASE status WHEN 3 THEN status ELSE NULL end) online;

DECLARE
  nextId NUMBER;
BEGIN
 LOOP
   FOR i IN 1..50 LOOP 
    SELECT id INTO nextId FROM diskcopy WHERE (CASE status WHEN 3 THEN status ELSE NULL end) = 3 AND ROWNUM < 2;
    DELETE FROM ID2TYPE WHERE ID = nextId;
    DELETE FROM DiskCopy WHERE id = nextId;
    COMMIT;
   END LOOP;
   DBMS_LOCK.sleep(seconds => 5.0);
 END LOOP;
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;


CREATE INDEX I_toto on DiskCopy
  (CASE status WHEN 5 THEN status ELSE NULL end);

DECLARE
  dcId NUMBER;
  cfId NUMBER;
BEGIN
 LOOP
   SELECT id, castorFile INTO dcId, cfId FROM diskcopy
    WHERE (CASE status WHEN 5 THEN status ELSE NULL END) = 5 AND ROWNUM < 2;
   DELETE FROM ID2TYPE WHERE ID = dcId;
   DELETE FROM ID2TYPE WHERE ID = cfId;
   DELETE FROM DiskCopy WHERE id = dcId;
   DELETE FROM CastorFile WHERE id = dcId;
   COMMIT;
 END LOOP;
END;





CREATE OR REPLACE PROCEDURE removeStream(sid IN NUMBER) AS
BEGIN
  DELETE FROM Stream2TapeCopy where parent = sid;
  UPDATE Tape SET stream = 0 where stream = sid;
  DELETE FROM id2Type WHERE id = sid;
  DELETE FROM Stream WHERE id = sid;
END;




CREATE OR REPLACE PROCEDURE main_dev.migrSmallFiles(limitSize IN NUMBER) AS
BEGIN
 for item in (select TapeCopy.id FROM TapeCopy, castorFile
             where CastorFile.fileSize <= limitSize
             AND TapeCopy.castorfile = CastorFile.id) LOOP
   INSERT INTO Stream2TapeCopy (Parent, Child) VALUES (380, item.id);
 END LOOP;
END;
CREATE OR REPLACE PROCEDURE main_dev.migrBigFiles(limitSize IN NUMBER) AS
BEGIN
 for item in (select TapeCopy.id FROM TapeCopy, castorFile
             where CastorFile.fileSize > limitSize
             AND TapeCopy.castorfile = CastorFile.id) LOOP
   INSERT INTO Stream2TapeCopy (Parent, Child) VALUES (379, item.id);
 END LOOP;
END;




DROP TABLE FileSystemVerbLog;
CREATE TABLE FileSystemVerbLog
 (time Date, type CHAR(7), free INTEGER, weight float, fsDeviation float, mountPoint VARCHAR2(2048),
  deltaWeight float, deltaFree NUMBER, reservedSpace NUMBER, id INTEGER,
  diskPool INTEGER, diskserver INTEGER, status INTEGER);

DROP TABLE FileSystemLog;
CREATE TABLE FileSystemLog
 (time Date, type CHAR(7), free INTEGER, weight float, fsDeviation float, mountPoint VARCHAR2(2048),
  deltaWeight float, deltaFree NUMBER, reservedSpace NUMBER, id INTEGER,
  diskPool INTEGER, diskserver INTEGER, status INTEGER);

CREATE OR REPLACE PROCEDURE updateFsFileOpened
(ds IN INTEGER, fs IN INTEGER,
 deviation IN INTEGER, fileSize IN INTEGER) AS
BEGIN
 INSERT INTO FileSystemVerbLog (select SYSDATE, 'OPEN', FileSystem.* from Filesystem);
 UPDATE FileSystem SET deltaWeight = deltaWeight - deviation
  WHERE diskServer = ds;
 UPDATE FileSystem SET fsDeviation = 2 * deviation,
                       reservedSpace = reservedSpace + fileSize
  WHERE id = fs;
 INSERT INTO FileSystemLog (select SYSDATE, 'OPEN', FileSystem.* from Filesystem where diskServer = ds);
END;

CREATE OR REPLACE PROCEDURE updateFsFileClosed
(fs IN INTEGER, reservation IN INTEGER, fileSize IN INTEGER) AS
BEGIN
 INSERT INTO FileSystemVerbLog (select SYSDATE, 'CLOSE', FileSystem.* from Filesystem);
 UPDATE FileSystem SET deltaFree = deltaFree - fileSize,
                       reservedSpace = reservedSpace - reservation
  WHERE id = fs;
 INSERT INTO FileSystemLog (select SYSDATE, 'CLOSE', FileSystem.* from Filesystem where id = fs);
END;




-- DEBUGGING FILE SELECTION

DROP TABLE FSGivenByMaui;
CREATE TABLE FSGivenByMaui (time Date, free INTEGER, weight float, fsDeviation float, mountPoint VARCHAR2(2048),
  deltaWeight float, deltaFree NUMBER, reservedSpace NUMBER, id INTEGER,
  diskPool INTEGER, diskserver INTEGER, status INTEGER);
DROP TABLE decisionsTaken;
CREATE Table decisionsTaken (time Date, mountPoint VARCHAR2(2048));


CREATE OR REPLACE PROCEDURE bestFileSystemForJob
(fileSystems IN castor."strList", machines IN castor."strList",
 minFree IN INTEGER, rMountPoint OUT VARCHAR2,
 rDiskServer OUT VARCHAR2) AS
 ds NUMBER;
 fs NUMBER;
 dev NUMBER;
 TYPE cursorContent IS RECORD
   (mountPoint VARCHAR2(2048), dsName VARCHAR2(2048),
    dsId NUMBER, fsId NUMBER, deviation NUMBER);
 TYPE AnyCursor IS REF CURSOR RETURN cursorContent;
 c1 AnyCursor;
BEGIN
 IF fileSystems.COUNT > 0 THEN
  DECLARE
   fsIds "numList" := "numList"();
  BEGIN
   fsIds.EXTEND(fileSystems.COUNT);
   FOR i in fileSystems.FIRST .. fileSystems.LAST LOOP
    SELECT FileSystem.id INTO fsIds(i)
      FROM FileSystem, DiskServer
     WHERE FileSystem.mountPoint = fileSystems(i)
       AND DiskServer.name = machines(i);
    INSERT INTO FSGivenByMaui (SELECT SYSDATE, FileSystem.* FROM FileSystem where id = fsIds(i));
   END LOOP;
   OPEN c1 FOR
    SELECT FileSystem.mountPoint, Diskserver.name,
           DiskServer.id, FileSystem.id, FileSystem.fsDeviation
    FROM FileSystem, DiskServer
    WHERE FileSystem.diskserver = DiskServer.id
     AND FileSystem.id MEMBER OF fsIds
     AND FileSystem.free + FileSystem.deltaFree - FileSystem.reservedSpace >= minFree
     AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
     AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
    ORDER by FileSystem.weight + FileSystem.deltaWeight DESC,
             FileSystem.fsDeviation ASC;
  END;
 ELSE
  OPEN c1 FOR
   SELECT FileSystem.mountPoint, Diskserver.name,
          DiskServer.id, FileSystem.id, FileSystem.fsDeviation
   FROM FileSystem, DiskServer
   WHERE FileSystem.diskserver = DiskServer.id
    AND FileSystem.free + FileSystem.deltaFree - FileSystem.reservedSpace >= minFree
    AND DiskServer.status IN (0, 1) -- DISKSERVER_PRODUCTION, DISKSERVER_DRAINING
    AND FileSystem.status IN (0, 1) -- FILESYSTEM_PRODUCTION, FILESYSTEM_DRAINING
   ORDER by FileSystem.weight + FileSystem.deltaWeight DESC,
            FileSystem.fsDeviation ASC;
 END IF;
 FETCH c1 INTO rMountPoint, rDiskServer, ds, fs, dev;
 CLOSE c1;
 INSERT INTO DecisionsTaken VALUES (SYSDATE, rMountPoint);
 updateFsFileOpened(ds, fs, dev, minFree);
END;



-- DEBUGGING REQUEST LIFECYCLE --

DROP TABLE RequestLife;
create table RequestLife (id number, arrival date, selected date,
                          putStartArrival date, putStartSelected DATE, putStartEnd date,
                          movercloseArrival date, moverCloseSelected DATE, moverCloseEnd date,
                          deletion DATE, fs varchar2(2048));

CREATE OR REPLACE TRIGGER tr_arrival
BEFORE INSERT ON StagePutRequest FOR EACH ROW
BEGIN
    INSERT INTO RequestLife (id, arrival) VALUES (:new.id, SYSDATE);
END;

CREATE OR REPLACE TRIGGER tr_RequestSelected
BEFORE UPDATE OF status ON SubRequest FOR EACH ROW WHEN (new.status = 3) -- WAITSCHED
BEGIN
  UPDATE RequestLife set selected = SYSDATE WHERE id = :new.request;
EXCEPTION WHEN OTHERS THEN NULL;
END;

CREATE OR REPLACE TRIGGER tr_deletion
BEFORE DELETE ON StagePutRequest FOR EACH ROW
BEGIN
  UPDATE RequestLife set deletion = SYSDATE WHERE id = :old.id;
EXCEPTION WHEN OTHERS THEN NULL;
END;

CREATE OR REPLACE TRIGGER tr_putStartArrival
BEFORE INSERT ON NewRequests FOR EACH ROW WHEN (new.type = 67)
BEGIN
  UPDATE RequestLife set putStartArrival = SYSDATE WHERE id =
    (SELECT SubRequest.request FROM SubRequest, PutStartRequest WHERE PutStartRequest.id = :new.id
        AND SubRequest.id = PutStartRequest.subreqid);
EXCEPTION WHEN OTHERS THEN NULL;
END;

CREATE OR REPLACE TRIGGER tr_putStartSelected
BEFORE DELETE ON NewRequests FOR EACH ROW WHEN (old.type = 67)
BEGIN
  UPDATE RequestLife set putStartSelected = SYSDATE WHERE id =
    (SELECT SubRequest.request FROM SubRequest, PutStartRequest WHERE PutStartRequest.id = :old.id
        AND SubRequest.id = PutStartRequest.subreqid);
EXCEPTION WHEN OTHERS THEN NULL;
END;

CREATE OR REPLACE TRIGGER tr_putStartEnd
BEFORE DELETE ON PutStartRequest FOR EACH ROW
BEGIN
  UPDATE RequestLife set putStartEnd = SYSDATE WHERE id = :old.id;
EXCEPTION WHEN OTHERS THEN NULL;
END;

CREATE OR REPLACE TRIGGER tr_movercloseArrival
BEFORE INSERT ON NewRequests FOR EACH ROW WHEN (new.type = 65)
BEGIN
  UPDATE RequestLife set moverCloseArrival = SYSDATE WHERE id =
    (SELECT SubRequest.request FROM SubRequest, MoverCloseRequest WHERE MoverCloseRequest.id = :new.id
        AND SubRequest.id = MoverCloseRequest.subreqid);
EXCEPTION WHEN OTHERS THEN NULL;
END;

CREATE OR REPLACE TRIGGER tr_movercloseSelected
BEFORE DELETE ON NewRequests FOR EACH ROW WHEN (old.type = 65)
BEGIN
  UPDATE RequestLife set moverCloseSelected = SYSDATE WHERE id =
    (SELECT SubRequest.request FROM SubRequest, MoverCloseRequest WHERE MoverCloseRequest.id = :old.id
        AND SubRequest.id = MoverCloseRequest.subreqid);
EXCEPTION WHEN OTHERS THEN NULL;
END;

CREATE OR REPLACE TRIGGER tr_moverCloseEnd
BEFORE DELETE ON MoverCloseRequest FOR EACH ROW
BEGIN
  UPDATE RequestLife set moverCLoseEnd = SYSDATE WHERE id = :old.id;
EXCEPTION WHEN OTHERS THEN NULL;
END;



/* see where GC is needed */
SELECT ds.name, fs.mountpoint,  fs.id, 
       (fs.free + fs.deltaFree - fs.reservedSpace + fs.spaceToBeFreed) / 1000000 as frspace,
       fs.minfreespace * fs.totalsize / 1000000
  FROM FileSystem fs, DiskServer ds
 WHERE fs.diskserver = ds.id
 ORDER BY frspace ASC;
 
