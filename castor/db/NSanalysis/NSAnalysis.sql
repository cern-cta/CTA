-- Cleanup
DROP TABLE dirs;
DROP TABLE tapeHelp;
DROP TABLE tapeData;

ALTER TABLE Cns_file_metadata PARALLEL (degree 8);
ALTER TABLE Cns_seg_metadata PARALLEL (degree 8);

-- get the time of the snapshot
COL maxMtime new_value snapshotStartTime;
SELECT mtime-365*30*86400 maxMtime FROM cns_file_metadata WHERE fileid = (SELECT MAX(fileid) FROM cns_file_metadata);

-- table to hold directory info
CREATE TABLE dirs (fileid NUMBER NOT NULL,
                   parent NUMBER NOT NULL,
                   name VARCHAR2 (255),
                   depth INTEGER,
                   fullName VARCHAR2(2048),
                   totalSize NUMBER,
                   sizeOnTape NUMBER,
                   dataOnTape NUMBER,
                   nbFiles NUMBER,
                   nbTapes NUMBER,
                   nbFilesOnTape NUMBER,
                   nbFileCopiesOnTape NUMBER,
                   nbSubDirs NUMBER,
                   oldestFileLastMod NUMBER,
                   avgFileLastMod NUMBER,
                   sigFileLastMod NUMBER,
                   newestFileLastMod NUMBER,
                   oldestFileOnTapeLastMod NUMBER,
                   avgFileOnTapeLastMod NUMBER,
                   sigFileOnTapeLastMod NUMBER,
                   newestFileOnTapeLastMod NUMBER);

ALTER TABLE dirs PARALLEL (degree 8);

-- Enter all directories into new dirs table (~13mn)
INSERT /*+ APPEND */ INTO dirs (SELECT fileid, parent_fileid, name, -1, '', 0, 0, 0, 0, 0, 0, 0, 0,
                                       NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
                                  FROM Cns_file_metadata WHERE BITAND(filemode,16384) = 16384);
COMMIT;
-- create some useful indexes
CREATE UNIQUE INDEX I_dirs_fileid ON dirs (fileid);
CREATE INDEX I_dirs_parent ON dirs (parent);
CREATE INDEX I_dirs_depth ON dirs (depth, fileid, parent);


-- compute hierarchy (~150mn)
UPDATE dirs SET depth = 0 WHERE parent = 0;
DECLARE
  d number := 0;
BEGIN
  LOOP
    UPDATE dirs SET depth = d+1 WHERE parent IN (SELECT fileid FROM dirs WHERE depth = d);
    EXIT WHEN SQL%ROWCOUNT = 0;
    COMMIT;
    d := d + 1;
  END LOOP;
  COMMIT;
END;
/

-- compute full names (~1h)
UPDATE dirs SET fullname = '/' WHERE parent = 0;
DECLARE
  maxdepth NUMBER;
  n NUMBER := 0;
BEGIN
  -- first level independently since parent in the root
  UPDATE dirs SET fullname = '/'||name WHERE depth = '1';
  -- start at max depth
  SELECT MAX(depth) INTO maxdepth FROM dirs;
  FOR d IN 1..maxdepth LOOP
    FOR parent IN (SELECT fileid, fullname FROM dirs WHERE depth = d) LOOP
      UPDATE dirs SET fullname = parent.fullname||'/'||name WHERE parent = parent.fileid;
      n := n + 1;
      IF n = 200 THEN
        COMMIT;
        n := 0;
      END IF;
    END LOOP;
  END LOOP;
END;
/
COMMIT;

-- prepare an help table as a join of Cns_seg_metadata and Cns_file_metadata (~50mn)
CREATE TABLE tapeHelp (fileid number, parent number, VID VARCHAR2(6), fsize NUMBER, lastModTime NUMBER) PCTFREE 0;
ALTER TABLE tapeHelp PARALLEL (degree 8);
INSERT /*+ APPEND */ INTO tapeHelp
  (SELECT fileid, parent_fileid, VID, segsize, mtime FROM Cns_seg_metadata, Cns_file_metadata WHERE s_fileid = fileid);
COMMIT;
CREATE INDEX I_tapeHelp_parent ON tapeHelp (parent);

-- some tape related data sorting
CREATE TABLE tapeData (VID VARCHAR2(6), nbFiles NUMBER, dataSize NUMBER,
                       avgFileSize NUMBER, stdDevFileSize NUMBER,
                       minFileLastModTime NUMBER, avgFileLastModTime NUMBER, stdDevFileLastModTime NUMBER, maxFileLastModTime NUMBER);
INSERT /*+ APPEND */ INTO tapeData
   (SELECT VID, COUNT(*), SUM(fsize), AVG(fsize), STDDEV(fsize), MIN(lastModTime), AVG(lastModTime), STDDEV(lastModTime), MAX(lastModTime)
      FROM tapeHelp GROUP BY VID);
COMMIT;

CREATE OR REPLACE FUNCTION minimum(a NUMBER, b NUMBER) RETURN NUMBER AS
BEGIN
  IF a IS NULL THEN RETURN b; END IF;
  IF b IS NULL THEN RETURN a; END IF;
  RETURN CASE WHEN a < b THEN a ELSE b END;
END;
/

CREATE OR REPLACE FUNCTION maximum(a NUMBER, b NUMBER) RETURN NUMBER AS
BEGIN
  IF a IS NULL THEN RETURN b; END IF;
  IF b IS NULL THEN RETURN a; END IF;
  RETURN CASE WHEN a < b THEN b ELSE a END;
END;
/

/* Get current time as a time_t. Not that easy in ORACLE */
CREATE OR REPLACE FUNCTION getTime RETURN NUMBER IS
  epoch            TIMESTAMP WITH TIME ZONE;
  now              TIMESTAMP WITH TIME ZONE;
  interval         INTERVAL DAY(9) TO SECOND;
  interval_days    NUMBER;
  interval_hours   NUMBER;
  interval_minutes NUMBER;
  interval_seconds NUMBER;
BEGIN
  epoch := TO_TIMESTAMP_TZ('01-JAN-1970 00:00:00 00:00',
    'DD-MON-YYYY HH24:MI:SS TZH:TZM');
  now := SYSTIMESTAMP AT TIME ZONE '00:00';
  interval         := now - epoch;
  interval_days    := EXTRACT(DAY    FROM (interval));
  interval_hours   := EXTRACT(HOUR   FROM (interval));
  interval_minutes := EXTRACT(MINUTE FROM (interval));
  interval_seconds := EXTRACT(SECOND FROM (interval));

  RETURN interval_days * 24 * 60 * 60 + interval_hours * 60 * 60 +
    interval_minutes * 60 + interval_seconds;
END;
/

-----------------------
-- global statistics --
-----------------------

-- fill leaves in dirs with tape numbers (~30mn)
DECLARE
  n NUMBER := 0;
BEGIN
  FOR v IN (SELECT parent, count(unique fileid) nbfiles, count(fileid) nbCopies, sum(fsize) localSize, count(unique VID) nbTapes,
                   min(lastModTime) oldestfile, sum(lastModTime) avgfile, sum(lastModTime*lastModTime) sigfile, max(lastModTime) newestfile
              FROM tapeHelp WHERE lastModTime > &snapshotStartTime GROUP BY parent) LOOP
    UPDATE dirs
       SET nbfilesOnTape = v.nbFiles, nbfileCopiesOnTape = v.nbCopies, sizeOnTape = v.localSize, nbtapes = v.nbTapes,
           newestfileOnTapeLastMod = v.newestfile, avgfileOnTapeLastMod = v.avgfile,
           sigfileOnTapeLastMod = v.sigfile, oldestfileOnTapeLastMod = v.oldestfile
     WHERE fileid = v.parent;
    n := n + 1;
    IF n = 10000 THEN
      n := 0;
      COMMIT;
    END IF;
  END LOOP;
  COMMIT;
END;
/

-- fill leaves in dirs with non tape numbers (~30mn)
DECLARE
  n NUMBER := 0;
BEGIN
  FOR v IN (SELECT /*+ FULL(Cns_file_metadata) */ parent_fileid, count(unique fileid) nbfiles, sum(filesize) localSize,
                   min(mtime) oldestfile, sum(mtime) avgfile, sum(mtime*mtime) sigfile, max(mtime) newestfile
              FROM Cns_file_metadata WHERE BITAND(filemode,16384) = 0
                                       AND mtime > &snapshotStartTime GROUP BY parent_fileid) LOOP
    UPDATE dirs
       SET nbfiles = v.nbFiles, totalSize = v.localSize,
           newestfileLastMod = v.newestfile, avgfileLastMod = v.avgfile,
           sigfileLastMod = v.sigfile, oldestfileLastMod = v.oldestfile
     WHERE fileid = v.parent_fileid;
    n := n + 1;
    IF n = 10000 THEN
      n := 0;
      COMMIT;
    END IF;
  END LOOP;
  COMMIT;
END;
/

-- fill dataOnTape field (~15mn)
DECLARE
  n NUMBER := 0;
BEGIN
  FOR v IN (SELECT /*+ FULL(Cns_file_metadata) */ parent_fileid, sum(filesize) localSize
              FROM Cns_file_metadata WHERE status = 'm'
                                       AND mtime > &snapshotStartTime GROUP BY parent_fileid) LOOP
    UPDATE dirs
       SET dataOnTape = v.localSize
     WHERE fileid = v.parent_fileid;
    n := n + 1;
    IF n = 10000 THEN
      n := 0;
      COMMIT;
    END IF;
  END LOOP;
  COMMIT;
END;
/

-- compute number of tapes per directory for the whole hierarchy

-- first create an help table and fill it with directories containing files (~10mn)
CREATE TABLE dir2tape (fileid NUMBER NOT NULL, VID VARCHAR2(6), depth number) PCTFREE 0;
ALTER TABLE dir2tape PARALLEL (degree 8);
INSERT /*+ APPEND */ INTO dir2tape (select unique TapeHelp.parent, VID, depth FROM TapeHelp, dirs
                                     WHERE TapeHelp.parent = dirs.fileid AND lastModTime > &snapshotStartTime);
COMMIT;
CREATE INDEX I_tapeHelp_lastModTime on tapeHelp (lastModTime);
CREATE UNIQUE INDEX I_dir2tape_fileidVidDepth on dir2tape (fileid, VID, depth);
CREATE INDEX I_dir2tape_depth on dir2tape (depth, fileid);

-- then start from the bottom and for each level, merge child dirs into parent (~2h)
CREATE OR REPLACE TYPE numList IS TABLE OF INTEGER;
/
DECLARE
  d number;
  nbt number;
  curDir number := -1;
  children numList;
  n number := 0;
BEGIN
  -- start at max depth
  SELECT MAX(depth) INTO d FROM dirs;
  LOOP
    BEGIN
      SELECT /*+ INDEX(dirs I_dirs_fileid) */ parent INTO curDir
        FROM dirs
       WHERE fileid = (SELECT /*+ INDEX(dir2tape I_dir2tape_depth) */ fileid
                         FROM dir2tape
                        WHERE dir2tape.depth = d AND fileid > curDir AND rownum < 2);
      SELECT fileid bulk collect INTO children FROM dirs WHERE parent = curDir;
      MERGE /*+ INDEX(d I_dir2tape_fileidVidDepth ) */ INTO dir2tape d
      USING (SELECT /*+ INDEX(x I_dir2tape_fileidVidDepth ) */ unique VID, depth
               FROM dir2tape x WHERE fileid IN (SELECT /*+ CARDINALITY(t 5) */ * FROM TABLE(children) t)) v
         ON (d.VID = v.VID AND d.fileid = curDir)
       WHEN NOT MATCHED THEN
        INSERT VALUES (curDir, v.VID, v.depth-1);
      DELETE FROM dir2tape WHERE fileid IN (SELECT /*+ CARDINALITY(t 5) */ * FROM TABLE(children) t);
      SELECT count(*) INTO nbt FROM dir2Tape WHERE fileid = curDir;
      UPDATE dirs SET nbTapes = nvl(nbTapes,0)+nvl(nbt,0) WHERE fileid = curDir;
      n := n + 1;
      IF n = 2000 THEN
        COMMIT;
        n := 0;
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      d := d - 1;
      curDir := -1;
    END;
    COMMIT;
    EXIT WHEN d = 1;
  END LOOP;
END;
/
DROP TABLE dir2tape;

-- compute the fileclasses for the whole hierarchy

-- create dir2fileclass table and its temporary counter part
CREATE TABLE dir2fileclass (fileid INTEGER NOT NULL, fileclass INTEGER, nbFiles INTEGER, amountData INTEGER) PCTFREE 0;
ALTER TABLE dir2fileclass PARALLEL (degree 8);
CREATE TABLE tmpdir2fileclass (fileid INTEGER NOT NULL, fileclass INTEGER, nbFiles INTEGER, amountData INTEGER) PCTFREE 0;
ALTER TABLE tmpdir2fileclass PARALLEL (degree 8);

-- fill tmpdir2fileclass table with informations coming from files (~30m for 21.5M rows)
INSERT /*+ APPEND */ INTO tmpdir2fileclass
  (SELECT Cns_file_metadata.parent_fileid, fileclass, count(*), sum(filesize)
     FROM Cns_file_metadata
    GROUP BY Cns_file_metadata.parent_fileid, fileclass);
COMMIT;

-- create index on temporary table (20s for 21.5M rows)
CREATE INDEX I_tmpdir2fileclass_fileid on tmpdir2fileclass(fileid);

-- then start from the bottom and for each level, merge child dirs into parent (~10h)
--CREATE OR REPLACE TYPE numList IS TABLE OF INTEGER;
--/
DECLARE
  d NUMBER;
  nbt NUMBER;
  curDir NUMBER := -1;
  lastFileId NUMBER := -1;
  children numList;
  n NUMBER := 0;
BEGIN
  -- start at max depth
  SELECT MAX(depth) INTO d FROM dirs;
  LOOP
    BEGIN
      SELECT /*+ USE_NL(dirs tmpdir2fileclass)
                 INDEX(dirs I_dirs_depth)
                 INDEX(tmpdir2fileclass I_tmpdir2fileclass_fileid) */
             dirs.parent, dirs.fileid INTO curDir, lastFileId
        FROM tmpdir2fileclass, dirs
       WHERE dirs.depth = d
         AND dirs.fileid = tmpdir2fileclass.fileid
         AND dirs.fileId > lastFileId
         AND rownum < 2;
      SELECT fileid bulk collect INTO children FROM dirs WHERE parent = curDir;
      MERGE INTO tmpdir2fileclass
      USING (SELECT /*+ INDEX(tmpdir2fileclass I_tmpdir2fileclass_fileid ) */ fileclass, sum(nbFiles) totNbFiles, sum(amountData) totAmountData
               FROM tmpdir2fileclass
              WHERE fileid IN (SELECT /*+ CARDINALITY(t 5) */ * FROM TABLE(children) t)
              GROUP BY fileclass) v
         ON (tmpdir2fileclass.fileid = curDir AND tmpdir2fileclass.fileclass=v.fileclass)
       WHEN NOT MATCHED THEN INSERT VALUES (curDir, v.fileclass, v.totNbFiles, v.totAmountData)
       WHEN MATCHED THEN UPDATE SET nbFiles = nbFiles + v.totNbFiles, amountData = amountData + v.totAmountData;
      INSERT INTO dir2fileclass (SELECT fileid, fileclass, nbFiles, amountData FROM tmpdir2fileclass WHERE fileid = curDir);
      DELETE FROM tmpdir2fileclass WHERE fileid IN (SELECT /*+ CARDINALITY(t 5) */ * FROM TABLE(children) t);
      n := n + 1;
      IF n = 2000 THEN
        COMMIT;
        n := 0;
      END IF;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      d := d - 1;
      lastFileId := -1;
    END;
    COMMIT;
    EXIT WHEN d = 1;
  END LOOP;
END;
/

DROP TABLE tmpdir2fileclass;
-- create index on fileid (10s)
CREATE INDEX I_dir2fileclass_fileid on dir2fileclass(fileid);


-- fill dirs table at all levels for all fields but nbTapes and fileclasses (~12mn)
DECLARE
  d number;
  n number := 0;
  ts NUMBER;
  st NUMBER;
  dt NUMBER;
  nb NUMBER;
  nt NUMBER;
  nc NUMBER;
  ol NUMBER;
  av NUMBER;
  si NUMBER;
  ne NUMBER;
  ot NUMBER;
  at2 NUMBER;
  st2 NUMBER;
  nt2 NUMBER;
  nsd NUMBER;
BEGIN
  -- start at max depth
  SELECT MAX(depth) INTO d FROM dirs;
  LOOP
    FOR l IN (SELECT fileid FROM dirs WHERE dirs.depth = d) LOOP
      SELECT SUM(totalSize), SUM(sizeOnTape), SUM(dataOnTape), SUM(nbFiles), SUM(nbFilesOnTape), SUM(nbFileCopiesOnTape),
             MIN(oldestFileLastMod), SUM(avgFileLastMod), SUM(sigFileLastMod), MAX(newestFileLastMod),
             MIN(oldestFileOnTapeLastMod), SUM(avgFileOnTapeLastMod), SUM(sigFileOnTapeLastMod), MAX(newestFileOnTapeLastMod),
             SUM(nbSubDirs)+count(*)
        INTO ts, st, dt, nb, nt, nc, ol, av, si, ne, ot, at2, st2, nt2, nsd
        FROM dirs WHERE parent = l.fileid;
      UPDATE dirs
         SET totalSize = nvl(totalSize,0)+nvl(ts,0),
             sizeOnTape = nvl(sizeOnTape,0)+nvl(st,0),
             dataOnTape = nvl(dataOnTape,0)+nvl(dt,0),
             nbFiles = nvl(nbFiles,0)+nvl(nb,0),
             nbFilesOnTape = nvl(nbFilesOnTape,0)+nvl(nt,0),
             nbFileCopiesOnTape = nvl(nbFileCopiesOnTape,0)+nvl(nc,0),
             nbSubDirs = nvl(nsd,0),
             oldestFileLastMod = minimum(oldestFileLastMod,ol),
             avgFileLastMod = nvl(avgFileLastMod,0) + nvl(av,0),
             sigFileLastMod = nvl(sigFileLastMod,0) + nvl(si,0),
             newestFileLastMod = maximum(newestFileLastMod,ne),
             oldestFileOnTapeLastMod = minimum(oldestFileOnTapeLastMod,ot),
             avgFileOnTapeLastMod = nvl(avgFileOnTapeLastMod,0) + nvl(at2,0),
             sigFileOnTapeLastMod = nvl(sigFileOnTapeLastMod,0) + nvl(st2,0),
             newestFileOnTapeLastMod = maximum(newestFileOnTapeLastMod,nt2)
       WHERE fileid = l.fileid;
      n := n + 1;
      IF n = 2000 THEN
        COMMIT;
        n := 0;
      END IF;
    END LOOP;
    d := d - 1;
    COMMIT;
    EXIT WHEN d = 1;
  END LOOP;  
END;
/

-- finally compute proper averages, sigmas and times
UPDATE dirs
   SET avgFileLastMod = avgFileLastMod/nbFiles,
       sigFileLastMod = SQRT(sigFileLastMod/nbFiles - (avgFileLastMod/nbFiles)*(avgFileLastMod/nbFiles))
 WHERE nbFiles > 0;
COMMIT;
UPDATE dirs
   SET avgFileOnTapeLastMod = avgFileOnTapeLastMod/nbFileCopiesOnTape,
       sigFileOnTapeLastMod = SQRT(sigFileOnTapeLastMod/nbFileCopiesOnTape - (avgFileOnTapeLastMod/nbFileCopiesOnTape)*(avgFileOnTapeLastMod/nbFileCopiesOnTape))
 WHERE nbFilesOnTape > 0;
COMMIT;

CREATE INDEX I_dirs_fullname ON dirs (fullname);

DROP FUNCTION minimum;
DROP FUNCTION maximum;
DROP FUNCTION getTime;
