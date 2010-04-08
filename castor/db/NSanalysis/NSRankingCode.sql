-----------------
-- useful code --
-----------------

CREATE GLOBAL TEMPORARY TABLE WorstMigTempTable (
    fullName VARCHAR2(2048),
    timeToMigrate VARCHAR2(2048),
    timeSpentInTapeMarks VARCHAR2(2048),
    tapeMarksPart VARCHAR2(2048),
    avgFileSize VARCHAR2(2048),
    nbFileCopiesOnTape NUMBER,
    totalSize VARCHAR2(2048))
ON COMMIT DELETE ROWS;

CREATE GLOBAL TEMPORARY TABLE WorstRecallTempTable (
    fullName VARCHAR2(2048),
    estTimeToRecall VARCHAR2(2048),
    lostTime VARCHAR2(2048),
    lostPart VARCHAR2(2048),
    nbTapes NUMBER,
    avgFileSize VARCHAR2(2048),
    nbFileCopiesOnTape NUMBER,
    totalSize VARCHAR2(2048))
ON COMMIT DELETE ROWS;

CREATE GLOBAL TEMPORARY TABLE BiggestOnTapeTempTable (
    fullName VARCHAR2(2048),
    sizeOnTape VARCHAR2(2048),
    nbFileCopiesOnTape NUMBER,
    nbTapes NUMBER,
    timeToMigrate VARCHAR2(2048),
    estTimeToRecall VARCHAR2(2048))
ON COMMIT DELETE ROWS;

CREATE GLOBAL TEMPORARY TABLE BiggestOnDiskTempTable (
    fullName VARCHAR2(2048),
    sizeOfDiskOnlyData VARCHAR2(2048),
    nbDiskOnlyFiles NUMBER)
ON COMMIT DELETE ROWS;

CREATE OR REPLACE FUNCTION sec2date(nbsecs NUMBER) RETURN DATE AS
BEGIN
  RETURN to_date('19700101','YYYYMMDD')+ nbsecs/86400;
END;
/

CREATE OR REPLACE FUNCTION duration2char(duration NUMBER) RETURN VARCHAR2 AS
  r VARCHAR2(2048) := '';
  d NUMBER := duration;
BEGIN
  IF d = 0 THEN
    RETURN TO_CHAR(d,'99') || 's';
  END IF;
  IF d >= 365*24*3600 THEN
    r := r || TO_CHAR(FLOOR(d/365/24/3600),'99') || 'y';
    d := d - 365*24*3600*FLOOR(d/365/24/3600);
  END IF;
  IF d >= 24*3600 THEN
    r := r || TO_CHAR(FLOOR(d/24/3600),'999') || 'd';
    d := d - 24*3600*FLOOR(d/24/3600);
  END IF;
  IF d >= 3600 THEN
    r := r || TO_CHAR(FLOOR(d/3600),'99') || 'h';
    d := d - 3600*FLOOR(d/3600);
  END IF;
  IF d >= 60 THEN
    r := r || TO_CHAR(FLOOR(d/60),'99') || 'm';
    d := d - 60*FLOOR(d/60);
  END IF;
  IF d > 0 THEN
    r := r || TO_CHAR(d,'99') || 's';
  END IF;
  RETURN r;
END;
/

CREATE OR REPLACE FUNCTION size2char(s NUMBER) RETURN VARCHAR2 AS
BEGIN
  IF s >= 1024.0*1024*1024*1024*1024.0 THEN
    RETURN TO_CHAR(s/1024/1024/1024/1024/1024,'9999.9')||' PiB';
  END IF;
  IF s >= 1024.0*1024*1024*1024 THEN
    RETURN TO_CHAR(s/1024/1024/1024/1024,'9999.9')||' TiB';
  END IF;
  IF s >= 1024*1024*1024 THEN
    RETURN TO_CHAR(s/1024/1024/1024,'9999.9')||' GiB';
  END IF;
  IF s >= 1024*1024 THEN
    RETURN TO_CHAR(s/1024/1024,'9999.9')||' MiB';
  END IF;
  IF s >= 1024 THEN
    RETURN TO_CHAR(s/1024,'9999.9')||' KiB';
  END IF;
  RETURN TO_CHAR(s,'9999')||' B';
END;
/

----------------------
-- Worst migrations --
----------------------

--
-- total duration of migration
-- percentage of time spent in tape marks -> drive efficiency
-- average file size
-- number of files
-- ordered by time lost in tape marks
--
-- max positionning time = 90s
-- avg positioning time = 45s + 20s load time
-- 3s for the tape marks of a given file
-- total duration =  65s * nbTapes + 3s * nbFilesOnTape + datavolume / 110MB/s
--
-- only lines where >80% time is spent in tape marks are kept, except for the monthly
-- and weekly reports where we keep lines where >50% time is spent in tape marks
--
CREATE OR REPLACE PROCEDURE worstMigr(path VARCHAR2) AS
BEGIN
  INSERT INTO WorstMigTempTable (
    SELECT *  FROM (
      SELECT fullname, duration2char(timeToMigrate),
             duration2char(timeLostInTapeMarks),
             TO_CHAR(timeLostInTapeMarks/timeToMigrate*100,'999.9')||'%',
             size2char(sizeOnTape/nbFileCopiesOnTape), nbFileCopiesOnTape,
             size2char(totalSize)
        FROM dirs
       WHERE nbFileCopiesOnTape > 0 AND fullname LIKE path||'%'
         AND timeLostInTapeMarks/timeToMigrate > .8
       ORDER BY timeLostInTapeMarks DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE worstMigrWeek(path VARCHAR2) AS
BEGIN
  INSERT INTO WorstMigTempTable (
    SELECT *  FROM (
      SELECT fullname, duration2char(timeToMigrate),
             duration2char(timeLostInTapeMarks),
             TO_CHAR(timeLostInTapeMarks/timeToMigrate*100,'999.9')||'%',
             size2char(sizeOnTape/nbFileCopiesOnTape), nbFileCopiesOnTape,
             size2char(totalSize)
        FROM wdirs
       WHERE nbFileCopiesOnTape > 0 AND fullname LIKE path||'%'
         AND timeLostInTapeMarks/timeToMigrate > .5
       ORDER BY timeLostInTapeMarks DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE worstMigrMonth(path VARCHAR2) AS
BEGIN
  INSERT INTO WorstMigTempTable (
    SELECT *  FROM (
      SELECT fullname, duration2char(timeToMigrate),
             duration2char(timeLostInTapeMarks),
             TO_CHAR(timeLostInTapeMarks/timeToMigrate*100,'999.9')||'%',
             size2char(sizeOnTape/nbFileCopiesOnTape), nbFileCopiesOnTape,
             size2char(totalSize)
        FROM mdirs
       WHERE nbFileCopiesOnTape > 0 AND fullname LIKE path||'%'
         AND timeLostInTapeMarks/timeToMigrate > .5
       ORDER BY timeLostInTapeMarks DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE worstMigrYear(path VARCHAR2) AS
BEGIN
  INSERT INTO WorstMigTempTable (
    SELECT *  FROM (
      SELECT fullname, duration2char(timeToMigrate),
             duration2char(timeLostInTapeMarks),
             TO_CHAR(timeLostInTapeMarks/timeToMigrate*100,'999.9')||'%',
             size2char(sizeOnTape/nbFileCopiesOnTape), nbFileCopiesOnTape,
             size2char(totalSize)
        FROM ydirs
       WHERE nbFileCopiesOnTape > 0 AND fullname LIKE path||'%'
         AND timeLostInTapeMarks/timeToMigrate > .8
       ORDER BY timeLostInTapeMarks DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE worstMigrGeneric(period VARCHAR2, path VARCHAR2) AS
BEGIN
  CASE period
    WHEN 'All' THEN worstMigr(path);
    WHEN 'Year' THEN worstMigrYear(path);
    WHEN 'Month' THEN worstMigrMonth(path);
    WHEN 'Week' THEN worstMigrWeek(path);
  END CASE;
END;
/


-------------------
-- Worst recalls --
-------------------

-- avg seek time between 2 files = max pos time / 3 = 30
-- full tape read time = 1T/100MB/s ~= 10000
-- total time to recall = 65s * nbTapes + min(30*(nbFilesOnTape-1)+sizeOnTape/110000000, 10000)
-- recall efficiency (optimal time / total time)
-- number of tapes
-- number of files
-- average file size
-- order by time lost (ie spent - minimum to be spent) in tape mounts+seeking
--
-- Only lines where >80% time is lost are kept, except for the weekly and monthly
-- reports where we keep lines where > 50% time is lost
--

CREATE OR REPLACE PROCEDURE worstRecall(path VARCHAR2) AS
BEGIN
  INSERT INTO WorstRecallTempTable (
    SELECT * FROM (
      SELECT fullname, duration2char(timeToRecall), duration2char(timeToRecall-optTimeToRecall),
             TO_CHAR((timeToRecall-optTimeToRecall)/timeToRecall*100,'999.9')||'%',
             nbTapes, size2char(sizeOnTape/nbFileCopiesOnTape), nbFileCopiesOnTape, size2char(totalSize)
        FROM dirs
       WHERE nbFileCopiesOnTape > 0 AND fullname LIKE path||'%'
         AND (timeToRecall-optTimeToRecall)/timeToRecall > .8
       ORDER BY timeToRecall-optTimeToRecall DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE worstRecallWeek(path VARCHAR2) AS
BEGIN
  INSERT INTO WorstRecallTempTable (
    SELECT * FROM (
      SELECT fullname, duration2char(timeToRecall), duration2char(timeToRecall-optTimeToRecall),
             TO_CHAR((timeToRecall-optTimeToRecall)/timeToRecall*100,'999.9')||'%',
             nbTapes, size2char(sizeOnTape/nbFileCopiesOnTape), nbFileCopiesOnTape, size2char(totalSize)
        FROM wdirs
       WHERE nbFileCopiesOnTape > 0 AND fullname LIKE path||'%'
         AND (timeToRecall-optTimeToRecall)/timeToRecall > .5
       ORDER BY timeToRecall-optTimeToRecall DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE worstRecallMonth(path VARCHAR2) AS
BEGIN
  INSERT INTO WorstRecallTempTable (
    SELECT * FROM (
      SELECT fullname, duration2char(timeToRecall), duration2char(timeToRecall-optTimeToRecall),
             TO_CHAR((timeToRecall-optTimeToRecall)/timeToRecall*100,'999.9')||'%',
             nbTapes, size2char(sizeOnTape/nbFileCopiesOnTape), nbFileCopiesOnTape, size2char(totalSize)
        FROM mdirs
       WHERE nbFileCopiesOnTape > 0 AND fullname LIKE path||'%'
         AND (timeToRecall-optTimeToRecall)/timeToRecall > .5
       ORDER BY timeToRecall-optTimeToRecall DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE worstRecallYear(path VARCHAR2) AS
BEGIN
  INSERT INTO WorstRecallTempTable (
    SELECT * FROM (
      SELECT fullname, duration2char(timeToRecall), duration2char(timeToRecall-optTimeToRecall),
             TO_CHAR((timeToRecall-optTimeToRecall)/timeToRecall*100,'999.9')||'%',
             nbTapes, size2char(sizeOnTape/nbFileCopiesOnTape), nbFileCopiesOnTape, size2char(totalSize)
        FROM ydirs
       WHERE nbFileCopiesOnTape > 0 AND fullname LIKE path||'%'
         AND (timeToRecall-optTimeToRecall)/timeToRecall > .8
       ORDER BY timeToRecall-optTimeToRecall DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE worstRecallGeneric(period VARCHAR2, path VARCHAR2) AS
BEGIN
  CASE period
    WHEN 'All' THEN worstRecall(path);
    WHEN 'Year' THEN worstRecallYear(path);
    WHEN 'Month' THEN worstRecallMonth(path);
    WHEN 'Week' THEN worstRecallWeek(path);
  END CASE;
END;
/

-----------------
-- big volumes --
-----------------

-- biggest volumes on tape

CREATE OR REPLACE PROCEDURE biggestOnTape(path VARCHAR2) AS
BEGIN
  INSERT INTO biggestOnTapeTempTable (
    SELECT * FROM (
      SELECT fullname, size2char(sizeOnTape), nbFileCopiesOnTape, nbTapes, duration2char(timeToMigrate), duration2char(timeToRecall)
        FROM dirs
       WHERE fullname LIKE path||'%'
         AND sizeOnTape > 0
       ORDER BY sizeOnTape DESC)
    WHERE ROWNUM < 30);
END;
/

CREATE OR REPLACE PROCEDURE biggestOnTapeWeek(path VARCHAR2) AS
BEGIN
  INSERT INTO biggestOnTapeTempTable (
    SELECT * FROM (
      SELECT fullname, size2char(sizeOnTape), nbFileCopiesOnTape, nbTapes, duration2char(timeToMigrate), duration2char(timeToRecall)
        FROM wdirs
       WHERE fullname LIKE path||'%'
         AND sizeOnTape > 0
       ORDER BY sizeOnTape DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE biggestOnTapeMonth(path VARCHAR2) AS
BEGIN
  INSERT INTO biggestOnTapeTempTable (
    SELECT * FROM (
      SELECT fullname, size2char(sizeOnTape), nbFileCopiesOnTape, nbTapes, duration2char(timeToMigrate), duration2char(timeToRecall)
        FROM mdirs
       WHERE fullname LIKE path||'%'
         AND sizeOnTape > 0
       ORDER BY sizeOnTape DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE biggestOnTapeYear(path VARCHAR2) AS
BEGIN
  INSERT INTO biggestOnTapeTempTable (
    SELECT * FROM (
      SELECT fullname, size2char(sizeOnTape), nbFileCopiesOnTape, nbTapes, duration2char(timeToMigrate), duration2char(timeToRecall)
        FROM ydirs
       WHERE fullname LIKE path||'%'
         AND sizeOnTape > 0
       ORDER BY sizeOnTape DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE biggestOnTapeGeneric(period VARCHAR2, path VARCHAR2) AS
BEGIN
  CASE period
    WHEN 'All' THEN biggestOnTape(path);
    WHEN 'Year' THEN biggestOnTapeYear(path);
    WHEN 'Month' THEN biggestOnTapeMonth(path);
    WHEN 'Week' THEN biggestOnTapeWeek(path);
  END CASE;
END;
/

-- biggest volume on disk

CREATE OR REPLACE PROCEDURE biggestOnDisk(path VARCHAR2) AS
BEGIN
  INSERT INTO biggestOnDiskTempTable (
    SELECT * FROM (
      SELECT fullname, size2char(totalSize-dataOnTape), nbFiles-nbFilesOnTape
        FROM dirs
       WHERE fullname LIKE path||'%'
         AND totalSize-dataOnTape > 0
       ORDER BY totalSize-dataOnTape DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE biggestOnDiskWeek(path VARCHAR2) AS
BEGIN
  INSERT INTO biggestOnDiskTempTable (
    SELECT * FROM (
      SELECT fullname, size2char(totalSize-dataOnTape), nbFiles-nbFilesOnTape
        FROM wdirs
       WHERE fullname LIKE path||'%'
         AND totalSize-dataOnTape > 0
       ORDER BY totalSize-dataOnTape DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE biggestOnDiskMonth(path VARCHAR2) AS
BEGIN
  INSERT INTO biggestOnDiskTempTable (
    SELECT * FROM (
      SELECT fullname, size2char(totalSize-dataOnTape), nbFiles-nbFilesOnTape
        FROM mdirs
       WHERE fullname LIKE path||'%'
         AND totalSize-dataOnTape > 0
       ORDER BY totalSize-dataOnTape DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE biggestOnDiskYear(path VARCHAR2) AS
BEGIN
  INSERT INTO biggestOnDiskTempTable (
    SELECT * FROM (
      SELECT fullname, size2char(totalSize-dataOnTape), nbFiles-nbFilesOnTape
        FROM ydirs
       WHERE fullname LIKE path||'%'
         AND totalSize-dataOnTape > 0
       ORDER BY totalSize-dataOnTape DESC)
    WHERE ROWNUM < 30);
END;
/
CREATE OR REPLACE PROCEDURE biggestOnDiskGeneric(period VARCHAR2, path VARCHAR2) AS
BEGIN
  CASE period
    WHEN 'All' THEN biggestOnDisk(path);
    WHEN 'Year' THEN biggestOnDiskYear(path);
    WHEN 'Month' THEN biggestOnDiskMonth(path);
    WHEN 'Week' THEN biggestOnDiskWeek(path);
  END CASE;
END;
/
