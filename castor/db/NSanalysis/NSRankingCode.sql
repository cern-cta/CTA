-----------------
-- useful code --
-----------------

CREATE GLOBAL TEMPORARY TABLE BiggestOnTapeTempTable (
    fullName VARCHAR2(2048),
    sizeOnTape VARCHAR2(2048),
    nbFileCopiesOnTape NUMBER,
    nbTapes NUMBER)
ON COMMIT DELETE ROWS;

CREATE GLOBAL TEMPORARY TABLE BiggestOnDiskTempTable (
    fullName VARCHAR2(2048),
    sizeOfDiskOnlyData VARCHAR2(2048),
    nbDiskOnlyFiles NUMBER)
ON COMMIT DELETE ROWS;

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

-----------------
-- big volumes --
-----------------

-- biggest volumes on tape

CREATE OR REPLACE PROCEDURE biggestOnTape(path VARCHAR2) AS
BEGIN
  INSERT INTO biggestOnTapeTempTable (
    SELECT * FROM (
      SELECT fullname, size2char(sizeOnTape), nbFileCopiesOnTape, nbTapes
        FROM dirs
       WHERE fullname LIKE path||'%'
         AND sizeOnTape > 0
       ORDER BY sizeOnTape DESC)
    WHERE ROWNUM < 30);
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
