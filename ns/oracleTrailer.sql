/*****************************************************************************
 *              oracleTrailer.sql
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_13_0';


/* Package holding type declarations for the NameServer PL/SQL API */
CREATE OR REPLACE PACKAGE castorns AS
  TYPE "strList" IS TABLE OF VARCHAR2(2048) index BY binary_integer;
  TYPE Segment IS RECORD (
    fileId NUMBER;
    lastModTime NUMBER;
    copyNo INTEGER;
    segSize INTEGER;
    comprSize INTEGER;
    vid VARCHAR2(6);
    fseq INTEGER;
    blockId RAW(4);
    checksum_name VARCHAR2(2);
    checksum INTEGER;
  );
  TYPE Segment_Cur IS REF CURSOR RETURN Segment;
  TYPE Log IS ResultLogHelper%ROWTYPE;
END castorns;
/

/* Useful types */
CREATE OR REPLACE TYPE strListTable AS TABLE OF VARCHAR2(2048);
CREATE OR REPLACE TYPE numListTable IS TABLE OF INTEGER;

/**
 * Package containing the definition of all DLF levels and messages for the SQL-to-DLF API.
 * The actual logging is performed by the stagers, the NS PL/SQL code pushes logs to the callers.
 */
CREATE OR REPLACE PACKAGE dlf
AS
  /* message levels */
  LVL_EMERGENCY  CONSTANT PLS_INTEGER := 0; /* LOG_EMERG   System is unusable */
  LVL_ALERT      CONSTANT PLS_INTEGER := 1; /* LOG_ALERT   Action must be taken immediately */
  LVL_CRIT       CONSTANT PLS_INTEGER := 2; /* LOG_CRIT    Critical conditions */
  LVL_ERROR      CONSTANT PLS_INTEGER := 3; /* LOG_ERR     Error conditions */
  LVL_WARNING    CONSTANT PLS_INTEGER := 4; /* LOG_WARNING Warning conditions */
  LVL_USER_ERROR CONSTANT PLS_INTEGER := 5; /* LOG_NOTICE  Normal but significant condition */
  LVL_AUTH       CONSTANT PLS_INTEGER := 5; /* LOG_NOTICE  Normal but significant condition */
  LVL_SECURITY   CONSTANT PLS_INTEGER := 5; /* LOG_NOTICE  Normal but significant condition */
  LVL_SYSTEM     CONSTANT PLS_INTEGER := 6; /* LOG_INFO    Informational */
  LVL_DEBUG      CONSTANT PLS_INTEGER := 7; /* LOG_DEBUG   Debug-level messages */
  
  /* (s)errno values */
  ENOENT          CONSTANT PLS_INTEGER := 2;    /* No such file or directory */
  EACCES          CONSTANT PLS_INTEGER := 13;   /* Permission denied */
  EEXIST          CONSTANT PLS_INTEGER := 17;   /* File exists */
  EISDIR          CONSTANT PLS_INTEGER := 21;   /* Is a directory */
  
  SEINTERNAL      CONSTANT PLS_INTEGER := 1015; /* Internal error */
  SECHECKSUM      CONSTANT PLS_INTEGER := 1037; /* Bad checksum */
  ENSFILECHG      CONSTANT PLS_INTEGER := 1402; /* File has been overwritten, request ignored */
  ENSNOSEG        CONSTANT PLS_INTEGER := 1403; /* Segment had been deleted */
  ENSISLINK       CONSTANT PLS_INTEGER := 1404; /* Is a symbolic link */
  ENSTOOMANYSEGS  CONSTANT PLS_INTEGER := 1406; /* Too many copies on tape */
  ENSOVERWHENREP  CONSTANT PLS_INTEGER := 1407; /* Cannot overwrite valid segment when replacing */
  
  /* messages */
END dlf;
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

/* A function to extract the full path of a file in one go */
CREATE OR REPLACE FUNCTION getPathForFileid(fid IN NUMBER) RETURN VARCHAR2 IS
  CURSOR c IS
    SELECT /*+ NO_CONNECT_BY_COST_BASED */ name
      FROM cns_file_metadata
    START WITH fileid = fid
    CONNECT BY fileid = PRIOR parent_fileid
    ORDER BY level DESC;
  p VARCHAR2(2048) := '';
BEGIN
   FOR i in c LOOP
     p := p ||  '/' || i.name;
   END LOOP;
   -- remove first '/'
   p := replace(p, '///', '/');
   IF length(p) > 1024 THEN
     -- the caller will return SENAMETOOLONG
     raise_application_error(-20001, '');
   END IF;
   RETURN p;
END;
/

/* This function returns a list of valid segments for a given file.
 * By valid, we mean valid segments on non disable tapes. So VMGR
 * is checked inside the function.
 * The returned REF CURSOR is of type Segment.
 */
CREATE OR REPLACE FUNCTION getValidSegmentsForRecall(fid IN NUMBER) RETURN castorns.Segment_Cur IS
  CURSOR res IS
    SELECT s_fileId, 0 as lastModTime, copyNo, segSize, 0 as comprSize,
           vid, fseq, blockId, checksum_value, nvl(checksum, 0)
      FROM Cns_seg_metadata, Vmgr_tape_side
     WHERE Cns_seg_metadata.s_fileid = fid
       AND Cns_seg_metadata.status = '-'
       AND Vmgr_tape_side.VID = Cns_seg_metadata.VID
       AND Vmgr_tape_side.status NOT IN (1, 2, 32)  -- DISABLED, EXPORTED, ARCHIVED
     ORDER BY copyno, fsec;
BEGIN
  OPEN res;
  RETURN res;
END;
/

/* This procedure creates a segment for a given file.
 * Return code:
 * 0  in case of success
 * ENOENT         if the file does not exist (e.g. it had been dropped).
 * EISDIR         if the file is a directory.
 * EEXIST         if the file already has a valid segment on the same tape.
 * ENSFILECHG     if the file has been modified meanwhile.
 * ENSTOOMANYSEGS if this copy exceeds the number of copies allowed by the file's fileclass.
 *                This also applies if a segment is attached to a file which fileclass allows 0 copies.
 */
CREATE OR REPLACE PROCEDURE setFileMigrated(segEntry IN castorns.Segment, rc OUT INTEGER, msg OUT NOCOPY VARCHAR2(2048)) AS
DECLARE
  fid NUMBER;
  fmode NUMBER(6);
  fLastMTime NUMBER;
  fClassId NUMBER;
  fcNbCopies NUMBER;
  fCkSumName VARCHAR2(2);
  fCkSum VARCHAR2(32);
  nbExistingSegs INTEGER;
  -- Trap `ORA-00001: unique constraint violated` errors
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
BEGIN
  rc := 0;
  msg := '';
  -- Get file data and lock the entry, exit if not found
  SELECT fileId, filemode, mtime, fileClass, csumType, csumValue
    INTO fid, fmode, fLastMTime, fClassId, fCkSumName, fCkSum
    FROM Cns_file_metadata
   WHERE fileId = segEntry.fileId FOR UPDATE;
  -- Is it a directory?
  IF bitand(fmode, 4*8*8*8*8) > 0 THEN  -- 0x40000 == S_IFDIR
    rc := dlf.EISDIR;
    ROLLBACK;
    RETURN;
  END IF;
  -- Has the file been changed meanwhile?
  IF fLastMTime > segEntry.lastModTime AND segEntry.lastModTime > 0 THEN
    rc := dlf.ENSFILECHG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Cross check file and segment checksums when adler32 (AD in the file entry):
  -- unfortunately we have to play with different representations...
  IF fCkSumName = 'AD' AND segEntry.checksum_name = 'adler32' AND
     segEntry.checksum != to_number(fCkSum, 'XXXXXXXX') THEN
    rc := dlf.SECHECKSUM;
    msg := 'Checksum mismatch between segment and file: '
      || to_char(segEntry.checksum, 'XXXXXXXX') || ' vs ' || fCkSum;
    ROLLBACK;
    RETURN;
  END IF;
  -- Prevent to write a second copy of this file
  -- on a tape that already holds a valid copy
  SELECT count(*) INTO nbExistingSegs
    FROM Cns_seg_metadata
   WHERE s_fileid = fid AND s_status = '-' AND vid = segEntry.vid;
  IF nbExistingSegs > 0 THEN
    rc := dlf.EEXIST;
    msg := 'File already has a copy on the tape';
    ROLLBACK;
    RETURN;
  END IF;
  
  -- We're done with the pre-checks, try and insert the segment metadata
  -- and deal with the possible exceptions: PK violation, unique (vid,fseq) violation
  BEGIN
    INSERT INTO Cns_segment_metadata (s_fileId, copyNo, fsec, segSize, s_status,
      vid, fseq, blockId, compression, side, checksum_name, checksum)
    VALUES (fid, segEntry.copyNb, 1, segEntry.segSize, '-',
      segEntry.vid, segEntry.fseq, segEntry.blockId, trunc(segEntry.segSize*100/segEntry.comprSize),
      0, segEntry.checksum_name, segEntry.checksum);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    -- We assume the PK was violated, i.e. a previous segment already exists and we need to update it.
    -- XXX This should not happen in post-2.1.13 versions as we drop previous versions of the segments
    -- XXX at file creation/truncation time!
    UPDATE Cns_segment_metadata
       SET fsec = 1, segSize = segEntry.segSize, s_status = '-',
           vid = segEntry.vid, fseq = segEntry.fseq, blockId = segEntry.blockId,
           compression = trunc(segEntry.segSize*100/segEntry.comprSize),
           side = 0, checksum_name = segEntry.checksum_name,
           checksum = segEntry.checksum
     WHERE s_fileId = fid
       AND copyNo = segEntry.copyNb;
    IF SQL%ROWCOUNT = 0 THEN
      -- The update failed, thus the CONSTRAINT_VIOLATED was due to an existing segment at that fseq position.
      -- This is forbidden!
      rc := dlf.SEINTERNAL;
      msg := 'A file already exists at fseq='|| segEntry.fseq ||' on VID='|| segEntry.vid;
      ROLLBACK;
      RETURN;
    END IF;
  END;
  -- Finally check for too many segments for this file
  SELECT nbCopies INTO fcNbCopies
    FROM Cns_class_metadata
   WHERE classid = fClassId;
  SELECT count(*) INTO effectiveNbCopies
    FROM Cns_seg_metadata
   WHERE s_fileid = fid AND s_status = '-';
  IF effectiveNbCopies > fcNbCopies THEN
    rc := dlf.ENSTOOMANYSEGS;
    ROLLBACK;
    RETURN;
  END IF;
  -- Update file status
  UPDATE Cns_file_metadata SET status = 'm' WHERE fileid = fid;
  COMMIT;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- The file entry was not found, just give up
  rc := dlf.ENOENT;
END;
/
