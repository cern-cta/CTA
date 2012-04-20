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
  TYPE Log_Cur IS REF CURSOR RETURN ResultsLogHelper%ROWTYPE;
  TYPE Segment_Rec IS RECORD (
    fileId NUMBER,
    lastModTime NUMBER,
    copyNo INTEGER,
    segSize INTEGER,
    comprSize INTEGER,
    vid VARCHAR2(6),
    fseq INTEGER,
    blockId RAW(4),
    checksum_name VARCHAR2(2),
    checksum INTEGER
  );
  TYPE Segment_Cur IS REF CURSOR RETURN Segment_Rec;
  TYPE SegmentList IS TABLE OF Segment_Rec;
END castorns;
/

/* Useful types */
CREATE OR REPLACE TYPE strListTable IS TABLE OF VARCHAR2(2048);
/
CREATE OR REPLACE TYPE numListTable IS TABLE OF INTEGER;
/

/**
 * Package containing the definition of all DLF levels and messages for the SQL-to-DLF API.
 * The actual logging is performed by the stagers, the NS PL/SQL code pushes logs to the callers.
 */
CREATE OR REPLACE PACKAGE dlf AS
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

CREATE OR REPLACE FUNCTION getSecs(startTime IN TIMESTAMP, endTime IN TIMESTAMP) RETURN NUMBER IS
BEGIN
  RETURN EXTRACT(SECOND FROM (endTime - startTime));
END;
/

/* Generate a universally unique id (UUID) */
CREATE OR REPLACE FUNCTION uuidGen RETURN VARCHAR2 IS
  ret VARCHAR2(36);
BEGIN
  -- Note: the guid generator provided by ORACLE produces sequential uuid's, not
  -- random ones. The reason for this is because random uuid's are not good for
  -- indexing!
  RETURN lower(regexp_replace(sys_guid(), '(.{8})(.{4})(.{4})(.{4})(.{12})', '\1-\2-\3-\4-\5'));
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
    SELECT s_fileId as fileId, 0 as lastModTime, copyNo, segSize, 0 as comprSize,
           Cns_seg_metadata.vid, fseq, blockId, checksum_name, nvl(checksum, 0) as checksum
      FROM Cns_seg_metadata, Vmgr_tape_side
     WHERE Cns_seg_metadata.s_fileid = fid
       AND cns_seg_metadata.s_status = '-'
       AND Vmgr_tape_side.VID = Cns_seg_metadata.VID
       AND Vmgr_tape_side.status NOT IN (1, 2, 32)  -- DISABLED, EXPORTED, ARCHIVED
     ORDER BY copyNo, fsec;
BEGIN
  OPEN res;
  RETURN res;
END;
/

/**
 * This procedure creates a segment for a given file.
 * Return code:
 * 0  in case of success
 * ENOENT         if the file does not exist (e.g. it had been dropped).
 * EISDIR         if the file is a directory.
 * EEXIST         if the file already has a valid segment on the same tape.
 * ENSFILECHG     if the file has been modified meanwhile.
 * SEINTERNAL     if another segment exists on the given tape at the given fseq location.
 * ENSTOOMANYSEGS if this copy exceeds the number of copies allowed by the file's fileclass.
 *                This also applies if a segment is attached to a file which fileclass allows 0 copies.
 */
CREATE OR REPLACE PROCEDURE setSegmentForFile(segEntry IN castorns.Segment_Rec, rc OUT INTEGER, msg OUT NOCOPY VARCHAR2) AS
  fid NUMBER;
  fmode NUMBER(6);
  fLastMTime NUMBER;
  fClassId NUMBER;
  fcNbCopies NUMBER;
  fCkSumName VARCHAR2(2);
  fCkSum VARCHAR2(32);
  varNb INTEGER;
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
    msg := 'ErrorMessage="File is a directory"';
    ROLLBACK;
    RETURN;
  END IF;
  -- Has the file been changed meanwhile?
  IF fLastMTime > segEntry.lastModTime AND segEntry.lastModTime > 0 THEN
    rc := dlf.ENSFILECHG;
    msg := 'ErrorMessage="File has been overwritten, request ignored"';
    ROLLBACK;
    RETURN;
  END IF;
  -- Cross check file and segment checksums when adler32 (AD in the file entry):
  -- unfortunately we have to play with different representations...
  IF fCkSumName = 'AD' AND segEntry.checksum_name = 'adler32' AND
     segEntry.checksum != to_number(fCkSum, 'XXXXXXXX') THEN
    rc := dlf.SECHECKSUM;
    msg := 'ErrorMessage="Checksum mismatch between segment and file: '
      || to_char(segEntry.checksum, 'XXXXXXXX') || ' vs ' || fCkSum ||'"';
    ROLLBACK;
    RETURN;
  END IF;
  -- Prevent to write a second copy of this file
  -- on a tape that already holds a valid copy
  SELECT count(*) INTO varNb
    FROM Cns_seg_metadata
   WHERE s_fileid = fid AND s_status = '-' AND vid = segEntry.vid;
  IF varNb > 0 THEN
    rc := dlf.EEXIST;
    msg := 'ErrorMessage="File already has a copy on the tape"';
    ROLLBACK;
    RETURN;
  END IF;
  
  -- We're done with the pre-checks, try and insert the segment metadata
  -- and deal with the possible exceptions: PK violation, unique (vid,fseq) violation
  BEGIN
    INSERT INTO Cns_seg_metadata (s_fileId, copyNo, fsec, segSize, s_status,
      vid, fseq, blockId, compression, side, checksum_name, checksum)
    VALUES (fid, segEntry.copyNo, 1, segEntry.segSize, '-',
      segEntry.vid, segEntry.fseq, segEntry.blockId, trunc(segEntry.segSize*100/segEntry.comprSize),
      0, segEntry.checksum_name, segEntry.checksum);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    -- We assume the PK was violated, i.e. a previous segment already exists and we need to update it.
    -- XXX This should not happen in post-2.1.13 versions as we drop previous versions of the segments
    -- XXX at file creation/truncation time!
    UPDATE Cns_seg_metadata
       SET fsec = 1, segSize = segEntry.segSize, s_status = '-',
           vid = segEntry.vid, fseq = segEntry.fseq, blockId = segEntry.blockId,
           compression = trunc(segEntry.segSize*100/segEntry.comprSize),
           side = 0, checksum_name = segEntry.checksum_name,
           checksum = segEntry.checksum
     WHERE s_fileId = fid
       AND copyNo = segEntry.copyNo;
    IF SQL%ROWCOUNT = 0 THEN
      -- The update failed, thus the CONSTRAINT_VIOLATED was due to an existing segment at that fseq position.
      -- This is forbidden!
      rc := dlf.SEINTERNAL;
      msg := 'ErrorMessage="A file already exists at fseq '|| segEntry.fseq ||' on VID '|| segEntry.vid ||'"';
      ROLLBACK;
      RETURN;
    END IF;
  END;
  -- Finally check for too many segments for this file
  SELECT nbCopies INTO fcNbCopies
    FROM Cns_class_metadata
   WHERE classid = fClassId;
  SELECT count(*) INTO varNb
    FROM Cns_seg_metadata
   WHERE s_fileid = fid AND s_status = '-';
  IF varNb > fcNbCopies THEN
    rc := dlf.ENSTOOMANYSEGS;
    msg := 'ErrorMessage="Too many copies on tape" VID="'|| segEntry.vid ||'"';
    ROLLBACK;
    RETURN;
  END IF;
  -- Update file status
  UPDATE Cns_file_metadata SET status = 'm' WHERE fileid = fid;
  COMMIT;
  -- Log
  msg := 'CopyNo='|| segEntry.copyNo ||' Fsec=1 SegmentSize='|| segEntry.segSize
    ||' Compression='|| trunc(segEntry.segSize*100/segEntry.comprSize) ||' VID='|| segEntry.vid
    ||' Fseq='|| segEntry.fseq ||' BlockId='-- || to_char(cast(segEntry.blockId as NUMBER), 'XXXX')  -- XXX need to find how to convert a RAW type (and wondering why to use RAW for a 32-bit word in the first place...)
    ||' ChecksumType='|| segEntry.checksum_name ||' ChecksumValue=' || fCkSum;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- The file entry was not found, just give up
  rc := dlf.ENOENT;
  msg := 'ErrorMessage="No such file or directory"';
END;
/

/**
 * This procedure creates segments for multiple files by calling setSegmentForFile in a loop.
 * The output is a log table ready to be sent to DLF from a stager database.
 */
CREATE OR REPLACE PROCEDURE setSegmentsForFiles(segs IN castorns.SegmentList, logs OUT castorns.Log_Cur) AS
  varRC INTEGER;
  varParams VARCHAR2(1000);
  varStartTime TIMESTAMP;
  varReqid VARCHAR2(36);
BEGIN
  varStartTime := SYSTIMESTAMP;
  varReqid := uuidGen();
  -- First clean any previous log
  EXECUTE IMMEDIATE 'TRUNCATE TABLE ResultsLogHelper';
  -- Loop over all files. Each call commits or rollbacks each file.
  FOR i IN segs.FIRST .. segs.LAST LOOP
    setSegmentForFile(segs(i), varRC, varParams);
    INSERT INTO ResultsLogHelper (timeinfo, lvl, reqid, msg, fileId, params)
      VALUES (getTime(), CASE rc WHEN 0 THEN dlf.LVL_SYSTEM ELSE dlf.LVL_ERROR END, varReqid, 
              CASE varRC WHEN 0 THEN 'New segment information' ELSE 'Error creating/updating segment' END,
              segs(i).fileId, varParams);
  END LOOP;
  -- Final logging
  varParams := 'Function=setSegmentsForFiles NbFiles='|| segs.COUNT ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP);
  INSERT INTO ResultsLogHelper (timeinfo, lvl, reqid, msg, fileId, params)
    VALUES (getTime(), dlf.LVL_SYSTEM, varReqid, 'Bulk processing complete', 0, varParams);
  -- Return logs to the stager
  OPEN logs FOR SELECT * FROM ResultsLogHelper;
END;
/
