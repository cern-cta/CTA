/******************************************************************************
 *                 cns_2.1.12-4_to_2.1.13-0.sql
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
 * This script upgrades a CASTOR v2.1.12-4 CNS database to v2.1.13-0
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  -- If we have encountered an error rollback any previously non committed
  -- operations. This prevents the UPDATE of the UpgradeLog from committing
  -- inconsistent data to the database.
  ROLLBACK;
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_13_0'
     AND release = '2_1_13_0'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'CNS'
     AND (release LIKE '2_1_12_4%');
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the CNS before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_13_0', '2_1_13_0', 'TRANSPARENT');
COMMIT;


/* Schema changes go here */
/**************************/

/* Package holding type declarations for the NameServer PL/SQL API */
CREATE OR REPLACE PACKAGE castorns AS
  TYPE cnumList IS TABLE OF INTEGER INDEX BY BINARY_INTEGER;
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
CREATE OR REPLACE TYPE strList IS TABLE OF VARCHAR2(2048);
/
CREATE OR REPLACE TYPE numList IS TABLE OF INTEGER;
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
  ENOENT_MSG          CONSTANT VARCHAR2(2048) := 'No such file or directory';
  EACCES_MSG          CONSTANT VARCHAR2(2048) := 'Permission denied';
  EEXIST_MSG          CONSTANT VARCHAR2(2048) := 'File exists';
  EISDIR_MSG          CONSTANT VARCHAR2(2048) := 'Is a directory';
  SEINTERNAL_MSG      CONSTANT VARCHAR2(2048) := 'Internal error';
  SECHECKSUM_MSG      CONSTANT VARCHAR2(2048) := 'Checksum mismatch between segment and file';
  ENSFILECHG_MSG      CONSTANT VARCHAR2(2048) := 'File has been overwritten, request ignored';
  ENSNOSEG_MSG        CONSTANT VARCHAR2(2048) := 'Segment had been deleted';
  ENSTOOMANYSEGS_MSG  CONSTANT VARCHAR2(2048) := 'Too many copies on tape';
  ENSISLINK_MSG       CONSTANT VARCHAR2(2048) := 'Is a symbolic link';
  ENSOVERWHENREP_MSG  CONSTANT VARCHAR2(2048) := 'Cannot overwrite valid segment when replacing';
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

/* Returns a time interval in seconds */
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

/* A small procedure to add a line to the temporary DLF log */
CREATE OR REPLACE PROCEDURE tmpDlfLog(inLvl IN INTEGER, inReqid IN VARCHAR2, inMsg IN VARCHAR2,
                                      inFileId IN NUMBER, inParams IN VARCHAR2) AS
BEGIN
  INSERT INTO ResultsLogHelper (timeinfo, lvl, reqid, msg, fileId, params)
    VALUES (getTime(), inLvl, inReqid, inMsg, inFileId, inParams);
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
CREATE OR REPLACE PROCEDURE setSegmentForFile(segEntry IN castorns.Segment_Rec,
                                              rc OUT INTEGER, msg OUT NOCOPY VARCHAR2) AS
  fid NUMBER;
  fmode NUMBER(6);
  fLastMTime NUMBER;
  fClassId NUMBER;
  fcNbCopies NUMBER;
  fCkSumName VARCHAR2(2);
  fCkSum VARCHAR2(32);
  varNb INTEGER;
  varBlockId VARCHAR2(8);
  varParams VARCHAR2(2048);
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
  IF bitand(fmode, 4*8*8*8*8) > 0 THEN  -- 040000 == S_IFDIR
    rc := dlf.EISDIR;
    msg := dlf.EISDIR_MSG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Has the file been changed meanwhile?
  IF fLastMTime > segEntry.lastModTime AND segEntry.lastModTime > 0 THEN
    rc := dlf.ENSFILECHG;
    msg := dlf.ENSFILECHG_MSG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Cross check file and segment checksums when adler32 (AD in the file entry):
  -- unfortunately we have to play with different representations...
  IF fCkSumName = 'AD' AND segEntry.checksum_name = 'adler32' AND
     segEntry.checksum != to_number(fCkSum, 'XXXXXXXX') THEN
    rc := dlf.SECHECKSUM;
    msg := dlf.SECHECKSUM_MSG || ' : '
      || to_char(segEntry.checksum, 'XXXXXXXX') || ' vs ' || fCkSum;
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
    msg := 'File already has a copy on the tape';
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
      msg := 'A file already exists at fseq '|| segEntry.fseq ||' on VID '|| segEntry.vid;
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
    msg := dlf.ENSTOOMANYSEGS_MSG ||', VID='|| segEntry.vid;
    ROLLBACK;
    RETURN;
  END IF;
  -- Update file status
  UPDATE Cns_file_metadata SET status = 'm' WHERE fileid = fid;
  COMMIT;
  SELECT segEntry.blockId INTO varBlockId FROM Dual;  -- to_char() of a RAW type does not work. This does the trick...
  varParams := 'CopyNo='|| segEntry.copyNo ||' Fsec=1 SegmentSize='|| segEntry.segSize
    ||' Compression='|| trunc(segEntry.segSize*100/segEntry.comprSize) ||' TPVID='|| segEntry.vid
    ||' Fseq='|| segEntry.fseq ||' BlockId="' || varBlockId
    ||'" ChecksumType="'|| segEntry.checksum_name ||'" ChecksumValue=' || fCkSum;
  tmpDlfLog(dlf.LVL_SYSTEM, uuidGen(), 'New segment information', fid, varParams);
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- The file entry was not found, just give up
  rc := dlf.ENOENT;
  msg := dlf.ENOENT_MSG;
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
    IF varRC != 0 THEN
      varParams := 'ErrorCode='|| to_char(varRC) ||' ErrorMessage="'|| varParams || '"';
      tmpDlfLog(dlf.LVL_ERROR, varReqid, 'Error creating/updating segment', segs(i).fileId, varParams);
    END IF;
  END LOOP;
  -- Final logging
  UPDATE ResultsLogHelper SET reqid = varReqid;
  varParams := 'Function="setSegmentsForFiles" NbFiles='|| segs.COUNT
    ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP);
  tmpDlfLog(dlf.LVL_SYSTEM, varReqid, 'Bulk processing complete', 0, varParams);
  -- Return logs to the stager
  OPEN logs FOR
    SELECT timeinfo, lvl, reqid, msg, fileId, params FROM ResultsLogHelper;
END;
/

/**
 * This procedure replaces one (or more) segments for a given file when repacked.
 * This can be a one to one replacement (same copy number) or a move, if the
 * original copy had a different copy number than the new one. In this last case,
 * the new copy may actually replace 2 old copies as the new copy number
 * may correspond to a second copy that will be replaced. Note that this
 * last case is only accepted if the second replaced copy was invalid.
 * Return code:
 * 0  in case of success
 * ENOENT         if the file does not exist (e.g. it had been dropped).
 * EISDIR         if the file is a directory.
 * EEXIST         if the file already has a valid segment on the same tape.
 * ENSFILECHG     if the file has been modified meanwhile.
 * SEINTERNAL     if another segment exists on the given tape at the given fseq location.
 * ENSNOSEG       if the to-be-replaced segment was not found
 * ENSOVERWHENREP if the oldCopyNo refers to a valid segment and the new segEntry has a different copyNo.
 */
CREATE OR REPLACE PROCEDURE repackSegmentForFile(oldCopyNo IN INTEGER, segEntry IN castorns.Segment_Rec,
                                                 rc OUT INTEGER, msg OUT NOCOPY VARCHAR2) AS
  varReqid VARCHAR2(36);
  fid NUMBER;
  fmode NUMBER(6);
  fLastMTime NUMBER;
  fClassId NUMBER;
  fcNbCopies NUMBER;
  fCkSumName VARCHAR2(2);
  fCkSum VARCHAR2(32);
  varNb INTEGER;
  varStatus INTEGER;
  varBlockId VARCHAR2(8);
  varOwSeg castorns.Segment_Rec;
  varRepSeg castorns.Segment_Rec;
  varParams VARCHAR2(2048);
  -- Trap `ORA-00001: unique constraint violated` errors
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
BEGIN
  rc := 0;
  msg := '';
  varReqid := uuidGen();
  -- Get file data and lock the entry, exit if not found
  SELECT fileId, filemode, mtime, fileClass, csumType, csumValue
    INTO fid, fmode, fLastMTime, fClassId, fCkSumName, fCkSum
    FROM Cns_file_metadata
   WHERE fileId = segEntry.fileId FOR UPDATE;
  -- Is it a directory?
  IF bitand(fmode, 4*8*8*8*8) > 0 THEN  -- 040000 == S_IFDIR
    rc := dlf.EISDIR;
    msg := dlf.EISDIR_MSG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Has the file been changed meanwhile?
  IF fLastMTime > segEntry.lastModTime AND segEntry.lastModTime > 0 THEN
    rc := dlf.ENSFILECHG;
    msg := dlf.ENSFILECHG_MSG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Cross check file and segment checksums when adler32 (AD in the file entry):
  -- unfortunately we have to play with different representations...
  IF fCkSumName = 'AD' AND segEntry.checksum_name = 'adler32' AND
     segEntry.checksum != to_number(fCkSum, 'XXXXXXXX') THEN
    rc := dlf.SECHECKSUM;
    msg := dlf.SECHECKSUM_MSG || ' : '
      || to_char(segEntry.checksum, 'XXXXXXXX') || ' vs ' || fCkSum;
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
    msg := 'File already has a copy on the tape';
    ROLLBACK;
    RETURN;
  END IF;
  -- Repack specific: --
  -- Make sure a segment exists for the given copyNo. There can only be one segment
  -- as we don't support multi-segmented files any longer.
  BEGIN
    SELECT s_status INTO varStatus
      FROM Cns_seg_metadata
     WHERE s_fileid = fid AND copyNo = segEntry.copyNo;
    -- Check status of segment to be replaced in case it's a different copyNo
    IF segEntry.copyNo != oldCopyNo THEN
      IF varStatus = '-' THEN
        -- We are asked to overwrite a valid segment and replace another one.
        -- This is forbidden.
        rc := dlf.ENSOVERWHENREP;
        msg := dlf.ENSOVERWHENREP_MSG;
        ROLLBACK;
        RETURN;
      END IF;
      -- OK, the segment being overwritten is invalid
      DELETE FROM Cns_seg_metadata
       WHERE s_fileid = fid AND copyNo = segEntry.copyNo
      RETURNING copyNo, segSize, compression, vid, fseq, blockId, checksum_name, checksum
           INTO varOwSeg.copyNo, varOwSeg.segSize, varOwSeg.comprSize, varOwSeg.vid,
                varOwSeg.fseq, varOwSeg.blockId, varOwSeg.checksum_name, varOwSeg.checksum;
      -- Log overwritten segment metadata
      SELECT varOwSeg.blockId INTO varBlockId FROM Dual;
      varParams := 'CopyNo='|| varOwSeg.copyNo ||' Fsec=1 SegmentSize='|| varOwSeg.segSize
        ||' Compression='|| varOwSeg.comprSize ||' TPVID='|| varOwSeg.vid
        ||' Fseq='|| varOwSeg.fseq ||' BlockId="' || varBlockId
        ||'" ChecksumType="'|| varOwSeg.checksum_name ||'" ChecksumValue=' || varOwSeg.checksum;
      tmpDlfLog(dlf.LVL_SYSTEM, varReqid, 'Unlinking segment (overwritten)', fid, varParams);
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Previous segment not found, give up
    rc := dlf.ENSNOSEG;
    msg := dlf.ENSNOSEG_MSG;
    ROLLBACK;
    RETURN;
  END;
  
  -- We're done with the pre-checks. Remove and log old segment metadata
  DELETE FROM Cns_seg_metadata
   WHERE s_fileid = fid AND copyNo = oldCopyNo
  RETURNING copyNo, segSize, compression, vid, fseq, blockId, checksum_name, checksum
       INTO varRepSeg.copyNo, varRepSeg.segSize, varRepSeg.comprSize, varRepSeg.vid,
            varRepSeg.fseq, varRepSeg.blockId, varRepSeg.checksum_name, varRepSeg.checksum;
  SELECT varRepSeg.blockId INTO varBlockId FROM Dual;
  varParams := 'CopyNo='|| varRepSeg.copyNo ||' Fsec=1 SegmentSize='|| varRepSeg.segSize
    ||' Compression='|| varRepSeg.comprSize ||' TPVID='|| varRepSeg.vid
    ||' Fseq='|| varRepSeg.fseq ||' BlockId="' || varBlockId
    ||'" ChecksumType="'|| varRepSeg.checksum_name ||'" ChecksumValue=' || varRepSeg.checksum;
  tmpDlfLog(dlf.LVL_SYSTEM, varReqid, 'Unlinking segment (replaced)', fid, varParams);
  -- Insert new segment metadata and deal with possible collisions with the fseq position
  BEGIN
    INSERT INTO Cns_seg_metadata (s_fileId, copyNo, fsec, segSize, s_status,
      vid, fseq, blockId, compression, side, checksum_name, checksum)
    VALUES (fid, segEntry.copyNo, 1, segEntry.segSize, '-',
      segEntry.vid, segEntry.fseq, segEntry.blockId, trunc(segEntry.segSize*100/segEntry.comprSize),
      0, segEntry.checksum_name, segEntry.checksum);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    -- There must already be an existing segment at that fseq position for a different file.
    -- This is forbidden! Abort the entire operation.
    rc := dlf.SEINTERNAL;
    msg := 'A file already exists at fseq '|| segEntry.fseq ||' on VID '|| segEntry.vid;
    ROLLBACK;
    RETURN;
  END;
  -- All right, commit and log
  COMMIT;
  SELECT segEntry.blockId INTO varBlockId FROM Dual;  -- to_char() of a RAW type does not work. This does the trick...
  varParams := 'CopyNo='|| segEntry.copyNo ||' Fsec=1 SegmentSize='|| segEntry.segSize
    ||' Compression='|| trunc(segEntry.segSize*100/segEntry.comprSize) ||' TPVID='|| segEntry.vid
    ||' Fseq='|| segEntry.fseq ||' BlockId="' || varBlockId
    ||'" ChecksumType="'|| segEntry.checksum_name ||'" ChecksumValue=' || fCkSum;
  tmpDlfLog(dlf.LVL_SYSTEM, varReqid, 'New segment information', fid, varParams);
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- The file entry was not found, just give up
  rc := dlf.ENOENT;
  msg := dlf.ENOENT_MSG;
END;
/

/**
 * This procedure replaces segments for multiple files by calling repackSegmentForFile in a loop.
 * The output is a log table ready to be sent to DLF from a stager database.
 */
CREATE OR REPLACE PROCEDURE repackSegmentsForFiles(oldCopyNos IN castorns.cnumList, segs IN castorns.SegmentList,
                                                   logs OUT castorns.Log_Cur) AS
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
    repackSegmentForFile(oldCopyNos(i), segs(i), varRC, varParams);
    IF varRC != 0 THEN
      varParams := 'ErrorCode='|| to_char(varRC) ||' ErrorMessage="'|| varParams || '"';
      tmpDlfLog(dlf.LVL_ERROR, varReqid, 'Error replacing segment', segs(i).fileId, varParams);
    END IF;
  END LOOP;
  -- Final logging
  UPDATE ResultsLogHelper SET reqid = varReqid;
  varParams := 'Function="repackSegmentsForFiles" NbFiles='|| segs.COUNT
    ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP);
  tmpDlfLog(dlf.LVL_SYSTEM, varReqid, 'Bulk processing complete', 0, varParams);
  -- Return logs to the stager
  OPEN logs FOR
    SELECT timeinfo, lvl, reqid, msg, fileId, params FROM ResultsLogHelper;
END;
/

/* This function sets the checksum of a segment if there is none and commits.
 * It otherwise exits silently.
 */
CREATE OR REPLACE PROCEDURE setSegChecksumWhenNull(fid IN INTEGER,
                                                   copyNb IN INTEGER,
                                                   cksumType IN VARCHAR2,
                                                   cksumValue IN INTEGER) IS
BEGIN
  UPDATE Cns_seg_metadata
     SET checksum_name = cksumType, checksum = cksumValue
   WHERE s_fileid = fid AND copyno = copyNb AND fsec = 1
     AND checksum_name IS NULL AND checksum IS NULL;
  COMMIT;
END;
/


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM user_objects
             WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION')
               AND status = 'INVALID')
  LOOP
    IF a.object_type = 'PROCEDURE' THEN
      EXECUTE IMMEDIATE 'ALTER PROCEDURE '||a.object_name||' COMPILE';
    ELSIF a.object_type = 'TRIGGER' THEN
      EXECUTE IMMEDIATE 'ALTER TRIGGER '||a.object_name||' COMPILE';
    ELSE
      EXECUTE IMMEDIATE 'ALTER FUNCTION '||a.object_name||' COMPILE';
    END IF;
  END LOOP;
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_13_0';
COMMIT;
