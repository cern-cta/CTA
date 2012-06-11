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
  TYPE cnumList IS TABLE OF INTEGER INDEX BY BINARY_INTEGER;
  TYPE Segment_Rec IS RECORD (
    fileId NUMBER,
    lastModTime NUMBER,
    copyNo INTEGER,
    segSize INTEGER,
    comprSize INTEGER,
    vid VARCHAR2(6),
    fseq INTEGER,
    blockId RAW(4),
    checksum_name VARCHAR2(16),
    checksum INTEGER
  );
  TYPE Segment_Cur IS REF CURSOR RETURN Segment_Rec;
END castorns;
/

/* Useful types */
CREATE OR REPLACE TYPE strList IS TABLE OF VARCHAR2(2048);
/
CREATE OR REPLACE TYPE numList IS TABLE OF INTEGER;
/

/**
 * Package containing the definition of some relevant (s)errno values and messages.
 * The actual logging is performed by the stagers.
 */
CREATE OR REPLACE PACKAGE serrno AS
  /* (s)errno values */
  ENOENT          CONSTANT PLS_INTEGER := 2;    /* No such file or directory */
  EACCES          CONSTANT PLS_INTEGER := 13;   /* Permission denied */
  EEXIST          CONSTANT PLS_INTEGER := 17;   /* File exists */
  EISDIR          CONSTANT PLS_INTEGER := 21;   /* Is a directory */
  
  SEINTERNAL      CONSTANT PLS_INTEGER := 1015; /* Internal error */
  SECHECKSUM      CONSTANT PLS_INTEGER := 1037; /* Bad checksum */
  ENSFILECHG      CONSTANT PLS_INTEGER := 1402; /* File has been overwritten, request ignored */
  ENSNOSEG        CONSTANT PLS_INTEGER := 1403; /* Segment had been deleted */
  ENSTOOMANYSEGS  CONSTANT PLS_INTEGER := 1406; /* Too many copies on tape */
  ENSOVERWHENREP  CONSTANT PLS_INTEGER := 1407; /* Cannot overwrite valid segment when replacing */
  ERTWRONGSIZE    CONSTANT PLS_INTEGER := 1613; /* (Recalled) file size incorrect */
  
  /* messages */
  ENOENT_MSG          CONSTANT VARCHAR2(2048) := 'No such file or directory';
  EACCES_MSG          CONSTANT VARCHAR2(2048) := 'Permission denied';
  EEXIST_MSG          CONSTANT VARCHAR2(2048) := 'File exists';
  EISDIR_MSG          CONSTANT VARCHAR2(2048) := 'Is a directory';
  SEINTERNAL_MSG      CONSTANT VARCHAR2(2048) := 'Internal error';
  SECHECKSUM_MSG      CONSTANT VARCHAR2(2048) := 'Checksum mismatch between file and segment';
  ENSFILECHG_MSG      CONSTANT VARCHAR2(2048) := 'File has been overwritten, request ignored';
  ENSNOSEG_MSG        CONSTANT VARCHAR2(2048) := 'Segment had been deleted';
  ENSTOOMANYSEGS_MSG  CONSTANT VARCHAR2(2048) := 'Too many copies on tape';
  ENSOVERWHENREP_MSG  CONSTANT VARCHAR2(2048) := 'Cannot overwrite valid segment when replacing';
  ERTWRONGSIZE_MSG    CONSTANT VARCHAR2(2048) := 'Incorrect file size';
END serrno;
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
  RETURN TRUNC(EXTRACT(SECOND FROM (endTime - startTime)), 6);
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
CREATE OR REPLACE PROCEDURE tmpDlfLog(inReqId IN VARCHAR2, inEC IN INTEGER, inMsg IN VARCHAR2,
                                      inFileId IN NUMBER, inParams IN VARCHAR2) AS
BEGIN
  INSERT INTO ResultsLogHelper (reqId, timeinfo, ec, msg, fileId, params)
    VALUES (inReqId, getTime(), inEC, inMsg, inFileId, inParams);
END;
/

/* A function to extract the full path of a file in one go */
CREATE OR REPLACE FUNCTION getPathForFileid(varFid IN NUMBER) RETURN VARCHAR2 IS
  CURSOR c IS
    SELECT /*+ NO_CONNECT_BY_COST_BASED */ name
      FROM cns_file_metadata
    START WITH fileid = varFid
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
 * ENOENT         if the file does not exist (e.g. it has been dropped meanwhile).
 * EISDIR         if the file is a directory.
 * EEXIST         if the file already has a valid segment on the same tape.
 * ENSFILECHG     if the file has been modified meanwhile.
 * SEINTERNAL     if another segment exists on the given tape at the given fseq location.
 * ENSTOOMANYSEGS if this copy exceeds the number of copies allowed by the file's fileclass.
 *                This also applies if inSegEntry refers to a file which fileclass allows 0 copies.
 */
CREATE OR REPLACE PROCEDURE setSegmentForFile(inSegEntry IN castorns.Segment_Rec,
                                              inReqId IN VARCHAR2,
                                              rc OUT INTEGER, msg OUT NOCOPY VARCHAR2) AS
  varFid NUMBER;
  varFmode NUMBER(6);
  varFLastMTime NUMBER;
  varFSize NUMBER;
  varFClassId NUMBER;
  varFCNbCopies NUMBER;
  varFCksumName VARCHAR2(2);
  varFCksum VARCHAR2(32);
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
  SELECT fileId, filemode, mtime, fileClass, fileSize, csumType, csumValue
    INTO varFid, varFmode, varFLastMTime, varFClassId, varFSize, varFCksumName, varFCksum
    FROM Cns_file_metadata
   WHERE fileId = inSegEntry.fileId FOR UPDATE;
  -- Is it a directory?
  IF bitand(varFmode, 4*8*8*8*8) > 0 THEN  -- 040000 == S_IFDIR
    rc := serrno.EISDIR;
    msg := serrno.EISDIR_MSG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Has the file been changed meanwhile?
  IF varFLastMTime > inSegEntry.lastModTime AND inSegEntry.lastModTime > 0 THEN
    rc := serrno.ENSFILECHG;
    msg := serrno.ENSFILECHG_MSG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Cross check file and segment checksums when adler32 (AD in the file entry):
  -- unfortunately we have to play with different representations...
  IF varFCksumName = 'AD' AND inSegEntry.checksum_name = 'adler32' AND
     inSegEntry.checksum != to_number(varFCksum, 'XXXXXXXX') THEN
    rc := serrno.SECHECKSUM;
    msg := serrno.SECHECKSUM_MSG ||' : '
      || varFCksum ||' vs '|| to_char(inSegEntry.checksum, 'XXXXXXXX');
    ROLLBACK;
    RETURN;
  END IF;
  -- Cross check segment (transfered) size and file size
  IF varFSize != inSegEntry.segSize THEN
    rc := serrno.ERTWRONGSIZE;
    msg := serrno.ERTWRONGSIZE ||' : expected '
      || to_char(varFSize) ||', got '|| to_char(inSegEntry.segSize);
    ROLLBACK;
    RETURN;
  END IF;
  -- Prevent to write a second copy of this file
  -- on a tape that already holds a valid copy
  SELECT count(*) INTO varNb
    FROM Cns_seg_metadata
   WHERE s_fileid = varFid AND s_status = '-' AND vid = inSegEntry.vid;
  IF varNb > 0 THEN
    rc := serrno.EEXIST;
    msg := 'File already has a copy on the tape';
    ROLLBACK;
    RETURN;
  END IF;
  
  -- We're done with the pre-checks, try and insert the segment metadata
  -- and deal with the possible exceptions: PK violation, unique (vid,fseq) violation
  BEGIN
    INSERT INTO Cns_seg_metadata (s_fileId, copyNo, fsec, segSize, s_status,
      vid, fseq, blockId, compression, side, checksum_name, checksum)
    VALUES (varFid, inSegEntry.copyNo, 1, inSegEntry.segSize, '-',
      inSegEntry.vid, inSegEntry.fseq, inSegEntry.blockId, trunc(inSegEntry.segSize*100/inSegEntry.comprSize),
      0, inSegEntry.checksum_name, inSegEntry.checksum);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    -- We assume the PK was violated, i.e. a previous segment already exists and we need to update it.
    -- XXX This won't happen anymore once we will drop previous versions of the segments at
    -- XXX file creation/truncation time!
    UPDATE Cns_seg_metadata
       SET fsec = 1, segSize = inSegEntry.segSize, s_status = '-',
           vid = inSegEntry.vid, fseq = inSegEntry.fseq, blockId = inSegEntry.blockId,
           compression = trunc(inSegEntry.segSize*100/inSegEntry.comprSize),
           side = 0, checksum_name = inSegEntry.checksum_name,
           checksum = inSegEntry.checksum
     WHERE s_fileId = varFid
       AND copyNo = inSegEntry.copyNo;
    IF SQL%ROWCOUNT = 0 THEN
      -- The update failed, thus the CONSTRAINT_VIOLATED was due to an existing segment at that fseq position.
      -- This is forbidden!
      rc := serrno.SEINTERNAL;
      msg := 'A file already exists at fseq '|| inSegEntry.fseq ||' on VID '|| inSegEntry.vid;
      ROLLBACK;
      RETURN;
    END IF;
  END;
  -- Finally check for too many segments for this file
  SELECT nbCopies INTO varFCNbCopies
    FROM Cns_class_metadata
   WHERE classid = varFClassId;
  SELECT count(*) INTO varNb
    FROM Cns_seg_metadata
   WHERE s_fileid = varFid AND s_status = '-';
  IF varNb > varFCNbCopies THEN
    rc := serrno.ENSTOOMANYSEGS;
    msg := serrno.ENSTOOMANYSEGS_MSG ||', VID='|| inSegEntry.vid;
    ROLLBACK;
    RETURN;
  END IF;
  -- Update file status
  UPDATE Cns_file_metadata SET status = 'm' WHERE fileid = varFid;
  COMMIT;
  SELECT inSegEntry.blockId INTO varBlockId FROM Dual;  -- to_char() of a RAW type does not work. This does the trick...
  varParams := 'CopyNo='|| inSegEntry.copyNo ||' Fsec=1 SegmentSize='|| inSegEntry.segSize
    ||' Compression='|| trunc(inSegEntry.segSize*100/inSegEntry.comprSize) ||' TPVID='|| inSegEntry.vid
    ||' Fseq='|| inSegEntry.fseq ||' BlockId="' || varBlockId
    ||'" ChecksumType="'|| inSegEntry.checksum_name ||'" ChecksumValue=' || varFCksum;
  tmpDlfLog(inReqId, 0, 'New segment information', varFid, varParams);
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- The file entry was not found, just give up
  rc := serrno.ENOENT;
  msg := serrno.ENOENT_MSG;
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
 * ENOENT         if the file does not exist (e.g. it has been dropped meanwhile).
 * EISDIR         if the file is a directory.
 * EEXIST         if the file already has a valid segment on the same tape.
 * ENSFILECHG     if the file has been modified meanwhile.
 * SEINTERNAL     if another segment exists on the given tape at the given fseq location.
 * ENSNOSEG       if the to-be-replaced segment was not found.
 * ENSOVERWHENREP if inOldCopyNo refers to a valid segment and the new inSegEntry has a different copyNo.
 */
CREATE OR REPLACE PROCEDURE replaceSegmentForFile(inOldCopyNo IN INTEGER, inSegEntry IN castorns.Segment_Rec,
                                                  inReqId IN VARCHAR2,
                                                  rc OUT INTEGER, msg OUT NOCOPY VARCHAR2) AS
  varFid NUMBER;
  varFmode NUMBER(6);
  varFLastMTime NUMBER;
  varFSize NUMBER;
  varFClassId NUMBER;
  varFCNbCopies NUMBER;
  varFCksumName VARCHAR2(2);
  varFCksum VARCHAR2(32);
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
  -- Get file data and lock the entry, exit if not found
  SELECT fileId, filemode, mtime, fileClass, csumType, csumValue
    INTO varFid, varFmode, varFLastMTime, varFClassId, varFCksumName, varFCksum
    FROM Cns_file_metadata
   WHERE fileId = inSegEntry.fileId FOR UPDATE;
  -- Is it a directory?
  IF bitand(varFmode, 4*8*8*8*8) > 0 THEN  -- 040000 == S_IFDIR
    rc := serrno.EISDIR;
    msg := serrno.EISDIR_MSG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Has the file been changed meanwhile?
  IF varFLastMTime > inSegEntry.lastModTime AND inSegEntry.lastModTime > 0 THEN
    rc := serrno.ENSFILECHG;
    msg := serrno.ENSFILECHG_MSG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Cross check file and segment checksums when adler32 (AD in the file entry):
  -- unfortunately we have to play with different representations...
  IF varFCksumName = 'AD' AND inSegEntry.checksum_name = 'adler32' AND
     inSegEntry.checksum != to_number(varFCksum, 'XXXXXXXX') THEN
    rc := serrno.SECHECKSUM;
    msg := serrno.SECHECKSUM_MSG ||' : '
      || varFCksum ||' vs '|| to_char(inSegEntry.checksum, 'XXXXXXXX');
    ROLLBACK;
    RETURN;
  END IF;
  -- Cross check segment (transfered) size and file size
  IF varFSize != inSegEntry.segSize THEN
    rc := serrno.ERTWRONGSIZE;
    msg := serrno.ERTWRONGSIZE ||' : expected '
      || to_char(varFSize) ||', got '|| to_char(inSegEntry.segSize);
    ROLLBACK;
    RETURN;
  END IF;
  -- Prevent to write a second copy of this file
  -- on a tape that already holds a valid copy
  SELECT count(*) INTO varNb
    FROM Cns_seg_metadata
   WHERE s_fileid = varFid AND s_status = '-' AND vid = inSegEntry.vid;
  IF varNb > 0 THEN
    rc := serrno.EEXIST;
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
     WHERE s_fileid = varFid AND copyNo = inSegEntry.copyNo;
    -- Check status of segment to be replaced in case it's a different copyNo
    IF inSegEntry.copyNo != inOldCopyNo THEN
      IF varStatus = '-' THEN
        -- We are asked to overwrite a valid segment and replace another one.
        -- This is forbidden.
        rc := serrno.ENSOVERWHENREP;
        msg := serrno.ENSOVERWHENREP_MSG;
        ROLLBACK;
        RETURN;
      END IF;
      -- OK, the segment being overwritten is invalid
      DELETE FROM Cns_seg_metadata
       WHERE s_fileid = varFid AND copyNo = inSegEntry.copyNo
      RETURNING copyNo, segSize, compression, vid, fseq, blockId, checksum_name, checksum
           INTO varOwSeg.copyNo, varOwSeg.segSize, varOwSeg.comprSize, varOwSeg.vid,
                varOwSeg.fseq, varOwSeg.blockId, varOwSeg.checksum_name, varOwSeg.checksum;
      -- Log overwritten segment metadata
      SELECT varOwSeg.blockId INTO varBlockId FROM Dual;
      varParams := 'CopyNo='|| varOwSeg.copyNo ||' Fsec=1 SegmentSize='|| varOwSeg.segSize
        ||' Compression='|| varOwSeg.comprSize ||' TPVID='|| varOwSeg.vid
        ||' Fseq='|| varOwSeg.fseq ||' BlockId="' || varBlockId
        ||'" ChecksumType="'|| varOwSeg.checksum_name ||'" ChecksumValue=' || varOwSeg.checksum;
      tmpDlfLog(inReqId, 0, 'Unlinking segment (overwritten)', varFid, varParams);
    END IF;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Previous segment not found, give up
    rc := serrno.ENSNOSEG;
    msg := serrno.ENSNOSEG_MSG;
    ROLLBACK;
    RETURN;
  END;
  
  -- We're done with the pre-checks. Remove and log old segment metadata
  DELETE FROM Cns_seg_metadata
   WHERE s_fileid = varFid AND copyNo = inOldCopyNo
  RETURNING copyNo, segSize, compression, vid, fseq, blockId, checksum_name, checksum
       INTO varRepSeg.copyNo, varRepSeg.segSize, varRepSeg.comprSize, varRepSeg.vid,
            varRepSeg.fseq, varRepSeg.blockId, varRepSeg.checksum_name, varRepSeg.checksum;
  SELECT varRepSeg.blockId INTO varBlockId FROM Dual;
  varParams := 'CopyNo='|| varRepSeg.copyNo ||' Fsec=1 SegmentSize='|| varRepSeg.segSize
    ||' Compression='|| varRepSeg.comprSize ||' TPVID='|| varRepSeg.vid
    ||' Fseq='|| varRepSeg.fseq ||' BlockId="' || varBlockId
    ||'" ChecksumType="'|| varRepSeg.checksum_name ||'" ChecksumValue=' || varRepSeg.checksum;
  tmpDlfLog(inReqId, 0, 'Unlinking segment (replaced)', varFid, varParams);
  -- Insert new segment metadata and deal with possible collisions with the fseq position
  BEGIN
    INSERT INTO Cns_seg_metadata (s_fileId, copyNo, fsec, segSize, s_status,
      vid, fseq, blockId, compression, side, checksum_name, checksum)
    VALUES (varFid, inSegEntry.copyNo, 1, inSegEntry.segSize, '-',
      inSegEntry.vid, inSegEntry.fseq, inSegEntry.blockId, trunc(inSegEntry.segSize*100/inSegEntry.comprSize),
      0, inSegEntry.checksum_name, inSegEntry.checksum);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    -- There must already be an existing segment at that fseq position for a different file.
    -- This is forbidden! Abort the entire operation.
    rc := serrno.SEINTERNAL;
    msg := 'A file already exists at fseq '|| inSegEntry.fseq ||' on VID '|| inSegEntry.vid;
    ROLLBACK;
    RETURN;
  END;
  -- All right, commit and log
  COMMIT;
  SELECT inSegEntry.blockId INTO varBlockId FROM Dual;  -- to_char() of a RAW type does not work. This does the trick...
  varParams := 'CopyNo='|| inSegEntry.copyNo ||' Fsec=1 SegmentSize='|| inSegEntry.segSize
    ||' Compression='|| trunc(inSegEntry.segSize*100/inSegEntry.comprSize) ||' TPVID='|| inSegEntry.vid
    ||' Fseq='|| inSegEntry.fseq ||' BlockId="' || varBlockId
    ||'" ChecksumType="'|| inSegEntry.checksum_name ||'" ChecksumValue=' || varFCksum;
  tmpDlfLog(inReqId, 0, 'New segment information', varFid, varParams);
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- The file entry was not found, just give up
  rc := serrno.ENOENT;
  msg := serrno.ENOENT_MSG;
END;
/

/**
 * This procedure sets or replaces segments for multiple files by calling either setSegmentForFile
 * or replaceSegmentForFile in a loop, the choice depending on the inOldCopyNos values:
 * when 0, a normal migration is assumed and setSegmentForFile is called.
 * The output is a set of arrays of logs to be sent to DLF from a stager database.
 */
CREATE OR REPLACE PROCEDURE setOrReplaceSegmentsForFiles(inReqId IN VARCHAR2) AS
  varRC INTEGER;
  varParams VARCHAR2(1000);
  varStartTime TIMESTAMP;
  varSeg castorns.Segment_Rec;
  varCount INTEGER;
BEGIN
  varStartTime := SYSTIMESTAMP;
  varCount := 0;
  -- Loop over all files. Each call commits or rollbacks each file.
  FOR s IN (SELECT fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
                   vid, fseq, blockId, checksumType, checksum
              FROM SetSegmentsForFilesHelper
             WHERE reqId = inReqId) LOOP
    varSeg.fileId := s.fileId;
    varSeg.lastModTime := s.lastModTime;
    varSeg.copyNo := s.copyNo;
    varSeg.segSize := s.transfSize;
    varSeg.comprSize := s.comprSize;
    varSeg.vid := s.vid;
    varSeg.fseq := s.fseq;
    varSeg.blockId := s.blockId;
    varSeg.checksum_name := s.checksumType;
    varSeg.checksum := s.checksum;
    IF s.oldCopyNo = 0 THEN
      setSegmentForFile(varSeg, inReqId, varRC, varParams);
    ELSE
      replaceSegmentForFile(s.oldCopyNo, varSeg, inReqId, varRC, varParams);
    END IF;
    IF varRC != 0 THEN
      varParams := 'ErrorCode='|| to_char(varRC) ||' ErrorMessage="'|| varParams ||'"';
      tmpDlfLog(inReqId, varRC, 'Error creating/replacing segment', s.fileId, varParams);
      COMMIT;
    END IF;
    varCount := varCount + 1;
  END LOOP;
  IF varCount > 0 THEN
    -- Final logging
    varParams := 'Function="setOrRepackSegmentsForFiles" NbFiles='|| varCount
      ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
      ||' AvgProcessingTime='|| getSecs(varStartTime, SYSTIMESTAMP)/varCount;
    tmpDlfLog(inReqId, 0, 'Bulk processing complete', 0, varParams);
  END IF;
  -- Clean input data
  DELETE FROM SetSegmentsForFilesHelper
   WHERE reqId = inReqId;
  COMMIT;
  -- Return results and logs to the stager. Unfortunately we can't OPEN CURSOR FOR ...
  -- because we would get ORA-24338: 'statement handle not executed' at run time.
  -- Moreover, temporary tables are not supported with distributed transactions,
  -- so the stager will remotely open the ResultsLogHelper table, and we clean
  -- the tables by hand using the reqId key.
END;
/

/* This function sets the checksum of a segment if there is none and commits.
 * It otherwise exits silently.
 */
CREATE OR REPLACE PROCEDURE setSegChecksumWhenNull(fid IN INTEGER,
                                                   copyNb IN INTEGER,
                                                   cksumType IN VARCHAR2,
                                                   cksumValue IN INTEGER) AS
BEGIN
  UPDATE Cns_seg_metadata
     SET checksum_name = cksumType, checksum = cksumValue
   WHERE s_fileid = fid AND copyno = copyNb AND fsec = 1
     AND checksum_name IS NULL AND checksum IS NULL;
  COMMIT;
END;
/
