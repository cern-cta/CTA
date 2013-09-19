/******************************************************************************
 *                 cns_2.1.13-9_to_2.1.14-2.sql
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
 * This script upgrades a CASTOR v2.1.13-9 CNS database to v2.1.14-2
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
   WHERE schemaVersion = '2_1_14_0'
     AND release = '2_1_14_2'
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
     AND release LIKE '2_1_13_9%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the CNS before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_2', 'NON TRANSPARENT');
COMMIT;

/* Schema changes go here */
/**************************/

/* The following changes may have been performed by the 2.1.14-0pre upgrade, check them */
DECLARE
  colName VARCHAR2(100);
BEGIN
  SELECT column_name INTO colName FROM all_tab_cols WHERE table_name = 'CNS_FILE_METADATA' AND column_name = 'STAGERTIME';
EXCEPTION WHEN NO_DATA_FOUND THEN
  EXECUTE IMMEDIATE 'ALTER TABLE Cns_seg_metadata ADD (
     gid NUMBER(6), creationTime NUMBER, lastModificationTime NUMBER);';
  EXECUTE IMMEDIATE 'ALTER TABLE Cns_file_metadata ADD (stagerTime NUMBER);';
END;
/

-- Table to store configuration items at the DB level
CREATE TABLE CastorConfig
  (class VARCHAR2(50) CONSTRAINT NN_CastorConfig_class NOT NULL,
   key VARCHAR2(50) CONSTRAINT NN_CastorConfig_key NOT NULL,
   value VARCHAR2(100) CONSTRAINT NN_CastorConfig_value NOT NULL,
   description VARCHAR2(1000));
ALTER TABLE CastorConfig ADD CONSTRAINT UN_CastorConfig_class_key UNIQUE (class, key);

-- Provide a default configuration
INSERT INTO CastorConfig (class, key, value, description)
  VALUES ('stager', 'openmode', 'C', 'Mode for stager open/close operations: C for Compatibility (pre 2.1.14), N for New (from 2.1.14 onwards)');

-- bug #101714: RFE: VMGR DB should provide tape statuses independently of table definitions
-- For Vmgr_tape_status_view
UNDEF vmgrSchema
ACCEPT vmgrSchema CHAR PROMPT 'Enter the name of the VMGR schema: ';
CREATE OR REPLACE VIEW Vmgr_tape_status_view AS SELECT * FROM &vmgrSchema..VMGR_TAPE_STATUS_VIEW;

-- Table for NS statistics
CREATE TABLE UsageStats (
  gid NUMBER(6) DEFAULT 0 CONSTRAINT NN_UsageStats_gid NOT NULL,
  timestamp NUMBER  DEFAULT 0 CONSTRAINT NN_UsageStats_ts NOT NULL,
  maxFileId INTEGER, fileCount INTEGER, fileSize INTEGER,
  segCount INTEGER, segSize INTEGER, segCompressedSize INTEGER,
  seg2Count INTEGER, seg2Size INTEGER, seg2CompressedSize INTEGER);

ALTER TABLE UsageStats ADD CONSTRAINT PK_UsageStats_gid_ts PRIMARY KEY (gid, timestamp);


/* Update and revalidation of PL-SQL code */
/******************************************/

/* Package holding type declarations for the NameServer PL/SQL API */
CREATE OR REPLACE PACKAGE castorns AS
  TYPE cnumList IS TABLE OF INTEGER INDEX BY BINARY_INTEGER;
  TYPE Segment_Rec IS RECORD (
    fileId NUMBER,
    lastOpenTime NUMBER,
    copyNo INTEGER,
    segSize INTEGER,
    comprSize INTEGER,
    vid VARCHAR2(6),
    fseq INTEGER,
    blockId RAW(4),
    checksum_name VARCHAR2(16),
    checksum INTEGER,
    gid INTEGER,
    creationTime NUMBER,
    lastModificationTime NUMBER
  );
  TYPE Segment_Cur IS REF CURSOR RETURN Segment_Rec;
  TYPE Stats_Rec IS RECORD (
    gid INTEGER,
    maxFileId INTEGER,
    fileCount INTEGER,
    fileSize INTEGER,
    segCount INTEGER,
    segSize INTEGER,
    segCompressedSize INTEGER,
    seg2Count INTEGER,
    seg2Size INTEGER,
    seg2CompressedSize INTEGER,
    fileIdDelta INTEGER,
    fileCountDelta INTEGER,
    fileSizeDelta INTEGER,
    segCountDelta INTEGER,
    segSizeDelta INTEGER,
    segCompressedSizeDelta INTEGER,
    seg2CountDelta INTEGER,
    seg2SizeDelta INTEGER,
    seg2CompressedSizeDelta INTEGER
  );
  TYPE Stats IS TABLE OF Stats_Rec;
END castorns;
/

/* Useful types */
CREATE OR REPLACE TYPE strList IS TABLE OF VARCHAR2(2048);
/
CREATE OR REPLACE TYPE numList IS TABLE OF INTEGER;
/

/**
 * Package containing the definition of some relevant (s)errno values and messages.
 */
CREATE OR REPLACE PACKAGE serrno AS
  /* (s)errno values */
  ENOENT          CONSTANT PLS_INTEGER := 2;    /* No such file or directory */
  EACCES          CONSTANT PLS_INTEGER := 13;   /* Permission denied */
  EBUSY           CONSTANT PLS_INTEGER := 16;   /* Device or resource busy */
  EEXIST          CONSTANT PLS_INTEGER := 17;   /* File exists */
  EISDIR          CONSTANT PLS_INTEGER := 21;   /* Is a directory */
  EINVAL          CONSTANT PLS_INTEGER := 22;   /* Invalid argument */
  
  SEINTERNAL      CONSTANT PLS_INTEGER := 1015; /* Internal error */
  SECHECKSUM      CONSTANT PLS_INTEGER := 1037; /* Bad checksum */
  ENSFILECHG      CONSTANT PLS_INTEGER := 1402; /* File has been overwritten, request ignored */
  ENSNOSEG        CONSTANT PLS_INTEGER := 1403; /* Segment had been deleted */
  ENSTOOMANYSEGS  CONSTANT PLS_INTEGER := 1406; /* Too many copies on tape */
  ENSOVERWHENREP  CONSTANT PLS_INTEGER := 1407; /* Cannot overwrite valid segment when replacing */
  ERTWRONGSIZE    CONSTANT PLS_INTEGER := 1613; /* (Recalled) file size incorrect */
  ESTNOSEGFOUND   CONSTANT PLS_INTEGER := 1723; /* File has no copy on tape or no diskcopies are accessible */
  
  /* messages */
  ENOENT_MSG          CONSTANT VARCHAR2(2048) := 'No such file or directory';
  EACCES_MSG          CONSTANT VARCHAR2(2048) := 'Permission denied';
  EBUSY_MSG           CONSTANT VARCHAR2(2048) := 'Device or resource busy';
  EEXIST_MSG          CONSTANT VARCHAR2(2048) := 'File exists';
  EISDIR_MSG          CONSTANT VARCHAR2(2048) := 'Is a directory';
  EINVAL_MSG          CONSTANT VARCHAR2(2048) := 'Invalid argument';
  
  SEINTERNAL_MSG      CONSTANT VARCHAR2(2048) := 'Internal error';
  SECHECKSUM_MSG      CONSTANT VARCHAR2(2048) := 'Checksum mismatch between segment and file';
  ENSFILECHG_MSG      CONSTANT VARCHAR2(2048) := 'File has been overwritten, request ignored';
  ENSNOSEG_MSG        CONSTANT VARCHAR2(2048) := 'Segment had been deleted';
  ENSTOOMANYSEGS_MSG  CONSTANT VARCHAR2(2048) := 'Too many copies on tape';
  ENSOVERWHENREP_MSG  CONSTANT VARCHAR2(2048) := 'Cannot overwrite valid segment when replacing';
  ERTWRONGSIZE_MSG    CONSTANT VARCHAR2(2048) := 'Incorrect file size';
  ESTNOSEGFOUND_MSG   CONSTANT VARCHAR2(2048) := 'File has no copy on tape or no diskcopies are accessible';
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

/* Function to extract a configuration option from the castor config table.
 */
CREATE OR REPLACE FUNCTION getConfigOption(className VARCHAR2, optionName VARCHAR2, defaultValue VARCHAR2)
RETURN VARCHAR2 IS
  returnValue VARCHAR2(2048) := defaultValue;
BEGIN
  SELECT value INTO returnValue
    FROM CastorConfig
   WHERE class = className
     AND key = optionName
     AND value != 'undefined';
  RETURN returnValue;
EXCEPTION WHEN NO_DATA_FOUND THEN
  RETURN returnValue;
END;
/

/* A small procedure to add a line to the temporary SetSegsForFilesResultsHelper table.
 * The table is ultimately NOT a temporary table because Oracle does not support temporary tables
 * with distributed transactions, but it is used as such: see castor/db/oracleTapeGateway.sql.
 */
CREATE OR REPLACE PROCEDURE addSegResult(inIsOnlyLog IN INTEGER, inReqId IN VARCHAR2, inErrorCode IN INTEGER,
                                         inMsg IN VARCHAR2, inFileId IN NUMBER, inParams IN VARCHAR2) AS
BEGIN
  INSERT INTO SetSegsForFilesResultsHelper (isOnlyLog, reqId, timeinfo, errorCode, msg, fileId, params)
    VALUES (inIsOnlyLog, inReqId, getTime(), inErrorCode, inMsg, inFileId, inParams);
END;
/

/* A function to extract the full path of a file in one go */
CREATE OR REPLACE FUNCTION getPathForFileid(inFid IN NUMBER) RETURN VARCHAR2 IS
  CURSOR c IS
    SELECT /*+ NO_CONNECT_BY_COST_BASED */ name
      FROM Cns_file_metadata
    START WITH fileid = inFid
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
 * This function drops a potentially existing segment with given copyNo, and then
 * checks for another copy of the same file on the same tape
 */
CREATE OR REPLACE FUNCTION checkAndDropSameCopynbSeg(inReqId IN VARCHAR2,
                                                     inFid IN INTEGER,
                                                     inVid IN VARCHAR2,
                                                     inCopyNo IN INTEGER) RETURN INTEGER IS
  varNb INTEGER;
  varRepSeg castorns.Segment_Rec;
  varBlockId VARCHAR2(8);
  varParams VARCHAR2(2048);
BEGIN
  -- First try and delete a previous segment with the same copyNo: this may exist
  -- even on fresh migrations when the stager fails to commit a previous migration attempt
  DELETE FROM Cns_seg_metadata
   WHERE s_fileid = inFid AND copyNo = inCopyNo
  RETURNING copyNo, segSize, compression, vid, fseq, blockId, checksum_name, checksum,
            gid, creationTime, lastModificationTime
       INTO varRepSeg.copyNo, varRepSeg.segSize, varRepSeg.comprSize, varRepSeg.vid,
            varRepSeg.fseq, varRepSeg.blockId, varRepSeg.checksum_name, varRepSeg.checksum,
            varRepSeg.gid, varRepSeg.creationTime, varRepSeg.lastModificationTime;
  IF varRepSeg.vid IS NOT NULL THEN
    -- we have some data, log
    SELECT varRepSeg.blockId INTO varBlockId FROM Dual;  -- to_char() of a RAW type does not work. This does the trick...
    varParams := 'copyNb=' || varRepSeg.copyNo ||' gid=' || varRepSeg.gid ||' SegmentSize=' || varRepSeg.segSize
      ||' Compression=' || varRepSeg.comprSize ||' TPVID=' || varRepSeg.vid
      ||' fseq=' || varRepSeg.fseq ||' BlockId="' || varBlockId
      ||'" ChecksumType="' || varRepSeg.checksum_name ||'" ChecksumValue=' || varRepSeg.checksum
      ||' creationTime=' || trunc(varRepSeg.creationTime, 6)
      ||' lastModificationTime=' || trunc(varRepSeg.lastModificationTime, 6);
    addSegResult(1, inReqId, 0, 'Unlinking segment (replaced)', inFid, varParams);
  END IF;
  -- Now that no previous segment exists with the same copyNo, prevent 2nd copy in the same tape
  SELECT count(*) INTO varNb
    FROM Cns_seg_metadata
   WHERE s_fileid = inFid AND s_status = '-' AND vid = inVid;
  IF varNb > 0 THEN
    RETURN serrno.EEXIST;
  ELSE
    RETURN 0;
  END IF;
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
  varFGid INTEGER;
  varFClassId NUMBER;
  varFCNbCopies NUMBER;
  varFCksumName VARCHAR2(2);
  varFCksum VARCHAR2(32);
  varNb INTEGER;
  varBlockId VARCHAR2(8);
  varParams VARCHAR2(2048);
  varOpenMode CHAR(1);
  varLastOpenTimeFromClient NUMBER;
  varSegCreationTime NUMBER;
  -- Trap `ORA-00001: unique constraint violated` errors
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
BEGIN
  rc := 0;
  msg := '';
  -- Retrieve open mode flag. To be dropped after v2.1.14 is in production.
  varOpenMode := getConfigOption('stager', 'openmode', NULL);
  -- Get file data and lock the entry, exit if not found
  IF varOpenMode = 'C' THEN
    SELECT fileId, filemode, mtime, fileClass, fileSize, csumType, csumValue, gid
      INTO varFid, varFmode, varFLastMTime, varFClassId, varFSize, varFCksumName, varFCksum, varFGid
      FROM Cns_file_metadata
     WHERE fileId = inSegEntry.fileId FOR UPDATE;
    -- in compatibility mode we only have second precision, thus we have to ceil
    -- the given lastOpenTime for a safe comparison with mtime
    varLastOpenTimeFromClient := CEIL(inSegEntry.lastOpenTime);
  ELSIF varOpenMode = 'N' THEN
    -- We truncate to 5 decimal digits, which is the precision of the lastOpenTime we get from the stager.
    -- Note this is just fitting the mantissa precision of a double, and it is due to the fact
    -- that those numbers go through OCI as double.
    SELECT fileId, filemode, TRUNC(stagertime, 5), fileClass, fileSize, csumType, csumValue, gid
      INTO varFid, varFmode, varFLastMTime, varFClassId, varFSize, varFCksumName, varFCksum, varFGid
      FROM Cns_file_metadata
     WHERE fileId = inSegEntry.fileId FOR UPDATE;
    varLastOpenTimeFromClient := inSegEntry.lastOpenTime;
  ELSE
    -- sanity check, should never happen
    rc := serrno.EINVAL;
    msg := 'Incorrect value found for openmode in CastorConfig: found '
            || varOpenMode ||', expected either C or N';
    ROLLBACK;
    RETURN;
  END IF;
  -- Is it a directory?
  IF bitand(varFmode, 4*8*8*8*8) > 0 THEN  -- 040000 == S_IFDIR
    rc := serrno.EISDIR;
    msg := serrno.EISDIR_MSG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Has the file been changed meanwhile?
  IF varFLastMTime > varLastOpenTimeFromClient THEN
    rc := serrno.ENSFILECHG;
    msg := serrno.ENSFILECHG_MSG ||' : NSLastOpenTime='|| varFLastMTime
      ||', StagerLastOpenTime='|| varLastOpenTimeFromClient;
    ROLLBACK;
    RETURN;
  END IF;
  -- Cross check file and segment checksums when adler32 (AD in the file entry):
  -- unfortunately we have to play with different representations...
  IF (varFCksumName = 'AD' OR varFCksumName = 'PA') AND inSegEntry.checksum_name = 'adler32' AND
     inSegEntry.checksum != to_number(varFCksum, 'XXXXXXXX') THEN
    rc := serrno.SECHECKSUM;
    msg := serrno.SECHECKSUM_MSG ||' : '
      || upper(varFCksum) ||' vs '|| to_char(inSegEntry.checksum, 'XXXXXXXX');
    ROLLBACK;
    RETURN;
  END IF;
  -- Cross check segment (transfered) size and file size
  IF varFSize != inSegEntry.segSize THEN
    rc := serrno.ERTWRONGSIZE;
    msg := serrno.ERTWRONGSIZE_MSG ||' : expected '
      || to_char(varFSize) ||', got '|| to_char(inSegEntry.segSize);
    ROLLBACK;
    RETURN;
  END IF;
  -- Prevent to write a second copy of this file on a tape that already holds a valid copy,
  -- whilst allowing the overwrite of a same copyNo segment: this makes the NS operation
  -- fully idempotent and allows to compensate from possible errors in the tapegateway
  -- after a segment had been committed in the namespace.
  rc := checkAndDropSameCopynbSeg(inReqId, varFid, inSegEntry.vid, inSegEntry.copyNo);
  IF rc = serrno.EEXIST THEN
    msg := 'File already has a copy on VID '|| inSegEntry.vid;
    ROLLBACK;
    RETURN;
  END IF;
  
  -- We're done with the pre-checks, try and insert the segment metadata
  -- and deal with the possible unique (vid,fseq) constraint violation exception
  BEGIN
    varSegCreationTime := getTime();
    INSERT INTO Cns_seg_metadata (s_fileId, copyNo, fsec, segSize, s_status, vid,
      fseq, blockId, compression, side, checksum_name, checksum, gid, creationTime, lastModificationTime)
    VALUES (varFid, inSegEntry.copyNo, 1, inSegEntry.segSize, '-', inSegEntry.vid,
      inSegEntry.fseq, inSegEntry.blockId,
      CASE inSegEntry.comprSize WHEN 0 THEN 100 ELSE trunc(inSegEntry.segSize*100/inSegEntry.comprSize) END,
      0, inSegEntry.checksum_name, inSegEntry.checksum, varFGid, varSegCreationTime, varSegCreationTime);
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    -- This can be due to a PK violation or to the unique (vid,fseq) violation. The first
    -- is excluded because of checkAndDropSameCopynbSeg(), thus the second is the case:
    -- we have an existing segment at that fseq position. This is forbidden!
    rc := serrno.SEINTERNAL;
    msg := 'A file already exists at fseq '|| inSegEntry.fseq ||' on VID '|| inSegEntry.vid;
    ROLLBACK;
    RETURN;
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
    msg := serrno.ENSTOOMANYSEGS_MSG ||' : expected '|| to_char(varFCNbCopies) ||', got '
      || to_char(varNb) ||' attempting to write a new segment on VID '|| inSegEntry.vid;
    ROLLBACK;
    RETURN;
  END IF;
  -- Update file status
  UPDATE Cns_file_metadata SET status = 'm' WHERE fileid = varFid;
  -- Commit and log
  SELECT inSegEntry.blockId INTO varBlockId FROM Dual;  -- to_char() of a RAW type does not work. This does the trick...
  varParams := 'copyNb='|| inSegEntry.copyNo ||' SegmentSize='|| inSegEntry.segSize
    ||' Compression='|| CASE inSegEntry.comprSize WHEN 0 THEN 'inf' ELSE trunc(inSegEntry.segSize*100/inSegEntry.comprSize) END
    ||' TPVID='|| inSegEntry.vid ||' fseq='|| inSegEntry.fseq ||' BlockId="' || varBlockId
    ||'" gid=' || varFGid ||' ChecksumType="'|| inSegEntry.checksum_name ||'" ChecksumValue=' || varFCksum
    ||' creationTime=' || trunc(varSegCreationTime, 6);
  addSegResult(0, inReqId, 0, 'New segment information', varFid, varParams);
  COMMIT;
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
  varFGid INTEGER;
  varFClassId NUMBER;
  varFCNbCopies NUMBER;
  varFCksumName VARCHAR2(2);
  varFCksum VARCHAR2(32);
  varNb INTEGER;
  varBlockId VARCHAR2(8);
  varOwSeg castorns.Segment_Rec;
  varRepSeg castorns.Segment_Rec;
  varStatus CHAR(1);
  varParams VARCHAR2(2048);
  varOpenMode CHAR(1);
  varLastOpenTimeFromClient NUMBER;
  varSegCreationTime NUMBER;
  -- Trap `ORA-00001: unique constraint violated` errors
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -00001);
BEGIN
  rc := 0;
  msg := '';
  -- Retrieve open mode flag
  varOpenMode := getConfigOption('stager', 'openmode', NULL);
  -- Get file data and lock the entry, exit if not found
  IF varOpenMode = 'C' THEN
    SELECT fileId, filemode, mtime, fileClass, fileSize, csumType, csumValue, gid
      INTO varFid, varFmode, varFLastMTime, varFClassId, varFSize, varFCksumName, varFCksum, varFGid
      FROM Cns_file_metadata
     WHERE fileId = inSegEntry.fileId FOR UPDATE;
    -- in compatibility mode we only have second precision, thus we have to ceil
    -- the given lastOpenTime for a safe comparison with mtime
    varLastOpenTimeFromClient := CEIL(inSegEntry.lastOpenTime);
  ELSIF varOpenMode = 'N' THEN
    -- We truncate to 5 decimal digits, which is the precision of the lastOpenTime we get from the stager.
    -- Note this is just fitting the mantissa precision of a double, and it is due to the fact
    -- that those numbers go through OCI as double.
    SELECT fileId, filemode, TRUNC(stagertime, 5), fileClass, fileSize, csumType, csumValue, gid
      INTO varFid, varFmode, varFLastMTime, varFClassId, varFSize, varFCksumName, varFCksum, varFGid
      FROM Cns_file_metadata
     WHERE fileId = inSegEntry.fileId FOR UPDATE;
    varLastOpenTimeFromClient := inSegEntry.lastOpenTime;
  ELSE
    -- sanity check, should never happen
    rc := serrno.EINVAL;
    msg := 'Incorrect value found for openmode in CastorConfig: found '
            || varOpenMode ||', expected either C or N';
    ROLLBACK;
    RETURN;
  END IF;
  -- Is it a directory?
  IF bitand(varFmode, 4*8*8*8*8) > 0 THEN  -- 040000 == S_IFDIR
    rc := serrno.EISDIR;
    msg := serrno.EISDIR_MSG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Has the file been changed meanwhile?
  IF varFLastMTime > varLastOpenTimeFromClient THEN
    rc := serrno.ENSFILECHG;
    msg := serrno.ENSFILECHG_MSG ||' : NSLastOpenTime='|| varFLastMTime
      ||', StagerLastOpenTime='|| varLastOpenTimeFromClient;
    ROLLBACK;
    RETURN;
  END IF;
  -- Cross check file and segment checksums when adler32 (AD in the file entry):
  -- unfortunately we have to play with different representations...
  IF (varFCksumName = 'AD' OR varFCksumName = 'PA') AND inSegEntry.checksum_name = 'adler32' AND
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
    msg := serrno.ERTWRONGSIZE_MSG ||' : expected '
      || to_char(varFSize) ||', got '|| to_char(inSegEntry.segSize);
    ROLLBACK;
    RETURN;
  END IF;
  -- Repack specific: --
  -- Make sure the segment for the given oldCopyNo still exists
  BEGIN
    SELECT s_status, creationTime INTO varStatus, varSegCreationTime
      FROM Cns_seg_metadata
     WHERE s_fileid = varFid AND copyNo = inOldCopyNo;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Previous segment not found, give up
    rc := serrno.ENSNOSEG;
    msg := serrno.ENSNOSEG_MSG;
    ROLLBACK;
    RETURN;
  END;
  -- Check if a segment exists with the target copyNo, in case this is different from the
  -- old copyNo: such a segment is about to be overwritten
  IF inSegEntry.copyNo != inOldCopyNo THEN
    BEGIN
      SELECT s_status INTO varStatus
        FROM Cns_seg_metadata
       WHERE s_fileid = varFid AND copyNo = inSegEntry.copyNo;
      IF varStatus = '-' THEN
        -- We are asked to overwrite a valid segment and replace another one.
        -- This is forbidden.
        rc := serrno.ENSOVERWHENREP;
        msg := serrno.ENSOVERWHENREP_MSG;
        ROLLBACK;
        RETURN;
      END IF;
      -- OK, the segment being overwritten is invalid, we can drop it
      DELETE FROM Cns_seg_metadata
       WHERE s_fileid = varFid AND copyNo = inSegEntry.copyNo
      RETURNING copyNo, segSize, compression, vid, fseq, blockId, checksum_name, checksum,
                gid, creationTime, lastModificationTime
           INTO varOwSeg.copyNo, varOwSeg.segSize, varOwSeg.comprSize, varOwSeg.vid,
                varOwSeg.fseq, varOwSeg.blockId, varOwSeg.checksum_name, varOwSeg.checksum,
                varOwSeg.gid, varOwSeg.creationTime, varOwSeg.lastModificationTime;
      -- Log overwritten segment metadata
      SELECT varOwSeg.blockId INTO varBlockId FROM Dual;
      varParams := 'copyNb='|| varOwSeg.copyNo ||' SegmentSize='|| varOwSeg.segSize
        ||' Compression='|| varOwSeg.comprSize ||' TPVID='|| varOwSeg.vid
        ||' fseq='|| varOwSeg.fseq ||' BlockId="' || varBlockId ||'" gid=' || varOwSeg.gid
        ||' ChecksumType="'|| varOwSeg.checksum_name ||'" ChecksumValue=' || varOwSeg.checksum
        ||' creationTime=' || trunc(varOwSeg.creationTime, 6)
        ||' lastModificationTime=' || trunc(varOwSeg.lastModificationTime, 6);
      addSegResult(1, inReqId, 0, 'Unlinking segment (overwritten)', varFid, varParams);
    EXCEPTION WHEN NO_DATA_FOUND THEN
      -- Nothing found with the target copyNo: this means the existing segment we're repacking
      -- actually had an incorrect copyNo (e.g. it was 2 but no copyNo=1 existed). In such a case
      -- we want to fix the existing segment so that it gets properly replaced.
      -- Note that we won't throw CONSTRAINT_VIOLATED because we know inSegEntry.copyNo
      -- does not exist, and we won't throw NO_DATA_FOUND because inOldCopyNo does exist.
      UPDATE Cns_seg_metadata
         SET copyNo = inSegEntry.copyNo
       WHERE s_fileid = varFid
         AND copyNo = inOldCopyNo;
      addSegResult(1, inReqId, 0, 'Updating copy number', varFid, 'oldCopyNb='|| inOldCopyNo
                   ||' newCopyNb='|| inSegEntry.copyNo);
    END;
  END IF;
  -- Prevent to write a second copy of this file on a tape that already holds a valid copy,
  -- whilst allowing the overwrite of a same copyNo segment: this makes the NS operation
  -- fully idempotent and allows to compensate from possible errors in the tapegateway
  -- after a segment had been committed in the namespace.
  rc := checkAndDropSameCopynbSeg(inReqId, varFid, inSegEntry.vid, inSegEntry.copyNo);
  IF rc = serrno.EEXIST THEN
    msg := 'File already has a copy on VID '|| inSegEntry.vid;
    ROLLBACK;
    RETURN;
  END IF;

  -- We're done with the pre-checks, try and insert the segment metadata
  -- and deal with the possible unique (vid,fseq) constraint violation exception
  BEGIN
    INSERT INTO Cns_seg_metadata (s_fileId, copyNo, fsec, segSize, s_status, vid,
      fseq, blockId, compression, side, checksum_name, checksum, gid, creationTime, lastModificationTime)
    VALUES (varFid, inSegEntry.copyNo, 1, inSegEntry.segSize, '-', inSegEntry.vid,
      inSegEntry.fseq, inSegEntry.blockId,
      CASE inSegEntry.comprSize WHEN 0 THEN 100 ELSE trunc(inSegEntry.segSize*100/inSegEntry.comprSize) END,
      0, inSegEntry.checksum_name, inSegEntry.checksum, varFGid, varSegCreationTime, getTime());
  EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
    -- There must already be an existing segment at that fseq position for a different file.
    -- This is forbidden! Abort the entire operation.
    rc := serrno.SEINTERNAL;
    msg := 'A file already exists at fseq '|| inSegEntry.fseq ||' on VID '|| inSegEntry.vid;
    ROLLBACK;
    RETURN;
  END;
  -- Finally check for too many segments for this file: this can only happen if manual operations
  -- have created a previous inconsistency. In such a case we rollback and make repack fail,
  -- without attempting to repair the inconsistency (i.e. drop a segment).
  SELECT nbCopies INTO varFCNbCopies
    FROM Cns_class_metadata
   WHERE classid = varFClassId;
  SELECT count(*) INTO varNb
    FROM Cns_seg_metadata
   WHERE s_fileid = varFid AND s_status = '-';
  IF varNb > varFCNbCopies THEN
    rc := serrno.ENSTOOMANYSEGS;
    msg := serrno.ENSTOOMANYSEGS_MSG ||' : expected '|| to_char(varFCNbCopies) ||', got '
      || to_char(varNb) ||' attempting to write a new segment on VID '|| inSegEntry.vid;
    ROLLBACK;
    RETURN;
  END IF;
  -- All right, commit and log
  SELECT inSegEntry.blockId INTO varBlockId FROM Dual;  -- to_char() of a RAW type does not work. This does the trick...
  varParams := 'copyNb='|| inSegEntry.copyNo ||' SegmentSize='|| inSegEntry.segSize
    ||' Compression='|| CASE inSegEntry.comprSize WHEN 0 THEN 'inf' ELSE trunc(inSegEntry.segSize*100/inSegEntry.comprSize) END
    ||' TPVID='|| inSegEntry.vid
    ||' fseq='|| inSegEntry.fseq ||' blockId="' || varBlockId ||'" gid=' || varFGid
    ||' ChecksumType="'|| inSegEntry.checksum_name ||'" ChecksumValue=' || varFCksum
    ||' creationTime=' || trunc(varSegCreationTime, 6) ||' Repack=True';
  addSegResult(0, inReqId, 0, 'New segment information', varFid, varParams);
  COMMIT;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- The file entry was not found, just give up
  rc := serrno.ENOENT;
  msg := serrno.ENOENT_MSG;
END;
/

/**
 * This procedure sets or replaces segments for multiple files by calling either setSegmentForFile
 * or replaceSegmentForFile in a loop, the choice depending on the oldCopyNo values:
 * when 0, a normal migration is assumed and setSegmentForFile is called.
 * The input is fetched from SetSegsForFilesInputHelper and deleted,
 * the output is stored into SetSegsForFilesResultsHelper. In both cases the
 * inReqId UUID is a unique key identifying one invocation of this procedure.
 */
CREATE OR REPLACE PROCEDURE setOrReplaceSegmentsForFiles(inReqId IN VARCHAR2) AS
  varRC INTEGER;
  varParams VARCHAR2(1000);
  varStartTime TIMESTAMP;
  varSeg castorns.Segment_Rec;
  varCount INTEGER := 0;
  varErrCount INTEGER := 0;
BEGIN
  varStartTime := SYSTIMESTAMP;
  -- Loop over all files. Each call commits or rollbacks each file.
  FOR s IN (SELECT fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
                   vid, fseq, blockId, checksumType, checksum
              FROM SetSegsForFilesInputHelper
             WHERE reqId = inReqId) LOOP
    varSeg.fileId := s.fileId;
    varSeg.lastOpenTime := s.lastModTime;
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
      addSegResult(0, inReqId, varRC, 'Error creating/replacing segment', s.fileId, varParams);
      varErrCount := varErrCount + 1;
      COMMIT;
    END IF;
    varCount := varCount + 1;
  END LOOP;
  IF varCount > 0 THEN
    -- Final logging
    varParams := 'Function="setOrReplaceSegmentsForFiles" NbFiles='|| varCount
      || ' NbErrors=' || varErrCount
      ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
      ||' AvgProcessingTime='|| trunc(getSecs(varStartTime, SYSTIMESTAMP)/varCount, 6);
    addSegResult(1, inReqId, 0, 'Bulk processing complete', 0, varParams);
  END IF;
  -- Clean input data
  DELETE FROM SetSegsForFilesInputHelper
   WHERE reqId = inReqId;
  COMMIT;
  -- Return results and logs to the stager. Unfortunately we can't OPEN CURSOR FOR ...
  -- because we would get ORA-24338: 'statement handle not executed' at run time.
  -- Moreover, temporary tables are not supported with distributed transactions,
  -- so the stager will remotely open the SetSegsForFilesResultsHelper table, and
  -- we clean the tables by hand using the reqId key.
EXCEPTION WHEN OTHERS THEN
  -- In case of an uncaught exception, log it and preserve the SetSegsForFilesResultsHelper
  -- content for the stager as other files may have already been committed. Any other
  -- remaining file from the input will have to be migrated again.
  varParams := 'Function="setOrReplaceSegmentsForFiles" errorMessage="' || SQLERRM
        ||'" stackTrace="' || dbms_utility.format_error_backtrace ||'"';
  addSegResult(1, inReqId, SQLCODE, 'Uncaught exception', 0, varParams);
  DELETE FROM SetSegsForFilesInputHelper
   WHERE reqId = inReqId;
  COMMIT;
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

/* This procedure is used by repack to update the new 2.1.14 metadata (file.stagerTime, seg.creationTime,
 * seg.lastAccessTime, seg.gid) should they be missing. It shall be dropped on 2.1.15 after dropping
 * the compatibility mode code. The NS post-upgrade script runs a job that populates the same data
 * for all files.
 */
CREATE OR REPLACE PROCEDURE update2114Data(inVid IN VARCHAR2) AS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  FOR f IN (SELECT fileid, mtime, f.gid FROM Cns_file_metadata f, Cns_seg_metadata s
                         WHERE f.fileid = s.s_fileid AND s.vid = inVid
                           AND f.stagertime IS NULL) LOOP
    UPDATE Cns_file_metadata
       SET stagerTime = mtime
     WHERE fileid = f.fileid;
    UPDATE Cns_seg_metadata
       SET creationTime = f.mtime,
           lastModificationTime = f.mtime,
           gid = f.gid
     WHERE s_fileid = f.fileid;
    COMMIT;
  END LOOP;
END;
/

CREATE OR REPLACE PROCEDURE insertNSStats(inGid IN INTEGER, inTimestamp IN NUMBER,
                                          inMaxFileId IN INTEGER, inFileCount IN INTEGER, inFileSize IN INTEGER,
                                          inSegCount IN INTEGER, inSegSize IN INTEGER, inSegCompressedSize IN INTEGER,
                                          inSeg2Count IN INTEGER, inSeg2Size IN INTEGER, inSeg2CompressedSize IN INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  INSERT INTO UsageStats (gid, timestamp, maxFileId, fileCount, fileSize, segCount, segSize,
                          segCompressedSize, seg2Count, seg2Size, seg2CompressedSize)
    VALUES (inGid, inTimestamp, inMaxFileId, inFileCount, inFileSize, inSegCount, inSegSize,
            inSegCompressedSize, inSeg2Count, inSeg2Size, inSeg2CompressedSize);
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  UPDATE UsageStats SET
    maxFileId = CASE WHEN inMaxFileId > maxFileId THEN inMaxFileId ELSE maxFileId END,
    fileCount = fileCount + inFileCount,
    fileSize = fileSize + inFileSize,
    segCount = segCount + inSegCount,
    segSize = segSize + inSegSize,
    segCompressedSize = segCompressedSize + inSegCompressedSize,
    seg2Count = seg2Count + inSeg2Count,
    seg2Size = seg2Size + inSeg2Size,
    seg2CompressedSize = seg2CompressedSize + inSeg2CompressedSize
  WHERE gid = inGid AND timestamp = inTimestamp;
END;
/

/* This procedure is run as a database job to generate statistics from the namespace */
CREATE OR REPLACE PROCEDURE gatherNSStats AS
  varTimestamp NUMBER := getTime();
BEGIN
  -- File-level statistics
  FOR g IN (SELECT gid, MAX(fileid) maxId, COUNT(*) fileCount, SUM(filesize) fileSize
              FROM Cns_file_metadata
             GROUP BY gid) LOOP
    insertNSStats(g.gid, varTimestamp, g.maxId, g.fileCount, g.fileSize, 0, 0, 0, 0, 0, 0);
  END LOOP;
  COMMIT;
  -- Tape-level statistics
  FOR g IN (SELECT gid, copyNo, SUM(segSize * 100 / decode(compression,0,100,compression)) segComprSize,
                   SUM(segSize) segSize, COUNT(*) segCount
              FROM Cns_seg_metadata
             GROUP BY gid, copyNo) LOOP
    IF g.copyNo = 1 THEN
      insertNSStats(g.gid, varTimestamp, 0, 0, 0, g.segCount, g.segSize, g.segComprSize, 0, 0, 0);
    ELSE
      insertNSStats(g.gid, varTimestamp, 0, 0, 0, 0, 0, 0, g.segCount, g.segSize, g.segComprSize);
    END IF;
  END LOOP;
  COMMIT;
  -- Also compute totals
  INSERT INTO UsageStats (gid, timestamp, maxFileId, fileCount, fileSize, segCount, segSize,
                          segCompressedSize, seg2Count, seg2Size, seg2CompressedSize)
    (SELECT -1, varTimestamp, MAX(maxFileId), SUM(fileCount), SUM(fileSize),
            SUM(segCount), SUM(segSize), SUM(segCompressedSize),
            SUM(seg2Count), SUM(seg2Size), SUM(seg2CompressedSize)
       FROM UsageStats
      WHERE timestamp = varTimestamp);
  COMMIT;
END;
/

/* This pipelined function returns a report of namespace statistics over the given number of days */
CREATE OR REPLACE FUNCTION NSStatsReport(inDays INTEGER) RETURN castorns.Stats PIPELINED IS
BEGIN
  FOR l IN (
        SELECT NewStats.gid,
               NewStats.maxFileId, NewStats.fileCount, NewStats.fileSize,
               NewStats.segCount, NewStats.segSize, NewStats.segCompressedSize,
               NewStats.seg2Count, NewStats.seg2Size, NewStats.seg2CompressedSize,
               NewStats.maxFileId-OldStats.maxFileId fileIdDelta, NewStats.fileCount-OldStats.fileCount fileCountDelta,
               NewStats.fileSize-OldStats.fileSize fileSizeDelta,
               NewStats.segCount-OldStats.segCount segCountDelta, NewStats.segSize-OldStats.segSize segSizeDelta,
               NewStats.segCompressedSize-OldStats.segCompressedSize segCompressedSizeDelta,
               NewStats.seg2Count-OldStats.seg2Count seg2CountDelta, NewStats.seg2Size-OldStats.seg2Size seg2SizeDelta,
               NewStats.seg2CompressedSize-OldStats.seg2CompressedSize seg2CompressedSizeDelta
          FROM
            (SELECT gid, timestamp, maxFileId, fileCount, fileSize, segCount, segSize, segCompressedSize,
                    seg2Count, seg2Size, seg2CompressedSize
               FROM UsageStats
              WHERE timestamp = (SELECT MAX(timestamp) FROM UsageStats
                                  WHERE timestamp < getTime() - 86400*inDays)) OldStats,
            (SELECT gid, timestamp, maxFileId, fileCount, fileSize, segCount, segSize, segCompressedSize,
                    seg2Count, seg2Size, seg2CompressedSize
               FROM UsageStats
              WHERE timestamp = (SELECT MAX(timestamp) FROM UsageStats)) NewStats
         WHERE OldStats.gid = NewStats.gid
         ORDER BY NewStats.gid DESC) LOOP    -- gid = -1, i.e. the totals line, comes last
    PIPE ROW(l);
  END LOOP;
END;
/

/* A convenience view to get weekly statistics */
CREATE OR REPLACE VIEW WeeklyReportView AS
  SELECT * FROM TABLE(NSStatsReport(7));

/*
 * Database jobs
 */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name = 'NSSTATSJOB')
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every day executing the gatherNSStats procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'NSStatsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN gatherNSStats(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=DAILY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Gathering of Nameserver usage statistics');
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_14_2';
COMMIT;
