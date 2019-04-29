-- commit any previous pending session
COMMIT;


--------------------------------------------------
-- 1. Usage stats. This section should be ported
-- to PostgreSQL at some time in the future.
--------------------------------------------------

-- Table for namespace statistics. Same logic as in CASTOR.
CREATE TABLE UsageStats (
  gid NUMBER(6) DEFAULT 0 CONSTRAINT NN_UsageStats_gid NOT NULL,
  timestamp NUMBER  DEFAULT 0 CONSTRAINT NN_UsageStats_ts NOT NULL,
  maxFileId INTEGER, fileCount INTEGER, fileSize INTEGER,
  segCount INTEGER, segSize INTEGER, segCompressedSize INTEGER,
  seg2Count INTEGER, seg2Size INTEGER, seg2CompressedSize INTEGER);

ALTER TABLE UsageStats ADD CONSTRAINT PK_UsageStats_gid_ts PRIMARY KEY (gid, timestamp);

-- This table will be used to safely store the legacy CASTOR usage statistics.
CREATE TABLE CastorUsageStats (
  gid NUMBER(6) DEFAULT 0 CONSTRAINT NN_UsageStats_gid NOT NULL,
  timestamp NUMBER  DEFAULT 0 CONSTRAINT NN_UsageStats_ts NOT NULL,
  maxFileId INTEGER, fileCount INTEGER, fileSize INTEGER,
  segCount INTEGER, segSize INTEGER, segCompressedSize INTEGER,
  seg2Count INTEGER, seg2Size INTEGER, seg2CompressedSize INTEGER);

-- This table is used to store the mapping gid -> experiment name, like in CASTOR.
-- Still to be manually updated, in the lack of an automated mechanism.
CREATE TABLE EXPERIMENTS (
   name VARCHAR2(20 BYTE),
   gid NUMBER(6,0) CONSTRAINT "GID_PK" PRIMARY KEY ("GID"));


-- Helper procedure to insert/accumulate statistics in the UsageStats table
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

-- This procedure is run as a database job to generate statistics from the namespace
-- Taken as is from CASTOR, cf. https://gitlab.cern.ch/castor/CASTOR/tree/master/ns/oracleTrailer.sql
CREATE OR REPLACE PROCEDURE gatherCatalogueStats AS
  varTimestamp NUMBER := trunc(getTime());
BEGIN
  -- File-level statistics
  FOR g IN (SELECT disk_file_gid, MAX(archive_file_id) maxId,
                   COUNT(*) fileCount, SUM(size_in_bytes) fileSize
              FROM Archive_File
             WHERE creation_time < varTimestamp
             GROUP BY disk_file_gid) LOOP
    insertNSStats(g.disk_file_gid, varTimestamp, g.maxId, g.fileCount, g.fileSize, 0, 0, 0, 0, 0, 0);
  END LOOP;
  COMMIT;
  -- Tape-level statistics
  FOR g IN (SELECT disk_file_gid, copy_nb, SUM(size_in_bytes) segComprSize,
                   SUM(size_in_bytes) segSize, COUNT(*) segCount
              FROM Tape_File, Archive_File
             WHERE Tape_File.archive_file_id = Archive_File.archive_file_id
               AND Archive_File.creation_time < varTimestamp
             GROUP BY disk_file_gid, copy_nb) LOOP
    IF g.copy_nb = 1 THEN
      insertNSStats(g.disk_file_gid, varTimestamp, 0, 0, 0, g.segCount, g.segSize, g.segComprSize, 0, 0, 0);
    ELSE
      insertNSStats(g.disk_file_gid, varTimestamp, 0, 0, 0, 0, 0, 0, g.segCount, g.segSize, g.segComprSize);
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

/* Database job for the statistics */
BEGIN
  -- Remove database jobs before recreating them
  FOR j IN (SELECT job_name FROM user_scheduler_jobs
             WHERE job_name = 'STATSJOB')
  LOOP
    DBMS_SCHEDULER.DROP_JOB(j.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every day executing the gatherNSStats procedure
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'StatsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN gatherCatalogueStats(); END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 60/1440,
      REPEAT_INTERVAL => 'FREQ=DAILY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Gathering of catalogue usage statistics');
END;
/



---------------------------------------------
-- 2. CASTOR to CTA migration. This code is
-- only supported for Oracle.
---------------------------------------------

-- Create synonyms for all relevant tables
-- XXX TBD XXX

-- Import metadata from the CASTOR namespace
CREATE OR REPLACE importFromCASTOR(inTapePool VARCHAR2, inEOSCTAInstance VARCHAR2,
	                               Dirs OUT SYS_REFCURSOR, Files OUT SYS_REFCURSOR) AS
  nbFiles INTEGER;
  pathInEos VARCHAR2;
  ct INTEGER := 0;
BEGIN
  -- XXX error handling is missing
  castor.prepareCTAExport(inTapePool, nbFiles);
  castor.dirsForCTAExport(inTapePool);
  -- Get all metadata for the EOS-side namespace
  OPEN Dirs FOR
    SELECT *
      FROM castor.CTADirsHelper;
  castor.filesForCTAExport(inTapePool);
  OPEN Files FOR
    SELECT *
      FROM castor.CTAFilesHelper;
END;
/

CREATE OR REPLACE populateCTAFromCASTOR AS
BEGIN
  -- Populate the CTA catalogue with the CASTOR file/tape metadata
  FOR f IN (SELECT * FROM castor.CTAFilesHelper) LOOP
    pathInEos := '/eos/cta/' || inEOSCTAInstance || f.path;   -- XXX how to massage this?
    -- insert file metadata
   	INSERT INTO Archive_File (archive_file_id, disk_instance_name, disk_file_id, disk_file_path,
   	  disk_gid, size_in_bytes, checksum_type, checksum_value,
   	  storage_class_id, creation_time, reconciliation_time)
    VALUES (f.fileId, inEOSCTAInstance, f.fileId, pathInEos, f.gid,
    	    f.filesize, 'AD', f.checksum, f.fileclass, f.atime, 0);
    -- insert tape metadata
   	INSERT INTO Tape_File (archive_file_id, vid, fseq, block_id, copy_nb, creation_time)
    VALUES (f.fileId, f.vid, f.fseq, f.blockId, f.copyno, f.s_mtime);
    IF ct = 10000 THEN
      COMMIT;
      ct := 0;
    END IF;
    ct := ct + 1;
  END LOOP;
END;
/

CREATE OR REPLACE completeImportFromCASTOR AS
BEGIN
  castor.completeCTAExport;
END;
/
