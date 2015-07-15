/******************************************************************************
 *                 cns_2.1.14-5_to_2.1.14-11.sql
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
 * This script upgrades a CASTOR v2.1.14-5 CNS database to v2.1.14-11
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
   WHERE schemaVersion = '2_1_14_2'
     AND release = '2_1_14_11'
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
     AND release LIKE '2_1_14_5%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the CNS before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_11', 'NON TRANSPARENT');
COMMIT;

/* useful procedure to recompile all invalid items in the DB
   as many times as needed, until nothing can be improved anymore.
   Also reports the list of invalid items if any */
CREATE OR REPLACE PROCEDURE recompileAll AS
  varNbInvalids INTEGER;
  varNbInvalidsLastRun INTEGER := -1;
BEGIN
  WHILE varNbInvalidsLastRun != 0 LOOP
    varNbInvalids := 0;
    FOR a IN (SELECT object_name, object_type
                FROM user_objects
               WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY')
                 AND status = 'INVALID')
    LOOP
      IF a.object_type = 'PACKAGE BODY' THEN a.object_type := 'PACKAGE'; END IF;
      BEGIN
        EXECUTE IMMEDIATE 'ALTER ' ||a.object_type||' '||a.object_name||' COMPILE';
      EXCEPTION WHEN OTHERS THEN
        -- ignore, so that we continue compiling the other invalid items
        NULL;
      END;
    END LOOP;
    -- check how many invalids are still around
    SELECT count(*) INTO varNbInvalids FROM user_objects
     WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY') AND status = 'INVALID';
    -- should we give up ?
    IF varNbInvalids = varNbInvalidsLastRun THEN
      DECLARE
        varInvalidItems VARCHAR(2048);
      BEGIN
        -- yes, as we did not move forward on this run
        SELECT LISTAGG(object_name, ', ') WITHIN GROUP (ORDER BY object_name) INTO varInvalidItems
          FROM user_objects
         WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY') AND status = 'INVALID';
        raise_application_error(-20000, 'Revalidation of PL/SQL code failed. Still ' ||
                                        varNbInvalids || ' invalid items : ' || varInvalidItems);
      END;
    END IF;
    -- prepare for next loop
    varNbInvalidsLastRun := varNbInvalids;
    varNbInvalids := 0;
  END LOOP;
END;
/

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
    timeInterval NUMBER,
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

/* This procedure is run as a database job to generate statistics from the namespace */
CREATE OR REPLACE PROCEDURE gatherNSStats AS
  varTimestamp NUMBER := getTime();
BEGIN
  -- File-level statistics
  FOR g IN (SELECT gid, MAX(fileid) maxId, COUNT(*) fileCount, SUM(filesize) fileSize
              FROM Cns_file_metadata
             WHERE stagerTime < varTimestamp
             GROUP BY gid) LOOP
    insertNSStats(g.gid, varTimestamp, g.maxId, g.fileCount, g.fileSize, 0, 0, 0, 0, 0, 0);
  END LOOP;
  COMMIT;
  -- Tape-level statistics
  FOR g IN (SELECT gid, copyNo, SUM(segSize * 100 / decode(compression,0,100,compression)) segComprSize,
                   SUM(segSize) segSize, COUNT(*) segCount
              FROM Cns_seg_metadata
             WHERE creationTime < varTimestamp
               AND gid IS NOT NULL    -- XXX this will be dropped once the post-upgrade phase is completed
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
        SELECT NewStats.timestamp-OldStats.timeStamp timeInterval, NewStats.gid,
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


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_14_11';
COMMIT;
