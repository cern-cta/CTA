/******************************************************************************
 *                 cns_2.1.14-2_to_2.1.14-3.sql
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
 * This script upgrades a CASTOR v2.1.14-2 CNS database to v2.1.14-3
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
     AND release = '2_1_14_3'
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
     AND release LIKE '2_1_14_2%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the CNS before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_3', 'TRANSPARENT');
COMMIT;

/* Update and revalidation of PL-SQL code */
/******************************************/

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
             WHERE gid IS NOT NULL    -- this will be dropped once the post-upgrade phase is completed
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


/* Flag the schema upgrade as COMPLETE */
/***************************************/

UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_14_3';
COMMIT;
