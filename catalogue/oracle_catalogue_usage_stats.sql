/*****************************************************************************
 *              oracle_catalogue_usage_stats.sql
 *
 * This file is part of the Castor/CTA project.
 * See http://cern.ch/castor and http://cern.ch/eoscta
 * Copyright (C) 2019  CERN
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 * This script adds the necessary PL/SQL code to an existing CTA Catalogue
 * schema in order to support the daily usage statistics gathering.
 *
 * This script should be ported to the other supported DBs in a future time.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Get current time as a time_t (Unix time) */
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

-- Helper procedure to insert/accumulate statistics in the UsageStats table
CREATE OR REPLACE PROCEDURE insertNSStats(inGid IN INTEGER, inTimestamp IN NUMBER, inMaxFileId IN INTEGER,
                                          inFileCount IN INTEGER, inFileSize IN INTEGER,
                                          inSegCount IN INTEGER, inSegSize IN INTEGER,
                                          inSeg2Count IN INTEGER, inSeg2Size IN INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  INSERT INTO UsageStats (gid, timestamp, maxFileId, fileCount, fileSize,
                          segCount, segSize, seg2Count, seg2Size)
    VALUES (inGid, inTimestamp, inMaxFileId, inFileCount, inFileSize,
            inSegCount, inSegSize, inSeg2Count, inSeg2Size);
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  UPDATE UsageStats SET
    maxFileId = CASE WHEN inMaxFileId > maxFileId THEN inMaxFileId ELSE maxFileId END,
    fileCount = fileCount + inFileCount,
    fileSize = fileSize + inFileSize,
    segCount = segCount + inSegCount,
    segSize = segSize + inSegSize,
    seg2Count = seg2Count + inSeg2Count,
    seg2Size = seg2Size + inSeg2Size
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
    insertNSStats(g.disk_file_gid, varTimestamp, g.maxId, g.fileCount, g.fileSize, 0, 0, 0, 0);
  END LOOP;
  COMMIT;
  -- Tape-level statistics
  FOR g IN (SELECT disk_file_gid, copy_nb, SUM(size_in_bytes) segSize, COUNT(*) segCount
              FROM Tape_File, Archive_File
             WHERE Tape_File.archive_file_id = Archive_File.archive_file_id
               AND Archive_File.creation_time < varTimestamp
             GROUP BY disk_file_gid, copy_nb) LOOP
    IF g.copy_nb = 1 THEN
      insertNSStats(g.disk_file_gid, varTimestamp, 0, 0, 0, g.segCount, g.segSize, 0, 0);
    ELSE
      insertNSStats(g.disk_file_gid, varTimestamp, 0, 0, 0, 0, 0, g.segCount, g.segSize);
    END IF;
  END LOOP;
  COMMIT;
  -- Also compute totals
  INSERT INTO UsageStats (gid, timestamp, maxFileId, fileCount, fileSize,
                          segCount, segSize, seg2Count, seg2Size)
    (SELECT -1, varTimestamp, MAX(maxFileId), SUM(fileCount), SUM(fileSize),
            SUM(segCount), SUM(segSize), SUM(seg2Count), SUM(seg2Size)
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
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=DAILY; INTERVAL=1',
      ENABLED         => TRUE,
      COMMENTS        => 'Gathering of catalogue usage statistics');
END;
/
