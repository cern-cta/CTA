/******************************************************************************
 *              oracleSchema.sql
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
 * @(#)RCSfile: oracleCreate.sql,v  Release: 1.2  Release Date: 2009/03/26 13:14:27  Author: waldron
 *
 * This script create a new Monitoring schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Determine the DLF schema that the monitoring procedures should run against */
UNDEF dlfschema
ACCEPT dlfschema DEFAULT castor_dlf PROMPT 'Enter the DLF schema to run monitoring queries against: (castor_dlf) ';
SET VER OFF

/* Check that the executing accounting can see the DLF base tables */
DECLARE
  unused VARCHAR2(2048);
BEGIN
  -- Check that the user exists
  BEGIN
    SELECT username INTO unused
      FROM all_users
     WHERE username = upper('&&dlfschema');
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20000, 'User &dlfschema does not exist');
  END;
  -- Check that the correct grants are present
  BEGIN
    SELECT owner INTO unused
      FROM all_tables
     WHERE owner = upper('&&dlfschema')
       AND table_name = 'DLF_VERSION';
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20001, 'Unable to access the &dlfschema..dlf_version table. Check that the correct grants have been issued!');
  END;
END;
/


/***** EXISTING/OLD MONITORING *****/

/* SQL statement for table LatencyStats */
CREATE TABLE LatencyStats (timestamp DATE CONSTRAINT NN_LatencyStats_ts NOT NULL, interval NUMBER, type VARCHAR2(255), started NUMBER, minTime NUMBER(*,4), maxTime NUMBER(*,4), avgTime NUMBER(*,4), stddevTime NUMBER(*,4), medianTime NUMBER(*,4))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table QueueTimeStats */
CREATE TABLE QueueTimeStats (timestamp DATE CONSTRAINT NN_QueueTimeStats_ts NOT NULL, interval NUMBER, type VARCHAR2(255), svcclass VARCHAR2(255), dispatched NUMBER, minTime NUMBER(*,4), maxTime NUMBER(*,4), avgTime NUMBER(*,4), stddevTime NUMBER(*,4), medianTime NUMBER(*,4))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table GarbageCollectionStats */
CREATE TABLE GarbageCollectionStats (timestamp DATE CONSTRAINT NN_GarbageCollectionStats_ts NOT NULL, interval NUMBER, diskserver VARCHAR2(255), type VARCHAR2(255), deleted NUMBER, totalSize NUMBER, minFileAge NUMBER(*,4), maxFileAge NUMBER(*,4), avgFileAge NUMBER(*,4), stddevFileAge NUMBER(*,4), medianFileAge NUMBER(*,4))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table RequestStats */
CREATE TABLE RequestStats (timestamp DATE CONSTRAINT NN_RequestStats_ts NOT NULL, interval NUMBER, type VARCHAR2(255), hostname VARCHAR2(255), euid VARCHAR2(255), requests NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table DiskCacheEfficiencyStats */
CREATE TABLE DiskCacheEfficiencyStats (timestamp DATE CONSTRAINT NN_DiskCacheEfficiencyStats_ts NOT NULL, interval NUMBER, type VARCHAR2(255), svcclass VARCHAR2(255), wait NUMBER, d2d NUMBER, recall NUMBER, staged NUMBER, total NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table FilesMigratedStats */
CREATE TABLE FilesMigratedStats (timestamp DATE CONSTRAINT NN_LFilesMigratedStats_ts NOT NULL, interval NUMBER, svcclass VARCHAR2(255), tapepool VARCHAR2(255), totalFiles NUMBER, totalSize NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table ReplicationStats */
CREATE TABLE ReplicationStats (timestamp DATE CONSTRAINT NN_ReplicationStats_ts NOT NULL, interval NUMBER, sourceSvcClass VARCHAR2(255), destSvcClass VARCHAR2(255), transferred NUMBER, totalSize NUMBER, minSize NUMBER(*,4), maxSize NUMBER(*,4), avgSize NUMBER(*,4), stddevSize NUMBER(*,4), medianSize NUMBER(*,4))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table TapeRecalledStats */
CREATE TABLE TapeRecalledStats (timestamp DATE CONSTRAINT NN_TapeRecalledStats_ts NOT NULL, interval NUMBER, type VARCHAR2(255), username VARCHAR2(255), groupname VARCHAR2(255), tapeVid VARCHAR2(255), tapeStatus VARCHAR2(255), files NUMBER, totalSize NUMBER, mountsPerDay NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table ProcessingTimeStats */
CREATE TABLE ProcessingTimeStats (timestamp DATE CONSTRAINT NN_ProcessingTimeStats_ts NOT NULL, interval NUMBER, daemon VARCHAR2(255), type VARCHAR2(255), requests NUMBER, minTime NUMBER(*,4), maxTime NUMBER(*,4), avgTime NUMBER(*,4), stddevTime NUMBER(*,4), medianTime NUMBER(*,4))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table ClientVersionStats */
CREATE TABLE ClientVersionStats (timestamp DATE CONSTRAINT NN_ClientVersionStats_ts NOT NULL, interval NUMBER, clientVersion VARCHAR2(255), requests NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for temporary table CacheEfficiencyHelper */
CREATE GLOBAL TEMPORARY TABLE CacheEfficiencyHelper (reqid CHAR(36))
  ON COMMIT DELETE ROWS;

/* SQL statement for table TapeMountsHelper */
CREATE TABLE TapeMountsHelper (timestamp DATE CONSTRAINT NN_TapeMountsHelper_ts NOT NULL, tapevid VARCHAR2(20))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for a view on the DLF_Monitoring table */
CREATE OR REPLACE VIEW DLFStats AS
  SELECT * FROM &dlfschema..dlf_monitoring;

/* SQL statement for a view on the DLF_Config table */
CREATE OR REPLACE VIEW DLFConfig AS
  SELECT * FROM &dlfschema..dlf_config;


/***** NEW MONITORING *****/

/* SQL statement for table ConfigSchema */
CREATE TABLE ConfigSchema (expiry NUMBER, runMaxTime DATE);
INSERT INTO ConfigSchema VALUES (90, SYSDATE);

/* SQL statement for table Requests */
CREATE TABLE Requests (subReqId CHAR(36) CONSTRAINT NN_Requests_subReqId NOT NULL CONSTRAINT PK_Requests_subReqId PRIMARY KEY, timestamp DATE CONSTRAINT NN_Requests_timestamp NOT NULL, reqId CHAR(36) CONSTRAINT NN_Requests_reqId NOT NULL, nsFileId NUMBER CONSTRAINT NN_Requests_nsFileId NOT NULL, type VARCHAR2(255), svcclass VARCHAR2(255), username VARCHAR2(255), state VARCHAR2(255), filename VARCHAR2(2048), filesize NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the Requests table */
CREATE INDEX I_Requests_timestamp ON Requests (timestamp) LOCAL;
CREATE INDEX I_Requests_reqId ON Requests (reqId) LOCAL;
CREATE INDEX I_Requests_svcclass ON Requests (svcclass) LOCAL;
CREATE INDEX I_Requests_filesize ON Requests (filesize) LOCAL;


/* SQL statement for table InternalDiskCopy */
CREATE TABLE InternalDiskCopy (timestamp DATE CONSTRAINT NN_InternalDiskCopy_ts NOT NULL, svcclass VARCHAR2(255), copies NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table TotalLatency */
CREATE TABLE TotalLatency (subReqId CHAR(36) CONSTRAINT NN_TotalLatency_subReqId NOT NULL CONSTRAINT PK_TotalLatency_subReqId PRIMARY KEY, timestamp DATE CONSTRAINT NN_TotalLatency_ts NOT NULL, nsFileId NUMBER CONSTRAINT NN_TotalLatency_nsFileId NOT NULL, totalLatency NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the TotalLatency table */
CREATE INDEX I_TotalLatency_timestamp ON TotalLatency (timestamp) LOCAL;
CREATE INDEX I_TotalLatency_totalLatency ON TotalLatency (totalLatency) LOCAL;

/* SQL statement for table TapeRecall */
CREATE TABLE TapeRecall (subReqId CHAR(36) CONSTRAINT NN_TapeRecall_subReqId NOT NULL CONSTRAINT PK_TapeRecall_subReqId PRIMARY KEY, timestamp DATE CONSTRAINT NN_TapeRecall_ts NOT NULL, tapeId VARCHAR2(255 BYTE), tapeMountState VARCHAR2(255 BYTE), readLatency INTEGER, copyLatency INTEGER, CONSTRAINT FK_TapeRecall_subReqId FOREIGN KEY (subReqId) REFERENCES Requests (subReqid) ON DELETE CASCADE)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));
/* SQL statements for indexes on the TapeRecall table */
CREATE INDEX I_TapeRecall_timestamp ON TapeRecall (timestamp) LOCAL;

  
/* SQL statement for table DiskCopy */
CREATE TABLE DiskCopy (nsFileId NUMBER CONSTRAINT NN_DiskCopy_nsFileId NOT NULL, timestamp DATE CONSTRAINT NN_DiskCopy_ts NOT NULL, originalPool VARCHAR2(255), targetPool VARCHAR2(255), readLatency INTEGER, copyLatency INTEGER, numCopiesInPools INTEGER, srcHost VARCHAR2(255), destHost VARCHAR2(255))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table GCFiles */
CREATE TABLE GcFiles (timestamp DATE CONSTRAINT NN_GCFiles_ts NOT NULL, nsFileId NUMBER CONSTRAINT NN_GCFiles_nsFileId NOT NULL, fileSize NUMBER, fileAge NUMBER, lastAccessTime NUMBER, nbAccesses NUMBER, gcType VARCHAR2(255), svcClass VARCHAR2(255))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the GcFiles table */
CREATE INDEX I_GcFiles_timestamp ON GcFiles (timestamp) LOCAL;
CREATE INDEX I_GcFiles_filesize ON GcFiles (filesize) LOCAL;
CREATE INDEX I_GcFiles_fileage ON GcFiles (fileage) LOCAL;

/* SQL statement for table Migration */
CREATE TABLE Migration (subReqId CHAR(36) CONSTRAINT NN_Migration_subReqId NOT NULL CONSTRAINT PK_Migration_subReqId PRIMARY KEY, timestamp DATE CONSTRAINT NN_Migration_ts NOT NULL, reqId CHAR(36) CONSTRAINT NN_Migration_reqId NOT NULL, nsfileid NUMBER CONSTRAINT NN_Migration_nsFileId NOT NULL, type VARCHAR2(255), svcclass VARCHAR2(255), username VARCHAR2(255), state VARCHAR2(255), filename VARCHAR2(2048), totalLatency NUMBER, filesize NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the Migration table */
CREATE INDEX I_Migration_timestamp ON Migration (timestamp) LOCAL;
CREATE INDEX I_Migration_reqId ON Migration (reqId) LOCAL;

/* SQL statement for creation of the Errors materialized view */
CREATE MATERIALIZED VIEW Errors_MV
  REFRESH FORCE ON DEMAND
  START WITH SYSDATE NEXT SYSDATE + 5/1440
AS
  SELECT fac.fac_name, txt.msg_text, count(*) errorsum
    FROM &dlfschema..dlf_messages mes, &dlfschema..dlf_msg_texts txt,
         &dlfschema..dlf_facilities fac
   WHERE mes.msg_no = txt.msg_no
     AND fac.fac_no = mes.facility
     AND mes.facility = txt.fac_no
     AND mes.severity = 3  -- Errors
     AND mes.timestamp >= sysdate - 15/1440
     AND mes.timestamp < sysdate - 5/1440
   GROUP BY fac.fac_name, txt.msg_text;

/* SQL statement for creation of the ReqDel materialized view */
CREATE MATERIALIZED VIEW ReqDel_MV
  REFRESH FORCE ON DEMAND
  START WITH SYSDATE NEXT SYSDATE + 10/1440
AS
  SELECT req.timestamp, req.svcclass, round((req.timestamp - gcFiles.timestamp) * 24, 5) dif
    FROM Requests req, GcFiles gcFiles
   WHERE req.nsFileId = gcFiles.nsFileId
     AND req.state = 'TapeRecall'
     AND req.timestamp > gcFiles.timestamp
     AND req.timestamp - gcFiles.timestamp <= 1;

CREATE INDEX I_ReqDel_MV_dif ON ReqDel_MV (dif);

/* SQL statement for creation of the GcMonitor materialized view */
CREATE MATERIALIZED VIEW GcMonitor_MV
 REFRESH COMPLETE ON DEMAND
 START WITH SYSDATE NEXT SYSDATE + 5/1440
AS
  SELECT a.tot total, round(a.avgage / 3600, 2) avg_age,
         round((a.avgage - b.avgage) / b.avgage, 4) age_per,
         round(a.avgsize / 1048576, 4) avg_size,
         round((a.avgsize - b.avgsize) / b.avgsize, 4) size_per
    FROM (SELECT count(*) tot, avg(fileAge) avgage, avg(fileSize) avgsize
            FROM GcFiles
           WHERE timestamp > sysdate - 10/1440
             AND timestamp <= sysdate - 5/1440) a,
         (SELECT avg(fileAge) avgage, avg(fileSize) avgsize
            FROM GcFiles
           WHERE timestamp > sysdate - 15/1440
             AND timestamp <= sysdate - 10/1140) b;

/* SQL statement for creation of the MainTableCounters materialized view */
CREATE MATERIALIZED VIEW MainTableCounters_MV
  REFRESH COMPLETE ON DEMAND
  START WITH SYSDATE NEXT SYSDATE + 5/1140
AS
  SELECT a.svcclass, a.state state, a.num num,
         round((a.num - b.num) / b.num, 2) per
    FROM (SELECT svcclass, state, count(*) num
              FROM Requests
           WHERE timestamp > sysdate - 10/1440
             AND timestamp <= sysdate -5/1440
             GROUP BY svcclass,state) a ,
         (SELECT svcclass, state, count(*) num
            FROM Requests
           WHERE timestamp > sysdate - 15/1440
             AND timestamp <= sysdate - 10/1440
           GROUP BY svcclass, state) b
   WHERE a.svcclass = b.svcclass
     AND a.state = b.state
   ORDER BY a.svcclass;

/* SQL statement for creation of the MigMonitor_MV materialized view */
CREATE MATERIALIZED VIEW MigMonitor_MV
  REFRESH COMPLETE ON DEMAND
  START WITH SYSDATE NEXT SYSDATE + 5/1140
AS
  SELECT svcclass, count(*) migs
    FROM Migration
   WHERE timestamp > sysdate - 10/1440
     AND timestamp <= sysdate - 5/1440
   GROUP BY svcclass;

/* SQL statement for creation of the SvcClassMap_MV materialized view */
CREATE MATERIALIZED VIEW SvcClassMap_MV
  REFRESH FORCE ON DEMAND
  START WITH SYSDATE + 30/1440 NEXT SYSDATE + 1
AS
  SELECT DISTINCT(svcclass) FROM Requests;

/* SQL statement to rename the NOT NULL constraints of the materialized view
 * created above.
 */
BEGIN
  FOR a IN (SELECT constraint_name, table_name, column_name
              FROM user_cons_columns
             WHERE table_name LIKE '%_MV'
               AND constraint_name LIKE 'SYS_%')
  LOOP
    EXECUTE IMMEDIATE 'ALTER MATERIALIZED VIEW '||a.table_name||' RENAME
                      CONSTRAINT '||a.constraint_name||
                      ' TO NN_'||a.table_name||'_'|| a.column_name;
  END LOOP;
END;
/

/* Error log tables */
BEGIN
  DBMS_ERRLOG.CREATE_ERROR_LOG ('Requests',     'Err_Requests');
  DBMS_ERRLOG.CREATE_ERROR_LOG ('Migration',    'Err_Migration');
  DBMS_ERRLOG.CREATE_ERROR_LOG ('DiskCopy',     'Err_DiskCopy');
  DBMS_ERRLOG.CREATE_ERROR_LOG ('TapeRecall',   'Err_TapeRecall');
  DBMS_ERRLOG.CREATE_ERROR_LOG ('TotalLatency', 'Err_Latency');
END;
/

/* SQL statement for the creation of the RequestedAfterGC view on the ReqDel_MV materialized view*/
CREATE OR REPLACE VIEW RequestedAfterGC
AS
  SELECT nvl(reqs_t.timestamp, sysdate) timestamp, nvl(reqs_t.interval,300) interval,
         reqs_t.svcclass, bin_t.bin, nvl(reqs_t.reqs,0) reqs
    FROM (SELECT * FROM (
            SELECT '0 0.1' bin       FROM dual
              UNION SELECT '0.1 0.5' FROM dual
              UNION SELECT '0.5 1'   FROM dual
              UNION SELECT '1 2'     FROM dual
              UNION SELECT '2 4'     FROM dual
              UNION SELECT '4 6'     FROM dual
              UNION SELECT '6 8'     FROM dual
              UNION SELECT '8 24'    FROM dual),
         (SELECT DISTINCT svcclass FROM ReqDel_MV)) bin_t
    LEFT OUTER JOIN (
      SELECT DISTINCT sysdate timestamp, 300 interval, svcclass,
                bin, count(bin) over (Partition BY svcclass, bin) reqs
        FROM (SELECT
                CASE WHEN dif >= 0   AND dif < 0.1 THEN '0 0.1'
                     WHEN dif >= 0.1 AND dif < 0.5 THEN '0.1 0.5'
                     WHEN dif >= 0.5 AND dif < 1   THEN  '0.5 1'
                     WHEN dif >= 1   AND dif < 2   THEN '1 2'
                     WHEN dif >= 2   AND dif < 4   THEN '2 4'
                     WHEN dif >= 4   AND dif < 6   THEN '4 6'
                     WHEN dif >= 6   AND dif < 8   THEN '6 8'
                     ELSE '8 24' END bin, svcclass
                FROM ReqDel_MV
               WHERE timestamp >  sysdate - 10/1440
                 AND timestamp <= sysdate - 5/1440)) reqs_t
    ON bin_t.bin = reqs_t.bin
   WHERE reqs_t.svcclass IS NOT NULL
   ORDER BY svcclass, bin_t.bin;

/* SQL statement for the creation of the GCStatsByFileSize view on the on the GcFiles table */
CREATE OR REPLACE VIEW GCStatsByFileSize
AS
  SELECT nvl(reqs_t.timestamp, sysdate) timestamp, nvl(reqs_t.interval,300) interval,
         reqs_t.svcclass, bin_t.bin, nvl(reqs_t.reqs,0) reqs
    FROM (SELECT * FROM (
            SELECT '0 1024' bin                   FROM dual
              UNION SELECT '1024 10240'           FROM dual
              UNION SELECT '10240 102400'         FROM dual
              UNION SELECT '102400 1048576'       FROM dual
              UNION SELECT '1048576 10485760'     FROM dual
              UNION SELECT '10485760 104857600'   FROM dual
              UNION SELECT '104857600 1073741000' FROM dual
              UNION SELECT '1073741000 -1'        FROM dual),
         (SELECT DISTINCT svcclass FROM GcFiles)) bin_t
    LEFT OUTER JOIN (
      SELECT DISTINCT sysdate timestamp, 300 interval, svcclass,
                bin, count(bin) over (Partition BY svcclass, bin) reqs
        FROM (SELECT
                CASE WHEN filesize < 1024                                 THEN '0 1024'
                     WHEN filesize >= 1024      AND filesize < 10240      THEN '1024 10240'
                     WHEN filesize >= 10240     AND filesize < 102400     THEN '10240 102400'
                     WHEN filesize >= 102400    AND filesize < 1048576    THEN '102400 1048576'
                     WHEN filesize >= 1048576   AND filesize < 10485760   THEN '1048576 10485760'
                     WHEN filesize >= 10485760  AND filesize < 104857600  THEN '10485760 104857600'
                     WHEN filesize >= 104857600 AND filesize < 1073741000 THEN '104857600 1073741000'
                     ELSE '1073741000 -1' END bin, svcclass
                FROM GcFiles
               WHERE filesize != 0
                 AND timestamp >= sysdate - 10/1440
                 AND timestamp <  sysdate - 5/1440)) reqs_t
    ON bin_t.bin = reqs_t.bin
   WHERE reqs_t.svcclass IS NOT NULL
   ORDER BY reqs_t.svcclass, bin_t.bin;

/* SQL statement for the creation of the GCStatsByFileAge view on the GcFiles table */
CREATE OR REPLACE VIEW GCStatsByFileAge
AS
  SELECT nvl(reqs_t.timestamp, sysdate) timestamp, nvl(reqs_t.interval,300) interval,
         reqs_t.svcclass, bin_t.bin, nvl(reqs_t.reqs,0) reqs
    FROM (SELECT * FROM (
            SELECT '0 10' bin               FROM dual
              UNION SELECT '10 60'          FROM dual
              UNION SELECT '60 900'         FROM dual
              UNION SELECT '900 1800'       FROM dual
              UNION SELECT '1800 3600'      FROM dual
              UNION SELECT '3600 21600'     FROM dual
              UNION SELECT '21600 43200'    FROM dual
              UNION SELECT '43200 86400'    FROM dual
              UNION SELECT '86400 172800'   FROM dual
              UNION SELECT '172800 345600'  FROM dual
              UNION SELECT '345600 691200'  FROM dual
              UNION SELECT '691200 1382400' FROM dual
              UNION SELECT '1382400 -1'     FROM dual),
         (SELECT DISTINCT svcclass FROM GcFiles)) bin_t
    LEFT OUTER JOIN (
      SELECT DISTINCT sysdate timestamp, 300 INTERVAL, svcclass,
                bin, count(bin) over (PARTITION BY svcclass, bin) reqs
        FROM (SELECT
                CASE WHEN fileage < 10                            THEN '0 10'
                     WHEN fileage >= 10     AND fileage < 60      THEN '10 60'
                     WHEN fileage >= 60     AND fileage < 900     THEN '60 900'
                     WHEN fileage >= 900    AND fileage < 1800    THEN '900 1800'
                     WHEN fileage >= 1800   AND fileage < 3600    THEN '1800 3600'
                     WHEN fileage >= 3600   AND fileage < 21600   THEN '3600 21600'
                     WHEN fileage >= 21600  AND fileage < 43200   THEN '21600 43200'
                     WHEN fileage >= 43200  AND fileage < 86400   THEN '43200 86400'
                     WHEN fileage >= 86400  AND fileage < 172800  THEN '86400 172800'
                     WHEN fileage >= 172800 AND fileage < 345600  THEN '172800 345600'
                     WHEN fileage >= 345600 AND fileage < 691200  THEN '345600 691200'
                     WHEN fileage >= 691200 AND fileage < 1382400 THEN '691200 1382400'
                     ELSE '1382400 -1' END bin, svcclass
                FROM GcFiles
               WHERE fileage IS NOT NULL
                 AND timestamp >= sysdate - 10/1440
                 AND timestamp <  sysdate - 5/1440)) reqs_t
    ON bin_t.bin = reqs_t.bin
   WHERE reqs_t.svcclass IS NOT NULL
   ORDER BY svcclass, bin_t.bin;
