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
CREATE TABLE LatencyStats (timestamp DATE CONSTRAINT NN_LatencyStats_ts NOT NULL, interval NUMBER, RequestType VARCHAR2(255), protocol VARCHAR2(255), started NUMBER(*,3), minLatencyTime NUMBER(*,3), maxLatencyTime NUMBER(*,3), avgLatencyTime NUMBER(*,3), stddevLatencyTime NUMBER(*,3), medianLatencyTime NUMBER(*,3))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table QueueTimeStats */
CREATE TABLE QueueTimeStats (timestamp DATE CONSTRAINT NN_QueueTimeStats_ts NOT NULL, interval NUMBER, requestType VARCHAR2(255), svcClass VARCHAR2(255), dispatched NUMBER(*,3), minQueueTime NUMBER(*,3), maxQueueTime NUMBER(*,3), avgQueueTime NUMBER(*,3), stddevQueueTime NUMBER(*,3), medianQueueTime NUMBER(*,3))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table GarbageCollectionStats */
CREATE TABLE GarbageCollectionStats (timestamp DATE CONSTRAINT NN_GarbageCollectionStats_ts NOT NULL, interval NUMBER, diskserver VARCHAR2(255), requestType VARCHAR2(255), deleted NUMBER(*,3), totalFileSize NUMBER, minFileAge NUMBER(*,0), maxFileAge NUMBER(*,0), avgFileAge NUMBER(*,0), stddevFileAge NUMBER(*,0), medianFileAge NUMBER(*,0), minFileSize NUMBER, maxFileSize NUMBER, avgFileSize NUMBER, stddevFileSize NUMBER, medianFileSize NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table RequestStats */
CREATE TABLE RequestStats (timestamp DATE CONSTRAINT NN_RequestStats_ts NOT NULL, interval NUMBER, requestType VARCHAR2(255), hostname VARCHAR2(255), euid VARCHAR2(255), requests NUMBER(*,3))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table DiskCacheEfficiencyStats */
CREATE TABLE DiskCacheEfficiencyStats (timestamp DATE CONSTRAINT NN_DiskCacheEfficiencyStats_ts NOT NULL, interval NUMBER, requestType VARCHAR2(255), svcClass VARCHAR2(255), wait NUMBER(*,3), d2d NUMBER(*,3), recall NUMBER(*,3), staged NUMBER(*,3), waitPerc NUMBER(*,2), d2dPerc NUMBER(*,2), recallPerc NUMBER(*,2), stagedPerc NUMBER(*,2), total NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table FilesMigratedStats */
CREATE TABLE FilesMigratedStats (timestamp DATE CONSTRAINT NN_FilesMigratedStats_ts NOT NULL, interval NUMBER, svcClass VARCHAR2(255), tapepool VARCHAR2(255), nbFiles NUMBER, totalFileSize NUMBER, NumBytesWriteAvg NUMBER(*,2))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table FilesRecalledStats */
CREATE TABLE FilesRecalledStats (timestamp DATE CONSTRAINT NN_FilesRecalledStats_ts NOT NULL, interval NUMBER, nbFiles NUMBER, totalFileSize NUMBER, NumBytesReadAvg NUMBER(*,2))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table ReplicationStats */
CREATE TABLE ReplicationStats (timestamp DATE CONSTRAINT NN_ReplicationStats_ts NOT NULL, interval NUMBER, sourceSvcClass VARCHAR2(255), destSvcClass VARCHAR2(255), transferred NUMBER(*,3), totalFileSize NUMBER, minFileSize NUMBER, maxFileSize NUMBER, avgFileSize NUMBER, stddevFileSize NUMBER, medianFileSize NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table TapeRecalledStats */
CREATE TABLE TapeRecalledStats (timestamp DATE CONSTRAINT NN_TapeRecalledStats_ts NOT NULL, interval NUMBER, requestType VARCHAR2(255), username VARCHAR2(255), groupname VARCHAR2(255), tapeVid VARCHAR2(255), tapeStatus VARCHAR2(255), nbFiles NUMBER, totalFileSize NUMBER, mountsPerDay NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table ProcessingTimeStats */
CREATE TABLE ProcessingTimeStats (timestamp DATE CONSTRAINT NN_ProcessingTimeStats_ts NOT NULL, interval NUMBER, daemon VARCHAR2(255), requestType VARCHAR2(255), svcClass VARCHAR2(255), requests NUMBER(*,3), minProcessingTime NUMBER(*,3), maxProcessingTime NUMBER(*,3), avgProcessingTime NUMBER(*,3), stddevProcessingTime NUMBER(*,3), medianProcessingTime NUMBER(*,3))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table ClientVersionStats */
CREATE TABLE ClientVersionStats (timestamp DATE CONSTRAINT NN_ClientVersionStats_ts NOT NULL, interval NUMBER, clientVersion VARCHAR2(255), requests NUMBER(*,3))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table TapeMountStats */
CREATE TABLE TapeMountStats (timestamp DATE CONSTRAINT NN_TapeMountStats_ts NOT NULL, interval NUMBER, direction VARCHAR2(20), nbMounts NUMBER, nbFiles NUMBER, totalFileSize NUMBER, avgRunTime NUMBER(*,3), nbFilesPerMount NUMBER(*,0), failures NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for temporary table CacheEfficiencyHelper */
CREATE GLOBAL TEMPORARY TABLE CacheEfficiencyHelper (reqid CHAR(36))
  ON COMMIT DELETE ROWS;

/* SQL statement for table TapeMountsHelper */
CREATE TABLE TapeMountsHelper (timestamp DATE CONSTRAINT NN_TapeMountsHelper_ts NOT NULL, facility NUMBER, tapevid VARCHAR2(20))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the TapeMountsHelper table */
CREATE INDEX I_TapeHelper_timestamp ON TapeMountsHelper (timestamp) LOCAL;

/* SQL statement for a view on the DLF_Config table */
CREATE OR REPLACE VIEW DLFConfig AS
  SELECT * FROM &dlfschema..dlf_config;


/***** NEW MONITORING *****/

/* SQL statement for table ConfigSchema */
CREATE TABLE ConfigSchema (expiry NUMBER);
INSERT INTO ConfigSchema VALUES (90);

/* SQL statement for table Requests */
CREATE TABLE Requests (subReqId CHAR(36) CONSTRAINT NN_Requests_subReqId NOT NULL CONSTRAINT PK_Requests_subReqId PRIMARY KEY, timestamp DATE CONSTRAINT NN_Requests_timestamp NOT NULL, reqId CHAR(36) CONSTRAINT NN_Requests_reqId NOT NULL, nsFileId NUMBER CONSTRAINT NN_Requests_nsFileId NOT NULL, type VARCHAR2(255), svcclass VARCHAR2(255), username VARCHAR2(255), state VARCHAR2(255), filename VARCHAR2(2048), filesize NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the Requests table */
CREATE INDEX I_Requests_timestamp ON Requests (timestamp) LOCAL;
CREATE INDEX I_Requests_reqId     ON Requests (reqId) LOCAL;
CREATE INDEX I_Requests_svcclass  ON Requests (svcclass) LOCAL;
CREATE INDEX I_Requests_filesize  ON Requests (filesize) LOCAL;

/* SQL statement for table InternalDiskCopy */
CREATE TABLE InternalDiskCopy (timestamp DATE CONSTRAINT NN_InternalDiskCopy_ts NOT NULL, svcclass VARCHAR2(255), copies NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table TotalLatency */
CREATE TABLE TotalLatency (subReqId CHAR(36) CONSTRAINT NN_TotalLatency_subReqId NOT NULL CONSTRAINT PK_TotalLatency_subReqId PRIMARY KEY, timestamp DATE CONSTRAINT NN_TotalLatency_ts NOT NULL, nsFileId NUMBER CONSTRAINT NN_TotalLatency_nsFileId NOT NULL, totalLatency NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the TotalLatency table */
CREATE INDEX I_TotalLatency_timestamp    ON TotalLatency (timestamp) LOCAL;
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
CREATE INDEX I_GcFiles_filesize  ON GcFiles (filesize) LOCAL;
CREATE INDEX I_GcFiles_fileage   ON GcFiles (fileage) LOCAL;

/* SQL statement for table Migration */
CREATE TABLE Migration (subReqId CHAR(36) CONSTRAINT NN_Migration_subReqId NOT NULL CONSTRAINT PK_Migration_subReqId PRIMARY KEY, timestamp DATE CONSTRAINT NN_Migration_ts NOT NULL, reqId CHAR(36) CONSTRAINT NN_Migration_reqId NOT NULL, nsfileid NUMBER CONSTRAINT NN_Migration_nsFileId NOT NULL, type VARCHAR2(255), svcclass VARCHAR2(255), username VARCHAR2(255), state VARCHAR2(255), filename VARCHAR2(2048), totalLatency NUMBER, filesize NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the Migration table */
CREATE INDEX I_Migration_timestamp ON Migration (timestamp) LOCAL;
CREATE INDEX I_Migration_reqId     ON Migration (reqId) LOCAL;

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
     AND mes.severity = 3  -- Error
     AND mes.timestamp >= SYSDATE - 15/1440
     AND mes.timestamp < SYSDATE - 5/1440
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
           WHERE timestamp > SYSDATE - 10/1440
             AND timestamp <= SYSDATE - 5/1440) a,
         (SELECT avg(fileAge) avgage, avg(fileSize) avgsize
            FROM GcFiles
           WHERE timestamp > SYSDATE - 15/1440
             AND timestamp <= SYSDATE - 10/1140) b;

/* SQL statement for creation of the MainTableCounters materialized view */
CREATE MATERIALIZED VIEW MainTableCounters_MV
  REFRESH COMPLETE ON DEMAND
  START WITH SYSDATE NEXT SYSDATE + 5/1140
AS
  SELECT a.svcclass, a.state state, a.num num,
         round((a.num - b.num) / b.num, 2) per
    FROM (SELECT svcclass, state, count(*) num
              FROM Requests
           WHERE timestamp > SYSDATE - 10/1440
             AND timestamp <= SYSDATE -5/1440
             GROUP BY svcclass,state) a ,
         (SELECT svcclass, state, count(*) num
            FROM Requests
           WHERE timestamp > SYSDATE - 15/1440
             AND timestamp <= SYSDATE - 10/1440
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
   WHERE timestamp > SYSDATE - 10/1440
     AND timestamp <= SYSDATE - 5/1440
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

CREATE TABLE CastorVersion (schemaVersion VARCHAR2(20), release VARCHAR2(20));
INSERT INTO CastorVersion VALUES ('-', '2_1_9_0');


/******************************************************************************
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
 * @(#)RCSfile: oracleCreate.sql,v  Release: 1.2  Release Date: 2009/03/26 13:14:27  Author: waldron
 *
 * This script create a new Monitoring schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/


/* Update the schema version */
UPDATE CastorVersion SET schemaVersion = '2_1_9_0';


/***** EXISTING/OLD MONITORING *****/

/* PL/SQL method implementing statsLatency
 *
 * Provides statistics on the amount of time a user has had to wait since their
 * request was entered into the system and it actually being served. The
 * returned data is broken down by request type and protocol.
 */
CREATE OR REPLACE PROCEDURE statsLatency (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: LatencyStats
  -- Frequency: 5 minutes
  INSERT INTO LatencyStats
    (timestamp, interval, requestType, protocol, started, minLatencyTime,
     maxLatencyTime, avgLatencyTime, stddevLatencyTime, medianLatencyTime)
    -- Gather data
    SELECT now - 5/1440 timestamp, interval,
           nvl(type, 'StageDiskCopyReplicaRequest') requestType, protocol,
           (count(*) / interval) started,
           min(waitTime)         minLatencyTime,
           max(waitTime)         maxLatencyTime,
           avg(waitTime)         avgLatencyTime,
           stddev_pop(waitTime)  stddevLatencyTime,
           median(waitTime)      medianLatencyTime
      FROM (
        SELECT waitTime,
               max(decode(params.name, 'Type',     params.value, NULL)) type,
               max(decode(params.name, 'Protocol', params.value, NULL)) protocol
          FROM (
            -- Extract the totalWaitTime for all stagerjob's or d2dtransfer's
            -- which have started.
            SELECT params.id, params.value waitTime
              FROM &dlfschema..dlf_messages messages,
                   &dlfschema..dlf_num_param_values params
             WHERE messages.id = params.id
               AND messages.severity = 8     -- Info
               AND ((messages.facility = 26  -- stagerjob
               AND   messages.msg_no = 20)   -- Job started
                OR  (messages.facility = 23  -- d2dtransfer
               AND   messages.msg_no = 25))  -- DiskCopyTransfer started
               AND messages.timestamp >  now - 10/1440
               AND messages.timestamp <= now - 5/1440
               AND params.name = 'TotalWaitTime'
               AND params.timestamp >  now - 10/1440
               AND params.timestamp <= now - 5/1440
          ) results
      -- For facility 23 (d2dtransfer) we can assume that the request type
      -- associated with the transfer is 133 (StageDiskCopyReplicaRequest).
      -- However, for normal jobs we must parse the Arguments attribute of the
      -- start message to determine the request type. Hence the left join,
      -- NULL's are 133!!
      LEFT JOIN &dlfschema..dlf_str_param_values params
        ON results.id = params.id
       AND params.name IN ('Type', 'Protocol')
       AND params.timestamp >  now - 10/1440
       AND params.timestamp <= now - 5/1440
     GROUP BY waitTime)
     GROUP BY type, protocol;
END;
/


/* PL/SQL method implementing statsQueueTime
 *
 * Provides statistics on the queue time of requests in LSF broken down by
 * request type and service class.
 */
CREATE OR REPLACE PROCEDURE statsQueueTime (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: QueueTimeStats
  -- Frequency: 5 minutes
  INSERT INTO QueueTimeStats
    (timestamp, interval, requestType, svcClass, dispatched, minQueueTime,
     maxQueueTime, avgQueueTime, stddevQueueTime, medianQueueTime)
    -- Gather data
    SELECT now - 5/1440 timestamp, interval, type, svcClass,
           (count(*) / interval) dispatched,
           min(queueTime)        minQueueTime,
           max(queueTime)        maxQueueTime,
           avg(queueTime)        avgQueueTime,
           stddev_pop(queueTime) stddevQueueTime,
           median(queueTime)     medianQueueTime
      FROM (
        -- Extract the type, service class and queue time for all jobs
        -- dispatched by LSF
        SELECT messages.id,
               max(decode(str.name, 'Type',      str.value, NULL)) type,
               max(decode(str.name, 'SvcClass',  str.value, NULL)) svcClass,
               max(decode(num.name, 'QueueTime', num.value, NULL)) queueTime
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_str_param_values str,
               &dlfschema..dlf_num_param_values num
         WHERE messages.id = str.id
           AND messages.id = num.id
           AND messages.severity = 8 -- Info
           AND messages.facility = 9 -- schedulerd
           AND messages.msg_no = 34  -- Wrote notification file
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND str.name IN ('Type', 'SvcClass')
           AND str.timestamp >  now - 10/1440
           AND str.timestamp <= now - 5/1440
           AND num.name = 'QueueTime'
           AND num.timestamp >  now - 10/1440
           AND num.timestamp <= now - 5/1440
         GROUP BY messages.id)
     GROUP BY type, svcClass;
END;
/


/* PL/SQL method implementing statsGarbageCollection
 *
 * Provides an overview of the garbage collection process which includes the
 * number of files removed during the last interval, the total volume of
 * reclaimed space and statistical information.
 */
CREATE OR REPLACE PROCEDURE statsGarbageCollection (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: GarbageCollectionStats
  -- Frequency: 5 minutes
  INSERT INTO GarbageCollectionStats
    (timestamp, interval, diskServer, requestType, deleted, totalFileSize,
     minFileAge, maxFileAge, avgFileAge, stddevFileAge, medianFileAge,
     minFileSize, maxFileSize, avgFileSize, stddevFileSize, medianFileSize)
    -- Gather data
    SELECT now - 5/1440 timestamp, interval, diskserver, type,
           (count(*) / interval) deleted,
           sum(fileSize)        totalFileSize,
           -- File age statistics
           min(fileAge)         minFileAge,
           max(fileAge)         maxFileAge,
           avg(fileAge)         avgFileAge,
           stddev_pop(fileAge)  stddevFileAge,
           median(fileAge)      medianFileAge,
           -- File size statistics
           min(fileSize)        minFileSize,
           max(fileSize)        maxFileSize,
           avg(fileSize)        avgFileSize,
           stddev_pop(fileSize) stddevFileSize,
           median(fileSize)     medianFileSize
      FROM (
        SELECT hostname diskserver, results.fileAge, results.fileSize,
               replace(params.value, ' ', '_') type
          FROM (
            -- Extract the file age, size and service class of all files
            -- successfully removed across all diskservers
            SELECT messages.id, messages.hostid,
                   max(decode(params.name, 'FileAge',  params.value, NULL)) fileAge,
                   max(decode(params.name, 'FileSize', params.value, NULL)) fileSize
              FROM &dlfschema..dlf_messages messages,
                   &dlfschema..dlf_num_param_values params
             WHERE messages.id = params.id
               AND messages.severity = 8 -- Info
               AND messages.facility = 8 -- gcd
               AND (messages.msg_no = 11 OR -- Removed file successfully
                    messages.msg_no = 27 OR -- Deleting ... nameserver
                    messages.msg_no = 36)   -- Deleting ... stager catalog
               AND messages.timestamp >  now - 10/1440
               AND messages.timestamp <= now - 5/1440
               AND params.name IN ('FileAge', 'FileSize')
               AND params.timestamp >  now - 10/1440
               AND params.timestamp <= now - 5/1440
             GROUP BY messages.id, messages.hostid
          ) results
        -- Attach the gctye to the results collected above
        INNER JOIN &dlfschema..dlf_str_param_values params
           ON results.id = params.id
          AND params.name = 'GcType'
        -- Resolve the hostid to names
        INNER JOIN &dlfschema..dlf_host_map hosts
           ON results.hostid = hosts.hostid)
    GROUP BY diskserver, type;
END;
/


/* PL/SQL method implementing statsRequest
 *
 * Provides statistical information on the types of requests recorded by the
 * request handler, the total for all users and a break down of the top 10
 * users per request type
 */
CREATE OR REPLACE PROCEDURE statsRequest (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: RequestStats
  -- Frequency: 5 minutes
  INSERT INTO RequestStats
    (timestamp, interval, requestType, hostname, euid, requests)
    -- Gather data
    SELECT now - 5/1440 timestamp, interval, type, hostname, euid, requests
      FROM (
        -- For each request type display the top 10 users + the total number of
        -- requests
        SELECT type, hostname, euid, (requests / interval) requests,
               RANK() OVER (PARTITION BY type
                            ORDER BY requests DESC, euid ASC) rank
          FROM (
            -- Determine the number of requests made for each request type and
            -- per user over the last sampling period. This gives us a user
            -- level breakdown.
            SELECT str.value type, hosts.hostname,
                   nvl(to_char(num.value), '-') euid, count(*) requests
              FROM &dlfschema..dlf_messages messages,
                   &dlfschema..dlf_str_param_values str,
                   &dlfschema..dlf_host_map hosts,
                   &dlfschema..dlf_num_param_values num
             WHERE messages.id = str.id
               AND messages.id = num.id
               AND messages.severity = 8  -- Info
               AND messages.facility = 4  -- rhd
               AND messages.msg_no = 10   -- Reply sent to client
               AND messages.timestamp >  now - 10/1440
               AND messages.timestamp <= now - 5/1440
               AND str.name = 'Type'
               AND str.timestamp >  now - 10/1440
               AND str.timestamp <= now - 5/1440
               AND num.name = 'Euid'
               AND num.timestamp >  now - 10/1440
               AND num.timestamp <= now - 5/1440
               AND messages.hostid = hosts.hostid
             GROUP BY GROUPING SETS (str.value, num.value),
                                    (hosts.hostname, str.value))
      ) WHERE rank < 12;
END;
/


/* PL/SQL method implementing statsDiskCachEfficiency
 *
 * Provides an overview of how effectively the disk cache is performing.
 */
CREATE OR REPLACE PROCEDURE statsDiskCacheEfficiency (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: DiskCacheEfficiencyStats
  -- Frequency: 5 minutes

  -- Collect a list of request ids that we are interested in. We dump this list
  -- into a temporary table so that the execution plan of the query afterwards
  -- is optimized
  INSERT INTO CacheEfficiencyHelper
    SELECT messages.reqid
      FROM &dlfschema..dlf_messages messages
     WHERE messages.severity = 8  -- Info
       AND messages.facility = 4  -- rhd
       AND messages.msg_no = 10   -- Reply sent to client
       AND messages.timestamp >  now - 10/1440
       AND messages.timestamp <= now - 5/1440;

  -- Record results
  INSERT INTO DiskCacheEfficiencyStats
    (timestamp, interval, requestType, svcClass, wait, d2d, recall, staged,
     waitPerc, d2dPerc, recallPerc, stagedPerc, total)
    -- Gather data
    SELECT now - 5/1440 timestamp, interval, results.type, results.svcClass,
           (results.wait   / interval),
           (results.d2d    / interval),
           (results.recall / interval),
           (results.staged / interval),
           -- Percentages
           (results.wait   / results.total) * 100 waitPerc,
           (results.d2d    / results.total) * 100 d2dPerc,
           (results.recall / results.total) * 100 recallPerc,
           (results.staged / results.total) * 100 stagedPerc,
           (results.total) total
      FROM (
        SELECT type, svcClass,
               nvl(sum(decode(msg_no, 53, requests, 0)), 0) wait,
               nvl(sum(decode(msg_no, 56, requests, 0)), 0) d2d,
               nvl(sum(decode(msg_no, 57, requests, 0)), 0) recall,
               nvl(sum(decode(msg_no, 60, requests, 0)), 0) staged,
               nvl(sum(requests), 0) Total
          FROM (
            SELECT type, svcClass, msg_no, count(*) requests FROM (
              SELECT * FROM (
                -- Get the first message issued for all subrequests of interest.
                -- This will indicate to us whether the request was a hit or a
                --  miss
                SELECT msg_no, type, svcClass,
                       RANK() OVER (PARTITION BY subreqid
                              ORDER BY timestamp ASC, timeusec ASC) rank
                  FROM (
                    -- Extract all subrequests processed by the stager that
                    -- resulted in read type access
                    SELECT messages.reqid, messages.subreqid, messages.timestamp,
                           messages.timeusec, messages.msg_no,
                           max(decode(params.name, 'Type',     params.value, NULL)) type,
                           max(decode(params.name, 'SvcClass', params.value, 'default')) svcClass
                      FROM &dlfschema..dlf_messages messages,
                           &dlfschema..dlf_str_param_values params
                     WHERE messages.id = params.id
                       AND messages.severity = 8   -- Info
                       AND messages.facility = 22  -- stagerd
                       AND messages.msg_no IN (53, 56, 57, 60)
                       AND messages.timestamp >  now - 10/1440
                       AND messages.timestamp <= now - 3/1440
                       AND params.name IN ('Type', 'SvcClass')
                       AND params.timestamp >  now - 10/1440
                       AND params.timestamp <= now - 3/1440
                     GROUP BY messages.reqid, messages.subreqid, messages.timestamp,
                              messages.timeusec, messages.msg_no
                  ) results
                 -- Filter the subrequests so that we only process requests
                 -- which entered the system through the request handler in the
                 -- last sampling interval. This stops us from recounting
                 -- subrequests that were restarted
                 WHERE results.reqid IN
                   (SELECT /*+ CARDINALITY(helper 10000) */ *
                      FROM CacheEfficiencyHelper helper))
              ) WHERE rank = 1
             GROUP BY type, svcClass, msg_no)
         GROUP BY type, svcClass) results;
END;
/


/* PL/SQL method implementing statsMigratedFiles
 *
 * Provides statistical information on the number of files migrated to tape in
 * the last sampling interval broken down by service class and tape pool.
 */
CREATE OR REPLACE PROCEDURE statsMigratedFiles (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: FilesMigratedStats
  -- Frequency: 5 minutes
  INSERT INTO FilesMigratedStats
    (timestamp, interval, svcClass, tapePool, nbFiles, totalFileSize,
     numBytesWriteAvg)
    -- Gather data
    SELECT now - 5/1440 timestamp, interval, svcClass,
           nvl(tapepool, '-') tapepool, count(*) files,
           sum(fileSize) totalFileSize,
           (sum(fileSize) / interval) numBytesWriteAvg
      FROM (
        -- Extract the messages to indicate when a file has been migrated
        SELECT messages.id,
               max(decode(str.name, 'SVCCLASS', str.value, NULL)) svcClass,
               max(decode(str.name, 'TAPEPOOL', str.value, NULL)) tapepool,
               max(decode(num.name, 'FILESIZE', num.value, NULL)) fileSize
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_str_param_values str,
               &dlfschema..dlf_num_param_values num
         WHERE messages.id = str.id
           AND messages.id = num.id
           AND messages.severity = 8  -- Info
           AND messages.facility = 1  -- migrator
           AND messages.msg_no = 55   -- File staged
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND str.name IN ('SVCCLASS', 'TAPEPOOL')
           AND str.timestamp >  now - 10/1440
           AND str.timestamp <= now - 5/1440
           AND num.name = 'FILESIZE'
           AND num.timestamp >  now - 10/1440
           AND num.timestamp <= now - 5/1440
         GROUP BY messages.id)
     GROUP BY GROUPING SETS (svcClass, tapepool), (svcClass);
END;
/


/* PL/SQL method implementing statsRecalledFiles
 *
 * Provides statistical information on the number of files recalled from tape in
 * the last sampling interval.
 */
CREATE OR REPLACE PROCEDURE statsRecalledFiles (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: FilesRecalledStats
  -- Frequency: 5 minutes
  INSERT INTO FilesRecalledStats
    (timestamp, interval, nbFiles, totalFileSize, numBytesReadAvg)
    -- Gather data
    SELECT now - 5/1440 timestamp, interval, count(*) files,
           nvl(sum(num.value), 0) totalFileSize,
           nvl((sum(num.value) / interval), 0) numBytesReadAvg
      FROM &dlfschema..dlf_messages messages,
           &dlfschema..dlf_num_param_values num
     WHERE messages.id = num.id
       AND messages.severity = 8  -- Info
       AND messages.facility = 2  -- recaller
       AND messages.msg_no = 55   -- File staged
       AND messages.timestamp >  now - 10/1440
       AND messages.timestamp <= now - 5/1440
       AND num.name = 'FILESIZE'
       AND num.timestamp >  now - 10/1440
       AND num.timestamp <= now - 5/1440;
END;
/


/* PL/SQL method implementing statsReplication
 *
 * Provides statistical information on disk copy replication requests both
 * across service classes and internally within the same service class.
 */
CREATE OR REPLACE PROCEDURE statsReplication (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: ReplicationStats
  -- Frequency: 5 minutes
  INSERT INTO ReplicationStats
    (timestamp, interval, sourceSvcClass, destSvcClass, transferred, totalFileSize,
     minFileSize, maxFileSize, avgFileSize, stddevFileSize, medianFileSize)
    -- Gather data
    SELECT now - 5/1440 timestamp, interval, src, dest,
           (count(*) / interval) transferred,
           sum(params.value)        totalFileSize,
           min(params.value)        minFileSize,
           max(params.value)        maxFileSize,
           avg(params.value)        avgFileSize,
           stddev_pop(params.value) stddevFileSize,
           median(params.value)     medianFileSize
      FROM (
        SELECT params.id,
               substr(params.value, 0, instr(params.value, '->', 1) - 2) src,
               substr(params.value, instr(params.value, '->', 1) + 3) dest
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_str_param_values params
         WHERE messages.id = params.id
           AND messages.severity = 8   -- Info
           AND messages.facility = 23  -- d2dtransfer
           AND messages.msg_no = 39    -- DiskCopy Transfer successful
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.name = 'Direction'
           AND params.timestamp >  now - 10/1440
           AND params.timestamp <= now - 5/1440
      ) results
     -- Attach the size of the file to each replication request. As a result of
     -- this we will have one line per request detailing the direction of the
     -- transfer and the amount of data transferred
     INNER JOIN &dlfschema..dlf_num_param_values params
        ON results.id = params.id
       AND params.name = 'FileSize'
       AND params.timestamp >  now - 10/1440
       AND params.timestamp <= now - 5/1440
     GROUP BY src, dest;
END;
/


/* PL/SQL method implementing statsTapeRecalled
 *
 * Provides statistical information on who triggered a tape recall, how many
 * files were requested, the status of the tape as the request was processed
 * and the type of request that triggered the recall.
 */
CREATE OR REPLACE PROCEDURE statsTapeRecalled (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: TapeRecalledStats
  -- Frequency: 5 minutes

  -- Populate the TapeMountsHelper table with a line for every recaller that
  -- has been started in the last sampling interval. This will act as a summary
  -- table
  INSERT INTO TapeMountsHelper
    SELECT messages.timestamp, messages.facility, messages.tapevid
      FROM &dlfschema..dlf_messages messages
     WHERE messages.severity = 8  -- Info
       AND ((messages.facility = 2    -- recaller
       AND   messages.msg_no   = 13)  -- Recaller started
        OR  (messages.facility = 1    -- migrator
       AND   messages.msg_no   = 14)) -- Migrator started
       AND messages.subreqid <> '00000000-0000-0000-0000-000000000000'
       AND messages.timestamp >  now - 10/1440
       AND messages.timestamp <= now - 5/1440;

  -- Record results
  INSERT INTO TapeRecalledStats
    (timestamp, interval, requestType, username, groupname, tapeVID, tapeStatus,
     nbFiles, totalFileSize, mountsPerDay)
    SELECT now - 5/1440 timestamp, interval, type, username, groupname, results.tapevid,
           tapestatus, count(*) nbFiles, sum(params.value) totalFileSize,
           max(nvl(mounts.mounted, 0)) mounted
      FROM (
        -- Extract all requests from the stager which triggered a tape recall
        -- including the request type, username and groupname associated with
        -- that request
        SELECT messages.id, messages.tapevid,
               max(decode(params.name, 'Type',       params.value, NULL)) type,
               max(decode(params.name, 'Username',   params.value, NULL)) username,
               max(decode(params.name, 'Groupname',  params.value, NULL)) groupname,
               max(decode(params.name, 'TapeStatus', params.value, NULL)) tapestatus
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_str_param_values params
         WHERE messages.id = params.id
           AND messages.severity = 8   -- Info
           AND messages.facility = 22  -- stagerd
           AND messages.msg_no = 57    -- Triggering Tape Recall
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.name IN ('Type', 'Username', 'Groupname', 'TapeStatus')
           AND params.timestamp >  now - 10/1440
           AND params.timestamp <= now - 5/1440
         GROUP BY messages.id, messages.tapevid
      ) results
     -- Attach the file size to be recalled
     INNER JOIN &dlfschema..dlf_num_param_values params
        ON results.id = params.id
       AND params.name = 'FileSize'
       AND params.timestamp >  now - 10/1440
       AND params.timestamp <= now - 5/1440
     -- Attached the number of mounts which took place for the tape over the
     -- last 24 hours
     LEFT JOIN (
       SELECT helper.tapevid, count(*) mounted
         FROM TapeMountsHelper helper
        WHERE helper.timestamp > (now - 1) - 5/1440
          AND helper.facility = 2  -- recaller
        GROUP BY helper.tapevid) mounts
       ON results.tapevid = mounts.tapevid
     GROUP BY type, username, groupname, results.tapevid, tapestatus;
END;
/


/* PL/SQL method implementing statsProcessingTime
 *
 * Provides statistics on the processing time in seconds of requests in the
 * stagerd, rhd and SRM daemons
 */
CREATE OR REPLACE PROCEDURE statsProcessingTime (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: ProcessingTimeStats
  -- Frequency: 5 minutes
  INSERT INTO ProcessingTimeStats
    (timestamp, interval, daemon, requestType, svcClass, requests,
     minProcessingTime, maxProcessingTime, avgProcessingTime,
     stddevProcessingTime, medianProcessingTime)
    -- Gather data
    SELECT now - 5/1440 timestamp, interval, results.fac_name daemon,
           type.value type, nvl(svcClass.value, 'all') svcClass,
           (count(*) / interval) requests,
           min(elapsed)        minProcessingTime,
           max(elapsed)        maxProcessingTime,
           avg(elapsed)        avgProcessingTime,
           stddev_pop(elapsed) stddevProcessingTime,
           median(elapsed)     medianProcessingTime
      FROM (
        -- Extract all the processing times
        SELECT messages.id, facilities.fac_name, params.value elapsed
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_num_param_values params,
               &dlfschema..dlf_facilities facilities
         WHERE messages.id = params.id
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.timestamp >    now - 10/1440
           AND params.timestamp <=   now - 5/1440
           -- stagerd
           AND ((messages.severity = 8  -- Info
           AND   messages.facility = 22 -- stagerd
           AND   messages.msg_no = 25   -- Request processed
           AND   params.name = 'ProcessingTime')
           -- rhd
            OR  (messages.severity = 8  -- Info
           AND   messages.facility = 4  -- rhd
           AND   messages.msg_no = 10   -- Reply sent to client
           AND   params.name = 'ElapsedTime')
           -- SRMDaemon
            OR  (messages.facility = 14 -- SRMDaemon
           AND   messages.msg_no = 9    -- Processing complete
           AND   params.name = 'ElapsedTime'))
           -- Resolve facility name
           AND messages.facility = facilities.fac_no
      ) results
     -- Attach the request type
     INNER JOIN &dlfschema..dlf_str_param_values type
        ON results.id = type.id
       AND type.name = 'Type'
       AND type.timestamp >  now - 10/1440
       AND type.timestamp <= now - 5/1440
     -- Attach the service class. Note: it would have been nive to convert the
     -- previous statement to in IN clause but the execution plan is not good!
     INNER JOIN &dlfschema..dlf_str_param_values svcClass
        ON results.id = svcClass.id
       AND svcClass.name = 'SvcClass'
       AND svcClass.timestamp >  now - 10/1440
       AND svcClass.timestamp <= now - 5/1440
     GROUP BY results.fac_name, type.value, svcClass.value;
END;
/


/* PL/SQL method implementing statsClientVersion
 *
 * Provides statistics on the different client versions seen by the
 * RequestHandler
 */
CREATE OR REPLACE PROCEDURE statsClientVersion (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: ClientVersionStats
  -- Frequency: 5 minutes
  INSERT INTO ClientVersionStats
    (timestamp, interval, clientVersion, requests)
    SELECT now - 5/1440 timestamp, interval, clientVersion,
           (count(*) / interval) requests
      FROM (
        SELECT nvl(params.value, 'Unknown') clientVersion
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_str_param_values params
         WHERE messages.id = params.id
           AND messages.severity = 8  -- Info
           AND messages.facility = 4  -- rhd
           AND messages.msg_no = 10   -- Reply sent to client
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.name = 'ClientVersion'
           AND params.timestamp >  now - 10/1440
           AND params.timestamp <= now - 5/1440)
     GROUP BY clientVersion;
END;
/

/* PL/SQL method implementing statsTapeMounts
 *
 * Provides statistics on tape mounts for recalls and migrations
 */
CREATE OR REPLACE PROCEDURE statsTapeMounts (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: TapeMountStas
  -- Frequency: 5 minutes
  INSERT INTO TapeMountStats
    (timestamp, interval, direction, nbMounts, nbFiles, totalFileSize,
     avgRunTime, nbFilesPerMount, failures)
    SELECT now - 5/1440 timestamp, interval,
           decode(facility, 1, 'WRITE', 'READ') direction,
           count(*) nbMounts,
           sum(nbFiles) nbFiles,
           sum(totalFileSize) totalFileSize,
           avg(runTime) avgRunTime,
           (sum(nbFiles) / count(*)) nbFilesPerMount,
           sum(failure) failures
      FROM (
        SELECT messages.id, messages.facility,
               max(decode(params.name, 'FILESCP', params.value, NULL)) nbFiles,
               max(decode(params.name, 'BYTESCP', params.value, NULL)) totalFileSize,
               max(decode(params.name, 'RUNTIME', params.value, NULL)) runTime,
               max(decode(params.name, 'serrno',
                 decode(params.value, 0, 0,
                   -- Ignore serrno=28 (No space left on device)
                   decode(params.value, 28, 0, 1)), 0)) failure
          FROM &dlfschema..dlf_messages messages,
               &dlfschema..dlf_num_param_values params
         WHERE messages.id = params.id
           AND messages.severity = 8       -- Info
           AND ((messages.facility = 1     -- migrator
           AND   messages.msg_no   = 21)   -- migrator ended
            OR  (messages.facility = 2     -- recaller
           AND   messages.msg_no   = 20))  -- recaller ended
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.name IN ('FILESCP', 'BYTESCP', 'RUNTIME', 'serrno')
           AND params.timestamp >  now - 10/1440
           AND params.timestamp <= now - 5/1440
         GROUP BY messages.id, messages.facility)
      GROUP BY facility;
END;
/


/***** NEW MONITORING *****/

/* PL/SQL method implementing statsTotalLat */
CREATE OR REPLACE PROCEDURE statsTotalLat(now IN DATE)
AS
BEGIN
  -- Extract total latency info from job started summary message
  INSERT INTO TotalLatency (subReqId, timestamp, nsfileid, totalLatency)
    (SELECT DISTINCT a.subReqId, a.timestamp, a.nsfileid, b.value
       FROM &dlfschema..dlf_messages a, &dlfschema..dlf_num_param_values b
      WHERE a.id = b.id
        AND a.facility = 26  -- stagerjob
        AND a.msg_no = 20    -- Job Started
        AND a.timestamp >= now
        AND b.timestamp >= now
        AND a.timestamp < now + 5/1440
        AND b.timestamp < now + 5/1440)
  LOG ERRORS INTO Err_Latency REJECT LIMIT 100000;
END;
/


/* PL/SQL method implementing statsDiskCopy */
CREATE OR REPLACE PROCEDURE statsDiskCopy(now IN DATE)
AS
BEGIN
  -- Info about external file replication: source - target SvcClass
  INSERT INTO DiskCopy
    (nsfileid, timestamp, originalPool, targetPool, srcHost, destHost)
    SELECT * FROM (
      SELECT a.nsfileid nsfileid,
             min(a.timestamp) timestamp,
             max(decode(b.name, 'Direction',
                        substr(b.value, 0, instr(b.value, '->', 1) - 2),
                        NULL)) src,
             max(decode(b.name, 'Direction',
                        substr(b.value, instr(b.value, '->', 1) + 3),
                        NULL)) dest,
             max(decode(b.name, 'SourcePath',
                        substr(b.value, 0, instr(b.value, ':', 1) - 1),
                        NULL)) src_host,
             max(decode(b.name, 'SourcePath', c.hostname, NULL)) dest_host
        FROM &dlfschema..dlf_messages a, &dlfschema..dlf_str_param_values b,
             &dlfschema..dlf_host_map c
       WHERE a.id = b.id
         AND a.facility = 23       -- d2dtransfer
         AND a.msg_no IN (39, 28)  -- DiskCopy Transfer Successful, Transfer information
         AND a.hostid = c.hostid
         AND a.timestamp >= now
         AND b.timestamp >= now
         AND a.timestamp < now + 5/1440
         AND b.timestamp < now + 5/1440
       GROUP BY nsfileid
    ) temp
   WHERE temp.src <> temp.dest
     AND temp.dest IS NOT NULL
     AND temp.src IS NOT NULL
     AND temp.dest_host IS NOT NULL
     AND temp.src_host IS NOT NULL;
END;
/


/* PL/SQL method implementing statsInternalDiskCopy */
CREATE OR REPLACE PROCEDURE statsInternalDiskCopy(now IN DATE)
AS
BEGIN
  -- Info about external file replication: source - target SvcClass
  INSERT ALL INTO InternalDiskCopy (timestamp, svcclass, copies)
  VALUES (now, src, copies)
    SELECT src, count(*) copies
     FROM (
       SELECT a.nsfileid nsfileid,
              substr(b.value, 0, instr(b.value, '->', 1) - 2) src,
              substr(b.value, instr(b.value, '->', 1) + 3) dest
         FROM &dlfschema..dlf_messages a, &dlfschema..dlf_str_param_values b
        WHERE a.id = b.id
          AND a.facility = 23  -- d2dtransfer
          AND a.msg_no = 39    -- DiskCopy Transfer Successful
          AND a.timestamp >= now
          AND b.timestamp >= now
          AND a.timestamp < now + 5/1440
          AND b.timestamp < now + 5/1440
     ) temp
    WHERE temp.src = temp.dest
    GROUP BY src;
END;
/


/* PL/SQL method implementing statsGCFiles */
CREATE OR REPLACE PROCEDURE statsGCFiles(now IN DATE)
AS
BEGIN
  INSERT INTO GcFiles
    (timestamp, nsfileid, fileSize, fileAge, lastAccessTime, nbAccesses,
     gcType, svcclass)
    SELECT mes.timestamp, mes.nsfileid,
           max(decode(num.name, 'FileSize', num.value, NULL)) fileSize,
           max(decode(num.name, 'FileAge', num.value, NULL)) fileAge,
           max(decode(num.name, 'LastAccessTime', num.value, NULL)) lastAccessTime,
           max(decode(num.name, 'NbAccesses', num.value, NULL)) nb_accesses,
           max(decode(str.name, 'GcType', str.value, NULL)) gcType,
           max(decode(str.name, 'SvcClass', str.value, NULL)) svcclass
      FROM &dlfschema..dlf_messages mes,
           &dlfschema..dlf_num_param_values num,
           &dlfschema..dlf_str_param_values str
     WHERE mes.facility = 8  -- gcd
       AND mes.msg_no = 11   -- Removed file successfully
       AND mes.id = num.id
       AND num.id = str.id
       AND num.name IN ('FileSize', 'FileAge', 'LastAccessTime', 'NbAccesses')
       AND str.name IN ('SvcClass', 'GcType')
       AND mes.timestamp >= now
       AND num.timestamp >= now
       AND str.timestamp >= now
       AND mes.timestamp < now + 5/1440
       AND num.timestamp < now + 5/1440
       AND str.timestamp < now + 5/1440
     GROUP BY mes.timestamp, mes.nsfileid;
END;
/


/* PL/SQL method implementing statsTapeRecall */
CREATE OR REPLACE PROCEDURE statsTapeRecall(now IN DATE)
AS
BEGIN
  -- Info about tape recalls: Tape Volume Id - Tape Status
  INSERT INTO TapeRecall (timestamp, subReqId, tapeId, tapeMountState)
    (SELECT mes.timestamp, mes.subreqid, mes.tapevid, str.value
       FROM &dlfschema..dlf_messages mes, &dlfschema..dlf_str_param_values str
      WHERE mes.id = str.id
        AND mes.facility = 22  -- stagerd
        AND mes.msg_no = 57    -- Triggering Tape Recall
        AND str.name = 'TapeStatus'
        AND mes.timestamp >= now
        AND str.timestamp >= now
        AND mes.timestamp < now + 5/1440
        AND str.timestamp < now + 5/1440)
  LOG ERRORS INTO Err_Taperecall REJECT LIMIT 100000;
  -- Insert info about size of recalled file
  FOR a IN (SELECT mes.subreqid, num.value
              FROM &dlfschema..dlf_messages mes, &dlfschema..dlf_num_param_values num
             WHERE mes.id = num.id
               AND mes.facility = 22  -- stagerd
               AND mes.msg_no = 57    -- Triggering Tape Recall
               AND num.name = 'FileSize'
               AND mes.timestamp >= now
               AND num.timestamp >= now
               AND mes.timestamp < now + 5/1440
               AND num.timestamp < now + 5/1440)
  LOOP
    UPDATE Requests
       SET filesize = to_number(a.value)
     WHERE subreqid = a.subReqId;
  END LOOP;
END;
/


/* PL/SQL method implementing statsMigs */
CREATE OR REPLACE PROCEDURE statsMigs(now IN DATE)
AS
BEGIN
  INSERT INTO Migration
    (timestamp, reqid, subreqid, nsfileid, type, svcclass, username, filename)
    SELECT mes.timestamp,mes.reqid,
           mes.subreqid, mes.nsfileid,
           max(decode(str.name, 'Type',     str.value, NULL)) type,
           max(decode(str.name, 'SvcClass', str.value, NULL)) svcclass,
           max(decode(str.name, 'Username', str.value, NULL)) username,
           max(decode(str.name, 'Filename', str.value, NULL)) filename
      FROM &dlfschema..dlf_messages mes, &dlfschema..dlf_str_param_values str
     WHERE mes.id = str.id
       AND str.id IN
         (SELECT id
            FROM &dlfschema..dlf_str_param_values
           WHERE name = 'Type'
             AND value LIKE 'Stage%'
             AND timestamp >= now
             AND timestamp < now + 5/1440)
       AND mes.facility = 22  -- stagerd
       AND mes.msg_no = 58    -- Recreating CastorFile
       AND str.name IN ('SvcClass', 'Username', 'Groupname', 'Type', 'Filename')
       AND mes.timestamp >= now
       AND mes.timestamp < now + 5/1440
       AND str.timestamp >= now
       AND str.timestamp < now + 5/1440
     GROUP BY mes.timestamp, mes.id, mes.reqid, mes.subreqid, mes.nsfileid
     ORDER BY mes.timestamp
  LOG ERRORS INTO Err_Migration REJECT LIMIT 100000;
END;
/


/* PL/SQL method implementing statsReqs */
CREATE OR REPLACE PROCEDURE statsReqs(now IN DATE)
AS
BEGIN
  INSERT INTO Requests
    (timestamp, reqId, subReqId, nsfileid, type, svcclass, username, state, filename)
    SELECT mes.timestamp, mes.reqid, mes.subreqid, mes.nsfileid,
           max(decode(str.name, 'Type', str.value, NULL)) type,
           max(decode(str.name, 'SvcClass', str.value, NULL)) svcclass,
           max(decode(str.name, 'Username', str.value, NULL)) username,
           max(decode(mes.msg_no, 60, 'DiskHit', 56, 'DiskCopy', 57, 'TapeRecall', NULL)) state,
           max(decode(str.name, 'Filename', str.value, NULL)) filename
      FROM &dlfschema..dlf_messages mes, &dlfschema..dlf_str_param_values str
     WHERE mes.id = str.id
       AND str.id IN
         (SELECT id
            FROM &dlfschema..dlf_str_param_values
           WHERE name = 'Type'
             AND value LIKE 'Stage%'
             AND timestamp >= now
             AND timestamp < now + 5/1440)
       AND mes.facility = 22           -- stagerd
       AND mes.msg_no IN (56, 57, 60)  -- Triggering internal DiskCopy replication, Triggering Tape Recall, Diskcopy available, scheduling job
       AND str.name IN ('SvcClass', 'Username', 'Groupname', 'Type', 'Filename')
       AND mes.timestamp >= now
       AND mes.timestamp < now + 5/1440
       AND str.timestamp >= now
       AND str.timestamp < now + 5/1440
     GROUP BY mes.timestamp, mes.id ,mes.reqid, mes.subreqid, mes.nsfileid
     ORDER BY mes.timestamp
  LOG ERRORS INTO Err_Requests REJECT LIMIT 1000000;
END;
/


/***** COMMON CODE *****/

/* PL/SQL method implementing createPartition */
CREATE OR REPLACE PROCEDURE createPartitions
AS
  username VARCHAR2(2048);
  partitionMax NUMBER;
  tableSpaceName VARCHAR2(2048);
  highValue DATE;
  cnt NUMBER;
BEGIN
  -- Set the nls_date_format
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = "DD-MON-YYYY"';

  -- Extract the name of the current user running the PL/SQL procedure. This
  -- name will be used within the tablespace names.
  SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER')
    INTO username
    FROM dual;

  -- Loop over all partitioned tables
  FOR a IN (SELECT DISTINCT(table_name)
              FROM user_tab_partitions
             ORDER BY table_name)
  LOOP
    -- Determine the high value on which to split the MAX_VALUE partition of
    -- all tables
    SELECT max(substr(partition_name, 3, 10))
      INTO partitionMax
      FROM user_tab_partitions
     WHERE partition_name <> 'MAX_VALUE'
       AND table_name = a.table_name;

    partitionMax := TO_NUMBER(
      TO_CHAR(TRUNC(TO_DATE(partitionMax, 'YYYYMMDD') + 1), 'YYYYMMDD'));

    -- If this is the first execution there will be no high value, so set it to
    -- today
    IF partitionMax IS NULL THEN
      partitionMax := TO_NUMBER(TO_CHAR(SYSDATE, 'YYYYMMDD'));
    END IF;

    -- Create partition
    FOR b IN (SELECT TO_DATE(partitionMax, 'YYYYMMDD') + rownum - 1 value
                FROM all_objects
               WHERE rownum <=
                     TO_DATE(TO_CHAR(SYSDATE + 7, 'YYYYMMDD'), 'YYYYMMDD') -
                     TO_DATE(partitionMax, 'YYYYMMDD') + 1)
    LOOP
      -- To improve data management each daily partition has its own tablespace
      -- http://www.oracle.com/technology/oramag/oracle/06-sep/o56partition.html

      -- Check if a new tablespace is required before creating the partition
      tableSpaceName := 'MON_'||TO_CHAR(b.value, 'YYYYMMDD')||'_'||username;
      SELECT count(*) INTO cnt
        FROM user_tablespaces
       WHERE tablespace_name = tableSpaceName;

      IF cnt = 0 THEN
        EXECUTE IMMEDIATE 'CREATE TABLESPACE '||tableSpaceName||'
                           DATAFILE SIZE 100M
                           AUTOEXTEND ON NEXT 200M
                           MAXSIZE 30G
                           EXTENT MANAGEMENT LOCAL
                           SEGMENT SPACE MANAGEMENT AUTO';
      END IF;

      -- If the tablespace is read only, alter its status to read write for this
      -- operation.
      FOR d IN (SELECT tablespace_name FROM user_tablespaces
                 WHERE tablespace_name = tableSpaceName
                   AND status = 'READ ONLY')
      LOOP
        EXECUTE IMMEDIATE 'ALTER TABLESPACE '||d.tablespace_name||' READ WRITE';
      END LOOP;

      highValue := TRUNC(b.value + 1);
      EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                         SPLIT PARTITION MAX_VALUE
                         AT    ('''||TO_CHAR(highValue, 'DD-MON-YYYY')||''')
                         INTO  (PARTITION P_'||TO_CHAR(b.value, 'YYYYMMDD')||'
                                TABLESPACE '||tableSpaceName||',
                                PARTITION MAX_VALUE)
                         UPDATE INDEXES';

      -- Move indexes to the correct tablespace
      FOR c IN (SELECT index_name
                  FROM user_indexes
                 WHERE table_name = a.table_name
                   AND partitioned = 'YES')
      LOOP
        EXECUTE IMMEDIATE 'ALTER INDEX '||c.index_name||'
                           REBUILD PARTITION P_'||TO_CHAR(b.value, 'YYYYMMDD')||'
                           TABLESPACE '||tableSpaceName;
      END LOOP;
    END LOOP;
  END LOOP;
END;
/


/* PL/SQL method implementing archiveData */
CREATE OR REPLACE PROCEDURE archiveData (expiry IN NUMBER)
AS
  username VARCHAR2(2048);
  expiryTime NUMBER;
BEGIN
  -- Set the nls_date_format
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = "DD-MON-YYYY"';

  -- Extract the name of the current user running the PL/SQL procedure. This
  -- name will be used within the tablespace names.
  SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER')
    INTO username
    FROM dual;

  -- Extract configurable expiry time
  expiryTime := expiry;
  IF expiryTime = -1 THEN
    SELECT expiry INTO expiryTime
      FROM ConfigSchema;
  END IF;

  -- Drop partitions across all tables
  FOR a IN (SELECT *
              FROM user_tab_partitions
             WHERE partition_name < concat('P_', TO_CHAR(SYSDATE - expiryTime, 'YYYYMMDD'))
               AND partition_name <> 'MAX_VALUE'
             ORDER BY partition_name DESC)
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                       DROP PARTITION '||a.partition_name;
  END LOOP;

  -- Drop tablespaces
  FOR a IN (SELECT tablespace_name
              FROM user_tablespaces
             WHERE status <> 'OFFLINE'
               AND tablespace_name LIKE 'MON_%_'||username
               AND tablespace_name < 'MON_'||TO_CHAR(SYSDATE - expiryTime, 'YYYYMMDD')||'_'||username
             ORDER BY tablespace_name ASC)
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||a.tablespace_name||' OFFLINE';
    EXECUTE IMMEDIATE 'DROP TABLESPACE '||a.tablespace_name||'
                       INCLUDING CONTENTS AND DATAFILES';
  END LOOP;

  -- Set the status of tablespaces older then 2 days to read only.
  FOR a IN (SELECT tablespace_name
              FROM user_tablespaces
             WHERE status <> 'OFFLINE'
               AND status <> 'READ ONLY'
               AND tablespace_name LIKE 'MON_%_'||username
               AND tablespace_name < 'MON_'||TO_CHAR(SYSDATE - 2, 'YYYYMMDD')||'_'||username
             ORDER BY tablespace_name ASC)
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||a.tablespace_name||' READ ONLY';
  END LOOP;
END;
/


/* Scheduler jobs */
BEGIN
  -- Remove scheduler jobs before recreation
  FOR a IN (SELECT job_name FROM user_scheduler_jobs)
  LOOP
    DBMS_SCHEDULER.DROP_JOB(a.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every day and create new partitions
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'partitionCreationJob',
      JOB_TYPE        => 'STORED_PROCEDURE',
      JOB_ACTION      => 'createPartitions',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => TRUNC(SYSDATE) + 1/24,
      REPEAT_INTERVAL => 'FREQ=DAILY',
      ENABLED         => TRUE,
      COMMENTS        => 'Daily partitioning creation');

  -- Create a db job to be run every day and drop old data from the database
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'archiveDataJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN
                            archiveData(-1);
                            FOR a IN (SELECT table_name FROM user_tables
                                       WHERE table_name LIKE ''ERR_%'')
                            LOOP
                              EXECUTE IMMEDIATE ''TRUNCATE TABLE ''||a.table_name;
                            END LOOP;
                          END;',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => TRUNC(SYSDATE) + 2/24,
      REPEAT_INTERVAL => 'FREQ=DAILY',
      ENABLED         => TRUE,
      COMMENTS        => 'Daily data archiving');

  -- Create a job to execute the procedures that create statistical information (OLD)
  DBMS_SCHEDULER.CREATE_JOB (
      JOB_NAME        => 'statisticJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'DECLARE
                            now DATE := SYSDATE;
                            interval NUMBER := 300;
                          BEGIN
                            statsLatency(now, interval);
                            statsQueueTime(now, interval);
                            statsGarbageCollection(now, interval);
                            statsRequest(now, interval);
                            statsDiskCacheEfficiency(now, interval);
                            statsMigratedFiles(now, interval);
                            statsRecalledFiles(now, interval);
                            statsReplication(now, interval);
                            statsTapeRecalled(now, interval);
                            statsProcessingTime(now, interval);
                            statsClientVersion(now, interval);
                            statsTapeMounts(now, interval);
                          END;',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'CASTOR2 Monitoring Statistics (OLD) (5 Minute Frequency)');

  -- Create a job to execute the procedures that create statistical information (NEW)
  DBMS_SCHEDULER.CREATE_JOB (
      JOB_NAME        => 'populateJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'DECLARE
                            now DATE := SYSDATE - 10/1440;
                          BEGIN
                            EXECUTE IMMEDIATE ''TRUNCATE TABLE ERR_Requests'';
                            statsReqs(now);
                            statsDiskCopy(now);
                            statsInternalDiskCopy(now);
                            statsTapeRecall(now);
                            statsMigs(now);
                            statsTotalLat(now);
                            statsGcFiles(now);
                          END;',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => SYSDATE + 10/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'CASTOR2 Monitoring Statistics (NEW) (5 Minute Frequency)');

END;
/


/* Trigger the initial creation of partitions */
BEGIN
  createPartitions();
END;
/
