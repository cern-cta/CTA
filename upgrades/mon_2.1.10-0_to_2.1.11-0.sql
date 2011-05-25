/******************************************************************************
 *              mon_2.1.10-0_to_2.1.11-0.sql
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
 * This script upgrades a CASTOR v2.1.10-0 MON database to v2.1.11-0
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  -- If we've encountered an error rollback any previously non committed
  -- operations. This prevents the UPDATE of the UpgradeLog from committing
  -- inconsistent data to the database.
  ROLLBACK;
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_9_7'
     AND release = '2_1_11_0'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'MON'
     AND release LIKE '2_1_10_0%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the MON before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_9_7', '2_1_11_0', 'NON TRANSPARENT');
COMMIT;

/* Schema changes go here */
/**************************/

/* Stop and delete all database jobs */
BEGIN
  FOR a IN (SELECT job_name, state FROM user_scheduler_jobs)
  LOOP
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name, force=>TRUE);
    END IF;
    dbms_scheduler.drop_job(a.job_name);
  END LOOP;
END;
/

/* Flag all READ ONLY tablespaces for READ WRITE to allow for data DML
 * operations.
 */
DECLARE
  username VARCHAR2(30) := SYS_CONTEXT('USERENV', 'CURRENT_USER');
BEGIN
  FOR a IN (SELECT tablespace_name FROM user_tablespaces
             WHERE tablespace_name LIKE 'MON_%_'||username
               AND status = 'READ ONLY')
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||a.tablespace_name||' READ WRITE';
  END LOOP;
END;
/

/* Rename all partitioned tables to <table_name>_OLD. The contents will be
 * copies to the new tables with new partitioning schema later. We could have
 * used MERGE partition logic for all of this but the data is so small that we
 * just copy it!
 */
BEGIN
  FOR a IN (SELECT table_name FROM user_tables
             WHERE partitioned = 'YES')
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                       RENAME TO '||a.table_name||'_OLD';
  END LOOP;
END;
/

/* Handle indexes and constraints associated to the _OLD tables */
BEGIN
  /* Indexes */
  FOR a IN (SELECT index_name, uniqueness FROM user_indexes
             WHERE table_name LIKE '%_OLD')
  LOOP
    IF a.uniqueness = 'UNIQUE' THEN
      EXECUTE IMMEDIATE 'ALTER INDEX '||a.index_name||'
                         RENAME TO '||a.index_name||'_OLD';
    ELSE
      EXECUTE IMMEDIATE 'DROP INDEX '||a.index_name;
    END IF;
  END LOOP;
  /* Constraints */
  FOR a IN (SELECT table_name, constraint_name FROM user_constraints
             WHERE table_name LIKE '%_OLD')
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                       DROP CONSTRAINT '||a.constraint_name;
  END LOOP;
END;
/

/* Create the new tables */

/* SQL statement for table LatencyStats */
CREATE TABLE LatencyStats (timestamp DATE CONSTRAINT NN_LatencyStats_ts NOT NULL, interval NUMBER, requestType VARCHAR2(255), svcClass VARCHAR2(255), protocol VARCHAR2(255), started NUMBER(*,3), minLatencyTime NUMBER(*,3), maxLatencyTime NUMBER(*,3), avgLatencyTime NUMBER(*,3), stddevLatencyTime NUMBER(*,3), medianLatencyTime NUMBER(*,3))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_LatencyStats_ts ON LatencyStats (timestamp) LOCAL;

/* SQL statement for table QueueTimeStats */
CREATE TABLE QueueTimeStats (timestamp DATE CONSTRAINT NN_QueueTimeStats_ts NOT NULL, interval NUMBER, requestType VARCHAR2(255), svcClass VARCHAR2(255), dispatched NUMBER(*,3), minQueueTime NUMBER(*,3), maxQueueTime NUMBER(*,3), avgQueueTime NUMBER(*,3), stddevQueueTime NUMBER(*,3), medianQueueTime NUMBER(*,3))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_QueueTimeStats_ts ON QueueTimeStats (timestamp) LOCAL;

/* SQL statement for table GarbageCollectionStats */
CREATE TABLE GarbageCollectionStats (timestamp DATE CONSTRAINT NN_GarbageCollectionStats_ts NOT NULL, interval NUMBER, diskserver VARCHAR2(255), requestType VARCHAR2(255), deleted NUMBER(*,3), totalFileSize NUMBER, minFileAge NUMBER(*,0), maxFileAge NUMBER(*,0), avgFileAge NUMBER(*,0), stddevFileAge NUMBER(*,0), medianFileAge NUMBER(*,0), minFileSize NUMBER(*,0), maxFileSize NUMBER(*,0), avgFileSize NUMBER(*,0), stddevFileSize NUMBER(*,0), medianFileSize NUMBER(*,0))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_GarbageCollectionStats_ts ON GarbageCollectionStats (timestamp) LOCAL;

/* SQL statement for table RequestStats */
CREATE TABLE RequestStats (timestamp DATE CONSTRAINT NN_RequestStats_ts NOT NULL, interval NUMBER, requestType VARCHAR2(255), hostname VARCHAR2(255), euid VARCHAR2(255), requests NUMBER(*,3))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_RequestStats_ts ON RequestStats (timestamp) LOCAL;

/* SQL statement for table DiskCacheEfficiencyStats */
CREATE TABLE DiskCacheEfficiencyStats (timestamp DATE CONSTRAINT NN_DiskCacheEfficiencyStats_ts NOT NULL, interval NUMBER, requestType VARCHAR2(255), svcClass VARCHAR2(255), wait NUMBER(*,3), d2d NUMBER(*,3), recall NUMBER(*,3), staged NUMBER(*,3), waitPerc NUMBER(*,2), d2dPerc NUMBER(*,2), recallPerc NUMBER(*,2), stagedPerc NUMBER(*,2), total NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_DiskCacheEfficiencyStats_ts ON DiskCacheEfficiencyStats (timestamp) LOCAL;

/* SQL statement for table FilesMigratedStats */
CREATE TABLE FilesMigratedStats (timestamp DATE CONSTRAINT NN_FilesMigratedStats_ts NOT NULL, interval NUMBER, svcClass VARCHAR2(255), tapepool VARCHAR2(255), nbFiles NUMBER, totalFileSize NUMBER, NumBytesWriteAvg NUMBER(*,2))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_FilesMigratedStats_ts ON FilesMigratedStats (timestamp) LOCAL;

/* SQL statement for table FilesRecalledStats */
CREATE TABLE FilesRecalledStats (timestamp DATE CONSTRAINT NN_FilesRecalledStats_ts NOT NULL, interval NUMBER, nbFiles NUMBER, totalFileSize NUMBER, NumBytesReadAvg NUMBER(*,2))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_FilesRecalledStats_ts ON FilesRecalledStats (timestamp) LOCAL;

/* SQL statement for table ReplicationStats */
CREATE TABLE ReplicationStats (timestamp DATE CONSTRAINT NN_ReplicationStats_ts NOT NULL, interval NUMBER, sourceSvcClass VARCHAR2(255), destSvcClass VARCHAR2(255), transferred NUMBER(*,3), totalFileSize NUMBER, minFileSize NUMBER(*,0), maxFileSize NUMBER(*,0), avgFileSize NUMBER(*,0), stddevFileSize NUMBER(*,0), medianFileSize NUMBER(*,0))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_ReplicationStats_ts ON ReplicationStats (timestamp) LOCAL;

/* SQL statement for table TapeRecalledStats */
CREATE TABLE TapeRecalledStats (timestamp DATE CONSTRAINT NN_TapeRecalledStats_ts NOT NULL, interval NUMBER, requestType VARCHAR2(255), username VARCHAR2(255), groupname VARCHAR2(255), tapeVid VARCHAR2(255), tapeStatus VARCHAR2(255), nbFiles NUMBER, totalFileSize NUMBER, mountsPerDay NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_TapeRecalledStats_ts ON TapeRecalledStats (timestamp) LOCAL;

/* SQL statement for table ProcessingTimeStats */
CREATE TABLE ProcessingTimeStats (timestamp DATE CONSTRAINT NN_ProcessingTimeStats_ts NOT NULL, interval NUMBER, daemon VARCHAR2(255), requestType VARCHAR2(255), svcClass VARCHAR2(255), requests NUMBER(*,3), minProcessingTime NUMBER(*,3), maxProcessingTime NUMBER(*,3), avgProcessingTime NUMBER(*,3), stddevProcessingTime NUMBER(*,3), medianProcessingTime NUMBER(*,3))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_ProcessingTimeStats_ts ON ProcessingTimeStats (timestamp) LOCAL;

/* SQL statement for table ClientVersionStats */
CREATE TABLE ClientVersionStats (timestamp DATE CONSTRAINT NN_ClientVersionStats_ts NOT NULL, interval NUMBER, clientVersion VARCHAR2(255), requests NUMBER(*,3))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_ClientVersionStats_ts ON ClientVersionStats (timestamp) LOCAL;

/* SQL statement for table TapeMountStats */
CREATE TABLE TapeMountStats (timestamp DATE CONSTRAINT NN_TapeMountStats_ts NOT NULL, interval NUMBER, direction VARCHAR2(20), nbMounts NUMBER, nbFiles NUMBER, totalFileSize NUMBER, avgRunTime NUMBER(*,3), nbFilesPerMount NUMBER(*,0), failures NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_TapeMountStats_ts ON TapeMountStats (timestamp) LOCAL;

/* SQL statement for table Top10Errors */
CREATE TABLE Top10Errors (timestamp DATE CONSTRAINT NN_TopTenErrors_ts NOT NULL, interval NUMBER, daemon VARCHAR2(255), nbErrors NUMBER, errorMessage VARCHAR2(512))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_Top10Errors_ts ON Top10Errors (timestamp) LOCAL;

/* SQL statement for table Requests */
CREATE TABLE Requests (subReqId CHAR(36) CONSTRAINT NN_Requests_subReqId NOT NULL CONSTRAINT PK_Requests_subReqId PRIMARY KEY, timestamp DATE CONSTRAINT NN_Requests_timestamp NOT NULL, reqId CHAR(36) CONSTRAINT NN_Requests_reqId NOT NULL, nsFileId NUMBER CONSTRAINT NN_Requests_nsFileId NOT NULL, type VARCHAR2(255), svcclass VARCHAR2(255), username VARCHAR2(255), state VARCHAR2(255), filename VARCHAR2(2048), filesize NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the Requests table */
CREATE INDEX I_Requests_ts       ON Requests (timestamp) LOCAL;
CREATE INDEX I_Requests_reqId    ON Requests (reqId) LOCAL;
CREATE INDEX I_Requests_svcclass ON Requests (svcclass) LOCAL;
CREATE INDEX I_Requests_filesize ON Requests (filesize) LOCAL;

/* SQL statement for table InternalDiskCopy */
CREATE TABLE InternalDiskCopy (timestamp DATE CONSTRAINT NN_InternalDiskCopy_ts NOT NULL, svcclass VARCHAR2(255), copies NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table TotalLatency */
CREATE TABLE TotalLatency (subReqId CHAR(36) CONSTRAINT NN_TotalLatency_subReqId NOT NULL CONSTRAINT PK_TotalLatency_subReqId PRIMARY KEY, timestamp DATE CONSTRAINT NN_TotalLatency_ts NOT NULL, nsFileId NUMBER CONSTRAINT NN_TotalLatency_nsFileId NOT NULL, totalLatency NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the TotalLatency table */
CREATE INDEX I_TotalLatency_ts           ON TotalLatency (timestamp) LOCAL;
CREATE INDEX I_TotalLatency_totalLatency ON TotalLatency (totalLatency) LOCAL;

/* SQL statement for table TapeRecall */
CREATE TABLE TapeRecall (subReqId CHAR(36) CONSTRAINT NN_TapeRecall_subReqId NOT NULL CONSTRAINT PK_TapeRecall_subReqId PRIMARY KEY, timestamp DATE CONSTRAINT NN_TapeRecall_ts NOT NULL, tapeId VARCHAR2(255 BYTE), tapeMountState VARCHAR2(255 BYTE), readLatency INTEGER, copyLatency INTEGER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the TapeRecall table */
CREATE INDEX I_TapeRecall_ts ON TapeRecall (timestamp) LOCAL;

/* SQL statement for table DiskCopy */
CREATE TABLE DiskCopy (nsFileId NUMBER CONSTRAINT NN_DiskCopy_nsFileId NOT NULL, timestamp DATE CONSTRAINT NN_DiskCopy_ts NOT NULL, originalPool VARCHAR2(255), targetPool VARCHAR2(255), readLatency INTEGER, copyLatency INTEGER, numCopiesInPools INTEGER, srcHost VARCHAR2(255), destHost VARCHAR2(255))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table GCFiles */
CREATE TABLE GcFiles (timestamp DATE CONSTRAINT NN_GCFiles_ts NOT NULL, nsFileId NUMBER CONSTRAINT NN_GCFiles_nsFileId NOT NULL, fileSize NUMBER, fileAge NUMBER, lastAccessTime NUMBER, nbAccesses NUMBER, gcType VARCHAR2(255), svcClass VARCHAR2(255))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the GcFiles table */
CREATE INDEX I_GcFiles_ts       ON GcFiles (timestamp) LOCAL;
CREATE INDEX I_GcFiles_filesize ON GcFiles (filesize) LOCAL;
CREATE INDEX I_GcFiles_fileage  ON GcFiles (fileage) LOCAL;

/* SQL statement for table Migration */
CREATE TABLE Migration (subReqId CHAR(36) CONSTRAINT NN_Migration_subReqId NOT NULL CONSTRAINT PK_Migration_subReqId PRIMARY KEY, timestamp DATE CONSTRAINT NN_Migration_ts NOT NULL, reqId CHAR(36) CONSTRAINT NN_Migration_reqId NOT NULL, nsfileid NUMBER CONSTRAINT NN_Migration_nsFileId NOT NULL, type VARCHAR2(255), svcclass VARCHAR2(255), username VARCHAR2(255), state VARCHAR2(255), filename VARCHAR2(2048), totalLatency NUMBER, filesize NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the Migration table */
CREATE INDEX I_Migration_ts    ON Migration (timestamp) LOCAL;
CREATE INDEX I_Migration_reqId ON Migration (reqId) LOCAL;

/* SQL statement for table TapeMountsHelper */
CREATE TABLE TapeMountsHelper (timestamp DATE CONSTRAINT NN_TapeMountsHelper_ts NOT NULL, facility NUMBER, tapevid VARCHAR2(20))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statements for indexes on the TapeMountsHelper table */
CREATE INDEX I_TapeMountsHelper_ts ON TapeMountsHelper (timestamp) LOCAL;

/* Partition the new tables using the new partitioning schema */
DECLARE
  username         VARCHAR2(30) := SYS_CONTEXT('USERENV', 'CURRENT_USER');
  tableSpaceName   VARCHAR2(30);
  tableSpaceStatus VARCHAR2(9);
  partitionName    VARCHAR2(30);
  oldest           DATE;
  nbMonths         NUMBER;
BEGIN
  -- Loop over all new partitioned tables
  FOR a IN (SELECT table_name FROM user_tables
             WHERE partitioned = 'YES'
               AND table_name NOT LIKE '%_OLD')
  LOOP
    -- Get the oldest partition date associated to the tables _OLD counterpart.
    SELECT to_date(min(substr(partition_name, 3)), 'YYYYMMDD') INTO oldest
      FROM user_tab_partitions
     WHERE table_name = a.table_name||'_OLD'
       AND partition_name != 'MAX_VALUE';     

    SELECT months_between(SYSDATE, oldest) INTO nbMonths
      FROM dual;

    -- Generate a list of the first day of every month for the next 7 months.
    FOR b IN (SELECT ADD_MONTHS(TRUNC(oldest, 'MON'), rownum - 1) value
                FROM all_objects
               WHERE rownum < nbMonths + 7)
    LOOP
      -- To improve data management each daily partition has its own tablespace
      -- http://www.oracle.com/technology/oramag/oracle/06-sep/o56partition.html
    
      -- Check if a new tablespace is required before creating the partition
      tableSpaceName := 'MON_'||TO_CHAR(b.value, 'YYYYMM')||'_'||username;
      BEGIN
        SELECT tablespace_name, status INTO tableSpaceName, tableSpaceStatus
          FROM user_tablespaces
         WHERE tablespace_name = tableSpaceName;

        -- If the tablespace is read only, alter its status to read write for
        -- upcoming operations.
        IF tableSpaceStatus = 'READ ONLY' THEN
          EXECUTE IMMEDIATE 'ALTER TABLESPACE '||tableSpaceName||' READ WRITE';
        END IF;      
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- The tablespace doesn't exist so lets create it!
        EXECUTE IMMEDIATE 'CREATE TABLESPACE '||tableSpaceName||'
                           DATAFILE SIZE 100M
                           AUTOEXTEND ON NEXT 200M
                           MAXSIZE 30G
                           EXTENT MANAGEMENT LOCAL
                           SEGMENT SPACE MANAGEMENT AUTO';
      END;

      -- Create the partition for the current table if need.
      BEGIN
        SELECT partition_name INTO partitionName
          FROM user_tab_partitions
         WHERE table_name = a.table_name
           AND partition_name = 'P_'||TO_CHAR(b.value, 'YYYYMM');
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- The partition doesn't exist so lets create it!
        EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                           SPLIT PARTITION MAX_VALUE
                           AT    ('''||TO_CHAR(b.value + 1, 'DD-MON-YYYY')||''')
                           INTO  (PARTITION P_'||TO_CHAR(b.value, 'YYYYMM')||'
                                  TABLESPACE '||tableSpaceName||',
                                  PARTITION MAX_VALUE)
                           UPDATE INDEXES';
      END;
    END LOOP;
  END LOOP;

  -- Rebuild any partitioned indexes which are in an UNUSABLE state. This
  -- happens sometimes after partitioning.
  FOR a IN (SELECT index_name, partition_name, tablespace_name
              FROM user_ind_partitions
             WHERE status != 'USABLE')
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||a.index_name||'
                       REBUILD PARTITION '||a.partition_name||'
                       TABLESPACE '||a.tablespace_name;
  END LOOP;
END;
/

/* Copy the contents of the _OLD tables to the new ones and drop the _OLD
 * tables.
 */
BEGIN
  FOR a IN (SELECT table_name FROM user_tables
             WHERE table_name LIKE '%_OLD'
               AND partitioned = 'YES'
             ORDER BY table_name)
  LOOP
    FOR b IN (SELECT partition_name 
                FROM user_tab_partitions
               WHERE table_name = a.table_name
               ORDER BY partition_name DESC)
    LOOP
      EXECUTE IMMEDIATE 'INSERT INTO '||substr(a.table_name, 0, length(a.table_name) - 4)||'
                         (SELECT * FROM '||a.table_name||'
                          PARTITION('||b.partition_name||'))';
      COMMIT;
    END LOOP;
    EXECUTE IMMEDIATE 'DROP TABLE '||a.table_name;
  END LOOP;
END;
/

/* Drop old tablespaces. */
DECLARE
  username VARCHAR2(30) := SYS_CONTEXT('USERENV', 'CURRENT_USER');
BEGIN
  FOR a IN (SELECT tablespace_name, status FROM user_tablespaces
             WHERE tablespace_name LIKE 'MON_%_'||username
               AND length(tablespace_name) = 13 + length(username))
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||a.tablespace_name||' OFFLINE';
    EXECUTE IMMEDIATE 'DROP TABLESPACE '||a.tablespace_name||'
                       INCLUDING CONTENTS AND DATAFILES';
  END LOOP;
END;
/

/* Update and revalidation of PL-SQL code */
/******************************************/

/* PL/SQL method implementing createPartition */
CREATE OR REPLACE PROCEDURE createPartitions
AS
  username         VARCHAR2(30) := SYS_CONTEXT('USERENV', 'CURRENT_USER');
  tableSpaceName   VARCHAR2(30);
  tableSpaceStatus VARCHAR2(9);
  partitionName    VARCHAR2(30);
  oldest           DATE;
  nbMonths         NUMBER;
BEGIN
  -- Loop over all partitioned tables
  FOR a IN (SELECT table_name FROM user_tables
             WHERE partitioned = 'YES'
             ORDER BY table_name)
  LOOP
    -- Get the oldest partition date associated to the table.
    SELECT to_date(min(substr(partition_name, 3)), 'YYYYMM') INTO oldest
      FROM user_tab_partitions
     WHERE table_name = a.table_name
       AND partition_name != 'MAX_VALUE';
    SELECT months_between(SYSDATE, oldest) INTO nbMonths
      FROM dual;

    -- Generate a list of the first day of every month for the next 2 months.
    FOR b IN (SELECT ADD_MONTHS(TRUNC(SYSDATE, 'MON'), rownum - 1) value
                FROM all_objects
               WHERE rownum < nbMonths + 2)
    LOOP
      -- To improve data management each daily partition has its own tablespace
      -- http://www.oracle.com/technology/oramag/oracle/06-sep/o56partition.html
    
      -- Check if a new tablespace is required before creating the partition
      tableSpaceName := 'MON_'||TO_CHAR(b.value, 'YYYYMM')||'_'||username;
      BEGIN
        SELECT tablespace_name, status INTO tableSpaceName, tableSpaceStatus
          FROM user_tablespaces
         WHERE tablespace_name = tableSpaceName;

        -- If the tablespace is read only, alter its status to read write for
        -- upcoming operations.
        IF tableSpaceStatus = 'READ ONLY' THEN
          EXECUTE IMMEDIATE 'ALTER TABLESPACE '||tableSpaceName||' READ WRITE';
        END IF;      
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- The tablespace doesn't exist so lets create it!
        EXECUTE IMMEDIATE 'CREATE TABLESPACE '||tableSpaceName||'
                           DATAFILE SIZE 100M
                           AUTOEXTEND ON NEXT 200M
                           MAXSIZE 30G
                           EXTENT MANAGEMENT LOCAL
                           SEGMENT SPACE MANAGEMENT AUTO';
      END;

      -- Create the partition for the current table if need.
      BEGIN
        SELECT partition_name INTO partitionName
          FROM user_tab_partitions
         WHERE table_name = a.table_name
           AND partition_name = 'P_'||TO_CHAR(b.value, 'YYYYMM');
      EXCEPTION WHEN NO_DATA_FOUND THEN
        -- The partition doesn't exist so lets create it!
        EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                           SPLIT PARTITION MAX_VALUE
                           AT    ('''||TO_CHAR(b.value + 1, 'DD-MON-YYYY')||''')
                           INTO  (PARTITION P_'||TO_CHAR(b.value, 'YYYYMM')||'
                                  TABLESPACE '||tableSpaceName||',
                                  PARTITION MAX_VALUE)
                           UPDATE INDEXES';
      END;
    END LOOP;
  END LOOP;

  -- Rebuild any partitioned indexes which are in an UNUSABLE state. This
  -- happens sometimes are partitioning.
  FOR a IN (SELECT index_name, partition_name, tablespace_name
              FROM user_ind_partitions
             WHERE status != 'USABLE')
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||a.index_name||'
                       REBUILD PARTITION '||a.partition_name||'
                       TABLESPACE '||a.tablespace_name;
  END LOOP;
END;
/


/* PL/SQL method implementing dropPartitions */
CREATE OR REPLACE PROCEDURE dropPartitions
AS
  username   VARCHAR2(30) := SYS_CONTEXT('USERENV', 'CURRENT_USER');
  expiryTime NUMBER;
BEGIN
  -- Get the retention period for the data.
  SELECT expiry INTO expiryTime FROM ConfigSchema;
  
  -- Drop partitions across all tables.
  FOR a IN (SELECT table_name, partition_name
              FROM user_tab_partitions
             WHERE partition_name < concat('P_', TO_CHAR(SYSDATE - expiryTime, 'YYYYMM'))
               AND partition_name <> 'MAX_VALUE'
             ORDER BY partition_name DESC)
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                       DROP PARTITION '||a.partition_name;
  END LOOP;
  -- Drop tablespaces.
  FOR a IN (SELECT tablespace_name
              FROM user_tablespaces
             WHERE status <> 'OFFLINE'
               AND tablespace_name LIKE 'MON_%_'||username
               AND tablespace_name < 'MON_'||TO_CHAR(SYSDATE - expiryTime, 'YYYYMM')||'_'||username
             ORDER BY tablespace_name ASC)
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||a.tablespace_name||' OFFLINE';
    EXECUTE IMMEDIATE 'DROP TABLESPACE '||a.tablespace_name||'
                       INCLUDING CONTENTS AND DATAFILES';
  END LOOP;
  -- Set the status of tablespaces older then 1 month to read only.
  FOR a IN (SELECT tablespace_name
              FROM user_tablespaces
             WHERE status <> 'OFFLINE'
               AND status <> 'READ ONLY'
               AND tablespace_name LIKE 'MON_%_'||username
               AND tablespace_name < 'MON_'||TO_CHAR(SYSDATE - 1, 'YYYYMM')||'_'||username
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
      JOB_NAME        => 'createPartitionsJob',
      JOB_TYPE        => 'STORED_PROCEDURE',
      JOB_ACTION      => 'createPartitions',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => TRUNC(SYSDATE) + 1/24,
      REPEAT_INTERVAL => 'FREQ=DAILY',
      ENABLED         => TRUE,
      COMMENTS        => 'Daily partitioning creation');

  -- Create a db job to be run every day and drop old data from the database
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'dropPartitionsJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN
                            dropPartitions();
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
      COMMENTS        => 'Daily data removal');

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
                            statsTop10Errors(now, interval);
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
           replace(type.value, 'REQUESTTYPE_', '') type,
           nvl(svcClass.value, 'all') svcClass,
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
           -- srmbed
            OR  (messages.facility = 14 -- srmbed
           AND   messages.msg_no = 20   -- TURL available
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

/* Trigger the initial creation of partitions */
BEGIN
  createPartitions();
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
 WHERE release = '2_1_11_0';
COMMIT;
