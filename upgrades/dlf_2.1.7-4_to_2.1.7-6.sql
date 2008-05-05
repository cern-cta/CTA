/******************************************************************************
 *              dlf_2.1.7-4_to_2.1.7-5.sql
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
 * @(#)$RCSfile: dlf_2.1.7-4_to_2.1.7-6.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/05/05 13:07:20 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.7-4 DLF database to 2.1.7-5
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus and sql developer */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM dlf_version WHERE release LIKE '2_1_7_4%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL revision mismatch. Please run previous upgrade scripts before this one.');
END;

UPDATE dlf_version SET release = '2_1_7_5';
COMMIT;

/* Remove scheduler jobs before recreation */
BEGIN
  FOR a IN (SELECT job_name FROM user_scheduler_jobs)
  LOOP
    DBMS_SCHEDULER.DROP_JOB(a.job_name, TRUE);
  END LOOP;
END;

/* Schema changes go here */
DROP TABLE LatencyStats;
DROP TABLE QueueTimeStats;
DROP TABLE GarbageCollectionStats;
DROP TABLE RequestStats;
ALTER TABLE FilesMigratedStats RENAME COLUMN Files TO TotalFiles;

/* SQL statement for table LatencyStats */
CREATE TABLE LatencyStats (timestamp DATE NOT NULL, interval NUMBER, type VARCHAR2(255), started NUMBER, minTime NUMBER(*,4), maxTime NUMBER(*,4), avgTime NUMBER(*,4), stddevTime NUMBER(*,4), medianTime NUMBER(*,4)) 
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table QueueTimeStats */
CREATE TABLE QueueTimeStats (timestamp DATE NOT NULL, interval NUMBER, type VARCHAR2(255), svcclass VARCHAR2(255), dispatched NUMBER, minTime NUMBER(*,4), maxTime NUMBER(*,4), avgTime NUMBER(*,4), stddevTime NUMBER(*,4), medianTime NUMBER(*,4))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table GarbageCollectionStats */
CREATE TABLE GarbageCollectionStats (timestamp DATE NOT NULL, interval NUMBER, diskserver VARCHAR2(255), type VARCHAR2(255), deleted NUMBER, totalSize NUMBER, minFileAge NUMBER(*,4), maxFileAge NUMBER(*,4), avgFileAge NUMBER(*,4), stddevFileAge NUMBER(*,4), medianFileAge NUMBER(*,4))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table RequestStats */
CREATE TABLE RequestStats (timestamp DATE NOT NULL, interval NUMBER, type VARCHAR2(255), hostname VARCHAR2(255), euid VARCHAR2(255), requests NUMBER) 
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* SQL statement for table ReplicationStats */
CREATE TABLE ReplicationStats (timestamp DATE NOT NULL, interval NUMBER, sourceSvcClass VARCHAR2(255), destSvcClass VARCHAR2(255), transferred NUMBER, totalSize NUMBER, minSize NUMBER(*,4), maxSize NUMBER(*,4), avgSize NUMBER(*,4), stddevSize NUMBER(*,4), medianSize NUMBER(*,4))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));


/* PL/SQL method implementing statsLatency */
CREATE OR REPLACE PROCEDURE statsLatency (now IN DATE) AS
BEGIN
  -- Stats table: LatencyStats
  -- Frequency: 5 minutes
  FOR a IN (
    -- Translate the type to a human readable string
    SELECT CASE reqType WHEN '35'  THEN 'StageGetRequest'
                        WHEN '40'  THEN 'StagePutRequest'
                        WHEN '44'  THEN 'StageUpdateRequest'
                        WHEN '133' THEN 'StageDiskCopyReplicaRequest'
                        ELSE 'Unknown' END type, 
           count(*) started, min(waitTime) min, max(waitTime) max, 
           avg(waitTime) avg, stddev_pop(waitTime) stddev, median(waitTime) median
      FROM (
        SELECT nvl(substr(value, instr(value, '@', 1, 2) + 1, 2), 133) reqType, waitTime
          FROM (
            -- Extract the totalWaitTime for all stagerJobs or diskCopyTransfers 
            -- which have started.
            SELECT params.id, params.value waitTime
              FROM dlf_messages messages, dlf_num_param_values params
             WHERE messages.id = params.id
               AND messages.severity = 8 -- System
               AND ((messages.facility = 5  AND messages.msg_no = 12)  -- Job started
                OR  (messages.facility = 23 AND messages.msg_no = 25)) -- DiskCopyTransfer started
               AND messages.timestamp >  now - 10/1440
               AND messages.timestamp <= now - 5/1440
               AND params.name = 'TotalWaitTime'
               AND params.timestamp >  now - 10/1440
               AND params.timestamp <= now - 5/1440
          ) results
      -- For facility 23 (DiskCopyTransfer) we can assume that the request type
      -- associated with the transfer is 133 (StageDiskCopyReplicaRequest). 
      -- However, for normal jobs we must parse the Arguments attribute of the
      -- start message to determine the request type. Hence the left join, 
      -- NULL's are 133!!
      LEFT JOIN dlf_str_param_values params
        ON results.id = params.id
       AND params.name = 'Arguments'
       AND params.timestamp >  now - 10/1440
       AND params.timestamp <= now - 5/1440)
     GROUP BY reqType 
     ORDER BY type
  )
  LOOP
    INSERT INTO LatencyStats 
      (timestamp, interval, type, started, minTime, maxTime, avgTime, stddevTime, medianTime)
    VALUES (now - 5/1440, 300, a.type, a.started, a.min, a.max, a.avg, a.stddev, a.median);
  END LOOP;
END;


/* PL/SQL method implementing statsQueueTime */
CREATE OR REPLACE PROCEDURE statsQueueTime (now in DATE) AS
BEGIN
  -- Stats table: QueueTimeStats
  -- Frequency: 5 minutes
  FOR a IN (
    SELECT params.value type, svcclass.value svcclass, 
           count(*) dispatched,
           nvl(min(queueTime), 0) min, 
           nvl(max(queueTime), 0) max, 
           nvl(avg(queueTime), 0) avg, 
           nvl(stddev_pop(queueTime), 0) stddev, 
           nvl(median(queueTime), 0) median
      FROM (
         SELECT messages.id, params.value queueTime
           FROM dlf_messages messages, dlf_num_param_values params
          WHERE messages.id = params.id
            AND messages.severity = 8 -- System
            AND messages.facility = 9 -- Scheduler
            AND messages.msg_no = 34  -- Wrote notification file
            AND messages.timestamp >  now - 10/1440
            AND messages.timestamp <= now - 5/1440
            AND params.name = 'QueueTime'
            AND params.timestamp >  now - 10/1440
            AND params.timestamp <= now - 5/1440
      ) results
     -- Attach the type of the request to its queuing time
     INNER JOIN dlf_str_param_values params
        ON results.id = params.id
       AND params.name = 'Type'
       AND params.timestamp >  now - 10/1440
       AND params.timestamp <= now - 5/1440
     -- Attach the service class of the request
     INNER JOIN dlf_str_param_values svcclass
        ON results.id = svcclass.id
       AND svcclass.name = 'SvcClass'
       AND svcclass.timestamp >  now - 10/1440
       AND svcclass.timestamp <= now - 5/1440
     GROUP BY params.value, svcclass.value
     ORDER BY params.value, svcclass.value
  )
  LOOP
    INSERT INTO QueueTimeStats 
      (timestamp, interval, type, svcclass, dispatched, minTime, maxTime, avgTime, stddevTime, medianTime)
    VALUES (now - 5/1440, 300, a.type, a.svcclass, a.dispatched, a.min, a.max, a.avg, a.stddev, a.median);
  END LOOP;
END;


/* PL/SQL method implementing statsGarbageCollection */
CREATE OR REPLACE PROCEDURE statsGarbageCollection (now IN DATE) AS
BEGIN
  -- Stats table: GarbageCollectionStats
  -- Frequency: 5 minutes
  FOR a IN (
    SELECT hostname diskserver, type, count(*) deleted,
           sum(params.value) totalSize, 
           min(fileAge) min,
           max(fileAge) max, 
           avg(fileAge) avg, 
           stddev_pop(fileAge) stddev, 
           median(fileAge) median
     FROM (
       -- Extract the file age of all files successfully removed across all
       -- diskservers.
       SELECT messages.id, messages.hostid, 
              decode(messages.msg_no, 11, 'Files2Delete', 
                decode(messages.msg_no, 27, 'NsFilesDeletd', 'StgFilesDeleted')) type, 
              params.value fileAge
         FROM dlf_messages messages, dlf_num_param_values params
        WHERE messages.id = params.id
          AND messages.severity = 8 -- System
          AND messages.facility = 8 -- GC
          AND (messages.msg_no = 11 OR -- Removed file successfully
               messages.msg_no = 27 OR -- Deleting ... nameserver
               messages.msg_no = 36)   -- Deleting ... stager catalog
          AND messages.timestamp >  now - 10/1440
          AND messages.timestamp <= now - 5/1440
          AND params.name = 'FileAge'
          AND params.timestamp >  now - 10/1440
          AND params.timestamp <= now - 5/1440
     ) results
    -- Attach the file size value from the same message to the result form the
    -- inner select above. As a result we'll have one row per file with its 
    -- corresponding age and size.
    INNER JOIN dlf_num_param_values params
       ON results.id = params.id
      AND params.name = 'FileSize'
      AND params.timestamp >  now - 10/1440
      AND params.timestamp <= now - 5/1440
    -- Resolve the host ids to names
    INNER JOIN dlf_host_map hosts
       ON results.hostid = hosts.hostid
    GROUP BY hostname, type
    ORDER BY hostname, type
  )
  LOOP
    INSERT INTO GarbageCollectionStats
      (timestamp, interval, diskserver, type, deleted, totalSize, minFileAge, maxFileAge, avgFileAge, stddevFileAge, medianFileAge)
    VALUES (now - 5/1440, 300, a.diskserver, a.type, a.deleted, a.totalSize, a.min, a.max, a.avg, a.stddev, a.median);
  END LOOP;
END;


/* PL/SQL method implementing statsRequest */
CREATE OR REPLACE PROCEDURE statsRequest (now IN DATE) AS
BEGIN
  -- Stats table: RequestStats
  -- Frequency: 5 minutes
  FOR a IN (
    SELECT type, hostname, euid, requests FROM (
      -- For each request type display the top 5 users + the total number of requests
      SELECT type, euid, hostname, requests,
             RANK() OVER (PARTITION BY type ORDER BY requests DESC, euid ASC) rank
        FROM(
          SELECT params.value type, hosts.hostname, '-' euid, count(*) requests
            FROM dlf_messages messages, dlf_str_param_values params, dlf_host_map hosts
           WHERE messages.id = params.id
             AND messages.severity = 10 -- Monitoring
             AND messages.facility = 4  -- RequestHandler
             AND messages.msg_no = 10   -- Reply sent to client
             AND messages.timestamp >  now - 10/1440
             AND messages.timestamp <= now - 5/1440
             AND params.name = 'Type'
             AND params.timestamp >  now - 10/1440
             AND params.timestamp <= now - 5/1440
             AND messages.hostid = hosts.hostid
           GROUP BY params.value, hosts.hostname
          -- Join the user and summary/aggregate level breakdowns together. Note: 
          -- this could probably be done using an analytical function or grouping 
          -- set!!!
           UNION
          -- Determine the number of requests made for each request type and per
          -- user over the last sampling period. This gives us a user level breakdown.
          SELECT results.value type, hostname, TO_CHAR(params.value) euid, count(*) requests
            FROM (
              SELECT params.id, params.value, hosts.hostname
                FROM dlf_messages messages, dlf_str_param_values params, dlf_host_map hosts
               WHERE messages.id = params.id
                 AND messages.severity = 10 -- Monitoring
                 AND messages.facility = 4  -- RequestHandler
                 AND messages.msg_no = 10   -- Reply sent to client
                 AND messages.timestamp >  now - 10/1440
                 AND messages.timestamp <= now - 5/1440
                 AND params.name = 'Type'
                 AND params.timestamp >  now - 10/1440
                 AND params.timestamp <= now - 5/1440
                 AND messages.hostid = hosts.hostid
            ) results
          -- Determine the uid of the user associated with each request
          INNER JOIN dlf_num_param_values params
             ON results.id = params.id
            AND params.name = 'Euid'
            AND params.timestamp >  now - 10/1440
            AND params.timestamp <= now - 5/1440
          GROUP BY results.value, hostname, params.value)
      ) WHERE rank < 6
     ORDER BY type, requests DESC
  )
  LOOP
    INSERT INTO RequestStats
      (timestamp, interval, type, hostname, euid, requests)
    VALUES (now - 5/1440, 300, a.type, a.hostname, a.euid, a.requests);
  END LOOP;
END;


/* PL/SQL method implementing statsDiskCachEfficiency */
CREATE OR REPLACE PROCEDURE statsDiskCacheEfficiency (now IN DATE) AS
BEGIN
  -- Stats table: DiskCacheEfficiencyStats
  -- Frequency: 5 minutes
  FOR a IN (
    SELECT nvl(sum(CASE WHEN msg_no = 53 THEN requests ELSE 0 END), 0) Wait,
           nvl(sum(CASE WHEN msg_no = 56 THEN requests ELSE 0 END), 0) D2D,
           nvl(sum(CASE WHEN msg_no = 57 THEN requests ELSE 0 END), 0) Recall,
           nvl(sum(CASE WHEN msg_no = 60 THEN requests ELSE 0 END), 0) Staged,
           nvl(sum(requests), 0) total
      FROM (
        SELECT msg_no, count(*) requests
          FROM (
            -- Get the first message issued for all subrequests of interest. This
            -- indicates to us whether the request was a hit or miss.
            SELECT sum(messages.id) KEEP (DENSE_RANK FIRST 
                   ORDER BY messages.timestamp ASC, messages.timeusec ASC) id
              FROM dlf_messages messages
             WHERE messages.severity = 8  -- System
               AND messages.facility = 22 -- Stager
               AND messages.msg_no IN (53, 56, 57, 60)
               AND messages.reqid IN (
                 -- Extract all new requests entering the system through the request
                 -- handler that may result in a read request. This result set will 
                 -- be used in outer select statement to prevent if from picking up
                 -- subrequests which have been restarted but did not enter the 
                 -- system within the required timeframe.
                 SELECT messages.reqid
                   FROM dlf_messages messages, dlf_str_param_values params
                  WHERE messages.id = params.id
                    AND messages.severity = 10 -- Monitoring
                    AND messages.facility = 4  -- RequestHandler
                    AND messages.msg_no = 10   -- Reply sent to client
                    AND messages.timestamp >  now - 10/1440
                    AND messages.timestamp <= now - 5/1440
                    AND params.name = 'Type'
                    AND params.value IN ('StageGetRequest', 
                                         'StagePrepareToGetRequest', 
                                         'StageUpdateRequest')
                    AND params.timestamp >  now - 10/1440
                    AND params.timestamp <= now - 5/1440
               )
               AND messages.timestamp >  now - 10/1440
               AND messages.timestamp <= now - 5/1440
             GROUP BY messages.subreqid
          ) results
         INNER JOIN dlf_messages messages
            ON messages.id = results.id
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
         GROUP BY msg_no)
  )
  LOOP
    INSERT INTO DiskCacheEfficiencyStats
      (timestamp, interval, wait, d2d, recall, staged, total)
    VALUES (now - 5/1440, 300, a.wait, a.d2d, a.recall, a.staged, a.total);
  END LOOP;
END;


/* PL/SQL method implementing statsMigratedFiles */
CREATE OR REPLACE PROCEDURE statsMigratedFiles (now IN DATE) AS
BEGIN
  -- Stats table: FilesMigratedStats
  -- Frequency: 5 minutes
  FOR a IN (
    SELECT svcclass, tapepool, count(*) files, sum(filesize) totalSize
      FROM (
        SELECT a.id, max(decode(a.name, 'SVCCLASS', a.value, NULL)) svcclass,
                     max(decode(a.name, 'TAPEPOOL', a.value, NULL)) tapepool,
                     max(decode(b.name, 'FILESIZE', b.value, NULL)) filesize
          FROM dlf_str_param_values a, dlf_num_param_values b
         WHERE a.id = b.id
           AND a.id IN (
             -- Extract the message ids of interest
             SELECT messages.id FROM dlf_messages messages
              WHERE messages.severity = 8 -- System
                AND messages.facility = 1 -- migrator
                AND messages.msg_no = 55  -- File staged
                AND messages.timestamp >  now - 10/1440
                AND messages.timestamp <= now - 5/1440
             )
             AND a.timestamp >  now - 10/1440
             AND a.timestamp <= now - 5/1440
             AND b.timestamp >  now - 10/1440
             AND b.timestamp <= now - 5/1440
             AND a.name IN ('SVCCLASS', 'TAPEPOOL')
             AND b.name IN ('FILESIZE', 'ELAPSEDTIME')
           GROUP BY a.id)
     GROUP BY svcclass, tapepool
  )
  LOOP
    INSERT INTO FilesMigratedStats
      (timestamp, interval, svcclass, tapepool, totalFiles, totalSize)
    VALUES (now - 5/1440, 300, a.svcclass, a.tapepool, a.files, a.totalSize);
  END LOOP;
END;


/* PL/SQL method implementing statsReplication */
CREATE OR REPLACE PROCEDURE statsReplication (now IN DATE) AS
BEGIN
  -- Stats table: ReplicationStats
  -- Frequency: 5 minutes
  FOR a IN (
    SELECT src, dest, count(*) transferred, sum(params.value) totalsize, 
           min(params.value) min, max(params.value) max, avg(params.value) avg, 
           stddev_pop(params.value) stddev, median(params.value) median
      FROM (
        SELECT params.id, 
               substr(params.value, 0, instr(params.value, '->', 1) - 2) src,
               substr(params.value, instr(params.value, '->', 1) + 3) dest
          FROM dlf_messages messages, dlf_str_param_values params
         WHERE messages.id = params.id
           AND messages.severity = 8  -- System
           AND messages.facility = 23 -- DiskCopyTransfer
           AND messages.msg_no = 39   -- DiskCopy Transfer successful
           AND messages.timestamp >  now - 10/1440
           AND messages.timestamp <= now - 5/1440
           AND params.name = 'Direction'
           AND params.timestamp >  now - 10/1440
           AND params.timestamp <= now - 5/1440
      ) results
     -- Attach the size of the file to each replication request. As a
     -- result of this we will have one line per request detailing the
     -- direction of the transfer and the amount of data transferred
     INNER JOIN dlf_num_param_values params
        ON results.id = params.id
       AND params.name = 'FileSize'
       AND params.timestamp >  now - 10/1440
       AND params.timestamp <= now - 5/1440
     GROUP BY src, dest
  )
  LOOP
    INSERT INTO ReplicationStats
      (timestamp, interval, sourceSvcClass, destSvcClass, transferred, totalSize, minSize, maxSize, avgSize, stddevSize, medianSize)
    VALUES (now - 5/1440, 300, a.src, a.dest, a.transferred, a.totalSize, a.min, a.max, a.avg, a.stddev, a.median);
  END LOOP;
END;


BEGIN
  -- Create a db job to be run every day and create new partitions
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'partitionCreationJob',
      JOB_TYPE        => 'STORED_PROCEDURE',
      JOB_ACTION      => 'createPartitions',
      START_DATE      => TRUNC(SYSDATE) + 1/24,
      REPEAT_INTERVAL => 'FREQ=DAILY',
      ENABLED         => TRUE,
      COMMENTS        => 'Daily partitioning creation');

  -- Create a db job to be run every day and drop old data from the database
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'archiveDataJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN archiveData(90); END;',
      START_DATE      => TRUNC(SYSDATE) + 2/24,
      REPEAT_INTERVAL => 'FREQ=DAILY',
      ENABLED         => TRUE,
      COMMENTS        => 'Daily data archiving');

  -- Create a job to execute the procedures that create statistical information
  DBMS_SCHEDULER.CREATE_JOB (
      JOB_NAME        => 'statisticJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'DECLARE
                            now DATE;
                          BEGIN
                            now := SYSDATE;
                            statsLatency(now);
                            statsQueueTime(now);
                            statsGarbageCollection(now);
                            statsRequest(now);
                            statsDiskCacheEfficiency(now);
                            statsMigratedFiles(now);
                            statsReplication(now);
                          END;',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'CASTOR2 Monitoring Statistics (5 Minute Frequency)');
END;
