/******************************************************************************
 *              mon_2.1.8-7_to_2.1.8-8.sql
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it AND/or
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
 * @(#)$RCSfile: mon_2.1.8-7_to_2.1.8-8.sql,v $ $Release: 1.2 $ $Release$ $Date: 2009/05/19 13:07:31 $ $Author: brabacal $
 *
 * This script upgrades a CASTOR v2.1.8-7 MONITORING database into v2.1.8-8
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works FROM sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Determine the DLF schema that the monitoring procedures should run against */
UNDEF dlfschema
ACCEPT dlfschema DEFAULT castor_dlf PROMPT 'Enter the DLF schema to run monitoring queries against: (castor_dlf) ';
SET VER OFF



/* Version cross check AND update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM dlf_version WHERE release LIKE '2_1_8_7%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

/* Job management */
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs)
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name);
    END IF;
    -- Schedule the start date of the job to 15 minutes FROM now. This
    -- basically pauses the job for 15 minutes so that the upgrade can
    -- go through as quickly as possible.
    dbms_scheduler.set_attribute(a.job_name, 'START_DATE', SYSDATE + 15/1440);
  END LOOP;
END;
/

/* Schema changes go here */
/**************************/

CREATE TABLE CastorVersion (schemaVersion VARCHAR2(20), release VARCHAR2(20));
INSERT INTO CastorVersion VALUES ('2_1_8_7', '2_1_8_8');

--Please verify if job started message is number 20!
CREATE OR REPLACE PROCEDURE statsTotalLat(maxTimeStamp IN DATE) 
AS
BEGIN
  -- Extract total latency info FROM job started summary message
  INSERT INTO TotalLatency (subReqId, timestamp, nsfileid, totalLatency)
    (SELECT DISTINCT a.subReqId, a.timestamp, a.nsfileid, b.value
       FROM &dlfschema..dlf_messages a, &dlfschema..dlf_num_param_values b
      WHERE a.id = b.id
        AND a.facility = 26 -- Job 
        AND a.msg_no = 20   -- Job Started
        AND a.timestamp >= maxTimeStamp
        AND b.timestamp >= maxTimeStamp
        AND a.timestamp < maxTimeStamp + 5/1440
        AND b.timestamp < maxTimeStamp + 5/1440)
  LOG ERRORS INTO Err_Latency REJECT LIMIT 100000;
END;
/

/* Create the TapeMountRecalledFiles view */
CREATE OR REPLACE VIEW TapeMountRecalledFiles AS
SELECT DISTINCT sysdate timestamp, 300 interval, bin, count(bin) over (Partition BY bin) reqs 
  FROM (
    SELECT value, 
      CASE
       WHEN value = 1 THEN '=1'
       WHEN value > 1 AND value < 5 THEN '[2-5)'
       WHEN value >= 5 AND value < 10 THEN '[5-10)'
       WHEN value >= 10 AND value < 40 THEN '[10-40)'
       WHEN value >= 40 AND value < 80 THEN '[40-80)'
       WHEN value >= 80 AND value < 120 THEN '[80-120)'
       WHEN value >= 120 AND value < 160 THEN '[120-160)'
       WHEN value >= 160 AND value < 200 THEN '[160-200)'
       WHEN value >= 200 AND value < 250 THEN '[200-250)'
       WHEN value >= 250 AND value < 300 THEN '[250-300)'
       WHEN value >= 300 AND value < 400 THEN '[300-400)'
       WHEN value >= 400 AND value < 500 THEN '[400-500)'
       WHEN value >= 500 AND value < 1000 THEN '[500-1000)'
      ELSE '>=1000' end bin
    FROM (
      SELECT b.value
        FROM castor_dlf.dlf_messages a, castor_dlf.dlf_num_param_values b
       WHERE a.id = b.id
         AND b.name = 'FILESCP'
         AND facility = 2 AND msg_no = 20
         AND a.timestamp >= sysdate - 10/1440
         AND a.timestamp < sysdate - 5/1440 
    ) 
  )
ORDER BY bin;

/* Create the GCAvgFileAge view */
CREATE OR REPLACE VIEW GCAvgFileAge AS
SELECT sysdate timestamp, 300 interval, svcclass, round(avg(fileage),4) avgfileage  
  FROM gcfiles
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
GROUP BY svcclass 
ORDER BY svcclass;

/* Create the GCAvgFileSize view */
CREATE OR REPLACE VIEW GCAvgFileSize AS
SELECT sysdate timestamp, 300 interval, svcclass, round(avg(filesize),4) avgfilesize  
  FROM gcfiles
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
GROUP BY svcclass 
ORDER BY svcclass;

/* Create the D2DTransactions view */
CREATE OR REPLACE VIEW D2DTransactions AS
SELECT sysdate timestamp, 300 interval, originalpool, targetpool, total
  FROM ( 
    SELECT originalpool, targetpool, count(*) total
      FROM diskcopy
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440 
     GROUP BY originalpool, targetpool
     UNION
     SELECT svcclass, svcclass, copies
       FROM internaldiskcopy
      WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440
  )
ORDER BY originalpool;

/* Create the TopTenErrors view */
CREATE OR REPLACE VIEW TopTenErrors AS
SELECT sysdate timestamp, 300 interval, fac.fac_name, count(*) errorsum
  FROM castor_dlf.dlf_messages mes, castor_dlf.dlf_facilities fac
 WHERE mes.facility = fac.fac_no
   AND mes.severity = 3
   AND mes.timestamp >= sysdate - 10/1440
   AND mes.timestamp < sysdate - 5/1440
GROUP BY fac.fac_name 
ORDER BY errorsum desc;

/* Create the GCStatsByFileSize view */
CREATE OR REPLACE VIEW GCStatsByFileSize AS
SELECT DISTINCT sysdate timestamp, 300 interval, svcclass, bin, count(*) over (Partition BY svcclass, bin) reqs 
  FROM (
    SELECT round(filesize/1024,4),
      CASE
       WHEN filesize < 1024 then '<1Kb'
       WHEN filesize >= 1024 AND filesize < 10240 then '[1-10)Kb'
       WHEN filesize >= 10240 AND filesize < 102400 then '[10-100)Kb'
       WHEN filesize >= 102400 AND filesize < 1048576 then '[100Kb-1Mb)'
       WHEN filesize >= 1048576 AND filesize < 10485760 then '[1-10)Mb'
       WHEN filesize >= 10485760 AND filesize < 104857600 then '[10-100)Mb'
       WHEN filesize >= 104857600 AND filesize <= 1073741824 then '[100Mb-1Gb]'
      ELSE '>1Gb' END bin, svcclass
      FROM gcfiles
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440
       AND filesize != 0
  )
ORDER BY svcclass, bin;

/* Create the GCStatsByFileAge view */
CREATE OR REPLACE VIEW GCStatsByFileAge AS 
SELECT DISTINCT sysdate timestamp, 300 interval, svcclass, bin, count(*) over (Partition BY svcclass, bin) reqs 
  FROM (
    SELECT round(fileage), 
      CASE
       WHEN fileage < 10 then '<10sec'
       WHEN fileage >= 10 AND fileage < 60 then '[10-60)sec'
       WHEN fileage >= 60 AND fileage < 900 then '[1-15)min'
       WHEN fileage >= 900 AND fileage < 1800 then '[15-30)min'
       WHEN fileage >= 1800 AND fileage < 3600 then '[30-60)min'
       WHEN fileage >= 3600 AND fileage < 21600 then '[1-6)hours'
       WHEN fileage >= 21600 AND fileage < 43200 then '[6-12)hours'
       WHEN fileage >= 43200 AND fileage < 86400 then '[12-24)hours'
       WHEN fileage >= 86400 AND fileage < 172800 then '[1-2)days'
       WHEN fileage >= 172800 AND fileage < 345600 then '[2-4)days'
       WHEN fileage >= 345600 AND fileage < 691200 then '[4-8)days'
       WHEN fileage >= 691200 AND fileage < 1382400 then '[8-16)days'
      ELSE '>=16days' END bin, svcclass
      FROM gcfiles
     WHERE fileage IS NOT NULL
      AND timestamp >= sysdate - 10/1440
      AND timestamp < sysdate - 5/1440
  )
ORDER BY svcclass, bin;

/* Create the RequestedAfterGC view */
CREATE OR REPLACE VIEW RequestedAfterGC AS --Unit is hour
SELECT DISTINCT sysdate timestamp, 300 interval, svcclass, bin, count(*) over (Partition BY svcclass, bin) reqs
  FROM (
    SELECT dif,
      CASE 
       WHEN dif < 0.1 then '<0.1 h'
       WHEN dif >= 0.1 AND dif < 0.5 then '[0.1-0.5) h'
       WHEN dif >= 0.5 AND dif < 1 then '[0.5-1) h'
       WHEN dif >= 1 AND dif < 2 then '[1-2) h'
       WHEN dif >= 2 AND dif < 4 then '[2-4) h'
       WHEN dif >= 4 AND dif < 6 then '[4-6) h'
       WHEN dif >= 6 AND dif <= 8 then '[6-8] h'
      ELSE '(8-24) h' END bin, svcclass
      FROM ReqDel_MV
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate -5/1440
  )
ORDER BY svcclass, bin;

/* Create the BinnedRequestsLatencies view */
CREATE OR REPLACE VIEW BinnedRequestsLatencies AS
SELECT sysdate timestamp, 300 interval, state, bin, count(bin) reqs 
  FROM (
    SELECT state, round(b.totallatency),
      CASE 
       WHEN b.totallatency < 1 THEN '<1sec'
       WHEN b.totallatency >= 1 AND b.totallatency < 10 THEN '[1-10)sec'
       WHEN b.totallatency >= 10 AND b.totallatency < 120 THEN '[10-120)sec'
       WHEN b.totallatency >= 120 AND b.totallatency < 300 THEN '[2-5)min'
       WHEN b.totallatency >= 300 AND b.totallatency < 600 THEN '[5-10)min'
       WHEN b.totallatency >= 600 AND b.totallatency < 1800 THEN '[10-30)min'
       WHEN b.totallatency >= 1800 AND b.totallatency < 3600 THEN '[30-60)min'
       WHEN b.totallatency >= 3600 AND b.totallatency < 18000 THEN '[1-5)hours'
       WHEN b.totallatency >= 18000 AND b.totallatency < 43200 THEN '[5-12)hours'
       WHEN b.totallatency >= 43200 AND b.totallatency < 86400 THEN '[12-24)hours'
       WHEN b.totallatency >= 86400 AND b.totallatency < 172800 THEN '[1-2]days'
      ELSE '>2days' end bin
      FROM requests a, totallatency b
     WHERE a.subreqid = b.subreqid
       AND a.timestamp >= sysdate - 10/1440
       AND a.timestamp < sysdate - 5/1440
       AND b.timestamp >= sysdate - 10/1440
       AND b.timestamp < sysdate - 5/1440
  )
GROUP BY state, bin
ORDER BY state, bin;

/* Create the BinnedMigrationLatencies view */
CREATE OR REPLACE VIEW BinnedMigrationLatencies AS
SELECT DISTINCT sysdate timestamp, 300 interval, bin, count(bin) over (Partition BY bin) migs 
  FROM (
    SELECT round(b.totallatency),
      CASE 
       WHEN b.totallatency < 1 THEN '<1sec'
       WHEN b.totallatency >= 1 AND b.totallatency < 2 THEN '[1-2)sec'
       WHEN b.totallatency >= 2 AND b.totallatency < 4 THEN '[2-4)sec'
       WHEN b.totallatency >= 4 AND b.totallatency < 6 THEN '[4-6)sec'
       WHEN b.totallatency >= 6 AND b.totallatency < 8 THEN '[6-8)sec'
       WHEN b.totallatency >= 8 AND b.totallatency < 10 THEN '[8-10)sec'
       WHEN b.totallatency >= 10 AND b.totallatency < 120 THEN '[10-120)sec'
       WHEN b.totallatency >= 120 AND b.totallatency < 300 THEN '[2-5)min'
       WHEN b.totallatency >= 300 AND b.totallatency < 600 THEN '[5-10)min'
       WHEN b.totallatency >= 600 AND b.totallatency < 1800 THEN '[10-30)min'
       WHEN b.totallatency >= 1800 AND b.totallatency < 3600 THEN '[30-60)min'
      ELSE '>=1 hour' end bin
      FROM migration a, totallatency b
     WHERE a.subreqid = b.subreqid
       AND a.timestamp >= sysdate - 10/1440
       AND a.timestamp < sysdate - 5/1440
       AND b.timestamp >= sysdate - 10/1440
       AND b.timestamp < sysdate - 5/1440
  )
ORDER BY bin;
  
/* Create the NewGCRequests view */  
CREATE OR REPLACE VIEW NewGCRequests AS
SELECT sysdate timestamp, 300 interval, gctype, svcclass, count(*) total_deleted
  FROM gcfiles
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
GROUP BY gctype, svcclass
ORDER BY gctype, svcclass;


/* Create the SVCClassStateRecalledFiles view */  
CREATE OR REPLACE VIEW SVCClassStateRecalledFiles AS
SELECT DISTINCT sysdate timestamp, 300 interval, svcclass, type, count(type) over (Partition BY svcclass, type) number_of_req 
  FROM requests
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
   AND state = 'TapeRecall'
ORDER BY svcclass, type;

/* Create the UserTypeRecalledFiles view */  
CREATE OR REPLACE VIEW UserTypeRecalledFiles AS
SELECT sysdate timestamp, 300 interval, a.username, a.reqs prestage , b.reqs stage
  FROM (
    SELECT username, count(username) reqs
      FROM requests 
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440
       AND type = 'StagePrepareToGetRequest'
       AND state = 'TapeRecall'
    GROUP BY username
    ORDER BY reqs desc 
  ) a, 
    (SELECT username, count(username) reqs
       FROM requests 
      WHERE timestamp >= sysdate - 10/1440
        AND timestamp < sysdate - 5/1440
        AND type = 'StageGetRequest'
        AND state = 'TapeRecall'
     GROUP BY username
     ORDER BY reqs desc
    ) b 
WHERE a.username = b.username
ORDER BY prestage desc;

/* Create the SchedulerReadWrite view */  
CREATE OR REPLACE VIEW SchedulerReadWrite AS
SELECT DISTINCT sysdate timestamp, 300 interval, sum (CASE WHEN type = 'StageGetRequest' THEN dispatched ELSE 0 end ) read, sum (CASE WHEN type = 'StagePutRequest' THEN dispatched ELSE 0 end ) write
  FROM queuetimestats
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440;

/* Create the SVCClassTopTenTapes view */  
CREATE OR REPLACE VIEW SVCClassTopTenTapes AS
SELECT * 
  FROM (
    SELECT sysdate timestamp, 300 interval, tapeid, count(tapeid) mounts
      FROM requests a, taperecall b
     WHERE a.subreqid = b.subreqid
       AND a.timestamp >= sysdate - 10/1440
       AND a.timestamp < sysdate - 5/1440
       AND b.timestamp >= sysdate - 10/1440
       AND b.timestamp < sysdate - 5/1440
    GROUP BY tapeid, svcclass 
    ORDER BY mounts desc
  )
WHERE rownum < 11;

/* Create the SVCClassUserRequestCounter view */  
CREATE OR REPLACE VIEW SVCClassUserRequestCounter AS
SELECT DISTINCT sysdate timestamp, 300 interval, username, svcclass, count(CASE WHEN state = 'DiskHit' THEN 1 ELSE NULL end) over (Partition BY svcclass, username) number_of_DH_req, count(CASE WHEN state = 'DiskCopy' THEN 1 ELSE NULL end) over (Partition BY svcclass, username) number_of_DC_req, count(CASE WHEN state = 'TapeRecall' THEN 1 ELSE NULL end) over (Partition BY svcclass, username) number_of_TR_req
  FROM requests
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
ORDER BY username, svcclass;

/* Create the SVCClassTopTenUsers view */  
CREATE OR REPLACE VIEW SVCClassTopTenUsers AS
SELECT sysdate timestamp, 300 interval, username, svcclass, reqs
  from (
    select username, svcclass, reqs, RANK() OVER (PARTITION BY username, svcclass ORDER BY reqs DESC, username) rank
      FROM (
        SELECT username, svcclass, count(username) reqs
          FROM requests
         WHERE timestamp >= sysdate - 10/1440
           AND timestamp < sysdate - 5/1440
        GROUP BY username, svcclass
      )
  ) WHERE rank < 11;

/* Create the UserTopTenTapeMounts view */  
CREATE OR REPLACE VIEW UserTopTenTapeMounts AS
SELECT *
  FROM (
    SELECT sysdate timestamp, 300 interval, username, count(username) con_mount
      FROM requests a, taperecall b
     WHERE  a.subreqid = b.subreqid
       AND a.timestamp >= sysdate - 10/1440
       AND a.timestamp < sysdate - 5/1440
       AND b.timestamp >= sysdate - 10/1440
       AND b.timestamp < sysdate - 5/1440
       AND b.tapemountstate in ('TAPE_PENDING', 'TAPE_WAITDRIVE', 'TAPE_WAITPOLICY', 'TAPE_WAITMOUNT') 
    GROUP BY username
    ORDER BY con_mount desc
  )
WHERE rownum < 11;

/* Create the SVCClassGCTypeFiles view */  
CREATE OR REPLACE VIEW SVCClassGCTypeFiles AS
SELECT sysdate timestamp, 300 interval, gctype, svcclass, count(*) total_deleted
  FROM gcfiles
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
GROUP BY gctype, svcclass
ORDER BY gctype, svcclass;

/* Create the UserRecalledFileSize view */  
CREATE OR REPLACE VIEW UserRecalledFileSize AS
SELECT DISTINCT sysdate timestamp, 300 interval, username, svcclass, bin, count(bin) over (Partition BY bin) reqs 
  FROM (
    SELECT username, svcclass, round(filesize/1024,4),
      CASE 
       WHEN filesize < 1048576 THEN '<1Mb'
       WHEN filesize >= 1048576 AND filesize < 10485760 THEN '[1-10)Mb'
       WHEN filesize >= 10485760 AND filesize < 104857600 THEN '[10-100)Mb'
       WHEN filesize >= 104857600 AND filesize <= 1073741824 THEN '[100Mb-1Gb)'
       WHEN filesize >= 1073741824 AND filesize <= 1610612736 THEN '[1-1.5)Gb'
       WHEN filesize >= 1610612736 AND filesize <= 2147483648 THEN '[1.5-2)Gb'
       WHEN filesize >= 2147483648 AND filesize <= 2684354560  THEN '[2-2.5]Gb'
      ELSE '>2.5' end bin
      FROM requests
     WHERE state = 'TapeRecall'
       AND timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440 
       AND filesize != 0
  )
ORDER BY bin, username, svcclass;

/* Create the SVCClassUserMigFilesCounter view */  
CREATE OR REPLACE VIEW SVCClassUserMigFilesCounter AS
SELECT sysdate timestamp, 300 interval, svcclass, username, count(1) total
  FROM migration
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440 
GROUP BY svcclass, username
ORDER BY total desc;

/* Update runmaxtime in ConfigSchema */
UPDATE ConfigSchema
 SET runmaxtime = SYSDATE;

/* Update AND revalidation of PL-SQL code */
/******************************************/

/* Scheduler jobs */
BEGIN
  -- Remove scheduler jobs before recreation
  FOR a IN (SELECT job_name FROM user_scheduler_jobs)
  LOOP
    DBMS_SCHEDULER.DROP_JOB(a.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every day AND create new partitions
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'partitionCreationJob',
      JOB_TYPE        => 'STORED_PROCEDURE',
      JOB_ACTION      => 'createPartitions',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => TRUNC(SYSDATE) + 1/24,
      REPEAT_INTERVAL => 'FREQ=DAILY',
      ENABLED         => TRUE,
      COMMENTS        => 'Daily partitioning creation');

  -- Create a db job to be run every day AND drop old data FROM the database
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
                            statsTapeRecalled(now);
                            statsProcessingTime(now);
                            statsClientVersion(now);
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
                            maxTimeStamp DATE;
                          BEGIN
                            EXECUTE IMMEDIATE ''TRUNCATE TABLE ERR_Requests'';
                            SELECT runmaxtime INTO maxTimeStamp FROM ConfigSchema; 
                      
                            statsReqs(maxTimeStamp);
                            statsDiskCopy(maxTimeStamp);
                            statsInternalDiskCopy(maxTimeStamp);
                            statsTapeRecall(maxTimeStamp);
                            statsMigs(maxTimeStamp);
                            statsTotalLat(maxTimeStamp);
                            statsGcFiles(maxTimeStamp);
                           
                            UPDATE ConfigSchema
                               SET runmaxtime = runmaxtime + 5/1440;
                          END;',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => SYSDATE + 10/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'CASTOR2 Monitoring Statistics (NEW) (5 Minute Frequency)');

END;
/


/* Recompile all invalid procedures, triggers AND functions */
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
