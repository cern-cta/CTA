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
 * @(#)$RCSfile: mon_2.1.8-7_to_2.1.8-8.sql,v $ $Release: 1.2 $ $Release$ $Date: 2009/05/18 09:38:03 $ $Author: brabacal $
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
SELECT distinct sysdate timestamp, 300 interval, bin, count(bin) over (Partition BY bin) reqs 
  FROM (
    SELECT value, 
      CASE WHEN value = 1  THEN 1
       WHEN value > 1 AND value < 5 THEN 2
       WHEN value >= 5 AND value < 10 THEN 3
       WHEN value >= 10 AND value < 40 THEN 4
       WHEN value >= 40 AND value < 80 THEN 5
       WHEN value >= 80 AND value < 120 THEN 6
       WHEN value >= 120 AND value < 160 THEN 7
       WHEN value >= 160 AND value < 200 THEN 8
       WHEN value >= 200 AND value < 250 THEN 9
       WHEN value >= 250 AND value < 300 THEN 10
       WHEN value >= 300 AND value < 400 THEN 11
       WHEN value >= 400 AND value < 500 THEN 12
       WHEN value >= 500 AND value < 1000 THEN 13
      ELSE 14 end bin
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
SELECT sysdate timestamp, 300 interval, to_char(bin,'HH24:MI') bin, number_of_req 
  FROM (
    SELECT trunc(timestamp,'Mi') bin, round(avg(fileage),4) number_of_req  
      FROM gcfiles
     WHERE timestamp >= trunc(sysdate - 10/1440,'Mi')
       AND timestamp < trunc(sysdate - 5/1440 ,'Mi')
       GROUP BY trunc(timestamp,'Mi')
  )
ORDER BY bin;

/* Create the GCAvgFileSize view */
CREATE OR REPLACE VIEW GCAvgFileSize AS
SELECT sysdate timestamp, 300 interval, to_char(bin,'HH24:MI') bin, number_of_req
  FROM (
    SELECT trunc(timestamp,'Mi') bin, round(avg(filesize),4) number_of_req  
      FROM gcfiles
     WHERE timestamp >= trunc(sysdate - 10/1440,'Mi')
       AND timestamp < trunc(sysdate - 5/1440 ,'Mi')
     GROUP BY trunc(timestamp,'Mi')
  )
ORDER BY bin;

/* Create the SVCClassUserTopTenMigFiles view */
CREATE OR REPLACE VIEW SVCClassUserTopTenMigFiles AS
SELECT * 
  FROM (
    SELECT sysdate timestamp, 300 interval, username, count(*) total
      FROM migration
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440 
    GROUP BY username, svcclass
    ORDER BY total desc, username, svcclass
  )
WHERE rownum < 11;

/* Create the Requests view for percentage display */
CREATE OR REPLACE VIEW StateRequests AS
SELECT sysdate timestamp, 300 interval, count (CASE WHEN state = 'DiskHit' THEN 1 ELSE null end) DiskHits, count(CASE WHEN state = 'DiskCopy' THEN 1 ELSE null end) DiskCopies, count(CASE WHEN state = 'TapeRecall' THEN 1 ELSE null end) TapeRecalls
  FROM requests 
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440;

/* Create the UserRequests view for percentage display */
CREATE OR REPLACE VIEW UserRequests  AS
SELECT sysdate timestamp, 300 interval, count (CASE WHEN state = 'DiskHit' THEN 1 ELSE null end) DiskHits, count(CASE WHEN state = 'DiskCopy' THEN 1 ELSE null end) DiskCopies, count(CASE WHEN state = 'TapeRecall' THEN 1 ELSE null end) TapeRecalls
  FROM requests 
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
GROUP BY username;

/* Create the SVCClassTopTenUserRequests view for percentage display */
CREATE OR REPLACE VIEW SVCClassTopTenUserRequests AS
SELECT * FROM (
  SELECT sysdate timestamp, 300 interval, username, count(*) total, count(CASE WHEN state = 'DiskHit' THEN 1 ELSE null end) dh, 
count(CASE WHEN state = 'DiskCopy' THEN 1 ELSE null end) dc,
count(CASE WHEN state = 'TapeRecall' THEN 1 ELSE null end) tr
    FROM requests
   WHERE timestamp >= sysdate - 10/1440
     AND timestamp < sysdate - 5/1440 
  GROUP BY username, svcclass
  ORDER BY total desc, username, svcclass
)
WHERE rownum < 11;

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

/* Create the RequestKind view */
CREATE OR REPLACE VIEW RequestKind AS
SELECT sysdate timestamp, 300 interval, svcclass, username, count(*) r
  FROM  requests
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440 
GROUP BY svcclass, username, state
ORDER BY svcclass, r desc;

/* Create the FacilityTopTenErrorCounter view */
CREATE OR REPLACE VIEW FacilityTopTenErrorCounter AS
SELECT * 
  FROM (
    SELECT sysdate timestamp, 300 interval, msg_text, count(*) sum
      FROM castor_dlf.dlf_messages a, castor_dlf.dlf_msg_texts b
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440 
       AND b.fac_no = a.facility
       AND a.msg_no = b.msg_no 
    GROUP BY msg_text, fac_no
    ORDER BY sum desc
)
WHERE rownum < 11;

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

/* Create the GCStateByFileSize view */
CREATE OR REPLACE VIEW GCStateByFileSize AS --Unit is byte
SELECT nvl(reqs_t.timestamp, sysdate) timestamp, nvl(reqs_t.interval,300) interval, reqs_t.svcclass, bin_t.bin, nvl(reqs_t.reqs,0) reqs 
  FROM  ( 
    SELECT * 
      FROM (
        SELECT '0 1024' bin FROM dual
        UNION SELECT '1024 10240' FROM dual
        UNION SELECT '10240 102400' FROM dual
        UNION SELECT '102400 1048576' FROM dual
        UNION SELECT '1048576 10485760' FROM dual
        UNION SELECT '10485760 104857600' FROM dual
        UNION SELECT '104857600 1073741000' FROM dual
        UNION SELECT '1073741000 -1' FROM dual), (SELECT DISTINCT svcclass FROM GcFiles)
  ) bin_t
  LEFT OUTER JOIN (
    SELECT DISTINCT sysdate timestamp, 300 interval, svcclass, bin, count(bin) over (Partition BY svcclass, bin) reqs 
      FROM ( 
        SELECT 
          CASE WHEN filesize < 1024 THEN '0 1024'
           WHEN filesize >= 1024 AND filesize < 10240 THEN '1024 10240'
           WHEN filesize >= 10240 AND filesize < 102400 THEN '10240 102400' 
           WHEN filesize >= 102400 AND filesize < 1048576 THEN '102400 1048576'
           WHEN filesize >= 1048576 AND filesize < 10485760 THEN '1048576 10485760'
           WHEN filesize >= 10485760 AND filesize < 104857600 THEN '10485760 104857600'
           WHEN filesize >= 104857600 AND filesize < 1073741000 THEN '104857600 1073741000'
          ELSE '1073741000 -1' end bin, svcclass
          FROM GcFiles
         WHERE filesize!=0
           AND timestamp >= sysdate - 10/1440
           AND timestamp < sysdate - 5/1440
      )
  ) reqs_t
ON bin_t.bin = reqs_t.bin
WHERE reqs_t.svcclass IS NOT NULL
ORDER BY reqs_t.svcclass, bin_t.bin;

/* Create the GCStatsByFileAge view */
CREATE OR REPLACE VIEW GCStatsByFileAge AS --Unit is second
SELECT nvl(reqs_t.timestamp, sysdate) timestamp, nvl(reqs_t.interval,300) interval, reqs_t.svcclass, bin_t.bin, nvl(reqs_t.reqs,0) reqs
  FROM  (
    SELECT * 
      FROM (
        SELECT '0 10' bin FROM dual
        UNION SELECT '10 60' FROM dual
        UNION SELECT '60 900' FROM dual
        UNION SELECT '900 1800' FROM dual
        UNION SELECT '1800 3600' FROM dual
        UNION SELECT '3600 21600' FROM dual
        UNION SELECT '21600 43200' FROM dual
        UNION SELECT '43200 86400' FROM dual
        UNION SELECT '86400 172800' FROM dual
        UNION SELECT '172800 345600' FROM dual
        UNION SELECT '345600 691200' FROM dual
        UNION SELECT '691200 1382400' FROM dual
        UNION SELECT '1382400 -1' FROM dual), (SELECT DISTINCT svcclass FROM GcFiles)
  ) bin_t
  LEFT OUTER JOIN (
    SELECT DISTINCT sysdate timestamp, 300 INTERVAL, svcclass, bin, count(bin) over (PARTITION BY svcclass, bin) reqs
      FROM (
        SELECT 
          CASE WHEN fileage < 10 THEN '0 10'
           WHEN fileage >= 10 AND fileage < 60 THEN '10 60'
           WHEN fileage >= 60 AND fileage < 900 THEN '60 900'
           WHEN fileage >= 900 AND fileage < 1800 THEN '900 1800'
           WHEN fileage >= 1800 AND fileage < 3600 THEN '1800 3600'
           WHEN fileage >= 3600 AND fileage < 21600 THEN '3600 21600'
           WHEN fileage >= 21600 AND fileage < 43200 THEN '21600 43200'
           WHEN fileage >= 43200 AND fileage < 86400 THEN '43200 86400'
           WHEN fileage >= 86400 AND fileage < 172800 THEN '86400 172800'
           WHEN fileage >= 172800 AND fileage < 345600 THEN '172800 345600'
           WHEN fileage >= 345600 AND fileage < 691200 THEN '345600 691200'
           WHEN fileage >= 691200 AND fileage < 1382400 THEN '691200 1382400'
          ELSE '1382400 -1' END bin, svcclass
          FROM GcFiles
         WHERE fileage IS NOT NULL
           AND timestamp >= sysdate - 10/1440
           AND timestamp < sysdate - 5/1440)
  ) reqs_t
ON bin_t.bin = reqs_t.bin
WHERE reqs_t.svcclass is NOT null
ORDER BY svcclass, bin_t.bin;

/* Create the RequestedAfterGC view */
CREATE OR REPLACE VIEW RequestedAfterGC AS --Unit is hour
SELECT nvl(reqs_t.timestamp, sysdate) timestamp, nvl(reqs_t.interval,300) interval, reqs_t.svcclass, bin_t.bin, nvl(reqs_t.reqs,0) reqs
  FROM (
    SELECT *
      FROM ( 
        SELECT '0 0.1' bin FROM dual
        UNION SELECT '0.1 0.5' FROM dual
        UNION SELECT '0.5 1' FROM dual
        UNION SELECT '1 2' FROM dual
        UNION SELECT '2 4' FROM dual
        UNION SELECT '4 6' FROM dual
        UNION SELECT '6 8' FROM dual
        UNION SELECT '8 24' FROM dual), (SELECT DISTINCT svcclass FROM ReqDel_MV)
  ) bin_t
  LEFT OUTER JOIN ( 
    SELECT DISTINCT sysdate timestamp, 300 interval, svcclass, bin, count(bin) over (Partition BY svcclass, bin) reqs
      FROM ( 
        SELECT
          CASE WHEN dif >= 0 AND dif < 0.1 THEN '0 0.1'
           WHEN dif >= 0.1 AND dif < 0.5 THEN '0.1 0.5'
           WHEN dif >= 0.5 AND dif < 1 THEN  '0.5 1'
           WHEN dif >= 1 AND dif < 2 THEN '1 2'
           WHEN dif >= 2 AND dif < 4 THEN '2 4'
           WHEN dif >= 4 AND dif < 6 THEN '4 6'
           WHEN dif >= 6 AND dif < 8 THEN '6 8'
          ELSE '8 24' END bin, svcclass 
          FROM ReqDel_MV
         WHERE timestamp > sysdate - 10/1440
           AND timestamp <= sysdate - 5/1440)
  ) reqs_t
ON bin_t.bin = reqs_t.bin
WHERE reqs_t.svcclass IS NOT NULL
ORDER BY svcclass, bin_t.bin;

/* Create the BinnedLatencies view */
CREATE OR REPLACE VIEW BinnedLatencies AS
SELECT sysdate timestamp, 300 interval, state, bin, count(bin) reqs 
  FROM (
    SELECT state, round(b.totallatency),
      CASE WHEN b.totallatency < 1 THEN 1
       WHEN b.totallatency >= 1 AND b.totallatency < 10 THEN 2
       WHEN b.totallatency >= 10 AND b.totallatency < 120 THEN 3
       WHEN b.totallatency >= 120 AND b.totallatency < 300 THEN 4
       WHEN b.totallatency >= 300 AND b.totallatency < 600 THEN 5
       WHEN b.totallatency >= 600 AND b.totallatency < 1800 THEN 6
       WHEN b.totallatency >= 1800 AND b.totallatency < 3600 THEN 7
       WHEN b.totallatency >= 3600 AND b.totallatency < 18000 THEN 8
       WHEN b.totallatency >= 18000 AND b.totallatency < 43200 THEN 9
       WHEN b.totallatency >= 43200 AND b.totallatency < 86400 THEN 10
       WHEN b.totallatency >= 86400 AND b.totallatency < 172800 THEN 11
      ELSE 12 end bin
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
SELECT distinct sysdate timestamp, 300 interval, bin, count(bin) over (Partition BY bin) migs 
  FROM (
    SELECT round(b.totallatency),
      CASE WHEN b.totallatency < 1 THEN 1
       WHEN b.totallatency >= 1 AND b.totallatency < 2 THEN 2
       WHEN b.totallatency >= 2 AND b.totallatency < 4 THEN 3
       WHEN b.totallatency >= 4 AND b.totallatency < 6 THEN 4
       WHEN b.totallatency >= 6 AND b.totallatency < 8 THEN 5
       WHEN b.totallatency >= 8 AND b.totallatency < 10 THEN 6
       WHEN b.totallatency >= 10 AND b.totallatency < 120 THEN 7
       WHEN b.totallatency >= 120 AND b.totallatency < 300 THEN 8
       WHEN b.totallatency >= 300 AND b.totallatency < 600 THEN 9
       WHEN b.totallatency >= 600 AND b.totallatency < 1800 THEN 10
       WHEN b.totallatency >= 1800 AND b.totallatency < 3600 THEN 11
      ELSE 12 end bin
      FROM migration a, totallatency b
     WHERE a.subreqid = b.subreqid
       AND a.timestamp >= sysdate - 10/1440
       AND a.timestamp < sysdate - 5/1440
       AND b.timestamp >= sysdate - 10/1440
       AND b.timestamp < sysdate - 5/1440
  )
ORDER BY bin;

/* Create the PoolMigrations view */
CREATE OR REPLACE VIEW PoolMigrations AS
SELECT sysdate timestamp, 300 interval, svcclass, count(*) migs
  FROM migration
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
GROUP BY svcclass
ORDER BY migs desc;

/* Create the MigrationsCounter view */
CREATE OR REPLACE VIEW MigrationsCounter AS
SELECT sysdate timestamp, 300 interval, bin, number_of_mig
  FROM (
    SELECT distinct trunc(timestamp,'Mi') bin, count(trunc(timestamp,'Mi')) over (Partition BY trunc(timestamp,'Mi')) number_of_mig  
      FROM migration
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440
    ORDER BY bin
  );

/* Create the SVCClassMigrationsCounter view */
CREATE OR REPLACE VIEW SVCClassMigrationsCounter AS
SELECT sysdate timestamp, 300 interval, bin, svcclass, number_of_mig
  FROM (
    SELECT distinct trunc(timestamp,'Mi') bin, count(trunc(timestamp,'Mi')) over (Partition BY trunc(timestamp,'Mi'), svcclass) number_of_mig, svcclass  
      FROM migration
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440
    ORDER BY bin
  );
  
/* Create the UserMigrationsCounter view */
CREATE OR REPLACE VIEW UserMigrationsCounter AS
SELECT sysdate timestamp, 300 interval, bin, username, number_of_mig
  FROM (
    SELECT distinct trunc(timestamp,'Mi') bin, count(trunc(timestamp,'Mi')) over (Partition BY trunc(timestamp,'Mi'), username) number_of_mig, username
      FROM migration
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440
    ORDER BY bin
  );
  
/* Create the NewRequests view */  
CREATE OR REPLACE VIEW NewRequests AS
SELECT sysdate timestamp, 300 interval, gctype, svcclass, count(*) total_deleted
  FROM gcfiles
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
GROUP BY gctype, svcclass
ORDER BY gctype, svcclass;


/* Create the SVCClassStateRecalledFiles view */  
CREATE OR REPLACE VIEW SVCClassStateRecalledFiles AS
SELECT distinct sysdate timestamp, 300 interval, svcclass, type, count(type) over (Partition BY svcclass,type) number_of_req 
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

/* Create the SVCClassRequests view */  
CREATE OR REPLACE VIEW SVCClassRequests AS
SELECT distinct sysdate timestamp, 300 interval, svcclass, state, count(*) over (Partition BY svcclass,state) reqs
  FROM requests
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
ORDER BY svcclass;

/* Create the SchedulerReadWrite view */  
CREATE OR REPLACE VIEW SchedulerReadWrite AS
SELECT distinct sysdate timestamp, 300 interval, to_char(trunc(timestamp,'Mi'), 'HH24:MI') bin, sum (CASE WHEN type = 'StageGetRequest' THEN dispatched ELSE 0 end ) read, sum (CASE WHEN type = 'StagePutRequest' THEN dispatched ELSE 0 end ) write
  FROM queuetimestats
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
GROUP BY to_char(trunc(timestamp,'Mi'), 'HH24:MI')
ORDER BY bin;

/* Create the SVCClassTopTenTapes view */  
CREATE OR REPLACE VIEW TopTenTapes AS
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


/* Create the StateRequestCounter view */  
CREATE OR REPLACE VIEW StateRequestCounter AS
SELECT distinct sysdate timestamp, 300 interval, to_char(trunc(timestamp,'Mi'), 'HH24:MI') bin, count(CASE WHEN state = 'DiskHit' THEN trunc(timestamp,'Mi') ELSE null end) over (Partition BY trunc(timestamp,'Mi')) number_of_DH_req, count(CASE WHEN state = 'DiskCopy' THEN trunc(timestamp,'Mi') ELSE null end) over (Partition BY trunc(timestamp,'Mi')) number_of_DC_req, count(CASE WHEN state = 'TapeRecall' THEN trunc(timestamp,'Mi') ELSE null end) over (Partition BY trunc(timestamp,'Mi')) number_of_TR_req
  FROM requests
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
ORDER BY bin;

/* Create the SVCClassStateRequestCounter view */  
CREATE OR REPLACE VIEW SVCClassStateRequestCounter AS
SELECT distinct sysdate timestamp, 300 interval, svcclass, to_char(trunc(timestamp,'Mi'), 'HH24:MI') bin, count(CASE WHEN state = 'DiskHit' THEN trunc(timestamp,'Mi') ELSE null end) over (Partition BY trunc(timestamp,'Mi'), svcclass) number_of_DH_req, count(CASE WHEN state = 'DiskCopy' THEN trunc(timestamp,'Mi') ELSE null end) over (Partition BY trunc(timestamp,'Mi'), svcclass) number_of_DC_req, count(CASE WHEN state = 'TapeRecall' THEN trunc(timestamp,'Mi') ELSE null end) over (Partition BY trunc(timestamp,'Mi'), svcclass) number_of_TR_req
  FROM requests
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
ORDER BY bin;

/* Create the UserStateRequestCounter view */  
CREATE OR REPLACE VIEW SVCClassStateRequestCounter AS
SELECT distinct sysdate timestamp, 300 interval, username, to_char(trunc(timestamp,'Mi'), 'HH24:MI') bin, count(CASE WHEN state = 'DiskHit' THEN trunc(timestamp,'Mi') ELSE null end) over (Partition BY trunc(timestamp,'Mi'), username) number_of_DH_req, count(CASE WHEN state = 'DiskCopy' THEN trunc(timestamp,'Mi') ELSE null end) over (Partition BY trunc(timestamp,'Mi'), username) number_of_DC_req, count(CASE WHEN state = 'TapeRecall' THEN trunc(timestamp,'Mi') ELSE null end) over (Partition BY trunc(timestamp,'Mi'), username) number_of_TR_req
  FROM requests
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440
ORDER BY bin;

/* Create the TopTenTapes view */  
CREATE OR REPLACE VIEW TopTenTapes AS
SELECT * 
  FROM (
    SELECT sysdate timestamp, 300 interval, tapeid, count(tapeid) mounts
      FROM requests a, taperecall b
     WHERE a.subreqid = b.subreqid
       AND a.timestamp >= sysdate - 10/1440
       AND a.timestamp < sysdate - 5/1440
       AND b.timestamp >= sysdate - 10/1440
       AND b.timestamp < sysdate - 5/1440GROUP BY tapeid 
    ORDER BY mounts desc
  )
WHERE rownum < 11;

/* Create the TopTenUsers view */  
CREATE OR REPLACE VIEW TopTenUsers AS
SELECT sysdate timestamp, 300 interval, username 
  FROM (
    SELECT username, count(username) reqs
      FROM requests
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440
    GROUP BY username 
    ORDER BY reqs desc
  )
WHERE rownum < 11;

/* Create the UserTopTenTapeMounts view */  
CREATE OR REPLACE VIEW UserTopTenTapes AS
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

/* Create the SVCClassStateUserTopTen view */  
CREATE OR REPLACE VIEW SVCClassStateUserTopTen AS
SELECT *
  FROM (
    SELECT sysdate timestamp, 300 interval, username, svcclass, state, count(*) r
      FROM requests
     WHERE timestamp >= sysdate -10/1440
       AND timestamp < sysdate - 5/1440 
    GROUP BY username, svcclass, state
    ORDER BY r desc
  )
WHERE rownum < 11;

/* Create the UserRecalledFileSize view */  
CREATE OR REPLACE VIEW UserRecalledFileSize AS
SELECT distinct sysdate timestamp, 300 interval, username, bin, count(bin) over (Partition BY bin, username) reqs 
  FROM (
    SELECT username, round(filesize/1024,4),
      CASE WHEN filesize < 1048576 THEN 1
       WHEN filesize >= 1048576 AND filesize < 10485760 THEN 2
       WHEN filesize >= 10485760 AND filesize < 104857600 THEN 3
       WHEN filesize >= 104857600 AND filesize <= 1073741824 THEN 4
       WHEN filesize >= 1073741824 AND filesize <= 1610612736 THEN 5
       WHEN filesize >= 1610612736 AND filesize <= 2147483648 THEN 6
       WHEN filesize >= 2147483648 AND filesize <= 2684354560  THEN 7
      ELSE 8 end bin
      FROM requests
     WHERE state = 'TapeRecall'
       AND timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440 
       AND filesize != 0
  )
ORDER BY bin;

/* Create the SVCClassStateBinRequestCounter view */  
CREATE OR REPLACE VIEW SVCClassStateBinRequestCounter AS
SELECT sysdate timestamp, 300 interval, state, svcclass, to_char(bin,'HH24:MI') bin, number_of_req 
  FROM (
    SELECT distinct trunc(timestamp,'Mi') bin, count(trunc(timestamp,'Mi')) over (Partition BY trunc(timestamp,'Mi'), state, svcclass) number_of_req, state, svcclass
      FROM requests
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440 
  )
ORDER BY bin;

/* Create the StateBinnedRequestCounter view */  
CREATE OR REPLACE VIEW StateBinnedRequestCounter AS
SELECT sysdate timestamp, 300 interval, state, to_char(bin,'HH24:MI') bin, number_of_req 
  FROM (
    SELECT distinct trunc(timestamp,'Mi') bin, count(trunc(timestamp,'Mi')) over (Partition BY trunc(timestamp,'Mi'), state) number_of_req, state
      FROM requests
     WHERE timestamp >= sysdate - 10/1440
       AND timestamp < sysdate - 5/1440 
  )
ORDER BY bin;

/* Create the UserSVCClassStateHistogram view */  
CREATE OR REPLACE VIEW UserSVCClassStateHistogram AS
SELECT sysdate timestamp, 300 interval, svcclass, username, count(1) total, count(CASE WHEN state = 'DiskHit' THEN 1 ELSE null end) dh, count(CASE WHEN state = 'DiskCopy' THEN 1 ELSE null end) dc, count(CASE WHEN state = 'TapeRecall' THEN 1 ELSE null end) tr, count(CASE WHEN (state = 'TapeRecall' AND type = 'StagePrepareToGetRequest') THEN 1 ELSE null end) pretr, count(CASE WHEN (state = 'TapeRecall' AND type = 'StageGetRequest') THEN 1 ELSE null end) immtr
  FROM requests
 WHERE timestamp >= sysdate - 10/1440
   AND timestamp < sysdate - 5/1440 
GROUP BY svcclass, username
ORDER BY total desc;

/* Create the UserSVCClassMigFilesCounter view */  
CREATE OR REPLACE VIEW UserSVCClassMigFilesCounter AS
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
