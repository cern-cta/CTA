/******************************************************************************
 *              mon_2.1.8-7_to_2.1.8-8.sql
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
 * @(#)$RCSfile: mon_2.1.8-7_to_2.1.8-8.sql,v $ $Release: 1.2 $ $Release$ $Date: 2009/05/15 15:10:04 $ $Author: brabacal $
 *
 * This script upgrades a CASTOR v2.1.8-7 MONITORING database into v2.1.8-8
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Determine the DLF schema that the monitoring procedures should run against */
UNDEF dlfschema
ACCEPT dlfschema DEFAULT castor_dlf PROMPT 'Enter the DLF schema to run monitoring queries against: (castor_dlf) ';
SET VER OFF



/* Version cross check and update */
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
    -- Schedule the start date of the job to 15 minutes from now. This
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
  -- Extract total latency info from job started summary message
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


/* Create the TapemountRecalledFiles view */
CREATE OR REPLACE VIEW TapemountRecalledFiles AS
select distinct sysdate timestamp, 300 interval, bin, count(bin) over (Partition by bin) reqs 
  from (
    select value, 
      case when value = 1  then 1
       when value > 1 and value < 5 then 2
       when value >= 5 and value < 10 then 3
       when value >= 10 and value < 40 then 4
       when value >= 40 and value < 80 then 5
       when value >= 80 and value < 120 then 6
       when value >= 120 and value < 160 then 7
       when value >= 160 and value < 200 then 8
       when value >= 200 and value < 250 then 9
       when value >= 250 and value < 300 then 10
       when value >= 300 and value < 400 then 11
       when value >= 400 and value < 500 then 12
       when value >= 500 and value < 1000 then 13
      else 14 end bin
    from (
      select b.value
	from castor_dlf.dlf_messages a, castor_dlf.dlf_num_param_values b
       where a.id = b.id
         and b.name = 'FILESCP'
	 and facility = 2 and msg_no = 20
	 and a.timestamp >= sysdate - 10/1440
	 and a.timestamp < sysdate - 5/1440 
    ) 
  )
order by bin;

/* Create the GCAvgFileAge view */
CREATE OR REPLACE VIEW GCAvgFileAge AS
select sysdate timestamp, 300 interval, to_char(bin,'HH24:MI') bin, number_of_req 
  from (
    select trunc(timestamp,'Mi') bin, round(avg(fileage),4) number_of_req  
      from gcfiles
     where timestamp >= trunc(sysdate - 10/1440,'Mi')
       and timestamp < trunc(sysdate - 5/1440 ,'Mi')
       group by trunc(timestamp,'Mi')
  )
order by bin;

/* Create the GCAvgFileSize view */
CREATE OR REPLACE VIEW GCAvgFileSize AS
select sysdate timestamp, 300 interval, to_char(bin,'HH24:MI') bin, number_of_req
  from (
    select trunc(timestamp,'Mi') bin, round(avg(filesize),4) number_of_req  
      from gcfiles
     where timestamp >= trunc(sysdate - 10/1440,'Mi')
       and timestamp < trunc(sysdate - 5/1440 ,'Mi')
     group by trunc(timestamp,'Mi')
  )
order by bin;

/* Create the SVCClassUserTopTenMigFiles view */
CREATE OR REPLACE VIEW SVCClassUserTopTenMigFiles AS
select * 
  from (
    select sysdate timestamp, 300 interval, username, count(*) total
      from migration
     where timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440 
    group by username, svcclass
    order by total desc, username, svcclass
  )
where rownum < 11;

/* Create the Requests view for percentage display */
CREATE OR REPLACE VIEW StateRequests AS
select sysdate timestamp, 300 interval, count (case when state='DiskHit' then 1 else null end) DiskHits, count(case when state='DiskCopy' then 1 else null end) DiskCopies, count(case when state='TapeRecall' then 1 else null end) TapeRecalls
  from requests 
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440;

/* Create the UserRequests view for percentage display */
CREATE OR REPLACE VIEW UserRequests  AS
select sysdate timestamp, 300 interval, count (case when state='DiskHit' then 1 else null end) DiskHits, count(case when state='DiskCopy' then 1 else null end) DiskCopies, count(case when state='TapeRecall' then 1 else null end) TapeRecalls
  from requests 
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440
group by username;

/* Create the SVCClassTopTenUserRequests view for percentage display */
CREATE OR REPLACE VIEW SVCClassTopTenUserRequests AS
select * from (
  select sysdate timestamp, 300 interval, username, count(*) total, count(case when state='DiskHit' then 1 else null end) dh, 
count(case when state='DiskCopy' then 1 else null end) dc,
count(case when state='TapeRecall' then 1 else null end) tr
    from requests
   where timestamp >= sysdate - 10/1440
     and timestamp < sysdate - 5/1440 
  group by username, svcclass
  order by total desc, username, svcclass
)
where rownum < 11;

/* Create the D2DTransactions view */
CREATE OR REPLACE VIEW D2DTransactions AS
select sysdate timestamp, 300 interval, originalpool, targetpool, total
  from ( 
    select originalpool, targetpool, count(*) total
      from diskcopy
     where timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440 
     group by originalpool, targetpool
     union
     select svcclass, svcclass, copies
       from internaldiskcopy
      where timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440
  )
order by originalpool;

/* Create the RequestKind view */
CREATE OR REPLACE VIEW RequestKind AS
select sysdate timestamp, 300 interval, svcclass, username, count(*) r
  from  requests
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440 
group by svcclass, username, state
order by svcclass, r desc;

/* Create the FacilityTopTenErrorCounter view */
CREATE OR REPLACE VIEW FacilityTopTenErrorCounter AS
select * 
  from (
    select sysdate timestamp, 300 interval, msg_text, count(*) sum
      from castor_dlf.dlf_messages a, castor_dlf.dlf_msg_texts b
     where timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440 
       and b.fac_no = a.facility
       and a.msg_no = b.msg_no 
    group by msg_text, fac_no
    order by sum desc
)
where rownum < 11;

/* Create the TopTenErrors view */
CREATE OR REPLACE VIEW TopTenErrors AS
select sysdate timestamp, 300 interval, fac.fac_name, count(*) errorsum
  from castor_dlf.dlf_messages mes, castor_dlf.dlf_facilities fac
 where mes.facility = fac.fac_no
   and mes.severity = 3
   and mes.timestamp >= sysdate - 10/1440
   and mes.timestamp < sysdate - 5/1440
group by fac.fac_name 
order by errorsum desc;

/* Create the GCStateByFileSize view */
CREATE OR REPLACE VIEW GCStateByFileSize AS --Unit is byte
SELECT nvl(reqs_t.timestamp, sysdate) timestamp, nvl(reqs_t.interval,300) interval, reqs_t.svcclass,    bin_t.bin, nvl(reqs_t.reqs,0) reqs 
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
    SELECT DISTINCT sysdate timestamp, 300 interval, svcclass, bin, count(bin) over (Partition by svcclass, bin) reqs
      FROM ( 
        SELECT
          CASE WHEN dif >= 0 and dif < 0.1 THEN '0 0.1'
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
ORDER by svcclass, bin_t.bin;

/* Create the BinnedLatencies view */
CREATE OR REPLACE VIEW BinnedLatencies AS
select sysdate timestamp, 300 interval, state, bin, count(bin) reqs 
  from (
    select state, round(b.totallatency),
      case when b.totallatency < 1 then 1
       when b.totallatency >= 1 and b.totallatency < 10 then 2
       when b.totallatency >= 10 and b.totallatency < 120 then 3
       when b.totallatency >= 120 and b.totallatency < 300 then 4
       when b.totallatency >= 300 and b.totallatency < 600 then 5
       when b.totallatency >= 600 and b.totallatency < 1800 then 6
       when b.totallatency >= 1800 and b.totallatency < 3600 then 7
       when b.totallatency >= 3600 and b.totallatency < 18000 then 8
       when b.totallatency >= 18000 and b.totallatency < 43200 then 9
       when b.totallatency >= 43200 and b.totallatency < 86400 then 10
       when b.totallatency >= 86400 and b.totallatency < 172800 then 11
      else 12 end bin
      from requests a, totallatency b
     where a.subreqid = b.subreqid
       and a.timestamp >= sysdate - 10/1440
       and a.timestamp < sysdate - 5/1440
       and b.timestamp >= sysdate - 10/1440
       and b.timestamp < sysdate - 5/1440
  )
group by state, bin
order by state, bin;

/* Create the BinnedMigrationLatencies view */
CREATE OR REPLACE VIEW BinnedMigrationLatencies AS
select distinct sysdate timestamp, 300 interval, bin, count(bin) over (Partition by bin) migs 
  from (
    select round(b.totallatency),
      case when b.totallatency < 1 then 1
       when b.totallatency >= 1 and b.totallatency < 2 then 2
       when b.totallatency >= 2 and b.totallatency < 4 then 3
       when b.totallatency >= 4 and b.totallatency < 6 then 4
       when b.totallatency >= 6 and b.totallatency < 8 then 5
       when b.totallatency >= 8 and b.totallatency < 10 then 6
       when b.totallatency >= 10 and b.totallatency < 120 then 7
       when b.totallatency >= 120 and b.totallatency < 300 then 8
       when b.totallatency >= 300 and b.totallatency < 600 then 9
       when b.totallatency >= 600 and b.totallatency < 1800 then 10
       when b.totallatency >= 1800 and b.totallatency < 3600 then 11
      else 12 end bin
      from migration a, totallatency b
     where a.subreqid = b.subreqid
       and a.timestamp >= sysdate - 10/1440
       and a.timestamp < sysdate - 5/1440
       and b.timestamp >= sysdate - 10/1440
       and b.timestamp < sysdate - 5/1440
  )
order by bin;

/* Create the PoolMigrations view */
CREATE OR REPLACE VIEW PoolMigrations AS
select sysdate timestamp, 300 interval, svcclass, count(*) migs
  from migration
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440
group by svcclass
order by migs desc;

/* Create the MigrationsCounter view */
CREATE OR REPLACE VIEW MigrationsCounter AS
select sysdate timestamp, 300 interval, bin, number_of_mig
  from (
    select distinct trunc(timestamp,'Mi') bin, count(trunc(timestamp,'Mi')) over (Partition by trunc(timestamp,'Mi')) number_of_mig  
      from migration
     where timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440
    order by bin
  );

/* Create the SVCClassMigrationsCounter view */
CREATE OR REPLACE VIEW SVCClassMigrationsCounter AS
select sysdate timestamp, 300 interval, bin, svcclass, number_of_mig
  from (
    select distinct trunc(timestamp,'Mi') bin, count(trunc(timestamp,'Mi')) over (Partition by trunc(timestamp,'Mi'), svcclass) number_of_mig, svcclass  
      from migration
     where timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440
    order by bin
  );
  
/* Create the UserMigrationsCounter view */
CREATE OR REPLACE VIEW UserMigrationsCounter AS
select sysdate timestamp, 300 interval, bin, username, number_of_mig
  from (
    select distinct trunc(timestamp,'Mi') bin, count(trunc(timestamp,'Mi')) over (Partition by trunc(timestamp,'Mi'), username) number_of_mig, username
      from migration
     where timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440
    order by bin
  );
  
/* Create the NewRequests view */  
CREATE OR REPLACE VIEW NewRequests AS
select sysdate timestamp, 300 interval, gctype, svcclass, count(*) total_deleted
  from gcfiles
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440
group by gctype, svcclass
order by gctype, svcclass;


/* Create the SVCClassStateRecalledFiles view */  
CREATE OR REPLACE VIEW SVCClassStateRecalledFiles AS
select distinct sysdate timestamp, 300 interval, svcclass, type, count(type) over (Partition by svcclass,type) number_of_req 
  from requests
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440
   and state ='TapeRecall'
order by svcclass, type;

/* Create the UserTypeRecalledFiles view */  
CREATE OR REPLACE VIEW UserTypeRecalledFiles AS
select sysdate timestamp, 300 interval, a.username, a.reqs prestage , b.reqs stage
  from (
    select username, count(username) reqs
      from requests 
     where timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440
       and type = 'StagePrepareToGetRequest'
       and state = 'TapeRecall'
    group by username
    order by reqs desc 
  ) a, 
    (select username, count(username) reqs
       from requests 
      where timestamp >= sysdate - 10/1440
        and timestamp < sysdate - 5/1440
        and type = 'StageGetRequest'
        and state = 'TapeRecall'
     group by username
     order by reqs desc
    ) b 
where a.username = b.username
order by prestage desc;

/* Create the SVCClassRequests view */  
CREATE OR REPLACE VIEW SVCClassRequests AS
select distinct sysdate timestamp, 300 interval, svcclass, state, count(*) over (Partition by svcclass,state) reqs
  from requests
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440
order by svcclass;

/* Create the SchedulerReadWrite view */  
CREATE OR REPLACE VIEW SchedulerReadWrite AS
select distinct sysdate timestamp, 300 interval, to_char(trunc(timestamp,'Mi'), 'HH24:MI') bin, sum (case when type = 'StageGetRequest' then dispatched else 0 end ) read, sum (case when type = 'StagePutRequest' then dispatched else 0 end ) write
  from queuetimestats
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440
group by to_char(trunc(timestamp,'Mi'), 'HH24:MI')
order by bin;

/* Create the SVCClassTopTenTapes view */  
CREATE OR REPLACE VIEW TopTenTapes AS
select * 
  from (
    select sysdate timestamp, 300 interval, tapeid, count(tapeid) mounts
      from requests a, taperecall b
     where a.subreqid = b.subreqid
       and a.timestamp >= sysdate - 10/1440
       and a.timestamp < sysdate - 5/1440
       and b.timestamp >= sysdate - 10/1440
       and b.timestamp < sysdate - 5/1440
    group by tapeid, svcclass 
    order by mounts desc
  )
where rownum < 11;


/* Create the StateRequestCounter view */  
CREATE OR REPLACE VIEW StateRequestCounter AS
select distinct sysdate timestamp, 300 interval, to_char(trunc(timestamp,'Mi'), 'HH24:MI') bin, count(case when state='DiskHit' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi')) number_of_DH_req, count(case when state='DiskCopy' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi')) number_of_DC_req, count(case when state='TapeRecall' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi')) number_of_TR_req
  from requests
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440
order by bin;

/* Create the SVCClassStateRequestCounter view */  
CREATE OR REPLACE VIEW SVCClassStateRequestCounter AS
select distinct sysdate timestamp, 300 interval, svcclass, to_char(trunc(timestamp,'Mi'), 'HH24:MI') bin, count(case when state='DiskHit' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi'), svcclass) number_of_DH_req, count(case when state='DiskCopy' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi'), svcclass) number_of_DC_req, count(case when state='TapeRecall' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi'), svcclass) number_of_TR_req
  from requests
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440
order by bin;

/* Create the UserStateRequestCounter view */  
CREATE OR REPLACE VIEW SVCClassStateRequestCounter AS
select distinct sysdate timestamp, 300 interval, username, to_char(trunc(timestamp,'Mi'), 'HH24:MI') bin, count(case when state='DiskHit' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi'), username) number_of_DH_req, count(case when state='DiskCopy' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi'), username) number_of_DC_req, count(case when state='TapeRecall' then trunc(timestamp,'Mi') else null end) over (Partition by trunc(timestamp,'Mi'), username) number_of_TR_req
  from requests
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440
order by bin;

/* Create the TopTenTapes view */  
CREATE OR REPLACE VIEW TopTenTapes AS
select * 
  from (
    select sysdate timestamp, 300 interval, tapeid, count(tapeid) mounts
      from requests a, taperecall b
     where a.subreqid = b.subreqid
       and a.timestamp >= sysdate - 10/1440
       and a.timestamp < sysdate - 5/1440
       and b.timestamp >= sysdate - 10/1440
       and b.timestamp < sysdate - 5/1440group by tapeid 
    order by mounts desc
  )
where rownum < 11;

/* Create the TopTenUsers view */  
CREATE OR REPLACE VIEW TopTenUsers AS
select sysdate timestamp, 300 interval, username 
  from (
    select username, count(username) reqs
      from requests
     where timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440
    group by username 
    order by reqs desc
  )
where rownum < 11;

/* Create the UserTopTenTapeMounts view */  
CREATE OR REPLACE VIEW UserTopTenTapes AS
select *
  from (
    select sysdate timestamp, 300 interval, username, count(username) con_mount
      from requests a, taperecall b
     where  a.subreqid = b.subreqid
       and a.timestamp >= sysdate - 10/1440
       and a.timestamp < sysdate - 5/1440
       and b.timestamp >= sysdate - 10/1440
       and b.timestamp < sysdate - 5/1440
       and b.tapemountstate in ('TAPE_PENDING','TAPE_WAITDRIVE','TAPE_WAITPOLICY','TAPE_WAITMOUNT') 
    group by username
    order by con_mount desc
  )
where rownum < 11;

/* Create the SVCClassGCTypeFiles view */  
CREATE OR REPLACE VIEW SVCClassGCTypeFiles AS
select sysdate timestamp, 300 interval, gctype, svcclass, count(*) total_deleted
  from gcfiles
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440
group by gctype, svcclass
order by gctype, svcclass;

/* Create the SVCClassStateUserTopTen view */  
CREATE OR REPLACE VIEW SVCClassStateUserTopTen AS
select *
  from (
    select sysdate timestamp, 300 interval, username, svcclass, state, count(*) r
      from requests
     where timestamp >= sysdate -10/1440
       and timestamp < sysdate - 5/1440 
    group by username, svcclass, state
    order by r desc
  )
where rownum < 11;

/* Create the UserRecalledFileSize view */  
CREATE OR REPLACE VIEW UserRecalledFileSize AS
select distinct sysdate timestamp, 300 interval, username, bin, count(bin) over (Partition by bin, username) reqs 
  from (
    select username, round(filesize/1024,4),
      case when filesize < 1048576 then 1
       when filesize >= 1048576 and filesize < 10485760 then 2
       when filesize >= 10485760 and filesize < 104857600 then 3
       when filesize >= 104857600 and filesize <= 1073741824 then 4
       when filesize >= 1073741824 and filesize <= 1610612736 then 5
       when filesize >= 1610612736 and filesize <= 2147483648 then 6
       when filesize >= 2147483648 and filesize <= 2684354560  then 7
      else 8 end bin
      from requests
     where state = 'TapeRecall'
       and timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440 
       and filesize != 0
  )
order by bin;

/* Create the SVCClassStateBinRequestCounter view */  
CREATE OR REPLACE VIEW SVCClassStateBinRequestCounter AS
select sysdate timestamp, 300 interval, state, svcclass, to_char(bin,'HH24:MI') bin, number_of_req 
  from (
    select distinct trunc(timestamp,'Mi') bin, count(trunc(timestamp,'Mi')) over (Partition by trunc(timestamp,'Mi'), state, svcclass) number_of_req, state, svcclass
      from requests
     where timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440 
  )
order by bin;

/* Create the StateBinnedRequestCounter view */  
CREATE OR REPLACE VIEW StateBinnedRequestCounter AS
select sysdate timestamp, 300 interval, state, to_char(bin,'HH24:MI') bin, number_of_req 
  from (
    select distinct trunc(timestamp,'Mi') bin, count(trunc(timestamp,'Mi')) over (Partition by trunc(timestamp,'Mi'), state) number_of_req, state
      from requests
     where timestamp >= sysdate - 10/1440
       and timestamp < sysdate - 5/1440 
  )
order by bin;

/* Create the UserSVCClassStateHistogram view */  
CREATE OR REPLACE VIEW UserSVCClassStateHistogram AS
select sysdate timestamp, 300 interval, svcclass, username, count(1) total,count(case when state='DiskHit' then 1 else null end) dh, count(case when state='DiskCopy' then 1 else null end) dc, count(case when state='TapeRecall' then 1 else null end) tr, count(case when (state='TapeRecall' and type='StagePrepareToGetRequest') then 1 else null end) pretr, count(case when (state='TapeRecall' and type='StageGetRequest') then 1 else null end) immtr
  from requests
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440 
group by svcclass, username
order by total desc;

/* Create the UserSVCClassMigFilesCounter view */  
CREATE OR REPLACE VIEW UserSVCClassMigFilesCounter AS
select sysdate timestamp, 300 interval, svcclass, username, count(1) total
  from migration
 where timestamp >= sysdate - 10/1440
   and timestamp < sysdate - 5/1440 
group by svcclass, username
order by total desc;

/* Update and revalidation of PL-SQL code */
/******************************************/

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
