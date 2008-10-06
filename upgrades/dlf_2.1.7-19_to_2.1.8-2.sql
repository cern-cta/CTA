/******************************************************************************
 *              dlf_2.1.7-19_to_2.1.8-1.sql
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
 * @(#)$RCSfile: dlf_2.1.7-19_to_2.1.8-2.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/10/06 11:56:34 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.7-19 DLF database into v2.1.8-1
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM dlf_version WHERE release LIKE '2_1_7_19%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;

UPDATE dlf_version SET release = '2_1_8_1';
COMMIT;

/* Schema changes go here */
/**************************/
DELETE FROM dlf_facilities WHERE fac_name = 'rmmaster';
INSERT INTO dlf_facilities VALUES (26, 'Job');

/* Update and revalidation of all PL-SQL code */
/**********************************************/

/* PL/SQL method implementing statsLatency
 *
 * Provides statistics on the amount of time a user has had to wait since their
 * request was entered into the system and it actually being served. The returned
 * data is broken down by request type.
 */
CREATE OR REPLACE PROCEDURE statsLatency (now IN DATE) AS
BEGIN
  -- Stats table: LatencyStats
  -- Frequency: 5 minutes
  FOR a IN (
    SELECT type, count(*) started, min(waitTime) min, max(waitTime) max,
           avg(waitTime) avg, stddev_pop(waitTime) stddev, median(waitTime) median
      FROM (
        SELECT nvl(value, 'StageDiskCopyReplicaRequest') type, waitTime
          FROM (
            -- Extract the totalWaitTime for all stagerJobs or diskCopyTransfers
            -- which have started.
            SELECT params.id, params.value waitTime
              FROM dlf_messages messages, dlf_num_param_values params
             WHERE messages.id = params.id
               AND messages.severity = 8 -- System
               AND ((messages.facility = 26 AND messages.msg_no = 20)  -- Job started
                OR  (messages.facility = 23 AND messages.msg_no = 25)) -- DiskCopyTransfer started
               AND messages.timestamp >  sysdate - 10/1440
               AND messages.timestamp <= sysdate - 5/1440
               AND params.name = 'TotalWaitTime'
               AND params.timestamp >  sysdate - 10/1440
               AND params.timestamp <= sysdate - 5/1440
          ) results
      -- For facility 23 (DiskCopyTransfer) we can assume that the request type
      -- associated with the transfer is 133 (StageDiskCopyReplicaRequest).
      -- However, for normal jobs we must parse the Arguments attribute of the
      -- start message to determine the request type. Hence the left join,
      -- NULL's are 133!!
      LEFT JOIN dlf_str_param_values params
        ON results.id = params.id
       AND params.name = 'Type'
       AND params.timestamp >  sysdate - 10/1440
       AND params.timestamp <= sysdate - 5/1440)
     GROUP BY type
     ORDER BY type
  )
  LOOP
    INSERT INTO LatencyStats
      (timestamp, interval, type, started, minTime, maxTime, avgTime, stddevTime, medianTime)
    VALUES (now - 5/1440, 300, a.type, a.started, a.min, a.max, a.avg, a.stddev, a.median);
  END LOOP;
END;


/* Recompile all procedures */
/***************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM all_objects
             WHERE object_type = 'PROCEDURE'
               AND status = 'INVALID')
  LOOP
    EXECUTE IMMEDIATE 'ALTER PROCEDURE '||a.object_name||' COMPILE';
  END LOOP;
END;
