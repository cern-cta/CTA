/******************************************************************************
 *              dlf_2.1.8-3_to_2.1.8-4.sql
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
 * @(#)$RCSfile: dlf_2.1.8-3_to_2.1.8-4.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/11/28 16:34:01 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.8-3 DLF database into v2.1.8-4
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM dlf_version WHERE release LIKE '2_1_8_3%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

UPDATE dlf_version SET release = '2_1_8_4';
COMMIT;


/* PL/SQL method implementing statsDiskCachEfficiency
 *
 * Provides an overview of how effectively the disk cache is performing. For example,
 * the greater the number of recalls the less effective the cache is.
 *
 * Example output:
 *   Type            SvcClass      Wait D2D  Recall Staged Total
 *   StageGetRequest dteam         0    0    0      3      3
 *   StageGetRequest compasschunks 0    0    0      1      1
 *   StageGetRequest na48          0    0    0      71     71
 *   StageGetRequest compassmdst   0    0    0      1      1
 *   StageGetRequest compass004d   0    0    0      55     55
 *   StageGetRequest compasscdr    0    0    1      1      2
 *   StageGetRequest na48goldcmp   0    0    0      154    154
 *   StageGetRequest default       0    0    0      100    100
 */
CREATE OR REPLACE PROCEDURE statsDiskCacheEfficiency (now IN DATE) AS
BEGIN
  -- Stats table: DiskCacheEfficiencyStats
  -- Frequency: 5 minutes

  -- Collect a list of request ids that we are interested in. We dump this list into
  -- a temporary table so that the execution plan of the query afterwards is optimized
  INSERT INTO CacheEfficiencyHelper
    SELECT messages.reqid
      FROM dlf_messages messages
     WHERE messages.severity = 10 -- Monitoring
       AND messages.facility = 4  -- RequestHandler
       AND messages.msg_no = 10   -- Reply sent to client
       AND messages.timestamp >  now - 10/1440
       AND messages.timestamp <= now - 5/1440;

  -- Record results
  FOR a IN (
    SELECT type, svcclass,
           nvl(sum(decode(msg_no, 53, requests, 0)), 0) Wait,
           nvl(sum(decode(msg_no, 56, requests, 0)), 0) D2D,
           nvl(sum(decode(msg_no, 57, requests, 0)), 0) Recall,
           nvl(sum(decode(msg_no, 60, requests, 0)), 0) Staged,
           nvl(sum(requests), 0) total
      FROM (
        SELECT type, svcclass, msg_no, count(*) requests FROM (
          SELECT * FROM (
            -- Get the first message issued for all subrequests of interest. This
            -- will indicate to us whether the request was a hit or a miss
            SELECT msg_no, type, svcclass,
                   RANK() OVER (PARTITION BY subreqid
                          ORDER BY timestamp ASC, timeusec ASC) rank
              FROM (
                -- Extract all subrequests processed by the stager that resulted in
                -- read type access
                SELECT messages.reqid, messages.subreqid, messages.timestamp,
                       messages.timeusec, messages.msg_no,
                       max(decode(params.name, 'Type',     params.value, NULL)) type,
                       max(decode(params.name, 'SvcClass', params.value, 'default')) svcclass
                  FROM dlf_messages messages, dlf_str_param_values params
                 WHERE messages.id = params.id
                   AND messages.severity = 8  -- System
                   AND messages.facility = 22 -- Stager
                   AND messages.msg_no IN (53, 56, 57, 60)
                   AND messages.timestamp >  now - 10/1440
                   AND messages.timestamp <= now - 3/1440
                   AND params.name IN ('Type', 'SvcClass')
                   AND params.timestamp >  now - 10/1440
                   AND params.timestamp <= now - 3/1440
                 GROUP BY messages.reqid, messages.subreqid, messages.timestamp,
                          messages.timeusec, messages.msg_no
              ) results
             -- Filter the subrequests so that we only process requests which entered
             -- the system through the request handler in the last sampling interval.
             -- This stops us from recounting subrequests that were restarted
             WHERE results.reqid IN 
               (SELECT /*+ CARDINALITY(helper 1000) */ * 
                  FROM CacheEfficiencyHelper helper))
         ) WHERE rank = 1
       GROUP BY type, svcclass, msg_no)
     GROUP BY type, svcclass
  )
  LOOP
    INSERT INTO DiskCacheEfficiencyStats
      (timestamp, interval, wait, type, svcclass, d2d, recall, staged, total)
    VALUES (now - 5/1440, 300, a.wait, a.type, a.svcclass, a.d2d, a.recall, a.staged, a.total);
  END LOOP;
END;
/


/* Recompile all procedures */
/****************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM all_objects
             WHERE object_type = 'PROCEDURE'
               AND status = 'INVALID')
  LOOP
    EXECUTE IMMEDIATE 'ALTER PROCEDURE '||a.object_name||' COMPILE';
  END LOOP;
END;
/
