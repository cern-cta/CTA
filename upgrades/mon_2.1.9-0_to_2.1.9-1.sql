/******************************************************************************
 *              mon_2.1.9-0_to_2.1.9-1.sql
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
 * This script upgrades a CASTOR v2.1.9-0 MONITORING database into v2.1.9-1
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_9_0%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

/* Determine the DLF schema that the monitoring procedures should run against */
UNDEF dlfschema
ACCEPT dlfschema DEFAULT castor_dlf PROMPT 'Enter the DLF schema to run monitoring queries against: (castor_dlf) ';
SET VER OFF

UPDATE CastorVersion SET release = '2_1_9_1';
COMMIT;


/* Update and revalidation of PL-SQL code */
/******************************************/

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
           -- srmbed
            OR  (messages.facility = 14 -- srmbed
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
