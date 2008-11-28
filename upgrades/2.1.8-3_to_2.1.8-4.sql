/******************************************************************************
 *              2.1.8-3_to_2.1.8-4.sql
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
 * @(#)$RCSfile: 2.1.8-3_to_2.1.8-4.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/11/28 16:34:01 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.8-3 STAGER database into v2.1.8-4
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_8_3%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

UPDATE CastorVersion SET release = '2_1_8_4';
COMMIT;

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

/* Rename all unique constraints */
BEGIN
  FOR a IN (SELECT constraint_name old_name, 
                   substr(constraint_name, 2, length(constraint_name)) new_name, 
                   table_name
              FROM user_constraints
             WHERE constraint_type = 'U'
               AND constraint_name NOT LIKE 'UN_%')
  LOOP
   EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||' 
                      RENAME CONSTRAINT '||a.old_name||' TO '||a.new_name;
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
