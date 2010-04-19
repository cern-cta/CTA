/******************************************************************************
 *              prevRelease_to_newRelease.sql
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
 * @(#)$RCSfile: template.sql,v $ $Release: 1.2 $ $Release$ $Date: 2009/02/16 09:13:00 $ $Author: waldron $
 *
 * This script upgrades a CASTOR vprevRelease DBNAME database to vnewRelease
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = 'schemaTag'
     AND release = 'newRelTag'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE release LIKE 'prevRelTag%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('schemaTag', 'newRelTag', 'TRANSPARENT');
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

/* Schema changes go here */
/**************************/


/* Update and revalidation of PL-SQL code */
/******************************************/



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
 WHERE release = 'newRelTag';
COMMIT;
