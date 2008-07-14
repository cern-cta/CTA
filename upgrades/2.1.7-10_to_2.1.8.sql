/******************************************************************************
 *              2.1.7-10_to_2.1.8.sql
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
 * @(#)$RCSfile: 2.1.7-10_to_2.1.8.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/07/14 12:41:44 $ $Author: itglp $
 *
 * This script upgrades a CASTOR v2.1.7-10 database into v2.1.8
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_7_10%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;

UPDATE CastorVersion SET castorVersion = '2_1_8_0', release = '2_1_8_x';
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

/* Schema changes go here */

-- Name remaining unnamed index
BEGIN
  FOR a IN (SELECT index_name
              FROM user_indexes
             WHERE table_name = 'TYPE2OBJ'
               AND index_name LIKE 'SYS_%')
  LOOP
    EXECUTE IMMEDIATE 'ALTER INDEX '|| a.index_name ||' RENAME TO I_Type2Obj_Obj';
  END LOOP;
END;

INSERT INTO Type2Obj (type, object) VALUES (159, 'VectorAddress');


/* Update and revalidation of all PL-SQL code */
/**********************************************/


