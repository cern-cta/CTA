/******************************************************************************
 *              vmgr_2.1.10-1_to_2.1.10-0.sql
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
 * This script upgrades a CASTOR v2.1.10-0 VMGR database to v2.1.10-0
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  -- If we've encountered an error rollback any previously non committed
  -- operations. This prevents the UPDATE of the UpgradeLog from committing
  -- inconsistent data to the database.
  ROLLBACK;
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_10_0'
     AND release = '2_1_10_0'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'VMGR'
     AND release LIKE '2_1_10_1%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the VMGR before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_10_0', '2_1_10_0', 'NON TRANSPARENT');
COMMIT;

/* Synchronise the tot_free_space column of the vmgr_tape_pool table with */
/* the estimated_free_space column of the vmgr_tape_side table as these   */
/* columns drift out of sync over time                                    */
UPDATE vmgr_tape_pool
   SET vmgr_tape_pool.tot_free_space = (
         SELECT NVL(SUM(vmgr_tape_side.estimated_free_space), 0)
           FROM vmgr_tape_side
          WHERE vmgr_tape_side.poolName = vmgr_tape_pool.name
            AND vmgr_tape_side.status in (0 /* FREE */, 4 /* BUSY */));

/* Convert all byte values in the database to kibibyte values */
UPDATE vmgr_tape_denmap
   SET native_capacity = native_capacity / 1024;
UPDATE vmgr_tape_pool
   SET capacity = capacity / 1024;
UPDATE vmgr_tape_pool
   SET tot_free_space = tot_free_space / 1024;
UPDATE vmgr_tape_side
   SET estimated_free_space = estimated_free_space / 1024;
COMMIT;

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
 WHERE release = '2_1_10_0';
COMMIT;
