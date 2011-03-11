/******************************************************************************
 *              repack_2.1.10-0_to_2.1.10-1.sql
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
 * This script upgrades a CASTOR v2.1.10-0 REPACK database to v2.1.10-1
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
   WHERE schemaVersion = '2_1_8_0'
     AND release = '2_1_10_1'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'REPACK'
     AND release LIKE '2_1_10%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the REPACK before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_8_0', '2_1_10_1', 'TRANSPARENT');
COMMIT;

/* Update and revalidation of PL-SQL code */
/******************************************/

CREATE OR REPLACE PROCEDURE repackCleanup AS
  t INTEGER;
  srIds "numList";
  segIds "numList";
  rIds "numList";
BEGIN
  -- First perform some cleanup of old stuff:
  -- for each, read relevant timeout from configuration table
  SELECT TO_NUMBER(value) INTO t FROM RepackConfig
   WHERE class = 'Repack'
     AND key = 'CleaningTimeout'
     AND ROWNUM < 2;
  SELECT id BULK COLLECT INTO srIds
    FROM RepackSubrequest
   WHERE status = 8
     AND submittime < gettime() + t * 3600;

  IF srIds.COUNT > 0 THEN
    -- Delete segments
    FOR i IN srIds.FIRST .. srIds.LAST LOOP
      DELETE FROM RepackSegment WHERE RepackSubrequest = srIds(i)
        RETURNING id BULK COLLECT INTO segIds;
      FORALL j IN segIds.FIRST .. segIds.LAST 
        DELETE FROM Id2Type WHERE id = segIds(j);
    END LOOP;
    COMMIT;
  END IF;
  
  -- Delete subrequests
  FORALL i IN srIds.FIRST .. srIds.LAST 
    DELETE FROM RepackSubrequest WHERE id = srIds(i);
  FORALL i IN srIds.FIRST .. srIds.LAST  
    DELETE FROM id2type WHERE id = srIds(i);

  -- Delete requests without any other subrequest
  DELETE FROM RepackRequest A WHERE NOT EXISTS 
    (SELECT 'x' FROM RepackSubRequest WHERE A.id = repackrequest)
    RETURNING A.id BULK COLLECT INTO rIds;
  FORALL i IN rIds.FIRST .. rIds.LAST
    DELETE FROM Id2Type WHERE id = rIds(i);
  COMMIT;

  -- Loop over all tables which support row movement and recover space from
  -- the object and all dependant objects. We deliberately ignore tables
  -- with function based indexes here as the 'shrink space' option is not
  -- supported.
  FOR t IN (SELECT table_name FROM user_tables
             WHERE row_movement = 'ENABLED'
               AND table_name NOT IN (
                 SELECT table_name FROM user_indexes
                  WHERE index_type LIKE 'FUNCTION-BASED%')
               AND temporary = 'N')
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '|| t.table_name ||' SHRINK SPACE CASCADE';
  END LOOP;
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

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_10_1';
COMMIT;
