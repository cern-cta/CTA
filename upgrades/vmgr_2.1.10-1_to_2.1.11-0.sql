/******************************************************************************
 *              vmgr_2.1.10-1_to_2.1.11-0.sql
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
 * This script upgrades a CASTOR v2.1.10-1 VMGR database to v2.1.11-0
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
   WHERE schemaVersion = '2_1_10_1'
     AND release = '2_1_11_0'
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
VALUES ('2_1_10_1', '2_1_11_0', 'TRANSPARENT');
COMMIT;

/* Schema changes go here */
/**************************/

/* Fix SYS_ generated constraint name on the Vmgr_Tape_Status_Code table */
DECLARE
  constraintName VARCHAR2(30);
BEGIN
  SELECT constraint_name INTO constraintName
    FROM user_cons_columns
   WHERE table_name = 'VMGR_TAPE_STATUS_CODE'
     AND column_name = 'STATUS_NUMBER'
     AND position IS NULL
     AND constraint_name LIKE 'SYS_%';
  EXECUTE IMMEDIATE 'ALTER TABLE Vmgr_Tape_Status_Code
                     RENAME CONSTRAINT '||constraintName||'
                     TO NN_VmgrTapeStatusCodes_Number';

EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
/

/* Update and revalidation of PL-SQL code */
/******************************************/

/**
 * Moves the specified tape to the specified destination tape pool.
 *
 * This procedure raises application error number -20504 if the specified
 * tape does not exist in the VMGR database.
 *
 * This procedure raises application error number -20505 if the capacity of the
 * specified tape does not exist in the VMGR database.
 *
 * This procedure raises application error number -20506 if the information
 * about the sides of the specified tape does not exist in the VMGR database.
 *
 * This procedure raises application error number -20507 if the specified
 * destination pool does not exist in the VMGR database.
 *
 * Developers please note that this procedure takes several locks which are and
 * must be taken in the following order:
 *
 * 1. vmgr_tape_info
 * 2. vmgr_tape_side
 * 3. vmgr_tape_pool
 *
 * @param vid_tape_var         The VID of the tape to be moved.
 * @param destination_pool_var The name of the destination tape pool.
 */
CREATE OR REPLACE PROCEDURE VMGR_MOVE_TAPE2POOL(
  vid_tape_var         IN VARCHAR2,
  destination_pool_var IN VARCHAR2)
IS
  dummy_destination_pool_var VARCHAR2(15);
  estimated_free_space_var   NUMBER;
  source_pool_var            VARCHAR2(15);
  tape_capacity_var          NUMBER;
  tape_density_var           VARCHAR2(8);
  model_var                  VARCHAR2(6);
  media_letter_var           VARCHAR2(1);
BEGIN
  BEGIN
    -- Lock the the vmgr_tape_info row
    SELECT density, model, media_letter
      INTO tape_density_var, model_var, media_letter_var
      FROM  vmgr_tape_info
      WHERE vid = vid_tape_var
      FOR UPDATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR(-20504, 'The ' || vid_tape_var || ' tape' ||
        ' cannot be moved, because it does not exist in the VMGR database.');
  END;

  BEGIN
    -- Get the capacity of the tape
    SELECT native_capacity
      INTO tape_capacity_var
      FROM vmgr_tape_denmap
      WHERE
         md_density      = tape_density_var AND
         md_model        = model_var        AND
         md_media_letter = media_letter_var;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR(-20505, 'The ' || vid_tape_var || ' tape' ||
        ' cannot be moved, because the capacity of the tape does not exist' ||
        ' in the VMGR database.');
  END;

  BEGIN
    -- Lock the tape side info row
    SELECT estimated_free_space    , poolname
      INTO estimated_free_space_var, source_pool_var
      FROM vmgr_tape_side
      WHERE vid = vid_tape_var AND side = 0
      FOR UPDATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR(-20506, 'The ' || vid_tape_var || ' tape' ||
        ' cannot be moved, because the information about the sides of the' ||
        ' tape is not in the database.');
  END;

  BEGIN
    -- Lock the tape pool row
    SELECT name
      INTO dummy_destination_pool_var
      FROM vmgr_tape_pool
      WHERE name = destination_pool_var
      FOR UPDATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      raise_application_error (-20507, 'The ' || vid_tape_var || ' tape' ||
        ' cannot be moved, because the destination pool ' ||
        destination_pool_var ||
        ' does not exist in the VMGR database.');
  END;

  -- Substract the tape's free space and capacity from the source pool
  UPDATE vmgr_tape_pool
    SET
      tot_free_space = tot_free_space - estimated_free_space_var,
      capacity       = capacity       - tape_capacity_var
    WHERE name = source_pool_var;

  -- Add the tape's free space and capacity from the source pool
  UPDATE vmgr_tape_pool
    SET
      tot_free_space = tot_free_space + estimated_free_space_var,
      capacity       = capacity       + tape_capacity_var
    WHERE name = destination_pool_var;

  -- Move the tape from the source pool to the destination pool
  UPDATE vmgr_tape_side
    SET poolname = destination_pool_var
    WHERE
      vid  = vid_tape_var AND
      side = 0;
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
 WHERE release = '2_1_11_0';
COMMIT;
