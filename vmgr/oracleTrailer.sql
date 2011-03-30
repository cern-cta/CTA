/*****************************************************************************
 *              oracleTrailer.sql
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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_10_1';


/**
 * Deletes the tape pool with the specified name from the VMGR database.
 *
 * This procedure raises application error number -20501 if the specified
 * tape pool does not exist in the VMGR database.
 *
 * This procedure raises application error number -20502 if the specified
 * tape pool is not empty.
 *
 * @param pool_name_var The name of the tape pool to be deleted.
 */
CREATE OR REPLACE PROCEDURE VMGR_DELETE_POOL (
  pool_name_var IN VARCHAR2)
IS
  capacity_var   NUMBER := NULL;
  free_space_var NUMBER := NULL;
BEGIN

  BEGIN
    -- Lock the database row representing the tape pool
    --
    -- The lock is required to make sure the following check to see if the pool
    -- still contains tapes is still valid when and the deletion of the row is
    -- carried out
    SELECT tot_free_space, capacity
      INTO free_space_var, capacity_var
      FROM vmgr_tape_pool
      WHERE name = pool_name_var
      FOR UPDATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR (-20501,
        'The ' || pool_name_var ||
        ' tape pool cannot be deleted,' ||
        ' because it does not exist in the VMGR database.');
  END;

  -- The tape pool must not be deleted if it is not empty
  IF ((capacity_var > 0) OR (free_space_var > 0)) THEN
    RAISE_APPLICATION_ERROR (-20502,
      'The ' || pool_name_var ||
      ' tape pool cannot be deleted, because it is not empty.' ||
      ' Both the capacity and the free space of the tape pool must be 0.');
  END IF;

  -- It is now safe to delete the tape pool
  DELETE FROM vmgr_tape_pool WHERE name = pool_name_var;

END VMGR_DELETE_POOL;
/


/**
 * Inserts the specified new tape pool into the VMGR database.
 *
 * This procedure raises application error number -20503 if the specified
 * tape pool already exists in the VMGR database.
 *
 * @param pool_name_var The name of the new tape pool.
 * @param owner_uid_var The user ID of the owner of the new tape pool.
 * @param owner_gid_var The group ID of the owner of the new tape pool.
 */
CREATE OR REPLACE PROCEDURE VMGR_INSERT_POOL (
  pool_name_var IN VARCHAR2,
  owner_uid_var IN NUMBER,
  owner_gid_var IN NUMBER)
IS
BEGIN
  INSERT INTO vmgr_tape_pool(name, owner_uid, gid, tot_free_space, capacity)
    VALUES (pool_name_var, owner_uid_var, owner_gid_var, 0, 0);
EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    RAISE_APPLICATION_ERROR (-20503,
      'The ' || pool_name_var ||
      ' tape pool cannot be inserted,' ||
      ' because it already exists in the VMGR database');
END VMGR_INSERT_POOL;
/


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


/**
 * Updates the owner user ID and group ID of the specified tape pool.
 *
 * This procedure raises application error number -20508 if the specified
 * tape pool does not exist in the VMGR database.
 *
 * @param pool_name_var The name of the tape pool to be updated.
 * @param owner_uid_var The new owner user ID.
 * @param owner_gid_var The new owner group ID.
 */
CREATE OR REPLACE PROCEDURE VMGR_UPDATE_POOL_OWNER(
  pool_name_var IN VARCHAR2,
  owner_uid_var IN NUMBER,
  owner_gid_var IN NUMBER)
IS
BEGIN
  UPDATE vmgr_tape_pool SET
      owner_uid = owner_uid_var,
      gid       = owner_gid_var
    WHERE name = pool_name_var;

  IF SQL%NOTFOUND THEN -- Update did not modify any rows
    RAISE_APPLICATION_ERROR(-20508, 'The ' || pool_name_var || ' tape pool' ||
      ' cannot be updated, because it does not exist in the VMGR database.');
  END IF;
END VMGR_UPDATE_POOL_OWNER;
/

/*
 * Create and populate the table VMGR_TAPE_STATUS_CODE.
 *
 * The first BEGIN-END block check if the table already exist, an if not
 * it create the table. The second BEGIN-END block populate the table
 * with the vmgr status number (from 0 to 63) and the corresponding
 * string value generated following this bit set convencion:
 * 	  DISABLED =  1  (000001)
 *        EXPORTED =  2  (000010)
 *        BUSY     =  4  (000100)
 *        FULL     =  8  (001000)
 *        RDONLY   = 16  (010000)
 *        ARCHIVED = 32  (100000)
 */
DECLARE
  nbTable         INTEGER        := 0;
  createTableStmt VARCHAR2(1024) :=
    'CREATE TABLE vmgr_tape_status_code ('                                         ||
    '  status_number NUMBER CONSTRAINT NN_VmgrTapeStatusCodes_Number NOT NULL, '   ||
    '  status_string VARCHAR2(100)   , '                                           ||
    '  CONSTRAINT PK_VmgrTapeStatusCodes_number PRIMARY KEY (status_number))';
BEGIN
  SELECT COUNT(*) INTO nbTable FROM USER_TABLES
   WHERE TABLE_NAME = 'VMGR_TAPE_STATUS_CODE';
  -- If the table still do not exist in the DB it will create it
  IF nbTable = 0 THEN
    DBMS_OUTPUT.PUT_LINE(createTableStmt);
    EXECUTE IMMEDIATE createTableStmt;
  END IF;
END;
/

DECLARE
  bitExponent         INTEGER := 0;
  bitValue            INTEGER := 0;
  alreadyFoundASetBit BOOLEAN := FALSE;
  status_str          VARCHAR2(100);
  nbTable             INTEGER := 0;
BEGIN

  SELECT COUNT(*) INTO nbTable FROM VMGR_TAPE_STATUS_CODE;
  -- If the table exist and is empty, then populate it
  IF nbTable = 0 THEN

    FOR statusNb IN 0..63 LOOP
      status_str := '';
      alreadyFoundASetBit := FALSE;
      FOR bitExponent IN REVERSE 0..5 LOOP
        bitValue := 2 ** bitExponent;

        -- Mask all other bits except for the one in "bitExponent" position
        IF BITAND(statusNb, bitValue) > 0 THEN
          IF alreadyFoundASetBit THEN
            status_str := status_str || '|';
          END IF;
          CASE bitValue
            WHEN  1 THEN status_str := status_str || 'DISABLED';
            WHEN  2 THEN status_str := status_str || 'EXPORTED';
            WHEN  4 THEN status_str := status_str || 'BUSY';
            WHEN  8 THEN status_str := status_str || 'FULL';
            WHEN 16 THEN status_str := status_str || 'RDONLY';
            WHEN 32 THEN status_str := status_str || 'ARCHIVED';
            ELSE         status_str := status_str || 'UNKNOWN';
          END CASE;
          alreadyFoundASetBit := TRUE;
        END IF;
      END LOOP;
      -- User visual feedback
      DBMS_OUTPUT.PUT_LINE(statusNb ||' '||  status_str);
      INSERT INTO VMGR_TAPE_STATUS_CODE(STATUS_NUMBER, STATUS_STRING)
      VALUES(statusNb, status_str);
    END LOOP;
    COMMIT;

  END IF;
END;
/

