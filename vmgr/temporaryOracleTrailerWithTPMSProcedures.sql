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
 * @(#)$RCSfile: oracleCreate.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/11/06 13:18:27 $ $Author: waldron $
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_9_3';


CREATE OR REPLACE PROCEDURE VMGR_DELETE_POOL (
  pool_name IN VARCHAR2)
IS
  tmp_capacity           NUMBER;
  tmp_free_space         NUMBER;
  nonempty_pool_deletion EXCEPTION;
BEGIN

  -- Check whether the pool is empty or not - do not allow the removal a pool
  -- that still contains tapes.

  SELECT tot_free_space, capacity
    INTO tmp_free_space, tmp_capacity
    FROM vmgr_tape_pool
    WHERE name = pool_name;

  IF ((tmp_capacity > 0) OR (tmp_free_space > 0)) THEN
    RAISE nonempty_pool_deletion;
  END IF;

  DELETE FROM vmgr_tape_pool WHERE name = pool_name;

EXCEPTION
  WHEN nonempty_pool_deletion
  THEN
    RAISE_APPLICATION_ERROR (-20501,
      'Pool: ' || pool_name ||
      ' can not be deleted because it is not empty.' ||
      ' Please check: capacity and free space - they must be both 0.');
END VMGR_DELETE_POOL;
/


CREATE OR REPLACE PROCEDURE VMGR_INSERT_POOL (
  pool_name IN VARCHAR2,
  owner_uid IN NUMBER,
  owner_gid IN NUMBER)
IS
BEGIN
  INSERT INTO vmgr_tape_pool(name, owner_uid, gid, tot_free_space, capacity)
    VALUES (pool_name, owner_uid, owner_gid, 0, 0);
END VMGR_INSERT_POOL;
/


CREATE OR REPLACE PROCEDURE VMGR_MOVE_TAPE2POOL(
  vid_tape         IN VARCHAR2,
  destination_pool IN VARCHAR2)
IS
  destination_pool_check  VARCHAR2(15);
  estimated_free_tape     NUMBER;
  source_pool             VARCHAR2(15);
  tape_capacity           NUMBER;
  tape_density            VARCHAR2(8);
  model                   VARCHAR2(6);
  media_letter            VARCHAR2(1);
  no_pool                 EXCEPTION;
  no_tape                 EXCEPTION;
BEGIN
  -- Lock the row with the VID to keep the estimated_free_space and the
  -- capacity consistent

  BEGIN
    SELECT density, model, media_letter
      INTO tape_density, model, media_letter
      FROM  vmgr_tape_info
      WHERE vid = vid_tape FOR UPDATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      raise_application_error (-20502, 'Cannot find tape ' || vid_tape ||
        ' in the VMGR_TAPE_INFO table.' ||
        ' Perhaps it does not exist in the system.');
  END;

  SELECT native_capacity
    INTO tape_capacity
    FROM vmgr_tape_denmap
    WHERE
       md_density      = tape_density AND
       md_model        = model        AND
       md_media_letter = media_letter;

  SELECT estimated_free_space, poolname
    INTO estimated_free_tape, source_pool
    FROM vmgr_tape_side
    WHERE vid = vid_tape AND side = 0 FOR UPDATE;

  -- Check whether the destination pool actually exists.
  BEGIN
    SELECT name
      INTO destination_pool_check
      FROM vmgr_tape_pool
      WHERE name = destination_pool
      FOR UPDATE;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      raise_application_error (-20503, 'Destination pool ' || destination_pool
        || ' does not exist in the VMGR_TAPE_POOL table. Tape ' || vid_tape ||
        ' can not be moved.');
  END;

  -- Substract the tape's free space and capacity from the source pool
  UPDATE vmgr_tape_pool
    SET tot_free_space = (tot_free_space - estimated_free_tape),
        capacity = (capacity - tape_capacity)
    WHERE name = source_pool;

  -- Add the tape's free space and capacity from the source pool
  UPDATE vmgr_tape_pool
    SET tot_free_space = (tot_free_space + estimated_free_tape),
        capacity = (capacity + tape_capacity)
    WHERE name = destination_pool;

  -- Move the tape from the source pool to the destination pool
  UPDATE vmgr_tape_side
    SET poolname = destination_pool
    WHERE vid = vid_tape AND side=0;

--  INSERT INTO tpms_log_table (source, message)
--    VALUES ('VMGR_MOVE_TAPE2POOL', 'moved tape ' || vid_tape || ' from ' || source_pool || ' to ' || destination_pool);

  COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE VMGR_UPDATE_POOL(
  pool_name     IN VARCHAR2,
  tmp_owner_uid IN NUMBER,
  owner_gid     IN NUMBER)
IS
BEGIN
  UPDATE vmgr_tape_pool
    SET owner_uid = tmp_owner_uid, gid = owner_gid
    WHERE name = pool_name;
END VMGR_UPDATE_POOL;
/
