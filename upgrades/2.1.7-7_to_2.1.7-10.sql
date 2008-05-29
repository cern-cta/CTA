/******************************************************************************
 *              2.1.7-7_to_2.1.7-8.sql
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
 * @(#)$RCSfile: 2.1.7-7_to_2.1.7-10.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/05/29 13:18:12 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.7-7 database into v2.1.7-8
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_7_7%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;

UPDATE CastorVersion SET schemaVersion = '2_1_7_8', release = '2_1_7_8';
COMMIT;

/* schema changes */
DECLARE
  cnt NUMBER;
BEGIN
  -- Enable row movement on the StageDiskCopyReplicaRequest table. This was
  -- missing in the 2.1.6 upgrade
  SELECT count(*) INTO cnt
    FROM user_tables
   WHERE table_name = 'STAGEDISKCOPYREPLICAREQUEST'
     AND row_movement = 'N';
  IF cnt = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE StageDiskCopyReplicaRequest ENABLE ROW MOVEMENT';
  END IF;

  -- Add an index on the tape column of the segment table.
  SELECT count(*) INTO cnt
    FROM user_indexes
   WHERE index_name = 'I_SEGMENT_TAPE';
  IF cnt = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX I_SEGMENT_TAPE ON SEGMENT (TAPE)';
  END IF;
END;

/* Modifications to the segment table to support prioritisations */
ALTER TABLE Segment ADD priority INTEGER;

/* SQL statements for type PriorityMap */ 
CREATE TABLE PriorityMap (priority INTEGER, euid INTEGER, egid INTEGER, id INTEGER CONSTRAINT I_PriorityMap_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT; 
ALTER TABLE PriorityMap ADD CONSTRAINT I_Unique_Priority UNIQUE (euid, egid);
