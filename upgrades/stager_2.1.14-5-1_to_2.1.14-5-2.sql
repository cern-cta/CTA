/******************************************************************************
 *                 stager_2.1.14-5-1_to_2.1.14-5-2.sql
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
 * This script upgrades a CASTOR v2.1.14-5-1 STAGER database to v2.1.14-5-2
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  -- If we have encountered an error rollback any previously non committed
  -- operations. This prevents the UPDATE of the UpgradeLog from committing
  -- inconsistent data to the database.
  ROLLBACK;
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_14_2'
     AND release = '2_1_14_5_2'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'STAGER'
     AND release = '2_1_14_5_1';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_5_2', 'TRANSPARENT');
COMMIT;

-- Fix constraint - transparent thanks to the NOVALIDATE clause
ALTER TABLE DiskCopy DROP CONSTRAINT CK_DiskCopy_GCType;
ALTER TABLE DiskCopy
  ADD CONSTRAINT CK_DiskCopy_GcType
  CHECK (gcType IN (0, 1, 2, 3, 4, 5, 6, 7)) ENABLE NOVALIDATE;

-- Draining schema change. Dropping the content because of the NOT NULL constraint.
TRUNCATE TABLE DrainingErrors;
ALTER TABLE DrainingErrors ADD (diskCopy INTEGER, timeStamp NUMBER CONSTRAINT NN_DrainingErrors_TimeStamp NOT NULL);

CREATE INDEX I_DrainingErrors_DC ON DrainingErrors (diskCopy);
ALTER TABLE DrainingErrors
  ADD CONSTRAINT FK_DrainingErrors_DC
    FOREIGN KEY (diskCopy)
    REFERENCES DiskCopy (id);

ALTER TABLE Disk2DiskCopyJob ADD (srcDcId INTEGER);

/* Recompile all procedures, triggers and functions */
BEGIN
  recompileAll();
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_14_5_2';
COMMIT;
