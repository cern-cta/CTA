/******************************************************************************
 *                 cns_2.1.14_switch_openmode.sql
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
 * This script switches the Open mode from Compatibility to New.
 * It is applicable on any CASTOR v2.1.14-x CNS database. See release notes
 * for version 2.1.14-2 for more details.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE;

/* Verify that the script is running against the correct schema and version,
   and check that it is safe to switch the Open mode. */
DECLARE
  unused VARCHAR(100);
BEGIN
  BEGIN
    SELECT release INTO unused FROM CastorVersion
     WHERE schemaName = 'CNS'
       AND release LIKE '2_1_14_%';
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- Error, we cannot apply this script
    raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the CNS before this one.');
  END;
  BEGIN
    SELECT 1 INTO unused FROM Cns_file_metadata
     WHERE stagerTime IS NULL
       AND ROWNUM < 2;
    -- we found at least one file with stagerTime not set, complain
    raise_application_error(-20000, 'Cannot proceed, at least one file has its stagertime field set to NULL');
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- that's what we want
    NULL;
  END;
END;
/

-- add constraint now that all rows are populated
ALTER TABLE Cns_file_metadata ADD CONSTRAINT NN_File_stagerTime (stagerTime NOT NULL);

-- update configuration
UPDATE CastorConfig SET value = 'N' WHERE class = 'stager' AND key = 'openmode';
COMMIT;
