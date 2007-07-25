/******************************************************************************
 *              2.1.3-23_to_2.1.4-0.sql
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
 * @(#)$RCSfile: 2.1.3-24_to_2.1.4-3.sql,v $ $Release: 1.2 $ $Release$ $Date: 2007/07/25 12:27:11 $ $Author: itglp $
 *
 * This script upgrades a CASTOR v2.1.3-23 database into v2.1.4-0
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release like '2_1_3_2%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

UPDATE CastorVersion SET schemaVersion = '2_1_4_0', release = '2_1_4_0';
COMMIT;

/* Apply schema changes */

ALTER TABLE SubRequest ADD (errorCode NUMBER, errorMessage VARCHAR2(2048), requestedFileSystems VARCHAR2(2048));
ALTER TABLE Client ADD (version VARCHAR2(2038));

DROP TABLE FilesDeletedProcOutput;
CREATE GLOBAL TEMPORARY TABLE FilesDeletedProcOutput (fileid NUMBER, nshost VARCHAR2(2048)) ON COMMIT PRESERVE ROWS;

/* From now on, all PL-SQL code is updated */
/*******************************************/

(oracleTrailer to be appended later)

