/******************************************************************************
 *              repack_2.1.7-10_to_2.1.7-11.sql
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
 * @(#)$RCSfile: repack_2.1.7-10_to_2.1.7-12.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/07/21 09:37:58 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.7-10 REPACK database to 2.1.7-11
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus and sql developer */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_7_10%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
 raise_application_error(-20000, 'PL/SQL revision mismatch. Please run previous upgrade scripts before this one.');
END;

UPDATE CastorVersion SET release = '2_1_7_11';
COMMIT;
