/******************************************************************************
 *              2.1.7-9_to_2.1.7-10.sql
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
 * @(#)$RCSfile: vdqm_2.1.7-9_to_2.1.7-10.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/06/19 12:53:37 $ $Author: murrayc3 $
 *
 * This script upgrades a CASTOR VDQM v2.1.7-9 database into v2.1.7-10
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_7_9%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;

UPDATE CastorVersion SET release = '2_1_7_10';
COMMIT;

/* Schema changes go here */

CREATE OR REPLACE VIEW TapeDriveShowqueues_VIEW AS SELECT
  TapeDrive.status,
  TapeDrive.id,
  TapeDrive.runningTapeReq,
  TapeDrive.jobId,
  TapeDrive.modificationTime,
  TapeDrive.resetTime,
  TapeDrive.useCount,
  TapeDrive.errCount,
  TapeDrive.transferredMB,
  TapeAccessSpecification.accessMode AS tapeAccessMode,
  TapeDrive.totalMB,
  TapeServer.serverName,
  VdqmTape.vid,
  TapeDrive.driveName,
  DeviceGroupName.dgName,
  castorVdqmView.getVdqmDedicate(TapeDrive.Id) AS dedicate
FROM
  TapeDrive
LEFT OUTER JOIN TapeServer ON
  TapeDrive.tapeServer = TapeServer.id
LEFT OUTER JOIN VdqmTape ON
  TapeDrive.tape = VDQMTAPE.ID
LEFT OUTER JOIN DEVICEGROUPNAME ON
  TapeDrive.deviceGroupName = DeviceGroupName.id
LEFT OUTER JOIN TapeDriveDedication ON
  TapeDrive.id = TapeDriveDedication.tapeDrive
LEFT OUTER JOIN TapeRequest ON
  TapeDrive.RunningTapeReq = TapeRequest.id
LEFT OUTER JOIN TapeAccessSpecification ON
  TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
ORDER BY
  dgName ASC,
  driveName ASC;
