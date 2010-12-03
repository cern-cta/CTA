/******************************************************************************
 *              vdqm_2.1.9-10_to_2.1.10-0.sql
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
 * This script upgrades a CASTOR v2.1.9-10 VDQM database to v2.1.10-0
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_8_3'
     AND release = '2_1_10_0'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'VDQM'
     AND release LIKE '2_1_9_10%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the VDQM before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_8_3', '2_1_10_0', 'TRANSPARENT');
COMMIT;

/* Schema changes go here */
/**************************/

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
LEFT OUTER JOIN TapeRequest ON
  TapeDrive.RunningTapeReq = TapeRequest.id
LEFT OUTER JOIN TapeAccessSpecification ON
  TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
ORDER BY
  dgName ASC,
  driveName ASC;

/**
 * This view shows candidate tape drive allocations before any dedications
 * have been taken into account.
 */
CREATE OR REPLACE VIEW PotentialMounts_VIEW AS SELECT UNIQUE
  TapeDrive.id as driveId,
  TapeDrive.driveName,
  TapeRequest.id as tapeRequestId,
  ClientIdentification.euid as clientEuid,
  ClientIdentification.egid as clientEgid,
  ClientIdentification.machine as clientMachine,
  TapeAccessSpecification.accessMode,
  VdqmTape.vid,
  TapeRequest.modificationTime,
  TapeRequest.creationTime,
  NVL(EffectiveVolumePriority_VIEW.priority,0) AS volumePriority
FROM
  TapeRequest
INNER JOIN TapeAccessSpecification ON
  TapeRequest.tapeAccessSpecification = TapeAccessSpecification.id
INNER JOIN VdqmTape ON
  TapeRequest.tape = VdqmTape.id
INNER JOIN ClientIdentification ON
  TapeRequest.client = ClientIdentification.id
INNER JOIN TapeDrive ON
  TapeRequest.deviceGroupName = TapeDrive.deviceGroupName
  AND (
    TapeRequest.requestedSrv IS NULL
    OR TapeRequest.requestedSrv = TapeDrive.tapeServer
  )
INNER JOIN TapeServer ON
  TapeDrive.tapeServer = TapeServer.id
LEFT OUTER JOIN EffectiveVolumePriority_VIEW ON
  VdqmTape.vid = EffectiveVolumePriority_VIEW.vid
  AND TapeAccessSpecification.accessMode = EffectiveVolumePriority_VIEW.tpMode
WHERE
  TapeDrive.status=0 -- UNIT_UP
  -- Exclude a request if its tape is associated with an on-going request
  AND NOT EXISTS (
    SELECT
      'x'
    FROM
      TapeRequest TapeRequest2
    WHERE
      TapeRequest2.tape = TapeRequest.tape
      AND TapeRequest2.tapeDrive IS NOT NULL
  )
  -- Exclude a request if its tape is already in a drive, such a request
  -- will be considered upon the release of the drive in question
  -- (cf. TapeDriveStatusHandler)
  AND NOT EXISTS (
    SELECT
      'x'
    FROM
      TapeDrive TapeDrive2
    WHERE
      TapeDrive2.tape = TapeRequest.tape
  )
  AND TapeServer.actingMode=0 -- TAPE_SERVER_ACTIVE
  AND TapeRequest.status=0 -- REQUEST_PENDING
ORDER BY
  TapeAccessSpecification.accessMode DESC,
  VolumePriority DESC,
  TapeRequest.creationTime ASC,
  DBMS_RANDOM.value;

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
 WHERE release = '2_1_10_0';
COMMIT;
