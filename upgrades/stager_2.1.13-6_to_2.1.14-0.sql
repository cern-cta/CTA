/******************************************************************************
 *                 stager_2.1.13-6_to_2.1.14-0.sql
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
 * This script upgrades a CASTOR v2.1.13-6 STAGER database to v2.1.14-0
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
   WHERE schemaVersion = '2_1_13_0'
     AND release = '2_1_14_0'
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
     AND release LIKE '2_1_13_6%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_13_0', '2_1_14_0', 'NON TRANSPARENT');
COMMIT;

/* Job management */
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs)
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name, force=>TRUE);
    END IF;
    -- Schedule the start date of the job to 15 minutes from now. This
    -- basically pauses the job for 15 minutes so that the upgrade can
    -- go through as quickly as possible.
    dbms_scheduler.set_attribute(a.job_name, 'START_DATE', SYSDATE + 15/1440);
  END LOOP;
END;
/

/* Schema changes */
/******************/

-- Drop the I_SubRequest_CT_ID index in case it was not dropped when deploying 2.1.13-6.
-- See SR #133974 for more details.
DECLARE
  unused VARCHAR2(2048);
BEGIN
  SELECT object_name INTO unused FROM User_Objects WHERE object_name = 'I_SUBREQUEST_CT_ID' AND ROWNUM < 2;
  EXECUTE IMMEDIATE 'DROP INDEX I_SUBREQUEST_CT_ID';
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
/

-- replace DiskCopy index on status 7 with one including the filesystem
DROP INDEX I_DiskCopy_Status_7;
CREATE INDEX I_DiskCopy_Status_7_FS ON DiskCopy (decode(status,7,status,NULL), fileSystem);

/* new rating of filesystems */
CREATE OR REPLACE FUNCTION fileSystemRate(nbReadStreams IN NUMBER, nbWriteStreams IN NUMBER)
RETURN NUMBER DETERMINISTIC IS
BEGIN
  RETURN - nbReadStreams - nbWriteStreams;
END;
/

/* change index on filesystem rating. */
DROP INDEX I_FileSystem_Rate;
CREATE INDEX I_FileSystem_Rate ON FileSystem(fileSystemRate(nbReadStreams, nbWriteStreams));

/* amend FileSystem and DiskServer tables */
ALTER TABLE FileSystem DROP (minFreeSpace, readRate, writeRate, nbReadWriteStreams);
ALTER TABLE DiskServer DROP (readRate, writeRate, nbReadStreams, nbWriteStreams, nbReadWriteStreams, nbMigratorStreams, nbRecallerStreams);
ALTER TABLE DiskServer ADD (lastHeartbeatTime NUMBER DEFAULT 0);

/* accessors to ObjStatus table */
CREATE OR REPLACE FUNCTION getObjStatusName(inObject VARCHAR2, inField VARCHAR2, inStatusCode INTEGER)
RETURN VARCHAR2 AS
  varstatusName VARCHAR2(2048);
BEGIN
  SELECT statusName INTO varstatusName
    FROM ObjStatus
   WHERE object = inObject
     AND field = inField
     AND statusCode = inStatusCode;
  RETURN varstatusName;
END;
/

CREATE OR REPLACE PROCEDURE setObjStatusName(inObject VARCHAR2, inField VARCHAR2,
                                             inStatusCode INTEGER, inStatusName VARCHAR2) AS
BEGIN
  INSERT INTO ObjStatus (object, field, statusCode, statusName)
  VALUES (inObject, inField, inStatusCode, inStatusName);
END;
/

-- drop obsoleted adminStatus fields
ALTER TABLE DiskServer DROP (adminStatus);
ALTER TABLE FileSystem DROP (adminStatus);
DELETE FROM ObjStatus
 WHERE object='FileSystem' AND field='adminStatus';
DELETE FROM ObjStatus
 WHERE object='DiskServer' AND field='adminStatus';

BEGIN
  setObjStatusName('StageRepackRequest', 'status', 6, 'REPACK_SUBMITTED');
END;
/

-- cleanup Type2Obj table
DELETE FROM Type2Obj
 WHERE object IN ('DiskServerStateReport', 'DiskServerMetricsReport', 'FileSystemStateReport',
                  'FileSystemMetricsReport', 'DiskServerAdminReport', 'FileSystemAdminReport',
                  'StreamReport', 'FileSystemStateAck', 'MonitorMessageAck', 'RmMasterReport');

-- drop unused function to elect rmmaster master and its related lock table
DROP FUNCTION isMonitoringMaster;
DROP TABLE RMMasterLock;

-- add the HeartbeatTimeout parameter with default value (180s)
INSERT INTO CastorConfig
  VALUES ('DiskServer', 'HeartbeatTimeout', '180', 'The maximum amount of time in seconds that a diskserver can spend without sending any hearbeat before it is automatically set to disabled state.');

-- introduce new statuses for read-only support
BEGIN
  setObjStatusName('DiskServer', 'status', 3, 'DISKSERVER_READONLY');
  setObjStatusName('FileSystem', 'status', 3, 'FILESYSTEM_READONLY');
END;
/

-- add online flag to diskservers 
ALTER TABLE DiskServer ADD (hwOnline INTEGER DEFAULT 0 CONSTRAINT NN_DiskServer_hwOnline NOT NULL);


/* PL/SQL code revalidation */
/****************************/


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM user_objects
             WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY')
               AND status = 'INVALID')
  LOOP
    IF a.object_type = 'PACKAGE BODY' THEN a.object_type := 'PACKAGE'; END IF;
    BEGIN
      EXECUTE IMMEDIATE 'ALTER ' ||a.object_type||' '||a.object_name||' COMPILE';
    EXCEPTION WHEN OTHERS THEN
      -- ignore, so that we continue compiling the other invalid items
      NULL;
    END;
  END LOOP;
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_14_0';
COMMIT;
