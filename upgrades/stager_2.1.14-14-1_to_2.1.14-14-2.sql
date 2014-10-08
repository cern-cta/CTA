/******************************************************************************
 *                 stager_2.1.14-14-1_to_2.1.14-14-2.sql
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
 * This script upgrades a CASTOR v2.1.14-14-1 STAGER database to v2.1.14-14-2
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
     AND release = '2_1_14_14_2'
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
     AND release = '2_1_14_14_1';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the STAGER before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_14_2', 'TRANSPARENT');
COMMIT;

/* Schema changes go here */
/**************************/
DECLARE
  unused VARCHAR2(2048);
BEGIN
  SELECT value INTO unused FROM CastorConfig
   WHERE class = 'Rebalancing' AND key = 'MaxDataScheduled';
EXCEPTION WHEN NO_DATA_FOUND THEN
  INSERT INTO CastorConfig
    VALUES ('Rebalancing', 'MaxNbFilesScheduled', '1000', 'The maximum number of disk to disk copies that each rebalancing run should send to the scheduler concurrently.');
  INSERT INTO CastorConfig
    VALUES ('Rebalancing', 'MaxDataScheduled', '10000000000', 'The maximum amount of data that each rebalancing run should send to the scheduler in one go.');
END;
/

/* Procedure responsible for rebalancing one given filesystem by moving away
 * the given amount of data */
CREATE OR REPLACE PROCEDURE rebalance(inFsId IN INTEGER, inDataAmount IN INTEGER,
                                      inDestSvcClassId IN INTEGER,
                                      inDiskServerName IN VARCHAR2, inMountPoint IN VARCHAR2) AS
  CURSOR DCcur IS
    SELECT /*+ FIRST_ROWS_10 */
           DiskCopy.id, DiskCopy.diskCopySize, CastorFile.id, CastorFile.nsOpenTime
      FROM DiskCopy, CastorFile
     WHERE DiskCopy.fileSystem = inFsId
       AND DiskCopy.status = dconst.DISKCOPY_VALID
       AND CastorFile.id = DiskCopy.castorFile;
  varDcId INTEGER;
  varDcSize INTEGER;
  varCfId INTEGER;
  varNsOpenTime INTEGER;
  varTotalRebalanced INTEGER := 0;
  varNbFilesRebalanced INTEGER := 0;
  varMaxNbFilesScheduled INTEGER := TO_NUMBER(getConfigOption('Rebalancing', 'MaxNbFilesScheduled', '1000'));
BEGIN
  -- disk to disk copy files out of this node until we reach inDataAmount
  -- "rebalancing : starting" message
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REBALANCING_START, 0, '', 'stagerd',
           'DiskServer=' || inDiskServerName || ' mountPoint=' || inMountPoint ||
           ' dataToMove=' || TO_CHAR(TRUNC(inDataAmount)));
  -- Loop on candidates until we can lock one
  OPEN DCcur;
  LOOP
    -- Fetch next candidate
    FETCH DCcur INTO varDcId, varDcSize, varCfId, varNsOpenTime;
    -- no next candidate : this is surprising, but nevertheless, we should go out of the loop
    IF DCcur%NOTFOUND THEN EXIT; END IF;
    -- stop if it would be too much
    IF varTotalRebalanced + varDcSize > inDataAmount
      OR varNbFilesRebalanced > varMaxNbFilesScheduled THEN EXIT; END IF;
    -- compute new totals
    varTotalRebalanced := varTotalRebalanced + varDcSize;
    varNbFilesRebalanced := varNbFilesRebalanced + 1;
    -- create disk2DiskCopyJob for this diskCopy
    createDisk2DiskCopyJob(varCfId, varNsOpenTime, inDestSvcClassId,
                           0, 0, dconst.REPLICATIONTYPE_REBALANCE,
                           varDcId, TRUE, NULL, FALSE);
  END LOOP;
  CLOSE DCcur;
  -- "rebalancing : stopping" message
  logToDLF(NULL, dlf.LVL_SYSTEM, dlf.REBALANCING_STOP, 0, '', 'stagerd',
           'DiskServer=' || inDiskServerName || ' mountPoint=' || inMountPoint ||
           ' dataMoveTriggered=' || TO_CHAR(varTotalRebalanced) ||
           ' nbFileMovesTriggered=' || TO_CHAR(varNbFilesRebalanced));
END;
/

/* Procedure responsible for rebalancing of data on nodes within diskpools */
CREATE OR REPLACE PROCEDURE rebalancingManager AS
  varFreeRef NUMBER;
  varSensitivity NUMBER;
  varNbDS INTEGER;
  varAlreadyRebalancing INTEGER;
  varMaxDataScheduled INTEGER;
BEGIN
  -- go through all service classes
  FOR SC IN (SELECT id FROM SvcClass) LOOP
    -- check if we are already rebalancing
    SELECT count(*) INTO varAlreadyRebalancing
      FROM Disk2DiskCopyJob
     WHERE destSvcClass = SC.id
       AND replicationType = dconst.REPLICATIONTYPE_REBALANCE
       AND ROWNUM < 2;
    -- if yes, do nothing for this round
    IF varAlreadyRebalancing > 0 THEN
      CONTINUE;
    END IF;
    -- check that we have more than one diskserver online
    SELECT count(unique DiskServer.name) INTO varNbDS
      FROM FileSystem, DiskPool2SvcClass, DiskServer
     WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
       AND DiskPool2SvcClass.child = SC.id
       AND DiskServer.id = FileSystem.diskServer
       AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
       AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
       AND DiskServer.hwOnline = 1;
    -- if only 1 diskserver available, do nothing for this round
    IF varNbDS < 2 THEN
      CONTINUE;
    END IF;
    -- compute average filling of filesystems on production machines
    -- note that read only ones are not taken into account as they cannot
    -- be filled anymore
    -- also note the use of decode and the extra totalSize > 0 to protect
    -- us against division by 0. The decode is needed in case this filter
    -- is applied first, before the totalSize > 0
    SELECT AVG(free/decode(totalSize, 0, 1, totalSize)) INTO varFreeRef
      FROM FileSystem, DiskPool2SvcClass, DiskServer
     WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
       AND DiskPool2SvcClass.child = SC.id
       AND DiskServer.id = FileSystem.diskServer
       AND FileSystem.status = dconst.FILESYSTEM_PRODUCTION
       AND DiskServer.status = dconst.DISKSERVER_PRODUCTION
       AND FileSystem.totalSize > 0
       AND DiskServer.hwOnline = 1
     GROUP BY SC.id;
    -- get sensitivity of the rebalancing
    varSensitivity := TO_NUMBER(getConfigOption('Rebalancing', 'Sensitivity', '5'))/100;
    -- get max data to move in a single round
    varMaxDataScheduled := TO_NUMBER(getConfigOption('Rebalancing', 'MaxDataScheduled', '10000000000')); -- 10 GB
    -- for each filesystem too full compared to average, rebalance
    -- note that we take the read only ones into account here
    -- also note the use of decode and the extra totalSize > 0 to protect
    -- us against division by 0. The decode is needed in case this filter
    -- is applied first, before the totalSize > 0
    FOR FS IN (SELECT FileSystem.id, varFreeRef*decode(totalSize, 0, 1, totalSize)-free dataToMove,
                      DiskServer.name ds, FileSystem.mountPoint
                 FROM FileSystem, DiskPool2SvcClass, DiskServer
                WHERE DiskPool2SvcClass.parent = FileSystem.DiskPool
                  AND DiskPool2SvcClass.child = SC.id
                  AND varFreeRef - free/totalSize > varSensitivity
                  AND DiskServer.id = FileSystem.diskServer
                  AND FileSystem.status IN (dconst.FILESYSTEM_PRODUCTION, dconst.FILESYSTEM_READONLY)
                  AND DiskServer.status IN (dconst.DISKSERVER_PRODUCTION, dconst.DISKSERVER_READONLY)
                  AND FileSystem.totalSize > 0
                  AND DiskServer.hwOnline = 1) LOOP
      rebalance(FS.id, LEAST(varMaxDataScheduled, FS.dataToMove), SC.id, FS.ds, FS.mountPoint);
    END LOOP;
  END LOOP;
END;
/


/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE'
 WHERE release = '2_1_14_14_2';
COMMIT;
