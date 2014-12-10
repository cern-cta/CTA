/******************************************************************************
 *                 cns_2.1.14_to_2.1.15-0.sql
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
 * This script upgrades a CASTOR v2.1.14-x CNS database to v2.1.15-0
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
     AND release = '2_1_15_0'
     AND state != 'COMPLETE';
  COMMIT;
END;
/

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'CNS'
     AND release LIKE '2_1_14%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the CNS before this one.');
END;
/

/* Verify that the open mode switch has been performed */
DECLARE
  openMode VARCHAR(100);
BEGIN
  SELECT value INTO openMode FROM CastorConfig
   WHERE key = 'openmode';
  IF openMode != 'N' THEN
    -- Error, we cannot apply this script
    raise_application_error(-20000, 'Nameserver Open mode value is '|| openMode ||', not the expected value N(ative). Please run the cns_2.1.14_switch-open-mode.sql script before this one.');
  END IF;
END;
/


INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_15_0', '2_1_15_0', 'TRANSPARENT');
COMMIT;

-- Not needed any longer
DROP PROCEDURE update2114Data;

-- enforce constraint on stagerTime. NOVALIDATE makes the operation transparent.
ALTER TABLE Cns_file_metadata MODIFY (stagerTime CONSTRAINT NN_File_stagerTime NOT NULL NOVALIDATE);


/* PL/SQL code update */
/* This procedure implements the Cns_closex API */
CREATE OR REPLACE PROCEDURE closex(inFid IN INTEGER,
                                   inFileSize IN INTEGER,
                                   inCksumType IN VARCHAR2,
                                   inCksumValue IN VARCHAR2,
                                   inMTime IN INTEGER,
                                   inLastOpenTime IN NUMBER,
                                   outRC OUT INTEGER,
                                   outMsg OUT VARCHAR2) AS
  -- the autonomous transaction is needed because we commit AND we have OUT parameters,
  -- otherwise we would get an ORA-02064 distributed operation not supported error.
  PRAGMA AUTONOMOUS_TRANSACTION;
  varFmode NUMBER(6);
  varFLastOpenTime NUMBER;
  varFCksumName VARCHAR2(2);
  varFCksum VARCHAR2(32);
  varUnused INTEGER;
BEGIN
  outRC := 0;
  outMsg := '';
  -- We truncate to 5 decimal digits, which is the precision of the lastOpenTime we get from the stager.
  -- Note this is just fitting the mantissa precision of a double, and it is due to the fact
  -- that those numbers go through OCI as double.
  SELECT filemode, TRUNC(stagertime, 5), csumType, csumValue
    INTO varFmode, varFLastOpenTime, varFCksumName, varFCksum
    FROM Cns_file_metadata
   WHERE fileId = inFid FOR UPDATE;
  -- Is it a directory?
  IF bitand(varFmode, 4*8*8*8*8) > 0 THEN  -- 040000 == S_IFDIR
    outRC := serrno.EISDIR;
    outMsg := serrno.EISDIR_MSG;
    ROLLBACK;
    RETURN;
  END IF;
  -- Has the file been changed meanwhile?
  IF varFLastOpenTime > inLastOpenTime THEN
    outRC := serrno.ENSFILECHG;
    outMsg := serrno.ENSFILECHG_MSG ||' : NSLastOpenTime='|| varFLastOpenTime
      ||', StagerLastOpenTime='|| inLastOpenTime;
    ROLLBACK;
    RETURN;
  END IF;
  -- Validate checksum type
  IF inCksumType != 'AD' THEN
    outRC := serrno.EINVAL;
    outMsg := serrno.EINVAL_MSG ||' : incorrect checksum type detected '|| inCksumType;
    ROLLBACK;
    RETURN;
  END IF;
  -- Validate checksum value
  BEGIN
    SELECT to_number(inCksumValue, 'XXXXXXXX') INTO varUnused FROM Dual;
  EXCEPTION WHEN INVALID_NUMBER THEN
    outRC := serrno.EINVAL;
    outMsg := serrno.EINVAL_MSG ||' : incorrect checksum value detected '|| inCksumValue;
    ROLLBACK;
    RETURN;
  END;
  -- Cross check file checksums when preset-adler32 (PA in the file entry):
  IF varFCksumName = 'PA' AND
     to_number(inCksumValue, 'XXXXXXXX') != to_number(varFCksum, 'XXXXXXXX') THEN
    outRC := serrno.SECHECKSUM;
    outMsg := 'Predefined file checksum mismatch : preset='|| varFCksum ||', actual='|| inCksumValue;
    ROLLBACK;
    RETURN;
  END IF;
  -- All right, update file size and other metadata
  UPDATE Cns_file_metadata
     SET fileSize = inFileSize,
         csumType = inCksumType,
         csumValue = inCksumValue,
         ctime = inMTime,
         mtime = inMTime
   WHERE fileId = inFid;
  outMsg := 'Function="closex" FileSize=' || inFileSize
    ||' ChecksumType="'|| inCksumType ||'" ChecksumValue="'|| inCksumValue
    ||'" NewModTime=' || inMTime ||' StagerLastOpenTime='|| inLastOpenTime ||' RtnCode=0';
  COMMIT;
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- The file entry was not found, just give up
  outRC := serrno.ENOENT;
  outMsg := serrno.ENOENT_MSG;
  ROLLBACK;  -- this is a no-op, but it's needed to close the autonomous transaction and avoid ORA-06519 errors
END;
/


/* Flag the schema upgrade as COMPLETE */
/***************************************/

UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_15_0';
COMMIT;

