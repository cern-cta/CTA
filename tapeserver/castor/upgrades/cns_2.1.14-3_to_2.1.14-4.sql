/******************************************************************************
 *                 cns_2.1.14-3_to_2.1.14-4.sql
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
 * This script upgrades a CASTOR v2.1.14-3 CNS database to v2.1.14-4
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
     AND release = '2_1_14_4'
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
     AND release LIKE '2_1_14_3%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the CNS before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release, type)
VALUES ('2_1_14_2', '2_1_14_4', 'TRANSPARENT');
COMMIT;


/* Update and revalidation of PL-SQL code */
/******************************************/

/**
 * This procedure sets or replaces segments for multiple files by calling either setSegmentForFile
 * or replaceSegmentForFile in a loop, the choice depending on the oldCopyNo values:
 * when 0, a normal migration is assumed and setSegmentForFile is called.
 * The input is fetched from SetSegsForFilesInputHelper and deleted,
 * the output is stored into SetSegsForFilesResultsHelper. In both cases the
 * inReqId UUID is a unique key identifying one invocation of this procedure.
 */
CREATE OR REPLACE PROCEDURE setOrReplaceSegmentsForFiles(inReqId IN VARCHAR2) AS
  varRC INTEGER;
  varParams VARCHAR2(1000);
  varStartTime TIMESTAMP;
  varSeg castorns.Segment_Rec;
  varCount INTEGER := 0;
  varErrCount INTEGER := 0;
BEGIN
  varStartTime := SYSTIMESTAMP;
  -- Loop over all files. Each call commits or rollbacks each file.
  FOR s IN (SELECT fileId, lastModTime, copyNo, oldCopyNo, transfSize, comprSize,
                   vid, fseq, blockId, checksumType, checksum
              FROM SetSegsForFilesInputHelper
             WHERE reqId = inReqId) LOOP
    varSeg.fileId := s.fileId;
    varSeg.lastOpenTime := s.lastModTime;
    varSeg.copyNo := s.copyNo;
    varSeg.segSize := s.transfSize;
    varSeg.comprSize := s.comprSize;
    varSeg.vid := s.vid;
    varSeg.fseq := s.fseq;
    varSeg.blockId := s.blockId;
    varSeg.checksum_name := s.checksumType;
    varSeg.checksum := s.checksum;
    IF s.oldCopyNo = 0 THEN
      setSegmentForFile(varSeg, inReqId, varRC, varParams);
    ELSE
      replaceSegmentForFile(s.oldCopyNo, varSeg, inReqId, varRC, varParams);
    END IF;
    IF varRC != 0 THEN
      varParams := 'ErrorCode='|| to_char(varRC) ||' ErrorMessage="'|| varParams ||'"';
      addSegResult(0, inReqId, varRC, 'Error creating/replacing segment', s.fileId, varParams);
      varErrCount := varErrCount + 1;
      COMMIT;
    END IF;
    varCount := varCount + 1;
  END LOOP;
  IF varCount > 0 THEN
    -- Final logging
    varParams := 'Function="setOrReplaceSegmentsForFiles" NbFiles='|| varCount
      || ' NbErrors=' || varErrCount
      ||' ElapsedTime='|| getSecs(varStartTime, SYSTIMESTAMP)
      ||' AvgProcessingTime='|| trunc(getSecs(varStartTime, SYSTIMESTAMP)/varCount, 6);
    addSegResult(1, inReqId, 0, 'Bulk processing complete', 0, varParams);
  END IF;
  -- Clean input data
  DELETE FROM SetSegsForFilesInputHelper
   WHERE reqId = inReqId;
  COMMIT;
  -- Return results and logs to the stager. Unfortunately we can't OPEN CURSOR FOR ...
  -- because we would get ORA-24338: 'statement handle not executed' at run time.
  -- Moreover, temporary tables are not supported with distributed transactions,
  -- so the stager will remotely open the SetSegsForFilesResultsHelper table, and
  -- we clean the tables by hand using the reqId key.
EXCEPTION WHEN OTHERS THEN
  -- In case of an uncaught exception, log it and preserve the SetSegsForFilesResultsHelper
  -- content for the stager as other files may have already been committed. Any other
  -- remaining file from the input will have to be migrated again.
  varParams := 'Function="setOrReplaceSegmentsForFiles" errorMessage="' || SQLERRM
        ||'" stackTrace="' || dbms_utility.format_error_backtrace ||'" fileId=' || varSeg.fileId
        || ' copyNo=' || varSeg.copyNo || ' VID=' || varSeg.vid
        || ' fSeq=' || varSeg.fseq;
  addSegResult(1, inReqId, SQLCODE, 'Uncaught exception', 0, varParams);
  DELETE FROM SetSegsForFilesInputHelper
   WHERE reqId = inReqId;
  COMMIT;
END;
/


/* Flag the schema upgrade as COMPLETE */
/***************************************/

UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_14_4';
COMMIT;
