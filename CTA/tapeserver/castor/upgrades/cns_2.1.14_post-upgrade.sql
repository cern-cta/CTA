/******************************************************************************
 *                 cns_2.1.14_post-upgrade.sql
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
 * This script creates a one-off database job to populate the new
 * metadata introduced in version 2.1.14.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE;

/* Verify that the script is running against the correct schema and version */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE schemaName = 'CNS'
     AND release LIKE '2_1_14_%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we cannot apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts for the CNS before this one.');
END;
/

/* Create a dedicated index to speed up the job. This takes ~25 minutes. ONLINE makes this intervention transparent. */
CREATE INDEX I_fmd_stagertime_null ON Cns_file_metadata (decode(nvl(stagertime, 1), 1, 1, NULL)) ONLINE;

/* Create a temporary procedure for updating the new metadata on all files. */
CREATE OR REPLACE PROCEDURE updateAll2114Data AS
  stop BOOLEAN := FALSE;
BEGIN
  WHILE NOT stop LOOP
    stop := TRUE;
    -- go for a run of 10,000 files: this takes about 5 seconds
    FOR f IN (SELECT /*+ INDEX(f I_fmd_stagertime_null) */
                     fileid, mtime, gid
                FROM Cns_file_metadata
               WHERE decode(nvl(stagertime, 1), 1, 1, NULL) = 1
                 AND ROWNUM <= 10000) LOOP
      stop := FALSE;
      -- update stagerTime but skip files that have been updated meanwhile
      UPDATE Cns_file_metadata
         SET stagerTime = mtime
       WHERE fileid = f.fileid
         AND stagerTime IS NULL;
      UPDATE Cns_seg_metadata
         SET creationTime = f.mtime,
             lastModificationTime = f.mtime,
             gid = f.gid
       WHERE s_fileid = f.fileid
         AND creationTime IS NULL;
      COMMIT;
    END LOOP;
  -- yield to normal activity
  DBMS_LOCK.SLEEP(1);
  END LOOP;
  -- drop temporary job to keep the index healty
  dbms_scheduler.stop_job('INDEXREBUILDJOB', force=>TRUE);
  dbms_scheduler.drop_job('INDEXREBUILDJOB');
END;
/

/* Create a temporary job to keep the index healty. This is needed to work around
 * an Oracle 'feature' that makes RANGE SCANs less and less efficient over time,
 * thus making the above procedure slower and slower.
 */
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'INDEXREBUILDJOB',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN EXECUTE IMMEDIATE ''ALTER INDEX I_fmd_stagertime_null REBUILD''; END;',
      JOB_CLASS       => 'CASTOR_JOB_CLASS',
      START_DATE      => SYSDATE + 1/3600,
      REPEAT_INTERVAL => 'FREQ=HOURLY; INTERVAL=12',
      ENABLED         => TRUE,
      COMMENTS        => 'Rebuild an index to prevent degradation');
END;
/

/* Create a one-off job to run the above procedure.
 * This is expected to take a few days with 300M files.
 */
DECLARE
  jobno NUMBER;
BEGIN
  DBMS_JOB.SUBMIT(jobno, 'updateAll2114Data();');
  COMMIT;
END;
/
