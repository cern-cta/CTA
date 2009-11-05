/******************************************************************************
 *              mon_2.1.9-1_to_2.1.9-3.sql
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
 * This script upgrades a CASTOR v2.1.9-1 MONITORING database into v2.1.9-3
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
DECLARE
  unused VARCHAR2(30);
BEGIN
  SELECT table_name INTO unused
    FROM user_tables
   WHERE table_name = 'UPGRADELOG';
  EXECUTE IMMEDIATE
   'UPDATE UpgradeLog
       SET failureCount = failureCount + 1
     WHERE schemaVersion = ''2_1_9_3''
       AND release = ''2_1_9_3''
       AND state != ''COMPLETE''';
  COMMIT;
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
/

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE release LIKE '2_1_9_1%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

UPDATE CastorVersion SET release = '2_1_9_3', schemaVersion = '2_1_9_3';
COMMIT;


/* Schema changes go here */
/**************************/

/* Disable all jobs */
/********************/
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs WHERE enabled = 'TRUE')
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name);
    END IF;
    dbms_scheduler.disable(a.job_name);
  END LOOP;
END;
/

DECLARE
  unused NUMBER;
BEGIN
  SELECT data_precision INTO unused
    FROM user_tab_columns
   WHERE table_name = 'GARBAGECOLLECTIONSTATS'
     AND column_name = 'MINFILESIZE'
     AND data_precision IS NULL;
  EXECUTE IMMEDIATE 'DROP TABLE GarbageCollectionStats';
  EXECUTE IMMEDIATE 'CREATE TABLE GarbageCollectionStats (timestamp DATE CONSTRAINT NN_GarbageCollectionStats_ts NOT NULL, interval NUMBER, diskserver VARCHAR2(255), requestType VARCHAR2(255), deleted NUMBER(*,3), totalFileSize NUMBER, minFileAge NUMBER(*,0), maxFileAge NUMBER(*,0), avgFileAge NUMBER(*,0), stddevFileAge NUMBER(*,0), medianFileAge NUMBER(*,0), minFileSize NUMBER(*,0), maxFileSize NUMBER(*,0), avgFileSize NUMBER(*,0), stddevFileSize NUMBER(*,0), medianFileSize NUMBER(*,0))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE))';

  SELECT data_precision INTO unused
    FROM user_tab_columns
   WHERE table_name = 'REPLICATIONSTATS'
     AND column_name = 'MINFILESIZE'
     AND data_precision IS NULL;
  EXECUTE IMMEDIATE 'DROP TABLE ReplicationStats';
  EXECUTE IMMEDIATE 'CREATE TABLE ReplicationStats (timestamp DATE CONSTRAINT NN_ReplicationStats_ts NOT NULL, interval NUMBER, sourceSvcClass VARCHAR2(255), destSvcClass VARCHAR2(255), transferred NUMBER(*,3), totalFileSize NUMBER, minFileSize NUMBER(*,0), maxFileSize NUMBER(*,0), avgFileSize NUMBER(*,0), stddevFileSize NUMBER(*,0), medianFileSize NUMBER(*,0))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE))';
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;  -- Nothing
END;
/

/* SQL statement for table Top10Errors */
CREATE TABLE Top10Errors (timestamp DATE CONSTRAINT NN_TopTenErrors_ts NOT NULL, interval NUMBER, daemon VARCHAR2(255), nbErrors NUMBER, errorMessage VARCHAR2(512))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

/* Trigger partition creation for the new tables */
BEGIN
  createPartitions();
END;
/

/* SQL statements for table UpgradeLog */
CREATE TABLE UpgradeLog (Username VARCHAR2(64) DEFAULT sys_context('USERENV', 'OS_USER') CONSTRAINT NN_UpgradeLog_Username NOT NULL, Machine VARCHAR2(64) DEFAULT sys_context('USERENV', 'HOST') CONSTRAINT NN_UpgradeLog_Machine NOT NULL, Program VARCHAR2(48) DEFAULT sys_context('USERENV', 'MODULE') CONSTRAINT NN_UpgradeLog_Program NOT NULL, StartDate TIMESTAMP(6) WITH TIME ZONE DEFAULT sysdate, EndDate TIMESTAMP(6) WITH TIME ZONE, FailureCount NUMBER DEFAULT 0, Type VARCHAR2(20) DEFAULT 'NON TRANSPARENT', State VARCHAR2(20) DEFAULT 'INCOMPLETE', SchemaVersion VARCHAR2(20) CONSTRAINT NN_UpgradeLog_SchemaVersion NOT NULL, Release VARCHAR2(20) CONSTRAINT NN_UpgradeLog_Release NOT NULL);

/* SQL statements for check constraints on the UpgradeLog table */
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_State
  CHECK (state IN ('COMPLETE', 'INCOMPLETE'));
  
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_Type
  CHECK (type IN ('TRANSPARENT', 'NON TRANSPARENT'));

/* SQL statement to populate the intial release value */
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('2_1_9_3', '2_1_9_3');
UPDATE UpgradeLog SET type = 'TRANSPARENT';
COMMIT;

DROP TABLE CastorVersion;

/* SQL statement to create the CastorVersion view */
CREATE OR REPLACE VIEW CastorVersion
AS
  SELECT decode(type, 'TRANSPARENT', schemaVersion,
           decode(state, 'INCOMPLETE', state, schemaVersion)) schemaVersion,
         decode(type, 'TRANSPARENT', release,
           decode(state, 'INCOMPLETE', state, release)) release
    FROM UpgradeLog
   WHERE startDate =
     (SELECT max(startDate) FROM UpgradeLog);


/* Update and revalidation of PL-SQL code */
/******************************************/

/* PL/SQL method implementing statsTop10Errors
 *
 * Provides statistics on top 10 errors broken down by daemon
 */
CREATE OR REPLACE PROCEDURE statsTop10Errors (now IN DATE, interval IN NUMBER) AS
BEGIN
  -- Stats table: Top10Errors
  -- Frequency: 5 minutes
  INSERT INTO Top10Errors (daemon, nbErrors, errorMessage)
    -- Gather data
    (SELECT facility, nbErrors, message FROM (
      -- For each daemon list the top 10 errors + the total of errors for that
      -- daemon
      SELECT facility, nbErrors, message,
             RANK() OVER (PARTITION BY facility ORDER BY facility DESC,
                          nbErrors DESC) rank
        FROM (
          SELECT facilities.fac_name facility, 
                 nvl(msgtexts.msg_text, '-') message, count(*) nbErrors
            FROM &dlfschema..dlf_messages   messages,
                 &dlfschema..dlf_facilities facilities,
                 &dlfschema..dlf_msg_texts  msgtexts
           WHERE messages.facility = facilities.fac_no
             AND (messages.msg_no   = msgtexts.msg_no
             AND  messages.facility = msgtexts.fac_no)
             AND messages.severity <= 3
             AND messages.timestamp >  now - 65/1440
             AND messages.timestamp <= now - 5/1440
           GROUP BY GROUPING SETS (facilities.fac_name, msgtexts.msg_text),
                                  (facilities.fac_name)))
       WHERE rank < 12);
END;
/

/* Scheduler jobs */
BEGIN
  -- Remove scheduler jobs before recreation
  FOR a IN (SELECT job_name FROM user_scheduler_jobs)
  LOOP
    DBMS_SCHEDULER.DROP_JOB(a.job_name, TRUE);
  END LOOP;

  -- Create a db job to be run every day and create new partitions
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'partitionCreationJob',
      JOB_TYPE        => 'STORED_PROCEDURE',
      JOB_ACTION      => 'createPartitions',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => TRUNC(SYSDATE) + 1/24,
      REPEAT_INTERVAL => 'FREQ=DAILY',
      ENABLED         => TRUE,
      COMMENTS        => 'Daily partitioning creation');

  -- Create a db job to be run every day and drop old data from the database
  DBMS_SCHEDULER.CREATE_JOB(
      JOB_NAME        => 'archiveDataJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'BEGIN
                            archiveData(-1);
                            FOR a IN (SELECT table_name FROM user_tables
                                       WHERE table_name LIKE ''ERR_%'')
                            LOOP
                              EXECUTE IMMEDIATE ''TRUNCATE TABLE ''||a.table_name;
                            END LOOP;
                          END;',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => TRUNC(SYSDATE) + 2/24,
      REPEAT_INTERVAL => 'FREQ=DAILY',
      ENABLED         => TRUE,
      COMMENTS        => 'Daily data archiving');

  -- Create a job to execute the procedures that create statistical information (OLD)
  DBMS_SCHEDULER.CREATE_JOB (
      JOB_NAME        => 'statisticJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'DECLARE
                            now DATE := SYSDATE;
                            interval NUMBER := 300;
                          BEGIN
                            statsLatency(now, interval);
                            statsQueueTime(now, interval);
                            statsGarbageCollection(now, interval);
                            statsRequest(now, interval);
                            statsDiskCacheEfficiency(now, interval);
                            statsMigratedFiles(now, interval);
                            statsRecalledFiles(now, interval);
                            statsReplication(now, interval);
                            statsTapeRecalled(now, interval);
                            statsProcessingTime(now, interval);
                            statsClientVersion(now, interval);
                            statsTapeMounts(now, interval);
                            statsTop10Errors(now, interval);
                          END;',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => SYSDATE,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'CASTOR2 Monitoring Statistics (OLD) (5 Minute Frequency)');

  -- Create a job to execute the procedures that create statistical information (NEW)
  DBMS_SCHEDULER.CREATE_JOB (
      JOB_NAME        => 'populateJob',
      JOB_TYPE        => 'PLSQL_BLOCK',
      JOB_ACTION      => 'DECLARE
                            now DATE := SYSDATE - 10/1440;
                          BEGIN
                            EXECUTE IMMEDIATE ''TRUNCATE TABLE ERR_Requests'';
                            statsReqs(now);
                            statsDiskCopy(now);
                            statsInternalDiskCopy(now);
                            statsTapeRecall(now);
                            statsMigs(now);
                            statsTotalLat(now);
                            statsGcFiles(now);
                          END;',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => SYSDATE + 10/1440,
      REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
      ENABLED         => TRUE,
      COMMENTS        => 'CASTOR2 Monitoring Statistics (NEW) (5 Minute Frequency)');

END;
/


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
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE';
COMMIT;
