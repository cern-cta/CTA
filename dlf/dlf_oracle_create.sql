/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE;

/******************************************************************************
 *              oracleSchema.sql
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
 * @(#)RCSfile: oracleCreate.sql,v  Release: 1.2  Release Date: 2009/08/18 09:42:58  Author: waldron 
 *
 * This script creates a new DLF schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* SQL statement for ids sequence */
CREATE SEQUENCE ids_seq INCREMENT BY 1 CACHE 300;

/* SQL statement for table dlf_config */
CREATE TABLE dlf_config(name VARCHAR2(255) CONSTRAINT NN_Config_Name NOT NULL, value VARCHAR2(255), description VARCHAR2(255));
ALTER TABLE dlf_config ADD CONSTRAINT UN_Config_Name UNIQUE (name) ENABLE;

/* SQL statements for table dlf_messages */
CREATE TABLE dlf_messages(id NUMBER, timestamp DATE CONSTRAINT NN_Messages_Timestamp NOT NULL, timeusec NUMBER, reqid CHAR(36), subreqid CHAR(36), hostid NUMBER, facility NUMBER(3), severity NUMBER(3), msg_no NUMBER(5), pid NUMBER(10), tid NUMBER(10), nshostid NUMBER, nsfileid NUMBER, tapevid VARCHAR2(20), userid NUMBER(10), groupid NUMBER(10), sec_type VARCHAR2(20), sec_name VARCHAR2(255))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_Messages_Timestamp ON dlf_messages (timestamp) LOCAL;
CREATE INDEX I_Messages_Facility ON dlf_messages (facility) LOCAL;
CREATE INDEX I_Messages_Pid ON dlf_messages (pid) LOCAL;
CREATE INDEX I_Messages_Reqid ON dlf_messages (reqid) LOCAL;
CREATE INDEX I_Messages_Subreqid ON dlf_messages (subreqid) LOCAL;
CREATE INDEX I_Messages_Hostid ON dlf_messages (hostid) LOCAL;
CREATE INDEX I_Messages_NSHostid ON dlf_messages (nshostid) LOCAL;
CREATE INDEX I_Messages_NSFileid ON dlf_messages (nsfileid) LOCAL;
CREATE INDEX I_Messages_Tapevid ON dlf_messages (tapevid) LOCAL;
CREATE INDEX I_Messages_Userid ON dlf_messages (userid) LOCAL;
CREATE INDEX I_Messages_Groupid ON dlf_messages (groupid) LOCAL;
CREATE INDEX I_Messages_Sec_type ON dlf_messages (sec_type) LOCAL;
CREATE INDEX I_Messages_Sec_name ON dlf_messages (sec_name) LOCAL;

/* SQL statements for table dlf_num_param_values */
CREATE TABLE dlf_num_param_values(id NUMBER, timestamp DATE CONSTRAINT NN_Num_Param_Values_Timestamp NOT NULL, name VARCHAR2(20), value NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_Num_Param_Values_id ON dlf_num_param_values (id) LOCAL;

/* SQL statements for table dlf_str_param_values */
CREATE TABLE dlf_str_param_values(id NUMBER, timestamp DATE CONSTRAINT NN_Str_Param_Values_Timestamp NOT NULL, name VARCHAR2(20), value VARCHAR2(2048))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_Str_Param_Values_id ON dlf_str_param_values (id) LOCAL;

/* SQL statements for table dlf_severities */
CREATE TABLE dlf_severities(sev_no NUMBER(3), sev_name VARCHAR2(20));

CREATE UNIQUE INDEX UN_Severities_Sev_NoName ON dlf_severities (sev_no, sev_name);

ALTER TABLE dlf_severities ADD CONSTRAINT UN_Severities_Sev_NoName UNIQUE (sev_no, sev_name) ENABLE;

/* SQL statements for table dlf_facilities */
CREATE TABLE dlf_facilities(fac_no NUMBER(3), fac_name VARCHAR2(20));

CREATE UNIQUE INDEX UN_Facilities_Fac_No ON dlf_facilities (fac_no);
CREATE UNIQUE INDEX UN_Facilities_Fac_Name ON dlf_facilities (fac_name);

ALTER TABLE dlf_facilities ADD CONSTRAINT UN_Facilities_Fac_No UNIQUE (fac_no) ENABLE;
ALTER TABLE dlf_facilities ADD CONSTRAINT UN_Facilities_Fac_Name UNIQUE (fac_name) ENABLE;

/* SQL statements for table dlf_msg_texts */
CREATE TABLE dlf_msg_texts(fac_no NUMBER(3), msg_no NUMBER(5), msg_text VARCHAR2(512));

CREATE UNIQUE INDEX UN_Msg_Texts_FacMsgNo ON dlf_msg_texts (fac_no, msg_no);

/* SQL statements for dlf_host_map */
CREATE TABLE dlf_host_map(hostid NUMBER, hostname VARCHAR2(64));

CREATE UNIQUE INDEX UN_Host_Map_Hostid ON dlf_host_map (hostid);
CREATE UNIQUE INDEX UN_Host_Map_Hostname ON dlf_host_map (hostname);

ALTER TABLE dlf_host_map ADD CONSTRAINT UN_Host_Map_Hostid UNIQUE (hostid) ENABLE;
ALTER TABLE dlf_host_map ADD CONSTRAINT UN_Host_Map_Hostname UNIQUE (hostname) ENABLE;

/* SQL statements for dlf_nshost_map */
CREATE TABLE dlf_nshost_map(nshostid NUMBER, nshostname VARCHAR2(64));

CREATE UNIQUE INDEX UN_NSHost_Map_NSHostid ON dlf_nshost_map (nshostid);
CREATE UNIQUE INDEX UN_NSHost_Map_NSHostname ON dlf_nshost_map (nshostname);

ALTER TABLE dlf_nshost_map ADD CONSTRAINT UN_NSHost_Map_NsHostid UNIQUE (nshostid) ENABLE;
ALTER TABLE dlf_nshost_map ADD CONSTRAINT UN_NSHost_Map_NsHostname UNIQUE (nshostname) ENABLE;

/* Fill the dlf_config table */
INSERT INTO dlf_config (name, value, description) VALUES ('instance', 'castordlf', 'The name of the castor2 instance');
INSERT INTO dlf_config (name, value, description) VALUES ('expiry', '90', 'The expiry time of the logging data in days');

/* Fill the dlf_severities table */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('1',  'Emerg');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('2',  'Alert');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('3',  'Error');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('4',  'Warn');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('5',  'Notice'); /* Auth */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('6',  'Notice'); /* Security */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('7',  'Debug');  /* Usage */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('8',  'Info');   /* System */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('10', 'Info');   /* Monitoring */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('11', 'Debug');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('12', 'Notice'); /* User Error */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('13', 'Crit');


/* Fill the dlf_facilities table */
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (0,  'rtcpclientd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (1,  'migrator');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (2,  'recaller');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (4,  'rhd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (8,  'gcd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (9,  'schedulerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (10, 'tperrhandler');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (11, 'vdqmd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (13, 'srmfed');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (14, 'srmbed');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (15, 'repackd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (17, 'taped');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (18, 'rtcpd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (19, 'rmmasterd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (20, 'rmnoded');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (21, 'jobmanagerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (22, 'stagerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (23, 'd2dtransfer');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (24, 'mighunterd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (25, 'rechandlerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (26, 'stagerjob');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (27, 'tapebridged');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (28, 'rmcd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (29, 'tapegatewayd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (30, 'operations');


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
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('-', '2_1_9_4');

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

/******************************************************************************
 *              oracleTrailer.sql
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
 * @(#)RCSfile: oracleCreate.sql,v  Release: 1.2  Release Date: 2009/08/18 09:42:58  Author: waldron 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_9_3';

/* PL/SQL method implementing createPartition */
CREATE OR REPLACE PROCEDURE createPartitions
AS
  username VARCHAR2(2048);
  partitionMax NUMBER;
  tableSpaceName VARCHAR2(2048);
  highValue DATE;
  cnt NUMBER;
BEGIN
  -- Set the nls_date_format
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = "DD-MON-YYYY"';

  -- Extract the name of the current user running the PL/SQL procedure. This name
  -- will be used within the tablespace names.
  SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER')
    INTO username
    FROM dual;

  -- Loop over all partitioned tables
  FOR a IN (SELECT DISTINCT(table_name)
              FROM user_tab_partitions
             ORDER BY table_name)
  LOOP
    -- Determine the high value on which to split the MAX_VALUE partition of all
    -- tables
    SELECT max(substr(partition_name, 3, 10))
      INTO partitionMax
      FROM user_tab_partitions
     WHERE partition_name <> 'MAX_VALUE'
       AND table_name = a.table_name;

    partitionMax := TO_NUMBER(
      TO_CHAR(TRUNC(TO_DATE(partitionMax, 'YYYYMMDD') + 1), 'YYYYMMDD'));

    -- If this is the first execution there will be no high value, so set it to
    -- today
    IF partitionMax IS NULL THEN
      partitionMax := TO_NUMBER(TO_CHAR(SYSDATE, 'YYYYMMDD'));
    END IF;

    -- Create partition
    FOR b IN (SELECT TO_DATE(partitionMax, 'YYYYMMDD') + rownum - 1 value
                FROM all_objects
               WHERE rownum <=
                     TO_DATE(TO_CHAR(SYSDATE + 7, 'YYYYMMDD'), 'YYYYMMDD') -
                     TO_DATE(partitionMax, 'YYYYMMDD') + 1)
    LOOP
      -- To improve data management each daily partition has its own tablespace
      -- http://www.oracle.com/technology/oramag/oracle/06-sep/o56partition.html

      -- Check if a new tablespace is required before creating the partition
      tableSpaceName := 'DLF_'||TO_CHAR(b.value, 'YYYYMMDD')||'_'||username;
      SELECT count(*) INTO cnt
        FROM user_tablespaces
       WHERE tablespace_name = tableSpaceName;

      IF cnt = 0 THEN
        EXECUTE IMMEDIATE 'CREATE TABLESPACE '||tableSpaceName||'
                           DATAFILE SIZE 100M
                           AUTOEXTEND ON NEXT 200M
                           MAXSIZE 30G
                           EXTENT MANAGEMENT LOCAL
                           SEGMENT SPACE MANAGEMENT AUTO';
      END IF;

      -- If the tablespace is read only, alter its status to read write for this
      -- operation.
      FOR d IN (SELECT tablespace_name FROM user_tablespaces
                 WHERE tablespace_name = tableSpaceName
                   AND status = 'READ ONLY')
      LOOP
        EXECUTE IMMEDIATE 'ALTER TABLESPACE '||d.tablespace_name||' READ WRITE';
      END LOOP;

      highValue := TRUNC(b.value + 1);
      EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                         SPLIT PARTITION MAX_VALUE
                         AT    ('''||TO_CHAR(highValue, 'DD-MON-YYYY')||''')
                         INTO  (PARTITION P_'||TO_CHAR(b.value, 'YYYYMMDD')||'
                                TABLESPACE '||tableSpaceName||',
                                PARTITION MAX_VALUE)
                         UPDATE INDEXES';

      -- Move indexes to the correct tablespace
      FOR c IN (SELECT index_name
                  FROM user_indexes
                 WHERE table_name = a.table_name)
      LOOP
        EXECUTE IMMEDIATE 'ALTER INDEX '||c.index_name||'
                           REBUILD PARTITION P_'||TO_CHAR(b.value, 'YYYYMMDD')||'
                           TABLESPACE '||tableSpaceName;
      END LOOP;
    END LOOP;
  END LOOP;
END;
/


/* PL/SQL method implementing archiveData */
CREATE OR REPLACE PROCEDURE archiveData (expiry IN NUMBER)
AS
  username VARCHAR2(2048);
  expiryTime NUMBER;
BEGIN
  -- Set the nls_date_format
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = "DD-MON-YYYY"';

  -- Extract the name of the current user running the PL/SQL procedure. This name
  -- will be used within the tablespace names.
  SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER')
    INTO username
    FROM dual;

  -- Extract configurable expiry time
  expiryTime := expiry;
  IF expiryTime = -1 THEN
    SELECT TO_NUMBER(value) INTO expiryTime
      FROM dlf_config
     WHERE name = 'expiry';
  END IF;

  -- Drop partitions across all tables
  FOR a IN (SELECT *
              FROM user_tab_partitions
             WHERE partition_name < concat('P_', TO_CHAR(SYSDATE - expiryTime, 'YYYYMMDD'))
               AND partition_name <> 'MAX_VALUE'
             ORDER BY partition_name DESC)
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                       DROP PARTITION '||a.partition_name;
  END LOOP;

  -- Drop tablespaces
  FOR a IN (SELECT tablespace_name
              FROM user_tablespaces
             WHERE status <> 'OFFLINE'
               AND tablespace_name LIKE 'DLF_%_'||username
               AND tablespace_name < 'DLF_'||TO_CHAR(SYSDATE - expiryTime, 'YYYYMMDD')||'_'||username
             ORDER BY tablespace_name ASC)
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||a.tablespace_name||' OFFLINE';
    EXECUTE IMMEDIATE 'DROP TABLESPACE '||a.tablespace_name||'
                       INCLUDING CONTENTS AND DATAFILES';
  END LOOP;

  -- Set the status of tablespaces older then 2 days to read only.
  FOR a IN (SELECT tablespace_name
              FROM user_tablespaces
             WHERE status <> 'OFFLINE'
	       AND status <> 'READ ONLY'
               AND tablespace_name LIKE 'DLF_%_'||username
               AND tablespace_name < 'DLF_'||TO_CHAR(SYSDATE - 2, 'YYYYMMDD')||'_'||username
             ORDER BY tablespace_name ASC)
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||a.tablespace_name||' READ ONLY';
  END LOOP;
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
      JOB_ACTION      => 'BEGIN archiveData(-1); END;',
      JOB_CLASS       => 'DLF_JOB_CLASS',
      START_DATE      => TRUNC(SYSDATE) + 2/24,
      REPEAT_INTERVAL => 'FREQ=DAILY',
      ENABLED         => TRUE,
      COMMENTS        => 'Daily data archiving');
END;
/


/* Trigger the initial creation of partitions */
BEGIN
  createPartitions();
END;
/

/* Flag the schema creation as COMPLETE */
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE';
COMMIT;
