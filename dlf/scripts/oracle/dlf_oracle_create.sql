/*                        ORACLE ENTERPRISE EDITION                         */
/*                                                                          */
/* This file contains SQL code that will generate the dlf database schema   */
/* tablespaces DLF_INDX and DLF_DATA must be present on the target database */
/* and DBA privileges must be present for scheduling the maintenance job    */

/*
 * dlf_settings
 */
CREATE TABLE dlf_settings
(
	name		VARCHAR2(50),
	value		VARCHAR2(255),
	description	VARCHAR2(255)
)
TABLESPACE dlf_data;

CREATE UNIQUE INDEX i_set_name ON dlf_settings (name) TABLESPACE dlf_indx;

/*
 * dlf_sequences
 */
CREATE TABLE dlf_sequences
(
	seq_name	CHAR(15),
	seq_no		NUMBER
)
TABLESPACE dlf_data;

/*
 * dlf_mode
 */
CREATE TABLE dlf_mode
(
	name		VARCHAR2(20),
	enabled		NUMBER(1)
)
TABLESPACE dlf_data;

/*
 * dlf_jobstats
 */
CREATE TABLE dlf_jobstats
(
	timestamp	DATE NOT NULL,
	type		NUMBER,
	min_time	NUMBER(*,4),
	max_time	NUMBER(*,4),
	avg_time	NUMBER(*,4),
	std_dev		NUMBER(*,4),
	starting	NUMBER,
	exiting		NUMBER,
	interval        NUMBER)
PARTITION BY RANGE (timestamp)
(
	PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE) TABLESPACE dlf_data
)
TABLESPACE dlf_data;

/*
 * dlf_reqstats
 */
CREATE TABLE dlf_reqstats
(
	timestamp	DATE NOT NULL,
	type		NUMBER,
	min_time	NUMBER(*,4),
	max_time	NUMBER(*,4),
	avg_time	NUMBER(*,4),
	std_dev		NUMBER(*,4),
	req_count	NUMBER,
	interval        NUMBER)
PARTITION BY RANGE (timestamp)
(
	PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE) TABLESPACE dlf_data
)
TABLESPACE dlf_data;

/*
 * dlf_monitoring
 */
CREATE TABLE dlf_monitoring
(
	timestamp	DATE NOT NULL,
	h_threads	NUMBER,
	h_messages	NUMBER,
	h_inits         NUMBER,
	h_errors	NUMBER,
	h_connections	NUMBER,
	h_clientpeak	NUMBER,
	h_timeouts      NUMBER,
	db_threads	NUMBER,
	db_commits	NUMBER,
	db_errors	NUMBER,
	db_inserts	NUMBER,
	db_rollbacks	NUMBER,
	db_selects	NUMBER,
	db_updates	NUMBER,
	db_cursors      NUMBER,
	db_messages	NUMBER,
	db_inits	NUMBER,
	db_hashstats	NUMBER,
	s_uptime	NUMBER,
	s_mode          NUMBER,
	s_queued	NUMBER,
	s_response	NUMBER(*,4),
	interval        NUMBER)
PARTITION BY RANGE (timestamp)
(
	PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE) TABLESPACE dlf_data
)
TABLESPACE dlf_data;

/*
 * dlf_messages main/primary link table
 */
CREATE TABLE dlf_messages
(
	id		NUMBER,
	timestamp	DATE NOT NULL,
	timeusec	NUMBER,
	reqid		CHAR(36),
	subreqid	CHAR(36),
	hostid		NUMBER,
	facility	NUMBER(3),
	severity	NUMBER(3),
	msg_no		NUMBER(5),
	pid		NUMBER(10),
	tid		NUMBER(10),
	nshostid	NUMBER,
	nsfileid	NUMBER,
	tapevid		VARCHAR2(20),
	userid		NUMBER(10),
	groupid		NUMBER(10),
	sec_type	VARCHAR2(20),
	sec_name	VARCHAR2(255))
PARTITION BY RANGE (timestamp)
(
	PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE) TABLESPACE dlf_data
)
TABLESPACE dlf_data;

/* local indexes */
CREATE INDEX i_msg_timestamp ON dlf_messages (timestamp) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_fac ON dlf_messages (facility) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_pid ON dlf_messages (pid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_reqid ON dlf_messages (reqid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_subreqid ON dlf_messages (subreqid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_hostid ON dlf_messages (hostid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_nshostid ON dlf_messages (nshostid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_fileid ON dlf_messages (nsfileid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_tapevid ON dlf_messages (tapevid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_userid ON dlf_messages (userid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_groupid ON dlf_messages (groupid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_sec_type ON dlf_messages (sec_type) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_sec_name ON dlf_messages (sec_name) LOCAL TABLESPACE dlf_indx;

/*
 * dlf_num_param_values
 */
CREATE TABLE dlf_num_param_values
(
	id		NUMBER,
	timestamp	DATE NOT NULL,
	name		VARCHAR2(20),
	value		NUMBER)
PARTITION BY RANGE (timestamp)
(
	PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE) TABLESPACE dlf_data
)
TABLESPACE dlf_data;

CREATE INDEX i_num_id ON dlf_num_param_values (id) LOCAL TABLESPACE dlf_indx;

/*
 * dlf_str_param_values
 */
CREATE TABLE dlf_str_param_values
(
	id		NUMBER,
	timestamp	DATE NOT NULL,
	name		VARCHAR2(20),
	value		VARCHAR2(2048))
PARTITION BY RANGE (timestamp)
(
	PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE) TABLESPACE dlf_data
)
TABLESPACE dlf_data;

CREATE INDEX i_str_id ON dlf_str_param_values (id) LOCAL TABLESPACE dlf_indx;

/*
 * dlf_severities
 */
CREATE TABLE dlf_severities
(
	sev_no		NUMBER(3),
	sev_name	VARCHAR2(20)
)
TABLESPACE dlf_data;

CREATE UNIQUE INDEX i_sev_no ON dlf_severities (sev_no) TABLESPACE dlf_indx;
CREATE UNIQUE INDEX i_sev_name ON dlf_severities (sev_name) TABLESPACE dlf_indx;

ALTER TABLE dlf_severities ADD CONSTRAINT i_sev_no UNIQUE (sev_no) ENABLE;
ALTER TABLE dlf_severities ADD CONSTRAINT i_sev_name UNIQUE (sev_name) ENABLE;

/*
 * dlf_facilities - Note: dlf_maintenace() will automatically create new facility
 *                        subpartitions based upon the information in this table
 */
CREATE TABLE dlf_facilities
(
	fac_no		NUMBER(3),
	fac_name	VARCHAR2(20)
)
TABLESPACE dlf_data;

CREATE UNIQUE INDEX i_fac_no ON dlf_facilities (fac_no) TABLESPACE dlf_indx;
CREATE UNIQUE INDEX i_fac_name ON dlf_facilities (fac_name) TABLESPACE dlf_indx;

ALTER TABLE dlf_facilities ADD CONSTRAINT i_fac_no UNIQUE (fac_no) ENABLE;
ALTER TABLE dlf_facilities ADD CONSTRAINT i_fac_name UNIQUE (fac_name) ENABLE;

/*
 * dlf_msg_texts
 */
CREATE TABLE dlf_msg_texts
(
	fac_no		NUMBER(3),
	msg_no		NUMBER(5),
	msg_text	VARCHAR2(512)
)
TABLESPACE dlf_data;

CREATE UNIQUE INDEX i_msg_texts ON dlf_msg_texts (fac_no, msg_no) TABLESPACE dlf_indx;

/*
 * dlf_host_map
 */
CREATE TABLE dlf_host_map
(
	hostid		NUMBER,
	hostname	VARCHAR2(64)
)
TABLESPACE dlf_data;

CREATE UNIQUE INDEX i_hostid ON dlf_host_map (hostid) TABLESPACE dlf_indx;
CREATE UNIQUE INDEX i_hostname ON dlf_host_map (hostname) TABLESPACE dlf_indx;

ALTER TABLE dlf_host_map ADD CONSTRAINT i_hostid UNIQUE (hostid) ENABLE;
ALTER TABLE dlf_host_map ADD CONSTRAINT i_hostname UNIQUE (hostname) ENABLE;

/*
 * dlf_nshost_map
 */
CREATE TABLE dlf_nshost_map
(
	nshostid	NUMBER,
	nshostname	VARCHAR2(64)
)
TABLESPACE dlf_data;

CREATE UNIQUE INDEX i_nshostid ON dlf_nshost_map (nshostid) TABLESPACE dlf_indx;
CREATE UNIQUE INDEX i_nshostname ON dlf_nshost_map (nshostname) TABLESPACE dlf_indx;

ALTER TABLE dlf_nshost_map ADD CONSTRAINT i_nshostid UNIQUE (nshostid) ENABLE;
ALTER TABLE dlf_nshost_map ADD CONSTRAINT i_nshostname UNIQUE (nshostname) ENABLE;


/* initialise severities */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('1', 'Emergency');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('2', 'Alert');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('3', 'Error');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('4', 'Warning');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('5', 'Auth');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('6', 'Security');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('7', 'Usage');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('8', 'System');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('9', 'Important');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('10', 'Monitoring');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('11', 'Debug');

/* initialise facilities */
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (0, 'rtcpcld');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (1, 'migrator');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (2, 'recaller');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (3, 'stager');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (4, 'RHLog');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (5, 'job');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (6, 'MigHunter');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (7, 'rmmaster');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (8, 'GC');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (9, 'scheduler');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (10, 'TapeErrorHandler');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (11, 'Vdqm');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (12, 'rfio');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (13, 'SRMServer');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (14, 'SRMDaemon');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (15, 'Repack');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (16, 'cleaning');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (17, 'tpdaemon');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (18, 'rtcpd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (19, 'RmMaster');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (20, 'RmNode');

/* initialise sequences */
INSERT INTO dlf_sequences (seq_name, seq_no) VALUES ('id',       1);
INSERT INTO dlf_sequences (seq_name, seq_no) VALUES ('hostid',   1);
INSERT INTO dlf_sequences (seq_name, seq_no) VALUES ('nshostid', 1);

/* initialise mode information */
INSERT INTO dlf_mode (name, enabled) VALUES ('queue_purge',      0);
INSERT INTO dlf_mode (name, enabled) VALUES ('queue_suspend',    0);
INSERT INTO dlf_mode (name, enabled) VALUES ('database_suspend', 0);

/* initialise settings */
INSERT INTO dlf_settings VALUES ('ARCHIVE_MODE',    '1',  'Defines the archiving policy that should be performed on the database. Where a value of 1 = drop and 2 = archive and drop.');
INSERT INTO dlf_settings VALUES ('ARCHIVE_EXPIRY',  '30', 'Defines how many days worth of data should remain online.');
INSERT INTO dlf_settings VALUES ('ENABLE_JOBSTATS', '0',  'Enable the automatic generation of job statistics.');
INSERT INTO dlf_settings VALUES ('ENABLE_REQSTATS', '0',  'Enable the automatic generation of request statistics.');

/*
 * dlf_partition procedure
 */
CREATE OR REPLACE PROCEDURE dlf_partition (boundary IN INTEGER)
AS

  -- variables
  v_partition_max INTEGER;
  v_high_value	  DATE;
  v_table_name	  VARCHAR2(20);
  v_exists        INTEGER;
  v_split_name    VARCHAR2(20);
  
BEGIN

  -- set the nls_date_format
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = "DD-MON-YYYY"';

  -- loop over all partitioned tables
  FOR a IN (SELECT DISTINCT(TABLE_NAME)
            FROM   USER_TAB_PARTITIONS
            WHERE  TABLE_NAME LIKE 'DLF_%'
            ORDER  BY TABLE_NAME)
  LOOP
  
   -- determine the name of the partition for table 'X' with the highest value
    IF boundary = 0 THEN
    
      -- automatic
      SELECT MAX(SUBSTR(PARTITION_NAME, 3, 10))
      INTO   v_partition_max
      FROM   USER_TAB_PARTITIONS
      WHERE  PARTITION_NAME <> 'MAX_VALUE'
      AND    TABLE_NAME = a.table_name;
      
      v_partition_max := TO_NUMBER(
          TO_CHAR(TRUNC(TO_DATE(v_partition_max, 'YYYYMMDD') + 1), 'YYYYMMDD'));

      -- first run
      IF v_partition_max IS NULL THEN
        v_partition_max := TO_NUMBER(TO_CHAR(SYSDATE, 'YYYYMMDD'));
      END IF;
      
      FOR b IN (SELECT TO_DATE(v_partition_max, 'YYYYMMDD') + ROWNUM - 1 value
                FROM   ALL_OBJECTS
                WHERE  ROWNUM <=
                TO_DATE(TO_CHAR(SYSDATE + 7, 'YYYYMMDD'), 'YYYYMMDD') -
                TO_DATE(v_partition_max, 'YYYYMMDD') + 1)
      LOOP    
        v_high_value := TRUNC(b.value + 1);
        EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||' 
                           SPLIT PARTITION MAX_VALUE 
                           AT    ('''||TO_CHAR(v_high_value, 'DD-MON-YYYY')||''') 
                           INTO  (PARTITION P_'||TO_CHAR(b.value, 'YYYYMMDD')||', 
                                  PARTITION MAX_VALUE)
                           UPDATE INDEXES';     
      END LOOP;
    ELSE
    
      -- user specified boundary
      FOR b IN (SELECT TO_DATE(boundary, 'YYYYMMDD') value FROM DUAL)
      LOOP
        v_high_value := TRUNC(b.value + 1);

        SELECT COUNT(PARTITION_NAME)
        INTO   v_exists
        FROM   USER_TAB_PARTITIONS
        WHERE  PARTITION_NAME = CONCAT('P_', TO_CHAR(b.value, 'YYYYMMDD'))
        AND    TABLE_NAME = a.table_name;

        IF v_exists = 0 THEN

          -- we cannot simply split MAX_VALUE like previously instead we must
          -- find the next highest partition in the list and split this one
          SELECT MIN(PARTITION_NAME)
          INTO   v_split_name
          FROM   USER_TAB_PARTITIONS
          WHERE  PARTITION_NAME > CONCAT('P_', TO_CHAR(b.value, 'YYYYMMDD'))
	  AND    PARTITION_NAME <> 'MAX_VALUE'
          AND    TABLE_NAME = a.table_name;

          IF v_split_name IS NULL THEN
            v_split_name := 'MAX_VALUE';
          END IF;
          
          EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||' 
                             SPLIT PARTITION '||v_split_name||'
                             AT    ('''||TO_CHAR(v_high_value, 'DD-MON-YYYY')||''') 
                             INTO  (PARTITION P_'||TO_CHAR(b.value, 'YYYYMMDD')||', 
                                    PARTITION '||v_split_name||')
                             UPDATE INDEXES';
        END IF;       
      END LOOP;
    END IF;
  END LOOP;

  -- compress tables older the today
  IF boundary = 0 THEN 
    v_table_name := 'P_'||TO_CHAR(SYSDATE, 'YYYYMMDD');
    FOR a IN (SELECT PARTITION_NAME, TABLE_NAME
              FROM   USER_TAB_PARTITIONS
              WHERE  COMPRESSION = 'DISABLED'
              AND    TABLE_NAME LIKE 'DLF_%'
              AND    (PARTITION_NAME <> 'MAX_VALUE' 
              AND    PARTITION_NAME < v_table_name)
              ORDER BY TABLE_NAME)
    LOOP
      EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||' 
                         MOVE PARTITION '||a.partition_name||' 
                         COMPRESS UPDATE INDEXES';
    END LOOP;
  END IF;
END;


/*
 * dlf_partition scheduler
 */
BEGIN
DBMS_SCHEDULER.CREATE_JOB (
	JOB_NAME 	=> 'dlf_partition_job',
	JOB_TYPE 	=> 'PLSQL_BLOCK',
	JOB_ACTION 	=> 'BEGIN
				DLF_PARTITION(0);
			   END;',
	START_DATE 	=> TRUNC(SYSDATE) + 1/24,
	REPEAT_INTERVAL => 'FREQ=DAILY',
	ENABLED 	=> TRUE,
	COMMENTS	=> 'Daily partitioning procedure');
END;


/* trigger a partition run immediately */
BEGIN
	dlf_partition(0);
END;


/*
 * archive/backup procedure 
 */
CREATE OR REPLACE PROCEDURE dlf_archive
AS

  -- variables
  v_expire      NUMBER := 0;
  v_mode        NUMBER := 0;
  v_name        VARCHAR2(10);
  v_value       VARCHAR2(255);

  -- data pump
  dp_handle     NUMBER;
  dp_job_state  VARCHAR(30);
  dp_status     KU$_STATUS; 
  
BEGIN

  -- set the nls_date_format
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = "DD-MON-YYYY"';

  -- extract the settings 
  SELECT value INTO v_value FROM dlf_settings WHERE name = 'ARCHIVE_MODE';
  v_mode := TO_NUMBER(v_value);

  SELECT value INTO v_value FROM dlf_settings WHERE name = 'ARCHIVE_EXPIRY';
  v_expire := TO_NUMBER(v_value);

  -- drop partition (no archiving) 
  IF v_mode = 1 THEN
    
    FOR a IN (SELECT TABLE_NAME, PARTITION_NAME
              FROM   USER_TAB_PARTITIONS
              WHERE  PARTITION_NAME = CONCAT('P_', TO_CHAR(SYSDATE - v_expire, 'YYYYMMDD'))
              AND    TABLE_NAME IN ('DLF_MESSAGES',  'DLF_NUM_PARAM_VALUES', 
                                    'DLF_STR_PARAM_VALUES')
              ORDER BY PARTITION_NAME)
    LOOP
      EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||'
                         DROP PARTITION '||a.partition_name;          
    END LOOP;

  -- drop partition with archiving
  ELSIF v_mode = 2 THEN
    
    -- retrieve a unique list of partition names older the 'v_expire' days old
    FOR a IN (SELECT DISTINCT(PARTITION_NAME)
              FROM   USER_TAB_PARTITIONS
              WHERE  PARTITION_NAME = CONCAT('P_', TO_CHAR(SYSDATE - v_expire, 'YYYYMMDD'))
              AND    TABLE_NAME IN ('DLF_MESSAGES',  'DLF_NUM_PARAM_VALUES', 
                                    'DLF_STR_PARAM_VALUES')
              ORDER BY PARTITION_NAME)
    LOOP    
      v_name := SUBSTR(a.partition_name, 3, 10);
      
      -- open datapump handle
      dp_handle := DBMS_DATAPUMP.OPEN(
          OPERATION   => 'EXPORT',
          JOB_MODE    => 'TABLE',
          JOB_NAME    => CONCAT(v_name, 'DP'),
          VERSION     => 'COMPATIBLE'
      );
      
      -- add the log and dump files
      DBMS_DATAPUMP.ADD_FILE(
          HANDLE      => dp_handle,
          FILENAME    => CONCAT(v_name, '.dmp'),
          DIRECTORY   => 'DLF_DATAPUMP_DIR',
          FILETYPE    => DBMS_DATAPUMP.KU$_FILE_TYPE_DUMP_FILE
      );
      DBMS_DATAPUMP.ADD_FILE(
          HANDLE      => dp_handle,
          FILENAME    => CONCAT(v_name, '.log'),
          DIRECTORY   => 'DLF_DATAPUMP_DIR',
          FILETYPE    => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE
      );
    
      DBMS_DATAPUMP.SET_PARAMETER(
          HANDLE      => dp_handle,
          NAME        => 'INCLUDE_METADATA',
          VALUE       => 0
      );   

      -- setup the metadata filters to restrict the export to a select partition
      FOR b IN (SELECT DISTINCT(TABLE_NAME) 
                FROM   USER_TAB_PARTITIONS
                WHERE  PARTITION_NAME <> 'MAX_VALUE'
                AND    PARTITION_NAME = a.partition_name
                ORDER BY TABLE_NAME ASC)
      LOOP
        DBMS_DATAPUMP.DATA_FILTER(
            HANDLE     => dp_handle,
            NAME       => 'PARTITION_EXPR',
            VALUE      => '= '''||a.partition_name||'''',
            TABLE_NAME => b.table_name
        );      
      END LOOP;    
      
      -- only include tables of interest
      DBMS_DATAPUMP.METADATA_FILTER(
          HANDLE      => dp_handle,
          NAME	      => 'NAME_EXPR',
          VALUE       => 'IN (''DLF_MESSAGES'',  ''DLF_NUM_PARAM_VALUES'',
                              ''DLF_STR_PARAM_VALUES'')',
          OBJECT_PATH => 'TABLE'
      );  
      
      -- start the data pump job
      DBMS_DATAPUMP.START_JOB(dp_handle);
      
      -- at this point we have a data pump job running we must wait for it to
      -- finish successfully before we can drop the data!
      dp_job_state := 'UNDEFINED';
      
      WHILE (dp_job_state != 'COMPLETED') AND (dp_job_state != 'STOPPED')
      LOOP
        DBMS_DATAPUMP.GET_STATUS(
            HANDLE     => dp_handle,
            MASK       => 15,
            TIMEOUT    => NULL,
            JOB_STATE  => dp_job_state,
            STATUS     => dp_status
        );     
      END LOOP;
      
      -- a completed job equals successful export
      IF dp_job_state = 'COMPLETED' THEN
        FOR b IN (SELECT TABLE_NAME 
                  FROM   USER_TAB_PARTITIONS
                  WHERE  PARTITION_NAME = a.partition_name
                  AND    TABLE_NAME IN ('DLF_MESSAGES',  'DLF_NUM_PARAM_VALUES', 
                                        'DLF_STR_PARAM_VALUES'))
        LOOP
          EXECUTE IMMEDIATE 'ALTER TABLE '||b.table_name||'
                             DROP PARTITION '||a.partition_name;       
        END LOOP;      
      END IF;    
    END LOOP;
  END IF;  
END;


/*
 * dlf_archive scheduler
 */
BEGIN
DBMS_SCHEDULER.CREATE_JOB (
	JOB_NAME 	=> 'dlf_archive_job',
	JOB_TYPE 	=> 'STORED_PROCEDURE',
	JOB_ACTION 	=> 'DLF_ARCHIVE',
	START_DATE 	=> TRUNC(SYSDATE) + 2/24,
	REPEAT_INTERVAL => 'FREQ=DAILY',
	ENABLED 	=> TRUE,
	COMMENTS	=> 'Daily archiving procedure');
END;


/*
 * dlf_stats_jobs (Populates table: dlf_jobstats)
 */
CREATE OR REPLACE PROCEDURE dlf_stats_jobs
AS

  -- variables
  v_time	DATE;
  v_mode        NUMBER := 0;
  v_value       VARCHAR2(255);

BEGIN

  -- job stats enabled ?
  SELECT value INTO v_value FROM dlf_settings WHERE name = 'ENABLE_JOBSTATS';
  v_mode := TO_NUMBER(v_value);

  IF v_mode = 1 THEN
    -- set the nls_date_format
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = "YYYYMMDDHH24MISS"';

    -- we define the time as a variable so that all data recorded will be synchronised
    v_time := TO_DATE(SYSDATE - 10/1440, 'YYYYMMDDHH24MISS');

    FOR i IN 35..40 LOOP
      IF ((i = 35)  OR
          (i = 36)  OR
          (i = 37)  OR
          (i = 40)) THEN
            INSERT INTO dlf_jobstats VALUES(v_time, i, 0, 0, 0, 0, 0, 0, 900);
      END IF;
    END LOOP;

    -- update the exiting statistics data
    FOR a IN (SELECT p.value, AVG(diff) avg, STDDEV_POP(diff) stddev, MIN(diff) min,
	  	     MAX(diff) max, COUNT(*) count
                   
              FROM (SELECT id, reqid, timestamp, msg_no,
                    LEAD(TO_NUMBER(timestamp), 1)
                      OVER (PARTITION BY reqid ORDER by timestamp, msg_no) next_timestamp,
                    LEAD(TO_NUMBER(timestamp), 1)
                      OVER (ORDER BY reqid) - TO_NUMBER(timestamp) diff
              FROM (

		  -- intersection of request ids
		  SELECT * FROM dlf_messages WHERE reqid IN(
		    SELECT reqid FROM dlf_messages
                      WHERE timestamp >  TO_DATE(SYSDATE - 25/1440, 'YYYYMMDDHH24MISS')
                      AND   timestamp <= TO_DATE(SYSDATE - 10/1440, 'YYYYMMDDHH24MISS')
                      AND   facility = 5
                      AND   msg_no   = 15
                  INTERSECT
                    SELECT reqid FROM dlf_messages
                      WHERE timestamp > TO_DATE(SYSDATE - 2, 'YYYYMMDDHH24MISS')
                      AND   facility = 5
                      AND   msg_no   = 12)
	 	  AND timestamp > TO_DATE(SYSDATE - 2, 'YYYYMMDDHH24MISS')
		  AND facility = 5
		  AND ((msg_no = 12) OR (msg_no = 15))

		  ) ORDER BY reqid
	      ) m, dlf_num_param_values p

	      WHERE m.msg_no = 12
	      AND   m.id = p.id
              AND   p.timestamp > TO_DATE(SYSDATE - 2, 'YYYYMMDDHH24MISS')
	      AND   p.name = 'type'
	      AND   p.value IN (35, 36, 37, 40)
	      GROUP BY p.value ORDER BY p.value)
    LOOP
      EXECUTE IMMEDIATE 'UPDATE dlf_jobstats SET
                                min_time  = '||a.min||',
                                max_time  = '||a.max||',
                                avg_time  = '||a.avg||',
                                std_dev   = '||a.stddev||',
                                exiting   = '||a.count||'
			  WHERE timestamp = '''||v_time||'''
			  AND   type      = '||a.value;    
    END LOOP;
       
    -- update the starting statistics data
    FOR a IN (SELECT p.value, COUNT(m.reqid) count 
              FROM   dlf_messages m, dlf_num_param_values p
              WHERE  m.timestamp >  TO_DATE(SYSDATE - 25/1440, 'YYYYMMDDHH24MISS')
              AND    m.timestamp <= TO_DATE(SYSDATE - 10/1440, 'YYYYMMDDHH24MISS')
              AND    m.facility = 5
              AND    m.msg_no   = 12
              AND    m.id       = p.id
              AND    p.name     = 'type'
              GROUP BY p.value)
    LOOP
      EXECUTE IMMEDIATE 'UPDATE dlf_jobstats SET starting = '||a.count||'
                         WHERE  timestamp = '''||v_time||'''
                         AND    type = '||a.value;
    END LOOP;
  END IF;
END;


/*
 * dlf_stats_requests (Populates table: dlf_reqstats)
 */
CREATE OR REPLACE PROCEDURE dlf_stats_requests
AS

  -- variables
  v_time	DATE;
  v_mode        NUMBER := 0;
  v_value       VARCHAR2(255);

BEGIN

  -- requets stats enabled ?
  SELECT value INTO v_value FROM dlf_settings WHERE name = 'ENABLE_REQSTATS';
  v_mode := TO_NUMBER(v_value);

  IF v_mode = 1 THEN
    -- set the nls_date_format
    EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = "YYYYMMDDHH24MISS"';

    -- we define the time as a variable so that all data recorded will be synchronised
    v_time := TO_DATE(SYSDATE - 10/1440, 'YYYYMMDDHH24MISS');

    FOR i IN 35..40 LOOP
      IF ((i = 35)  OR
          (i = 36)  OR
          (i = 37)  OR
          (i = 40)) THEN
            INSERT INTO dlf_reqstats VALUES(v_time, i, 0, 0, 0, 0, 0, 900);    
      END IF;
    END LOOP;
  
    -- update the statistics data
    FOR a IN (SELECT type, avg(proctime) avg, stddev_pop(proctime) stddev, min(proctime) min,
		     max(proctime) max, count(*) count
	      FROM (

		  SELECT m.msg_no, (p.value * 0.001) proctime,
			  LEAD (p.value, 1) 
			  OVER (PARTITION BY m.reqid ORDER BY m.timestamp, m.msg_no) type
		  FROM (

                    -- intersection of request ids
                    SELECT * FROM dlf_messages WHERE reqid IN (
                      SELECT reqid FROM dlf_messages
                        WHERE timestamp >  TO_DATE(SYSDATE - 25/1440, 'YYYYMMDDHH24MISS')
                        AND   timestamp <= TO_DATE(SYSDATE - 10/1440, 'YYYYMMDDHH24MISS')
                        AND   facility = 4
                        AND   msg_no   = 10
                    INTERSECT
                      SELECT reqid FROM dlf_messages
                        WHERE timestamp > TO_DATE(SYSDATE - 2, 'YYYYMMDDHH24MISS')
                        AND   facility = 4
                        AND   msg_no   = 12
                    )
		    AND timestamp > TO_DATE(SYSDATE - 2, 'YYYYMMDDHH24MISS')
                    AND facility = 4
                    AND ((msg_no = 10) OR (msg_no = 12))		    

		  ) m, dlf_num_param_values p
		  WHERE p.timestamp > TO_DATE(SYSDATE - 2, 'YYYYMMDDHH24MISS')
		  AND   m.id = p.id
		  AND   p.name IN ('Type', 'ProcTime')

	      )
	      WHERE msg_no = 10
	      AND   type IN (35, 36, 37, 40)
	      GROUP BY type ORDER BY type)
    LOOP
      EXECUTE IMMEDIATE 'UPDATE dlf_reqstats SET
                                min_time  = '||a.min||',
                                max_time  = '||a.max||',
                                avg_time  = '||a.avg||',
                                std_dev   = '||a.stddev||',
                                req_count = '||a.count||'
			  WHERE timestamp = '''||v_time||'''
			  AND   type      = '||a.type;    
    END LOOP;
  END IF;
END;


/*
 * statistics scheduler
 */
BEGIN
DBMS_SCHEDULER.CREATE_JOB (
	JOB_NAME 	=> 'DLF_STATS_15MINS',
	JOB_TYPE 	=> 'PLSQL_BLOCK',
	JOB_ACTION 	=> 'BEGIN
				DLF_STATS_JOBS();
				DLF_STATS_REQUESTS();
			   END;',
	START_DATE 	=> SYSDATE,
	REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=15',
	ENABLED 	=> TRUE,
	COMMENTS	=> 'CASTOR2 Monitoring Statistics (15 Minute Frequency)');
END;


/** End-of-File **/
