/*                        ORACLE ENTERPRISE EDITION                         */
/*                                                                          */
/* This file contains SQL code that will generate the dlf database schema   */
/* tablespaces DLF_INDX and DLF_DATA must be present on the target database */
/* and DBA privileges must be present for scheduling the maintenance job    */

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
	hostid		NUMBER,
	facility	NUMBER(3),
	severity	NUMBER(3),
	msg_no		NUMBER(5),
	pid		NUMBER(10),
	tid		NUMBER(10),
	nshostid	NUMBER,
	nsfileid	NUMBER)
COMPRESS
PARTITION BY RANGE (timestamp)
SUBPARTITION BY LIST (facility)
SUBPARTITION TEMPLATE
(
	SUBPARTITION S_DEFAULT VALUES(DEFAULT) TABLESPACE dlf_data
)
(
	PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE) TABLESPACE dlf_data
)
TABLESPACE dlf_data;

/* local indexes */
CREATE INDEX i_msg_timestamp ON dlf_message (timestamp) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_fac ON dlf_messages (facility) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_pid ON dlf_messages (pid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_reqid ON dlf_messages (reqid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_hostid ON dlf_messages (hostid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_nshostid ON dlf_messages (nshostid) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_msg_fileid ON dlf_messages (nsfileid) LOCAL TABLESPACE dlf_indx;

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
 * dlf_reqid_map
 */
CREATE TABLE dlf_reqid_map
(
	id		NUMBER,
	timestamp	DATE NOT NULL,
	reqid		CHAR(36),
	subreqid	CHAR(36))
PARTITION BY RANGE (timestamp)
(
	PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE) TABLESPACE dlf_data
)
TABLESPACE dlf_data;

CREATE INDEX i_req_id ON dlf_reqid_map (id) LOCAL TABLESPACE dlf_indx;
CREATE INDEX i_req_subreqid ON dlf_reqid_map (subreqid) LOCAL TABLESPACE dlf_indx;

/*
 * dlf_tape_ids
 */
CREATE TABLE dlf_tape_ids
(
	id		NUMBER,
	timestamp	DATE NOT NULL,
	tapevid		VARCHAR2(20))
PARTITION BY RANGE (timestamp)
(
	PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE) TABLESPACE dlf_data
)
TABLESPACE dlf_data;

CREATE INDEX i_tape_id ON dlf_tape_ids (id) LOCAL TABLESPACE dlf_indx;

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

CREATE UNIQUE INDEX i_msg_texts ON dlf_msg_texts (fac_no, msg_no);

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

/* initialise sequences */
INSERT INTO dlf_sequences (seq_name, seq_no) VALUES ('id',       1);
INSERT INTO dlf_sequences (seq_name, seq_no) VALUES ('hostid',   1);
INSERT INTO dlf_sequences (seq_name, seq_no) VALUES ('nshostid', 1);

/* initialise mode information */
INSERT INTO dlf_mode (name, enabled) VALUES ('queue_purge',      0);
INSERT INTO dlf_mode (name, enabled) VALUES ('queue_suspend',    0);
INSERT INTO dlf_mode (name, enabled) VALUES ('database_suspend', 0);


/*
 * dlf_partition procedure
 */
CREATE OR REPLACE PROCEDURE dlf_partition
AS
	-- cursors
	v_cur_table	INTEGER;
	v_cur_partition	INTEGER;
	v_cur_range	INTEGER;
	v_cur_subpart	INTEGER;
	v_cur_setting	INTEGER;

	-- variables
	v_tablename	USER_TABLES.TABLE_NAME%TYPE;
	v_partname	USER_TAB_PARTITIONS.PARTITION_NAME%TYPE;
	v_subname	USER_TAB_SUBPARTITIONS.SUBPARTITION_NAME%TYPE;
	v_subflag	INTEGER;
	v_facno		INTEGER;
	v_date		VARCHAR(30);
	v_rows		INTEGER;
	v_boundary	DATE;

BEGIN

	-- set the nls_time_date for this session, the partitioning is sensitive to the
	-- date format reported by sysdate. (alternative: to_char sysdate)
	v_cur_setting := DBMS_SQL.OPEN_CURSOR;
	v_rows := 0;

	DBMS_SQL.PARSE(v_cur_setting,
		'ALTER SESSION
		 SET NLS_DATE_FORMAT = ''DD-MON-YYYY''',
		DBMS_SQL.NATIVE);
	v_rows := DBMS_SQL.EXECUTE(v_cur_setting);

	-- free resources
	DBMS_SQL.CLOSE_CURSOR(v_cur_setting);

	-- variables
	v_cur_table := DBMS_SQL.OPEN_CURSOR;
	v_rows := 0;

	-- determine those tables which already have partitions 'MAX_VALUE'
	DBMS_SQL.PARSE(v_cur_table,
		'SELECT TABLE_NAME, SUM(SUBPARTITION_COUNT)
		 FROM USER_TAB_PARTITIONS
		 WHERE TABLE_NAME LIKE ''DLF_%''
		 GROUP BY TABLE_NAME
		 ORDER BY TABLE_NAME',
		DBMS_SQL.NATIVE);

	-- define columns, execute statement
	DBMS_SQL.DEFINE_COLUMN(v_cur_table, 1, v_tablename, 30);
	DBMS_SQL.DEFINE_COLUMN(v_cur_table, 2, v_subflag);
	v_rows := DBMS_SQL.EXECUTE(v_cur_table);

	-- loop over tables
	LOOP
		IF DBMS_SQL.FETCH_ROWS(v_cur_table) = 0 THEN
			EXIT;
		END IF;
		DBMS_SQL.COLUMN_VALUE(v_cur_table, 1, v_tablename);
		DBMS_SQL.COLUMN_VALUE(v_cur_table, 2, v_subflag);

		-- determine the maximum partition boundary
		v_cur_partition := DBMS_SQL.OPEN_CURSOR;
		v_rows := 0;

		DBMS_SQL.PARSE(v_cur_partition,
			'SELECT MAX(TO_DATE(SUBSTR(PARTITION_NAME, 3, 10), ''YYYYMMDD''))
			 FROM USER_TAB_PARTITIONS
			 WHERE PARTITION_NAME <> ''MAX_VALUE''
			 AND TABLE_NAME = :x',
			DBMS_SQL.NATIVE);
		DBMS_SQL.BIND_VARIABLE(v_cur_partition, ':x', v_tablename);

		-- define columns, execute statement
		DBMS_SQL.DEFINE_COLUMN(v_cur_partition, 1, v_date, 30);
		v_rows := DBMS_SQL.EXECUTE(v_cur_partition);

		-- get max partition date
		LOOP
			IF DBMS_SQL.FETCH_ROWS(v_cur_partition) = 0 THEN
				EXIT;
			END IF;
			DBMS_SQL.COLUMN_VALUE(v_cur_partition, 1, v_date);
		END LOOP;

		-- no date defined, this must be our first execution
		IF v_date IS NULL THEN
			v_date := TO_DATE(TRUNC(SYSDATE - 1), 'DD-MON-YY');
		END IF;

		-- free resources
		DBMS_SQL.CLOSE_CURSOR(v_cur_partition);

		-- generate a list of partition boundary dates from 'v_date' for 7 days
		-- into the future
		v_cur_range := DBMS_SQL.OPEN_CURSOR;
		v_rows := 0;

		DBMS_SQL.PARSE(v_cur_range,
			'SELECT TO_DATE(:s, ''DD-MON-YYYY'') + ROWNUM - 1
			 FROM ALL_OBJECTS
			 WHERE ROWNUM <=
			 TO_DATE(TO_CHAR(SYSDATE + 7, ''DD-MON-YYYY''), ''DD-MON-YYYY'') -
			 TO_DATE(:s , ''DD-MON-YYYY'') + 1',
			DBMS_SQL.NATIVE);
		DBMS_SQL.BIND_VARIABLE(v_cur_range, ':s', TO_DATE(v_date, 'DD-MON-YYYY') + 1);

		-- define columns, execute statement
		DBMS_SQL.DEFINE_COLUMN(v_cur_range, 1, v_date, 30);
		v_rows := DBMS_SQL.EXECUTE(v_cur_range);

		-- loop over results
		LOOP
			IF DBMS_SQL.FETCH_ROWS(v_cur_range) = 0 THEN
				EXIT;
			END IF;
			DBMS_SQL.COLUMN_VALUE(v_cur_range, 1, v_date);
			v_boundary := TRUNC(TO_DATE(v_date, 'DD-MON-YYYY') + 1);

			-- split the MAX_VALUE partition
			v_cur_partition := DBMS_SQL.OPEN_CURSOR;
			v_rows := 0;

			DBMS_SQL.PARSE(v_cur_partition,'ALTER TABLE '||v_tablename||'
				 SPLIT PARTITION MAX_VALUE
				 AT (TO_DATE('''||TO_CHAR(v_boundary,'DD-MON-YYYY')||''',''DD-MON-YYYY''))
				 INTO (PARTITION '||TO_CHAR(TO_DATE(v_date, 'DD-MON-YYYY'), '"P_"YYYYMMDD')||', PARTITION MAX_VALUE)',
				DBMS_SQL.NATIVE);
			v_rows := DBMS_SQL.EXECUTE(v_cur_partition);

			-- free resources
			DBMS_SQL.CLOSE_CURSOR(v_cur_partition);

		END LOOP;

		-- free resources
		DBMS_SQL.CLOSE_CURSOR(v_cur_range);

		-- if the table has no subpartitions jump to end of table loop
		IF v_subflag = 0 THEN
			GOTO eotl;
		END IF;

		-- loop over all partitions for 'v_tablename'
		v_cur_partition := DBMS_SQL.OPEN_CURSOR;
		v_rows := 0;

		DBMS_SQL.PARSE(v_cur_partition,
			'SELECT PARTITION_NAME
			 FROM USER_TAB_PARTITIONS
			 WHERE TABLE_NAME = :x
			 AND PARTITION_NAME <> ''MAX_VALUE''',
			DBMS_SQL.NATIVE);
		DBMS_SQL.BIND_VARIABLE(v_cur_partition, ':x', v_tablename);

		-- define columns, execute statement
		DBMS_SQL.DEFINE_COLUMN(v_cur_partition, 1, v_partname, 30);
		v_rows := DBMS_SQL.EXECUTE(v_cur_partition);

		-- loop over results
		LOOP
			IF DBMS_SQL.FETCH_ROWS(v_cur_partition) = 0 THEN
				EXIT;
			END IF;
			DBMS_SQL.COLUMN_VALUE(v_cur_partition, 1, v_partname);

			-- generate a list of facilities which do not have a subpartition
			v_cur_range := DBMS_SQL.OPEN_CURSOR;
			v_rows := 0;

			DBMS_SQL.PARSE(v_cur_range,
				'SELECT FAC_NO FROM DLF_FACILITIES
				 WHERE FAC_NO NOT IN
				 (SELECT TO_NUMBER(SUBSTR(SUBPARTITION_NAME, 13), ''99'')
				 FROM USER_TAB_SUBPARTITIONS
				 WHERE SUBPARTITION_NAME NOT LIKE ''%DEFAULT%''
				 AND TABLE_NAME = :x
				 AND PARTITION_NAME = :y)
				 ORDER BY FAC_NO',
				DBMS_SQL.NATIVE);
			DBMS_SQL.BIND_VARIABLE(v_cur_range, ':x', v_tablename);
			DBMS_SQL.BIND_VARIABLE(v_cur_range, ':y', v_partname);

			-- define columns, execute statement
			DBMS_SQL.DEFINE_COLUMN(v_cur_range, 1, v_facno);
			v_rows := DBMS_SQL.EXECUTE(v_cur_range);

			-- loop over results
			LOOP
				IF DBMS_SQL.FETCH_ROWS(v_cur_range) = 0 THEN
					EXIT;
				END IF;
				DBMS_SQL.COLUMN_VALUE(v_cur_range, 1, v_facno);
				v_subname := CONCAT(v_partname, '_S'||v_facno);

				-- split the default subpartition
				v_cur_subpart := DBMS_SQL.OPEN_CURSOR;
				v_rows := 0;

				DBMS_SQL.PARSE(v_cur_subpart,
					'ALTER TABLE '||v_tablename||'
					 SPLIT SUBPARTITION '||v_partname||'_S_DEFAULT VALUES ('||v_facno||')
					 INTO (SUBPARTITION '||v_subname||', SUBPARTITION '||v_partname||'_S_DEFAULT)',
					DBMS_SQL.NATIVE);
				v_rows := DBMS_SQL.EXECUTE(v_cur_subpart);

				-- free resources
				DBMS_SQL.CLOSE_CURSOR(v_cur_subpart);
			END LOOP;

			-- free resources
			DBMS_SQL.CLOSE_CURSOR(v_cur_range);

		END LOOP;

		-- free resources
		DBMS_SQL.CLOSE_CURSOR(v_cur_partition);

		<<eotl>>
		NULL;
	END LOOP;

	-- free resources
	DBMS_SQL.CLOSE_CURSOR(v_cur_table);

	-- compress historical partitions. This can only be done on the range partitions as
	-- subpartitions cannot be compressed. Either the whole table is compressed or not.
	v_cur_table := DBMS_SQL.OPEN_CURSOR;
	v_rows := 0;

	DBMS_SQL.PARSE(v_cur_table,
		'SELECT PARTITION_NAME, TABLE_NAME
		 FROM USER_TAB_PARTITIONS
		 WHERE COMPRESSION = ''DISABLED''
		 AND TABLE_NAME LIKE ''DLF_%''
		 AND SUBPARTITION_COUNT = 0
		 AND (PARTITION_NAME <> ''MAX_VALUE''
		 AND PARTITION_NAME < CONCAT(''P_'', TO_CHAR(SYSDATE, ''YYYYMMDD'')))
		 ORDER BY TABLE_NAME',
		DBMS_SQL.NATIVE);

	-- define columns, execute statment
	DBMS_SQL.DEFINE_COLUMN(v_cur_table, 1, v_partname, 30);
	DBMS_SQL.DEFINE_COLUMN(v_cur_table, 2, v_tablename, 30);
	v_rows := DBMS_SQL.EXECUTE(v_cur_table);

	-- loop over results
	LOOP
		IF DBMS_SQL.FETCH_ROWS(v_cur_table) = 0 THEN
			EXIT;
		END IF;
		DBMS_SQL.COLUMN_VALUE(v_cur_table, 1, v_partname);
		DBMS_SQL.COLUMN_VALUE(v_cur_table, 2, v_tablename);

		-- compress the partition
		v_cur_partition := DBMS_SQL.OPEN_CURSOR;
		v_rows := 0;

		DBMS_SQL.PARSE(v_cur_partition,
			'ALTER TABLE '||v_tablename||'
			 MOVE PARTITION '||v_partname||'
			 COMPRESS',
			DBMS_SQL.NATIVE);
		v_rows := DBMS_SQL.EXECUTE(v_cur_partition);

		-- free resources
		DBMS_SQL.CLOSE_CURSOR(v_cur_partition);
	END LOOP;

	-- free resources
	DBMS_SQL.CLOSE_CURSOR(v_cur_table);

EXCEPTION
	WHEN OTHERS THEN

		-- close all open cursors
		IF DBMS_SQL.IS_OPEN(v_cur_table) THEN
			DBMS_SQL.CLOSE_CURSOR(v_cur_table);
		END IF;
		IF DBMS_SQL.IS_OPEN(v_cur_partition) THEN
			DBMS_SQL.CLOSE_CURSOR(v_cur_partition);
		END IF;
		IF DBMS_SQL.IS_OPEN(v_cur_range) THEN
			DBMS_SQL.CLOSE_CURSOR(v_cur_range);
		END IF;
		IF DBMS_SQL.IS_OPEN(v_cur_subpart) THEN
			DBMS_SQL.CLOSE_CURSOR(v_cur_subpart);
		END IF;
		IF DBMS_SQL.IS_OPEN(v_cur_setting) THEN
			DBMS_SQL.CLOSE_CURSOR(v_cur_setting);
		END IF;
END;

/*
 * dlf_partition scheduler
 */
BEGIN
DBMS_SCHEDULER.CREATE_JOB (
	JOB_NAME 	=> 'dlf_partition_job',
	JOB_TYPE 	=> 'STORED_PROCEDURE',
	JOB_ACTION 	=> 'DLF_PARTITION',
	START_DATE 	=> TRUNC(SYSDATE) + 1/24,
	REPEAT_INTERVAL => 'FREQ=DAILY',
	ENABLED 	=> TRUE,
	COMMENTS	=> 'Daily partitioning procedure');
END;

/* trigger a partition run immediately */
BEGIN
	dlf_partition;
END;


/* archive/backup procedure */
CREATE OR REPLACE
PROCEDURE dlf_archive
AS
	-- cursors
	v_cur_partition	INTEGER;
	v_cur_drop	INTEGER;
	v_cur_setting	INTEGER;
	v_cur_table	INTEGER;

	-- variables
	v_partname	USER_TAB_PARTITIONS.PARTITION_NAME%TYPE;
	v_tablename	USER_TABLES.TABLE_NAME%TYPE;
	v_rows		INTEGER;
	v_expire	INTEGER;

	-- data pump
	dp_handle	NUMBER;
	dp_job_state 	VARCHAR(30);
        dp_status       KU$_STATUS;
        
BEGIN

	-- set the nls_time_date for this session, the partitioning is sensitive to the
	-- date format reported by sysdate. (alternative: to_char sysdate)
	v_cur_setting := DBMS_SQL.OPEN_CURSOR;
	v_rows := 0;

	DBMS_SQL.PARSE(v_cur_setting,
		'ALTER SESSION
		 SET NLS_DATE_FORMAT = ''DD-MON-YYYY''',
		DBMS_SQL.NATIVE);
	v_rows := DBMS_SQL.EXECUTE(v_cur_setting);

	-- free resources
	DBMS_SQL.CLOSE_CURSOR(v_cur_setting);

	-- variables
	v_cur_partition := DBMS_SQL.OPEN_CURSOR;
	v_rows   := 0;
	v_expire := 30;

	-- retrieve a unqiue list of partition names older the 'x' days old
	DBMS_SQL.PARSE(v_cur_partition,
		'SELECT DISTINCT(PARTITION_NAME)
		 FROM USER_TAB_PARTITIONS
		 WHERE PARTITION_NAME <> ''MAX_VALUE''
		 AND PARTITION_NAME = CONCAT(''P_'', TO_CHAR(SYSDATE - '||v_expire||', ''YYYYMMDD''))
                 AND TABLE_NAME IN (''DLF_MESSAGES'',''DLF_NUM_PARAM_VALUES'',''DLF_REQID_MAP'',''DLF_STR_PARAM_VALUES'',''DLF_TAPE_IDS'')
		 ORDER BY PARTITION_NAME ASC',
		DBMS_SQL.NATIVE);

	-- define columns, execute statement
	DBMS_SQL.DEFINE_COLUMN(v_cur_partition, 1, v_partname, 30);
	v_rows := DBMS_SQL.EXECUTE(v_cur_partition);

	-- loop over results
	LOOP
		IF DBMS_SQL.FETCH_ROWS(v_cur_partition) = 0 THEN
			EXIT;
		END IF;
		DBMS_SQL.COLUMN_VALUE(v_cur_partition, 1, v_partname);

		-- open a datapump handle
		dp_handle := DBMS_DATAPUMP.OPEN(
			OPERATION  => 'EXPORT',
			JOB_MODE   => 'TABLE',
			JOB_NAME   => CONCAT(SUBSTR(v_partname, 3, 10), 'DP'),
			VERSION	   => 'COMPATIBLE'
		);

		-- add the log and dump files
		DBMS_DATAPUMP.ADD_FILE(
			HANDLE     => dp_handle,
			FILENAME   => CONCAT(SUBSTR(v_partname, 3, 10), '.dmp'),
			DIRECTORY  => 'DLF_DATAPUMP_DIR',
			FILETYPE   => DBMS_DATAPUMP.KU$_FILE_TYPE_DUMP_FILE
		);
		DBMS_DATAPUMP.ADD_FILE(
			HANDLE     => dp_handle,
			FILENAME   => CONCAT(SUBSTR(v_partname, 3, 10), '.log'),
			DIRECTORY  => 'DLF_DATAPUMP_DIR',
			FILETYPE   => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE
		);
    
		DBMS_DATAPUMP.SET_PARAMETER(
			HANDLE     => dp_handle,
			NAME       => 'INCLUDE_METADATA',
			VALUE      => 0
		);
                
		-- setup the metadata and data filters to restrict the export to partitioned tables
		-- and a specific partition name representing a given day.
		v_cur_table := DBMS_SQL.OPEN_CURSOR;
		v_rows   := 0;

		-- retrieve a unique list of partition names older the 'x' days old
		DBMS_SQL.PARSE(v_cur_table,
			'SELECT DISTINCT(TABLE_NAME)
			 FROM USER_TAB_PARTITIONS
			 WHERE PARTITION_NAME <> ''MAX_VALUE''
			 AND PARTITION_NAME = :x
			 ORDER BY TABLE_NAME ASC',
			DBMS_SQL.NATIVE);
		DBMS_SQL.BIND_VARIABLE(v_cur_table, ':x', v_partname);

		-- define columns, execute statement
		DBMS_SQL.DEFINE_COLUMN(v_cur_table, 1, v_tablename, 30);
		v_rows := DBMS_SQL.EXECUTE(v_cur_table);
        
		-- only include the major dlf tables with partitions. We deliberately ignore the
		-- smaller statistics tables as their size has relatively no impact on storage space. 
		DBMS_DATAPUMP.METADATA_FILTER(
			HANDLE 	    => dp_handle,
			NAME	    => 'NAME_EXPR',
			VALUE	    => 'IN (''DLF_MESSAGES'',''DLF_NUM_PARAM_VALUES'',''DLF_REQID_MAP'',''DLF_STR_PARAM_VALUES'',''DLF_TAPE_IDS'')',
			OBJECT_PATH => 'TABLE'
		);    

		LOOP
			IF DBMS_SQL.FETCH_ROWS(v_cur_table) = 0 THEN
				EXIT;
			END IF;
			DBMS_SQL.COLUMN_VALUE(v_cur_table, 1, v_tablename);

			-- exclude partitions not in the scope of the dump
			DBMS_DATAPUMP.DATA_FILTER(
				HANDLE 	   => dp_handle,
				NAME	   => 'PARTITION_EXPR',
				VALUE	   => '= '''||v_partname||'''',
				TABLE_NAME => v_tablename
			);
		END LOOP;

		-- free resources
		DBMS_SQL.CLOSE_CURSOR(v_cur_table);

		-- start the data pump job
		DBMS_DATAPUMP.START_JOB(dp_handle);

		-- at this point we have a data pump job visible through: 'select * from dba_datapump_jobs'
		-- in order to determine whether it is safe to drop the partition from the schema we must
		-- monitoring the jobs progress and wait for completion
		dp_job_state := 'UNDEFINED';
                
		WHILE (dp_job_state != 'COMPLETED') AND (dp_job_state != 'STOPPED')
			LOOP
				DBMS_DATAPUMP.GET_STATUS(
					HANDLE    => dp_handle,
					MASK      => 15,
					TIMEOUT   => NULL,
					JOB_STATE => dp_job_state,
					STATUS    => dp_status                   
				);                        
			END LOOP;

		-- a completed state equals a successful export
		IF dp_job_state = 'COMPLETED' THEN
                      
		v_cur_table := DBMS_SQL.OPEN_CURSOR;
		v_rows   := 0;
                     
		DBMS_SQL.PARSE(v_cur_table,
			'SELECT TABLE_NAME
			 FROM USER_TABLES
			 WHERE TABLE_NAME IN (''DLF_MESSAGES'',''DLF_NUM_PARAM_VALUES'',''DLF_REQID_MAP'',''DLF_STR_PARAM_VALUES'',''DLF_TAPE_IDS'')
			 ORDER BY TABLE_NAME ASC',
			DBMS_SQL.NATIVE);
                
		-- define columns, execute statement
		DBMS_SQL.DEFINE_COLUMN(v_cur_table, 1, v_tablename, 30);
		v_rows := DBMS_SQL.EXECUTE(v_cur_table);
                
		LOOP
			IF DBMS_SQL.FETCH_ROWS(v_cur_table) = 0 THEN
				EXIT;
			END IF;
			DBMS_SQL.COLUMN_VALUE(v_cur_table, 1, v_tablename);

			-- drop the partition
			v_cur_drop := DBMS_SQL.OPEN_CURSOR;
			v_rows := 0;

			DBMS_SQL.PARSE(v_cur_drop,
				'ALTER TABLE '||v_tablename||'
				 TRUNCATE PARTITION '||v_partname||'',
				DBMS_SQL.NATIVE);
			v_rows := DBMS_SQL.EXECUTE(v_cur_drop);

			-- free resources
			DBMS_SQL.CLOSE_CURSOR(v_cur_drop);
   
 		END LOOP;
                
 		DBMS_SQL.CLOSE_CURSOR(v_cur_table);
           
		END IF;
	END LOOP;

	-- free resources
	DBMS_SQL.CLOSE_CURSOR(v_cur_partition);

EXCEPTION
	WHEN OTHERS THEN

		-- close all open cursors
		IF DBMS_SQL.IS_OPEN(v_cur_partition) THEN
			DBMS_SQL.CLOSE_CURSOR(v_cur_partition);
		END IF;
		IF DBMS_SQL.IS_OPEN(v_cur_drop) THEN
			DBMS_SQL.CLOSE_CURSOR(v_cur_drop);
		END IF;
		IF DBMS_SQL.IS_OPEN(v_cur_setting) THEN
			DBMS_SQL.CLOSE_CURSOR(v_cur_setting);
		END IF;
		IF DBMS_SQL.IS_OPEN(v_cur_table) THEN
			DBMS_SQL.CLOSE_CURSOR(v_cur_table);
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

/** End-of-File **/
