/*                        ORACLE ENTERPRISE EDITION                         */
/*                                                                          */
/* This file contains SQL code that will upgrade the DLF database schema    */
/* from version 2.1.1-9 to 2.1.x                                            */
/*                                                                          */
/* WARNING:                                                                 */
/*    - This upgrade procedure is not for use with ORACLE express           */
/*    - The dlf server should be stopped during this upgrade                */
/*    - Once the script has progressed to the MERGE stage the dlf server    */
/*      can be restarted.                                                   */
/*    - Depending on the size of the database the MERGE operation can take  */
/*      several hours!                                                      */


/* table alterations
 *    - addition tapevid column on dlf_messages
 */
ALTER TABLE DLF_MESSAGES ADD (tapevid VARCHAR2(20));
UPDATE DLF_MESSAGES SET TAPEVID = 'N/A';

/* merge
 *    - contents of dlf_tape_ids merged into dlf_messages
 */
MERGE INTO DLF_MESSAGES t1
USING (SELECT * FROM DLF_TAPE_IDS) t2
ON (t1.id = t2.id)
WHEN MATCHED THEN UPDATE SET t1.tapevid = t2.tapevid;

/*
 * indexes
 */
CREATE INDEX i_msg_tapevid ON dlf_messages (tapevid) LOCAL TABLESPACE dlf_indx;

/*
 * drop tables
 */
DROP TABLE DLF_TAPE_IDS;

/*
 * rename the dlf_monitoring table (2.1.1-9 bug)
 */
ALTER TABLE DLF_MONITORING2 RENAME TO DLF_MONITORING;

/*
 * procedures
 */
CREATE OR REPLACE PROCEDURE dlf_archive
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
	dp_status	KU$_STATUS;
        
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
                 AND TABLE_NAME IN (''DLF_MESSAGES'',''DLF_NUM_PARAM_VALUES'',''DLF_REQID_MAP'',''DLF_STR_PARAM_VALUES'')
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
			VALUE	    => 'IN (''DLF_MESSAGES'',''DLF_NUM_PARAM_VALUES'',''DLF_REQID_MAP'',''DLF_STR_PARAM_VALUES'')',
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
			 WHERE TABLE_NAME IN (''DLF_MESSAGES'',''DLF_NUM_PARAM_VALUES'',''DLF_REQID_MAP'',''DLF_STR_PARAM_VALUES'')
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


/** End-of-File **/