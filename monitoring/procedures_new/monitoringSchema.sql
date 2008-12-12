/******************************************************************************
 *              monitoringSchema.sqlplus
 *
 * This file is part of the Castor Monitoring project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2008  CERN
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
 * @(#)RCSfile: monitoringSchema.sql,v  Release: 1.0  Release Date: 2008/04/11   Author: rekatsinas 
 *
 * This script create a new monitoring schema
 *
 * @author Theodoros Rekatsinas, 
 *****************************************************************************/
/* Create Patritioned Tables*/
DECLARE
	ts_var varchar2(15);
	temp varchar2(100);
	sql_txt varchar2(2000);
	CreationDate varchar2(15);
BEGIN
	--Create First Partition 
	--Select current date to create first partition
	--    
	SELECT 'P_'||TO_CHAR(SYSDATE, 'YYYYMMDD') into CreationDate FROM DUAL;
	SELECT to_char(trunc(sysdate +1),'DD-MON-YYYY') INTO ts_var FROM dual;
	select to_char(sysdate,'DD-MON-RR HH24.MI.SS') into temp from dual;
	-- ConfigSchema Table 
	--Contains: 
	--->maxtimestamp for each of the next tables: Requests,DiskHit,DiskCopy,TapeRecall,TotalLat
	--  (We use these timestamps to configure next 5 minutes timewindow)
	--->expiry period: Delete Data Older than Expiry Period
	-- 
	sql_txt := 'CREATE TABLE ConfigSchema (expiry NUMBER, reqsmaxtime DATE,dhmaxtime DATE,dcmaxtime DATE, trmaxtime DATE, gcmaxtime DATE, totallatmaxtime DATE,migsmaxtime DATE, idcmaxtime DATE,ermaxtime DATE)';
	execute immediate sql_txt;
	commit;
	--ConfigSchema Initialization
	--
	sql_txt := 'INSERT INTO ConfigSchema (expiry, reqsmaxtime,dhmaxtime,dcmaxtime,trmaxtime,gcmaxtime,totallatmaxtime,migsmaxtime,idcmaxtime,ermaxtime) VALUES (60, '''||to_date(temp,'DD-MON-RR HH24.MI.SS')||''' , '''||to_date(temp,'DD-MON-RR HH24.MI.SS')||''' , '''||to_date(temp,'DD-MON-RR HH24.MI.SS')||''' , '''||to_date(temp,'DD-MON-RR HH24.MI.SS')||''' , '''||to_date(temp,'DD-MON-RR HH24.MI.SS')||''','''||to_date(temp,'DD-MON-RR HH24.MI.SS')||''','''||to_date(temp,'DD-MON-RR HH24.MI.SS')||''','''||to_date(temp,'DD-MON-RR HH24.MI.SS')||''','''||to_date(temp,'DD-MON-RR HH24.MI.SS')||''')';
	execute immediate sql_txt;
	commit;
	--Requests Table
	--Info about File Requests
	-- 
	sql_txt := 'CREATE TABLE Requests (subreqid CHAR(36) NOT NULL PRIMARY KEY, timestamp DATE NOT NULL, reqid CHAR(36) NOT NULL,  nsfileid NUMBER NOT NULL, type VARCHAR2(255), svcclass VARCHAR2(255), username VARCHAR2(255), state VARCHAR2(255),filename VARCHAR2(2048), filesize NUMBER) 
	PARTITION BY RANGE (timestamp) (PARTITION ' ||CreationDate || ' VALUES LESS THAN ( to_date('''||ts_var||''',''DD-MON-YYYY'')))';
	execute immediate sql_txt;
	commit;
	--Internal DiskCopies Table
	sql_txt := 'CREATE TABLE internalDiskCopy (timestamp DATE NOT NULL,svcclass VARCHAR2(255), copies NUMBER) 
	PARTITION BY RANGE (timestamp) (PARTITION ' ||CreationDate || ' VALUES LESS THAN ( to_date('''||ts_var||''',''DD-MON-YYYY'')))';
	execute immediate sql_txt;
	commit;
	--Errors Table
	sql_txt := 'CREATE TABLE Errors (timestamp DATE NOT NULL,reqid CHAR(36) NOT NULL,subreqid CHAR(36) NOT NULL,facility NUMBER, msg_no NUMBER) 
	PARTITION BY RANGE (timestamp) (PARTITION ' ||CreationDate || ' VALUES LESS THAN ( to_date('''||ts_var||''',''DD-MON-YYYY'')))';
	execute immediate sql_txt;
	commit;
	--TotalLatency
	sql_txt := 'CREATE TABLE TotalLatency (subreqid CHAR(36) NOT NULL PRIMERY KEY,timestamp DATE NOT NULL,nsfileid NUMBER NOT NULL, totallatency NUMBER) 
	PARTITION BY RANGE (timestamp) (PARTITION ' ||CreationDate || ' VALUES LESS THAN ( to_date('''||ts_var||''',''DD-MON-YYYY'')))';
	execute immediate sql_txt;
	commit;
	--TapeRecall Table
	--Specific Info about TapeRecalls' Subcategory
	--
	sql_txt := 'CREATE TABLE TapeRecall( subreqid CHAR(36) NOT NULL PRIMARY KEY,timestamp DATE NOT NULL, TapeId VARCHAR2(255 BYTE), TapeMountState VARCHAR2(255 BYTE),ReadLatency INTEGER,CopyLatency INTEGER,CONSTRAINT fk_column_trecall FOREIGN KEY (subreqid) REFERENCES requests (subreqid) ON DELETE CASCADE)
	PARTITION BY RANGE (timestamp) (PARTITION ' ||CreationDate || ' VALUES LESS THAN ( to_date('''||ts_var||''',''DD-MON-YYYY'')))';
	execute immediate sql_txt;
	commit;
	--DiskCopy Table
	--Specific Info about DiskCopy's Subcategory
	--
	sql_txt := 'CREATE TABLE DiskCopy(subreqid CHAR(36) NOT NULL PRIMARY KEY,timestamp DATE NOT NULL, OriginalPool VARCHAR2(255), TargetPool VARCHAR2(255),dest_host Number, src_host VARCHAR2(255), ReadLatency INTEGER,CopyLatency INTEGER, NumCopiesInPools INTEGER,CONSTRAINT fk_column_dcopy FOREIGN KEY (subreqid) REFERENCES requests (subreqid) ON DELETE CASCADE)
	PARTITION BY RANGE (timestamp) (PARTITION ' ||CreationDate || ' VALUES LESS THAN ( to_date('''||ts_var||''',''DD-MON-YYYY'')))';
	execute immediate sql_txt;
	commit;
	--DiskHits Table
	--Specific Info about DiskHits' Subcategory
	--
	sql_txt := 'CREATE TABLE DiskHits(timestamp DATE NOT NULL,subreqid CHAR(36) NOT NULL PRIMARY KEY, FileAge INTEGER, NumAccesses INTEGER, NumCopies INTEGER,CONSTRAINT fk_column_dhit FOREIGN KEY (subreqid) REFERENCES Requests (subreqid) ON DELETE CASCADE)
	PARTITION BY RANGE (timestamp) (PARTITION ' ||CreationDate || ' VALUES LESS THAN ( to_date('''||ts_var||''',''DD-MON-YYYY'')))';
	execute immediate sql_txt;
	commit;
	--GCFiles Table
	--Info about Garbage Collection
	--
	sql_txt := 'CREATE TABLE GCFiles(timestamp DATE NOT NULL,nsfileid NUMBER NOT NULL,FileSize NUMBER, FileAge NUMBER)
	PARTITION BY RANGE (timestamp) (PARTITION ' ||CreationDate || ' VALUES LESS THAN ( to_date('''||ts_var||''',''DD-MON-YYYY'')))';
	execute immediate sql_txt;
	commit;
	--Xrootd Table
	--Info about Xroot Protocol
	-- 
	sql_txt := 'CREATE TABLE Xrootd(timestamp DATE NOT NULL,message_type INTEGER, message_string VARCHAR2(100),message_int INTEGER, servername VARCHAR2(100))
	PARTITION BY RANGE (timestamp) (PARTITION ' ||CreationDate || ' VALUES LESS THAN ( to_date('''||ts_var||''',''DD-MON-YYYY'')))';
	execute immediate sql_txt;
	commit;
	--Migration Table
	--Info about File Migration
	--
	sql_txt := 'CREATE TABLE Migration (subreqid CHAR(36) NOT NULL PRIMARY KEY, timestamp DATE NOT NULL, reqid CHAR(36) NOT NULL,  nsfileid NUMBER NOT NULL, type VARCHAR2(255), 	svcclass VARCHAR2(255), username VARCHAR2(255), state VARCHAR2(255),filename VARCHAR2(2048), totallatency NUMBER, filesize NUMBER) 
	PARTITION BY RANGE (timestamp) (PARTITION ' ||CreationDate || ' VALUES LESS THAN ( to_date('''||ts_var||''',''DD-MON-YYYY'')))';
	execute immediate sql_txt;
	commit;
END;
/
--Table Indexes
CREATE INDEX i_req_timestamp ON Requests (timestamp) LOCAL;
CREATE INDEX i_req_reqid ON Requests (reqid);
CREATE INDEX i_mig_timestamp ON Migration (timestamp) LOCAL;
CREATE INDEX i_mig_reqid ON Migration (reqid);
CREATE INDEX i_dhit_timestamp ON DiskHits (timestamp) LOCAL;
CREATE INDEX i_dcopy_timestamp ON  DiskCopy (timestamp) LOCAL;
CREATE INDEX i_trecall_timestamp ON  TapeRecall (timestamp) LOCAL;
CREATE INDEX i_gcfiles_timestamp ON GCFiles (timestamp) LOCAL;
CREATE INDEX i_xrootd_timestamp ON Xrootd (timestamp) LOCAL;
CREATE INDEX i_idc_timestamp ON internalDiskCopy(timestamp) LOCAL;
CREATE INDEX i_errors_timestamp ON Errors(timestamp) LOCAL;
CREATE INDEX i_errors_facility ON Errors(facility);
CREATE INdex i_errors_msg_no ON Errors(msg_no);
CREATE INDEX i_tlat_timestamp ON TotalLatency(timestamp) LOCAL;
CREATE INDEX i_tlat_totallatency ON TotalLatency(totallatency);

--Materialized View - Files Requested After Deletion
CREATE MATERIALIZED VIEW req_del	
REFRESH FORCE ON DEMAND  START WITH sysdate NEXT sysdate + 10/1440
AS (select a.timestamp, round((a.timestamp - b.timestamp)*24,5) dif
	from requests a , gcfiles b
	where a.nsfileid = b.nsfileid
	and a.state = 'TapeRecall'
	and a.timestamp > b.timestamp
	and a.timestamp - b.timestamp <= 1);
CREATE INDEX i_req_del ON req_del(dif);

--Materialized Views Used by Dashboard Feature
-- GC_monitor_view
CREATE MATERIALIZED VIEW gc_monitor 
REFRESH COMPLETE ON DEMAND START WITH sysdate NEXT sysdate + 2/1440
AS ( select a.tot total, round(a.avgage/3600,2) avg_age, round((a.avgage - b.avgage)/b.avgage,4) age_per, 	round(a.avgsize/102400,4) avg_size, round((a.avgsize - b.avgsize)/b.avgsize,4) size_per
from (	select count(*) tot, avg(fileage) avgage, avg(filesize) avgsize
      	from gcfiles
	where timestamp > sysdate - 10/1440
	and timestamp <= sysdate - 5/1440) a,
     (	select avg(fileage) avgage, avg(filesize) avgsize
	from gcfiles
	where timestamp > sysdate - 15/1440
	and timestamp <= sysdate - 10/1140) b);
-- Main_Table_Counters
CREATE MATERIALIZED VIEW main_table_counters 
REFRESH COMPLETE ON DEMAND START WITH sysdate NEXT sysdate + 2/1140
AS ( 
select a.svcclass , a.state state , a.num num , round((a.num - b.num)/b.num,2) per
from (	select svcclass , state , count(*) num
  	from requests 
  	where timestamp > sysdate - 10/1440 
              and timestamp <= sysdate -5/1440 
  	group by svcclass,state) a ,
     (	select svcclass , state , count(*) num
	from requests 
	where timestamp > sysdate - 15/1440
	      and timestamp <= sysdate - 10/1440 
	group by svcclass,state) b
where a.svcclass = b.svcclass
and a.state = b.state
order by a.svcclass) ;
-- Mig_Monitor
CREATE MATERIALIZED VIEW mig_monitor
REFRESH COMPLETE ON DEMAND START WITH sysdate NEXT sysdate + 2/1140
AS ( 
select svcclass svcclass , count(*) migs 
from migration
where timestamp > sysdate - 10/1440 
      and timestamp <= sysdate - 5/1440
group by svcclass);


--Error Log Tables		
--We use these tables to avoid loops into populating procedures
--
EXECUTE DBMS_ERRLOG.CREATE_ERROR_LOG ('Requests','Err_Requests');
EXECUTE DBMS_ERRLOG.CREATE_ERROR_LOG ('Migration','Err_Migration');
EXECUTE DBMS_ERRLOG.CREATE_ERROR_LOG ('DiskHits','Err_Diskhits');
EXECUTE DBMS_ERRLOG.CREATE_ERROR_LOG ('DiskCopy','Err_DiskCopy');
EXECUTE DBMS_ERRLOG.CREATE_ERROR_LOG ('TapeRecall','Err_Taperecall');
EXECUTE DBMS_ERRLOG.CREATE_ERROR_LOG('Errors','Err_Errors');
EXECUTE DBMS_ERRLOG.CREATE_ERROR_LOG('totallatency','Err_Latency');
	
-- newPartitions Procedure
-- Create New Partition Every Day
--	
	CREATE OR REPLACE PROCEDURE newPartitions
	AS
	partitionMax 	VARCHAR2(15);
	newpartition 	VARCHAR2(30);
	ts_var	     	VARCHAR2(15);
	sql_txt 	VARCHAR2(200);	

	BEGIN
		FOR a IN (SELECT DISTINCT(table_name)
		FROM user_tab_partitions
		ORDER BY table_name)
	LOOP
		SELECT max(substr(partition_name, 3, 10))
		INTO partitionMax
		FROM user_tab_partitions
		WHERE table_name = a.table_name;
		--select max date as value of partitionMax 
		newpartition := 'P_'||TO_CHAR(TRUNC(TO_DATE(partitionMax, 'YYYYMMDD') + 1), 'YYYYMMDD');
		partitionMax := TO_CHAR(TRUNC(TO_DATE(partitionMax, 'YYYY-MM-DD') + 2), 'YYYY-MM-DD');
		sql_txt := 'ALTER TABLE '||a.table_name||' ADD PARTITION '||newpartition||' VALUES LESS THAN ( to_date('''||partitionMax||''',''YYYY-MM-DD'')) UPDATE INDEXES';
		EXECUTE IMMEDIATE sql_txt;
		COMMIT;
	END LOOP;
END;
/
--errorProc Procedure --Populates Errors Table
create or replace procedure errorProc
AS
maxtimestamp date;
Begin
	--configure current timewindow
	select ermaxtime into maxtimestamp
	from ConfigSchema;
	--Extract and Insert new Data
	--Info about errors per_facility
	INSERT INTO Errors
	(timestamp,reqid,subreqid,facility,msg_no)
	select timestamp,reqid,subreqid,facility,msg_no
	from castor_dlf.dlf_messages
	where severity = 3
	and timestamp >= maxtimestamp
	and timestamp < maxtimestamp + 5/1440
	LOG ERRORS INTO Err_Errors REJECT LIMIT UNLIMITED;
	UPDATE ConfigSchema
	SET ermaxtime = ermaxtime + 5/1440;
	COMMIT;
end;
/
-- diskCopyProc Procedure -- Populates DiskCopy Table
create or replace procedure diskCopyProc
AS
maxtimestamp date;
new_timestamp date;

BEGIN
	--Configure current Timewindow
	SELECT dcmaxtime INTO maxtimestamp
	FROM ConfigSchema;
	--Extract and Insert new Data
	--Info about external file replication: source - target SvcClass (source and target servers)
	INSERT INTO DiskCopy
	(subreqid, timestamp, OriginalPool,TargetPool,dest_host,src_host)
	select distinct reqs.subreqid ,reqs.timestamp, temp.src,temp.dest,temp.dest_host,temp.src_host
	from requests reqs, (
	SELECT a.nsfileid nsfileid,
  max(case when a.msg_no = 39 and b.name = 'Direction' then substr(b.value, 0, instr(b.value, '->', 1) - 2) else null end) src, 
  max(case when a.msg_no = 39 and b.name = 'Direction' then substr(b.value, instr(b.value, '->', 1) + 3) else null end) dest ,
  max(case when a.msg_no = 28 and b.name = 'SourcePath' then a.hostid else null end) dest_host, 
  max(case when a.msg_no = 28 and b.name = 'SourcePath' then substr(b.value, 0, instr(b.value, '.', 1) - 1) else null end) src_host
  FROM castor_dlf.dlf_messages a ,castor_dlf.dlf_str_param_values b
	WHERE a.id = b.id
    AND a.facility = 23
    AND a.msg_no in (39,28)
    AND a.timestamp >= maxtimestamp
    and b.timestamp >= maxtimestamp
    and a.timestamp < maxtimestamp + 5/1440
    and b.timestamp < maxtimestamp + 5/1440
  group by nsfileid
	) temp
	where reqs.nsfileid = temp.nsfileid
	and reqs.timestamp >= maxtimestamp
	and reqs.timestamp < maxtimestamp + 5/1440
	and temp.src <> temp.dest
	and temp.dest = reqs.svcclass
	and reqs.state = 'DiskCopy'
order by reqs.timestamp
	--Log errors in Err_DiskCopy Table-prevent procedure from failing
	--with this table we avoid using a cursor (direct insert instead)
	--
	LOG ERRORS INTO Err_DiskCopy REJECT LIMIT UNLIMITED;
	COMMIT;
	--Update maximum timestamp in ConfigSchema Table
	select max(timestamp) into new_timestamp from DiskCopy WHERE timestamp > maxtimestamp;
	if ((new_timestamp is NULL)or(new_timestamp - maxtimestamp < 5/1440)) then
		begin
			UPDATE ConfigSchema
			SET dcmaxtime = dcmaxtime + 5/1440;
			COMMIT;
		end;
	else
		begin
			UPDATE ConfigSchema
			SET dcmaxtime = new_timestamp;
			COMMIT;
		end;
	end if;
END;
/

create or replace procedure internaldiskCopyProc
AS
maxtimestamp date;
BEGIN
	--Configure current Timewindow
	SELECT idcmaxtime INTO maxtimestamp FROM ConfigSchema;
	--Extract and Insert new Data
	--Info about external file replication: source - target SvcClass
	INSERT all
	INTO InternalDiskCopy
	(timestamp,svcclass,copies)
	values (maxtimestamp,src,copies)
	select src,count(*) copies 
	from (
	SELECT a.nsfileid nsfileid, substr(b.value, 0, instr(b.value, '->', 1) - 2) src, substr(b.value, instr(b.value, '->', 1) + 3) dest
	FROM castor_dlf.dlf_messages a ,castor_dlf.dlf_str_param_values b
	WHERE a.id = b.id
		    AND a.facility = 23
		    AND a.msg_no = 39
		    AND a.timestamp >= maxtimestamp
		    AND b.timestamp >= maxtimestamp 
		    AND a.timestamp < maxtimestamp + 5/1440
		    AND b.timestamp < maxtimestamp + 5/1440
	) temp
	where  temp.src = temp.dest
	group by src;
	--Update maximum timestamp in ConfigSchema Table
	UPDATE ConfigSchema
	SET idcmaxtime = idcmaxtime + 5/1440;
	COMMIT;
	EXCEPTION when others then NULL;
END;
/

-- Taperecall Procedure - Populates Taperecall Table
create or replace procedure tapeRecallProc AS
maxtimestamp 	DATE;
new_timestamp	DATE;
subid 		VARCHAR2(36);
fsize		VARCHAR2(2048);
BEGIN
	--Configure current Timewindow
	SELECT trmaxtime INTO maxtimestamp
	FROM ConfigSchema;
	--Extract and Insert new Data
	--Info about tape recalls: Tape Volume Id - Tape Status
	INSERT INTO TAPERECALL
	(timestamp,subreqid,TapeId,TapeMountState)
	(select mes.timestamp, mes.subreqid, mes.tapevid, str.value 
	from castor_dlf.dlf_messages mes, castor_dlf.dlf_str_param_values str
	where mes.id = str.id and 
	mes.facility = 22
	and mes.msg_no = 57
	and str.name ='TapeStatus'
	and mes.timestamp >= maxtimestamp
	and str.timestamp >= maxtimestamp
	and mes.timestamp < maxtimestamp + 5/1440
	and str.timestamp < maxtimestamp + 5/1440)
	LOG ERRORS INTO Err_Taperecall REJECT LIMIT UNLIMITED;
	COMMIT;
	--Insert info about size of recalled file
	DECLARE 
	CURSOR C1 IS (select mes.subreqid, num.value 
			from castor_dlf.dlf_messages mes, castor_dlf.dlf_num_param_values num
			where mes.id = num.id and 
			mes.facility = 22
			and mes.msg_no = 57
			and num.name ='FileSize'
			and mes.timestamp >= maxtimestamp
			and num.timestamp >= maxtimestamp
			and mes.timestamp < maxtimestamp + 5/1440
			and num.timestamp < maxtimestamp + 5/1440);
	BEGIN
		OPEN C1;
		LOOP
		FETCH C1 into subid,fsize;
		EXIT WHEN C1%NOTFOUND;
		UPDATE Requests r
		SET filesize = to_number(fsize)
		where subreqid = subid;
		COMMIT;
		END LOOP;
	END;
	---Update maximum timestamp in ConfigSchema Table
	select max(timestamp) into new_timestamp from TapeRecall WHERE timestamp > maxtimestamp;
  if ((new_timestamp is NULL)or(new_timestamp - maxtimestamp < 5/1440)) then 
		begin
			UPDATE ConfigSchema
			SET trmaxtime = trmaxtime + 5/1440;
			COMMIT;
		end;
	else 
		begin
			UPDATE ConfigSchema
			SET trmaxtime = new_timestamp;
			COMMIT;
		end; 
	end if;
	
END;
/
-- TotalLat Procedure - Completes Total Latency Field in Requests and Migration Tables
create or replace PROCEDURE TotalLat AS
maxtimestamp 	DATE;
subid		VARCHAR2(36);
tlatency	VARCHAR2(2048);
BEGIN
	--Configure Current Timewindow
	SELECT totallatmaxtime INTO maxtimestamp
	FROM ConfigSchema;
	--Extract Total Latency Info from job started summary message
	insert into totallatency
	(subreqid,timestamp,nsfileid,totallatency)
	select distinct a.subreqid,a.timestamp,a.nsfileid,b.value
	from castor_dlf.dlf_messages a, castor_dlf.dlf_num_param_values b
	where a.id=b.id
	      and a.facility = 5
	      and a.msg_no = 12
	      and a.timestamp >= maxtimestamp
	      and b.timestamp >= maxtimestamp
	      and a.timestamp < maxtimestamp + 5/1440
	      and b.timestamp < maxtimestamp + 5/1440
	LOG ERRORS INTO Err_Latency REJECT LIMIT UNLIMITED;
	commit;
	--Update Maximum total latency timestamp in ConfigSchema Table
	UPDATE ConfigSchema
	SET totallatmaxtime = totallatmaxtime + 5/1440;
	COMMIT;
END;
/
--procedure that clears old db data(deletes old partitions) and keeps last 'ConfigSchema.expiry' days of data
CREATE OR REPLACE PROCEDURE cleanOldData AS
maxdate 	NUMBER;
delPartition 	VARCHAR2(15);
delPart 	date;
sql_txt 	VARCHAR2(200); 
partition_does_not_exist EXCEPTION;
PRAGMA EXCEPTION_INIT (partition_does_not_exist, -2149);
BEGIN
	SELECT expiry INTO maxdate FROM ConfigSchema; 
	FOR a IN (
		SELECT DISTINCT(table_name) 
		FROM user_tab_partitions
		ORDER BY table_name
	)
	LOOP 
		BEGIN
		SELECT max(substr(partition_name, 3, 10))
		INTO delPartition
		FROM user_tab_partitions
		WHERE table_name = a.table_name;
		delPart := to_date(delPartition,'YYYYMMDD') - round(maxdate) - 1;
		delPartition := 'P_'||to_char(delPart,'YYYYMMDD');
		sql_txt := 'ALTER TABLE '||a.table_name||'  DROP PARTITION ' ||delPartition;
		EXECUTE IMMEDIATE sql_txt;
		COMMIT;
		EXCEPTION 
			when partition_does_not_exist then null;
		END;
	END LOOP;
END;
/
create or replace procedure GCFilesProc
AS
maxtimestamp 	DATE;
new_timestamp	DATE;
FileSize	NUMBER;
FileAge 	NUMBER;
NumberOfReqs 	NUMBER;

BEGIN
	/*Configure Current Timewindow*/
	SELECT gcmaxtime INTO maxtimestamp 
	FROM ConfigSchema;
	INSERT INTO GCFiles
	(timestamp,nsfileid, FileSize, FileAge)
	select a.timestamp,b.nsfileid, max(decode(a.name, 'FileSize', to_number(a.value), NULL)) FileSize,
		max(decode(A.name, 'FileAge', to_number(a.value), NULL)) FileAge
	from castor_dlf.dlf_num_param_values a, castor_dlf.dlf_messages b 
	where b.facility=8 and b.msg_no=11 and a.id=b.id  and a.name in ('FileSize','FileAge')
	AND a.timestamp >= maxtimestamp
	and b.timestamp >= maxtimestamp
	and a.timestamp < maxtimestamp  + 5/1440 
	and b.timestamp < maxtimestamp  + 5/1440 
	group by a.timestamp,b.nsfileid;
	COMMIT;
	--Update maximum gc timestamp in ConfigSchema Table
	SELECT max(timestamp) into new_timestamp FROM GCFiles WHERE timestamp > maxtimestamp;
	if ((new_timestamp is NULL)or(new_timestamp - maxtimestamp < 5/1440)) then
		begin
			UPDATE ConfigSchema
			SET gcmaxtime = gcmaxtime + 5/1440;
			COMMIT;
		end;
	else
		begin
			UPDATE ConfigSchema
			SET gcmaxtime = new_timestamp;
			COMMIT;
		end;
	end if;
END;

-- (select to_number(value) NbAccesses from castor_dlf.dlf_num_param_values a, castor_dlf.dlf_messages b where b.facility=8 and b.msg_no=11 and a.id=b.id  and a.name='NbAccesses' and
--	AND a.timestamp > now - 10/1440
--        and b.timestamp > now - 10/1440
--	and a.timestamp <= now - 5/1440
--	and b.timestamp <= now - 5/1440)
/

--Reqs Procedure - Extraxt Info for file Requests 
--Global Requests Info 
--
create or replace PROCEDURE reqs AS
maxtimestamp DATE;
new_maxtime  date;
--define unique constraint exception
--Use exception handling to avoid proc failure
--Expected Unique Constraint Violation because of repeated message in DLF DB
--
con_violation EXCEPTION;
PRAGMA EXCEPTION_INIT (con_violation, -0001);
BEGIN
	SELECT reqsmaxtime INTO maxtimestamp 
	FROM ConfigSchema;
	INSERT INTO Requests 
	(timestamp, reqid,subreqid,nsfileid,type,svcclass,username,state,filename)
	SELECT mes.timestamp,mes.reqid, mes.subreqid, mes.nsfileid,
		max(decode(str.name, 'Type',  str.value, NULL)) Type,
		max(decode(str.name, 'SvcClass',  str.value, NULL)) SvcClass,
		max(decode(str.name, 'Username',  str.value, NULL)) Username,
		max(decode(mes.msg_no, 60 ,'DiskHit',56,'DiskCopy',57,'TapeRecall',NULL)) state,
		max(decode(str.name, 'Filename', str.value, NULL)) Filename
	FROM castor_dlf.dlf_messages mes , castor_dlf.dlf_str_param_values str
	WHERE  mes.id = str.id 
		and str.id in
			(SELECT id 
			FROM  castor_dlf.dlf_str_param_values 
			WHERE  name = 'Type' and value like 'Stage%'
			and timestamp >= maxtimestamp
			and timestamp < maxtimestamp + 5/1440)
	and mes.facility = 22 
	and mes.msg_no in (56,57,60)
	and str.name in ('SvcClass','Username','Groupname','Type','Filename')
	and mes.timestamp >= maxtimestamp
	and mes.timestamp < maxtimestamp + 5/1440
	and str.timestamp >= maxtimestamp
	and str.timestamp < maxtimestamp + 5/1440
	GROUP BY mes.timestamp, mes.id ,mes.reqid , mes.subreqid, mes.nsfileid
	ORDER BY mes.timestamp
	LOG ERRORS INTO Err_Requests REJECT LIMIT UNLIMITED;
	COMMIT;
  select max(timestamp) into new_maxtime from requests;
  if ((new_maxtime is NULL)or(new_maxtime - maxtimestamp < 5/1440)) then
		begin
			UPDATE ConfigSchema
			SET reqsmaxtime = reqsmaxtime + 5/1440;
			COMMIT;
		end;
	else
		begin
			UPDATE ConfigSchema
			SET reqsmaxtime = new_maxtime;
			COMMIT;
		end;
	end if;
END;
/
--Migs Procedure - Info about File Migration
create or replace PROCEDURE migs AS
maxtimestamp DATE;
new_maxtime DATE;
con_violation EXCEPTION;
PRAGMA EXCEPTION_INIT (con_violation, -0001);
BEGIN
	SELECT migsmaxtime INTO maxtimestamp 
	FROM ConfigSchema;
	INSERT INTO Migration 
	(timestamp, reqid,subreqid,nsfileid,type,svcclass,username,filename)
	SELECT mes.timestamp,mes.reqid, mes.subreqid, mes.nsfileid,
		max(decode(str.name, 'Type',  str.value, NULL)) Type,
		max(decode(str.name, 'SvcClass',  str.value, NULL)) SvcClass,
		max(decode(str.name, 'Username',  str.value, NULL)) Username,
		max(decode(str.name, 'Filename', str.value, NULL)) Filename
	FROM castor_dlf.dlf_messages mes , castor_dlf.dlf_str_param_values str
	WHERE  mes.id = str.id 
		and str.id in
			(SELECT id 
			FROM  castor_dlf.dlf_str_param_values 
			WHERE  name = 'Type' and value like 'Stage%'
			and timestamp >= maxtimestamp
			and timestamp < maxtimestamp + 5/1440)
	and mes.facility = 22 
	and mes.msg_no = 58
	and str.name in ('SvcClass','Username','Groupname','Type','Filename')
	and mes.timestamp >= maxtimestamp
	and mes.timestamp < maxtimestamp + 5/1440
	and str.timestamp >= maxtimestamp
	and str.timestamp < maxtimestamp + 5/1440
	GROUP BY mes.timestamp, mes.id ,mes.reqid , mes.subreqid, mes.nsfileid
	ORDER BY mes.timestamp
	LOG ERRORS INTO Err_Migration REJECT LIMIT UNLIMITED;
	COMMIT;
  select max(timestamp) into new_maxtime from migration;
  if ((new_maxtime is NULL)or(new_maxtime - maxtimestamp < 5/1440)) then
		begin
			UPDATE ConfigSchema
			SET migsmaxtime = migsmaxtime + 5/1440;
			COMMIT;
		end;
	else
		begin
			UPDATE ConfigSchema
			SET migsmaxtime = new_maxtime;
			COMMIT;
		end;
	end if;
END;
/

--removes all former jobs
BEGIN
	FOR a IN (SELECT job_name FROM user_scheduler_jobs)
	LOOP
		DBMS_SCHEDULER.DROP_JOB(a.job_name, TRUE);
	END LOOP;
END;
/
--Creates an extra Partition
BEGIN
newPartitions;
END;
/
--Add new Jobs 
BEGIN
DBMS_SCHEDULER.CREATE_JOB (
JOB_NAME        => 'cleanJob',
JOB_TYPE        => 'PLSQL_BLOCK',
JOB_ACTION      => 'declare
                    sql_txt VARCHAR2(50);
                    BEGIN
                    sql_txt := ''truncate table err_requests'';               
                    execute immediate sql_txt;
		    sql_txt := ''truncate table err_diskcopy'';               
                    execute immediate sql_txt;
		    sql_txt := ''truncate table err_errors'';               
                    execute immediate sql_txt;
		    sql_txt := ''truncate table err_taperecall'';               
                    execute immediate sql_txt;
		    sql_txt := ''truncate table err_migration'';               
                    execute immediate sql_txt;
		    sql_txt := ''truncate table err_latency'';               
                    execute immediate sql_txt;
			newPartitions;
			cleanOldData;
		    END;',
START_DATE      => TRUNC(SYSDATE) + 1,
REPEAT_INTERVAL => 'FREQ=DAILY',
ENABLED         => TRUE,
COMMENTS        => 'daily data deletion');

DBMS_SCHEDULER.CREATE_JOB (
JOB_NAME        => 'populateJob',
JOB_TYPE        => 'PLSQL_BLOCK',
JOB_ACTION      => 'declare
                    sql_txt VARCHAR2(50);
                    BEGIN
                    sql_txt := ''truncate table err_requests'';               
                    execute immediate sql_txt;
			reqs;
			diskcopyproc;
			internaldiskcopyproc;
			taperecallproc;
			migs;
			errorproc;
			TotalLat;
			GCFilesProc;
		    END;',
START_DATE      => SYSDATE + 5/1440,
REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
ENABLED         => TRUE,
COMMENTS        => 'CASTOR2 Monitoring(5 Minute Frequency)');

END;
/























