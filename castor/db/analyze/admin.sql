as oracle user:
$ sqlplus "/ as sysdba"

-- better output format in sqlplus
set pagesize 500
set linesize 500
column columnname format a20  -- alphanumeric 20 chars max

-- to create a new user:
select * from dba_tablespaces;
create user username identified by passwd default tablespace tsname quota 300M on tsname;

grant designer to username;

grant execute on dbms_lock to username;

-- to take a snapshot of the db for monitoring:
begin dbms_workload_repository.create_snapshot; end;

-- to get how much space tables use on disk:
select sum(bytes)/(1024*1024) "MB", segment_name from user_extents
where segment_type='TABLE' group by segment_name;

-- To see deadlocks and sessions
https://oraweb.cern.ch/pls/castor_dev/webinstance.sessions.show_sessions

-- Recalculate statistics
BEGIN
  DBMS_STATS.GATHER_TABLE_STATS (ownname=>'CASTOR_STAGER', tabname=>'...', estimate_percent=>100, 
	  method_opt=>'FOR ALL COLUMNS SIZE 1',no_invalidate=>FALSE,force=>TRUE);
END;
/

-- Show running sessions and their PIDs
select (select a.spid from v$process a where a.addr=b.paddr) SERVER_PID
        ,b.username || '*' || b.sid ORACLE_USER
         ,b.osuser || '@' || b.machine CLIENT_USER
         ,decode(b.process,NULL,'*',b.process) CLIENT_PID, b.client_Info
   from v$session b
  where b.type = 'USER' and
        b.username is not null and b.machine='<your client machine>' and
        b.status not in ('KILLED','SNIPED')
  order by SERVER_PID, ORACLE_USER;

-- To trace a session
SQL> oradebug setospid PID   -- where PID is the process id at the server level

SQL> oradebug trace name context forever, level 12;
SQL> oradebug trace name context off;
