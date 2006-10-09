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

-- too see deadlocks and sessions
https://oraweb.cern.ch/pls/castor_dev/webinstance.sessions.show_sessions

