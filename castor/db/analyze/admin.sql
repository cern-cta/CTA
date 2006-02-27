as oracle user:
$ sqlplus "/ as sysdba"

-- to create a new user:
select * from dba_tablespaces;

create user username identified by passwd default tablespace tsname quota 300M on tsname;

grant designer to username;

grant execute on dbms_lock to username;

-- to take a snapshot of the db for monitoring:
begin dbms_workload_repository.create_snapshot; end;



