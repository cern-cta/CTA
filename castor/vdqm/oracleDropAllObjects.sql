set feedback OFF
set pagesize 0
set linesize 1000
column cmd format a1000
set trimspool on

spool oracleDropAllObjects.tmp
prompt -- Drop triggers
select unique
  'drop ' || object_type || ' ' || object_name || ';' as cmd
  from user_objects
  where object_type='TRIGGER';
prompt
prompt -- Drop packages
select unique
  'drop ' || object_type || ' ' || object_name || ';' as cmd
  from user_objects
  where object_type='PACKAGE';
prompt
prompt -- Drop views
select unique
  'drop view ' || view_name || ';' as cmd
  from user_views;
prompt
prompt -- Drop sequences
select unique
  'drop sequence ' || SEQUENCE_NAME || ';' as cmd
  from user_sequences;
prompt
prompt -- Drop foreign key constraints
select unique
  'alter table ' || table_name || ' drop constraint ' || constraint_name || ';'
    as cmd
  from user_constraints
  where constraint_type='R';
prompt
prompt -- Drop tables
select unique
  'drop table ' || table_name || ';' as cmd
  from user_tables;
prompt
prompt -- Drop synonyms
select unique
  'drop synonym ' || synonym_name || ';' as cmd
  from user_synonyms;
spool off

set feedback on

@oracleDropAllObjects.tmp
