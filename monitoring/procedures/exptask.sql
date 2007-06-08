BEGIN
dbms_scheduler.create_job(
  job_name => 'task_expsummary',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      proc_experimentsummary();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=WEEKLY',
  enabled => TRUE,
  comments => 'Information about experiments system usage');
END;
/

BEGIN
dbms_scheduler.create_job(
  job_name => 'task_TapeSummary',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      proc_TapeSummary();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=WEEKLY',
  enabled => TRUE,
  comments => 'Information about experiments tape usage');
END;
/

create table monitoring_TapeSummary (
	runtime DATE,
	EXPERIMENT varchar2(2048),
	NS_USAGE number,
	NBFILES NUMBER
);
