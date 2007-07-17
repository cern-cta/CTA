BEGIN
dbms_scheduler.create_job(
  job_name => 'task_tablesize',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_tablesize();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=DAILY',
  enabled => TRUE,
  comments => 'count of rows in various tables');
END;
/
BEGIN
dbms_scheduler.create_job(
  job_name => 'task_DiskServerStat',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_DiskServerStat();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=HOURLY',
  enabled => TRUE,
  comments => 'Information about files on diskservers');
END;
/
BEGIN
dbms_scheduler.create_job(
  job_name => 'task_Segment',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_Segment();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=HOURLY',
  enabled => TRUE,
  comments => 'Information about Segments');
END;
/
BEGIN
dbms_scheduler.create_job(
  job_name => 'task_Stream',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_STREAM();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=HOURLY',
  enabled => TRUE,
  comments => 'Information about Stream');
END;
/
BEGIN
dbms_scheduler.create_job(
  job_name => 'task_Subrequest',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_SubRequest();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=HOURLY',
  enabled => TRUE,
  comments => 'Information about SubRequests');
END;
/
BEGIN
dbms_scheduler.create_job(
  job_name => 'task_Tape',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_Tape();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=HOURLY',
  enabled => TRUE,
  comments => 'Information about Tape');
END;
/
BEGIN
dbms_scheduler.create_job(
  job_name => 'task_TapeCopy',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_TAPECOPY();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=HOURLY',
  enabled => TRUE,
  comments => 'Information about tape copies');
END;
/
BEGIN
dbms_scheduler.create_job(
  job_name => 'task_DiskCopy',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_DISKCOPY();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=HOURLY',
  enabled => TRUE,
  comments => 'Information about disk copies');
END;
/
BEGIN
dbms_scheduler.create_job(
  job_name => 'task_FilesData',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_FILEMONITORING();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=HOURLY',
  enabled => TRUE,
  comments => 'Information about files inc access count, age and size');
END;
/
BEGIN
dbms_scheduler.create_job(
  job_name => 'task_MetaRecall',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_MetaRecallRunning();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=MINUTELY;INTERVAL=15',
  enabled => TRUE,
  comments => 'Information about active recalls and their lifetimes');
END;
/

BEGIN
dbms_scheduler.create_job(
  job_name => 'task_MigPending',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_MetaMigPending();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=MINUTELY;INTERVAL=15',
  enabled => TRUE,
  comments => 'Create min, max, avg age of pending files and count for each svcclass');
END;
/

BEGIN
dbms_scheduler.create_job(
  job_name => 'task_MigSelected',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
      castor_stager.Proc_MetaMigSelected();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=MINUTELY;INTERVAL=15',
  enabled => TRUE,
  comments => 'Create min, max, avg age of selected files and count for each svcclass');
END;
/
                                                                                                                             
BEGIN
dbms_scheduler.create_job(
  job_name => 'task_VeryOldFiles',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
        castor_stager.Proc_OLDFILES();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=DAILY',
  enabled => TRUE,
  comments => 'Produce list of files in stager in diskcopy state 10, tapecopy state 0 or 1 older than specified time');
END;
/

BEGIN
dbms_scheduler.create_job(
  job_name => 'task_waittaperecall',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
        castor_stager.Proc_waittaperecall();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=DAILY',
  enabled => TRUE,
  comments => 'Produce list of files in stager in diskcopy state 2 older than specified time');
END;
/

BEGIN
dbms_scheduler.create_job(
  job_name => 'task_waitdisk2diskcopy',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
        castor_stager.Proc_waitdisk2diskcopy();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=DAILY',
  enabled => TRUE,
  comments => 'Produce list of files in stager in diskcopy state 1 older than specified time');
END;
/

BEGIN
dbms_scheduler.create_job(
  job_name => 'task_STAGEOUT',
  job_type => 'PLSQL_BLOCK',
  job_action => 'BEGIN
        castor_stager.Proc_STAGEOUT();
  END;',
  start_date => sysdate,
  repeat_interval => 'FREQ=DAILY',
  enabled => TRUE,
  comments => 'Produce list of files in stager in diskcopy state 6 -STAGEOUT- older than 10 days');
END;
/
