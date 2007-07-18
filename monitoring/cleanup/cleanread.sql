BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_migstall'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_recallstall'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_tablesize'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_Segment'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_Stream'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_Subrequest'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_Tape'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_TapeCopy'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_DiskCopy'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_MetaRecall'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_waittaperecall'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_waitdisk2diskcopy'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_STAGEOUT'
  );
END;
/
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_VeryOldFiles'
  );
END;
/    
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_FilesData'
  );
END;
/    
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_MigPending'
  );
END;
/    
BEGIN
dbms_scheduler.drop_job(
  job_name => 'task_MigSelected'
  );
END;
/    
