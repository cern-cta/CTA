/*                        ORACLE ENTERPRISE EDITION                         */
/*                                                                          */
/* This file contains SQL code that will destroy the dlf database schema    */
/* DBA privileges must be present for removing the scheduled job            */

/* tables */
DROP TABLE dlf_num_param_values;
DROP TABLE dlf_str_param_values;
DROP TABLE dlf_reqid_map;
DROP TABLE dlf_tape_ids;
DROP TABLE dlf_facilities;
DROP TABLE dlf_severities;
DROP TABLE dlf_msg_texts;
DROP TABLE dlf_messages;
DROP TABLE dlf_host_map;
DROP TABLE dlf_nshost_map;
DROP TABLE dlf_sequences;
DROP TABLE dlf_monitoring;
DROP TABLE dlf_jobstats;
DROP TABLE dlf_reqstats;

/* procedures */
DROP PROCEDURE dlf_partition;
DROP PROCEDURE dlf_archive;

/* scheduler */
exec DBMS_SCHEDULER.DROP_JOB(JOB_NAME => 'dlf_partition');
exec DBMS_SCHEDULER.DROP_JOB(JOB_NAME => 'dlf_archive');


/* End-of-File */




























