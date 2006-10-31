/*                        ORACLE ENTERPRISE EDITION                         */
/*                                                                          */
/* This file contains SQL code that will grant the user castordlf_read      */
/* select privileges on all dlf tables int the database schema.             */
/* Note: the user must be created prior to using this script                */

GRANT SELECT ON castor_dlf.dlf_num_param_values TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_str_param_values TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_reqid_map        TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_tape_ids         TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_facilities       TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_severities       TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_msg_texts        TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_messages         TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_host_map         TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_nshost_map       TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_sequences        TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_monitoring       TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_jobstats         TO castordlf_read;
GRANT SELECT ON castor_dlf.dlf_reqstats         TO castordlf_read;



/** End-of-File **/
