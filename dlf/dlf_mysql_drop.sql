--
--
-- Copyright (C) 2003 by CERN/IT/ADC/CA
-- All rights reserved
--
-- @(#)$RCSfile: dlf_mysql_drop.sql,v $ $Revision: 1.1 $ $Date: 2003/12/28 12:12:33 $ CERN IT-ADC Vitaly Motyakov
--
--     Delete logging facility MySQL tables.

USE dlf_db;

DROP TABLE dlf_num_param_values;
DROP TABLE dlf_str_param_values;
DROP TABLE dlf_rq_ids_map;
DROP TABLE dlf_messages;
DROP TABLE dlf_facilities;
DROP TABLE dlf_severities;
DROP TABLE dlf_msg_texts;
DROP TABLE dlf_tape_ids;
DROP TABLE dlf_host_map;
DROP TABLE dlf_ns_host_map;
