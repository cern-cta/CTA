--
--
-- Copyright (C) 2003 by CERN/IT/ADC/CA
-- All rights reserved
--
-- @(#)$RCSfile: dlf_mysql_drop.sql,v $ $Revision: 1.2 $ $Date: 2005/09/07 07:57:17 $ CERN IT-ADC Vitaly Motyakov
--
--     Delete logging facility MySQL tables.

USE dlf_db;

DROP TABLE dlf_num_param_values;
DROP TABLE dlf_str_param_values;
DROP TABLE dlf_rq_ids_map;
DROP TABLE dlf_severities;
DROP TABLE dlf_msg_texts;
DROP TABLE dlf_facilities;
DROP TABLE dlf_tape_ids;
DROP TABLE dlf_messages;
DROP TABLE dlf_host_map;
DROP TABLE dlf_ns_host_map;

DROP DATABASE dlf_db;
