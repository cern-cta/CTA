--
--
-- Copyright (C) 2003 by CERN/IT/ADC/CA
-- All rights reserved
--
-- @(#)$RCSfile: dlf_mysql_tbl.sql,v $ $Revision: 1.4 $ $Date: 2005/05/10 16:34:52 $ CERN IT-ADC Vitaly Motyakov
--
--     Create logging facility MySQL tables.

CREATE DATABASE dlf_db;
USE dlf_db;

CREATE TABLE dlf_host_map (
	host_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	host_name VARCHAR(64),
	KEY host_name_idx(host_name)
) TYPE = InnoDB;

CREATE TABLE dlf_ns_host_map (
	ns_host_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	ns_host_name VARCHAR(64),
	KEY ns_host_name_idx(ns_host_name)
) TYPE = InnoDB;

CREATE TABLE dlf_messages (
	msg_seq_no BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	time DATETIME,
	time_usec INT UNSIGNED,
	req_id CHAR(36) BINARY,
	host_id INT UNSIGNED,
	facility TINYINT UNSIGNED,
	severity TINYINT UNSIGNED,
	msg_no SMALLINT UNSIGNED,
	pid INT,
	thread_id INT,
	ns_host_id INT UNSIGNED,
	ns_file_id BIGINT UNSIGNED,
	INDEX host_id_idx(host_id),
	INDEX ns_host_id_idx(ns_host_id),
	KEY time_idx (time),
	KEY fac_idx (facility),
	KEY ns_inv_idx (ns_file_id),
	FOREIGN KEY (host_id) REFERENCES dlf_host_map(host_id),
	FOREIGN KEY (ns_host_id) REFERENCES dlf_ns_host_map(ns_host_id)
) TYPE = InnoDB;

CREATE TABLE dlf_num_param_values (
	msg_seq_no BIGINT UNSIGNED,
	par_name VARCHAR(20),
	value DECIMAL(32,9),
	INDEX msg_no_idx(msg_seq_no),
	FOREIGN KEY (msg_seq_no) REFERENCES dlf_messages(msg_seq_no)
) TYPE = InnoDB;

CREATE TABLE dlf_str_param_values (
	msg_seq_no BIGINT UNSIGNED,
	par_name VARCHAR(20),
	value VARCHAR(255),
	INDEX msg_no_idx(msg_seq_no),
	FOREIGN KEY (msg_seq_no) REFERENCES dlf_messages(msg_seq_no)
) TYPE = InnoDB;

CREATE TABLE dlf_rq_ids_map (
	msg_seq_no BIGINT UNSIGNED,
	req_id CHAR(36) BINARY,
	subreq_id CHAR(36) BINARY,
	INDEX msg_no_idx(msg_seq_no),
	KEY rq_id_idx(req_id),
	KEY subreq_id_idx(subreq_id),
	FOREIGN KEY (msg_seq_no) REFERENCES dlf_messages(msg_seq_no)
) TYPE = InnoDB;

CREATE TABLE dlf_facilities (
	fac_no TINYINT UNSIGNED,
	fac_name VARCHAR(20),
	UNIQUE (fac_no),
	UNIQUE (fac_name)
) TYPE = InnoDB;

CREATE TABLE dlf_severities (
	sev_no TINYINT UNSIGNED,
	sev_name VARCHAR(20),
	UNIQUE (sev_no)
) TYPE = InnoDB;

CREATE TABLE dlf_msg_texts (
	fac_no TINYINT UNSIGNED,
	msg_no SMALLINT UNSIGNED,
	msg_text VARCHAR(255) BINARY,
	UNIQUE (fac_no, msg_no),
	INDEX fac_idx(fac_no),
	KEY msg_no_idx(msg_no),
	FOREIGN KEY (fac_no) REFERENCES dlf_facilities(fac_no)
) TYPE = InnoDB;

CREATE TABLE dlf_tape_ids (
	msg_seq_no BIGINT UNSIGNED,
	tape_vid VARCHAR(20) BINARY,
	INDEX msg_no_idx(msg_seq_no),
	KEY tape_vid_idx(tape_vid),
	FOREIGN KEY (msg_seq_no) REFERENCES dlf_messages(msg_seq_no)
) TYPE = InnoDB;

INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('1', 'Emergency');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('2', 'Alert');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('3', 'Error');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('4', 'Warning');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('5', 'Auth');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('6', 'Security');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('7', 'Usage');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('8', 'System');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('9', 'Important');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('10', 'Debug');
