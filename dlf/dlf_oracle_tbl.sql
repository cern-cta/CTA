--
-- Copyright (C) 2003 by CERN/IT/ADC/CA
-- All rights reserved
--
-- @(#)$RCSfile: dlf_oracle_tbl.sql,v $ $Revision: 1.2 $ $Date: 2003/12/08 06:28:10 $ CERN IT-ADC Vitaly Motyakov
--
--     Create logging facility ORACLE tables.

CREATE SEQUENCE message_seq;
CREATE SEQUENCE host_seq;

CREATE TABLE dlf_host_map (
	host_id NUMBER,
	host_name VARCHAR2(64),
	UNIQUE(host_id),
	UNIQUE(host_name));

CREATE TABLE dlf_messages (
	msg_seq_no NUMBER,
	time DATE,
	time_usec NUMBER,
	req_id CHAR (32),
	host_id NUMBER,
	facility NUMBER(3),
	severity NUMBER(3),
	msg_no NUMBER(5),
	pid NUMBER(10),
	thread_id NUMBER(10),
	ns_invariant NUMBER);


CREATE TABLE dlf_num_param_values (
	msg_seq_no NUMBER,
	par_name VARCHAR2(20),
	value NUMBER);

CREATE TABLE dlf_str_param_values (
	msg_seq_no NUMBER,
	par_name VARCHAR2(20),
	value VARCHAR2(255));

CREATE TABLE dlf_rq_ids_map (
	msg_seq_no NUMBER,
	req_id CHAR(32),
	subreq_id CHAR(32));

CREATE TABLE dlf_facilities (
	fac_no NUMBER(3),
	fac_name VARCHAR2(20),
	UNIQUE (fac_no),
	UNIQUE (fac_name));

CREATE TABLE dlf_severities (
	sev_no NUMBER(3),
	sev_name VARCHAR2(20),
	UNIQUE (sev_no));

CREATE TABLE dlf_msg_texts (
	fac_no NUMBER(3),
	msg_no NUMBER(5),
	msg_text VARCHAR2(255),
	UNIQUE (fac_no, msg_no));

CREATE TABLE dlf_tape_ids (
	msg_seq_no NUMBER,
	tape_vid VARCHAR2(20));

ALTER TABLE dlf_messages
         ADD CONSTRAINT pk_msg_no PRIMARY KEY (msg_seq_no)
         ADD CONSTRAINT fk_hid FOREIGN KEY (host_id) REFERENCES dlf_host_map(host_id);
ALTER TABLE dlf_num_param_values 
         ADD CONSTRAINT fk_msg_np FOREIGN KEY (msg_seq_no) REFERENCES dlf_messages(msg_seq_no);
ALTER TABLE dlf_str_param_values 
         ADD CONSTRAINT fk_msg_sp FOREIGN KEY (msg_seq_no) REFERENCES dlf_messages(msg_seq_no);
ALTER TABLE dlf_rq_ids_map 
         ADD CONSTRAINT fk_msg_im FOREIGN KEY (msg_seq_no) REFERENCES dlf_messages(msg_seq_no);
ALTER TABLE dlf_tape_ids 
         ADD CONSTRAINT fk_msg_tp FOREIGN KEY (msg_seq_no) REFERENCES dlf_messages(msg_seq_no);
ALTER TABLE dlf_msg_texts
         ADD CONSTRAINT fk_fac_no FOREIGN KEY (fac_no) REFERENCES dlf_facilities(fac_no);

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





























