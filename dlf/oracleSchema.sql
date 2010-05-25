/******************************************************************************
 *              oracleSchema.sql
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: oracleCreate.sql,v $ $Release: 1.2 $ $Release$ $Date: 2009/08/18 09:42:58 $ $Author: waldron $
 *
 * This script creates a new DLF schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* SQL statement for ids sequence */
CREATE SEQUENCE ids_seq INCREMENT BY 1 CACHE 300;

/* SQL statement for table dlf_config */
CREATE TABLE dlf_config(name VARCHAR2(255) CONSTRAINT NN_Config_Name NOT NULL, value VARCHAR2(255) CONSTRAINT NN_Config_Value NOT NULL, description VARCHAR2(255) CONSTRAINT NN_Config_Description NOT NULL);
ALTER TABLE dlf_config ADD CONSTRAINT UN_Config_Name UNIQUE (name) ENABLE;

/* SQL statements for table dlf_messages */
CREATE TABLE dlf_messages(id NUMBER, timestamp DATE CONSTRAINT NN_Messages_Timestamp NOT NULL, timeusec NUMBER, reqid CHAR(36), subreqid CHAR(36), hostid NUMBER, facility NUMBER(3), severity NUMBER(3), msg_no NUMBER(5), pid NUMBER(10), tid NUMBER(10), nshostid NUMBER, nsfileid NUMBER, tapevid VARCHAR2(20), userid NUMBER(10), groupid NUMBER(10), sec_type VARCHAR2(20), sec_name VARCHAR2(255))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_Messages_Timestamp ON dlf_messages (timestamp) LOCAL;
CREATE BITMAP INDEX I_Messages_Facility ON dlf_messages (facility) LOCAL;
CREATE BITMAP INDEX I_Messages_Severity ON dlf_messages (severity) LOCAL;
CREATE BITMAP INDEX I_Messages_MsgNo ON dlf_messages (msg_no) LOCAL;
CREATE INDEX I_Messages_Pid ON dlf_messages (pid) LOCAL;
CREATE INDEX I_Messages_Reqid ON dlf_messages (reqid) LOCAL;
CREATE INDEX I_Messages_Subreqid ON dlf_messages (subreqid) LOCAL;
CREATE INDEX I_Messages_Hostid ON dlf_messages (hostid) LOCAL;
CREATE INDEX I_Messages_NSHostid ON dlf_messages (nshostid) LOCAL;
CREATE INDEX I_Messages_NSFileid ON dlf_messages (nsfileid) LOCAL;
CREATE INDEX I_Messages_Tapevid ON dlf_messages (tapevid) LOCAL;
CREATE INDEX I_Messages_Userid ON dlf_messages (userid) LOCAL;
CREATE INDEX I_Messages_Groupid ON dlf_messages (groupid) LOCAL;
CREATE INDEX I_Messages_Sec_type ON dlf_messages (sec_type) LOCAL;
CREATE INDEX I_Messages_Sec_name ON dlf_messages (sec_name) LOCAL;

/* SQL statements for table dlf_num_param_values */
CREATE TABLE dlf_num_param_values(id NUMBER, timestamp DATE CONSTRAINT NN_Num_Param_Values_Timestamp NOT NULL, name VARCHAR2(20), value NUMBER)
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_Num_Param_Values_id ON dlf_num_param_values (id) LOCAL;

/* SQL statements for table dlf_str_param_values */
CREATE TABLE dlf_str_param_values(id NUMBER, timestamp DATE CONSTRAINT NN_Str_Param_Values_Timestamp NOT NULL, name VARCHAR2(20), value VARCHAR2(2048))
  PARTITION BY RANGE (timestamp) (PARTITION MAX_VALUE VALUES LESS THAN (MAXVALUE));

CREATE INDEX I_Str_Param_Values_id ON dlf_str_param_values (id) LOCAL;

/* SQL statements for table dlf_severities */
CREATE TABLE dlf_severities(sev_no NUMBER(3) CONSTRAINT PK_Severities_Sev_No PRIMARY KEY CONSTRAINT NN_Severities_Sev_No NOT NULL, sev_name VARCHAR2(20) CONSTRAINT NN_Severities_Sev_Name NOT NULL);

/* SQL statements for table dlf_facilities */
CREATE TABLE dlf_facilities(fac_no NUMBER(3) CONSTRAINT PK_Facilities_Fac_No PRIMARY KEY CONSTRAINT NN_Facilities_Fac_No NOT NULL, fac_name VARCHAR2(20) CONSTRAINT NN_Facilities_Fac_Name NOT NULL);

/* SQL statements for table dlf_msg_texts */
CREATE TABLE dlf_msg_texts(fac_no NUMBER(3) CONSTRAINT NN_Msg_Texts_Fac_No NOT NULL, msg_no NUMBER(5) CONSTRAINT NN_Msg_Texts_Msg_No NOT NULL, msg_text VARCHAR2(512) CONSTRAINT NN_Msg_Texts_Msg_Text NOT NULL);

ALTER TABLE dlf_msg_texts
  ADD CONSTRAINT PK_Msg_Texts_FacMsgNo PRIMARY KEY (fac_no, msg_no);

/* SQL statements for dlf_host_map */
CREATE TABLE dlf_host_map(hostid NUMBER CONSTRAINT PK_Host_Map_HostID PRIMARY KEY CONSTRAINT NN_Host_Map_HostID NOT NULL, hostname VARCHAR2(64) CONSTRAINT NN_Host_Map_HostName NOT NULL);

/* SQL statements for dlf_nshost_map */
CREATE TABLE dlf_nshost_map(nshostid NUMBER CONSTRAINT PK_NSHost_Map_NSHostID PRIMARY KEY CONSTRAINT NN_NSHost_Map_NSHostID NOT NULL, nshostname VARCHAR2(64) CONSTRAINT NN_NSHost_Map_NSHostName NOT NULL);

/* Fill the dlf_config table */
INSERT INTO dlf_config (name, value, description) VALUES ('instance', 'castordlf', 'The name of the castor2 instance');
INSERT INTO dlf_config (name, value, description) VALUES ('expiry', '90', 'The expiry time of the logging data in days');

/* Fill the dlf_severities table */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('1',  'Emerg');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('2',  'Alert');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('3',  'Error');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('4',  'Warn');
/* Previously Auth */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('5',  'Notice');
/* Previously Security */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('6',  'Notice');
/* Previously Usage */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('7',  'Debug');
/* Previously System */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('8',  'Info');
/* Previously Monitoring */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('10', 'Info');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('11', 'Debug');
/* Previously User Error */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('12', 'Notice');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES ('13', 'Crit');


/* Fill the dlf_facilities table */
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (0,  'rtcpclientd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (1,  'migrator');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (2,  'recaller');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (4,  'rhd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (8,  'gcd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (9,  'schedulerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (10, 'tperrhandler');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (11, 'vdqmd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (13, 'srmfed');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (14, 'srmbed');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (15, 'repackd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (17, 'taped');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (18, 'rtcpd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (19, 'rmmasterd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (20, 'rmnoded');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (21, 'jobmanagerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (22, 'stagerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (23, 'd2dtransfer');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (24, 'mighunterd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (25, 'rechandlerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (26, 'stagerjob');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (27, 'tapebridged');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (28, 'rmcd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (29, 'tapegatewayd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (30, 'operations');

