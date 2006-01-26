--
--  Copyright (C) 1999-2005 by CERN/IT/PDP/DM
--  All rights reserved
--
--       @(#)$RCSfile: oracleCreate.sql,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:19 $ CERN IT-PDP/DM Jean-Philippe Baud
 
--     Create name server Oracle tables.

CREATE TABLE Cns_class_metadata (
       classid NUMBER(5),
       name VARCHAR2(15),
       owner_uid NUMBER(6),
       gid NUMBER(6),
       min_filesize NUMBER,
       max_filesize NUMBER,
       flags NUMBER(2),
       maxdrives NUMBER(3),
       max_segsize NUMBER,
       migr_time_interval NUMBER,
       mintime_beforemigr NUMBER,
       nbcopies NUMBER(1),
       nbdirs_using_class NUMBER,
       retenp_on_disk NUMBER);

CREATE TABLE Cns_tp_pool (
       classid NUMBER(5),
       tape_pool VARCHAR2(15));

CREATE TABLE Cns_file_metadata (
       fileid NUMBER,
       parent_fileid NUMBER,
       guid CHAR(36),
       name VARCHAR2(231),
       filemode NUMBER(6),
       nlink NUMBER(6),
       owner_uid NUMBER(6),
       gid NUMBER(6),
       filesize NUMBER,
       atime NUMBER(10),
       mtime NUMBER(10),
       ctime NUMBER(10),
       fileclass NUMBER(5),
       status CHAR(1),
       csumtype VARCHAR2(2),
       csumvalue VARCHAR2(32),
       acl VARCHAR2(3900))
	STORAGE (INITIAL 5M NEXT 5M PCTINCREASE 0);

CREATE TABLE Cns_user_metadata (
       u_fileid NUMBER,
       comments VARCHAR2(255));

CREATE TABLE Cns_seg_metadata (
       s_fileid NUMBER,
       copyno NUMBER(1),
       fsec NUMBER(3),
       segsize NUMBER,
       compression NUMBER,
       s_status CHAR(1),
       vid VARCHAR2(6),
       side NUMBER (1),
       fseq NUMBER(10),
       blockid RAW(4),
       checksum_name VARCHAR2(16),
       checksum NUMBER);


CREATE TABLE Cns_symlinks (
       fileid NUMBER,
       linkname VARCHAR2(1023));

CREATE TABLE Cns_file_replica (
       fileid NUMBER,
       nbaccesses NUMBER,
       atime NUMBER(10),
       ptime NUMBER(10),
       status CHAR(1),
       f_type CHAR(1),
       poolname VARCHAR2(15),
       host VARCHAR2(63),
       fs VARCHAR2(79),
       sfn VARCHAR2(1103));

CREATE TABLE Cns_groupinfo (
       gid NUMBER(10),
       groupname VARCHAR2(255));

CREATE TABLE Cns_userinfo (
       userid NUMBER(10),
       username VARCHAR2(255));

CREATE SEQUENCE Cns_gid_seq START WITH 101 INCREMENT BY 1;
CREATE SEQUENCE Cns_uid_seq START WITH 101 INCREMENT BY 1;
CREATE SEQUENCE Cns_unique_id START WITH 3 INCREMENT BY 1;

ALTER TABLE Cns_class_metadata
       ADD CONSTRAINT pk_classid PRIMARY KEY (classid)
       ADD CONSTRAINT classname UNIQUE (name);
ALTER TABLE Cns_file_metadata
       ADD CONSTRAINT pk_fileid PRIMARY KEY (fileid)
       ADD CONSTRAINT file_full_id UNIQUE (parent_fileid, name)
       ADD CONSTRAINT file_guid UNIQUE (guid)
	USING INDEX STORAGE (INITIAL 2M NEXT 2M PCTINCREASE 0);
ALTER TABLE Cns_user_metadata
       ADD CONSTRAINT pk_u_fileid PRIMARY KEY (u_fileid);
ALTER TABLE Cns_seg_metadata
       ADD CONSTRAINT pk_s_fileid PRIMARY KEY (s_fileid, copyno, fsec);
ALTER TABLE Cns_symlinks
       ADD CONSTRAINT pk_l_fileid PRIMARY KEY (fileid);
ALTER TABLE Cns_file_replica
       ADD CONSTRAINT repl_sfn UNIQUE (sfn);
ALTER TABLE Cns_groupinfo
       ADD CONSTRAINT map_groupname UNIQUE (groupname);
ALTER TABLE Cns_userinfo
       ADD CONSTRAINT map_username UNIQUE (username);
CREATE INDEX replica_id ON Cns_file_replica(fileid);

ALTER TABLE Cns_user_metadata
       ADD CONSTRAINT fk_u_fileid FOREIGN KEY (u_fileid) REFERENCES Cns_file_metadata(fileid);
ALTER TABLE Cns_seg_metadata
       ADD CONSTRAINT fk_s_fileid FOREIGN KEY (s_fileid) REFERENCES Cns_file_metadata(fileid)
       ADD CONSTRAINT tapeseg UNIQUE (vid, side, fseq);
ALTER TABLE Cns_tp_pool
       ADD CONSTRAINT classpool UNIQUE (classid, tape_pool);
ALTER TABLE Cns_symlinks
       ADD CONSTRAINT fk_l_fileid FOREIGN KEY (fileid) REFERENCES Cns_file_metadata(fileid);
ALTER TABLE Cns_file_replica
       ADD CONSTRAINT fk_r_fileid FOREIGN KEY (fileid) REFERENCES Cns_file_metadata(fileid);


-- Create an index on Cns_file_metadata(PARENT_FILEID)

CREATE INDEX PARENT_FILEID_IDX on Cns_file_metadata(PARENT_FILEID);

-- Create the "schema_version" table

DROP TABLE schema_version;
CREATE TABLE schema_version (major NUMBER(1), minor NUMBER(1), patch NUMBER(1));
INSERT INTO schema_version VALUES (1, 1, 1);

