--
--  Copyright (C) 2001-2005 by CERN/IT/DS/HSM
--  All rights reserved
--
--       @(#)$RCSfile: Cns_mysql_tbl.sql,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:19 $ CERN IT-DS/HSM Jean-Philippe Baud
 
--     Create name server MySQL tables.

CREATE DATABASE cns_db;
USE cns_db;
CREATE TABLE Cns_class_metadata (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       classid INTEGER,
       name VARCHAR(15) BINARY,
       owner_uid INTEGER,
       gid INTEGER,
       min_filesize INTEGER,
       max_filesize INTEGER,
       flags INTEGER,
       maxdrives INTEGER,
       max_segsize INTEGER,
       migr_time_interval INTEGER,
       mintime_beforemigr INTEGER,
       nbcopies INTEGER,
       nbdirs_using_class INTEGER,
       retenp_on_disk INTEGER)
	TYPE = InnoDB;

CREATE TABLE Cns_tp_pool (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       classid INTEGER,
       tape_pool VARCHAR(15) BINARY)
	TYPE = InnoDB;

CREATE TABLE Cns_file_metadata (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       fileid BIGINT UNSIGNED,
       parent_fileid BIGINT UNSIGNED,
       guid CHAR(36) BINARY,
       name VARCHAR(231) BINARY,
       filemode INTEGER UNSIGNED,
       nlink INTEGER,
       owner_uid INTEGER UNSIGNED,
       gid INTEGER UNSIGNED,
       filesize BIGINT UNSIGNED,
       atime INTEGER,
       mtime INTEGER,
       ctime INTEGER,
       fileclass SMALLINT,
       status CHAR(1) BINARY,
       csumtype VARCHAR(2) BINARY,
       csumvalue VARCHAR(32) BINARY,
       acl BLOB)
	TYPE = InnoDB;

CREATE TABLE Cns_user_metadata (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       u_fileid BIGINT UNSIGNED,
       comments VARCHAR(255) BINARY)
	TYPE = InnoDB;

CREATE TABLE Cns_seg_metadata (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       s_fileid BIGINT UNSIGNED,
       copyno INTEGER,
       fsec INTEGER,
       segsize BIGINT UNSIGNED,
       compression INTEGER,
       s_status CHAR(1) BINARY,
       vid VARCHAR(6) BINARY,
       side INTEGER,
       fseq INTEGER,
       blockid CHAR(8),
       checksum_name VARCHAR(16),
       checksum INTEGER)
	TYPE = InnoDB;

CREATE TABLE Cns_symlinks (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       fileid BIGINT UNSIGNED,
       linkname BLOB)
	TYPE = InnoDB;

CREATE TABLE Cns_file_replica (
       rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       fileid BIGINT UNSIGNED,
       nbaccesses BIGINT UNSIGNED,
       atime INTEGER,
       ptime INTEGER,
       status CHAR(1) BINARY,
       f_type CHAR(1) BINARY,
       poolname VARCHAR(15) BINARY,
       host VARCHAR(63) BINARY,
       fs VARCHAR(79) BINARY,
       sfn BLOB)
	TYPE = InnoDB;

CREATE TABLE Cns_groupinfo (
       rowid INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       gid INTEGER,
       groupname VARCHAR(255) BINARY)
	TYPE = InnoDB;

CREATE TABLE Cns_userinfo (
       rowid INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
       userid INTEGER,
       username VARCHAR(255) BINARY)
	TYPE = InnoDB;

CREATE TABLE Cns_unique_id (
       id BIGINT UNSIGNED)
	TYPE = InnoDB;

CREATE TABLE Cns_unique_gid (
       id INTEGER UNSIGNED)
	TYPE = InnoDB;

CREATE TABLE Cns_unique_uid (
       id INTEGER UNSIGNED)
	TYPE = InnoDB;

ALTER TABLE Cns_class_metadata
       ADD UNIQUE (classid),
       ADD UNIQUE classname (name);
ALTER TABLE Cns_file_metadata
       ADD UNIQUE (fileid),
       ADD UNIQUE file_full_id (parent_fileid, name),
       ADD UNIQUE (guid);
ALTER TABLE Cns_user_metadata
       ADD UNIQUE (u_fileid);
ALTER TABLE Cns_seg_metadata
       ADD UNIQUE (s_fileid, copyno, fsec);

ALTER TABLE Cns_symlinks
       ADD UNIQUE (fileid);
ALTER TABLE Cns_file_replica
       ADD INDEX (fileid),
       ADD UNIQUE (sfn(255));
ALTER TABLE Cns_groupinfo
       ADD UNIQUE (groupname);
ALTER TABLE Cns_userinfo
       ADD UNIQUE (username);

ALTER TABLE Cns_user_metadata
       ADD FOREIGN KEY fk_u_fileid (u_fileid) REFERENCES Cns_file_metadata(fileid);
ALTER TABLE Cns_seg_metadata
       ADD FOREIGN KEY fk_s_fileid (s_fileid) REFERENCES Cns_file_metadata(fileid),
       ADD UNIQUE tapeseg (vid, side, fseq);
ALTER TABLE Cns_tp_pool
       ADD UNIQUE classpool (classid, tape_pool);
ALTER TABLE Cns_symlinks
       ADD FOREIGN KEY fk_l_fileid (fileid) REFERENCES Cns_file_metadata(fileid);
ALTER TABLE Cns_file_replica
       ADD FOREIGN KEY fk_r_fileid (fileid) REFERENCES Cns_file_metadata(fileid);

-- Create an index on Cns_file_metadata(PARENT_FILEID)

CREATE INDEX PARENT_FILEID_IDX ON Cns_file_metadata (PARENT_FILEID);

-- Create the "schema_version" table

DROP TABLE IF EXISTS schema_version;
CREATE TABLE schema_version (
  major_number INTEGER NOT NULL,
  minor_number INTEGER NOT NULL,
  patch_number INTEGER NOT NULL
) TYPE=INNODB;

INSERT INTO schema_version (major_number, minor_number, patch_number) 
  VALUES (1, 1, 1);
