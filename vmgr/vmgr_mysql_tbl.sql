--
--  Copyright (C) 2001-2003 by CERN/IT/DS/HSM
--  All rights reserved
--
--       @(#)$RCSfile: vmgr_mysql_tbl.sql,v $ $Revision: 1.1 $ $Date: 2005/03/17 10:24:57 $ CERN IT-DS/HSM Jean-Philippe Baud
 
--     Create volume manager MySQL tables.

CREATE DATABASE vmgr_db;
USE vmgr_db;
CREATE TABLE vmgr_tape_info (
	rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	vid VARCHAR(6) BINARY,
	vsn VARCHAR(6) BINARY,
	library VARCHAR(8) BINARY,
	density VARCHAR(8) BINARY,
	lbltype VARCHAR(3) BINARY,
	model VARCHAR(6) BINARY,
	media_letter VARCHAR(1) BINARY,
	manufacturer VARCHAR(12) BINARY,
	sn VARCHAR(24) BINARY,
	nbsides INTEGER,
	etime INTEGER,
	rcount INTEGER,
	wcount INTEGER,
	rhost VARCHAR(10) BINARY,
	whost VARCHAR(10) BINARY,
	rjid INTEGER,
	wjid INTEGER,
	rtime INTEGER,
	wtime INTEGER)
	 TYPE = InnoDB;

CREATE TABLE vmgr_tape_side (
	rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	vid VARCHAR(6) BINARY,
	side INTEGER,
	poolname VARCHAR(15) BINARY,
	status SMALLINT,
	estimated_free_space INTEGER,
	nbfiles INTEGER)
	 TYPE = InnoDB;

CREATE TABLE vmgr_tape_library (
	rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(8) BINARY,
	capacity INTEGER,
	nb_free_slots INTEGER,
	status SMALLINT)
	 TYPE = InnoDB;

CREATE TABLE vmgr_tape_media (
	rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	m_model VARCHAR(6) BINARY,
	m_media_letter VARCHAR(1) BINARY,
	media_cost INTEGER)
	 TYPE = InnoDB;

CREATE TABLE vmgr_tape_denmap (
	rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	md_model VARCHAR(6) BINARY,
	md_media_letter VARCHAR(1) BINARY,
	md_density VARCHAR(8) BINARY,
	native_capacity INTEGER)
	 TYPE = InnoDB;

CREATE TABLE vmgr_tape_dgnmap (
	rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	dgn VARCHAR(6) BINARY,
	model VARCHAR(6) BINARY,
	library VARCHAR(8) BINARY)
	 TYPE = InnoDB;

CREATE TABLE vmgr_tape_pool (
	rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(15) BINARY,
	owner_uid INTEGER,
	gid INTEGER,
	capacity BIGINT UNSIGNED,
	tot_free_space BIGINT UNSIGNED)
	 TYPE = InnoDB;

CREATE TABLE vmgr_tape_tag (
	rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
	vid VARCHAR(6) BINARY,
	text VARCHAR(255) BINARY)
	 TYPE = InnoDB;

create table vmgr_tape_dgn (
        rowid BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        dgn VARCHAR(6) BINARY,
        weight int(11),
        deltaWeight int(11))
         TYPE = InnoDB;

alter table vmgr_tape_dgn
        add unique pk_dgn (dgn);

ALTER TABLE vmgr_tape_dgn 
	ADD UNIQUE pk_dgn (dgn);
ALTER TABLE vmgr_tape_info
        ADD UNIQUE pk_vid (vid);
ALTER TABLE vmgr_tape_side
        ADD FOREIGN KEY fk_vid (vid) REFERENCES vmgr_tape_info(vid),
        ADD UNIQUE pk_side (vid, side);
ALTER TABLE vmgr_tape_library
        ADD UNIQUE pk_library (name);
ALTER TABLE vmgr_tape_media
        ADD UNIQUE pk_model (m_model);
ALTER TABLE vmgr_tape_denmap
        ADD UNIQUE pk_denmap (md_model, md_media_letter, md_density);
ALTER TABLE vmgr_tape_dgnmap
        ADD UNIQUE pk_dgnmap (model, library);
ALTER TABLE vmgr_tape_pool
        ADD UNIQUE pk_poolname (name);
ALTER TABLE vmgr_tape_tag
        ADD FOREIGN KEY fk_t_vid(vid) REFERENCES vmgr_tape_info(vid),
        ADD UNIQUE pk_tag (vid);
