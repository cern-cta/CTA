--
--  Copyright (C) 1999-2003 by CERN/IT/PDP/DM
--  All rights reserved
--
--       @(#)$RCSfile: vmgr_oracle_tbl.sql,v $ $Revision: 1.1 $ $Date: 2005/03/17 10:24:58 $ CERN IT-PDP/DM Jean-Philippe Baud
 
--     Create volume manager Oracle tables.

CREATE TABLE vmgr_tape_info (
	vid VARCHAR2(6),
	vsn VARCHAR2(6),
	library VARCHAR2(8),
	density VARCHAR2(8),
	lbltype VARCHAR2(3),
	model VARCHAR2(6),
	media_letter VARCHAR2(1),
	manufacturer VARCHAR2(12),
	sn VARCHAR2(24),
	nbsides NUMBER(1),
	etime NUMBER(10),
	rcount NUMBER,
	wcount NUMBER,
	rhost VARCHAR2(10),
	whost VARCHAR2(10),
	rjid NUMBER(10),
	wjid NUMBER(10),
	rtime NUMBER(10),
	wtime NUMBER(10));

CREATE TABLE vmgr_tape_side (
	vid VARCHAR2(6),
	side NUMBER(1),
	poolname VARCHAR2(15),
	status NUMBER(2),
	estimated_free_space NUMBER,
	nbfiles NUMBER);

CREATE TABLE vmgr_tape_library (
	name VARCHAR2(8),
	capacity NUMBER,
	nb_free_slots NUMBER,
	status NUMBER(2));

CREATE TABLE vmgr_tape_media (
	m_model VARCHAR2(6),
	m_media_letter VARCHAR2(1),
	media_cost NUMBER(3));

CREATE TABLE vmgr_tape_denmap (
	md_model VARCHAR2(6),
	md_media_letter VARCHAR2(1),
	md_density VARCHAR2(8),
	native_capacity NUMBER);

CREATE TABLE vmgr_tape_dgnmap (
	dgn VARCHAR2(6),
	model VARCHAR2(6),
	library VARCHAR2(8));

CREATE TABLE vmgr_tape_pool (
	name VARCHAR2(15),
	owner_uid NUMBER(6),
	gid NUMBER(6),
	capacity NUMBER,
	tot_free_space NUMBER);

CREATE TABLE vmgr_tape_tag (
	vid VARCHAR2(6),
	text VARCHAR2(255));

                                                      
CREATE TABLE vmgr_tape_dgn (
    dgn VARCHAR2(6),
    weight NUMBER(10),
    deltaWeight NUMBER(10));
 

ALTER TABLE vmgr_tape_info
        ADD CONSTRAINT pk_vid PRIMARY KEY (vid);
ALTER TABLE vmgr_tape_side
        ADD CONSTRAINT fk_vid FOREIGN KEY (vid) REFERENCES vmgr_tape_info(vid)
        ADD CONSTRAINT pk_side PRIMARY KEY (vid, side);
ALTER TABLE vmgr_tape_library
        ADD CONSTRAINT pk_library PRIMARY KEY (name);
ALTER TABLE vmgr_tape_media
        ADD CONSTRAINT pk_model PRIMARY KEY (m_model);
ALTER TABLE vmgr_tape_denmap
        ADD CONSTRAINT pk_denmap PRIMARY KEY (md_model, md_media_letter, md_density);
ALTER TABLE vmgr_tape_dgnmap
        ADD CONSTRAINT pk_dgnmap PRIMARY KEY (model, library);
ALTER TABLE vmgr_tape_pool
        ADD CONSTRAINT pk_poolname PRIMARY KEY (name);
ALTER TABLE vmgr_tape_tag
        ADD CONSTRAINT fk_t_vid FOREIGN KEY (vid) REFERENCES vmgr_tape_info(vid)
        ADD CONSTRAINT pk_tag PRIMARY KEY (vid);
ALTER TABLE vmgr_tape_dgn
        ADD CONSTRAINT pk_dgn PRIMARY KEY (dgn);

