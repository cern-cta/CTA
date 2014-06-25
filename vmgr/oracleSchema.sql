/*****************************************************************************
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
 *
 * This script creates a new Castor Volume Manager schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

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
	owner_uid NUMBER,
	gid NUMBER(6),
	capacity NUMBER,
	tot_free_space NUMBER);

CREATE TABLE vmgr_tape_tag (
	vid VARCHAR2(6),
	text VARCHAR2(255));

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

-- Permissions should be granted to the CNS schema for VMGR_TAPE_SIDE and
-- VMGR_TAPE_STATUS_VIEW.
UNDEF cnsSchema
ACCEPT cnsSchema CHAR DEFAULT 'castor_ns' PROMPT 'Enter the name of the NS schema (default castor_ns): ';
GRANT SELECT ON vmgr_tape_side TO &cnsSchema;
GRANT SELECT ON vmgr_tape_status_view TO &cnsSchema;