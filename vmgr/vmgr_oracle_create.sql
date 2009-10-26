/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE;

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
 * @(#)RCSfile: oracleCreate.sql,v  Release: 1.2  Release Date: 2008/11/06 13:18:27  Author: waldron 
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
	owner_uid NUMBER(6),
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

/* SQL statements for table UpgradeLog */
CREATE TABLE UpgradeLog (Username VARCHAR2(64) DEFAULT sys_context('USERENV', 'OS_USER') CONSTRAINT NN_UpgradeLog_Username NOT NULL, Machine VARCHAR2(64) DEFAULT sys_context('USERENV', 'HOST') CONSTRAINT NN_UpgradeLog_Machine NOT NULL, Program VARCHAR2(48) DEFAULT sys_context('USERENV', 'MODULE') CONSTRAINT NN_UpgradeLog_Program NOT NULL, StartDate TIMESTAMP(6) WITH TIME ZONE DEFAULT sysdate, EndDate TIMESTAMP(6) WITH TIME ZONE, FailureCount NUMBER DEFAULT 0, Type VARCHAR2(20) DEFAULT 'NON TRANSPARENT', State VARCHAR2(20) DEFAULT 'INCOMPLETE', SchemaVersion VARCHAR2(20) CONSTRAINT NN_UpgradeLog_SchemaVersion NOT NULL, Release VARCHAR2(20) CONSTRAINT NN_UpgradeLog_Release NOT NULL);

/* SQL statements for check constraints on the UpgradeLog table */
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_State
  CHECK (state IN ('COMPLETE', 'INCOMPLETE'));
  
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_Type
  CHECK (type IN ('TRANSPARENT', 'NON TRANSPARENT'));

/* SQL statement to populate the intial release value */
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('-', '2_1_9_3');

/* SQL statement to create the CastorVersion view */
CREATE OR REPLACE VIEW CastorVersion
AS
  SELECT decode(type, 'TRANSPARENT', schemaVersion,
           decode(state, 'INCOMPLETE', state, schemaVersion)) schemaVersion,
         decode(type, 'TRANSPARENT', release,
           decode(state, 'INCOMPLETE', state, release)) release
    FROM UpgradeLog
   WHERE startDate =
     (SELECT max(startDate) FROM UpgradeLog);

/*****************************************************************************
 *              oracleTrailer.sql
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
 * @(#)RCSfile: oracleCreate.sql,v  Release: 1.2  Release Date: 2008/11/06 13:18:27  Author: waldron 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_9_3';

/* Flag the schema creation as COMPLETE */
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE';
COMMIT;
