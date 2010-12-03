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
 * @(#)RCSfile: oracleCreate.sql,v  Release: 1.2  Release Date: 2009/08/18 09:40:13  Author: waldron 
 *
 * This script creates a new Castor Name Server schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

CREATE TABLE Cns_class_metadata (classid NUMBER(5), name VARCHAR2(15), owner_uid NUMBER(6), gid NUMBER(6), min_filesize NUMBER, max_filesize NUMBER, flags NUMBER(2), maxdrives NUMBER(3), max_segsize NUMBER, migr_time_interval NUMBER, mintime_beforemigr NUMBER, nbcopies NUMBER(1), retenp_on_disk NUMBER);

CREATE TABLE Cns_tp_pool (classid NUMBER(5), tape_pool VARCHAR2(15));

CREATE TABLE Cns_file_metadata (fileid NUMBER, parent_fileid NUMBER, guid CHAR(36), name VARCHAR2(255), filemode NUMBER(6), nlink NUMBER(6), owner_uid NUMBER(6), gid NUMBER(6), filesize NUMBER, atime NUMBER(10), mtime NUMBER(10), ctime NUMBER(10), fileclass NUMBER(5), status CHAR(1), csumtype VARCHAR2(2), csumvalue VARCHAR2(32), acl VARCHAR2(3900)) STORAGE (INITIAL 5M NEXT 5M PCTINCREASE 0);

CREATE TABLE Cns_user_metadata (u_fileid NUMBER, comments VARCHAR2(255));

CREATE TABLE Cns_seg_metadata (s_fileid NUMBER, copyno NUMBER(1),fsec NUMBER(3), segsize NUMBER, compression NUMBER, s_status CHAR(1), vid VARCHAR2(6), side NUMBER (1), fseq NUMBER(10), blockid RAW(4), checksum_name VARCHAR2(16), checksum NUMBER);

CREATE TABLE Cns_symlinks (fileid NUMBER, linkname VARCHAR2(1023));

CREATE TABLE Cns_groupinfo (gid NUMBER(10), groupname VARCHAR2(255));

CREATE TABLE Cns_userinfo (userid NUMBER(10), username VARCHAR2(255));

CREATE SEQUENCE Cns_unique_id START WITH 3 INCREMENT BY 1 CACHE 20;

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

ALTER TABLE Cns_groupinfo
  ADD CONSTRAINT map_groupname UNIQUE (groupname);

ALTER TABLE Cns_userinfo
  ADD CONSTRAINT map_username UNIQUE (username);

ALTER TABLE Cns_user_metadata
  ADD CONSTRAINT fk_u_fileid FOREIGN KEY (u_fileid)
  REFERENCES Cns_file_metadata (fileid);

ALTER TABLE Cns_file_metadata
  ADD CONSTRAINT fk_class FOREIGN KEY (fileclass)
  REFERENCES Cns_class_metadata (classid);

ALTER TABLE Cns_seg_metadata
  ADD CONSTRAINT fk_s_fileid FOREIGN KEY (s_fileid)
  REFERENCES Cns_file_metadata (fileid)
  ADD CONSTRAINT tapeseg UNIQUE (vid, side, fseq);

ALTER TABLE Cns_tp_pool
  ADD CONSTRAINT classpool UNIQUE (classid, tape_pool);

ALTER TABLE Cns_symlinks
  ADD CONSTRAINT fk_l_fileid FOREIGN KEY (fileid)
  REFERENCES Cns_file_metadata (fileid);

-- Constraint to prevent negative nlinks
ALTER TABLE Cns_file_metadata 
  ADD CONSTRAINT CK_FileMetadata_Nlink
  CHECK (nlink >= 0 AND nlink IS NOT NULL);

-- Constraint to prevent a NULL file class on non symlink entries
ALTER TABLE Cns_file_metadata
  ADD CONSTRAINT CK_FileMetadata_FileClass
  CHECK ((bitand(filemode, 40960) = 40960  AND fileclass IS NULL)
     OR  (bitand(filemode, 40960) != 40960 AND fileclass IS NOT NULL));

-- Create indexes on Cns_file_metadata
CREATE INDEX Parent_FileId_Idx ON Cns_file_metadata (parent_fileid);
CREATE INDEX I_file_metadata_fileclass ON Cns_file_metadata (fileclass);

-- Create indexes on Cns_seg_metadata
CREATE INDEX I_seg_metadata_tapesum ON Cns_seg_metadata (vid, s_fileid, segsize, compression);

-- Temporary table to support Cns_bulkexist calls
CREATE GLOBAL TEMPORARY TABLE Cns_files_exist_tmp
  (tmpFileId NUMBER) ON COMMIT DELETE ROWS;

/* SQL statements for table UpgradeLog */
CREATE TABLE UpgradeLog (Username VARCHAR2(64) DEFAULT sys_context('USERENV', 'OS_USER') CONSTRAINT NN_UpgradeLog_Username NOT NULL, SchemaName VARCHAR2(64) DEFAULT 'CNS' CONSTRAINT NN_UpgradeLog_SchemaName NOT NULL, Machine VARCHAR2(64) DEFAULT sys_context('USERENV', 'HOST') CONSTRAINT NN_UpgradeLog_Machine NOT NULL, Program VARCHAR2(48) DEFAULT sys_context('USERENV', 'MODULE') CONSTRAINT NN_UpgradeLog_Program NOT NULL, StartDate TIMESTAMP(6) WITH TIME ZONE DEFAULT sysdate, EndDate TIMESTAMP(6) WITH TIME ZONE, FailureCount NUMBER DEFAULT 0, Type VARCHAR2(20) DEFAULT 'NON TRANSPARENT', State VARCHAR2(20) DEFAULT 'INCOMPLETE', SchemaVersion VARCHAR2(20) CONSTRAINT NN_UpgradeLog_SchemaVersion NOT NULL, Release VARCHAR2(20) CONSTRAINT NN_UpgradeLog_Release NOT NULL);

/* SQL statements for check constraints on the UpgradeLog table */
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_State
  CHECK (state IN ('COMPLETE', 'INCOMPLETE'));
  
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_Type
  CHECK (type IN ('TRANSPARENT', 'NON TRANSPARENT'));

/* SQL statement to populate the intial release value */
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('-', '2_1_10_9001');

/* SQL statement to create the CastorVersion view */
CREATE OR REPLACE VIEW CastorVersion
AS
  SELECT decode(type, 'TRANSPARENT', schemaVersion,
           decode(state, 'INCOMPLETE', state, schemaVersion)) schemaVersion,
         decode(type, 'TRANSPARENT', release,
           decode(state, 'INCOMPLETE', state, release)) release,
         schemaName
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
 * @(#)RCSfile: oracleCreate.sql,v  Release: 1.2  Release Date: 2009/08/18 09:40:13  Author: waldron 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_9_3';

/* A function to extract the full path of a file in one go */
CREATE OR REPLACE FUNCTION getPathForFileid(fid IN NUMBER) RETURN VARCHAR2 IS
  CURSOR c IS
    SELECT /*+ NO_CONNECT_BY_COST_BASED */ name
      FROM cns_file_metadata
    START WITH fileid = fid
    CONNECT BY fileid = PRIOR parent_fileid
    ORDER BY level DESC;
  p VARCHAR2(2048) := '';
BEGIN
   FOR i in c LOOP
     p := p ||  '/' || i.name;
   END LOOP;
   -- remove first '/'
   p := replace(p, '///', '/');
   IF length(p) > 1024 THEN
     -- the caller will return SENAMETOOLONG
     raise_application_error(-20001, '');
   END IF;
   RETURN p;
END;
/

/* Flag the schema creation as COMPLETE */
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE';
COMMIT;
