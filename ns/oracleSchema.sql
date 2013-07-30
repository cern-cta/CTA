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
 * This script creates a new Castor Name Server schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

CREATE TABLE Cns_class_metadata (classid NUMBER(5), name VARCHAR2(15), owner_uid NUMBER(6), gid NUMBER(6), min_filesize NUMBER, max_filesize NUMBER, flags NUMBER(2), maxdrives NUMBER(3), max_segsize NUMBER, migr_time_interval NUMBER, mintime_beforemigr NUMBER, nbcopies NUMBER(1), retenp_on_disk NUMBER);

CREATE TABLE Cns_tp_pool (classid NUMBER(5), tape_pool VARCHAR2(15));

CREATE TABLE Cns_file_metadata (fileid NUMBER, parent_fileid NUMBER, guid CHAR(36), name VARCHAR2(255), filemode NUMBER(6), nlink NUMBER, owner_uid NUMBER(6), gid NUMBER(6), filesize NUMBER, atime NUMBER(10), mtime NUMBER(10), ctime NUMBER(10), stagertime NUMBER DEFAULT 0 NOT NULL, fileclass NUMBER(5), status CHAR(1), csumtype VARCHAR2(2), csumvalue VARCHAR2(32), acl VARCHAR2(3900)) STORAGE (INITIAL 5M NEXT 5M PCTINCREASE 0);

CREATE TABLE Cns_user_metadata (u_fileid NUMBER, comments VARCHAR2(255));

CREATE TABLE Cns_seg_metadata (s_fileid NUMBER, copyno NUMBER(1),fsec NUMBER(3), segsize NUMBER, compression NUMBER, s_status CHAR(1), vid VARCHAR2(6), side NUMBER (1), fseq NUMBER(10), blockid RAW(4), checksum_name VARCHAR2(16), checksum NUMBER, gid NUMBER(6), creationTime NUMBER, lastModificationTime NUMBER);

CREATE TABLE Cns_symlinks (fileid NUMBER, linkname VARCHAR2(1023));

-- Table for NS statistics
CREATE TABLE UsageStats (
  gid NUMBER(6) DEFAULT 0 CONSTRAINT NN_UsageStats_gid NOT NULL,
  timestamp NUMBER  DEFAULT 0 CONSTRAINT NN_UsageStats_ts NOT NULL,
  maxFileId INTEGER, fileCount INTEGER, fileSize INTEGER,
  segCount INTEGER, segSize INTEGER, segCompressedSize INTEGER,
  seg2Count INTEGER, seg2Size INTEGER, seg2CompressedSize INTEGER);

ALTER TABLE UsageStats ADD CONSTRAINT PK_UsageStats_gid_ts PRIMARY KEY (gid, timestamp);

CREATE SEQUENCE Cns_unique_id START WITH 3 INCREMENT BY 1 CACHE 1000;

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
CREATE INDEX I_file_metadata_gid ON Cns_file_metadata (gid);

-- Create indexes on Cns_seg_metadata
CREATE INDEX I_seg_metadata_tapesum ON Cns_seg_metadata (vid, s_fileid, segsize, compression);
CREATE INDEX I_seg_metadata_gid ON Cns_Seg_metadata (gid);

-- Temporary table to support Cns_bulkexist calls
CREATE GLOBAL TEMPORARY TABLE Cns_files_exist_tmp
  (tmpFileId NUMBER) ON COMMIT DELETE ROWS;

-- Table to store configuration items at the DB level
CREATE TABLE CastorConfig
  (class VARCHAR2(50) CONSTRAINT NN_CastorConfig_class NOT NULL,
   key VARCHAR2(50) CONSTRAINT NN_CastorConfig_key NOT NULL,
   value VARCHAR2(100) CONSTRAINT NN_CastorConfig_value NOT NULL,
   description VARCHAR2(1000));

-- Provide a default configuration
INSERT INTO CastorConfig (class, key, value, description)
  VALUES ('stager', 'openmode', 'C', 'Mode for stager open/close operations: C for Compatibility (pre 2.1.14), N for New (from 2.1.14 onwards)');

-- A synonym allowing to acces the VMGR_TAPE_SIDE table from within the nameserver DB.
-- Note that the VMGR schema shall grant read access to the NS account with something like:
-- GRANT SELECT ON Vmgr_Tape_Side TO <CastorNsAccount>;
UNDEF vmgrSchema
ACCEPT vmgrSchema CHAR DEFAULT 'castor_vmgr' PROMPT 'Enter the name of the VMGR schema (default castor_vmgr): ';
CREATE OR REPLACE SYNONYM Vmgr_tape_side FOR &vmgrSchema..Vmgr_tape_side;

-- A synonym allowing access to the VMGR_TAPE_STATUS_VIEW view of the VMGR
-- schema from within the nameserver DB.
-- Note that the VMGR schema shall grant read access to the NS account with
-- something like:
-- GRANT SELECT ON VMGR_TAPE_STATUS_VIEW TO <CastorNsAccount>;
CREATE OR REPLACE VIEW Vmgr_tape_status_view AS SELECT * FROM &vmgrSchema..VMGR_TAPE_STATUS_VIEW;

-- Tables to store intermediate data to be passed from/to the stager.
-- Note that we cannot use temporary tables with distributed transactions.
CREATE TABLE SetSegsForFilesInputHelper
  (reqId VARCHAR2(36), fileId NUMBER, lastModTime NUMBER, copyNo NUMBER, oldCopyNo NUMBER, transfSize NUMBER,
   comprSize NUMBER, vid VARCHAR2(6), fseq NUMBER, blockId RAW(4), checksumType VARCHAR2(16), checksum NUMBER);

CREATE TABLE SetSegsForFilesResultsHelper
  (isOnlyLog NUMBER(1), reqId VARCHAR2(36), timeInfo NUMBER, errorCode INTEGER, fileId NUMBER, msg VARCHAR2(2048), params VARCHAR2(4000));

/* Insert the bare minimum to get a working system.
 * - Create a default 'system' fileclass. Pre-requisite to next step.
 * - Create the root directory.
 */
INSERT INTO Cns_class_metadata (classid, name, owner_uid, gid, min_filesize, max_filesize, flags, maxdrives,
  max_segsize, migr_time_interval, mintime_beforemigr, nbcopies, retenp_on_disk)
  VALUES (1, 'system', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
INSERT INTO cns_file_metadata (fileid, parent_fileid, guid, name, filemode, nlink, owner_uid, gid, filesize,
  atime, mtime, ctime, stagertime, fileclass, status, csumtype, csumvalue, acl)
  VALUES (2, 0, NULL,  '/', 16877, 0, 0, 0, 0, 0,  0, 0, 0, 1, '-', NULL, NULL, NULL);
COMMIT;
