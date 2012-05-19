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

CREATE TABLE Cns_file_metadata (fileid NUMBER, parent_fileid NUMBER, guid CHAR(36), name VARCHAR2(255), filemode NUMBER(6), nlink NUMBER(6), owner_uid NUMBER(6), gid NUMBER(6), filesize NUMBER, atime NUMBER(10), mtime NUMBER(10), ctime NUMBER(10), fileclass NUMBER(5), status CHAR(1), csumtype VARCHAR2(2), csumvalue VARCHAR2(32), acl VARCHAR2(3900)) STORAGE (INITIAL 5M NEXT 5M PCTINCREASE 0);

CREATE TABLE Cns_user_metadata (u_fileid NUMBER, comments VARCHAR2(255));

CREATE TABLE Cns_seg_metadata (s_fileid NUMBER, copyno NUMBER(1),fsec NUMBER(3), segsize NUMBER, compression NUMBER, s_status CHAR(1), vid VARCHAR2(6), side NUMBER (1), fseq NUMBER(10), blockid RAW(4), checksum_name VARCHAR2(16), checksum NUMBER);

CREATE TABLE Cns_symlinks (fileid NUMBER, linkname VARCHAR2(1023));

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

-- Temporary tables to store intermediate data to be passed from/to the stager.
-- We keep the data on commit and truncate it explicitly afterwards as we
-- want to use it over multiple commits (e.g. on bulk multi-file operations).
CREATE GLOBAL TEMPORARY TABLE SetSegmentsForFilesHelper
  (reqId VARCHAR2(36) CONSTRAINT PK_SetSegsHelper_ReqId PRIMARY KEY,
   fileId NUMBER, lastModTime NUMBER, copyNo NUMBER, oldCopyNo NUMBER, transfSize NUMBER,
   comprSize NUMBER, vid VARCHAR2(6), fseq NUMBER, blockId RAW(4), checksumType VARCHAR2(2), checksum NUMBER)
  ON COMMIT PRESERVE ROWS;

CREATE GLOBAL TEMPORARY TABLE ResultsLogHelper
  (reqId VARCHAR2(36) CONSTRAINT PK_ResLogHelper_ReqId PRIMARY KEY,
   timeinfo NUMBER, ec INTEGER, fileId NUMBER, msg VARCHAR2(2048), params VARCHAR2(4000))
  ON COMMIT PRESERVE ROWS;
