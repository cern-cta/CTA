/*****************************************************************************
 *              oracleCreate.sql
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
 * @(#)$RCSfile: oracleCreate.sql,v $ $Release: 1.2 $ $Release$ $Date: 2009/06/30 12:54:05 $ $Author: waldron $
 *
 * This script creates a new Castor Name Server schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

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
       retenp_on_disk NUMBER);

CREATE TABLE Cns_tp_pool (
       classid NUMBER(5),
       tape_pool VARCHAR2(15));

CREATE TABLE Cns_file_metadata (
       fileid NUMBER,
       parent_fileid NUMBER,
       guid CHAR(36),
       name VARCHAR2(255),
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
	USING INDEX STORAGE (INITIAL 2M NEXT 2M PCTINCREASE 0)
       ADD CONSTRAINT fk_class FOREIGN KEY (fileclass) REFERENCES Cns_class_metadata(classid);

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
       ADD CONSTRAINT fk_u_fileid FOREIGN KEY (u_fileid) REFERENCES Cns_file_metadata(fileid);

ALTER TABLE Cns_seg_metadata
       ADD CONSTRAINT fk_s_fileid FOREIGN KEY (s_fileid) REFERENCES Cns_file_metadata(fileid)
       ADD CONSTRAINT tapeseg UNIQUE (vid, side, fseq);

ALTER TABLE Cns_tp_pool
       ADD CONSTRAINT classpool UNIQUE (classid, tape_pool);

ALTER TABLE Cns_symlinks
       ADD CONSTRAINT fk_l_fileid FOREIGN KEY (fileid) REFERENCES Cns_file_metadata(fileid);

-- Create the Cns_version table
CREATE TABLE Cns_version (schemaVersion VARCHAR2(20), release VARCHAR2(20));
INSERT INTO Cns_version VALUES ('2_1_9_0', 'releaseTag');

-- Create indexes on Cns_file_metadata
CREATE INDEX PARENT_FILEID_IDX on Cns_file_metadata(parent_fileid);
CREATE INDEX I_file_metadata_fileclass on Cns_file_metadata(fileclass);
CREATE INDEX I_file_metadata_par_name_id on Cns_file_metadata(parent_fileid, name, fileid, acl);

-- Temporary table to support Cns_bulkexist calls
CREATE GLOBAL TEMPORARY TABLE Cns_files_exist_tmp
  (tmpFileId NUMBER) ON COMMIT DELETE ROWS;

-- Useful type for the tokenizer function
CREATE OR REPLACE TYPE strList AS TABLE OF VARCHAR2(2048);

-- PL/SQL declaration for the castorNS package
CREATE OR REPLACE PACKAGE castorns AS
  TYPE Cns_Stat IS RECORD (
    fileid NUMBER,
    parent_fileid NUMBER,
    filemode NUMBER(6),
    nlink NUMBER(6),
    euid NUMBER(6),
    egid NUMBER(6),
    filesize NUMBER,
    atime NUMBER(10),
    mtime NUMBER(10),
    ctime NUMBER(10),
    fileclass NUMBER(10),
    status CHAR(1),
    name VARCHAR2(1024)
  );
  TYPE Cns_Stat_Cur IS REF CURSOR RETURN Cns_Stat;
END castorns;
/

-- A function to extract the full path of a file in one go
CREATE OR REPLACE FUNCTION getPathForFileid(fid IN NUMBER) RETURN VARCHAR2 IS
  CURSOR c IS
    SELECT name
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

-- A string tokenizer in PL/SQL
CREATE OR REPLACE FUNCTION strTokenizer (p_list VARCHAR2, p_del VARCHAR2 := ',')
  RETURN strList pipelined IS
  l_idx INTEGER;
  l_list VARCHAR2(32767) := p_list;
  l_value VARCHAR2(32767);
BEGIN
  LOOP
    l_idx := instr(l_list, p_del);
    IF l_idx > 0 THEN
      PIPE ROW(ltrim(rtrim(substr(l_list, 1, l_idx-1))));
      l_list := substr(l_list, l_idx + length(p_del));
    ELSE
      PIPE ROW(ltrim(rtrim(l_list)));
      EXIT;
    END IF;
  END LOOP;
  RETURN;
END;
/

-- PL/SQL function to canonicalize a filepath, i.e. to drop multiple '/'s and resolve any '..'
CREATE OR REPLACE FUNCTION canonicalizePath(path IN VARCHAR2) RETURN VARCHAR2 IS
  buf VARCHAR2(2048);
  ret VARCHAR2(2048);
BEGIN
  ret := REGEXP_REPLACE(path, '(/){2,}', '/');
  LOOP
    buf := ret;
    -- a path component is by definition anything between two slashes
    ret := REGEXP_REPLACE(buf, '/[^/]+/../', '/');
    EXIT WHEN ret = buf;
  END LOOP;
  RETURN ret;
END;
/

-- PL/SQL function implementing the stat functionality, as in Cns_srv_statx()
CREATE OR REPLACE PROCEDURE cnsStat(filename IN VARCHAR2, stat OUT castorns.Cns_Stat_Cur) AS
-- This is for the moment without permission checking. The final version will have to
-- include the uid,gid of the requestor + the CWD - see Cns_procreq.c:6088
  fid NUMBER;
  parentFid NUMBER;
  acl NUMBER;
BEGIN
  fid := 2;
  FOR component IN (SELECT * FROM TABLE(strTokenizer(canonicalizePath(filename), '/'))
                     WHERE column_value IS NOT NULL) LOOP
    --dbms_output.put_line(component.column_value || ' ' || fid);
    SELECT /*+ INDEX(Cns_file_metadata I_file_metadata_par_name_id) */ 
           fileId, parent_fileId, acl
      INTO fid, parentFid, acl
      FROM Cns_file_metadata
     WHERE parent_fileId = fid
       AND name = component.column_value;
    -- ACL verification goes here
    -- ...
  END LOOP;
  OPEN stat FOR
    SELECT
       FILEID, PARENT_FILEID,
       FILEMODE, NLINK, OWNER_UID, GID, FILESIZE,
       ATIME, MTIME, CTIME,
       FILECLASS, STATUS, NAME
     FROM Cns_file_metadata
    WHERE fileid = fid;
  --dbms_output.put_line(lastComponent || ' ' || fid);
END;
/
