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
 * This script contains prototyped code for SQL-based NS functions.
 * For the time being, a partial implementation of the stat functionality is provided.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

CREATE UNIQUE INDEX I_file_metadata_par_name_id on Cns_file_metadata(parent_fileid, name, fileid, acl);

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


-- A string tokenizer in PL/SQL
CREATE OR REPLACE FUNCTION strTokenizer(p_list VARCHAR2, p_del VARCHAR2 := ',')
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

-- PL/SQL function to normalize a filepath, i.e. to drop multiple '/'s and resolve any '..'
CREATE OR REPLACE FUNCTION normalizePath(path IN VARCHAR2) RETURN VARCHAR2 IS
  buf VARCHAR2(2048);
  ret VARCHAR2(2048);
BEGIN
  ret := REGEXP_REPLACE(path, '(/){2,}', '/');
  LOOP
    buf := ret;
    -- a path component is by definition anything between two slashes
    ret := REGEXP_REPLACE(buf, '/[^/]+/\.\./', '/');
    EXIT WHEN ret = buf;
  END LOOP;
  RETURN ret;
END;
/

-- PL/SQL function implementing the stat functionality, as in Cns_srv_statx()
CREATE OR REPLACE PROCEDURE cnsStat(filename IN VARCHAR2, euid IN NUMBER, egid IN NUMBER, stat OUT castorns.Cns_Stat_Cur) AS
-- This is for the moment without permission checking. The final version will have to
-- include the uid,gid of the requestor + the CWD - see Cns_procreq.c:6088
  fid NUMBER;
  parentFid NUMBER;
  acl NUMBER;
BEGIN
  fid := 2;
  FOR component IN (SELECT * FROM TABLE(strTokenizer(normalizePath(filename), '/'))
                     WHERE column_value IS NOT NULL) LOOP
    --dbms_output.put_line(component.column_value || ' ' || fid);
    SELECT /*+ INDEX(Cns_file_metadata I_file_metadata_par_name_id) */ 
           fileId, parent_fileId, acl, fileMode, owner_euid, gid
      INTO fid, parentFid, acl, mode, ouid, ogid
      FROM Cns_file_metadata
     WHERE parent_fileId = fid
       AND name = component.column_value;
    -- Authorization checks
    IF ouid != euid THEN
      effMode := trunc(mode/8);  -- shift 3 bits
      IF ogid != egid THEN
        effMode := trunc(effMode/8);
      END IF;
    END IF;
    IF BITAND(mode, effMode) <> effMode THEN
      -- IF aclCheck(acl) THEN
        -- IF Cupv check THEN
          raise_application_error(EACCESS);
          RETURN;
        -- END IF;
      -- END IF;
    END IF;
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

-- A function to extract the full path of a file with permission checking
CREATE OR REPLACE FUNCTION castor.getPathForFileidWithPerm(fid IN NUMBER, euid IN NUMBER, egid IN NUMBER, fmode IN NUMBER) RETURN VARCHAR2 IS
  CURSOR c IS
    SELECT name, owner_uid, gid, filemode
      FROM cns_file_metadata
    START WITH fileid = fid
    CONNECT BY fileid = PRIOR parent_fileid
    ORDER BY level DESC;
  p VARCHAR2(2048) := '';
  currMode NUMBER;
BEGIN
   FOR e in c LOOP
     currMode := fmode;
     IF e.owner_uid != euid THEN
       currMode := trunc(currMode/8);  -- shift 3 bits
       IF e.gid != egid THEN
         currMode := trunc(currMode/8);
       END IF;
     END IF;
     IF BITAND(e.filemode, currMode) <> currMode THEN
       RETURN '';
     END IF;
     p := p ||  '/' || e.name;
   END LOOP;
   -- remove first '/'
   RETURN replace(p,'///','/');
END;

