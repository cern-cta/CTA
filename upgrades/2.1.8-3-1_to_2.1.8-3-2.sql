/******************************************************************************
 *              2.1.8-3-1_to_2.1.8-3-2.sql
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
 * @(#)$RCSfile: 2.1.8-3-1_to_2.1.8-3-2.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/11/24 16:10:28 $ $Author: sponcec3 $
 *
 * This script upgrades a CASTOR v2.1.8-3-1 STAGER database into v2.1.8-3-2
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_8_3_1%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

UPDATE CastorVersion SET release = '2_1_8_3_2';
COMMIT;

/* update all lastUpdateTime of 10s to avoid falling into the bug
 * of the nameserver taking the request arrival time instead of
 * the file closing time as lastUpdateTime
 */
UPDATE Castorfile SET lastUpdateTime = lastUpdateTime + 10 WHERE lastUpdatetime > 0;
COMMIT;

/* PL/SQL method implementing selectCastorFile */
CREATE OR REPLACE PROCEDURE selectCastorFile (fId IN INTEGER,
                                              nh IN VARCHAR2,
                                              sc IN INTEGER,
                                              fc IN INTEGER,
                                              fs IN INTEGER,
                                              fn IN VARCHAR2,
                                              rid OUT INTEGER,
                                              rfs OUT INTEGER) AS
  CONSTRAINT_VIOLATED EXCEPTION;
  PRAGMA EXCEPTION_INIT(CONSTRAINT_VIOLATED, -1);
BEGIN
  BEGIN
    -- try to find an existing file and lock it
    SELECT id, fileSize INTO rid, rfs FROM CastorFile
     WHERE fileId = fid AND nsHost = nh FOR UPDATE;
    -- update lastAccess time
    UPDATE CastorFile SET LastAccessTime = getTime(),
                          lastKnownFileName = REGEXP_REPLACE(fn,'(/){2,}','/')
     WHERE id = rid;
  EXCEPTION WHEN NO_DATA_FOUND THEN
    -- insert new row
    INSERT INTO CastorFile (id, fileId, nsHost, svcClass, fileClass, fileSize,
                            creationTime, lastAccessTime, lastupdatetime, lastKnownFileName)
      VALUES (ids_seq.nextval, fId, nh, sc, fc, fs, getTime(), getTime(), getTime(), REGEXP_REPLACE(fn,'(/){2,}','/'))
      RETURNING id, fileSize INTO rid, rfs;
    INSERT INTO Id2Type (id, type) VALUES (rid, 2); -- OBJ_CastorFile
  END;
EXCEPTION WHEN CONSTRAINT_VIOLATED THEN
  -- retry the select since a creation was done in between
  SELECT id, fileSize INTO rid, rfs FROM CastorFile
    WHERE fileId = fid AND nsHost = nh FOR UPDATE;
END;
/

/*
 * PL/SQL method implementing the stager_qry based on file id
 */
CREATE OR REPLACE PROCEDURE fileIdStageQuery
 (fid IN NUMBER,
  nh IN VARCHAR2,
  svcClassId IN INTEGER,
  fileName IN VARCHAR2,
  result OUT castor.QueryLine_Cur) AS
  cfs "numList";
  currentFileName VARCHAR2(2048);  
BEGIN
  -- Extract CastorFile ids FROM the fileid
  SELECT id BULK COLLECT INTO cfs FROM CastorFile WHERE fileId = fid AND nshost = nh;
  -- Check and fix when needed the LastKnownFileNames
  IF (cfs.COUNT > 0) THEN
    SELECT lastKnownFileName INTO currentFileName
      FROM CastorFile
     WHERE id = cfs(cfs.FIRST);
    IF currentFileName != fileName THEN
      UPDATE CastorFile SET lastKnownFileName = fileName
       WHERE id = cfs(cfs.FIRST);
      COMMIT;
    END IF;
  END IF;
  -- Finally issue the actual query
  internalStageQuery(cfs, svcClassId, result);
END;
/
