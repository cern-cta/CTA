/******************************************************************************
 *              cns_2.1.8-13_to_2.1.9-3.sql
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
 * This script upgrades a CASTOR v2.1.8-13 CNS database into v2.1.9-3
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
DECLARE
  unused VARCHAR2(30);
BEGIN
  SELECT table_name INTO unused
    FROM user_tables
   WHERE table_name = 'UPGRADELOG';
  EXECUTE IMMEDIATE
   'UPDATE UpgradeLog
       SET failureCount = failureCount + 1
     WHERE schemaVersion = ''2_1_9_3''
       AND release = ''2_1_9_3''
       AND state != ''COMPLETE''';
  COMMIT;
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
/

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM cns_version
   WHERE release LIKE '2_1_8_13%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

UPDATE cns_version SET schemaVersion = '2_1_9_3', release = '2_1_9_3';
COMMIT;


/* Schema changes go here */
/**************************/

/* Drop the file class check constraints */
DECLARE
  unused VARCHAR2(30);
BEGIN
  SELECT owner INTO unused
    FROM user_constraints
   WHERE constraint_name = 'CK_FILEMETADATA_FILECLASS'
     AND table_name = 'CNS_FILE_METADATA';
  EXECUTE IMMEDIATE 'ALTER TABLE Cns_file_metadata
                     DROP CONSTRAINT CK_FileMetadata_FileClass';
  EXECUTE IMMEDIATE 'ALTER TABLE Cns_file_metadata
                     DROP CONSTRAINT NN_FileMetadata_FileClass';
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;  -- Nothing
END;
/

-- Constraint to prevent a NULL file class on non symlink entries
ALTER TABLE Cns_file_metadata
  ADD CONSTRAINT CK_FileMetadata_FileClass
  CHECK ((bitand(filemode, 40960) = 40960  AND fileclass IS NULL)
     OR  (bitand(filemode, 40960) != 40960 AND fileclass IS NOT NULL));

-- Constraint to prevent negative nlinks
ALTER TABLE Cns_file_metadata 
  ADD CONSTRAINT CK_FileMetadata_Nlink
  CHECK (nlink >= 0 AND nlink IS NOT NULL);

/* Set all file class entries for symlinks to NULL */
UPDATE Cns_file_metadata SET fileclass = NULL
 WHERE bitand(filemode, 40960) = 40960;
COMMIT;

/* New foreign key constraint: #45700 */
ALTER TABLE Cns_file_metadata
  ADD CONSTRAINT fk_class FOREIGN KEY (fileclass)
  REFERENCES Cns_class_metadata (classid);

/* Create new index: #45700 */
CREATE INDEX I_file_metadata_fileclass ON Cns_file_metadata (fileclass);

/* Drop unneeded column: #45700 */
ALTER TABLE Cns_class_metadata DROP COLUMN nbdirs_using_class;

/* Drop unneeded sequences */
DROP SEQUENCE Cns_gid_seq;
DROP SEQUENCE Cns_uid_seq;

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
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('2_1_9_3', '2_1_9_3');
COMMIT;

DROP TABLE cns_version;

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


/* Update and revalidation of PL-SQL code */
/******************************************/

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


/* Recompile all invalid procedures, triggers and functions */
/************************************************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM user_objects
             WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION')
               AND status = 'INVALID')
  LOOP
    IF a.object_type = 'PROCEDURE' THEN
      EXECUTE IMMEDIATE 'ALTER PROCEDURE '||a.object_name||' COMPILE';
    ELSIF a.object_type = 'TRIGGER' THEN
      EXECUTE IMMEDIATE 'ALTER TRIGGER '||a.object_name||' COMPILE';
    ELSE
      EXECUTE IMMEDIATE 'ALTER FUNCTION '||a.object_name||' COMPILE';
    END IF;
  END LOOP;
END;
/

/* Flag the schema upgrade as COMPLETE */
/***************************************/
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE';
COMMIT;
