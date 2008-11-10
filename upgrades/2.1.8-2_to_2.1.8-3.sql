/******************************************************************************
 *              2.1.8-2_to_2.1.8-3.sql
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
 * @(#)$RCSfile: 2.1.8-2_to_2.1.8-3.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/11/10 16:11:36 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.8-2 STAGER database into v2.1.8-3
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_8_2%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

UPDATE CastorVersion SET schemaVersion = '2_1_8_3', release = '2_1_8_3';
COMMIT;

/* Job management */
BEGIN
  FOR a IN (SELECT * FROM user_scheduler_jobs)
  LOOP
    -- Stop any running jobs
    IF a.state = 'RUNNING' THEN
      dbms_scheduler.stop_job(a.job_name);
    END IF;
    -- Schedule the start date of the job to 1 hour from now. This
    -- basically pauses the job for 1 hours so that the upgrade can
    -- go through as quickly as possible.
    dbms_scheduler.set_attribute(a.job_name, 'START_DATE', SYSDATE + 60/1440);
  END LOOP;
END;
/

/* Schema changes go here */
/**************************/

/* DiskCopy constraints */
ALTER TABLE DiskCopy ADD CONSTRAINT FK_DiskCopy_CastorFile
  FOREIGN KEY (castorFile) REFERENCES CastorFile (id)
  INITIALLY DEFERRED DEFERRABLE;

/* CastorFile constraints */
ALTER TABLE CastorFile ADD CONSTRAINT FK_CastorFile_SvcClass
  FOREIGN KEY (svcClass) REFERENCES SvcClass (id)
  INITIALLY DEFERRED DEFERRABLE;

ALTER TABLE CastorFile ADD CONSTRAINT FK_CastorFile_FileClass
  FOREIGN KEY (fileClass) REFERENCES FileClass (id)
  INITIALLY DEFERRED DEFERRABLE;

/* Stream constraints */
ALTER TABLE Stream ADD CONSTRAINT FK_Stream_TapePool
  FOREIGN KEY (tapePool) REFERENCES TapePool (id);

/* Extension of GCLocalFile to support logging of the service class name when
 * files are garbage collected
 */
ALTER TABLE GCLocalFile ADD (svcClassName VARCHAR2(2048));

/* Removal of unused attribute */
ALTER TABLE SvcClass DROP COLUMN replicationPolicy;

/* Constraints on SubRequest */
ALTER TABLE SubRequest ADD (svcHandler VARCHAR2(2048));
UPDATE SubRequest a
   SET svchandler = (
    SELECT Type2Obj.svchandler handler
      FROM Id2Type, Type2Obj
     WHERE a.request = Id2Type.id
       AND Id2Type.type = Type2Obj.type
       AND Type2Obj.svchandler IS NOT NULL);
UPDATE SubRequest SET svcHandler = 'NotNullNeeded' WHERE svcHandler IS NULL;
ALTER TABLE SubRequest MODIFY (svcHandler NOT NULL);
ALTER TABLE SubRequest MODIFY (creationTime NOT NULL);

/* Index on SubRequest to do ordered request processing */
CREATE INDEX I_SubRequest_RT_CT_ID ON 
  SubRequest(svcHandler, creationTime, id) LOCAL 
 (PARTITION P_STATUS_0_1_2,
  PARTITION P_STATUS_3_13_14,
  PARTITION P_STATUS_4,
  PARTITION P_STATUS_5,
  PARTITION P_STATUS_6,
  PARTITION P_STATUS_7,
  PARTITION P_STATUS_8,
  PARTITION P_STATUS_9_10,
  PARTITION P_STATUS_11,
  PARTITION P_STATUS_12,
  PARTITION P_STATUS_OTHER);  

/* Global temporary table to store the information on diskcopyies which need to
 * processed to see if too many replicas are online. This temporary table is
 * required to solve the error: `ORA-04091: table is mutating, trigger/function`
 */
CREATE GLOBAL TEMPORARY TABLE TooManyReplicasHelper
  (id NUMBER, fileSystem NUMBER, castorFile NUMBER)
  ON COMMIT DELETE ROWS;

/* Rename to castorDebug */
DROP PACKAGE CASTOR_DEBUG;

/* Drop DiskServer_PK constraint */
DECLARE
  unused VARCHAR2(30);
BEGIN
  SELECT owner INTO unused
    FROM user_constraints
   WHERE constraint_name = 'DISKSERVER_FK'
     AND constraint_type = 'R';
  EXECUTE IMMEDIATE 'ALTER TABLE FileSystem DROP CONSTRAINT DISKSERVER_FK';
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;  -- Do nothing
END;
/

/* Rename FileSystem to DiskServer constraint */
ALTER TABLE FileSystem ADD CONSTRAINT FK_FileSystem_DiskServer 
  FOREIGN KEY (diskServer) REFERENCES DiskServer(id);

/* Rename all primary keys prefixed with I_ to PK_ */
BEGIN
  FOR a IN (SELECT table_name,
                   constraint_name old, 
                   substr(substr(constraint_name, 3, length(constraint_name) - 5), 0, 24) new
              FROM user_constraints
             WHERE constraint_name LIKE 'I_%_ID'
               AND constraint_type = 'P')
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLE '||a.table_name||' RENAME CONSTRAINT '||
                       a.old||' TO PK_'||a.new||'_ID';
  END LOOP;
  EXECUTE IMMEDIATE 'ALTER TABLE SubRequest RENAME CONSTRAINT I_SUBREQUEST_PK TO PK_SubRequest_ID';
END;
/

/* Name generated SYS_* primary key constraints */
DECLARE
  cname VARCHAR2(30);
BEGIN
  -- LockTable
  SELECT constraint_name INTO cname
    FROM user_constraints
   WHERE table_name = 'LOCKTABLE'
     AND constraint_type = 'P'
     AND generated = 'GENERATED NAME';
  EXECUTE IMMEDIATE 'ALTER TABLE LOCKTABLE RENAME CONSTRAINT '||cname||' TO
                     PK_LockTable_DSId';
  -- GcPolicy
  SELECT constraint_name INTO cname
    FROM user_constraints
   WHERE table_name = 'GCPOLICY'
     AND constraint_type = 'P'
     AND generated = 'GENERATED NAME';
  EXECUTE IMMEDIATE 'ALTER TABLE GCPOLICY RENAME CONSTRAINT '||cname||' TO
                     PK_GcPolicy_name';
  -- FileSystemsToCheck
  SELECT constraint_name INTO cname
    FROM user_constraints
   WHERE table_name = 'FILESYSTEMSTOCHECK'
     AND constraint_type = 'P'
     AND generated = 'GENERATED NAME';
  EXECUTE IMMEDIATE 'ALTER TABLE FILESYSTEMSTOCHECK RENAME CONSTRAINT '||cname||' TO
                     PK_FSToCheck_FS';
END;
/

/* Rename all the indexes associated with the primary keys above */
BEGIN
  EXECUTE IMMEDIATE 'PURGE RECYCLEBIN';
  FOR a IN (SELECT * FROM user_constraints
             WHERE constraint_type = 'P'
               AND constraint_name != index_name)
  LOOP
    EXECUTE IMMEDIATE 'ALTER INDEX '||a.index_name||' RENAME TO '||a.constraint_name; 
  END LOOP;
END;
/

/* Drop unused constraint on DiskPool2SvcClass */
ALTER TABLE DiskPool2SvcClass DROP CONSTRAINT I_DISKPOOL2SVCCLA_PARENTCHILD;

/* Drop unneeded tables */
DROP TABLE StageFindRequestRequest;
DROP TABLE StageRequestQueryRequest;

/* Update the Type2Obj table */
DELETE FROM Type2Obj WHERE type IN (34, 41, 110, 112);

/* Drop unneeded functions */
DROP FUNCTION maxReplicaNbForSvcClass;


/* Update and revalidation of PL-SQL code */
/******************************************/



/* Recompile all procedures */
/****************************/
BEGIN
  FOR a IN (SELECT object_name, object_type
              FROM all_objects
             WHERE object_type = 'PROCEDURE'
               AND status = 'INVALID')
  LOOP
    EXECUTE IMMEDIATE 'ALTER PROCEDURE '||a.object_name||' COMPILE';
  END LOOP;
END;
/