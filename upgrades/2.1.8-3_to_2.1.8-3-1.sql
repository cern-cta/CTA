/******************************************************************************
 *              2.1.8-3_to_2.1.8-3-1.sql
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
 * @(#)$RCSfile: 2.1.8-3_to_2.1.8-3-1.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/11/18 10:19:00 $ $Author: waldron $
 *
 * This script upgrades a CASTOR v2.1.8-3 STAGER database into v2.1.8-3-1
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_8_3%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

UPDATE CastorVersion SET release = '2_1_8_3_1';
COMMIT;

/* Check that the PK_CASTORFILE_ID index exists */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT table_owner INTO unused
    FROM user_indexes
   WHERE table_name = 'CASTORFILE'
     AND index_name = 'PK_CASTORFILE_ID';
EXCEPTION WHEN NO_DATA_FOUND THEN
  raise_application_error(-20001, 'PK_CASTORFILE_ID index missing on CastorFile table');
END;
/

/* Get input for python migration policy */
CREATE OR REPLACE PROCEDURE inputForMigrationPolicy
(svcclassName IN VARCHAR2,
 policyName OUT VARCHAR2,
 svcId OUT NUMBER,
 dbInfo OUT castor.DbMigrationInfo_Cur) AS
 tcIds "numList";
BEGIN
  -- do the same operation of getMigrCandidate and return the dbInfoMigrationPolicy
  -- we look first for repack condidates for this svcclass
  -- we update atomically WAITPOLICY
  SELECT SvcClass.migratorpolicy, SvcClass.id INTO policyName, svcId
    FROM SvcClass
   WHERE SvcClass.name = svcClassName;

  UPDATE /*+ INDEX(TC I_TAPECOPY_STATUS) */
         TapeCopy TC 
     SET status = 7
   WHERE status IN (0, 1)
     AND (EXISTS
       (SELECT 'x' FROM SubRequest, StageRepackRequest
         WHERE StageRepackRequest.svcclass = svcId
           AND SubRequest.request = StageRepackRequest.id
           AND SubRequest.status = 12  -- SUBREQUEST_REPACK
           AND TC.castorfile = SubRequest.castorfile
      ) OR EXISTS (
        SELECT /*+ INDEX(CF PK_CASTORFILE_ID)
                   NO_INDEX(CF I_CASTORFILE_SVCCLASS) */ 'x' 
          FROM CastorFile CF
         WHERE TC.castorFile = CF.id
           AND CF.svcClass = svcId))
    RETURNING TC.id -- CREATED / TOBEMIGRATED
    BULK COLLECT INTO tcIds;
  COMMIT;
  -- return the full resultset
  OPEN dbInfo FOR
    SELECT TapeCopy.id, TapeCopy.copyNb, CastorFile.lastknownfilename,
           CastorFile.nsHost, CastorFile.fileid, CastorFile.filesize
      FROM Tapecopy,CastorFile
     WHERE CastorFile.id = TapeCopy.castorfile
       AND TapeCopy.id IN 
         (SELECT /*+ CARDINALITY(tcidTable 5) */ * 
            FROM table(tcIds) tcidTable);
END;
/
