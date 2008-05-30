/******************************************************************************
 *              2.1.7-7_to_2.1.7-8.sql
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
 * @(#)$RCSfile: 2.1.7-7_to_2.1.7-10.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/05/30 15:48:13 $ $Author: sponcec3 $
 *
 * This script upgrades a CASTOR v2.1.7-7 database into v2.1.7-8
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion WHERE release LIKE '2_1_7_7%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;

UPDATE CastorVersion SET schemaVersion = '2_1_7_8', release = '2_1_7_8';
COMMIT;

/* schema changes */
DECLARE
  cnt NUMBER;
BEGIN
  -- Enable row movement on the StageDiskCopyReplicaRequest table. This was
  -- missing in the 2.1.6 upgrade
  SELECT count(*) INTO cnt
    FROM user_tables
   WHERE table_name = 'STAGEDISKCOPYREPLICAREQUEST'
     AND row_movement = 'N';
  IF cnt = 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE StageDiskCopyReplicaRequest ENABLE ROW MOVEMENT';
  END IF;

  -- Add an index on the tape column of the segment table.
  SELECT count(*) INTO cnt
    FROM user_indexes
   WHERE index_name = 'I_SEGMENT_TAPE';
  IF cnt = 0 THEN
    EXECUTE IMMEDIATE 'CREATE INDEX I_SEGMENT_TAPE ON SEGMENT (TAPE)';
  END IF;
END;

/* New index to speedup SubRequest cleanup procedure */
CREATE INDEX I_SubRequest_LastModTime on SubRequest (lastModificationTime) LOCAL;

/* Modifications to the segment table to support prioritisations */
ALTER TABLE Segment ADD priority INTEGER;

/* SQL statements for type PriorityMap */ 
CREATE TABLE PriorityMap (priority INTEGER, euid INTEGER, egid INTEGER, id INTEGER CONSTRAINT I_PriorityMap_Id PRIMARY KEY) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT; 
ALTER TABLE PriorityMap ADD CONSTRAINT I_Unique_Priority UNIQUE (euid, egid);

/* New request types ChangePrivilege and ListPrivileges */
INSERT INTO Type2Obj (type, object, svcHandler) VALUES (152, 'ChangePrivilege', 'QueryReqSvc');
INSERT INTO Type2Obj (type, object, svcHandler) VALUES (155, 'ListPrivileges', 'QueryReqSvc');

/* SQL statements for type ChangePrivilege */
CREATE TABLE ChangePrivilege (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, isGranted NUMBER, id INTEGER CONSTRAINT I_ChangePrivilege_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type BWUser */
CREATE TABLE BWUser (euid NUMBER, egid NUMBER, id INTEGER CONSTRAINT I_BWUser_Id PRIMARY KEY, request INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type RequestType */
CREATE TABLE RequestType (reqType NUMBER, id INTEGER CONSTRAINT I_RequestType_Id PRIMARY KEY, request INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* SQL statements for type ListPrivileges */
CREATE TABLE ListPrivileges (flags INTEGER, userName VARCHAR2(2048), euid NUMBER, egid NUMBER, mask NUMBER, pid NUMBER, machine VARCHAR2(2048), svcClassName VARCHAR2(2048), userTag VARCHAR2(2048), reqId VARCHAR2(2048), creationTime INTEGER, lastModificationTime INTEGER, userId NUMBER, groupId NUMBER, requestType NUMBER, id INTEGER CONSTRAINT I_ListPrivileges_Id PRIMARY KEY, svcClass INTEGER, client INTEGER) INITRANS 50 PCTFREE 50 ENABLE ROW MOVEMENT;

/* Drop cleaningDaemon related objects not used any longer */
DROP PROCEDURE deleteOutOfDateRequests;
DROP PROCEDURE deleteArchivedRequests;
DROP TABLE OutOfDateStageOutDropped;
DROP TABLE OutOfDateStageOutPutDone;

/* Table to log the activity performed by the cleanup procedure */ 
CREATE TABLE CleanupJobLog
  (fileId NUMBER NOT NULL, nsHost VARCHAR2(2048) NOT NULL, 
   operation INTEGER NOT NULL);

/* Define a table for some configuration key-value pairs and populate it */
CREATE TABLE CastorConfig
  (class VARCHAR2(2048) NOT NULL, key VARCHAR2(2048) NOT NULL, value VARCHAR2(2048) NOT NULL, description VARCHAR2(2048));

-- Service managers are requested to override this entry with a meaningful value reflecting the instance
INSERT INTO CastorConfig
  VALUES ('general', 'instance', 'castorstager', 'Name of this Castor instance');

INSERT INTO CastorConfig
  VALUES ('cleaning', 'terminatedRequestsTimeout', '120', 'Maximum timeout for successful and failed requests in hours');
INSERT INTO CastorConfig
  VALUES ('cleaning', 'outOfDateStageOutDCsTimeout', '72', 'Timeout for STAGEOUT diskCopies in hours');
INSERT INTO CastorConfig
  VALUES ('cleaning', 'failedDCsTimeout', '72', 'Timeout for failed diskCopies in hour');

-- new version of the castorbw package
/* PL/SQL method implementing checkPermission */
CREATE OR REPLACE PROCEDURE checkPermission(isvcClass IN VARCHAR2,
                                            ieuid IN NUMBER,
                                            iegid IN NUMBER,
                                            ireqType IN NUMBER,
                                            res OUT NUMBER) AS
  unused NUMBER;
BEGIN
  SELECT count(*) INTO unused
    FROM WhiteList
   WHERE (svcClass = isvcClass OR svcClass IS NULL
          OR (length(isvcClass) IS NULL AND svcClass = 'default'))
     AND (egid = iegid OR egid IS NULL)
     AND (euid = ieuid OR euid IS NULL)
     AND (reqType = ireqType OR reqType IS NULL);
  IF unused = 0 THEN
    -- Not found in White list -> no access
    res := -1;
  ELSE
    SELECT count(*) INTO unused
      FROM BlackList
     WHERE (svcClass = isvcClass OR svcClass IS NULL
            OR (length(isvcClass) IS NULL AND svcClass = 'default'))
       AND (egid = iegid OR egid IS NULL)
       AND (euid = ieuid OR euid IS NULL)
       AND (reqType = ireqType OR reqType IS NULL);
    IF unused = 0 THEN
      -- Not Found in Black list -> access
      res := 0;
    ELSE
      -- Found in Black list -> no access
      res := -1;
    END IF;
  END IF;
END;


/* Function to wrap the checkPermission procedure so that is can be
   used within SQL queries. The function returns 0 if the user has
   access on the service class for the given request type otherwise
   1 if access is denied */
CREATE OR REPLACE 
FUNCTION checkPermissionOnSvcClass(reqSvcClass IN VARCHAR2,
                                   reqEuid IN NUMBER,
                                   reqEgid IN NUMBER,
                                   reqType IN NUMBER)
RETURN NUMBER AS
  res NUMBER;
BEGIN
  -- Check the users access rights */
  checkPermission(reqSvcClass, reqEuid, reqEgid, reqType, res);
  IF res = 0 THEN
    RETURN 0;
  END IF;
  RETURN 1;
END;


/**
  * Black and while list mechanism
  * In order to be able to enter a request for a given service class, you need :
  *   - to be in the white list for this service class
  *   - to not be in the black list for this services class
  * Being in a list means :
  *   - either that your uid,gid is explicitely in the list
  *   - or that your gid is in the list with null uid (that is group wildcard)
  *   - or there is an entry with null uid and null gid (full wild card)
  * The permissions can also have a request type. Default is null, that is everything
  */
CREATE OR REPLACE PACKAGE castorBW AS
  TYPE Privilege IS RECORD (
    svcClass VARCHAR2(2048),
    euid NUMBER,
    egid NUMBER,
    reqType NUMBER);
  -- a cursor of privileges
  TYPE Privilege_Cur IS REF CURSOR RETURN Privilege;
  -- Intersection of 2 priviledges
  -- raises -20109, "Empty privilege" in case the intersection is empty
  FUNCTION intersection(p1 IN Privilege, p2 IN Privilege) RETURN Privilege;
  -- Does one priviledge P1 contain another one P2 ?
  FUNCTION contains(p1 Privilege, p2 Privilege) RETURN Boolean;
  -- Intersection of a priviledge P with the WhiteList
  -- The result is stored in the temporary table removePrivilegeTmpTable
  -- that is cleaned up when the procedure starts
  PROCEDURE intersectionWithWhiteList(p Privilege);
  -- Difference between priviledge P1 and priviledge P2
  -- raises -20108, "Invalid privilege intersection" in case the difference
  -- can not be computed
  -- raises -20109, "Empty privilege" in case the difference is empty
  FUNCTION diff(P1 Privilege, P2 Privilege) RETURN Privilege;
  -- remove priviledge P from list L
  PROCEDURE removePrivilegeFromBlackList(p Privilege);
  -- Add priviledge P to WhiteList
  PROCEDURE addPrivilegeToWL(p Privilege);
  -- Add priviledge P to BlackList
  PROCEDURE addPrivilegeToBL(p Privilege);
  -- cleanup BlackList after privileges were removed from the whitelist
  PROCEDURE cleanupBL;
  -- Add priviledge P
  PROCEDURE addPrivilege(P Privilege);
  -- Remove priviledge P
  PROCEDURE removePrivilege(P Privilege);
  -- Add priviledge(s)
  PROCEDURE addPrivilege(svcClassId NUMBER, euid NUMBER, egid NUMBER, reqType NUMBER);
  -- Remove priviledge(S)
  PROCEDURE removePrivilege(svcClassId NUMBER, euid NUMBER, egid NUMBER, reqType NUMBER);
  -- List priviledge(s)
  PROCEDURE listPrivileges(svcClassId IN NUMBER, ieuid IN NUMBER,
                           iegid IN NUMBER, ireqType IN NUMBER,
                           wlist OUT Privilege_Cur, blist OUT Privilege_Cur);
END castorBW;

CREATE OR REPLACE PACKAGE BODY castorBW AS

  -- Intersection of 2 priviledges
  FUNCTION intersection(p1 IN Privilege, p2 IN Privilege)
  RETURN Privilege AS
    res Privilege;
  BEGIN
    IF p1.euid IS NULL OR p1.euid = p2.euid THEN
      res.euid := p2.euid;
    ELSIF p2.euid IS NULL THEN
      res.euid := p1.euid;
    ELSE
      raise_application_error(-20109, 'Empty privilege');
    END IF;
    IF p1.egid IS NULL OR p1.egid = p2.egid THEN
      res.egid := p2.egid;
    ELSIF p2.egid IS NULL THEN
      res.egid := p1.egid;
    ELSE
      raise_application_error(-20109, 'Empty privilege');
    END IF;
    IF p1.svcClass IS NULL OR p1.svcClass = p2.svcClass THEN
      res.svcClass := p2.svcClass;
    ELSIF p2.svcClass IS NULL THEN
      res.svcClass := p1.svcClass;
    ELSE
      raise_application_error(-20109, 'Empty privilege');
    END IF;
    IF p1.reqType IS NULL OR p1.reqType = p2.reqType THEN
      res.reqType := p2.reqType;
    ELSIF p2.reqType IS NULL THEN
      res.reqType := p1.reqType;
    ELSE
      raise_application_error(-20109, 'Empty privilege');
    END IF;
    RETURN res;
  END;
  
  -- Does one priviledge P1 contain another one P2 ?
  FUNCTION contains(p1 Privilege, p2 Privilege) RETURN Boolean AS
  BEGIN
    IF p1.euid IS NOT NULL -- p1 NULL means it contains everything !
       AND (p2.euid IS NULL OR p1.euid != p2.euid) THEN
      RETURN FALSE;
    END IF;
    IF p1.egid IS NOT NULL -- p1 NULL means it contains everything !
       AND (p2.egid IS NULL OR p1.egid != p2.egid) THEN
      RETURN FALSE;
    END IF;
    IF p1.svcClass IS NOT NULL -- p1 NULL means it contains everything !
       AND (p2.svcClass IS NULL OR p1.svcClass != p2.svcClass) THEN
      RETURN FALSE;
    END IF;
    IF p1.reqType IS NOT NULL -- p1 NULL means it contains everything !
       AND (p2.reqType IS NULL OR p1.reqType != p2.reqType) THEN
      RETURN FALSE;
    END IF;
    RETURN TRUE;
  END;
  
  -- Intersection of a priviledge P with the WhiteList
  -- The result is stored in the temporary table removePrivilegeTmpTable
  PROCEDURE intersectionWithWhiteList(p Privilege) AS
    wlr Privilege;
    tmp Privilege;
    empty_privilege EXCEPTION;
    PRAGMA EXCEPTION_INIT(empty_privilege, -20109);
  BEGIN
    DELETE FROM removePrivilegeTmpTable;
    FOR r IN (SELECT * FROM WhiteList) LOOP
      BEGIN
        wlr.svcClass := r.svcClass;
        wlr.euid := r.euid;
        wlr.egid := r.egid;
        wlr.reqType := r.reqType;
        tmp := intersection(wlr, p);
        INSERT INTO removePrivilegeTmpTable
        VALUES (tmp.svcClass, tmp.euid, tmp.egid, tmp.reqType);
      EXCEPTION WHEN empty_privilege THEN
        NULL;
      END;
    END LOOP;
  END;

  -- Difference between priviledge P1 and priviledge P2
  FUNCTION diff(P1 Privilege, P2 Privilege) RETURN Privilege AS
    empty_privilege EXCEPTION;
    PRAGMA EXCEPTION_INIT(empty_privilege, -20109);
    unused Privilege;
  BEGIN
    IF contains(P1, P2) THEN
      IF P1.euid = P2.euid AND P1.egid = P2.egid AND P1.svcClass = P2.svcClass AND P1.reqType = P2.reqType THEN
        raise_application_error(-20109, 'Empty privilege');
      ELSE
        raise_application_error(-20108, 'Invalid privilege intersection');
      END IF;
    ELSIF contains(P2, P1) THEN
      raise_application_error(-20109, 'Empty privilege');
    ELSE
      BEGIN
        unused := intersection(P1, P2);
        -- no exception, so the intersection is not empty.
        -- we don't know how to handle such a case
        raise_application_error(-20108, 'Invalid privilege intersection');
      EXCEPTION WHEN empty_privilege THEN
      -- P1 and P2 do not intersect, the diff is thus P1
        RETURN P1;
      END;
    END IF;
  END;
  
  -- remove priviledge P from list L
  PROCEDURE removePrivilegeFromBlackList(p Privilege) AS
    blr Privilege;
    tmp Privilege;
    empty_privilege EXCEPTION;
    PRAGMA EXCEPTION_INIT(empty_privilege, -20109);
  BEGIN
    FOR r IN (SELECT * FROM BlackList) LOOP
      BEGIN
        blr.svcClass := r.svcClass;
        blr.euid := r.euid;
        blr.egid := r.egid;
        blr.reqType := r.reqType;
        tmp := diff(blr, p);
      EXCEPTION WHEN empty_privilege THEN
        -- diff raised an exception saying that the diff is empty
        -- thus we drop the line
        DELETE FROM BlackList
         WHERE  nvl(svcClass, -1) = nvl(r.svcClass, -1) AND
               nvl(euid, -1) = nvl(r.euid, -1) AND
               nvl(egid, -1) = nvl(r.egid, -1) AND
               nvl(reqType, -1) = nvl(r.reqType, -1);
      END;
    END LOOP;
  END;
  
  -- Add priviledge P to list L :
  PROCEDURE addPrivilegeToWL(p Privilege) AS
    wlr Privilege;
    extended boolean := FALSE;
  BEGIN
    FOR r IN (SELECT * FROM WhiteList) LOOP
      wlr.svcClass := r.svcClass;
      wlr.euid := r.euid;
      wlr.egid := r.egid;
      wlr.reqType := r.reqType;
      -- check if we extend a privilege
      IF contains(p, wlr) THEN
        IF extended THEN
          -- drop this row, it merged into the new one
          DELETE FROM WhiteList
           WHERE nvl(svcClass, -1) = nvl(wlr.svcClass, -1) AND
                 nvl(euid, -1) = nvl(wlr.euid, -1) AND
                 nvl(egid, -1) = nvl(wlr.egid, -1) AND
                 nvl(reqType, -1) = nvl(wlr.reqType, -1);
        ELSE
          -- replace old row with new one
          UPDATE WhiteList
             SET svcClass = p.svcClass,
                 euid = p.euid,
                 egid = p.egid,
                 reqType = p.reqType
           WHERE nvl(svcClass, -1) = nvl(wlr.svcClass, -1) AND
                 nvl(euid, -1) = nvl(wlr.euid, -1) AND
                 nvl(egid, -1) = nvl(wlr.egid, -1) AND
                 nvl(reqType, -1) = nvl(wlr.reqType, -1);
          extended := TRUE;
        END IF;
      END IF;
      -- check if privilege is there
      IF contains(wlr, p) THEN RETURN; END IF;
    END LOOP;
    IF NOT extended THEN
      INSERT INTO WhiteList VALUES p;
    END IF;
  END;
  
  -- Add priviledge P to list L :
  PROCEDURE addPrivilegeToBL(p Privilege) AS
    blr Privilege;
    extended boolean := FALSE;
  BEGIN
    FOR r IN (SELECT * FROM BlackList) LOOP
      blr.svcClass := r.svcClass;
      blr.euid := r.euid;
      blr.egid := r.egid;
      blr.reqType := r.reqType;
      -- check if privilege is there
      IF contains(blr, p) THEN RETURN; END IF;
      -- check if we extend a privilege
      IF contains(p, blr) THEN
        IF extended THEN
          -- drop this row, it merged into the new one
          DELETE FROM BlackList
           WHERE nvl(svcClass, -1) = nvl(blr.svcClass, -1) AND
                 nvl(euid, -1) = nvl(blr.euid, -1) AND
                 nvl(egid, -1) = nvl(blr.egid, -1) AND
                 nvl(reqType, -1) = nvl(blr.reqType, -1);
        ELSE
          -- replace old row with new one
          UPDATE BlackList
             SET svcClass = p.svcClass,
                 euid = p.euid,
                 egid = p.egid,
                 reqType = p.reqType
           WHERE nvl(svcClass, -1) = nvl(blr.svcClass, -1) AND
                 nvl(euid, -1) = nvl(blr.euid, -1) AND
                 nvl(egid, -1) = nvl(blr.egid, -1) AND
                 nvl(reqType, -1) = nvl(blr.reqType, -1);
          extended := TRUE;
        END IF;
      END IF;
    END LOOP;
    IF NOT extended THEN
      INSERT INTO BlackList VALUES p;
    END IF;
  END;
  
  -- cleanup BlackList when a privilege was removed from the whitelist
  PROCEDURE cleanupBL AS
    blr Privilege;
    c NUMBER;
  BEGIN
    FOR r IN (SELECT * FROM BlackList) LOOP
      blr.svcClass := r.svcClass;
      blr.euid := r.euid;
      blr.egid := r.egid;
      blr.reqType := r.reqType;
      intersectionWithWhiteList(blr);
      SELECT COUNT(*) INTO c FROM removePrivilegeTmpTable;
      IF c = 0 THEN
        -- we can safely drop this line
        DELETE FROM BlackList
         WHERE  nvl(svcClass, -1) = nvl(r.svcClass, -1) AND
               nvl(euid, -1) = nvl(r.euid, -1) AND
               nvl(egid, -1) = nvl(r.egid, -1) AND
               nvl(reqType, -1) = nvl(r.reqType, -1);
      END IF;
    END LOOP;
  END;

  -- Add priviledge P
  PROCEDURE addPrivilege(P Privilege) AS
  BEGIN
    removePrivilegeFromBlackList(P);
    addPrivilegeToWL(P);
    COMMIT;
  EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
  END;
  
  -- Remove priviledge P
  PROCEDURE removePrivilege(P Privilege) AS
    c NUMBER;
    wlr Privilege;
  BEGIN
    -- Check first whether there is something to remove
    intersectionWithWhiteList(P);
    SELECT COUNT(*) INTO c FROM removePrivilegeTmpTable;
    IF c = 0 THEN RETURN; END IF;
    -- Remove effectively what can be removed
    FOR r IN (SELECT * FROM WHITELIST) LOOP
      wlr.svcClass := r.svcClass;
      wlr.euid := r.euid;
      wlr.egid := r.egid;
      wlr.reqType := r.reqType;
      IF contains(P, wlr) THEN
        DELETE FROM WhiteList
         WHERE nvl(svcClass, -1) = nvl(wlr.svcClass, -1) AND
               nvl(euid, -1) = nvl(wlr.euid, -1) AND
               nvl(egid, -1) = nvl(wlr.egid, -1) AND
               nvl(reqType, -1) = nvl(wlr.reqType, -1);
      END IF;
    END LOOP;
    -- cleanup blackList
    cleanUpBL();
    -- check what remains
    intersectionWithWhiteList(P);
    SELECT COUNT(*) INTO c FROM removePrivilegeTmpTable;
    IF c = 0 THEN RETURN; END IF;
    -- If any, add them to blackList
    FOR q IN (SELECT * FROM removePrivilegeTmpTable) LOOP
      wlr.svcClass := q.svcClass;
      wlr.euid := q.euid;
      wlr.egid := q.egid;
      wlr.reqType := q.reqType;
      addPrivilegeToBL(wlr);
    END LOOP;
  END;

  -- Add priviledge
  PROCEDURE addPrivilege(svcClassId NUMBER, euid NUMBER, egid NUMBER, reqType NUMBER) AS
    p castorBW.Privilege;
  BEGIN
    p.svcClass := svcClassId;
    p.euid := euid;
    p.egid := egid;
    p.reqType := reqType;
    addPrivilege(p);
  END;

  -- Remove priviledge
  PROCEDURE removePrivilege(svcClassId NUMBER, euid NUMBER, egid NUMBER, reqType NUMBER) AS
    p castorBW.Privilege;
  BEGIN
    p.svcClass := svcClassId;
    p.euid := euid;
    p.egid := egid;
    p.reqType := reqType;
    removePrivilege(p);
  END;

  -- Remove priviledge
  PROCEDURE listPrivileges(svcClassId IN NUMBER, ieuid IN NUMBER,
                           iegid IN NUMBER, ireqType IN NUMBER,
                           wlist OUT Privilege_Cur, blist OUT Privilege_Cur) AS
  BEGIN
    OPEN wlist FOR
      SELECT decode(svcClass, NULL, '*',
                    nvl((SELECT name FROM SvcClass
                          WHERE id = svcClass),
                        '['||TO_CHAR(svcClass)||']')),
             euid, egid, reqType
        FROM WhiteList
       WHERE (WhiteList.svcClass = svcClassId OR svcClassId = 0)
         AND (WhiteList.euid = ieuid OR ieuid = -1)
         AND (WhiteList.egid = iegid OR iegid = -1)
         AND (WhiteList.reqType = ireqType OR ireqType = 0);
    OPEN blist FOR
      SELECT decode(svcClass, NULL, '*',
                    nvl((SELECT name FROM SvcClass
                          WHERE id = svcClass),
                        '['||TO_CHAR(svcClass)||']')),
             euid, egid, reqType
        FROM BlackList
       WHERE (BlackList.svcClass = svcClassId OR svcClassId = 0)
         AND (BlackList.euid = ieuid OR ieuid = -1)
         AND (BlackList.egid = iegid OR iegid = -1)
         AND (BlackList.reqType = ireqType OR ireqType = 0);
  END;

END castorBW;
