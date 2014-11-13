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
 * This script creates a new Castor User Privilege Validator schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* SQL statement for User Privilege table */
CREATE TABLE USER_PRIVILEGE
  (U_ID NUMBER CONSTRAINT NN_UserPrivilege_UID NOT NULL,
   G_ID NUMBER(6) CONSTRAINT NN_UserPrivilege_GID NOT NULL,
   SRC_HOST VARCHAR2(63) CONSTRAINT NN_UserPrivilege_SrcHost NOT NULL,
   TGT_HOST VARCHAR2(63) CONSTRAINT NN_UserPrivilege_TgtHost NOT NULL,
   PRIV_CAT NUMBER(6) CONSTRAINT NN_UserPrivilege_PrivCat NOT NULL);

ALTER TABLE USER_PRIVILEGE
  ADD CONSTRAINT usr_priv_uk UNIQUE (U_ID, G_ID, SRC_HOST, TGT_HOST, PRIV_CAT);

/* SQL statements for table UpgradeLog */
CREATE TABLE UpgradeLog (Username VARCHAR2(64) DEFAULT sys_context('USERENV', 'OS_USER') CONSTRAINT NN_UpgradeLog_Username NOT NULL, SchemaName VARCHAR2(64) DEFAULT 'CUPV' CONSTRAINT NN_UpgradeLog_SchemaName NOT NULL, Machine VARCHAR2(64) DEFAULT sys_context('USERENV', 'HOST') CONSTRAINT NN_UpgradeLog_Machine NOT NULL, Program VARCHAR2(48) DEFAULT sys_context('USERENV', 'MODULE') CONSTRAINT NN_UpgradeLog_Program NOT NULL, StartDate TIMESTAMP(6) WITH TIME ZONE DEFAULT systimestamp, EndDate TIMESTAMP(6) WITH TIME ZONE, FailureCount NUMBER DEFAULT 0, Type VARCHAR2(20) DEFAULT 'NON TRANSPARENT', State VARCHAR2(20) DEFAULT 'INCOMPLETE', SchemaVersion VARCHAR2(20) CONSTRAINT NN_UpgradeLog_SchemaVersion NOT NULL, Release VARCHAR2(20) CONSTRAINT NN_UpgradeLog_Release NOT NULL);

/* SQL statements for check constraints on the UpgradeLog table */
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_State
  CHECK (state IN ('COMPLETE', 'INCOMPLETE'));
  
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_Type
  CHECK (type IN ('TRANSPARENT', 'NON TRANSPARENT'));

/* SQL statement to populate the intial release value */
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('-', '2_1_14_15');

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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* SQL statement to populate the intial schema version */
UPDATE UpgradeLog SET schemaVersion = '2_1_9_3'
 WHERE startDate = (SELECT max(startDate) FROM UpgradeLog);

/* useful procedure to recompile all invalid items in the DB
   as many times as needed, until nothing can be improved anymore.
   Also reports the list of invalid items if any */
CREATE OR REPLACE PROCEDURE recompileAll AS
  varNbInvalids INTEGER;
  varNbInvalidsLastRun INTEGER := -1;
BEGIN
  WHILE varNbInvalidsLastRun != 0 LOOP
    varNbInvalids := 0;
    FOR a IN (SELECT object_name, object_type
                FROM user_objects
               WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY')
                 AND status = 'INVALID')
    LOOP
      IF a.object_type = 'PACKAGE BODY' THEN a.object_type := 'PACKAGE'; END IF;
      BEGIN
        EXECUTE IMMEDIATE 'ALTER ' ||a.object_type||' '||a.object_name||' COMPILE';
      EXCEPTION WHEN OTHERS THEN
        -- ignore, so that we continue compiling the other invalid items
        NULL;
      END;
    END LOOP;
    -- check how many invalids are still around
    SELECT count(*) INTO varNbInvalids FROM user_objects
     WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY') AND status = 'INVALID';
    -- should we give up ?
    IF varNbInvalids = varNbInvalidsLastRun THEN
      DECLARE
        varInvalidItems VARCHAR(2048);
      BEGIN
        -- yes, as we did not move forward on this run
        SELECT LISTAGG(object_name, ', ') WITHIN GROUP (ORDER BY object_name) INTO varInvalidItems
          FROM user_objects
         WHERE object_type IN ('PROCEDURE', 'TRIGGER', 'FUNCTION', 'VIEW', 'PACKAGE BODY') AND status = 'INVALID';
        raise_application_error(-20000, 'Revalidation of PL/SQL code failed. Still ' ||
                                        varNbInvalids || ' invalid items : ' || varInvalidItems);
      END;
    END IF;
    -- prepare for next loop
    varNbInvalidsLastRun := varNbInvalids;
    varNbInvalids := 0;
  END LOOP;
END;
/

/* Flag the schema creation as COMPLETE */
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE';
COMMIT;
