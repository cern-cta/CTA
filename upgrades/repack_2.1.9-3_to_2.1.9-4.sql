/******************************************************************************
 *              repack_2.1.9-3_to_2.1.9-4.sql
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
 * This script upgrades a CASTOR v2.1.9-3 REPACK database to v2.1.9-4
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE
DECLARE
  unused VARCHAR2(30);
BEGIN
  UPDATE UpgradeLog
     SET failureCount = failureCount + 1
   WHERE schemaVersion = '2_1_8_0'
     AND release = '2_1_9_4'
     AND state != 'COMPLETE';
  COMMIT;
EXCEPTION WHEN NO_DATA_FOUND THEN
  NULL;
END;
/

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM CastorVersion
   WHERE release LIKE '2_1_9_3%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('2_1_8_0', '2_1_9_4');
UPDATE UpgradeLog SET type = 'TRANSPARENT';
COMMIT;


/* Update and revalidation of PL-SQL code */
/******************************************/

/* PL/SQL method implementing resurrectTapesOnHold */

CREATE OR REPLACE
PROCEDURE resurrectTapesOnHold (maxFiles IN INTEGER, maxTapes IN INTEGER)AS
filesOnGoing INTEGER;
tapesOnGoing INTEGER;
newFiles NUMBER;
BEGIN
	SELECT count(vid), sum(filesStaging) + sum(filesMigrating) INTO  tapesOnGoing, filesOnGoing FROM RepackSubrequest WHERE  status IN (1,2); -- TOBESTAGED ONGOING
  IF filesongoing IS NULL THEN
   filesongoing:=0;
  END IF;
-- Set the subrequest to TOBESTAGED FROM ON-HOLD if there is no ongoing repack for any of the files on the tape
  FOR sr IN (SELECT RepackSubRequest.id FROM RepackSubRequest,RepackRequest WHERE  RepackRequest.id=RepackSubrequest.repackrequest AND RepackSubRequest.status=9 ORDER BY RepackRequest.creationTime ) LOOP
			UPDATE RepackSubRequest SET status=1 WHERE id=sr.id AND status=9
			AND filesOnGoing + files <= maxFiles AND tapesOnGoing+1 <= maxTapes
			AND NOT EXISTS (SELECT 'x' FROM RepackSegment WHERE
				RepackSegment.RepackSubRequest=sr.id AND
				RepackSegment.fileid IN (SELECT DISTINCT RepackSegment.fileid FROM RepackSegment
			             WHERE RepackSegment.RepackSubrequest
			             	IN (SELECT RepackSubRequest.id FROM RepackSubRequest WHERE RepackSubRequest.id<>sr.id AND RepackSubRequest.status NOT IN (4,5,8,9)
			             	 )
				)
			) RETURNING files INTO newFiles; -- FINISHED ARCHIVED FAILED ONHOLD
      IF newFiles IS NOT NULL THEN
        filesOnGoing:=filesOnGoing+newFiles;
        tapesOnGoing:=tapesOnGoing+1;
      END IF;
      COMMIT;
   END LOOP;
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
UPDATE UpgradeLog SET endDate = sysdate, state = 'COMPLETE'
 WHERE release = '2_1_9_4';
COMMIT;
