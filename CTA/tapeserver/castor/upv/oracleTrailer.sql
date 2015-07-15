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
