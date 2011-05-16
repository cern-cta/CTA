/******************************************************************************
 *              grant_oracle_user.sql
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
 * @(#)$RCSfile: grant_oracle_user.sql,v $ $Release: 1.2 $ $Release$ $Date: 2009/03/06 15:05:48 $ $Author: waldron $
 *
 * This script grants SELECT rights on all tables and views within a database
 * schema to a given user.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Determine the user to grant SELECT rights on all tables and views */
UNDEF username
ACCEPT username DEFAULT castor_read PROMPT 'Grant SELECT rights on all tables and views to user: (castor_read) '
SET VER OFF

DECLARE
  unused VARCHAR2(2048);
BEGIN
  -- Check that the user exists
  BEGIN
    SELECT username INTO unused
      FROM all_users
     WHERE username = upper('&username');
  EXCEPTION WHEN NO_DATA_FOUND THEN
    raise_application_error(-20000, 'User &username does not exist');
  END;

  -- Grant select on all tables, views and materialized views
  FOR a IN (SELECT table_name FROM user_tables WHERE temporary = 'N'
             UNION ALL
            SELECT view_name FROM user_views
             UNION ALL
            SELECT mview_name FROM user_mviews)
  LOOP
    IF a.table_name IN ('DLF_CONFIG', 'DLF_MONITORING', 'CASTORVERSION') THEN
      EXECUTE IMMEDIATE 'GRANT SELECT ON '||a.table_name||' TO &username WITH GRANT OPTION';
    ELSE
      EXECUTE IMMEDIATE 'GRANT SELECT ON '||a.table_name||' TO &username';
    END IF;
  END LOOP;
END;
/

/* End-of-File */
