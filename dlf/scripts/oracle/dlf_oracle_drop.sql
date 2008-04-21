/******************************************************************************
 *              dlf_oracle_drop.sql
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
 * @(#)$RCSfile: dlf_oracle_drop.sql,v $ $Release: 1.2 $ $Release$ $Date: 2008/04/21 12:08:21 $ $Author: waldron $
 *
 * This script drops a DLF schema
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

DECLARE
  username VARCHAR2(2048);
BEGIN

  -- Purge the recycle bin
  EXECUTE IMMEDIATE 'PURGE RECYCLEBIN';

  -- Drop all objects (ignore monitoring ones!)
  FOR rec IN (SELECT object_name, object_type FROM user_objects
              WHERE  object_name NOT LIKE 'PROC_%'
              AND    object_name NOT LIKE 'MONITORING_%'
              ORDER BY object_name, object_type)
  LOOP
    IF rec.object_type = 'TABLE' THEN
      EXECUTE IMMEDIATE 'DROP TABLE '||rec.object_name||' CASCADE CONSTRAINTS';
    ELSIF rec.object_type = 'PROCEDURE' THEN
      EXECUTE IMMEDIATE 'DROP PROCEDURE '||rec.object_name;
    ELSIF rec.object_type = 'FUNCTION' THEN
      EXECUTE IMMEDIATE 'DROP FUNCTION '||rec.object_name;
    ELSIF rec.object_type = 'PACKAGE' THEN
      EXECUTE IMMEDIATE 'DROP PACKAGE '||rec.object_name;
    ELSIF rec.object_type = 'SEQUENCE' THEN
      EXECUTE IMMEDIATE 'DROP SEQUENCE '||rec.object_name;
    ELSIF rec.object_type = 'TYPE' THEN
      EXECUTE IMMEDIATE 'DROP TYPE "'||rec.object_name||'" FORCE';
    ELSIF rec.object_type = 'JOB' THEN
      DBMS_SCHEDULER.DROP_JOB(JOB_NAME => rec.object_name, FORCE => TRUE);
    END IF;
  END LOOP;

  -- Extract the name of the current user running the PLSQL procedure.
  SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER') 
    INTO username
    FROM dual;

  -- Drop tablespaces
  FOR rec IN (SELECT tablespace_name
                FROM user_tablespaces
               WHERE tablespace_name LIKE CONCAT('DLF_%_', username))
  LOOP
    EXECUTE IMMEDIATE 'ALTER TABLESPACE '||rec.tablespace_name||' OFFLINE';
    EXECUTE IMMEDIATE 'DROP TABLESPACE '||rec.tablespace_name||'
                       INCLUDING CONTENTS AND DATAFILES';
  END LOOP;

  -- Purge the recycle bin
  EXECUTE IMMEDIATE 'PURGE RECYCLEBIN';
END;


/* End-of-File */
