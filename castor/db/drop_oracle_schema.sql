/*******************************************************************
 *
 * @(#)$RCSfile: drop_oracle_schema.sql,v $ $Revision: 1.5 $ $Date: 2009/04/14 14:12:45 $ $Author: waldron $
 *
 * This file drops all defined objects from a database schema.
 *
 * WARNING: This script should be run as the user for which you want
 *          to drop all schema objects. Do not run this as sysdba!!
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *******************************************************************/

DECLARE
  username VARCHAR2(2048);
BEGIN

  -- Purge the recycle bin
  EXECUTE IMMEDIATE 'PURGE RECYCLEBIN';
 
  -- Drop all objects
  FOR rec IN (SELECT object_name, object_type
                FROM user_objects
               ORDER BY object_name, object_type)
  LOOP
    BEGIN
      IF rec.object_type = 'TABLE' THEN
        EXECUTE IMMEDIATE 'DROP TABLE '||rec.object_name||' CASCADE CONSTRAINTS PURGE';
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
      ELSIF rec.object_type = 'MATERIALIZED VIEW' THEN
        EXECUTE IMMEDIATE 'DROP MATERIALIZED VIEW '||rec.object_name;
      ELSIF rec.object_type = 'VIEW' THEN
        EXECUTE IMMEDIATE 'DROP VIEW '||rec.object_name;
      ELSIF rec.object_type = 'JOB' THEN
        DBMS_SCHEDULER.DROP_JOB(JOB_NAME => rec.object_name, FORCE => TRUE);
      END IF;
    EXCEPTION WHEN OTHERS THEN
      -- Ignore: ORA-04043: "object string does not exist" or
      --         ORA-00942: "table or view does not exist" errors
      IF SQLCODE != -04043 AND SQLCODE != -00942 THEN
        RAISE;
      END IF;
    END;
  END LOOP;

  -- This is a DLF or MON based schema so we drop its associated tablespaces
  username := SYS_CONTEXT('USERENV', 'CURRENT_USER');
 
  -- Drop tablespaces
  FOR rec IN (SELECT tablespace_name, status
                FROM user_tablespaces
               WHERE (tablespace_name LIKE CONCAT('DLF_%_', username)
                  OR  tablespace_name LIKE CONCAT('MON_%_', username)))
  LOOP
    IF rec.status = 'ONLINE' THEN
      EXECUTE IMMEDIATE 'ALTER TABLESPACE '||rec.tablespace_name||'
                         OFFLINE';
    END IF;
    EXECUTE IMMEDIATE 'DROP TABLESPACE '||rec.tablespace_name||'
                       INCLUDING CONTENTS AND DATAFILES';
  END LOOP;

  -- Purge the recycle bin
  EXECUTE IMMEDIATE 'PURGE RECYCLEBIN';
END;
/