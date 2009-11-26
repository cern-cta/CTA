/******************************************************************************
 *              dlf_2.1.8-11_to_2.1.9-0.sql
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
 * This script upgrades a CASTOR v2.1.8-11 DLF database into v2.1.9-0
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

/* Stop on errors - this only works from sqlplus */
WHENEVER SQLERROR EXIT FAILURE;

/* Version cross check and update */
DECLARE
  unused VARCHAR(100);
BEGIN
  SELECT release INTO unused FROM dlf_version WHERE release LIKE '2_1_8_11%';
EXCEPTION WHEN NO_DATA_FOUND THEN
  -- Error, we can't apply this script
  raise_application_error(-20000, 'PL/SQL release mismatch. Please run previous upgrade scripts before this one.');
END;
/

UPDATE dlf_version SET schemaVersion = '2_1_9_0', release = '2_1_9_0';
COMMIT;

/* Schema changes go here */
/**************************/

/* Create new sequence */
DECLARE
  seq_start NUMBER;
BEGIN
  SELECT max(seq_no) INTO seq_start FROM DLF_Sequences;
  EXECUTE IMMEDIATE 'CREATE SEQUENCE ids_seq INCREMENT BY 1 START WITH '||seq_start||' CACHE 300';
END;
/

/* Drop old sequence table */
DROP TABLE DLF_Sequences;

/* Drop old monitoring table */
DROP TABLE DLF_Monitoring;

/* Recreate facility list */
TRUNCATE TABLE DLF_Facilities;
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (0,  'rtcpclientd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (1,  'migrator');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (2,  'recaller');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (4,  'rhd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (8,  'gcd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (9,  'schedulerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (10, 'tperrhandler');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (11, 'vdqmd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (13, 'SRMServer');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (14, 'SRMDaemon');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (15, 'repackd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (17, 'taped');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (18, 'rtcpd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (19, 'rmmasterd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (20, 'rmnoded');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (21, 'jobmanagerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (22, 'stagerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (23, 'd2dtransfer');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (24, 'mighunterd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (25, 'rechandlerd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (26, 'stagerjob');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (27, 'aggregatord');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (28, 'rmcd');
INSERT INTO dlf_facilities (fac_no, fac_name) VALUES (29, 'tapegatewayd');

/* Recreate severities list */
TRUNCATE TABLE DLF_Severities;
ALTER TABLE DLF_Severities DROP CONSTRAINT UN_Severities_Sev_No;
ALTER TABLE DLF_Severities DROP CONSTRAINT UN_Severities_Sev_Name;
DROP INDEX UN_Severities_Sev_No;
DROP INDEX UN_Severities_Sev_Name;
CREATE UNIQUE INDEX UN_Severities_Sev_NoName ON dlf_severities (sev_no, sev_name);
ALTER TABLE dlf_severities ADD CONSTRAINT UN_Severities_Sev_NoName UNIQUE (sev_no, sev_name) ENABLE;
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (1,  'Emerg');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (2,  'Alert');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (3,  'Error');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (4,  'Warn');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (5,  'Notice'); /* Auth */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (6,  'Notice'); /* Security */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (7,  'Debug');  /* Usage */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (8,  'Info');   /* System */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (10, 'Info');   /* Monitoring */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (11, 'Debug');
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (12, 'Notice'); /* User Error */
INSERT INTO dlf_severities (sev_no, sev_name) VALUES (13, 'Crit');

COMMIT;


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
