-- This script should be ran using:
--     sqlplus /nolog @oracleRecreateDb.sql

whenever oserror exit failure
whenever sqlerror exit failure rollback

PROMPT Login as the VDQM schema owner
ACCEPT vdqm_conn char PROMPT 'Enter connection string (user@tns): '
CONNECT &vdqm_conn

-- Drop all the exisiting database objects
@oracleDropAllObjects.sql

-- Create the new schema
@vdqm_oracle_create.sql

-- Note that vdqmDBInit uses the database connection details given in:
--     /etc/castor/ORAVDQMCONFIG
!vdqmDBInit
