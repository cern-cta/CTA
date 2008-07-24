-- This script should be ran using:
--     sqlplus /nolog @oracleRecreateDb.sqlplus

whenever oserror exit failure
whenever sqlerror exit failure rollback

PROMPT Login as the VDQM schema owner
ACCEPT vdqm_conn char PROMPT 'Enter connection string (user@tns): '
CONNECT &vdqm_conn

-- Drop all the exisiting database objects
@oracleDropAllObjects.sqlplus

-- Create the new schema
@vdqm_oracle_create.sqlplus

-- Grant writer account priviledges
-- Note that oracleWriterGrants.sqlplus assumes the writer account name is:
--    vdqm_writer
-- Replace this account name if the writer account is different
@oracleWriterGrants.sqlplus

PROMPT Login as the VDQM writer
ACCEPT vdqm_writer_conn char PROMPT 'Enter VDQM writer connection string (user@tns): '
CONNECT &vdqm_writer_conn

-- Drop all the exisiting database objects
@oracleDropAllObjects.sqlplus

-- Create the writer synonyms
@oracleWriterSynonyms.sqlplus

-- Note that vdqmDBInit uses the database connection details given in:
--     /etc/castor/ORAVDQMCONFIG
-- These connection details should be those of the writer account.
!$CASTOR_CVS/hsmtools/vdqmDBInit
