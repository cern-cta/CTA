-- This script should be ran using:
--     sqlplus /nolog @oracleRecreatedb.sqlplus

whenever oserror exit failure
whenever sqlerror exit failure rollback

prompt Login as the VDQM schema owner
-- Replace connection string if the schema owner account is different
connect vdqm@castordev64

-- Drop all the exisiting database objects
@oracleDropAllObjects.sqlplus

-- Create the new schema
@vdqm_oracle_create.sqlplus

-- Grant writer account priviledges
-- Note that oracleWriterGrants.sqlplus assumes the writer account name is:
--    vdqm_writer
-- Replace this account name if the writer account is different
@oracleWriterGrants.sqlplus

prompt Login as the VDQM writer
-- Replace connection string if the writer account is different
connect vdqm_writer@castordev64

-- Drop all the exisiting database objects
@oracleDropAllObjects.sqlplus

-- Create the writer synonyms
@oracleWriterSynonyms.sqlplus

-- Note that vdqmDBInit uses the database connection details given in:
--     /etc/castor/ORAVDQMCONFIG
-- These connection details should be those of the writer account.
!$CASTOR_CVS/hsmtools/vdqmDBInit
