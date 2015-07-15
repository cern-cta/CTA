#!/bin/bash

# check arguments
if [ $# != 3 -a $# != 4 ]; then
  echo usage: $0 component releaseTag targetDir [installDir]
  echo valid components are: stager, repack, vdqm, cns, vmgr, cupv, srm.
  echo
  exit
fi

cd $3
if ! [ -f oracleSchema.sql ]; then
  echo oracleSchema.sql not found, exiting
  echo
  exit
fi

# Construct the database schema name in uppercase
schemaname=`echo $1 | tr '[a-z]' '[A-Z]'`

if [ $# == 4 ]; then
  destdir=`readlink -f $4`
  destdir=${destdir}/
else
  destdir=""
fi

rm -rf ${destdir}$1_oracle_create.sql*

# force the SQL client to exit on errors
/bin/cat > ${destdir}$1_oracle_create.sql <<EOF
/* Stop on errors */
WHENEVER SQLERROR EXIT FAILURE;

EOF

if [ -f oracleHeader.sql ]; then
  cat oracleHeader.sql >> ${destdir}$1_oracle_create.sql
fi

cat oracleSchema.sql >> ${destdir}$1_oracle_create.sql

# insert release number
/bin/cat >> ${destdir}$1_oracle_create.sql <<EOF

/* SQL statements for table UpgradeLog */
CREATE TABLE UpgradeLog (Username VARCHAR2(64) DEFAULT sys_context('USERENV', 'OS_USER') CONSTRAINT NN_UpgradeLog_Username NOT NULL, SchemaName VARCHAR2(64) DEFAULT '$schemaname' CONSTRAINT NN_UpgradeLog_SchemaName NOT NULL, Machine VARCHAR2(64) DEFAULT sys_context('USERENV', 'HOST') CONSTRAINT NN_UpgradeLog_Machine NOT NULL, Program VARCHAR2(48) DEFAULT sys_context('USERENV', 'MODULE') CONSTRAINT NN_UpgradeLog_Program NOT NULL, StartDate TIMESTAMP(6) WITH TIME ZONE DEFAULT systimestamp, EndDate TIMESTAMP(6) WITH TIME ZONE, FailureCount NUMBER DEFAULT 0, Type VARCHAR2(20) DEFAULT 'NON TRANSPARENT', State VARCHAR2(20) DEFAULT 'INCOMPLETE', SchemaVersion VARCHAR2(20) CONSTRAINT NN_UpgradeLog_SchemaVersion NOT NULL, Release VARCHAR2(20) CONSTRAINT NN_UpgradeLog_Release NOT NULL);

/* SQL statements for check constraints on the UpgradeLog table */
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_State
  CHECK (state IN ('COMPLETE', 'INCOMPLETE'));
  
ALTER TABLE UpgradeLog
  ADD CONSTRAINT CK_UpgradeLog_Type
  CHECK (type IN ('TRANSPARENT', 'NON TRANSPARENT'));

/* SQL statement to populate the intial release value */
INSERT INTO UpgradeLog (schemaVersion, release) VALUES ('-', '$2');

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

EOF

# append trailers
if [ -f $1_oracle_create.list ]; then
  for f in `cat $1_oracle_create.list`; do
    cat $f >> ${destdir}$1_oracle_create.sql
  done
else
  cat oracleTrailer.sql >> ${destdir}$1_oracle_create.sql
fi

# flag creation script completion
/bin/cat >> ${destdir}$1_oracle_create.sql <<EOF

/* Flag the schema creation as COMPLETE */
UPDATE UpgradeLog SET endDate = systimestamp, state = 'COMPLETE';
COMMIT;
EOF

echo "Creation script for $1 (${destdir}$1_oracle_create.sql) generated with tag $2"

