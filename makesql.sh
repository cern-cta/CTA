#!/bin/sh

# guess version
ver=`egrep "^castor" debian/changelog | head -1 | awk '{print $2}' | sed 's/)//' | sed 's/(//'`
tag=`echo $ver | sed 's/\./_/g' | sed 's/-/_/g'`

# generate creation scripts for the db-based CASTOR components
tools/makeSqlScripts.sh castor castor/db $tag
tools/makeSqlScripts.sh repack castor/repack $tag
cd dlf/scripts
./makeDlfScripts.sh $tag oracle/oracleCreate.sql oracle/dlf_oracle_create.sql
./makeDlfScripts.sh $tag oracle/express/oracleCreate.sql oracle/express/dlf_oracle_create.sql
./makeDlfScripts.sh $tag mysql/mysqlCreate.sql mysql/dlf_mysql_create.sql
echo Creation scripts for dlf generated with tag $tag
cd ../..

# commit and tag in CVS
cvs commit -m "Regenerated creation scripts" castor/db/castor_oracle_create.sql castor/db/castor_oracle_create.sqlplus castor/repack/repack_oracle_create.sql castor/repack/repack_oracle_create.sqlplus dlf/scripts
cvs tag v$tag castor/db/castor_oracle_create.sql castor/db/castor_oracle_create.sqlplus castor/repack/repack_oracle_create.sql castor/repack/repack_oracle_create.sqlplus dlf/scripts

echo All SQL creation scripts for release $ver generated and tagged
echo

