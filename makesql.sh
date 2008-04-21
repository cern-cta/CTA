#!/bin/sh

# guess version
ver=`egrep "^castor" debian/changelog | head -1 | awk '{print $2}' | sed 's/)//' | sed 's/(//'`
tag=`echo $ver | sed 's/\./_/g' | sed 's/-/_/g'`

# generate creation scripts for the db-based CASTOR components
tools/makeSqlScripts.sh castor castor/db $tag
tools/makeSqlScripts.sh repack castor/repack $tag
tools/makeSqlScripts.sh vdqm castor/vdqm $tag
cd dlf/scripts
./makeDlfScripts.sh $tag oracle/oracleCreate.sql oracle/dlf_oracle_create.sql
echo Creation scripts for dlf generated with tag $tag
cd ../..

# commit and tag in CVS
sqlscripts='castor/db/castor_oracle_create.sql castor/db/castor_oracle_create.sqlplus castor/repack/repack_oracle_create.sql castor/repack/repack_oracle_create.sqlplus dlf/scripts/oracle/dlf_oracle_create.sql dlf/scripts/oracle/dlf_oracle_create.sqlplus'
echo Running CVS diff...
cvs diff $sqlscripts > /tmp/cvsdiffs
less /tmp/cvsdiffs
echo 'Commit and tag in CVS? (y/n)'
read ask
if [ "$ask" == "y" ]; then
  cvs commit -m "Regenerated creation scripts" $sqlscripts
  cvs tag v$tag $sqlscripts
  echo All SQL creation scripts for release $ver generated and tagged
fi
rm -rf /tmp/cvsdiffs
echo

