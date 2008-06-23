#!/bin/sh

# guess version
ver=`egrep "^castor" debian/changelog | head -1 | awk '{print $2}' | sed 's/)//' | sed 's/(//'`
tag=`echo $ver | sed 's/\./_/g' | sed 's/-/_/g'`

# generate creation scripts for the db-based CASTOR components
# if $1 != NULL, it is interpreted as destination directory where to install the generated script
tools/makeSqlScripts.sh castor castor/db $tag $1
tools/makeSqlScripts.sh repack castor/repack $tag $1
tools/makeSqlScripts.sh vdqm castor/vdqm $tag $1
cd dlf/scripts
./makeDlfScripts.sh $tag oracle/oracleCreate.sql oracle/dlf_oracle_create.sql $1
cd ../..

# commit and tag in CVS
sqlscripts='castor/db/castor_oracle_create.sql castor/db/castor_oracle_create.sqlplus castor/repack/repack_oracle_create.sql castor/repack/repack_oracle_create.sqlplus castor/vdqm/vdqm_oracle_create.sql castor/vdqm/vdqm_oracle_create.sqlplus dlf/scripts/oracle/dlf_oracle_create.sql dlf/scripts/oracle/dlf_oracle_create.sqlplus'
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
 
