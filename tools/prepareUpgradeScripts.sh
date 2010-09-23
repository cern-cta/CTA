#!/bin/sh

# check arguments
if [ $# != 3 ]; then
  echo usage: $0 component previousReleaseTag newReleaseTag
  echo valid components are: stager, dlf, repack, vdqm, cns, vmgr, cupv, mon, srm.
  echo
  exit
fi

# get versions in standard format
dbver1=`echo $2 | sed 's/v//'`
dbver2=`echo $3 | sed 's/v//'`
ver1=`echo $dbver1 | sed 's/_/\./' | sed 's/_/\./' | sed 's/_/-/g'`
ver2=`echo $dbver2 | sed 's/_/\./' | sed 's/_/\./' | sed 's/_/-/g'`
sqlscript=`echo $1_$ver1''_to_$ver2''.sql`

# prepare script
schemaname=`echo $1 | tr '[a-z]' '[A-Z]'`
[ -e $sqlscript ] && rm $sqlscript
cat template.sql | sed 's/component/'$1'/' | sed 's/SCHEMANAME/'$schemaname'/g' | sed 's/prevRelease/'$ver1'/g' | sed 's/newRelease/'$ver2'/g' | sed 's/prevRelTag/'$dbver1'/' | sed 's/newRelTag/'$dbver2'/' > $sqlscript

echo $sqlscript skeleton script created. Please insert schema and PL/SQL changes in the appropriate sections,
echo and replace schemaTag with the appropriate db schema version.
