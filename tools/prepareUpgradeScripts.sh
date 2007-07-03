#!/bin/sh

# check arguments
if [ $# != 2 ]; then
  echo usage: $0 previousReleaseTag newReleaseTag
  echo
  exit
fi

# get versions in standard format
ver1=`echo $1 | sed 's/v//' | sed 's/_/\./' | sed 's/_/\./' | sed 's/_/-/g'`
ver2=`echo $2 | sed 's/v//' | sed 's/_/\./' | sed 's/_/\./' | sed 's/_/-/g'`
sqlscript=$ver1
sqlscript+=_to_$ver2.sql

# prepare script
[ -e $sqlscript ] && rm $sqlscript
cat template.sql | sed 's/prevRelease/'$ver1'/g' | sed 's/newRelease/'$ver2'/g' | sed 's/prevRelTag/'$1'/' | sed 's/newRelTag/'$2'/' > $sqlscript
echo $sqlscript skeleton script created. Please append appropriate schema changes and code from oracleTrailer.sql.

