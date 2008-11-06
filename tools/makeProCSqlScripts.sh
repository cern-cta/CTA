#!/bin/sh

# check arguments
if [ $# != 3 -a $# != 4 ]; then
  echo usage: $0 component releaseTag targetDir [installDir]
  echo valid components are: dlf, cns
  echo
  exit
fi

cd $3
if ! [ -f oracleCreate.sql ]; then
  echo oracleCreate.sql not found, exiting
  echo
  exit
fi

if [ $# == 3 ]; then
  rm -rf $1_oracle_create.sql

  # insert release number
  sed 's/releaseTag/'$2'/' oracleCreate.sql > $1_oracle_create.sql

  # remove CVS keywords
  sed 's/\$//g' $1_oracle_create.sql > tmp.sql
  mv tmp.sql $1_oracle_create.sql

  echo Creation script for $1 generated with tag $2

else
  # install
  cp $1_oracle_create.sql $4
  echo Creation script for $1 installed in $4
fi

