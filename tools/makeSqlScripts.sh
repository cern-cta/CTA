#!/bin/bash

# check arguments
if [ $# != 3 -a $# != 4 ]; then
  echo usage: $0 component releaseTag targetDir [installDir]
  echo valid components are: castor, repack, vdqm, srm
  echo
  exit
fi

cd $3
if ! [ -f oracleSchema.sql ]; then
  echo oracleSchema.sql not found, exiting
  echo
  exit
fi

if [ $# == 3 ]; then
  rm -rf $1_oracle_create.sql*
  cp oracleSchema.sql $1_oracle_create.sql

  # insert release number
  echo >> $1_oracle_create.sql
  echo "CREATE TABLE CastorVersion (schemaVersion VARCHAR2(20), release VARCHAR2(20));" >> $1_oracle_create.sql
  echo "INSERT INTO CastorVersion VALUES ('-', '"$2"');" >> $1_oracle_create.sql
  echo >> $1_oracle_create.sql

  # append trailers
  if [ -f $1_oracle_create.list ]; then
    for f in `cat $1_oracle_create.list`; do
      cat $f >> $1_oracle_create.sql
    done
  else
    cat oracleTrailer.sql >> $1_oracle_create.sql
  fi

  # remove CVS keywords
  sed 's/\$//g' $1_oracle_create.sql > tmp.sql
  mv tmp.sql $1_oracle_create.sql

  echo Creation scripts for $1 generated with tag $2

else
  # install
  cp $1_oracle_create.sql $4
  echo Creation scripts for $1 installed in $4
fi

