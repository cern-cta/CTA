#!/bin/sh

# check arguments
if [ $# != 3 ]; then
  echo usage: $0 component targetDir releaseTag
  echo supported components are: castor, repack, srm
  echo
  exit
fi
cd $2
if ! [ -f oracleSchema.sql ]; then
  echo oracleSchema.sql not found, exiting
  echo
  exit
fi
rm -rf $1_oracle_create.sql*
cp oracleSchema.sql $1_oracle_create.sql

# insert release number
echo >> $1_oracle_create.sql
echo "CREATE TABLE CastorVersion (schemaVersion VARCHAR2(20), release VARCHAR2(20));" >> $1_oracle_create.sql
echo "INSERT INTO CastorVersion VALUES ('-', '"$3"');" >> $1_oracle_create.sql
echo >> $1_oracle_create.sql

# append trailers
if [ -f $1_oracle_create.sql ]; then
  for f in `cat $1_oracle_create.list`; do
    cat $f >> $1_oracle_create.sql
  done
else
  cat oracleTrailer.sql >> $1_oracle_create.sql
fi

# remove CVS keywords
sed 's/\$//g' $1_oracle_create.sql > ora.sql
mv ora.sql $1_oracle_create.sql

# generate sqlplus version
sed 's/^END;/END;\n\//' $1_oracle_create.sql | sed 's/^\(END castor[a-zA-Z]*;\)/\1\n\//' | sed 's/\(CREATE OR REPLACE TYPE .*;\)$/\1\n\//'  | sed 's/^);$/);\n\//' > $1_oracle_create.sqlplus

echo Creation scripts for $1 generated with tag $3

