#!/bin/sh
# 
# This script is ran by cmake in order to generate the C++ map that will store all schemas in it.
# Takes as argument the disk emplacement where the .sql files are located
# Example :
#        {
#        {"1.0",
#            {
#                {"oracle","oracle sqlStatements 1.0"},
#                {"sqlite","sqlite sqlStatements 1.0"},
#            }
#        },
#        {"1.1",
#            {
#                {"oracle","oracle sqlStatements 1.1"},
#                {"sqlite","sqlite sqlStatements 1.1"},
#            }
#        },
#        
#    };
#
die() {
  echo "$@" 1>&2
  test -z $TAILPID || kill ${TAILPID} &> /dev/null
  exit 1
}

databaseTypes=('oracle' 'mysql' 'sqlite' 'postgres')
schemaPostfix='_catalogue_schema.sql'
cd $1
tempFilePath="./temp"

trap "rm -f $tempFilePath" EXIT

schemaVersionsDirectories=`find . -type d -regex '^./[0-9]+\.[0-9]+$'`

mapSchemaCode='
{
'
for schemaVersionDir in $schemaVersionsDirectories
do
  #extract schema version
  schemaVersion=${schemaVersionDir#"./"}
  mapSchemaCode+="  {\"$schemaVersion\",
    {
    "
  for databaseType in ${databaseTypes[@]}
  do
    schemaSqlFilePath="$schemaVersionDir/$databaseType$schemaPostfix"
    notTranslatedSchemaSQL=`cat $schemaSqlFilePath` || die "Unable to open file $schemaSqlFilePath"
    schemaSql=`cat $schemaVersionDir/$databaseType$schemaPostfix | sed 's/^/\ \ \"/' | sed 's/$/\"/'`
    mapSchemaCode+="  {\"$databaseType\",$schemaSql
      },
"
  done
  mapSchemaCode+="    }"
  mapSchemaCode+="  },"
done
mapSchemaCode+="};"
echo "$mapSchemaCode" > $tempFilePath
sed "/ALL_SCHEMA_MAP/r $tempFilePath" ./AllCatalogueSchema.hpp.in > ./AllCatalogueSchema.hpp || die "Unable to create the map containing all catalogue schemas"
#awk -v r="$mapSchemaCode" '{gsub(/ALL_SCHEMA_MAP/,r)}1' ./AllCatalogueSchema.hpp.in > ./AllCatalogueSchema.hpp || die "Unable to create the map containing all catalogue schemas"

exit 0