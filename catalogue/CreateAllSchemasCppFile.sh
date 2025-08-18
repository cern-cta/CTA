#!/bin/sh

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.


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

catalogue_dir="$(dirname ${BASH_SOURCE[0]})"

databaseTypes=('oracle' 'sqlite' 'postgres')
schemaPostfix='_catalogue_schema.sql'
cd $1
buffFile="./temp"
tempFilePath="$catalogue_dir/TMPAllCatalogueSchema.hpp"
finalFilePath="$catalogue_dir/AllCatalogueSchema.hpp"

trap "rm -f $buffFile" EXIT

schemaVersionsDirectories=$(find . -type d -regex '^./[0-9]+\.[0-9]+$' | sort)

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
    notTranslatedSchemaSQL=$(cat $schemaSqlFilePath) || die "Unable to open file $schemaSqlFilePath"
    schemaSql=$(cat $schemaVersionDir/$databaseType$schemaPostfix | sed 's/^/\ \ \"/' | sed 's/$/\"/')
    mapSchemaCode+="  {\"$databaseType\",$schemaSql
      },
"
  done
  mapSchemaCode+="    }"
  mapSchemaCode+="  },
"
done
mapSchemaCode+="};"
echo "$mapSchemaCode" > $buffFile
sed "/ALL_SCHEMA_MAP/r $buffFile" "$catalogue_dir/AllCatalogueSchema.hpp.in" > $tempFilePath || die "Unable to create the map containing all catalogue schemas"
#awk -v r="$mapSchemaCode" '{gsub(/ALL_SCHEMA_MAP/,r)}1' ./AllCatalogueSchema.hpp.in > ./AllCatalogueSchema.hpp || die "Unable to create the map containing all catalogue schemas"

# Compare the temporary output file with the existing output file to avoid regenerating it if it hasn't changed
if ! cmp -s $tempFilePath $finalFilePath; then
  mv $tempFilePath $finalFilePath
else
  rm $tempFilePath
fi

exit 0
