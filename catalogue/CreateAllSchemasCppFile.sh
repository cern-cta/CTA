#!/bin/bash

# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


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
  test -z "$TAILPID" || kill "$TAILPID" >/dev/null 2>&1
  exit 1
}

databaseTypes=('oracle' 'sqlite' 'postgres')
schemaPostfix='_catalogue_schema.sql'
cd "$1/cta-catalogue-schema" || exit
buffFile="./temp"
tempFilePath="../TMPAllCatalogueSchema.hpp"
finalFilePath="../AllCatalogueSchema.hpp"

trap 'rm -f "$buffFile"' EXIT

schemaVersionsDirectories=$(find . -type d -regex '^./[0-9]+\.[0-9]+$' | sort)

mapSchemaCode='
{
'
for schemaVersionDir in $schemaVersionsDirectories; do
  #extract schema version
  schemaVersion=${schemaVersionDir#"./"}
  mapSchemaCode+="  {\"$schemaVersion\",
    {
    "
  for databaseType in "${databaseTypes[@]}"; do
    schemaSqlFilePath="$schemaVersionDir/$databaseType$schemaPostfix"
    [ -r "$schemaSqlFilePath" ] || die "Unable to open file $schemaSqlFilePath"
    schemaSql=$(cat "$schemaSqlFilePath" | sed 's/^/\ \ \"/' | sed 's/$/\"/')
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
sed "/ALL_SCHEMA_MAP/r $buffFile" ../AllCatalogueSchema.hpp.in > $tempFilePath || die "Unable to create the map containing all catalogue schemas"
#awk -v r="$mapSchemaCode" '{gsub(/ALL_SCHEMA_MAP/,r)}1' ./AllCatalogueSchema.hpp.in > ./AllCatalogueSchema.hpp || die "Unable to create the map containing all catalogue schemas"

# Compare the temporary output file with the existing output file to avoid regenerating it if it hasn't changed
if ! cmp -s $tempFilePath $finalFilePath; then
  mv $tempFilePath $finalFilePath
else
  rm $tempFilePath
fi

exit 0
