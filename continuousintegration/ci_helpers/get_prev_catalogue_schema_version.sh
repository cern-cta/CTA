#!/bin/bash

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2024 CERN
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Paths relative to the script's location
catalogue_major_ver=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MAJOR "$SCRIPT_DIR/../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake" | sed 's/[^0-9]*//g')
catalogue_minor_ver=$(grep CTA_CATALOGUE_SCHEMA_VERSION_MINOR "$SCRIPT_DIR/../../catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake" | sed 's/[^0-9]*//g')
catalogue_schema_version="$catalogue_major_ver.$catalogue_minor_ver"
migration_files=$(find "$SCRIPT_DIR/../../catalogue/cta-catalogue-schema" -name "*to${catalogue_schema_version}.sql")
prev_catalogue_schema_version=$(echo "$migration_files" | grep -o -E '[0-9]+\.[0-9]' | head -1)

echo "$prev_catalogue_schema_version"
