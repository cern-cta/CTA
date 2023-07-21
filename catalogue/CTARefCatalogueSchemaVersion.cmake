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

# ========================================
# Instructions
# ========================================

# For catalogue releases:
#  - Set the new CTA catalogue schema versions in CTA_CATALOGUE_REF_SCHEMA_VERSION_CURR
#  - Set the previous CTA catalogue schema versions - as a list - in CTA_CATALOGUE_REF_SCHEMA_VERSION_PREV
#  - There must exist a migration script from every previous version to the new one
#  - Example:
#     set(CTA_CATALOGUE_REF_SCHEMA_VERSION_CURR 13)
#     set(CTA_CATALOGUE_REF_SCHEMA_VERSION_PREV 11 12)

# For non-catalogue releases (default):
#  - Current and previous CTA catalogue schema versions must be the same
#  - Example:
#     set(CTA_CATALOGUE_REF_SCHEMA_VERSION_CURR 13)
#     set(CTA_CATALOGUE_REF_SCHEMA_VERSION_PREV 13)

set(CTA_CATALOGUE_REF_SCHEMA_VERSION_CURR 13)
set(CTA_CATALOGUE_REF_SCHEMA_VERSION_PREV 13)
