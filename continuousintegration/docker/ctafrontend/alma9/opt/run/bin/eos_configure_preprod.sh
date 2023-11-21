#!/bin/bash

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

PREPROD_DIR=/eos/ctaeos/preprod

eos mkdir ${PREPROD_DIR}
eos chmod 555 ${PREPROD_DIR}
eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+dp,u:poweruser2:rwx+dp,z:'!'u'!'d ${PREPROD_DIR}

eos attr set sys.archive.storage_class=ctaStorageClass ${PREPROD_DIR}

eos attr set sys.workflow.sync::create.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::closew.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::archived.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::archive_failed.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::prepare.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::abort_prepare.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::evict_prepare.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::closew.retrieve_written="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::retrieve_failed.default="proto" ${PREPROD_DIR}
eos attr set sys.workflow.sync::delete.default="proto" ${PREPROD_DIR}
