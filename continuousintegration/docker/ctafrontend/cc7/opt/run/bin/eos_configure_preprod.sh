#!/bin/bash

# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

PREPROD_DIR=/eos/ctaeos/preprod

eos mkdir ${PREPROD_DIR}
eos chmod 555 ${PREPROD_DIR}
eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+dp,u:poweruser2:rwx+dp,z:'!'u,u:root:+u ${PREPROD_DIR}

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
