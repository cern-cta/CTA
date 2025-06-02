/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */
#include "version.h"

#pragma once

namespace cta {

const int CA_MAXVIDLEN = 6;  // maximum length for a VID
const int CTA_SCHEMA_VERSION_MAJOR = CTA_CATALOGUE_SCHEMA_VERSION_MAJOR;
const int CTA_SCHEMA_VERSION_MINOR = CTA_CATALOGUE_SCHEMA_VERSION_MINOR;
const int TAPE_LABEL_UNITREADY_TIMEOUT = 300;

}  // namespace cta
