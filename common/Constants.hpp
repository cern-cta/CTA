/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "version.h"

#pragma once

namespace cta {

const int CA_MAXVIDLEN = 6; // maximum length for a VID
const int CTA_SCHEMA_VERSION_MAJOR = CTA_CATALOGUE_SCHEMA_VERSION_MAJOR;
const int CTA_SCHEMA_VERSION_MINOR = CTA_CATALOGUE_SCHEMA_VERSION_MINOR;
const int TAPE_LABEL_UNITREADY_TIMEOUT = 300;

} // namespace cta


