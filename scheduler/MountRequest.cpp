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

#include "scheduler/MountRequest.hpp"

//------------------------------------------------------------------------------
// transferTypeToStr
//------------------------------------------------------------------------------
const char *cta::MountRequest::TapeJobTypeToStr(const EntryType enumValue)
  throw() {
  switch(enumValue) {
  case TRANSFERTYPE_NONE     : return "NONE";
  case TRANSFERTYPE_ARCHIVE  : return "ARCHIVE";
  case TRANSFERTYPE_RETRIEVE : return "RETRIEVE";
  default                    : return "UNKNOWN";
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MountRequest::MountRequest(): m_transferType(TRANSFERTYPE_NONE) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MountRequest::MountRequest(
  const std::string &mountId,
  const std::string &vid,
  const TransferType transferType):
  mountId(mountId),
  vid(vid),
  transferType(transferType) {
}
