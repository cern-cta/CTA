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

#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CancelRetrieveRequest::CancelRetrieveRequest():
  archiveFileID(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool CancelRetrieveRequest::operator==(const CancelRetrieveRequest &rhs) const {
  return requester==rhs.requester
      && archiveFileID==rhs.archiveFileID
      && dstURL==rhs.dstURL
      && diskFileInfo==rhs.diskFileInfo;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool CancelRetrieveRequest::operator!=(const CancelRetrieveRequest &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const CancelRetrieveRequest &obj) {
  os << "(requester=" << obj.requester
     << " archiveFileID=" << obj.archiveFileID
     << " dstURL=" << obj.dstURL
     << " diskFileInfo=" << obj.diskFileInfo << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
