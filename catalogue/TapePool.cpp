/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include "catalogue/TapePool.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapePool::TapePool():
  nbPartialTapes(0),
  encryption(false),
  nbTapes(0),
  nbEmptyTapes(0),
  nbDisabledTapes(0),
  nbFullTapes(0),
  nbReadOnlyTapes(0),
  nbWritableTapes(0),
  capacityBytes(0),
  dataBytes(0),
  nbPhysicalFiles(0) {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapePool::operator==(const TapePool &rhs) const {
  return name == rhs.name;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool TapePool::operator!=(const TapePool &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const TapePool &obj) {
  os << "(name=" << obj.name
     << " vo=" << obj.vo.name
     << " nbPartialTapes=" << obj.nbPartialTapes
     << " encryption=" << obj.encryption
     << " nbTapes=" << obj.nbTapes
     << " nbEmptyTapes=" << obj.nbEmptyTapes
     << " nbDisabledTapes=" << obj.nbDisabledTapes
     << " nbFullTapes=" << obj.nbFullTapes
     << " nbReadOnlyTapes=" << obj.nbReadOnlyTapes
     << " capacityBytes=" << obj.capacityBytes
     << " dataBytes=" << obj.dataBytes
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment << ")";
  return os;
}

} // namespace catalogue
} // namespace cta
