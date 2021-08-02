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

#include "common/dataStructures/MountType.hpp"

namespace cta {
namespace common {
namespace dataStructures {

std::string toString(cta::common::dataStructures::MountType type) {
  switch(type) {
    case MountType::ArchiveForUser:
      return "ArchiveForUser";
    case MountType::ArchiveForRepack:
      return "ArchiveForRepack";
    case MountType::ArchiveAllTypes:
      return "ArchiveAllTypes";
    case MountType::Retrieve:
      return "Retrieve";
    case MountType::Label:
      return "Label";
    case MountType::NoMount:
      return "-";
    default:
      return "UNKNOWN";
  }
}

MountType getMountBasicType(MountType type) {
  switch(type) {
    case MountType::ArchiveForUser:
    case MountType::ArchiveForRepack:
      return MountType::ArchiveAllTypes;
    default:
      return type;
  }
}

std::ostream & operator<<(std::ostream &os, 
  const cta::common::dataStructures::MountType &obj) {
  return os << toString(obj);
}

}}} // namespace cta::common::dataStructures
