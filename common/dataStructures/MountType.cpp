/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/dataStructures/MountType.hpp"

std::string cta::common::dataStructures::toString(cta::common::dataStructures::MountType type) {
  switch(type) {
    case cta::common::dataStructures::MountType::Archive:
      return "Archive";
    case cta::common::dataStructures::MountType::Retrieve:
      return "Retrieve";
    case cta::common::dataStructures::MountType::Label:
      return "Label";
    case cta::common::dataStructures::MountType::NoMount:
      return "-";
    default:
      return "UNKNOWN";
  }
}

std::ostream & cta::common::dataStructures::operator<<(std::ostream &os, 
  const cta::common::dataStructures::MountType &obj) {
  return os << toString(obj);
}
