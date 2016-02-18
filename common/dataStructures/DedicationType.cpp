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

#include "common/dataStructures/DedicationType.hpp"

std::string cta::common::dataStructures::toString(cta::common::dataStructures::DedicationType type) {
  switch(type) {
    case cta::common::dataStructures::DedicationType::readonly:
      return "readonly";
    case cta::common::dataStructures::DedicationType::writeonly:
      return "writeonly";
    case cta::common::dataStructures::DedicationType::readwrite:
      return "readwrite";
    default:
      return "UNKNOWN";
  }
}