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

#pragma once

#include <string>

namespace cta {
namespace common {
namespace dataStructures {
enum class MountType: uint32_t {
  ArchiveForUser = 1,
  ArchiveForRepack = 2,
  Retrieve = 3,
  Label = 4,
  NoMount = 0,
  /// A summary type used in scheduling.
  ArchiveAllTypes = 99
};

/// A function summarizing subtypes (currently only Archive) to simplify scheduling.
MountType getMountBasicType(MountType type);

std::string toString(MountType type);

std::ostream &operator <<(std::ostream& os, const MountType &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
