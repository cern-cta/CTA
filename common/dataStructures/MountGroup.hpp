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

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/EntryLog.hpp"

namespace cta {
namespace common {
namespace dataStructures {

struct MountGroup {

  MountGroup():
    archive_minBytesQueued(0),
    archive_minFilesQueued(0),
    archive_minRequestAge(0),
    archive_priority(0),
    maxDrivesAllowed(0),
    retrieve_minBytesQueued(0),
    retrieve_minFilesQueued(0),
    retrieve_minRequestAge(0),
    retrieve_priority(0) {
  }

  bool operator==(const MountGroup &rhs) const;

  bool operator!=(const MountGroup &rhs) const;

  uint64_t archive_minBytesQueued;
  uint64_t archive_minFilesQueued;
  uint64_t archive_minRequestAge;
  uint64_t archive_priority;
  std::string comment;
  cta::common::dataStructures::EntryLog creationLog;
  cta::common::dataStructures::EntryLog lastModificationLog;
  uint64_t maxDrivesAllowed;
  std::string name;
  uint64_t retrieve_minBytesQueued;
  uint64_t retrieve_minFilesQueued;
  uint64_t retrieve_minRequestAge;
  uint64_t retrieve_priority;

}; // struct MountGroup

} // namespace dataStructures
} // namespace common
} // namespace cta

std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::MountGroup &obj);
