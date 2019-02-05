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

#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MountPolicy::MountPolicy():
  archivePriority(0),
  archiveMinRequestAge(0),
  retrievePriority(0),
  retrieveMinRequestAge(0),
  maxDrivesAllowed(0) {}

MountPolicy::MountPolicy(const std::string name, const uint64_t archivePriority,const uint64_t archiveMinRequestAge, const uint64_t retrievePriority, 
        const uint64_t retrieveMinRequestAge, const uint64_t maxDrivesAllowed):
        name(name), archivePriority(archivePriority), archiveMinRequestAge(archiveMinRequestAge), retrievePriority(retrievePriority), 
        retrieveMinRequestAge(retrieveMinRequestAge), maxDrivesAllowed(maxDrivesAllowed) {}
//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool MountPolicy::operator==(const MountPolicy &rhs) const {
  return name==rhs.name
      && archivePriority==rhs.archivePriority
      && archiveMinRequestAge==rhs.archiveMinRequestAge
      && retrievePriority==rhs.retrievePriority
      && retrieveMinRequestAge==rhs.retrieveMinRequestAge
      && maxDrivesAllowed==rhs.maxDrivesAllowed
      && creationLog==rhs.creationLog
      && lastModificationLog==rhs.lastModificationLog
      && comment==rhs.comment;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool MountPolicy::operator!=(const MountPolicy &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const MountPolicy &obj) {
  os << "(name=" << obj.name
     << " archive_priority=" << obj.archivePriority
     << " archive_minRequestAge=" << obj.archiveMinRequestAge
     << " retrieve_priority=" << obj.retrievePriority
     << " retrieve_minRequestAge=" << obj.retrieveMinRequestAge
     << " maxDrivesAllowed=" << obj.maxDrivesAllowed
     << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog
     << " comment=" << obj.comment << ")";
  return os;
}

MountPolicy MountPolicy::s_defaultMountPolicyForRepack("default_mount_policy_repack",1,1,1,1,1);

} // namespace dataStructures
} // namespace common
} // namespace cta
