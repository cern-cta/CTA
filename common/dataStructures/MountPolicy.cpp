/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/dataStructures/MountPolicy.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"
#include "MountPolicy.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MountPolicy::MountPolicy() :
archivePriority(0),
archiveMinRequestAge(0),
retrievePriority(0),
retrieveMinRequestAge(0) {}

MountPolicy::MountPolicy(const std::string& name,
                         const uint64_t archivePriority,
                         const uint64_t archiveMinRequestAge,
                         const uint64_t retrievePriority,
                         const uint64_t retrieveMinRequestAge) :
name(name),
archivePriority(archivePriority),
archiveMinRequestAge(archiveMinRequestAge),
retrievePriority(retrievePriority),
retrieveMinRequestAge(retrieveMinRequestAge) {}

MountPolicy::MountPolicy(const MountPolicy& other) {
  this->archiveMinRequestAge = other.archiveMinRequestAge;
  this->archivePriority = other.archivePriority;
  this->comment = other.comment;
  this->creationLog = other.creationLog;
  this->lastModificationLog = other.lastModificationLog;
  this->name = other.name;
  this->retrieveMinRequestAge = other.retrieveMinRequestAge;
  this->retrievePriority = other.retrievePriority;
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool MountPolicy::operator==(const MountPolicy& rhs) const {
  return name == rhs.name && archivePriority == rhs.archivePriority &&
         archiveMinRequestAge == rhs.archiveMinRequestAge && retrievePriority == rhs.retrievePriority &&
         retrieveMinRequestAge == rhs.retrieveMinRequestAge && creationLog == rhs.creationLog &&
         lastModificationLog == rhs.lastModificationLog && comment == rhs.comment;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool MountPolicy::operator!=(const MountPolicy& rhs) const {
  return !operator==(rhs);
}

MountPolicy MountPolicy::operator=(const MountPolicy& other) {
  if (this != &other) {
    this->archiveMinRequestAge = other.archiveMinRequestAge;
    this->archivePriority = other.archivePriority;
    this->comment = other.comment;
    this->creationLog = other.creationLog;
    this->lastModificationLog = other.lastModificationLog;
    this->name = other.name;
    this->retrieveMinRequestAge = other.retrieveMinRequestAge;
    this->retrievePriority = other.retrievePriority;
  }
  return *this;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& os, const MountPolicy& obj) {
  os << "(name=" << obj.name << " archive_priority=" << obj.archivePriority
     << " archive_minRequestAge=" << obj.archiveMinRequestAge << " retrieve_priority=" << obj.retrievePriority
     << " retrieve_minRequestAge=" << obj.retrieveMinRequestAge << " creationLog=" << obj.creationLog
     << " lastModificationLog=" << obj.lastModificationLog << " comment=" << obj.comment << ")";
  return os;
}

MountPolicy MountPolicy::s_defaultMountPolicyForRepack("default_mount_policy_repack", 1, 1, 1, 1);

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
