/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/MountPolicy.hpp"

#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

//------------------------------------------------------------------------------
// private constructor (for static default repack mount policy)
//------------------------------------------------------------------------------
MountPolicy::MountPolicy(std::string_view name,
                         uint64_t archivePriority,
                         uint64_t archiveMinRequestAge,
                         uint64_t retrievePriority,
                         uint64_t retrieveMinRequestAge)
    : name(name),
      archivePriority(archivePriority),
      archiveMinRequestAge(archiveMinRequestAge),
      retrievePriority(retrievePriority),
      retrieveMinRequestAge(retrieveMinRequestAge) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool MountPolicy::operator==(const MountPolicy& rhs) const {
  return name == rhs.name && archivePriority == rhs.archivePriority && archiveMinRequestAge == rhs.archiveMinRequestAge
         && retrievePriority == rhs.retrievePriority && retrieveMinRequestAge == rhs.retrieveMinRequestAge
         && creationLog == rhs.creationLog && lastModificationLog == rhs.lastModificationLog && comment == rhs.comment;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool MountPolicy::operator!=(const MountPolicy& rhs) const {
  return !operator==(rhs);
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

const MountPolicy MountPolicy::s_defaultMountPolicyForRepack("default_mount_policy_repack", 1, 1, 1, 1);

}  // namespace cta::common::dataStructures
