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

#include "scheduler/SchedulerDatabase.hpp"

namespace cta {

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::SchedulerDatabase::~SchedulerDatabase() = default;

SchedulerDatabase::RepackRequestStatistics::RepackRequestStatistics() {
  typedef common::dataStructures::RepackInfo::Status Status;
  for (auto& s :
       {Status::Complete, Status::Failed, Status::Pending, Status::Running, Status::Starting, Status::ToExpand}) {
    operator[](s) = 0;
  }
}

void SchedulerDatabase::DiskSpaceReservationRequest::addRequest(const std::string& diskSystemName, uint64_t size) {
  try {
    at(diskSystemName) += size;
  } catch (std::out_of_range&) {
    operator[](diskSystemName) = size;
  }
}

//------------------------------------------------------------------------------
// SchedulerDatabase::getArchiveMountPolicyMaxPriorityMinAge()
//------------------------------------------------------------------------------
std::pair<uint64_t, uint64_t>
SchedulerDatabase::getArchiveMountPolicyMaxPriorityMinAge(const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
  if (mountPolicies.empty()) {
    throw cta::exception::Exception("In SchedulerDatabase::getArchiveMountPolicyMaxPriorityMinAge(), empty mount policy list.");
  }
  uint64_t maxPriority = std::numeric_limits<uint64_t>::min();
  uint64_t minMinRequestAge = std::numeric_limits<uint64_t>::max();
  for (auto & mountPolicy : mountPolicies) {
    maxPriority = std::max(maxPriority, mountPolicy.archivePriority);
    minMinRequestAge = std::min(minMinRequestAge, mountPolicy.archiveMinRequestAge);
  }
  return std::pair{maxPriority, minMinRequestAge};
}


//------------------------------------------------------------------------------
// SchedulerDatabase::getRetrieveMountPolicyMaxPriorityMinAge()
//------------------------------------------------------------------------------
std::pair<uint64_t, uint64_t>
SchedulerDatabase::getRetrieveMountPolicyMaxPriorityMinAge(const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
  if (mountPolicies.empty()) {
    throw cta::exception::Exception("In SchedulerDatabase::getRetrieveMountPolicyMaxPriorityMinAge(), empty mount policy list.");
  }
  uint64_t maxPriority = std::numeric_limits<uint64_t>::min();
  uint64_t minMinRequestAge = std::numeric_limits<uint64_t>::max();
  for (auto & mountPolicy : mountPolicies) {
    maxPriority = std::max(maxPriority, mountPolicy.retrievePriority);
    minMinRequestAge = std::min(minMinRequestAge, mountPolicy.retrieveMinRequestAge);
  }
  return std::pair{maxPriority, minMinRequestAge};
}

//------------------------------------------------------------------------------
// SchedulerDatabase::getHighestPriorityArchiveMountPolicyName()
//------------------------------------------------------------------------------
std::string SchedulerDatabase::getHighestPriorityArchiveMountPolicyName(
  const std::list<common::dataStructures::MountPolicy>& mountPolicies) const {
  if (mountPolicies.empty()) {
    throw cta::exception::Exception(
      "In SchedulerDatabase::getHighestPriorityArchiveMountPolicyName(), empty mount policy list.");
  }
  //We need to get the most advantageous mountPolicy
  //As the Init element of the reduce function is the first element of the list, we start the reduce with the second element (++mountPolicyInQueueList.begin())
  common::dataStructures::MountPolicy bestMountPolicy = cta::utils::reduce(
    ++mountPolicies.begin(),
    mountPolicies.end(),
    mountPolicies.front(),
    [](const common::dataStructures::MountPolicy& mp1, const common::dataStructures::MountPolicy& mp2) {
      if (mp2.archivePriority > mp1.archivePriority) {
        return mp2;
      }
      return mp1;
    });
  return bestMountPolicy.name;
}

//------------------------------------------------------------------------------
// SchedulerDatabase::getLowestRequestAgeArchiveMountPolicyName()
//------------------------------------------------------------------------------
std::string SchedulerDatabase::getLowestRequestAgeArchiveMountPolicyName(
  const std::list<common::dataStructures::MountPolicy>& mountPolicies) const {
  if (mountPolicies.empty()) {
    throw cta::exception::Exception(
      "In SchedulerDatabase::getLowestRequestAgeArchiveMountPolicyName(), empty mount policy list.");
  }
  //We need to get the most advantageous mountPolicy
  //As the Init element of the reduce function is the first element of the list, we start the reduce with the second element (++mountPolicyInQueueList.begin())
  common::dataStructures::MountPolicy bestMountPolicy = cta::utils::reduce(
    ++mountPolicies.begin(),
    mountPolicies.end(),
    mountPolicies.front(),
    [](const common::dataStructures::MountPolicy& mp1, const common::dataStructures::MountPolicy& mp2) {
      if (mp1.archiveMinRequestAge < mp2.archiveMinRequestAge) {
        return mp1;
      }
      return mp2;
    });
  return bestMountPolicy.name;
}

//------------------------------------------------------------------------------
// SchedulerDatabase::getHighestPriorityRetrieveMountPolicyName()
//------------------------------------------------------------------------------
std::string SchedulerDatabase::getHighestPriorityRetrieveMountPolicyName(
  const std::list<common::dataStructures::MountPolicy>& mountPolicies) const {
  if (mountPolicies.empty()) {
    throw cta::exception::Exception(
      "In SchedulerDatabase::getHighestPriorityRetrieveMountPolicyName(), empty mount policy list.");
  }
  //We need to get the most advantageous mountPolicy
  //As the Init element of the reduce function is the first element of the list, we start the reduce with the second element (++mountPolicyInQueueList.begin())
  common::dataStructures::MountPolicy bestMountPolicy = cta::utils::reduce(
    ++mountPolicies.begin(),
    mountPolicies.end(),
    mountPolicies.front(),
    [](const common::dataStructures::MountPolicy& mp1, const common::dataStructures::MountPolicy& mp2) {
      if (mp1.retrievePriority > mp2.retrievePriority) {
        return mp1;
      }
      return mp2;
    });
  return bestMountPolicy.name;
}

//------------------------------------------------------------------------------
// SchedulerDatabase::getLowestRequestAgeRetrieveMountPolicyName()
//------------------------------------------------------------------------------
std::string SchedulerDatabase::getLowestRequestAgeRetrieveMountPolicyName(
  const std::list<common::dataStructures::MountPolicy>& mountPolicies) const {
  if (mountPolicies.empty()) {
    throw cta::exception::Exception(
      "In SchedulerDatabase::getLowestRequestAgeRetrieveMountPolicyName(), empty mount policy list.");
  }
  //We need to get the most advantageous mountPolicy
  //As the Init element of the reduce function is the first element of the list, we start the reduce with the second element (++mountPolicyInQueueList.begin())
  common::dataStructures::MountPolicy bestMountPolicy = cta::utils::reduce(
    ++mountPolicies.begin(),
    mountPolicies.end(),
    mountPolicies.front(),
    [](const common::dataStructures::MountPolicy& mp1, const common::dataStructures::MountPolicy& mp2) {
      if (mp1.retrieveMinRequestAge < mp2.retrieveMinRequestAge) {
        return mp1;
      }
      return mp2;
    });
  return bestMountPolicy.name;
}

}  //namespace cta
