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
  const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
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
  const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
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
  const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
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
  const std::list<common::dataStructures::MountPolicy>& mountPolicies) {
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


//------------------------------------------------------------------------------
// SchedulerDatabase::RetrieveMount::testReserveDiskSpace()
//------------------------------------------------------------------------------
bool SchedulerDatabase::RetrieveMount::testReserveDiskSpace(const cta::DiskSpaceReservationRequest& diskSpaceReservationRequest,
  const std::string& externalFreeDiskSpaceScript, log::LogContext& logContext) {
  // if the problem is that the script is throwing errors (and not that the disk space is insufficient),
  // we will issue a warning, but otherwise we will not sleep the queues and we will act like no disk
  // system was present
  // Get the current file systems list from the catalogue
  cta::disk::DiskSystemList diskSystemList;
  auto& impl_catalogue = getCatalogue();
  diskSystemList = impl_catalogue.DiskSystem()->getAllDiskSystems();
  diskSystemList.setExternalFreeDiskSpaceScript(externalFreeDiskSpaceScript);
  cta::disk::DiskSystemFreeSpaceList diskSystemFreeSpace(diskSystemList);

  // Get the existing reservation map from drives.
  auto previousDrivesReservations = impl_catalogue.DriveState()->getDiskSpaceReservations();
  // Get the free space from disk systems involved.
  std::set<std::string> diskSystemNames;
  for (const auto& dsrr : diskSpaceReservationRequest) {
    diskSystemNames.insert(dsrr.first);
  }

  try {
    diskSystemFreeSpace.fetchDiskSystemFreeSpace(diskSystemNames, impl_catalogue, logContext);
  } catch (const cta::disk::DiskSystemFreeSpaceListException& ex) {
    // Could not get free space for one of the disk systems due to a script error.
    // The queue will not be put to sleep (backpressure will not be applied), and we return true
    // because we want to allow staging files for retrieve in case of script errors.
    for (const auto& failedDiskSystem : ex.m_failedDiskSystems) {
      cta::log::ScopedParamContainer(logContext)
        .add("diskSystemName", failedDiskSystem.first)
        .add("failureReason", failedDiskSystem.second.getMessageValue())
        .log(cta::log::WARNING,
            "In OStoreDB::RetrieveMount::testReserveDiskSpace(): unable to request EOS free space "
            "for disk system using external script, backpressure will not be applied");
    }
    return true;
  } catch (std::exception& ex) {
    // Leave a log message before letting the possible exception go up the stack.
    cta::log::ScopedParamContainer(logContext)
      .add("exceptionWhat", ex.what())
      .log(cta::log::ERR,
           "In OStoreDB::RetrieveMount::testReserveDiskSpace(): got an exception from "
           "diskSystemFreeSpace.fetchDiskSystemFreeSpace().");
    throw;
  }

  // If a file system does not have enough space fail the disk space reservation,  put the queue to sleep and
  // the retrieve mount will immediately stop
  for (const auto& ds : diskSystemNames) {
    uint64_t previousDrivesReservationTotal = 0;
    auto diskSystem = diskSystemFreeSpace.getDiskSystemList().at(ds);
    // Compute previous drives reservation for the physical space of the current disk system.
    for (auto previousDriveReservation : previousDrivesReservations) {
      //avoid empty string when no disk space reservation exists for drive
      if (previousDriveReservation.second != 0) {
        auto previousDiskSystem = diskSystemFreeSpace.getDiskSystemList().at(previousDriveReservation.first);
        if (diskSystem.diskInstanceSpace.freeSpaceQueryURL == previousDiskSystem.diskInstanceSpace.freeSpaceQueryURL) {
          previousDrivesReservationTotal += previousDriveReservation.second;
        }
      }
    }
    if (diskSystemFreeSpace.at(ds).freeSpace < diskSpaceReservationRequest.at(ds) +
                                                 diskSystemFreeSpace.at(ds).targetedFreeSpace +
                                                 previousDrivesReservationTotal) {
      cta::log::ScopedParamContainer(logContext)
        .add("diskSystemName", ds)
        .add("freeSpace", diskSystemFreeSpace.at(ds).freeSpace)
        .add("existingReservations", previousDrivesReservationTotal)
        .add("spaceToReserve", diskSpaceReservationRequest.at(ds))
        .add("targetedFreeSpace", diskSystemFreeSpace.at(ds).targetedFreeSpace)
        .log(cta::log::WARNING,
             "In OStoreDB::RetrieveMount::testReserveDiskSpace(): could not allocate disk space for job, "
             "applying backpressure");

      auto sleepTime = diskSystem.sleepTime;
      putQueueToSleep(ds, sleepTime, logContext);
      return false;
    }
  }
  return true;
}

//------------------------------------------------------------------------------
// SchedulerDatabase::RetrieveMount::reserveDiskSpace()
//------------------------------------------------------------------------------
bool SchedulerDatabase::RetrieveMount::reserveDiskSpace(const cta::DiskSpaceReservationRequest& diskSpaceReservationRequest,
                                               const std::string& externalFreeDiskSpaceScript,
                                               log::LogContext& logContext) {
  // Get the current file systems list from the catalogue
  cta::disk::DiskSystemList diskSystemList;
  auto& impl_catalogue = getCatalogue();
  diskSystemList = impl_catalogue.DiskSystem()->getAllDiskSystems();
  diskSystemList.setExternalFreeDiskSpaceScript(externalFreeDiskSpaceScript);
  cta::disk::DiskSystemFreeSpaceList diskSystemFreeSpace(diskSystemList);

  // Get the existing reservation map from drives.
  auto previousDrivesReservations = impl_catalogue.DriveState()->getDiskSpaceReservations();
  // Get the free space from disk systems involved.
  std::set<std::string> diskSystemNames;
  for (const auto& dsrr : diskSpaceReservationRequest) {
    diskSystemNames.insert(dsrr.first);
  }

  try {
    diskSystemFreeSpace.fetchDiskSystemFreeSpace(diskSystemNames, impl_catalogue, logContext);
  } catch (const cta::disk::DiskSystemFreeSpaceListException& ex) {
    // Could not get free space for one of the disk systems due to a script error.
    // The queue will not be put to sleep (backpressure will not be applied), and we return
    // true, because we want to allow staging files for retrieve in case of script errors.
    for (const auto& failedDiskSystem : ex.m_failedDiskSystems) {
      cta::log::ScopedParamContainer(logContext)
        .add("diskSystemName", failedDiskSystem.first)
        .add("failureReason", failedDiskSystem.second.getMessageValue())
        .log(cta::log::WARNING,
            "In OStoreDB::RetrieveMount::reserveDiskSpace(): unable to request EOS free space for "
            "disk system using external script, backpressure will not be applied");
    }
    return true;
  } catch (std::exception& ex) {
    // Leave a log message before letting the possible exception go up the stack.
    cta::log::ScopedParamContainer(logContext)
      .add("exceptionWhat", ex.what())
      .log(cta::log::ERR,
           "In OStoreDB::RetrieveMount::reserveDiskSpace(): got an exception from "
           "diskSystemFreeSpace.fetchDiskSystemFreeSpace().");
    throw;
  }

  // If a file system does not have enough space fail the disk space reservation,  put the queue to sleep and
  // the retrieve mount will immediately stop
  for (const auto& ds : diskSystemNames) {
    uint64_t previousDrivesReservationTotal = 0;
    auto diskSystem = diskSystemFreeSpace.getDiskSystemList().at(ds);
    // Compute previous drives reservation for the physical space of the current disk system.
    for (auto previousDriveReservation : previousDrivesReservations) {
      //avoid empty string when no disk space reservation exists for drive
      if (previousDriveReservation.second != 0) {
        auto previousDiskSystem = diskSystemFreeSpace.getDiskSystemList().at(previousDriveReservation.first);
        if (diskSystem.diskInstanceSpace.freeSpaceQueryURL == previousDiskSystem.diskInstanceSpace.freeSpaceQueryURL) {
          previousDrivesReservationTotal += previousDriveReservation.second;
        }
      }
    }
    if (diskSystemFreeSpace.at(ds).freeSpace < diskSpaceReservationRequest.at(ds) +
                                                 diskSystemFreeSpace.at(ds).targetedFreeSpace +
                                                 previousDrivesReservationTotal) {
      cta::log::ScopedParamContainer(logContext)
        .add("diskSystemName", ds)
        .add("freeSpace", diskSystemFreeSpace.at(ds).freeSpace)
        .add("existingReservations", previousDrivesReservationTotal)
        .add("spaceToReserve", diskSpaceReservationRequest.at(ds))
        .add("targetedFreeSpace", diskSystemFreeSpace.at(ds).targetedFreeSpace)
        .log(cta::log::WARNING,
             "In OStoreDB::RetrieveMount::reserveDiskSpace(): could not allocate disk space for job, "
             "applying backpressure");

      auto sleepTime = diskSystem.sleepTime;
      putQueueToSleep(ds, sleepTime, logContext);
      return false;
    }
  }

  impl_catalogue.DriveState()->reserveDiskSpace(mountInfo.drive,
                                             mountInfo.mountId,
                                             diskSpaceReservationRequest,
                                             logContext);
  return true;
}

}  //namespace cta
