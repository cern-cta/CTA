/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/SchedulerDatabase.hpp"

namespace cta {

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::SchedulerDatabase::~SchedulerDatabase() = default;

common::dataStructures::TapeSessionReport SchedulerDatabase::ArchiveMount::MountInfo::makeTapeSessionReport(
  common::dataStructures::TapeSessionState state) const {
  return {.state = state,
          .mountType = mountType,
          .mountSessionId = mountId,
          .vid = vid,
          .tapePool = tapePool,
          .vo = vo,
          .priority = priority};
}

common::dataStructures::TapeSessionReport SchedulerDatabase::RetrieveMount::MountInfo::makeTapeSessionReport(
  common::dataStructures::TapeSessionState state) const {
  return {.state = state,
          .mountType = common::dataStructures::MountType::Retrieve,
          .mountSessionId = mountId,
          .vid = vid,
          .tapePool = tapePool,
          .vo = vo,
          .priority = priority,
          .activity = activity};
}

SchedulerDatabase::RepackRequestStatistics::RepackRequestStatistics() {
  using Status = common::dataStructures::RepackInfo::Status;
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

}  //namespace cta
