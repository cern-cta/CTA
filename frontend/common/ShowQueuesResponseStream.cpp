/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ShowQueuesResponseStream.hpp"

#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "frontend/common/AdminCmdOptions.hpp"

namespace cta::frontend {

ShowQueuesResponseStream::ShowQueuesResponseStream(cta::catalogue::Catalogue& catalogue,
                                                   cta::Scheduler& scheduler,
                                                   const std::string& instanceName,
                                                   cta::log::LogContext& lc)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_lc(lc),
      m_schedulerBackendName(scheduler.getSchedulerBackendName()) {
  if (!m_schedulerBackendName) {
    m_lc.log(cta::log::ERR,
             "ShowQueuesStream constructor, the cta.scheduler_backend_name is not set in the frontend configuration.");
  }

  m_queuesAndMounts = m_scheduler.getQueuesAndMountSummaries(m_lc);
}

bool ShowQueuesResponseStream::isDone() {
  return m_queuesAndMountsIdx >= m_queuesAndMounts.size();
}

cta::xrd::Data ShowQueuesResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto sq = m_queuesAndMounts[m_queuesAndMountsIdx++];

  cta::xrd::Data data;
  auto sq_item = data.mutable_sq_item();

  switch (sq.mountType) {
    case cta::common::dataStructures::MountType::ArchiveForRepack:
    case cta::common::dataStructures::MountType::ArchiveForUser:
      sq_item->set_priority(sq.mountPolicy.archivePriority);
      sq_item->set_min_age(sq.mountPolicy.archiveMinRequestAge);
      break;
    case cta::common::dataStructures::MountType::Retrieve:
      sq_item->set_priority(sq.mountPolicy.retrievePriority);
      sq_item->set_min_age(sq.mountPolicy.retrieveMinRequestAge);
      break;
    default:
      break;
  }

  sq_item->set_mount_type(cta::admin::MountTypeToProtobuf(sq.mountType));
  sq_item->set_instance_name(m_instanceName);
  sq_item->set_scheduler_backend_name(m_schedulerBackendName.value_or(""));
  sq_item->set_tapepool(sq.tapePool);
  sq_item->set_logical_library(sq.logicalLibrary);
  sq_item->set_vid(sq.vid);
  sq_item->set_queued_files(sq.filesQueued);
  sq_item->set_queued_bytes(sq.bytesQueued);
  sq_item->set_oldest_age(sq.oldestJobAge);
  sq_item->set_youngest_age(sq.youngestJobAge);
  sq_item->set_cur_mounts(sq.currentMounts);
  sq_item->set_cur_files(sq.currentFiles);
  sq_item->set_cur_bytes(sq.currentBytes);
  sq_item->set_tapes_capacity(sq.tapesCapacity);
  sq_item->set_tapes_files(sq.filesOnTapes);
  sq_item->set_tapes_bytes(sq.dataOnTapes);
  sq_item->set_full_tapes(sq.fullTapes);
  sq_item->set_writable_tapes(sq.writableTapes);
  sq_item->set_vo(sq.vo);
  sq_item->set_read_max_drives(sq.readMaxDrives);
  sq_item->set_write_max_drives(sq.writeMaxDrives);
  if (sq.sleepForSpaceInfo) {
    sq_item->set_sleeping_for_space(true);
    sq_item->set_sleep_start_time(sq.sleepForSpaceInfo.value().startTime);
    sq_item->set_disk_system_slept_for(sq.sleepForSpaceInfo.value().diskSystemName);
  } else {
    sq_item->set_sleeping_for_space(false);
  }
  for (auto& policyName : sq.mountPolicies) {
    sq_item->add_mount_policies(policyName);
  }
  sq_item->set_highest_priority_mount_policy(sq.highestPriorityMountPolicy);
  sq_item->set_lowest_request_age_mount_policy(sq.lowestRequestAgeMountPolicy);

  return data;
}

}  // namespace cta::frontend
