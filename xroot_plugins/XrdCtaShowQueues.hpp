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

#pragma once

#include "xroot_plugins/XrdCtaStream.hpp"

namespace cta {
namespace xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class ShowQueuesStream : public XrdCtaStream {
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  ShowQueuesStream(const frontend::AdminCmdStream& requestMsg,
                   cta::catalogue::Catalogue& catalogue,
                   cta::Scheduler& scheduler,
                   log::LogContext& lc);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const { return m_queuesAndMountsList.empty(); }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf);

  std::list<cta::common::dataStructures::QueueAndMountSummary>
    m_queuesAndMountsList;  //!< List of queues and mounts from the scheduler

  static constexpr const char* const LOG_SUFFIX = "ShowQueuesStream";  //!< Identifier for log messages
};

ShowQueuesStream::ShowQueuesStream(const frontend::AdminCmdStream& requestMsg,
                                   cta::catalogue::Catalogue& catalogue,
                                   cta::Scheduler& scheduler,
                                   log::LogContext& lc) :
XrdCtaStream(catalogue, scheduler),
m_queuesAndMountsList(scheduler.getQueuesAndMountSummaries(lc)) {
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "ShowQueuesStream() constructor");
}

int ShowQueuesStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf) {
  using namespace cta::admin;

  for (bool is_buffer_full = false; !m_queuesAndMountsList.empty() && !is_buffer_full;
       m_queuesAndMountsList.pop_front()) {
    Data record;

    auto& sq = m_queuesAndMountsList.front();
    auto sq_item = record.mutable_sq_item();

    switch (sq.mountType) {
      case common::dataStructures::MountType::ArchiveForRepack:
      case common::dataStructures::MountType::ArchiveForUser:
        sq_item->set_priority(sq.mountPolicy.archivePriority);
        sq_item->set_min_age(sq.mountPolicy.archiveMinRequestAge);
        break;
      case common::dataStructures::MountType::Retrieve:
        sq_item->set_priority(sq.mountPolicy.retrievePriority);
        sq_item->set_min_age(sq.mountPolicy.retrieveMinRequestAge);
        break;
      default:
        break;
    }

    sq_item->set_mount_type(MountTypeToProtobuf(sq.mountType));
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
    }
    else {
      sq_item->set_sleeping_for_space(false);
    }
    for (auto& policyName : sq.mountPolicies) {
      sq_item->add_mount_policies(policyName);
    }
    sq_item->set_highest_priority_mount_policy(sq.highestPriorityMountPolicy);
    sq_item->set_lowest_request_age_mount_policy(sq.lowestRequestAgeMountPolicy);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}  // namespace xrd
}  // namespace cta
