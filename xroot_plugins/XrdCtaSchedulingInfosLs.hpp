/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <xroot_plugins/XrdCtaStream.hpp>
#include <xroot_plugins/XrdSsiCtaRequestMessage.hpp>
#include <common/dataStructures/MountTypeSerDeser.hpp>

namespace cta { namespace xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class SchedulingInfosLsStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  SchedulingInfosLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue,
    cta::Scheduler &scheduler, log::LogContext &lc);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_schedulingInfosList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::SchedulingInfos> m_schedulingInfosList;    //!< List of queues and mounts from the scheduler

  static constexpr const char* const LOG_SUFFIX  = "SchedulingInfosStream";                   //!< Identifier for log messages
};

SchedulingInfosLsStream::SchedulingInfosLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue,
  cta::Scheduler &scheduler, log::LogContext &lc) :
  XrdCtaStream(catalogue, scheduler),
  m_schedulingInfosList(scheduler.getSchedulingInformations(lc))
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "SchedulingInfosLsStream() constructor");
}

int SchedulingInfosLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  using namespace cta::admin;

  for(bool is_buffer_full = false; !m_schedulingInfosList.empty() && !is_buffer_full; m_schedulingInfosList.pop_front()) {
    Data record;

    auto &schedulingInfo      = m_schedulingInfosList.front();
    auto sils_item = record.mutable_sils_item();
    sils_item->set_logical_library(schedulingInfo.getLogicalLibraryName());
    auto potentialMounts = schedulingInfo.getPotentialMounts();
    for(auto & potentialMount: potentialMounts){
      auto potentialMountToAdd = sils_item->mutable_potential_mounts()->Add();
      potentialMountToAdd->set_vid(potentialMount.vid);
      potentialMountToAdd->set_tapepool(potentialMount.tapePool);
      potentialMountToAdd->set_vo(potentialMount.vo);
      potentialMountToAdd->set_media_type(potentialMount.mediaType);
      potentialMountToAdd->set_vendor(potentialMount.vendor);
      potentialMountToAdd->set_mount_type(MountTypeToProtobuf(potentialMount.type));
      potentialMountToAdd->set_tape_capacity_in_bytes(potentialMount.capacityInBytes);
      potentialMountToAdd->set_mount_policy_priority(potentialMount.priority);
      potentialMountToAdd->set_mount_policy_min_request_age(potentialMount.minRequestAge);
      potentialMountToAdd->set_files_queued(potentialMount.filesQueued);
      potentialMountToAdd->set_bytes_queued(potentialMount.bytesQueued);
      potentialMountToAdd->set_oldest_job_start_time(potentialMount.oldestJobStartTime);
      potentialMountToAdd->set_sleeping_mount(potentialMount.sleepingMount);
      potentialMountToAdd->set_sleep_time(potentialMount.sleepTime);
      potentialMountToAdd->set_disk_system_slept_for(potentialMount.diskSystemSleptFor);
      potentialMountToAdd->set_mount_count(potentialMount.mountCount);
      potentialMountToAdd->set_logical_library(potentialMount.logicalLibrary);
    }
    is_buffer_full = streambuf->Push(record);
    /*

    sq_item->set_mount_type(MountTypeToProtobuf(sq.mountType));
    sq_item->set_tapepool(sq.tapePool);
    sq_item->set_logical_library(sq.logicalLibrary);
    sq_item->set_vid(sq.vid);
    sq_item->set_queued_files(sq.filesQueued);
    sq_item->set_queued_bytes(sq.bytesQueued);
    sq_item->set_oldest_age(sq.oldestJobAge);
    sq_item->set_cur_mounts(sq.currentMounts);
    sq_item->set_cur_files(sq.currentFiles);
    sq_item->set_cur_bytes(sq.currentBytes);
    sq_item->set_next_mounts(sq.nextMounts);
    sq_item->set_tapes_capacity(sq.tapesCapacity);
    sq_item->set_tapes_files(sq.filesOnTapes);
    sq_item->set_tapes_bytes(sq.dataOnTapes);
    sq_item->set_full_tapes(sq.fullTapes);
    sq_item->set_empty_tapes(sq.emptyTapes);
    sq_item->set_disabled_tapes(sq.disabledTapes);
    sq_item->set_rdonly_tapes(sq.readOnlyTapes);
    sq_item->set_writable_tapes(sq.writableTapes);
    if (sq.sleepForSpaceInfo) {
      sq_item->set_sleeping_for_space(true);
      sq_item->set_sleep_start_time(sq.sleepForSpaceInfo.value().startTime);
      sq_item->set_disk_system_slept_for(sq.sleepForSpaceInfo.value().diskSystemName);
    } else {
      sq_item->set_sleeping_for_space(false);
    }

    is_buffer_full = streambuf->Push(record);*/
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
