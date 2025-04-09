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
#include "cmdline/admin_common/DataItemMessageFill.hpp"

namespace cta::xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class ShowQueuesStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  ShowQueuesStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue& catalogue,
    cta::Scheduler& scheduler, log::LogContext& lc);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_queuesAndMountsList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::common::dataStructures::QueueAndMountSummary> m_queuesAndMountsList;    //!< List of queues and mounts from the scheduler
  std::optional<std::string> m_schedulerBackendName;
  const std::string m_instanceName;

  static constexpr const char* const LOG_SUFFIX  = "ShowQueuesStream";                   //!< Identifier for log messages
};

ShowQueuesStream::ShowQueuesStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue& catalogue,
  cta::Scheduler& scheduler, log::LogContext& lc) :
  XrdCtaStream(catalogue, scheduler),
  m_queuesAndMountsList(scheduler.getQueuesAndMountSummaries(lc)),
  m_schedulerBackendName(scheduler.getSchedulerBackendName()),
  m_instanceName(requestMsg.getInstanceName())
{
  using namespace cta::admin;
  if (!m_schedulerBackendName) {
    XrdSsiPb::Log::Msg(
      XrdSsiPb::Log::ERROR,
      LOG_SUFFIX,
      "ShowQueuesStream constructor, the cta.scheduler_backend_name is not set in the frontend configuration.");
  }
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "ShowQueuesStream() constructor");
}

int ShowQueuesStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  using namespace cta::admin;

  for(bool is_buffer_full = false; !m_queuesAndMountsList.empty() && !is_buffer_full; m_queuesAndMountsList.pop_front()) {
    Data record;

    const auto &sq      = m_queuesAndMountsList.front();
    auto  sq_item = record.mutable_sq_item();

    fillSqItem(sq, sq_item, m_schedulerBackendName, m_instanceName);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

} // namespace cta::xrd
