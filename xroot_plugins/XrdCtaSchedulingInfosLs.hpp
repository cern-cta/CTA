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
#include "common/dataStructures/MountTypeSerDeser.hpp"

namespace cta::xrd {

/*!
 * Stream object which implements "schedulinginfo ls" command
 */
class SchedulingInfosLsStream: public XrdCtaStream {
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  SchedulingInfosLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue,
    cta::Scheduler &scheduler, log::LogContext &lc);

private:
  /*!
   * Can we close the stream?
   */
  virtual bool isDone() const {
    return m_schedulingInfoList.empty() && m_potentialMountList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::SchedulingInfos> m_schedulingInfoList;                     //!< List of queues and mounts from the scheduler
  std::string m_logicalLibrary;                                             //!< Logical library for potential archive mounts
  std::list<cta::SchedulerDatabase::PotentialMount> m_potentialMountList;   //!< List of potential mounts

  static constexpr const char* const LOG_SUFFIX  = "SchedulingInfosStream"; //!< Identifier for log messages
};

SchedulingInfosLsStream::SchedulingInfosLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue,
  cta::Scheduler &scheduler, log::LogContext &lc) :
  XrdCtaStream(catalogue, scheduler),
  m_schedulingInfoList(scheduler.getSchedulingInformations(lc))
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "SchedulingInfosLsStream() constructor");
}

} // namespace cta::xrd
