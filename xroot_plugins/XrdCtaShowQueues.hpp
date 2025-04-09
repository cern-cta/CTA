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
#include "cmdline/ShowQueuesResponseStream.hpp"

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
  bool isDone() const override { return m_stream->isDone(); }
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) override;

  static constexpr const char* const LOG_SUFFIX  = "ShowQueuesStream";                   //!< Identifier for log messages
};

ShowQueuesStream::ShowQueuesStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue& catalogue,
  cta::Scheduler& scheduler, log::LogContext& lc) :
  XrdCtaStream(catalogue, scheduler)
{  
  initializeStream<cta::cmdline::ShowQueuesResponseStream>(requestMsg, LOG_SUFFIX, lc);
}

int ShowQueuesStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for(bool is_buffer_full = false; !m_stream->isDone() && !is_buffer_full;) {
    Data record = m_stream->next();
    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

} // namespace cta::xrd
