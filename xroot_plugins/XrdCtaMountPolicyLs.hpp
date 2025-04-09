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

#include "XrdCtaStream.hpp"
#include "cmdline/MountPolicyLsResponseStream.hpp"

namespace cta::xrd {

class MountPolicyLsStream : public XrdCtaStream {
public:
    MountPolicyLsStream(const frontend::AdminCmdStream& requestMsg, 
                       cta::catalogue::Catalogue& catalogue, 
                       cta::Scheduler& scheduler);

private:
    bool isDone() const override { return m_stream->isDone(); }
    int fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf) override;
    
    std::unique_ptr<cta::cmdline::MountPolicyLsResponseStream> m_stream;
    
    static constexpr const char* const LOG_SUFFIX = "MountPolicyLsStream";
};

MountPolicyLsStream::MountPolicyLsStream(const frontend::AdminCmdStream& requestMsg, 
                                        cta::catalogue::Catalogue& catalogue, 
                                        cta::Scheduler& scheduler)
    : XrdCtaStream(catalogue, scheduler) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "MountPolicyLsStream() constructor");

  try {
    m_stream = std::make_unique<cta::cmdline::MountPolicyLsResponseStream>(catalogue,
                                                                           scheduler,
                                                                           requestMsg.getInstanceName());

    m_stream->init(requestMsg.getAdminCmd());
  } catch (const std::exception& ex) {
    throw exception::UserError(ex.what());
  }
}

int MountPolicyLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf) {
    for (bool is_buffer_full = false; !m_stream->isDone() && !is_buffer_full;) {
        Data record = m_stream->next();
        is_buffer_full = streambuf->Push(record);
    }
    return streambuf->Size();
}

} // namespace cta::xrd
