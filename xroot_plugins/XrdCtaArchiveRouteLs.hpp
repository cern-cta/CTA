/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "XrdCtaStream.hpp"
#include "cmdline/ArchiveRouteLsResponseStream.hpp"

namespace cta::xrd {

class ArchiveRouteLsStream : public XrdCtaStream {
public:
  ArchiveRouteLsStream(const frontend::AdminCmdStream& requestMsg,
                       cta::catalogue::Catalogue& catalogue,
                       cta::Scheduler& scheduler);

private:
  static constexpr const char* const LOG_SUFFIX = "ArchiveRouteLsStream";
};

ArchiveRouteLsStream::ArchiveRouteLsStream(const frontend::AdminCmdStream& requestMsg,
                                           cta::catalogue::Catalogue& catalogue,
                                           cta::Scheduler& scheduler)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::cmdline::ArchiveRouteLsResponseStream>(catalogue,
                                                                                scheduler,
                                                                                requestMsg.getInstanceName(),
                                                                                requestMsg.getAdminCmd())) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

} // namespace cta::xrd
