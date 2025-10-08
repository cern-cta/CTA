/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "XrdCtaStream.hpp"
#include "cmdline/TapeLsResponseStream.hpp"

namespace cta::xrd {

class TapeLsStream : public XrdCtaStream {
public:
  TapeLsStream(const frontend::AdminCmdStream& requestMsg,
               cta::catalogue::Catalogue& catalogue,
               cta::Scheduler& scheduler);

private:
  static constexpr const char* const LOG_SUFFIX = "TapeLsStream";
};

TapeLsStream::TapeLsStream(const frontend::AdminCmdStream& requestMsg,
                           cta::catalogue::Catalogue& catalogue,
                           cta::Scheduler& scheduler)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::cmdline::TapeLsResponseStream>(catalogue,
                                                                        scheduler,
                                                                        requestMsg.getInstanceName(),
                                                                        requestMsg.getAdminCmd(),
                                                                        requestMsg.getMissingFileCopiesMinAgeSecs())) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

} // namespace cta::xrd
