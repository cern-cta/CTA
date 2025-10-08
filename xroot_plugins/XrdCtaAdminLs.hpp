/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "XrdCtaStream.hpp"
#include "cmdline/AdminLsResponseStream.hpp"

namespace cta::xrd {

class AdminLsStream : public XrdCtaStream {
public:
  AdminLsStream(const frontend::AdminCmdStream& requestMsg,
                cta::catalogue::Catalogue& catalogue,
                cta::Scheduler& scheduler);

private:
  static constexpr const char* const LOG_SUFFIX = "AdminLsStream";
};

AdminLsStream::AdminLsStream(const frontend::AdminCmdStream& requestMsg,
                             cta::catalogue::Catalogue& catalogue,
                             cta::Scheduler& scheduler)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::cmdline::AdminLsResponseStream>(catalogue,
                                                                         scheduler,
                                                                         requestMsg.getInstanceName(),
                                                                         requestMsg.getAdminCmd())) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

} // namespace cta::xrd
