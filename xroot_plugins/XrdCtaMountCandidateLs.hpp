/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "frontend/common/MountCandidateLsResponseStream.hpp"
#include "xroot_plugins/XrdCtaStream.hpp"

namespace cta::xrd {

/*!
 * Stream object which implements "mountcandidate ls" command
 */
class MountCandidateLsStream : public XrdCtaStream {
public:
  MountCandidateLsStream(const frontend::AdminCmdStream& requestMsg,
                         cta::catalogue::Catalogue& catalogue,
                         cta::Scheduler& scheduler,
                         cta::mountdecision::MountDecisionDB& mountDecisionDb,
                         log::LogContext& lc);

private:
  static constexpr const char* const LOG_SUFFIX = "MountCandidateLsStream";
};

inline MountCandidateLsStream::MountCandidateLsStream(const frontend::AdminCmdStream& requestMsg,
                                                      cta::catalogue::Catalogue& catalogue,
                                                      cta::Scheduler& scheduler,
                                                      cta::mountdecision::MountDecisionDB& mountDecisionDb,
                                                      log::LogContext& lc)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::frontend::MountCandidateLsResponseStream>(catalogue,
                                                                                   scheduler,
                                                                                   mountDecisionDb,
                                                                                   requestMsg.getInstanceName(),
                                                                                   lc)) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

}  // namespace cta::xrd
