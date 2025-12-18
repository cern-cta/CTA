/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "XrdCtaStream.hpp"
#include "frontend/common/FailedRequestLsResponseStream.hpp"

namespace cta::xrd {

class FailedRequestLsStream : public XrdCtaStream {
public:
  FailedRequestLsStream(const frontend::AdminCmdStream& requestMsg,
                        cta::catalogue::Catalogue& catalogue,
                        cta::Scheduler& scheduler,
                        SchedulerDatabase& schedDb,
                        cta::log::LogContext& lc);

private:
  static constexpr const char* const LOG_SUFFIX = "FailedRequestLsStream";
};

inline FailedRequestLsStream::FailedRequestLsStream(const frontend::AdminCmdStream& requestMsg,
                                                    cta::catalogue::Catalogue& catalogue,
                                                    cta::Scheduler& scheduler,
                                                    SchedulerDatabase& schedDb,
                                                    cta::log::LogContext& lc)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::frontend::FailedRequestLsResponseStream>(catalogue,
                                                                                  scheduler,
                                                                                  requestMsg.getInstanceName(),
                                                                                  requestMsg.getAdminCmd(),
                                                                                  schedDb,
                                                                                  lc)) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

}  // namespace cta::xrd
