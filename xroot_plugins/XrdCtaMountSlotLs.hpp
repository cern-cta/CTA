/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "frontend/common/MountSlotLsResponseStream.hpp"
#include "xroot_plugins/XrdCtaStream.hpp"

namespace cta::xrd {

/*!
 * Stream object which implements "mountslot ls" command
 */
class MountSlotLsStream : public XrdCtaStream {
public:
  MountSlotLsStream(const frontend::AdminCmdStream& requestMsg,
                    cta::catalogue::Catalogue& catalogue,
                    cta::Scheduler& scheduler,
                    cta::SchedulerDatabase& schedulerDb,
                    log::LogContext& lc);

private:
  static constexpr const char* const LOG_SUFFIX = "MountSlotLsStream";
};

inline MountSlotLsStream::MountSlotLsStream(const frontend::AdminCmdStream& requestMsg,
                                            cta::catalogue::Catalogue& catalogue,
                                            cta::Scheduler& scheduler,
                                            cta::SchedulerDatabase& schedulerDb,
                                            log::LogContext& lc)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::frontend::MountSlotLsResponseStream>(catalogue,
                                                                              scheduler,
                                                                              schedulerDb,
                                                                              requestMsg.getInstanceName(),
                                                                              lc)) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

}  // namespace cta::xrd
