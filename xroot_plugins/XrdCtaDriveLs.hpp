/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "XrdCtaStream.hpp"
#include "frontend/common/DriveLsResponseStream.hpp"

namespace cta::xrd {

class DriveLsStream : public XrdCtaStream {
public:
  DriveLsStream(const frontend::AdminCmdStream& requestMsg,
                cta::catalogue::Catalogue& catalogue,
                cta::Scheduler& scheduler,
                cta::log::LogContext& lc);

private:
  static constexpr const char* const LOG_SUFFIX = "DriveLsStream";
};

inline DriveLsStream::DriveLsStream(const frontend::AdminCmdStream& requestMsg,
                                    cta::catalogue::Catalogue& catalogue,
                                    cta::Scheduler& scheduler,
                                    cta::log::LogContext& lc)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::frontend::DriveLsResponseStream>(catalogue,
                                                                          scheduler,
                                                                          requestMsg.getInstanceName(),
                                                                          requestMsg.getAdminCmd(),
                                                                          lc)) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

}  // namespace cta::xrd
