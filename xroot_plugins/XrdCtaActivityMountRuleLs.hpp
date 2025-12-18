/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "XrdCtaStream.hpp"
#include "frontend/common/ActivityMountRuleLsResponseStream.hpp"

namespace cta::xrd {

class ActivityMountRuleLsStream : public XrdCtaStream {
public:
  ActivityMountRuleLsStream(const frontend::AdminCmdStream& requestMsg,
                            cta::catalogue::Catalogue& catalogue,
                            cta::Scheduler& scheduler);

private:
  static constexpr const char* const LOG_SUFFIX = "ActivityMountRuleLsStream";
};

inline ActivityMountRuleLsStream::ActivityMountRuleLsStream(const frontend::AdminCmdStream& requestMsg,
                                                            cta::catalogue::Catalogue& catalogue,
                                                            cta::Scheduler& scheduler)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::frontend::ActivityMountRuleLsResponseStream>(catalogue,
                                                                                      scheduler,
                                                                                      requestMsg.getInstanceName())) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

}  // namespace cta::xrd
