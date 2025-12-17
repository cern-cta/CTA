/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "XrdCtaStream.hpp"
#include "frontend/common/TapePoolLsResponseStream.hpp"

namespace cta::xrd {

class TapePoolLsStream : public XrdCtaStream {
public:
  TapePoolLsStream(const frontend::AdminCmdStream& requestMsg,
                   cta::catalogue::Catalogue& catalogue,
                   cta::Scheduler& scheduler);

private:
  static constexpr const char* const LOG_SUFFIX = "TapePoolLsStream";
};

inline TapePoolLsStream::TapePoolLsStream(const frontend::AdminCmdStream& requestMsg,
                                          cta::catalogue::Catalogue& catalogue,
                                          cta::Scheduler& scheduler)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::frontend::TapePoolLsResponseStream>(catalogue,
                                                                             scheduler,
                                                                             requestMsg.getInstanceName(),
                                                                             requestMsg.getAdminCmd())) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

}  // namespace cta::xrd
