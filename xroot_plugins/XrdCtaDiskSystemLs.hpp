/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "XrdCtaStream.hpp"
#include "frontend/common/DiskSystemLsResponseStream.hpp"

namespace cta::xrd {

class DiskSystemLsStream : public XrdCtaStream {
public:
  DiskSystemLsStream(const frontend::AdminCmdStream& requestMsg,
                     cta::catalogue::Catalogue& catalogue,
                     cta::Scheduler& scheduler);

private:
  static constexpr const char* const LOG_SUFFIX = "DiskSystemLsStream";
};

inline DiskSystemLsStream::DiskSystemLsStream(const frontend::AdminCmdStream& requestMsg,
                                              cta::catalogue::Catalogue& catalogue,
                                              cta::Scheduler& scheduler)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::frontend::DiskSystemLsResponseStream>(catalogue,
                                                                               scheduler,
                                                                               requestMsg.getInstanceName())) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

}  // namespace cta::xrd
