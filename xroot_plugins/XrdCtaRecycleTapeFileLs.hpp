/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "XrdCtaStream.hpp"
#include "frontend/common/RecycleTapeFileLsResponseStream.hpp"

namespace cta::xrd {

class RecycleTapeFileLsStream : public XrdCtaStream {
public:
  RecycleTapeFileLsStream(const frontend::AdminCmdStream& requestMsg,
                          cta::catalogue::Catalogue& catalogue,
                          cta::Scheduler& scheduler);

private:
  static constexpr const char* const LOG_SUFFIX = "RecycleTapeFileLsStream";
};

inline RecycleTapeFileLsStream::RecycleTapeFileLsStream(const frontend::AdminCmdStream& requestMsg,
                                                        cta::catalogue::Catalogue& catalogue,
                                                        cta::Scheduler& scheduler)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::frontend::RecycleTapeFileLsResponseStream>(catalogue,
                                                                                    scheduler,
                                                                                    requestMsg.getInstanceName(),
                                                                                    requestMsg.getAdminCmd())) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

}  // namespace cta::xrd
