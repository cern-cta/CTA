/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "XrdCtaStream.hpp"
#include "cmdline/TapeFileLsResponseStream.hpp"

namespace cta::xrd {

/*!
 * Stream object which implements "tapefile ls" command.
 */
class TapeFileLsStream : public XrdCtaStream {
public:
  TapeFileLsStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler);

private:
  static constexpr const char* const LOG_SUFFIX = "TapeFileLsStream";  //!< Identifier for log messages
};

TapeFileLsStream::TapeFileLsStream(const frontend::AdminCmdStream& requestMsg,
                                   cta::catalogue::Catalogue& catalogue,
                                   cta::Scheduler& scheduler)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::cmdline::TapeFileLsResponseStream>(catalogue,
                                                                            scheduler,
                                                                            requestMsg.getInstanceName(),
                                                                            requestMsg.getAdminCmd())) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

} // namespace cta::xrd
