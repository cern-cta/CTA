/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "xroot_plugins/XrdCtaStream.hpp"
#include "cmdline/ShowQueuesResponseStream.hpp"

namespace cta::xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class ShowQueuesStream: public XrdCtaStream{
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  ShowQueuesStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue& catalogue,
    cta::Scheduler& scheduler, log::LogContext& lc);

private:
  static constexpr const char* const LOG_SUFFIX  = "ShowQueuesStream";                   //!< Identifier for log messages
};

ShowQueuesStream::ShowQueuesStream(const frontend::AdminCmdStream& requestMsg,
                                   cta::catalogue::Catalogue& catalogue,
                                   cta::Scheduler& scheduler,
                                   log::LogContext& lc)
    : XrdCtaStream(catalogue,
                   scheduler,
                   std::make_unique<cta::cmdline::ShowQueuesResponseStream>(catalogue,
                                                                            scheduler,
                                                                            requestMsg.getInstanceName(),
                                                                            requestMsg.getAdminCmd(),
                                                                            lc)) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, " constructor");
}

} // namespace cta::xrd
