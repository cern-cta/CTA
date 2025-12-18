/**
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminParsedCmd.hpp"
#include "version.h"

namespace cta::admin {

class CtaAdminXrdCmd {
public:
  //! Send the protocol buffer across the XRootD SSI transport
  void send(const CtaAdminParsedCmd& parsedCmd) const;

private:
  const std::string StreamBufferSize = "1024";     //!< Buffer size for Data/Stream Responses
  const std::string DefaultRequestTimeout = "10";  //!< Default Request Timeout. Can be overridden by
                                                   //!< XRD_REQUESTTIMEOUT environment variable.

  static constexpr const char* const LOG_SUFFIX = "CtaAdminXrdCmd";  //!< Identifier for log messages
};

}  // namespace cta::admin
