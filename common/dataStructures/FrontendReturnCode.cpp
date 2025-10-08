/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FrontendReturnCode.hpp"

namespace cta::common::dataStructures {

std::string toString(FrontendReturnCode rc) {
  switch(rc) {
    case FrontendReturnCode::ok:
      return "ok";
    case FrontendReturnCode::userErrorNoRetry:
      return "userErrorNoRetry";
    case FrontendReturnCode::ctaErrorNoRetry:
      return "ctaErrorNoRetry";
    case FrontendReturnCode::ctaErrorRetry:
      return "ctaErrorRetry";
    case FrontendReturnCode::ctaFrontendTimeout:
      return "ctaFrontendTimeout";
    default:
      return "UNKNOWN";
  }
}

} // namespace cta::common::dataStructures
