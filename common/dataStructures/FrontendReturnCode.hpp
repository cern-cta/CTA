/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

namespace cta::common::dataStructures {

  /**
   * This is used by both CLI and Frontend so it should work for SLC6 as well
   */
enum FrontendReturnCode {
  ok,
  userErrorNoRetry,
  ctaErrorNoRetry,
  ctaErrorRetry,
  ctaFrontendTimeout
};

std::string toString(FrontendReturnCode code);

} // namespace cta::common::dataStructures

