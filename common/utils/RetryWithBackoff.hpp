/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/semconv/Logging.hpp"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

namespace cta::utils {

/**
 * Runs fn() and, if it throws a cta::exception::Exception, retries after each delay in
 * delaysBetweenAttempts (sleeping the CALLING thread), re-throwing the last exception once every
 * attempt has been exhausted. Logs a WARNING before each retry and an ERR if all attempts fail.
 *
 * Only ever call this from a background/recovery path that can tolerate blocking for up to the
 * sum of delaysBetweenAttempts - never from a latency-sensitive synchronous path such as a
 * frontend RPC, where a caller is already waiting on a response and a long retry would only
 * delay an eventual client-side failure rather than help it.
 */
template<typename F>
void retryWithBackoff(F&& fn,
                      const std::vector<std::chrono::milliseconds>& delaysBetweenAttempts,
                      cta::log::LogContext& lc,
                      const std::string& description) {
  for (size_t attempt = 0;; ++attempt) {
    try {
      fn();
      return;
    } catch (const cta::exception::Exception& ex) {
      if (attempt >= delaysBetweenAttempts.size()) {
        cta::log::ScopedParamContainer(lc)
          .add(cta::semconv::log::exceptionMessage, ex.getMessageValue())
          .add("attempts", attempt + 1)
          .log(cta::log::ERR, description + ": all retry attempts exhausted, giving up.");
        throw;
      }
      cta::log::ScopedParamContainer(lc)
        .add(cta::semconv::log::exceptionMessage, ex.getMessageValue())
        .add("attempt", attempt + 1)
        .add("nextRetryInMs", delaysBetweenAttempts[attempt].count())
        .log(cta::log::WARNING, description + ": attempt failed, retrying after backoff.");
      std::this_thread::sleep_for(delaysBetweenAttempts[attempt]);
    }
  }
}

}  // namespace cta::utils
