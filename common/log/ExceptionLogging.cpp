/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/log/ExceptionLogging.hpp"

#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/semconv/Logging.hpp"

#include <exception>

namespace cta::log {
void logCurrentExceptionNoThrow(LogContext& lc,
                                std::string_view message,
                                int priority,
                                std::source_location location) noexcept {
  try {
    ScopedParamContainer params(lc);
    const auto failure = std::current_exception();
    if (failure) {
      try {
        std::rethrow_exception(failure);
      } catch (const exception::Exception& ex) {
        params.add(semconv::log::exceptionMessage, ex.getMessageValue());
      } catch (const std::exception& ex) {
        params.add(semconv::log::exceptionMessage, ex.what());
      } catch (...) {
        params.add(semconv::log::exceptionMessage, "Unknown exception");
      }
    } else {
      params.add(semconv::log::exceptionMessage, "No active exception");
    }
    lc.log(priority, message, location);
  } catch (...) {}
}
}  // namespace cta::log
