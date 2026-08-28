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
    const auto messageDetail = std::current_exception() ? exception::currentExceptionMessage() : "No active exception";
    params.add(semconv::log::exceptionMessage, messageDetail);
    lc.log(priority, message, location);
  } catch (...) {}
}
}  // namespace cta::log
