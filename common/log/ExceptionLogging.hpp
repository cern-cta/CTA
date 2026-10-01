/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/Constants.hpp"

#include <source_location>
#include <string_view>

namespace cta::log {
class LogContext;

// Log the exception currently handled by a catch block without replacing it if diagnostics fail.
// Without an active exception, log a fallback diagnostic instead.
void logCurrentExceptionNoThrow(LogContext& lc,
                                std::string_view message,
                                int priority = ERR,
                                std::source_location location = std::source_location::current()) noexcept;
}  // namespace cta::log
