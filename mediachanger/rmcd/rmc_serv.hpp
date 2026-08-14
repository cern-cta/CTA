/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <string>

namespace cta::log {
class LogContext;
}

int rmc_main(const std::string& robot, int port, const std::string& listen_scope, cta::log::LogContext& lc);
