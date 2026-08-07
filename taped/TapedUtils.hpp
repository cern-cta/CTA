/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"

#include <optional>
#include <string>
#include <vector>

namespace cta::taped::utils {

std::vector<std::string> getTapedConfigPaths();

std::string constructProcessName(const std::string& driveName, const std::string& postfix, cta::log::LogContext& lc);

std::string getFirstTapedConfigPath(const std::optional<std::string>& driveName);

std::string getFirstDriveName();

}  // namespace cta::taped::utils
