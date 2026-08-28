/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <string>
#include <vector>

namespace cta::taped::utils {

/// @brief Return matching cta-taped TOML paths in /etc/cta/ in lexical order.
///
/// Exclude cta-taped.example.toml; filesystem errors propagate.
std::vector<std::string> getTapedConfigPaths();

/// @brief Return `/etc/cta/cta-taped-<driveName>.toml`, or the first sorted match when no drive name is given.
///
/// Throw if the requested path does not exist or no configuration matches; filesystem errors propagate.
std::string getFirstTapedConfigPath(const std::optional<std::string>& driveName);

/// @brief Load the first sorted configuration and return its drive name.
///
/// Missing configuration, parsing and validation failures propagate.
std::string getFirstDriveName();

}  // namespace cta::taped::utils
