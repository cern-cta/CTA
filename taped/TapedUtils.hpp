/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <string>
#include <vector>

namespace cta::taped::utils {

/**
 * @brief List matching cta-taped TOML files in /etc/cta/ in alphabetical order.
 *
 * Exclude cta-taped.example.toml; filesystem errors propagate.
 *
 * @return Matching configuration paths sorted lexically.
 */
std::vector<std::string> getTapedConfigPaths();

/**
 * @brief Find the named drive configuration, or the first matching configuration when no drive is given.
 *
 * @param driveName Optional drive name; std::nullopt selects the first matching configuration.
 * @return Existing configuration path for the requested drive, or the first matching path.
 * @throws cta::exception::Exception If the requested file or any matching configuration is absent.
 */
std::string getFirstTapedConfigPath(const std::optional<std::string>& driveName);

/**
 * @brief Load the first matching configuration and return its configured drive name.
 *
 * Missing configuration and parsing failures propagate.
 *
 * @return Drive name loaded from the first matching configuration.
 */
std::string getFirstDriveName();

}  // namespace cta::taped::utils
