/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <regex>
#include <string>
#include <tuple>

namespace cta::utils {

/// Split a URL into its components: scheme, authority, path, and query string.
/// The authority is empty when the URL has none.
/// @param url The URL to split.
/// @return An optional tuple containing the scheme, authority, path, and query string if the URL is valid;
///         `std::nullopt` otherwise.
std::optional<std::tuple<std::string, std::string, std::string, std::string>> splitURL(const std::string& url);

}  // namespace cta::utils
