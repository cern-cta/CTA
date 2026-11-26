/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/utils/Urls.hpp"

namespace cta::utils {

inline const std::regex URL_REGEX(R"(^([^:/?#]+):(?://([^/?#]*))?([^?#]*)(?:\?([^#]*))?(?:#(.*))?)");

std::optional<std::tuple<std::string, std::string, std::string, std::string>> splitURL(const std::string& url) {
  if (std::smatch matches; std::regex_match(url, matches, URL_REGEX)) {
    return std::make_tuple(matches[1], matches[2], matches[3], matches[4]);
  }
  return std::nullopt;
}

}  // namespace cta::utils
