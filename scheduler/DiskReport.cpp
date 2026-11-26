/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/DiskReport.hpp"

#include "common/utils/Base64.hpp"
#include "common/utils/Urls.hpp"

#include <algorithm>
#include <cctype>
#include <format>
#include <regex>
#include <string_view>

namespace cta::disk_system {

std::string sanitizeFailureReason(std::string_view failureReason) {
  // A URL embedded in free text starts at its scheme and ends at whitespace.
  static const std::regex urlInText(R"([A-Za-z][A-Za-z0-9+.\-]*://\S+)");
  std::string out;
  using UrlIterator = std::regex_iterator<std::string_view::const_iterator>;
  auto last = failureReason.cbegin();
  for (UrlIterator it(failureReason.cbegin(), failureReason.cend(), urlInText), end; it != end; ++it) {  // NOSONAR
    const auto& match = (*it)[0];
    out.append(last, match.first);
    if (const auto parts = utils::splitURL(match.str())) {
      const auto& [scheme, authority, path, query] = *parts;
      const std::size_t unredactedLength = scheme.size() + 3 + authority.size() + path.size();
      // User information before the last '@' of the authority may hold a password.
      if (const auto at = authority.rfind('@'); at != std::string::npos) {
        out.append(std::format("{}://{}{}{}", scheme, REDACTED_USERINFO, authority.substr(at + 1), path));
      } else {
        out.append(std::format("{}://{}{}", scheme, authority, path));
      }
      // Anything past the path is a query string or fragment; mark it so readers know text was removed.
      if (unredactedLength < static_cast<std::size_t>(match.length())) {
        out.append(REDACTED_QUERY);
      }
    } else {
      out.append(match.first, match.second);
    }
    last = match.second;
  }
  out.append(last, failureReason.cend());
  // Disk systems store the reason as a single-line attribute: replace newlines and other control characters.
  std::ranges::replace_if(out, [](unsigned char c) { return std::iscntrl(c) != 0; }, ' ');
  if (out.size() > MAX_REPORTED_FAILURE_LENGTH) {
    out = std::format("...{}", out.substr(out.size() - (MAX_REPORTED_FAILURE_LENGTH - 3)));
  }
  return out;
}

std::string failureReportURL(std::string_view errorReportURL, std::string_view failureReason) {
  if (failureReason.empty() || !errorReportURL.ends_with('=')) {
    return std::string(errorReportURL);
  }
  return std::string(errorReportURL) + utils::base64encode(sanitizeFailureReason(failureReason));
}

}  // namespace cta::disk_system
