/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace cta::disk_system {

/// Longest failure reason sent to a disk system; the most recent end of the reason is kept.
constexpr std::size_t MAX_REPORTED_FAILURE_LENGTH = 1024;

/// Replaces the query string and fragment of a URL in a reported failure reason.
inline constexpr std::string_view REDACTED_QUERY = "?<...>";

/// Replaces the user information, such as `user:password@`, of a URL in a reported failure reason.
inline constexpr std::string_view REDACTED_USERINFO = "<...>@";

/// Prepare a failure reason for a disk system: the user information, query string and fragment of each URL in the
/// text, which may carry credentials, are replaced with REDACTED_USERINFO and REDACTED_QUERY, control characters such
/// as newlines are replaced with spaces, and the reason is truncated to its last MAX_REPORTED_FAILURE_LENGTH
/// characters.
std::string sanitizeFailureReason(std::string_view failureReason);

/// The URL used to report a failure to a disk system. Disk systems that accept a failure reason end the error report
/// URL with an empty query parameter, such as `mgm.errmsg=` (EOS) or `error=` (dCache): the sanitised, base64-encoded
/// reason is appended to it. Other URLs, and any URL when the reason is empty, are returned unchanged.
std::string failureReportURL(std::string_view errorReportURL, std::string_view failureReason);

}  // namespace cta::disk_system
