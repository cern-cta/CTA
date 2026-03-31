/*
* SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/OcciUtils.hpp"

#include <charconv>
#include <cstring>
#include <set>

namespace cta::rdbms::wrapper {

bool OcciUtils::isLostConnection(const std::exception& se) {
  constexpr int errorLength = 5;
  const std::set<int> lostConnectionErrors {
    25401,
  };

  constexpr char prefix[] = "ORA-";
  auto pos = std::strstr(se.what(), prefix);
  if (!pos) {
    return false;
  }

  pos += sizeof(prefix);
  if (int errorCode = 0; std::from_chars(pos, pos + errorLength, errorCode).ec == std::errc {}) {
    return lostConnectionErrors.contains(errorCode);
  }
  return false;
}

}  // namespace cta::rdbms::wrapper
