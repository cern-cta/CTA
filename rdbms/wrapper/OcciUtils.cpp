/*
* SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/OcciUtils.hpp"

#include <charconv>
#include <cstring>
#include <set>

namespace cta::rdbms::wrapper {

bool OcciUtils::isLostConnection(std::exception& se) {
  const std::set<int> lostConnectionErrors {
    25401,
  };

  const auto prefix = "ORA-";
  auto pos = std::strstr(se.what(), prefix);
  if (!pos) {
    return false;
  }

  pos += 4;
  if (int errorCode = 0; std::from_chars(pos, pos + 5, errorCode).ec == std::errc {}) {
    return lostConnectionErrors.contains(errorCode);
  }
  return false;
}

}  // namespace cta::rdbms::wrapper
