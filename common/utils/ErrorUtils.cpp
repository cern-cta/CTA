/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */


#include "common/utils/strerror_r_wrapper.hpp"
#include "common/utils/ErrorUtils.hpp"
#include <sstream>
#include <stdint.h>

namespace cta::utils {
//------------------------------------------------------------------------------
// errnoToString
//------------------------------------------------------------------------------
std::string errnoToString(const int errnoValue) {
  char buf[100];

  if (!strerror_r_wrapper(errnoValue, buf, sizeof(buf))) {
    return buf;
  } else {
    const int errnoSetByStrerror_r_wrapper = errno;
    std::ostringstream oss;

    switch (errnoSetByStrerror_r_wrapper) {
      case EINVAL:
        oss << "Failed to convert errnoValue to string: Invalid errnoValue"
               ": errnoValue="
            << errnoValue;
        break;
      case ERANGE:
        oss << "Failed to convert errnoValue to string"
               ": Destination buffer for error string is too small"
               ": errnoValue="
            << errnoValue;
        break;
      default:
        oss << "Failed to convert errnoValue to string"
               ": strerror_r_wrapper failed in an unknown way"
               ": errnoValue="
            << errnoValue;
        break;
    }

    return oss.str();
  }
}
}  // namespace cta::utils
