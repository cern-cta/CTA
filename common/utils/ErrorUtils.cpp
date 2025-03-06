/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
