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

#include "common/exception/Exception.hpp"
#include "common/utils/StringConversions.hpp"
//#include "common/utils/strerror_r_wrapper.hpp"
#include "common/utils/ErrorUtils.hpp"

#include <limits>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <charconv>

using cta::exception::Exception;

namespace cta::utils {

//------------------------------------------------------------------------------
// toUint8
//------------------------------------------------------------------------------
uint8_t toUint8(std::string_view str) {
  if (str.empty()) {
    throw exception::Exception(
      "Failed to convert empty string to uint8_t: An empty string is not a valid unsigned integer");
  }

  uint8_t value;
  auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), value);

  if (ec == std::errc::invalid_argument) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint8_t: Invalid number format";
    throw exception::Exception(msg.str());
  } else if (ec == std::errc::result_out_of_range || value > std::numeric_limits<uint8_t>::max()) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint8_t: Value out of range";
    throw exception::Exception(msg.str());
  } else if (ptr != str.data() + str.size()) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint8_t: Extra characters found";
    throw exception::Exception(msg.str());
  }
  return value;
}

//------------------------------------------------------------------------------
// toUint16
//------------------------------------------------------------------------------
uint16_t toUint16(std::string_view str) {
  if (str.empty()) {
    throw exception::Exception(
      "Failed to convert empty string to uint16_t: An empty string is not a valid unsigned integer");
  }

  uint16_t value;
  auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), value);

  if (ec == std::errc::invalid_argument) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint16_t: Invalid number format";
    throw exception::Exception(msg.str());
  }
  if (ec == std::errc::result_out_of_range || value > std::numeric_limits<uint16_t>::max()) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint16_t: Number too big";
    throw exception::Exception(msg.str());
  }
  if (ptr != str.data() + str.size()) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint16_t: Extra characters found";
    throw exception::Exception(msg.str());
  }

  return value;
}

uint32_t toUint32(std::string_view str) {
  if (str.empty()) {
    throw exception::Exception(
      "Failed to convert empty string to uint32_t: An empty string is not a valid unsigned integer");
  }

  uint32_t value;
  auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), value);

  if (ec == std::errc::invalid_argument) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint32_t: Invalid number format";
    throw exception::Exception(msg.str());
  } else if (ec == std::errc::result_out_of_range || value > std::numeric_limits<uint32_t>::max()) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint32_t: Value out of range";
    throw exception::Exception(msg.str());
  } else if (ptr != str.data() + str.size()) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint32_t: Extra characters found";
    throw exception::Exception(msg.str());
  }

  return value;
}

uint64_t toUint64(std::string_view str) {
  if (str.empty()) {
    throw exception::Exception(
      "Failed to convert empty string to uint64_t: An empty string is not a valid unsigned integer");
  }
  uint64_t value;
  auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), value);

  if (ec == std::errc::invalid_argument) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint64_t: Invalid number format";
    throw exception::Exception(msg.str());
  } else if (ec == std::errc::result_out_of_range || value > std::numeric_limits<uint64_t>::max()) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint64_t: Value out of range";
    throw exception::Exception(msg.str());
  } else if (ptr != str.data() + str.size()) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint64_t: Extra characters found";
    throw exception::Exception(msg.str());
  }
  return value;
}

double toDouble(std::string_view str) {
  if (str.empty()) {
    throw exception::Exception("Failed to convert empty string to double: An empty string is not a valid number");
  }

  double value;
  auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), value);

  if (ec == std::errc::invalid_argument) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to double: Invalid number format";
    throw exception::Exception(msg.str());
  } else if (ec == std::errc::result_out_of_range) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to double: Value out of range";
    throw exception::Exception(msg.str());
  } else if (ptr != str.data() + str.size()) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to double: Extra characters found";
    throw exception::Exception(msg.str());
  }

  return value;
}

//------------------------------------------------------------------------------
// toUid
//------------------------------------------------------------------------------
uid_t toUid(const std::string& str) {
  if (str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to uid_t: An empty string is not"
           " a valid uid_t value";
    throw exception::Exception(msg.str());
  }

  errno = 0;
  const long int value = strtol(str.c_str(), (char**) nullptr, 10);
  const int savedErrno = errno;
  if (savedErrno) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uid_t: " << errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  if (0 > value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uid_t: Negative number";
    throw exception::Exception(msg.str());
  }

  if (std::numeric_limits<uid_t>::max() < value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uid_t: Number too big";
    throw exception::Exception(msg.str());
  }

  return value;
}

//------------------------------------------------------------------------------
// toGid
//------------------------------------------------------------------------------
gid_t toGid(const std::string& str) {
  if (str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to gid_t: An empty string is not"
           " a valid gid_t value";
    throw exception::Exception(msg.str());
  }

  errno = 0;
  const long int value = strtol(str.c_str(), (char**) nullptr, 10);
  const int savedErrno = errno;
  if (savedErrno) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to gid_t: " << errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  if (0 > value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to gid_t: Negative number";
    throw exception::Exception(msg.str());
  }

  if (std::numeric_limits<gid_t>::max() < value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to gid_t: Number too big";
    throw exception::Exception(msg.str());
  }

  return value;
}

//------------------------------------------------------------------------------
// isValidUInt
//------------------------------------------------------------------------------
bool isValidUInt(const std::string& str) {
  // An empty string is not a valid unsigned integer
  if (str.empty()) {
    return false;
  }

  // For each character in the string
  for (std::string::const_iterator itor = str.begin(); itor != str.end(); ++itor) {
    // If the current character is not a valid numerical digit
    if (*itor < '0' || *itor > '9') {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// isValidDecimal
//------------------------------------------------------------------------------
bool isValidDecimal(const std::string& str) {
  // An empty string is not a valid decimal
  if (str.empty()) {
    return false;
  }

  uint64_t nbDecimalPoints = 0;

  // For each character in the string
  for (std::string::const_iterator itor = str.begin(); itor != str.end(); ++itor) {
    const bool isFirstChar = itor == str.begin();
    const bool isMinusChar = '-' == *itor;
    const bool isANumericalDigit = '0' <= *itor && *itor <= '9';
    const bool isADecimalPoint = '.' == *itor;

    if (!(isFirstChar && isMinusChar) && !isANumericalDigit && !isADecimalPoint) {
      return false;
    }

    if (isADecimalPoint) {
      nbDecimalPoints++;
    }

    if (1 < nbDecimalPoints) {
      return false;
    }
  }

  return true;
}

//------------------------------------------------------------------------------
// toUpper
//------------------------------------------------------------------------------
void toUpper(std::string& str) {
  for (std::string::iterator itor = str.begin(); itor != str.end(); ++itor) {
    *itor = toupper(*itor);
  }
}

//------------------------------------------------------------------------------
// isUpper
//------------------------------------------------------------------------------
bool isUpper(const std::string& str) {
  std::string str_upper = str;
  toUpper(str_upper);
  return str_upper == str;
}

//------------------------------------------------------------------------------
// toLower
//------------------------------------------------------------------------------
void toLower(std::string& str) {
  for (std::string::iterator itor = str.begin(); itor != str.end(); ++itor) {
    *itor = tolower(*itor);
  }
}
}  // namespace cta::utils
