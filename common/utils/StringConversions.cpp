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

using cta::exception::Exception;

namespace cta::utils {

//------------------------------------------------------------------------------
// toPGUint8
//------------------------------------------------------------------------------
uint8_t toPGUint8(std::string_view str) {
  if (str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to uint8_t: An empty string is not a valid unsigned integer";
    throw exception::Exception(msg.str());
  }

  try {
    unsigned long value = std::stoul(std::string(str));

    if (value > std::numeric_limits<uint8_t>::max()) {
      std::ostringstream msg;
      msg << "Failed to convert '" << str << "' to uint8_t: Number too big";
      throw exception::Exception(msg.str());
    }

    return static_cast<uint8_t>(value);
  } catch (const std::invalid_argument&) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint8_t: Invalid number format";
    throw exception::Exception(msg.str());
  } catch (const std::out_of_range&) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint8_t: Value out of range";
    throw exception::Exception(msg.str());
  }
}

//------------------------------------------------------------------------------
// toUint16
//------------------------------------------------------------------------------
uint16_t toPGUint16(std::string_view str) {
  if (str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to uint16_t: An empty string is not a valid unsigned integer";
    throw exception::Exception(msg.str());
  }

  try {
    unsigned long value = std::stoul(std::string(str));

    if (value > std::numeric_limits<uint16_t>::max()) {
      std::ostringstream msg;
      msg << "Failed to convert '" << str << "' to uint16_t: Number too big";
      throw exception::Exception(msg.str());
    }

    return static_cast<uint16_t>(value);
  } catch (const std::invalid_argument&) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint16_t: Invalid number format";
    throw exception::Exception(msg.str());
  } catch (const std::out_of_range&) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint16_t: Value out of range";
    throw exception::Exception(msg.str());
  }
}

uint32_t toPGUint32(std::string_view str) {
  if (str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to uint32_t: An empty string is not a valid unsigned integer";
    throw exception::Exception(msg.str());
  }

  try {
    unsigned long value = std::stoul(std::string(str));

    if (value > std::numeric_limits<uint32_t>::max()) {
      std::ostringstream msg;
      msg << "Failed to convert '" << str << "' to uint32_t: Number too big";
      throw exception::Exception(msg.str());
    }

    return static_cast<uint32_t>(value);
  } catch (const std::invalid_argument&) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint32_t: Invalid number format";
    throw exception::Exception(msg.str());
  } catch (const std::out_of_range&) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint32_t: Value out of range";
    throw exception::Exception(msg.str());
  }
}

uint64_t toPGUint64(std::string_view str) {
  if (str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to uint64_t: An empty string is not a valid unsigned integer";
    throw exception::Exception(msg.str());
  }

  try {
    unsigned long long value = std::stoull(std::string(str));

    if (value > std::numeric_limits<uint64_t>::max()) {
      std::ostringstream msg;
      msg << "Failed to convert '" << str << "' to uint64_t: Number too big";
      throw exception::Exception(msg.str());
    }

    return static_cast<uint64_t>(value);
  } catch (const std::invalid_argument&) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint64_t: Invalid number format";
    throw exception::Exception(msg.str());
  } catch (const std::out_of_range&) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to uint64_t: Value out of range";
    throw exception::Exception(msg.str());
  }
}

double toPGDouble(std::string_view str) {
  if (str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to double: An empty string is not a valid number";
    throw exception::Exception(msg.str());
  }
  try {
    return std::stod(std::string(str));
  } catch (const std::invalid_argument&) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to double: Invalid number format";
    throw exception::Exception(msg.str());
  } catch (const std::out_of_range&) {
    std::ostringstream msg;
    msg << "Failed to convert '" << str << "' to double: Value out of range";
    throw exception::Exception(msg.str());
  }
}

//------------------------------------------------------------------------------
// toUint8
//------------------------------------------------------------------------------
uint8_t toUint8(const std::string& str) {
  if (str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to uint8_t: An empty string is not"
           " a valid unsigned integer";
    throw exception::Exception(msg.str());
  }

  errno = 0;
  const long int value = strtol(str.c_str(), (char**) nullptr, 10);
  const int savedErrno = errno;
  if (savedErrno) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint8_t: " << errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  if (0 > value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint8_t: Negative number";
    throw exception::Exception(msg.str());
  }

  if (255 < value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint8_t: Number too big";
    throw exception::Exception(msg.str());
  }

  return value;
}

//------------------------------------------------------------------------------
// toUint16
//------------------------------------------------------------------------------
uint16_t toUint16(const std::string& str) {
  if (str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to uint16_t: An empty string is not"
           " a valid unsigned integer";
    throw exception::Exception(msg.str());
  }

  errno = 0;
  const long int value = strtol(str.c_str(), (char**) nullptr, 10);
  const int savedErrno = errno;
  if (savedErrno) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint16_t: " << errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  if (0 > value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint16_t: Negative number";
    throw exception::Exception(msg.str());
  }

  if (65535 < value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint16_t: Number too big";
    throw exception::Exception(msg.str());
  }

  return value;
}

//------------------------------------------------------------------------------
// toUint32
//------------------------------------------------------------------------------
uint32_t toUint32(const std::string& str) {
  if (str.empty()) {
    std::ostringstream msg;
    msg << "Failed to convert empty string to uint32_t: An empty string is not"
           " a valid unsigned integer";
    throw exception::Exception(msg.str());
  }

  errno = 0;
  const long int value = strtol(str.c_str(), (char**) nullptr, 10);
  const int savedErrno = errno;
  if (savedErrno) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint32_t: " << errnoToString(savedErrno);
    throw exception::Exception(msg.str());
  }

  if (0 > value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint32_t: Negative number";
    throw exception::Exception(msg.str());
  }

  if (4294967295 < value) {
    std::ostringstream msg;
    msg << "Failed to convert \'" << str << "' to uint32_t: Number too big";
    throw exception::Exception(msg.str());
  }

  return value;
}

//------------------------------------------------------------------------------
// toUint64
//------------------------------------------------------------------------------
uint64_t toUint64(const std::string& str) {
  try {
    try {
      return std::stoul(str);
    } catch (std::invalid_argument&) {
      throw exception::Exception("Invalid uint64");
    } catch (std::out_of_range&) {
      throw exception::Exception("Out of range");
    } catch (std::exception& se) {
      throw exception::Exception(se.what());
    }
  } catch (exception::Exception& ex) {
    throw exception::Exception(std::string("Failed to parse ") + str +
                               " as an unsigned 64-bit integer: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// toDouble
//------------------------------------------------------------------------------
double toDouble(const std::string& str) {
  try {
    try {
      return std::stod(str);
    } catch (std::invalid_argument&) {
      throw exception::Exception("Invalid double");
    } catch (std::out_of_range&) {
      throw exception::Exception("Out of range");
    } catch (std::exception& se) {
      throw exception::Exception(se.what());
    }
  } catch (exception::Exception& ex) {
    throw exception::Exception(std::string("Failed to parse ") + str + " as a double: " + ex.getMessage().str());
  }
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
