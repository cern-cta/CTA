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

#include "common/log/Param.hpp"

#include <iostream>
#include <iomanip>
#include <algorithm>

namespace cta::log {

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &Param::getName() const noexcept {
  return m_name;
}

//------------------------------------------------------------------------------
// getValueVariant
//------------------------------------------------------------------------------
const ParamValType &Param::getValueVariant() const noexcept {
  return m_value;
}

//------------------------------------------------------------------------------
// getValueStr
//------------------------------------------------------------------------------
std::string Param::getValueStr() const noexcept {
  std::ostringstream oss;
  if (m_value.has_value()) {
    std::visit([&oss](auto &&arg) {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_floating_point_v<T>) {
        oss << floatingPointFormatting(arg);
      } else {
        oss << arg;
      }
    }, m_value.value());
  } else {
    oss << "";
  }
  return oss.str();
}

//------------------------------------------------------------------------------
// getKeyValueJSON
//------------------------------------------------------------------------------
std::string Param::getKeyValueJSON() const noexcept {
  std::ostringstream oss;
  oss << "\"" << stringFormattingJSON(m_name) << "\":";
  if (m_value.has_value()) {
    std::visit([&oss](auto &&arg) {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, bool>) {
        oss << (arg ? "true" : "false");
      } else if constexpr (std::is_same_v<T, std::string>) {
        oss << "\"" << stringFormattingJSON(arg) << "\"";
      } else if constexpr (std::is_integral_v<T>) {
        oss << arg;
      } else if constexpr (std::is_floating_point_v<T>) {
        oss << floatingPointFormatting(arg);
      } else {
        static_assert(always_false<T>::value, "Type not supported");
      }
    }, m_value.value());
  } else {
    oss << "null";
  }
  return oss.str();
}

template<>
void Param::setValue<ParamValType>(const ParamValType& value) noexcept {
  m_value = value;
}

//------------------------------------------------------------------------------
// stringFormattingJSON nested class
//------------------------------------------------------------------------------
Param::stringFormattingJSON::stringFormattingJSON(const std::string& str) : m_value(str) {}

//------------------------------------------------------------------------------
// stringFormattingJSON << operator overload
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& oss, const Param::stringFormattingJSON& fp) {
  std::ostringstream oss_tmp;
  for (char c : fp.m_value) {
    switch (c) {
    case '\"': oss_tmp << R"(\")"; break;
    case '\\': oss_tmp << R"(\\)"; break;
    case '\b': oss_tmp << R"(\b)"; break;
    case '\f': oss_tmp << R"(\f)"; break;
    case '\n': oss_tmp << R"(\n)"; break;
    case '\r': oss_tmp << R"(\r)"; break;
    case '\t': oss_tmp << R"(\t)"; break;
    default:
      if ('\x00' <= c && c <= '\x1f') {
        oss_tmp << R"(\u)" << std::hex << std::setw(4) << std::setfill('0') << static_cast<unsigned int>(c);
      } else {
        oss_tmp << c;
      }
    }
  }
  oss << oss_tmp.str();
  return oss;
}

} // namespace cta::log
