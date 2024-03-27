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

namespace cta::log {

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &Param::getName() const noexcept {
  return m_name;
}

//------------------------------------------------------------------------------
// getVariant
//------------------------------------------------------------------------------
const ParamValType &Param::getVariant() const noexcept {
  return m_value;
}

//------------------------------------------------------------------------------
// getValue
//------------------------------------------------------------------------------
std::string Param::getValue() const noexcept {
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
  oss << "\"" << m_name << "\":";
  if (m_value.has_value()) {
    std::visit([&oss](auto &&arg) {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, bool>) {
        oss << (arg ? "true" : "false");
      } else if constexpr (std::is_same_v<T, std::string>) {
        oss << "\"" << arg << "\"";
      } else if constexpr (std::is_same_v<T, int64_t>) {
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

} // namespace cta::log
