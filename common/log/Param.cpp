/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

template<>
void Param::setValue<ParamValType>(const ParamValType& value) noexcept {
  m_value = value;
}

} // namespace cta::log
