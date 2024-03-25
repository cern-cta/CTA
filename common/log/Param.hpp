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

#pragma once

#include <cstdio>
#include <optional>
#include <variant>
#include <sstream>
#include <string.h>

namespace cta::log {

/**
 * A name/value parameter for the CASTOR logging system.
 */
class Param {

public:

  /**
   * A JSON element can be one of the following types (https://www.json.org/json-en.html):
   *  - object
   *  - array
   *  - string
   *  - number
   *  - `true`
   *  - `false`
   *  - `null`
   *
   *  In order to be able to preserve to correctly log there types - and, in addition, distinguish floats from
   *  integers - we will support the following types in a variant variable:
   *  - string
   *  - int64_t
   *  - double
   *  - bool
   *  - nullopt_t
   */
  using VarValueType = std::variant<std::string, int64_t, double, bool, std::nullopt_t>;

  /**
   * Constructor.
   *
   * @param name The name of the parameter.
   * @param value The value of the parameter that will be converted to a string
   * using std::ostringstream.
   */
  template <typename T> Param(std::string_view name, const T &value) noexcept: m_name(name) {
    setValue(value);
  }

  /**
   * Value changer. Useful for log contexts.
   * @param value
   */
  template <typename T>
  void setValue (const T &value) noexcept {
    if constexpr (is_optional_type<T>::value) {
      if (value.has_value()) {
        setValue(value.value());
      } else {
        setValue(std::nullopt);
      }
    } else if constexpr (std::is_same_v<T, bool>) {
      std::stringstream oss;
      oss << value;
      m_value = oss.str();
      m_value_v = static_cast<bool>(value);
    } else if constexpr (std::is_integral_v<T>) {
      std::stringstream oss;
      oss << value;
      m_value = oss.str();
      m_value_v = static_cast<int64_t>(value);
    } else if constexpr (std::is_floating_point_v<T>) {
      std::stringstream oss;
      oss << value;
      m_value = oss.str();
      m_value_v = static_cast<double>(value);
    } else if constexpr (std::is_same_v<T, std::nullopt_t>) {
      m_value = "";
      m_value_v = std::nullopt;
    } else {
      std::stringstream oss;
      oss << value;
      m_value = oss.str();
      m_value_v = oss.str();
    }
  }

  /**
   * Returns a const reference to the name of the parameter.
   */
  const std::string &getName() const noexcept;

  /**
   * Returns a const reference to the value of the parameter.
   */
  const std::string &getValue() const noexcept;

  /**
   * Returns a const reference to the value of the parameter.
   */
  const VarValueType &getVarValue() const noexcept;

protected:

  /**
   * `is_optional_type<T>` will be used to detect, at compile time, if a variable is of type `std::optional`
   */
  // Assume T is not std::optional by defaulrt
  template<typename T>
  struct is_optional_type : std::false_type {};

  // Specialization for std::optional<T>
  template<typename T>
  struct is_optional_type<std::optional<T>> : std::true_type {};

  /**
   * Name of the parameter
   */
  std::string m_name;

  /**
   * The value of the parameter.
   */
  std::string m_value;

  /**
   * The value of the parameter in the original type
   */
  VarValueType m_value_v;

}; // class Param

/**
 * An helper class allowing the construction of a Param class with sprintf
 * formatting for a double.
 */
class ParamDoubleSnprintf: public Param {
public:
  ParamDoubleSnprintf(std::string_view name, const double value);
}; // class ParamDoubleSnprintf

} // namespace cta::log
