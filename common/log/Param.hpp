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
#include <iomanip>
#include <limits>
#include <string.h>

namespace cta::log {

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
 *  In order to preserve correctly the log parameter types and values - in addition to distinguishing floats from
 *  integers - we will support the following types in a variant variable:
 *  - string
 *  - int64_t
 *  - uint64_t
 *  - double
 *  - float
 *  - bool
 *  - nullopt_t (using the std::optional<>)
 */
using ParamValType = std::optional<std::variant<std::string, int64_t, uint64_t, float, double, bool>>;

/**
 * A name/value parameter for the CASTOR logging system.
 */
class Param {

public:

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
    if constexpr (std::is_same_v<T, ParamValType>) {
      m_value = value;
    } else if constexpr (is_optional_type<T>::value) {
      if (value.has_value()) {
        setValue(value.value());
      } else {
        setValue(std::nullopt);
      }
    } else if constexpr (std::is_same_v<T, bool>) {
      m_value = static_cast<bool>(value);
    } else if constexpr (std::is_integral_v<T>) {
      if constexpr (std::is_signed_v<T>) {
        m_value = static_cast<int64_t>(value);
      } else {
        m_value = static_cast<uint64_t>(value);
      }
    } else if constexpr (std::is_same_v<T, float>) {
      m_value = static_cast<float>(value);
    } else if constexpr (std::is_floating_point_v<T>) {
      m_value = static_cast<double>(value);
    } else if constexpr (std::is_same_v<T, std::nullopt_t>) {
      m_value = std::nullopt;
    } else if constexpr (has_ostream_operator<T>::value) {
      std::ostringstream oss;
      oss << value;
      m_value = oss.str();
    } else {
      static_assert(always_false<T>::value, "Type not supported");
    }
  }

  /**
   * Returns a const reference to the name of the parameter.
   */
  const std::string &getName() const noexcept;

  /**
   * Returns the value of the parameter as a string.
   */
  std::string getValueStr() const noexcept;

  /**
   * Returns a const reference to the variant of the parameter.
   */
  const ParamValType &getValueVariant() const noexcept;

protected:

  /**
   * `is_optional_type<T>` will be used to detect, at compile time, if a variable is of type `std::optional`
   */
  // Assume T is not std::optional by default
  template<typename T>
  struct is_optional_type : std::false_type {};

  // Specialization for std::optional<T>
  template<typename T>
  struct is_optional_type<std::optional<T>> : std::true_type {};

  // A helper template that is always false
  template<typename T>
  struct always_false : std::false_type {};

  // A helper trait to check for operator<<
  template <typename, typename = void>
  struct has_ostream_operator : std::false_type {};
  template <typename T>
  struct has_ostream_operator<T, std::void_t<decltype(std::declval<std::ostream&>() << std::declval<T>())>> : std::true_type {};

  /**
   * Name of the parameter
   */
  std::string m_name;

  /**
   * The value of the parameter in the original type
   */
  ParamValType m_value;
  /**
   * Helper class to format floating-point values
   */
  template<typename T>
  class floatingPointFormatting {
  public:
    explicit floatingPointFormatting(T value) : m_value(value) {
      static_assert(std::is_floating_point_v<T>, "Template parameter must be a floating point type.");
    }

    friend std::ostream& operator<<(std::ostream& oss, const floatingPointFormatting& fp) {
      constexpr int nr_digits = std::numeric_limits<T>::digits10;
      std::ostringstream oss_tmp;
      oss_tmp << std::setprecision(nr_digits) << fp.m_value;
      // Find if value contains a '.' or 'e/E', making it clear that it's a floating-point value
      if (oss_tmp.str().find_first_of(".eE") == std::string::npos) {
        // If not, append ".0" to make it clear that it's a floating point
        oss_tmp << ".0";
      }
      oss << oss_tmp.str();
      return oss;
    }

  private:
    T m_value;
  };
}; // class Param

} // namespace cta::log
