/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <cstdio>
#include <iomanip>
#include <limits>
#include <optional>
#include <sstream>
#include <string.h>
#include <variant>

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
  template<typename S, typename T>
    requires std::constructible_from<std::string, S&&>
  Param(S&& name, T&& value) noexcept : m_name(std::forward<S>(name)) {
    setValue(std::forward<T>(value));
  }

  /**
   * Value changer. Useful for log contexts.
   * @param value
   */
  template<typename T>
  void setValue(T&& value) noexcept {
    using U = std::remove_cvref_t<T>;
    if constexpr (std::is_same_v<U, ParamValType>) {
      m_value = std::forward<T>(value);
    } else if constexpr (is_optional_type<U>::value) {
      if (value.has_value()) {
        setValue(std::forward<decltype(*value)>(*value));
      } else {
        m_value.reset();
      }
    } else if constexpr (std::is_same_v<U, std::nullopt_t>) {
      m_value.reset();
    } else if constexpr (std::is_same_v<U, std::string>) {
      m_value.emplace(std::in_place_type<std::string>, std::forward<T>(value));
    } else if constexpr (std::is_convertible_v<T, std::string_view>) {
      m_value.emplace(std::in_place_type<std::string>, std::string_view(value));
    } else if constexpr (std::is_same_v<U, bool>) {
      m_value.emplace(std::in_place_type<bool>, static_cast<bool>(value));
    } else if constexpr (std::is_integral_v<U>) {
      if constexpr (std::is_signed_v<U>) {
        m_value.emplace(std::in_place_type<int64_t>, static_cast<int64_t>(value));
      } else {
        m_value.emplace(std::in_place_type<uint64_t>, static_cast<uint64_t>(value));
      }
    } else if constexpr (std::is_same_v<U, float>) {
      m_value.emplace(std::in_place_type<float>, static_cast<float>(value));
    } else if constexpr (std::is_floating_point_v<U>) {
      m_value.emplace(std::in_place_type<double>, static_cast<double>(value));
    } else if constexpr (has_ostream_operator<U>::value) {
      std::ostringstream oss;
      oss << value;
      m_value.emplace(std::in_place_type<std::string>, std::move(oss).str());
    } else {
      static_assert(always_false<U>::value, "Type not supported");
    }
  }

  /**
   * Returns a const reference to the name of the parameter.
   */
  const std::string& getName() const noexcept;

  /**
   * Returns the value of the parameter as a string.
   */
  std::string getValueStr() const noexcept;

  /**
   * Returns a const reference to the variant of the parameter.
   */
  const ParamValType& getValueVariant() const noexcept;

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
  template<typename, typename = void>
  struct has_ostream_operator : std::false_type {};

  template<typename T>
  struct has_ostream_operator<T, std::void_t<decltype(std::declval<std::ostream&>() << std::declval<T>())>>
      : std::true_type {};

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
};  // class Param

}  // namespace cta::log
