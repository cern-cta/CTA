/**
 * SPDX-FileCopyrightText: 2019 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/Logger.hpp"

#include <algorithm>
#include <map>
#include <optional>
#include <sstream>
#include <vector>

namespace cta::common {

template<typename T>
struct always_false : std::false_type {};

// Undefined trait (default behaviour)
template<typename T>
struct FromString;

// A concept representing a type which implements FromString::tryFrom(...)
template<typename T>
concept HasFromString = requires(std::string_view sv) {
  { FromString<T>::tryFrom(sv) } -> std::convertible_to<std::optional<T>>;
};

// Trait definition to tag vectors
template<typename T>
struct is_vector : std::false_type {};

template<typename T>
struct is_vector<std::vector<T>> : std::true_type {};

/*!
 * Interface to the CTA Frontend configuration file
 */
class Config {
public:
  /*!
   * Construct from a configuration file
   */
  explicit Config(const std::string& filename, cta::log::Logger* log = nullptr);

  /*!
   * Get a option string list vector from config
   */
  std::vector<std::string> getOptionValueStrVector(const std::string& key) const;

  /*!
   * Get a single option string value from config
   */
  std::optional<std::string> getOptionValueStr(const std::string& key) const;

  /*!
   * Get a single option integer value from config
   *
   * Throws std::invalid_argument or std::out_of_range if the key exists but the value cannot be
   * converted to an integer
   */
  std::optional<int> getOptionValueInt(const std::string& key) const;
  /*!
   * Get a single option unsigned integer value from config
   *
   * Throws std::invalid_argument or std::out_of_range if the key exists but the value cannot be
   * converted to an unsigned integer
   */
  std::optional<uint32_t> getOptionValueUInt(const std::string& key) const;

  /*!
  * Get a single option bool value from config
  */
  std::optional<bool> getOptionValueBool(const std::string& key) const;

  /**
   * @brief Same as getOptionValue(const std:string&) but runs a validator
   *
   * @param key the config option as shown in the file
   * @param validator a validator lambda (takes value if it exists, doesn't return anything)
   */
  template<typename T, typename F>
  std::optional<T> getOptionValue(const std::string_view key, F&& validator) const {
    auto res = getOptionValue<T>(key);
    if (res.has_value()) {
      validator(res);
    }
    return res;
  }

  /**
   * @brief Same as getOptionValue(const std:string_view) but places the result in
   * a reference to a value type
   *
   * @param key the config option as shown in the file
   * @param out a reference where to store the result
   */
  template<typename T>
  void getOptionValueInto(const std::string_view key, T& out) const {
    auto value = getOptionValue<T>(key);
    if (value.has_value()) {
      out = value.value();
    }
  }

  // if the target is an optional, set it directly instead of double-wrapping
  template<typename T>
  void getOptionValueInto(const std::string_view key, std::optional<T>& out) const {
    out = getOptionValue<T>(key);
  }

  /**
   * @brief Same as getOptionValue(const std::string_view, T&) but will set
   * the target ref to a default value if the option is not present in the file
   *
   * @param key the config option as shown in the file
   * @param out a reference where to store the result
   * @param default_value the default value to use if the option is not present
   */
  template<typename T>
  void getOptionValueInto(const std::string_view key, T& out, T default_value) const {
    auto value = getOptionValue<T>(key);
    out = value.value_or(default_value);
  }

  // actual implementation of getOptionValue for the various types
  template<typename T>
  std::optional<T> getOptionValue(const std::string_view key) const {
    std::string keyStr {key};
    if constexpr (std::is_same_v<T, std::string>) {
      return getOptionValueStr(keyStr);
    } else if constexpr (std::is_same_v<T, int>) {
      return getOptionValueInt(keyStr);
    } else if constexpr (std::is_same_v<T, uint32_t>) {
      return getOptionValueUInt(keyStr);
    } else if constexpr (std::is_same_v<T, bool>) {
      return getOptionValueBool(keyStr);
    } else if constexpr (HasFromString<T>) {
      // T can be turned into a string through FromString::tryFrom
      auto str = getOptionValueStr(keyStr);
      if (!str.has_value()) {
        return std::nullopt;
      }
      auto parsedVal = FromString<T>::tryFrom(str.value());
      if (!parsedVal.has_value()) {
        throw std::invalid_argument("Can't parse: " + str.value());
      }
      return parsedVal;
    } else if constexpr (is_vector<T>::value && HasFromString<typename T::value_type>) {
      // if T has been tagged as a vector of FromString, we'll parse it element by element
      auto options = getOptionValueStrVector(keyStr);
      if (options.empty()) {
        return std::nullopt;
      }
      std::vector<typename T::value_type> vec;
      std::ranges::transform(options, std::back_inserter(vec), [](const auto& str) {
        auto val = FromString<typename T::value_type>::tryFrom(str);
        if (!val.has_value()) {
          throw std::invalid_argument("Can't parse argument: " + str);
        }
        return val.value();
      });
      return vec;
    } else {
      static_assert(always_false<T>::value, "No getOptionValue specialization for vector element type");
    }
  }

  /*!
   * Parse config file
   */
  void parse(log::Logger& log);

private:
  //! Configuration option list type
  using optionlist_t = std::vector<std::string>;

  /*!
   * Get option list from config
   */
  const optionlist_t& getOptionList(const std::string& key) const;

  /*!
   * Tokenize a stringstream
   */
  optionlist_t tokenize(std::istringstream& input) const;

  /*!
   * Interprets an unsigned integer value in the string
   * expected correct value is in the range [0, std::numeric_limits<T>::max()]
   * or if std::numeric_limits<T>::max() > std::numeric_limits<int64_t>::max()
   * [0, std::numeric_limits<int64_t>::max()]
   */
  template<class T>
  T stou(const std::string& key) const;

  // Member variables
  const optionlist_t m_nulloptionlist;                  //!< Empty option list returned when key not found
  std::map<std::string, optionlist_t> m_configuration;  //!< Parsed configuration options
  const std::string m_configFileName;
};

}  // namespace cta::common
