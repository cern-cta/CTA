/**
 * SPDX-FileCopyrightText: 2019 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <map>
#include <optional>
#include <sstream>
#include <vector>

namespace cta::common {

/*!
 * Interface to the CTA Frontend configuration file
 */
class Config {
public:
  /*!
   * Construct from a configuration file
   */
  explicit Config(const std::string& filename);

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

private:
  //! Configuration option list type
  using optionlist_t = std::vector<std::string>;

  /*!
   * Parse config file
   */
  void parse(std::ifstream& file);

  /*!
   * Get option list from config
   */
  const optionlist_t& getOptionList(const std::string& key) const;

  /*!
   * Tokenize a stringstream
   */
  optionlist_t tokenize(std::istringstream& input);

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
};

}  // namespace cta::common
