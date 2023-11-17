/**
 * @project     The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2019-2022 CERN
 * @license     This program is free software, distributed under the terms of the GNU General Public
 *              Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *              redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *              option) any later version.
 *
 *              This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *              WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *              PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *              In applying this licence, CERN does not waive the privileges and immunities
 *              granted to it by virtue of its status as an Intergovernmental Organization or
 *              submit itself to any jurisdiction.
 */

#pragma once

#include <map>
#include <optional>
#include <sstream>

namespace cta::frontend {

/*!
 * Interface to the CTA Frontend configuration file
 */
class Config
{
public:
  /*!
   * Construct from a configuration file
   */
  explicit Config(const std::string& filename);

  /*!
   * Get a single option string value from config
   */
  std::optional<std::string> getOptionValueStr(const std::string &key) const;

  /*!
   * Get a single option integer value from config
   *
   * Throws std::invalid_argument or std::out_of_range if the key exists but the value cannot be
   * converted to an integer
   */
  std::optional<int> getOptionValueInt(const std::string &key) const;
  /*!
   * Get a single option unsigned integer value from config
   *
   * Throws std::invalid_argument or std::out_of_range if the key exists but the value cannot be
   * converted to an unsigned integer
   */
  std::optional<uint32_t> getOptionValueUInt(const std::string &key) const;


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
  T stou(const std::string &key) const;

  // Member variables
  const optionlist_t                  m_nulloptionlist;    //!< Empty option list returned when key not found
  std::map<std::string, optionlist_t> m_configuration;     //!< Parsed configuration options
};

} // namespace cta::frontend
