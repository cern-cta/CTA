/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2023 CERN
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

#include "cta_frontend.pb.h"
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace cta::frontend {

class AdminCmdOptions {
public:
  AdminCmdOptions() = default;
  virtual ~AdminCmdOptions() = default;

  /*!
   * Get a required option
   */
  const std::string& getRequired(admin::OptionString::Key key) const { return m_option_str.at(key); }

  const std::vector<std::string>& getRequired(admin::OptionStrList::Key key) const { return m_option_str_list.at(key); }

  const uint64_t& getRequired(admin::OptionUInt64::Key key) const { return m_option_uint64.at(key); }

  const bool& getRequired(admin::OptionBoolean::Key key) const { return m_option_bool.at(key); }

  /*!
   * Get an optional option
   *
   * The has_option parameter is set if the option exists and left unmodified if the option does not
   * exist. This is provided as a convenience to monitor whether at least one option was set from a
   * list of optional options.
   *
   * @param[in]     key           option key to look up in options
   * @param[out]    has_option    Set to true if the option exists, unmodified if it does not
   *
   * @returns       value of the option if it exists, an object of type std::nullopt_t if it does not
   */
  template<typename K, typename V>
  std::optional<V> getOptional(K key, const std::map<K, V>& options, bool* has_option = nullptr) const {
    auto it = options.find(key);

    if (it != options.end()) {
      if (has_option != nullptr) {
        *has_option = true;
      }
      return it->second;
    } else {
      return std::nullopt;
    }
  }

  /*!
   * Overloaded versions of getOptional
   *
   * These map the key type to the template specialization of <key,value> pairs
   */
  std::optional<std::string> getOptional(admin::OptionString::Key key, bool* has_option = nullptr) const {
    return getOptional(key, m_option_str, has_option);
  }

  std::optional<std::vector<std::string>> getOptional(admin::OptionStrList::Key key, bool* has_option = nullptr) const {
    return getOptional(key, m_option_str_list, has_option);
  }

  std::optional<uint64_t> getOptional(admin::OptionUInt64::Key key, bool* has_option = nullptr) const {
    return getOptional(key, m_option_uint64, has_option);
  }

  std::optional<bool> getOptional(admin::OptionBoolean::Key key, bool* has_option = nullptr) const {
    return getOptional(key, m_option_bool, has_option);
  }

  /*!
   * Check if an optional flag has been set
   *
   * This is a simpler version of getOptional for checking flags which are either present
   * or not. In the case of flags, they should always have the value true if the flag is
   * present, but we do a redundant check anyway.
   *
   * @param[in]  option        Optional command line option
   * @param[out] has_option    Set to true if the option exists, unmodified if it does not
   *
   * @retval    true      The flag is present in the options map, and its value is true
   * @retval    false     The flag is either not present or is present and set to false
   */
  bool has_flag(admin::OptionBoolean::Key option, bool* has_option = nullptr) const {
    auto opt_it = m_option_bool.find(option);
    if (opt_it != m_option_bool.end()) {
      if (has_option != nullptr) {
        *has_option = true;
      }
      return opt_it->second;
    }
    return false;
  }

  /*!
   * Both parameters FXID and DISK_FILE_ID expect the same information but with different formats.
   * They can't both be set simultaneously, to avoid inconsistencies.
   *
   * This function tries to parse one of these values and return it as a string.
   *
   * @throws UserError if both FXID and DISK_FILE_ID are defined
   * @throws UserError if format of FXID is wrong (DISK_FILE_ID can be any string)
   *
   * @return       The disk file ID as a string, or nullopt if none defined.
   */
  std::optional<std::string> getAndValidateDiskFileIdOptional(bool* has_any = nullptr) const;

  /*!
   * Convert protobuf options to maps
   */
  void importOptions(const admin::AdminCmd& adminCmd);

protected:
  std::map<admin::OptionBoolean::Key, bool> m_option_bool;                          //!< Boolean options
  std::map<admin::OptionUInt64::Key, uint64_t> m_option_uint64;                     //!< UInt64 options
  std::map<admin::OptionString::Key, std::string> m_option_str;                     //!< String options
  std::map<admin::OptionStrList::Key, std::vector<std::string>> m_option_str_list;  //!< String List options
};

}  // namespace cta::frontend