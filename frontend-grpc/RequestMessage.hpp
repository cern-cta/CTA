/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2023 CERN
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

#include "cta_grpc_frontend.grpc.pb.h"

#include <map>
#include <optional>
#include <string>

namespace cta {
namespace frontend {
namespace grpc {
namespace request {

class RequestMessage {
public:
  RequestMessage(const cta::frontend::rpc::AdminRequest& request);
  ~RequestMessage() = default;

  /*!
   * Get a required option
   */
  const std::string& getRequired(cta::admin::OptionString::Key key) const { return m_option_str.at(key); }

  const std::vector<std::string>& getRequired(cta::admin::OptionStrList::Key key) const {
    return m_option_str_list.at(key);
  }

  const uint64_t& getRequired(cta::admin::OptionUInt64::Key key) const { return m_option_uint64.at(key); }

  const bool& getRequired(cta::admin::OptionBoolean::Key key) const { return m_option_bool.at(key); }

  // TODO:
  // const Versions &getClientVersions() const {
  //   return m_client_versions;
  // }
  // const std::string &getClientXrdSsiProtoIntVersion() const {
  //   return m_client_xrd_ssi_proto_int_version;
  // }

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
    }
    else {
      return std::nullopt;
    }
  }

  /*!
   * Overloaded versions of getOptional
   *
   * These map the key type to the template specialization of <key,value> pairs
   */
  std::optional<std::string> getOptional(cta::admin::OptionString::Key key, bool* has_option = nullptr) const {
    return getOptional(key, m_option_str, has_option);
  }

  std::optional<std::vector<std::string>> getOptional(cta::admin::OptionStrList::Key key,
                                                      bool* has_option = nullptr) const {
    return getOptional(key, m_option_str_list, has_option);
  }

  std::optional<uint64_t> getOptional(cta::admin::OptionUInt64::Key key, bool* has_option = nullptr) const {
    return getOptional(key, m_option_uint64, has_option);
  }

  std::optional<bool> getOptional(cta::admin::OptionBoolean::Key key, bool* has_option = nullptr) const {
    return getOptional(key, m_option_bool, has_option);
  }

  /*!
   * Check if an optional flag has been set
   *
   * This is a simpler version of getOptional for checking flags which are either present
   * or not. In the case of flags, they should always have the value true if the flag is
   * present, but we do a redundant check anyway.
   *
   * @param[in] option    Optional command line option
   *
   * @retval    true      The flag is present in the options map, and its value is true
   * @retval    false     The flag is either not present or is present and set to false
   */
  bool hasFlag(cta::admin::OptionBoolean::Key option) const {
    auto opt_it = m_option_bool.find(option);
    return opt_it != m_option_bool.end() && opt_it->second;
  }

private:
  std::map<cta::admin::OptionBoolean::Key, bool> m_option_bool;       //!< Boolean options
  std::map<cta::admin::OptionUInt64::Key, uint64_t> m_option_uint64;  //!< UInt64 options
  std::map<cta::admin::OptionString::Key, std::string> m_option_str;  //!< String options
  std::map<cta::admin::OptionStrList::Key,
           std::vector<std::string>> m_option_str_list;  //!< String List options

  /*!
   * Import Google Protobuf option fields into maps
   *
   * @param[in]     admincmd        CTA Admin command request message
   */
  void importOptions(const cta::admin::AdminCmd& admincmd);
};

}  // namespace request
}  // namespace grpc
}  // namespace frontend
}  // namespace cta