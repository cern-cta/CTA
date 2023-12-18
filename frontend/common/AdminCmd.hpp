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
#include "frontend/common/FrontendService.hpp"

namespace cta::frontend {

class AdminCmd {
public:
  AdminCmd(const frontend::FrontendService& frontendService,
    const common::dataStructures::SecurityIdentity& clientIdentity,
    const admin::AdminCmd& adminCmd);

  ~AdminCmd() = default;

  /*!
   * Process the admin command
   *
   * @return Protobuf to return to the client
   */
  xrd::Response process();

  /*!
   * Get a required option
   */
  const std::string& getRequired(admin::OptionString::Key key) const {
    return m_option_str.at(key);
  }
  const std::vector<std::string>& getRequired(admin::OptionStrList::Key key) const {
    return m_option_str_list.at(key);
  }
  const uint64_t& getRequired(admin::OptionUInt64::Key key) const {
    return m_option_uint64.at(key);
  }
  const bool& getRequired(admin::OptionBoolean::Key key) const {
    return m_option_bool.at(key);
  }

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
  std::optional<V> getOptional(K key, const std::map<K,V>& options, bool* has_option) const {
    auto it = options.find(key);

    if(it != options.end()) {
      if(has_option != nullptr) *has_option = true;
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
   * @param[in] option    Optional command line option
   *
   * @retval    true      The flag is present in the options map, and its value is true
   * @retval    false     The flag is either not present or is present and set to false
   */
  bool has_flag(admin::OptionBoolean::Key option) const {
    auto opt_it = m_option_bool.find(option);
    return opt_it != m_option_bool.end() && opt_it->second;
  }

protected:
  /*!
   * Convert AdminCmd <Cmd, SubCmd> pair to an integer so that it can be used in a switch statement
   */
  static constexpr unsigned int cmd_pair(admin::AdminCmd::Cmd cmd, admin::AdminCmd::SubCmd subcmd) {
    return (cmd << 16) + subcmd;
  }

  /*!
   * Convert protobuf options to maps
   */
  void importOptions();

  /*!
   * Log an admin command
   *
   * @param[in]    function    Calling function
   * @param[in]    status      Success or failure
   * @param[in]    reason      Reason for this log message (error message)
   * @param[in]    t           Timer
   */
  void logAdminCmd(const std::string& function, const std::string& status, const std::string& reason, utils::Timer& t);

  /*!
   * Drive state enum
   */
  enum DriveState { Up, Down };

  /*!
   * Changes state for the drives by a given regex.
   *
   * @param[in]    regex                The regex to match drive name(s) to change
   * @param[in]    desiredDriveState    Desired drive state (up, forceDown, reason, comment)
   * 
   * @return       The result of the operation, to return to the client
   */
  std::string setDriveState(const std::string& regex, const common::dataStructures::DesiredDriveState& desiredDriveState);

  const admin::AdminCmd    m_adminCmd;        //!< Administrator Command protocol buffer
  catalogue::Catalogue    &m_catalogue;       //!< Reference to CTA Catalogue
  cta::Scheduler          &m_scheduler;       //!< Reference to CTA Scheduler
  const NamespaceMap_t     m_namespaceMap;    //!< Identifiers for namespace queries
  log::LogContext          m_lc;              //!< CTA Log Context

private:
  /*!
   * Process admin commands
   *
   * @param[out]    response    CTA Admin Command response message
   */
  void processAdmin_Add               (xrd::Response& response);
  void processAdmin_Ch                (xrd::Response& response);
  void processAdmin_Rm                (xrd::Response& response);
  void processArchiveRoute_Add        (xrd::Response& response);
  void processArchiveRoute_Ch         (xrd::Response& response);
  void processArchiveRoute_Rm         (xrd::Response& response);
  void processDrive_Up                (xrd::Response& response);
  void processDrive_Down              (xrd::Response& response);
  void processDrive_Ch                (xrd::Response& response);
  void processDrive_Rm                (xrd::Response& response);
  void processFailedRequest_Rm        (xrd::Response& response);
  void processGroupMountRule_Add      (xrd::Response& response);
  void processGroupMountRule_Ch       (xrd::Response& response);
  void processGroupMountRule_Rm       (xrd::Response& response);
  void processLogicalLibrary_Add      (xrd::Response& response);
  void processLogicalLibrary_Ch       (xrd::Response& response);
  void processLogicalLibrary_Rm       (xrd::Response& response);
  void processMediaType_Add           (xrd::Response& response);
  void processMediaType_Ch            (xrd::Response& response);
  void processMediaType_Rm            (xrd::Response& response);
  void processMountPolicy_Add         (xrd::Response& response);
  void processMountPolicy_Ch          (xrd::Response& response);
  void processMountPolicy_Rm          (xrd::Response& response);
  void processRepack_Add              (xrd::Response& response);
  void processRepack_Rm               (xrd::Response& response);
  void processRepack_Err              (xrd::Response& response);
  void processRequesterMountRule_Add  (xrd::Response& response);
  void processRequesterMountRule_Ch   (xrd::Response& response);
  void processRequesterMountRule_Rm   (xrd::Response& response);
  void processActivityMountRule_Add   (xrd::Response& response);
  void processActivityMountRule_Ch    (xrd::Response& response);
  void processActivityMountRule_Rm    (xrd::Response& response);
  void processStorageClass_Add        (xrd::Response& response);
  void processStorageClass_Ch         (xrd::Response& response);
  void processStorageClass_Rm         (xrd::Response& response);
  void processTape_Add                (xrd::Response& response);
  void processTape_Ch                 (xrd::Response& response);
  void processTape_Rm                 (xrd::Response& response);
  void processTape_Reclaim            (xrd::Response& response);
  void processTape_Label              (xrd::Response& response);
  void processTapeFile_Rm             (xrd::Response& response);
  void processTapePool_Add            (xrd::Response& response);
  void processTapePool_Ch             (xrd::Response& response);
  void processTapePool_Rm             (xrd::Response& response);
  void processDiskSystem_Add          (xrd::Response& response);
  void processDiskSystem_Ch           (xrd::Response& response);
  void processDiskSystem_Rm           (xrd::Response& response);
  void processDiskInstance_Add        (xrd::Response& response);
  void processDiskInstance_Ch         (xrd::Response& response);
  void processDiskInstance_Rm         (xrd::Response& response);
  void processDiskInstanceSpace_Add   (xrd::Response& response);
  void processDiskInstanceSpace_Ch    (xrd::Response& response);
  void processDiskInstanceSpace_Rm    (xrd::Response& response);
  void processVirtualOrganization_Add (xrd::Response& response);
  void processVirtualOrganization_Ch  (xrd::Response& response);
  void processVirtualOrganization_Rm  (xrd::Response& response);
  void processPhysicalLibrary_Add     (xrd::Response& response);
  void processPhysicalLibrary_Ch      (xrd::Response& response);
  void processPhysicalLibrary_Rm      (xrd::Response& response);
  void processRecycleTapeFile_Restore (xrd::Response& response);
  void processModifyArchiveFile       (xrd::Response& response);

  common::dataStructures::SecurityIdentity    m_cliIdentity;           //!< Client identity: username, host, authentication
  const uint64_t                              m_archiveFileMaxSize;    //!< Maximum allowed file size for archive requests
  const std::optional<std::string>            m_repackBufferURL;       //!< Repack buffer URL
  const std::optional<std::uint64_t>          m_repackMaxFilesToSelect;//!< Repack max files to expand

  // Command options extracted from protobuf
  std::map<admin::OptionBoolean::Key, bool>                        m_option_bool;        //!< Boolean options
  std::map<admin::OptionUInt64::Key,  uint64_t>                    m_option_uint64;      //!< UInt64 options
  std::map<admin::OptionString::Key,  std::string>                 m_option_str;         //!< String options
  std::map<admin::OptionStrList::Key, std::vector<std::string>>    m_option_str_list;    //!< String List options
};

} // namespace cta::frontend
