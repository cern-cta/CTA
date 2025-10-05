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

#include "AdminCmdOptions.hpp"
#include "catalogue/Catalogue.hpp"
#include "cta_frontend.pb.h"
#include "frontend/common/FrontendService.hpp"

namespace cta::frontend {

enum AdminCmdStatus { SUCCESS, USER_ERROR, EXCEPTION };

class AdminCmd : public AdminCmdOptions {
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
   * @return       The missing tape file copies minimum age.
   */
  uint64_t getMissingFileCopiesMinAgeSecs() const { return m_missingFileCopiesMinAgeSecs; }

protected:
  /*!
   * Convert AdminCmd <Cmd, SubCmd> pair to an integer so that it can be used in a switch statement
   */
  static constexpr unsigned int cmd_pair(admin::AdminCmd::Cmd cmd, admin::AdminCmd::SubCmd subcmd) {
    return (cmd << 16) + subcmd;
  }

  /*!
   * Log an admin command
   *
   * @param[in]    function    Calling function
   * @param[in]    status      Status of the executed admin command
   * @param[in]    reason      Reason for this log message (error message)
   * @param[in]    t           Timer
   */
  void
  logAdminCmd(const std::string& function, const AdminCmdStatus status, const std::string& reason, utils::Timer& t);

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

  common::dataStructures::SecurityIdentity    m_cliIdentity;                 //!< Client identity: username, host, authentication
  const uint64_t                              m_archiveFileMaxSize;          //!< Maximum allowed file size for archive requests
  const std::optional<std::string>            m_repackBufferURL;             //!< Repack buffer URL
  const std::optional<std::uint64_t>          m_repackMaxFilesToSelect;      //!< Repack max files to expand
  const uint64_t                              m_missingFileCopiesMinAgeSecs; //!< Missing tape file copies minimum age
  std::optional<std::string> m_schedulerBackendName;  //!< Name of the Scheduler DB to which Frontend connects
};

} // namespace cta::frontend
