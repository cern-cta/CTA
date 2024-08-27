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

#include "common/dataStructures/LogicalLibrary.hpp"
#include "cmdline/CtaAdminCmdParse.hpp"
#include "frontend/common/AdminCmd.hpp"
#include "AdminCmdStream.hpp"

#include "XrdCtaActivityMountRuleLs.hpp"
#include "XrdCtaAdminLs.hpp"
#include "XrdCtaArchiveRouteLs.hpp"
#include "XrdCtaDiskInstanceLs.hpp"
#include "XrdCtaDiskInstanceSpaceLs.hpp"
#include "XrdCtaDiskSystemLs.hpp"
#include "XrdCtaDriveLs.hpp"
#include "XrdCtaFailedRequestLs.hpp"
#include "XrdCtaGroupMountRuleLs.hpp"
#include "XrdCtaLogicalLibraryLs.hpp"
#include "XrdCtaPhysicalLibraryLs.hpp"
#include "XrdCtaMediaTypeLs.hpp"
#include "XrdCtaMountPolicyLs.hpp"
#include "XrdCtaRecycleTapeFileLs.hpp"
#include "XrdCtaRepackLs.hpp"
#include "XrdCtaRequesterMountRuleLs.hpp"
#include "XrdCtaShowQueues.hpp"
#include "XrdCtaStorageClassLs.hpp"
#include "XrdCtaTapeFileLs.hpp"
#include "XrdCtaTapeLs.hpp"
#include "XrdCtaTapePoolLs.hpp"
#include "XrdCtaVersion.hpp"
#include "XrdCtaVirtualOrganizationLs.hpp"

#include "frontend/common/PbException.hpp"
// #include "frontend/common/GrpcEndpoint.hpp"
#include "frontend/common/AdminCmd.hpp"

namespace cta::frontend {

AdminCmdStream::AdminCmdStream(const frontend::FrontendService& frontendService,
  const common::dataStructures::SecurityIdentity& clientIdentity,
  const admin::AdminCmd& adminCmd,
  XrdSsiStream*& stream) :
  AdminCmd(frontendService, clientIdentity, adminCmd),
  m_stream(stream),
  m_schedDb(frontendService.getSchedDb()),
  m_catalogueConnString(frontendService.getCatalogueConnString()) { }

xrd::Response AdminCmdStream::process() {
  xrd::Response response;

  utils::Timer t;

  try {
    // Map the <Cmd, SubCmd> to a method
    switch(cmd_pair(m_adminCmd.cmd(), m_adminCmd.subcmd())) {
      using namespace cta::admin;

      case cmd_pair(admin::AdminCmd::CMD_ADMIN, admin::AdminCmd::SUBCMD_LS):
        processAdmin_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_ARCHIVEROUTE, admin::AdminCmd::SUBCMD_LS):
        processArchiveRoute_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_DRIVE, admin::AdminCmd::SUBCMD_LS):
        processDrive_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_FAILEDREQUEST, admin::AdminCmd::SUBCMD_LS):
        processFailedRequest_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_GROUPMOUNTRULE, admin::AdminCmd::SUBCMD_LS):
        processGroupMountRule_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_LOGICALLIBRARY, admin::AdminCmd::SUBCMD_LS):
        processLogicalLibrary_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_PHYSICALLIBRARY, admin::AdminCmd::SUBCMD_LS):
        processPhysicalLibrary_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_MEDIATYPE, admin::AdminCmd::SUBCMD_LS):
        processMediaType_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_MOUNTPOLICY, admin::AdminCmd::SUBCMD_LS):
        processMountPolicy_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_REPACK, admin::AdminCmd::SUBCMD_LS):
        processRepack_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_REQUESTERMOUNTRULE, admin::AdminCmd::SUBCMD_LS):
        processRequesterMountRule_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_ACTIVITYMOUNTRULE, admin::AdminCmd::SUBCMD_LS):
        processActivityMountRule_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_SHOWQUEUES, admin::AdminCmd::SUBCMD_NONE):
        processShowQueues(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_STORAGECLASS, admin::AdminCmd::SUBCMD_LS):
        processStorageClass_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_TAPE, admin::AdminCmd::SUBCMD_LS):
        processTape_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_TAPEFILE, admin::AdminCmd::SUBCMD_LS):
        processTapeFile_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_TAPEPOOL, admin::AdminCmd::SUBCMD_LS):
        processTapePool_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_DISKSYSTEM, admin::AdminCmd::SUBCMD_LS):
        processDiskSystem_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_DISKINSTANCE, admin::AdminCmd::SUBCMD_LS):
        processDiskInstance_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_DISKINSTANCESPACE, admin::AdminCmd::SUBCMD_LS):
        processDiskInstanceSpace_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_VIRTUALORGANIZATION, admin::AdminCmd::SUBCMD_LS):
        processVirtualOrganization_Ls(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_VERSION, admin::AdminCmd::SUBCMD_NONE):
        processVersion(response);
        break;
      case cmd_pair(admin::AdminCmd::CMD_RECYCLETAPEFILE, admin::AdminCmd::SUBCMD_LS):
        processRecycleTapeFile_Ls(response);
        break;
      default:
        throw exception::PbException("Admin command pair <" +
              AdminCmd_Cmd_Name(m_adminCmd.cmd()) + ", " +
              AdminCmd_SubCmd_Name(m_adminCmd.subcmd()) + "> is not a stream command.");
    }
     
    // Log the admin command
    logAdminCmd(__FUNCTION__, "success", "", t);
  } catch(exception::PbException& ex) {
    logAdminCmd(__FUNCTION__, "failure", ex.what(), t);
    throw ex;
  } catch(exception::UserError& ex) {
    logAdminCmd(__FUNCTION__, "failure", ex.getMessageValue(), t);
    throw ex;
  } catch(exception::Exception& ex) {
    logAdminCmd(__FUNCTION__, "failure", ex.what(), t);
    throw ex;
  } catch(std::runtime_error& ex) {
    logAdminCmd(__FUNCTION__, "failure", ex.what(), t);
    throw ex;
  }
  return response;
}

frontend::Version AdminCmdStream::getClientVersion() const {
  frontend::Version clientVersion;

  clientVersion.ctaVersion  = m_adminCmd.client_version();
  clientVersion.protobufTag = m_adminCmd.protobuf_tag();

  return clientVersion;
}

bool AdminCmdStream::isStreamCmd(const admin::AdminCmd& adminCmd) {
  return admin::streamCmds.count(std::make_pair(adminCmd.cmd(), adminCmd.subcmd())) > 0;
}

void AdminCmdStream::processAdmin_Ls(xrd::Response& response) {
  m_stream = new xrd::AdminLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::ADMIN_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processArchiveRoute_Ls(xrd::Response& response) {
  m_stream = new xrd::ArchiveRouteLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::ARCHIVEROUTE_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processDrive_Ls(xrd::Response& response) {
  m_stream = new xrd::DriveLsStream(*this, m_catalogue, m_scheduler, m_lc);

  response.set_show_header(admin::HeaderType::DRIVE_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processFailedRequest_Ls(xrd::Response& response) {
  m_stream = new xrd::FailedRequestLsStream(*this, m_catalogue, m_scheduler, m_schedDb, m_lc);

  // Display the correct column headers
  response.set_show_header(has_flag(admin::OptionBoolean::SUMMARY) ?
                           admin::HeaderType::FAILEDREQUEST_LS_SUMMARY :
                           admin::HeaderType::FAILEDREQUEST_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processGroupMountRule_Ls(xrd::Response& response) {
  m_stream = new xrd::GroupMountRuleLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::GROUPMOUNTRULE_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processLogicalLibrary_Ls(xrd::Response& response) {
  const auto disabled = getOptional(admin::OptionBoolean::DISABLED);

  m_stream = new xrd::LogicalLibraryLsStream(*this, m_catalogue, m_scheduler, disabled);

  response.set_show_header(admin::HeaderType::LOGICALLIBRARY_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processPhysicalLibrary_Ls(xrd::Response& response) {
  m_stream = new xrd::PhysicalLibraryLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::PHYSICALLIBRARY_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processMediaType_Ls(xrd::Response& response) {
  m_stream = new xrd::MediaTypeLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::MEDIATYPE_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processMountPolicy_Ls(xrd::Response& response) {
  m_stream = new xrd::MountPolicyLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::MOUNTPOLICY_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processRepack_Ls(xrd::Response& response) {
  auto vid = getOptional(admin::OptionString::VID);

  m_stream = new xrd::RepackLsStream(m_scheduler, m_catalogue, vid);

  response.set_show_header(admin::HeaderType::REPACK_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processRequesterMountRule_Ls(xrd::Response& response) {
  m_stream = new xrd::RequesterMountRuleLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::REQUESTERMOUNTRULE_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processActivityMountRule_Ls(xrd::Response& response) {
  m_stream = new xrd::ActivityMountRuleLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::ACTIVITYMOUNTRULE_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processShowQueues(xrd::Response& response) {
  m_stream = new xrd::ShowQueuesStream(*this, m_catalogue, m_scheduler, m_lc);

  response.set_show_header(admin::HeaderType::SHOWQUEUES);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processStorageClass_Ls(xrd::Response& response) {
  std::optional<std::string> scn = getOptional(admin::OptionString::STORAGE_CLASS);

  m_stream = new xrd::StorageClassLsStream(*this, m_catalogue, m_scheduler, scn);

  response.set_show_header(admin::HeaderType::STORAGECLASS_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processTape_Ls(xrd::Response& response) {
  m_stream = new xrd::TapeLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::TAPE_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processTapeFile_Ls(xrd::Response& response) {
  m_stream = new xrd::TapeFileLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::TAPEFILE_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processTapePool_Ls(xrd::Response& response) {
  m_stream = new xrd::TapePoolLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::TAPEPOOL_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processDiskSystem_Ls(xrd::Response& response) {
  m_stream = new xrd::DiskSystemLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::DISKSYSTEM_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processDiskInstance_Ls(xrd::Response& response) {
  m_stream = new xrd::DiskInstanceLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::DISKINSTANCE_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processDiskInstanceSpace_Ls(xrd::Response& response) {
  m_stream = new xrd::DiskInstanceSpaceLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::DISKINSTANCESPACE_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processVersion(xrd::Response& response) {
  m_stream = new xrd::VersionStream(*this, m_catalogue, m_scheduler, m_catalogueConnString);

  response.set_show_header(admin::HeaderType::VERSION_CMD);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processRecycleTapeFile_Ls(xrd::Response& response) {
  m_stream = new xrd::RecycleTapeFileLsStream(*this,m_catalogue,m_scheduler);

  response.set_show_header(admin::HeaderType::RECYLETAPEFILE_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

void AdminCmdStream::processVirtualOrganization_Ls(xrd::Response& response){
  m_stream = new xrd::VirtualOrganizationLsStream(*this, m_catalogue, m_scheduler);

  response.set_show_header(admin::HeaderType::VIRTUALORGANIZATION_LS);
  response.set_type(xrd::Response::RSP_SUCCESS);
}

} // namespace cta::frontend
