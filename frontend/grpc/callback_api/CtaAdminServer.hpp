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

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include "common/log/Logger.hpp"
#include "common/log/LogContext.hpp"
#include "common/exception/Exception.hpp"
#include "cmdline/TapeLsResponseStream.hpp"
#include "cmdline/StorageClassLsResponseStream.hpp"
#include "cmdline/TapePoolLsResponseStream.hpp"
#include "cmdline/VirtualOrganizationLsResponseStream.hpp"
#include "cmdline/DiskInstanceLsResponseStream.hpp"
#include "cmdline/DriveLsResponseStream.hpp"
#include "cmdline/AdminLsResponseStream.hpp"
#include "cmdline/ArchiveRouteLsResponseStream.hpp"
#include "cmdline/GroupMountRuleLsResponseStream.hpp"
#include "cmdline/MediaTypeLsResponseStream.hpp"
#include "cmdline/RequesterMountRuleLsResponseStream.hpp"
#include "cmdline/PhysicalLibraryLsResponseStream.hpp"
#include "cmdline/MountPolicyLsResponseStream.hpp"
#include "cmdline/DiskSystemLsResponseStream.hpp"
#include "cmdline/DiskInstanceSpaceLsResponseStream.hpp"
#include "cmdline/TapeFileLsResponseStream.hpp"
#include "cmdline/ActivityMountRuleLsResponseStream.hpp"
#include "cmdline/ShowQueuesResponseStream.hpp"
#include "cmdline/RepackLsResponseStream.hpp"
#include "cmdline/RecycleTapeFileLsResponseStream.hpp"
#include "cmdline/LogicalLibraryLsResponseStream.hpp"
#include "cmdline/FailedRequestLsResponseStream.hpp"
#include "ServerVersion.hpp"
#include "ServerDefaultReactor.hpp"

#include <grpcpp/grpcpp.h>

#include <string>
#include <memory>
#include <mutex>
#include <thread>

/*
 * Convert AdminCmd <Cmd, SubCmd> pair to an integer so that it can be used in a switch statement
 */
constexpr unsigned int cmd_pair(cta::admin::AdminCmd::Cmd cmd, cta::admin::AdminCmd::SubCmd subcmd) {
  return (cmd << 16) + subcmd;
}

constexpr const char* CTA_ADMIN_COMMANDS_DISABLED_ERROR =
  "CTA admin commands are disabled by configuration flag cta.experimental.grpc.cta_admin_commands.enabled";

namespace cta::frontend::grpc {

/* CallbackService class is the one that must implement the rpc methods,
 * using gRPC's async callback API (https://grpc.io/docs/languages/cpp/callback/)
 * this is the recommended way of implementing asynchronous calls (https://grpc.io/docs/languages/cpp/best_practices/).
 */
class CtaRpcStreamImpl : public cta::xrd::CtaRpcStream::CallbackService {
public:
  cta::log::LogContext getLogContext() const { return m_lc; }

  // CtaRpcStreamImpl() = delete;
  CtaRpcStreamImpl(cta::catalogue::Catalogue& catalogue,
                   cta::Scheduler& scheduler,
                   cta::SchedulerDB_t& schedDB,
                   const std::string& instanceName,
                   const std::string& connstr,
                   uint64_t missingFileCopiesMinAgeSecs,
                   bool enableCtaAdminCommands,
                   cta::log::LogContext logContext)
      : m_lc(logContext),
        m_catalogue(catalogue),
        m_scheduler(scheduler),
        m_instanceName(instanceName),
        m_schedDb(schedDB),
        m_catalogueConnString(connstr),
        m_missingFileCopiesMinAgeSecs(missingFileCopiesMinAgeSecs),
        m_enableCtaAdminCommands(enableCtaAdminCommands) {}

  /* gRPC expects the return type of an RPC implemented using the callback API to be a pointer to ::grpc::ServerWriteReactor */
  ::grpc::ServerWriteReactor<cta::xrd::StreamResponse>* GenericAdminStream(::grpc::CallbackServerContext* context,
                                                                           const cta::xrd::Request* request);

private:
  cta::log::LogContext m_lc;               // <! Provided by the frontendService
  cta::catalogue::Catalogue& m_catalogue;  //!< Reference to CTA Catalogue
  cta::Scheduler& m_scheduler;             //!< Reference to CTA Scheduler
  std::string m_instanceName;              //!< Instance name
  cta::SchedulerDB_t& m_schedDb;           //!< Reference to CTA SchedulerDB
  std::string m_catalogueConnString;       //!< Provided by frontendService
  uint64_t m_missingFileCopiesMinAgeSecs;  //!< Provided by the frontendService
  bool m_enableCtaAdminCommands;           //!< Feature flag to disable CTA admin commands
};

// request object will be filled in by the Parser of the command on the client-side.
::grpc::ServerWriteReactor<cta::xrd::StreamResponse>*
CtaRpcStreamImpl::GenericAdminStream(::grpc::CallbackServerContext* context, const cta::xrd::Request* request) {
  if (!m_enableCtaAdminCommands) {
    return new DefaultWriteReactor(CTA_ADMIN_COMMANDS_DISABLED_ERROR, ::grpc::StatusCode::UNIMPLEMENTED);
  }

  std::unique_ptr<cta::cmdline::CtaAdminResponseStream> stream;
  cta::admin::HeaderType headerType;
  try {
    switch (cmd_pair(request->admincmd().cmd(), request->admincmd().subcmd())) {
      using namespace cta::admin;
      case cmd_pair(cta::admin::AdminCmd::CMD_TAPE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::TapeLsResponseStream>(m_catalogue,
                                                                      m_scheduler,
                                                                      m_instanceName,
                                                                      request->admincmd(),
                                                                      m_missingFileCopiesMinAgeSecs);
        headerType = cta::admin::HeaderType::TAPE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_STORAGECLASS, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::StorageClassLsResponseStream>(m_catalogue,
                                                                              m_scheduler,
                                                                              m_instanceName,
                                                                              request->admincmd());
        headerType = HeaderType::STORAGECLASS_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_TAPEPOOL, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::TapePoolLsResponseStream>(m_catalogue,
                                                                          m_scheduler,
                                                                          m_instanceName,
                                                                          request->admincmd());
        headerType = HeaderType::TAPEPOOL_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_VIRTUALORGANIZATION, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::VirtualOrganizationLsResponseStream>(m_catalogue,
                                                                                     m_scheduler,
                                                                                     m_instanceName,
                                                                                     request->admincmd());
        headerType = HeaderType::VIRTUALORGANIZATION_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_DISKINSTANCE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::DiskInstanceLsResponseStream>(m_catalogue,
                                                                              m_scheduler,
                                                                              m_instanceName,
                                                                              request->admincmd());
        headerType = HeaderType::DISKINSTANCE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_DRIVE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::DriveLsResponseStream>(m_catalogue,
                                                                       m_scheduler,
                                                                       m_instanceName,
                                                                       request->admincmd(),
                                                                       m_lc);
        headerType = HeaderType::DRIVE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_ADMIN, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::AdminLsResponseStream>(m_catalogue,
                                                                       m_scheduler,
                                                                       m_instanceName,
                                                                       request->admincmd());
        headerType = HeaderType::ADMIN_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_VERSION, cta::admin::AdminCmd::SUBCMD_NONE):
        return new VersionWriteReactor(m_catalogue, m_scheduler, m_instanceName, m_catalogueConnString, request);
      case cmd_pair(cta::admin::AdminCmd::CMD_ARCHIVEROUTE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::ArchiveRouteLsResponseStream>(m_catalogue,
                                                                              m_scheduler,
                                                                              m_instanceName,
                                                                              request->admincmd());
        headerType = HeaderType::ARCHIVEROUTE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_GROUPMOUNTRULE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::GroupMountRuleLsResponseStream>(m_catalogue,
                                                                                m_scheduler,
                                                                                m_instanceName,
                                                                                request->admincmd());
        headerType = HeaderType::GROUPMOUNTRULE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_MEDIATYPE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::MediaTypeLsResponseStream>(m_catalogue,
                                                                           m_scheduler,
                                                                           m_instanceName,
                                                                           request->admincmd());
        headerType = HeaderType::MEDIATYPE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_REQUESTERMOUNTRULE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::RequesterMountRuleLsResponseStream>(m_catalogue,
                                                                                    m_scheduler,
                                                                                    m_instanceName,
                                                                                    request->admincmd());
        headerType = HeaderType::REQUESTERMOUNTRULE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_PHYSICALLIBRARY, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::PhysicalLibraryLsResponseStream>(m_catalogue,
                                                                                 m_scheduler,
                                                                                 m_instanceName,
                                                                                 request->admincmd());
        headerType = HeaderType::PHYSICALLIBRARY_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_MOUNTPOLICY, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::MountPolicyLsResponseStream>(m_catalogue,
                                                                             m_scheduler,
                                                                             m_instanceName,
                                                                             request->admincmd());
        headerType = HeaderType::MOUNTPOLICY_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_DISKSYSTEM, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::DiskSystemLsResponseStream>(m_catalogue,
                                                                            m_scheduler,
                                                                            m_instanceName,
                                                                            request->admincmd());
        headerType = HeaderType::DISKSYSTEM_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_DISKINSTANCESPACE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::DiskInstanceSpaceLsResponseStream>(m_catalogue,
                                                                                   m_scheduler,
                                                                                   m_instanceName,
                                                                                   request->admincmd());
        headerType = HeaderType::DISKINSTANCESPACE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_TAPEFILE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::TapeFileLsResponseStream>(m_catalogue,
                                                                          m_scheduler,
                                                                          m_instanceName,
                                                                          request->admincmd());
        headerType = HeaderType::TAPEFILE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_ACTIVITYMOUNTRULE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::ActivityMountRuleLsResponseStream>(m_catalogue,
                                                                                   m_scheduler,
                                                                                   m_instanceName,
                                                                                   request->admincmd());
        headerType = HeaderType::ACTIVITYMOUNTRULE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_SHOWQUEUES, cta::admin::AdminCmd::SUBCMD_NONE):
        stream = std::make_unique<cta::cmdline::ShowQueuesResponseStream>(m_catalogue,
                                                                          m_scheduler,
                                                                          m_instanceName,
                                                                          request->admincmd(),
                                                                          m_lc);
        headerType = HeaderType::SHOWQUEUES;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_REPACK, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::RepackLsResponseStream>(m_catalogue,
                                                                        m_scheduler,
                                                                        m_instanceName,
                                                                        request->admincmd());
        headerType = HeaderType::REPACK_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_RECYCLETAPEFILE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::RecycleTapeFileLsResponseStream>(m_catalogue,
                                                                                 m_scheduler,
                                                                                 m_instanceName,
                                                                                 request->admincmd());
        headerType = HeaderType::RECYLETAPEFILE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_LOGICALLIBRARY, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::LogicalLibraryLsResponseStream>(m_catalogue,
                                                                                m_scheduler,
                                                                                m_instanceName,
                                                                                request->admincmd());
        headerType = HeaderType::LOGICALLIBRARY_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_FAILEDREQUEST, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<cta::cmdline::FailedRequestLsResponseStream>(m_catalogue,
                                                                               m_scheduler,
                                                                               m_instanceName,
                                                                               request->admincmd(),
                                                                               m_schedDb,
                                                                               m_lc);
        headerType = HeaderType::FAILEDREQUEST_LS;
        break;
      default:
        // Just to return an error status code when the specified command is not implemented
        const std::string errMsg("Admin command pair <" + AdminCmd_Cmd_Name(request->admincmd().cmd()) + ", " +
                                 AdminCmd_SubCmd_Name(request->admincmd().subcmd()) + "> is not implemented.");
        return new DefaultWriteReactor(errMsg);
    }  // switch
  } catch (const cta::exception::UserError& ex) {
    return new DefaultWriteReactor(ex.getMessageValue());
  }  // try-catch
  // proceed with command execution if no exception was thrown
  return new CtaAdminServerWriteReactor(m_catalogue, m_scheduler, m_instanceName, std::move(stream), headerType);
}
}  // namespace cta::frontend::grpc
