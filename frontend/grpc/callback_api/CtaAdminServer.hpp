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

#include "ServerDefaultReactor.hpp"
#include "ServerVersion.hpp"
#include "common/JwkCache.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "common/semconv/Attributes.hpp"
#include "frontend/common/ActivityMountRuleLsResponseStream.hpp"
#include "frontend/common/AdminLsResponseStream.hpp"
#include "frontend/common/ArchiveRouteLsResponseStream.hpp"
#include "frontend/common/DiskInstanceLsResponseStream.hpp"
#include "frontend/common/DiskInstanceSpaceLsResponseStream.hpp"
#include "frontend/common/DiskSystemLsResponseStream.hpp"
#include "frontend/common/DriveLsResponseStream.hpp"
#include "frontend/common/FailedRequestLsResponseStream.hpp"
#include "frontend/common/GroupMountRuleLsResponseStream.hpp"
#include "frontend/common/LogicalLibraryLsResponseStream.hpp"
#include "frontend/common/MediaTypeLsResponseStream.hpp"
#include "frontend/common/MountPolicyLsResponseStream.hpp"
#include "frontend/common/PhysicalLibraryLsResponseStream.hpp"
#include "frontend/common/RecycleTapeFileLsResponseStream.hpp"
#include "frontend/common/RepackLsResponseStream.hpp"
#include "frontend/common/RequestTracker.hpp"
#include "frontend/common/RequesterMountRuleLsResponseStream.hpp"
#include "frontend/common/ShowQueuesResponseStream.hpp"
#include "frontend/common/StorageClassLsResponseStream.hpp"
#include "frontend/common/TapeFileLsResponseStream.hpp"
#include "frontend/common/TapeLsResponseStream.hpp"
#include "frontend/common/TapePoolLsResponseStream.hpp"
#include "frontend/common/VirtualOrganizationLsResponseStream.hpp"
#include "frontend/grpc/common/GrpcAuthUtils.hpp"

#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

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

  CtaRpcStreamImpl(cta::catalogue::Catalogue& catalogue,
                   cta::Scheduler& scheduler,
                   cta::SchedulerDB_t& schedDB,
                   const std::string& instanceName,
                   const std::string& connstr,
                   uint64_t missingFileCopiesMinAgeSecs,
                   bool enableCtaAdminCommands,
                   cta::log::LogContext logContext,
                   bool jwtAuthEnabled,
                   std::shared_ptr<JwkCache> pubkeyCache,
                   server::TokenStorage& tokenStorage)
      : m_lc(logContext),
        m_catalogue(catalogue),
        m_scheduler(scheduler),
        m_instanceName(instanceName),
        m_schedDb(schedDB),
        m_catalogueConnString(connstr),
        m_missingFileCopiesMinAgeSecs(missingFileCopiesMinAgeSecs),
        m_enableCtaAdminCommands(enableCtaAdminCommands),
        m_jwtAuthEnabled(jwtAuthEnabled),
        m_pubkeyCache(pubkeyCache),
        m_tokenStorage(tokenStorage) {}

  /* gRPC expects the return type of an RPC implemented using the callback API to be
   * a pointer to ::grpc::ServerWriteReactor
   */
  ::grpc::ServerWriteReactor<cta::xrd::StreamResponse>* GenericAdminStream(::grpc::CallbackServerContext* context,
                                                                           const cta::xrd::Request* request) final;

private:
  cta::log::LogContext m_lc;                // <! Provided by the frontendService
  cta::catalogue::Catalogue& m_catalogue;   //!< Reference to CTA Catalogue
  cta::Scheduler& m_scheduler;              //!< Reference to CTA Scheduler
  std::string m_instanceName;               //!< Instance name
  cta::SchedulerDB_t& m_schedDb;            //!< Reference to CTA SchedulerDB
  std::string m_catalogueConnString;        //!< Provided by frontendService
  uint64_t m_missingFileCopiesMinAgeSecs;   //!< Provided by the frontendService
  bool m_enableCtaAdminCommands;            //!< Feature flag to disable CTA admin commands
  bool m_jwtAuthEnabled;                    //!< Whether JWT authentication is enabled
  std::shared_ptr<JwkCache> m_pubkeyCache;  //!< Shared JWK cache for token validation
  server::TokenStorage& m_tokenStorage;     //!< Required for Kerberos token validation
};

// request object will be filled in by the Parser of the command on the client-side.
::grpc::ServerWriteReactor<cta::xrd::StreamResponse>*
CtaRpcStreamImpl::GenericAdminStream(::grpc::CallbackServerContext* context, const cta::xrd::Request* request) {
  if (!m_enableCtaAdminCommands) {
    return new DefaultWriteReactor(CTA_ADMIN_COMMANDS_DISABLED_ERROR, ::grpc::StatusCode::UNIMPLEMENTED);
  }

  // Authenticate the request using JWT if enabled
  cta::log::LogContext lc(m_lc);
  // get the client metadata for authentication
  auto client_metadata = context->client_metadata();

  auto [status, clientIdentity] = cta::frontend::grpc::common::extractAuthHeaderAndValidate(client_metadata,
                                                                                            m_jwtAuthEnabled,
                                                                                            m_pubkeyCache,
                                                                                            m_tokenStorage,
                                                                                            m_instanceName,
                                                                                            context->peer(),
                                                                                            lc);
  if (!status.ok()) {
    return new DefaultWriteReactor(status.error_message(), status.error_code());
  }

  cta::frontend::RequestTracker requestTracker("ADMIN_STREAMING", "admin");
  std::unique_ptr<cta::frontend::CtaAdminResponseStream> stream;
  cta::admin::HeaderType headerType;
  try {
    switch (cmd_pair(request->admincmd().cmd(), request->admincmd().subcmd())) {
      using namespace cta::admin;
      case cmd_pair(cta::admin::AdminCmd::CMD_TAPE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<TapeLsResponseStream>(m_catalogue,
                                                        m_scheduler,
                                                        m_instanceName,
                                                        request->admincmd(),
                                                        m_missingFileCopiesMinAgeSecs);
        headerType = cta::admin::HeaderType::TAPE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_STORAGECLASS, cta::admin::AdminCmd::SUBCMD_LS):
        stream =
          std::make_unique<StorageClassLsResponseStream>(m_catalogue, m_scheduler, m_instanceName, request->admincmd());
        headerType = HeaderType::STORAGECLASS_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_TAPEPOOL, cta::admin::AdminCmd::SUBCMD_LS):
        stream =
          std::make_unique<TapePoolLsResponseStream>(m_catalogue, m_scheduler, m_instanceName, request->admincmd());
        headerType = HeaderType::TAPEPOOL_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_VIRTUALORGANIZATION, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<VirtualOrganizationLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::VIRTUALORGANIZATION_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_DISKINSTANCE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<DiskInstanceLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::DISKINSTANCE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_DRIVE, cta::admin::AdminCmd::SUBCMD_LS):
        stream =
          std::make_unique<DriveLsResponseStream>(m_catalogue, m_scheduler, m_instanceName, request->admincmd(), m_lc);
        headerType = HeaderType::DRIVE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_ADMIN, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<AdminLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::ADMIN_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_VERSION, cta::admin::AdminCmd::SUBCMD_NONE):
        return new VersionWriteReactor(m_catalogue, m_scheduler, m_instanceName, m_catalogueConnString, request);
      case cmd_pair(cta::admin::AdminCmd::CMD_ARCHIVEROUTE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<ArchiveRouteLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::ARCHIVEROUTE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_GROUPMOUNTRULE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<GroupMountRuleLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::GROUPMOUNTRULE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_MEDIATYPE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<MediaTypeLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::MEDIATYPE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_REQUESTERMOUNTRULE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<RequesterMountRuleLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::REQUESTERMOUNTRULE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_PHYSICALLIBRARY, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<PhysicalLibraryLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::PHYSICALLIBRARY_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_MOUNTPOLICY, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<MountPolicyLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::MOUNTPOLICY_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_DISKSYSTEM, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<DiskSystemLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::DISKSYSTEM_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_DISKINSTANCESPACE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<DiskInstanceSpaceLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::DISKINSTANCESPACE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_TAPEFILE, cta::admin::AdminCmd::SUBCMD_LS):
        stream =
          std::make_unique<TapeFileLsResponseStream>(m_catalogue, m_scheduler, m_instanceName, request->admincmd());
        headerType = HeaderType::TAPEFILE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_ACTIVITYMOUNTRULE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<ActivityMountRuleLsResponseStream>(m_catalogue, m_scheduler, m_instanceName);
        headerType = HeaderType::ACTIVITYMOUNTRULE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_SHOWQUEUES, cta::admin::AdminCmd::SUBCMD_NONE):
        stream = std::make_unique<ShowQueuesResponseStream>(m_catalogue, m_scheduler, m_instanceName, m_lc);
        headerType = HeaderType::SHOWQUEUES;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_REPACK, cta::admin::AdminCmd::SUBCMD_LS):
        stream =
          std::make_unique<RepackLsResponseStream>(m_catalogue, m_scheduler, m_instanceName, request->admincmd());
        headerType = HeaderType::REPACK_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_RECYCLETAPEFILE, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<RecycleTapeFileLsResponseStream>(m_catalogue,
                                                                   m_scheduler,
                                                                   m_instanceName,
                                                                   request->admincmd());
        headerType = HeaderType::RECYLETAPEFILE_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_LOGICALLIBRARY, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<LogicalLibraryLsResponseStream>(m_catalogue,
                                                                  m_scheduler,
                                                                  m_instanceName,
                                                                  request->admincmd());
        headerType = HeaderType::LOGICALLIBRARY_LS;
        break;
      case cmd_pair(cta::admin::AdminCmd::CMD_FAILEDREQUEST, cta::admin::AdminCmd::SUBCMD_LS):
        stream = std::make_unique<FailedRequestLsResponseStream>(m_catalogue,
                                                                 m_scheduler,
                                                                 m_instanceName,
                                                                 request->admincmd(),
                                                                 m_schedDb,
                                                                 m_lc);
        headerType = HeaderType::FAILEDREQUEST_LS;
        break;
      default:
        // Just to return an error status code when the specified command is not implemented
        const std::string errMsg("Admin command pair <" + AdminCmd_Cmd_Name(request->admincmd().cmd()) + ", "
                                 + AdminCmd_SubCmd_Name(request->admincmd().subcmd()) + "> is not implemented.");
        requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
        return new DefaultWriteReactor(errMsg);
    }  // switch
  } catch (const cta::exception::UserError& ex) {
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kUserError);
    return new DefaultWriteReactor(ex.getMessageValue());
  }  // try-catch
  // proceed with command execution if no exception was thrown
  return new CtaAdminServerWriteReactor(m_scheduler, m_instanceName, std::move(stream), headerType);
}
}  // namespace cta::frontend::grpc
