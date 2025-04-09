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
#include "ServerTapeLs.hpp"
#include "ServerTapePoolLs.hpp"
#include "ServerVirtualOrganizationLs.hpp"
#include "ServerDiskInstanceLs.hpp"
#include "ServerDriveLs.hpp"
#include "ServerAdminLs.hpp"
#include "ServerVersion.hpp"
#include "ServerArchiveRouteLs.hpp"
#include "ServerGroupMountRuleLs.hpp"
#include "ServerMediaTypeLs.hpp"
#include "ServerRequesterMountRuleLs.hpp"
#include "ServerPhysicalLibraryLs.hpp"
#include "ServerMountPolicyLs.hpp"
#include "ServerDiskSystemLs.hpp"
#include "ServerDiskInstanceSpaceLs.hpp"
#include "ServerTapeFileLs.hpp"
#include "ServerActivityMountRuleLs.hpp"
#include "ServerShowQueues.hpp"
#include "ServerStorageClassLs.hpp"
#include "ServerRepackLs.hpp"
#include "ServerRecycleTapeFileLs.hpp"
#include "ServerLogicalLibraryLs.hpp"
#include "ServerFailedRequestLs.hpp"
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

namespace cta::frontend::grpc {

// callbackService class is the one that must implement the rpc methods
// a streaming rpc method must have return type ServerWriteReactor
class CtaRpcStreamImpl : public cta::xrd::CtaRpcStream::CallbackService {
  public:
    cta::log::LogContext getLogContext() const { return m_lc; }
    // CtaRpcStreamImpl() = delete;
    CtaRpcStreamImpl(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, cta::SchedulerDB_t &schedDB,
      const std::string& instanceName, const std::string& connstr, cta::log::LogContext logContext) :
      m_lc(logContext),
      m_catalogue(catalogue),
      m_scheduler(scheduler),
      m_instanceName(instanceName),
      m_schedDb(schedDB),
      m_catalogueConnString(connstr) {}
    /* CtaAdminServerWriteReactor is what the type of GenericAdminStream could be */
    ::grpc::ServerWriteReactor<cta::xrd::StreamResponse>* GenericAdminStream(::grpc::CallbackServerContext* context, const cta::xrd::Request* request);

  private:
    cta::log::LogContext m_lc; // <! Provided by the frontendService
    cta::catalogue::Catalogue &m_catalogue;    //!< Reference to CTA Catalogue
    cta::Scheduler            &m_scheduler;    //!< Reference to CTA Scheduler
    std::string               m_instanceName;  //!< Instance name
    cta::SchedulerDB_t        &m_schedDb;      //!< Reference to CTA SchedulerDB
    std::string m_catalogueConnString; //!< Provided by frontendService
    // I do not think a reactor could be a member of this class because it must be reinitialized upon each call
    // CtaAdminServerWriteReactor *m_reactor;      // this will have to be initialized to TapeLs or StorageClassLs or whatever...
};

// request object will be filled in by the Parser of the command on the client-side.
// Currently I am calling this class CtaAdminCmdStreamingClient
::grpc::ServerWriteReactor<cta::xrd::StreamResponse>*
CtaRpcStreamImpl::GenericAdminStream(::grpc::CallbackServerContext* context, const cta::xrd::Request* request) {
  std::cout << "In GenericAdminStream, just entered" << std::endl;

  switch(cmd_pair(request->admincmd().cmd(), request->admincmd().subcmd())) {
    case cmd_pair(cta::admin::AdminCmd::CMD_TAPE, cta::admin::AdminCmd::SUBCMD_LS):
      return new TapeLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_STORAGECLASS, cta::admin::AdminCmd::SUBCMD_LS):
      return new StorageClassLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_TAPEPOOL, cta::admin::AdminCmd::SUBCMD_LS):
      return new TapePoolLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_VIRTUALORGANIZATION, cta::admin::AdminCmd::SUBCMD_LS):
      return new VirtualOrganizationLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_DISKINSTANCE, cta::admin::AdminCmd::SUBCMD_LS):
      return new DiskInstanceLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_DRIVE, cta::admin::AdminCmd::SUBCMD_LS):
      return new DriveLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, m_lc, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_ADMIN, cta::admin::AdminCmd::SUBCMD_LS):
      return new AdminLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_VERSION, cta::admin::AdminCmd::SUBCMD_NONE):
      return new VersionWriteReactor(m_catalogue, m_scheduler, m_instanceName, m_catalogueConnString, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_ARCHIVEROUTE, cta::admin::AdminCmd::SUBCMD_LS):
      return new ArchiveRouteLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_GROUPMOUNTRULE, cta::admin::AdminCmd::SUBCMD_LS):
      return new GroupMountRuleLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_MEDIATYPE, cta::admin::AdminCmd::SUBCMD_LS):
      return new MediaTypeLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_REQUESTERMOUNTRULE, cta::admin::AdminCmd::SUBCMD_LS):
      return new RequesterMountRuleLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_PHYSICALLIBRARY, cta::admin::AdminCmd::SUBCMD_LS):
      return new PhysicalLibraryLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_MOUNTPOLICY, cta::admin::AdminCmd::SUBCMD_LS):
      return new MountPolicyLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_DISKSYSTEM, cta::admin::AdminCmd::SUBCMD_LS):
      return new DiskSystemLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_DISKINSTANCESPACE, cta::admin::AdminCmd::SUBCMD_LS):
      return new DiskInstanceSpaceLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_TAPEFILE, cta::admin::AdminCmd::SUBCMD_LS):
      return new TapeFileLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_ACTIVITYMOUNTRULE, cta::admin::AdminCmd::SUBCMD_LS):
      return new ActivityMountRuleLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_SHOWQUEUES, cta::admin::AdminCmd::SUBCMD_NONE):
      return new ShowQueuesWriteReactor(m_catalogue, m_scheduler,m_instanceName, m_lc, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_REPACK, cta::admin::AdminCmd::SUBCMD_LS):
      return new RepackLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_RECYCLETAPEFILE, cta::admin::AdminCmd::SUBCMD_LS):
      return new RecycleTapeFileLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_LOGICALLIBRARY, cta::admin::AdminCmd::SUBCMD_LS):
      return new LogicalLibraryLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_FAILEDREQUEST, cta::admin::AdminCmd::SUBCMD_LS):
      return new FailedRequestLsWriteReactor(m_catalogue, m_scheduler, m_instanceName, m_schedDb, m_lc, request);
    default:
      // Just to return an error status code when the specified command is not implemented
      const std::string errMsg("Admin command pair <" +
        AdminCmd_Cmd_Name(request->admincmd().cmd()) + ", " +
        AdminCmd_SubCmd_Name(request->admincmd().subcmd()) + "> is not implemented.");
      return new DefaultWriteReactor(errMsg);
    // dCache impl. prints unrecognized Request message
      // Finish(::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED	, "Method to handle this command is not implemented")); // we will not get into the unimplemented path,
      // as it will be detected earlier on by the client at the time of making the rpc call. But, still need a way to return an error here
      // should return an error that we have not implemented this, but this function does not return errors..?
      // Open question, what do I do for errors? This API is really not clear on this
  }
}
} // namespace cta::frontend::grpc

// XXXXX TODO:
// What are these functions: StartWriteLast etc that I see in test_service_impl.cc in test/cpp/end2end/test_service_impl of grpc source code?
// check also this resource: https://lastviking.eu/fun_with_gRPC_and_C++/callback-server.html
/* Important notice: 
 * 
 *
 * One thing to keep in mind is that the callback's may be called simultaneously from different threads.
 * Our implementation must be thread-safe.
 */