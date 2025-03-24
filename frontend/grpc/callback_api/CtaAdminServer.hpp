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
#include "ServerTapeLs.hpp" // and all the rest of them
#include "ServerTapePoolLs.hpp"
#include "ServerVirtualOrganizationLs.hpp"
#include "ServerDiskInstanceLs.hpp"

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
    CtaRpcStreamImpl(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler, cta::log::LogContext logContext) :
      m_lc(logContext),
      m_catalogue(catalogue),
      m_scheduler(scheduler) {}
    /* CtaAdminServerWriteReactor is what the type of GenericAdminStream could be */
    ::grpc::ServerWriteReactor<cta::xrd::StreamResponse>* GenericAdminStream(::grpc::CallbackServerContext* context, const cta::xrd::Request* request);
    // ::grpc::ServerWriteReactor<cta::xrd::StreamResponse>* TapeLs(::grpc::CallbackServerContext* context, const cta::xrd::Request* request);
    // ::grpc::ServerWriteReactor<cta::xrd::Response>* StorageClassLs(::grpc::CallbackServerContext* context, const cta::xrd::Request* request);

  private:
    cta::log::LogContext m_lc; // <! Provided by the frontendService
    cta::catalogue::Catalogue &m_catalogue;    //!< Reference to CTA Catalogue
    cta::Scheduler            &m_scheduler;    //!< Reference to CTA Scheduler
    // I do not think a reactor could be a member of this class because it must be reinitialized upon each call
    // CtaAdminServerWriteReactor *m_reactor;      // this will have to be initialized to TapeLs or StorageClassLs or whatever...
};

// serverWriteReactor<Feature> is the RETURN TYPE of the function which is server-side streaming!!!
// Is serverWriteReactor a class? yes it is
// but clientReadReactor is a class
// ServerWriteReactor class defined in server_callback.h
// methods: StartWrite(), StartWriteAndFinish(), StartSendInitialMetadata(), StartWriteLast,
// Finish(), OnSendInitialMetadata(), OnWriteDone(), OnDone(), OnCancel()
// class CtaAdminServerWriteReactor : public grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
//   public:
//     void OnWriteDone(bool ok) override {
//       if (!ok) {
//         Finish(Status(grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone")); // Finish is a method of the parent class
//       }
//       NextWrite();
//     }
//   private:
//     // bool isHeaderSent = false;
//     xrd::cta::Response stream_item_;
//     std::vector<> // will hold the database query, each protobuf sent as the response is of the type cta::xrd::Response (which is either Header or Data)
//     // for tapeLS for example, we hold the results in std::list<common::dataStructures::Tape> m_tapeList; - XrdCtaTapeLs.hpp implementation
// }

// request object will be filled in by the Parser of the command on the client-side.
// Currently I am calling this class CtaAdminCmdStreamingClient
::grpc::ServerWriteReactor<cta::xrd::StreamResponse>*
CtaRpcStreamImpl::GenericAdminStream(::grpc::CallbackServerContext* context, const cta::xrd::Request* request) {
  std::cout << "In GenericAdminStream, just entered" << std::endl;
  // lister class implements all the overriden methods
  // its constructor calls the NextWrite() function to begin writing
  // return new Lister(rectangle, &feature_list_);
  // so I could here, based on the request, have a switch statement calling the constructor of the appropriate child class
  switch(cmd_pair(request->admincmd().cmd(), request->admincmd().subcmd())) {
    case cmd_pair(cta::admin::AdminCmd::CMD_TAPE, cta::admin::AdminCmd::SUBCMD_LS):
      return new TapeLsWriteReactor(m_catalogue, m_scheduler, request);
    // case cmd_pair(cta::admin::AdminCmd::CMD_TAPE, cta::admin::AdminCmd::SUBCMD_LS):
    //   return new StorageClassLsWriteReactor(catalogue, scheduler);
    case cmd_pair(cta::admin::AdminCmd::CMD_TAPEPOOL, cta::admin::AdminCmd::SUBCMD_LS):
      return new TapePoolLsWriteReactor(m_catalogue, m_scheduler, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_VIRTUALORGANIZATION, cta::admin::AdminCmd::SUBCMD_LS):
      return new VirtualOrganizationLsWriteReactor(m_catalogue, m_scheduler, request);
    case cmd_pair(cta::admin::AdminCmd::CMD_DISKINSTANCE, cta::admin::AdminCmd::SUBCMD_LS):
      return new DiskInstanceLsWriteReactor(m_catalogue, m_scheduler, request);
    default:
      // make the compiler happy maybe and return
      return new TapeLsWriteReactor(m_catalogue, m_scheduler, request);
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