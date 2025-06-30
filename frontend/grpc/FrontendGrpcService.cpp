/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @copyright      Copyright(C) 2021 DESY
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "FrontendGrpcService.h"

#include "catalogue/Catalogue.hpp"
#include "common/log/LogLevel.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "frontend/common/FrontendService.hpp"
#include "frontend/common/WorkflowEvent.hpp"

/*
 * Validate the storage class and issue the archive ID which should be used for the Archive request
 */

namespace cta::frontend::grpc {

void
CtaRpcImpl::ProcessGrpcRequest(::grpc::ServerUnaryReactor* reactor, const cta::xrd::Request* request, cta::xrd::Response* response, cta::log::LogContext &lc) const {
  try {
    cta::common::dataStructures::SecurityIdentity clientIdentity(request->notification().wf().instance().name(), cta::utils::getShortHostname());
    cta::frontend::WorkflowEvent wfe(*m_frontendService, clientIdentity, request->notification());
    *response = wfe.process();
  } catch (cta::exception::PbException &ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_PROTOBUF);
    response->set_message_txt(ex.getMessageValue());
    reactor->Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, ex.getMessageValue()));
  } catch (cta::exception::UserError &ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(ex.getMessageValue());
    /* Logic-wise, this should also be INVALID_ARGUMENT, but we need a way to
     * differentiate between different kinds of errors on the client side,
     * which is why we return ABORTED
     */
    reactor->Finish(::grpc::Status(::grpc::StatusCode::ABORTED, ex.getMessageValue()));
    return;
  } catch (cta::exception::Exception &ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    response->set_message_txt(ex.getMessageValue());
    reactor->Finish(::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, ex.getMessageValue()));
    return;
  } catch (std::runtime_error &ex) {
    lc.log(cta::log::ERR, ex.what());
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    response->set_message_txt(ex.what());
    reactor->Finish(::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, ex.what()));
    return;
  } catch (...) {
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    reactor->Finish(::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, "Error processing gRPC request"));
    return;
  }
  reactor->Finish(Status::OK);
  return;
}

::grpc::ServerUnaryReactor*
CtaRpcImpl::Create(::grpc::CallbackServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  auto* reactor = context->DefaultReactor();

  sp.add("remoteHost", context->peer());
  sp.add("request", "create");
  // check that the workflow is set appropriately for the create event
  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::CREATE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected CREATE, found " + cta::eos::Workflow_EventType_Name(event));
    reactor->Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected CREATE, found " + cta::eos::Workflow_EventType_Name(event)));
    return reactor;
  }
  ProcessGrpcRequest(reactor, request, response, lc);
  return reactor;
}

::grpc::ServerUnaryReactor*
CtaRpcImpl::Archive(::grpc::CallbackServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  auto* reactor = context->DefaultReactor();

  sp.add("remoteHost", context->peer());
  sp.add("request", "archive");
  // check validate request args
  const std::string storageClass = request->notification().file().storage_class();
  if (storageClass.empty()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Storage class is not set.");
    reactor->Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set."));
    return reactor;
  }

  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::CLOSEW) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected CLOSEW, found " + cta::eos::Workflow_EventType_Name(event));
    reactor->Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected CLOSEW, found " + cta::eos::Workflow_EventType_Name(event)));
    return reactor;
  }

  ProcessGrpcRequest(reactor, request, response, lc);
  return reactor;
}

::grpc::ServerUnaryReactor*
CtaRpcImpl::Delete(::grpc::CallbackServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());
  sp.add("request", "delete");

  auto* reactor = context->DefaultReactor();

  // check validate request args
  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::DELETE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected DELETE, found " + cta::eos::Workflow_EventType_Name(event));
    reactor->Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected DELETE, found " + cta::eos::Workflow_EventType_Name(event)));
    return reactor;
  }

  if (request->notification().file().archive_file_id() == 0) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Invalid archive file id");
    reactor->Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id."));
    return reactor;
  }

  // done with validation, now do the workflow processing
  ProcessGrpcRequest(reactor, request, response, lc);
  return reactor;
}

::grpc::ServerUnaryReactor*
CtaRpcImpl::Retrieve(::grpc::CallbackServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());
  sp.add("request", "retrieve");

  auto* reactor = context->DefaultReactor();

  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::PREPARE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected PREPARE, found " + cta::eos::Workflow_EventType_Name(event));
    reactor->Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected PREPARE, found " + cta::eos::Workflow_EventType_Name(event)));
    return reactor;
  }

  const std::string storageClass = request->notification().file().storage_class();
  if (storageClass.empty()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Storage class is not set");
    reactor->Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set."));
    return reactor;
  }

  // check validate request args
  if (request->notification().file().archive_file_id() == 0) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Invalid archive file id");
    reactor->Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id."));
    return reactor;
  }

  auto instance = request->notification().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());
  sp.add("storageClass", storageClass);
  sp.add("archiveID", request->notification().file().archive_file_id());
  sp.add("fileID", request->notification().file().disk_file_id());

  ProcessGrpcRequest(reactor, request, response, lc);
  return reactor;
}

::grpc::ServerUnaryReactor* CtaRpcImpl::CancelRetrieve(::grpc::CallbackServerContext* context,
                                  const cta::xrd::Request* request,
                                  cta::xrd::Response* response) {
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());

  lc.log(cta::log::DEBUG, "CancelRetrieve request");
  sp.add("request", "cancel");

  auto* reactor = context->DefaultReactor();

  // check validate request args
  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::ABORT_PREPARE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected ABORT_PREPARE, found " + cta::eos::Workflow_EventType_Name(event));
    reactor->Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected ABORT_PREPARE, found " + cta::eos::Workflow_EventType_Name(event)));
    return reactor;
  }

  if (!request->notification().file().archive_file_id()) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Invalid archive file id");
    reactor->Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id."));
    return reactor;
  }

  auto instance = request->notification().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());
  sp.add("fileID", request->notification().file().archive_file_id());
  sp.add("schedulerJobID", request->notification().file().archive_file_id());

  // field verification done, now try to call the process method
  ProcessGrpcRequest(reactor, request, response, lc);
  return reactor;
}

/* initialize the frontend service
 * this iniitalizes the catalogue, scheduler, logger
 * and makes the rpc calls available through this class
 */
CtaRpcImpl::CtaRpcImpl(const std::string& config)
    : m_frontendService(std::make_unique<cta::frontend::FrontendService>(config)) {}

} // namespace cta::frontend::grpc