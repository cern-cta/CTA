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

Status
CtaRpcImpl::GenericRequest(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  try {
    cta::eos::Client client = request->notification().cli();
    cta::common::dataStructures::SecurityIdentity clientIdentity(client.sec().name(), cta::utils::getShortHostname(),
                                                                  client.sec().host(), client.sec().prot());
    cta::frontend::WorkflowEvent wfe(*m_frontendService, clientIdentity, request->notification());
    *response = wfe.process();
  } catch (cta::exception::PbException &ex) {
    m_lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_PROTOBUF);
    response->set_message_txt(ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, ex.getMessageValue());
  } catch (cta::exception::UserError &ex) {
    m_lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, ex.getMessageValue());
  } catch (cta::exception::Exception &ex) {
    m_lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    response->set_message_txt(ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::UNKNOWN, ex.getMessageValue());
  } catch (std::runtime_error &ex) {
    m_lc.log(cta::log::ERR, ex.what());
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    response->set_message_txt(ex.what());
    return ::grpc::Status(::grpc::StatusCode::UNKNOWN, ex.what());
  } catch (...) {
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    return ::grpc::Status(::grpc::StatusCode::UNKNOWN, "Error processing gRPC GenericRequest");
  }
  return Status::OK;
}

Status
CtaRpcImpl::Create(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) noexcept {
  // check that the workflow is set appropriately for the create event
  auto event = request->notification().wf().event();
  if (event != cta::eos::Workflow::CREATE)
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected CREATE, found " + cta::eos::Workflow_EventType_Name(event));
  return GenericRequest(context, request, response);
}

Status
CtaRpcImpl::Archive(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) noexcept {
  // check validate request args
  const std::string storageClass = request->notification().file().storage_class();
  if (storageClass.empty()) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
  }

  auto event = request->notification().wf().event();
  if (event != cta::eos::Workflow::CLOSEW)
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected CLOSEW, found " + cta::eos::Workflow_EventType_Name(event));

  return GenericRequest(context, request, response);
}

Status
CtaRpcImpl::Delete(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) noexcept {
  cta::log::ScopedParamContainer sp(m_lc);

  sp.add("remoteHost", context->peer());
  sp.add("request", "delete");

  // check validate request args
  auto event = request->notification().wf().event();
  if (event != cta::eos::Workflow::DELETE)
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected DELETE, found " + cta::eos::Workflow_EventType_Name(event));

  if (request->notification().file().archive_file_id() == 0) {
    m_lc.log(cta::log::WARNING, "Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  // done with validation, now do the workflow processing
  return GenericRequest(context, request, response);
}

Status
CtaRpcImpl::Retrieve(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) noexcept {
  cta::log::ScopedParamContainer sp(m_lc);

  sp.add("remoteHost", context->peer());
  sp.add("request", "retrieve");

  auto event = request->notification().wf().event();
  if (event != cta::eos::Workflow::PREPARE)
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected PREPARE, found " + cta::eos::Workflow_EventType_Name(event));

  const std::string storageClass = request->notification().file().storage_class();
  if (storageClass.empty()) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
  }

  // check validate request args
  if (request->notification().file().archive_file_id() == 0) {
    m_lc.log(cta::log::WARNING, "Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  auto instance = request->notification().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());
  sp.add("storageClass", storageClass);
  sp.add("archiveID", request->notification().file().archive_file_id());
  sp.add("fileID", request->notification().file().disk_file_id());

  return GenericRequest(context, request, response);
}

Status CtaRpcImpl::CancelRetrieve(::grpc::ServerContext* context,
                                  const cta::xrd::Request* request,
                                  cta::xrd::Response* response) noexcept {
  cta::log::ScopedParamContainer sp(m_lc);

  sp.add("remoteHost", context->peer());

  m_lc.log(cta::log::DEBUG, "CancelRetrieve request");
  sp.add("request", "cancel");

  // check validate request args
  auto event = request->notification().wf().event();
  if (event != cta::eos::Workflow::ABORT_PREPARE)
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Unexpected workflow event type. Expected ABORT_PREPARE, found " + cta::eos::Workflow_EventType_Name(event));

  if (!request->notification().file().archive_file_id()) {
    m_lc.log(cta::log::WARNING, "Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  auto instance = request->notification().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());
  sp.add("fileID", request->notification().file().archive_file_id());
  sp.add("schedulerJobID", request->notification().file().archive_file_id());

  // field verification done, now try to call the process method
  return GenericRequest(context, request, response);
}

/* initialize the frontend service
 * this iniitalizes the catalogue, scheduler, logger
 * and makes the rpc calls available through this class
 */
CtaRpcImpl::CtaRpcImpl(const std::string& config)
    : m_frontendService(std::make_unique<cta::frontend::FrontendService>(config))
    , m_lc(m_frontendService->getLogContext()) {}

} // namespace cta::frontend::grpc