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
#include "FrontendGrpcService.hpp"

#include "catalogue/Catalogue.hpp"
#include "common/log/LogLevel.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "frontend/common/FrontendService.hpp"
#include "frontend/common/WorkflowEvent.hpp"
#include "frontend/common/ValidateToken.hpp"
#include "frontend/grpc/common/GrpcAuthUtils.hpp"

#include "jwt-cpp/jwt.h"
#include <optional>

/*
 * Validate the storage class and issue the archive ID which should be used for the Archive request
 */

constexpr const char* CTA_ADMIN_COMMANDS_DISABLED_ERROR =
  "CTA admin commands are disabled by configuration flag cta.experimental.grpc.cta_admin_commands.enabled";

constexpr const char* CLIENT_IDENTITY_NOT_SET_ERROR = "clientIdentity not set in gRPC authentication";

namespace cta::frontend::grpc {

std::pair<::grpc::Status, std::optional<cta::common::dataStructures::SecurityIdentity>>
CtaRpcImpl::checkWFERequestAuthMetadata(::grpc::ServerContext* context,
                                        const cta::xrd::Request* request,
                                        cta::log::LogContext& lc) {
  // Retrieve metadata from the incoming request
  auto metadata = context->client_metadata();

  // skip any metadata checks in case JWT Auth is disabled
  if (bool jwtAuthEnabled = m_frontendService->getJwtAuth(); !jwtAuthEnabled) {
    lc.log(cta::log::INFO, "Skipping token validation step as token authentication is disabled");
    cta::common::dataStructures::SecurityIdentity clientIdentity(request->notification().wf().instance().name(), context->peer());
    return {::grpc::Status::OK, clientIdentity};
  } else {
    auto [status, clientIdentity] =
      cta::frontend::grpc::common::extractAuthHeaderAndValidate(metadata,
                                                                m_frontendService->getJwtAuth(),
                                                                m_pubkeyCache,
                                                                m_tokenStorage,
                                                                request->notification().wf().instance().name(),
                                                                context->peer(),
                                                                lc);
    return std::make_pair(status, clientIdentity);
  }
}

Status CtaRpcImpl::processGrpcRequest(const cta::xrd::Request* request,
                                      cta::xrd::Response* response,
                                      cta::log::LogContext& lc,
                                      const cta::common::dataStructures::SecurityIdentity& clientIdentity) const {

  if(!m_frontendService->getUserRequestsAllowed()){
    constexpr const char* USER_REQUESTS_DISABLED_ERROR = "User requests are disabled.";
    response->set_message_txt(USER_REQUESTS_DISABLED_ERROR);
    response->set_type(xrd::Response::RSP_ERR_USER);
    return ::grpc::Status(::grpc::StatusCode::ABORTED, USER_REQUESTS_DISABLED_ERROR);
  }
  try {
    cta::frontend::WorkflowEvent wfe(*m_frontendService, clientIdentity, request->notification());
    *response = wfe.process();
  } catch (cta::exception::PbException& ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_PROTOBUF);
    response->set_message_txt(ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, ex.getMessageValue());
  } catch (cta::exception::UserError& ex) {
    lc.log(cta::log::INFO, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(ex.getMessageValue());
    /* Logic-wise, this should also be INVALID_ARGUMENT, but we need a way to
     * differentiate between different kinds of errors on the client side,
     * which is why we return ABORTED
     */
    return ::grpc::Status(::grpc::StatusCode::ABORTED, ex.getMessageValue());
  } catch (cta::exception::Exception& ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    response->set_message_txt(ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, ex.getMessageValue());
  } catch (std::runtime_error& ex) {
    lc.log(cta::log::ERR, ex.what());
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    response->set_message_txt(ex.what());
    return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, ex.what());
  } catch (...) {
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, "Error processing gRPC request");
  }
  return Status::OK;
}

Status
CtaRpcImpl::Create(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  auto [status, clientIdentity] = checkWFERequestAuthMetadata(context, request, lc);
  if (!status.ok()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(status.error_message());
    return status;
  }

  if (!clientIdentity.has_value()) {
    return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, CLIENT_IDENTITY_NOT_SET_ERROR);
  }

  sp.add("remoteHost", context->peer());
  sp.add("request", "create");
  // check that the workflow is set appropriately for the create event
  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::CREATE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected CREATE, found " +
                              cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "Unexpected workflow event type. Expected CREATE, found " +
                            cta::eos::Workflow_EventType_Name(event));
  }
  return processGrpcRequest(request, response, lc, clientIdentity.value());
}

Status
CtaRpcImpl::Archive(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  auto [status, clientIdentity] = checkWFERequestAuthMetadata(context, request, lc);

  if (!status.ok()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(status.error_message());
    return status;
  }

  if (!clientIdentity.has_value()) {
    return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, CLIENT_IDENTITY_NOT_SET_ERROR);
  }

  sp.add("remoteHost", context->peer());
  sp.add("request", "archive");
  // check validate request args
  if (const std::string storageClass = request->notification().file().storage_class(); storageClass.empty()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Storage class is not set.");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
  }

  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::CLOSEW) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected CLOSEW, found " +
                              cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "Unexpected workflow event type. Expected CLOSEW, found " +
                            cta::eos::Workflow_EventType_Name(event));
  }

  return processGrpcRequest(request, response, lc, clientIdentity.value());
}

Status
CtaRpcImpl::Delete(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  auto [status, clientIdentity] = checkWFERequestAuthMetadata(context, request, lc);

  if (!status.ok()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(status.error_message());
    return status;
  }

  if (!clientIdentity.has_value()) {
    return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, CLIENT_IDENTITY_NOT_SET_ERROR);
  }

  sp.add("remoteHost", context->peer());
  sp.add("request", "delete");

  // check validate request args
  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::DELETE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected DELETE, found " +
                              cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "Unexpected workflow event type. Expected DELETE, found " +
                            cta::eos::Workflow_EventType_Name(event));
  }

  if (request->notification().file().archive_file_id() == 0) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  // done with validation, now do the workflow processing
  return processGrpcRequest(request, response, lc, clientIdentity.value());
}

Status
CtaRpcImpl::Retrieve(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  auto [status, clientIdentity] = checkWFERequestAuthMetadata(context, request, lc);

  if (!status.ok()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(status.error_message());
    return status;
  }

  if (!clientIdentity.has_value()) {
    return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, CLIENT_IDENTITY_NOT_SET_ERROR);
  }

  sp.add("remoteHost", context->peer());
  sp.add("request", "retrieve");

  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::PREPARE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected PREPARE, found " +
                              cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "Unexpected workflow event type. Expected PREPARE, found " +
                            cta::eos::Workflow_EventType_Name(event));
  }

  const std::string storageClass = request->notification().file().storage_class();
  if (storageClass.empty()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Storage class is not set");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Storage class is not set.");
  }

  // check validate request args
  if (request->notification().file().archive_file_id() == 0) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  auto instance = request->notification().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());
  sp.add("storageClass", storageClass);
  sp.add("archiveID", request->notification().file().archive_file_id());
  sp.add("fileID", request->notification().file().disk_file_id());

  return processGrpcRequest(request, response, lc, clientIdentity.value());
}

Status CtaRpcImpl::CancelRetrieve(::grpc::ServerContext* context,
                                  const cta::xrd::Request* request,
                                  cta::xrd::Response* response) {
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  auto [status, clientIdentity] = checkWFERequestAuthMetadata(context, request, lc);

  if (!status.ok()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(status.error_message());
    return status;
  }

  if (!clientIdentity.has_value()) {
    return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, CLIENT_IDENTITY_NOT_SET_ERROR);
  }

  sp.add("remoteHost", context->peer());

  lc.log(cta::log::DEBUG, "CancelRetrieve request");
  sp.add("request", "cancel");

  // check validate request args
  if (auto event = request->notification().wf().event(); event != cta::eos::Workflow::ABORT_PREPARE) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Unexpected workflow event type. Expected ABORT_PREPARE, found " +
                              cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "Unexpected workflow event type. Expected ABORT_PREPARE, found " +
                            cta::eos::Workflow_EventType_Name(event));
  }

  if (!request->notification().file().archive_file_id()) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }

  auto instance = request->notification().wf().instance().name();

  sp.add("instance", instance);
  sp.add("username", request->notification().cli().user().username());
  sp.add("groupname", request->notification().cli().user().groupname());
  sp.add("fileID", request->notification().file().archive_file_id());
  sp.add("schedulerJobID", request->notification().file().archive_file_id());

  // field verification done, now try to call the process method
  return processGrpcRequest(request, response, lc, clientIdentity.value());
}

Status
CtaRpcImpl::Admin(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  if (!m_frontendService->getenableCtaAdminCommands()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(CTA_ADMIN_COMMANDS_DISABLED_ERROR);
    return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, CTA_ADMIN_COMMANDS_DISABLED_ERROR);
  }

  // Check if repack requests are blocked
  if(!m_frontendService->getRepackRequestsAllowed() &&
        request->admincmd().subcmd() != admin::AdminCmd::SUBCMD_LS &&
        request->admincmd().cmd() == admin::AdminCmd::CMD_REPACK){
    constexpr const char* REPACK_REQUESTS_DISABLED_ERROR = "Repack requests are disabled.";
    response->set_message_txt(REPACK_REQUESTS_DISABLED_ERROR);
    response->set_type(xrd::Response::RSP_ERR_USER);
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, REPACK_REQUESTS_DISABLED_ERROR);
  }

  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  // Retrieve metadata from the incoming request
  auto metadata = context->client_metadata();

  auto [status, clientIdentity] =
    cta::frontend::grpc::common::extractAuthHeaderAndValidate(metadata,
                                                              m_frontendService->getJwtAuth(),
                                                              m_pubkeyCache,
                                                              m_tokenStorage,
                                                              request->notification().wf().instance().name(),
                                                              context->peer(),
                                                              lc);
  if (!status.ok()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(status.error_message());
    return status;
  }
  if (!clientIdentity.has_value()) {
    return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, CLIENT_IDENTITY_NOT_SET_ERROR);
  }

  sp.add("remoteHost", context->peer());

  lc.log(cta::log::DEBUG, "CTA-Admin non-streaming command");
  // sp.add("request", "admin");
  // process the admin command
  // create a securityIdentity cli_Identity
  try {
    cta::frontend::AdminCmd adminCmd(*m_frontendService, clientIdentity.value(), request->admincmd());
    *response = adminCmd.process();  // success response code will be set in here if processing goes well
  } catch (cta::exception::PbException& ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_PROTOBUF);
    response->set_message_txt(ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, ex.getMessageValue());
  } catch (cta::exception::UserError& ex) {
    lc.log(cta::log::INFO, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, ex.getMessageValue());
  } catch (cta::exception::Exception& ex) {
    lc.log(cta::log::ERR, ex.getMessageValue());
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    response->set_message_txt(ex.getMessageValue());
    return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, ex.getMessageValue());
  } catch (std::runtime_error& ex) {
    lc.log(cta::log::ERR, ex.what());
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    response->set_message_txt(ex.what());
    return ::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, ex.what());
  } catch (...) {
    response->set_type(cta::xrd::Response::RSP_ERR_CTA);
    return ::grpc::Status(::grpc::StatusCode::UNKNOWN, "Error processing gRPC Admin request");
  }

  return Status::OK;
}

/* initialize the frontend service
 * this iniitalizes the catalogue, scheduler, logger
 * and makes the rpc calls available through this class
 */
CtaRpcImpl::CtaRpcImpl(std::shared_ptr<cta::frontend::FrontendService> frontendService,
                       std::shared_ptr<JwkCache> pubkeyCache,
                       server::TokenStorage& tokenStorage)
    : m_frontendService(frontendService),
      m_pubkeyCache(pubkeyCache),
      m_tokenStorage(tokenStorage) {}
}  // namespace cta::frontend::grpc
