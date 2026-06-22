/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-FileCopyrightText: 2021 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FrontendGrpcService.hpp"

#include "catalogue/Catalogue.hpp"
#include "common/auth/JwtValidation.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogLevel.hpp"
#include "frontend/common/FrontendService.hpp"
#include "frontend/common/WorkflowEvent.hpp"
#include "frontend/grpc/common/GrpcAuthUtils.hpp"

#include <optional>

/*
 * Validate the storage class and issue the archive ID which should be used for the Archive request
 */

constexpr const char* CLIENT_IDENTITY_NOT_SET_ERROR = "clientIdentity not set in gRPC authentication";

using cta::common::dataStructures::SecurityIdentity;

namespace cta::frontend::grpc {

std::pair<::grpc::Status, std::optional<SecurityIdentity>>
CtaRpcImpl::checkWFERequestAuthMetadata(::grpc::ServerContext* context,
                                        const cta::xrd::Request* request,
                                        cta::log::LogContext& lc) {
  std::string clientHost = request->notification().wf().instance().name();
  std::string ourHost = m_frontendService->getInstanceName();

  if (m_frontendService->getWfeAuthMethod() == AuthMethod::MTLS) {
    auto auth_context = context->auth_context();
    // fetch the certificate identities from the auth context
    auto cert_idents = auth_context->GetPeerIdentity();

    std::set<std::string, std::less<>> cert_identities;
    std::ranges::transform(cert_idents, std::inserter(cert_identities, cert_identities.begin()), [](const auto& ref) {
      return std::string(ref.data(), ref.size());
    });

    lc.log(cta::log::DEBUG,
           "Received mTLS identities: "
             + cta::utils::joinCommaSeparated(std::vector(cert_identities.begin(), cert_identities.end())));

    auto instance_identities = m_frontendService->getMtlsCertIdentitiesForInstance(clientHost);

    lc.log(cta::log::DEBUG,
           "Resolved certificate identities for instance: "
             + cta::utils::joinCommaSeparated(std::vector(instance_identities.begin(), instance_identities.end())));

    std::set<std::string, std::less<>> intersect_set {};
    std::ranges::set_intersection(cert_identities,
                                  instance_identities,
                                  std::inserter(intersect_set, intersect_set.begin()));

    if (intersect_set.empty()) {
      // No identities match
      lc.log(cta::log::ERR, "Name " + clientHost + " doesn't match any of the certificate identities.");
      return {::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Certificate doesn't match identity"), std::nullopt};
    }

    SecurityIdentity clientIdentity(clientHost, ourHost, context->peer(), "grpc_mtls");
    return {::grpc::Status::OK, clientIdentity};
  } else if (m_frontendService->getWfeAuthMethod() == AuthMethod::JWT) {
    const auto& metadata = context->client_metadata();

    auto [status, clientIdentity] = cta::frontend::grpc::common::extractAuthHeaderAndValidate(
      metadata,
      m_frontendService->getWfeAuthMethod() == AuthMethod::JWT,
      m_pubkeyCache,
      m_tokenStorage,
      ourHost,
      context->peer(),
      lc);
    return {status, clientIdentity};
  } else {
    throw cta::exception::UserError("Unsupported authentication method for WFE requests: "
                                    + toString(m_frontendService->getWfeAuthMethod()));
  }
}

Status CtaRpcImpl::processGrpcRequest(const cta::xrd::Request* request,
                                      cta::xrd::Response* response,
                                      cta::log::LogContext& lc,
                                      const SecurityIdentity& clientIdentity) const {
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
    response->set_message_txt("Unexpected workflow event type. Expected CREATE, found "
                              + cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "Unexpected workflow event type. Expected CREATE, found "
                            + cta::eos::Workflow_EventType_Name(event));
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
    response->set_message_txt("Unexpected workflow event type. Expected CLOSEW, found "
                              + cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "Unexpected workflow event type. Expected CLOSEW, found "
                            + cta::eos::Workflow_EventType_Name(event));
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
    response->set_message_txt("Unexpected workflow event type. Expected DELETE, found "
                              + cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "Unexpected workflow event type. Expected DELETE, found "
                            + cta::eos::Workflow_EventType_Name(event));
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
    response->set_message_txt("Unexpected workflow event type. Expected PREPARE, found "
                              + cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "Unexpected workflow event type. Expected PREPARE, found "
                            + cta::eos::Workflow_EventType_Name(event));
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
    response->set_message_txt("Unexpected workflow event type. Expected ABORT_PREPARE, found "
                              + cta::eos::Workflow_EventType_Name(event));
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                          "Unexpected workflow event type. Expected ABORT_PREPARE, found "
                            + cta::eos::Workflow_EventType_Name(event));
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
  cta::log::LogContext lc(m_frontendService->getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  // Retrieve metadata from the incoming request
  const auto& metadata = context->client_metadata();

  auto [status, clientIdentity] =
    cta::frontend::grpc::common::extractAuthHeaderAndValidate(metadata,
                                                              m_frontendService->usesAdminAuthMethod(AuthMethod::JWT),
                                                              m_pubkeyCache,
                                                              m_tokenStorage,
                                                              m_frontendService->getInstanceName(),
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
                       std::shared_ptr<cta::auth::JwkCache> pubkeyCache,
                       server::TokenStorage& tokenStorage)
    : m_frontendService(frontendService),
      m_pubkeyCache(pubkeyCache),
      m_tokenStorage(tokenStorage) {}
}  // namespace cta::frontend::grpc
