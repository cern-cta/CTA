/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "CtaRpcWfeImpl.hpp"

#include "WorkflowEvent.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogLevel.hpp"
#include "grpc-server/PbException.hpp"
#include "grpc-server/auth/Authentication.hpp"
#include "grpc-server/auth/jwt/JwtAuthManager.hpp"
#include "protobuf/common/ChecksumBlobSerDeser.hpp"

#include <optional>

const std::string CLIENT_IDENTITY_NOT_SET_ERROR {"clientIdentity not set in gRPC authentication"};

using cta::common::dataStructures::SecurityIdentity;
using grpc::Status;

namespace cta::workflow_api {

std::pair<::grpc::Status, std::optional<SecurityIdentity>>
CtaRpcWfeImpl::validateRequestAuthMetadata(::grpc::ServerContext* context,
                                           const cta::xrd::Request* request,
                                           cta::log::LogContext& lc) const {
  // config validation guarantees that exactly one of mTLS or JWT is enabled
  if (usesMtls()) {
    std::string clientHost = request->notification().wf().instance().name();
    return cta::grpc_server::auth::authenticateMtls(context,
                                                    clientHost,
                                                    m_config.instance_name,
                                                    getMtlsCertIdentitiesForInstance(clientHost),
                                                    lc);
  }
  return cta::grpc_server::auth::authenticateJwt(context->client_metadata(),
                                                 *m_jwtAuthManager,
                                                 m_config.instance_name,
                                                 context->peer(),
                                                 lc);
}

Status CtaRpcWfeImpl::processGrpcRequest(const cta::xrd::Request* request,
                                         cta::xrd::Response* response,
                                         cta::log::LogContext& lc,
                                         const SecurityIdentity& clientIdentity) const {
  try {
    cta::frontend::WorkflowEvent wfe(m_serviceResources,
                                     m_config,
                                     clientIdentity,
#ifdef CTA_PGSCHED
                                     m_batchingLayer,
#endif
                                     request->notification());
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

namespace {

/// @brief Rejects the request if the archive file id is not set
Status checkArchiveFileId(const cta::xrd::Request* request, cta::xrd::Response* response, cta::log::LogContext& lc) {
  if (request->notification().file().archive_file_id() == 0) {
    lc.log(cta::log::WARNING, "Invalid archive file id");
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt("Invalid archive file id");
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Invalid archive file id.");
  }
  return Status::OK;
}

}  // namespace

Status CtaRpcWfeImpl::handleWorkflowEvent(::grpc::ServerContext* context,
                                          const cta::xrd::Request* request,
                                          cta::xrd::Response* response,
                                          const char* requestName,
                                          cta::eos::Workflow_EventType expectedEvent,
                                          const RequestCheck& check) const {
  cta::log::LogContext lc(m_serviceResources.getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  sp.add("remoteHost", context->peer());
  sp.add("request", requestName);

  auto [status, clientIdentity] = validateRequestAuthMetadata(context, request, lc);
  if (!status.ok()) {
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(status.error_message());
    return status;
  }

  if (!clientIdentity.has_value()) {
    return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, CLIENT_IDENTITY_NOT_SET_ERROR);
  }

  if (auto event = request->notification().wf().event(); event != expectedEvent) {
    const std::string message = "Unexpected workflow event type. Expected "
                                + cta::eos::Workflow_EventType_Name(expectedEvent) + ", found "
                                + cta::eos::Workflow_EventType_Name(event);
    response->set_type(cta::xrd::Response::RSP_ERR_USER);
    response->set_message_txt(message);
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, message);
  }

  if (check) {
    if (Status checkStatus = check(lc, sp); !checkStatus.ok()) {
      return checkStatus;
    }
  }

  return processGrpcRequest(request, response, lc, clientIdentity.value());
}

Status
CtaRpcWfeImpl::Create(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  return handleWorkflowEvent(context, request, response, "create", cta::eos::Workflow::CREATE, nullptr);
}

Status
CtaRpcWfeImpl::Archive(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  return handleWorkflowEvent(context,
                             request,
                             response,
                             "archive",
                             cta::eos::Workflow::CLOSEW,
                             [request, response](cta::log::LogContext&, cta::log::ScopedParamContainer&) -> Status {
                               if (request->notification().file().storage_class().empty()) {
                                 response->set_type(cta::xrd::Response::RSP_ERR_USER);
                                 response->set_message_txt("Storage class is not set.");
                                 return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT,
                                                       "Storage class is not set.");
                               }
                               return Status::OK;
                             });
}

Status
CtaRpcWfeImpl::Delete(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  return handleWorkflowEvent(context,
                             request,
                             response,
                             "delete",
                             cta::eos::Workflow::DELETE,
                             [request, response](cta::log::LogContext& lc, cta::log::ScopedParamContainer&) -> Status {
                               return checkArchiveFileId(request, response, lc);
                             });
}

Status CtaRpcWfeImpl::Retrieve(::grpc::ServerContext* context,
                               const cta::xrd::Request* request,
                               cta::xrd::Response* response) {
  return handleWorkflowEvent(
    context,
    request,
    response,
    "retrieve",
    cta::eos::Workflow::PREPARE,
    [request, response](cta::log::LogContext& lc, cta::log::ScopedParamContainer& sp) -> Status {
      if (Status status = checkArchiveFileId(request, response, lc); !status.ok()) {
        return status;
      }

      sp.add("instance", request->notification().wf().instance().name());
      sp.add("username", request->notification().cli().user().username());
      sp.add("groupname", request->notification().cli().user().groupname());
      sp.add("archiveID", request->notification().file().archive_file_id());
      sp.add("fileID", request->notification().file().disk_file_id());
      return Status::OK;
    });
}

Status CtaRpcWfeImpl::CancelRetrieve(::grpc::ServerContext* context,
                                     const cta::xrd::Request* request,
                                     cta::xrd::Response* response) {
  return handleWorkflowEvent(
    context,
    request,
    response,
    "cancel",
    cta::eos::Workflow::ABORT_PREPARE,
    [request, response](cta::log::LogContext& lc, cta::log::ScopedParamContainer& sp) -> Status {
      lc.log(cta::log::DEBUG, "CancelRetrieve request");

      if (Status status = checkArchiveFileId(request, response, lc); !status.ok()) {
        return status;
      }

      sp.add("instance", request->notification().wf().instance().name());
      sp.add("username", request->notification().cli().user().username());
      sp.add("groupname", request->notification().cli().user().groupname());
      sp.add("fileID", request->notification().file().archive_file_id());
      sp.add("schedulerJobID", request->notification().file().archive_file_id());
      return Status::OK;
    });
}

bool CtaRpcWfeImpl::usesMtls() const {
  return m_config.auth.mtls.has_value() && m_config.auth.mtls->enabled;
}

std::set<std::string, std::less<>> CtaRpcWfeImpl::getMtlsCertIdentitiesForInstance(const std::string& instance) const {
  std::set<std::string, std::less<>> identities;
  if (m_config.auth.mtls.has_value()) {
    if (const auto search = m_config.auth.mtls->aliases.find(instance); search != m_config.auth.mtls->aliases.end()) {
      identities.insert(search->second.begin(), search->second.end());
    }
  }
  return identities;
}

CtaRpcWfeImpl::CtaRpcWfeImpl(service_resources::ServiceResources& serviceResources,
                             const WorkflowApiConfig& config,
#ifdef CTA_PGSCHED
                             FrontendBatchingLayer& batchingLayer,
#endif
                             std::shared_ptr<grpc_server::auth::JwtAuthManager> jwtAuthManager)
    : m_serviceResources(serviceResources),
      m_config(config),
#ifdef CTA_PGSCHED
      m_batchingLayer(batchingLayer),
#endif
      m_jwtAuthManager(std::move(jwtAuthManager)) {
}

}  // namespace cta::workflow_api
