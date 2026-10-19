/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rpc/CtaRpcSyncImpl.hpp"

#include "commands/sync/SyncCmd.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "grpc-server/PbException.hpp"

namespace cta::admin_api::rpc {

namespace {
constexpr const char* CLIENT_IDENTITY_NOT_SET_ERROR = "clientIdentity not set in gRPC authentication";
}  // namespace

CtaRpcSyncImpl::CtaRpcSyncImpl(grpc_server::ServiceContext& serviceContext,
                               const AdminApiConfig& config,
                               RequestAuthenticator authenticator)
    : m_serviceContext(serviceContext),
      m_config(config),
      m_authenticator(std::move(authenticator)) {}

::grpc::Status
CtaRpcSyncImpl::Admin(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) {
  cta::log::LogContext lc(m_serviceContext.getLogContext());
  cta::log::ScopedParamContainer sp(lc);

  // Retrieve metadata from the incoming request
  const auto& metadata = context->client_metadata();

  auto [status, clientIdentity] = m_authenticator.authenticate(metadata, context->peer(), lc);
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
  // process the admin command
  try {
    cta::admin_api::commands::sync::SyncCmd syncCmd(m_serviceContext,
                                                    m_config,
                                                    clientIdentity.value(),
                                                    request->admincmd());
    *response = syncCmd.process();  // success response code will be set in here if processing goes well
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

  return ::grpc::Status::OK;
}

}  // namespace cta::admin_api::rpc
