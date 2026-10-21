/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rpc/CtaRpcStreamImpl.hpp"

#include "commands/stream/StreamFactory.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "common/semconv/Attributes.hpp"
#include "grpc-server/PbException.hpp"
#include "grpc-server/auth/jwt/JwtAuthManager.hpp"
#include "grpc-server/server/RequestTracker.hpp"
#include "rpc/reactors/CtaAdminServerWriteReactor.hpp"
#include "rpc/reactors/DefaultWriteReactor.hpp"
#include "service-resources/ServiceResources.hpp"

#include <grpcpp/grpcpp.h>
#include <memory>
#include <optional>
#include <string>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

namespace cta::admin_api::rpc {

CtaRpcStreamImpl::CtaRpcStreamImpl(const cta::service_resources::ServiceResources& serviceResources,
                                   const cta::admin_api::AdminApiConfig& config,
                                   RequestAuthenticator authenticator)
    : m_lc(serviceResources.getLogger()),
      m_catalogue(serviceResources.getCatalogue()),
      m_scheduler(serviceResources.getScheduler()),
      m_instanceName(config.instance_name),
      m_schedDb(serviceResources.getSchedDb()),
      m_catalogueConnString(serviceResources.getCatalogueConnString()),
      m_missingFileCopiesMinAgeSecs(config.files.missing_file_copies_min_age_secs),
      m_adminCommandMode(config.admin_mode),
      m_authenticator(std::move(authenticator)) {}

::grpc::ServerWriteReactor<cta::xrd::StreamResponse>*
CtaRpcStreamImpl::GenericAdminStream(::grpc::CallbackServerContext* context, const cta::xrd::Request* request) {
  // Authenticate the request using JWT if enabled
  cta::log::LogContext lc(m_lc);
  // get the client metadata for authentication
  const auto& client_metadata = context->client_metadata();
  auto [status, clientIdentity] = m_authenticator.authenticate(client_metadata, context->peer(), lc);
  if (!status.ok()) {
    return new DefaultWriteReactor(status.error_message(), status.error_code());
  }

  cta::grpc_server::RequestTracker requestTracker("ADMIN_STREAMING", "admin");
  const auto& adminCmd = request->admincmd();

  std::unique_ptr<commands::stream::CtaAdminResponseStream> stream;
  try {
    const commands::stream::StreamContext streamContext {m_catalogue,
                                                         m_scheduler,
                                                         m_schedDb,
                                                         m_lc,
                                                         m_instanceName,
                                                         m_catalogueConnString,
                                                         m_missingFileCopiesMinAgeSecs,
                                                         m_adminCommandMode};
    stream = commands::stream::createStream(streamContext, adminCmd);
  } catch (const cta::exception::UserError& ex) {
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kUserError);
    return new DefaultWriteReactor(ex.getMessageValue(), ::grpc::StatusCode::INVALID_ARGUMENT);
  } catch (const cta::exception::PbException& ex) {
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
    return new DefaultWriteReactor(ex.getMessageValue(), ::grpc::StatusCode::FAILED_PRECONDITION);
  } catch (const cta::exception::Exception& ex) {
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
    return new DefaultWriteReactor(ex.getMessageValue(), ::grpc::StatusCode::FAILED_PRECONDITION);
  } catch (const std::exception& ex) {
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
    return new DefaultWriteReactor(ex.what(), ::grpc::StatusCode::UNKNOWN);
  }

  if (!stream) {
    // Just to return an error status code when the specified command is not implemented
    const std::string errMsg("Admin command pair <" + cta::admin::AdminCmd_Cmd_Name(adminCmd.cmd()) + ", "
                             + cta::admin::AdminCmd_SubCmd_Name(adminCmd.subcmd()) + "> is not implemented.");
    requestTracker.setErrorType(cta::semconv::attr::ErrorTypeValues::kException);
    return new DefaultWriteReactor(errMsg);
  }

  // proceed with command execution if no exception was thrown
  return new CtaAdminServerWriteReactor(std::move(stream));
}

}  // namespace cta::admin_api::rpc
