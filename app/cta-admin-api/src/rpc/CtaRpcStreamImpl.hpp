/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "AdminApiConfig.hpp"
#include "common/log/LogContext.hpp"
#include "rpc/auth/RequestAuthenticator.hpp"
#include "service-resources/ServiceResources.hpp"

#include <cstdint>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

namespace cta::admin_api::rpc {

/// CallbackService class is the one that must implement the rpc methods,
/// using gRPC's async callback API (https://grpc.io/docs/languages/cpp/callback/)
/// this is the recommended way of implementing asynchronous calls (https://grpc.io/docs/languages/cpp/best_practices/).
class CtaRpcStreamImpl : public cta::xrd::CtaRpcStream::CallbackService {
public:
  cta::log::LogContext getLogContext() const { return m_lc; }

  CtaRpcStreamImpl(const cta::service_resources::ServiceResources& serviceResources,
                   const cta::admin_api::AdminApiConfig& config,
                   RequestAuthenticator authenticator);

  /// We override the base GenericAdminStream method with the actual code which is
  /// triggered by the gRPC service
  ///
  /// gRPCpp expects the return type of an RPC implemented using the callback API to be
  /// a pointer to ::grpc::ServerWriteReactor
  ::grpc::ServerWriteReactor<cta::xrd::StreamResponse>* GenericAdminStream(::grpc::CallbackServerContext* context,
                                                                           const cta::xrd::Request* request) final;

private:
  /// Provided by the server context
  cta::log::LogContext m_lc;
  /// Reference to CTA Catalogue
  cta::catalogue::Catalogue& m_catalogue;
  /// Reference to CTA Scheduler
  cta::Scheduler& m_scheduler;
  /// Instance name
  std::string m_instanceName;
  /// Reference to CTA SchedulerDB
  cta::SchedulerDB_t& m_schedDb;
  /// Provided by the server context
  std::string m_catalogueConnString;
  /// From the [policies.tapes] configuration
  uint64_t m_missingFileCopiesMinAgeSecs;
  /// Which commands this admin API serves
  AdminCmdMode m_adminCommandMode;
  /// The request authenticator
  RequestAuthenticator m_authenticator;
};

}  // namespace cta::admin_api::rpc
