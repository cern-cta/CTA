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

/* CallbackService class is the one that must implement the rpc methods,
 * using gRPC's async callback API (https://grpc.io/docs/languages/cpp/callback/)
 * this is the recommended way of implementing asynchronous calls (https://grpc.io/docs/languages/cpp/best_practices/).
 */
class CtaRpcStreamImpl : public cta::xrd::CtaRpcStream::CallbackService {
public:
  cta::log::LogContext getLogContext() const { return m_lc; }

  CtaRpcStreamImpl(const cta::service_resources::ServiceResources& serviceResources,
                   const cta::admin_api::AdminApiConfig& config,
                   RequestAuthenticator authenticator);

  /*
   * We override the base GenericAdminStream method with the actual code which is
   * triggered by the gRPC service
   *
   * gRPCpp expects the return type of an RPC implemented using the callback API to be
   * a pointer to ::grpc::ServerWriteReactor
   */
  ::grpc::ServerWriteReactor<cta::xrd::StreamResponse>* GenericAdminStream(::grpc::CallbackServerContext* context,
                                                                           const cta::xrd::Request* request) final;

private:
  cta::log::LogContext m_lc;               // <! Provided by the server context
  cta::catalogue::Catalogue& m_catalogue;  //!< Reference to CTA Catalogue
  cta::Scheduler& m_scheduler;             //!< Reference to CTA Scheduler
  std::string m_instanceName;              //!< Instance name
  cta::SchedulerDB_t& m_schedDb;           //!< Reference to CTA SchedulerDB
  std::string m_catalogueConnString;       //!< Provided by the server context
  uint64_t m_missingFileCopiesMinAgeSecs;  //!< Provided by the server context
  AdminCmdMode m_adminCommandMode;         //!< Which commands this admin API serves
  RequestAuthenticator m_authenticator;    //!< The request authenticator
};

}  // namespace cta::admin_api::rpc
