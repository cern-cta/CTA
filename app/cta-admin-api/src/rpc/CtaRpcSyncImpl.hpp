/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "AdminApiConfig.hpp"
#include "common/log/Logger.hpp"
#include "grpc-server/server/ServiceContext.hpp"
#include "rpc/auth/RequestAuthenticator.hpp"

#include <memory>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

namespace cta::admin_api::rpc {

/**
 * @brief Implements the non-streaming Admin RPC of the admin API.
 *
 * The other methods of the CtaRpc service (the workflow archive/retrieve/delete ones) are not served by the admin API,
 * so they keep the default UNIMPLEMENTED behaviour of the generated service.
 */
class CtaRpcSyncImpl final : public cta::xrd::CtaRpc::Service {
public:
  CtaRpcSyncImpl(grpc_server::ServiceContext& serviceContext,
                 const AdminApiConfig& config,
                 RequestAuthenticator authenticator);

  ::grpc::Status
  Admin(::grpc::ServerContext* context, const cta::xrd::Request* request, cta::xrd::Response* response) final;

private:
  grpc_server::ServiceContext& m_serviceContext;
  const AdminApiConfig& m_config;
  RequestAuthenticator m_authenticator;
};

}  // namespace cta::admin_api::rpc
