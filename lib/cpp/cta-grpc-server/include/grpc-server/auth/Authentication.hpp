/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"
#include "grpc-server/auth/jwt/JwtAuthManager.hpp"

#include <grpcpp/grpcpp.h>
#include <map>
#include <optional>
#include <set>
#include <string>

namespace cta::grpc_server::auth {

/** @brief Client metadata of a gRPC call, as returned by ServerContext::client_metadata() */
using GrpcMetadata = std::multimap<::grpc::string_ref, ::grpc::string_ref>;

/**
 * Checks the client's mTLS certificate identities against the identities accepted for the connecting instance.
 *
 * @param context            The gRPC server context, used to retrieve the peer's certificate identities
 * @param clientHost         The instance name claimed by the client, used for the resulting SecurityIdentity
 * @param serverInstanceName The instance name to use for the SecurityIdentity
 * @param acceptedIdentities Certificate identities (SAN) accepted for clientHost
 * @param lc                 Log context for logging
 * @return Pair of gRPC Status and optional SecurityIdentity (populated if authentication succeeds)
 */
std::pair<::grpc::Status, std::optional<cta::common::dataStructures::SecurityIdentity>>
authenticateMtls(::grpc::ServerContext* context,
                 const std::string& clientHost,
                 const std::string& serverInstanceName,
                 const std::set<std::string, std::less<>>& acceptedIdentities,
                 cta::log::LogContext& lc);

/**
 * Extract and validate a JWT ("Bearer") authorization header from gRPC metadata.
 * Any other authorization scheme is rejected.
 *
 * @param client_metadata The gRPC client metadata containing authorization header
 * @param jwtAuthManager JwtAuthManager used for token validation
 * @param ourInstance The instance name to use for the SecurityIdentity
 * @param clientHost The peer connection string
 * @param lc Log context for logging
 * @return Pair of gRPC Status and optional SecurityIdentity (populated if authentication succeeds)
 */
std::pair<::grpc::Status, std::optional<cta::common::dataStructures::SecurityIdentity>>
authenticateJwt(const GrpcMetadata& client_metadata,
                JwtAuthManager& jwtAuthManager,
                const std::string& ourInstance,
                const std::string& clientHost,
                cta::log::LogContext& lc);

}  // namespace cta::grpc_server::auth
