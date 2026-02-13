/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/auth/JwkCache.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"
#include "frontend/grpc/TokenStorage.hpp"

#include <grpcpp/grpcpp.h>
#include <optional>
#include <string>

namespace cta::frontend::grpc::common {

/**
 * Extract and validate JWT authorization header from gRPC metadata
 *
 * @param client_metadata The gRPC client metadata containing authorization header
 * @param jwtAuthEnabled Whether JWT authentication is enabled
 * @param pubkeyCache Shared pointer to the JWK cache for token validation (can be nullptr if JWT auth is disabled)
 * @param instanceName The instance name to use when JWT auth is disabled
 * @param peer The peer connection string
 * @param lc Log context for logging
 * @return Pair of gRPC Status and optional SecurityIdentity (populated if authentication succeeds)
 */
std::pair<::grpc::Status, std::optional<cta::common::dataStructures::SecurityIdentity>>
extractAuthHeaderAndValidate(const std::multimap<::grpc::string_ref, ::grpc::string_ref>& client_metadata,
                             bool jwtAuthEnabled,
                             std::shared_ptr<cta::auth::JwkCache> pubkeyCache,
                             server::TokenStorage& tokenStorage,
                             const std::string& instanceName,
                             const std::string& peer,
                             cta::log::LogContext& lc);

std::pair<::grpc::Status, std::string>
validateKrb5Token(const std::string& token, server::TokenStorage& tokenStorage, cta::log::LogContext& lc);

}  // namespace cta::frontend::grpc::common
