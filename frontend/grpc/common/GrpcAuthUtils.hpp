/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/auth/jwt/JwtAuthManager.hpp"
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
 * @param jwtAuthManager Shared pointer to the JwtAuthManager for token validation (can be nullptr if JWT auth is disabled)
 * @param instanceName The instance name to use when JWT auth is disabled
 * @param peer The peer connection string
 * @param lc Log context for logging
 * @return Pair of gRPC Status and optional SecurityIdentity (populated if authentication succeeds)
 */
std::pair<::grpc::Status, std::optional<cta::common::dataStructures::SecurityIdentity>>
extractAuthHeaderAndValidate(const std::multimap<::grpc::string_ref, ::grpc::string_ref>& client_metadata,
                             std::shared_ptr<cta::auth::JwtAuthManager> jwtAuthManager,
                             server::TokenStorage& tokenStorage,
                             const std::string& ourInstance,
                             const std::string& clientHost,
                             cta::log::LogContext& lc);

std::pair<::grpc::Status, std::string>
validateKrb5Token(const std::string& token, server::TokenStorage& tokenStorage, cta::log::LogContext& lc);

}  // namespace cta::frontend::grpc::common
