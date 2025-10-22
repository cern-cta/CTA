/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <string>
#include <optional>
#include <grpcpp/grpcpp.h>
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/LogContext.hpp"
#include "common/JwkCache.hpp"
#include "../TokenStorage.hpp"

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
                             std::shared_ptr<JwkCache> pubkeyCache,
                             server::TokenStorage& tokenStorage,
                             const std::string& instanceName,
                             const std::string& peer,
                             cta::log::LogContext& lc);

std::pair<::grpc::Status, std::string> validateKrb5Token(const std::string& token, server::TokenStorage& tokenStorage, cta::log::LogContext& lc);

}  // namespace cta::frontend::grpc::common
