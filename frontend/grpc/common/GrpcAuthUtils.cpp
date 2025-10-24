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

#include "GrpcAuthUtils.hpp"
#include "frontend/common/ValidateToken.hpp"
#include "common/log/LogLevel.hpp"

namespace cta::frontend::grpc::common {

std::pair<::grpc::Status, std::optional<cta::common::dataStructures::SecurityIdentity>>
extractAuthHeaderAndValidate(const std::multimap<::grpc::string_ref, ::grpc::string_ref>& client_metadata,
                             bool jwtAuthEnabled,
                             std::shared_ptr<JwkCache> pubkeyCache,
                             const std::string& instanceName,
                             const std::string& peer,
                             cta::log::LogContext& lc) {
  cta::log::ScopedParamContainer sp(lc);

  // skip any metadata checks in case JWT Auth is disabled
  if (!jwtAuthEnabled) {
    lc.log(cta::log::INFO, "Skipping token validation step as token authentication is disabled");
    cta::common::dataStructures::SecurityIdentity clientIdentity(instanceName, peer);
    return {::grpc::Status::OK, clientIdentity};
  }

  std::string token;

  // Search for the authorization token in the metadata
  if (auto it = client_metadata.find("authorization"); it != client_metadata.end()) {
    // convert from grpc structure to string
    const ::grpc::string_ref& r = it->second;
    auto auth_header = std::string(r.data(), r.size());  // "Bearer <token>"
    token = auth_header.substr(
      7);  // Extract the token part, use substr(7) because that is the length of "Bearer" plus a space character
    lc.log(cta::log::DEBUG, std::string("Received token: ") + token);
    if (token.empty()) {
      lc.log(cta::log::WARNING, "Authorization token missing");
      return {::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Missing Authorization token"), std::nullopt};
    }
  } else {
    lc.log(cta::log::WARNING, "Authorization header missing");
    return {::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Missing Authorization header"), std::nullopt};
  }

  auto validationResult = validateToken(token, pubkeyCache, lc);
  if (validationResult.isValid) {
    cta::common::dataStructures::SecurityIdentity clientIdentity(validationResult.subjectClaim.value(), peer);
    return {::grpc::Status::OK, clientIdentity};
  } else {
    lc.log(cta::log::WARNING, "JWT authorization process error. Token validation failed.");
    return {
      ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "JWT authorization process error. Token validation failed."),
      std::nullopt};
  }
}

} // namespace cta::frontend::grpc::common
