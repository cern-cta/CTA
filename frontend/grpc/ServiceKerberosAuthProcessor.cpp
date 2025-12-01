/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2023 CERN
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
 

#include "ServiceKerberosAuthProcessor.hpp"

#include "common/exception/Exception.hpp"
#include <unordered_set>

::grpc::Status cta::frontend::grpc::server::ServiceKerberosAuthProcessor::Process(
  const ::grpc::AuthMetadataProcessor::InputMetadata& authMetadata, ::grpc::AuthContext* pAuthCtx,
  ::grpc::AuthMetadataProcessor::OutputMetadata* pConsumedAuthMetadata,
  ::grpc::AuthMetadataProcessor::OutputMetadata* pResponseMetadata) {
  
  
  if (!pAuthCtx) {
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, std::string("KRB5 authorization process internal error. AuthContext is set to NULL."));
  }
  if (!pConsumedAuthMetadata) {
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, std::string("KRB5 authorization process internal error. ConsumedAuthMetadata is set to NULL."));
  }
  if (!pResponseMetadata) {
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, std::string("KRB5 authorization process internal error. ResponseMetadata is set to NULL."));
  }
  
  // Determine intercepted method
  std::string strAuthMetadataKey = {":path"};
  std::string strAuthMetadataValue = {""};
  std::string strDecodedToken = {""};
  
  std::multimap<::grpc::string_ref, ::grpc::string_ref>::const_iterator iterAuthMetadata = authMetadata.find(strAuthMetadataKey);
  
  if (iterAuthMetadata == authMetadata.end()) {
    return ::grpc::Status(::grpc::StatusCode::INTERNAL, "KRB5 authorization process metadata error. Path to the authorization token not found.");
  }
  
  /*
   * Skipping token checking and early return for services,
   * that do not require authorization.
   */
  strAuthMetadataValue = std::string(iterAuthMetadata->second.data(), iterAuthMetadata->second.length());
  if (strAuthMetadataValue == "/cta.xrd.Negotiation/Negotiate") {
    return ::grpc::Status::OK;
  }
  // Skip authentication for gRPC health checks
  if (strAuthMetadataValue == "/grpc.health.v1.Health/Check") {
    return ::grpc::Status::OK;
  }
  // Skip Kerberos auth for the physics workflow events, because these will be checked inside the rpc implementation for credentials
  if (std::unordered_set<std::string> allowed {"/cta.xrd.CtaRpc/Create",
                                               "/cta.xrd.CtaRpc/Archive",
                                               "/cta.xrd.CtaRpc/Retrieve",
                                               "/cta.xrd.CtaRpc/Delete",
                                               "/cta.xrd.CtaRpc/CancelRetrieve"};
      allowed.contains(strAuthMetadataValue)) {
    return ::grpc::Status::OK;
  }
  /*
   * Checking the presence of the token.
   */
  iterAuthMetadata = authMetadata.find(TOKEN_AUTH_METADATA_KEY);
  if (iterAuthMetadata == authMetadata.end()) {
    return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "KRB5 authorization process metadata error. Missing authorization token.");
  }
  /*
   * Validation 
   */
  strAuthMetadataValue = std::string(iterAuthMetadata->second.data(), iterAuthMetadata->second.length());
  /*
   * Token decoding
   */
  if(m_tokenStorage.validate(strAuthMetadataValue)) {
    // If ok consume
    pConsumedAuthMetadata->insert(std::make_pair(TOKEN_AUTH_METADATA_KEY, strAuthMetadataValue));
    m_tokenStorage.remove(strAuthMetadataValue);
    return ::grpc::Status::OK;
  }
  // else UNAUTHENTICATED
  return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "KRB5 authorization process error. Invalid principal.");
}
