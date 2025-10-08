/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */


#include "ServiceKerberosAuthProcessor.hpp"

#include "common/exception/Exception.hpp"

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
    return ::grpc::Status::OK;
  }
  // else UNAUTHENTICATED
  return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "KRB5 authorization process error. Invalid principal.");
}
