/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once


#include "TokenStorage.hpp"

#include <grpcpp/grpcpp.h>

#include <unordered_map>
#include <string>

namespace cta::frontend::grpc::server {

class ServiceKerberosAuthProcessor : public ::grpc::AuthMetadataProcessor {

public:
  explicit ServiceKerberosAuthProcessor(const TokenStorage& tokenStorage) : m_tokenStorage(tokenStorage) {}

  ::grpc::Status Process(const ::grpc::AuthMetadataProcessor::InputMetadata& authMetadata, ::grpc::AuthContext* pAuthCtx,
                 ::grpc::AuthMetadataProcessor::OutputMetadata* pConsumedAuthMetadata,
                 ::grpc::AuthMetadataProcessor::OutputMetadata* pResponseMetadata) override;

private:
  const std::string TOKEN_AUTH_METADATA_KEY = {"cat-grpc-kerberos-auth-token"};
  const TokenStorage& m_tokenStorage;

};

} // namespace cta::frontend::grpc::server
