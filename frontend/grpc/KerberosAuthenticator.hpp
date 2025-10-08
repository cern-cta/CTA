/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <grpcpp/security/credentials.h>

namespace cta::frontend::grpc::client {

class KerberosAuthenticator : public ::grpc::MetadataCredentialsPlugin {

public:
  explicit KerberosAuthenticator(const ::grpc::string& grpcstrToken) : m_grpcstrToken(grpcstrToken) {}

  ::grpc::Status GetMetadata(::grpc::string_ref serviceUrl,
                             ::grpc::string_ref methodName,
                             const ::grpc::AuthContext& channelAuthContext,
                             std::multimap<::grpc::string, ::grpc::string>* mumapMetadata) override {
    mumapMetadata->insert(std::make_pair("cat-grpc-kerberos-auth-token", m_grpcstrToken));
    return ::grpc::Status::OK;
  }

private:
  ::grpc::string m_grpcstrToken;

};

} // namespace cta::frontend::grpc::client
