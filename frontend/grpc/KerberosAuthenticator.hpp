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
 
#pragma once

#include <grpcpp/security/credentials.h>

namespace cta::frontend::grpc::client {

class KerberosAuthenticator : public ::grpc::MetadataCredentialsPlugin {
  
public:
  KerberosAuthenticator(const ::grpc::string& grpcstrToken) : m_grpcstrToken(grpcstrToken) {
    
  }

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
