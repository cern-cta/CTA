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


#include "TokenStorage.hpp"

#include <grpcpp/grpcpp.h>

#include <unordered_map>
#include <string>

namespace cta {
namespace frontend {
namespace grpc {
namespace server {

class ServiceAuthProcessor : public ::grpc::AuthMetadataProcessor {

public:
  ServiceAuthProcessor(const TokenStorage& tokenStorage) : m_tokenStorage(tokenStorage) {
    
  }
  
  ::grpc::Status Process(const ::grpc::AuthMetadataProcessor::InputMetadata& authMetadata, ::grpc::AuthContext* pAuthCtx,
                 ::grpc::AuthMetadataProcessor::OutputMetadata* pConsumedAuthMetadata,
                 ::grpc::AuthMetadataProcessor::OutputMetadata* pResponseMetadata) override;
  
private:
  const std::string TOKEN_AUTH_METADATA_KEY = {"cat-grpc-kerberos-auth-token"};
  const TokenStorage& m_tokenStorage;
  
};

} // namespace server
} // namespace grpc
} // namespace frontend
} // namespace cta
