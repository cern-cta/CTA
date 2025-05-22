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


#include <grpcpp/grpcpp.h>

#include <unordered_map>
#include <string>
#include <json/json.h>

class ServiceJWTAuthProcessor : public ::grpc::AuthMetadataProcessor {

public:
    explicit ServiceJWTAuthProcessor(const std::string& jwksUri): m_jwksUri(jwksUri) {}

    ::grpc::Status Process(const ::grpc::AuthMetadataProcessor::InputMetadata& authMetadata, ::grpc::AuthContext* context,
                    ::grpc::AuthMetadataProcessor::OutputMetadata* consumedAuthMetadata,
                    ::grpc::AuthMetadataProcessor::OutputMetadata* processedResponseMetadata) override;
    bool Validate(const std::string& encodedJWT);
    bool ValidateToken(const std::string& encodedJWT);

private:
    const std::string JWT_TOKEN_AUTH_METADATA_KEY = {"cta-grpc-jwt-auth-token"};
    Json::Value FetchJWKS(const std::string& jwksUrl);
    std::string m_jwksUri;
};
