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

#include "ServiceJWTAuthProcessor.hpp"
#include <jwt-cpp/jwt.h>

::grpc::Status ServiceJWTAuthProcessor::Process(const ::grpc::AuthMetadataProcessor::InputMetadata& authMetadata,
    ::grpc::AuthContext* authContext,
    ::grpc::AuthMetadataProcessor::OutputMetadata* consumedAuthMetadata,
    ::grpc::AuthMetadataProcessor::OutputMetadata* responseMetadata) {
    
    if (!authContext) {
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, std::string("JWT authorization process internal error. AuthContext is set to NULL"));
    }
    if (!consumedAuthMetadata) {
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, std::string("JWT authorization process internal error. ConsumedAuthMetadata is set to NULL"));
    }
    if (!responseMetadata) {
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, std::string("JWT authorization process internal error. ResponseMetadata is set to NULL"));
    }
    auto iterAuthMetadata = authMetadata.find(JWT_TOKEN_AUTH_METADATA_KEY);
    if (iterAuthMetadata == authMetadata.end()) {
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, "JWT authorization process metadata error. Authorization token not found.");
    }

}