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

bool ServiceJWTAuthProcessor::Validate(std::string encodedJWT) {
    std::cout << "Passed in token is " << encodedJWT << std::endl;
    return true; // unimplemented, this is what will validate the passed in token
}

// alternative
// bool validateToken(const std::string& token) {
//         try {
//             auto decoded = jwt::decode(token);
//             // Example validation: check if the token is expired
//             auto exp = decoded.get_payload_claim("exp").as_datetime();
//             if (exp < std::chrono::system_clock::now()) {
//                 return false;  // Token has expired
//             }
//             // Further validations (like signature, audience, etc.) can be added here
//             return true;
//         } catch (const jwt::token_verification_exception& e) {
//             return false;
//         }
//     }

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
    /*
     * Validation 
     */
    const auto strAuthMetadataValue = std::string(iterAuthMetadata->second.data(), iterAuthMetadata->second.length());
    /*
     * Token decoding
     */
    if(Validate(strAuthMetadataValue)) {
        // If ok consume
        consumedAuthMetadata->insert(std::make_pair(JWT_TOKEN_AUTH_METADATA_KEY, strAuthMetadataValue));
        return ::grpc::Status::OK;
    }
    // else UNAUTHENTICATED
    return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "JWT authorization process error. Invalid principal.");

}