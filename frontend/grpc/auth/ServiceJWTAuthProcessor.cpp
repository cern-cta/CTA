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
#include <json/json.h>
#include <curl/curl.h>

bool ServiceJWTAuthProcessor::Validate(const std::string& encodedJWT) {
    std::cout << "Passed in token is " << encodedJWT << std::endl;
    return true; // unimplemented, this is what will validate the passed in token
}

// Function to handle curl responses
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* output) {
    size_t totalSize = size * nmemb;
    output->append((char*)contents, totalSize);
    return totalSize;
}

Json::Value ServiceJWTAuthProcessor::FetchJWKS(const std::string& jwksUrl) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;

    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, jwksUrl.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        res = curl_easy_perform(curl);
        if(res != CURLE_OK) {
            std::cout << "Curl failed: " << curl_easy_strerror(res) << std::endl;
            throw std::runtime_error("CURL failed in FetchJWKS");
        }
        curl_easy_cleanup(curl);
    }
    curl_global_cleanup();

    // Parse the response JSON
    Json::CharReaderBuilder readerBuilder;
    Json::Value jwks;
    std::istringstream sstream(readBuffer);
    std::string errs;
    if (!Json::parseFromStream(readerBuilder, sstream, &jwks, &errs)) {
        std::cout << "Failed to parse JSON: " << errs << std::endl;
        // throw some exception here
        throw std::runtime_error("Failed to parse JSON in FetchJWKS");
    }

    return jwks;
}

// alternative
// bool ServiceJWTAuthProcessor::ValidateToken(const std::string& encodedJWT) {
//     try {
//         auto decoded = jwt::decode(encodedJWT);
//         // Example validation: check if the token is expired
//         auto exp = decoded.get_payload_claim("exp").as_datetime();
//         if (exp < std::chrono::system_clock::now()) {
//             std::cout << "Passed-in token has expired!" << std::endl;
//             return false;  // Token has expired
//         }
//         // Get the JWKS endpoint, find the matching with our token, obtain the public key
//         // used to sign the token and validate it
//         // Further validations (like signature, audience, etc.) can be added here
//         return true;
//     } catch (const jwt::token_verification_exception& e) {
//         return false;
//     }
// }

::grpc::Status ServiceJWTAuthProcessor::Process(const ::grpc::AuthMetadataProcessor::InputMetadata& authMetadata,
    ::grpc::AuthContext* authContext,
    ::grpc::AuthMetadataProcessor::OutputMetadata* consumedAuthMetadata,
    ::grpc::AuthMetadataProcessor::OutputMetadata* responseMetadata) {

    std::cout << "In ServiceJWTAuthProcessor, method Process, just got in here" << std::endl;
    std::string msg;

    if (!authContext) {
        msg = std::string("JWT authorization process internal error. AuthContext is set to NULL");
        std::cout << msg << std::endl;
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, msg);
    }
    if (!consumedAuthMetadata) {
        msg = std::string("JWT authorization process internal error. ConsumedAuthMetadata is set to NULL");
        std::cout << msg << std::endl;
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, msg);
    }
    if (!responseMetadata) {
        msg = std::string("JWT authorization process internal error. ResponseMetadata is set to NULL");
        std::cout << msg << std::endl;
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, msg);
    }
    auto iterAuthMetadata = authMetadata.find(JWT_TOKEN_AUTH_METADATA_KEY);
    if (iterAuthMetadata == authMetadata.end()) {
        msg = "JWT authorization process metadata error. Authorization token not found.";
        std::cout << msg << std::endl;
        return ::grpc::Status(::grpc::StatusCode::INTERNAL, msg);
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
        std::cout << "Validate went ok, returning status OK" << std::endl;
        return ::grpc::Status::OK;
    }
    // else UNAUTHENTICATED
    std::cout << "JWT authorization process error. Invalid principal." << std::endl;
    return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "JWT authorization process error. Invalid principal.");

}