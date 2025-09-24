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

#include "KeycloakClient.hpp"
#include <sstream>
#include <stdexcept>
#include "common/log/Logger.hpp"

namespace cta::frontend::grpc::client {

KeycloakClient::KeycloakClient(const std::string& keycloakUrl, const std::string& realm, cta::log::LogContext& lc)
    : m_keycloakUrl(keycloakUrl), m_realm(realm), m_lc(lc) {

    // Construct token endpoint URL
    m_tokenEndpoint = m_keycloakUrl + "/realms/" + m_realm + "/protocol/openid-connect/token";

    // Initialize curl
    m_curl = curl_easy_init();
    if (!m_curl) {
        throw std::runtime_error("Failed to initialize curl");
    }

    cta::log::ScopedParamContainer params(m_lc);
    params.add("keycloakUrl", m_keycloakUrl);
    params.add("realm", m_realm);
    params.add("tokenEndpoint", m_tokenEndpoint);
    m_lc.log(cta::log::DEBUG, "KeycloakClient initialized");
}

KeycloakClient::~KeycloakClient() {
    if (m_curl) {
        curl_easy_cleanup(m_curl);
    }
}

size_t KeycloakClient::WriteCallback(void* contents, size_t size, size_t nmemb, std::string* response) {
    size_t totalSize = size * nmemb;
    response->append(static_cast<char*>(contents), totalSize);
    return totalSize;
}

std::string KeycloakClient::extractAccessToken(const std::string& jsonResponse) {
    // Simple JSON parsing to extract access_token
    // In production, consider using a proper JSON library like nlohmann/json
    size_t tokenStart = jsonResponse.find("\"access_token\":\"");
    if (tokenStart == std::string::npos) {
        throw std::runtime_error("No access_token found in Keycloak response");
    }
    tokenStart += 16; // Length of "access_token":""

    size_t tokenEnd = jsonResponse.find("\"", tokenStart);
    if (tokenEnd == std::string::npos) {
        throw std::runtime_error("Malformed access_token in Keycloak response");
    }

    return jsonResponse.substr(tokenStart, tokenEnd - tokenStart);
}

std::string KeycloakClient::getJWTToken() {
    std::string response;

    cta::log::ScopedParamContainer params(m_lc);
    params.add("tokenEndpoint", m_tokenEndpoint);
    m_lc.log(cta::log::DEBUG, "Requesting JWT token from Keycloak");

    // Set up curl for SPNEGO/Kerberos authentication
    curl_easy_setopt(m_curl, CURLOPT_URL, m_tokenEndpoint.c_str());
    curl_easy_setopt(m_curl, CURLOPT_HTTPAUTH, CURLAUTH_NEGOTIATE);
    curl_easy_setopt(m_curl, CURLOPT_USERPWD, ":");  // Empty user:pass for Kerberos
    curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, &response);

    // Set POST data for client credentials grant
    std::string postData = "grant_type=client_credentials";
    curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, postData.c_str());

    // Set headers
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/x-www-form-urlencoded");
    curl_easy_setopt(m_curl, CURLOPT_HTTPHEADER, headers);

    // Enable verbose output for debugging (remove in production)
    // curl_easy_setopt(m_curl, CURLOPT_VERBOSE, 1L);

    // Perform the request
    CURLcode res = curl_easy_perform(m_curl);
    curl_slist_free_all(headers);

    if (res != CURLE_OK) {
        cta::log::ScopedParamContainer errorParams(m_lc);
        errorParams.add("curlError", curl_easy_strerror(res));
        m_lc.log(cta::log::ERR, "Keycloak token request failed");
        throw std::runtime_error("Failed to get JWT from Keycloak: " + std::string(curl_easy_strerror(res)));
    }

    long response_code;
    curl_easy_getinfo(m_curl, CURLINFO_RESPONSE_CODE, &response_code);

    cta::log::ScopedParamContainer responseParams(m_lc);
    responseParams.add("httpCode", response_code);
    responseParams.add("responseLength", response.length());

    if (response_code != 200) {
        m_lc.log(cta::log::ERR, "Keycloak returned error code");
        throw std::runtime_error("Keycloak authentication failed with code: " + std::to_string(response_code) +
                                " Response: " + response);
    }

    m_lc.log(cta::log::DEBUG, "Successfully received response from Keycloak");

    // Extract JWT token from response
    std::string jwtToken = extractAccessToken(response);

    m_lc.log(cta::log::INFO, "Successfully obtained JWT token from Keycloak");
    return jwtToken;
}

} // namespace cta::frontend::grpc::client