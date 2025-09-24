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

#include <string>
#include <curl/curl.h>
#include "common/log/LogContext.hpp"

namespace cta::frontend::grpc::client {

class KeycloakClient {
public:
    KeycloakClient(const std::string& keycloakUrl, const std::string& realm, cta::log::LogContext& lc);
    ~KeycloakClient();

    // Get JWT token using Kerberos SPNEGO authentication
    std::string getJWTToken();

private:
    std::string m_keycloakUrl;
    std::string m_realm;
    std::string m_tokenEndpoint;
    cta::log::LogContext& m_lc;
    CURL* m_curl;

    static size_t WriteCallback(void* contents, size_t size, size_t nmemb, std::string* response);
    std::string extractAccessToken(const std::string& jsonResponse);
};

} // namespace cta::frontend::grpc::client