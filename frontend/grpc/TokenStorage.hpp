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

#include <unordered_map>
#include <string>
#include <mutex>

namespace cta::frontend::grpc::server {

struct TokenInfo {
  std::string clientPrincipal;  // Authenticated username (e.g., "username@TEST.CTA")
  std::string servicePrincipal; // Service principal name (e.g., "cta/cta-frontend-grpc")
};

class TokenStorage {

public:
  TokenStorage() = default;
  ~TokenStorage() = default;
  /*
   * Store token for the given spn and username
   */
  void store(const std::string& strToken, const std::string& strClientPrincipal, const std::string& strServicePrincipal);
  /*
   * Token validation
   */
  bool validate(const std::string& strToken) const;

  /*
   * Get the client principal (username) for a validated token
   * Needed to check if they are authorized to perform admin commands
   */
  std::string getClientPrincipal(const std::string& strToken) const;

  /*
   * Remove a token from storage (tokens are not needed beyond the duration of a single call)
   */
  void remove(const std::string& strToken);

private:
  std::unordered_map<std::string, TokenInfo> m_umapTokens;
  mutable std::mutex m_mtxLockStorage;

};

} // namespace cta::frontend::grpc::server
