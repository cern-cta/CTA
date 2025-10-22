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
#include <shared_mutex>
#include <mutex>

namespace cta::frontend::grpc::server {

class TokenStorage {

public:
  TokenStorage() = default;
  ~TokenStorage() = default;
  /*
   * Store token for the given username
   * @param strToken The last token returned during the negotiation. We use it as a session token.
   * @param strClientPrincipal The authenticated username (e.g., "username@TEST.CTA")
   */
  void store(const std::string& strToken, const std::string& strClientPrincipal);
  /*
   * Token validation
   * @param strEncodedToken The session token encoded in base64 format 
   */
  bool validate(const std::string& strEncodedToken) const;

  /*
   * Get the client principal (username) for a validated token
   * Needed to check if they are authorized to perform admin commands
   * @param strEncodedToken The session token encoded in base64 format
   */
  std::string getClientPrincipal(const std::string& strEncodedToken) const;

  /*
   * Remove a token from storage (tokens are not needed beyond the duration of a single call)
   * @param strEncodedToken The session token encoded in base64 format 
   */
  void remove(const std::string& strEncodedToken);

private:
  std::unordered_map<std::string, std::string> m_umapTokens;
  mutable std::shared_mutex m_mtxLockStorage;

};

} // namespace cta::frontend::grpc::server
