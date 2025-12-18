/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

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

}  // namespace cta::frontend::grpc::server
