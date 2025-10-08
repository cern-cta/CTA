/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <unordered_map>
#include <string>
#include <mutex>

namespace cta::frontend::grpc::server {

class TokenStorage {

public:
  TokenStorage() = default;
  ~TokenStorage() = default;
  /*
   * Store token for the given spn
   */
  void store(const std::string& strToken, const std::string& strSpn);
  /*
   * Token validation & decoding
   */
  bool validate(const std::string& strToken) const;

  // TODO: removing a token

private:
  std::unordered_map<std::string, std::string> m_umapTokens;
  mutable std::mutex m_mtxLockStorage;

};

} // namespace cta::frontend::grpc::server
