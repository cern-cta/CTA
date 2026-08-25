/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "frontend/common/AuthMethod.hpp"
#include "frontend/common/OperationModes.hpp"

#include <optional>
#include <set>
#include <string>
#include <toml++/toml.hpp>
#include <unordered_map>

namespace cta::frontend::grpc::common {

struct GrpcTlsConfig final {
  std::string server_key_path;
  std::string server_cert_path;
  std::optional<std::string> chain_cert_path;

  static constexpr std::size_t memberCount() { return 3; }
};

struct GeneralGrpcConfig final {
  uint16_t port = 17017;
  std::optional<int> number_of_threads;
  GrpcTlsConfig tls;

  static constexpr std::size_t memberCount() { return 3; }
};

struct RevokedTokenEntry final {
  std::string jti;
  std::string reason;
  toml::date_time revoked_at;

  static constexpr std::size_t memberCount() { return 3; }
};

struct JwtAuthConfig final {
  bool enabled;
  std::string jwks_uri;
  int cache_refresh_interval = 600;  //!< Time (s) for the public key cache TTL
  int pub_key_timeout = 0;           //!< Time (s) after which to update the cache entry for a cached key (0 = never)
  int jwks_total_timeout = 60;       //!< Time (s) for JWKS endpoint cache (default 60)
  std::string expected_issuer;       //!< The expected issuer of the JWT tokens

  std::vector<RevokedTokenEntry> revoked_tokens;

  void check(log::Logger& log);

  static constexpr std::size_t memberCount() { return 7; }
};

struct MtlsAuthConfig final {
  bool enabled;
  std::map<std::string, std::vector<std::string>, std::less<>> aliases;

  void check(OperationMode operationMode, log::Logger& log) const;

  static constexpr std::size_t memberCount() { return 2; }
};

struct KerberosAuthConfig final {
  bool enabled;
  std::string keytab_path;
  std::string service_principal;

  void check(OperationMode operationMode) const;

  static constexpr std::size_t memberCount() { return 3; }
};

struct AuthConfig final {
  std::optional<JwtAuthConfig> jwt;
  std::optional<MtlsAuthConfig> mtls;
  std::optional<KerberosAuthConfig> kerberos;

  void check(OperationMode operationMode, log::Logger& log);
  std::set<AuthMethod, std::less<>> getEnabledMethods() const;

  static constexpr std::size_t memberCount() { return 3; }
};

struct GrpcConfig {
  GeneralGrpcConfig grpc;
  AuthConfig auth;

  static constexpr std::size_t memberCount() { return 2; }

  void check(OperationMode operationMode, log::Logger& log);
};

}  // namespace cta::frontend::grpc::common
