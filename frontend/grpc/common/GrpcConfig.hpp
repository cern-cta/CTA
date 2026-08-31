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

using AliasMap = std::map<std::string, std::vector<std::string>, std::less<>>;

/**
 * @brief TLS Config
 */
struct GrpcTlsConfig final {
  std::string server_key_path;                                //!< The path to the private key file (REQUIRED)
  std::string server_cert_path;                               //!< The path to the certificate file (REQUIRED)
  std::optional<std::string> chain_cert_path = std::nullopt;  //!< The path to the CA chain file (optional)

  /**
   * @brief Validate the TLS configuration parameters
   */
  void validate() const;

  static constexpr std::size_t memberCount() { return 3; }
};

/**
 * @brief General gRPC config parameters
 */
struct GeneralGrpcConfig final {
  uint16_t port = 50051;                 //!< Port to listen on for gRPC connections
  std::optional<int> number_of_threads;  //!< Maximum number of threads for the gRPC server
  GrpcTlsConfig tls;                     //!< TLS config (mandatory)

  static constexpr std::size_t memberCount() { return 3; }
};

/**
 * @brief A revoked token
 */
struct RevokedTokenEntry final {
  std::string jti;             //!< The token's JTI (UUID)
  std::string reason;          //!< The token's revocation reason (only for audit purposes)
  toml::date_time revoked_at;  //!< The token's revocation date/time in UTC (only for audit purposes)

  static constexpr std::size_t memberCount() { return 3; }
};

/**
 * @brief A JWT auth config block
 */
struct JwtAuthConfig final {
  bool enabled = false;                   //!< Whether JWT auth is enabled (default: disabled)
  std::string jwks_uri;                   //!< The URI of the JWKS endpoint (REQUIRED if enabled)
  uint32_t cache_refresh_interval = 600;  //!< TTL (s) of the JWKS cache, after which the JWKS will be re-fetched
  uint32_t pub_key_timeout = 0;           //!< TTL (s) of a cached public key (0 = never expires)
  uint32_t jwks_total_timeout = 60;       //!< Timeout for JWKS retrieval from a URL (default 60)
  std::string expected_issuer;            //!< The expected issuer of the JWT tokens (REQUIRED if enabled)
  std::string expected_audience;          //!< The expected audience of the JWT tokens (REQUIRED if enabled)

  std::vector<RevokedTokenEntry> revoked_tokens;

  /**
   * @brief Validate the JWT configuration for consistency
   * @param log A logger object
   */
  void validate(log::Logger& log);

  static constexpr std::size_t memberCount() { return 8; }
};

/**
 * @brief An mTLS auth config block
 */
struct MtlsAuthConfig final {
  bool enabled = false;  //!< Whether mTLS auth is enabled (default: disabled)
  AliasMap aliases;      //!< Mapping of identity -> (host)name aliases (SAN)

  /**
   * @brief Validate the mTLS configuration for consistency
   * @param operationMode Operation mode the frontend is working in (WFE/Admin)
   * @param log A logger object
   */
  void validate(OperationMode operationMode, log::Logger& log) const;

  static constexpr std::size_t memberCount() { return 2; }
};

/**
 * @brief A Kerberos auth config block
 */
struct KerberosAuthConfig final {
  bool enabled = false;           //!< Whether Kerberos auth is enabled (default: disabled)
  std::string keytab_path;        //!< Path to keytab file (REQUIRED if enabled)
  std::string service_principal;  //!< Kerberos service principal which will be accepted (REQUIRED if enabled)

  /**
   * @brief Validate the Kerberos configuration for consistency
   * @param operationMode Operation mode the frontend is working in (WFE/Admin)
   */
  void validate(OperationMode operationMode) const;

  static constexpr std::size_t memberCount() { return 3; }
};

struct AuthConfig final {
  std::optional<JwtAuthConfig> jwt;            //!< JWT auth config (optional)
  std::optional<MtlsAuthConfig> mtls;          //!< mTLS auth config (optional)
  std::optional<KerberosAuthConfig> kerberos;  //!< Kerberos auth config (optional)

  /**
   * @brief Validate the authentication configuration for consistency
   * @param operationMode Operation mode the frontend is working in (WFE/Admin)
   * @param log A logger object
   */
  void validate(OperationMode operationMode, log::Logger& log);

  /**
   * @brief Get all enabled authentication methods
   * @return std::set<AuthMethod>
   */
  std::set<AuthMethod, std::less<>> getEnabledMethods() const;

  static constexpr std::size_t memberCount() { return 3; }
};

/**
 * @brief Top-level gRPC config for the CTA frontend
 */
struct GrpcConfig final {
  GeneralGrpcConfig grpc;  //!< General gRPC config parameters
  AuthConfig auth;         //!< Authentication configuration

  /**
   * @brief Validate the gRPC configuration for consistency
   * @param operationMode Operation mode the frontend is working in (WFE/Admin)
   * @param log A logger object
   */
  void validate(OperationMode operationMode, log::Logger& log);

  static constexpr std::size_t memberCount() { return 2; }
};

}  // namespace cta::frontend::grpc::common
