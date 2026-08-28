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
  std::string server_key_path;                 //!< The path to the private key file
  std::string server_cert_path;                //!< The path to the certificate file
  std::optional<std::string> chain_cert_path;  //!< The path to the CA chain file

  /**
   * @brief Check the TLS configuration parameters for validity
   */
  void check() const;

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
  bool enabled;                      //!< Whether JWT auth is enabled
  std::string jwks_uri;              //!< The URI of the JWKS endpoint/file listing trustworthy public keys
  int cache_refresh_interval = 600;  //!< TTL (s) of the JWKS cache, after which the JWKS will be re-fetched
  int pub_key_timeout = 0;           //!< TTL (s) of a cached public key (0 = never expires)
  int jwks_total_timeout = 60;       //!< Timeout for JWKS retrieval from a URL (default 60)
  std::string expected_issuer;       //!< The expected issuer of the JWT tokens
  std::string expected_audience;     //!< The expected audience of the JWT tokens

  std::vector<RevokedTokenEntry> revoked_tokens;

  /**
   * @brief Check the configuration parameters for inconsistencies
   * @param operationMode Operation mode the frontend is working in (WFE/Admin)
   * @param log A logger object
   */
  void check(log::Logger& log);

  static constexpr std::size_t memberCount() { return 8; }
};

/**
 * @brief An mTLS auth config block
 */
struct MtlsAuthConfig final {
  bool enabled;      //!< Whether mTLS auth is enabled
  AliasMap aliases;  //!< Mapping of identity -> (host)name aliases (SAN)

  /**
   * @brief Check the configuration parameters for inconsistencies
   * @param operationMode Operation mode the frontend is working in (WFE/Admin)
   * @param log A logger object
   */
  void check(OperationMode operationMode, log::Logger& log) const;

  static constexpr std::size_t memberCount() { return 2; }
};

/**
 * @brief A Kerberos auth config block
 */
struct KerberosAuthConfig final {
  bool enabled;                   //!< Whether Kerberos auth is enabled
  std::string keytab_path;        //!< Path to keytab file
  std::string service_principal;  //!< Kerberos service principal which will be accepted

  /**
   * @brief Check the configuration parameters for inconsistencies
   * @param operationMode Operation mode the frontend is working in (WFE/Admin)
   * @param log A logger object
   */
  void check(OperationMode operationMode) const;

  static constexpr std::size_t memberCount() { return 3; }
};

struct AuthConfig final {
  std::optional<JwtAuthConfig> jwt;            //!< JWT auth config (optional)
  std::optional<MtlsAuthConfig> mtls;          //!< mTLS auth config (optional)
  std::optional<KerberosAuthConfig> kerberos;  //!< Kerberos auth config (optional)

  /**
   * @brief Check the configuration parameters for inconsistencies
   * @param operationMode Operation mode the frontend is working in (WFE/Admin)
   * @param log A logger object
   */
  void check(OperationMode operationMode, log::Logger& log);

  /**
   * @brief Get all auth methods enabled in the configuration
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
   * @brief Check the configuration parameters for inconsistencies
   * @param operationMode Operation mode the frontend is working in (WFE/Admin)
   * @param log A logger object
   */
  void check(OperationMode operationMode, log::Logger& log);

  static constexpr std::size_t memberCount() { return 2; }
};

}  // namespace cta::frontend::grpc::common
