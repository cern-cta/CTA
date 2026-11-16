/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "grpc-server/config/AuthMethod.hpp"
#include "runtime/config/FeatureConfig.hpp"
#include "runtime/config/ValidationResult.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace cta::grpc_server {

using AliasMap = std::map<std::string, std::vector<std::string>, std::less<>>;

/**
 * @brief A JWT auth config block
 */
struct JwtAuthConfig final {
  std::string jwks_uri;                   //!< The URI of the JWKS endpoint (REQUIRED if enabled)
  uint32_t cache_refresh_interval = 600;  //!< TTL (s) of the JWKS cache, after which the JWKS will be re-fetched
  uint32_t pub_key_timeout = 0;           //!< TTL (s) of a cached public key (0 = never expires)
  uint32_t jwks_total_timeout = 60;       //!< Timeout for JWKS retrieval from a URL (default 60)
  std::string expected_issuer;            //!< The expected issuer of the JWT tokens (REQUIRED if enabled)
  std::string expected_audience;          //!< The expected audience of the JWT tokens (REQUIRED if enabled)
  uint32_t min_generation = 0;            //!< The minimum generation (version) of accepted JWT tokens (default: 0)

  std::optional<std::string> revoke_list_path =
    std::nullopt;  //!< Path to an external TOML file listing revoked tokens (optional)

  /**
   * @brief Validate the JWT configuration for consistency
   * @return The validation result (will contain the errors if present)
   */
  cta::runtime::ValidationResult validate() const;

  static constexpr std::size_t memberCount() { return 8; }
};

/**
 * @brief An mTLS auth config block
 */
struct MtlsAuthConfig final {
  AliasMap aliases;  //!< Mapping of identity -> (host)name aliases (SAN)

  /**
   * @brief Validate the mTLS configuration for consistency
   * @return The validation result (will contain the errors if present)
   */
  cta::runtime::ValidationResult validate() const;

  static constexpr std::size_t memberCount() { return 1; }
};

/**
 * @brief A Kerberos auth config block
 */
struct KerberosAuthConfig final {
  std::string keytab_path;        //!< Path to keytab file (REQUIRED if enabled)
  std::string service_principal;  //!< Kerberos service principal which will be accepted (REQUIRED if enabled)

  /**
   * @brief Validate the Kerberos configuration for consistency
   * @return The validation result (will contain the errors if present)
   */
  cta::runtime::ValidationResult validate() const;

  static constexpr std::size_t memberCount() { return 2; }
};

/// @brief Authentication config of the admin API
struct AdminAuthConfig final {
  /// JWT is the non-strict default; strict mode requires an explicit selection.
  std::vector<std::string> enabled_methods = {"jwt"};
  /// JWT auth config (optional)
  std::optional<JwtAuthConfig> jwt;
  /// Kerberos auth config (optional)
  std::optional<KerberosAuthConfig> kerberos;

  /// @brief Validate the authentication configuration for consistency
  /// @return The validation result (will contain the errors if present)
  cta::runtime::ValidationResult validate() const;

  /// @brief Get all enabled authentication methods
  std::set<AuthMethod, std::less<>> getEnabledMethods() const;

  static constexpr std::size_t memberCount() { return 3; }
};

/// @brief Authentication config of the WFE API
struct WfeAuthConfig final {
  /// JWT is the non-strict default; strict mode requires an explicit selection.
  std::vector<std::string> enabled_methods = {"jwt"};
  /// JWT auth config (optional)
  std::optional<JwtAuthConfig> jwt;
  /// mTLS auth config (optional)
  std::optional<MtlsAuthConfig> mtls;

  /// @brief Validate the authentication configuration for consistency
  /// @return The validation result (will contain the errors if present)
  cta::runtime::ValidationResult validate() const;

  /// @brief Get all enabled authentication methods
  std::set<AuthMethod, std::less<>> getEnabledMethods() const;

  static constexpr std::size_t memberCount() { return 3; }
};

}  // namespace cta::grpc_server
