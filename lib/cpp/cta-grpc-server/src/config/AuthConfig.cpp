/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "grpc-server/config/AuthConfig.hpp"

#include "common/exception/UserError.hpp"

#include <set>

namespace cta::grpc_server {

cta::runtime::ValidationResult JwtAuthConfig::validate() const {
  cta::runtime::ValidationResult result;

  // check that the timeouts are not zero
  if (cache_refresh_interval == 0) {
    result.addError("cache_refresh_interval", "must be greater than zero");
  }
  if (jwks_total_timeout == 0) {
    result.addError("jwks_total_timeout", "must be greater than zero");
  }

  // a zero pub_key_timeout means "never expire"
  if (pub_key_timeout != 0 && pub_key_timeout < cache_refresh_interval) {
    result.addError("pub_key_timeout", "must be zero or greater than or equal to 'cache_refresh_interval'");
  }

  if (expected_issuer.empty()) {
    result.addError("expected_issuer", "cannot be empty");
  }
  if (expected_audience.empty()) {
    result.addError("expected_audience", "cannot be empty");
  }
  if (jwks_uri.empty()) {
    result.addError("jwks_uri", "cannot be empty");
  }
  if (revoke_list_path.has_value() && revoke_list_path->empty()) {
    result.addError("revoke_list_path", "cannot be empty when provided");
  }
  return result;
}

cta::runtime::ValidationResult AdminAuthConfig::validate() const {
  auto result = cta::runtime::validateFeatureSelection(*this, enabled_methods, methods, "enabled_methods");
  if (!isJwtEnabled() && !isKerberosEnabled()) {
    result.addError("enabled_methods",
                    "Admin frontend authentication requires at least one of JWT or Kerberos to be enabled");
  }
  return result;
}

std::set<AuthMethod, std::less<>> AdminAuthConfig::getEnabledMethods() const {
  using enum AuthMethod;
  std::set<AuthMethod, std::less<>> result;
  if (isJwtEnabled()) {
    result.emplace(JWT);
  }
  if (isKerberosEnabled()) {
    result.emplace(KERBEROS);
  }
  return result;
}

cta::runtime::ValidationResult WfeAuthConfig::validate() const {
  auto result = cta::runtime::validateFeatureSelection(*this, enabled_methods, methods, "enabled_methods");
  if (isJwtEnabled() == isMtlsEnabled()) {
    result.addError("enabled_methods", "WFE frontend authentication requires exactly one of JWT or mTLS to be enabled");
  }
  return result;
}

std::set<AuthMethod, std::less<>> WfeAuthConfig::getEnabledMethods() const {
  using enum AuthMethod;
  std::set<AuthMethod, std::less<>> result;
  if (isJwtEnabled()) {
    result.emplace(JWT);
  }
  if (isMtlsEnabled()) {
    result.emplace(MTLS);
  }
  return result;
}

cta::runtime::ValidationResult MtlsAuthConfig::validate() const {
  cta::runtime::ValidationResult result;
  for (const auto& [identity, hostnames] : aliases) {
    if (identity.empty()) {
      result.addError("aliases", "cannot contain an empty identity");
    } else if (hostnames.empty()) {
      result.addError("aliases", "must provide at least one alias for identity '" + identity + "'");
    }
  }
  return result;
}

cta::runtime::ValidationResult KerberosAuthConfig::validate() const {
  cta::runtime::ValidationResult result;

  if (keytab_path.empty()) {
    result.addError("keytab_path", "cannot be empty when Kerberos authentication is enabled");
  }
  if (service_principal.empty()) {
    result.addError("service_principal", "cannot be empty when Kerberos authentication is enabled");
  }
  return result;
}

}  // namespace cta::grpc_server
