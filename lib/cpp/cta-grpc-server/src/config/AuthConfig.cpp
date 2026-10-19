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
  if (!enabled) {
    return result;
  }

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
  const bool jwtEnabled = jwt && jwt->enabled;
  const bool kerberosEnabled = kerberos && kerberos->enabled;

  // This is a constraint on the auth block as a whole: without a usable combination of authentication methods there
  // is nothing left worth reporting, so fail fast instead of collecting field errors.
  if (!jwtEnabled && !kerberosEnabled) {
    throw exception::UserError("Admin frontend authentication requires at least one of JWT or Kerberos to be enabled");
  }

  cta::runtime::ValidationResult result;
  if (jwt) {
    result.merge("jwt", jwt->validate());
  }
  if (kerberos) {
    result.merge("kerberos", kerberos->validate());
  }
  return result;
}

std::set<AuthMethod, std::less<>> AdminAuthConfig::getEnabledMethods() const {
  using enum AuthMethod;
  std::set<AuthMethod, std::less<>> result;
  if (jwt && jwt->enabled) {
    result.emplace(JWT);
  }
  if (kerberos && kerberos->enabled) {
    result.emplace(KERBEROS);
  }
  return result;
}

cta::runtime::ValidationResult WfeAuthConfig::validate() const {
  const bool jwtEnabled = jwt && jwt->enabled;
  const bool mtlsEnabled = mtls && mtls->enabled;

  if (jwtEnabled == mtlsEnabled) {
    throw exception::UserError("WFE frontend authentication requires exactly one of JWT or mTLS to be enabled");
  }

  cta::runtime::ValidationResult result;
  if (jwt) {
    result.merge("jwt", jwt->validate());
  }
  if (mtls) {
    result.merge("mtls", mtls->validate());
  }
  return result;
}

std::set<AuthMethod, std::less<>> WfeAuthConfig::getEnabledMethods() const {
  using enum AuthMethod;
  std::set<AuthMethod, std::less<>> result;
  if (jwt && jwt->enabled) {
    result.emplace(JWT);
  }
  if (mtls && mtls->enabled) {
    result.emplace(MTLS);
  }
  return result;
}

cta::runtime::ValidationResult MtlsAuthConfig::validate() const {
  cta::runtime::ValidationResult result;
  if (!enabled) {
    return result;
  }
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
  if (!enabled) {
    return result;
  }

  if (keytab_path.empty()) {
    result.addError("keytab_path", "cannot be empty when Kerberos authentication is enabled");
  }
  if (service_principal.empty()) {
    result.addError("service_principal", "cannot be empty when Kerberos authentication is enabled");
  }
  return result;
}

}  // namespace cta::grpc_server
