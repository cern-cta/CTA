/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "GrpcConfig.hpp"

#include "common/exception/UserError.hpp"

#include <set>

namespace cta::frontend::grpc::common {

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

cta::runtime::ValidationResult AuthConfig::validate(OperationMode operationMode) const {
  bool jwtEnabled = jwt && jwt->enabled;
  bool mtlsEnabled = mtls && mtls->enabled;
  bool kerberosEnabled = kerberos && kerberos->enabled;

  // These are not constraints on an individual field but on the auth block as a whole: without a usable combination of
  // authentication methods there is nothing left worth reporting, so fail fast instead of collecting field errors.
  if (operationMode == OperationMode::WFE && (jwtEnabled == mtlsEnabled)) {
    throw exception::UserError("WFE frontend authentication requires exactly one of JWT or mTLS to be enabled");
  }
  if (operationMode != OperationMode::WFE && !jwtEnabled && !kerberosEnabled) {
    throw exception::UserError("Admin frontend authentication requires at least one of JWT or Kerberos to be enabled");
  }

  cta::runtime::ValidationResult result;
  if (jwt) {
    result.merge("jwt", jwt->validate());
  }
  if (mtls) {
    result.merge("mtls", mtls->validate(operationMode));
  }
  if (kerberos) {
    result.merge("kerberos", kerberos->validate(operationMode));
  }
  return result;
}

std::set<AuthMethod, std::less<>> AuthConfig::getEnabledMethods() const {
  using enum AuthMethod;
  std::set<AuthMethod, std::less<>> result {{}};

  if (jwt && jwt->enabled) {
    result.emplace(JWT);
  }
  if (mtls && mtls->enabled) {
    result.emplace(MTLS);
  }
  if (kerberos && kerberos->enabled) {
    result.emplace(KERBEROS);
  }
  return result;
}

cta::runtime::ValidationResult MtlsAuthConfig::validate(OperationMode operationMode) const {
  cta::runtime::ValidationResult result;
  if (!enabled) {
    return result;
  }
  if (operationMode != OperationMode::WFE) {
    result.addError("enabled", "cannot be set outside of WFE mode; mTLS authentication is only usable in WFE mode");
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

cta::runtime::ValidationResult KerberosAuthConfig::validate(OperationMode operationMode) const {
  cta::runtime::ValidationResult result;
  if (!enabled) {
    return result;
  }

  if (operationMode == OperationMode::WFE) {
    result.addError("enabled", "cannot be set in WFE mode; Kerberos authentication is not usable in WFE mode");
  }
  if (keytab_path.empty()) {
    result.addError("keytab_path", "cannot be empty when Kerberos authentication is enabled");
  }
  if (service_principal.empty()) {
    result.addError("service_principal", "cannot be empty when Kerberos authentication is enabled");
  }
  return result;
}

cta::runtime::ValidationResult GrpcTlsConfig::validate() const {
  cta::runtime::ValidationResult result;
  if (server_key_path.empty()) {
    result.addError("server_key_path", "cannot be empty");
  }
  if (server_cert_path.empty()) {
    result.addError("server_cert_path", "cannot be empty");
  }
  if (chain_cert_path.has_value() && chain_cert_path->empty()) {
    result.addError("chain_cert_path", "cannot be empty when provided");
  }
  return result;
}

cta::runtime::ValidationResult GeneralGrpcConfig::validate() const {
  cta::runtime::ValidationResult result;
  if (port == 0) {
    result.addError("port", "must be greater than zero");
  }
  // check that our number of threads is OK
  if (number_of_threads.has_value() && *number_of_threads < 1) {
    result.addError("number_of_threads", "must be at least 1");
  }
  result.merge("tls", tls.validate());
  return result;
}

cta::runtime::ValidationResult GrpcConfig::validate(OperationMode operationMode) const {
  cta::runtime::ValidationResult result;
  result.merge("grpc", grpc.validate());
  result.merge("auth", auth.validate(operationMode));
  return result;
}

}  // namespace cta::frontend::grpc::common
