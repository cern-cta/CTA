/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "GrpcConfig.hpp"

#include "common/exception/UserError.hpp"

#include <set>

namespace cta::frontend::grpc::common {

void JwtAuthConfig::validate(log::Logger& log) {
  auto checkTimeoutBounds = [&](const std::string& paramName, uint32_t value, bool checkForZero) {
    if (checkForZero && value == 0) {
      throw exception::UserError("value of '" + paramName + "' cannot be zero");
    }
  };

  if (!enabled) {
    return;
  }

  // check that the timeouts are not zero
  checkTimeoutBounds("cache_refresh_interval", cache_refresh_interval, true);
  checkTimeoutBounds("pub_key_timeout", pub_key_timeout, false);
  checkTimeoutBounds("jwks_total_timeout", jwks_total_timeout, true);

  if (pub_key_timeout == 0) {
    log(log::WARNING, "'pub_key_timeout' is set to zero. Cached public keys will not expire");
  } else if (pub_key_timeout < cache_refresh_interval) {
    log(log::WARNING,
        "Cannot use a value for 'pub_key_timeout' that is less than 'cache_refresh_interval'. "
        "Setting 'pub_key_timeout' equal to 'cache_refresh_interval'.");
    pub_key_timeout = cache_refresh_interval;
  }

  if (expected_issuer.empty()) {
    throw exception::UserError("'expected_issuer' cannot be empty");
  }

  if (expected_audience.empty()) {
    throw exception::UserError("'expected_audience' cannot be empty");
  }

  if (jwks_uri.empty()) {
    throw exception::UserError("'jwks_uri' cannot be a empty");
  }
}

void AuthConfig::validate(OperationMode operationMode, log::Logger& log) {
  bool jwtEnabled = jwt && jwt->enabled;
  bool mtlsEnabled = mtls && mtls->enabled;
  bool kerberosEnabled = kerberos && kerberos->enabled;

  if (operationMode == OperationMode::WFE && (jwtEnabled == mtlsEnabled)) {
    throw exception::UserError("WFE frontend authentication requires exactly one of JWT or mTLS to be enabled");
  } else if (operationMode != OperationMode::WFE && !jwtEnabled && !kerberosEnabled) {
    throw exception::UserError("Admin frontend authentication requires at least one of JWT or Kerberos to be enabled");
  }

  if (jwt) {
    jwt.value().validate(log);
  }
  if (mtls) {
    mtls.value().validate(operationMode, log);
  }
  if (kerberos) {
    kerberos.value().validate(operationMode);
  }
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

void MtlsAuthConfig::validate(OperationMode operationMode, log::Logger& log) const {
  if (!enabled) {
    return;
  }
  if (operationMode != OperationMode::WFE) {
    throw exception::UserError("mTLS authentication is only usable in WFE mode");
  }
  if (aliases.empty()) {
    log(log::WARNING, "WFE authentication method is set to mTLS, but no certificate aliases were provided");
  }
}

void KerberosAuthConfig::validate(OperationMode operationMode) const {
  if (!enabled) {
    return;
  }

  if (operationMode == OperationMode::WFE) {
    throw exception::UserError("Kerberos authentication cannot be used in WFE mode");
  }

  if (keytab_path.empty()) {
    throw exception::UserError("'keytab_path' cannot be empty when Kerberos authentication is enabled");
  }

  if (service_principal.empty()) {
    throw exception::UserError("'service_principal' cannot be empty when Kerberos authentication is enabled");
  }
}

void GrpcTlsConfig::validate() const {
  if (server_key_path.empty()) {
    throw exception::UserError("'grpc.tls.server_key_path' cannot be empty");
  }

  if (server_cert_path.empty()) {
    throw exception::UserError("'grpc.tls.server_cert_path' cannot be empty");
  }
}

void GrpcConfig::validate(OperationMode operationMode, log::Logger& log) {
  // check that our number of threads is OK
  if (auto threads = grpc.number_of_threads; threads.has_value() && threads.value() < 1) {
    throw exception::UserError("value of grpc.number_of_threads must be at least 1");
  }

  auth.validate(operationMode, log);
  grpc.tls.validate();
}

}  // namespace cta::frontend::grpc::common
