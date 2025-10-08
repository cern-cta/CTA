/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include "TelemetryConfig.hpp"

#include <stdexcept>
#include <fstream>
#include <string>

#include "common/exception/InvalidArgument.hpp"
#include "common/utils/Base64.hpp"

namespace cta::telemetry {

MetricsBackend stringToMetricsBackend(const std::string& name) {
  if (name == "NOOP") {
    return MetricsBackend::NOOP;
  }
  if (name == "STDOUT") {
    return MetricsBackend::STDOUT;
  }
  if (name == "FILE") {
    return MetricsBackend::FILE;
  }
  if (name == "OTLP_GRPC") {
    return MetricsBackend::OTLP_GRPC;
  }
  if (name == "OTLP_HTTP") {
    return MetricsBackend::OTLP_HTTP;
  }
  throw cta::exception::InvalidArgument("Invalid MetricBackend: " + name);
}

std::string metricsBackendToString(MetricsBackend backend) {
  switch (backend) {
    using enum MetricsBackend;
    case NOOP:
      return "NOOP";
    case STDOUT:
      return "STDOUT";
    case FILE:
      return "FILE";
    case OTLP_GRPC:
      return "OTLP_GRPC";
    case OTLP_HTTP:
      return "OTLP_HTTP";
  }
  throw cta::exception::InvalidArgument("Provided MetricsBackend cannot be converted to string");
}

std::string authStringFromFile(const std::string& filePath) {
  std::ifstream file(filePath);
  if (!file.is_open()) {
    throw cta::exception::Exception("Failed to open auth file: " + filePath);
  }

  std::string line;
  while (std::getline(file, line)) {
    // Skip comment lines
    if (!line.empty() && line[0] != '#') {
      return line;
    }
  }
  throw cta::exception::Exception("No valid auth string found in file: " + filePath);
}

TelemetryConfigBuilder& TelemetryConfigBuilder::serviceName(std::string serviceName) {
  m_config.serviceName = std::move(serviceName);
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::serviceNamespace(std::string serviceNamespace) {
  m_config.serviceNamespace = std::move(serviceNamespace);
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::serviceVersion(std::string serviceVersion) {
  m_config.serviceVersion = std::move(serviceVersion);
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::retainInstanceIdOnRestart(bool retainInstanceIdOnRestart) {
  m_config.retainInstanceIdOnRestart = retainInstanceIdOnRestart;
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::resourceAttribute(std::string key, std::string value) {
  m_config.resourceAttributes[std::move(key)] = std::move(value);
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::metricsBackend(const std::string& backendType) {
  m_config.metrics.backend = stringToMetricsBackend(backendType);
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::metricsExportInterval(std::chrono::milliseconds interval) {
  m_config.metrics.exportInterval = interval;
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::metricsExportTimeout(std::chrono::milliseconds timeout) {
  m_config.metrics.exportTimeout = timeout;
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::metricsOtlpEndpoint(std::string endpoint) {
  m_config.metrics.otlpEndpoint = std::move(endpoint);
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::metricsOtlpBasicAuthString(const std::string& authString) {
  if (authString.empty()) {
    // Ensure we don't add any headers if not configured
    return *this;
  }
  std::string authStringBase64 = cta::utils::base64encode(authString);
  m_config.metrics.otlpHeaders["Authorization"] = "Basic " + authStringBase64;
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::metricsFileEndpoint(std::string endpoint) {
  m_config.metrics.fileEndpoint = std::move(endpoint);
  return *this;
}

TelemetryConfig TelemetryConfigBuilder::build() const {
  if (m_config.serviceName.empty()) {
    throw cta::exception::InvalidArgument("TelemetryConfig: serviceName is required.");
  }

  if (m_config.serviceNamespace.empty()) {
    throw cta::exception::InvalidArgument("TelemetryConfig: serviceNamespace is required.");
  }

  if (m_config.serviceVersion.empty()) {
    throw cta::exception::InvalidArgument("TelemetryConfig: serviceVersion is required.");
  }
  if (m_config.metrics.backend == MetricsBackend::OTLP_HTTP && m_config.metrics.otlpEndpoint.empty()) {
    throw cta::exception::InvalidArgument(
      "TelemetryConfig: OTLP_HTTP metrics backend requires otlpEndpoint to be configured.");
  }

  if (m_config.metrics.backend == MetricsBackend::OTLP_GRPC && m_config.metrics.otlpEndpoint.empty()) {
    throw cta::exception::InvalidArgument(
      "TelemetryConfig: OTLP_HTTP metrics backend requires otlpEndpoint to be configured.");
  }

  return m_config;
}

}  // namespace cta::telemetry
