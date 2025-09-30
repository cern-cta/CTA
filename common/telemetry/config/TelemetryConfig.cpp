#include "TelemetryConfig.hpp"

#include <stdexcept>
#include <fstream>
#include <string>

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
  throw std::invalid_argument("Invalid MetricBackend: " + name);
}

std::string metricsBackendToString(MetricsBackend backend) {
  switch (backend) {
    case MetricsBackend::NOOP:
      return "NOOP";
    case MetricsBackend::STDOUT:
      return "STDOUT";
    case MetricsBackend::FILE:
      return "FILE";
    case MetricsBackend::OTLP_GRPC:
      return "OTLP_GRPC";
    case MetricsBackend::OTLP_HTTP:
      return "OTLP_HTTP";
  }
  throw std::invalid_argument("Provided MetricsBackend cannot be converted to string");
}

std::string authStringFromFile(const std::string& filePath) {
  std::ifstream file(filePath);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open auth file: " + filePath);
  }

  std::string line;
  while (std::getline(file, line)) {
    // Skip comment lines
    if (!line.empty() && line[0] != '#') {
      return line;
    }
  }
  throw std::runtime_error("No valid auth string found in file: " + filePath);
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

TelemetryConfigBuilder& TelemetryConfigBuilder::metricsOtlpHttpEndpoint(std::string endpoint) {
  m_config.metrics.otlpHttpEndpoint = std::move(endpoint);
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::metricsOtlpHttpBasicAuthString(const std::string& authString) {
  if (authString.empty()) {
    // Ensure we don't add any headers if not configured
    return *this;
  }
  std::string authStringBase64;
  cta::utils::base64encode(authString, authStringBase64);
  m_config.metrics.otlpHttpHeaders["Authorization"] = "Basic " + authStringBase64;
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::metricsFileEndpoint(std::string endpoint) {
  m_config.metrics.fileEndpoint = std::move(endpoint);
  return *this;
}

TelemetryConfig TelemetryConfigBuilder::build() const {
  if (m_config.serviceName.empty()) {
    throw std::invalid_argument("TelemetryConfig: serviceName is required.");
  }

  if (m_config.serviceNamespace.empty()) {
    throw std::invalid_argument("TelemetryConfig: serviceNamespace is required.");
  }

  if (m_config.serviceVersion.empty()) {
    throw std::invalid_argument("TelemetryConfig: serviceVersion is required.");
  }
  if (m_config.metrics.backend == MetricsBackend::OTLP_HTTP && m_config.metrics.otlpHttpEndpoint.empty()) {
    throw std::invalid_argument("TelemetryConfig: OTLP_HTTP metrics backend requires an otlpHttpEndpoint.");
  }

  // See https://github.com/open-telemetry/opentelemetry-cpp/blob/main/exporters/otlp/src/otlp_environment.cc#L133
  constexpr char kGrpcEndpointSignalEnv[] = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
  constexpr char kGrpcEndpointGenericEnv[] = "OTEL_EXPORTER_OTLP_ENDPOINT";
  if (m_config.metrics.backend == MetricsBackend::OTLP_GRPC && !std::getenv(kGrpcEndpointSignalEnv) &&
      !std::getenv(kGrpcEndpointGenericEnv)) {
    throw std::invalid_argument("TelemetryConfig: OTLP_GRPC metrics backend requires the env variable "
                                "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT.");
  }

  return m_config;
}

}  // namespace cta::telemetry
