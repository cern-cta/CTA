#include "TelemetryConfig.hpp"

#include <stdexcept>

namespace cta::telemetry {

inline MetricsBackend stringToMetricsBackend(const std::string& name) {
  if (name == "NOOP") {
    return MetricsBackend::NOOP;
  }
  if (name == "STDOUT") {
    return MetricsBackend::STDOUT;
  }
  if (name == "OTLP") {
    return MetricsBackend::OTLP;
  }
  throw std::invalid_argument("Invalid MetricBackend: " + name);
}

TelemetryConfigBuilder& TelemetryConfigBuilder::serviceName(std::string name) {
  m_config.serviceName = std::move(name);
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::resourceAttribute(std::string key, std::string value) {
  m_config.resourceAttributes[std::move(key)] = std::move(value);
  return *this;
}

TelemetryConfigBuilder& TelemetryConfigBuilder::metricsBackend(std::string backendType) {
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

TelemetryConfigBuilder& TelemetryConfigBuilder::metricsHeader(std::string key, std::string value) {
  m_config.metrics.headers[std::move(key)] = std::move(value);
  return *this;
}

TelemetryConfig TelemetryConfigBuilder::build() const {
  if (m_config.serviceName.empty()) {
    throw std::invalid_argument("TelemetryConfig: serviceName is required.");
  }

  if (m_config.metrics.backend == MetricsBackend::OTLP && m_config.metrics.otlpEndpoint.empty()) {
    throw std::invalid_argument("TelemetryConfig: OTLP metrics backend requires otlpEndpoint.");
  }

  return m_config;
}

}  // namespace cta::telemetry
