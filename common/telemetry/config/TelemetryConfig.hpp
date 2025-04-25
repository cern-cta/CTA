#pragma once

#include <string>
#include <chrono>
#include <map>
#include <iostream>

namespace cta::telemetry {

enum class MetricsBackend { NOOP, STDOUT, OTLP };

typedef struct TelemetryConfig {
  std::string serviceName;
  std::map<std::string, std::string> resourceAttributes;

  struct Metrics {
    MetricsBackend backend;
    std::chrono::milliseconds exportInterval;
    std::chrono::milliseconds exportTimeout;
    std::string otlpEndpoint;
    std::map<std::string, std::string> headers;
  } metrics;
} TelemetryConfig;

inline void printTelemetryConfig(const TelemetryConfig& config, std::ostream& os = std::cout) {
  os << "TelemetryConfig:\n";
  os << "  serviceName: " << config.serviceName << "\n";

  os << "  resourceAttributes:\n";
  for (const auto& [key, value] : config.resourceAttributes) {
    os << "    " << key << ": " << value << "\n";
  }

  os << "  metrics:\n";
  os << "    backend: ";
  switch (config.metrics.backend) {
    case MetricsBackend::NOOP:   os << "NOOP"; break;
    case MetricsBackend::STDOUT: os << "STDOUT"; break;
    case MetricsBackend::OTLP:   os << "OTLP"; break;
  }
  os << "\n";

  os << "    exportInterval: " << config.metrics.exportInterval.count() << "ms\n";
  os << "    exportTimeout: " << config.metrics.exportTimeout.count() << "ms\n";
  os << "    otlpEndpoint: " << config.metrics.otlpEndpoint << "\n";

  os << "    headers:\n";
  for (const auto& [key, value] : config.metrics.headers) {
    os << "      " << key << ": " << value << "\n";
  }
}

class TelemetryConfigBuilder {
public:
  TelemetryConfigBuilder& serviceName(std::string name);
  TelemetryConfigBuilder& resourceAttribute(std::string key, std::string value);
  TelemetryConfigBuilder& metricsBackend(const std::string& backendType);
  TelemetryConfigBuilder& metricsExportInterval(std::chrono::milliseconds interval);
  TelemetryConfigBuilder& metricsExportTimeout(std::chrono::milliseconds timeout);
  TelemetryConfigBuilder& metricsOtlpEndpoint(std::string endpoint);
  TelemetryConfigBuilder& metricsHeader(std::string key, std::string value);

  TelemetryConfig build() const;

private:
  TelemetryConfig m_config;
};

}  // namespace cta::telemetry
