#pragma once

#include <string>
#include <chrono>
#include <map>

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

class TelemetryConfigBuilder {
public:
  TelemetryConfigBuilder& serviceName(std::string name);
  TelemetryConfigBuilder& resourceAttribute(std::string key, std::string value);
  TelemetryConfigBuilder& metricsBackend(std::string backendType);
  TelemetryConfigBuilder& metricsExportInterval(std::chrono::milliseconds interval);
  TelemetryConfigBuilder& metricsExportTimeout(std::chrono::milliseconds timeout);
  TelemetryConfigBuilder& metricsOtlpEndpoint(std::string endpoint);
  TelemetryConfigBuilder& metricsHeader(std::string key, std::string value);

  TelemetryConfig build() const;

private:
  TelemetryConfig m_config;
};

}  // namespace cta::telemetry
