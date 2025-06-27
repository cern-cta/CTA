#pragma once

#include <string>
#include <chrono>
#include <map>
#include <iostream>

namespace cta::telemetry {

enum class MetricsBackend { NOOP, STDOUT, OTLP };

typedef struct TelemetryConfig {
  std::string serviceName;
  std::string serviceNamespace;
  std::string instanceHint;
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
  TelemetryConfigBuilder& serviceName(std::string serviceName);
  TelemetryConfigBuilder& serviceNamespace(std::string serviceNamespace);
  // The instance hint is an optional field that can be used to generate a deterministic service.instance.id that persists across restarts
  // For this to work, the instanceHint must allow the triplet (hostname, processName, instanceHint) to be globally unique
  TelemetryConfigBuilder& instanceHint(std::string instanceHint);
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
