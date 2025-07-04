#pragma once

#include <string>
#include <chrono>
#include <map>
#include <iostream>

namespace cta::telemetry {

enum class MetricsBackend { NOOP, STDOUT, OTLP };

MetricsBackend stringToMetricsBackend(const std::string& string);
std::string metricsBackendToString(MetricsBackend backend);

typedef struct TelemetryConfig {
  std::string serviceName;
  std::string serviceNamespace;
  std::map<std::string, std::string> resourceAttributes;

  struct Metrics {
    MetricsBackend backend = MetricsBackend::NOOP;
    std::chrono::milliseconds exportInterval;
    std::chrono::milliseconds exportTimeout;
    std::string otlpEndpoint;
    std::map<std::string, std::string> headers;
  } metrics;
} TelemetryConfig;

class TelemetryConfigBuilder {
public:
  /**
   * Name of the service. Used to construct service.name
   *
   * The triplet (service.namespace, service.name, service.instance.id) must be globally unique.
   *
   * See https://opentelemetry.io/docs/specs/semconv/registry/attributes/service/
   */
  TelemetryConfigBuilder& serviceName(std::string serviceName);

  /**
   * Namespace the service is running in. Used to construct service.namespace
   *
   * The triplet (service.namespace, service.name, service.instance.id) must be globally unique.
   *
   * See https://opentelemetry.io/docs/specs/semconv/registry/attributes/service/
   */
  TelemetryConfigBuilder& serviceNamespace(std::string serviceNamespace);
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
