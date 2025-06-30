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
  std::string serviceInstanceHint;
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
  /**
   * Used to generate service.instance.id as follows: <hostname.processname.serviceInstanceHint>.
   * When providing this, take care that the serviceInstanceHint is sufficient to distinguish between
   * replicaes of a service running on the same host.
   *
   * The triplet (service.namespace, service.name, service.instance.id) must be globally unique.
   *
   * See https://opentelemetry.io/docs/specs/semconv/registry/attributes/service/
   */
  TelemetryConfigBuilder& serviceInstanceHint(std::string serviceInstanceHint);
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
