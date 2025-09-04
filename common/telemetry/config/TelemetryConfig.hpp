#pragma once

#include <string>
#include <chrono>
#include <map>
#include <iostream>

namespace cta::telemetry {

enum class MetricsBackend { NOOP, STDOUT, OTLP };

MetricsBackend stringToMetricsBackend(const std::string& string);
std::string metricsBackendToString(MetricsBackend backend);

/**
 * Holds all telemetry-related configuration
 */
typedef struct TelemetryConfig {
  std::string serviceName;
  std::string serviceNamespace;
  std::string serviceVersion;
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

  /**
   * Version of the service. Used to construct service.version
   *
   * See https://opentelemetry.io/docs/specs/semconv/registry/attributes/service/
   */
  TelemetryConfigBuilder& serviceVersion(std::string serviceVersion);

  /**
   * Can be used to add additional resource-level attributes. These can be used to help identify
   * a given resource (typically a resource is a running process).
   * Multiple resource attributes can be added, provided they use distinct keys.
   *
   * See https://opentelemetry.io/docs/specs/semconv/resource/
   */
  TelemetryConfigBuilder& resourceAttribute(std::string key, std::string value);

  /**
   * Which metrics backend to use. Available options are:
   * - "NOOP": a backend that does nothing.
   * - "STDOUT": a backend that writes the telemetry to stdout. Can be used for debugging.
   * - "OTLP": a backend that pushes metrics to an OpenTelemetry Collector.
   */
  TelemetryConfigBuilder& metricsBackend(const std::string& backendType);

  /**
   * Metrics are exported periodically.
   * The export interval denotes how often an export happens to the given backend.
   */
  TelemetryConfigBuilder& metricsExportInterval(std::chrono::milliseconds interval);

  /**
   * The export timeout denotes how long to wait before timing out on an export.
   * Must be smaller than the export interval.
   */
  TelemetryConfigBuilder& metricsExportTimeout(std::chrono::milliseconds timeout);

  /**
   * If the metrics backend is "OTLP", this endpoint tells the telemetry SDK where to push the metrics to.
   */
  TelemetryConfigBuilder& metricsOtlpEndpoint(std::string endpoint);

  /**
   * Additional headers that will be included when pushing to the "OTLP" backend.
   */
  TelemetryConfigBuilder& metricsHeader(std::string key, std::string value);

  /**
   * Constructs a telemetry config from the builder.
   */
  TelemetryConfig build() const;

private:
  TelemetryConfig m_config;
};

}  // namespace cta::telemetry
