/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */
#pragma once

#include <string>
#include <chrono>
#include <map>
#include <iostream>

namespace cta::telemetry {

enum class MetricsBackend { NOOP, STDOUT, FILE, OTLP_GRPC, OTLP_HTTP };

MetricsBackend stringToMetricsBackend(const std::string& string);
std::string metricsBackendToString(MetricsBackend backend);

/**
 * Reads the authentication string from a given file.
 * The auth string is the first line not starting with a # (to allow for comments).
 */
std::string authStringFromFile(const std::string& filePath);

/**
 * Holds all telemetry-related configuration
 */
struct TelemetryConfig {
  std::string serviceName;
  std::string serviceNamespace;
  std::string serviceVersion;
  std::map<std::string, std::string, std::less<>> resourceAttributes;

  bool retainInstanceIdOnRestart = false;

  struct Metrics {
    MetricsBackend backend = MetricsBackend::NOOP;
    std::chrono::milliseconds exportInterval;
    std::chrono::milliseconds exportTimeout;
    std::string fileEndpoint;
    std::string otlpEndpoint;
    std::map<std::string, std::string, std::less<>> otlpHeaders;
  };

  Metrics metrics;
};

class TelemetryConfigBuilder {
public:
  /**
   * Used to construct service.name
   *
   * The triplet (service.namespace, service.name, service.instance.id) must be globally unique.
   *
   * See https://opentelemetry.io/docs/specs/semconv/registry/attributes/service/
   *
   * @param serviceName Name of the service.
   */
  TelemetryConfigBuilder& serviceName(std::string serviceName);

  /**
   * Used to construct service.namespace
   *
   * The triplet (service.namespace, service.name, service.instance.id) must be globally unique.
   *
   * See https://opentelemetry.io/docs/specs/semconv/registry/attributes/service/
   *
   * @param serviceNamespace Namespace the service is running in.
   */
  TelemetryConfigBuilder& serviceNamespace(std::string serviceNamespace);

  /**
   * Version of the service. Used to construct service.version
   *
   * See https://opentelemetry.io/docs/specs/semconv/registry/attributes/service/
   *
   * @param serviceVersion Version of the service.
   */
  TelemetryConfigBuilder& serviceVersion(std::string serviceVersion);

  /**
   * If set to false, each restart of a process will generate a new unique ID for the `service.instance.id`.
   * If set to true, the `service.instance.id` is constructed by concatenating the hostname and the process name.
   * As such, across restarts, `service.instance.id` remains constant in this case.
   *
   * @param retainInstanceIdOnRestart Set to true if the process should not generate a new unique `service.instance.id` on every restart.
   */
  TelemetryConfigBuilder& retainInstanceIdOnRestart(bool retainInstanceIdOnRestart);

  /**
   * Can be used to add additional resource-level attributes. These can be used to help identify
   * a given resource (typically a resource is a running process).
   * Multiple resource attributes can be added, provided they use distinct keys.
   *
   * See https://opentelemetry.io/docs/specs/semconv/resource/
   *
   * @param key Attribute key.
   * @param key Attribute value.
   */
  TelemetryConfigBuilder& resourceAttribute(std::string key, std::string value);

  /**
   * Which metrics backend to use. Available options are:
   * - "NOOP": a backend that does nothing.
   * - "STDOUT": a backend that writes the telemetry to stdout. Can be used for debugging.
   * - "OTLP_HTTP": a backend that pushes metrics to an OpenTelemetry Collector over HTTP.
   * - "OTLP_GRPC": a backend that pushes metrics to an OpenTelemetry Collector over GRPC.
   * @param backendType Backend type.
   */
  TelemetryConfigBuilder& metricsBackend(const std::string& backendType);

  /**
   * Metrics are exported periodically.
   * The export interval denotes how often an export happens to the given backend.
   * @param interval Interval in milliseconds that determines how often to push metrics to the backend.
   */
  TelemetryConfigBuilder& metricsExportInterval(std::chrono::milliseconds interval);

  /**
   * The export timeout denotes how long to wait before timing out on an export.
   * Must be smaller than the export interval.
   * @param timeout Time in milliseconds to wait before an export times out.
   */
  TelemetryConfigBuilder& metricsExportTimeout(std::chrono::milliseconds timeout);

  /**
   * If the metrics backend is "OTLP_HTTP" or "OTLP_GRPC", this endpoint tells the telemetry SDK where to push the metrics to.
   * @param endpoint HTTP endpoint to push the metrics to.
   */
  TelemetryConfigBuilder& metricsOtlpEndpoint(std::string endpoint);

  /**
   * If the metrics backend is "OTLP_HTTP" or "OTLP_GRPC", setting this option will add the header "authorization: Basic <base64(username:password)>"
   * @param authString The raw string "username:password" (not base64 encoded).
   */
  TelemetryConfigBuilder& metricsOtlpBasicAuthString(const std::string& authString);

  /**
   * If the metrics backend is "FILE", this endpoint tells the telemetry SDK which file to write the metrics to.
   * @param endpoint File location to write metrics to.
   */
  TelemetryConfigBuilder& metricsFileEndpoint(std::string endpoint);

  /**
   * Constructs a telemetry config from the builder.
   */
  TelemetryConfig build() const;

private:
  TelemetryConfig m_config;
};

}  // namespace cta::telemetry
