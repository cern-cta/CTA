#pragma once

#include <string>
#include <chrono>
#include <map>

namespace cta::telemetry::metrics {

enum class MetricsBackend { NOOP, STDOUT, OTLP };

typedef struct MetricsConfig {
  MetricsConfig(const std::string& backendType,
    std::string serviceName,
    std::chrono::milliseconds exportInterval,
    std::chrono::milliseconds exportTimeout,
    std::string otlpEndpoint = "",
    std::map<std::string, std::string> resourceAttributes = {},
    std::map<std::string, std::string> headers = {});

  const MetricsBackend backend;
  const std::string serviceName;
  const std::chrono::milliseconds exportInterval;
  const std::chrono::milliseconds exportTimeout;
  const std::string otlpEndpoint;
  const std::map<std::string, std::string> resourceAttributes;
  const std::map<std::string, std::string> headers;
} MetricsConfig;

}  // namespace cta::telemetry::metrics
