#include "MetricsConfig.hpp"

#include <stdexcept>

namespace cta::telemetry::metrics {

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

MetricsConfig::MetricsConfig(const std::string& backendType,
                             std::string serviceName,
                             std::chrono::milliseconds exportInterval,
                             std::chrono::milliseconds exportTimeout,
                             std::string otlpEndpoint,
                             std::map<std::string, std::string> resourceAttributes,
                             std::map<std::string, std::string> headers)
    : backend(stringToMetricsBackend(backendType)),
      serviceName(std::move(serviceName)),
      exportInterval(exportInterval),
      exportTimeout(exportTimeout),
      otlpEndpoint(std::move(otlpEndpoint)),
      resourceAttributes(std::move(resourceAttributes)),
      headers(std::move(headers)) {
  if (serviceName.empty()) {
    throw std::invalid_argument("MetricConfig: serviceName is required for OTLP backend");
  }
  if (backend == MetricsBackend::OTLP) {
    if (otlpEndpoint.empty()) {
      throw std::invalid_argument("MetricConfig: otlpEndpoint is required for OTLP backend");
    }
  }
}

}  // namespace cta::telemetry::metrics
